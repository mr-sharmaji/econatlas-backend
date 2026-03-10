from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from pydantic import ValidationError

from app.schemas.tax_schema import TaxConfigResponse
from app.services import tax_fy, tax_official_source, tax_service

_REQUIRED_CALCULATORS = {"income_tax", "capital_gains", "advance_tax", "tds"}


@dataclass
class TaxSyncResult:
    status: str
    version: str | None
    message: str
    errors: list[str]


def _validate_monotonic_slabs(
    slabs: list[dict[str, Any]],
    *,
    label: str,
    errors: list[str],
) -> None:
    prev = -1.0
    for idx, slab in enumerate(slabs):
        upper = float(slab.get("upper_limit") or 0.0)
        rate = float(slab.get("rate") or 0.0)
        if upper <= prev:
            errors.append(f"{label}: slab[{idx}] upper_limit not increasing")
        if rate < 0.0 or rate > 1.0:
            errors.append(f"{label}: slab[{idx}] rate out of range")
        prev = upper


def _resolve_default_fy(
    *,
    default_fy: str,
    supported_ids: set[str],
    rules_by_fy: dict[str, Any],
) -> str:
    if default_fy and default_fy in supported_ids and default_fy in rules_by_fy:
        return default_fy
    if not rules_by_fy:
        return ""
    try:
        return max(rules_by_fy.keys(), key=tax_fy.fy_start_year)
    except Exception:
        return sorted(rules_by_fy.keys())[-1]


def _validate_semantics(payload: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
    errors: list[str] = []
    try:
        parsed = TaxConfigResponse(**payload)
    except ValidationError as exc:
        return None, [f"schema_validation_failed: {exc}"]

    normalized = parsed.model_dump()
    rules_by_fy = normalized.get("rules_by_fy") or {}
    if not isinstance(rules_by_fy, dict) or not rules_by_fy:
        return None, ["rules_by_fy_empty"]

    supported_raw = normalized.get("supported_fy") or []
    supported_ids: set[str] = set()
    deduped_supported: list[dict[str, str]] = []
    for item in supported_raw:
        if not isinstance(item, dict):
            continue
        fy_id = str(item.get("id") or "").strip()
        label = str(item.get("label") or "").strip()
        if not fy_id or fy_id in supported_ids:
            continue
        supported_ids.add(fy_id)
        deduped_supported.append({"id": fy_id, "label": label})

    if not deduped_supported:
        errors.append("supported_fy_empty")

    missing_supported = [fy for fy in rules_by_fy if fy not in supported_ids]
    if missing_supported:
        errors.append(f"rules_missing_supported_fy_entries: {', '.join(sorted(missing_supported))}")

    missing_rules = [fy for fy in supported_ids if fy not in rules_by_fy]
    if missing_rules:
        errors.append(f"supported_fy_missing_rules: {', '.join(sorted(missing_rules))}")

    for fy, rule_set in rules_by_fy.items():
        calculators = set((rule_set or {}).keys())
        missing = sorted(_REQUIRED_CALCULATORS.difference(calculators))
        if missing:
            errors.append(f"{fy}: missing_calculators: {', '.join(missing)}")
            continue

        income = rule_set.get("income_tax") or {}
        capital = rule_set.get("capital_gains") or {}
        advance = rule_set.get("advance_tax") or {}
        tds = rule_set.get("tds") or {}

        if not {"old", "new"}.issubset(set((income.get("standard_deduction") or {}).keys())):
            errors.append(f"{fy}: standard_deduction must include old/new")
        _validate_monotonic_slabs(
            list(income.get("old_slabs") or []),
            label=f"{fy}:old_slabs",
            errors=errors,
        )
        _validate_monotonic_slabs(
            list(income.get("new_slabs") or []),
            label=f"{fy}:new_slabs",
            errors=errors,
        )
        cess = float(income.get("cess_rate") or 0.0)
        if cess < 0 or cess > 1:
            errors.append(f"{fy}: cess_rate out of range")

        assets = capital.get("assets") or {}
        if not assets:
            errors.append(f"{fy}: capital_gains assets empty")
        for asset_key, asset_rule in assets.items():
            try:
                stcg = float(asset_rule.get("stcg_rate") or 0.0)
                ltcg = float(asset_rule.get("ltcg_rate") or 0.0)
                holding = int(float(asset_rule.get("holding_period_months") or 0))
            except (TypeError, ValueError):
                errors.append(f"{fy}:{asset_key}: invalid numeric value")
                continue
            if stcg < 0 or stcg > 1 or ltcg < 0 or ltcg > 1:
                errors.append(f"{fy}:{asset_key}: gains rates out of range")
            if holding <= 0:
                errors.append(f"{fy}:{asset_key}: holding_period_months must be > 0")

        installments = list(advance.get("installments") or [])
        if not installments:
            errors.append(f"{fy}: advance_tax installments empty")
        prev_percent = -1.0
        for idx, installment in enumerate(installments):
            percent = float(installment.get("cumulative_percent") or 0.0)
            if percent <= prev_percent:
                errors.append(f"{fy}:advance_tax installments not increasing at index {idx}")
            prev_percent = percent
        if installments and abs(prev_percent - 100.0) > 1e-6:
            errors.append(f"{fy}: advance_tax final cumulative_percent must be 100")

        sections = list(tds.get("sections") or [])
        if not sections:
            errors.append(f"{fy}: tds sections empty")
        for idx, section in enumerate(sections):
            rate = float(section.get("rate") or 0.0)
            threshold = float(section.get("threshold") or 0.0)
            if rate < 0 or rate > 1:
                errors.append(f"{fy}:tds section[{idx}] rate out of range")
            if threshold < 0:
                errors.append(f"{fy}:tds section[{idx}] threshold must be >= 0")

    if errors:
        return None, errors

    normalized["supported_fy"] = deduped_supported
    normalized["default_fy"] = _resolve_default_fy(
        default_fy=str(normalized.get("default_fy") or "").strip(),
        supported_ids=supported_ids,
        rules_by_fy=rules_by_fy,
    )
    if not normalized["default_fy"]:
        return None, ["unable_to_resolve_default_fy"]
    normalized["hash"] = tax_service.compute_config_hash(normalized)
    return normalized, []


async def run_tax_sync_cycle(
    *,
    timeout_seconds: int = 30,
    now_utc: datetime | None = None,
) -> TaxSyncResult:
    now = now_utc or datetime.now(timezone.utc)
    try:
        bundle = await tax_official_source.fetch_official_tax_bundle(
            timeout_seconds=timeout_seconds,
            now_utc=now,
        )
    except Exception as exc:
        return TaxSyncResult(
            status="failed",
            version=None,
            message="Failed to fetch ClearTax tax sources.",
            errors=[f"official_source_fetch_failed: {exc}"],
        )

    staged = await tax_service.upsert_tax_config(
        bundle.config_payload,
        source=f"tax_scheduler_{bundle.source_name}",
        source_mode="official_web",
        activate=False,
    )
    version = str(staged["version"])

    semantically_validated, pass1_errors = _validate_semantics(staged["payload"])
    if pass1_errors:
        reason = "; ".join(pass1_errors[:8])
        await tax_service.mark_config_validation(
            version=version,
            status="failed_pass1",
            reason=reason,
            success=False,
        )
        active_version = await tax_service.get_active_config_version()
        if active_version:
            await tax_service.archive_inactive_configs(active_version=active_version)
        return TaxSyncResult(
            status="failed",
            version=version,
            message="Tax pass1 validation failed.",
            errors=pass1_errors,
        )

    saved = await tax_service.upsert_tax_config(
        semantically_validated,
        source="tax_scheduler_cleartax_web",
        source_mode="official_web",
        activate=True,
    )
    active_version = str(saved["version"])
    await tax_service.archive_inactive_configs(active_version=active_version)
    await tax_service.mark_config_validation(
        version=active_version,
        status="passed_pass1",
        reason="pass1",
        success=True,
    )
    return TaxSyncResult(
        status="activated",
        version=active_version,
        message=f"Activated tax config {active_version}",
        errors=[],
    )

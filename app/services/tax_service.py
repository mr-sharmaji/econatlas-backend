from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Any

from app.core.database import get_pool
from app.services import tax_fy

_CONFIG_TABLE = "tax_config_versions"
_DEFAULT_HELPER_POINTS = {
    "hub": [],
    "income_tax": [],
    "capital_gains": [],
    "advance_tax": [],
    "tds": [],
}


class TaxConfigNotFoundError(RuntimeError):
    """Raised when no tax configuration exists in the database."""


def _as_json(value: Any, *, fallback: Any) -> Any:
    if value is None:
        return fallback
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed
        except json.JSONDecodeError:
            return fallback
    return fallback


def _as_iso(value: Any) -> str | None:
    if isinstance(value, datetime):
        return value.isoformat()
    return None


def _payload_for_hash(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "version": payload.get("version"),
        "supported_fy": payload.get("supported_fy"),
        "default_fy": payload.get("default_fy"),
        "disclaimer": payload.get("disclaimer"),
        "helper_points": payload.get("helper_points"),
        "rounding_policy": payload.get("rounding_policy"),
        "rules_by_fy": payload.get("rules_by_fy"),
    }


def compute_config_hash(payload: dict[str, Any]) -> str:
    raw = json.dumps(_payload_for_hash(payload), sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(raw).hexdigest()[:16]


def _normalize_config_payload_from_row(row: Any) -> dict[str, Any]:
    payload = {
        "version": row["version"],
        "supported_fy": _as_json(row["supported_fy"], fallback=[]),
        "default_fy": row["default_fy"],
        "last_synced_at": _as_iso(row["last_sync_success_at"]),
        "disclaimer": row["disclaimer"],
        "helper_points": _as_json(row["helper_points"], fallback=dict(_DEFAULT_HELPER_POINTS)),
        "rounding_policy": _as_json(row["rounding_policy"], fallback={}),
        "rules_by_fy": _as_json(row["rules_by_fy"], fallback={}),
    }
    payload["hash"] = row["content_hash"] or compute_config_hash(payload)
    return payload


def _normalize_config_row(row: Any) -> dict[str, Any]:
    payload = _normalize_config_payload_from_row(row)
    return {
        "version": row["version"],
        "is_active": bool(row["is_active"]),
        "source": row["source"],
        "source_mode": row["source_mode"],
        "archived_at": row["archived_at"],
        "last_validation_status": row["last_validation_status"],
        "last_validation_reason": row["last_validation_reason"],
        "last_sync_attempt_at": row["last_sync_attempt_at"],
        "last_sync_success_at": row["last_sync_success_at"],
        "updated_at": row["updated_at"],
        "payload": payload,
    }


async def _get_active_or_latest_non_archived_row() -> Any:
    pool = await get_pool()
    row = await pool.fetchrow(
        f"""
        SELECT version, default_fy, disclaimer, supported_fy, rounding_policy,
               helper_points, rules_by_fy, content_hash, source, source_mode,
               is_active, archived_at,
               last_validation_status, last_validation_reason,
               last_sync_attempt_at, last_sync_success_at, updated_at
        FROM {_CONFIG_TABLE}
        WHERE is_active = TRUE
          AND archived_at IS NULL
        ORDER BY updated_at DESC
        LIMIT 1
        """
    )
    if row is None:
        row = await pool.fetchrow(
            f"""
            SELECT version, default_fy, disclaimer, supported_fy, rounding_policy,
                   helper_points, rules_by_fy, content_hash, source, source_mode,
                   is_active, archived_at,
                   last_validation_status, last_validation_reason,
                   last_sync_attempt_at, last_sync_success_at, updated_at
            FROM {_CONFIG_TABLE}
            WHERE archived_at IS NULL
            ORDER BY updated_at DESC
            LIMIT 1
            """
        )
    return row


async def get_tax_config_row_by_version(*, version: str, include_archived: bool = False) -> dict[str, Any] | None:
    pool = await get_pool()
    where_archived = "" if include_archived else "AND archived_at IS NULL"
    row = await pool.fetchrow(
        f"""
        SELECT version, default_fy, disclaimer, supported_fy, rounding_policy,
               helper_points, rules_by_fy, content_hash, source, source_mode,
               is_active, archived_at,
               last_validation_status, last_validation_reason,
               last_sync_attempt_at, last_sync_success_at, updated_at
        FROM {_CONFIG_TABLE}
        WHERE version = $1
          {where_archived}
        ORDER BY updated_at DESC
        LIMIT 1
        """,
        version,
    )
    if row is None:
        return None
    return _normalize_config_row(row)


async def get_latest_staged_config_row(*, source_mode: str | None = "official_web") -> dict[str, Any] | None:
    pool = await get_pool()
    if source_mode is None:
        source_clause = ""
        params: list[Any] = []
    else:
        source_clause = "AND COALESCE(source_mode, 'official_web') = $1"
        params = [source_mode]
    row = await pool.fetchrow(
        f"""
        SELECT version, default_fy, disclaimer, supported_fy, rounding_policy,
               helper_points, rules_by_fy, content_hash, source, source_mode,
               is_active, archived_at,
               last_validation_status, last_validation_reason,
               last_sync_attempt_at, last_sync_success_at, updated_at
        FROM {_CONFIG_TABLE}
        WHERE is_active = FALSE
          AND archived_at IS NULL
          {source_clause}
        ORDER BY updated_at DESC
        LIMIT 1
        """,
        *params,
    )
    if row is None:
        return None
    return _normalize_config_row(row)


async def get_tax_config_payload() -> dict[str, Any]:
    row = await _get_active_or_latest_non_archived_row()
    if row is None:
        raise TaxConfigNotFoundError("Tax config not seeded in database.")
    payload = _normalize_config_payload_from_row(row)
    supported_fy = payload.get("supported_fy")
    if not isinstance(supported_fy, list):
        supported_fy = []
    rules_by_fy = payload.get("rules_by_fy")
    available_rule_ids = set(rules_by_fy.keys()) if isinstance(rules_by_fy, dict) else set()
    resolved_default = tax_fy.resolve_default_fy(
        supported_fy=supported_fy,
        available_rule_ids=available_rule_ids or None,
    )
    if resolved_default:
        payload["default_fy"] = resolved_default
    payload["hash"] = compute_config_hash(payload)
    return payload


async def upsert_tax_config(
    payload: dict[str, Any],
    *,
    source: str | None = None,
    source_mode: str | None = None,
    activate: bool = True,
) -> dict[str, Any]:
    version = str(payload.get("version") or "").strip()
    default_fy = str(payload.get("default_fy") or "").strip()
    disclaimer = str(payload.get("disclaimer") or "").strip()
    helper_points = payload.get("helper_points")
    supported_fy = payload.get("supported_fy")
    rounding_policy = payload.get("rounding_policy")
    rules_by_fy = payload.get("rules_by_fy")

    if not version:
        raise ValueError("Tax config version is required.")
    if not default_fy:
        raise ValueError("Tax config default_fy is required.")
    if not disclaimer:
        raise ValueError("Tax config disclaimer is required.")
    if not isinstance(helper_points, dict):
        raise ValueError("Tax config helper_points must be an object.")
    if not isinstance(supported_fy, list) or not supported_fy:
        raise ValueError("Tax config supported_fy must be a non-empty list.")
    if not isinstance(rounding_policy, dict):
        raise ValueError("Tax config rounding_policy must be an object.")
    if not isinstance(rules_by_fy, dict) or not rules_by_fy:
        raise ValueError("Tax config rules_by_fy must be a non-empty object.")
    if default_fy not in rules_by_fy:
        raise ValueError("Tax config default_fy must exist in rules_by_fy.")

    payload_for_hash = {
        "version": version,
        "supported_fy": supported_fy,
        "default_fy": default_fy,
        "disclaimer": disclaimer,
        "helper_points": helper_points,
        "rounding_policy": rounding_policy,
        "rules_by_fy": rules_by_fy,
    }
    content_hash = str(payload.get("hash") or "").strip() or compute_config_hash(payload_for_hash)

    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                f"""
                INSERT INTO {_CONFIG_TABLE} (
                    version, default_fy, disclaimer, supported_fy,
                    helper_points, rounding_policy, rules_by_fy, content_hash,
                    source, source_mode, is_active,
                    created_at, updated_at, archived_at
                )
                VALUES (
                    $1, $2, $3, $4::jsonb, $5::jsonb, $6::jsonb, $7,
                    $8, $9, $10, $11,
                    NOW(), NOW(), NULL
                )
                ON CONFLICT (version)
                DO UPDATE SET
                    default_fy = EXCLUDED.default_fy,
                    disclaimer = EXCLUDED.disclaimer,
                    supported_fy = EXCLUDED.supported_fy,
                    helper_points = EXCLUDED.helper_points,
                    rounding_policy = EXCLUDED.rounding_policy,
                    rules_by_fy = EXCLUDED.rules_by_fy,
                    content_hash = EXCLUDED.content_hash,
                    source = EXCLUDED.source,
                    source_mode = COALESCE(EXCLUDED.source_mode, {_CONFIG_TABLE}.source_mode),
                    is_active = {_CONFIG_TABLE}.is_active,
                    archived_at = NULL,
                    updated_at = NOW()
                """,
                version,
                default_fy,
                disclaimer,
                json.dumps(supported_fy),
                json.dumps(helper_points),
                json.dumps(rounding_policy),
                json.dumps(rules_by_fy),
                content_hash,
                source,
                source_mode,
                activate,
            )
            if activate:
                await conn.execute(
                    f"""
                    UPDATE {_CONFIG_TABLE}
                    SET is_active = (version = $1),
                        archived_at = CASE WHEN version = $1 THEN NULL ELSE archived_at END,
                        updated_at = NOW()
                    WHERE is_active = TRUE OR version = $1
                    """,
                    version,
                )

    saved = await get_tax_config_row_by_version(version=version, include_archived=True)
    if saved is None:
        raise TaxConfigNotFoundError(f"Unable to read tax config after upsert: {version}")
    payload_out = saved["payload"]
    payload_out["hash"] = compute_config_hash(payload_out)
    return payload_out


async def mark_config_validation(
    *,
    version: str,
    status: str,
    reason: str,
    success: bool,
) -> None:
    pool = await get_pool()
    if success:
        await pool.execute(
            f"""
            UPDATE {_CONFIG_TABLE}
            SET last_validation_status = $2,
                last_validation_reason = $3,
                last_sync_attempt_at = NOW(),
                last_sync_success_at = NOW(),
                updated_at = NOW()
            WHERE version = $1
            """,
            version,
            status,
            reason,
        )
        return

    await pool.execute(
        f"""
        UPDATE {_CONFIG_TABLE}
        SET last_validation_status = $2,
            last_validation_reason = $3,
            last_sync_attempt_at = NOW(),
            updated_at = NOW()
        WHERE version = $1
        """,
        version,
        status,
        reason,
    )


async def archive_inactive_configs(*, active_version: str) -> int:
    pool = await get_pool()
    status = await pool.execute(
        f"""
        DELETE FROM {_CONFIG_TABLE}
        WHERE version <> $1
        """,
        active_version,
    )
    try:
        return int(str(status).split()[-1])
    except Exception:
        return 0


async def get_active_config_version() -> str | None:
    pool = await get_pool()
    row = await pool.fetchrow(
        f"""
        SELECT version
        FROM {_CONFIG_TABLE}
        WHERE is_active = TRUE
          AND archived_at IS NULL
        ORDER BY updated_at DESC
        LIMIT 1
        """
    )
    return str(row["version"]) if row else None

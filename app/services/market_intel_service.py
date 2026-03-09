from __future__ import annotations

from datetime import datetime, timezone

from app.core.asset_catalog import (
    benchmark_for_region,
    default_watchlist_assets,
    get_asset_meta,
    list_asset_catalog,
)
from app.core.database import get_pool, parse_ts
from app.services import market_service

_QUALITY_SCORE = {
    "primary": 1.0,
    "fallback": 0.55,
    "unknown": 0.75,
    None: 0.75,
}


async def get_asset_catalog(
    *,
    region: str | None = None,
    instrument_type: str | None = None,
) -> list[dict]:
    return [i.__dict__ for i in list_asset_catalog(region=region, instrument_type=instrument_type)]


def _normalize_assets(raw_assets: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for a in raw_assets:
        name = (a or "").strip()
        if not name or name in seen:
            continue
        seen.add(name)
        out.append(name)
    return out


def _validate_assets_exist(assets: list[str]) -> list[str]:
    unknown = [a for a in assets if get_asset_meta(a) is None]
    return unknown


async def get_or_seed_watchlist(device_id: str) -> list[str]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT asset
            FROM device_watchlists
            WHERE device_id = $1
            ORDER BY position ASC, updated_at ASC
            """,
            device_id,
        )
    assets = [str(r["asset"]) for r in rows]
    if assets:
        return assets
    defaults = default_watchlist_assets()
    await put_watchlist(device_id=device_id, assets=defaults)
    return defaults


async def put_watchlist(device_id: str, assets: list[str]) -> list[str]:
    normalized = _normalize_assets(assets)
    unknown = _validate_assets_exist(normalized)
    if unknown:
        raise ValueError(f"Unknown assets in watchlist: {', '.join(unknown)}")

    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("DELETE FROM device_watchlists WHERE device_id = $1", device_id)
            for idx, asset in enumerate(normalized):
                await conn.execute(
                    """
                    INSERT INTO device_watchlists (device_id, asset, position, updated_at)
                    VALUES ($1, $2, $3, NOW())
                    """,
                    device_id,
                    asset,
                    idx,
                )
    return normalized


def _to_float(value) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _quality_score(name: str | None) -> float:
    return _QUALITY_SCORE.get((name or "").strip().lower(), 0.75)


def _preset_score(
    *,
    preset: str,
    change_pct: float,
    relative_strength: float,
    is_stale: bool,
    quality: str | None,
    instrument_type: str,
) -> float:
    abs_change = abs(change_pct)
    abs_rel = abs(relative_strength)
    quality_factor = _quality_score(quality)
    stale_factor = 0.55 if is_stale else 1.0

    if preset == "reversal":
        raw = (-change_pct * 0.9) + (abs_change * 0.65) + (abs_rel * 0.25)
    elif preset == "volatility":
        raw = (abs_change * 1.25) + (abs_rel * 0.35)
    elif preset == "macro-sensitive":
        macro_weight = 1.3 if instrument_type in {"currency", "commodity", "bond_yield"} else 0.7
        raw = (abs_change * 0.9) + (abs_rel * 0.4) + macro_weight
    else:  # momentum
        raw = (change_pct * 0.95) + (relative_strength * 0.4) + (abs_change * 0.2)

    return round(raw * quality_factor * stale_factor, 4)


def _signal_tags(
    *,
    change_pct: float,
    relative_strength: float,
    is_stale: bool,
    quality: str | None,
) -> list[str]:
    tags: list[str] = []
    if change_pct >= 1.0:
        tags.append("momentum_up")
    elif change_pct <= -1.0:
        tags.append("momentum_down")
    if relative_strength >= 1.0:
        tags.append("outperforming_benchmark")
    elif relative_strength <= -1.0:
        tags.append("underperforming_benchmark")
    if abs(change_pct) >= 2.0:
        tags.append("high_volatility")
    if is_stale:
        tags.append("stale_data")
    if (quality or "").lower() == "fallback":
        tags.append("fallback_feed")
    if not tags:
        tags.append("stable")
    return tags


async def get_screener(
    *,
    preset: str = "momentum",
    region: str | None = None,
    instrument_type: str | None = None,
    limit: int = 25,
    min_quality: float = 0.0,
) -> list[dict]:
    latest = await market_service.get_latest_prices(instrument_type=instrument_type)
    latest_by_asset = {str(r.get("asset")): r for r in latest}

    preset_norm = (preset or "momentum").strip().lower()
    if preset_norm not in {"momentum", "reversal", "volatility", "macro-sensitive"}:
        preset_norm = "momentum"
    region_norm = (region or "").strip().lower()

    out: list[dict] = []
    for row in latest:
        asset = str(row.get("asset") or "")
        meta = get_asset_meta(asset)
        if not meta:
            continue
        if region_norm and meta.region.lower() != region_norm:
            continue
        if instrument_type and (row.get("instrument_type") or "") != instrument_type:
            continue

        change_pct = _to_float(row.get("change_percent")) or 0.0
        benchmark_asset = benchmark_for_region(meta.region)
        benchmark_change = None
        if benchmark_asset and benchmark_asset in latest_by_asset:
            benchmark_change = _to_float(latest_by_asset[benchmark_asset].get("change_percent"))
        relative_strength = change_pct - (benchmark_change or 0.0)

        quality = row.get("data_quality")
        if _quality_score(quality) < max(0.0, min(1.0, min_quality)):
            continue
        is_stale = bool(row.get("is_stale"))
        score = _preset_score(
            preset=preset_norm,
            change_pct=change_pct,
            relative_strength=relative_strength,
            is_stale=is_stale,
            quality=quality,
            instrument_type=str(row.get("instrument_type") or ""),
        )
        out.append(
            {
                "asset": asset,
                "instrument_type": row.get("instrument_type"),
                "region": meta.region,
                "exchange": meta.exchange,
                "price": float(row.get("price") or 0.0),
                "change_percent": change_pct,
                "score": score,
                "signal_tags": _signal_tags(
                    change_pct=change_pct,
                    relative_strength=relative_strength,
                    is_stale=is_stale,
                    quality=quality,
                ),
                "market_phase": row.get("market_phase"),
                "is_stale": row.get("is_stale"),
                "last_tick_timestamp": parse_ts(row.get("last_tick_timestamp") or row.get("timestamp")),
                "data_quality": quality,
                "change_window": row.get("change_window"),
                "benchmark_asset": benchmark_asset,
                "benchmark_change_percent": benchmark_change,
                "relative_strength": round(relative_strength, 4),
            }
        )
    out.sort(key=lambda r: (float(r["score"]), float(r.get("change_percent") or 0.0)), reverse=True)
    if limit > 0:
        out = out[:limit]
    return out


def _add_health_bucket(bucket: dict, phase: str, latency: float | None) -> None:
    bucket["total"] += 1
    if phase == "live":
        bucket["live"] += 1
    elif phase == "stale":
        bucket["stale"] += 1
    else:
        bucket["closed"] += 1
    if latency is not None:
        bucket["_latency_sum"] += latency
        bucket["_latency_count"] += 1


def _finalize_bucket(bucket: dict) -> dict:
    avg = None
    if bucket["_latency_count"] > 0:
        avg = round(bucket["_latency_sum"] / bucket["_latency_count"], 2)
    return {
        "total": bucket["total"],
        "live": bucket["live"],
        "stale": bucket["stale"],
        "closed": bucket["closed"],
        "avg_latency_seconds": avg,
    }


async def get_data_health() -> dict:
    latest = await market_service.get_latest_prices()
    now = datetime.now(timezone.utc)

    by_region: dict[str, dict] = {}
    by_type: dict[str, dict] = {}
    quality_counts: dict[str, int] = {}
    stale_assets = 0
    latency_values: list[float] = []

    for row in latest:
        asset = str(row.get("asset") or "")
        inst = str(row.get("instrument_type") or "unknown")
        meta = get_asset_meta(asset)
        region = meta.region if meta else "Global"
        phase = str(row.get("market_phase") or "closed").lower()
        if phase == "stale":
            stale_assets += 1

        tick = parse_ts(row.get("last_tick_timestamp") or row.get("timestamp"))
        latency = None
        if tick is not None:
            if tick.tzinfo is None:
                tick = tick.replace(tzinfo=timezone.utc)
            latency = max(0.0, (now - tick).total_seconds())
            latency_values.append(latency)

        quality = str(row.get("data_quality") or "unknown").lower()
        quality_counts[quality] = quality_counts.get(quality, 0) + 1

        if region not in by_region:
            by_region[region] = {"total": 0, "live": 0, "stale": 0, "closed": 0, "_latency_sum": 0.0, "_latency_count": 0}
        if inst not in by_type:
            by_type[inst] = {"total": 0, "live": 0, "stale": 0, "closed": 0, "_latency_sum": 0.0, "_latency_count": 0}

        _add_health_bucket(by_region[region], phase, latency)
        _add_health_bucket(by_type[inst], phase, latency)

    avg_latency = round(sum(latency_values) / len(latency_values), 2) if latency_values else None
    return {
        "timestamp": now,
        "total_assets": len(latest),
        "stale_assets": stale_assets,
        "avg_latency_seconds": avg_latency,
        "by_region": {k: _finalize_bucket(v) for k, v in sorted(by_region.items())},
        "by_instrument_type": {k: _finalize_bucket(v) for k, v in sorted(by_type.items())},
        "quality_counts": dict(sorted(quality_counts.items())),
    }

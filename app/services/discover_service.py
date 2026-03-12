from __future__ import annotations

import json
from datetime import date, datetime, timezone
from typing import Literal

from app.core.database import get_pool, parse_ts, record_to_dict

STOCK_TABLE = "discover_stock_snapshots"
MF_TABLE = "discover_mutual_fund_snapshots"

SourceStatus = Literal["primary", "fallback", "limited"]


def _to_float(value) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _to_date(value) -> date | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if not text:
        return None
    for token in ("%d-%b-%Y", "%Y-%m-%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(text, token).date()
        except ValueError:
            continue
    return None


def _to_jsonb(value, default):
    payload = value if value is not None else default
    return json.dumps(payload, separators=(",", ":"), ensure_ascii=True)


def _normalize_source_status(value: str | None) -> SourceStatus:
    v = str(value or "").strip().lower()
    if v == "primary":
        return "primary"
    if v == "fallback":
        return "fallback"
    return "limited"


def _resolve_batch_source_status(rows: list[dict]) -> SourceStatus:
    if not rows:
        return "limited"
    counts = {"primary": 0, "fallback": 0, "limited": 0}
    for row in rows:
        cur = _normalize_source_status(row.get("source_status"))
        counts[cur] += 1
    total = max(1, len(rows))
    primary_ratio = counts["primary"] / total
    if primary_ratio >= 0.6:
        return "primary"
    if counts["fallback"] > 0 or counts["primary"] > 0:
        return "fallback"
    return "limited"


def _stock_breakdown_payload(row: dict) -> dict:
    momentum = _to_float(row.get("score_momentum")) or 0.0
    liquidity = _to_float(row.get("score_liquidity")) or 0.0
    fundamentals = _to_float(row.get("score_fundamentals")) or 0.0
    combined_signal = ((momentum + liquidity) / 2.0) if (momentum or liquidity) else 0.0
    return {
        "momentum": round(momentum, 2),
        "liquidity": round(liquidity, 2),
        "fundamentals": round(fundamentals, 2),
        "combined_signal": round(combined_signal, 2),
    }


def _mf_breakdown_payload(row: dict) -> dict:
    return {
        "return_score": round(_to_float(row.get("score_return")) or 0.0, 2),
        "risk_score": round(_to_float(row.get("score_risk")) or 0.0, 2),
        "cost_score": round(_to_float(row.get("score_cost")) or 0.0, 2),
        "consistency_score": round(_to_float(row.get("score_consistency")) or 0.0, 2),
    }


def _stock_why_ranked(row: dict, sector_stats: dict | None = None) -> list[str]:
    reasons: list[str] = []
    pct = _to_float(row.get("percent_change"))
    if pct is not None and pct >= 2.0:
        reasons.append(f"Strong momentum at +{pct:.1f}% today.")
    elif pct is not None and pct <= -2.0:
        reasons.append(f"High-volatility signal at {pct:.1f}% today.")

    tv = _to_float(row.get("traded_value"))
    if tv is not None and tv >= 500_000_000:
        tv_cr = tv / 10_000_000
        reasons.append(f"High traded value of {tv_cr:.0f} Cr supports liquidity.")

    roe = _to_float(row.get("roe"))
    if roe is not None and roe >= 15:
        if sector_stats and sector_stats.get("avg_roe"):
            reasons.append(f"ROE of {roe:.1f}% vs sector avg {sector_stats['avg_roe']:.1f}%.")
        else:
            reasons.append(f"ROE of {roe:.1f}% indicates strong profitability.")

    roce = _to_float(row.get("roce"))
    if roce is not None and roce >= 15 and len(reasons) < 3:
        reasons.append(f"ROCE of {roce:.1f}% shows efficient capital use.")

    dte = _to_float(row.get("debt_to_equity"))
    if dte is not None and dte <= 0.6:
        reasons.append(f"Conservative D/E ratio of {dte:.2f}.")

    eps = _to_float(row.get("eps"))
    if eps is not None and eps < 0:
        reasons.append("Negative EPS — profitability concern.")

    pe = _to_float(row.get("pe_ratio"))
    if pe is not None and pe > 0 and sector_stats and sector_stats.get("median_pe"):
        med = sector_stats["median_pe"]
        if pe < med * 0.8:
            reasons.append(f"PE of {pe:.1f} is below sector median of {med:.1f}.")

    status = _normalize_source_status(row.get("source_status"))
    if status != "primary":
        reasons.append("Some metrics are based on fallback or limited data.")
    if not reasons:
        reasons.append("Balanced rank across momentum, liquidity, and fundamentals.")
    return reasons[:4]


def _mf_why_ranked(row: dict, category_stats: dict | None = None) -> list[str]:
    reasons: list[str] = []
    ret3 = _to_float(row.get("returns_3y"))
    if ret3 is not None and ret3 >= 12:
        if category_stats and category_stats.get("avg_ret3y"):
            reasons.append(f"3Y return of {ret3:.1f}% vs category avg {category_stats['avg_ret3y']:.1f}%.")
        else:
            reasons.append(f"Strong 3Y return of {ret3:.1f}%.")

    ret5 = _to_float(row.get("returns_5y"))
    if ret5 is not None and ret5 >= 12:
        reasons.append(f"Consistent 5Y return of {ret5:.1f}%.")

    expense = _to_float(row.get("expense_ratio"))
    if expense is not None and expense <= 1.0:
        reasons.append(f"Low expense ratio of {expense:.2f}% supports compounding.")

    risk = str(row.get("risk_level") or "").strip().lower()
    if risk in {"low", "moderately low"}:
        reasons.append(f"Risk level: {row.get('risk_level', '').strip()}.")

    sharpe = _to_float(row.get("sharpe"))
    if sharpe is not None and sharpe >= 1.5:
        reasons.append(f"Sharpe ratio of {sharpe:.2f} indicates strong risk-adjusted returns.")

    aum = _to_float(row.get("aum_cr"))
    if aum is not None and aum < 100:
        reasons.append("Small fund size (< 100 Cr) — metrics may be less stable.")

    if str(row.get("plan_type") or "").strip().lower() == "direct":
        reasons.append("Direct plan selected for lower cost drag.")

    status = _normalize_source_status(row.get("source_status"))
    if status != "primary":
        reasons.append("Some advanced metrics are unavailable in fallback mode.")
    if not reasons:
        reasons.append("Balanced risk-return-cost score within category.")
    return reasons[:4]


def _decorate_stock_row(row: dict, sector_stats: dict | None = None) -> dict:
    item = dict(row)
    item["source_status"] = _normalize_source_status(item.get("source_status"))
    item["score_breakdown"] = _stock_breakdown_payload(item)
    tags = item.get("tags")
    item["tags"] = tags if isinstance(tags, list) else []
    item["why_ranked"] = _stock_why_ranked(item, sector_stats)
    return item


def _decorate_mf_row(row: dict, category_stats: dict | None = None) -> dict:
    item = dict(row)
    item["source_status"] = _normalize_source_status(item.get("source_status"))
    item["score_breakdown"] = _mf_breakdown_payload(item)
    tags = item.get("tags")
    item["tags"] = tags if isinstance(tags, list) else []
    item["why_ranked"] = _mf_why_ranked(item, category_stats)
    return item


async def upsert_discover_stock_snapshots(rows: list[dict]) -> int:
    if not rows:
        return 0
    pool = await get_pool()
    count = 0
    async with pool.acquire() as conn:
        for row in rows:
            await conn.execute(
                f"""
                INSERT INTO {STOCK_TABLE}
                (
                    market, symbol, display_name, sector, last_price, point_change, percent_change,
                    volume, traded_value, pe_ratio, roe, roce, debt_to_equity, price_to_book, eps,
                    score, score_momentum, score_liquidity, score_fundamentals,
                    score_breakdown, tags, source_status, source_timestamp, ingested_at,
                    primary_source, secondary_source
                )
                VALUES (
                    $1, $2, $3, $4, $5, $6, $7,
                    $8, $9, $10, $11, $12, $13, $14, $15,
                    $16, $17, $18, $19,
                    $20, $21, $22, $23, NOW(),
                    $24, $25
                )
                ON CONFLICT (symbol)
                DO UPDATE SET
                    market = EXCLUDED.market,
                    display_name = EXCLUDED.display_name,
                    sector = EXCLUDED.sector,
                    last_price = EXCLUDED.last_price,
                    point_change = EXCLUDED.point_change,
                    percent_change = EXCLUDED.percent_change,
                    volume = EXCLUDED.volume,
                    traded_value = EXCLUDED.traded_value,
                    pe_ratio = EXCLUDED.pe_ratio,
                    roe = EXCLUDED.roe,
                    roce = EXCLUDED.roce,
                    debt_to_equity = EXCLUDED.debt_to_equity,
                    price_to_book = EXCLUDED.price_to_book,
                    eps = EXCLUDED.eps,
                    score = EXCLUDED.score,
                    score_momentum = EXCLUDED.score_momentum,
                    score_liquidity = EXCLUDED.score_liquidity,
                    score_fundamentals = EXCLUDED.score_fundamentals,
                    score_breakdown = EXCLUDED.score_breakdown,
                    tags = EXCLUDED.tags,
                    source_status = EXCLUDED.source_status,
                    source_timestamp = EXCLUDED.source_timestamp,
                    ingested_at = NOW(),
                    primary_source = EXCLUDED.primary_source,
                    secondary_source = EXCLUDED.secondary_source
                """,
                str(row.get("market") or "IN"),
                str(row.get("symbol") or ""),
                str(row.get("display_name") or row.get("symbol") or ""),
                row.get("sector"),
                _to_float(row.get("last_price")) or 0.0,
                _to_float(row.get("point_change")),
                _to_float(row.get("percent_change")),
                _to_int(row.get("volume")),
                _to_float(row.get("traded_value")),
                _to_float(row.get("pe_ratio")),
                _to_float(row.get("roe")),
                _to_float(row.get("roce")),
                _to_float(row.get("debt_to_equity")),
                _to_float(row.get("price_to_book")),
                _to_float(row.get("eps")),
                _to_float(row.get("score")) or 0.0,
                _to_float(row.get("score_momentum")) or 0.0,
                _to_float(row.get("score_liquidity")) or 0.0,
                _to_float(row.get("score_fundamentals")) or 0.0,
                _to_jsonb(row.get("score_breakdown"), _stock_breakdown_payload(row)),
                _to_jsonb(row.get("tags"), []),
                _normalize_source_status(row.get("source_status")),
                parse_ts(row.get("source_timestamp")) or datetime.now(timezone.utc),
                row.get("primary_source"),
                row.get("secondary_source"),
            )
            count += 1
    return count


async def upsert_discover_mutual_fund_snapshots(rows: list[dict]) -> int:
    if not rows:
        return 0
    pool = await get_pool()
    count = 0
    async with pool.acquire() as conn:
        for row in rows:
            await conn.execute(
                f"""
                INSERT INTO {MF_TABLE}
                (
                    scheme_code, scheme_name, amc, category, sub_category, plan_type, option_type,
                    nav, nav_date, expense_ratio, aum_cr, risk_level,
                    returns_1y, returns_3y, returns_5y, std_dev, sharpe, sortino,
                    score, score_return, score_risk, score_cost, score_consistency,
                    score_breakdown, tags, source_status, source_timestamp, ingested_at,
                    primary_source, secondary_source
                )
                VALUES (
                    $1, $2, $3, $4, $5, $6, $7,
                    $8, $9, $10, $11, $12,
                    $13, $14, $15, $16, $17, $18,
                    $19, $20, $21, $22, $23,
                    $24, $25, $26, $27, NOW(),
                    $28, $29
                )
                ON CONFLICT (scheme_code)
                DO UPDATE SET
                    scheme_name = EXCLUDED.scheme_name,
                    amc = EXCLUDED.amc,
                    category = EXCLUDED.category,
                    sub_category = EXCLUDED.sub_category,
                    plan_type = EXCLUDED.plan_type,
                    option_type = EXCLUDED.option_type,
                    nav = EXCLUDED.nav,
                    nav_date = EXCLUDED.nav_date,
                    expense_ratio = EXCLUDED.expense_ratio,
                    aum_cr = EXCLUDED.aum_cr,
                    risk_level = EXCLUDED.risk_level,
                    returns_1y = EXCLUDED.returns_1y,
                    returns_3y = EXCLUDED.returns_3y,
                    returns_5y = EXCLUDED.returns_5y,
                    std_dev = EXCLUDED.std_dev,
                    sharpe = EXCLUDED.sharpe,
                    sortino = EXCLUDED.sortino,
                    score = EXCLUDED.score,
                    score_return = EXCLUDED.score_return,
                    score_risk = EXCLUDED.score_risk,
                    score_cost = EXCLUDED.score_cost,
                    score_consistency = EXCLUDED.score_consistency,
                    score_breakdown = EXCLUDED.score_breakdown,
                    tags = EXCLUDED.tags,
                    source_status = EXCLUDED.source_status,
                    source_timestamp = EXCLUDED.source_timestamp,
                    ingested_at = NOW(),
                    primary_source = EXCLUDED.primary_source,
                    secondary_source = EXCLUDED.secondary_source
                """,
                str(row.get("scheme_code") or ""),
                str(row.get("scheme_name") or ""),
                row.get("amc"),
                row.get("category"),
                row.get("sub_category"),
                str(row.get("plan_type") or "direct"),
                row.get("option_type"),
                _to_float(row.get("nav")) or 0.0,
                _to_date(row.get("nav_date")),
                _to_float(row.get("expense_ratio")),
                _to_float(row.get("aum_cr")),
                row.get("risk_level"),
                _to_float(row.get("returns_1y")),
                _to_float(row.get("returns_3y")),
                _to_float(row.get("returns_5y")),
                _to_float(row.get("std_dev")),
                _to_float(row.get("sharpe")),
                _to_float(row.get("sortino")),
                _to_float(row.get("score")) or 0.0,
                _to_float(row.get("score_return")) or 0.0,
                _to_float(row.get("score_risk")) or 0.0,
                _to_float(row.get("score_cost")) or 0.0,
                _to_float(row.get("score_consistency")) or 0.0,
                _to_jsonb(row.get("score_breakdown"), _mf_breakdown_payload(row)),
                _to_jsonb(row.get("tags"), []),
                _normalize_source_status(row.get("source_status")),
                parse_ts(row.get("source_timestamp")) or datetime.now(timezone.utc),
                row.get("primary_source"),
                row.get("secondary_source"),
            )
            count += 1
    return count


async def _as_of(table: str) -> datetime | None:
    pool = await get_pool()
    row = await pool.fetchrow(f"SELECT MAX(source_timestamp) AS as_of FROM {table}")
    value = row["as_of"] if row else None
    return parse_ts(value)


def _normalize_order(sort_order: str | None) -> str:
    return "ASC" if str(sort_order or "").strip().lower() == "asc" else "DESC"


async def _get_stock_sector_stats(pool) -> dict[str, dict]:
    """Fetch per-sector avg ROE, median PE for contextual why_ranked."""
    rows = await pool.fetch(f"""
        SELECT
            COALESCE(NULLIF(sector, ''), 'Other') AS sector,
            ROUND(AVG(roe)::numeric, 1) AS avg_roe,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY pe_ratio)
                FILTER (WHERE pe_ratio > 0) AS median_pe
        FROM {STOCK_TABLE}
        WHERE market = 'IN'
        GROUP BY COALESCE(NULLIF(sector, ''), 'Other')
    """)
    out: dict[str, dict] = {}
    for r in rows:
        sector = str(r["sector"])
        out[sector] = {
            "avg_roe": float(r["avg_roe"]) if r["avg_roe"] is not None else None,
            "median_pe": float(r["median_pe"]) if r["median_pe"] is not None else None,
        }
    return out


async def _get_mf_category_stats(pool) -> dict[str, dict]:
    """Fetch per-category avg 3Y returns for contextual why_ranked."""
    rows = await pool.fetch(f"""
        SELECT
            COALESCE(NULLIF(category, ''), 'Other') AS category,
            ROUND(AVG(returns_3y)::numeric, 1) AS avg_ret3y
        FROM {MF_TABLE}
        WHERE returns_3y IS NOT NULL
        GROUP BY COALESCE(NULLIF(category, ''), 'Other')
    """)
    out: dict[str, dict] = {}
    for r in rows:
        cat = str(r["category"])
        out[cat] = {
            "avg_ret3y": float(r["avg_ret3y"]) if r["avg_ret3y"] is not None else None,
        }
    return out


async def list_discover_stocks(
    *,
    preset: str = "momentum",
    search: str | None = None,
    sector: str | None = None,
    min_score: float | None = None,
    max_score: float | None = None,
    min_price: float | None = None,
    max_price: float | None = None,
    min_pe: float | None = None,
    max_pe: float | None = None,
    min_roe: float | None = None,
    min_roce: float | None = None,
    max_debt_to_equity: float | None = None,
    min_volume: int | None = None,
    min_traded_value: float | None = None,
    source_status: str | None = None,
    sort_by: str = "score",
    sort_order: str = "desc",
    limit: int = 25,
    offset: int = 0,
) -> dict:
    allowed_sorts = {
        "score": "score",
        "change": "percent_change",
        "volume": "volume",
        "traded_value": "traded_value",
        "pe": "pe_ratio",
        "roe": "roe",
        "price": "last_price",
    }
    order_col = allowed_sorts.get(str(sort_by or "").strip().lower(), "score")
    order_dir = _normalize_order(sort_order)

    conds: list[str] = ["market = 'IN'"]
    args: list[object] = []

    def _add(cond: str, value: object) -> None:
        args.append(value)
        conds.append(cond.format(idx=len(args)))

    preset_norm = str(preset or "momentum").strip().lower()
    if preset_norm == "value":
        conds.append("score_fundamentals >= 55")
        conds.append("(pe_ratio IS NULL OR pe_ratio <= 35)")
    elif preset_norm == "low-volatility":
        conds.append("ABS(COALESCE(percent_change, 0)) <= 1.5")
    elif preset_norm == "high-volume":
        conds.append("COALESCE(traded_value, 0) >= 100000000")
    elif preset_norm == "breakout":
        conds.append("ABS(COALESCE(percent_change, 0)) >= 2")
        conds.append("COALESCE(volume, 0) >= 500000")
    elif preset_norm == "quality":
        conds.append("COALESCE(roe, 0) >= 15")
        conds.append("COALESCE(roce, 0) >= 15")
        conds.append("(debt_to_equity IS NULL OR debt_to_equity <= 1.0)")
    elif preset_norm == "dividend":
        conds.append("COALESCE(eps, 0) > 0")
        conds.append("(pe_ratio IS NULL OR pe_ratio <= 25)")
        conds.append("score_fundamentals >= 50")

    if search and search.strip():
        q = f"%{search.strip()}%"
        _add("(symbol ILIKE ${idx} OR display_name ILIKE ${idx})", q)
    if sector and sector.strip() and sector.strip().lower() != "all":
        _add("sector = ${idx}", sector.strip())
    if min_score is not None:
        _add("score >= ${idx}", float(min_score))
    if max_score is not None:
        _add("score <= ${idx}", float(max_score))
    if min_price is not None:
        _add("last_price >= ${idx}", float(min_price))
    if max_price is not None:
        _add("last_price <= ${idx}", float(max_price))
    if min_pe is not None:
        _add("pe_ratio >= ${idx}", float(min_pe))
    if max_pe is not None:
        _add("pe_ratio <= ${idx}", float(max_pe))
    if min_roe is not None:
        _add("roe >= ${idx}", float(min_roe))
    if min_roce is not None:
        _add("roce >= ${idx}", float(min_roce))
    if max_debt_to_equity is not None:
        _add("debt_to_equity <= ${idx}", float(max_debt_to_equity))
    if min_volume is not None:
        _add("volume >= ${idx}", int(min_volume))
    if min_traded_value is not None:
        _add("traded_value >= ${idx}", float(min_traded_value))
    if source_status and source_status.strip().lower() != "all":
        _add("source_status = ${idx}", _normalize_source_status(source_status))

    where_clause = " AND ".join(conds)
    filter_args = list(args)

    args.extend([max(1, min(limit, 250)), max(0, offset)])

    pool = await get_pool()
    sector_stats = await _get_stock_sector_stats(pool)

    rows = await pool.fetch(
        f"""
        SELECT
            symbol, display_name, market, sector,
            last_price, point_change, percent_change, volume, traded_value,
            pe_ratio, roe, roce, debt_to_equity, price_to_book, eps,
            score, score_momentum, score_liquidity, score_fundamentals,
            score_breakdown, tags, source_status, source_timestamp, ingested_at,
            primary_source, secondary_source
        FROM {STOCK_TABLE}
        WHERE {where_clause}
        ORDER BY {order_col} {order_dir} NULLS LAST, symbol ASC
        LIMIT ${len(args) - 1} OFFSET ${len(args)}
        """,
        *args,
    )

    total_row = await pool.fetchrow(
        f"SELECT COUNT(*) AS total FROM {STOCK_TABLE} WHERE {where_clause}",
        *filter_args,
    )
    total_count = int(total_row["total"]) if total_row else 0

    items = []
    for r in rows:
        d = record_to_dict(r)
        s = str(d.get("sector") or "Other")
        items.append(_decorate_stock_row(d, sector_stats.get(s)))

    return {
        "preset": preset_norm,
        "as_of": await _as_of(STOCK_TABLE),
        "source_status": _resolve_batch_source_status(items),
        "items": items,
        "count": len(items),
        "total_count": total_count,
    }


async def list_discover_mutual_funds(
    *,
    preset: str = "all",
    search: str | None = None,
    category: str | None = None,
    risk_level: str | None = None,
    direct_only: bool = True,
    min_score: float | None = None,
    min_aum_cr: float | None = None,
    max_expense_ratio: float | None = None,
    min_return_3y: float | None = None,
    source_status: str | None = None,
    sort_by: str = "score",
    sort_order: str = "desc",
    limit: int = 25,
    offset: int = 0,
) -> dict:
    allowed_sorts = {
        "score": "score",
        "returns_3y": "returns_3y",
        "returns_1y": "returns_1y",
        "aum": "aum_cr",
        "expense": "expense_ratio",
        "nav": "nav",
        "risk": "score_risk",
    }
    order_col = allowed_sorts.get(str(sort_by or "").strip().lower(), "score")
    order_dir = _normalize_order(sort_order)

    conds: list[str] = []
    args: list[object] = []

    def _add(cond: str, value: object) -> None:
        args.append(value)
        conds.append(cond.format(idx=len(args)))

    if direct_only:
        conds.append("LOWER(COALESCE(plan_type, 'direct')) = 'direct'")

    preset_norm = str(preset or "all").strip().lower()
    if preset_norm == "large-cap":
        conds.append(
            "(LOWER(COALESCE(category, '')) LIKE '%large%' OR LOWER(COALESCE(sub_category, '')) LIKE '%large%')"
        )
    elif preset_norm == "flexi-cap":
        conds.append(
            "(LOWER(COALESCE(category, '')) LIKE '%flexi%' OR LOWER(COALESCE(sub_category, '')) LIKE '%flexi%')"
        )
    elif preset_norm == "index":
        conds.append(
            "(LOWER(COALESCE(category, '')) LIKE '%index%' OR LOWER(COALESCE(sub_category, '')) LIKE '%index%')"
        )
    elif preset_norm == "low-risk":
        conds.append(
            "(LOWER(COALESCE(risk_level, '')) IN ('low','moderately low') OR COALESCE(std_dev, 999) <= 8)"
        )
    elif preset_norm == "mid-cap":
        conds.append(
            "(LOWER(COALESCE(category, '')) LIKE '%mid%' OR LOWER(COALESCE(sub_category, '')) LIKE '%mid%')"
        )
    elif preset_norm == "debt":
        conds.append(
            "(LOWER(COALESCE(category, '')) ~ '(debt|bond|gilt|money market|liquid|overnight|ultra short)')"
        )

    if search and search.strip():
        q = f"%{search.strip()}%"
        _add("(scheme_name ILIKE ${idx} OR amc ILIKE ${idx})", q)
    if category and category.strip() and category.strip().lower() != "all":
        q = f"%{category.strip().lower()}%"
        _add("(LOWER(COALESCE(category, '')) LIKE ${idx} OR LOWER(COALESCE(sub_category, '')) LIKE ${idx})", q)
    if risk_level and risk_level.strip() and risk_level.strip().lower() != "all":
        risk_norm = risk_level.strip().lower()
        if risk_norm == "low":
            conds.append("LOWER(COALESCE(risk_level, '')) IN ('low', 'moderately low')")
        elif risk_norm == "moderate":
            conds.append("LOWER(COALESCE(risk_level, '')) IN ('moderate', 'moderately low', 'moderately high')")
        elif risk_norm == "high":
            conds.append("LOWER(COALESCE(risk_level, '')) IN ('high', 'very high', 'moderately high')")
        else:
            _add("LOWER(COALESCE(risk_level, '')) = LOWER(${idx})", risk_level.strip())
    if min_score is not None:
        _add("score >= ${idx}", float(min_score))
    if min_aum_cr is not None:
        _add("aum_cr >= ${idx}", float(min_aum_cr))
    if max_expense_ratio is not None:
        _add("(expense_ratio <= ${idx} OR expense_ratio IS NULL)", float(max_expense_ratio))
    if min_return_3y is not None:
        _add("(returns_3y >= ${idx} OR returns_3y IS NULL)", float(min_return_3y))
    if source_status and source_status.strip().lower() != "all":
        _add("source_status = ${idx}", _normalize_source_status(source_status))

    where_sql = f"WHERE {' AND '.join(conds)}" if conds else ""
    filter_args = list(args)

    args.extend([max(1, min(limit, 250)), max(0, offset)])

    pool = await get_pool()
    category_stats = await _get_mf_category_stats(pool)

    rows = await pool.fetch(
        f"""
        SELECT
            scheme_code, scheme_name, amc, category, sub_category,
            plan_type, option_type, nav, nav_date,
            expense_ratio, aum_cr, risk_level,
            returns_1y, returns_3y, returns_5y, std_dev, sharpe, sortino,
            score, score_return, score_risk, score_cost, score_consistency,
            score_breakdown, tags, source_status, source_timestamp, ingested_at,
            primary_source, secondary_source
        FROM {MF_TABLE}
        {where_sql}
        ORDER BY {order_col} {order_dir} NULLS LAST, scheme_name ASC
        LIMIT ${len(args) - 1} OFFSET ${len(args)}
        """,
        *args,
    )

    total_count_sql = f"SELECT COUNT(*) AS total FROM {MF_TABLE} {where_sql}"
    total_row = await pool.fetchrow(total_count_sql, *filter_args)
    total_count = int(total_row["total"]) if total_row else 0

    items = []
    for r in rows:
        d = record_to_dict(r)
        cat = str(d.get("category") or "Other")
        items.append(_decorate_mf_row(d, category_stats.get(cat)))

    return {
        "preset": preset_norm,
        "as_of": await _as_of(MF_TABLE),
        "source_status": _resolve_batch_source_status(items),
        "items": items,
        "count": len(items),
        "total_count": total_count,
    }


async def get_discover_overview(
    *, segment: Literal["stocks", "mutual_funds"]
) -> dict:
    pool = await get_pool()
    if segment == "stocks":
        total_row = await pool.fetchrow(f"SELECT COUNT(*) AS c FROM {STOCK_TABLE} WHERE market = 'IN'")
        leader_rows = await pool.fetch(
            f"SELECT symbol FROM {STOCK_TABLE} WHERE market = 'IN' ORDER BY score DESC, symbol ASC LIMIT 3"
        )
        laggard_rows = await pool.fetch(
            f"SELECT symbol FROM {STOCK_TABLE} WHERE market = 'IN' ORDER BY score ASC, symbol ASC LIMIT 3"
        )
        sample_rows = await pool.fetch(
            f"SELECT source_status FROM {STOCK_TABLE} WHERE market = 'IN' ORDER BY score DESC LIMIT 20"
        )
        source = _resolve_batch_source_status([record_to_dict(r) for r in sample_rows])

        stats_row = await pool.fetchrow(f"""
            SELECT
                ROUND(AVG(score)::numeric, 1) AS avg_score,
                COUNT(*) FILTER (WHERE score >= 75) AS excellent,
                COUNT(*) FILTER (WHERE score >= 50 AND score < 75) AS good,
                COUNT(*) FILTER (WHERE score >= 25 AND score < 50) AS average,
                COUNT(*) FILTER (WHERE score < 25) AS poor
            FROM {STOCK_TABLE} WHERE market = 'IN'
        """)

        sector_rows = await pool.fetch(f"""
            SELECT
                COALESCE(NULLIF(sector, ''), 'Other') AS name,
                ROUND(AVG(score)::numeric, 1) AS avg_score,
                COUNT(*) AS count
            FROM {STOCK_TABLE} WHERE market = 'IN'
            GROUP BY COALESCE(NULLIF(sector, ''), 'Other')
            HAVING COUNT(*) >= 2
            ORDER BY avg_score DESC LIMIT 3
        """)

        freshness_row = await pool.fetchrow(
            f"SELECT MAX(ingested_at) AS latest FROM {STOCK_TABLE} WHERE market = 'IN'"
        )
        freshness_minutes = None
        if freshness_row and freshness_row["latest"]:
            delta = datetime.now(timezone.utc) - freshness_row["latest"]
            freshness_minutes = round(delta.total_seconds() / 60.0, 1)

        return {
            "segment": "stocks",
            "as_of": await _as_of(STOCK_TABLE),
            "total_items": int(total_row["c"] if total_row else 0),
            "source_status": source,
            "leaders": [str(r["symbol"]) for r in leader_rows],
            "laggards": [str(r["symbol"]) for r in laggard_rows],
            "avg_score": float(stats_row["avg_score"]) if stats_row and stats_row["avg_score"] is not None else None,
            "score_distribution": {
                "excellent": int(stats_row["excellent"] or 0) if stats_row else 0,
                "good": int(stats_row["good"] or 0) if stats_row else 0,
                "average": int(stats_row["average"] or 0) if stats_row else 0,
                "poor": int(stats_row["poor"] or 0) if stats_row else 0,
            } if stats_row else None,
            "top_sectors": [
                {"name": str(r["name"]), "avg_score": float(r["avg_score"]), "count": int(r["count"])}
                for r in sector_rows
            ],
            "top_categories": [],
            "data_freshness_minutes": freshness_minutes,
        }

    total_row = await pool.fetchrow(f"SELECT COUNT(*) AS c FROM {MF_TABLE}")
    leader_rows = await pool.fetch(
        f"SELECT scheme_name FROM {MF_TABLE} ORDER BY score DESC, scheme_name ASC LIMIT 3"
    )
    laggard_rows = await pool.fetch(
        f"SELECT scheme_name FROM {MF_TABLE} ORDER BY score ASC, scheme_name ASC LIMIT 3"
    )
    sample_rows = await pool.fetch(
        f"SELECT source_status FROM {MF_TABLE} ORDER BY score DESC LIMIT 20"
    )
    source = _resolve_batch_source_status([record_to_dict(r) for r in sample_rows])

    stats_row = await pool.fetchrow(f"""
        SELECT
            ROUND(AVG(score)::numeric, 1) AS avg_score,
            COUNT(*) FILTER (WHERE score >= 75) AS excellent,
            COUNT(*) FILTER (WHERE score >= 50 AND score < 75) AS good,
            COUNT(*) FILTER (WHERE score >= 25 AND score < 50) AS average,
            COUNT(*) FILTER (WHERE score < 25) AS poor
        FROM {MF_TABLE}
    """)

    cat_rows = await pool.fetch(f"""
        SELECT
            COALESCE(NULLIF(category, ''), 'Other') AS name,
            ROUND(AVG(score)::numeric, 1) AS avg_score,
            COUNT(*) AS count
        FROM {MF_TABLE}
        GROUP BY COALESCE(NULLIF(category, ''), 'Other')
        HAVING COUNT(*) >= 2
        ORDER BY avg_score DESC LIMIT 3
    """)

    freshness_row = await pool.fetchrow(
        f"SELECT MAX(ingested_at) AS latest FROM {MF_TABLE}"
    )
    freshness_minutes = None
    if freshness_row and freshness_row["latest"]:
        delta = datetime.now(timezone.utc) - freshness_row["latest"]
        freshness_minutes = round(delta.total_seconds() / 60.0, 1)

    return {
        "segment": "mutual_funds",
        "as_of": await _as_of(MF_TABLE),
        "total_items": int(total_row["c"] if total_row else 0),
        "source_status": source,
        "leaders": [str(r["scheme_name"]) for r in leader_rows],
        "laggards": [str(r["scheme_name"]) for r in laggard_rows],
        "avg_score": float(stats_row["avg_score"]) if stats_row and stats_row["avg_score"] is not None else None,
        "score_distribution": {
            "excellent": int(stats_row["excellent"] or 0) if stats_row else 0,
            "good": int(stats_row["good"] or 0) if stats_row else 0,
            "average": int(stats_row["average"] or 0) if stats_row else 0,
            "poor": int(stats_row["poor"] or 0) if stats_row else 0,
        } if stats_row else None,
        "top_sectors": [],
        "top_categories": [
            {"name": str(r["name"]), "avg_score": float(r["avg_score"]), "count": int(r["count"])}
            for r in cat_rows
        ],
        "data_freshness_minutes": freshness_minutes,
    }


def _compute_stock_metric_winners(items: list[dict]) -> dict[str, str]:
    winners: dict[str, str] = {}
    higher_better = {"score": "score", "roe": "roe", "roce": "roce", "volume": "volume"}
    lower_better = {"pe_ratio": "pe_ratio", "debt_to_equity": "debt_to_equity"}
    for label, key in higher_better.items():
        best_val, best_sym = None, None
        for item in items:
            v = _to_float(item.get(key))
            if v is not None and (best_val is None or v > best_val):
                best_val, best_sym = v, str(item.get("symbol") or "")
        if best_sym:
            winners[label] = best_sym
    for label, key in lower_better.items():
        best_val, best_sym = None, None
        for item in items:
            v = _to_float(item.get(key))
            if v is not None and v > 0 and (best_val is None or v < best_val):
                best_val, best_sym = v, str(item.get("symbol") or "")
        if best_sym:
            winners[label] = best_sym
    return winners


def _compute_mf_metric_winners(items: list[dict]) -> dict[str, str]:
    winners: dict[str, str] = {}
    higher_better = {"score": "score", "returns_3y": "returns_3y", "sharpe": "sharpe"}
    lower_better = {"expense_ratio": "expense_ratio"}
    for label, key in higher_better.items():
        best_val, best_code = None, None
        for item in items:
            v = _to_float(item.get(key))
            if v is not None and (best_val is None or v > best_val):
                best_val, best_code = v, str(item.get("scheme_code") or "")
        if best_code:
            winners[label] = best_code
    for label, key in lower_better.items():
        best_val, best_code = None, None
        for item in items:
            v = _to_float(item.get(key))
            if v is not None and v > 0 and (best_val is None or v < best_val):
                best_val, best_code = v, str(item.get("scheme_code") or "")
        if best_code:
            winners[label] = best_code
    return winners


async def get_discover_compare(
    *, segment: Literal["stocks", "mutual_funds"], ids: list[str]
) -> dict:
    clean_ids = [str(i).strip() for i in ids if str(i).strip()]
    clean_ids = clean_ids[:3]
    if not clean_ids:
        return {
            "segment": segment,
            "as_of": None,
            "count": 0,
            "source_status": "limited",
            "stock_items": [],
            "mutual_fund_items": [],
            "comparison_summary": None,
        }

    pool = await get_pool()
    if segment == "stocks":
        sector_stats = await _get_stock_sector_stats(pool)
        rows = await pool.fetch(
            f"""
            SELECT
                symbol, display_name, market, sector,
                last_price, point_change, percent_change, volume, traded_value,
                pe_ratio, roe, roce, debt_to_equity, price_to_book, eps,
                score, score_momentum, score_liquidity, score_fundamentals,
                score_breakdown, tags, source_status, source_timestamp, ingested_at,
                primary_source, secondary_source
            FROM {STOCK_TABLE}
            WHERE symbol = ANY($1::text[])
            ORDER BY score DESC, symbol ASC
            """,
            clean_ids,
        )
        items = []
        for r in rows:
            d = record_to_dict(r)
            s = str(d.get("sector") or "Other")
            items.append(_decorate_stock_row(d, sector_stats.get(s)))

        summary = None
        if len(items) >= 2:
            best = max(items, key=lambda x: float(x.get("score") or 0))
            worst = min(items, key=lambda x: float(x.get("score") or 0))
            summary = {
                "winner": str(best.get("symbol") or ""),
                "score_delta": round(float(best.get("score") or 0) - float(worst.get("score") or 0), 2),
                "metric_winners": _compute_stock_metric_winners(items),
            }

        return {
            "segment": "stocks",
            "as_of": await _as_of(STOCK_TABLE),
            "count": len(items),
            "source_status": _resolve_batch_source_status(items),
            "stock_items": items,
            "mutual_fund_items": [],
            "comparison_summary": summary,
        }

    category_stats = await _get_mf_category_stats(pool)
    rows = await pool.fetch(
        f"""
        SELECT
            scheme_code, scheme_name, amc, category, sub_category,
            plan_type, option_type, nav, nav_date,
            expense_ratio, aum_cr, risk_level,
            returns_1y, returns_3y, returns_5y, std_dev, sharpe, sortino,
            score, score_return, score_risk, score_cost, score_consistency,
            score_breakdown, tags, source_status, source_timestamp, ingested_at,
            primary_source, secondary_source
        FROM {MF_TABLE}
        WHERE scheme_code = ANY($1::text[])
        ORDER BY score DESC, scheme_name ASC
        """,
        clean_ids,
    )
    items = []
    for r in rows:
        d = record_to_dict(r)
        cat = str(d.get("category") or "Other")
        items.append(_decorate_mf_row(d, category_stats.get(cat)))

    summary = None
    if len(items) >= 2:
        best = max(items, key=lambda x: float(x.get("score") or 0))
        worst = min(items, key=lambda x: float(x.get("score") or 0))
        summary = {
            "winner": str(best.get("scheme_code") or ""),
            "score_delta": round(float(best.get("score") or 0) - float(worst.get("score") or 0), 2),
            "metric_winners": _compute_mf_metric_winners(items),
        }

    return {
        "segment": "mutual_funds",
        "as_of": await _as_of(MF_TABLE),
        "count": len(items),
        "source_status": _resolve_batch_source_status(items),
        "stock_items": [],
        "mutual_fund_items": items,
        "comparison_summary": summary,
    }

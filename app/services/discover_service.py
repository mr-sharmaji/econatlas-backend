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
    # Worst case in batch drives the batch-level badge.
    rank = {"limited": 0, "fallback": 1, "primary": 2}
    worst = "primary"
    for row in rows:
        cur = _normalize_source_status(row.get("source_status"))
        if rank[cur] < rank[worst]:
            worst = cur
    return worst


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


def _stock_why_ranked(row: dict) -> list[str]:
    reasons: list[str] = []
    pct = _to_float(row.get("percent_change"))
    if pct is not None and pct >= 2.0:
        reasons.append("Strong momentum above +2%.")
    elif pct is not None and pct <= -2.0:
        reasons.append("Large downside move; high-volatility signal.")
    tv = _to_float(row.get("traded_value"))
    if tv is not None and tv >= 500_000_000:
        reasons.append("High traded value supports liquidity.")
    roe = _to_float(row.get("roe"))
    if roe is not None and roe >= 15:
        reasons.append("Healthy ROE profile.")
    dte = _to_float(row.get("debt_to_equity"))
    if dte is not None and dte <= 0.6:
        reasons.append("Conservative leverage profile.")
    status = _normalize_source_status(row.get("source_status"))
    if status != "primary":
        reasons.append("Some metrics are based on fallback or limited data.")
    if not reasons:
        reasons.append("Balanced rank across momentum, liquidity, and fundamentals.")
    return reasons[:3]


def _mf_why_ranked(row: dict) -> list[str]:
    reasons: list[str] = []
    ret3 = _to_float(row.get("returns_3y"))
    if ret3 is not None and ret3 >= 12:
        reasons.append("Strong 3Y return profile.")
    expense = _to_float(row.get("expense_ratio"))
    if expense is not None and expense <= 1.0:
        reasons.append("Low expense ratio supports long-term compounding.")
    risk = str(row.get("risk_level") or "").strip().lower()
    if risk in {"low", "moderately low"}:
        reasons.append("Risk profile is relatively conservative.")
    if str(row.get("plan_type") or "").strip().lower() == "direct":
        reasons.append("Direct plan selected for lower cost drag.")
    status = _normalize_source_status(row.get("source_status"))
    if status != "primary":
        reasons.append("Some advanced metrics are unavailable in fallback mode.")
    if not reasons:
        reasons.append("Balanced risk-return-cost score within category.")
    return reasons[:3]


def _decorate_stock_row(row: dict) -> dict:
    item = dict(row)
    item["source_status"] = _normalize_source_status(item.get("source_status"))
    item["score_breakdown"] = _stock_breakdown_payload(item)
    tags = item.get("tags")
    item["tags"] = tags if isinstance(tags, list) else []
    item["why_ranked"] = _stock_why_ranked(item)
    return item


def _decorate_mf_row(row: dict) -> dict:
    item = dict(row)
    item["source_status"] = _normalize_source_status(item.get("source_status"))
    item["score_breakdown"] = _mf_breakdown_payload(item)
    tags = item.get("tags")
    item["tags"] = tags if isinstance(tags, list) else []
    item["why_ranked"] = _mf_why_ranked(item)
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

    args.extend([max(1, min(limit, 250)), max(0, offset)])

    pool = await get_pool()
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
        WHERE {' AND '.join(conds)}
        ORDER BY {order_col} {order_dir} NULLS LAST, symbol ASC
        LIMIT ${len(args) - 1} OFFSET ${len(args)}
        """,
        *args,
    )

    items = [_decorate_stock_row(record_to_dict(r)) for r in rows]
    return {
        "preset": preset_norm,
        "as_of": await _as_of(STOCK_TABLE),
        "source_status": _resolve_batch_source_status(items),
        "items": items,
        "count": len(items),
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
    args.extend([max(1, min(limit, 250)), max(0, offset)])

    pool = await get_pool()
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

    items = [_decorate_mf_row(record_to_dict(r)) for r in rows]
    return {
        "preset": preset_norm,
        "as_of": await _as_of(MF_TABLE),
        "source_status": _resolve_batch_source_status(items),
        "items": items,
        "count": len(items),
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
        return {
            "segment": "stocks",
            "as_of": await _as_of(STOCK_TABLE),
            "total_items": int(total_row["c"] if total_row else 0),
            "source_status": source,
            "leaders": [str(r["symbol"]) for r in leader_rows],
            "laggards": [str(r["symbol"]) for r in laggard_rows],
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
    return {
        "segment": "mutual_funds",
        "as_of": await _as_of(MF_TABLE),
        "total_items": int(total_row["c"] if total_row else 0),
        "source_status": source,
        "leaders": [str(r["scheme_name"]) for r in leader_rows],
        "laggards": [str(r["scheme_name"]) for r in laggard_rows],
    }


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
        }

    pool = await get_pool()
    if segment == "stocks":
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
        items = [_decorate_stock_row(record_to_dict(r)) for r in rows]
        return {
            "segment": "stocks",
            "as_of": await _as_of(STOCK_TABLE),
            "count": len(items),
            "source_status": _resolve_batch_source_status(items),
            "stock_items": items,
            "mutual_fund_items": [],
        }

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
    items = [_decorate_mf_row(record_to_dict(r)) for r in rows]
    return {
        "segment": "mutual_funds",
        "as_of": await _as_of(MF_TABLE),
        "count": len(items),
        "source_status": _resolve_batch_source_status(items),
        "stock_items": [],
        "mutual_fund_items": items,
    }

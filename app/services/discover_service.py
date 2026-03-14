from __future__ import annotations

import json
import math
import re
from datetime import date, datetime, timezone
from typing import Literal

from app.core.database import get_pool, parse_ts, record_to_dict

STOCK_TABLE = "discover_stock_snapshots"
MF_TABLE = "discover_mutual_fund_snapshots"

SourceStatus = Literal["primary", "fallback", "limited"]


def _clean_mf_display_name(name: str) -> str:
    """Strip plan type and option suffixes from MF scheme name for cleaner display."""
    result = name
    # Remove common suffixes (order matters — longer patterns first)
    patterns = [
        r'\s*[-–]\s*Direct\s+Plan\s*[-–]\s*Growth\s+Option\s*',
        r'\s*[-–]\s*Direct\s+Plan\s*[-–]\s*Growth\s*',
        r'\s*[-–]\s*Direct\s+Plan\s*[-–]\s*IDCW\s+Option\s*',
        r'\s*[-–]\s*Direct\s+Plan\s*[-–]\s*IDCW\s*',
        r'\s*[-–]\s*Direct\s+Plan\s*[-–]\s*Dividend\s+Option\s*',
        r'\s*[-–]\s*Direct\s+Plan\s*',
        r'\s*[-–]\s*GROWTH\s+OPTION\s*[-–]\s*Direct\s+Plan\s*',
        r'\s*[-–]\s*Growth\s+Direct\s*',
        r'\s*[-–]\s*IDCW\s+Direct\s+Plan\s*',
        r'\s*[-–]\s*Direct\s*Plan\s*',
        r'\s*[-–]\s*Direct\s+Growth\s*',
        r'\s*\bDirect\s*[Pp]lan\b\s*',
        r'\s*\bDIRECT\s*PLAN\b\s*',
        r'\s*[-–]\s*Growth\s*$',
        r'\s*[-–]\s*GROWTH\s*$',
        r'\s*[-–]\s*Direct\s*$',
        r'\s*-GROWTH\s+OPTION\s*',
        r'\s*[-–]\s*GROWTH\s+OPTION\s*',
    ]
    for p in patterns:
        result = re.sub(p, '', result, flags=re.IGNORECASE).strip()
    # Trim trailing dashes and whitespace
    result = re.sub(r'\s*[-–]+\s*$', '', result).strip()
    return result if result else name


def _to_float(value) -> float | None:
    try:
        if value is None:
            return None
        f = float(value)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
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


def _safe_json_dumps(obj) -> str:
    """JSON serialize with NaN/Inf replaced by None."""
    def _sanitize(o):
        if isinstance(o, float) and (math.isnan(o) or math.isinf(o)):
            return None
        return o

    class _SafeEncoder(json.JSONEncoder):
        def default(self, o):
            if isinstance(o, float) and (math.isnan(o) or math.isinf(o)):
                return None
            return super().default(o)

        def encode(self, o):
            return super().encode(_walk(o))

    def _walk(node):
        if isinstance(node, dict):
            return {k: _walk(v) for k, v in node.items()}
        if isinstance(node, list):
            return [_walk(v) for v in node]
        if isinstance(node, float) and (math.isnan(node) or math.isinf(node)):
            return None
        return node

    return json.dumps(_walk(obj), separators=(",", ":"), ensure_ascii=True)


def _to_jsonb(value, default):
    payload = value if value is not None else default
    return _safe_json_dumps(payload)


def _to_jsonb_raw(value) -> str | None:
    """Convert a value to a JSON string for asyncpg JSONB columns.

    asyncpg sends text params to PostgreSQL, which parses them as JSONB.
    So we need: dict → json.dumps → string like '{"key": val}' → PG stores as JSONB object.
    If value is already a string (e.g. from a previous DB read of double-encoded data),
    try to parse it first to ensure we store a proper JSONB object, not a JSONB string scalar.
    """
    if value is None:
        return None
    if isinstance(value, dict):
        return _safe_json_dumps(value)
    if isinstance(value, str):
        # Could be a previously double-encoded string — try to parse it
        try:
            parsed = json.loads(value)
            if isinstance(parsed, dict):
                return _safe_json_dumps(parsed)
        except (ValueError, TypeError):
            pass
        return value
    return _safe_json_dumps(value)


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
    """Parse the stored score_breakdown JSONB which contains the 6-layer scoring data."""
    import json as _json

    stored_sb = row.get("score_breakdown")
    if isinstance(stored_sb, str):
        try:
            stored_sb = _json.loads(stored_sb)
        except (ValueError, TypeError):
            stored_sb = {}
    elif not isinstance(stored_sb, dict):
        stored_sb = {}

    quality = _to_float(stored_sb.get("quality") or row.get("score_quality")) or 0.0
    valuation = _to_float(stored_sb.get("valuation") or row.get("score_valuation")) or 0.0
    growth = _to_float(stored_sb.get("growth") or row.get("score_growth")) or 0.0
    momentum = _to_float(stored_sb.get("momentum") or row.get("score_momentum")) or 0.0
    institutional = _to_float(stored_sb.get("institutional") or row.get("score_institutional")) or 0.0
    risk = _to_float(stored_sb.get("risk") or row.get("score_risk")) or 0.0

    combined_signal = _to_float(stored_sb.get("combined_signal")) or 0.0

    result = {
        "quality": round(quality, 2),
        "valuation": round(valuation, 2),
        "growth": round(growth, 2),
        "momentum": round(momentum, 2),
        "institutional": round(institutional, 2),
        "risk": round(risk, 2),
        "combined_signal": round(combined_signal, 2),
    }

    pos_52w = _to_float(stored_sb.get("52w_position"))
    if pos_52w is not None:
        result["52w_position"] = round(pos_52w, 2)

    qc = stored_sb.get("quality_coverage")
    if qc is not None:
        result["quality_coverage"] = str(qc)

    dq = stored_sb.get("data_quality")
    if dq is not None:
        result["data_quality"] = str(dq)

    sp = _to_float(stored_sb.get("sector_percentile") or row.get("sector_percentile"))
    if sp is not None:
        result["sector_percentile"] = round(sp, 2)

    lc = stored_sb.get("lynch_classification") or row.get("lynch_classification")
    if lc is not None:
        result["lynch_classification"] = str(lc)

    mr = stored_sb.get("market_regime")
    if mr is not None:
        result["market_regime"] = str(mr)

    peg = _to_float(stored_sb.get("peg_ratio"))
    if peg is not None:
        result["peg_ratio"] = round(peg, 2)

    sdcp = _to_float(stored_sb.get("sector_data_coverage_pct"))
    if sdcp is not None:
        result["sector_data_coverage_pct"] = round(sdcp, 2)

    wn = stored_sb.get("why_narrative")
    if wn:
        result["why_narrative"] = str(wn)

    return result


def _mf_breakdown_payload(row: dict) -> dict:
    result = {
        "return_score": round(_to_float(row.get("score_return")) or 0.0, 2),
        "risk_score": round(_to_float(row.get("score_risk")) or 0.0, 2),
        "cost_score": round(_to_float(row.get("score_cost")) or 0.0, 2),
        "consistency_score": round(_to_float(row.get("score_consistency")) or 0.0, 2),
    }
    alpha_score = _to_float(row.get("score_alpha"))
    beta_score = _to_float(row.get("score_beta"))
    if alpha_score:
        result["alpha_score"] = round(alpha_score, 2)
    if beta_score:
        result["beta_score"] = round(beta_score, 2)
    return result


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


def _compute_quality_badges(row: dict) -> list[str]:
    badges: list[str] = []
    category_rank = _to_int(row.get("category_rank"))
    category_total = _to_int(row.get("category_total"))
    if category_rank is not None and category_total is not None:
        if category_rank <= max(1, int(category_total * 0.1)):
            badges.append("Top Performer")
    returns_1y = _to_float(row.get("returns_1y"))
    returns_3y = _to_float(row.get("returns_3y"))
    returns_5y = _to_float(row.get("returns_5y"))
    if (returns_1y is not None and returns_1y > 0
            and returns_3y is not None and returns_3y > 0
            and returns_5y is not None and returns_5y > 0):
        badges.append("Consistent Returns")
    expense_ratio = _to_float(row.get("expense_ratio"))
    if expense_ratio is not None and expense_ratio < 1.0:
        badges.append("Cost Efficient")
    fund_age_years = _to_float(row.get("fund_age_years"))
    if fund_age_years is not None and fund_age_years >= 5:
        badges.append("Proven Track Record")
    return badges


def _decorate_stock_row(row: dict, sector_stats: dict | None = None) -> dict:
    import json as _json
    item = dict(row)
    item["source_status"] = _normalize_source_status(item.get("source_status"))
    item["score_breakdown"] = _stock_breakdown_payload(item)
    tags = item.get("tags")
    if isinstance(tags, str):
        try:
            tags = _json.loads(tags)
        except (ValueError, TypeError):
            tags = []
    item["tags"] = tags if isinstance(tags, list) else []
    # Parse JSONB columns that come back as strings from asyncpg
    for jkey in ("pl_annual", "bs_annual", "cf_annual", "shareholding_quarterly"):
        val = item.get(jkey)
        if isinstance(val, str):
            try:
                item[jkey] = _json.loads(val)
            except (ValueError, TypeError):
                item[jkey] = None
    item["why_ranked"] = _stock_why_ranked(item, sector_stats)
    return item


def _decorate_mf_row(row: dict, category_stats: dict | None = None) -> dict:
    item = dict(row)
    item["source_status"] = _normalize_source_status(item.get("source_status"))
    item["score_breakdown"] = _mf_breakdown_payload(item)
    item["display_name"] = _clean_mf_display_name(item.get("scheme_name", ""))
    tags = item.get("tags")
    item["tags"] = tags if isinstance(tags, list) else []
    item["why_ranked"] = _mf_why_ranked(item, category_stats)
    item["quality_badges"] = _compute_quality_badges(item)
    if category_stats:
        item["category_avg_returns_1y"] = category_stats.get("avg_ret1y")
        item["category_avg_returns_3y"] = category_stats.get("avg_ret3y")
        item["category_avg_returns_5y"] = category_stats.get("avg_ret5y")
    else:
        item["category_avg_returns_1y"] = None
        item["category_avg_returns_3y"] = None
        item["category_avg_returns_5y"] = None
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
                    score_volatility, score_growth,
                    score_ownership, score_financial_health, score_analyst,
                    percent_change_3m, percent_change_1w,
                    percent_change_1y, percent_change_3y,
                    score_breakdown, tags, source_status, source_timestamp, ingested_at,
                    primary_source, secondary_source,
                    high_52w, low_52w, market_cap, dividend_yield,
                    promoter_holding, fii_holding, dii_holding, government_holding, public_holding,
                    num_shareholders, promoter_holding_change, fii_holding_change, dii_holding_change,
                    beta, free_cash_flow, operating_cash_flow, total_cash, total_debt, total_revenue,
                    gross_margins, operating_margins, profit_margins,
                    revenue_growth, earnings_growth, forward_pe,
                    analyst_target_mean, analyst_count, analyst_recommendation, analyst_recommendation_mean,
                    industry, payout_ratio,
                    pledged_promoter_pct,
                    sales_growth_yoy, profit_growth_yoy, opm_change, interest_coverage,
                    compounded_sales_growth_3y, compounded_profit_growth_3y,
                    total_assets, asset_growth_yoy, reserves_growth, debt_direction, cwip,
                    cash_from_operations, cash_from_investing, cash_from_financing,
                    num_shareholders_change_qoq, num_shareholders_change_yoy,
                    synthetic_forward_pe,
                    score_valuation, score_earnings_quality, score_smart_money,
                    pl_annual, bs_annual, cf_annual,
                    shareholding_quarterly,
                    score_quality, score_institutional, score_risk,
                    sector_percentile, lynch_classification, percent_change_5y
                )
                VALUES (
                    $1, $2, $3, $4, $5, $6, $7,
                    $8, $9, $10, $11, $12, $13, $14, $15,
                    $16, $17, NULL, NULL,
                    NULL, $18,
                    NULL, NULL, NULL,
                    $19, $20,
                    $21, $22,
                    $23, $24, $25, $26, NOW(),
                    $27, $28,
                    $29, $30, $31, $32,
                    $33, $34, $35, $36, $37,
                    $38, $39, $40, $41,
                    $42, $43, $44, $45, $46, $47,
                    $48, $49, $50,
                    $51, $52, $53,
                    $54, $55, $56, $57,
                    $58, $59,
                    $60,
                    $61, $62, $63, $64,
                    $65, $66,
                    $67, $68, $69, $70, $71,
                    $72, $73, $74,
                    $75, $76,
                    $77,
                    $78, NULL, NULL,
                    $79, $80, $81,
                    $82,
                    $83, $84, $85,
                    $86, $87, $88
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
                    score = COALESCE(EXCLUDED.score, {STOCK_TABLE}.score),
                    score_momentum = COALESCE(EXCLUDED.score_momentum, {STOCK_TABLE}.score_momentum),
                    score_liquidity = NULL,
                    score_fundamentals = NULL,
                    score_volatility = NULL,
                    score_growth = COALESCE(EXCLUDED.score_growth, {STOCK_TABLE}.score_growth),
                    score_ownership = NULL,
                    score_financial_health = NULL,
                    score_analyst = NULL,
                    percent_change_3m = EXCLUDED.percent_change_3m,
                    percent_change_1w = EXCLUDED.percent_change_1w,
                    percent_change_1y = EXCLUDED.percent_change_1y,
                    percent_change_3y = EXCLUDED.percent_change_3y,
                    score_breakdown = COALESCE(EXCLUDED.score_breakdown, {STOCK_TABLE}.score_breakdown),
                    tags = COALESCE(EXCLUDED.tags, {STOCK_TABLE}.tags),
                    source_status = EXCLUDED.source_status,
                    source_timestamp = EXCLUDED.source_timestamp,
                    ingested_at = NOW(),
                    primary_source = EXCLUDED.primary_source,
                    secondary_source = EXCLUDED.secondary_source,
                    high_52w = EXCLUDED.high_52w,
                    low_52w = EXCLUDED.low_52w,
                    market_cap = EXCLUDED.market_cap,
                    dividend_yield = EXCLUDED.dividend_yield,
                    promoter_holding = EXCLUDED.promoter_holding,
                    fii_holding = EXCLUDED.fii_holding,
                    dii_holding = EXCLUDED.dii_holding,
                    government_holding = EXCLUDED.government_holding,
                    public_holding = EXCLUDED.public_holding,
                    num_shareholders = EXCLUDED.num_shareholders,
                    promoter_holding_change = EXCLUDED.promoter_holding_change,
                    fii_holding_change = EXCLUDED.fii_holding_change,
                    dii_holding_change = EXCLUDED.dii_holding_change,
                    beta = EXCLUDED.beta,
                    free_cash_flow = EXCLUDED.free_cash_flow,
                    operating_cash_flow = EXCLUDED.operating_cash_flow,
                    total_cash = EXCLUDED.total_cash,
                    total_debt = EXCLUDED.total_debt,
                    total_revenue = EXCLUDED.total_revenue,
                    gross_margins = EXCLUDED.gross_margins,
                    operating_margins = EXCLUDED.operating_margins,
                    profit_margins = EXCLUDED.profit_margins,
                    revenue_growth = EXCLUDED.revenue_growth,
                    earnings_growth = EXCLUDED.earnings_growth,
                    forward_pe = EXCLUDED.forward_pe,
                    analyst_target_mean = EXCLUDED.analyst_target_mean,
                    analyst_count = EXCLUDED.analyst_count,
                    analyst_recommendation = EXCLUDED.analyst_recommendation,
                    analyst_recommendation_mean = EXCLUDED.analyst_recommendation_mean,
                    industry = EXCLUDED.industry,
                    payout_ratio = EXCLUDED.payout_ratio,
                    pledged_promoter_pct = EXCLUDED.pledged_promoter_pct,
                    sales_growth_yoy = EXCLUDED.sales_growth_yoy,
                    profit_growth_yoy = EXCLUDED.profit_growth_yoy,
                    opm_change = EXCLUDED.opm_change,
                    interest_coverage = EXCLUDED.interest_coverage,
                    compounded_sales_growth_3y = EXCLUDED.compounded_sales_growth_3y,
                    compounded_profit_growth_3y = EXCLUDED.compounded_profit_growth_3y,
                    total_assets = EXCLUDED.total_assets,
                    asset_growth_yoy = EXCLUDED.asset_growth_yoy,
                    reserves_growth = EXCLUDED.reserves_growth,
                    debt_direction = EXCLUDED.debt_direction,
                    cwip = EXCLUDED.cwip,
                    cash_from_operations = EXCLUDED.cash_from_operations,
                    cash_from_investing = EXCLUDED.cash_from_investing,
                    cash_from_financing = EXCLUDED.cash_from_financing,
                    num_shareholders_change_qoq = EXCLUDED.num_shareholders_change_qoq,
                    num_shareholders_change_yoy = EXCLUDED.num_shareholders_change_yoy,
                    synthetic_forward_pe = EXCLUDED.synthetic_forward_pe,
                    score_valuation = COALESCE(EXCLUDED.score_valuation, {STOCK_TABLE}.score_valuation),
                    score_earnings_quality = NULL,
                    score_smart_money = NULL,
                    pl_annual = COALESCE(EXCLUDED.pl_annual, {STOCK_TABLE}.pl_annual),
                    bs_annual = COALESCE(EXCLUDED.bs_annual, {STOCK_TABLE}.bs_annual),
                    cf_annual = COALESCE(EXCLUDED.cf_annual, {STOCK_TABLE}.cf_annual),
                    shareholding_quarterly = COALESCE(EXCLUDED.shareholding_quarterly, {STOCK_TABLE}.shareholding_quarterly),
                    score_quality = COALESCE(EXCLUDED.score_quality, {STOCK_TABLE}.score_quality),
                    score_institutional = COALESCE(EXCLUDED.score_institutional, {STOCK_TABLE}.score_institutional),
                    score_risk = COALESCE(EXCLUDED.score_risk, {STOCK_TABLE}.score_risk),
                    sector_percentile = COALESCE(EXCLUDED.sector_percentile, {STOCK_TABLE}.sector_percentile),
                    lynch_classification = COALESCE(EXCLUDED.lynch_classification, {STOCK_TABLE}.lynch_classification),
                    percent_change_5y = EXCLUDED.percent_change_5y
                """,
                str(row.get("market") or "IN"),                                    # $1
                str(row.get("symbol") or ""),                                      # $2
                str(row.get("display_name") or row.get("symbol") or ""),           # $3
                row.get("sector"),                                                 # $4
                _to_float(row.get("last_price")) or 0.0,                           # $5
                _to_float(row.get("point_change")),                                # $6
                _to_float(row.get("percent_change")),                              # $7
                _to_int(row.get("volume")),                                        # $8
                _to_float(row.get("traded_value")),                                # $9
                _to_float(row.get("pe_ratio")),                                    # $10
                _to_float(row.get("roe")),                                         # $11
                _to_float(row.get("roce")),                                        # $12
                _to_float(row.get("debt_to_equity")),                              # $13
                _to_float(row.get("price_to_book")),                               # $14
                _to_float(row.get("eps")),                                         # $15
                _to_float(row.get("score")),                                       # $16
                _to_float(row.get("score_momentum")),                              # $17
                _to_float(row.get("score_growth")),                                # $18
                _to_float(row.get("percent_change_3m")),                           # $19
                _to_float(row.get("percent_change_1w")),                           # $20
                _to_float(row.get("percent_change_1y")),                           # $21
                _to_float(row.get("percent_change_3y")),                           # $22
                _to_jsonb(row.get("score_breakdown"), _stock_breakdown_payload(row)) if row.get("score") is not None else None,  # $23
                _to_jsonb(row.get("tags"), []) if row.get("score") is not None else None,  # $24
                _normalize_source_status(row.get("source_status")),                # $25
                parse_ts(row.get("source_timestamp")) or datetime.now(timezone.utc),  # $26
                row.get("primary_source"),                                         # $27
                row.get("secondary_source"),                                       # $28
                _to_float(row.get("high_52w")),                                    # $29
                _to_float(row.get("low_52w")),                                     # $30
                _to_float(row.get("market_cap")),                                  # $31
                _to_float(row.get("dividend_yield")),                              # $32
                _to_float(row.get("promoter_holding")),                            # $33
                _to_float(row.get("fii_holding")),                                 # $34
                _to_float(row.get("dii_holding")),                                 # $35
                _to_float(row.get("government_holding")),                          # $36
                _to_float(row.get("public_holding")),                              # $37
                _to_int(row.get("num_shareholders")),                              # $38
                _to_float(row.get("promoter_holding_change")),                     # $39
                _to_float(row.get("fii_holding_change")),                          # $40
                _to_float(row.get("dii_holding_change")),                          # $41
                _to_float(row.get("beta")),                                        # $42
                _to_float(row.get("free_cash_flow")),                              # $43
                _to_float(row.get("operating_cash_flow")),                         # $44
                _to_float(row.get("total_cash")),                                  # $45
                _to_float(row.get("total_debt")),                                  # $46
                _to_float(row.get("total_revenue")),                               # $47
                _to_float(row.get("gross_margins")),                               # $48
                _to_float(row.get("operating_margins")),                           # $49
                _to_float(row.get("profit_margins")),                              # $50
                _to_float(row.get("revenue_growth")),                              # $51
                _to_float(row.get("earnings_growth")),                             # $52
                _to_float(row.get("forward_pe")),                                  # $53
                _to_float(row.get("analyst_target_mean")),                         # $54
                _to_int(row.get("analyst_count")),                                 # $55
                row.get("analyst_recommendation"),                                 # $56
                _to_float(row.get("analyst_recommendation_mean")),                 # $57
                row.get("industry"),                                               # $58
                _to_float(row.get("payout_ratio")),                                # $59
                _to_float(row.get("pledged_promoter_pct")),                        # $60
                _to_float(row.get("sales_growth_yoy")),                            # $61
                _to_float(row.get("profit_growth_yoy")),                           # $62
                _to_float(row.get("opm_change")),                                  # $63
                _to_float(row.get("interest_coverage")),                           # $64
                _to_float(row.get("compounded_sales_growth_3y")),                  # $65
                _to_float(row.get("compounded_profit_growth_3y")),                 # $66
                _to_float(row.get("total_assets")),                                # $67
                _to_float(row.get("asset_growth_yoy")),                            # $68
                _to_float(row.get("reserves_growth")),                             # $69
                _to_float(row.get("debt_direction")),                              # $70
                _to_float(row.get("cwip")),                                        # $71
                _to_float(row.get("cash_from_operations")),                        # $72
                _to_float(row.get("cash_from_investing")),                         # $73
                _to_float(row.get("cash_from_financing")),                         # $74
                _to_float(row.get("num_shareholders_change_qoq")),                 # $75
                _to_float(row.get("num_shareholders_change_yoy")),                 # $76
                _to_float(row.get("synthetic_forward_pe")),                        # $77
                _to_float(row.get("score_valuation")),                             # $78
                _to_jsonb_raw(row.get("pl_annual")),                               # $79
                _to_jsonb_raw(row.get("bs_annual")),                               # $80
                _to_jsonb_raw(row.get("cf_annual")),                               # $81
                _to_jsonb_raw(row.get("shareholding_quarterly")),                   # $82
                _to_float(row.get("score_quality")),                               # $83
                _to_float(row.get("score_institutional")),                         # $84
                _to_float(row.get("score_risk")),                                  # $85
                _to_float(row.get("sector_percentile")),                           # $86
                row.get("lynch_classification"),                                   # $87
                _to_float(row.get("percent_change_5y")),                           # $88
            )
            count += 1
    return count


# ── Score history (trend tracking) ─────────────────────────────


async def insert_score_history(rows: list[dict]) -> int:
    """Insert score snapshots for trend tracking."""
    if not rows:
        return 0
    pool = await get_pool()
    async with pool.acquire() as conn:
        values = [
            (str(r.get("symbol", "")), float(r.get("score", 0)))
            for r in rows
            if r.get("symbol") and r.get("score") is not None
        ]
        if values:
            await conn.executemany(
                "INSERT INTO discover_stock_score_history (symbol, score) VALUES ($1, $2)",
                values,
            )
    return len(values)


async def prune_score_history(days: int = 30) -> int:
    """Delete score history rows older than N days."""
    from datetime import timedelta
    pool = await get_pool()
    async with pool.acquire() as conn:
        result = await conn.execute(
            "DELETE FROM discover_stock_score_history WHERE scored_at < NOW() - $1::interval",
            timedelta(days=days),
        )
    return int(result.split()[-1]) if result else 0


async def get_previous_score(symbol: str, days_ago: int = 7) -> float | None:
    """Fetch the most recent historical score at least `days_ago` days old."""
    from datetime import timedelta
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT score FROM discover_stock_score_history "
            "WHERE symbol = $1 AND scored_at < NOW() - $2::interval "
            "ORDER BY scored_at DESC LIMIT 1",
            symbol,
            timedelta(days=days_ago),
        )
    return float(row["score"]) if row else None


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
                    primary_source, secondary_source, fund_age_years,
                    max_drawdown, rolling_return_consistency,
                    alpha, beta, score_alpha, score_beta
                )
                VALUES (
                    $1, $2, $3, $4, $5, $6, $7,
                    $8, $9, $10, $11, $12,
                    $13, $14, $15, $16, $17, $18,
                    $19, $20, $21, $22, $23,
                    $24, $25, $26, $27, NOW(),
                    $28, $29, $30,
                    $31, $32,
                    $33, $34, $35, $36
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
                    expense_ratio = COALESCE(EXCLUDED.expense_ratio, discover_mutual_fund_snapshots.expense_ratio),
                    aum_cr = COALESCE(EXCLUDED.aum_cr, discover_mutual_fund_snapshots.aum_cr),
                    risk_level = EXCLUDED.risk_level,
                    returns_1y = EXCLUDED.returns_1y,
                    returns_3y = EXCLUDED.returns_3y,
                    returns_5y = EXCLUDED.returns_5y,
                    std_dev = COALESCE(EXCLUDED.std_dev, discover_mutual_fund_snapshots.std_dev),
                    sharpe = COALESCE(EXCLUDED.sharpe, discover_mutual_fund_snapshots.sharpe),
                    sortino = COALESCE(EXCLUDED.sortino, discover_mutual_fund_snapshots.sortino),
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
                    secondary_source = EXCLUDED.secondary_source,
                    fund_age_years = COALESCE(EXCLUDED.fund_age_years, discover_mutual_fund_snapshots.fund_age_years),
                    max_drawdown = COALESCE(EXCLUDED.max_drawdown, discover_mutual_fund_snapshots.max_drawdown),
                    rolling_return_consistency = COALESCE(EXCLUDED.rolling_return_consistency, discover_mutual_fund_snapshots.rolling_return_consistency),
                    alpha = EXCLUDED.alpha,
                    beta = EXCLUDED.beta,
                    score_alpha = EXCLUDED.score_alpha,
                    score_beta = EXCLUDED.score_beta
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
                _to_float(row.get("fund_age_years")),
                _to_float(row.get("max_drawdown")),
                _to_float(row.get("rolling_return_consistency")),
                _to_float(row.get("alpha")),
                _to_float(row.get("beta")),
                _to_float(row.get("score_alpha")),
                _to_float(row.get("score_beta")),
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
    """Fetch per-category avg returns for contextual why_ranked."""
    rows = await pool.fetch(f"""
        SELECT
            COALESCE(NULLIF(category, ''), 'Other') AS category,
            ROUND(AVG(returns_1y)::numeric, 1) AS avg_ret1y,
            ROUND(AVG(returns_3y)::numeric, 1) AS avg_ret3y,
            ROUND(AVG(returns_5y)::numeric, 1) AS avg_ret5y
        FROM {MF_TABLE}
        WHERE returns_3y IS NOT NULL
        GROUP BY COALESCE(NULLIF(category, ''), 'Other')
    """)
    out: dict[str, dict] = {}
    for r in rows:
        cat = str(r["category"])
        out[cat] = {
            "avg_ret1y": float(r["avg_ret1y"]) if r["avg_ret1y"] is not None else None,
            "avg_ret3y": float(r["avg_ret3y"]) if r["avg_ret3y"] is not None else None,
            "avg_ret5y": float(r["avg_ret5y"]) if r["avg_ret5y"] is not None else None,
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
    min_market_cap: float | None = None,
    max_market_cap: float | None = None,
    min_dividend_yield: float | None = None,
    min_pb: float | None = None,
    max_pb: float | None = None,
    source_status: str | None = None,
    sort_by: str = "score",
    sort_order: str = "desc",
    limit: int = 25,
    offset: int = 0,
) -> dict:
    allowed_sorts = {
        "score": "score",
        "change": "percent_change",
        "change_3m": "percent_change_3m",
        "change_1y": "percent_change_1y",
        "volume": "volume",
        "traded_value": "traded_value",
        "pe": "pe_ratio",
        "roe": "roe",
        "price": "last_price",
        "market_cap": "market_cap",
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
        conds.append("score_quality >= 55")
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
        conds.append("score_quality >= 50")

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
    if min_market_cap is not None:
        _add("market_cap >= ${idx}", float(min_market_cap))
    if max_market_cap is not None:
        _add("market_cap <= ${idx}", float(max_market_cap))
    if min_dividend_yield is not None:
        _add("dividend_yield >= ${idx}", float(min_dividend_yield))
    if min_pb is not None:
        _add("price_to_book >= ${idx}", float(min_pb))
    if max_pb is not None:
        _add("price_to_book <= ${idx}", float(max_pb))
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
            symbol, display_name, market, sector, industry,
            last_price, point_change, percent_change, volume, traded_value,
            pe_ratio, roe, roce, debt_to_equity, price_to_book, eps,
            score, score_momentum, score_growth,
            score_valuation, score_quality, score_institutional, score_risk,
            sector_percentile, lynch_classification, percent_change_5y,
            pledged_promoter_pct,
            sales_growth_yoy, profit_growth_yoy, opm_change, interest_coverage,
            compounded_sales_growth_3y, compounded_profit_growth_3y,
            total_assets, asset_growth_yoy, reserves_growth, debt_direction, cwip,
            cash_from_operations, cash_from_investing, cash_from_financing,
            num_shareholders_change_qoq, num_shareholders_change_yoy,
            synthetic_forward_pe,
            pl_annual, bs_annual, cf_annual, shareholding_quarterly,
            percent_change_3m, percent_change_1w,
            percent_change_1y, percent_change_3y,
            score_breakdown, tags, source_status, source_timestamp, ingested_at,
            primary_source, secondary_source,
            high_52w, low_52w, market_cap, dividend_yield,
            promoter_holding, fii_holding, dii_holding, government_holding, public_holding,
            num_shareholders, promoter_holding_change, fii_holding_change, dii_holding_change,
            beta, free_cash_flow, operating_cash_flow, total_cash, total_debt, total_revenue,
            gross_margins, operating_margins, profit_margins,
            revenue_growth, earnings_growth, forward_pe,
            analyst_target_mean, analyst_count, analyst_recommendation, analyst_recommendation_mean,
            payout_ratio
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
    min_return_1y: float | None = None,
    min_return_3y: float | None = None,
    min_return_5y: float | None = None,
    min_fund_age: float | None = None,
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
        "returns_5y": "returns_5y",
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
    elif preset_norm == "equity":
        conds.append(
            "(category ILIKE '%equity%' OR sub_category ILIKE '%cap%' OR sub_category ILIKE '%elss%'"
            " OR sub_category ILIKE '%value%' OR sub_category ILIKE '%focused%'"
            " OR sub_category ILIKE '%sector%' OR sub_category ILIKE '%thematic%'"
            " OR sub_category ILIKE '%index%')"
        )
    elif preset_norm == "hybrid":
        conds.append("(category ILIKE '%hybrid%')")
    elif preset_norm == "small-cap":
        conds.append("(sub_category ILIKE '%small%cap%')")
    elif preset_norm == "multi-cap":
        conds.append("(sub_category ILIKE '%multi%cap%')")
    elif preset_norm == "elss":
        conds.append("(sub_category ILIKE '%elss%' OR sub_category ILIKE '%tax%sav%')")
    elif preset_norm == "value-mf":
        conds.append("(sub_category ILIKE '%value%')")
    elif preset_norm == "focused":
        conds.append("(sub_category ILIKE '%focused%')")
    elif preset_norm == "sectoral":
        conds.append("(sub_category ILIKE '%sector%' OR sub_category ILIKE '%thematic%')")
    elif preset_norm == "short-duration":
        conds.append("(sub_category ILIKE '%short%dur%')")
    elif preset_norm == "corporate-bond":
        conds.append("(sub_category ILIKE '%corporate%bond%')")
    elif preset_norm == "banking-psu":
        conds.append("(sub_category ILIKE '%banking%' AND sub_category ILIKE '%psu%')")
    elif preset_norm == "gilt":
        conds.append("(sub_category ILIKE '%gilt%')")
    elif preset_norm == "liquid":
        conds.append("(sub_category ILIKE '%liquid%')")
    elif preset_norm == "overnight":
        conds.append("(sub_category ILIKE '%overnight%')")
    elif preset_norm == "dynamic-bond":
        conds.append("(sub_category ILIKE '%dynamic%bond%')")
    elif preset_norm == "money-market":
        conds.append("(sub_category ILIKE '%money%market%')")
    elif preset_norm == "aggressive-hybrid":
        conds.append("(sub_category ILIKE '%aggressive%')")
    elif preset_norm == "balanced-hybrid":
        conds.append("(sub_category ILIKE '%balanced%' OR sub_category ILIKE '%equity%savings%')")
    elif preset_norm == "conservative-hybrid":
        conds.append("(sub_category ILIKE '%conservative%')")

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
    if min_return_1y is not None:
        _add("(returns_1y >= ${idx} OR returns_1y IS NULL)", float(min_return_1y))
    if min_return_3y is not None:
        _add("(returns_3y >= ${idx} OR returns_3y IS NULL)", float(min_return_3y))
    if min_return_5y is not None:
        _add("(returns_5y >= ${idx} OR returns_5y IS NULL)", float(min_return_5y))
    if min_fund_age is not None:
        _add("(fund_age_years >= ${idx} OR fund_age_years IS NULL)", float(min_fund_age))
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
            primary_source, secondary_source,
            category_rank, category_total, fund_age_years,
            max_drawdown, rolling_return_consistency,
            alpha, beta, score_alpha, score_beta
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


async def unified_search(*, query: str, limit: int = 10) -> dict:
    pool = await get_pool()
    q = f"%{query.strip()}%"

    stock_rows = await pool.fetch(
        f"""
        SELECT symbol, display_name, sector, last_price, percent_change, score
        FROM {STOCK_TABLE}
        WHERE symbol ILIKE $1 OR display_name ILIKE $1
        ORDER BY score DESC NULLS LAST, symbol ASC
        LIMIT $2
        """,
        q,
        max(1, min(limit, 50)),
    )

    mf_rows = await pool.fetch(
        f"""
        SELECT scheme_code, scheme_name, category, nav, returns_3y, score
        FROM {MF_TABLE}
        WHERE scheme_name ILIKE $1 OR scheme_code ILIKE $1
        ORDER BY score DESC NULLS LAST, scheme_name ASC
        LIMIT $2
        """,
        q,
        max(1, min(limit, 50)),
    )

    return {
        "stocks": [record_to_dict(r) for r in stock_rows],
        "mutual_funds": [record_to_dict(r) for r in mf_rows],
    }


async def get_discover_home_data() -> dict:
    pool = await get_pool()

    _stock_cols = (
        "symbol, display_name, sector, last_price, percent_change, "
        "percent_change_3m, percent_change_1w, score, score_quality, score_growth, "
        "high_52w, low_52w, market_cap"
    )

    def _decorate_stock_list(rows) -> list[dict]:
        out = []
        for r in rows:
            d = record_to_dict(r)
            out.append(d)
        return out

    # Top stocks by 3M change (sector-diversified), fallback to score when 3M data unavailable
    has_3m = await pool.fetchval(
        f"SELECT EXISTS(SELECT 1 FROM {STOCK_TABLE} WHERE percent_change_3m IS NOT NULL LIMIT 1)"
    )
    if has_3m:
        top_stock_rows = await pool.fetch(
            f"""
            SELECT * FROM (
                SELECT {_stock_cols},
                       ROW_NUMBER() OVER (PARTITION BY COALESCE(sector, 'Other') ORDER BY percent_change_3m DESC) AS rn
                FROM {STOCK_TABLE}
                WHERE market = 'IN' AND source_status IN ('primary', 'fallback')
                      AND percent_change_3m IS NOT NULL AND percent_change_3m > 0
            ) sub WHERE rn <= 2
            ORDER BY percent_change_3m DESC LIMIT 8
            """
        )
    else:
        top_stock_rows = await pool.fetch(
            f"""
            SELECT * FROM (
                SELECT {_stock_cols},
                       ROW_NUMBER() OVER (PARTITION BY COALESCE(sector, 'Other') ORDER BY score DESC) AS rn
                FROM {STOCK_TABLE}
                WHERE market = 'IN' AND source_status IN ('primary', 'fallback') AND score >= 50
            ) sub WHERE rn <= 2
            ORDER BY score DESC LIMIT 8
            """
        )
    top_stocks = _decorate_stock_list(top_stock_rows)

    # Top equity mutual funds by score
    top_equity_mf_rows = await pool.fetch(
        f"""
        SELECT * FROM (
            SELECT scheme_code, scheme_name, category, sub_category, score, returns_1y,
                   ROW_NUMBER() OVER (PARTITION BY COALESCE(sub_category, category, 'Other') ORDER BY score DESC) AS rn
            FROM {MF_TABLE}
            WHERE plan_type = 'direct'
              AND (returns_1y IS NOT NULL OR returns_3y IS NOT NULL)
              AND score >= 50
              AND LOWER(COALESCE(category, '')) NOT LIKE '%%debt%%'
              AND LOWER(COALESCE(category, '')) NOT LIKE '%%liquid%%'
              AND LOWER(COALESCE(category, '')) NOT LIKE '%%gilt%%'
              AND LOWER(COALESCE(category, '')) NOT LIKE '%%money market%%'
              AND LOWER(COALESCE(category, '')) NOT LIKE '%%hybrid%%'
        ) sub WHERE rn <= 2
        ORDER BY score DESC LIMIT 5
        """
    )
    top_equity_funds = []
    for r in top_equity_mf_rows:
        d = record_to_dict(r)
        d["quality_badges"] = _compute_quality_badges(d)
        d["display_name"] = _clean_mf_display_name(d.get("scheme_name", ""))
        top_equity_funds.append(d)

    # Top debt mutual funds by score
    top_debt_mf_rows = await pool.fetch(
        f"""
        SELECT * FROM (
            SELECT scheme_code, scheme_name, category, sub_category, score, returns_1y,
                   ROW_NUMBER() OVER (PARTITION BY COALESCE(sub_category, category, 'Other') ORDER BY score DESC) AS rn
            FROM {MF_TABLE}
            WHERE plan_type = 'direct'
              AND (returns_1y IS NOT NULL OR returns_3y IS NOT NULL)
              AND score >= 40
              AND (
                  LOWER(COALESCE(category, '')) LIKE '%%debt%%'
                  OR LOWER(COALESCE(category, '')) LIKE '%%liquid%%'
                  OR LOWER(COALESCE(category, '')) LIKE '%%gilt%%'
                  OR LOWER(COALESCE(category, '')) LIKE '%%money market%%'
                  OR LOWER(COALESCE(sub_category, '')) LIKE '%%corporate bond%%'
                  OR LOWER(COALESCE(sub_category, '')) LIKE '%%overnight%%'
              )
        ) sub WHERE rn <= 2
        ORDER BY score DESC LIMIT 5
        """
    )
    top_debt_funds = []
    for r in top_debt_mf_rows:
        d = record_to_dict(r)
        d["quality_badges"] = _compute_quality_badges(d)
        d["display_name"] = _clean_mf_display_name(d.get("scheme_name", ""))
        top_debt_funds.append(d)

    # Trending this week: stocks with highest traded_value
    trending_rows = await pool.fetch(
        f"""
        SELECT {_stock_cols}
        FROM {STOCK_TABLE}
        WHERE market = 'IN'
        ORDER BY traded_value DESC NULLS LAST, symbol ASC
        LIMIT 8
        """
    )
    trending_this_week = _decorate_stock_list(trending_rows)

    # Top gainers (daily)
    gainer_rows = await pool.fetch(
        f"""
        SELECT {_stock_cols}
        FROM {STOCK_TABLE}
        WHERE market = 'IN' AND percent_change IS NOT NULL AND percent_change > 0
        ORDER BY percent_change DESC
        LIMIT 8
        """
    )
    gainers = _decorate_stock_list(gainer_rows)

    # Top gainers (3M)
    gainer_3m_rows = await pool.fetch(
        f"""
        SELECT {_stock_cols}
        FROM {STOCK_TABLE}
        WHERE market = 'IN' AND percent_change_3m IS NOT NULL AND percent_change_3m > 0
        ORDER BY percent_change_3m DESC
        LIMIT 8
        """
    )
    gainers_3m = _decorate_stock_list(gainer_3m_rows)

    # Top losers (daily)
    loser_rows = await pool.fetch(
        f"""
        SELECT {_stock_cols}
        FROM {STOCK_TABLE}
        WHERE market = 'IN' AND percent_change IS NOT NULL AND percent_change < 0
        ORDER BY percent_change ASC
        LIMIT 8
        """
    )
    losers = _decorate_stock_list(loser_rows)

    # Top losers (3M)
    loser_3m_rows = await pool.fetch(
        f"""
        SELECT {_stock_cols}
        FROM {STOCK_TABLE}
        WHERE market = 'IN' AND percent_change_3m IS NOT NULL AND percent_change_3m < 0
        ORDER BY percent_change_3m ASC
        LIMIT 8
        """
    )
    losers_3m = _decorate_stock_list(loser_3m_rows)

    # Sector Champions: top 1 stock from each sector by 70% score + 30% 3M change
    champion_rows = await pool.fetch(
        f"""
        SELECT * FROM (
            SELECT {_stock_cols},
                   ROW_NUMBER() OVER (
                       PARTITION BY sector
                       ORDER BY (score * 0.70 + COALESCE(percent_change_3m, 0) * 0.30) DESC
                   ) AS rn
            FROM {STOCK_TABLE}
            WHERE market = 'IN'
              AND sector IS NOT NULL
              AND sector != 'Other'
              AND source_status IN ('primary', 'fallback')
              AND score >= 30
        ) sub WHERE rn = 1
        ORDER BY (score * 0.70 + COALESCE(percent_change_3m, 0) * 0.30) DESC
        LIMIT 10
        """
    )
    sector_champions = _decorate_stock_list(champion_rows)

    quick_categories = [
        {"name": "Quality Stocks", "segment": "stocks", "preset": "quality"},
        {"name": "Value Stocks", "segment": "stocks", "preset": "value"},
        {"name": "High Volume", "segment": "stocks", "preset": "high-volume"},
        {"name": "Large Cap Funds", "segment": "mutual_funds", "preset": "large-cap"},
        {"name": "Flexi Cap Funds", "segment": "mutual_funds", "preset": "flexi-cap"},
        {"name": "Low Risk Funds", "segment": "mutual_funds", "preset": "low-risk"},
        {"name": "Index Funds", "segment": "mutual_funds", "preset": "index"},
        {"name": "Debt Funds", "segment": "mutual_funds", "preset": "debt"},
    ]

    return {
        "top_stocks": top_stocks,
        "top_equity_funds": top_equity_funds,
        "top_debt_funds": top_debt_funds,
        "trending_this_week": trending_this_week,
        "gainers": gainers,
        "gainers_3m": gainers_3m,
        "losers": losers,
        "losers_3m": losers_3m,
        "sector_champions": sector_champions,
        "quick_categories": quick_categories,
    }


async def get_stock_by_symbol(*, symbol: str) -> dict | None:
    """Fetch a single stock snapshot by symbol and return decorated data."""
    pool = await get_pool()
    row = await pool.fetchrow(
        f"SELECT * FROM {STOCK_TABLE} WHERE symbol = $1",
        symbol,
    )
    if row is None:
        return None
    d = record_to_dict(row)
    # Fetch sector stats for decoration
    sector = str(d.get("sector") or "Other")
    sector_stats_rows = await pool.fetch(
        f"""
        SELECT AVG(pe_ratio) AS avg_pe, AVG(roe) AS avg_roe
        FROM {STOCK_TABLE}
        WHERE sector = $1 AND pe_ratio IS NOT NULL
        """,
        sector,
    )
    stats = record_to_dict(sector_stats_rows[0]) if sector_stats_rows else {}
    return _decorate_stock_row(d, stats)


async def get_mf_by_scheme_code(*, scheme_code: str) -> dict | None:
    """Fetch a single MF snapshot by scheme_code and return decorated data."""
    pool = await get_pool()
    row = await pool.fetchrow(
        f"SELECT * FROM {MF_TABLE} WHERE scheme_code = $1",
        scheme_code,
    )
    if row is None:
        return None
    d = record_to_dict(row)
    # Fetch category stats for decoration
    category = str(d.get("category") or "Other")
    cat_stats_rows = await pool.fetch(
        f"""
        SELECT AVG(returns_1y) AS avg_ret1y,
               AVG(returns_3y) AS avg_ret3y,
               AVG(returns_5y) AS avg_ret5y,
               COUNT(*) AS total
        FROM {MF_TABLE}
        WHERE category = $1
        """,
        category,
    )
    stats = record_to_dict(cat_stats_rows[0]) if cat_stats_rows else {}
    return _decorate_mf_row(d, stats)


async def get_stock_price_history(*, symbol: str, days: int = 365) -> list[dict]:
    pool = await get_pool()
    rows = await pool.fetch(
        """
        SELECT trade_date, close
        FROM discover_stock_price_history
        WHERE symbol = $1
          AND trade_date >= CURRENT_DATE - make_interval(days => $2)
        ORDER BY trade_date ASC
        """,
        symbol,
        days,
    )
    return [record_to_dict(r) for r in rows]


async def get_bulk_stock_volatility_data() -> dict[str, dict]:
    """Fetch 3M price stats + short-term stats + 1Y/3Y/5Y returns for all stocks.

    Returns {symbol: {
        "std_dev": float,          # 3M daily return std dev (annualized, ×√252)
        "pct_change_3m": float,    # 3M price change %
        "pct_change_1w": float,    # 1W price change %
        "pct_change_1y": float,    # 1Y price change %
        "pct_change_3y": float,    # 3Y price change %
        "pct_change_5y": float,    # 5Y price change %
        "avg_vol_5d": float,       # 5-day avg volume
        "avg_vol_20d": float,      # 20-day avg volume
        "momentum_5d": float,      # 5-day return %
        "momentum_20d": float,     # 20-day return %
        "data_points": int,
    }}.
    """
    import math as _math

    pool = await get_pool()
    rows = await pool.fetch(
        """
        WITH daily_3m AS (
            SELECT symbol, close,
                   LAG(close) OVER (PARTITION BY symbol ORDER BY trade_date) AS prev_close,
                   FIRST_VALUE(close) OVER (PARTITION BY symbol ORDER BY trade_date ASC
                       ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_close,
                   LAST_VALUE(close) OVER (PARTITION BY symbol ORDER BY trade_date ASC
                       ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_close,
                   COUNT(*) OVER (PARTITION BY symbol) AS cnt
            FROM discover_stock_price_history
            WHERE trade_date >= CURRENT_DATE - INTERVAL '90 days'
        ),
        returns_3m AS (
            SELECT symbol,
                   (close - prev_close) / NULLIF(prev_close, 0) AS daily_return,
                   first_close, last_close, cnt
            FROM daily_3m
            WHERE prev_close IS NOT NULL AND prev_close > 0
        ),
        stats_3m AS (
            SELECT symbol,
                   STDDEV_SAMP(daily_return) AS std_dev,
                   MIN(first_close) AS first_close,
                   MAX(last_close) AS last_close,
                   MAX(cnt) AS data_points
            FROM returns_3m
            GROUP BY symbol
            HAVING COUNT(*) >= 5
        ),
        recent AS (
            SELECT symbol, close, volume,
                   ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY trade_date DESC) AS rn
            FROM discover_stock_price_history
            WHERE trade_date >= CURRENT_DATE - INTERVAL '30 days'
        ),
        short_term AS (
            SELECT symbol,
                   MAX(close) FILTER (WHERE rn = 1) AS latest_close,
                   MAX(close) FILTER (WHERE rn = 5) AS close_5d_ago,
                   MAX(close) FILTER (WHERE rn = 20) AS close_20d_ago,
                   AVG(volume) FILTER (WHERE rn <= 5) AS avg_vol_5d,
                   AVG(volume) FILTER (WHERE rn <= 20) AS avg_vol_20d
            FROM recent
            GROUP BY symbol
        ),
        -- 1Y return: first and last close over 365 days
        range_1y AS (
            SELECT symbol,
                   FIRST_VALUE(close) OVER (PARTITION BY symbol ORDER BY trade_date ASC
                       ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_close_1y,
                   LAST_VALUE(close) OVER (PARTITION BY symbol ORDER BY trade_date ASC
                       ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_close_1y,
                   COUNT(*) OVER (PARTITION BY symbol) AS cnt_1y
            FROM discover_stock_price_history
            WHERE trade_date >= CURRENT_DATE - INTERVAL '365 days'
        ),
        stats_1y AS (
            SELECT symbol,
                   MIN(first_close_1y) AS first_close_1y,
                   MAX(last_close_1y) AS last_close_1y,
                   MAX(cnt_1y) AS cnt_1y
            FROM range_1y
            GROUP BY symbol
            HAVING MAX(cnt_1y) >= 20
        ),
        -- 3Y return: first and last close over 1095 days
        range_3y AS (
            SELECT symbol,
                   FIRST_VALUE(close) OVER (PARTITION BY symbol ORDER BY trade_date ASC
                       ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_close_3y,
                   LAST_VALUE(close) OVER (PARTITION BY symbol ORDER BY trade_date ASC
                       ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_close_3y,
                   COUNT(*) OVER (PARTITION BY symbol) AS cnt_3y
            FROM discover_stock_price_history
            WHERE trade_date >= CURRENT_DATE - INTERVAL '1095 days'
        ),
        stats_3y AS (
            SELECT symbol,
                   MIN(first_close_3y) AS first_close_3y,
                   MAX(last_close_3y) AS last_close_3y,
                   MAX(cnt_3y) AS cnt_3y
            FROM range_3y
            GROUP BY symbol
            HAVING MAX(cnt_3y) >= 60
        ),
        -- 5Y return: first and last close over 1825 days
        range_5y AS (
            SELECT symbol,
                   FIRST_VALUE(close) OVER (PARTITION BY symbol ORDER BY trade_date ASC
                       ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_close_5y,
                   LAST_VALUE(close) OVER (PARTITION BY symbol ORDER BY trade_date ASC
                       ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_close_5y,
                   COUNT(*) OVER (PARTITION BY symbol) AS cnt_5y
            FROM discover_stock_price_history
            WHERE trade_date >= CURRENT_DATE - INTERVAL '1825 days'
        ),
        stats_5y AS (
            SELECT symbol,
                   MIN(first_close_5y) AS first_close_5y,
                   MAX(last_close_5y) AS last_close_5y,
                   MAX(cnt_5y) AS cnt_5y
            FROM range_5y
            GROUP BY symbol
            HAVING MAX(cnt_5y) >= 120
        )
        SELECT s.symbol, s.std_dev, s.first_close, s.last_close, s.data_points,
               st.latest_close, st.close_5d_ago, st.close_20d_ago,
               st.avg_vol_5d, st.avg_vol_20d,
               y.first_close_1y, y.last_close_1y,
               y3.first_close_3y, y3.last_close_3y,
               y5.first_close_5y, y5.last_close_5y
        FROM stats_3m s
        LEFT JOIN short_term st ON s.symbol = st.symbol
        LEFT JOIN stats_1y y ON s.symbol = y.symbol
        LEFT JOIN stats_3y y3 ON s.symbol = y3.symbol
        LEFT JOIN stats_5y y5 ON s.symbol = y5.symbol
        """
    )
    _SQRT_252 = _math.sqrt(252)
    result: dict[str, dict] = {}
    for r in rows:
        sym = r["symbol"]
        first_close = float(r["first_close"]) if r["first_close"] else None
        last_close = float(r["last_close"]) if r["last_close"] else None
        latest = float(r["latest_close"]) if r["latest_close"] else None
        close_5d = float(r["close_5d_ago"]) if r["close_5d_ago"] else None
        close_20d = float(r["close_20d_ago"]) if r["close_20d_ago"] else None

        pct_3m = None
        if first_close and first_close > 0 and last_close:
            pct_3m = round(((last_close - first_close) / first_close) * 100, 2)

        pct_1w = None
        if latest and close_5d and close_5d > 0:
            pct_1w = round(((latest - close_5d) / close_5d) * 100, 2)

        momentum_5d = None
        if latest and close_5d and close_5d > 0:
            momentum_5d = round(((latest - close_5d) / close_5d) * 100, 2)

        momentum_20d = None
        if latest and close_20d and close_20d > 0:
            momentum_20d = round(((latest - close_20d) / close_20d) * 100, 2)

        # 1Y and 3Y returns
        pct_1y = None
        fc_1y = float(r["first_close_1y"]) if r.get("first_close_1y") else None
        lc_1y = float(r["last_close_1y"]) if r.get("last_close_1y") else None
        if fc_1y and fc_1y > 0 and lc_1y:
            pct_1y = round(((lc_1y - fc_1y) / fc_1y) * 100, 2)

        pct_3y = None
        fc_3y = float(r["first_close_3y"]) if r.get("first_close_3y") else None
        lc_3y = float(r["last_close_3y"]) if r.get("last_close_3y") else None
        if fc_3y and fc_3y > 0 and lc_3y:
            pct_3y = round(((lc_3y - fc_3y) / fc_3y) * 100, 2)

        pct_5y = None
        fc_5y = float(r["first_close_5y"]) if r.get("first_close_5y") else None
        lc_5y = float(r["last_close_5y"]) if r.get("last_close_5y") else None
        if fc_5y and fc_5y > 0 and lc_5y:
            pct_5y = round(((lc_5y - fc_5y) / fc_5y) * 100, 2)

        # Annualize std_dev: daily → annual (×√252)
        raw_std = float(r["std_dev"]) if r["std_dev"] else None
        annualized_std = round(raw_std * _SQRT_252, 4) if raw_std is not None else None

        result[sym] = {
            "std_dev": annualized_std,
            "pct_change_3m": pct_3m,
            "pct_change_1w": pct_1w,
            "pct_change_1y": pct_1y,
            "pct_change_3y": pct_3y,
            "pct_change_5y": pct_5y,
            "momentum_5d": momentum_5d,
            "momentum_20d": momentum_20d,
            "avg_vol_5d": float(r["avg_vol_5d"]) if r["avg_vol_5d"] else None,
            "avg_vol_20d": float(r["avg_vol_20d"]) if r["avg_vol_20d"] else None,
            "data_points": int(r["data_points"]) if r["data_points"] else 0,
        }
    return result


async def get_mf_nav_history(*, scheme_code: str, days: int = 365) -> list[dict]:
    pool = await get_pool()
    rows = await pool.fetch(
        """
        SELECT nav_date, nav
        FROM discover_mf_nav_history
        WHERE scheme_code = $1
          AND nav_date >= CURRENT_DATE - make_interval(days => $2)
        ORDER BY nav_date ASC
        """,
        scheme_code,
        days,
    )
    return [record_to_dict(r) for r in rows]


async def get_stock_peers(*, symbol: str, limit: int = 5) -> list[dict]:
    """Get peer stocks in the same sector, sorted by score descending."""
    pool = await get_pool()
    sector_stats = await _get_stock_sector_stats(pool)

    target = await pool.fetchrow(
        f"SELECT sector FROM {STOCK_TABLE} WHERE symbol = $1",
        symbol,
    )
    if target is None:
        return []

    sector = target["sector"]
    rows = await pool.fetch(
        f"""
        SELECT
            symbol, display_name, market, sector, industry,
            last_price, point_change, percent_change, volume, traded_value,
            pe_ratio, roe, roce, debt_to_equity, price_to_book, eps,
            score, score_momentum, score_growth,
            score_valuation, score_quality, score_institutional, score_risk,
            sector_percentile, lynch_classification, percent_change_5y,
            pledged_promoter_pct,
            sales_growth_yoy, profit_growth_yoy, opm_change, interest_coverage,
            compounded_sales_growth_3y, compounded_profit_growth_3y,
            total_assets, asset_growth_yoy, reserves_growth, debt_direction, cwip,
            cash_from_operations, cash_from_investing, cash_from_financing,
            num_shareholders_change_qoq, num_shareholders_change_yoy,
            synthetic_forward_pe,
            pl_annual, bs_annual, cf_annual, shareholding_quarterly,
            percent_change_3m, percent_change_1w,
            percent_change_1y, percent_change_3y,
            score_breakdown, tags, source_status, source_timestamp, ingested_at,
            primary_source, secondary_source,
            high_52w, low_52w, market_cap, dividend_yield,
            promoter_holding, fii_holding, dii_holding, government_holding, public_holding,
            num_shareholders, promoter_holding_change, fii_holding_change, dii_holding_change,
            beta, free_cash_flow, operating_cash_flow, total_cash, total_debt, total_revenue,
            gross_margins, operating_margins, profit_margins,
            revenue_growth, earnings_growth, forward_pe,
            analyst_target_mean, analyst_count, analyst_recommendation, analyst_recommendation_mean,
            payout_ratio
        FROM {STOCK_TABLE}
        WHERE sector = $1 AND symbol != $2
        ORDER BY score DESC NULLS LAST
        LIMIT $3
        """,
        sector,
        symbol,
        limit,
    )

    items = []
    for r in rows:
        d = record_to_dict(r)
        s = str(d.get("sector") or "Other")
        items.append(_decorate_stock_row(d, sector_stats.get(s)))
    return items


async def get_mf_peers(*, scheme_code: str, limit: int = 5) -> list[dict]:
    """Get peer mutual funds in the same sub-category (or category fallback), sorted by score."""
    pool = await get_pool()
    category_stats = await _get_mf_category_stats(pool)

    target = await pool.fetchrow(
        f"SELECT category, sub_category FROM {MF_TABLE} WHERE scheme_code = $1",
        scheme_code,
    )
    if target is None:
        return []

    sub_cat = target["sub_category"]
    cat = target["category"]

    # Try sub_category first, fall back to category if too few peers.
    peer_label = sub_cat or cat
    rows = await pool.fetch(
        f"""
        SELECT
            scheme_code, scheme_name, amc, category, sub_category,
            plan_type, option_type, nav, nav_date,
            expense_ratio, aum_cr, risk_level,
            returns_1y, returns_3y, returns_5y, std_dev, sharpe, sortino,
            score, score_return, score_risk, score_cost, score_consistency,
            score_breakdown, tags, source_status, source_timestamp, ingested_at,
            primary_source, secondary_source,
            category_rank, category_total, fund_age_years,
            max_drawdown, rolling_return_consistency,
            alpha, beta, score_alpha, score_beta
        FROM {MF_TABLE}
        WHERE COALESCE(NULLIF(sub_category, ''), category) = $1
          AND scheme_code != $2
          AND LOWER(COALESCE(plan_type, 'direct')) = 'direct'
        ORDER BY score DESC NULLS LAST
        LIMIT $3
        """,
        peer_label,
        scheme_code,
        limit,
    )

    # Fallback to broader category if sub_category yielded < 3 peers.
    if len(rows) < 3 and sub_cat and sub_cat != cat:
        rows = await pool.fetch(
            f"""
            SELECT
                scheme_code, scheme_name, amc, category, sub_category,
                plan_type, option_type, nav, nav_date,
                expense_ratio, aum_cr, risk_level,
                returns_1y, returns_3y, returns_5y, std_dev, sharpe, sortino,
                score, score_return, score_risk, score_cost, score_consistency,
                score_breakdown, tags, source_status, source_timestamp, ingested_at,
                primary_source, secondary_source,
                category_rank, category_total, fund_age_years
            FROM {MF_TABLE}
            WHERE category = $1
              AND scheme_code != $2
              AND LOWER(COALESCE(plan_type, 'direct')) = 'direct'
            ORDER BY score DESC NULLS LAST
            LIMIT $3
            """,
            cat,
            scheme_code,
            limit,
        )

    items = []
    for r in rows:
        d = record_to_dict(r)
        c = str(d.get("category") or "Other")
        items.append(_decorate_mf_row(d, category_stats.get(c)))
    return items

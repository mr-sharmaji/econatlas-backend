from __future__ import annotations

import json
import logging
import math
import re
from datetime import date, datetime, timezone
from typing import Literal

from app.core.database import get_pool, parse_ts, record_to_dict

logger = logging.getLogger(__name__)

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
    # Remove trailing "Growth" even without separator (e.g. "FundGrowth")
    result = re.sub(r'\s*Growth\s*(?:Plan\s*)?(?:Option\s*)?$', '', result, flags=re.IGNORECASE).strip()
    # Remove "Growth Plan" and everything after it (e.g. "FundGrowth Plan - Bonus Option")
    result = re.sub(r'\s*Growth\s+Plan\b.*$', '', result, flags=re.IGNORECASE).strip()
    # Remove "Direct Growth" mid-string remnants
    result = re.sub(r'\s+Direct\s+Growth\b', '', result, flags=re.IGNORECASE).strip()
    # Remove "(Growth)" and "(Direct)" in parentheses
    result = re.sub(r'\s*\(Growth\)\s*', ' ', result, flags=re.IGNORECASE).strip()
    result = re.sub(r'\s*\(Direct\)\s*', ' ', result, flags=re.IGNORECASE).strip()
    # Remove "Growth / Payment" or "Growth/Payment"
    result = re.sub(r'\s*[-–]?\s*Growth\s*/\s*Payment\b', '', result, flags=re.IGNORECASE).strip()
    # Remove trailing "Direct" (with or without dash)
    result = re.sub(r'\s*[-–]?\s*Direct\s*$', '', result, flags=re.IGNORECASE).strip()
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

    ts = _to_float(stored_sb.get("technical_score") or row.get("technical_score"))
    if ts is not None:
        result["technical_score"] = round(ts, 2)

    rsi = _to_float(stored_sb.get("rsi_14") or row.get("rsi_14"))
    if rsi is not None:
        result["rsi_14"] = round(rsi, 2)

    at = stored_sb.get("action_tag") or row.get("action_tag")
    if at is not None:
        result["action_tag"] = str(at)

    atr = stored_sb.get("action_tag_reasoning") or row.get("action_tag_reasoning")
    if atr is not None:
        result["action_tag_reasoning"] = str(atr)

    sc = stored_sb.get("score_confidence") or row.get("score_confidence")
    if sc is not None:
        result["score_confidence"] = str(sc)

    ta = stored_sb.get("trend_alignment") or row.get("trend_alignment")
    if ta is not None:
        result["trend_alignment"] = str(ta)

    bs = stored_sb.get("breakout_signal") or row.get("breakout_signal")
    if bs is not None:
        result["breakout_signal"] = str(bs)

    return result


def _mf_breakdown_payload(row: dict) -> dict:
    # If score_breakdown is already a dict with the new keys, use it directly
    existing = row.get("score_breakdown")
    if isinstance(existing, dict) and "performance_score" in existing:
        return existing
    perf = _to_float(row.get("score_performance"))
    cat_fit = _to_float(row.get("score_category_fit"))
    beta_score = _to_float(row.get("score_beta"))
    return {
        "performance_score": round(perf, 2) if perf is not None else None,
        "consistency_score": round(_to_float(row.get("score_consistency")) or 0.0, 2) if _to_float(row.get("score_consistency")) is not None else None,
        "risk_score": round(_to_float(row.get("score_risk")) or 0.0, 2) if _to_float(row.get("score_risk")) is not None else None,
        "cost_score": round(_to_float(row.get("score_cost")) or 0.0, 2) if _to_float(row.get("score_cost")) is not None else None,
        "category_fit_score": round(cat_fit, 2) if cat_fit is not None else None,
        "beta_score": round(beta_score, 2) if beta_score is not None else None,
        # Legacy
        "return_score": round(perf, 2) if perf is not None else None,
    }


def _stock_why_ranked(row: dict, industry_stats: dict | None = None) -> list[str]:
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
        if industry_stats and industry_stats.get("avg_roe"):
            reasons.append(f"ROE of {roe:.1f}% vs industry avg {industry_stats['avg_roe']:.1f}%.")
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
    if pe is not None and pe > 0 and industry_stats and industry_stats.get("median_pe"):
        med = industry_stats["median_pe"]
        if pe < med * 0.8:
            reasons.append(f"PE of {pe:.1f} is below industry median of {med:.1f}.")

    status = _normalize_source_status(row.get("source_status"))
    if status != "primary":
        reasons.append("Some metrics are based on fallback or limited data.")
    if not reasons:
        reasons.append("Balanced rank across momentum, liquidity, and fundamentals.")
    return reasons[:4]


def _mf_why_ranked(row: dict, category_stats: dict | None = None) -> list[str]:
    reasons: list[str] = []
    sub_cat = row.get("fund_classification") or row.get("sub_category") or ""
    ret3 = _to_float(row.get("returns_3y"))

    # Return context vs sub-category avg
    if ret3 is not None:
        avg = category_stats.get("avg_ret3y") if category_stats else None
        if avg is not None:
            reasons.append(f"3Y return of {ret3:.1f}% vs {sub_cat} avg {avg:.1f}%.")
        elif ret3 >= 12:
            reasons.append(f"Strong 3Y return of {ret3:.1f}%.")

    # Risk-adjusted quality
    sharpe = _to_float(row.get("sharpe"))
    sortino = _to_float(row.get("sortino"))
    if sortino is not None and sortino >= 1.5:
        reasons.append(f"Sortino of {sortino:.2f} shows strong risk-adjusted performance.")
    elif sharpe is not None and sharpe >= 1.5:
        reasons.append(f"Sharpe of {sharpe:.2f} indicates strong risk-adjusted returns.")

    # Cost context
    expense = _to_float(row.get("expense_ratio"))
    if expense is not None and expense <= 1.0:
        reasons.append(f"Low expense ratio of {expense:.2f}%.")

    # Rank context
    sub_pctl = _to_float(row.get("sub_category_percentile"))
    if sub_pctl is not None and sub_pctl >= 75:
        reasons.append(f"Top {100 - sub_pctl:.0f}% within {sub_cat} funds.")

    aum = _to_float(row.get("aum_cr"))
    if aum is not None and aum < 100:
        reasons.append("Small fund size (< 100 Cr) — metrics may be less stable.")

    status = _normalize_source_status(row.get("source_status"))
    if status != "primary":
        reasons.append("Some metrics are based on limited data.")
    if not reasons:
        reasons.append("Balanced risk-return-cost score within sub-category.")
    return reasons[:4]


def _compute_quality_badges(row: dict) -> list[str]:
    badges: list[str] = []
    # Use sub_category_percentile if available, fallback to category_rank
    sub_pctl = _to_float(row.get("sub_category_percentile"))
    if sub_pctl is not None and sub_pctl >= 90:
        badges.append("Top Performer")
    elif sub_pctl is None:
        # Prefer sub_category_rank (more granular), fallback to category_rank
        sub_rank = _to_int(row.get("sub_category_rank"))
        sub_total = _to_int(row.get("sub_category_total"))
        if sub_rank is not None and sub_total is not None:
            if sub_rank <= max(1, int(sub_total * 0.1)):
                badges.append("Top Performer")
        else:
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
    fund_age_years = _to_float(row.get("fund_age_years"))
    if fund_age_years is not None and fund_age_years >= 5:
        badges.append("Proven Track Record")
    return badges


def _mf_fund_insights(row: dict, category_stats: dict | None = None) -> list[dict]:
    """Build a list of positive/negative fund insights for the MF detail screen."""
    insights: list[dict] = []

    score = _to_float(row.get("score")) or 0
    ret3 = _to_float(row.get("returns_3y"))
    avg_ret3 = category_stats.get("avg_ret3y") if category_stats else None

    # Score-based insight
    if score < 35:
        insights.append({"text": "Below average overall score — consider alternatives in this category", "sentiment": "negative"})
    elif score >= 80:
        insights.append({"text": "Top-tier fund with excellent overall score", "sentiment": "positive"})

    # ── Positive signals ──
    if ret3 is not None and avg_ret3 is not None and ret3 > avg_ret3:
        insights.append({"text": f"Outperforms category average over 3 years ({ret3:.1f}% vs {avg_ret3:.1f}%)", "sentiment": "positive"})

    expense = _to_float(row.get("expense_ratio"))
    if expense is not None and expense < 1.0:
        insights.append({"text": f"Low cost with expense ratio of {expense:.2f}%", "sentiment": "positive"})

    fund_age = _to_float(row.get("fund_age_years"))
    if fund_age is not None and fund_age >= 5:
        insights.append({"text": f"Established fund with {fund_age:.1f} years of track record", "sentiment": "positive"})

    sub_rank = _to_int(row.get("sub_category_rank"))
    sub_total = _to_int(row.get("sub_category_total"))
    cat_rank = _to_int(row.get("category_rank"))
    cat_total = _to_int(row.get("category_total"))
    sub_cat = row.get("fund_classification") or row.get("sub_category") or row.get("category") or "its category"

    if sub_rank is not None and sub_total and sub_total > 0 and sub_rank <= max(1, int(sub_total * 0.2)):
        insights.append({"text": f"Ranked in top 20% of {sub_cat}", "sentiment": "positive"})
    elif cat_rank is not None and cat_total and cat_total > 0 and cat_rank <= max(1, int(cat_total * 0.2)):
        cat_name = row.get("category") or "its category"
        insights.append({"text": f"Ranked in top 20% of {cat_name}", "sentiment": "positive"})

    sharpe = _to_float(row.get("sharpe"))
    if sharpe is not None and sharpe > 1.5:
        insights.append({"text": f"Strong risk-adjusted returns (Sharpe {sharpe:.2f})", "sentiment": "positive"})

    # ── Negative signals ──
    if ret3 is not None and avg_ret3 is not None and ret3 < avg_ret3:
        insights.append({"text": f"Underperforms category average over 3 years ({ret3:.1f}% vs {avg_ret3:.1f}%)", "sentiment": "negative"})

    # Negative returns (works even without category avg)
    ret1 = _to_float(row.get("returns_1y"))
    if ret1 is not None and ret1 < 0:
        insights.append({"text": f"Negative 1Y return of {ret1:.1f}%", "sentiment": "negative"})

    if ret3 is not None and ret3 < 5 and avg_ret3 is None:
        insights.append({"text": f"Low 3Y return of {ret3:.1f}%", "sentiment": "negative"})

    if expense is not None and expense > 2.0:
        insights.append({"text": f"High expense ratio at {expense:.2f}%", "sentiment": "negative"})

    if fund_age is not None and fund_age < 3:
        insights.append({"text": f"Relatively new fund with only {fund_age:.1f} years of track record", "sentiment": "negative"})

    if sub_rank is not None and sub_total and sub_total > 0 and sub_rank > max(1, int(sub_total * 0.7)):
        insights.append({"text": f"Ranked in bottom 30% of {sub_cat}", "sentiment": "negative"})
    elif cat_rank is not None and cat_total and cat_total > 0 and cat_rank > max(1, int(cat_total * 0.7)):
        cat_name = row.get("category") or "its category"
        insights.append({"text": f"Ranked in bottom 30% of {cat_name}", "sentiment": "negative"})

    max_dd = _to_float(row.get("max_drawdown"))
    if max_dd is not None and max_dd < -20:
        insights.append({"text": f"High drawdown risk ({max_dd:.1f}%)", "sentiment": "negative"})

    if sharpe is not None and sharpe < 0.8:
        insights.append({"text": f"Below-average risk-adjusted returns (Sharpe {sharpe:.2f})", "sentiment": "negative"})

    aum = _to_float(row.get("aum_cr"))
    if aum is not None and aum < 100:
        insights.append({"text": "Small fund size (< 100 Cr) — metrics may be less stable", "sentiment": "negative"})
    elif aum is not None and aum > 10000:
        insights.append({"text": f"Large AUM ({aum:,.0f} Cr) — well-established and liquid", "sentiment": "positive"})

    # ── Deeper signals (backend-only data) ──
    sortino = _to_float(row.get("sortino"))
    if sortino is not None and sortino >= 2.0:
        insights.append({"text": f"Excellent downside-adjusted returns (Sortino {sortino:.2f})", "sentiment": "positive"})
    elif sortino is not None and sortino < 0.5:
        insights.append({"text": f"Poor downside-adjusted returns (Sortino {sortino:.2f})", "sentiment": "negative"})

    alpha = _to_float(row.get("alpha"))
    if alpha is not None and alpha > 3:
        insights.append({"text": f"Generates strong alpha of {alpha:.1f}% over benchmark", "sentiment": "positive"})
    elif alpha is not None and alpha < -1:
        insights.append({"text": f"Negative alpha of {alpha:.1f}% — lags behind benchmark", "sentiment": "negative"})

    beta = _to_float(row.get("beta"))
    if beta is not None and beta > 1.3:
        insights.append({"text": f"High beta ({beta:.2f}) — amplifies market swings", "sentiment": "negative"})

    rolling_consistency = _to_float(row.get("rolling_return_consistency"))
    if rolling_consistency is not None and rolling_consistency >= 80:
        insights.append({"text": f"Highly predictable returns ({rolling_consistency:.0f}% rolling consistency)", "sentiment": "positive"})
    elif rolling_consistency is not None and rolling_consistency < 40:
        insights.append({"text": f"Unpredictable returns ({rolling_consistency:.0f}% rolling consistency)", "sentiment": "negative"})

    # Returns momentum: 1Y vs 3Y
    ret1 = _to_float(row.get("returns_1y"))
    if ret1 is not None and ret3 is not None and ret3 > 0:
        if ret1 > ret3 * 1.5:
            insights.append({"text": "Recent returns momentum is accelerating", "sentiment": "positive"})
        elif ret1 < ret3 * 0.3 and ret3 > 5:
            insights.append({"text": "Recent performance is slowing down vs historical", "sentiment": "negative"})

    # Category fit score
    cat_fit = _to_float(row.get("score_category_fit"))
    if cat_fit is not None and cat_fit < 40:
        insights.append({"text": "Fund deviates significantly from its stated mandate", "sentiment": "negative"})

    return insights


def _generate_metric_insights(row: dict, industry_stats: dict | None = None) -> dict:
    """Generate contextual metric explanations + sentiment for a stock.

    Returns {metric_key: {"explanation": str, "sentiment": str}} where
    sentiment is one of "positive", "negative", "neutral", "warning".
    Only metrics with non-null values get an entry.
    """
    ss = industry_stats or {}
    insights: dict[str, dict] = {}

    def _f(key: str) -> float | None:
        return _to_float(row.get(key))

    def _sf(key: str) -> float | None:
        return _to_float(ss.get(key))

    def _add(key: str, explanation: str, sentiment: str) -> None:
        insights[key] = {"explanation": explanation, "sentiment": sentiment}

    industry = row.get("industry") or "the industry"

    # ── Valuation ───────────────────────────────────────────────────
    pe = _f("pe_ratio")
    if pe is None:
        eps_val = _f("eps")
        if eps_val is not None and eps_val <= 0:
            _add("pe_ratio",
                 f"PE ratio is not available because the company is currently loss-making (EPS: ₹{eps_val:.1f}). "
                 f"PE requires positive earnings — it becomes meaningless when a company is unprofitable. "
                 f"Look at Price-to-Book or EV/Revenue for alternative valuation.",
                 "negative")
    if pe is not None:
        avg_pe = _sf("avg_pe")
        if avg_pe and avg_pe > 0:
            ratio = pe / avg_pe
            if ratio < 0.8:
                _add("pe_ratio",
                     f"PE ratio tells you how many years of current earnings you're paying for. "
                     f"At {pe:.1f}x, this stock trades below the {industry} average of {avg_pe:.1f}x. "
                     f"You're getting a discount relative to peers — worth investigating why.",
                     "positive")
            elif ratio > 1.5:
                _add("pe_ratio",
                     f"PE ratio tells you how many years of current earnings you're paying for. "
                     f"At {pe:.1f}x, this is significantly above the {industry} average of {avg_pe:.1f}x. "
                     f"You're paying a steep premium — justified only if growth is strong enough to bring the PE down over time.",
                     "negative")
            else:
                _add("pe_ratio",
                     f"PE ratio tells you how many years of current earnings you're paying for. "
                     f"At {pe:.1f}x, this is broadly in line with the {industry} average of {avg_pe:.1f}x. "
                     f"The market prices this stock fairly relative to its industry peers.",
                     "neutral")
        else:
            if pe < 15:
                _add("pe_ratio",
                     f"PE ratio tells you how many years of current earnings you're paying for. "
                     f"At {pe:.1f}x, this is on the lower end. "
                     f"Could signal undervaluation, or the market may see limited growth ahead.",
                     "positive")
            elif pe > 40:
                _add("pe_ratio",
                     f"PE ratio tells you how many years of current earnings you're paying for. "
                     f"At {pe:.1f}x, the market has high growth expectations baked in. "
                     f"Any earnings miss could lead to a sharp correction.",
                     "negative")
            else:
                _add("pe_ratio",
                     f"PE ratio tells you how many years of current earnings you're paying for. "
                     f"At {pe:.1f}x, this is in a moderate range. "
                     f"Neither cheap nor expensive — look at growth and margins for a fuller picture.",
                     "neutral")

    pb = _f("price_to_book")
    if pb is None:
        bs = row.get("bs_annual")
        if isinstance(bs, dict):
            _res = bs.get("reserves") or []
            if _res and isinstance(_res[-1], (int, float)) and _res[-1] < 0:
                _add("price_to_book",
                     f"Price-to-Book is not available because book value is negative — "
                     f"accumulated losses (₹{abs(_res[-1]):,.0f} Cr) have eroded all shareholder equity. "
                     f"This indicates severe financial distress.",
                     "negative")
    if pb is not None:
        avg_pb = _sf("avg_pb")
        if avg_pb and avg_pb > 0:
            if pb < avg_pb * 0.7:
                _add("price_to_book",
                     f"Price-to-Book compares the stock price to the company's net asset value per share. "
                     f"P/B of {pb:.2f} is below the {industry} average of {avg_pb:.2f}. "
                     f"You're paying less per rupee of book value than peers — could indicate undervaluation.",
                     "positive")
            elif pb > avg_pb * 1.5:
                _add("price_to_book",
                     f"Price-to-Book compares the stock price to the company's net asset value per share. "
                     f"P/B of {pb:.2f} is well above the {industry} average of {avg_pb:.2f}. "
                     f"The market is assigning a premium to this company's assets — typically seen in high-ROE businesses.",
                     "negative")
            else:
                _add("price_to_book",
                     f"Price-to-Book compares the stock price to the company's net asset value per share. "
                     f"P/B of {pb:.2f} is close to the {industry} average of {avg_pb:.2f}. "
                     f"Fairly valued on a book-value basis.",
                     "neutral")
        else:
            if pb < 2:
                _add("price_to_book",
                     f"Price-to-Book compares the stock price to the company's net asset value per share. "
                     f"P/B of {pb:.2f} is relatively low. "
                     f"Could signal undervaluation, especially for asset-heavy businesses.",
                     "positive")
            elif pb > 5:
                _add("price_to_book",
                     f"Price-to-Book compares the stock price to the company's net asset value per share. "
                     f"P/B of {pb:.2f} is on the higher side. "
                     f"The market expects significant future earnings growth beyond current assets.",
                     "negative")
            else:
                _add("price_to_book",
                     f"Price-to-Book compares the stock price to the company's net asset value per share. "
                     f"P/B of {pb:.2f} is in a moderate range. "
                     f"Neither stretched nor cheap — check ROE to see if the premium is justified.",
                     "neutral")

    eps = _f("eps")
    if eps is not None:
        if eps > 0:
            _add("eps",
                 f"EPS is the net profit divided by total shares — it tells you how much the company earns per share. "
                 f"At ₹{eps:.1f}, the company is profitable on a per-share basis. "
                 f"Look for consistently growing EPS over time as a sign of a quality investment.",
                 "positive")
        else:
            _add("eps",
                 f"EPS is the net profit divided by total shares — it tells you how much the company earns per share. "
                 f"At ₹{eps:.1f}, the company is currently loss-making. "
                 f"Unless it's in an early growth phase with a clear path to profitability, this is a red flag.",
                 "negative")

    fwd_pe = _f("forward_pe")
    if fwd_pe is not None and pe is not None and pe > 0:
        discount_pct = ((pe - fwd_pe) / pe) * 100
        if fwd_pe < pe * 0.85:
            _add("forward_pe",
                 f"Forward PE uses analyst estimates of future earnings instead of past earnings. "
                 f"At {fwd_pe:.1f}x vs trailing PE of {pe:.1f}x, analysts expect earnings to grow ~{discount_pct:.0f}%. "
                 f"The stock looks cheaper on future profits — a good sign if growth materialises.",
                 "positive")
        elif fwd_pe > pe * 1.1:
            _add("forward_pe",
                 f"Forward PE uses analyst estimates of future earnings instead of past earnings. "
                 f"At {fwd_pe:.1f}x vs trailing PE of {pe:.1f}x, analysts expect earnings to decline. "
                 f"The stock is getting more expensive on future profits — proceed with caution.",
                 "negative")
        else:
            _add("forward_pe",
                 f"Forward PE uses analyst estimates of future earnings instead of past earnings. "
                 f"At {fwd_pe:.1f}x, it's close to the trailing PE of {pe:.1f}x. "
                 f"Analysts expect earnings to remain roughly stable — no major surprise expected either way.",
                 "neutral")
    elif fwd_pe is not None:
        _add("forward_pe",
             f"Forward PE uses analyst estimates of future earnings instead of past earnings. "
             f"Currently at {fwd_pe:.1f}x based on consensus estimates. "
             f"Compare with industry peers and trailing PE for a complete valuation picture.",
             "neutral")

    # PEG from score_breakdown
    sb = row.get("score_breakdown") or {}
    if isinstance(sb, str):
        import json as _j
        try:
            sb = _j.loads(sb)
        except (ValueError, TypeError):
            sb = {}
    peg = _to_float(sb.get("peg_ratio"))
    if peg is None or peg <= 0:
        if pe is None:
            _add("peg_ratio",
                 f"PEG ratio is not available because PE ratio is undefined — the company is loss-making. "
                 f"PEG requires both a positive PE and a positive growth rate to be meaningful.",
                 "negative")
    if peg is not None and peg > 0:
        if peg < 1:
            _add("peg_ratio",
                 f"PEG adjusts the PE ratio for the company's growth rate — below 1 generally signals undervaluation. "
                 f"At {peg:.2f}, you're paying less than 1x the growth rate. "
                 f"This suggests the market hasn't fully priced in the company's growth potential.",
                 "positive")
        elif peg > 2:
            _add("peg_ratio",
                 f"PEG adjusts the PE ratio for the company's growth rate — above 2 often signals overvaluation. "
                 f"At {peg:.2f}, you're paying a significant premium relative to growth. "
                 f"Unless there's a strong moat or turnaround story, this looks expensive for the growth on offer.",
                 "negative")
        else:
            _add("peg_ratio",
                 f"PEG adjusts the PE ratio for the company's growth rate — between 1 and 2 is generally fair. "
                 f"At {peg:.2f}, the price seems reasonable for the growth rate. "
                 f"Not a bargain, but not overpaying either.",
                 "neutral")

    div_yield = _f("dividend_yield")
    if div_yield is not None:
        avg_dy = _sf("avg_dy")
        payout_raw = _f("payout_ratio")
        payout_pct = (payout_raw * 100 if payout_raw is not None and abs(payout_raw) < 1 else payout_raw) if payout_raw is not None else None
        if div_yield > 2 and (payout_pct is None or payout_pct < 80):
            ctx = f"At {div_yield:.2f}%"
            if avg_dy and avg_dy > 0:
                ctx += f" vs the {industry} average of {avg_dy:.2f}%"
            ctx += ", this stock offers above-average income."
            sust = ""
            if payout_pct is not None:
                sust = f" With a payout ratio of {payout_pct:.1f}%, the dividend looks sustainable."
            _add("dividend_yield",
                 f"Dividend yield tells you how much annual income you earn per rupee invested. "
                 f"{ctx}{sust}",
                 "positive")
        elif div_yield > 0:
            _add("dividend_yield",
                 f"Dividend yield tells you how much annual income you earn per rupee invested. "
                 f"At {div_yield:.2f}%, the yield is modest. "
                 f"The company returns some cash to shareholders but is likely retaining most earnings for growth.",
                 "neutral")
        else:
            _add("dividend_yield",
                 f"Dividend yield tells you how much annual income you earn per rupee invested. "
                 f"This company doesn't currently pay a dividend. "
                 f"Typical for growth-stage companies that reinvest all profits back into the business.",
                 "neutral")

    beta = _f("beta")
    if beta is not None:
        if beta < 0.7:
            _add("beta",
                 f"Beta measures how much a stock moves relative to the market (Nifty). "
                 f"At {beta:.2f}, this stock is significantly less volatile than the market. "
                 f"A defensive pick — may lag in rallies but tends to hold up better during downturns.",
                 "neutral")
        elif beta > 1.3:
            _add("beta",
                 f"Beta measures how much a stock moves relative to the market (Nifty). "
                 f"At {beta:.2f}, this stock amplifies market moves — both up and down. "
                 f"Higher reward potential but also higher risk; size your position accordingly.",
                 "warning")
        else:
            _add("beta",
                 f"Beta measures how much a stock moves relative to the market (Nifty). "
                 f"At {beta:.2f}, this stock moves roughly in line with the broader market. "
                 f"Neither overly defensive nor aggressively volatile.",
                 "neutral")

    mcap = _f("market_cap")
    if mcap is not None:
        if mcap > 100_000:
            band, desc = "Large Cap", "among India's biggest companies — stable, liquid, and well-researched"
        elif mcap > 20_000:
            band, desc = "Mid Cap", "a mid-sized company — offers a blend of growth potential and relative stability"
        elif mcap > 5_000:
            band, desc = "Small Cap", "a small-cap stock — higher growth potential but also higher volatility and liquidity risk"
        else:
            band, desc = "Micro Cap", "a micro-cap stock — can deliver outsized returns but comes with significant risk and low liquidity"
        _add("market_cap",
             f"Market cap is the total market value of all outstanding shares. "
             f"At ₹{mcap:,.0f} Cr, this is a {band} stock — {desc}.",
             "neutral")

    # ── Profitability ──────────────────────────────────────────────
    roe = _f("roe")
    if roe is not None:
        avg_roe = _sf("avg_roe")
        if avg_roe and avg_roe > 0:
            if roe > avg_roe * 1.2:
                _add("roe",
                     f"ROE measures how much profit the company generates for every ₹100 of shareholder equity. "
                     f"At {roe:.1f}% vs the {industry} average of {avg_roe:.1f}%, this company is significantly more efficient. "
                     f"Consistently high ROE indicates strong management and a durable competitive advantage.",
                     "positive")
            elif roe < avg_roe * 0.6:
                _add("roe",
                     f"ROE measures how much profit the company generates for every ₹100 of shareholder equity. "
                     f"At {roe:.1f}% vs the {industry} average of {avg_roe:.1f}%, returns are lagging peers. "
                     f"The company is either less efficient or may be going through a cyclical downturn.",
                     "negative")
            else:
                _add("roe",
                     f"ROE measures how much profit the company generates for every ₹100 of shareholder equity. "
                     f"At {roe:.1f}%, it's broadly in line with the {industry} average of {avg_roe:.1f}%. "
                     f"Competitive positioning is on par with peers.",
                     "neutral")
        else:
            if roe > 15:
                _add("roe",
                     f"ROE measures how much profit the company generates for every ₹100 of shareholder equity. "
                     f"At {roe:.1f}%, this is above the 15% benchmark for quality businesses. "
                     f"Strong returns suggest efficient capital allocation.",
                     "positive")
            elif roe < 8:
                _add("roe",
                     f"ROE measures how much profit the company generates for every ₹100 of shareholder equity. "
                     f"At {roe:.1f}%, returns are below the 8% threshold for concern. "
                     f"The company may be struggling with profitability or carrying excess equity.",
                     "negative")
            else:
                _add("roe",
                     f"ROE measures how much profit the company generates for every ₹100 of shareholder equity. "
                     f"At {roe:.1f}%, returns are in a moderate range. "
                     f"Adequate but not exceptional — check the trend over time.",
                     "neutral")

    roce = _f("roce")
    if roce is not None:
        _roce_context = ""
        _gr = row.get("growth_ranges")
        _gr_roce = _gr.get("return_on_capital_employed") if isinstance(_gr, dict) else None
        if isinstance(_gr_roce, dict):
            _ctx_parts = []
            _r_latest = _to_float(_gr_roce.get("1y") if _gr_roce.get("1y") is not None else _gr_roce.get("ttm"))
            _r3 = _to_float(_gr_roce.get("3y"))
            if _r_latest is not None and _r3 is not None:
                if _r_latest > _r3 + 1:
                    _ctx_parts.append(
                        f"Trend: ROCE improved from {_r3:.0f}% (3Y) to {_r_latest:.0f}% (latest)."
                    )
                elif _r_latest < _r3 - 1:
                    _ctx_parts.append(
                        f"Trend: ROCE softened from {_r3:.0f}% (3Y) to {_r_latest:.0f}% (latest)."
                    )
                else:
                    _ctx_parts.append(
                        f"Trend: ROCE is broadly stable around {_r_latest:.0f}%."
                    )
            _snapshots = []
            for _k, _label in (("10y", "10Y"), ("5y", "5Y"), ("3y", "3Y"), ("1y", "1Y"), ("ttm", "TTM")):
                _v = _to_float(_gr_roce.get(_k))
                if _v is not None:
                    _snapshots.append(f"{_label} {_v:.0f}%")
            if _snapshots:
                _ctx_parts.append("Historical ROCE: " + ", ".join(_snapshots) + ".")
            if _ctx_parts:
                _roce_context = " " + " ".join(_ctx_parts)

        avg_roce = _sf("avg_roce")
        if avg_roce and avg_roce > 0:
            if roce > avg_roce * 1.2:
                _add("roce",
                     f"ROCE measures how efficiently the company uses all its capital — both equity and debt. "
                     f"At {roce:.1f}% vs the {industry} average of {avg_roce:.1f}%, capital efficiency is well above peers. "
                     f"This suggests the business has strong pricing power or operates in a high-return niche.{_roce_context}",
                     "positive")
            elif roce < avg_roce * 0.6:
                _add("roce",
                     f"ROCE measures how efficiently the company uses all its capital — both equity and debt. "
                     f"At {roce:.1f}% vs the {industry} average of {avg_roce:.1f}%, returns on capital are lagging. "
                     f"The company may be over-invested or operating in a low-margin segment.{_roce_context}",
                     "negative")
            else:
                _add("roce",
                     f"ROCE measures how efficiently the company uses all its capital — both equity and debt. "
                     f"At {roce:.1f}%, it's close to the {industry} average of {avg_roce:.1f}%. "
                     f"Capital deployment is on par with peers — neither a standout nor a laggard.{_roce_context}",
                     "neutral")
        else:
            if roce > 15:
                _add("roce",
                     f"ROCE measures how efficiently the company uses all its capital — both equity and debt. "
                     f"At {roce:.1f}%, this exceeds the 15% benchmark for well-run businesses. "
                     f"The company is generating solid returns on every rupee of capital deployed.{_roce_context}",
                     "positive")
            elif roce < 8:
                _add("roce",
                     f"ROCE measures how efficiently the company uses all its capital — both equity and debt. "
                     f"At {roce:.1f}%, returns are below the 8% mark. "
                     f"The company isn't generating enough from its capital — a concern for long-term investors.{_roce_context}",
                     "negative")
            else:
                _add("roce",
                     f"ROCE measures how efficiently the company uses all its capital — both equity and debt. "
                     f"At {roce:.1f}%, returns are in a moderate range. "
                     f"Not bad, but there's room for improvement in capital efficiency.{_roce_context}",
                     "neutral")

    # Financial businesses (banks/NBFCs) use financing margin from Screener
    # instead of operating margin.
    _pl_data = row.get("pl_annual")
    _is_financial_margin = (
        isinstance(_pl_data, dict)
        and bool(_pl_data.get("financing_margin_pct"))
        and not bool(_pl_data.get("opm_pct"))
    )
    _margin_key = "financial_margin" if _is_financial_margin else "operating_margins"
    _margin_name = "Financial margin" if _is_financial_margin else "Operating margin"
    _margin_scope = (
        "net interest income relative to earning assets"
        if _is_financial_margin
        else "profit from core business operations before interest and taxes"
    )

    opm = _f("operating_margins")
    # For financial businesses, always prefer Screener financing_margin_pct
    # over generic operating_margins (which can be misleading for lenders).
    if isinstance(_pl_data, dict) and _is_financial_margin:
        _fm = _pl_data.get("financing_margin_pct") or []
        _yrs = _pl_data.get("years") or []
        _n_yr = len(_yrs)
        _idx = _n_yr if len(_fm) > _n_yr else len(_fm) - 1
        if _idx >= 0 and _idx < len(_fm):
            _v = _fm[_idx]
            if isinstance(_v, (int, float)):
                opm = float(_v) / 100.0

    if opm is not None:
        avg_opm = _sf("avg_opm")
        opm_pct = opm * 100 if abs(opm) < 1 else opm
        # Industry avg_opm is not a meaningful peer baseline for financial margins.
        avg_opm_pct = None if _is_financial_margin else (
            (avg_opm * 100 if avg_opm and abs(avg_opm) < 1 else avg_opm) if avg_opm else None
        )

        # Compute historical margin trend from P&L arrays.
        _opm_trend = ""
        if isinstance(_pl_data, dict):
            _pl_opm = (
                _pl_data.get("financing_margin_pct") or []
                if _is_financial_margin
                else _pl_data.get("opm_pct") or []
            )
            _pl_years = _pl_data.get("years") or []
            _n_yr = len(_pl_years)
            if _n_yr >= 3 and len(_pl_opm) >= _n_yr and _pl_opm[0] is not None:
                _opm_old = _pl_opm[0]
                _yr_start = _pl_years[0] if _pl_years else ""
                if opm_pct > _opm_old + 2:
                    _opm_trend = f" {_margin_name} has expanded from {_opm_old:.0f}% ({_yr_start}) to {opm_pct:.0f}% (TTM)."
                elif opm_pct < _opm_old - 2:
                    _opm_trend = f" {_margin_name} has compressed from {_opm_old:.0f}% ({_yr_start}) to {opm_pct:.0f}% (TTM)."
                else:
                    _opm_trend = f" {_margin_name} has remained around {opm_pct:.0f}% over {_n_yr} years."

        if avg_opm_pct and avg_opm_pct > 0:
            if opm_pct > avg_opm_pct * 1.2:
                _add(_margin_key,
                     f"{_margin_name} shows {_margin_scope}. "
                     f"At {opm_pct:.1f}%, this exceeds the {industry} average of {avg_opm_pct:.1f}%. "
                     f"The company has better cost control or pricing power than its peers.{_opm_trend}",
                     "positive")
            elif opm_pct < avg_opm_pct * 0.6:
                _add(_margin_key,
                     f"{_margin_name} shows {_margin_scope}. "
                     f"At {opm_pct:.1f}%, this is below the {industry} average of {avg_opm_pct:.1f}%. "
                     f"The company may be facing competitive pressure on pricing or higher input costs.{_opm_trend}",
                     "negative")
            else:
                _add(_margin_key,
                     f"{_margin_name} shows {_margin_scope}. "
                     f"At {opm_pct:.1f}%, it's near the {industry} average of {avg_opm_pct:.1f}%. "
                     f"Operational efficiency is on par with peers.{_opm_trend}",
                     "neutral")
        else:
            if _is_financial_margin:
                if opm_pct > 4:
                    _add(_margin_key,
                         f"{_margin_name} shows {_margin_scope}. "
                         f"At {opm_pct:.1f}%, spreads are healthy for lending economics.{_opm_trend}",
                         "positive")
                elif opm_pct < 2:
                    _add(_margin_key,
                         f"{_margin_name} shows {_margin_scope}. "
                         f"At {opm_pct:.1f}%, spreads are thin and earnings can be sensitive to funding costs.{_opm_trend}",
                         "negative")
                else:
                    _add(_margin_key,
                         f"{_margin_name} shows {_margin_scope}. "
                         f"At {opm_pct:.1f}%, spreads are in a moderate range.{_opm_trend}",
                         "neutral")
            else:
                if opm_pct > 15:
                    _add(_margin_key,
                         f"{_margin_name} shows {_margin_scope}. "
                         f"At {opm_pct:.1f}%, the company retains a healthy share of revenue as operating profit. "
                         f"Good cost discipline and potential pricing power.{_opm_trend}",
                         "positive")
                elif opm_pct < 5:
                    _add(_margin_key,
                         f"{_margin_name} shows {_margin_scope}. "
                         f"At {opm_pct:.1f}%, margins are thin. "
                         f"The business has little buffer against cost increases or revenue slowdowns.{_opm_trend}",
                         "negative")
                else:
                    _add(_margin_key,
                         f"{_margin_name} shows {_margin_scope}. "
                         f"At {opm_pct:.1f}%, margins are in a moderate range. "
                         f"Watch the trend — expanding margins are a bullish signal.{_opm_trend}",
                         "neutral")

    npm = _f("profit_margins")
    if npm is not None:
        npm_pct = npm * 100 if abs(npm) < 1 else npm
        avg_npm = _sf("avg_npm")
        avg_npm_pct = (avg_npm * 100 if avg_npm and abs(avg_npm) < 1 else avg_npm) if avg_npm else None

        # Compute historical net margin trend from P&L
        _npm_trend = ""
        _pl_data = row.get("pl_annual")
        if isinstance(_pl_data, dict):
            _pl_s = _pl_data.get("sales") or []
            _pl_np = _pl_data.get("net_profit") or []
            _pl_years = _pl_data.get("years") or []
            _n_yr = len(_pl_years)
            if _n_yr >= 3 and len(_pl_s) >= _n_yr and len(_pl_np) >= _n_yr:
                _s0 = _pl_s[0]
                _np0 = _pl_np[0]
                if _s0 and _np0 is not None and _s0 != 0:
                    _old_npm = _np0 / _s0 * 100
                    # Compute range of net margins across years
                    _npm_vals = []
                    for _i in range(_n_yr):
                        _si = _pl_s[_i]
                        _ni = _pl_np[_i]
                        if _si and _ni is not None and _si != 0:
                            _npm_vals.append(_ni / _si * 100)
                    _range_str = ""
                    if len(_npm_vals) >= 2:
                        _npm_min = min(_npm_vals)
                        _npm_max = max(_npm_vals)
                        if _npm_min != _npm_max:
                            _range_str = f" (range: {_npm_min:.0f}%–{_npm_max:.0f}%)"
                    _yr_start = _pl_years[0] if _pl_years else ""
                    if npm_pct > _old_npm + 2:
                        _npm_trend = f" Net margin has expanded from {_old_npm:.0f}% ({_yr_start}) to {npm_pct:.0f}% (TTM){_range_str} — improving profitability."
                    elif npm_pct < _old_npm - 2:
                        _npm_trend = f" Net margin has compressed from {_old_npm:.0f}% ({_yr_start}) to {npm_pct:.0f}% (TTM){_range_str} — profitability is declining."
                    else:
                        _npm_trend = f" Net margin has been stable around {npm_pct:.0f}% over {_n_yr} years{_range_str}."

        if avg_npm_pct and avg_npm_pct > 0 and npm_pct > avg_npm_pct * 1.2:
            _add("profit_margins",
                 f"Net profit margin is what the company keeps after paying all expenses — operations, interest, and taxes. "
                 f"At {npm_pct:.1f}%, this beats the {industry} average of {avg_npm_pct:.1f}%. "
                 f"Superior profitability that translates into better earnings growth for shareholders.{_npm_trend}",
                 "positive")
        elif avg_npm_pct and avg_npm_pct > 0 and npm_pct < avg_npm_pct * 0.6:
            _add("profit_margins",
                 f"Net profit margin is what the company keeps after paying all expenses — operations, interest, and taxes. "
                 f"At {npm_pct:.1f}%, this trails the {industry} average of {avg_npm_pct:.1f}%. "
                 f"Higher costs or interest burden may be eating into bottom-line profitability.{_npm_trend}",
                 "negative")
        else:
            if npm_pct > 10:
                _add("profit_margins",
                     f"Net profit margin is what the company keeps after paying all expenses — operations, interest, and taxes. "
                     f"At {npm_pct:.1f}%, the company converts a healthy portion of revenue into profit.{_npm_trend}",
                     "positive")
            elif npm_pct < 3:
                _add("profit_margins",
                     f"Net profit margin is what the company keeps after paying all expenses — operations, interest, and taxes. "
                     f"At {npm_pct:.1f}%, very little revenue flows to the bottom line. "
                     f"The company needs volume or pricing improvements to become meaningfully profitable.{_npm_trend}",
                     "negative")
            else:
                _add("profit_margins",
                     f"Net profit margin is what the company keeps after paying all expenses — operations, interest, and taxes. "
                     f"At {npm_pct:.1f}%, margins are moderate.{_npm_trend}",
                     "neutral")

    # ── Revenue Profit chart ────────────────────────────
    pl = row.get("pl_annual")
    if isinstance(pl, dict):
        _pl_sales = pl.get("sales") or []
        _pl_np = pl.get("net_profit") or []
        _n_pl = len(pl.get("years") or [])
        if _n_pl >= 3 and len(_pl_sales) >= _n_pl and len(_pl_np) >= _n_pl:
            # Compare oldest available vs latest annual year
            _s_old = _pl_sales[0]
            _s_new = _pl_sales[_n_pl - 1]
            _np_old = _pl_np[0]
            _np_new = _pl_np[_n_pl - 1]
            _rev_grew = _s_new > _s_old if _s_old and _s_new else None
            _profit_grew = _np_new > _np_old if _np_old is not None and _np_new is not None else None

            parts = []
            if _rev_grew is True and _profit_grew is True:
                parts.append(
                    f"Revenue and profit have both grown over the last {_n_pl} years — "
                    f"a healthy sign of business scale and earnings strength.")
                _sent = "positive"
            elif _rev_grew is True and _profit_grew is False:
                parts.append(
                    f"Revenue has grown over {_n_pl} years but profits haven't kept pace — "
                    f"rising costs or reinvestment may be pressuring earnings.")
                _sent = "warning"
            elif _rev_grew is False and _profit_grew is True:
                parts.append(
                    f"Revenue has declined over {_n_pl} years, but profit has improved. "
                    f"This can happen with cost optimization, but verify whether this is sustainable.")
                _sent = "neutral"
            elif _rev_grew is False:
                parts.append(
                    f"Revenue and profit have weakened over {_n_pl} years — "
                    f"the business may be facing demand pressure or structural challenges.")
                _sent = "negative"
            else:
                parts.append(f"The chart shows {_n_pl} years of revenue and profit data.")
                _sent = "neutral"

            _add("revenue_profit_margins", " ".join(parts), _sent)

    # ── Growth ─────────────────────────────────────────────────────
    rg_val = _f("revenue_growth")
    if rg_val is not None:
        rg_pct = rg_val * 100 if abs(rg_val) < 2 else rg_val
        if rg_pct > 20:
            _add("revenue_growth",
                 f"Revenue growth shows how fast the company's top line is expanding year over year. "
                 f"At {rg_pct:.1f}%, the company is growing rapidly — gaining market share or expanding into new segments. "
                 f"Sustained high growth like this typically drives stock price appreciation over time.",
                 "positive")
        elif rg_pct > 0:
            _add("revenue_growth",
                 f"Revenue growth shows how fast the company's top line is expanding year over year. "
                 f"At {rg_pct:.1f}%, the business is growing but at a measured pace. "
                 f"Steady growth is fine for mature businesses, but fast-growing industries demand more.",
                 "neutral")
        else:
            _add("revenue_growth",
                 f"Revenue growth shows how fast the company's top line is expanding year over year. "
                 f"At {rg_pct:.1f}%, revenue is actually declining. "
                 f"The company may be losing market share or facing demand headwinds — a warning sign unless it's a temporary blip.",
                 "negative")

    eg_val = _f("earnings_growth")
    if eg_val is not None:
        eg_pct = eg_val * 100 if abs(eg_val) < 2 else eg_val
        # Detect if the company is currently in losses
        npm = _f("profit_margins")
        is_loss_making = npm is not None and npm < 0
        if eg_pct > 20:
            if is_loss_making:
                _add("earnings_growth",
                     f"Earnings growth tracks how fast profits are changing year over year. "
                     f"At {eg_pct:+.1f}%, losses have narrowed significantly compared to last year. "
                     f"Improvement is encouraging, but the company is still unprofitable — watch for a sustained turnaround.",
                     "neutral")
            else:
                _add("earnings_growth",
                     f"Earnings growth tracks how fast profits are increasing year over year. "
                     f"At {eg_pct:+.1f}%, profits are expanding strongly — faster than revenue growth means improving margins. "
                     f"This is exactly what drives PE compression and share price growth.",
                     "positive")
        elif eg_pct > 0:
            if is_loss_making:
                _add("earnings_growth",
                     f"Earnings growth tracks how fast profits are changing year over year. "
                     f"At {eg_pct:+.1f}%, losses have narrowed slightly. "
                     f"The company is still in the red — a full turnaround to profitability remains some distance away.",
                     "neutral")
            else:
                _add("earnings_growth",
                     f"Earnings growth tracks how fast profits are increasing year over year. "
                     f"At {eg_pct:+.1f}%, profits are growing at a moderate pace. "
                     f"Acceptable for stable businesses, but watch whether margins are steady or compressing.",
                     "neutral")
        else:
            if is_loss_making:
                _add("earnings_growth",
                     f"Earnings growth tracks how fast profits are changing year over year. "
                     f"At {eg_pct:+.1f}%, losses have widened compared to last year. "
                     f"The company is burning more cash — financial distress risk rises the longer this continues.",
                     "negative")
            else:
                _add("earnings_growth",
                     f"Earnings growth tracks how fast profits are increasing year over year. "
                     f"At {eg_pct:+.1f}%, profits are declining. "
                     f"Unless the company is investing heavily for future growth, falling earnings erode the investment case.",
                     "negative")

    csg_val = _f("compounded_sales_growth_3y")
    gr = row.get("growth_ranges") or {}
    _gr_sales = gr.get("compounded_sales_growth", {})
    _gr_profit = gr.get("compounded_profit_growth", {})
    _csg_10y = _gr_sales.get("10y")
    _csg_5y = _gr_sales.get("5y")
    _csg_ttm = _gr_sales.get("ttm")
    # Use the oldest available period as the primary display value (matches app)
    _csg_display = _csg_10y or _csg_5y or csg_val
    _csg_period = "10-year" if _csg_10y is not None else ("5-year" if _csg_5y is not None else "3-year")
    if csg_val is not None:
        # Build multi-period context: all periods
        _ctx_parts = []
        if _csg_10y is not None:
            _ctx_parts.append(f"10Y: {_csg_10y:.0f}%")
        if _csg_5y is not None:
            _ctx_parts.append(f"5Y: {_csg_5y:.0f}%")
        _ctx_parts.append(f"3Y: {csg_val:.0f}%")
        if _csg_ttm is not None:
            _ctx_parts.append(f"TTM: {_csg_ttm:.0f}%")
        _ctx = f" ({', '.join(_ctx_parts)})"
        # Detect deceleration: current growth below long-term average
        _decel = ""
        _oldest_csg = _csg_10y or _csg_5y
        _current_csg = _csg_ttm if _csg_ttm is not None else csg_val
        if _oldest_csg is not None and _current_csg < _oldest_csg - 3:
            _decel = f" However, growth has decelerated from {_oldest_csg:.0f}% to {_current_csg:.0f}% recently — the pace is slowing."
        if _csg_display > 15:
            _add("compounded_sales_growth_3y",
                 f"Revenue CAGR smooths out annual fluctuations to show the underlying growth trajectory. "
                 f"At {_csg_display:.0f}% over {_csg_period}, revenue has compounded at a strong pace. "
                 f"Consistent top-line expansion signals a business with structural tailwinds.{_ctx}{_decel}",
                 "warning" if _decel else "positive")
        elif _csg_display > 5:
            _add("compounded_sales_growth_3y",
                 f"Revenue CAGR smooths out annual fluctuations to show the underlying growth trajectory. "
                 f"At {_csg_display:.0f}% over {_csg_period}, the company has grown steadily. "
                 f"Moderate growth — typical of mature businesses in established industries.{_ctx}{_decel}",
                 "warning" if _decel else "neutral")
        elif _csg_display > 0:
            _add("compounded_sales_growth_3y",
                 f"Revenue CAGR smooths out annual fluctuations to show the underlying growth trajectory. "
                 f"At {_csg_display:.0f}% over {_csg_period}, growth has been sluggish. "
                 f"The business may be in a mature phase or facing competitive headwinds.{_ctx}{_decel}",
                 "warning" if _decel else "neutral")
        else:
            _add("compounded_sales_growth_3y",
                 f"Revenue CAGR smooths out annual fluctuations to show the underlying growth trajectory. "
                 f"At {_csg_display:.0f}% over {_csg_period}, revenue has been shrinking. "
                 f"A structural decline unless the company is pivoting to a new business model.{_ctx}",
                 "negative")

    cpg_val = _f("compounded_profit_growth_3y")
    _cpg_10y = _gr_profit.get("10y")
    _cpg_5y = _gr_profit.get("5y")
    _cpg_ttm = _gr_profit.get("ttm")
    _cpg_display = _cpg_10y or _cpg_5y or cpg_val
    _cpg_period = "10-year" if _cpg_10y is not None else ("5-year" if _cpg_5y is not None else "3-year")
    if cpg_val is not None:
        _ctx_parts = []
        if _cpg_10y is not None:
            _ctx_parts.append(f"10Y: {_cpg_10y:.0f}%")
        if _cpg_5y is not None:
            _ctx_parts.append(f"5Y: {_cpg_5y:.0f}%")
        _ctx_parts.append(f"3Y: {cpg_val:.0f}%")
        if _cpg_ttm is not None:
            _ctx_parts.append(f"TTM: {_cpg_ttm:.0f}%")
        _ctx = f" ({', '.join(_ctx_parts)})"
        # Detect deceleration
        _decel = ""
        _oldest_cpg = _cpg_10y or _cpg_5y
        _current_cpg = _cpg_ttm if _cpg_ttm is not None else cpg_val
        if _oldest_cpg is not None and _current_cpg < _oldest_cpg - 3:
            _decel = f" However, profit growth has decelerated from {_oldest_cpg:.0f}% to {_current_cpg:.0f}% recently — momentum is fading."
        if _cpg_display > 15:
            _add("compounded_profit_growth_3y",
                 f"Profit CAGR shows whether profitability is consistently improving. "
                 f"At {_cpg_display:.0f}% over {_cpg_period}, profits have compounded strongly. "
                 f"This signals improving efficiency and operating leverage — a quality indicator.{_ctx}{_decel}",
                 "warning" if _decel else "positive")
        elif _cpg_display > 5:
            _add("compounded_profit_growth_3y",
                 f"Profit CAGR shows whether profitability is consistently improving. "
                 f"At {_cpg_display:.0f}% over {_cpg_period}, profit growth has been steady. "
                 f"The company is profitable and stable, if not a high-growth compounder.{_ctx}{_decel}",
                 "warning" if _decel else "neutral")
        elif _cpg_display > 0:
            _add("compounded_profit_growth_3y",
                 f"Profit CAGR shows whether profitability is consistently improving. "
                 f"At {_cpg_display:.0f}% over {_cpg_period}, profits are barely growing. "
                 f"Check if rising costs or competitive pressure is squeezing margins.{_ctx}{_decel}",
                 "warning" if _decel else "neutral")
        else:
            _add("compounded_profit_growth_3y",
                 f"Profit CAGR shows whether profitability is consistently improving. "
                 f"At {_cpg_display:.0f}% over {_cpg_period}, profits have been declining. "
                 f"A serious concern — the business is becoming less profitable over time.{_ctx}",
                 "negative")

    # ── Stock Price CAGR ────────────────────────────────────────────
    _gr_price = gr.get("stock_price_cagr", {})
    _spc_1y = _gr_price.get("1y")
    _spc_10y = _gr_price.get("10y")
    _spc_5y = _gr_price.get("5y")
    _spc_3y = _gr_price.get("3y")
    # Use 1Y as the primary display value (matches app)
    _spc_val = _spc_1y
    if _spc_val is not None:
        _ctx_parts = []
        if _spc_10y is not None:
            _ctx_parts.append(f"10Y: {_spc_10y:.0f}%")
        if _spc_5y is not None:
            _ctx_parts.append(f"5Y: {_spc_5y:.0f}%")
        if _spc_3y is not None:
            _ctx_parts.append(f"3Y: {_spc_3y:.0f}%")
        _ctx_parts.append(f"1Y: {_spc_val:.0f}%")
        _ctx = f" ({', '.join(_ctx_parts)})"
        # Detect deceleration from long-term
        _decel = ""
        _oldest_spc = _spc_10y or _spc_5y or _spc_3y
        if _oldest_spc is not None and _spc_val < _oldest_spc - 5:
            _decel = f" Recent returns ({_spc_val:.0f}%) lag the long-term CAGR ({_oldest_spc:.0f}%) — the stock may have run ahead of fundamentals or hit a correction."
        if _spc_val > 20:
            _add("stock_price_cagr",
                 f"Stock price CAGR shows how the stock has performed for investors over time. "
                 f"At {_spc_val:.0f}% over the last year, the stock has delivered strong returns. "
                 f"Check if fundamentals support this move or if the rally is speculative.{_ctx}{_decel}",
                 "warning" if _decel else "positive")
        elif _spc_val > 0:
            _add("stock_price_cagr",
                 f"Stock price CAGR shows how the stock has performed for investors over time. "
                 f"At {_spc_val:.0f}% over the last year, the stock has delivered modest positive returns. "
                 f"Steady appreciation in line with or slightly above market averages.{_ctx}{_decel}",
                 "warning" if _decel else "neutral")
        elif _spc_val > -10:
            _add("stock_price_cagr",
                 f"Stock price CAGR shows how the stock has performed for investors over time. "
                 f"At {_spc_val:.0f}% over the last year, the stock has slightly underperformed. "
                 f"A small decline — could be a buying opportunity if fundamentals remain strong.{_ctx}{_decel}",
                 "warning")
        else:
            _add("stock_price_cagr",
                 f"Stock price CAGR shows how the stock has performed for investors over time. "
                 f"At {_spc_val:.0f}% over the last year, the stock has fallen significantly. "
                 f"Investigate whether the decline reflects deteriorating fundamentals or a temporary setback.{_ctx}",
                 "negative")

    # ── Debt & Leverage ────────────────────────────────────────────
    de = _f("debt_to_equity")
    if de is not None:
        avg_de = _sf("avg_de")
        if de < 0:
            _add("debt_to_equity",
                 f"Debt-to-Equity compares total borrowings to shareholder equity — lower means less financial risk. "
                 f"D/E is negative ({de:.2f}) because shareholder equity is negative — accumulated losses have wiped out the equity base. "
                 f"This is a severe financial distress signal.",
                 "negative")
        elif de < 0.01:
            _add("debt_to_equity",
                 f"Debt-to-Equity compares total borrowings to shareholder equity — lower means less financial risk. "
                 f"This company is essentially debt-free. "
                 f"Zero leverage means no interest burden and full financial flexibility, though it may also mean the company is under-utilising cheap capital.",
                 "positive")
        elif de < 0.5:
            ctx = f"At {de:.2f}, leverage is conservative"
            if avg_de and avg_de > 0:
                ctx += f" (below the {industry} average of {avg_de:.2f})"
            _add("debt_to_equity",
                 f"Debt-to-Equity compares total borrowings to shareholder equity — lower means less financial risk. "
                 f"{ctx}. "
                 f"The company uses minimal debt, leaving a comfortable buffer for economic downturns.",
                 "positive")
        elif de > 1.5:
            ctx = f"At {de:.2f}, the company is heavily leveraged"
            if avg_de and avg_de > 0:
                ctx += f" ({industry} average is {avg_de:.2f})"
            _add("debt_to_equity",
                 f"Debt-to-Equity compares total borrowings to shareholder equity — lower means less financial risk. "
                 f"{ctx}. "
                 f"High debt increases risk during downturns and limits the company's flexibility — make sure earnings can service the interest.",
                 "negative")
        else:
            ctx = f"At {de:.2f}, leverage is moderate"
            if avg_de and avg_de > 0:
                ctx += f" ({industry} average is {avg_de:.2f})"
            _add("debt_to_equity",
                 f"Debt-to-Equity compares total borrowings to shareholder equity — lower means less financial risk. "
                 f"{ctx}. "
                 f"A reasonable balance between using debt for growth and keeping risk manageable.",
                 "neutral")

    ic = _f("interest_coverage")
    if ic is not None:
        if ic > 5:
            _add("interest_coverage",
                 f"Interest coverage tells you how many times the company's operating profit can pay its interest bill. "
                 f"At {ic:.1f}x, the company comfortably covers its debt obligations. "
                 f"Even if profits dip temporarily, there's a wide margin of safety.",
                 "positive")
        elif ic > 2:
            _add("interest_coverage",
                 f"Interest coverage tells you how many times the company's operating profit can pay its interest bill. "
                 f"At {ic:.1f}x, coverage is adequate but not generous. "
                 f"The company can service its debt today, but a significant earnings dip could strain finances.",
                 "neutral")
        elif ic > 0:
            _add("interest_coverage",
                 f"Interest coverage tells you how many times the company's operating profit can pay its interest bill. "
                 f"At {ic:.1f}x, the company is barely covering interest payments. "
                 f"Any further earnings decline could make debt service difficult — a significant financial risk.",
                 "negative")
        else:
            _add("interest_coverage",
                 f"Interest coverage tells you how many times the company's operating profit can pay its interest bill. "
                 f"At {ic:.1f}x, operating profit doesn't cover interest — the company is losing money before even paying its debt. "
                 f"This is a serious red flag that signals financial distress.",
                 "negative")

    td = _f("total_debt")
    if td is not None and mcap and mcap > 0:
        # total_debt from Yahoo is in raw ₹; market_cap from Screener is in ₹ Cr
        td_cr = td / 1e7 if td > 1e6 else td
        ratio = td_cr / mcap
        if ratio < 0.1:
            _add("total_debt",
                 f"Total debt is the combined short-term and long-term borrowings on the balance sheet. "
                 f"Debt is minimal relative to the company's market cap. "
                 f"The business is funded primarily through equity and internal cash flows — low financial risk.",
                 "positive")
        elif ratio > 1.0:
            _add("total_debt",
                 f"Total debt is the combined short-term and long-term borrowings on the balance sheet. "
                 f"Debt exceeds market cap — the company owes more than it is worth in the market. "
                 f"Heavy leverage increases bankruptcy risk and leaves little room for shareholders.",
                 "negative")
        elif ratio > 0.5:
            _add("total_debt",
                 f"Total debt is the combined short-term and long-term borrowings on the balance sheet. "
                 f"Debt is significant relative to market cap. "
                 f"Elevated leverage means a larger portion of profits goes to servicing debt rather than rewarding shareholders.",
                 "warning")
        else:
            _add("total_debt",
                 f"Total debt is the combined short-term and long-term borrowings on the balance sheet. "
                 f"Debt levels are moderate relative to the company's size. "
                 f"Manageable as long as earnings remain stable.",
                 "neutral")

    tc = _f("total_cash")
    if tc is not None and mcap and mcap > 0:
        # total_cash from Yahoo is in raw ₹; market_cap from Screener is in ₹ Cr
        tc_cr = tc / 1e7 if tc > 1e6 else tc
        ratio = tc_cr / mcap
        if ratio > 0.15:
            _add("total_cash",
                 f"Total cash includes cash and cash equivalents available on the balance sheet. "
                 f"The company is sitting on a significant cash pile relative to its market cap. "
                 f"This provides a strong safety cushion and optionality for acquisitions, buybacks, or weathering downturns.",
                 "positive")
        else:
            _add("total_cash",
                 f"Total cash includes cash and cash equivalents available on the balance sheet. "
                 f"Cash reserves are at normal levels. "
                 f"Adequate for operations but not a standout war chest.",
                 "neutral")

    payout_val = _f("payout_ratio")
    if payout_val is not None:
        po_pct = payout_val * 100 if abs(payout_val) < 1 else payout_val
        if 20 <= po_pct <= 60:
            _add("payout_ratio",
                 f"Payout ratio is the percentage of profits distributed as dividends to shareholders. "
                 f"At {po_pct:.1f}%, the company strikes a good balance — returning cash to shareholders while retaining enough for reinvestment. "
                 f"A sustainable level that signals confidence in future earnings.",
                 "positive")
        elif po_pct > 80:
            _add("payout_ratio",
                 f"Payout ratio is the percentage of profits distributed as dividends to shareholders. "
                 f"At {po_pct:.1f}%, the company is distributing most of its earnings. "
                 f"This may not be sustainable if profits dip — leaves very little room for reinvestment or debt reduction.",
                 "warning")
        elif po_pct > 0:
            _add("payout_ratio",
                 f"Payout ratio is the percentage of profits distributed as dividends to shareholders. "
                 f"At {po_pct:.1f}%, the company distributes a modest share of profits. "
                 f"Most earnings are being retained for growth and operations.",
                 "neutral")

    # ── Cash Flow ──────────────────────────────────────────────────
    fcf = _f("free_cash_flow")
    if fcf is not None:
        if fcf > 0:
            _add("free_cash_flow",
                 f"Free cash flow is the cash left after paying for operations and capital expenditure. "
                 f"The company generates positive FCF — it earns more cash than it spends. "
                 f"This surplus can fund dividends, buybacks, or debt repayment without relying on external funding.",
                 "positive")
        else:
            _add("free_cash_flow",
                 f"Free cash flow is the cash left after paying for operations and capital expenditure. "
                 f"FCF is negative — the company is spending more than it earns from operations. "
                 f"Acceptable during heavy investment phases, but concerning if it persists over multiple years.",
                 "negative")

    cfo = _f("cash_from_operations")
    if cfo is not None:
        if cfo > 0:
            _add("cash_from_operations",
                 f"Cash from operations shows cash generated by the company's core business — selling products and services. "
                 f"Positive operating cash flow means the business is self-sustaining at its core. "
                 f"This is the most important cash flow line — it validates that reported profits are backed by real cash.",
                 "positive")
        else:
            _add("cash_from_operations",
                 f"Cash from operations shows cash generated by the company's core business — selling products and services. "
                 f"Negative operating cash flow means the core business is consuming cash rather than generating it. "
                 f"A red flag unless the company is in an early growth phase with clear path to profitability.",
                 "negative")

    cfi = _f("cash_from_investing")
    if cfi is not None:
        if cfi < 0:
            _add("cash_from_investing",
                 f"Cash from investing reflects money spent on long-term assets — factories, equipment, acquisitions. "
                 f"Negative investing cash flow means the company is actively investing in growth. "
                 f"This is normal and healthy for growing businesses — it builds capacity for future revenue.",
                 "neutral")
        else:
            _add("cash_from_investing",
                 f"Cash from investing reflects money spent on long-term assets — factories, equipment, acquisitions. "
                 f"Positive cash flow from investing means the company is selling assets or reducing investments. "
                 f"Could signal a strategic pivot, but watch if it's a sign of shrinking operations.",
                 "warning")

    cff = _f("cash_from_financing")
    if cff is not None:
        if cff < 0:
            _add("cash_from_financing",
                 f"Cash from financing shows money flowing between the company and its investors/lenders. "
                 f"Negative financing cash flow means the company is repaying debt or returning capital through dividends/buybacks. "
                 f"A sign of financial maturity — the business generates enough to fund itself and still return cash.",
                 "neutral")
        else:
            _add("cash_from_financing",
                 f"Cash from financing shows money flowing between the company and its investors/lenders. "
                 f"Positive financing cash flow means the company is raising capital — through new debt or equity. "
                 f"Fine if it's funding growth, but concerning if it's needed just to keep the lights on.",
                 "neutral")

    # ── Technical ──────────────────────────────────────────────────
    rsi = _f("rsi_14")
    if rsi is not None:
        if rsi < 30:
            _add("rsi_14",
                 f"RSI is a momentum indicator that ranges from 0 to 100 — below 30 is considered oversold, above 70 is overbought. "
                 f"At {rsi:.0f}, the stock is in oversold territory. "
                 f"It may bounce, but oversold stocks can stay low in strong downtrends — don't catch a falling knife.",
                 "warning")
        elif rsi > 70:
            _add("rsi_14",
                 f"RSI is a momentum indicator that ranges from 0 to 100 — below 30 is considered oversold, above 70 is overbought. "
                 f"At {rsi:.0f}, the stock is in overbought territory. "
                 f"Strong momentum, but the risk of a short-term pullback increases at these levels.",
                 "warning")
        elif rsi > 55:
            _add("rsi_14",
                 f"RSI is a momentum indicator that ranges from 0 to 100 — below 30 is considered oversold, above 70 is overbought. "
                 f"At {rsi:.0f}, the stock has positive momentum without being stretched. "
                 f"A healthy range that suggests the current trend has room to continue.",
                 "positive")
        elif rsi < 40:
            _add("rsi_14",
                 f"RSI is a momentum indicator that ranges from 0 to 100 — below 30 is considered oversold, above 70 is overbought. "
                 f"At {rsi:.0f}, momentum is weak and approaching oversold levels. "
                 f"The stock is under selling pressure — could be a buying opportunity or a sign of further decline.",
                 "negative")
        else:
            _add("rsi_14",
                 f"RSI is a momentum indicator that ranges from 0 to 100 — below 30 is considered oversold, above 70 is overbought. "
                 f"At {rsi:.0f}, the stock is in neutral territory with no strong directional signal. "
                 f"Neither overbought nor oversold — price action could go either way from here.",
                 "neutral")

    # ── Ownership ──────────────────────────────────────────────────
    promo = _f("promoter_holding")
    if promo is not None:
        promo_chg = _f("promoter_holding_change")
        if promo > 60:
            base = (f"Promoter holding shows what percentage of shares the founders/insiders own — high stakes signal confidence. "
                    f"At {promo:.1f}%, promoters have strong skin in the game.")
            sent = "positive"
        elif promo < 25:
            base = (f"Promoter holding shows what percentage of shares the founders/insiders own — high stakes signal confidence. "
                    f"At {promo:.1f}%, the promoter stake is low.")
            sent = "warning"
        else:
            base = (f"Promoter holding shows what percentage of shares the founders/insiders own — high stakes signal confidence. "
                    f"At {promo:.1f}%, the promoter stake is at a moderate level.")
            sent = "neutral"
        if promo_chg is not None and abs(promo_chg) > 0.5:
            direction = "increased" if promo_chg > 0 else "decreased"
            base += f" It has {direction} by {abs(promo_chg):.1f}% recently"
            if promo_chg < -2:
                base += " — a notable reduction that warrants attention."
                sent = "warning"
            elif promo_chg > 1:
                base += " — insiders are buying, which is a confidence signal."
            else:
                base += "."
        else:
            if promo > 60:
                base += " When insiders hold this much, their interests are closely aligned with yours."
            elif promo < 25:
                base += " Lower promoter stakes sometimes correlate with less accountability."
            else:
                base += " A reasonable level — neither a red flag nor an outstanding signal."
        _add("promoter_holding", base, sent)

    fii = _f("fii_holding")
    if fii is not None:
        fii_chg = _f("fii_holding_change")
        if fii > 20:
            base = (f"FII holding reflects how much foreign institutional investors — global mutual funds, hedge funds, pension funds — own. "
                    f"At {fii:.1f}%, there's strong foreign institutional interest.")
            sent = "positive"
        elif fii < 5:
            base = (f"FII holding reflects how much foreign institutional investors — global mutual funds, hedge funds, pension funds — own. "
                    f"At {fii:.1f}%, foreign institutional interest is limited.")
            sent = "neutral"
        else:
            base = (f"FII holding reflects how much foreign institutional investors — global mutual funds, hedge funds, pension funds — own. "
                    f"At {fii:.1f}%, the stock has moderate foreign interest.")
            sent = "neutral"
        if fii_chg is not None and abs(fii_chg) > 0.5:
            direction = "increasing" if fii_chg > 0 else "decreasing"
            base += f" FII stake is {direction} ({fii_chg:+.1f}%)"
            if fii_chg > 1:
                base += " — global smart money is buying in."
                sent = "positive"
            elif fii_chg < -1:
                base += " — foreign funds are trimming positions, possibly rotating to other markets."
                sent = "warning"
            else:
                base += "."
        else:
            if fii > 20:
                base += " High FII ownership validates that the stock meets global investment standards."
            else:
                base += " FII presence alone doesn't determine quality — many good companies have low FII."
        _add("fii_holding", base, sent)

    dii = _f("dii_holding")
    if dii is not None:
        dii_chg = _f("dii_holding_change")
        if dii > 20:
            base = (f"DII holding shows ownership by domestic institutions — Indian mutual funds, LIC, pension funds. "
                    f"At {dii:.1f}%, there's strong domestic institutional support.")
            sent = "positive"
        else:
            base = (f"DII holding shows ownership by domestic institutions — Indian mutual funds, LIC, pension funds. "
                    f"At {dii:.1f}%, domestic institutional ownership is moderate.")
            sent = "neutral"
        if dii_chg is not None and abs(dii_chg) > 0.5:
            direction = "increasing" if dii_chg > 0 else "decreasing"
            base += f" DII stake is {direction} ({dii_chg:+.1f}%)."
        else:
            if dii > 20:
                base += " Strong DII presence provides buying support during market corrections."
            else:
                base += " DIIs tend to accumulate positions with a long-term view."
        _add("dii_holding", base, sent)

    # ── Score Layer Explanations ───────────────────────────────────
    sq = _f("score_quality")
    if sq is not None:
        parts = []
        if roe is not None:
            parts.append(f"ROE {roe:.1f}%")
        if roce is not None:
            parts.append(f"ROCE {roce:.1f}%")
        if de is not None:
            parts.append(f"D/E {de:.2f}")
        detail = ", ".join(parts) if parts else "financial fundamentals"
        if sq >= 70:
            _add("score_financial_health",
                 f"Financial Health score evaluates ROE, ROCE, debt levels, and cash flow quality. "
                 f"At {sq:.0f}/100, this company scores well — driven by {detail}. "
                 f"Strong fundamentals suggest the business can weather downturns and fund its own growth.",
                 "positive")
        elif sq >= 45:
            _add("score_financial_health",
                 f"Financial Health score evaluates ROE, ROCE, debt levels, and cash flow quality. "
                 f"At {sq:.0f}/100, the company has average financial health based on {detail}. "
                 f"Not a concern, but there's room for improvement in profitability or capital efficiency.",
                 "neutral")
        else:
            _add("score_financial_health",
                 f"Financial Health score evaluates ROE, ROCE, debt levels, and cash flow quality. "
                 f"At {sq:.0f}/100, financial health is a concern — key metrics are {detail}. "
                 f"Weak fundamentals increase the risk of capital erosion during market downturns.",
                 "negative")

    sv = _f("score_valuation")
    if sv is not None:
        parts = []
        if pe is not None:
            parts.append(f"PE {pe:.1f}")
        if pb is not None:
            parts.append(f"P/B {pb:.2f}")
        if peg is not None:
            parts.append(f"PEG {peg:.2f}")
        detail = ", ".join(parts) if parts else "valuation multiples"
        if sv >= 70:
            _add("score_valuation",
                 f"Valuation score evaluates whether the stock is fairly priced using PE, PEG, and P/B ratios. "
                 f"At {sv:.0f}/100, the stock appears attractively valued based on {detail}. "
                 f"You may be getting a good deal relative to the company's earnings and assets.",
                 "positive")
        elif sv >= 45:
            _add("score_valuation",
                 f"Valuation score evaluates whether the stock is fairly priced using PE, PEG, and P/B ratios. "
                 f"At {sv:.0f}/100, the stock seems fairly valued based on {detail}. "
                 f"Neither cheap nor expensive — the market has it about right at current levels.",
                 "neutral")
        else:
            _add("score_valuation",
                 f"Valuation score evaluates whether the stock is fairly priced using PE, PEG, and P/B ratios. "
                 f"At {sv:.0f}/100, the stock looks expensive — key multiples are {detail}. "
                 f"The price may already reflect most of the optimism; consider whether growth justifies the premium.",
                 "negative")

    sg = _f("score_growth")
    if sg is not None:
        parts = []
        rg = _f("revenue_growth")
        eg = _f("earnings_growth")
        csg = _f("compounded_sales_growth_3y")
        if rg is not None:
            rg_pct = rg * 100 if abs(rg) < 2 else rg
            parts.append(f"revenue growth {rg_pct:.1f}%")
        if eg is not None:
            eg_pct = eg * 100 if abs(eg) < 2 else eg
            parts.append(f"earnings growth {eg_pct:.1f}%")
        if csg is not None:
            parts.append(f"3Y sales CAGR {csg:.1f}%")
        detail = ", ".join(parts) if parts else "growth indicators"
        if sg >= 70:
            _add("score_growth",
                 f"Growth score assesses revenue expansion, earnings trajectory, and margin trends. "
                 f"At {sg:.0f}/100, the company is on a strong growth trajectory with {detail}. "
                 f"Fast-growing businesses tend to outperform over time — the key is whether this pace can be sustained.",
                 "positive")
        elif sg >= 45:
            _add("score_growth",
                 f"Growth score assesses revenue expansion, earnings trajectory, and margin trends. "
                 f"At {sg:.0f}/100, growth is moderate — {detail}. "
                 f"The company is expanding but not at a pace that would excite growth investors.",
                 "neutral")
        else:
            _add("score_growth",
                 f"Growth score assesses revenue expansion, earnings trajectory, and margin trends. "
                 f"At {sg:.0f}/100, growth is weak — {detail}. "
                 f"Stagnating or declining growth makes it harder to justify a premium valuation.",
                 "negative")

    sm = _f("score_momentum")
    if sm is not None:
        parts = []
        if rsi is not None:
            parts.append(f"RSI {rsi:.0f}")
        pct3m = _f("percent_change_3m")
        if pct3m is not None:
            parts.append(f"3M return {pct3m:+.1f}%")
        ta = row.get("trend_alignment")
        if ta:
            parts.append(f"trend {ta}")
        detail = ", ".join(parts) if parts else "technical indicators"
        if sm >= 70:
            _add("score_momentum",
                 f"Momentum score tracks recent price action using RSI, moving averages, and trend signals. "
                 f"At {sm:.0f}/100, the stock has strong upward momentum — {detail}. "
                 f"Momentum tends to persist in the short term — stocks in motion tend to stay in motion.",
                 "positive")
        elif sm >= 45:
            _add("score_momentum",
                 f"Momentum score tracks recent price action using RSI, moving averages, and trend signals. "
                 f"At {sm:.0f}/100, momentum is moderate — {detail}. "
                 f"No strong directional signal — the stock could break either way from here.",
                 "neutral")
        else:
            _add("score_momentum",
                 f"Momentum score tracks recent price action using RSI, moving averages, and trend signals. "
                 f"At {sm:.0f}/100, the stock has weak momentum — {detail}. "
                 f"Negative momentum can persist; avoid trying to catch the bottom unless fundamentals are compelling.",
                 "negative")

    si = _f("score_institutional")
    if si is not None:
        parts = []
        if fii is not None:
            parts.append(f"FII {fii:.1f}%")
        if dii is not None:
            parts.append(f"DII {dii:.1f}%")
        if promo is not None:
            parts.append(f"Promoters {promo:.1f}%")
        detail = ", ".join(parts) if parts else "ownership data"
        if si >= 70:
            _add("score_smart_money",
                 f"Smart Money score analyses ownership by FIIs, DIIs, and promoters — plus recent changes. "
                 f"At {si:.0f}/100, institutional backing is strong — {detail}. "
                 f"Professional fund managers and insiders have significant stakes, validating the investment thesis.",
                 "positive")
        elif si >= 45:
            _add("score_smart_money",
                 f"Smart Money score analyses ownership by FIIs, DIIs, and promoters — plus recent changes. "
                 f"At {si:.0f}/100, institutional interest is average — {detail}. "
                 f"The stock is on institutional radars but isn't a top conviction pick yet.",
                 "neutral")
        else:
            _add("score_smart_money",
                 f"Smart Money score analyses ownership by FIIs, DIIs, and promoters — plus recent changes. "
                 f"At {si:.0f}/100, institutional interest is low — {detail}. "
                 f"Professional investors haven't built meaningful positions — could be a discovery opportunity or a pass.",
                 "negative")

    sr = _f("score_risk")
    if sr is not None:
        parts = []
        if beta is not None:
            parts.append(f"beta {beta:.2f}")
        pledged = _f("pledged_promoter_pct")
        if pledged is not None and pledged > 0:
            parts.append(f"pledged {pledged:.1f}%")
        if de is not None:
            parts.append(f"D/E {de:.2f}")
        detail = ", ".join(parts) if parts else "risk factors"
        if sr >= 70:
            _add("score_risk_shield",
                 f"Risk Shield score measures downside protection using beta, pledged shares, debt, and earnings stability. "
                 f"At {sr:.0f}/100, the stock has a low risk profile — {detail}. "
                 f"Good downside protection — this stock should hold up better than most during market corrections.",
                 "positive")
        elif sr >= 45:
            _add("score_risk_shield",
                 f"Risk Shield score measures downside protection using beta, pledged shares, debt, and earnings stability. "
                 f"At {sr:.0f}/100, risk is moderate — {detail}. "
                 f"Standard risk levels — neither a fortress nor a fragile position.",
                 "neutral")
        else:
            _add("score_risk_shield",
                 f"Risk Shield score measures downside protection using beta, pledged shares, debt, and earnings stability. "
                 f"At {sr:.0f}/100, the risk profile is elevated — {detail}. "
                 f"This stock could see sharper drawdowns during market corrections — size your position conservatively.",
                 "negative")

    return insights


def _decorate_stock_row(row: dict, industry_stats: dict | None = None) -> dict:
    import json as _json
    item = dict(row)
    item["source_status"] = _normalize_source_status(item.get("source_status"))
    item["score_breakdown"] = _stock_breakdown_payload(item)
    # Promote action_tag / technical_score from breakdown if top-level columns are NULL
    sb = item["score_breakdown"]
    for _k in ("action_tag", "action_tag_reasoning", "technical_score", "rsi_14",
                "score_confidence", "trend_alignment", "breakout_signal"):
        if item.get(_k) is None and sb.get(_k) is not None:
            item[_k] = sb[_k]
    # Parse tags_v2 structured tags → expose as "tags" in API
    tags_v2 = item.get("tags_v2")
    if isinstance(tags_v2, str):
        try:
            tags_v2 = _json.loads(tags_v2)
        except (ValueError, TypeError):
            tags_v2 = []
    item["tags"] = tags_v2 if isinstance(tags_v2, list) else []
    item.pop("tags_v2", None)
    # Parse JSONB columns that come back as strings from asyncpg
    for jkey in ("pl_annual", "bs_annual", "cf_annual", "shareholding_quarterly", "growth_ranges"):
        val = item.get(jkey)
        if isinstance(val, str):
            try:
                item[jkey] = _json.loads(val)
            except (ValueError, TypeError):
                item[jkey] = None
    # Growth priority: Screener pre-computed → Yahoo → compute from P&L arrays
    if item.get("sales_growth_yoy") is not None:
        item["revenue_growth"] = item["sales_growth_yoy"]
    if item.get("profit_growth_yoy") is not None:
        item["earnings_growth"] = item["profit_growth_yoy"]
    # Last resort: compute from pl_annual arrays when both sources are null
    pl = item.get("pl_annual")
    if isinstance(pl, dict):
        if item.get("revenue_growth") is None:
            sales = pl.get("sales") or pl.get("revenue") or []
            if len(sales) >= 2:
                prev, curr = sales[-2], sales[-1]
                if isinstance(prev, (int, float)) and isinstance(curr, (int, float)) and prev > 0:
                    item["revenue_growth"] = round((curr / prev) - 1, 4)
        if item.get("earnings_growth") is None:
            profits = pl.get("net_profit") or []
            if len(profits) >= 2:
                prev, curr = profits[-2], profits[-1]
                if isinstance(prev, (int, float)) and isinstance(curr, (int, float)):
                    if prev > 0:
                        item["earnings_growth"] = round((curr / prev) - 1, 4)
                    elif prev < 0:
                        # Loss-to-loss or loss-to-profit: use absolute change relative to abs(prev)
                        item["earnings_growth"] = round((curr - prev) / abs(prev), 4)
    # D/E ratio fallback: compute from balance sheet when Yahoo doesn't provide it
    if item.get("debt_to_equity") is None:
        bs = item.get("bs_annual")
        if isinstance(bs, dict):
            borrowings = bs.get("borrowings") or []
            reserves = bs.get("reserves") or []
            eq_cap = bs.get("equity_capital") or bs.get("share_capital") or []
            if borrowings and reserves:
                debt = borrowings[-1] if isinstance(borrowings[-1], (int, float)) else None
                res = reserves[-1] if isinstance(reserves[-1], (int, float)) else None
                ec = eq_cap[-1] if eq_cap and isinstance(eq_cap[-1], (int, float)) else 0
                if debt is not None and res is not None:
                    equity = ec + res
                    if equity != 0:
                        item["debt_to_equity"] = round(debt / equity, 2)
    item["why_ranked"] = _stock_why_ranked(item, industry_stats)
    item["metric_insights"] = _generate_metric_insights(item, industry_stats)
    return item


def _generate_mf_metric_insights(row: dict) -> dict:
    """Generate contextual metric explanations + sentiment for a mutual fund.

    Returns {metric_key: {"explanation": str, "sentiment": str}} where
    sentiment is one of "positive", "cautionary", "negative".
    Only metrics with non-null values get an entry.
    """
    insights: dict[str, dict] = {}

    def _f(key: str) -> float | None:
        return _to_float(row.get(key))

    def _add(key: str, explanation: str, sentiment: str) -> None:
        insights[key] = {"explanation": explanation, "sentiment": sentiment}

    # ── Sharpe Ratio ──
    sharpe = _f("sharpe")
    if sharpe is not None:
        if sharpe > 1.5:
            _add("sharpe", f"Sharpe ratio of {sharpe:.2f} is excellent — the fund generates strong risk-adjusted returns. Every unit of risk taken is well-compensated with returns above the risk-free rate.", "positive")
        elif sharpe >= 0.5:
            _add("sharpe", f"Sharpe ratio of {sharpe:.2f} is average — the fund generates moderate risk-adjusted returns. Returns are positive relative to the risk taken.", "cautionary")
        else:
            _add("sharpe", f"Sharpe ratio of {sharpe:.2f} is weak — the fund's returns don't adequately compensate for the risk taken. Consider whether the volatility is justified.", "negative")

    # ── Sortino Ratio ──
    sortino = _f("sortino")
    if sortino is not None:
        if sortino > 2.0:
            _add("sortino", f"Sortino ratio of {sortino:.2f} is excellent — the fund delivers strong returns relative to downside risk. Unlike Sharpe, Sortino only penalizes harmful volatility (losses), not upside swings.", "positive")
        elif sortino >= 1.0:
            _add("sortino", f"Sortino ratio of {sortino:.2f} is acceptable — downside risk is reasonably managed. The fund doesn't frequently experience sharp declines.", "cautionary")
        else:
            _add("sortino", f"Sortino ratio of {sortino:.2f} is concerning — the fund experiences significant downside volatility. Losses are not well-controlled relative to the returns generated.", "negative")

    # ── Max Drawdown ──
    max_dd = _f("max_drawdown")
    if max_dd is not None:
        dd_abs = abs(max_dd)
        if dd_abs < 10:
            _add("max_drawdown", f"Maximum drawdown of {dd_abs:.1f}% is low — the fund has historically avoided large declines. This indicates strong downside protection and capital preservation.", "positive")
        elif dd_abs <= 25:
            _add("max_drawdown", f"Maximum drawdown of {dd_abs:.1f}% is moderate — the fund has experienced noticeable declines during market corrections, but within expected range for its category.", "cautionary")
        else:
            _add("max_drawdown", f"Maximum drawdown of {dd_abs:.1f}% is significant — the fund has experienced deep declines. Investors should be prepared for substantial temporary losses during market stress.", "negative")

    # ── Alpha ──
    alpha = _f("alpha")
    if alpha is not None:
        if alpha > 2:
            _add("alpha", f"Alpha of {alpha:.2f}% means the fund outperforms its benchmark by {alpha:.2f}% annually after adjusting for risk. Strong alpha indicates skilled fund management.", "positive")
        elif alpha >= 0:
            _add("alpha", f"Alpha of {alpha:.2f}% indicates the fund roughly matches its benchmark performance. Returns are in line with what the market risk exposure would predict.", "cautionary")
        else:
            _add("alpha", f"Alpha of {alpha:.2f}% means the fund underperforms its benchmark by {abs(alpha):.2f}% annually. The fund is destroying value relative to a passive index alternative.", "negative")

    # ── Beta ──
    beta = _f("beta")
    if beta is not None:
        if beta < 0.8:
            _add("beta", f"Beta of {beta:.2f} means the fund is defensive — it moves less than the market. When the market falls 10%, this fund historically falls only {beta * 10:.1f}%. Good for risk-averse investors.", "positive")
        elif beta <= 1.2:
            _add("beta", f"Beta of {beta:.2f} means the fund moves roughly in line with the market. It captures most of the market's upside and downside.", "cautionary")
        else:
            _add("beta", f"Beta of {beta:.2f} means the fund is aggressive — it amplifies market movements. When the market rises 10%, this fund may rise {beta * 10:.1f}%, but losses are equally amplified.", "negative")

    # ── Rolling Return Consistency ──
    rolling = _f("rolling_return_consistency")
    if rolling is not None:
        if rolling < 10:
            _add("rolling_return_consistency", f"Rolling return std dev of {rolling:.1f}% indicates highly consistent performance — returns are predictable across different time periods. This fund delivers reliable outcomes.", "positive")
        elif rolling <= 20:
            _add("rolling_return_consistency", f"Rolling return std dev of {rolling:.1f}% indicates moderate consistency — returns vary somewhat depending on when you invest, but within an acceptable range.", "cautionary")
        else:
            _add("rolling_return_consistency", f"Rolling return std dev of {rolling:.1f}% indicates unpredictable returns — performance varies significantly depending on entry timing. Investors may see very different outcomes over similar holding periods.", "negative")

    # ── Standard Deviation ──
    std_dev = _f("std_dev")
    if std_dev is not None:
        if std_dev < 10:
            _add("std_dev", f"Annualized volatility of {std_dev:.1f}% is low — the fund's NAV fluctuates minimally. Suitable for conservative investors seeking stability.", "positive")
        elif std_dev <= 20:
            _add("std_dev", f"Annualized volatility of {std_dev:.1f}% is moderate — typical for equity-oriented funds. Expect regular NAV fluctuations but within normal market ranges.", "cautionary")
        else:
            _add("std_dev", f"Annualized volatility of {std_dev:.1f}% is high — the fund experiences significant price swings. Only suitable for investors with high risk tolerance and long time horizons.", "negative")

    # ── Expense Ratio ──
    expense = _f("expense_ratio")
    if expense is not None:
        if expense < 0.5:
            _add("expense_ratio", f"Expense ratio of {expense:.2f}% is very low — you keep more of your returns. Typical of index and passive funds.", "positive")
        elif expense <= 1.5:
            _add("expense_ratio", f"Expense ratio of {expense:.2f}% is reasonable for an actively managed fund. The cost is within industry norms.", "cautionary")
        else:
            _add("expense_ratio", f"Expense ratio of {expense:.2f}% is high — a significant portion of returns goes toward fund management fees. Consider if the fund's alpha justifies this cost.", "negative")

    return insights


def _generate_mf_tags(row: dict) -> list[dict]:
    """Generate balanced tags (max 3) with sentiment for MF detail page.

    Returns list of {tag, sentiment, preset} where sentiment is positive/cautionary/negative
    and preset is the screener preset to navigate to (or null).
    """
    tags: list[tuple[int, dict]] = []  # (priority, tag_dict)

    score = _to_float(row.get("score")) or 0
    sub_pctl = _to_float(row.get("sub_category_percentile"))
    ret1y = _to_float(row.get("returns_1y"))
    ret3y = _to_float(row.get("returns_3y"))
    expense = _to_float(row.get("expense_ratio"))
    risk = (row.get("risk_level") or "").lower()
    age = _to_float(row.get("fund_age_years"))
    aum = _to_float(row.get("aum_cr"))
    max_dd = _to_float(row.get("max_drawdown"))
    sharpe = _to_float(row.get("sharpe"))
    alpha = _to_float(row.get("alpha"))

    # Positive tags
    if sub_pctl is not None and sub_pctl >= 90:
        tags.append((1, {"tag": "Top Performer", "sentiment": "positive", "preset": None}))
    if ret3y is not None and ret3y > 15:
        tags.append((2, {"tag": "Strong Returns", "sentiment": "positive", "preset": None}))
    if expense is not None and expense < 0.5:
        tags.append((2, {"tag": "Low Cost", "sentiment": "positive", "preset": None}))
    if sharpe is not None and sharpe > 1.5:
        tags.append((3, {"tag": "High Sharpe", "sentiment": "positive", "preset": None}))
    if alpha is not None and alpha > 3:
        tags.append((3, {"tag": "Strong Alpha", "sentiment": "positive", "preset": None}))
    if age is not None and age >= 10:
        tags.append((3, {"tag": "Established Fund", "sentiment": "positive", "preset": None}))

    # Cautionary tags
    if age is not None and age < 3:
        tags.append((2, {"tag": "New Fund", "sentiment": "cautionary", "preset": None}))
    if expense is not None and expense > 2.0:
        tags.append((2, {"tag": "High Cost", "sentiment": "negative", "preset": None}))

    # Negative tags
    if risk in ("high", "very high"):
        tags.append((1, {"tag": "High Risk", "sentiment": "negative", "preset": None}))
    if ret1y is not None and ret1y < -10:
        tags.append((1, {"tag": "Negative Returns", "sentiment": "negative", "preset": None}))
    if aum is not None and aum < 50:
        tags.append((2, {"tag": "Small Fund", "sentiment": "cautionary", "preset": None}))
    if max_dd is not None and abs(max_dd) > 30:
        tags.append((2, {"tag": "Deep Drawdown", "sentiment": "negative", "preset": None}))
    if score < 30 and score > 0:
        tags.append((2, {"tag": "Low Score", "sentiment": "negative", "preset": None}))

    # Sort by priority, take max 3, ensure mix of sentiments
    tags.sort(key=lambda x: x[0])
    result = [t[1] for t in tags[:3]]

    # Add sub-category tag with preset navigation
    fc = row.get("fund_classification") or row.get("sub_category") or ""
    if fc:
        preset_map = {
            "Large Cap": "large-cap", "Mid Cap": "mid-cap", "Small Cap": "small-cap",
            "Flexi Cap": "flexi-cap", "Multi Cap": "multi-cap", "ELSS": "elss",
            "Value": "value-mf", "Focused": "focused", "Index": "index",
            "Sectoral": "sectoral", "Thematic": "sectoral",
            "Liquid": "liquid", "Overnight": "overnight", "Money Market": "money-market",
            "Gilt": "gilt", "Corporate Bond": "corporate-bond",
            "Aggressive Hybrid": "aggressive-hybrid", "Arbitrage": "arbitrage",
            "FoF Domestic": "fof-domestic", "FoF Overseas": "fof-overseas",
            "Retirement": "retirement", "Children": "children",
        }
        preset = preset_map.get(fc)
        if len(result) < 3:
            result.append({"tag": fc, "sentiment": "neutral", "preset": preset})

    return result


def _decorate_mf_row(row: dict, category_stats: dict | None = None) -> dict:
    import json as _json
    item = dict(row)
    item["source_status"] = _normalize_source_status(item.get("source_status"))
    item["score_breakdown"] = _mf_breakdown_payload(item)
    item["display_name"] = _clean_mf_display_name(item.get("scheme_name", ""))
    item.pop("tags_v2", None)
    # Look up sub-category stats using fund_classification
    sub_cat = item.get("fund_classification") or item.get("sub_category") or ""
    sub_stats = None
    if category_stats:
        sub_stats = category_stats.get(sub_cat) or category_stats.get(item.get("category", ""))
    item["why_ranked"] = _mf_why_ranked(item, sub_stats)
    item["quality_badges"] = _compute_quality_badges(item)
    item["tags"] = _generate_mf_tags(item)
    item["fund_insights"] = _mf_fund_insights(item, sub_stats)
    item["metric_insights"] = _generate_mf_metric_insights(item)
    # Sanitize JSONB fields that may contain JSON null as string 'null'
    for jk in ("fund_managers", "top_holdings", "sector_allocation", "asset_allocation"):
        v = item.get(jk)
        if v is None or v == "null" or (isinstance(v, str) and v.strip() == "null"):
            item[jk] = None
        elif isinstance(v, str):
            try:
                parsed = _json.loads(v)
                # _json.loads('"null"') → "null" string, not None
                if parsed is None or parsed == "null" or (isinstance(parsed, str) and parsed.strip() == "null"):
                    item[jk] = None
                else:
                    item[jk] = parsed
            except Exception:
                item[jk] = None
    if sub_stats:
        item["category_avg_returns_1y"] = sub_stats.get("avg_ret1y")
        item["category_avg_returns_3y"] = sub_stats.get("avg_ret3y")
        item["category_avg_returns_5y"] = sub_stats.get("avg_ret5y")
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
                    percent_change_1w, percent_change_1m, percent_change_3m, percent_change_6m,
                    percent_change_1y, percent_change_3y,
                    score_breakdown, source_status, source_timestamp, ingested_at,
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
                    sector_percentile, lynch_classification, percent_change_5y,
                    technical_score, rsi_14, action_tag, action_tag_reasoning,
                    score_confidence, trend_alignment, breakout_signal,
                    tags_v2, growth_ranges
                )
                VALUES (
                    -- 1-7: market, symbol, display_name, sector, last_price, point_change, percent_change
                    $1, $2, $3, $4, $5, $6, $7,
                    -- 8-15: volume, traded_value, pe_ratio, roe, roce, debt_to_equity, price_to_book, eps
                    $8, $9, $10, $11, $12, $13, $14, $15,
                    -- 16-24: score, score_momentum, score_liquidity, score_fundamentals,
                    --        score_volatility, score_growth,
                    --        score_ownership, score_financial_health, score_analyst
                    $16, $17, NULL, NULL,
                    NULL, $18,
                    NULL, NULL, NULL,
                    -- 25-30: percent_change_1w, _1m, _3m, _6m, _1y, _3y
                    $19, $20, $21, $22, $23, $24,
                    -- 31-34: score_breakdown, source_status, source_timestamp, ingested_at
                    $25, $26, $27, NOW(),
                    -- 35-36: primary_source, secondary_source
                    $28, $29,
                    -- 37-40: high_52w, low_52w, market_cap, dividend_yield
                    $30, $31, $32, $33,
                    -- 41-45: promoter_holding, fii_holding, dii_holding, government_holding, public_holding
                    $34, $35, $36, $37, $38,
                    -- 46-49: num_shareholders, promoter_holding_change, fii_holding_change, dii_holding_change
                    $39, $40, $41, $42,
                    -- 50-55: beta, free_cash_flow, operating_cash_flow, total_cash, total_debt, total_revenue
                    $43, $44, $45, $46, $47, $48,
                    -- 56-58: gross_margins, operating_margins, profit_margins
                    $49, $50, $51,
                    -- 59-61: revenue_growth, earnings_growth, forward_pe
                    $52, $53, $54,
                    -- 62-65: analyst_target_mean, analyst_count, analyst_recommendation, analyst_recommendation_mean
                    $55, $56, $57, $58,
                    -- 66-67: industry, payout_ratio
                    $59, $60,
                    -- 68: pledged_promoter_pct
                    $61,
                    -- 69-72: sales_growth_yoy, profit_growth_yoy, opm_change, interest_coverage
                    $62, $63, $64, $65,
                    -- 73-74: compounded_sales_growth_3y, compounded_profit_growth_3y
                    $66, $67,
                    -- 75-79: total_assets, asset_growth_yoy, reserves_growth, debt_direction, cwip
                    $68, $69, $70, $71, $72,
                    -- 80-82: cash_from_operations, cash_from_investing, cash_from_financing
                    $73, $74, $75,
                    -- 83-84: num_shareholders_change_qoq, num_shareholders_change_yoy
                    $76, $77,
                    -- 85-88: synthetic_forward_pe, score_valuation,
                    --        score_earnings_quality, score_smart_money
                    $78, $79, NULL, NULL,
                    -- 89-91: pl_annual, bs_annual, cf_annual
                    $80, $81, $82,
                    -- 92: shareholding_quarterly
                    $83,
                    -- 93-95: score_quality, score_institutional, score_risk
                    $84, $85, $86,
                    -- 96-98: sector_percentile, lynch_classification, percent_change_5y
                    $87, $88, $89,
                    -- 99-102: technical_score, rsi_14, action_tag, action_tag_reasoning
                    $90, $91, $92, $93,
                    -- 103-105: score_confidence, trend_alignment, breakout_signal
                    $94, $95, $96,
                    -- 106-107: tags_v2, growth_ranges
                    COALESCE($97, '[]'::jsonb),
                    $98
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
                    percent_change_1w = EXCLUDED.percent_change_1w,
                    percent_change_1m = EXCLUDED.percent_change_1m,
                    percent_change_3m = EXCLUDED.percent_change_3m,
                    percent_change_6m = EXCLUDED.percent_change_6m,
                    percent_change_1y = EXCLUDED.percent_change_1y,
                    percent_change_3y = EXCLUDED.percent_change_3y,
                    score_breakdown = CASE WHEN EXCLUDED.score_breakdown IS NOT NULL THEN EXCLUDED.score_breakdown ELSE {STOCK_TABLE}.score_breakdown END,
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
                    percent_change_5y = EXCLUDED.percent_change_5y,
                    technical_score = COALESCE(EXCLUDED.technical_score, {STOCK_TABLE}.technical_score),
                    rsi_14 = COALESCE(EXCLUDED.rsi_14, {STOCK_TABLE}.rsi_14),
                    action_tag = COALESCE(EXCLUDED.action_tag, {STOCK_TABLE}.action_tag),
                    action_tag_reasoning = COALESCE(EXCLUDED.action_tag_reasoning, {STOCK_TABLE}.action_tag_reasoning),
                    score_confidence = COALESCE(EXCLUDED.score_confidence, {STOCK_TABLE}.score_confidence),
                    trend_alignment = COALESCE(EXCLUDED.trend_alignment, {STOCK_TABLE}.trend_alignment),
                    breakout_signal = COALESCE(EXCLUDED.breakout_signal, {STOCK_TABLE}.breakout_signal),
                    tags_v2 = CASE WHEN EXCLUDED.tags_v2 IS NOT NULL AND EXCLUDED.tags_v2 != '[]'::jsonb THEN EXCLUDED.tags_v2 ELSE {STOCK_TABLE}.tags_v2 END,
                    growth_ranges = COALESCE(EXCLUDED.growth_ranges, {STOCK_TABLE}.growth_ranges)
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
                _to_float(row.get("percent_change_1w")),                           # $19
                _to_float(row.get("percent_change_1m")),                           # $20
                _to_float(row.get("percent_change_3m")),                           # $21
                _to_float(row.get("percent_change_6m")),                           # $22
                _to_float(row.get("percent_change_1y")),                           # $23
                _to_float(row.get("percent_change_3y")),                           # $24
                _to_jsonb(row.get("score_breakdown"), _stock_breakdown_payload(row)) if row.get("score") is not None else None,  # $23
                _normalize_source_status(row.get("source_status")),                # $24
                parse_ts(row.get("source_timestamp")) or datetime.now(timezone.utc),  # $25
                row.get("primary_source"),                                         # $26
                row.get("secondary_source"),                                       # $27
                _to_float(row.get("high_52w")),                                    # $28
                _to_float(row.get("low_52w")),                                     # $29
                _to_float(row.get("market_cap")),                                  # $30
                _to_float(row.get("dividend_yield")),                              # $31
                _to_float(row.get("promoter_holding")),                            # $32
                _to_float(row.get("fii_holding")),                                 # $33
                _to_float(row.get("dii_holding")),                                 # $34
                _to_float(row.get("government_holding")),                          # $35
                _to_float(row.get("public_holding")),                              # $36
                _to_int(row.get("num_shareholders")),                              # $37
                _to_float(row.get("promoter_holding_change")),                     # $38
                _to_float(row.get("fii_holding_change")),                          # $39
                _to_float(row.get("dii_holding_change")),                          # $40
                _to_float(row.get("beta")),                                        # $41
                _to_float(row.get("free_cash_flow")),                              # $42
                _to_float(row.get("operating_cash_flow")),                         # $43
                _to_float(row.get("total_cash")),                                  # $44
                _to_float(row.get("total_debt")),                                  # $45
                _to_float(row.get("total_revenue")),                               # $46
                _to_float(row.get("gross_margins")),                               # $47
                _to_float(row.get("operating_margins")),                           # $48
                _to_float(row.get("profit_margins")),                              # $49
                _to_float(row.get("revenue_growth")),                              # $50
                _to_float(row.get("earnings_growth")),                             # $51
                _to_float(row.get("forward_pe")),                                  # $52
                _to_float(row.get("analyst_target_mean")),                         # $53
                _to_int(row.get("analyst_count")),                                 # $54
                row.get("analyst_recommendation"),                                 # $55
                _to_float(row.get("analyst_recommendation_mean")),                 # $56
                row.get("industry"),                                               # $57
                _to_float(row.get("payout_ratio")),                                # $58
                _to_float(row.get("pledged_promoter_pct")),                        # $59
                _to_float(row.get("sales_growth_yoy")),                            # $60
                _to_float(row.get("profit_growth_yoy")),                           # $61
                _to_float(row.get("opm_change")),                                  # $62
                _to_float(row.get("interest_coverage")),                           # $63
                _to_float(row.get("compounded_sales_growth_3y")),                  # $64
                _to_float(row.get("compounded_profit_growth_3y")),                 # $65
                _to_float(row.get("total_assets")),                                # $66
                _to_float(row.get("asset_growth_yoy")),                            # $67
                _to_float(row.get("reserves_growth")),                             # $68
                _to_float(row.get("debt_direction")),                              # $69
                _to_float(row.get("cwip")),                                        # $70
                _to_float(row.get("cash_from_operations")),                        # $71
                _to_float(row.get("cash_from_investing")),                         # $72
                _to_float(row.get("cash_from_financing")),                         # $73
                _to_float(row.get("num_shareholders_change_qoq")),                 # $74
                _to_float(row.get("num_shareholders_change_yoy")),                 # $75
                _to_float(row.get("synthetic_forward_pe")),                        # $76
                _to_float(row.get("score_valuation")),                             # $77
                _to_jsonb_raw(row.get("pl_annual")),                               # $78
                _to_jsonb_raw(row.get("bs_annual")),                               # $79
                _to_jsonb_raw(row.get("cf_annual")),                               # $80
                _to_jsonb_raw(row.get("shareholding_quarterly")),                   # $81
                _to_float(row.get("score_quality")),                               # $82
                _to_float(row.get("score_institutional")),                         # $83
                _to_float(row.get("score_risk")),                                  # $84
                _to_float(row.get("sector_percentile")),                           # $85
                row.get("lynch_classification"),                                   # $86
                _to_float(row.get("percent_change_5y")),                           # $87
                _to_float(row.get("technical_score")),                             # $88
                _to_float(row.get("rsi_14")),                                     # $89
                row.get("action_tag"),                                             # $90
                row.get("action_tag_reasoning"),                                   # $91
                row.get("score_confidence"),                                       # $92
                row.get("trend_alignment"),                                        # $93
                row.get("breakout_signal"),                                        # $94
                _to_jsonb(row.get("tags_v2"), []) if row.get("score") is not None else None,  # $95
                _to_jsonb_raw(row.get("growth_ranges")),                              # $96
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


async def get_score_history(symbol: str, days: int = 30) -> list[dict]:
    """Fetch score history points for a stock over the last N days."""
    from datetime import timedelta
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT score, scored_at FROM discover_stock_score_history "
            "WHERE symbol = $1 AND scored_at > NOW() - $2::interval "
            "ORDER BY scored_at ASC",
            symbol,
            timedelta(days=days),
        )
    return [{"score": float(r["score"]), "scored_at": r["scored_at"]} for r in rows]


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
                    returns_1m, returns_3m, returns_6m,
                    returns_1y, returns_3y, returns_5y, std_dev, sharpe, sortino,
                    score, score_return, score_risk, score_cost, score_consistency,
                    score_breakdown, source_status, source_timestamp, ingested_at,
                    primary_source, secondary_source, fund_age_years,
                    max_drawdown, rolling_return_consistency,
                    alpha, beta, score_alpha, score_beta,
                    score_performance, score_category_fit, sub_category_percentile, fund_classification,
                    tags_v2, fund_managers, fund_type
                )
                VALUES (
                    $1, $2, $3, $4, $5, $6, $7,
                    $8, $9, $10, $11, $12,
                    $13, $14, $15,
                    $16, $17, $18, $19, $20, $21,
                    $22, $23, $24, $25, $26,
                    $27, $28, $29, NOW(),
                    $30, $31, $32,
                    $33, $34,
                    $35, $36, $37, $38,
                    $39, $40, $41, $42,
                    $43, $44, $45
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
                    returns_1m = COALESCE(EXCLUDED.returns_1m, discover_mutual_fund_snapshots.returns_1m),
                    returns_3m = COALESCE(EXCLUDED.returns_3m, discover_mutual_fund_snapshots.returns_3m),
                    returns_6m = COALESCE(EXCLUDED.returns_6m, discover_mutual_fund_snapshots.returns_6m),
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
                    score_beta = EXCLUDED.score_beta,
                    score_performance = EXCLUDED.score_performance,
                    score_category_fit = EXCLUDED.score_category_fit,
                    sub_category_percentile = EXCLUDED.sub_category_percentile,
                    fund_classification = EXCLUDED.fund_classification,
                    tags_v2 = EXCLUDED.tags_v2,
                    fund_managers = COALESCE(EXCLUDED.fund_managers, discover_mutual_fund_snapshots.fund_managers),
                    fund_type = EXCLUDED.fund_type
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
                _to_float(row.get("returns_1m")),
                _to_float(row.get("returns_3m")),
                _to_float(row.get("returns_6m")),
                _to_float(row.get("returns_1y")),
                _to_float(row.get("returns_3y")),
                _to_float(row.get("returns_5y")),
                _to_float(row.get("std_dev")),
                _to_float(row.get("sharpe")),
                _to_float(row.get("sortino")),
                _to_float(row.get("score")),
                _to_float(row.get("score_return")),
                _to_float(row.get("score_risk")),
                _to_float(row.get("score_cost")),
                _to_float(row.get("score_consistency")),
                _to_jsonb(row.get("score_breakdown"), _mf_breakdown_payload(row)),
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
                _to_float(row.get("score_performance")),
                _to_float(row.get("score_category_fit")),
                _to_float(row.get("sub_category_percentile")),
                row.get("fund_classification"),
                _to_jsonb(row.get("tags_v2"), []),                                    # $40
                _to_jsonb(row.get("fund_managers"), None),                             # $41
                row.get("fund_type"),                                                  # $42
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


async def _get_stock_industry_stats(pool) -> dict[str, dict]:
    """Fetch per-industry averages for contextual insights and why_ranked."""
    rows = await pool.fetch(f"""
        SELECT
            COALESCE(NULLIF(industry, ''), 'Other') AS industry,
            ROUND(AVG(roe)::numeric, 1) AS avg_roe,
            ROUND(AVG(roce)::numeric, 1) AS avg_roce,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY pe_ratio)
                FILTER (WHERE pe_ratio > 0) AS median_pe,
            ROUND((AVG(pe_ratio) FILTER (WHERE pe_ratio > 0))::numeric, 1) AS avg_pe,
            ROUND((AVG(price_to_book) FILTER (WHERE price_to_book > 0))::numeric, 2) AS avg_pb,
            ROUND((AVG(debt_to_equity) FILTER (WHERE debt_to_equity >= 0))::numeric, 2) AS avg_de,
            ROUND((AVG(operating_margins * 100) FILTER (WHERE operating_margins IS NOT NULL))::numeric, 1) AS avg_opm,
            ROUND((AVG(profit_margins * 100) FILTER (WHERE profit_margins IS NOT NULL))::numeric, 1) AS avg_npm,
            ROUND((AVG(dividend_yield) FILTER (WHERE dividend_yield > 0))::numeric, 2) AS avg_dy
        FROM {STOCK_TABLE}
        WHERE market = 'IN'
        GROUP BY COALESCE(NULLIF(industry, ''), 'Other')
    """)
    out: dict[str, dict] = {}
    for r in rows:
        industry = str(r["industry"])
        out[industry] = {
            "avg_roe": float(r["avg_roe"]) if r["avg_roe"] is not None else None,
            "avg_roce": float(r["avg_roce"]) if r["avg_roce"] is not None else None,
            "median_pe": float(r["median_pe"]) if r["median_pe"] is not None else None,
            "avg_pe": float(r["avg_pe"]) if r["avg_pe"] is not None else None,
            "avg_pb": float(r["avg_pb"]) if r["avg_pb"] is not None else None,
            "avg_de": float(r["avg_de"]) if r["avg_de"] is not None else None,
            "avg_opm": float(r["avg_opm"]) if r["avg_opm"] is not None else None,
            "avg_npm": float(r["avg_npm"]) if r["avg_npm"] is not None else None,
            "avg_dy": float(r["avg_dy"]) if r["avg_dy"] is not None else None,
        }
    return out


async def _get_mf_category_stats(pool) -> dict[str, dict]:
    """Fetch per-sub-category avg returns for contextual why_ranked.

    Uses fund_classification (canonical sub-category) when available,
    falls back to raw category for backward compat.
    """
    rows = await pool.fetch(f"""
        SELECT
            COALESCE(NULLIF(fund_classification, ''), COALESCE(NULLIF(category, ''), 'Other')) AS sub_cat,
            ROUND(AVG(returns_1y)::numeric, 1) AS avg_ret1y,
            ROUND(AVG(returns_3y)::numeric, 1) AS avg_ret3y,
            ROUND(AVG(returns_5y)::numeric, 1) AS avg_ret5y,
            COUNT(*) AS fund_count
        FROM {MF_TABLE}
        WHERE returns_3y IS NOT NULL
        GROUP BY COALESCE(NULLIF(fund_classification, ''), COALESCE(NULLIF(category, ''), 'Other'))
    """)
    out: dict[str, dict] = {}
    for r in rows:
        sub_cat = str(r["sub_cat"])
        out[sub_cat] = {
            "avg_ret1y": float(r["avg_ret1y"]) if r["avg_ret1y"] is not None else None,
            "avg_ret3y": float(r["avg_ret3y"]) if r["avg_ret3y"] is not None else None,
            "avg_ret5y": float(r["avg_ret5y"]) if r["avg_ret5y"] is not None else None,
            "fund_count": int(r["fund_count"]),
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
    tag: str | None = None,
    tags_category: str | None = None,
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
        "quality": "score_quality",
        "valuation": "score_valuation",
        "growth": "score_growth",
    }
    order_col = allowed_sorts.get(str(sort_by or "").strip().lower(), "score")
    order_dir = _normalize_order(sort_order)

    conds: list[str] = ["market = 'IN'"]
    args: list[object] = []

    def _add(cond: str, value: object) -> None:
        args.append(value)
        conds.append(cond.format(idx=len(args)))

    preset_norm = str(preset or "momentum").strip().lower()
    if preset_norm == "momentum":
        conds.append("COALESCE(score_momentum, 0) >= 45")
    elif preset_norm == "value":
        conds.append("score_quality >= 55")
        conds.append("(pe_ratio IS NULL OR pe_ratio <= 35)")
    elif preset_norm == "low-volatility":
        conds.append("ABS(COALESCE(percent_change, 0)) <= 1.5")
    elif preset_norm == "quality":
        conds.append("COALESCE(roe, 0) >= 15")
        conds.append("COALESCE(roce, 0) >= 15")
        conds.append("(debt_to_equity IS NULL OR debt_to_equity <= 1.0)")
    elif preset_norm == "dividend":
        conds.append("COALESCE(dividend_yield, 0) > 0.5")
        conds.append("COALESCE(eps, 0) > 0")
    elif preset_norm == "large-cap":
        conds.append("COALESCE(market_cap, 0) >= 20000")
    elif preset_norm == "mid-cap":
        conds.append("COALESCE(market_cap, 0) >= 5000")
        conds.append("COALESCE(market_cap, 0) < 20000")
    elif preset_norm == "small-cap":
        conds.append("COALESCE(market_cap, 0) > 0")
        conds.append("COALESCE(market_cap, 0) < 5000")

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
    if tag and tag.strip():
        import json as _json_mod
        _add("tags_v2 @> ${idx}::jsonb", _json_mod.dumps([{"tag": tag.strip()}]))
    if tags_category and tags_category.strip():
        _add("EXISTS (SELECT 1 FROM jsonb_array_elements(tags_v2) t WHERE t->>'category' = ${idx})", tags_category.strip())

    where_clause = " AND ".join(conds)
    filter_args = list(args)

    args.extend([max(1, min(limit, 250)), max(0, offset)])

    pool = await get_pool()
    industry_stats = await _get_stock_industry_stats(pool)

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
            pl_annual, bs_annual, cf_annual, shareholding_quarterly, growth_ranges,
            percent_change_1w, percent_change_1m, percent_change_3m, percent_change_6m,
            percent_change_1y, percent_change_3y,
            score_breakdown, tags_v2, source_status, source_timestamp, ingested_at,
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
        ind = str(d.get("industry") or "Other")
        items.append(_decorate_stock_row(d, industry_stats.get(ind)))

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
    include_idcw: bool = False,
    min_score: float | None = None,
    min_aum_cr: float | None = None,
    max_expense_ratio: float | None = None,
    min_return_1y: float | None = None,
    min_return_3y: float | None = None,
    min_return_5y: float | None = None,
    min_fund_age: float | None = None,
    source_status: str | None = None,
    tag: str | None = None,
    tags_category: str | None = None,
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

    # Filter IDCW variants by default
    if not include_idcw:
        conds.append(
            "(COALESCE(option_type, '') NOT ILIKE '%idcw%'"
            " AND scheme_name NOT ILIKE '%idcw%'"
            " AND scheme_name NOT ILIKE '%income distribution%')"
        )

    # Exclude dead/discontinued/closed-ended funds:
    # - NAV not updated in the last 90 days (discontinued/merged/matured)
    # - Scheme names containing FMP/Fixed Maturity/Close Ended/Interval
    conds.append("nav_date >= CURRENT_DATE - INTERVAL '90 days'")
    conds.append(
        "(scheme_name NOT ILIKE '%fmp%'"
        " AND scheme_name NOT ILIKE '%fixed maturity%'"
        " AND scheme_name NOT ILIKE '%fixed horizon%'"
        " AND scheme_name NOT ILIKE '%close ended%'"
        " AND scheme_name NOT ILIKE '%closed ended%'"
        " AND scheme_name NOT ILIKE '%interval%fund%'"
        " AND scheme_name NOT ILIKE '%capital protection%'"
        " AND scheme_name NOT ILIKE '%fixed term%'"
        " AND scheme_name NOT ILIKE '%unclaimed%'"
        " AND scheme_name NOT ILIKE '%bonus%'"
        " AND scheme_name NOT ILIKE '%payout%'"
        " AND scheme_name NOT ILIKE '%- monthly%'"
        " AND scheme_name NOT ILIKE '%- quarterly%'"
        " AND scheme_name NOT ILIKE '%- half yearly%'"
        " AND scheme_name NOT ILIKE '%- annual%'"
        " AND scheme_name NOT ILIKE '%icdw%'"
        " AND scheme_name NOT ILIKE '%idwc%'"
        " AND scheme_name NOT ILIKE '%p f option%'"
        " AND scheme_name NOT ILIKE '%weekly%'"
        " AND scheme_name NOT ILIKE '%daily%'"
        " AND scheme_name NOT ILIKE '%linked insurance%')"
    )
    # Exclude "Income" category — interval/matured FMPs from AMFI
    conds.append("category != 'Income'")

    preset_norm = str(preset or "all").strip().lower()
    if preset_norm == "large-cap":
        conds.append(
            "(LOWER(COALESCE(fund_classification, sub_category, '')) LIKE '%large%cap%'"
            " AND LOWER(COALESCE(fund_classification, sub_category, '')) NOT LIKE '%mid%')"
        )
    elif preset_norm == "large-mid-cap":
        conds.append(
            "(LOWER(COALESCE(fund_classification, sub_category, '')) LIKE '%large%mid%cap%'"
            " OR LOWER(COALESCE(fund_classification, sub_category, '')) LIKE '%large%&%mid%')"
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
            "(LOWER(COALESCE(fund_classification, sub_category, '')) LIKE '%mid%cap%'"
            " AND LOWER(COALESCE(fund_classification, sub_category, '')) NOT LIKE '%large%')"
        )
    elif preset_norm == "debt":
        conds.append(
            "(LOWER(COALESCE(category, '')) ~ '(debt|bond|gilt|money market|liquid|overnight|ultra short)'"
            " OR fund_type = 'debt')"
        )
    elif preset_norm == "equity":
        conds.append(
            "(category ILIKE '%equity%' OR sub_category ILIKE '%cap%' OR sub_category ILIKE '%elss%'"
            " OR sub_category ILIKE '%value%' OR sub_category ILIKE '%focused%'"
            " OR sub_category ILIKE '%sector%' OR sub_category ILIKE '%thematic%'"
            " OR (sub_category ILIKE '%index%'"
            "     AND scheme_name NOT ILIKE '%g-sec%'"
            "     AND scheme_name NOT ILIKE '%g sec%'"
            "     AND scheme_name NOT ILIKE '%gsec%'"
            "     AND scheme_name NOT ILIKE '%gilt%'"
            "     AND scheme_name NOT ILIKE '% sdl %'"
            "     AND scheme_name NOT ILIKE '%government sec%'"
            "     AND scheme_name NOT ILIKE '%govt sec%'"
            "     AND scheme_name NOT ILIKE '%bond index%'"
            "     AND scheme_name NOT ILIKE '%aaa bond%'"
            "     AND scheme_name NOT ILIKE '%nifty aaa%'"
            "     AND scheme_name NOT ILIKE '%corporate bond%'"
            "     AND scheme_name NOT ILIKE '%composite bond%'"
            "     AND scheme_name NOT ILIKE '%crisil%'"
            "     AND scheme_name NOT ILIKE '%bharat bond%'"
            "     AND scheme_name NOT ILIKE '%target mat%'"
            "     AND scheme_name NOT ILIKE '%floating rate%'"
            "     AND COALESCE(fund_type, '') != 'debt'"
            "))"
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
        conds.append("(sub_category ILIKE '%value%' OR fund_classification ILIKE '%value%')")
        conds.append("(scheme_name NOT ILIKE '%focused%')")
    elif preset_norm == "focused":
        conds.append("(sub_category ILIKE '%focused%' OR scheme_name ILIKE '%focused%fund%')")
    elif preset_norm == "sectoral":
        conds.append("(sub_category ILIKE '%sector%' OR sub_category ILIKE '%thematic%')")
    elif preset_norm == "short-duration":
        conds.append("(sub_category ILIKE '%short%dur%' AND sub_category NOT ILIKE '%ultra%')")
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
        conds.append("(sub_category ILIKE '%balanced%hybrid%' OR (sub_category ILIKE '%balanced%' AND sub_category NOT ILIKE '%dynamic%' AND sub_category NOT ILIKE '%advantage%'))")
    elif preset_norm == "conservative-hybrid":
        conds.append("(sub_category ILIKE '%conservative%')")
    elif preset_norm == "arbitrage":
        conds.append("(sub_category ILIKE '%arbitrage%')")
    elif preset_norm == "dynamic-asset-allocation":
        conds.append("(sub_category ILIKE '%dynamic%asset%' OR fund_classification ILIKE '%dynamic%asset%')")
    elif preset_norm == "multi-asset":
        conds.append("(sub_category ILIKE '%multi%asset%' OR fund_classification ILIKE '%multi%asset%')")
    elif preset_norm == "equity-savings":
        conds.append("(sub_category ILIKE '%equity%savings%')")
    elif preset_norm == "international":
        conds.append("(sub_category ILIKE '%international%' OR fund_classification ILIKE '%international%')")
    elif preset_norm == "ultra-short":
        conds.append("(sub_category ILIKE '%ultra%short%')")
    elif preset_norm == "low-duration":
        conds.append("(sub_category ILIKE '%low%dur%')")
    elif preset_norm == "medium-duration":
        conds.append("(sub_category ILIKE '%medium%dur%' AND sub_category NOT ILIKE '%long%')")
    elif preset_norm == "floater":
        conds.append("(sub_category ILIKE '%floater%')")
    elif preset_norm == "target-maturity":
        conds.append("(sub_category ILIKE '%target%maturity%' OR fund_classification ILIKE '%target%maturity%')")
    elif preset_norm == "credit-risk":
        conds.append("(sub_category ILIKE '%credit%risk%')")
    elif preset_norm == "other":
        conds.append("(category NOT IN ('Equity', 'Debt', 'Hybrid') OR category IS NULL)")
    elif preset_norm == "fof-domestic":
        conds.append("(sub_category ILIKE '%fof%domestic%' OR fund_classification ILIKE '%fof%domestic%')")
    elif preset_norm == "fof-overseas":
        conds.append("(sub_category ILIKE '%fof%overseas%' OR fund_classification ILIKE '%fof%overseas%')")
    elif preset_norm == "gold-silver":
        conds.append("(scheme_name ILIKE '%gold%' OR scheme_name ILIKE '%silver%')")
    elif preset_norm == "retirement":
        conds.append("(sub_category ILIKE '%retirement%' OR fund_classification ILIKE '%retirement%')")
    elif preset_norm == "children":
        conds.append("(sub_category ILIKE '%child%' OR fund_classification ILIKE '%child%')")

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
        _add("expense_ratio IS NOT NULL AND expense_ratio <= ${idx}", float(max_expense_ratio))
    if min_return_1y is not None:
        _add("returns_1y IS NOT NULL AND returns_1y >= ${idx}", float(min_return_1y))
    if min_return_3y is not None:
        _add("returns_3y IS NOT NULL AND returns_3y >= ${idx}", float(min_return_3y))
    if min_return_5y is not None:
        _add("returns_5y IS NOT NULL AND returns_5y >= ${idx}", float(min_return_5y))
    if min_fund_age is not None:
        _add("fund_age_years IS NOT NULL AND fund_age_years >= ${idx}", float(min_fund_age))
    if source_status and source_status.strip().lower() != "all":
        _add("source_status = ${idx}", _normalize_source_status(source_status))
    if tag and tag.strip():
        import json as _json_mod
        _add("tags_v2 @> ${idx}::jsonb", _json_mod.dumps([{"tag": tag.strip()}]))
    if tags_category and tags_category.strip():
        _add("EXISTS (SELECT 1 FROM jsonb_array_elements(tags_v2) t WHERE t->>'category' = ${idx})", tags_category.strip())

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
            returns_1m, returns_3m, returns_6m,
            returns_1y, returns_3y, returns_5y, std_dev, sharpe, sortino,
            score, score_return, score_risk, score_cost, score_consistency,
            score_performance, score_category_fit,
            score_breakdown, tags_v2, source_status, source_timestamp, ingested_at,
            primary_source, secondary_source,
            category_rank, category_total,
            sub_category_rank, sub_category_total,
            fund_age_years,
            max_drawdown, rolling_return_consistency,
            alpha, beta, score_alpha, score_beta,
            sub_category_percentile, fund_classification,
            fund_managers,
            asset_allocation, holdings_as_of,
            fund_type
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
        cat = str(d.get("fund_classification") or d.get("category") or "Other")
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
        SELECT symbol, display_name, sector, last_price, percent_change,
               percent_change_3m,
               COALESCE(score, 0) AS score
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
        SELECT scheme_code, scheme_name, category, nav, returns_1y,
               COALESCE(score, 0) AS score
        FROM {MF_TABLE}
        WHERE (scheme_name ILIKE $1 OR scheme_code ILIKE $1)
          AND nav_date >= CURRENT_DATE - INTERVAL '90 days'
          AND category != 'Income'
          AND scheme_name NOT ILIKE '%fmp%'
          AND scheme_name NOT ILIKE '%fixed maturity%'
          AND scheme_name NOT ILIKE '%fixed horizon%'
          AND scheme_name NOT ILIKE '%close ended%'
          AND scheme_name NOT ILIKE '%closed ended%'
          AND scheme_name NOT ILIKE '%interval%fund%'
          AND scheme_name NOT ILIKE '%capital protection%'
          AND scheme_name NOT ILIKE '%fixed term%'
          AND scheme_name NOT ILIKE '%idcw%'
          AND scheme_name NOT ILIKE '%income distribution%'
          AND scheme_name NOT ILIKE '%unclaimed%'
          AND scheme_name NOT ILIKE '%bonus%'
          AND scheme_name NOT ILIKE '%payout%'
          AND scheme_name NOT ILIKE '%- monthly%'
          AND scheme_name NOT ILIKE '%- quarterly%'
          AND scheme_name NOT ILIKE '%- half yearly%'
          AND scheme_name NOT ILIKE '%- annual%'
          AND scheme_name NOT ILIKE '%icdw%'
          AND scheme_name NOT ILIKE '%idwc%'
          AND scheme_name NOT ILIKE '%p f option%'
          AND scheme_name NOT ILIKE '%weekly%'
          AND scheme_name NOT ILIKE '%daily%'
          AND scheme_name NOT ILIKE '%linked insurance%'
          AND COALESCE(option_type, '') NOT ILIKE '%idcw%'
        ORDER BY score DESC NULLS LAST, scheme_name ASC
        LIMIT $2
        """,
        q,
        max(1, min(limit, 50)),
    )

    mf_items = []
    for r in mf_rows:
        d = record_to_dict(r)
        d["display_name"] = _clean_mf_display_name(d.get("scheme_name", ""))
        mf_items.append(d)

    return {
        "stocks": [record_to_dict(r) for r in stock_rows],
        "mutual_funds": mf_items,
    }


_home_cache: dict | None = None
_home_cache_until: float = 0.0
_HOME_CACHE_TTL_SECONDS: int = 30 * 60  # 30 minutes


async def get_discover_home_data() -> dict:
    import asyncio
    import time as _time

    global _home_cache, _home_cache_until
    now = _time.monotonic()
    if _home_cache is not None and now < _home_cache_until:
        return _home_cache

    pool = await get_pool()

    _stock_cols = (
        "symbol, display_name, sector, industry, last_price, percent_change, "
        "percent_change_1w, percent_change_1m, percent_change_3m, percent_change_6m, percent_change_1y, "
        "score, score_quality, score_growth, score_valuation, "
        "high_52w, low_52w, market_cap, pe_ratio, roe, "
        "debt_to_equity, dividend_yield, action_tag"
    )

    _base_where = f"""
        FROM {STOCK_TABLE}
        WHERE market = 'IN'
          AND source_status IN ('primary', 'fallback')
    """

    def _to_items(rows) -> list[dict]:
        return [record_to_dict(r) for r in rows]

    # ── Signal & Theme section definitions ──
    # Each: (key, title, subtitle, where_clause, order, limit)
    _signal_sections = [
        (
            "undervalued_quality",
            "Undervalued Quality",
            "High-scoring stocks trading below fair value",
            f"{_base_where} AND score >= 50 AND COALESCE(score_valuation, 0) >= 50",
            "score DESC",
            8,
        ),
        (
            "momentum_building",
            "Momentum Building",
            "Gaining strength with solid fundamentals",
            f"{_base_where} AND COALESCE(technical_score, 0) >= 45 AND score >= 45 AND COALESCE(percent_change_3m, 0) > 0",
            "COALESCE(technical_score, 0) DESC",
            8,
        ),
        (
            "turnaround_candidates",
            "Turnaround Candidates",
            "Quality stocks beaten down in the correction",
            f"{_base_where} AND score >= 45 AND COALESCE(percent_change_3m, 0) < -10 AND COALESCE(score_quality, 0) >= 45",
            "score DESC",
            8,
        ),
        (
            "oversold_quality",
            "Oversold Quality",
            "Strong companies at technically oversold levels",
            f"{_base_where} AND COALESCE(rsi_14, 50) < 40 AND score >= 45",
            "score DESC",
            8,
        ),
        (
            "high_growth_smallcap",
            "High Growth Small Caps",
            "Small companies growing revenue rapidly",
            f"{_base_where} AND market_cap < 10000 AND COALESCE(compounded_sales_growth_3y, 0) > 15 AND score >= 40",
            "COALESCE(compounded_sales_growth_3y, 0) DESC",
            8,
        ),
        (
            "fii_favorites",
            "FII Favorites",
            "Stocks where foreign institutions are increasing stakes",
            f"{_base_where} AND COALESCE(fii_holding, 0) > 10 AND COALESCE(fii_holding_change, 0) > 0 AND score >= 40",
            "COALESCE(fii_holding_change, 0) DESC",
            8,
        ),
    ]

    _theme_sections = [
        (
            "consistent_growers",
            "Consistent Growers",
            "Steady revenue and profit growth over multiple years",
            f"{_base_where} AND COALESCE(compounded_sales_growth_3y, 0) > 10 AND COALESCE(compounded_profit_growth_3y, 0) > 10 AND score >= 45",
            "score DESC",
            8,
        ),
        (
            "rising_stars",
            "Rising Stars",
            "Mid & small caps breaking out with strong momentum",
            f"{_base_where} AND market_cap < 20000 AND COALESCE(percent_change_3m, 0) > 10 AND score >= 45",
            "COALESCE(percent_change_3m, 0) DESC",
            8,
        ),
        (
            "bluechip_bargains",
            "Blue Chip Bargains",
            "Large caps dipping — potential buying opportunities",
            f"{_base_where} AND market_cap > 50000 AND COALESCE(percent_change_3m, 0) < -5 AND score >= 45",
            "score DESC",
            8,
        ),
        (
            "low_volatility",
            "Low Volatility",
            "Stable stocks with low beta — defensive picks",
            f"{_base_where} AND COALESCE(beta, 1) < 0.8 AND COALESCE(beta, 1) > 0 AND score >= 45",
            "score DESC",
            8,
        ),
        (
            "debt_free_compounders",
            "Debt-Free Compounders",
            "Zero-debt companies with strong returns on equity",
            f"{_base_where} AND (debt_to_equity IS NULL OR debt_to_equity = 0) AND COALESCE(roe, 0) > 12 AND score >= 45",
            "COALESCE(roe, 0) DESC",
            8,
        ),
        (
            "dividend_aristocrats",
            "Dividend Aristocrats",
            "Reliable dividend payers with sustainable payouts",
            f"{_base_where} AND COALESCE(dividend_yield, 0) > 1.5 AND score >= 40 AND (payout_ratio IS NULL OR payout_ratio < 0.80)",
            "COALESCE(dividend_yield, 0) DESC",
            8,
        ),
        (
            "cash_rich",
            "Cash Rich Companies",
            "Generating strong operating cash flow",
            f"{_base_where} AND COALESCE(free_cash_flow, 0) > 0 AND COALESCE(cash_from_operations, 0) > 0 AND score >= 40",
            "score DESC",
            8,
        ),
        (
            "sector_leaders",
            "Sector Leaders",
            "Top-ranked stocks in their sectors",
            f"{_base_where} AND COALESCE(sector_percentile, 0) >= 85 AND score >= 45",
            "score DESC",
            8,
        ),
    ]

    all_section_defs = _signal_sections + _theme_sections

    # Run all section queries in parallel
    async def _fetch_section(key, title, subtitle, where, order, limit):
        rows = await pool.fetch(
            f"SELECT {_stock_cols} {where} ORDER BY {order} LIMIT {limit}"
        )
        items = _to_items(rows)
        if len(items) < 3:
            return None  # skip sections with too few results
        return {"key": key, "title": title, "subtitle": subtitle, "items": items}

    # Detect current market regime from latest score_breakdown
    _regime_row = await pool.fetchval(
        f"SELECT score_breakdown->>'market_regime' FROM {STOCK_TABLE}"
        " WHERE score_breakdown IS NOT NULL LIMIT 1"
    )
    _regime = _regime_row or "neutral"

    stock_section_results = await asyncio.gather(
        *[_fetch_section(*s) for s in all_section_defs]
    )
    # Build key→section map, then reorder by regime priority
    _section_map = {}
    for s in stock_section_results:
        if s is not None:
            _section_map[s["key"]] = s

    _regime_order: dict[str, list[str]] = {
        "bull": [
            "momentum_building", "rising_stars", "high_growth_smallcap",
            "consistent_growers", "fii_favorites", "sector_leaders",
            "undervalued_quality", "debt_free_compounders",
            "dividend_aristocrats", "cash_rich", "low_volatility",
            "bluechip_bargains", "turnaround_candidates", "oversold_quality",
        ],
        "correction": [
            "undervalued_quality", "turnaround_candidates", "oversold_quality",
            "bluechip_bargains", "consistent_growers", "debt_free_compounders",
            "low_volatility", "fii_favorites", "sector_leaders", "cash_rich",
            "dividend_aristocrats", "momentum_building", "rising_stars",
            "high_growth_smallcap",
        ],
        "bear": [
            "oversold_quality", "bluechip_bargains", "low_volatility",
            "debt_free_compounders", "dividend_aristocrats", "cash_rich",
            "turnaround_candidates", "undervalued_quality", "consistent_growers",
            "sector_leaders", "fii_favorites", "momentum_building",
            "rising_stars", "high_growth_smallcap",
        ],
    }
    # Aliases
    _regime_order["neutral"] = _regime_order["bull"]
    _regime_order["recovery"] = _regime_order["correction"]
    _regime_order["crisis"] = _regime_order["bear"]

    _order = _regime_order.get(_regime, _regime_order["neutral"])
    stock_sections = [_section_map[k] for k in _order if k in _section_map]

    # ── Mutual Fund sections ──
    _mf_cols = "scheme_code, scheme_name, category, sub_category, score, returns_1y, returns_3y, rolling_return_consistency, fund_classification, fund_type, risk_level, expense_ratio"

    async def _fetch_mf_section(key, title, subtitle, where_extra, order, limit):
        rows = await pool.fetch(
            f"""
            SELECT * FROM (
                SELECT {_mf_cols},
                       ROW_NUMBER() OVER (PARTITION BY COALESCE(sub_category, category, 'Other') ORDER BY score DESC) AS rn
                FROM {MF_TABLE}
                WHERE plan_type = 'direct'
                  AND score IS NOT NULL
                  AND (returns_1y IS NOT NULL OR returns_3y IS NOT NULL)
                  AND nav_date >= CURRENT_DATE - INTERVAL '90 days'
                  {where_extra}
            ) sub WHERE rn <= 2
            ORDER BY {order} LIMIT {limit}
            """
        )
        items = []
        for r in rows:
            d = record_to_dict(r)
            d["quality_badges"] = _compute_quality_badges(d)
            d["display_name"] = _clean_mf_display_name(d.get("scheme_name", ""))
            items.append(d)
        if len(items) < 3:
            return None
        return {"key": key, "title": title, "subtitle": subtitle, "items": items}

    _mf_alive = """AND category != 'Income'
               AND nav_date >= CURRENT_DATE - INTERVAL '90 days'
               AND scheme_name NOT ILIKE '%%fmp%%'
               AND scheme_name NOT ILIKE '%%fixed maturity%%'
               AND scheme_name NOT ILIKE '%%fixed horizon%%'
               AND scheme_name NOT ILIKE '%%close ended%%'
               AND scheme_name NOT ILIKE '%%closed ended%%'
               AND scheme_name NOT ILIKE '%%capital protection%%'
               AND scheme_name NOT ILIKE '%%fixed term%%'"""

    mf_section_results = await asyncio.gather(
        _fetch_mf_section(
            "top_equity", "Top Equity Funds", "Best performing equity mutual funds",
            f"""AND score >= 50
               AND LOWER(COALESCE(category, '')) NOT LIKE '%%debt%%'
               AND LOWER(COALESCE(category, '')) NOT LIKE '%%liquid%%'
               AND LOWER(COALESCE(category, '')) NOT LIKE '%%gilt%%'
               AND LOWER(COALESCE(category, '')) NOT LIKE '%%money market%%'
               AND LOWER(COALESCE(category, '')) NOT LIKE '%%hybrid%%'
               AND LOWER(COALESCE(category, '')) NOT LIKE '%%other%%'
               AND LOWER(COALESCE(category, '')) NOT LIKE '%%income%%'
               AND COALESCE(fund_type, '') != 'debt'
               {_mf_alive}""",
            "score DESC", 8,
        ),
        _fetch_mf_section(
            "top_debt", "Top Debt Funds", "Stable returns from fixed income",
            f"""AND score >= 40
               AND (
                   LOWER(COALESCE(category, '')) LIKE '%%debt%%'
                   OR LOWER(COALESCE(category, '')) LIKE '%%liquid%%'
                   OR LOWER(COALESCE(category, '')) LIKE '%%gilt%%'
                   OR LOWER(COALESCE(category, '')) LIKE '%%money market%%'
                   OR LOWER(COALESCE(sub_category, '')) LIKE '%%corporate bond%%'
                   OR LOWER(COALESCE(sub_category, '')) LIKE '%%overnight%%'
                   OR fund_type = 'debt'
               )
               {_mf_alive}""",
            "score DESC", 8,
        ),
        _fetch_mf_section(
            "top_index", "Top Index Funds", "Best passive funds with low costs",
            f"""AND score >= 50
               AND fund_classification = 'Index'
               AND COALESCE(fund_type, 'equity') = 'equity'
               {_mf_alive}""",
            "score DESC", 8,
        ),
        _fetch_mf_section(
            "tax_saver", "Tax Saver (ELSS)", "Save tax under Section 80C with 3-year lock-in",
            f"""AND score >= 40
               AND (sub_category ILIKE '%%elss%%' OR sub_category ILIKE '%%tax%%sav%%')
               {_mf_alive}""",
            "score DESC", 8,
        ),
        _fetch_mf_section(
            "small_mid_cap", "Small & Mid Cap Picks", "High-growth potential in smaller companies",
            f"""AND score >= 45
               AND fund_classification IN ('Small Cap', 'Mid Cap')
               {_mf_alive}""",
            "score DESC", 8,
        ),
        _fetch_mf_section(
            "consistent_performers", "Consistent Performers",
            "Equity funds with steady returns across market cycles",
            f"""AND score >= 55 AND rolling_return_consistency IS NOT NULL
               AND COALESCE(fund_type, 'equity') = 'equity'
               AND LOWER(COALESCE(category, '')) NOT LIKE '%%other%%'
               {_mf_alive}""",
            "rolling_return_consistency ASC", 8,
        ),
        _fetch_mf_section(
            "sector_thematic", "Trending Sectors", "Top performing sectoral and thematic funds",
            f"""AND score >= 50
               AND (sub_category ILIKE '%%sector%%' OR sub_category ILIKE '%%thematic%%')
               AND returns_1y IS NOT NULL
               {_mf_alive}""",
            "returns_1y DESC", 8,
        ),
    )
    mf_sections = [s for s in mf_section_results if s is not None]

    quick_categories = [
        # Stocks
        {"name": "All", "segment": "stocks", "preset": "all"},
        {"name": "Large Cap", "segment": "stocks", "preset": "large-cap"},
        {"name": "Mid Cap", "segment": "stocks", "preset": "mid-cap"},
        {"name": "Small Cap", "segment": "stocks", "preset": "small-cap"},
        {"name": "Quality", "segment": "stocks", "preset": "quality"},
        {"name": "Value", "segment": "stocks", "preset": "value"},
        {"name": "Momentum", "segment": "stocks", "preset": "momentum"},
        {"name": "Low Volatility", "segment": "stocks", "preset": "low-volatility"},
        {"name": "Dividend", "segment": "stocks", "preset": "dividend"},
        # Mutual Funds
        {"name": "All", "segment": "mutual_funds", "preset": "all"},
        {"name": "Equity", "segment": "mutual_funds", "preset": "equity"},
        {"name": "Debt", "segment": "mutual_funds", "preset": "debt"},
        {"name": "Hybrid", "segment": "mutual_funds", "preset": "hybrid"},
        {"name": "Large Cap", "segment": "mutual_funds", "preset": "large-cap"},
        {"name": "Mid Cap", "segment": "mutual_funds", "preset": "mid-cap"},
        {"name": "Small Cap", "segment": "mutual_funds", "preset": "small-cap"},
        {"name": "Flexi Cap", "segment": "mutual_funds", "preset": "flexi-cap"},
        {"name": "Multi Cap", "segment": "mutual_funds", "preset": "multi-cap"},
        {"name": "ELSS", "segment": "mutual_funds", "preset": "elss"},
        {"name": "Index", "segment": "mutual_funds", "preset": "index"},
        {"name": "Sectoral", "segment": "mutual_funds", "preset": "sectoral"},
        {"name": "Value", "segment": "mutual_funds", "preset": "value-mf"},
    ]

    result = {
        "stock_sections": stock_sections,
        "mf_sections": mf_sections,
        "quick_categories": quick_categories,
    }

    _home_cache_until = _time.monotonic() + _HOME_CACHE_TTL_SECONDS
    _home_cache = result
    return result


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
    # Fetch industry stats for decoration
    ind = str(d.get("industry") or "Other")
    industry_stats_rows = await pool.fetch(
        f"""
        SELECT
            AVG(pe_ratio) FILTER (WHERE pe_ratio > 0) AS avg_pe,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY pe_ratio)
                FILTER (WHERE pe_ratio > 0) AS median_pe,
            AVG(roe) AS avg_roe,
            AVG(roce) AS avg_roce,
            AVG(price_to_book) FILTER (WHERE price_to_book > 0) AS avg_pb,
            AVG(operating_margins * 100) FILTER (WHERE operating_margins IS NOT NULL) AS avg_opm,
            AVG(profit_margins * 100) FILTER (WHERE profit_margins IS NOT NULL) AS avg_npm,
            AVG(dividend_yield) FILTER (WHERE dividend_yield > 0) AS avg_dy,
            AVG(debt_to_equity) FILTER (WHERE debt_to_equity >= 0) AS avg_de,
            AVG(beta) AS avg_beta
        FROM {STOCK_TABLE}
        WHERE industry = $1 AND source_status IN ('primary', 'fallback')
        """,
        ind,
    )
    stats = record_to_dict(industry_stats_rows[0]) if industry_stats_rows else {}
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


import time as _time

# 60s is short enough to expose a fresh 30-min tick soon after it lands,
# long enough to absorb app-open bursts without hammering Yahoo.
_INTRADAY_CACHE_TTL = 60
_INTRADAY_CACHE: dict[str, tuple[float, list[dict]]] = {}


async def _fetch_yahoo_intraday(symbol: str) -> list[dict]:
    """Fetch 30-min ticks for the last 2 sessions from Upstox.

    Historically this hit Yahoo v8/chart directly, but that endpoint
    is hard-blocked (HTTP 429) on datacenter IPs, so every 1D chart
    came back blank. Upstox's public `/v2/historical-candle` API is
    designed for server-side use, returns clean OHLCV candles, and
    works unauthenticated for market data.

    Returns points or []. Kept under the same function name so the
    existing cache wrapper in get_stock_intraday_history doesn't need
    to change.
    """
    import httpx

    # Load the Upstox instrument master on demand and memoise it.
    instr_map = await _get_upstox_instrument_map()
    key = instr_map.get(symbol.upper().strip())
    if not key:
        return []
    from datetime import timedelta
    today = datetime.now(timezone.utc).date()
    from_date = (today - timedelta(days=3)).isoformat()
    to_date = today.isoformat()
    import urllib.parse as _up
    url = (
        "https://api.upstox.com/v2/historical-candle/"
        f"{_up.quote(key, safe='')}"
        f"/30minute/{to_date}/{from_date}"
    )
    try:
        async with httpx.AsyncClient(timeout=6) as client:
            resp = await client.get(
                url, headers={"Accept": "application/json"},
            )
            resp.raise_for_status()
            payload = resp.json()
    except Exception as exc:
        logger.debug("upstox intraday fetch failed for %s: %s", symbol, exc)
        return []

    if not isinstance(payload, dict) or payload.get("status") != "success":
        return []
    candles = (payload.get("data") or {}).get("candles") or []
    from zoneinfo import ZoneInfo
    _IST = ZoneInfo("Asia/Kolkata")
    out: list[dict] = []
    for row in candles:
        if not isinstance(row, list) or len(row) < 5:
            continue
        try:
            ts_utc = datetime.fromisoformat(row[0]).astimezone(timezone.utc)
            close = float(row[4])
        except (TypeError, ValueError):
            continue
        out.append({
            "ts": ts_utc,
            "price": close,
            "volume": int(row[5]) if len(row) > 5 and row[5] is not None else None,
            "percent_change": None,
        })
    out.sort(key=lambda p: p["ts"])
    # "1D" must mean exactly ONE trading session, not 2-3 days of
    # candles — the app renders intraday on an HH:MM axis and the
    # user rightly reported that a chart spanning Apr 8 → Apr 10
    # isn't "today's chart". Keep only candles whose IST trade-date
    # matches the most recent one in the response. This gives us
    # Friday's session on a Saturday, Thursday's on a Friday pre-
    # market, etc.
    if out:
        latest_date = max(
            p["ts"].astimezone(_IST).date() for p in out
        )
        out = [
            p for p in out
            if p["ts"].astimezone(_IST).date() == latest_date
        ]
    return out


# Memoised Upstox instrument master for the live fallback path.
_UPSTOX_INSTR_CACHE: dict[str, str] = {}
_UPSTOX_INSTR_CACHE_TS: float = 0.0


async def _get_upstox_instrument_map() -> dict[str, str]:
    """Return {NSE tradingsymbol → 'NSE_EQ|ISIN'}. Cached for 24h."""
    global _UPSTOX_INSTR_CACHE, _UPSTOX_INSTR_CACHE_TS
    import time as _time
    now = _time.monotonic()
    if _UPSTOX_INSTR_CACHE and now - _UPSTOX_INSTR_CACHE_TS < 24 * 3600:
        return _UPSTOX_INSTR_CACHE
    import gzip
    import io
    import httpx
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(
                "https://assets.upstox.com/market-quote/instruments/"
                "exchange/complete.csv.gz",
            )
            resp.raise_for_status()
        raw = gzip.GzipFile(fileobj=io.BytesIO(resp.content)).read().decode(
            "utf-8", errors="replace",
        )
    except Exception as exc:
        logger.debug("upstox instrument master load failed: %s", exc)
        return _UPSTOX_INSTR_CACHE
    mapping: dict[str, str] = {}
    for line in raw.splitlines()[1:]:
        if not line.startswith('"NSE_EQ'):
            continue
        parts = line.split(",", 4)
        if len(parts) < 4:
            continue
        key = parts[0].strip('"')
        ts = parts[2].strip('"').upper()
        if ts and key.startswith("NSE_EQ|"):
            mapping.setdefault(ts, key)
    if mapping:
        _UPSTOX_INSTR_CACHE = mapping
        _UPSTOX_INSTR_CACHE_TS = now
    return mapping


async def get_stock_intraday_history(*, symbol: str) -> list[dict]:
    """Return the most-recent trading session's intraday ticks.

    "1D" in the UI means ONE trading session — not N calendar days —
    so on a Saturday or holiday we return Friday's close, on a pre-
    market Monday we return Friday's candles, and during a live
    session we return today's ticks. The app renders the x-axis as
    HH:MM, so returning a multi-day span (what the previous code
    did) produced a garbled chart spanning Apr 8 → Apr 10.

    Strategy:
    1. Stored ``discover_stock_intraday`` table for the last 3 days.
       Group by IST trade-date, keep only the rows whose date is the
       most recent one in the slice.
    2. If the table is empty / thin, fall back to the Upstox live
       fetch (which also self-filters to the latest session).
    3. 60-second process-local cache keyed by symbol to absorb bursts.
    """
    from zoneinfo import ZoneInfo
    _IST = ZoneInfo("Asia/Kolkata")

    key = symbol.upper().strip()
    now = _time.time()
    cached = _INTRADAY_CACHE.get(key)
    if cached and now - cached[0] < _INTRADAY_CACHE_TTL:
        return cached[1]

    pool = await get_pool()
    # Level 1: the last 3 days of stored ticks. We'll pick the most
    # recent day below so weekends / holidays still surface Friday.
    rows = await pool.fetch(
        """
        SELECT ts, price, volume, percent_change
        FROM discover_stock_intraday
        WHERE symbol = $1
          AND ts >= NOW() - INTERVAL '4 days'
        ORDER BY ts ASC
        """,
        key,
    )
    points = [record_to_dict(r) for r in rows]

    # Level 2: live Upstox fetch when the stored table has almost
    # nothing. Upstox path already filters to the latest session.
    if len(points) < 3:
        upstox_points = await _fetch_yahoo_intraday(key)
        if upstox_points:
            points = upstox_points

    # Filter the combined set to only the most recent IST session
    # so "1D" is truly one day's worth of ticks.
    if points:
        latest_date = max(
            p["ts"].astimezone(_IST).date() for p in points
        )
        points = [
            p for p in points
            if p["ts"].astimezone(_IST).date() == latest_date
        ]

    _INTRADAY_CACHE[key] = (now, points)
    # Bound the cache size to prevent unbounded growth.
    if len(_INTRADAY_CACHE) > 2000:
        for k in list(_INTRADAY_CACHE.keys())[:500]:
            _INTRADAY_CACHE.pop(k, None)
    return points


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
        -- 1M return: first and last close over 30 days
        range_1m AS (
            SELECT symbol,
                   FIRST_VALUE(close) OVER (PARTITION BY symbol ORDER BY trade_date ASC
                       ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_close_1m,
                   LAST_VALUE(close) OVER (PARTITION BY symbol ORDER BY trade_date ASC
                       ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_close_1m,
                   COUNT(*) OVER (PARTITION BY symbol) AS cnt_1m
            FROM discover_stock_price_history
            WHERE trade_date >= CURRENT_DATE - INTERVAL '30 days'
        ),
        stats_1m AS (
            SELECT symbol,
                   MIN(first_close_1m) AS first_close_1m,
                   MAX(last_close_1m) AS last_close_1m
            FROM range_1m
            GROUP BY symbol
            HAVING MAX(cnt_1m) >= 10
        ),
        -- 6M return: first and last close over 180 days
        range_6m AS (
            SELECT symbol,
                   FIRST_VALUE(close) OVER (PARTITION BY symbol ORDER BY trade_date ASC
                       ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_close_6m,
                   LAST_VALUE(close) OVER (PARTITION BY symbol ORDER BY trade_date ASC
                       ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_close_6m,
                   COUNT(*) OVER (PARTITION BY symbol) AS cnt_6m
            FROM discover_stock_price_history
            WHERE trade_date >= CURRENT_DATE - INTERVAL '180 days'
        ),
        stats_6m AS (
            SELECT symbol,
                   MIN(first_close_6m) AS first_close_6m,
                   MAX(last_close_6m) AS last_close_6m
            FROM range_6m
            GROUP BY symbol
            HAVING MAX(cnt_6m) >= 60
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
               m1.first_close_1m, m1.last_close_1m,
               m6.first_close_6m, m6.last_close_6m,
               y.first_close_1y, y.last_close_1y,
               y3.first_close_3y, y3.last_close_3y,
               y5.first_close_5y, y5.last_close_5y
        FROM stats_3m s
        LEFT JOIN short_term st ON s.symbol = st.symbol
        LEFT JOIN stats_1m m1 ON s.symbol = m1.symbol
        LEFT JOIN stats_6m m6 ON s.symbol = m6.symbol
        LEFT JOIN stats_1y y ON s.symbol = y.symbol
        LEFT JOIN stats_3y y3 ON s.symbol = y3.symbol
        LEFT JOIN stats_5y y5 ON s.symbol = y5.symbol
        """,
        timeout=120,
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

        # 1M and 6M returns
        pct_1m = None
        fc_1m = float(r["first_close_1m"]) if r.get("first_close_1m") else None
        lc_1m = float(r["last_close_1m"]) if r.get("last_close_1m") else None
        if fc_1m and fc_1m > 0 and lc_1m:
            pct_1m = round(((lc_1m - fc_1m) / fc_1m) * 100, 2)

        pct_6m = None
        fc_6m = float(r["first_close_6m"]) if r.get("first_close_6m") else None
        lc_6m = float(r["last_close_6m"]) if r.get("last_close_6m") else None
        if fc_6m and fc_6m > 0 and lc_6m:
            pct_6m = round(((lc_6m - fc_6m) / fc_6m) * 100, 2)

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
            "pct_change_1w": pct_1w,
            "pct_change_1m": pct_1m,
            "pct_change_3m": pct_3m,
            "pct_change_6m": pct_6m,
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
    """Get peer stocks in the same industry (fallback to sector), sorted by score descending."""
    pool = await get_pool()
    industry_stats = await _get_stock_industry_stats(pool)

    target = await pool.fetchrow(
        f"SELECT sector, industry FROM {STOCK_TABLE} WHERE symbol = $1",
        symbol,
    )
    if target is None:
        return []

    industry = target["industry"]
    sector = target["sector"]

    _PEER_COLS = f"""
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
            pl_annual, bs_annual, cf_annual, shareholding_quarterly, growth_ranges,
            percent_change_1w, percent_change_1m, percent_change_3m, percent_change_6m,
            percent_change_1y, percent_change_3y,
            score_breakdown, tags_v2, source_status, source_timestamp, ingested_at,
            primary_source, secondary_source,
            high_52w, low_52w, market_cap, dividend_yield,
            promoter_holding, fii_holding, dii_holding, government_holding, public_holding,
            num_shareholders, promoter_holding_change, fii_holding_change, dii_holding_change,
            beta, free_cash_flow, operating_cash_flow, total_cash, total_debt, total_revenue,
            gross_margins, operating_margins, profit_margins,
            revenue_growth, earnings_growth, forward_pe,
            analyst_target_mean, analyst_count, analyst_recommendation, analyst_recommendation_mean,
            payout_ratio
    """

    # Try industry first, fall back to sector if too few peers
    rows = []
    if industry:
        rows = await pool.fetch(
            f"""
            SELECT {_PEER_COLS}
            FROM {STOCK_TABLE}
            WHERE industry = $1 AND symbol != $2
            ORDER BY score DESC NULLS LAST
            LIMIT $3
            """,
            industry,
            symbol,
            limit,
        )

    if len(rows) < 3 and sector:
        # Fall back to sector peers
        rows = await pool.fetch(
            f"""
            SELECT {_PEER_COLS}
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
        ind = str(d.get("industry") or "Other")
        items.append(_decorate_stock_row(d, industry_stats.get(ind)))
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
            returns_1m, returns_3m, returns_6m,
            returns_1y, returns_3y, returns_5y, std_dev, sharpe, sortino,
            score, score_return, score_risk, score_cost, score_consistency,
            score_performance, score_category_fit,
            score_breakdown, tags_v2, source_status, source_timestamp, ingested_at,
            primary_source, secondary_source,
            category_rank, category_total,
            sub_category_rank, sub_category_total,
            fund_age_years,
            max_drawdown, rolling_return_consistency,
            alpha, beta, score_alpha, score_beta,
            sub_category_percentile, fund_classification
        FROM {MF_TABLE}
        WHERE COALESCE(NULLIF(sub_category, ''), category) = $1
          AND scheme_code != $2
          AND LOWER(COALESCE(plan_type, 'direct')) = 'direct'
          AND category != 'Income'
          AND nav_date >= CURRENT_DATE - INTERVAL '90 days'
          AND score IS NOT NULL
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
                score_breakdown, tags_v2, source_status, source_timestamp, ingested_at,
                primary_source, secondary_source,
                category_rank, category_total,
                sub_category_rank, sub_category_total,
                fund_age_years
            FROM {MF_TABLE}
            WHERE category = $1
              AND scheme_code != $2
              AND LOWER(COALESCE(plan_type, 'direct')) = 'direct'
              AND category != 'Income'
              AND nav_date >= CURRENT_DATE - INTERVAL '90 days'
              AND score IS NOT NULL
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


# ---------------------------------------------------------------------------
# Batch sparklines
# ---------------------------------------------------------------------------

def _downsample(points: list[dict], max_points: int) -> list[dict]:
    """Downsample a list of points to at most max_points, keeping first and last."""
    n = len(points)
    if n <= max_points:
        return points
    # Always keep first and last; evenly sample the rest
    if max_points <= 2:
        return [points[0], points[-1]]
    step = (n - 1) / (max_points - 1)
    indices = {0, n - 1}
    for i in range(1, max_points - 1):
        indices.add(round(i * step))
    return [points[i] for i in sorted(indices)]


async def get_stock_sparklines(*, symbols: list[str], days: int = 7, max_points: int = 30) -> dict[str, list[dict]]:
    """Fetch price history for multiple symbols in a single query."""
    if not symbols:
        return {}
    pool = await get_pool()
    rows = await pool.fetch(
        """
        SELECT symbol, trade_date, close
        FROM discover_stock_price_history
        WHERE symbol = ANY($1::text[])
          AND trade_date >= CURRENT_DATE - make_interval(days => $2)
        ORDER BY symbol, trade_date ASC
        """,
        symbols,
        days,
    )
    result: dict[str, list[dict]] = {s: [] for s in symbols}
    for r in rows:
        sym = r["symbol"]
        if sym in result:
            result[sym].append({"date": str(r["trade_date"]), "value": float(r["close"])})
    # Downsample each series
    for key in result:
        result[key] = _downsample(result[key], max_points)
    return result


async def get_mf_sparklines(*, scheme_codes: list[str], days: int = 7, max_points: int = 30) -> dict[str, list[dict]]:
    """Fetch NAV history for multiple scheme codes in a single query."""
    if not scheme_codes:
        return {}
    pool = await get_pool()
    rows = await pool.fetch(
        """
        SELECT scheme_code, nav_date, nav
        FROM discover_mf_nav_history
        WHERE scheme_code = ANY($1::text[])
          AND nav_date >= CURRENT_DATE - make_interval(days => $2)
        ORDER BY scheme_code, nav_date ASC
        """,
        scheme_codes,
        days,
    )
    result: dict[str, list[dict]] = {s: [] for s in scheme_codes}
    for r in rows:
        sc = r["scheme_code"]
        if sc in result:
            result[sc].append({"date": str(r["nav_date"]), "value": float(r["nav"])})
    # Downsample each series
    for key in result:
        result[key] = _downsample(result[key], max_points)
    return result


# ---------------------------------------------------------------------------
# Phase 2: Stock Story
# ---------------------------------------------------------------------------

async def get_stock_story(*, symbol: str) -> dict | None:
    """Build a narrative story for a stock: verdict, tags, score changes.

    Loads the full signal set that `_build_stock_prompt` consumes so
    Artha's narrative is computed over all 10 buckets (action,
    valuation, quality, growth, balance sheet, ownership, technicals,
    score layers, growth history, tags/red flags).
    """
    pool = await get_pool()
    row = await pool.fetchrow(
        f"""
        SELECT
            -- Identity
            symbol, display_name, sector,
            -- Action + tags
            action_tag, action_tag_reasoning,
            score_confidence, lynch_classification,
            tags_v2,
            -- Score layers
            score, score_quality, score_valuation, score_growth,
            score_momentum, score_institutional, score_risk,
            score_smart_money, score_earnings_quality,
            score_financial_health, score_liquidity,
            score_breakdown,
            -- Valuation
            pe_ratio, price_to_book, last_price, analyst_target_mean,
            dividend_yield,
            -- Quality
            roe, roce, gross_margins, operating_margins, profit_margins,
            -- Growth
            revenue_growth, earnings_growth,
            compounded_sales_growth_3y, compounded_profit_growth_3y,
            sales_growth_yoy, profit_growth_yoy,
            growth_ranges,
            -- Balance sheet + cash
            debt_to_equity, debt_direction, total_debt, total_cash,
            free_cash_flow, payout_ratio, market_cap,
            -- Ownership
            promoter_holding, promoter_holding_change,
            pledged_promoter_pct,
            fii_holding, fii_holding_change,
            dii_holding, dii_holding_change,
            -- Technicals
            trend_alignment, breakout_signal, beta,
            high_52w, low_52w, risk_reward_tag,
            -- Narrative cache
            ai_narrative, ai_narrative_at
        FROM {STOCK_TABLE}
        WHERE symbol = $1
        """,
        symbol,
    )
    if row is None:
        return None

    import json as _json
    d = record_to_dict(row)
    sb = d.get("score_breakdown") or {}
    if isinstance(sb, str):
        try:
            sb = _json.loads(sb)
        except (ValueError, TypeError):
            sb = {}

    tags_v2 = d.get("tags_v2")
    if isinstance(tags_v2, str):
        try:
            tags_v2 = _json.loads(tags_v2)
        except (ValueError, TypeError):
            tags_v2 = []
    if not isinstance(tags_v2, list):
        tags_v2 = []

    # Compute 7-day overall score delta from history
    old_score_row = await pool.fetchrow(
        """
        SELECT score, scored_at
        FROM discover_stock_score_history
        WHERE symbol = $1 AND scored_at < NOW() - interval '6 days'
        ORDER BY scored_at DESC
        LIMIT 1
        """,
        symbol,
    )

    score_changes: list[dict] = []
    # Show current layer scores
    layers = [
        ("quality", "score_quality"),
        ("valuation", "score_valuation"),
        ("growth", "score_growth"),
        ("momentum", "score_momentum"),
        ("institutional", "score_institutional"),
        ("risk", "score_risk"),
    ]
    for layer_name, col in layers:
        new_val = _to_float(d.get(col))
        if new_val is not None:
            score_changes.append({
                "layer": layer_name,
                "old_value": None,
                "new_value": new_val,
                "direction": "unchanged",
            })

    # If we have a historical overall score, add overall change
    if old_score_row is not None:
        old_overall = _to_float(old_score_row["score"])
        new_overall = _to_float(d.get("score"))
        if old_overall is not None and new_overall is not None:
            diff = new_overall - old_overall
            direction = "up" if diff > 0.5 else ("down" if diff < -0.5 else "unchanged")
            score_changes.insert(0, {
                "layer": "overall",
                "old_value": old_overall,
                "new_value": new_overall,
                "direction": direction,
            })

    # Build verdict from score tier
    score = _to_float(d.get("score"))
    if score is not None:
        if score >= 75:
            verdict = "Strong fundamentals and momentum align well"
        elif score >= 60:
            verdict = "Above-average on most dimensions"
        elif score >= 45:
            verdict = "Mixed signals across score layers"
        elif score >= 30:
            verdict = "Below average with some concerns"
        else:
            verdict = "Significant fundamental or technical concerns"
    else:
        verdict = None

    # --- AI narrative: DB-cached + fire-and-forget refresh ---
    #
    # Previously this blocked every /story request on a 5-15s LLM
    # call, so the top verdict card on the stock detail screen took
    # forever to load. Now we serve whatever's cached in the
    # `ai_narrative` column (same row we just read) and, if it's
    # stale or missing, schedule a background regen that writes the
    # column — the NEXT screen open will have a fresh narrative with
    # zero LLM latency on the critical path.
    ai_narrative = d.get("ai_narrative")
    ai_narrative_at = d.get("ai_narrative_at")
    _STALE_AFTER_HOURS = 24
    _stale = True
    if ai_narrative and ai_narrative_at:
        try:
            age_hours = (
                datetime.now(timezone.utc) - ai_narrative_at
            ).total_seconds() / 3600
            _stale = age_hours > _STALE_AFTER_HOURS
        except Exception:
            _stale = True

    if _stale:
        # Parse growth_ranges for the AI prompt.
        gr = d.get("growth_ranges")
        if isinstance(gr, str):
            try:
                gr = _json.loads(gr)
            except (ValueError, TypeError):
                gr = None
        ai_payload = {**d, "tags_v2": tags_v2, "growth_ranges": gr}

        async def _regen_and_persist(symbol_: str, payload: dict) -> None:
            try:
                from app.services.ai_service import generate_stock_narrative
                text = await generate_stock_narrative(payload)
                if not text:
                    return
                async with pool.acquire() as conn:
                    await conn.execute(
                        f"UPDATE {STOCK_TABLE} "
                        f"SET ai_narrative = $2, ai_narrative_at = NOW() "
                        f"WHERE symbol = $1",
                        symbol_, text,
                    )
            except Exception:
                logger.debug(
                    "ai_narrative regen failed for %s", symbol_, exc_info=True,
                )

        try:
            import asyncio as _asyncio
            loop = _asyncio.get_running_loop()
            loop.create_task(_regen_and_persist(symbol, ai_payload))
        except RuntimeError:
            pass  # not in an event loop — fine, just skip

    return {
        "symbol": symbol,
        "verdict": verdict,
        "ai_narrative": ai_narrative,
        "action_tag": sb.get("action_tag"),
        "action_tag_reasoning": sb.get("action_tag_reasoning"),
        "trend_alignment": sb.get("trend_alignment"),
        "breakout_signal": sb.get("breakout_signal"),
        "lynch_classification": sb.get("lynch_classification") or d.get("lynch_classification"),
        "why_narrative": sb.get("why_narrative"),
        "score_confidence": sb.get("score_confidence"),
        "score_changes": score_changes,
    }


# ---------------------------------------------------------------------------
# Phase 2: Stock Compare
# ---------------------------------------------------------------------------

async def compare_stocks(*, symbols: list[str]) -> dict:
    """Side-by-side comparison of multiple stocks."""
    pool = await get_pool()
    industry_stats = await _get_stock_industry_stats(pool)

    rows = await pool.fetch(
        f"SELECT * FROM {STOCK_TABLE} WHERE symbol = ANY($1::text[])",
        symbols,
    )

    items = []
    for r in rows:
        d = record_to_dict(r)
        ind = str(d.get("industry") or "Other")
        items.append(_decorate_stock_row(d, industry_stats.get(ind)))

    # Sort items to match the requested symbol order
    sym_order = {s: i for i, s in enumerate(symbols)}
    items.sort(key=lambda x: sym_order.get(x.get("symbol", ""), 999))

    # Build comparison dimensions
    metrics = [
        ("score", "Overall Score", True),       # higher is better
        ("pe_ratio", "P/E Ratio", False),        # lower is better
        ("roe", "ROE (%)", True),
        ("roce", "ROCE (%)", True),
        ("debt_to_equity", "Debt/Equity", False),
        ("market_cap", "Market Cap (Cr)", True),
        ("profit_margins", "Net Margin (%)", True),
        ("revenue_growth", "Revenue Growth (%)", True),
        ("dividend_yield", "Dividend Yield (%)", True),
        ("score_quality", "Quality Score", True),
    ]

    dimensions: list[dict] = []
    for metric_key, label, higher_better in metrics:
        values = [_to_float(item.get(metric_key)) for item in items]
        # Determine winner
        non_null = [(i, v) for i, v in enumerate(values) if v is not None]
        winner_index = None
        if len(non_null) >= 2:
            if higher_better:
                winner_index = max(non_null, key=lambda x: x[1])[0]
            else:
                winner_index = min(non_null, key=lambda x: x[1])[0]
        dimensions.append({
            "metric": metric_key,
            "label": label,
            "values": values,
            "winner_index": winner_index,
        })

    # AI comparison insight
    ai_insight = None
    try:
        from app.services.ai_service import generate_compare_insight

        ai_insight = await generate_compare_insight(items)
    except Exception:
        pass

    return {
        "items": items,
        "comparison_dimensions": dimensions,
        "ai_insight": ai_insight,
    }


# ---------------------------------------------------------------------------
# Phase 2: Market Mood
# ---------------------------------------------------------------------------

async def get_market_mood() -> dict:
    """Aggregate score distribution across all tracked stocks."""
    pool = await get_pool()
    row = await pool.fetchrow(
        f"""
        SELECT
            COUNT(*) AS total,
            AVG(score) AS avg_score,
            COUNT(*) FILTER (WHERE score >= 75) AS excellent,
            COUNT(*) FILTER (WHERE score >= 60 AND score < 75) AS good,
            COUNT(*) FILTER (WHERE score >= 40 AND score < 60) AS average,
            COUNT(*) FILTER (WHERE score < 40) AS poor
        FROM {STOCK_TABLE}
        WHERE market = 'IN' AND score IS NOT NULL
        """
    )
    if row is None or row["total"] == 0:
        return {"avg_score": None, "score_distribution": None, "summary": None}

    total = int(row["total"])
    excellent = int(row["excellent"])
    good = int(row["good"])
    average = int(row["average"])
    poor = int(row["poor"])
    avg_score = round(float(row["avg_score"]), 1)
    above_good_pct = round((excellent + good) / total * 100) if total > 0 else 0

    fallback_summary = f"{above_good_pct}% of tracked stocks are rated Good or above"

    # Try AI-enhanced summary
    ai_summary = None
    try:
        from app.services.ai_service import generate_market_mood_summary

        ai_summary = await generate_market_mood_summary({
            "avg_score": avg_score,
            "total": total,
            "excellent": excellent,
            "good": good,
            "average": average,
            "poor": poor,
            "above_good_pct": above_good_pct,
        })
    except Exception:
        pass

    return {
        "avg_score": avg_score,
        "score_distribution": {
            "excellent": excellent,
            "good": good,
            "average": average,
            "poor": poor,
        },
        "summary": ai_summary or fallback_summary,
    }

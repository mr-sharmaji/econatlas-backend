"""
Standalone tag generation engine for stocks and mutual funds.

Produces structured TagV2 objects with category, severity, priority,
confidence, and plain-English explanation.  Extracted from
discover_stock_job._generate_tags and discover_mutual_fund_job._generate_mf_tags
so that tag logic can be tested independently and reused in multiple contexts.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TypedDict


# ---------------------------------------------------------------------------
# Tag v2 typed dict
# ---------------------------------------------------------------------------

class TagV2(TypedDict, total=False):
    tag: str
    category: str        # classification | style | strength | valuation | risk | trend | ownership
    severity: str        # positive | negative | neutral
    priority: int
    confidence: float | None
    explanation: str | None
    expires_at: str | None   # ISO-8601 or None


# ---------------------------------------------------------------------------
# Category / severity constants
# ---------------------------------------------------------------------------

_CAT_CLASSIFICATION = "classification"
_CAT_STYLE = "style"
_CAT_STRENGTH = "strength"
_CAT_VALUATION = "valuation"
_CAT_RISK = "risk"
_CAT_TREND = "trend"
_CAT_OWNERSHIP = "ownership"

_SEV_POSITIVE = "positive"
_SEV_NEGATIVE = "negative"
_SEV_NEUTRAL = "neutral"

# Tags that rely on transient market conditions and should be refreshed
_TRANSIENT_TAGS = frozenset({
    "Bullish Trend", "Bearish Trend", "Promoter Buying", "FII Buying",
    "DII Buying", "Defensive Pick", "Bear Market Resilient",
    "Momentum Without Quality", "Quality Weak Momentum",
})


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_float(val, default=None) -> float | None:
    if val is None:
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def _fmt_pct(v: float | None) -> str:
    if v is None:
        return "N/A"
    return f"{v:.1f}%"


def _fmt_inr(v: float | None) -> str:
    if v is None:
        return "N/A"
    return f"\u20B9{v:,.0f}"


def _transient_expiry(refresh_hours: int = 6) -> str | None:
    """Return an ISO-8601 expiry timestamp for transient tags."""
    from datetime import timedelta
    return (datetime.now(timezone.utc) + timedelta(hours=refresh_hours)).isoformat()


def _tag(
    tag: str,
    category: str,
    severity: str,
    priority: int,
    explanation: str | None = None,
    confidence: float | None = None,
    expires_at: str | None = None,
) -> tuple[int, TagV2]:
    return (priority, TagV2(
        tag=tag,
        category=category,
        severity=severity,
        priority=priority,
        confidence=confidence,
        explanation=explanation,
        expires_at=expires_at,
    ))


# ---------------------------------------------------------------------------
# Stock tag generation
# ---------------------------------------------------------------------------

def generate_stock_tags(
    row: dict,
    *,
    quality_score: float,
    valuation_score: float | None,
    growth_score: float | None,
    momentum_score: float,
    institutional_score: float | None,
    risk_score: float | None,
    lynch_classification: str,
    market_regime: str,
    sector: str,
    pct_change_5y: float | None,
    peg_ratio: float | None,
    paper_profits: bool = False,
    sector_pe_median: float = 25.0,
) -> list[TagV2]:
    """Generate prioritized structured tags for the 6-layer stock model.

    Returns a list of TagV2 dicts (max 15), sorted by priority.
    """
    tagged: list[tuple[int, TagV2]] = []

    mcap = _safe_float(row.get("market_cap"))
    pe = _safe_float(row.get("pe_ratio"))
    roe = _safe_float(row.get("roe"))
    roce = _safe_float(row.get("roce"))
    dte = _safe_float(row.get("debt_to_equity"))
    eps = _safe_float(row.get("eps"))
    dy = _safe_float(row.get("dividend_yield"))
    promoter = _safe_float(row.get("promoter_holding"))
    fii = _safe_float(row.get("fii_holding"))
    dii = _safe_float(row.get("dii_holding"))
    public = _safe_float(row.get("public_holding"))
    pledged = _safe_float(row.get("pledged_promoter_pct"))
    fii_chg = _safe_float(row.get("fii_holding_change"))
    dii_chg = _safe_float(row.get("dii_holding_change"))
    prom_chg = _safe_float(row.get("promoter_holding_change"))
    rg = _safe_float(row.get("revenue_growth"))
    eg = _safe_float(row.get("earnings_growth"))
    opm_trend = _safe_float(row.get("_hist_opm_trend_3y"))
    debt_traj = _safe_float(row.get("_hist_debt_trajectory"))
    ocf_years = _safe_float(row.get("_hist_ocf_positive_years"))
    profit_cons = _safe_float(row.get("_hist_profit_growth_consistency"), 0)
    sales_cons = _safe_float(row.get("_hist_sales_growth_consistency"), 0)
    cwip_to_assets = _safe_float(row.get("_hist_cwip_to_assets"))
    opm_std = _safe_float(row.get("_hist_opm_std_5y"))
    inc_roe = _safe_float(row.get("_hist_incremental_roe"))
    rev_cagr_5y = _safe_float(row.get("_hist_5y_revenue_cagr"))
    prof_cagr_5y = _safe_float(row.get("_hist_5y_profit_cagr"))
    fcf = _safe_float(row.get("free_cash_flow"))
    total_cash = _safe_float(row.get("total_cash"))
    total_debt = _safe_float(row.get("total_debt"))
    rec_mean = _safe_float(row.get("analyst_recommendation_mean"))
    analyst_count = row.get("analyst_count")
    if analyst_count is not None:
        try:
            analyst_count = int(analyst_count)
        except (ValueError, TypeError):
            analyst_count = None
    target = _safe_float(row.get("analyst_target_mean"))
    price = _safe_float(row.get("last_price"))
    ma_50 = _safe_float(row.get("fifty_day_avg"))
    ma_200 = _safe_float(row.get("two_hundred_day_avg"))
    beta_val = _safe_float(row.get("beta"))

    # ── Priority 1: Market Cap ──────────────────────────────────────
    if mcap is not None:
        if mcap >= 20000:
            tagged.append(_tag(
                "Large Cap", _CAT_CLASSIFICATION, _SEV_NEUTRAL, 1,
                explanation=f"Market cap \u20B9{mcap:,.0f} Cr",
                confidence=1.0,
            ))
        elif mcap >= 5000:
            tagged.append(_tag(
                "Mid Cap", _CAT_CLASSIFICATION, _SEV_NEUTRAL, 1,
                explanation=f"Market cap \u20B9{mcap:,.0f} Cr",
                confidence=1.0,
            ))
        else:
            tagged.append(_tag(
                "Small Cap", _CAT_CLASSIFICATION, _SEV_NEUTRAL, 1,
                explanation=f"Market cap \u20B9{mcap:,.0f} Cr",
                confidence=1.0,
            ))

    # ── Priority 2: Lynch classification ────────────────────────────
    lynch_tag_map = {
        "turnaround": "Turnaround Story",
        "fast_grower": "Fast Grower",
        "stalwart": "Stalwart",
        "cyclical": "Cyclical Play",
        "asset_play": "Asset Play",
        "slow_grower": "Slow Grower",
    }
    if lynch_classification in lynch_tag_map:
        tagged.append(_tag(
            lynch_tag_map[lynch_classification], _CAT_STYLE, _SEV_NEUTRAL, 2,
            explanation=f"Peter Lynch classification based on growth profile",
            confidence=0.8 if growth_score is not None else 0.5,
        ))

    # ── Priority 2: Quality ─────────────────────────────────────────
    if quality_score >= 75:
        parts = []
        if roe is not None:
            parts.append(f"ROE {roe:.0f}%")
        if roce is not None:
            parts.append(f"ROCE {roce:.0f}%")
        tagged.append(_tag(
            "High Quality", _CAT_STRENGTH, _SEV_POSITIVE, 2,
            explanation=", ".join(parts) if parts else "Quality score above 75",
            confidence=0.9,
        ))
    if paper_profits:
        tagged.append(_tag(
            "Paper Profits", _CAT_RISK, _SEV_NEGATIVE, 2,
            explanation="Reported profits may not reflect operating cash flows",
            confidence=0.8,
        ))
    if pledged is not None and pledged > 20:
        tagged.append(_tag(
            "High Pledge Risk", _CAT_RISK, _SEV_NEGATIVE, 2,
            explanation=f"Promoter pledged {pledged:.1f}% of holdings",
            confidence=1.0,
        ))
    if sales_cons >= 4 and profit_cons >= 4:
        tagged.append(_tag(
            "Consistent Compounder", _CAT_STRENGTH, _SEV_POSITIVE, 2,
            explanation="Revenue and profit growth consistent for 4+ years",
            confidence=0.9,
        ))

    # ── Priority 3: Compounding ─────────────────────────────────────
    if rev_cagr_5y is not None and rev_cagr_5y > 0.15 and prof_cagr_5y is not None and prof_cagr_5y > 0.15:
        tagged.append(_tag(
            "Decade Compounder", _CAT_STRENGTH, _SEV_POSITIVE, 3,
            explanation=f"5Y revenue CAGR {rev_cagr_5y * 100:.0f}%, profit CAGR {prof_cagr_5y * 100:.0f}%",
            confidence=0.85,
        ))
    elif (rev_cagr_5y is not None and rev_cagr_5y > 0.15) or (prof_cagr_5y is not None and prof_cagr_5y > 0.15):
        cagr_parts = []
        if rev_cagr_5y is not None and rev_cagr_5y > 0.15:
            cagr_parts.append(f"revenue CAGR {rev_cagr_5y * 100:.0f}%")
        if prof_cagr_5y is not None and prof_cagr_5y > 0.15:
            cagr_parts.append(f"profit CAGR {prof_cagr_5y * 100:.0f}%")
        tagged.append(_tag(
            "5Y Wealth Creator", _CAT_STRENGTH, _SEV_POSITIVE, 3,
            explanation=f"Strong 5Y {', '.join(cagr_parts)}",
            confidence=0.8,
        ))

    # ── Priority 3: Quality signals ─────────────────────────────────
    if dte is not None and dte == 0 and eps is not None and eps > 0:
        tagged.append(_tag(
            "Debt Free", _CAT_STRENGTH, _SEV_POSITIVE, 3,
            explanation=f"Zero debt with positive EPS of \u20B9{eps:.1f}",
            confidence=1.0,
        ))
    if fcf is not None and mcap and mcap > 0:
        fcf_yield = (fcf / (mcap * 1e7)) * 100
        if fcf_yield > 5:
            tagged.append(_tag(
                "FCF Machine", _CAT_STRENGTH, _SEV_POSITIVE, 3,
                explanation=f"Free cash flow yield of {fcf_yield:.1f}%",
                confidence=0.9,
            ))
    if total_cash is not None and total_debt is not None and mcap and mcap > 0:
        net_cash_pct = ((total_cash - total_debt) / (mcap * 1e7)) * 100
        if net_cash_pct > 10:
            tagged.append(_tag(
                "Cash Rich", _CAT_STRENGTH, _SEV_POSITIVE, 3,
                explanation=f"Net cash is {net_cash_pct:.0f}% of market cap",
                confidence=0.85,
            ))

    # ── Priority 3: Valuation ───────────────────────────────────────
    if peg_ratio is not None and peg_ratio < 0.8 and quality_score > 50:
        tagged.append(_tag(
            "PEG Bargain", _CAT_VALUATION, _SEV_POSITIVE, 3,
            explanation=f"PEG ratio {peg_ratio:.2f} with quality score {quality_score:.0f}",
            confidence=0.8,
        ))
    if pe is not None and pe > 0 and pe < sector_pe_median * 0.7 and quality_score > 55:
        tagged.append(_tag(
            "Value Pick", _CAT_VALUATION, _SEV_POSITIVE, 3,
            explanation=f"P/E {pe:.1f} vs sector median {sector_pe_median:.1f}",
            confidence=0.8,
        ))
    if rec_mean is not None and rec_mean <= 1.5 and analyst_count is not None and analyst_count >= 10:
        tagged.append(_tag(
            "Analyst Strong Buy", _CAT_VALUATION, _SEV_POSITIVE, 3,
            explanation=f"Consensus {rec_mean:.1f} from {analyst_count} analysts",
            confidence=0.9,
        ))
    if inc_roe is not None and inc_roe > 20:
        tagged.append(_tag(
            "Capital Allocator", _CAT_STRENGTH, _SEV_POSITIVE, 3,
            explanation=f"Incremental ROE of {inc_roe:.0f}%",
            confidence=0.8,
        ))

    # ── Priority 4: Growth & Valuation ──────────────────────────────
    if peg_ratio is not None and 0.8 <= peg_ratio <= 1.2 and growth_score is not None and growth_score > 60:
        tagged.append(_tag(
            "Growth at Fair Price", _CAT_VALUATION, _SEV_POSITIVE, 4,
            explanation=f"PEG {peg_ratio:.2f} with growth score {growth_score:.0f}",
            confidence=0.75,
        ))
    if peg_ratio is not None and peg_ratio > 2.5:
        tagged.append(_tag(
            "Richly Valued", _CAT_RISK, _SEV_NEGATIVE, 4,
            explanation=f"PEG ratio {peg_ratio:.2f} indicates expensive valuation",
            confidence=0.75,
        ))
    elif pe is not None and pe > sector_pe_median * 1.8:
        tagged.append(_tag(
            "Richly Valued", _CAT_RISK, _SEV_NEGATIVE, 4,
            explanation=f"P/E {pe:.1f} is {pe / sector_pe_median:.1f}x sector median",
            confidence=0.7,
        ))
    if rg is not None and rg >= 0.15:
        tagged.append(_tag(
            "Growth Stock", _CAT_VALUATION, _SEV_POSITIVE, 4,
            explanation=f"Revenue growing at {rg * 100:.0f}%",
            confidence=0.8,
        ))
    elif eg is not None and eg >= 0.20:
        tagged.append(_tag(
            "Growth Stock", _CAT_VALUATION, _SEV_POSITIVE, 4,
            explanation=f"Earnings growing at {eg * 100:.0f}%",
            confidence=0.75,
        ))
    if dy is not None and dy >= 2.0:
        tagged.append(_tag(
            "High Dividend", _CAT_VALUATION, _SEV_POSITIVE, 4,
            explanation=f"Dividend yield {dy:.1f}%",
            confidence=1.0,
        ))
    if target and price and price > 0 and analyst_count and analyst_count >= 5:
        upside = ((target - price) / price) * 100
        if upside >= 25:
            tagged.append(_tag(
                "Analyst Undervalued", _CAT_VALUATION, _SEV_POSITIVE, 4,
                explanation=f"Target \u20B9{target:,.0f} implies {upside:.0f}% upside",
                confidence=0.7,
            ))

    # ── Priority 4: Quality trends ──────────────────────────────────
    if opm_trend is not None and opm_trend >= 3:
        tagged.append(_tag(
            "Margin Expansion", _CAT_TREND, _SEV_POSITIVE, 4,
            explanation="Operating margins expanding over 3+ years",
            confidence=0.8,
        ))
    if debt_traj is not None and debt_traj < -0.15:
        tagged.append(_tag(
            "Deleveraging", _CAT_TREND, _SEV_POSITIVE, 4,
            explanation=f"Debt reducing at {abs(debt_traj) * 100:.0f}% per year",
            confidence=0.8,
        ))
    if ocf_years is not None and ocf_years >= 4:
        tagged.append(_tag(
            "Strong Cash Flow", _CAT_STRENGTH, _SEV_POSITIVE, 4,
            explanation=f"Positive operating cash flow for {int(ocf_years)} consecutive years",
            confidence=0.85,
        ))

    # ── Priority 4: Divergence ──────────────────────────────────────
    transient_exp = _transient_expiry()
    if momentum_score >= 65 and quality_score < 40:
        tagged.append(_tag(
            "Momentum Without Quality", _CAT_RISK, _SEV_NEGATIVE, 4,
            explanation="Strong price momentum but weak fundamentals",
            confidence=0.7,
            expires_at=transient_exp,
        ))
    if quality_score >= 65 and momentum_score < 35:
        tagged.append(_tag(
            "Quality Weak Momentum", _CAT_TREND, _SEV_NEUTRAL, 4,
            explanation="Strong fundamentals but weak recent price action",
            confidence=0.7,
            expires_at=transient_exp,
        ))

    # ── Priority 4: Sector-specific ─────────────────────────────────
    if sector == "Financials" and opm_trend is not None and opm_trend > 0:
        tagged.append(_tag(
            "NIM Expander", _CAT_TREND, _SEV_POSITIVE, 4,
            explanation="Net interest margins expanding",
            confidence=0.7,
        ))
    if sector == "Healthcare" and cwip_to_assets is not None and cwip_to_assets > 0.10:
        tagged.append(_tag(
            "R&D Intensive", _CAT_TREND, _SEV_NEUTRAL, 4,
            explanation=f"CWIP is {cwip_to_assets * 100:.0f}% of total assets",
            confidence=0.7,
        ))
    if sector in ("Industrials", "Auto") and cwip_to_assets is not None and cwip_to_assets > 0.15:
        tagged.append(_tag(
            "Capacity Expansion", _CAT_TREND, _SEV_POSITIVE, 4,
            explanation=f"Capital work-in-progress is {cwip_to_assets * 100:.0f}% of assets",
            confidence=0.7,
        ))
    if sector == "FMCG" and opm_std is not None and opm_std < 2:
        tagged.append(_tag(
            "Margin Fortress", _CAT_STRENGTH, _SEV_POSITIVE, 4,
            explanation=f"Operating margin std dev only {opm_std:.1f}% over 5 years",
            confidence=0.8,
        ))

    # ── Priority 5: Regime ──────────────────────────────────────────
    if market_regime == "bear":
        if quality_score > 75 and risk_score is not None and risk_score > 70:
            tagged.append(_tag(
                "Defensive Pick", _CAT_STRENGTH, _SEV_POSITIVE, 5,
                explanation="High quality + low risk in bear market",
                confidence=0.7,
                expires_at=transient_exp,
            ))
        if beta_val is not None and beta_val < 0.7 and quality_score > 70:
            tagged.append(_tag(
                "Bear Market Resilient", _CAT_STRENGTH, _SEV_POSITIVE, 5,
                explanation=f"Beta {beta_val:.2f} with quality score {quality_score:.0f}",
                confidence=0.75,
                expires_at=transient_exp,
            ))

    # ── Priority 5: Ownership ───────────────────────────────────────
    if promoter is not None and promoter >= 55:
        tagged.append(_tag(
            "High Promoter", _CAT_OWNERSHIP, _SEV_NEUTRAL, 5,
            explanation=f"Promoter holds {promoter:.1f}%",
            confidence=1.0,
        ))
    if fii is not None and fii >= 20:
        tagged.append(_tag(
            "FII Favorite", _CAT_OWNERSHIP, _SEV_NEUTRAL, 5,
            explanation=f"FII holding {fii:.1f}%",
            confidence=1.0,
        ))
    if dii is not None and dii >= 25:
        tagged.append(_tag(
            "DII Backed", _CAT_OWNERSHIP, _SEV_NEUTRAL, 5,
            explanation=f"DII holding {dii:.1f}%",
            confidence=1.0,
        ))
    if prom_chg is not None and prom_chg > 0.5:
        tagged.append(_tag(
            "Promoter Buying", _CAT_OWNERSHIP, _SEV_POSITIVE, 5,
            explanation=f"Promoter increased stake by {prom_chg:.1f}% this quarter",
            confidence=0.9,
            expires_at=transient_exp,
        ))
    if fii_chg is not None and fii_chg > 0.5:
        tagged.append(_tag(
            "FII Buying", _CAT_OWNERSHIP, _SEV_POSITIVE, 5,
            explanation=f"FII stake increased by {fii_chg:.1f}% this quarter",
            confidence=0.9,
            expires_at=transient_exp,
        ))
    if dii_chg is not None and dii_chg > 0.5:
        tagged.append(_tag(
            "DII Buying", _CAT_OWNERSHIP, _SEV_POSITIVE, 5,
            explanation=f"DII stake increased by {dii_chg:.1f}% this quarter",
            confidence=0.9,
            expires_at=transient_exp,
        ))

    # ── Priority 5: Technicals ──────────────────────────────────────
    if ma_50 and ma_200 and price:
        if ma_50 > ma_200 and price > ma_50:
            tagged.append(_tag(
                "Bullish Trend", _CAT_TREND, _SEV_POSITIVE, 5,
                explanation="Price above 50-DMA, 50-DMA above 200-DMA",
                confidence=0.7,
                expires_at=transient_exp,
            ))
        elif ma_50 < ma_200 and price < ma_50:
            tagged.append(_tag(
                "Bearish Trend", _CAT_RISK, _SEV_NEGATIVE, 5,
                explanation="Price below 50-DMA, 50-DMA below 200-DMA",
                confidence=0.7,
                expires_at=transient_exp,
            ))

    if public is not None and public < 25:
        tagged.append(_tag(
            "Low Free Float", _CAT_RISK, _SEV_NEGATIVE, 5,
            explanation=f"Only {public:.1f}% public float",
            confidence=1.0,
        ))

    # ── Priority 6 ──────────────────────────────────────────────────
    if eps is not None and eps < 0:
        tagged.append(_tag(
            "Negative EPS", _CAT_RISK, _SEV_NEGATIVE, 6,
            explanation=f"EPS is \u20B9{eps:.1f}",
            confidence=1.0,
        ))

    # Sort by priority, deduplicate, limit to 15
    return _finalize(tagged, max_tags=15)


# ---------------------------------------------------------------------------
# Mutual fund tag generation
# ---------------------------------------------------------------------------

def generate_mf_tags(
    row: dict,
    sub_cat: str,
    sub_cat_scores: list[float],
    sub_cat_expenses: list[float],
    sub_cat_avg_ret3y: float | None,
) -> list[TagV2]:
    """Generate prioritized structured tags for mutual funds.

    Returns a list of TagV2 dicts (max 10), sorted by priority.
    """
    tagged: list[tuple[int, TagV2]] = []

    score = _safe_float(row.get("_final_score"))
    ret3 = _safe_float(row.get("returns_3y"))
    ret5 = _safe_float(row.get("returns_5y"))
    ret1 = _safe_float(row.get("returns_1y"))
    expense = _safe_float(row.get("expense_ratio"))
    sharpe = _safe_float(row.get("sharpe"))
    std_dev = _safe_float(row.get("std_dev"))
    rolling = _safe_float(row.get("rolling_return_consistency"))
    aum = _safe_float(row.get("aum_cr"))
    age = _safe_float(row.get("fund_age_years"))
    risk = str(row.get("risk_level") or "").strip().lower()

    # ── Pri 1: Sub-category leader ──────────────────────────────────
    if score is not None and sub_cat_scores:
        pctl = _percentile_rank(sub_cat_scores, score)
        if pctl >= 90:
            label = f"{sub_cat} Leader" if sub_cat != "DEFAULT" else "Category Leader"
            tagged.append(_tag(
                label, _CAT_STRENGTH, _SEV_POSITIVE, 1,
                explanation=f"Top 10% in {sub_cat} by overall score",
                confidence=0.9,
            ))

    # ── Pri 1: Sub-category label ───────────────────────────────────
    if sub_cat not in ("DEFAULT",):
        tagged.append(_tag(
            sub_cat, _CAT_CLASSIFICATION, _SEV_NEUTRAL, 1,
            explanation=f"Fund sub-category: {sub_cat}",
            confidence=1.0,
        ))

    # ── Pri 2: Consistency ──────────────────────────────────────────
    if rolling is not None and rolling < 5.0 and std_dev is not None and std_dev < 15.0:
        tagged.append(_tag(
            "Consistent Compounder", _CAT_STRENGTH, _SEV_POSITIVE, 2,
            explanation=f"Rolling return consistency {rolling:.1f}%, std dev {std_dev:.1f}%",
            confidence=0.85,
        ))

    # ── Pri 2: Low cost ─────────────────────────────────────────────
    if expense is not None and sub_cat_expenses:
        cost_pctl = _percentile_rank(sub_cat_expenses, expense)
        if cost_pctl <= 20:
            tagged.append(_tag(
                "Low Cost Leader", _CAT_STRENGTH, _SEV_POSITIVE, 2,
                explanation=f"Expense ratio {expense:.2f}% is bottom 20% in category",
                confidence=0.9,
            ))

    # ── Pri 3: Performance ──────────────────────────────────────────
    if ret3 is not None and sub_cat_avg_ret3y is not None and ret3 > sub_cat_avg_ret3y + 3.0:
        tagged.append(_tag(
            "Strong Alpha", _CAT_STRENGTH, _SEV_POSITIVE, 3,
            explanation=f"3Y return {ret3:.1f}% vs category avg {sub_cat_avg_ret3y:.1f}%",
            confidence=0.85,
        ))
    if sharpe is not None and sharpe > 1.5:
        tagged.append(_tag(
            "High Sharpe", _CAT_STRENGTH, _SEV_POSITIVE, 3,
            explanation=f"Sharpe ratio {sharpe:.2f} indicates excellent risk-adjusted returns",
            confidence=0.85,
        ))
    if age is not None and age > 10:
        tagged.append(_tag(
            "Decade Veteran", _CAT_CLASSIFICATION, _SEV_NEUTRAL, 3,
            explanation=f"Fund is {age:.0f} years old",
            confidence=1.0,
        ))
    if ret3 is not None and ret3 >= 12:
        tagged.append(_tag(
            "Strong Returns", _CAT_STRENGTH, _SEV_POSITIVE, 3,
            explanation=f"3Y annualized return {ret3:.1f}%",
            confidence=0.85,
        ))
    if ret5 is not None and ret5 >= 12:
        tagged.append(_tag(
            "5Y Consistent", _CAT_STRENGTH, _SEV_POSITIVE, 3,
            explanation=f"5Y annualized return {ret5:.1f}%",
            confidence=0.9,
        ))

    # ── Pri 4: Risk / warning tags ──────────────────────────────────
    if risk in ("high", "very high"):
        tagged.append(_tag(
            "High Risk", _CAT_RISK, _SEV_NEGATIVE, 4,
            explanation=f"Risk level: {risk}",
            confidence=1.0,
        ))
    if age is not None and age < 3:
        tagged.append(_tag(
            "New Fund", _CAT_RISK, _SEV_NEGATIVE, 4,
            explanation=f"Fund is only {age:.1f} years old — limited track record",
            confidence=1.0,
        ))
    if aum is not None and aum < 25:
        tagged.append(_tag(
            "Very Small Fund", _CAT_RISK, _SEV_NEGATIVE, 4,
            explanation=f"AUM \u20B9{aum:.0f} Cr — liquidity risk",
            confidence=0.9,
        ))
    elif aum is not None and aum < 50:
        tagged.append(_tag(
            "Small Fund", _CAT_RISK, _SEV_NEGATIVE, 4,
            explanation=f"AUM \u20B9{aum:.0f} Cr",
            confidence=0.85,
        ))
    if expense is not None and expense > 2.0:
        tagged.append(_tag(
            "High Cost", _CAT_RISK, _SEV_NEGATIVE, 4,
            explanation=f"Expense ratio {expense:.2f}% is above 2%",
            confidence=1.0,
        ))
    if ret1 is not None and ret1 < 0:
        tagged.append(_tag(
            "Negative Returns", _CAT_RISK, _SEV_NEGATIVE, 4,
            explanation=f"1Y return is {ret1:.1f}%",
            confidence=1.0,
        ))

    return _finalize(tagged, max_tags=10)


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _finalize(tagged: list[tuple[int, TagV2]], max_tags: int) -> list[TagV2]:
    """Sort by priority, deduplicate, and cap at max_tags."""
    seen: set[str] = set()
    result: list[TagV2] = []
    for _, tag_obj in sorted(tagged, key=lambda x: x[0]):
        name = tag_obj["tag"]
        if name not in seen:
            seen.add(name)
            result.append(tag_obj)
        if len(result) >= max_tags:
            break
    return result


def _percentile_rank(values: list[float], target: float) -> float:
    """Return percentile rank (0-100) of target within values."""
    if not values:
        return 50.0
    below = sum(1 for v in values if v < target)
    return (below / len(values)) * 100


def tags_v2_to_flat(tags_v2: list[TagV2]) -> list[str]:
    """Convert structured tags to flat string list for backward compat."""
    return [t["tag"] for t in tags_v2]

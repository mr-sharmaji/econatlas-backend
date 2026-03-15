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
    # Normalise mixed units: some sources report in crores, others in raw rupees.
    # Values above 1e7 are almost certainly raw rupees — convert to crores.
    if mcap is not None and mcap > 1e7:
        mcap = mcap / 1e7
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

    # \u2500\u2500 Priority 1: Market Cap \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
    if mcap is not None:
        if mcap >= 20000:
            tagged.append(_tag(
                "Large Cap", _CAT_CLASSIFICATION, _SEV_NEUTRAL, 1,
                explanation=f"Market cap \u20B9{mcap:,.0f} Cr. Large-cap companies are well-established industry leaders. They tend to be more stable with lower volatility, making them suitable for conservative investors seeking steady returns.",
            ))
        elif mcap >= 5000:
            tagged.append(_tag(
                "Mid Cap", _CAT_CLASSIFICATION, _SEV_NEUTRAL, 1,
                explanation=f"Market cap \u20B9{mcap:,.0f} Cr. Mid-cap companies offer a balance between stability and growth. They are growing businesses that can deliver higher returns than large-caps, but with more price swings.",
            ))
        else:
            tagged.append(_tag(
                "Small Cap", _CAT_CLASSIFICATION, _SEV_NEUTRAL, 1,
                explanation=f"Market cap \u20B9{mcap:,.0f} Cr. Small-cap companies have high growth potential but come with higher risk and volatility. They can be less liquid, meaning it may be harder to buy or sell large quantities.",
            ))

    # \u2500\u2500 Priority 2: Lynch classification \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
    lynch_tag_map = {
        "turnaround": "Turnaround Story",
        "fast_grower": "Fast Grower",
        "stalwart": "Stalwart",
        "cyclical": "Cyclical Play",
        "asset_play": "Asset Play",
        "slow_grower": "Slow Grower",
        "speculative": "Speculative",
    }
    lynch_explanations = {
        "turnaround": "This company is recovering from a difficult period. Turnaround stocks can deliver big returns if the recovery succeeds, but carry higher risk if the business doesn't improve as expected.",
        "fast_grower": "This company is growing revenue and profits rapidly (15%+ per year). Fast growers can deliver outstanding returns, but watch for signs of slowing growth or overvaluation.",
        "stalwart": "A large, steady company growing at a moderate pace (10-15% annually). Stalwarts offer reliable returns with lower risk \u2014 ideal as core portfolio holdings.",
        "cyclical": "This company's profits rise and fall with economic cycles. Timing matters \u2014 buying near the bottom of a cycle and selling near the top can be very profitable.",
        "asset_play": "The company may be undervalued because the market isn't fully recognizing the value of its assets (land, cash, subsidiaries, or patents).",
        "slow_grower": "A mature company growing slowly (under 10% per year), often paying good dividends. Best suited for income-oriented investors rather than capital appreciation.",
        "speculative": "This company is currently loss-making (negative EPS). Speculative stocks can deliver big returns if the business becomes profitable, but carry significant risk of further losses. Only for risk-tolerant investors.",
    }
    if lynch_classification in lynch_tag_map:
        tagged.append(_tag(
            lynch_tag_map[lynch_classification], _CAT_STYLE, _SEV_NEUTRAL, 2,
            explanation=lynch_explanations.get(lynch_classification, "Peter Lynch classification based on growth profile"),
        ))

    # \u2500\u2500 Priority 2: Quality \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
    if quality_score >= 75:
        parts = []
        if roe is not None:
            parts.append(f"ROE {roe:.0f}%")
        if roce is not None:
            parts.append(f"ROCE {roce:.0f}%")
        detail = f" ({', '.join(parts)})" if parts else ""
        tagged.append(_tag(
            "High Quality", _CAT_STRENGTH, _SEV_POSITIVE, 2,
            explanation=f"Quality score above 75{detail}. This company efficiently generates profits from its capital and has strong financial fundamentals. High-quality businesses tend to weather market downturns better.",
        ))
    if paper_profits:
        tagged.append(_tag(
            "Paper Profits", _CAT_RISK, _SEV_NEGATIVE, 2,
            explanation="The company reports profits on paper, but isn't generating equivalent cash from operations. This can happen due to accounting adjustments, receivables piling up, or one-time gains. Watch for sustained divergence between reported profit and actual cash flow.",
        ))
    if pledged is not None and pledged > 20:
        tagged.append(_tag(
            "High Pledge Risk", _CAT_RISK, _SEV_NEGATIVE, 2,
            explanation=f"Promoters have pledged {pledged:.1f}% of their shares as collateral for loans. If the stock price drops, lenders may force-sell these shares, creating a downward spiral. Pledge above 20% is a significant risk factor.",
        ))
    if sales_cons >= 4 and profit_cons >= 4:
        tagged.append(_tag(
            "Consistent Compounder", _CAT_STRENGTH, _SEV_POSITIVE, 2,
            explanation="Both revenue and profit have grown consistently for 4+ years without major setbacks. Consistent compounders are rare \u2014 they indicate a strong business model, good management, and sustainable competitive advantages.",
        ))

    # \u2500\u2500 Priority 3: Compounding \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
    if rev_cagr_5y is not None and rev_cagr_5y > 0.15 and prof_cagr_5y is not None and prof_cagr_5y > 0.15:
        tagged.append(_tag(
            "Decade Compounder", _CAT_STRENGTH, _SEV_POSITIVE, 3,
            explanation=f"5-year revenue CAGR of {rev_cagr_5y * 100:.0f}% and profit CAGR of {prof_cagr_5y * 100:.0f}%. Both revenue and profits have been compounding at 15%+ annually \u2014 a hallmark of businesses that can create substantial long-term wealth.",
        ))
    elif (rev_cagr_5y is not None and rev_cagr_5y > 0.15) or (prof_cagr_5y is not None and prof_cagr_5y > 0.15):
        cagr_parts = []
        if rev_cagr_5y is not None and rev_cagr_5y > 0.15:
            cagr_parts.append(f"revenue CAGR {rev_cagr_5y * 100:.0f}%")
        if prof_cagr_5y is not None and prof_cagr_5y > 0.15:
            cagr_parts.append(f"profit CAGR {prof_cagr_5y * 100:.0f}%")
        tagged.append(_tag(
            "5Y Wealth Creator", _CAT_STRENGTH, _SEV_POSITIVE, 3,
            explanation=f"Strong 5-year {', '.join(cagr_parts)}. Companies that sustain 15%+ growth over 5 years have a proven ability to scale their business and generate increasing shareholder value.",
        ))

    # \u2500\u2500 Priority 3: Quality signals \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
    if dte is not None and dte == 0 and eps is not None and eps > 0:
        tagged.append(_tag(
            "Debt Free", _CAT_STRENGTH, _SEV_POSITIVE, 3,
            explanation=f"The company has zero debt and earns \u20B9{eps:.1f} per share. Debt-free companies have no interest burden, face lower risk during economic downturns, and have full flexibility to invest in growth.",
        ))
    if fcf is not None and mcap and mcap > 0:
        fcf_yield = (fcf / (mcap * 1e7)) * 100
        if fcf_yield > 5:
            tagged.append(_tag(
                "FCF Machine", _CAT_STRENGTH, _SEV_POSITIVE, 3,
                explanation=f"Free cash flow yield of {fcf_yield:.1f}%. The company generates significant cash after all expenses and investments. High FCF yield means the business is a cash-generating machine \u2014 it can fund growth, pay dividends, or buy back shares.",
            ))
    if total_cash is not None and total_debt is not None and mcap and mcap > 0:
        net_cash_pct = ((total_cash - total_debt) / (mcap * 1e7)) * 100
        if net_cash_pct > 10:
            tagged.append(_tag(
                "Cash Rich", _CAT_STRENGTH, _SEV_POSITIVE, 3,
                explanation=f"Net cash (cash minus debt) is {net_cash_pct:.0f}% of market cap. The company has more cash than debt, providing a safety cushion and the ability to seize acquisition or expansion opportunities.",
            ))

    # \u2500\u2500 Priority 3: Valuation \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
    if peg_ratio is not None and peg_ratio < 0.8 and quality_score > 50:
        tagged.append(_tag(
            "PEG Bargain", _CAT_VALUATION, _SEV_POSITIVE, 3,
            explanation=f"PEG ratio of {peg_ratio:.2f} (P/E adjusted for growth rate). A PEG below 1 suggests the stock is cheap relative to its growth rate. Below 0.8 with decent quality is a strong value signal.",
        ))
    if pe is not None and pe > 0 and pe < sector_pe_median * 0.7 and quality_score > 55:
        tagged.append(_tag(
            "Value Pick", _CAT_VALUATION, _SEV_POSITIVE, 3,
            explanation=f"P/E of {pe:.1f} is significantly below sector median of {sector_pe_median:.1f}. The stock appears undervalued compared to peers in the same sector, which could mean the market is underappreciating its earnings potential.",
        ))
    if rec_mean is not None and rec_mean <= 1.5 and analyst_count is not None and analyst_count >= 10:
        tagged.append(_tag(
            "Analyst Strong Buy", _CAT_VALUATION, _SEV_POSITIVE, 3,
            explanation=f"Average analyst rating of {rec_mean:.1f} (on 1-5 scale, 1=Strong Buy) from {analyst_count} analysts. A strong consensus from many analysts indicates professional investors see significant upside potential.",
        ))
    if inc_roe is not None and inc_roe > 20:
        tagged.append(_tag(
            "Capital Allocator", _CAT_STRENGTH, _SEV_POSITIVE, 3,
            explanation=f"Incremental ROE of {inc_roe:.0f}%. The company is earning 20%+ return on every additional rupee of equity invested. This means management is excellent at deploying capital into high-return projects.",
        ))

    # \u2500\u2500 Priority 4: Growth & Valuation \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
    if peg_ratio is not None and 0.8 <= peg_ratio <= 1.2 and growth_score is not None and growth_score > 60:
        tagged.append(_tag(
            "Growth at Fair Price", _CAT_VALUATION, _SEV_POSITIVE, 4,
            explanation=f"PEG ratio of {peg_ratio:.2f} with a growth score of {growth_score:.0f}. The stock is priced fairly relative to its growth rate \u2014 not cheap but not expensive either. A good entry point for growth investors who don't want to overpay.",
        ))
    if peg_ratio is not None and peg_ratio > 2.5:
        tagged.append(_tag(
            "Richly Valued", _CAT_RISK, _SEV_NEGATIVE, 4,
            explanation=f"PEG ratio of {peg_ratio:.2f} is well above 1, meaning the stock's P/E is much higher than its growth rate justifies. The market is pricing in very high expectations \u2014 any growth disappointment could lead to a sharp price correction.",
        ))
    elif pe is not None and pe > sector_pe_median * 1.8:
        tagged.append(_tag(
            "Richly Valued", _CAT_RISK, _SEV_NEGATIVE, 4,
            explanation=f"P/E of {pe:.1f} is {pe / sector_pe_median:.1f}x the sector median. The stock trades at a significant premium to peers, meaning investors are paying a lot for each rupee of earnings. This leaves little room for error.",
        ))
    if rg is not None and rg >= 0.15:
        tagged.append(_tag(
            "Growth Stock", _CAT_VALUATION, _SEV_POSITIVE, 4,
            explanation=f"Revenue growing at {rg * 100:.0f}% year-over-year. A growth rate above 15% indicates the company is rapidly expanding its business, gaining market share, or entering new markets.",
        ))
    elif eg is not None and eg >= 0.20:
        tagged.append(_tag(
            "Growth Stock", _CAT_VALUATION, _SEV_POSITIVE, 4,
            explanation=f"Earnings growing at {eg * 100:.0f}% year-over-year. Strong profit growth suggests improving efficiency, pricing power, or operating leverage even if revenue growth is moderate.",
        ))
    if dy is not None and dy >= 2.0:
        tagged.append(_tag(
            "High Dividend", _CAT_VALUATION, _SEV_POSITIVE, 4,
            explanation=f"Dividend yield of {dy:.1f}%. The company returns a meaningful portion of profits to shareholders as dividends. This provides regular income and signals management's confidence in future cash flows.",
        ))
    if target and price and price > 0 and analyst_count and analyst_count >= 5:
        upside = ((target - price) / price) * 100
        if upside >= 25:
            tagged.append(_tag(
                "Analyst Undervalued", _CAT_VALUATION, _SEV_POSITIVE, 4,
                explanation=f"Analyst target price of \u20B9{target:,.0f} implies {upside:.0f}% upside from the current price. Based on {analyst_count} analysts' estimates, the stock appears undervalued by the market.",
            ))

    # \u2500\u2500 Priority 4: Quality trends \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
    if opm_trend is not None and opm_trend >= 3:
        tagged.append(_tag(
            "Margin Expansion", _CAT_TREND, _SEV_POSITIVE, 4,
            explanation="Operating margins have been expanding for 3+ years. This means the company is becoming more profitable per rupee of revenue \u2014 a sign of improving efficiency, pricing power, or better cost management.",
        ))
    if debt_traj is not None and debt_traj < -0.15:
        tagged.append(_tag(
            "Deleveraging", _CAT_TREND, _SEV_POSITIVE, 4,
            explanation=f"Debt is reducing at {abs(debt_traj) * 100:.0f}% per year. The company is actively paying down its borrowings, which reduces interest costs and financial risk. This frees up cash for growth or dividends.",
        ))
    if ocf_years is not None and ocf_years >= 4:
        tagged.append(_tag(
            "Strong Cash Flow", _CAT_STRENGTH, _SEV_POSITIVE, 4,
            explanation=f"Positive operating cash flow for {int(ocf_years)} consecutive years. The business consistently converts its profits into actual cash \u2014 a key sign of earnings quality. Companies with sustained positive cash flow are less likely to face financial distress.",
        ))

    # \u2500\u2500 Priority 4: Divergence \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
    transient_exp = _transient_expiry()
    if momentum_score >= 65 and quality_score < 40:
        tagged.append(_tag(
            "Momentum Without Quality", _CAT_RISK, _SEV_NEGATIVE, 4,
            explanation="The stock price has strong upward momentum, but the company's fundamentals are weak. This is a warning sign \u2014 the price may be driven by speculation rather than business strength. Such rallies often don't sustain.",
            expires_at=transient_exp,
        ))
    if quality_score >= 65 and momentum_score < 35:
        tagged.append(_tag(
            "Quality Weak Momentum", _CAT_TREND, _SEV_NEUTRAL, 4,
            explanation="The company has strong fundamentals but the stock price hasn't been performing well recently. This can be a buying opportunity if you believe the market will eventually recognize the company's value.",
            expires_at=transient_exp,
        ))

    # \u2500\u2500 Priority 4: Sector-specific \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
    if sector == "Financials" and opm_trend is not None and opm_trend > 0:
        tagged.append(_tag(
            "NIM Expander", _CAT_TREND, _SEV_POSITIVE, 4,
            explanation="Net interest margins (the spread between lending and borrowing rates) are expanding. For banks and NBFCs, this is a key profitability driver \u2014 it means they're earning more on every rupee they lend.",
        ))
    if sector == "Healthcare" and cwip_to_assets is not None and cwip_to_assets > 0.10:
        tagged.append(_tag(
            "R&D Intensive", _CAT_TREND, _SEV_NEUTRAL, 4,
            explanation=f"Capital work-in-progress (CWIP) is {cwip_to_assets * 100:.0f}% of total assets. The company is investing heavily in R&D or new facilities. This could lead to future revenue growth but also means higher near-term spending.",
        ))
    if sector in ("Industrials", "Auto") and cwip_to_assets is not None and cwip_to_assets > 0.15:
        tagged.append(_tag(
            "Capacity Expansion", _CAT_TREND, _SEV_POSITIVE, 4,
            explanation=f"Capital work-in-progress (CWIP) is {cwip_to_assets * 100:.0f}% of total assets. The company is building new manufacturing capacity, which should drive revenue growth once these projects are completed and operational.",
        ))
    if sector == "FMCG" and opm_std is not None and opm_std < 2:
        tagged.append(_tag(
            "Margin Fortress", _CAT_STRENGTH, _SEV_POSITIVE, 4,
            explanation=f"Operating margin standard deviation is only {opm_std:.1f}% over 5 years. The company's profit margins are extremely stable \u2014 indicating strong brand pricing power and predictable earnings, which is rare even among FMCG companies.",
        ))

    # \u2500\u2500 Priority 5: Regime \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
    if market_regime == "bear":
        if quality_score > 75 and risk_score is not None and risk_score > 70:
            tagged.append(_tag(
                "Defensive Pick", _CAT_STRENGTH, _SEV_POSITIVE, 5,
                explanation="High quality with low risk profile, ideal for the current bear market. Defensive picks tend to fall less during downturns and recover faster. They provide stability when the broader market is declining.",
                expires_at=transient_exp,
            ))
        if beta_val is not None and beta_val < 0.7 and quality_score > 70:
            tagged.append(_tag(
                "Bear Market Resilient", _CAT_STRENGTH, _SEV_POSITIVE, 5,
                explanation=f"Beta of {beta_val:.2f} (quality score {quality_score:.0f}). A beta below 0.7 means the stock moves less than the market \u2014 when Nifty falls 10%, this stock historically falls only {beta_val * 10:.0f}%. Combined with strong quality, it's a safer hold during market corrections.",
                expires_at=transient_exp,
            ))

    # \u2500\u2500 Priority 5: Ownership \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
    if promoter is not None and promoter >= 55:
        tagged.append(_tag(
            "High Promoter", _CAT_OWNERSHIP, _SEV_NEUTRAL, 5,
            explanation=f"Promoters hold {promoter:.1f}% of the company. High promoter holding (above 55%) shows strong insider confidence \u2014 the people who know the business best have significant skin in the game.",
        ))
    if fii is not None and fii >= 20:
        tagged.append(_tag(
            "FII Favorite", _CAT_OWNERSHIP, _SEV_NEUTRAL, 5,
            explanation=f"Foreign Institutional Investors hold {fii:.1f}% of shares. High FII ownership signals that global fund managers have vetted and approved this company. It also adds liquidity but can cause volatility during global sell-offs.",
        ))
    if dii is not None and dii >= 25:
        tagged.append(_tag(
            "DII Backed", _CAT_OWNERSHIP, _SEV_NEUTRAL, 5,
            explanation=f"Domestic Institutional Investors (mutual funds, insurance companies) hold {dii:.1f}%. Strong DII backing provides buying support during market dips, as these institutions invest with a long-term view.",
        ))
    if prom_chg is not None and prom_chg > 0.5:
        tagged.append(_tag(
            "Promoter Buying", _CAT_OWNERSHIP, _SEV_POSITIVE, 5,
            explanation=f"Promoters increased their stake by {prom_chg:.1f}% this quarter. When company insiders are buying more shares with their own money, it's a strong vote of confidence in the company's future prospects.",
            expires_at=transient_exp,
        ))
    if fii_chg is not None and fii_chg > 0.5:
        tagged.append(_tag(
            "FII Buying", _CAT_OWNERSHIP, _SEV_POSITIVE, 5,
            explanation=f"Foreign Institutional Investors increased their stake by {fii_chg:.1f}% this quarter. FII buying indicates growing global institutional interest, which often precedes sustained price appreciation.",
            expires_at=transient_exp,
        ))
    if dii_chg is not None and dii_chg > 0.5:
        tagged.append(_tag(
            "DII Buying", _CAT_OWNERSHIP, _SEV_POSITIVE, 5,
            explanation=f"Domestic Institutional Investors increased their stake by {dii_chg:.1f}% this quarter. DII buying (mutual funds, insurance companies) signals that professional domestic fund managers see value at current levels.",
            expires_at=transient_exp,
        ))

    # \u2500\u2500 Priority 5: Technicals \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
    if ma_50 and ma_200 and price:
        if ma_50 > ma_200 and price > ma_50:
            tagged.append(_tag(
                "Bullish Trend", _CAT_TREND, _SEV_POSITIVE, 5,
                explanation="Price is above the 50-day moving average, and the 50-DMA is above the 200-DMA. This 'golden cross' pattern indicates the stock is in a strong uptrend with both short-term and long-term momentum aligned upward.",
                expires_at=transient_exp,
            ))
        elif ma_50 < ma_200 and price < ma_50:
            tagged.append(_tag(
                "Bearish Trend", _CAT_RISK, _SEV_NEGATIVE, 5,
                explanation="Price is below the 50-day moving average, and the 50-DMA is below the 200-DMA. This 'death cross' pattern indicates the stock is in a downtrend with both short-term and long-term momentum pointing downward.",
                expires_at=transient_exp,
            ))

    if public is not None and public < 25:
        tagged.append(_tag(
            "Low Free Float", _CAT_RISK, _SEV_NEGATIVE, 5,
            explanation=f"Only {public:.1f}% of shares are available for public trading. Low free float means fewer shares in circulation, which can cause exaggerated price movements and make it harder to buy or sell at desired prices.",
        ))

    if promoter is not None and promoter < 30:
        tagged.append(_tag(
            "Low Promoter", _CAT_RISK, _SEV_NEGATIVE, 5,
            explanation=f"Promoters hold only {promoter:.1f}% of the company. Low promoter stake means less insider commitment \u2014 the stock may be more vulnerable to speculative swings and lack strong management alignment.",
        ))

    # \u2500\u2500 Priority 6 \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
    if eps is not None and eps < 0:
        tagged.append(_tag(
            "Negative EPS", _CAT_RISK, _SEV_NEGATIVE, 6,
            explanation=f"Earnings per share is \u20B9{eps:.1f} (negative). The company is currently losing money. While this may be temporary (turnaround or heavy investment phase), sustained losses erode shareholder value.",
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

    # \u2500\u2500 Pri 1: Sub-category leader \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
    if score is not None and sub_cat_scores:
        pctl = _percentile_rank(sub_cat_scores, score)
        if pctl >= 90:
            label = f"{sub_cat} Leader" if sub_cat != "DEFAULT" else "Category Leader"
            tagged.append(_tag(
                label, _CAT_STRENGTH, _SEV_POSITIVE, 1,
                explanation=f"Ranks in the top 10% among all {sub_cat} funds by overall score. This fund consistently outperforms most of its peers across key metrics like returns, risk-adjusted performance, and cost efficiency.",
            ))

    # \u2500\u2500 Pri 1: Sub-category label \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
    if sub_cat not in ("DEFAULT",):
        tagged.append(_tag(
            sub_cat, _CAT_CLASSIFICATION, _SEV_NEUTRAL, 1,
            explanation=f"This fund belongs to the {sub_cat} sub-category as classified by SEBI. Comparing funds within the same category gives you a fair like-for-like comparison.",
        ))

    # \u2500\u2500 Pri 2: Consistency \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
    if rolling is not None and rolling < 5.0 and std_dev is not None and std_dev < 15.0:
        tagged.append(_tag(
            "Consistent Compounder", _CAT_STRENGTH, _SEV_POSITIVE, 2,
            explanation=f"Rolling return consistency of {rolling:.1f}% with standard deviation of {std_dev:.1f}%. The fund delivers steady returns without wild swings \u2014 ideal for investors who want predictable growth without stomach-churning volatility.",
        ))

    # \u2500\u2500 Pri 2: Low cost \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
    if expense is not None and sub_cat_expenses:
        cost_pctl = _percentile_rank(sub_cat_expenses, expense)
        if cost_pctl <= 20:
            tagged.append(_tag(
                "Low Cost Leader", _CAT_STRENGTH, _SEV_POSITIVE, 2,
                explanation=f"Expense ratio of {expense:.2f}% is in the bottom 20% of its category. Lower fees mean more of your returns stay in your pocket. Over long periods, even a 0.5% difference in fees can significantly impact wealth creation.",
            ))

    # \u2500\u2500 Pri 3: Performance \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
    if ret3 is not None and sub_cat_avg_ret3y is not None and ret3 > sub_cat_avg_ret3y + 3.0:
        tagged.append(_tag(
            "Strong Alpha", _CAT_STRENGTH, _SEV_POSITIVE, 3,
            explanation=f"3-year return of {ret3:.1f}% vs category average of {sub_cat_avg_ret3y:.1f}%. The fund is delivering returns well above its peers, indicating strong stock selection or sector allocation by the fund manager.",
        ))
    if sharpe is not None and sharpe > 1.5:
        tagged.append(_tag(
            "High Sharpe", _CAT_STRENGTH, _SEV_POSITIVE, 3,
            explanation=f"Sharpe ratio of {sharpe:.2f}. This measures return per unit of risk \u2014 above 1.5 is excellent. It means the fund isn't just delivering good returns, it's doing so without taking excessive risk.",
        ))
    if age is not None and age > 10:
        tagged.append(_tag(
            "Decade Veteran", _CAT_CLASSIFICATION, _SEV_NEUTRAL, 3,
            explanation=f"This fund has been running for {age:.0f} years. A 10+ year track record means the fund has survived multiple market cycles (bull runs, corrections, and crashes), giving you reliable data to evaluate its true performance.",
        ))
    if ret3 is not None and ret3 >= 12:
        tagged.append(_tag(
            "Strong Returns", _CAT_STRENGTH, _SEV_POSITIVE, 3,
            explanation=f"3-year annualized return of {ret3:.1f}%. Delivering 12%+ CAGR over 3 years indicates the fund is generating solid wealth for investors, beating typical fixed-deposit returns by a significant margin.",
        ))
    if ret5 is not None and ret5 >= 12:
        tagged.append(_tag(
            "5Y Consistent", _CAT_STRENGTH, _SEV_POSITIVE, 3,
            explanation=f"5-year annualized return of {ret5:.1f}%. Sustaining 12%+ returns over 5 years through various market conditions demonstrates the fund manager's skill and the fund's resilience.",
        ))

    # \u2500\u2500 Pri 4: Risk / warning tags \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
    if risk in ("high", "very high"):
        tagged.append(_tag(
            "High Risk", _CAT_RISK, _SEV_NEGATIVE, 4,
            explanation=f"SEBI risk classification: {risk}. This fund can experience significant short-term losses during market downturns. Suitable for investors with a long time horizon (5+ years) and high risk tolerance.",
        ))
    if age is not None and age < 3:
        tagged.append(_tag(
            "New Fund", _CAT_RISK, _SEV_NEGATIVE, 4,
            explanation=f"Fund is only {age:.1f} years old with limited track record. New funds haven't been tested through different market cycles, so past performance may not be a reliable indicator of future results. Prefer funds with 5+ year histories.",
        ))
    if aum is not None and aum < 25:
        tagged.append(_tag(
            "Very Small Fund", _CAT_RISK, _SEV_NEGATIVE, 4,
            explanation=f"AUM of \u20B9{aum:.0f} Cr is very small. Small funds face liquidity risk (difficulty buying/selling large positions), higher impact costs, and risk of closure if they don't attract enough investors.",
        ))
    elif aum is not None and aum < 50:
        tagged.append(_tag(
            "Small Fund", _CAT_RISK, _SEV_NEGATIVE, 4,
            explanation=f"AUM of \u20B9{aum:.0f} Cr is on the smaller side. While not critically small, the fund may face higher impact costs and could be at risk of merger or closure if AUM doesn't grow.",
        ))
    if expense is not None and expense > 2.0:
        tagged.append(_tag(
            "High Cost", _CAT_RISK, _SEV_NEGATIVE, 4,
            explanation=f"Expense ratio of {expense:.2f}% is above 2%. High fees eat into your returns year after year. Over 10+ years, even a 1% higher expense ratio can reduce your final corpus by 10-15%. Consider if the fund's performance justifies this cost.",
        ))
    if ret1 is not None and ret1 < 0:
        tagged.append(_tag(
            "Negative Returns", _CAT_RISK, _SEV_NEGATIVE, 4,
            explanation=f"1-year return is {ret1:.1f}% (negative). The fund has lost money over the past year. While short-term losses can be normal, check if this is due to broader market conditions or fund-specific issues.",
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

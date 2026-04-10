"""Seed + embed a baseline set of educational finance concepts.

Populates artha_educational_concepts with ~40 foundational concepts so
Artha can answer meta/educational queries ("what is P/E?", "how does SIP
work?") via semantic lookup instead of burning an LLM call.

Idempotent: uses INSERT ... ON CONFLICT (slug) DO NOTHING for text, and
backfills missing embeddings on each run.
"""
from __future__ import annotations

import logging
import time
from typing import Any

import numpy as np

from app.core.database import ensure_vector_registered, get_pool

logger = logging.getLogger(__name__)


# Baseline concepts — kept short so embeddings fit in one sentence each.
# Categorised for future filter support.
_CONCEPTS: list[dict[str, str]] = [
    # --- Valuation ---
    {"slug": "pe-ratio", "category": "valuation", "title": "P/E Ratio",
     "body": "Price-to-earnings ratio compares a stock's price to its earnings per share. Lower P/E suggests cheaper valuation; higher P/E suggests growth expectations. Sector context matters — IT stocks typically trade at 25-35x, banks at 10-15x."},
    {"slug": "pb-ratio", "category": "valuation", "title": "P/B Ratio",
     "body": "Price-to-book ratio compares market price to book value per share. Below 1 suggests undervaluation relative to accounting assets; above 3 signals a premium. Useful for asset-heavy sectors like banks and metals."},
    {"slug": "peg-ratio", "category": "valuation", "title": "PEG Ratio",
     "body": "PEG divides P/E by earnings growth rate. Below 1 suggests a stock is undervalued relative to its growth; above 2 suggests it's expensive. Peter Lynch popularised PEG as a growth-adjusted valuation lens."},
    {"slug": "ev-ebitda", "category": "valuation", "title": "EV/EBITDA",
     "body": "Enterprise value divided by EBITDA. A capital-structure-neutral valuation metric useful for comparing companies with different debt levels. Lower is cheaper; 8-12x is typical for mature businesses."},
    {"slug": "dividend-yield", "category": "valuation", "title": "Dividend Yield",
     "body": "Annual dividends per share divided by stock price. A 3-5% yield is attractive for income investors but sustainability matters — check payout ratio and earnings coverage before chasing high yields."},

    # --- Profitability ---
    {"slug": "roe", "category": "profitability", "title": "Return on Equity (ROE)",
     "body": "Net profit divided by shareholders' equity. Measures how efficiently management uses shareholder capital. Above 15% is good; above 20% is excellent. Watch for ROE boosted by high leverage."},
    {"slug": "roce", "category": "profitability", "title": "Return on Capital Employed (ROCE)",
     "body": "EBIT divided by (total assets minus current liabilities). Measures how efficiently a company uses all capital including debt. Better than ROE for comparing capital-intensive businesses."},
    {"slug": "operating-margin", "category": "profitability", "title": "Operating Margin",
     "body": "Operating profit as a percentage of revenue. Shows core business profitability before interest and tax. Consistent margins across cycles indicate pricing power; shrinking margins flag competitive pressure."},
    {"slug": "net-margin", "category": "profitability", "title": "Net Margin",
     "body": "Net profit as a percentage of revenue. Final profitability after all expenses, interest, and taxes. Compare against sector peers — FMCG typically 15-20%, IT services 20-25%, banks by NIM not margin."},

    # --- Balance sheet ---
    {"slug": "debt-equity", "category": "balance-sheet", "title": "Debt-to-Equity",
     "body": "Total debt divided by shareholders' equity. Measures financial leverage. Below 0.5 is conservative; above 2 is risky for most sectors except banks and NBFCs where leverage is structural."},
    {"slug": "interest-coverage", "category": "balance-sheet", "title": "Interest Coverage Ratio",
     "body": "EBIT divided by interest expense. Shows how comfortably a company can pay interest. Above 3 is safe; below 1.5 is a red flag signalling debt servicing stress."},
    {"slug": "current-ratio", "category": "balance-sheet", "title": "Current Ratio",
     "body": "Current assets divided by current liabilities. Measures short-term liquidity. Above 1.5 is healthy; below 1 signals working capital stress. Too high (above 3) may indicate idle cash."},
    {"slug": "promoter-pledging", "category": "balance-sheet", "title": "Promoter Pledging",
     "body": "Percentage of promoter shares pledged as collateral for loans. High pledging (above 30%) is a major red flag — if stock price falls, forced selling can cascade."},

    # --- Technical ---
    {"slug": "rsi", "category": "technical", "title": "RSI (Relative Strength Index)",
     "body": "Momentum oscillator ranging 0-100. Above 70 suggests overbought conditions; below 30 suggests oversold. Works best in ranging markets, less reliable in strong trends."},
    {"slug": "macd", "category": "technical", "title": "MACD",
     "body": "Moving Average Convergence Divergence. Tracks the gap between 12-day and 26-day exponential moving averages, with a 9-day signal line. Crossovers signal momentum shifts."},
    {"slug": "moving-average", "category": "technical", "title": "Moving Averages",
     "body": "Running average of closing prices over N days. 50-DMA is a short-term trend indicator; 200-DMA is the long-term trend. Price above both is bullish; golden cross (50 crosses 200 up) is a classic buy signal."},
    {"slug": "support-resistance", "category": "technical", "title": "Support and Resistance",
     "body": "Support is a price level where buying tends to emerge; resistance is where selling tends to emerge. Previous highs and lows, round numbers, and moving averages often act as key levels."},

    # --- Mutual funds ---
    {"slug": "sip", "category": "mutual-funds", "title": "SIP (Systematic Investment Plan)",
     "body": "Automated monthly investment in a mutual fund. Averages your purchase price over time (rupee-cost averaging) and removes market-timing stress. Ideal for long-term equity exposure."},
    {"slug": "nav", "category": "mutual-funds", "title": "NAV (Net Asset Value)",
     "body": "Per-unit value of a mutual fund scheme, computed daily as (assets minus liabilities) divided by units outstanding. A lower NAV does not mean a cheaper fund — compare returns, not NAV."},
    {"slug": "expense-ratio", "category": "mutual-funds", "title": "Expense Ratio",
     "body": "Annual fee charged by a mutual fund as a percentage of AUM. Direct plans have lower ratios (0.5-1.2%) than regular plans (1.5-2.5%). Over 20 years, a 1% difference can cost you 20% of your corpus."},
    {"slug": "elss", "category": "mutual-funds", "title": "ELSS",
     "body": "Equity Linked Savings Scheme — tax-saving mutual funds with a 3-year lock-in. Eligible for 80C deduction up to 1.5 lakh. Among the shortest lock-ins of any 80C option, historically good returns."},
    {"slug": "sharpe-ratio", "category": "mutual-funds", "title": "Sharpe Ratio",
     "body": "Risk-adjusted return metric: (fund return minus risk-free rate) divided by volatility. Above 1 is good, above 2 is excellent. Compares funds with different risk profiles on a level field."},

    # --- Tax ---
    {"slug": "ltcg-equity", "category": "tax", "title": "LTCG on Equity",
     "body": "Long-term capital gains on equity held over 12 months are taxed at 10% above 1 lakh per year (without indexation). Below 1 lakh annual gains are exempt."},
    {"slug": "stcg-equity", "category": "tax", "title": "STCG on Equity",
     "body": "Short-term capital gains on equity held under 12 months are taxed at 15% regardless of your income slab. No 1 lakh exemption applies to STCG."},
    {"slug": "80c", "category": "tax", "title": "Section 80C",
     "body": "Allows deduction up to 1.5 lakh from taxable income for investments in ELSS, PPF, EPF, NSC, life insurance, and home loan principal. Only available under the old tax regime."},
    {"slug": "old-vs-new-regime", "category": "tax", "title": "Old vs New Tax Regime",
     "body": "New regime has lower rates but no deductions (except standard deduction). Old regime allows 80C, HRA, home loan interest, etc. Choose new regime if you have few deductions; old if you use 80C + HRA fully."},

    # --- Macro ---
    {"slug": "repo-rate", "category": "macro", "title": "Repo Rate",
     "body": "The rate at which RBI lends to commercial banks. Higher repo slows credit growth and tames inflation; lower repo stimulates lending. RBI's primary monetary policy lever, revised every two months."},
    {"slug": "inflation-cpi", "category": "macro", "title": "CPI Inflation",
     "body": "Consumer Price Index tracks price changes in a household basket (food, fuel, housing, etc.). RBI targets 4% CPI with a 2-6% tolerance band. Above 6% triggers monetary tightening."},
    {"slug": "gdp-growth", "category": "macro", "title": "GDP Growth",
     "body": "Year-over-year growth in gross domestic product. India's long-term potential is around 6-7%. Growth above potential fuels inflation; below potential means slack in the economy."},
    {"slug": "fii-dii", "category": "macro", "title": "FII and DII Flows",
     "body": "FIIs (Foreign Institutional Investors) and DIIs (Domestic Institutional Investors) track large pools of equity capital. Sustained FII selling pressures markets; DII buying often absorbs it."},

    # --- Behavioural ---
    {"slug": "diversification", "category": "behavioural", "title": "Diversification",
     "body": "Spreading investments across sectors, asset classes, and geographies to reduce risk without sacrificing much expected return. The only free lunch in investing, per Harry Markowitz."},
    {"slug": "rebalancing", "category": "behavioural", "title": "Portfolio Rebalancing",
     "body": "Periodically selling winners and buying laggards to maintain your target asset allocation. Forces you to buy low and sell high. Do it annually or when any allocation drifts 5%+ from target."},
    {"slug": "emergency-fund", "category": "behavioural", "title": "Emergency Fund",
     "body": "6-12 months of living expenses kept in liquid instruments (savings account, liquid mutual funds). Your first financial priority — protects you from having to liquidate investments at bad times."},

    # --- Market structure ---
    {"slug": "market-cap", "category": "market-structure", "title": "Market Capitalisation",
     "body": "Share price multiplied by outstanding shares. SEBI classifies: Large-cap (top 100), Mid-cap (101-250), Small-cap (251+). Large-caps are stable; small-caps have higher return and risk."},
    {"slug": "circuit-limits", "category": "market-structure", "title": "Circuit Limits",
     "body": "Price bands that halt trading when a stock or index moves beyond a preset percentage (5/10/20%). Upper/lower circuit prevents panic cascades and protects retail investors."},
    {"slug": "bulk-block-deals", "category": "market-structure", "title": "Bulk and Block Deals",
     "body": "Bulk deals are trades above 0.5% of outstanding shares. Block deals are large pre-negotiated trades on a separate window. Both are disclosed daily — watch for institutional accumulation or exit."},

    # --- Strategy ---
    {"slug": "value-investing", "category": "strategy", "title": "Value Investing",
     "body": "Buying stocks trading below their intrinsic value. Popularised by Benjamin Graham and Warren Buffett. Look for low P/E, low P/B, strong balance sheets, and durable competitive advantages."},
    {"slug": "growth-investing", "category": "strategy", "title": "Growth Investing",
     "body": "Buying companies with above-average earnings growth, even at premium valuations. Focuses on revenue scale, TAM, and reinvestment rates. Pays off when growth compounds; hurts when growth disappoints."},
    {"slug": "momentum-investing", "category": "strategy", "title": "Momentum Investing",
     "body": "Buying stocks that have outperformed recently, on the thesis that trends persist. Works best in bull markets; can reverse sharply in corrections. Use with stop-losses."},
    {"slug": "dollar-cost-averaging", "category": "strategy", "title": "Rupee-Cost Averaging",
     "body": "Investing a fixed amount at regular intervals regardless of price. Buys more units when cheap, fewer when expensive. Removes timing pressure and works especially well in volatile markets."},
]


async def run_educational_concepts_seed_job() -> dict[str, Any]:
    start = time.monotonic()
    logger.info("edu_concepts_seed: START n_concepts=%d", len(_CONCEPTS))

    pool = await get_pool()

    try:
        from app.services.embedding_service import embed_texts, warmup
    except ImportError as e:
        logger.error("edu_concepts_seed: embedding service unavailable: %s", e)
        return {"status": "error", "reason": "embedding_service_unavailable"}

    if not await warmup():
        return {"status": "error", "reason": "embedding_model_load_failed"}

    inserted = 0
    updated = 0
    skipped = 0

    async with pool.acquire() as conn:
        if not await ensure_vector_registered(conn):
            return {"status": "error", "reason": "pgvector_not_available"}

        # Step 1: Upsert text rows (no embedding yet — ON CONFLICT ignores).
        for c in _CONCEPTS:
            try:
                result = await conn.execute(
                    """
                    INSERT INTO artha_educational_concepts
                        (slug, title, body, category)
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT (slug) DO NOTHING
                    """,
                    c["slug"], c["title"], c["body"], c["category"],
                )
                if result and result.endswith("1"):
                    inserted += 1
            except Exception as e:
                logger.warning(
                    "edu_concepts_seed: insert failed slug=%s err=%s",
                    c["slug"], str(e)[:100],
                )
                skipped += 1

        # Step 2: Embed any rows missing embedding (covers both fresh inserts
        # and any legacy rows that got inserted without vectors).
        rows = await conn.fetch(
            "SELECT id, title, body FROM artha_educational_concepts "
            "WHERE embedding IS NULL ORDER BY id"
        )
        if rows:
            texts = [f"{r['title']}: {r['body']}" for r in rows]
            vectors = await embed_texts(texts)
            for r, vec in zip(rows, vectors):
                if not vec:
                    skipped += 1
                    continue
                try:
                    await conn.execute(
                        "UPDATE artha_educational_concepts "
                        "SET embedding = $1, updated_at = NOW() WHERE id = $2",
                        np.array(vec, dtype=np.float32),
                        r["id"],
                    )
                    updated += 1
                except Exception as e:
                    logger.warning(
                        "edu_concepts_seed: update failed id=%s err=%s",
                        r["id"], str(e)[:100],
                    )
                    skipped += 1

    elapsed = time.monotonic() - start
    summary = {
        "status": "ok",
        "inserted": inserted,
        "embedded": updated,
        "skipped": skipped,
        "total_concepts": len(_CONCEPTS),
        "elapsed_seconds": round(elapsed, 2),
    }
    logger.info(
        "edu_concepts_seed: COMPLETE inserted=%d embedded=%d skipped=%d elapsed=%.2fs",
        inserted, updated, skipped, elapsed,
    )
    return summary

"""Artha AI chat service — orchestrates LLM, tools, sessions, and streaming.

Core flow:
1. User sends message → rate-limit check → save to DB
2. Build context (system prompt + last N messages + tool instructions)
3. Call LLM → parse tool markers → execute tools → re-call LLM with results
4. Stream response tokens via SSE
5. Save assistant message with stock/MF cards to DB
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, AsyncGenerator

import httpx

from app.core.database import get_pool, record_to_dict
from app.services.ai_service import _get_api_key, _MODELS, _OPENROUTER_BASE, _clean_response

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
MAX_CONTEXT_MESSAGES = 15  # Send last N messages for multi-turn context
MAX_MESSAGES_PER_DAY = 100
MAX_SESSIONS_PER_DEVICE = 50
STREAM_TIMEOUT = 60.0  # 60s ceiling per LLM call (was 30s — too tight for tool chains)
HEARTBEAT_INTERVAL = 5.0  # Send a "thinking" keepalive every 5s during long calls
MAX_TOKENS_CHAT = 1500  # Generous ceiling so long analyses don't truncate mid-answer

# Two model chains: FAST is the default for almost everything, SLOW is
# only invoked when the query needs deep analysis or multi-step
# reasoning (per user directive: "use faster models mostly, only use
# slower model when require deep thinking").
#
# Fast chain: 20B model runs in ~2-3s per call, handles intent
# detection, tool dispatch, simple Q&A, and most final-answer
# composition for one-shot queries. Falls through to larger models
# only if the small one fails or rate-limits.
#
# Slow chain: 120B model runs in ~8-12s per call, reserved for:
#   - multi-stock comparisons (3+ stocks)
#   - explain-style questions ("explain why…", "analyse the impact…")
#   - long user messages (>200 chars) signalling a nuanced ask
#   - responses that depend on >1 tool result
#
# The _is_deep_thinking_needed() helper classifies queries at phase 3
# dispatch time — see the stream_chat_response() call site.
_FAST_MODELS = [
    "openai/gpt-oss-20b:free",
    "google/gemma-4-31b-it:free",
    "openai/gpt-oss-120b:free",  # fallback if the smaller ones fail
]
_SLOW_MODELS = [
    "openai/gpt-oss-120b:free",
    "nvidia/nemotron-3-super-120b-a12b:free",
    "openai/gpt-oss-20b:free",  # fallback if the big ones fail
]

# ──────────────────────────────────────────────────────────────────
# Deep-thinking detection for smart model routing
# ──────────────────────────────────────────────────────────────────
#
# STRATEGY: a trigger fires if EITHER of two matchers hit:
#   1. Exact phrase contains (for multi-word triggers like "what would happen")
#   2. Word-level prefix/fuzzy match (for single words with typo tolerance)
#
# The prefix matcher catches typos automatically:
#   "anal"     matches  analyse, analyze, analysis, analytical
#   "compar"   matches  compare, comparison, comparative, comparing
#   "valuat"   matches  valuation, valuate, evaluate, evaluator
#
# difflib adds Levenshtein-like fuzzy match for anything that doesn't
# hit by prefix (catches "analze", "analys", "copmare", "straetgy").

# Single-word roots — any word starting with these prefixes counts.
# Typos that preserve the prefix are handled for free. Typos that
# mangle the prefix are caught by the fuzzy matcher below.
_DEEP_WORD_PREFIXES = frozenset({
    # Analysis / reasoning
    "analy",      # analyse, analyze, analysis, analytical, analyst
    "evaluat",    # evaluate, evaluation, evaluating
    "assess",     # assess, assessment, assessing
    "review",     # review, reviewing, reviewer
    "breakdown",  # breakdown
    "dissect",    # dissect, dissecting
    "interpret",  # interpret, interpretation, interpreting

    # Comparison
    "compar",     # compare, comparison, comparative, comparing
    "contrast",   # contrast, contrasting
    "differ",     # differ, difference, differentiate
    "versus", "vs",

    # Valuation / financial reasoning
    "valuat",     # valuation, valuate
    "price-t",    # price-to-earnings, price-to-book
    "intrinsic",  # intrinsic value
    "discount",   # discounted cash flow, DCF
    "forecast",   # forecast, forecasting
    "project",    # projection, projected

    # Strategic / planning
    "strateg",    # strategy, strategic, strategize
    "recomm",     # recommend, recommendation
    "allocat",    # allocate, allocation
    "diversif",   # diversify, diversification
    "rebalanc",   # rebalance, rebalancing
    "hedg",       # hedge, hedging

    # Explanation
    "explain",    # explain, explanation, explaining
    "elaborat",   # elaborate, elaborating
    "clarif",     # clarify, clarification
    "reason",     # reason, reasoning

    # Opinion / judgment
    "opinion",
    "thought",    # thoughts, thinking
    "perspective",
    "outlook",
    "thesis",
    "argument",

    # Risk / trade-off
    "risk",
    "tradeoff", "trade-off",
    "upside",
    "downside",
    "advantage",
    "disadvantage",
    "pros",
    "cons",

    # Deep horizon signals
    "long-term", "longterm",
    "short-term",
    "medium-term",
    "fundamental",   # fundamental analysis
    "technical",     # technical analysis
    "historical",
    "retrospect",
    "backtest",

    # Decision framing
    "should", "would", "could",
    "worth",       # worth buying
    "bullish", "bearish",
    "overvalued", "undervalued",
    "oversold", "overbought",
    "correction", "rally",

    # Macro / thematic
    "inflat",      # inflation, inflationary
    "recess",      # recession, recessionary
    "sentiment",
    "catalyst",
    "headwind",
    "tailwind",
    "thematic", "theme",
})

# Multi-word phrases — must appear as a contiguous substring.
_DEEP_PHRASES = (
    "what would happen", "what if", "how would", "how will",
    "why is", "why are", "why did", "why would",
    "pros and cons",
    "in depth", "deep dive", "deep into",
    "explain why", "explain how", "explain what",
    "help me decide", "help me understand", "help me pick",
    "give me your take", "your opinion",
    "is it worth", "is this worth",
    "compare", "comparison between",
    "risks involved", "risk factors",
    "best time to", "right time to",
    "long run", "long term", "long-term",
    "over the next",
    "5 year", "10 year",
    "which is better", "which one is",
    "buy or sell", "hold or sell", "buy the dip",
)

# Difflib fuzzy match: anything not caught by prefix falls through to
# difflib.get_close_matches() against this canonical set. Catches
# badly-mangled typos like "analze", "straetgy", "compair".
_DEEP_CANONICAL_TOKENS = (
    "analyse", "analyze", "analysis", "analytical",
    "compare", "comparison", "contrast",
    "evaluate", "evaluation", "assess", "assessment", "review",
    "valuation", "intrinsic", "discounted", "forecast", "projection",
    "strategy", "strategic", "recommend", "allocation", "diversify",
    "rebalance", "hedge",
    "explain", "explanation", "elaborate", "clarify", "reason",
    "opinion", "thoughts", "perspective", "outlook", "thesis",
    "risk", "tradeoff", "upside", "downside", "advantage", "disadvantage",
    "longterm", "shortterm", "fundamental", "technical", "historical",
    "bullish", "bearish", "overvalued", "undervalued",
    "correction", "rally", "sentiment", "catalyst",
    "inflation", "recession", "headwind", "tailwind", "thematic",
    "dissect", "interpret", "breakdown",
)

# Regex that finds word-like tokens for fuzzy matching.
_DEEP_TOKEN_PATTERN = re.compile(r"[a-z][a-z'\-]{2,}")


def _is_deep_thinking_needed(
    user_message: str,
    tool_results: dict[str, Any],
) -> bool:
    """Return True if the final answer needs the slow / high-quality model.

    Signals (any one is enough):
      1. Word prefix match (handles most typos automatically)
      2. Multi-word phrase match
      3. Fuzzy (difflib) match on tokens that missed prefix/phrase
      4. User message is long (> 200 chars)
      5. Multiple tools invoked (indicates multi-part query)
      6. Screen/compare tool returned >= 3 rows (needs synthesis)
    """
    text = (user_message or "").lower().strip()
    if not text:
        return False

    # (2) Multi-word phrases first (cheap early exit)
    for phrase in _DEEP_PHRASES:
        if phrase in text:
            return True

    # (1) Word-prefix match — iterate tokens, check each against prefixes
    tokens = _DEEP_TOKEN_PATTERN.findall(text)
    for tok in tokens:
        for prefix in _DEEP_WORD_PREFIXES:
            if tok.startswith(prefix):
                return True

    # (3) Fuzzy match on leftover tokens using difflib. This catches
    # typos that mangle the prefix beyond recognition.
    # Only check tokens >= 5 chars to avoid noise on short words.
    import difflib
    long_tokens = [t for t in tokens if len(t) >= 5]
    if long_tokens:
        for tok in long_tokens:
            # cutoff=0.82 = ~1-2 char edit distance on a 6-8 char word
            matches = difflib.get_close_matches(
                tok, _DEEP_CANONICAL_TOKENS, n=1, cutoff=0.82,
            )
            if matches:
                return True

    # (4) Long user query
    if len(text) > 200:
        return True

    # (5) Multiple tools invoked
    if len(tool_results) >= 2:
        return True

    # (6) Screen/compare returned several rows
    for name, result in tool_results.items():
        if not isinstance(result, dict):
            continue
        if name in ("stock_screen", "stock_compare"):
            stocks = result.get("stocks") or []
            if len(stocks) >= 3:
                return True
        if name in ("mf_screen",):
            funds = result.get("funds") or []
            if len(funds) >= 3:
                return True

    return False

# ---------------------------------------------------------------------------
# System prompt — the brain of Artha
# ---------------------------------------------------------------------------
_ARTHA_SYSTEM = """You are **Artha** (अर्थ), an AI market analyst built into the EconAtlas app — an Indian finance app for retail investors.

The blocks ABOVE this one (LIVE MARKET SNAPSHOT, USER CONTEXT) are refreshed on every request — always prefer those over calling a tool for the same data.

## Your tools
Emit a tool call inline as: `[TOOL:tool_name:{"param":"value"}]`. Multiple markers per response are allowed. Use tools ONLY when the answer needs data not already in the LIVE MARKET SNAPSHOT block above.

Available tools:
- [TOOL:stock_lookup:{"symbol":"TCS"}] — Full details for one stock: price, valuation (PE/PB), ROE/ROCE, growth, margins, holdings, score, 52w range.
- [TOOL:stock_screen:{"query":"sector = 'Information Technology' AND roe > 20", "limit":5}] — Filter stocks. Valid columns: symbol, display_name, sector, industry, last_price, percent_change, pe_ratio, price_to_book, roe, roce, debt_to_equity, market_cap, dividend_yield, score, revenue_growth, earnings_growth, operating_margins, profit_margins, beta, free_cash_flow, promoter_holding, fii_holding, dii_holding, compounded_sales_growth_3y, compounded_profit_growth_3y. Supports =, <, >, LIKE.
- [TOOL:stock_compare:{"symbols":["TCS","INFY","WIPRO"]}] — Side-by-side comparison (max 3 symbols).
- [TOOL:mf_lookup:{"scheme_code":"119551"}] — Mutual fund details.
- [TOOL:mf_screen:{"query":"category = 'Equity' AND returns_1y > 15", "limit":5}] — Filter mutual funds. Valid columns: scheme_code, scheme_name, category, sub_category, nav, expense_ratio, aum_cr, returns_1y, returns_3y, returns_5y, sharpe, sortino, score, risk_level.
- [TOOL:watchlist:{}] — User's saved stocks with live data. Use this when the user says "my stocks", "my watchlist", "my portfolio".
- [TOOL:market_status:{}] — All indices (India/US/Europe/Japan) + FX + key commodities + market hours. Only call this if the LIVE MARKET SNAPSHOT above is stale or missing what you need.
- [TOOL:stock_price_history:{"symbol":"TCS","period":"3mo"}] — Historical closes. Periods: 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y.
- [TOOL:sector_performance:{"sector":"Information Technology"}] — Per-sector detail OR all-sectors overview (omit the sector param).
- [TOOL:ipo_list:{"status":"open"}] — List IPOs (open/upcoming/closed).
- [TOOL:news:{"entity":"Reliance","since":"24h"}] — Latest news. Optional since: 6h, 12h, 24h, 48h, 2d, 3d, week, month (default 48h).
- [TOOL:macro:{"indicator":"gdp_growth"}] — Macro indicators (gdp_growth, inflation_cpi, repo_rate, usd_inr, fiscal_deficit, current_account, iip_growth).
- [TOOL:commodity:{}] — All commodity prices (only if not in LIVE SNAPSHOT).
- [TOOL:crypto:{}] — Crypto prices (BTC, ETH, etc.).
- [TOOL:tax:{"type":"ltcg","profit":500000}] — Real tax math. Types: ltcg (12.5% above ₹1.25L), stcg (20% flat), income_tax (new regime slabs).
- [TOOL:educational:{"concept":"pe_ratio"}] — Explainer. Concepts: pe_ratio, pb_ratio, roe, roce, ebitda, dcf, ltcg, stcg, debt_to_equity, dividend_yield.

## CORE RULES (non-negotiable)

### 1. MANDATORY — wrap reasoning in `<thinking>...</thinking>` tags
Every non-trivial response MUST begin with a `<thinking>` block that explains your plan in 2-4 sentences: which tools you'll call, what data you need, how you'll structure the answer. The tags are required — they are parsed by the client and rendered separately from the actual answer.

Format:
```
<thinking>
I'll fetch TCS fundamentals via stock_lookup, then present price,
valuation, and profitability in a bullet list with a stock card.
</thinking>

(Actual answer goes here, OUTSIDE the thinking tags.)
```

Rules for the thinking block:
- Always wrap reasoning in `<thinking>...</thinking>` — NEVER use `### Thinking` headings or other markers.
- The block must be the FIRST thing in your response.
- Keep it 2-4 sentences max.
- Skip it only for one-line greetings ("hi", "hello", "what's up").
- The actual answer starts AFTER `</thinking>` on a new line.

### 2. CITATION — specific numbers MUST come from tools or the LIVE SNAPSHOT
Every specific price, percentage, ratio, metric, or historical data point in your answer MUST come from either:
  (a) a value visible in the LIVE MARKET SNAPSHOT block above, or
  (b) a tool result you received in the current turn.

NEVER invent numbers. NEVER fall back to training-data knowledge for specific values. NEVER make claims about historical trends ("has been hovering near X for weeks") without a `stock_price_history` or `macro` tool call.

QUALITATIVE analysis (verdicts, opinions, risk commentary, pros & cons, drawbacks, recommendations) is allowed and encouraged when based on the fundamentals you already have. The citation rule applies to SPECIFIC NUMBERS only, not to reasoning.

### 3. TRY TOOLS FIRST before saying "I don't have data"
When the user asks about a stock, fund, concept, or topic that might have tool coverage, CALL THE TOOL FIRST. Don't refuse with "I don't have that data" until you've actually tried. Examples:
- "Compare WEBELSOLAR with another solar stock" → call `stock_lookup({"symbol":"WEBELSOLAR"})` + `stock_screen({"query":"sector LIKE '%Solar%'"})` FIRST. Only say "no data" if those actually return empty.
- "What are the drawbacks of my picks?" → analyze the risks from the data you already have. Don't refuse — write the analysis.
- "What's the news on X?" → call `news({"entity":"X"})` first. Only refuse if it returns zero rows.

ONLY use the "I don't have that data right now. Want me to try fetching it?" phrase when:
- You actually called a tool and it returned empty, OR
- No tool exists for the asked-about data (e.g. "what's my bank account balance?"), OR
- The data is genuinely outside the Indian market domain.

### 4. UNKNOWN HANDLING — say it, don't fake it
When you genuinely don't have data (after trying the right tool), respond with:
> "I don't have that data right now. Want me to try fetching it?"

Never say "approximately ₹X" or "around Y%" or "roughly 30 days" without a tool source. Never invent company names like "XYZ Tech" or "Company A". Never invent business descriptions ("FIRSTCRY is a kids-education platform") — use only the `display_name` from tool results. If you don't know what a company does, just say its name.

### 5. CURRENCY — match the asset, don't force ₹ everywhere
- **Indian stock PRICES and market cap** → `₹` with Indian units:
  - Prices: `₹3,450.25` (2 decimals)
  - Market cap: `₹12,345 Cr`, `₹1.2 L Cr`, `₹45 L`
  - Never `Rs`, `INR`, `rupees`, `million`, `billion`, `$`
- **Indian INDEX VALUES** (Nifty 50, Sensex, Nifty Bank, etc.) → **no currency symbol, just the number** (indices are points, not money):
  - ✅ `Nifty 50 at 24,004.25 (+0.96%)`
  - ❌ `Nifty 50 at ₹24,004.25` (wrong — indices aren't money)
- **Global INDEX VALUES** (S&P 500, Nasdaq, Dow Jones, Nikkei 225, FTSE 100, DAX) → **no currency symbol either, just the number**:
  - ✅ `S&P 500 at 6,824.66 (+0.62%)`
  - ✅ `Nasdaq at 22,822.42 (+0.83%)`
  - ✅ `Nikkei 225 at 56,924.11 (+1.84%)`
  - ❌ `S&P 500 at ₹6,824.66` (wrong — global indices are points, not rupees)
  - ❌ `Nasdaq at $22,822.42` (wrong — index values don't use currency)
- **Global commodities** → USD with `$`:
  - Gold: `$4,776.90/oz` (per troy ounce), NOT `₹4,776.90 per 10g`
  - Silver: `$28.50/oz`
  - Crude / Brent: `$98.69/bbl`
  - Natural gas: `$2.67/MMBtu`
- **Cryptocurrencies** → USD with `$` (`$67,450`)
- **FX pairs** → quote currency as-is (`USD/INR at ₹92.64`, `EUR/USD at $1.08`)
- **Conversions** → exact math. `$100 at ₹92.64 = ₹9,264` (not `₹9,200`).
- **Percentages** → 2 decimals with explicit sign. Positive always gets `+` prefix: `+1.23%`, `-0.45%`.

### 6. OUTPUT FORMAT — match the query type
- **Single stock / MF lookup** → bullet list with **bold** key numbers + a [CARD:SYMBOL] marker. 3-6 bullets max.
- **Comparison (2-3 stocks or funds)** → markdown table with rows per metric + [CARD:] markers + a 1-line verdict.
- **Screener results** → bullet list of top 5 + mention total count if more.
- **Macro / educational** → short prose (2-3 sentences) + 1-2 key bullets.
- **Market status / gainers / losers** → bullet list with bold numbers.
- **"What should I buy" / recommendations** → bulleted picks with 1-line rationale each + a "Takeaway" line.
- **Drawbacks / pros & cons / risk analysis** → bulleted list of specific concerns pulled from the stock's fundamentals you already have.
- **Greetings / meta questions** → 1-2 sentences, no bullets, no Thinking section.
- NEVER nest bullets deeper than 2 levels. NEVER use headings (##) inside a chat response — only `### Thinking` at the top.

### 7. CARDS & TOOL MARKERS
- Emit `[CARD:SYMBOL]` for every stock / MF you discuss by name. Max 5 cards per response.
- Tool markers `[TOOL:...]` are ONLY for the first pass. On the composition pass (after tool results are returned) you must NOT output any `[TOOL:...]` markers — just the final answer.

### 8. OPINIONATED BUT HONEST — label predictions as opinions
Be direct: say if a stock looks strong, overvalued, or risky based on the numbers. But when you make a FORWARD PREDICTION (e.g. "upside of 5-10% in the next 2-3 months"), prefix it with "**Opinion:**" or "**My view:**" so the user knows it's a subjective take, not a tool output.

Don't hedge with "consult a financial advisor". Base every opinion on the numbers you actually have — not on vibes.

### 9. LANGUAGE
English by default. Match Hinglish (Hindi in Latin script) if the user writes that way. Never Devanagari unless the user writes in Devanagari first.

### 10. NO DISCLAIMERS
Do not add "NFA", "do your own research", "consult a financial advisor", "past performance is not indicative of future results" unless the user specifically asks. These waste the user's time.

## FEW-SHOT EXAMPLES

### Example 1 — Single stock lookup
USER: What's the status of TCS?
ASSISTANT:
<thinking>
I'll pull TCS fundamentals with stock_lookup and present price,
valuation, and profitability in a quick-scan bullet list with the
stock card.
</thinking>

[TOOL:stock_lookup:{"symbol":"TCS"}]

(After tool returns)
**TCS** [CARD:TCS] is trading at **₹3,450.25 (+0.87%)** today.
- **Market Cap:** ₹12.5 L Cr (large-cap leader)
- **Valuation:** PE **28.5×**, PB **12.1×** — premium to IT average
- **Profitability:** ROE **52.4%**, operating margin **27.8%** — top tier
- **Debt:** D/E **0.1** — effectively debt-free
- **Score:** **78/100**

*Verdict:* Strong fundamentals, richly valued. Growth is the swing factor.

### Example 2 — Screener with index reference
USER: Top 5 IT stocks by ROE. How is Nifty IT doing?
ASSISTANT:
<thinking>
I'll screen IT stocks with ROE > 20 sorted by score. The Nifty IT
reading comes straight from the LIVE SNAPSHOT — no tool needed for it.
</thinking>

[TOOL:stock_screen:{"query":"sector = 'Information Technology' AND roe > 20","limit":5}]

(After tool returns)
**Top 5 IT stocks by ROE:**
- **TCS** [CARD:TCS] — ROE **52.4%**, PE 28.5×, ₹3,450.25
- **Infosys** [CARD:INFY] — ROE **31.2%**, PE 25.1×, ₹1,620.00
- **HCL Technologies** [CARD:HCLTECH] — ROE **26.8%**, PE 22.3×, ₹1,480.00
- **Wipro** [CARD:WIPRO] — ROE **21.5%**, PE 19.8×, ₹485.50
- **LTI Mindtree** [CARD:LTIM] — ROE **21.1%**, PE 30.2×, ₹5,120.00

**Nifty IT** is at **30,902.25 (-2.32%)** today — the sector is lagging.
(Note: no ₹ prefix — indices are points, not currency.)

*Verdict:* TCS dominates on capital efficiency. Infosys offers the best balance of ROE and valuation at a weak-sector moment.

### Example 3 — Global indices + commodities
USER: What are US markets and gold doing?
ASSISTANT:
<thinking>
Both are in the LIVE SNAPSHOT above. I'll quote the US index points
(no $ prefix, indices are points) and gold in USD per ounce.
</thinking>

**US Markets:**
- S&P 500: **6,824.66 (+0.62%)**
- Nasdaq: **22,822.42 (+0.83%)**
- Dow Jones: **48,185.80 (+0.58%)**

**Gold:** **$4,776.90/oz** (−0.10%) — range-bound near recent highs.

## FOLLOW-UP SUGGESTIONS (append at end of EVERY response)
At the very end of every response, append exactly 5 follow-up suggestions in this format:
[SUGGESTIONS]
- Follow-up question 1
- Follow-up question 2
- Follow-up question 3
- Follow-up question 4
- Follow-up question 5
[/SUGGESTIONS]

Rules for suggestions:
- MAXIMUM 12 WORDS each. Count them.
- First-person — what the user would type, not what they should ask.
- Specific — reference real names/numbers from the current response.
- No instructional verbs ("Ask for", "Check", "Get", "Inquire about", "See", "Show me how").
- Direct questions or commands only.

Good: "Compare TCS fundamentals with Infosys and HCL Tech"
Good: "How has TCS performed over the last 3 years?"
Good: "Is TCS trading above or below fair value?"
Bad: "Ask for more details about TCS" (instructional)
Bad: "Compare" (too terse)
"""

# ---------------------------------------------------------------------------
# Tool definitions for parsing and execution
# ---------------------------------------------------------------------------
_TOOL_PATTERN = re.compile(r'\[TOOL:(\w+):(.*?)\]', re.DOTALL)
_CARD_PATTERN = re.compile(r'\[CARD:(\S+)\]')
# Tolerant match: accepts either an explicit [/SUGGESTIONS] close tag
# OR just end-of-string. Many models emit the opening tag reliably but
# forget the closing tag, so we parse everything from [SUGGESTIONS]
# to the end of the message when no close tag is present.
_SUGGESTIONS_PATTERN = re.compile(
    r'\[SUGGESTIONS\](.*?)(?:\[/SUGGESTIONS\]|\Z)',
    re.DOTALL,
)


async def _news_hybrid_search(
    pool,
    query: str,
    limit: int = 5,
    since_interval: str = "7 days",
) -> tuple[list[dict], str]:
    """Hybrid news search: vector semantic → trigram fuzzy → ILIKE fallback.

    Returns a tuple ``(rows, search_mode)`` where ``search_mode`` is one of
    ``"vector"``, ``"trigram"``, ``"ilike"``, or ``"none"``.

    Each strategy is tried in order; the first one that returns ≥1 result
    wins. Strategies further down the list are used as fallbacks if the
    extension / embeddings aren't available or the higher-quality search
    returns nothing. Every strategy decision is logged at INFO so we can
    diagnose "which search path ran for this query?" from the log tail.
    """
    start_total = time.monotonic()
    logger.info("news_search: query=%r limit=%d", query, limit)

    # --- Strategy 1: Vector semantic search via pgvector -----------------
    try:
        from app.services.embedding_service import embed_text
        from app.core.database import ensure_vector_registered

        t0 = time.monotonic()
        query_vec = await embed_text(query)
        embed_ms = (time.monotonic() - t0) * 1000
        if query_vec:
            logger.debug(
                "news_search: query embedded dim=%d in %.1fms",
                len(query_vec), embed_ms,
            )
            async with pool.acquire() as conn:
                registered = await ensure_vector_registered(conn)
                if not registered:
                    logger.info(
                        "news_search: pgvector type not registered — skipping "
                        "vector path (extension missing?)"
                    )
                else:
                    # Check embedding column is populated for at least some rows.
                    has_embeddings = await conn.fetchval(
                        "SELECT EXISTS(SELECT 1 FROM news_articles "
                        "WHERE embedding IS NOT NULL LIMIT 1)"
                    )
                    if not has_embeddings:
                        logger.info(
                            "news_search: no articles have embeddings yet — "
                            "run POST /ops/jobs/trigger/news_embed to backfill"
                        )
                    else:
                        import numpy as np
                        vec = np.array(query_vec, dtype=np.float32)
                        t1 = time.monotonic()
                        rows = await conn.fetch(
                            "SELECT title, summary, source, timestamp, url, "
                            "primary_entity, impact, "
                            "(embedding <=> $1) AS distance "
                            "FROM news_articles "
                            "WHERE embedding IS NOT NULL "
                            f"  AND timestamp > NOW() - INTERVAL '{since_interval}' "
                            "  AND (embedding <=> $1) < 0.6 "
                            "ORDER BY embedding <=> $1 "
                            "LIMIT $2",
                            vec,
                            limit,
                        )
                        sql_ms = (time.monotonic() - t1) * 1000
                        if rows:
                            best = float(rows[0]["distance"]) if rows[0].get("distance") is not None else None
                            worst = float(rows[-1]["distance"]) if rows[-1].get("distance") is not None else None
                            total_ms = (time.monotonic() - start_total) * 1000
                            logger.info(
                                "news_search: VECTOR hit — %d rows, "
                                "best_dist=%.3f worst_dist=%.3f embed_ms=%.1f "
                                "sql_ms=%.1f total_ms=%.1f",
                                len(rows), best or 0, worst or 0,
                                embed_ms, sql_ms, total_ms,
                            )
                            return [dict(r) for r in rows], "vector"
                        else:
                            logger.info(
                                "news_search: vector path found 0 rows above "
                                "similarity threshold (distance<0.6) — falling "
                                "back to trigram"
                            )
        else:
            logger.warning(
                "news_search: embedding failed for query=%r — skipping vector path",
                query,
            )
    except Exception as e:
        logger.warning(
            "news_search: vector path raised, falling back: %s", e, exc_info=True,
        )

    # --- Strategy 2: Trigram fuzzy match via pg_trgm ---------------------
    try:
        t0 = time.monotonic()
        rows = await pool.fetch(
            "SELECT title, summary, source, timestamp, url, primary_entity, impact, "
            "GREATEST(similarity(COALESCE(title,''), $1), "
            "         similarity(COALESCE(summary,''), $1)) AS score "
            "FROM news_articles "
            f"WHERE timestamp > NOW() - INTERVAL '{since_interval}' "
            "  AND (COALESCE(title,'') % $1 OR COALESCE(summary,'') % $1) "
            "ORDER BY score DESC, timestamp DESC "
            "LIMIT $2",
            query,
            limit,
            timeout=5,
        )
        sql_ms = (time.monotonic() - t0) * 1000
        if rows:
            best = float(rows[0]["score"]) if rows[0].get("score") is not None else None
            total_ms = (time.monotonic() - start_total) * 1000
            logger.info(
                "news_search: TRIGRAM hit — %d rows, best_score=%.3f "
                "sql_ms=%.1f total_ms=%.1f",
                len(rows), best or 0, sql_ms, total_ms,
            )
            return [dict(r) for r in rows], "trigram"
        else:
            logger.info(
                "news_search: trigram found 0 rows for %r — falling back to ILIKE",
                query,
            )
    except Exception as e:
        logger.warning(
            "news_search: trigram path raised, falling back: %s — "
            "is pg_trgm extension installed?", e,
        )

    # --- Strategy 3: Plain ILIKE ------------------------------------------
    try:
        pattern = f"%{query}%"
        t0 = time.monotonic()
        rows = await pool.fetch(
            "SELECT title, summary, source, timestamp, url, primary_entity, impact "
            "FROM news_articles "
            f"WHERE timestamp > NOW() - INTERVAL '{since_interval}' "
            "  AND (title ILIKE $1 OR summary ILIKE $1 OR body ILIKE $1 "
            "       OR primary_entity ILIKE $1) "
            "ORDER BY timestamp DESC LIMIT $2",
            pattern,
            limit,
            timeout=5,
        )
        sql_ms = (time.monotonic() - t0) * 1000
        total_ms = (time.monotonic() - start_total) * 1000
        if rows:
            logger.info(
                "news_search: ILIKE hit — %d rows, sql_ms=%.1f total_ms=%.1f",
                len(rows), sql_ms, total_ms,
            )
            return [dict(r) for r in rows], "ilike"
        else:
            logger.info(
                "news_search: ILIKE found 0 rows for %r — no results",
                query,
            )
    except Exception as e:
        logger.warning("news_search: ILIKE path raised: %s", e, exc_info=True)

    total_ms = (time.monotonic() - start_total) * 1000
    logger.warning(
        "news_search: NO RESULTS for query=%r across all 3 strategies total_ms=%.1f",
        query, total_ms,
    )
    return [], "none"


async def _execute_tool(
    tool_name: str,
    params: dict,
    device_id: str,
) -> dict[str, Any]:
    """Execute a tool and return results. Never raises — returns error dict on failure."""
    tool_start = time.monotonic()
    device_tag = (device_id or "")[:16]
    logger.info("tool_call: START name=%s device_id=%s params=%s", tool_name, device_tag, params)
    try:
        pool = await get_pool()

        if tool_name == "stock_lookup":
            symbol = params.get("symbol", "").upper().strip()
            if not symbol:
                return {"error": "No symbol provided"}
            row = await pool.fetchrow(
                f"SELECT * FROM discover_stock_snapshots WHERE symbol = $1",
                symbol,
            )
            if not row:
                return {"error": f"Stock '{symbol}' not found"}
            d = record_to_dict(row)
            # Return key fields for LLM context
            return {
                "symbol": d.get("symbol"),
                "display_name": d.get("display_name"),
                "sector": d.get("sector"),
                "industry": d.get("industry"),
                "last_price": d.get("last_price"),
                "percent_change": d.get("percent_change"),
                "pe_ratio": d.get("pe_ratio"),
                "price_to_book": d.get("price_to_book"),
                "roe": d.get("roe"),
                "roce": d.get("roce"),
                "debt_to_equity": d.get("debt_to_equity"),
                "operating_margins": d.get("operating_margins"),
                "profit_margins": d.get("profit_margins"),
                "market_cap": d.get("market_cap"),
                "dividend_yield": d.get("dividend_yield"),
                "revenue_growth": d.get("revenue_growth"),
                "earnings_growth": d.get("earnings_growth"),
                "score": d.get("score"),
                "promoter_holding": d.get("promoter_holding"),
                "fii_holding": d.get("fii_holding"),
                "dii_holding": d.get("dii_holding"),
                "high_52w": d.get("high_52w"),
                "low_52w": d.get("low_52w"),
                "beta": d.get("beta"),
                "compounded_sales_growth_3y": d.get("compounded_sales_growth_3y"),
                "compounded_profit_growth_3y": d.get("compounded_profit_growth_3y"),
            }

        elif tool_name == "stock_screen":
            query_where = params.get("query", "")
            limit = min(int(params.get("limit", 5)), 50)
            if not query_where:
                return {"error": "No query provided"}
            # Auto-rewrite common alias mistakes (e.g. `name` → `display_name`)
            query_where = _rewrite_column_aliases(query_where, _COLUMN_ALIASES_STOCK)
            # Validate against the stock column whitelist
            ok, err = _validate_screen_query(query_where, _STOCK_SCREEN_COLUMNS)
            if not ok:
                return {"error": err}
            rows = await pool.fetch(
                f"SELECT symbol, display_name, sector, last_price, percent_change, "
                f"pe_ratio, roe, roce, debt_to_equity, market_cap, score, "
                f"revenue_growth, operating_margins, dividend_yield "
                f"FROM discover_stock_snapshots "
                f"WHERE {query_where} "
                f"ORDER BY score DESC NULLS LAST "
                f"LIMIT {limit}",
                timeout=5,
            )
            return {
                "count": len(rows),
                "stocks": [record_to_dict(r) for r in rows],
            }

        elif tool_name == "stock_compare":
            raw_symbols = params.get("symbols")
            if not raw_symbols or not isinstance(raw_symbols, list):
                return {
                    "error": (
                        "Missing 'symbols' list. Example correct call: "
                        '[TOOL:stock_compare:{"symbols":["TCS","INFY","WIPRO"]}]. '
                        "You must include the symbols list with 2-3 items."
                    ),
                    "hint": "Call stock_compare with a symbols array",
                    "example": {"symbols": ["TCS", "INFY", "WIPRO"]},
                }
            symbols = [s.upper().strip() for s in raw_symbols if s][:3]
            if len(symbols) < 2:
                return {
                    "error": (
                        f"Need at least 2 symbols to compare, got {len(symbols)}. "
                        'Example: {"symbols":["TCS","INFY","WIPRO"]}'
                    ),
                    "hint": "Provide 2-3 stock symbols in the symbols array",
                }
            placeholders = ", ".join(f"${i+1}" for i in range(len(symbols)))
            rows = await pool.fetch(
                f"SELECT symbol, display_name, sector, last_price, percent_change, "
                f"pe_ratio, roe, roce, debt_to_equity, market_cap, score, "
                f"revenue_growth, operating_margins, dividend_yield, "
                f"promoter_holding, fii_holding "
                f"FROM discover_stock_snapshots "
                f"WHERE symbol IN ({placeholders})",
                *symbols,
            )
            return {"stocks": [record_to_dict(r) for r in rows]}

        elif tool_name == "mf_lookup":
            code = params.get("scheme_code", "").strip()
            if not code:
                return {"error": "No scheme_code provided"}
            row = await pool.fetchrow(
                f"SELECT * FROM discover_mutual_fund_snapshots WHERE scheme_code = $1",
                code,
            )
            if not row:
                return {"error": f"Fund '{code}' not found"}
            d = record_to_dict(row)
            return {
                "scheme_code": d.get("scheme_code"),
                "scheme_name": d.get("scheme_name"),
                "category": d.get("category"),
                "sub_category": d.get("sub_category"),
                "nav": d.get("nav"),
                "expense_ratio": d.get("expense_ratio"),
                "aum_cr": d.get("aum_cr"),
                "returns_1y": d.get("returns_1y"),
                "returns_3y": d.get("returns_3y"),
                "returns_5y": d.get("returns_5y"),
                "sharpe": d.get("sharpe"),
                "sortino": d.get("sortino"),
                "score": d.get("score"),
                "risk_level": d.get("risk_level"),
            }

        elif tool_name == "mf_screen":
            query_where = params.get("query", "")
            limit = min(int(params.get("limit", 5)), 50)
            if not query_where:
                return {"error": "No query provided"}
            # Auto-rewrite aliases (e.g. `name` → `scheme_name`)
            query_where = _rewrite_column_aliases(query_where, _COLUMN_ALIASES_MF)
            ok, err = _validate_screen_query(query_where, _MF_SCREEN_COLUMNS)
            if not ok:
                return {"error": err}
            rows = await pool.fetch(
                f"SELECT scheme_code, scheme_name, category, nav, "
                f"returns_1y, returns_3y, returns_5y, expense_ratio, "
                f"aum_cr, score, risk_level "
                f"FROM discover_mutual_fund_snapshots "
                f"WHERE {query_where} "
                f"ORDER BY score DESC NULLS LAST "
                f"LIMIT {limit}",
                timeout=5,
            )
            return {
                "count": len(rows),
                "funds": [record_to_dict(r) for r in rows],
            }

        elif tool_name == "watchlist":
            rows = await pool.fetch(
                "SELECT dw.asset AS symbol, ds.display_name, ds.sector, "
                "ds.last_price, ds.percent_change, ds.score, ds.market_cap "
                "FROM device_watchlists dw "
                "LEFT JOIN discover_stock_snapshots ds ON ds.symbol = dw.asset "
                "WHERE dw.device_id = $1 "
                "ORDER BY dw.position ASC",
                device_id,
            )
            return {
                "count": len(rows),
                "stocks": [record_to_dict(r) for r in rows],
            }

        elif tool_name == "market_status":
            from app.services.market_service import get_latest_prices
            from app.services.market_service import get_market_status as _mkt_status
            from app.core.asset_catalog import get_asset_meta

            prices = await get_latest_prices(instrument_type="index")

            def _fmt(p: dict) -> dict:
                return {
                    "name": p.get("asset"),
                    "price": p.get("price"),
                    "previous_close": p.get("previous_close"),
                    "change_pct": p.get("change_percent"),
                }

            # Group indices by region using the asset catalog
            by_region: dict[str, list] = {}
            for p in prices:
                name = str(p.get("asset") or "")
                if not name:
                    continue
                meta = get_asset_meta(name)
                region = meta.region if meta else "Other"
                by_region.setdefault(region, []).append(_fmt(p))

            # Sort each region's indices by priority (benchmark first)
            def _sort_key(i: dict) -> tuple[int, str]:
                m = get_asset_meta(i["name"])
                return (m.priority_rank if m else 9999, i["name"])
            for region in by_region:
                by_region[region].sort(key=_sort_key)

            # FX majors — look up the most recent USD/INR + key pairs
            fx_majors = []
            fx_prices = await get_latest_prices(instrument_type="currency")
            _FX_TARGETS = {"usd/inr", "eur/inr", "gbp/inr", "jpy/inr"}
            for p in fx_prices or []:
                name = str(p.get("asset") or "")
                if name.lower() in _FX_TARGETS:
                    fx_majors.append(_fmt(p))

            # Key commodities
            commodities_key = []
            com_prices = await get_latest_prices(instrument_type="commodity")
            _COM_TARGETS = {"gold", "silver", "crude oil", "brent crude", "natural gas"}
            for p in com_prices or []:
                name = str(p.get("asset") or "")
                if name.lower() in _COM_TARGETS:
                    commodities_key.append(_fmt(p))

            status = _mkt_status(datetime.now(timezone.utc))
            return {
                "indices_by_region": by_region,
                "market_hours": {
                    "india_open": bool(status.get("nse_open")),
                    "us_open": bool(status.get("nyse_open")),
                    "europe_open": bool(status.get("europe_open")),
                    "japan_open": bool(status.get("japan_open")),
                    "gift_nifty_open": bool(status.get("gift_nifty_open")),
                },
                "fx_majors": fx_majors,
                "commodities": commodities_key,
                "total_indices": sum(len(v) for v in by_region.values()),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }

        elif tool_name == "ipo_list":
            from app.services.ipo_service import get_ipos
            status = params.get("status", "open")
            data = await get_ipos(status=status, limit=10)
            items = data.get("items", [])
            return {
                "count": len(items),
                "ipos": [
                    {
                        "name": i.get("company_name"),
                        "status": i.get("status"),
                        "open_date": i.get("open_date"),
                        "close_date": i.get("close_date"),
                        "price_band": i.get("price_band"),
                        "lot_size": i.get("lot_size"),
                        "subscription_total": i.get("subscription_total"),
                        "gmp": i.get("gmp"),
                    }
                    for i in items[:10]
                ],
            }

        elif tool_name == "news":
            # Hybrid semantic + trigram search on news_articles with
            # a configurable time filter (since='24h'|'week'|'month').
            entity = (params.get("entity") or "").strip()
            since = str(params.get("since", "48h") or "48h").lower().strip()
            # Parse the time window
            _SINCE_MAP = {
                "6h": "6 hours",
                "12h": "12 hours",
                "24h": "24 hours",
                "48h": "48 hours",
                "today": "24 hours",
                "2d": "2 days",
                "3d": "3 days",
                "week": "7 days",
                "1w": "7 days",
                "month": "30 days",
                "1m": "30 days",
            }
            pg_interval = _SINCE_MAP.get(since, "48 hours")

            if not entity:
                rows = await pool.fetch(
                    "SELECT title, summary, source, timestamp, url, primary_entity, impact "
                    "FROM news_articles "
                    f"WHERE timestamp > NOW() - INTERVAL '{pg_interval}' "
                    "ORDER BY timestamp DESC LIMIT 5",
                    timeout=5,
                )
                search_mode = "recent"
            else:
                rows, search_mode = await _news_hybrid_search(
                    pool, entity, limit=5, since_interval=pg_interval,
                )

            return {
                "query": entity or None,
                "search_mode": search_mode,
                "count": len(rows),
                "articles": [
                    {
                        "title": r.get("title"),
                        "source": r.get("source"),
                        "published": (
                            r["timestamp"].isoformat()
                            if r.get("timestamp") and hasattr(r["timestamp"], "isoformat")
                            else r.get("timestamp")
                        ),
                        "summary": (r.get("summary") or "")[:300],
                        "url": r.get("url"),
                        "topic": r.get("primary_entity"),
                        "impact": r.get("impact"),
                    }
                    for r in rows
                ],
            }

        elif tool_name == "macro":
            # Fetch macro indicator readings. Defaults to India but
            # accepts an explicit country param (IN, US, EU, JP, CN).
            #
            # Bug fix: the old query was `ORDER BY timestamp DESC
            # LIMIT 5` without a country filter, which returned the
            # 5 most-recent rows globally. ECB (EU) updates daily but
            # RBI (IN) updates monthly, so `repo_rate` always returned
            # EU data and the LLM then said "no India data". Now we
            # default to India and allow country override.
            indicator = str(params.get("indicator", "") or "").strip()
            if not indicator:
                return {
                    "error": "No indicator specified",
                    "hint": "Valid indicators: repo_rate, inflation_cpi, gdp_growth, usd_inr, fiscal_deficit, current_account, iip_growth",
                }
            country_raw = str(params.get("country", "") or "").strip().upper()
            # Normalise common aliases
            country_map = {
                "": "IN",          # default to India
                "INDIA": "IN",
                "INR": "IN",
                "US": "US",
                "USA": "US",
                "AMERICA": "US",
                "EU": "EU",
                "EUROPE": "EU",
                "EUROZONE": "EU",
                "JP": "JP",
                "JAPAN": "JP",
                "CN": "CN",
                "CHINA": "CN",
            }
            country = country_map.get(country_raw, country_raw or "IN")

            rows = await pool.fetch(
                "SELECT indicator_name, country, value, timestamp "
                "FROM macro_indicators "
                "WHERE indicator_name ILIKE $1 "
                "  AND country = $2 "
                "ORDER BY timestamp DESC LIMIT 12",
                f"%{indicator}%",
                country,
            )
            if not rows:
                return {
                    "error": f"No {indicator} data found for {country}",
                    "indicator": indicator,
                    "country": country,
                    "hint": "Try a different country (US, EU, JP) or indicator name",
                }
            data = [record_to_dict(r) for r in rows]
            latest = data[0]
            prev = data[1] if len(data) > 1 else None
            change = None
            if prev and latest.get("value") is not None and prev.get("value") is not None:
                change = float(latest["value"]) - float(prev["value"])
            return {
                "indicator": indicator,
                "country": country,
                "latest_value": latest.get("value"),
                "latest_date": latest.get("timestamp"),
                "prev_value": prev.get("value") if prev else None,
                "change_vs_prev": change,
                "history": data[:12],
                "count": len(data),
            }

        elif tool_name == "commodity":
            # market_service returns `price` (not `close` / `last_price`).
            # Filter to commodities the user actually asks about most.
            from app.services.market_service import get_latest_prices
            prices = await get_latest_prices(instrument_type="commodity")
            # Sort so the most commonly-asked-about ones come first.
            _COMMODITY_PRIORITY = {
                "gold": 1, "silver": 2, "crude oil": 3, "brent crude": 4,
                "natural gas": 5, "copper": 6, "aluminum": 7,
                "platinum": 8, "palladium": 9,
            }
            sorted_prices = sorted(
                prices or [],
                key=lambda p: _COMMODITY_PRIORITY.get(
                    str(p.get("asset") or "").lower(), 99
                ),
            )
            return {
                "count": len(sorted_prices),
                "commodities": [
                    {
                        "name": p.get("asset"),
                        "price": p.get("price"),
                        "previous_close": p.get("previous_close"),
                        "change_pct": p.get("change_percent"),
                        "unit": p.get("unit"),
                    }
                    for p in sorted_prices[:12]
                    if p.get("price") is not None
                ],
            }

        elif tool_name == "crypto":
            # Same key fix as commodity — use `price` not `close`.
            from app.services.market_service import get_latest_prices
            prices = await get_latest_prices(instrument_type="crypto")
            _CRYPTO_PRIORITY = {
                "btc": 1, "bitcoin": 1, "eth": 2, "ethereum": 2,
                "bnb": 3, "sol": 4, "solana": 4, "xrp": 5, "ada": 6,
            }
            sorted_prices = sorted(
                prices or [],
                key=lambda p: _CRYPTO_PRIORITY.get(
                    str(p.get("asset") or "").lower(), 99
                ),
            )
            return {
                "count": len(sorted_prices),
                "crypto": [
                    {
                        "name": p.get("asset"),
                        "price": p.get("price"),
                        "previous_close": p.get("previous_close"),
                        "change_pct": p.get("change_percent"),
                    }
                    for p in sorted_prices[:10]
                    if p.get("price") is not None
                ],
            }

        elif tool_name == "tax":
            # Real tax calculation for Indian equity capital gains.
            # Supported types: ltcg, stcg, income_tax
            tax_type = str(params.get("type", "ltcg")).lower().strip()
            profit = float(params.get("profit", 0) or 0)
            purchase_year = str(params.get("purchase_year", "") or "").strip()
            sale_year = str(params.get("sale_year", "") or "").strip()

            if tax_type in ("ltcg", "long_term", "longterm"):
                # LTCG on equity (held > 1 year): 12.5% on gains above
                # ₹1.25L exemption (FY 2024-25 onwards).
                exemption = 125_000
                taxable = max(0, profit - exemption)
                tax = taxable * 0.125
                return {
                    "type": "ltcg",
                    "profit": profit,
                    "exemption": exemption,
                    "taxable": taxable,
                    "tax_rate_pct": 12.5,
                    "tax_amount": round(tax, 2),
                    "effective_rate_pct": round(tax / profit * 100, 2) if profit > 0 else 0,
                    "note": "Indian LTCG on listed equity (held >1 year). ₹1.25L annual exemption, 12.5% flat rate (FY 2024-25).",
                    "purchase_year": purchase_year or None,
                    "sale_year": sale_year or None,
                }
            elif tax_type in ("stcg", "short_term", "shortterm"):
                # STCG on equity (held ≤ 1 year): 20% flat from FY 2024-25
                tax = profit * 0.20
                return {
                    "type": "stcg",
                    "profit": profit,
                    "tax_rate_pct": 20.0,
                    "tax_amount": round(tax, 2),
                    "note": "Indian STCG on listed equity (held ≤1 year). 20% flat rate (FY 2024-25 onwards).",
                }
            elif tax_type in ("income_tax", "income", "it"):
                # New tax regime slabs (FY 2024-25), without rebate/surcharge
                income = profit  # user passes total income as "profit"
                slabs = [
                    (300_000, 0.00),
                    (700_000, 0.05),
                    (1_000_000, 0.10),
                    (1_200_000, 0.15),
                    (1_500_000, 0.20),
                    (float("inf"), 0.30),
                ]
                remaining = income
                tax = 0.0
                last = 0
                breakdown = []
                for limit, rate in slabs:
                    if remaining <= 0:
                        break
                    slab = min(remaining, limit - last)
                    slab_tax = slab * rate
                    breakdown.append({
                        "slab": f"₹{last:,.0f}–{limit if limit != float('inf') else 'above':,}",
                        "rate_pct": rate * 100,
                        "slab_taxable": slab,
                        "slab_tax": round(slab_tax, 2),
                    })
                    tax += slab_tax
                    remaining -= slab
                    last = limit
                cess = tax * 0.04  # 4% health & education cess
                return {
                    "type": "income_tax",
                    "regime": "new",
                    "gross_income": income,
                    "base_tax": round(tax, 2),
                    "cess_4pct": round(cess, 2),
                    "total_tax": round(tax + cess, 2),
                    "effective_rate_pct": round((tax + cess) / income * 100, 2) if income > 0 else 0,
                    "slab_breakdown": breakdown,
                    "note": "New tax regime slabs FY 2024-25. No standard deduction or rebates applied. Old regime not supported here.",
                }
            else:
                return {
                    "error": (
                        f"Unknown tax type: {tax_type}. "
                        "Supported: ltcg, stcg, income_tax"
                    ),
                }

        elif tool_name == "stock_price_history":
            # Historical OHLCV-ish prices for a single stock.
            # Periods: 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y
            symbol = str(params.get("symbol", "")).upper().strip()
            period = str(params.get("period", "1mo")).lower().strip()
            if not symbol:
                return {"error": "No symbol provided"}
            _PERIOD_DAYS = {
                "5d": 5, "1mo": 30, "3mo": 90, "6mo": 180,
                "1y": 365, "2y": 730, "5y": 1825,
            }
            days = _PERIOD_DAYS.get(period, 90)
            rows = await pool.fetch(
                "SELECT trade_date, close, volume "
                "FROM discover_stock_price_history "
                "WHERE symbol = $1 "
                "  AND trade_date >= CURRENT_DATE - ($2 || ' days')::interval "
                "ORDER BY trade_date ASC",
                symbol, days,
            )
            if not rows:
                return {
                    "error": f"No historical price data for {symbol}",
                    "symbol": symbol,
                    "period": period,
                }
            closes = [float(r["close"]) for r in rows if r["close"] is not None]
            if not closes:
                return {
                    "error": f"Price history exists but has no close values for {symbol}",
                    "symbol": symbol,
                }
            first_close = closes[0]
            last_close = closes[-1]
            pct_change = (
                ((last_close - first_close) / first_close * 100)
                if first_close > 0 else 0
            )
            high = max(closes)
            low = min(closes)
            avg = sum(closes) / len(closes)
            # Keep the returned array short for LLM context — downsample
            # to ~30 points max so the model can describe the trend.
            sample_size = min(30, len(rows))
            step = max(1, len(rows) // sample_size)
            sampled = [
                {
                    "date": rows[i]["trade_date"].isoformat(),
                    "close": round(float(rows[i]["close"]), 2),
                    "volume": rows[i]["volume"],
                }
                for i in range(0, len(rows), step)
            ][:sample_size]
            return {
                "symbol": symbol,
                "period": period,
                "count": len(rows),
                "first_date": rows[0]["trade_date"].isoformat(),
                "last_date": rows[-1]["trade_date"].isoformat(),
                "first_close": round(first_close, 2),
                "last_close": round(last_close, 2),
                "high": round(high, 2),
                "low": round(low, 2),
                "avg": round(avg, 2),
                "pct_change": round(pct_change, 2),
                "series": sampled,
            }

        elif tool_name == "sector_performance":
            # Aggregate view across all stocks in a sector or all sectors.
            sector = str(params.get("sector", "") or "").strip()
            if sector:
                # Single sector detail
                rows = await pool.fetch(
                    "SELECT symbol, display_name, last_price, percent_change, "
                    "market_cap, score "
                    "FROM discover_stock_snapshots "
                    "WHERE sector = $1 AND percent_change IS NOT NULL "
                    "ORDER BY percent_change DESC",
                    sector,
                )
                if not rows:
                    return {"error": f"No stocks found in sector '{sector}'", "sector": sector}
                changes = [float(r["percent_change"]) for r in rows if r["percent_change"] is not None]
                avg_change = sum(changes) / len(changes) if changes else 0
                gainers = [r for r in rows if (r["percent_change"] or 0) > 0]
                losers = [r for r in rows if (r["percent_change"] or 0) < 0]
                return {
                    "sector": sector,
                    "total_stocks": len(rows),
                    "gainer_count": len(gainers),
                    "loser_count": len(losers),
                    "avg_change_pct": round(avg_change, 2),
                    "top_gainers": [
                        {
                            "symbol": r["symbol"],
                            "name": r["display_name"],
                            "change_pct": float(r["percent_change"] or 0),
                            "price": float(r["last_price"] or 0),
                            "score": float(r["score"] or 0) if r.get("score") is not None else None,
                        }
                        for r in rows[:5]
                    ],
                    "top_losers": [
                        {
                            "symbol": r["symbol"],
                            "name": r["display_name"],
                            "change_pct": float(r["percent_change"] or 0),
                            "price": float(r["last_price"] or 0),
                            "score": float(r["score"] or 0) if r.get("score") is not None else None,
                        }
                        for r in sorted(rows, key=lambda x: x["percent_change"] or 0)[:5]
                    ],
                }
            else:
                # All sectors overview — average change per sector
                rows = await pool.fetch(
                    "SELECT sector, "
                    "COUNT(*) AS stock_count, "
                    "AVG(percent_change) AS avg_change, "
                    "SUM(CASE WHEN percent_change > 0 THEN 1 ELSE 0 END) AS gainers, "
                    "SUM(CASE WHEN percent_change < 0 THEN 1 ELSE 0 END) AS losers "
                    "FROM discover_stock_snapshots "
                    "WHERE sector IS NOT NULL AND percent_change IS NOT NULL "
                    "GROUP BY sector "
                    "ORDER BY avg_change DESC NULLS LAST"
                )
                return {
                    "sectors": [
                        {
                            "sector": r["sector"],
                            "stock_count": int(r["stock_count"]),
                            "avg_change_pct": round(float(r["avg_change"] or 0), 2),
                            "gainers": int(r["gainers"] or 0),
                            "losers": int(r["losers"] or 0),
                        }
                        for r in rows
                    ],
                }

        elif tool_name == "educational":
            # Explain a financial concept using a curated dictionary.
            # LLM can then expand with context-specific examples.
            concept = str(params.get("concept", "")).lower().strip()
            _CONCEPTS = {
                "pe_ratio": {
                    "name": "P/E Ratio (Price-to-Earnings)",
                    "definition": "Share price divided by earnings per share (EPS). Tells you how many rupees investors are paying for every rupee of annual profit.",
                    "formula": "P/E = Price / EPS",
                    "typical_range": "Indian market average ~20-25. <15 = value territory, 30+ = growth/premium, 50+ = expensive unless growing fast.",
                    "example": "TCS trading at ₹3,500 with EPS ₹120 → P/E = 29.2. That's above market average, which is normal for a high-quality IT leader.",
                    "caveats": "Meaningless for loss-making companies. Needs comparison within the same sector.",
                },
                "roe": {
                    "name": "Return on Equity (ROE)",
                    "definition": "Net profit as a percentage of shareholders' equity. Measures how efficiently a company generates profit from every rupee of owner capital.",
                    "formula": "ROE = Net Profit / Shareholders' Equity × 100",
                    "typical_range": "15-20% is good, 25%+ is excellent, <10% is weak.",
                    "example": "HDFC Bank with ROE ~17% means every ₹100 of equity generates ₹17 of annual profit.",
                    "caveats": "High ROE with high debt can be misleading — check D/E ratio together.",
                },
                "roce": {
                    "name": "Return on Capital Employed (ROCE)",
                    "definition": "Operating profit divided by total capital employed (equity + debt). More comprehensive than ROE because it includes debt in the denominator.",
                    "formula": "ROCE = EBIT / Capital Employed × 100",
                    "typical_range": "15%+ is good, 25%+ is excellent.",
                    "example": "Asian Paints with ROCE ~30% means it generates ₹30 of operating profit per ₹100 of total capital.",
                    "caveats": "Better metric than ROE for leveraged businesses.",
                },
                "ltcg": {
                    "name": "Long-Term Capital Gains (LTCG) Tax",
                    "definition": "Tax on profits from selling equity shares or equity mutual funds held for more than 12 months.",
                    "formula": "LTCG Tax = (Profit − ₹1.25L exemption) × 12.5%",
                    "typical_range": "₹1.25L annual exemption. Flat 12.5% rate (FY 2024-25 onwards).",
                    "example": "Sell TCS for ₹6L profit held 2 years → (₹6L − ₹1.25L) × 12.5% = ₹59,375 tax.",
                    "caveats": "Exemption is PER YEAR, not per transaction. Indexation benefit removed in FY 2024-25.",
                },
                "stcg": {
                    "name": "Short-Term Capital Gains (STCG) Tax",
                    "definition": "Tax on profits from selling equity shares or equity mutual funds held for 12 months or less.",
                    "formula": "STCG Tax = Profit × 20%",
                    "typical_range": "Flat 20% rate (FY 2024-25). No exemption.",
                    "example": "Sell Reliance for ₹2L profit held 6 months → ₹2L × 20% = ₹40,000 tax.",
                    "caveats": "Applies regardless of your income tax slab. Short-term trading is heavily taxed.",
                },
                "dcf": {
                    "name": "Discounted Cash Flow (DCF) Valuation",
                    "definition": "Method to estimate intrinsic value of a stock by forecasting future cash flows and discounting them to present value.",
                    "formula": "Value = Σ (FCFₜ / (1+r)ᵗ) for all future years",
                    "typical_range": "WACC (discount rate) usually 10-14% in Indian markets.",
                    "example": "If TCS's projected FCFs discount to ₹4,200/share and it trades at ₹3,500, DCF says it's undervalued.",
                    "caveats": "Highly sensitive to growth and discount rate assumptions. Small changes swing the result wildly.",
                },
                "pb_ratio": {
                    "name": "P/B Ratio (Price-to-Book)",
                    "definition": "Share price divided by book value per share (net assets per share). Tells you what you're paying relative to the company's accounting net worth.",
                    "formula": "P/B = Price / Book Value per Share",
                    "typical_range": "<1 = below book, 1-3 = normal, 3-5 = premium, >5 = usually needs justification.",
                    "example": "A bank trading at P/B of 2 means you pay ₹2 for every ₹1 of net assets.",
                    "caveats": "Most useful for financial companies (banks, NBFCs) where book value is meaningful.",
                },
                "debt_to_equity": {
                    "name": "Debt-to-Equity Ratio (D/E)",
                    "definition": "Total debt divided by shareholders' equity. Measures financial leverage and balance sheet health.",
                    "formula": "D/E = Total Debt / Equity",
                    "typical_range": "<0.5 = conservative, 0.5-1 = moderate, >1 = aggressive, >2 = high risk.",
                    "example": "TCS with D/E of 0.1 is effectively debt-free. Real estate companies often run 1-2+.",
                    "caveats": "Varies massively by sector — compare within peers.",
                },
                "ebitda": {
                    "name": "EBITDA",
                    "definition": "Earnings Before Interest, Taxes, Depreciation, and Amortization. Proxy for operating cash flow before capital structure and non-cash charges.",
                    "formula": "EBITDA = Net Profit + Interest + Taxes + D&A",
                    "typical_range": "EBITDA margin 15-25% = healthy for most sectors.",
                    "example": "If revenue is ₹100Cr and EBITDA is ₹20Cr, margin is 20%.",
                    "caveats": "Ignores capex — dangerous for capital-intensive businesses.",
                },
                "dividend_yield": {
                    "name": "Dividend Yield",
                    "definition": "Annual dividend per share divided by share price. Tells you what cash yield you get from holding the stock.",
                    "formula": "Yield = Annual Dividend / Price × 100",
                    "typical_range": "Nifty average ~1.5%. 3-5% = good income stock. 6%+ often means stress.",
                    "example": "ITC paying ₹15 annual dividend at price ₹450 → yield 3.3%.",
                    "caveats": "A falling price inflates yield. Check if dividend is sustainable.",
                },
            }
            # Fuzzy match on concept key
            if concept in _CONCEPTS:
                return _CONCEPTS[concept]
            # Try matching by name substring
            for key, info in _CONCEPTS.items():
                if concept in key or concept in info["name"].lower():
                    return info
            return {
                "error": f"Unknown concept: '{concept}'",
                "available_concepts": sorted(_CONCEPTS.keys()),
            }

        else:
            logger.warning("tool_call: unknown tool=%s params=%s", tool_name, params)
            return {"error": f"Unknown tool: {tool_name}"}

        # If we reach here, the return statement in the matched branch has
        # already produced the result. The actual return happens in the
        # matched if/elif branch above — this line is unreachable but kept
        # for type-checker clarity.
    except Exception as e:
        elapsed_ms = (time.monotonic() - tool_start) * 1000
        logger.warning(
            "tool_call: FAILED name=%s elapsed_ms=%.1f error=%s",
            tool_name, elapsed_ms, e,
            exc_info=True,
        )
        return {"error": f"Tool failed: {str(e)[:100]}"}


# Valid columns for each screener table. Any identifier in a WHERE clause
# that isn't in this list (or a SQL keyword / literal) is rejected before
# the query hits the DB, so the LLM gets a helpful error message instead
# of a generic Postgres "column does not exist" failure.
_STOCK_SCREEN_COLUMNS = {
    "symbol", "display_name", "sector", "industry",
    "last_price", "percent_change", "previous_close",
    "pe_ratio", "price_to_book", "dividend_yield", "market_cap",
    "roe", "roce", "debt_to_equity",
    "operating_margins", "profit_margins",
    "revenue_growth", "earnings_growth",
    "compounded_sales_growth_3y", "compounded_profit_growth_3y",
    "free_cash_flow", "beta",
    "promoter_holding", "fii_holding", "dii_holding",
    "high_52w", "low_52w", "score",
}

_MF_SCREEN_COLUMNS = {
    "scheme_code", "scheme_name", "category", "sub_category",
    "nav", "expense_ratio", "aum_cr",
    "returns_1y", "returns_3y", "returns_5y",
    "sharpe", "sortino", "score", "risk_level",
    "fund_house", "fund_manager",
}

# Common LLM mistakes → correct column. We auto-rewrite before validation
# so a query like "name LIKE '%Nifty%'" becomes "scheme_name LIKE '%Nifty%'".
_COLUMN_ALIASES_STOCK = {
    "name": "display_name",
    "company": "display_name",
    "company_name": "display_name",
    "ticker": "symbol",
    "pe": "pe_ratio",
    "pb": "price_to_book",
    "market_capitalization": "market_cap",
    "de_ratio": "debt_to_equity",
    "price": "last_price",
    "change_pct": "percent_change",
    "price_change": "percent_change",
}

_COLUMN_ALIASES_MF = {
    "name": "scheme_name",
    "fund_name": "scheme_name",
    "code": "scheme_code",
    "nav_value": "nav",
    "aum": "aum_cr",
    "returns_1_year": "returns_1y",
    "returns_3_year": "returns_3y",
    "returns_5_year": "returns_5y",
    "return_1y": "returns_1y",
    "return_3y": "returns_3y",
    "return_5y": "returns_5y",
    "expense": "expense_ratio",
}

# Regex to extract potential identifiers (col names, keywords) from a query.
_IDENT_PATTERN = re.compile(r'\b([a-zA-Z_][a-zA-Z0-9_]*)\b')

# SQL keywords that may legitimately appear in a WHERE clause.
_SQL_KEYWORDS = {
    "and", "or", "not", "in", "is", "null", "like", "ilike", "between",
    "true", "false", "asc", "desc", "where",
    "any", "all", "exists", "distinct", "case", "when", "then", "else", "end",
    "current_date", "current_time", "current_timestamp", "now",
}


def _rewrite_column_aliases(query: str, aliases: dict[str, str]) -> str:
    """Auto-rewrite common column aliases to their canonical names."""
    def _sub(match: re.Match) -> str:
        ident = match.group(1)
        canonical = aliases.get(ident.lower())
        return canonical if canonical else ident
    return _IDENT_PATTERN.sub(_sub, query)


def _validate_screen_query(
    query: str,
    valid_columns: set[str] | None = None,
) -> tuple[bool, str | None]:
    """Validate a screener WHERE clause.

    Returns ``(ok, error_message)``. When ``valid_columns`` is provided,
    every identifier in the query must be either a valid column, a SQL
    keyword, or a literal — unknown identifiers produce a clear error
    message listing the column whitelist.

    Blocks dangerous SQL keywords that shouldn't appear in a WHERE clause
    at all (DROP, DELETE, UNION, etc.) to prevent injection attempts.
    """
    if not query or not query.strip():
        return False, "Query is empty"

    upper = query.upper()
    # Block dangerous SQL keywords
    blocked = [
        "DROP", "DELETE", "INSERT", "UPDATE", "ALTER", "CREATE",
        "TRUNCATE", "GRANT", "REVOKE", "EXEC", "EXECUTE",
        "INTO", "UNION", "JOIN", ";", "--", "/*",
    ]
    for kw in blocked:
        if kw in upper:
            return False, f"Blocked SQL keyword: {kw}"

    # Must look like a filter — has at least one comparison operator OR a LIKE
    if not (re.search(r'[=<>]', query) or re.search(r'\b(like|ilike|in|is)\b', query, re.IGNORECASE)):
        return False, "Query must contain a filter condition (=, <, >, LIKE, IN, IS)"

    # Column whitelist check
    if valid_columns:
        # Extract identifiers that aren't inside string literals.
        # Simple approach: strip 'quoted strings' first.
        stripped = re.sub(r"'[^']*'", "''", query)
        stripped = re.sub(r'"[^"]*"', '""', stripped)
        idents = _IDENT_PATTERN.findall(stripped)
        unknown = []
        for ident in idents:
            low = ident.lower()
            # Skip keywords
            if low in _SQL_KEYWORDS:
                continue
            # Skip pure numerics (shouldn't match \b[a-z_] but just in case)
            if ident.isdigit():
                continue
            if low not in {c.lower() for c in valid_columns}:
                unknown.append(ident)
        if unknown:
            valid_list = ", ".join(sorted(valid_columns))
            return False, (
                f"Unknown column(s): {', '.join(sorted(set(unknown)))}. "
                f"Valid columns: {valid_list}"
            )

    return True, None


# ---------------------------------------------------------------------------
# Session management
# ---------------------------------------------------------------------------

async def create_session(device_id: str) -> str:
    """Create a new chat session. Returns session_id. Enforces max 50 sessions."""
    pool = await get_pool()
    session_id = str(uuid.uuid4())

    # Enforce max sessions — delete oldest beyond limit
    await pool.execute(
        """
        DELETE FROM chat_sessions WHERE id IN (
            SELECT id FROM chat_sessions
            WHERE device_id = $1
            ORDER BY updated_at DESC
            OFFSET $2
        )
        """,
        device_id,
        MAX_SESSIONS_PER_DEVICE - 1,  # -1 because we're about to add one
    )

    await pool.execute(
        "INSERT INTO chat_sessions (id, device_id, created_at, updated_at) "
        "VALUES ($1, $2, NOW(), NOW())",
        session_id,
        device_id,
    )
    return session_id


async def list_sessions(device_id: str) -> list[dict]:
    """List chat sessions for a device, most recent first."""
    pool = await get_pool()
    rows = await pool.fetch(
        """
        SELECT cs.id, cs.device_id, cs.title, cs.created_at, cs.updated_at,
               COALESCE(mc.cnt, 0) AS message_count
        FROM chat_sessions cs
        LEFT JOIN (
            SELECT session_id, COUNT(*) AS cnt
            FROM chat_messages GROUP BY session_id
        ) mc ON mc.session_id = cs.id
        WHERE cs.device_id = $1
        ORDER BY cs.updated_at DESC
        LIMIT 50
        """,
        device_id,
    )
    return [record_to_dict(r) for r in rows]


async def get_session_messages(session_id: str, device_id: str) -> list[dict]:
    """Get all messages in a session (with device_id ownership check)."""
    pool = await get_pool()
    rows = await pool.fetch(
        """
        SELECT cm.* FROM chat_messages cm
        JOIN chat_sessions cs ON cs.id = cm.session_id
        WHERE cm.session_id = $1 AND cs.device_id = $2
        ORDER BY cm.created_at ASC
        """,
        session_id,
        device_id,
    )
    return [record_to_dict(r) for r in rows]


async def delete_session(session_id: str, device_id: str) -> bool:
    """Delete a session and its messages. Returns True if deleted."""
    pool = await get_pool()
    result = await pool.execute(
        "DELETE FROM chat_sessions WHERE id = $1 AND device_id = $2",
        session_id,
        device_id,
    )
    return result.endswith("1")  # "DELETE 1"


async def save_message(
    session_id: str,
    role: str,
    content: str,
    stock_cards: list[dict] | None = None,
    mf_cards: list[dict] | None = None,
    tool_calls: list[dict] | None = None,
) -> str:
    """Save a message and return its ID."""
    pool = await get_pool()
    msg_id = str(uuid.uuid4())
    await pool.execute(
        """
        INSERT INTO chat_messages (id, session_id, role, content, stock_cards, mf_cards, tool_calls, created_at)
        VALUES ($1, $2, $3, $4, $5::jsonb, $6::jsonb, $7::jsonb, NOW())
        """,
        msg_id,
        session_id,
        role,
        content,
        json.dumps(stock_cards) if stock_cards else None,
        json.dumps(mf_cards) if mf_cards else None,
        json.dumps(tool_calls) if tool_calls else None,
    )
    # Update session title from first user message
    if role == "user":
        await pool.execute(
            """
            UPDATE chat_sessions
            SET title = COALESCE(title, $2),
                updated_at = NOW()
            WHERE id = $1
            """,
            session_id,
            content[:80],  # First message becomes title
        )
    else:
        await pool.execute(
            "UPDATE chat_sessions SET updated_at = NOW() WHERE id = $1",
            session_id,
        )
    return msg_id


async def set_feedback(message_id: str, device_id: str, feedback: int) -> bool:
    """Set thumbs up/down feedback on a message."""
    pool = await get_pool()
    result = await pool.execute(
        """
        UPDATE chat_messages cm SET feedback = $1
        FROM chat_sessions cs
        WHERE cm.id = $2 AND cm.session_id = cs.id AND cs.device_id = $3
        """,
        feedback,
        message_id,
        device_id,
    )
    return "UPDATE" in result


# ---------------------------------------------------------------------------
# Rate limiting
# ---------------------------------------------------------------------------

async def check_rate_limit(device_id: str) -> tuple[bool, int]:
    """Check if device is within daily limit. Returns (allowed, remaining)."""
    pool = await get_pool()
    row = await pool.fetchrow(
        "SELECT count FROM chat_rate_limits WHERE device_id = $1 AND date = CURRENT_DATE",
        device_id,
    )
    current = row["count"] if row else 0
    remaining = max(0, MAX_MESSAGES_PER_DAY - current)
    return current < MAX_MESSAGES_PER_DAY, remaining


async def increment_rate_limit(device_id: str) -> None:
    """Increment daily message count."""
    pool = await get_pool()
    await pool.execute(
        """
        INSERT INTO chat_rate_limits (device_id, date, count)
        VALUES ($1, CURRENT_DATE, 1)
        ON CONFLICT (device_id, date) DO UPDATE SET count = chat_rate_limits.count + 1
        """,
        device_id,
    )


# ---------------------------------------------------------------------------
# Context-aware greeting
# ---------------------------------------------------------------------------

async def generate_greeting() -> dict:
    """Generate a context-aware greeting using live market data."""
    try:
        from app.services.market_service import get_latest_prices
        prices = await get_latest_prices(instrument_type="index")
        nifty = None
        for p in prices:
            if p.get("asset") == "NIFTY 50":
                nifty = p
                break

        now = datetime.now(timezone.utc)
        ist_hour = (now.hour + 5) % 24 + (30 // 60)  # rough IST

        if ist_hour < 12:
            tod = "Good morning"
        elif ist_hour < 17:
            tod = "Good afternoon"
        else:
            tod = "Good evening"

        if nifty:
            change = nifty.get("change_percent", 0) or 0
            direction = "up" if change > 0 else "down" if change < 0 else "flat"
            greeting = f"{tod}! Nifty 50 is {direction} {abs(change):.1f}% today. What would you like to know?"
        else:
            greeting = f"{tod}! I'm Artha, your market analyst. Ask me anything about stocks, mutual funds, or the economy."
    except Exception:
        greeting = "Namaste! I'm Artha, your market analyst. Ask me anything about Indian markets."

    return {"greeting": greeting}


# Suggestion POOL strategy
# ─────────────────────────
# Instead of generating N suggestions per request, we maintain a POOL of
# ~30 suggestions that gets refreshed in the background every few minutes
# using live context (real top movers, real IPOs, real news). Each client
# request picks N RANDOM items from the current pool, giving the feeling
# of "fresh every app open" without hammering the LLM.
#
# Two pools exist:
#   * _suggestions_pool["_global"]  — generated without device_id, seeded
#     with market + news + IPO context, used for anonymous welcome requests
#   * _suggestions_pool[device_id]  — per-device, also includes that
#     device's watchlist. Lazily populated on first request.
#
# The pool refresh is throttled: if the pool is younger than
# _POOL_FRESH_SECONDS we reuse it; older than that and a background
# refresh is kicked. No client ever waits for the LLM.
_suggestions_pool: dict[str, tuple[list[str], float]] = {}
_POOL_FRESH_SECONDS = 3 * 60   # refresh pool every 3 minutes
_POOL_MAX_AGE = 15 * 60        # pool older than 15 min is considered stale
_POOL_SIZE = 30                # generate ~30 suggestions per pool refresh
_DEFAULT_PICK = 10             # number of suggestions returned per request
# Track in-flight refresh tasks to prevent concurrent duplicate LLM calls
_suggestions_inflight: dict[str, asyncio.Task] = {}


async def _refresh_pool_bg(device_id: str | None, pool_key: str) -> None:
    """Background task: refresh a suggestions pool with live context."""
    try:
        suggestions = await _compute_suggestions_llm(device_id)
        if suggestions and len(suggestions) >= 6:
            _suggestions_pool[pool_key] = (suggestions, time.time())
            logger.info(
                "suggestions: pool refreshed key=%s size=%d",
                pool_key, len(suggestions),
            )
    except Exception:
        logger.warning("suggestions: pool refresh failed", exc_info=True)
    finally:
        _suggestions_inflight.pop(pool_key, None)


def _kick_pool_refresh(device_id: str | None, pool_key: str) -> None:
    """Start a background refresh if one isn't already running for this pool."""
    if pool_key in _suggestions_inflight:
        return
    try:
        loop = asyncio.get_running_loop()
        task = loop.create_task(_refresh_pool_bg(device_id, pool_key))
        _suggestions_inflight[pool_key] = task
    except RuntimeError:
        pass


async def generate_suggestions(
    device_id: str | None = None,
    count: int = _DEFAULT_PICK,
) -> list[str]:
    """Return a random sample of *count* suggestions from the pool.

    On every request we pick a FRESH random sample — no two consecutive
    app opens should see the same set. The underlying pool is refreshed
    every ~3 minutes in the background using live market / news / IPO
    context, so the user feels like they get a new batch each time
    without paying the LLM latency per request.
    """
    import random

    pool_key = device_id or "_global"
    now_ts = time.time()
    now = datetime.now(timezone.utc)
    ist_hour = (now.hour + 5) % 24 + (30 // 60)

    pool_entry = _suggestions_pool.get(pool_key)

    if pool_entry is not None:
        pool_items, ts = pool_entry
        age = now_ts - ts
        if age >= _POOL_FRESH_SECONDS:
            # Pool is getting old — refresh in background but serve from
            # current pool right now. No user wait.
            _kick_pool_refresh(device_id, pool_key)
        if age < _POOL_MAX_AGE and pool_items:
            sample_size = min(count, len(pool_items))
            picks = random.sample(pool_items, sample_size)
            logger.debug(
                "suggestions: pool pick key=%s age=%.0fs size=%d pick=%d",
                pool_key, age, len(pool_items), sample_size,
            )
            return picks

    # No pool or too stale — fire a bg refresh and return static fallback
    # this one time. Subsequent requests (within ~15s) hit the warm pool.
    logger.info(
        "suggestions: pool MISS key=%s — returning static, refreshing in bg",
        pool_key,
    )
    _kick_pool_refresh(device_id, pool_key)
    static = _static_suggestions(ist_hour)
    # Extend static with a few more so the "cold" fallback doesn't feel thin
    return static[:count]


async def _compute_suggestions_llm(device_id: str | None) -> list[str]:
    """Generate a POOL of ~30 suggestions using live context.

    Never called from the request path — only from the background
    refresh task. Latency here doesn't affect users.
    """
    try:
        context_parts: list[str] = []
        real_names: list[str] = []  # tracker for "only use these names" guardrail

        now = datetime.now(timezone.utc)
        ist_hour = (now.hour + 5) % 24 + (30 // 60)

        # ── 1. Time-of-day context ─────────────────────────────────
        if ist_hour < 9:
            context_parts.append("Time: Pre-market (before 9:15 AM IST) — Gift Nifty is active")
        elif ist_hour < 15:
            context_parts.append("Time: Indian markets OPEN (9:15 AM – 3:30 PM IST)")
        elif ist_hour < 20:
            context_parts.append("Time: Post-market (after 3:30 PM IST) — US pre-market starting")
        else:
            context_parts.append("Time: Late evening — US markets active")

        # ── 2. Indian indices snapshot ─────────────────────────────
        try:
            from app.services.market_service import get_latest_prices
            prices = await get_latest_prices(instrument_type="index")
            snaps = []
            wanted = {"nifty 50", "sensex", "nifty bank", "nifty it", "nifty auto", "nifty pharma"}
            for p in (prices or []):
                name = str(p.get("asset") or "")
                if name.lower() in wanted and p.get("price") is not None:
                    pct = p.get("change_percent") or 0
                    snaps.append(f"{name} {pct:+.2f}%")
            if snaps:
                context_parts.append("Indian indices: " + ", ".join(snaps))
        except Exception:
            pass

        # ── 3. Top 5 gainers + top 5 losers (REAL symbols) ──────────
        try:
            pool_conn = await get_pool()
            top_gainers = await pool_conn.fetch(
                "SELECT symbol, display_name, percent_change FROM discover_stock_snapshots "
                "WHERE percent_change IS NOT NULL "
                "ORDER BY percent_change DESC LIMIT 5"
            )
            top_losers = await pool_conn.fetch(
                "SELECT symbol, display_name, percent_change FROM discover_stock_snapshots "
                "WHERE percent_change IS NOT NULL "
                "ORDER BY percent_change ASC LIMIT 5"
            )
            if top_gainers:
                gs = ", ".join(f"{r['symbol']} ({r['percent_change']:+.1f}%)" for r in top_gainers)
                context_parts.append(f"Top gainers today: {gs}")
                real_names.extend(r["symbol"] for r in top_gainers)
                real_names.extend(str(r.get("display_name") or "") for r in top_gainers)
            if top_losers:
                ls = ", ".join(f"{r['symbol']} ({r['percent_change']:+.1f}%)" for r in top_losers)
                context_parts.append(f"Top losers today: {ls}")
                real_names.extend(r["symbol"] for r in top_losers)
                real_names.extend(str(r.get("display_name") or "") for r in top_losers)
        except Exception:
            pass

        # ── 4. Real open/upcoming IPOs ─────────────────────────────
        try:
            from app.services.ipo_service import get_ipos
            ipo_data = await get_ipos(status="open", limit=5)
            open_ipos = [i for i in (ipo_data.get("items") or []) if i.get("company_name")]
            if not open_ipos:
                ipo_data = await get_ipos(status="upcoming", limit=5)
                open_ipos = [i for i in (ipo_data.get("items") or []) if i.get("company_name")]
            if open_ipos:
                names = [str(i["company_name"]) for i in open_ipos[:5]]
                context_parts.append(f"Current IPOs: {', '.join(names)}")
                real_names.extend(names)
        except Exception:
            pass

        # ── 5. User's watchlist ─────────────────────────────────────
        if device_id:
            try:
                pool_conn = await get_pool()
                rows = await pool_conn.fetch(
                    "SELECT asset FROM device_watchlists "
                    "WHERE device_id = $1 ORDER BY position ASC LIMIT 10",
                    device_id,
                )
                watchlist_symbols = [r["asset"] for r in rows if r.get("asset")]
                if watchlist_symbols:
                    context_parts.append(
                        f"User's watchlist: {', '.join(watchlist_symbols[:8])}"
                    )
                    real_names.extend(watchlist_symbols[:8])
            except Exception:
                pass

        # ── 6. Recent news headlines (from news_articles, top 5) ────
        try:
            pool_conn = await get_pool()
            news_rows = await pool_conn.fetch(
                "SELECT title, primary_entity FROM news_articles "
                "WHERE timestamp > NOW() - INTERVAL '24 hours' "
                "  AND title IS NOT NULL "
                "ORDER BY timestamp DESC LIMIT 5"
            )
            if news_rows:
                heads = [
                    f"\"{(r['title'] or '')[:90]}\""
                    for r in news_rows
                ]
                context_parts.append("Recent news:\n- " + "\n- ".join(heads))
        except Exception:
            pass

        # ── 7. Macro snapshot (latest inflation / repo / GDP) ──────
        try:
            pool_conn = await get_pool()
            macro_rows = await pool_conn.fetch(
                "SELECT indicator_name, country, value FROM macro_indicators "
                "WHERE country IN ('India', 'IN') "
                "  AND indicator_name IN ('inflation_cpi', 'repo_rate', 'gdp_growth') "
                "ORDER BY timestamp DESC LIMIT 6"
            )
            if macro_rows:
                seen: set[str] = set()
                parts = []
                for r in macro_rows:
                    ind = str(r["indicator_name"])
                    if ind in seen:
                        continue
                    seen.add(ind)
                    parts.append(f"{ind}={r['value']}")
                if parts:
                    context_parts.append("India macro: " + ", ".join(parts))
        except Exception:
            pass

        context_str = "\n".join(context_parts) if context_parts else "No additional context."

        # ── LLM call ────────────────────────────────────────────────
        api_key = _get_api_key()
        if not api_key:
            return _static_suggestions(ist_hour)

        # Build a crisp guardrail listing real names we want the model to use
        real_name_list = ", ".join(sorted(set(n for n in real_names if n)))[:500]

        system = (
            "You generate a POOL of exactly 30 short, diverse suggested "
            "prompts for an Indian market AI chatbot called Artha. These "
            "are questions a retail investor would type into the app. "
            "\n\n"
            "STRICT RULES:\n"
            "1. Every prompt MUST reference REAL data from the live context "
            "below. Never invent company names. Never use placeholders like "
            "'XYZ', 'ABC', 'Company X', 'Stock A'. If you don't know a real "
            "name, ask a generic market question instead.\n"
            "2. Max 12 words per prompt. Conversational tone, no instructional "
            "verbs like 'Ask for...' or 'Check...'. Write as if the user is "
            "typing the question.\n"
            "3. English only, first-person, no numbering, no bullets.\n"
            "4. DIVERSE: mix stocks, mutual funds, macro, IPOs, tax, "
            "commodities, crypto, news reactions, sector views, watchlist "
            "references.\n"
            "5. Output exactly 30 prompts, one per line. No headings."
        )
        user_msg = (
            f"Live market context (use only these real names):\n{context_str}\n\n"
        )
        if real_name_list:
            user_msg += (
                f"Approved entity names (pick from these for stock/IPO "
                f"mentions): {real_name_list}\n\n"
            )
        user_msg += "Generate 30 diverse prompts NOW (one per line):"

        prompt_messages = [
            {"role": "system", "content": system},
            {"role": "user", "content": user_msg},
        ]

        result = await _call_llm_blocking(api_key, prompt_messages, max_tokens=900)
        if not result:
            return _static_suggestions(ist_hour)

        raw_lines = [
            line.strip().lstrip("0123456789.-) *•")
            for line in result.strip().split("\n")
        ]
        # Filter: non-empty, minimum length, and reject obvious placeholders
        placeholder_markers = (
            "xyz", "abc ", "company x", "company y", "stock a", "stock b",
            "<company>", "<stock>", "[company]", "[stock]", "placeholder",
        )
        cleaned: list[str] = []
        for line in raw_lines:
            if not line or len(line) < 6:
                continue
            low = line.lower()
            if any(p in low for p in placeholder_markers):
                logger.debug("suggestions: filtered placeholder line: %r", line)
                continue
            cleaned.append(line)

        # Dedup (case-insensitive)
        seen_low: set[str] = set()
        unique: list[str] = []
        for line in cleaned:
            k = line.lower()
            if k in seen_low:
                continue
            seen_low.add(k)
            unique.append(line)

        if len(unique) >= 6:
            return unique[:_POOL_SIZE]

        return _static_suggestions(ist_hour)

    except Exception as e:
        logger.warning("suggestions: compute_llm failed: %s", e, exc_info=True)
        now = datetime.now(timezone.utc)
        ist_hour = (now.hour + 5) % 24 + (30 // 60)
        return _static_suggestions(ist_hour)


def _static_suggestions(ist_hour: int) -> list[str]:
    """Time-based static fallback suggestions."""
    if ist_hour < 9:
        return [
            "How is Gift Nifty today?",
            "Top 5 stocks by score",
            "Any upcoming IPOs?",
            "Best large cap mutual funds",
        ]
    elif ist_hour < 15:
        return [
            "Market summary",
            "How are my watchlist stocks doing?",
            "Top gainers today",
            "Compare TCS vs Infosys",
        ]
    elif ist_hour < 20:
        return [
            "How did the market close?",
            "Best performing sectors today",
            "IT stocks with ROE > 20%",
            "Gold price today",
        ]
    else:
        return [
            "Top 5 stocks by score",
            "Best SIP mutual funds",
            "Calculate LTCG tax on ₹5L profit",
            "India GDP growth trend",
        ]


# ---------------------------------------------------------------------------
# LLM streaming with tool execution
# ---------------------------------------------------------------------------

async def stream_chat_response(
    device_id: str,
    session_id: str,
    user_message: str,
) -> AsyncGenerator[dict, None]:
    """Stream chat response as SSE events.

    Yields dicts with 'event' and 'data' keys:
    - {"event": "thinking", "data": {"status": "..."}}
    - {"event": "token", "data": {"text": "..."}}
    - {"event": "stock_card", "data": {...}}
    - {"event": "mf_card", "data": {...}}
    - {"event": "done", "data": {"message_id": "...", "session_id": "..."}}
    - {"event": "error", "data": {"message": "...", "retry": bool}}
    """
    stream_start = time.monotonic()
    device_tag = (device_id or "")[:16]
    logger.info(
        "chat_stream: START device=%s session=%s msg_len=%d preview=%r",
        device_tag, session_id[:12], len(user_message or ""),
        (user_message or "")[:80],
    )

    api_key = _get_api_key()
    if not api_key:
        logger.error("chat_stream: no OPENROUTER_API_KEY configured")
        yield {"event": "error", "data": {"message": "AI service is temporarily unavailable.", "retry": True}}
        return

    # Save user message
    try:
        await save_message(session_id, "user", user_message)
        await increment_rate_limit(device_id)
    except Exception:
        logger.exception("chat_stream: failed to save user message or update rate limit")
        yield {"event": "error", "data": {"message": "Could not save your message. Please try again.", "retry": True}}
        return

    # Build conversation context
    messages = await _build_context(session_id, device_id)

    yield {"event": "thinking", "data": {"status": "Artha is thinking..."}}

    # ─────────────────────────────────────────────────────────────────
    # PHASE 1 — Intent + tool detection (FAST model)
    # ─────────────────────────────────────────────────────────────────
    # The first call only needs to decide "which tool(s) should I
    # invoke?" and emit [TOOL:...] markers. A small 20B model handles
    # this reliably in ~2-3s. We reserve the slow 120B model for the
    # final answer composition where writing quality matters.
    initial_response = await _call_llm_blocking(
        api_key, messages, chain=_FAST_MODELS,
    )
    if not initial_response:
        yield {"event": "error", "data": {"message": "Artha is taking a break. Try again in a few minutes.", "retry": True}}
        return

    # ─────────────────────────────────────────────────────────────────
    # PHASE 2 — Parse and execute tools (with retry loop)
    # ─────────────────────────────────────────────────────────────────
    tool_markers = _TOOL_PATTERN.findall(initial_response)
    tool_results: dict[str, Any] = {}
    stock_cards: list[dict] = []
    mf_cards: list[dict] = []
    tools_used: list[dict] = []

    async def _run_tool_markers(markers: list[tuple[str, str]]) -> bool:
        """Execute a batch of [TOOL:...] markers. Returns True if at
        least one tool errored (so caller can decide to retry)."""
        nonlocal tool_results, stock_cards, mf_cards, tools_used
        any_error = False
        for tool_name, params_str in markers:
            try:
                params = json.loads(params_str)
            except json.JSONDecodeError:
                params = {}
            result = await _execute_tool(tool_name, params, device_id)
            if isinstance(result, dict) and "error" not in result:
                # Build a compact shape summary for the INFO log. Bug fix:
                # the previous dict comprehension referenced `v` which was
                # never defined (`for k in (...)` iterates only `k`), so
                # every tool call crashed with NameError and the whole
                # stream aborted with "Something went wrong".
                _SUMMARY_KEYS = (
                    "stocks", "funds", "articles", "ipos",
                    "indices_by_region", "data", "commodities", "crypto",
                    "series", "sectors", "top_gainers", "top_losers",
                )
                summary: dict[str, Any] = {}
                for k in _SUMMARY_KEYS:
                    if k not in result:
                        continue
                    val = result[k]
                    summary[k] = len(val) if isinstance(val, list) else "obj"
                logger.info(
                    "tool_call: OK name=%s summary=%s",
                    tool_name, summary or "{scalar}",
                )
            else:
                logger.info("tool_call: ERROR_RESULT name=%s error=%s", tool_name, result.get("error"))
                any_error = True
            tool_results[tool_name] = result
            tools_used.append({"tool": tool_name, "params": params})

            # Card extraction
            if tool_name in ("stock_lookup", "stock_screen", "stock_compare", "watchlist"):
                stocks = result.get("stocks", [])
                if not stocks and "symbol" in result:
                    stocks = [result]
                for s in stocks[:5]:
                    if s.get("symbol"):
                        stock_cards.append({
                            "symbol": s.get("symbol"),
                            "display_name": s.get("display_name", s.get("symbol")),
                            "sector": s.get("sector"),
                            "last_price": s.get("last_price"),
                            "percent_change": s.get("percent_change"),
                            "score": s.get("score"),
                            "market_cap": s.get("market_cap"),
                        })
            if tool_name in ("mf_lookup", "mf_screen"):
                funds = result.get("funds", [])
                if not funds and "scheme_code" in result:
                    funds = [result]
                for f in funds[:5]:
                    if f.get("scheme_code"):
                        mf_cards.append({
                            "scheme_code": f.get("scheme_code"),
                            "scheme_name": f.get("scheme_name"),
                            "display_name": f.get("scheme_name"),
                            "category": f.get("category"),
                            "nav": f.get("nav"),
                            "returns_1y": f.get("returns_1y"),
                            "score": f.get("score"),
                        })
        return any_error

    if tool_markers:
        yield {"event": "thinking", "data": {"status": "Querying data..."}}
        had_error = await _run_tool_markers(tool_markers)

        # ── Retry loop: if any tool errored, feed the errors back to
        # the fast model ONCE and let it regenerate the tool call.
        # Catches LLM mistakes like wrong column names that our
        # whitelist/alias layer couldn't auto-fix.
        if had_error:
            error_summary_lines = []
            for tn, tr in tool_results.items():
                if isinstance(tr, dict) and "error" in tr:
                    error_summary_lines.append(f"[{tn}] → {tr['error']}")
            if error_summary_lines:
                error_feedback = "\n".join(error_summary_lines)
                logger.info("tool_retry: feeding errors back to LLM: %s", error_feedback[:400])
                retry_messages = messages + [
                    {"role": "assistant", "content": initial_response},
                    {
                        "role": "user",
                        "content": (
                            "Some tools you called returned errors:\n"
                            f"{error_feedback}\n\n"
                            "Try the SAME request again, but FIX the tool call. "
                            "Pay attention to the valid column names mentioned in "
                            "the error. Use [TOOL:...] markers for the corrected calls. "
                            "If the tool simply has no data, proceed without it and "
                            "write your answer using whatever other data you have."
                        ),
                    },
                ]
                yield {"event": "thinking", "data": {"status": "Correcting query..."}}
                retry_response = await _call_llm_blocking(
                    api_key, retry_messages, chain=_FAST_MODELS,
                )
                if retry_response:
                    retry_markers = _TOOL_PATTERN.findall(retry_response)
                    if retry_markers:
                        logger.info(
                            "tool_retry: retrying %d tool calls", len(retry_markers),
                        )
                        # Merge: if a retry tool succeeds, overwrite the
                        # failed previous result. stock_cards/mf_cards
                        # accumulate as normal.
                        await _run_tool_markers(retry_markers)
                    # Update "initial_response" to the retry so phase 3
                    # includes the corrected thinking.
                    initial_response = retry_response

        # ── Phase 3: Compose final answer with SLOW model + REAL streaming
        tool_context = "\n\n--- Tool Results ---\n"
        for tn, tr in tool_results.items():
            tool_context += f"\n[{tn}]: {json.dumps(tr, default=str)[:2000]}\n"

        messages.append({"role": "assistant", "content": initial_response})
        messages.append({
            "role": "user",
            "content": (
                "Here are the tool results. Compose your FINAL response now.\n\n"
                "⚠️ CRITICAL: You have NO MORE TOOL CALLS available. "
                "Any [TOOL:...] markers you emit will be IGNORED and your "
                "response will look broken to the user. Work ONLY with the "
                "tool results shown below. If you need data you don't have, "
                "be honest: say 'I don't have that data right now. Want me "
                "to try fetching it?' and stop.\n\n"
                "FORMAT REQUIREMENTS (mandatory, non-negotiable):\n"
                "1. START with a `<thinking>...</thinking>` block (2-4 sentences "
                "explaining which data you're pulling from the tool results and "
                "how you'll structure the answer). Use the literal XML-style "
                "tags `<thinking>` and `</thinking>` — NOT `### Thinking` "
                "headings. The tags are parsed out by the client.\n"
                "2. Then write the actual answer AFTER `</thinking>` using the "
                "format rules for this query type (bullets / table / prose).\n"
                "3. END with a [SUGGESTIONS]...[/SUGGESTIONS] block containing "
                "EXACTLY 5 short follow-up questions (max 12 words each, "
                "first-person, no instructional verbs). Include the closing "
                "[/SUGGESTIONS] tag.\n"
                "4. Do NOT include any [TOOL:...] markers — they will be ignored.\n"
                "5. Be specific with the numbers from the tool results above.\n"
                "6. If a tool returned an error or empty data, be honest: say "
                "'I couldn't fetch X right now, want me to try again?' instead "
                "of inventing numbers.\n"
                "7. Currency rules: commodities (gold/silver/crude/brent) in "
                "USD ($). Indian stock prices / market cap in ₹. "
                "Index VALUES (Nifty, Sensex, S&P 500, Nasdaq, Nikkei, FTSE, "
                "DAX) have NO currency symbol — they are points, not money."
                f"{tool_context}"
            ),
        })

        yield {"event": "thinking", "data": {"status": "Composing response..."}}
        compose_messages = messages
    else:
        # No tools — stream the initial (fast model) response directly.
        # The fast model already composed a full answer.
        compose_messages = None

    # ─────────────────────────────────────────────────────────────────
    # REAL STREAMING PHASE (final answer)
    # ─────────────────────────────────────────────────────────────────
    # We stream tokens directly from OpenRouter's SSE endpoint. No more
    # fake sleep loop — pacing is naturally determined by the model.
    #
    # Cursor-based filtering strips [TOOL:...], [CARD:...], and the
    # [SUGGESTIONS]...[/SUGGESTIONS] block on-the-fly so the client
    # never sees the bracketed syntax leak into the chat bubble, even
    # as the tags land mid-chunk in the SSE stream.

    # Decide which model chain handles the final answer. Default is
    # fast; upgrade to slow only if the query needs deep analysis.
    needs_deep = _is_deep_thinking_needed(user_message, tool_results)
    compose_chain = _SLOW_MODELS if needs_deep else _FAST_MODELS
    logger.info(
        "chat_stream: compose_chain=%s (deep=%s, tools=%d, msg_len=%d)",
        "slow" if needs_deep else "fast",
        needs_deep,
        len(tool_results),
        len(user_message or ""),
    )

    async def _stream_final() -> AsyncGenerator[str, None]:
        """Yield raw token deltas from the appropriate model chain."""
        if compose_messages is not None:
            # Tools were used — compose via real OpenRouter streaming
            async for delta in _stream_llm_response(
                api_key, compose_messages, chain=compose_chain,
            ):
                yield delta
        else:
            # No tools — the fast model already wrote the answer. Fake
            # stream it word-by-word for UX parity with the real-stream
            # path. We can't re-stream the blocking call's output as
            # SSE from OpenRouter, so we split and emit.
            for chunk in re.split(r'(\s+)', initial_response):
                if chunk:
                    yield chunk
                    await asyncio.sleep(0.01)

    # ─────────────────────────────────────────────────────────────────
    # Cursor-based filter with THREE channels:
    #   * thinking_text events — content inside <thinking>...</thinking>
    #   * token events         — the actual user-facing answer
    #   * (suppressed)         — content inside [SUGGESTIONS]...[/SUGGESTIONS]
    #
    # The LLM's raw stream looks like:
    #   <thinking>plan goes here</thinking>
    #   Real answer body.
    #   [SUGGESTIONS]...[/SUGGESTIONS]
    #
    # We walk the accumulated text with a cursor. When we cross a tag
    # boundary we switch channels. [TOOL:...] and [CARD:...] markers are
    # left in the stream and stripped post-hoc.
    # ─────────────────────────────────────────────────────────────────
    raw_accum = ""
    yielded_up_to = 0

    # Channels: 'answer' (default), 'thinking', 'suppressed' (suggestions).
    channel = "answer"

    # Longest sentinel we need to hold back at the tail of each iteration
    # so we don't emit a partial tag prefix that would complete next delta.
    _THINK_START = "<thinking>"
    _THINK_END = "</thinking>"
    _SUG_START = "[SUGGESTIONS]"
    _SUG_END = "[/SUGGESTIONS]"
    _HOLDBACK_LEN = max(
        len(_THINK_START), len(_THINK_END),
        len(_SUG_START), len(_SUG_END),
    )

    def _emit(event_name: str, text: str):
        if not text:
            return None
        return {"event": event_name, "data": {"text": text}}

    async for delta in _stream_final():
        if not delta:
            continue
        raw_accum += delta

        # Iteratively consume the accumulated buffer, switching channels
        # whenever we hit a tag boundary. Each iteration either advances
        # yielded_up_to to a safe point or breaks out to wait for more.
        while yielded_up_to < len(raw_accum):
            if channel == "suppressed":
                # Skip everything until [/SUGGESTIONS]
                end_idx = raw_accum.find(_SUG_END, yielded_up_to)
                if end_idx >= 0:
                    yielded_up_to = end_idx + len(_SUG_END)
                    channel = "answer"
                    continue
                else:
                    break  # need more input

            if channel == "thinking":
                # Emit thinking_text up to </thinking> (or safe point)
                end_idx = raw_accum.find(_THINK_END, yielded_up_to)
                if end_idx >= 0:
                    if end_idx > yielded_up_to:
                        emitted = _emit(
                            "thinking_text",
                            raw_accum[yielded_up_to:end_idx],
                        )
                        if emitted:
                            yield emitted
                    yielded_up_to = end_idx + len(_THINK_END)
                    channel = "answer"
                    continue
                # No end tag yet — emit up to holdback boundary
                safe_end = max(yielded_up_to, len(raw_accum) - _HOLDBACK_LEN)
                if safe_end > yielded_up_to:
                    emitted = _emit(
                        "thinking_text",
                        raw_accum[yielded_up_to:safe_end],
                    )
                    if emitted:
                        yield emitted
                    yielded_up_to = safe_end
                break  # need more input

            # channel == "answer": check for next boundary (<thinking> or [SUGGESTIONS])
            think_idx = raw_accum.find(_THINK_START, yielded_up_to)
            sug_idx = raw_accum.find(_SUG_START, yielded_up_to)
            # Pick whichever comes first (if any)
            candidates = [
                (idx, tag, new_channel)
                for idx, tag, new_channel in [
                    (think_idx, _THINK_START, "thinking"),
                    (sug_idx, _SUG_START, "suppressed"),
                ]
                if idx >= 0
            ]
            if candidates:
                candidates.sort(key=lambda x: x[0])
                boundary_idx, boundary_tag, new_channel = candidates[0]
                # Emit answer text up to the boundary
                if boundary_idx > yielded_up_to:
                    emitted = _emit(
                        "token",
                        raw_accum[yielded_up_to:boundary_idx],
                    )
                    if emitted:
                        yield emitted
                yielded_up_to = boundary_idx + len(boundary_tag)
                channel = new_channel
                continue

            # No boundary found — emit up to holdback boundary
            safe_end = max(yielded_up_to, len(raw_accum) - _HOLDBACK_LEN)
            if safe_end > yielded_up_to:
                emitted = _emit("token", raw_accum[yielded_up_to:safe_end])
                if emitted:
                    yield emitted
                yielded_up_to = safe_end
            break  # need more input

    # Stream is done — flush any tail that's still buffered.
    if yielded_up_to < len(raw_accum):
        tail = raw_accum[yielded_up_to:]
        if channel == "answer":
            emitted = _emit("token", tail)
            if emitted:
                yield emitted
        elif channel == "thinking":
            emitted = _emit("thinking_text", tail)
            if emitted:
                yield emitted
        # suppressed: drop
        yielded_up_to = len(raw_accum)

    # Build the full response for DB save + suggestion extraction.
    final_response = raw_accum
    if not final_response:
        final_response = "I couldn't generate a response. Please try again."

    # Clean: strip <thinking> blocks, tool markers, card markers.
    # The <thinking> content has already been streamed as thinking_text
    # SSE events — the saved DB content should have the answer only.
    final_response = _clean_response(final_response)
    final_response = re.sub(
        r"<thinking>.*?</thinking>", "", final_response, flags=re.DOTALL,
    ).strip()
    final_response = _TOOL_PATTERN.sub("", final_response).strip()
    final_response = _CARD_PATTERN.sub("", final_response).strip()

    # Extract follow-up suggestions (max 5 chips, max 12 words each).
    follow_ups = []
    suggestions_match = _SUGGESTIONS_PATTERN.search(final_response)
    if suggestions_match:
        raw = suggestions_match.group(1).strip()
        for line in raw.split("\n"):
            cleaned = line.lstrip("- *•").strip().strip('"').strip()
            if not cleaned or cleaned == "-":
                continue
            words = cleaned.split()
            if len(words) > 12:
                cleaned = " ".join(words[:12]).rstrip(",.;:") + "…"
            follow_ups.append(cleaned)
            if len(follow_ups) >= 5:
                break
        final_response = _SUGGESTIONS_PATTERN.sub("", final_response).strip()
        if follow_ups:
            logger.info(
                "chat_stream: follow_ups extracted count=%d preview=%r",
                len(follow_ups), follow_ups,
            )

    # Fallback: if the LLM forgot to emit [SUGGESTIONS], generate follow-ups
    # with a tiny secondary fast-model call. Happens most often on phase-1
    # no-tool responses where the small model forgets the tail instructions.
    if not follow_ups and final_response and len(final_response) > 40:
        try:
            fallback_messages = [
                {
                    "role": "system",
                    "content": (
                        "You generate exactly 5 short follow-up questions that a "
                        "user would ask after reading an Indian-market chatbot "
                        "response. Rules:\n"
                        "- Max 12 words each\n"
                        "- First-person (what the user types), e.g. 'Compare TCS "
                        "with Infosys'\n"
                        "- Reference specific names/numbers from the response\n"
                        "- No instructional verbs ('Ask for', 'Check', 'Get', 'See')\n"
                        "- Output ONLY the 5 questions, one per line, no numbering, "
                        "no bullets, no extra text"
                    ),
                },
                {
                    "role": "user",
                    "content": (
                        f"User asked: {user_message}\n\n"
                        f"Chatbot replied:\n{final_response[:1500]}\n\n"
                        "Generate 5 follow-up questions now:"
                    ),
                },
            ]
            fallback_result = await _call_llm_blocking(
                api_key, fallback_messages, max_tokens=200, chain=_FAST_MODELS,
            )
            if fallback_result:
                for line in fallback_result.strip().split("\n"):
                    cleaned = line.lstrip("- *•0123456789.)").strip().strip('"').strip()
                    if not cleaned or len(cleaned) < 5:
                        continue
                    words = cleaned.split()
                    if len(words) > 12:
                        cleaned = " ".join(words[:12]).rstrip(",.;:") + "…"
                    follow_ups.append(cleaned)
                    if len(follow_ups) >= 5:
                        break
                if follow_ups:
                    logger.info(
                        "chat_stream: follow_ups generated via fallback count=%d",
                        len(follow_ups),
                    )
        except Exception:
            logger.debug("chat_stream: follow-up fallback failed", exc_info=True)

    # Send stock cards
    for card in stock_cards[:5]:
        yield {"event": "stock_card", "data": card}

    # Send MF cards
    for card in mf_cards[:5]:
        yield {"event": "mf_card", "data": card}

    # Send follow-up suggestions
    if follow_ups:
        yield {"event": "suggestions", "data": {"suggestions": follow_ups}}

    # Save assistant message
    try:
        msg_id = await save_message(
            session_id,
            "assistant",
            final_response,
            stock_cards=stock_cards if stock_cards else None,
            mf_cards=mf_cards if mf_cards else None,
            tool_calls=tools_used if tools_used else None,
        )
    except Exception:
        logger.exception("chat_stream: failed to save assistant message")
        msg_id = None

    total_elapsed_ms = (time.monotonic() - stream_start) * 1000
    logger.info(
        "chat_stream: DONE device=%s session=%s msg_id=%s tools=%d "
        "response_len=%d stock_cards=%d mf_cards=%d follow_ups=%d "
        "elapsed_ms=%.0f",
        device_tag, session_id[:12], (msg_id or "")[:12],
        len(tools_used), len(final_response or ""),
        len(stock_cards), len(mf_cards), len(follow_ups),
        total_elapsed_ms,
    )

    yield {"event": "done", "data": {"message_id": msg_id, "session_id": session_id}}


# ──────────────────────────────────────────────────────────────────
# PREFETCH CACHE — live market data refreshed every 30s
# ──────────────────────────────────────────────────────────────────
#
# Goal: queries like "market status", "top gainers", "how's the
# market" are answered with zero tool calls — the LLM reads the
# injected snapshot in the system prompt. Cuts latency dramatically
# for the most common interactive queries.
#
# A background task refreshes this cache every 30s. The snapshot is
# formatted as a compact, LLM-friendly string and prepended to every
# system prompt via _build_system_prompt().

_prefetch_cache: dict[str, Any] = {
    "snapshot": None,         # pre-rendered markdown block
    "updated_at": 0.0,        # epoch seconds of last refresh
    "indices": {},            # {asset_name: {price, change_pct, prev_close}}
    "top_gainers": [],        # [{symbol, name, change_pct, price}, ...]
    "top_losers": [],
    "fx_majors": {},          # {pair: {price, change_pct}}
    "commodities": {},        # {name: {price, change_pct}}
    "market_hours": {},       # {india_open, us_open, ...}
}
_PREFETCH_INTERVAL = 30        # seconds between background refreshes
_PREFETCH_STALE_AFTER = 120    # treat as stale after 2 minutes
_prefetch_task: asyncio.Task | None = None


async def _refresh_prefetch_cache_once() -> None:
    """Pull the live market snapshot in one go and populate the cache."""
    try:
        t0 = time.monotonic()
        pool = await get_pool()

        # 1. Indices (Nifty 50, Sensex, Bank, IT, + global benchmarks)
        from app.services.market_service import get_latest_prices
        index_rows = await get_latest_prices(instrument_type="index")
        indices: dict[str, dict] = {}
        _WATCH_INDICES = {
            "nifty 50", "sensex", "nifty bank", "nifty it",
            "nifty auto", "nifty pharma", "nifty midcap 150",
            "gift nifty", "india vix",
            "s&p500", "nasdaq", "dow jones",
            "ftse 100", "dax",
            "nikkei 225",
        }
        for p in index_rows or []:
            name = str(p.get("asset") or "")
            if name.lower() in _WATCH_INDICES and p.get("price") is not None:
                indices[name] = {
                    "price": float(p.get("price") or 0),
                    "change_pct": float(p.get("change_percent") or 0),
                    "prev_close": p.get("previous_close"),
                }

        # 2. Top 5 gainers + top 5 losers (NSE universe)
        gain_rows = await pool.fetch(
            "SELECT symbol, display_name, percent_change, last_price "
            "FROM discover_stock_snapshots "
            "WHERE percent_change IS NOT NULL "
            "ORDER BY percent_change DESC LIMIT 5"
        )
        lose_rows = await pool.fetch(
            "SELECT symbol, display_name, percent_change, last_price "
            "FROM discover_stock_snapshots "
            "WHERE percent_change IS NOT NULL "
            "ORDER BY percent_change ASC LIMIT 5"
        )
        top_gainers = [
            {
                "symbol": r["symbol"],
                "name": r["display_name"],
                "change_pct": float(r["percent_change"] or 0),
                "price": float(r["last_price"] or 0),
            }
            for r in gain_rows
        ]
        top_losers = [
            {
                "symbol": r["symbol"],
                "name": r["display_name"],
                "change_pct": float(r["percent_change"] or 0),
                "price": float(r["last_price"] or 0),
            }
            for r in lose_rows
        ]

        # 3. FX majors
        fx_rows = await get_latest_prices(instrument_type="currency")
        _FX_TARGETS = {"usd/inr", "eur/inr", "gbp/inr", "jpy/inr"}
        fx_majors: dict[str, dict] = {}
        for p in fx_rows or []:
            name = str(p.get("asset") or "")
            if name.lower() in _FX_TARGETS and p.get("price") is not None:
                fx_majors[name] = {
                    "price": float(p.get("price") or 0),
                    "change_pct": float(p.get("change_percent") or 0),
                }

        # 4. Key commodities
        com_rows = await get_latest_prices(instrument_type="commodity")
        _COM_TARGETS = {"gold", "silver", "crude oil", "brent crude"}
        commodities: dict[str, dict] = {}
        for p in com_rows or []:
            name = str(p.get("asset") or "")
            if name.lower() in _COM_TARGETS and p.get("price") is not None:
                commodities[name] = {
                    "price": float(p.get("price") or 0),
                    "change_pct": float(p.get("change_percent") or 0),
                }

        # 5. Market hours
        from app.services.market_service import get_market_status
        status = get_market_status(datetime.now(timezone.utc))
        market_hours = {
            "india_open": bool(status.get("nse_open")),
            "us_open": bool(status.get("nyse_open")),
            "europe_open": bool(status.get("europe_open")),
            "japan_open": bool(status.get("japan_open")),
            "gift_nifty_open": bool(status.get("gift_nifty_open")),
        }

        # 6. Render compact snapshot
        snapshot = _render_prefetch_snapshot(
            indices, top_gainers, top_losers, fx_majors, commodities,
            market_hours,
        )

        _prefetch_cache.update({
            "snapshot": snapshot,
            "updated_at": time.time(),
            "indices": indices,
            "top_gainers": top_gainers,
            "top_losers": top_losers,
            "fx_majors": fx_majors,
            "commodities": commodities,
            "market_hours": market_hours,
        })
        elapsed_ms = (time.monotonic() - t0) * 1000
        logger.debug(
            "prefetch: refreshed indices=%d gainers=%d losers=%d "
            "fx=%d commodities=%d elapsed_ms=%.0f",
            len(indices), len(top_gainers), len(top_losers),
            len(fx_majors), len(commodities), elapsed_ms,
        )
    except Exception as e:
        logger.warning("prefetch: refresh failed: %s", e, exc_info=True)


def _render_prefetch_snapshot(
    indices: dict,
    top_gainers: list,
    top_losers: list,
    fx_majors: dict,
    commodities: dict,
    market_hours: dict,
) -> str:
    """Render the prefetch data as a compact markdown block suitable
    for injection into the system prompt. Aim for < 600 tokens."""
    now_utc = datetime.now(timezone.utc)
    now_ist = now_utc + timedelta(hours=5, minutes=30)
    ts = now_ist.strftime("%d %b %Y, %H:%M IST")

    lines = [f"**LIVE MARKET SNAPSHOT** — {ts}"]

    # Market hours
    hrs = market_hours
    status_bits = []
    status_bits.append(f"NSE {'OPEN' if hrs.get('india_open') else 'CLOSED'}")
    status_bits.append(f"NYSE {'OPEN' if hrs.get('us_open') else 'CLOSED'}")
    if hrs.get("gift_nifty_open"):
        status_bits.append("Gift Nifty LIVE")
    lines.append(f"Markets: {', '.join(status_bits)}")
    lines.append("")

    # Indices
    if indices:
        lines.append("**Indices:**")
        # Sort: Nifty 50, Sensex first, then Indian, then global
        _priority = {
            "Nifty 50": 0, "Sensex": 1, "Nifty Bank": 2, "Nifty IT": 3,
            "Nifty Auto": 4, "Nifty Pharma": 5, "Nifty Midcap 150": 6,
            "India VIX": 7, "Gift Nifty": 8,
            "S&P500": 10, "NASDAQ": 11, "Dow Jones": 12,
            "Nikkei 225": 20, "FTSE 100": 30, "DAX": 31,
        }
        sorted_names = sorted(indices.keys(), key=lambda k: _priority.get(k, 99))
        for name in sorted_names:
            data = indices[name]
            sign = "+" if data["change_pct"] >= 0 else ""
            # Index values are POINTS, not currency — no ₹/$ prefix.
            lines.append(
                f"- {name}: {data['price']:,.2f} ({sign}{data['change_pct']:.2f}%)"
            )
        lines.append("")

    # Top movers
    if top_gainers:
        lines.append("**Top Gainers (NSE):**")
        for g in top_gainers[:5]:
            lines.append(
                f"- {g['symbol']} ({g['name']}): ₹{g['price']:,.2f} (+{g['change_pct']:.2f}%)"
            )
        lines.append("")
    if top_losers:
        lines.append("**Top Losers (NSE):**")
        for l in top_losers[:5]:
            lines.append(
                f"- {l['symbol']} ({l['name']}): ₹{l['price']:,.2f} ({l['change_pct']:+.2f}%)"
            )
        lines.append("")

    # FX
    if fx_majors:
        pairs = []
        for pair, data in fx_majors.items():
            sign = "+" if data["change_pct"] >= 0 else ""
            pairs.append(f"{pair} ₹{data['price']:.2f} ({sign}{data['change_pct']:.2f}%)")
        lines.append("**FX:** " + " | ".join(pairs))

    # Commodities
    if commodities:
        items = []
        for name, data in commodities.items():
            sign = "+" if data["change_pct"] >= 0 else ""
            items.append(f"{name.title()} ${data['price']:,.2f} ({sign}{data['change_pct']:.2f}%)")
        lines.append("**Commodities:** " + " | ".join(items))

    return "\n".join(lines)


async def _prefetch_refresh_loop() -> None:
    """Forever-loop task that refreshes the prefetch cache every 30s."""
    while True:
        await _refresh_prefetch_cache_once()
        await asyncio.sleep(_PREFETCH_INTERVAL)


def start_prefetch_task() -> None:
    """Kick off the background prefetch loop. Idempotent."""
    global _prefetch_task
    if _prefetch_task is not None and not _prefetch_task.done():
        return
    try:
        loop = asyncio.get_running_loop()
        _prefetch_task = loop.create_task(_prefetch_refresh_loop())
        logger.info("prefetch: background task started (interval=%ds)", _PREFETCH_INTERVAL)
    except RuntimeError:
        logger.warning("prefetch: no running loop, cannot start task")


async def _build_user_profile_context(device_id: str) -> str | None:
    """Build a compact USER CONTEXT block for the device.
    Returns None if the device has no meaningful context."""
    try:
        pool = await get_pool()
        # Watchlist
        wl_rows = await pool.fetch(
            "SELECT asset FROM device_watchlists "
            "WHERE device_id = $1 ORDER BY position ASC LIMIT 10",
            device_id,
        )
        watchlist = [r["asset"] for r in wl_rows if r.get("asset")]

        # Recent topics — peek at the last few user messages across
        # sessions to see what the user has been asking about.
        topic_rows = await pool.fetch(
            "SELECT LEFT(content, 80) AS preview "
            "FROM chat_messages m "
            "JOIN chat_sessions s ON s.id = m.session_id "
            "WHERE s.device_id = $1 AND m.role = 'user' "
            "ORDER BY m.created_at DESC LIMIT 5",
            device_id,
        )
        recent_topics = [r["preview"] for r in topic_rows if r.get("preview")]

        # Session count today
        today_ist = (datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)).date()
        count_row = await pool.fetchrow(
            "SELECT COUNT(*) AS c FROM chat_sessions "
            "WHERE device_id = $1 AND created_at::date = $2",
            device_id, today_ist,
        )
        sessions_today = int(count_row["c"] or 0) if count_row else 0

        if not watchlist and not recent_topics and sessions_today <= 1:
            return None  # Nothing meaningful to inject

        parts = ["**USER CONTEXT:**"]
        if watchlist:
            parts.append(f"Watchlist: {', '.join(watchlist[:10])}")
        if recent_topics:
            topics_str = " | ".join(f'"{t}"' for t in recent_topics[:3])
            parts.append(f"Last 3 queries: {topics_str}")
        if sessions_today > 1:
            parts.append(f"Sessions today: {sessions_today}")
        return "\n".join(parts)
    except Exception:
        logger.debug("profile: build failed", exc_info=True)
        return None


def _build_system_prompt(
    live_snapshot: str | None,
    user_profile: str | None,
) -> str:
    """Assemble the final system prompt with dynamically-injected context.

    Layers (top-down, so the most critical state is freshest to the model):
      1. Live market snapshot (indices, movers, hours) — refreshed every 30s
      2. User profile (watchlist, recent topics, sessions today)
      3. The static _ARTHA_SYSTEM base prompt (role, tools, rules, etc.)
    """
    parts: list[str] = []
    if live_snapshot:
        parts.append(live_snapshot)
        parts.append("")
    if user_profile:
        parts.append(user_profile)
        parts.append("")
    parts.append(_ARTHA_SYSTEM)
    return "\n".join(parts)


async def _build_context(session_id: str, device_id: str) -> list[dict]:
    """Build LLM message context from session history with dynamic injections.

    Pre-pends the dynamic system prompt (live snapshot + user profile +
    base rules), then appends the last N messages from the session.
    Uses a sliding-window policy: anything older than MAX_CONTEXT_MESSAGES
    is dropped (not summarised).
    """
    # Dynamic system prompt with live data
    live_snapshot = _prefetch_cache.get("snapshot")
    user_profile = await _build_user_profile_context(device_id)
    system_prompt = _build_system_prompt(live_snapshot, user_profile)

    messages = [{"role": "system", "content": system_prompt}]

    # Get last N messages from this session (sliding window)
    pool = await get_pool()
    rows = await pool.fetch(
        """
        SELECT role, content FROM chat_messages
        WHERE session_id = $1
        ORDER BY created_at DESC
        LIMIT $2
        """,
        session_id,
        MAX_CONTEXT_MESSAGES,
    )

    # Reverse to chronological order
    for row in reversed(rows):
        messages.append({"role": row["role"], "content": row["content"]})

    return messages


async def _call_llm_blocking(
    api_key: str,
    messages: list[dict],
    max_tokens: int | None = None,
    *,
    chain: list[str] | None = None,
) -> str | None:
    """Call OpenRouter LLM (blocking, not streaming). Returns text or None.

    Args:
        chain: which model chain to use. None = fall back to the
               legacy _MODELS list. Typical usage:
                  _FAST_MODELS  — intent/tool detection, suggestions
                  _SLOW_MODELS  — final answer composition
    """
    models = chain or _MODELS
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://econatlas.com",
        "X-Title": "EconAtlas Artha",
    }

    for model in models:
        try:
            async with httpx.AsyncClient(timeout=STREAM_TIMEOUT) as client:
                t0 = time.monotonic()
                resp = await client.post(
                    _OPENROUTER_BASE,
                    headers=headers,
                    json={
                        "model": model,
                        "max_tokens": max_tokens or MAX_TOKENS_CHAT,
                        "temperature": 0.7,
                        "messages": messages,
                    },
                )
                if resp.status_code == 429:
                    logger.info("llm: rate-limited on %s, trying next", model)
                    continue
                resp.raise_for_status()
                data = resp.json()
                content = data["choices"][0]["message"].get("content")
                if not content:
                    continue
                text = content.strip()
                if not text:
                    continue
                elapsed_ms = (time.monotonic() - t0) * 1000
                logger.info(
                    "llm: %s OK len=%d elapsed_ms=%.0f",
                    model, len(text), elapsed_ms,
                )
                return text
        except Exception:
            logger.info("llm: %s FAILED", model, exc_info=True)
            continue

    logger.warning("llm: all models failed for chain=%s", [m.split('/')[-1] for m in models])
    return None


async def _stream_llm_response(
    api_key: str,
    messages: list[dict],
    max_tokens: int | None = None,
    *,
    chain: list[str] | None = None,
) -> AsyncGenerator[str, None]:
    """Stream tokens from OpenRouter as they arrive.

    This is REAL streaming — we use OpenRouter's native SSE support
    (the `stream: true` flag) instead of the old fake typewriter
    loop. Tokens are yielded as the model generates them, giving a
    genuine ChatGPT-style experience with natural pacing determined
    by the model itself, not by artificial sleeps.

    Yields one string per partial token. The string may be empty if
    the chunk is a control frame. On full failure, falls through to
    a blocking call at the end and yields the whole response at once
    as a safety net — callers still get SOMETHING.
    """
    models = chain or _MODELS
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://econatlas.com",
        "X-Title": "EconAtlas Artha",
        "Accept": "text/event-stream",
    }

    for model in models:
        any_tokens = False
        try:
            async with httpx.AsyncClient(timeout=STREAM_TIMEOUT) as client:
                t0 = time.monotonic()
                async with client.stream(
                    "POST",
                    _OPENROUTER_BASE,
                    headers=headers,
                    json={
                        "model": model,
                        "max_tokens": max_tokens or MAX_TOKENS_CHAT,
                        "temperature": 0.7,
                        "stream": True,
                        "messages": messages,
                    },
                ) as resp:
                    if resp.status_code == 429:
                        logger.info("llm-stream: rate-limited on %s, trying next", model)
                        continue
                    if resp.status_code >= 400:
                        logger.info(
                            "llm-stream: HTTP %d on %s, trying next",
                            resp.status_code, model,
                        )
                        continue
                    async for raw_line in resp.aiter_lines():
                        if not raw_line:
                            continue
                        line = raw_line.strip()
                        if not line.startswith("data:"):
                            continue
                        payload = line[5:].strip()
                        if payload == "[DONE]":
                            continue
                        try:
                            frame = json.loads(payload)
                        except json.JSONDecodeError:
                            continue
                        delta = (
                            frame.get("choices", [{}])[0]
                            .get("delta", {})
                            .get("content")
                        )
                        if delta:
                            any_tokens = True
                            yield delta
                    if any_tokens:
                        elapsed_ms = (time.monotonic() - t0) * 1000
                        logger.info(
                            "llm-stream: %s OK elapsed_ms=%.0f",
                            model, elapsed_ms,
                        )
                        return
        except Exception:
            logger.info("llm-stream: %s FAILED", model, exc_info=True)
            continue

    # Fallback: blocking call on the same chain, yield the whole response
    logger.warning("llm-stream: all streaming attempts failed, falling back to blocking")
    text = await _call_llm_blocking(api_key, messages, max_tokens=max_tokens, chain=chain)
    if text:
        yield text


# ---------------------------------------------------------------------------
# Autocomplete
# ---------------------------------------------------------------------------

async def autocomplete(query: str, limit: int = 8) -> list[dict]:
    """Search stocks and MFs for autocomplete. Returns mixed results."""
    pool = await get_pool()
    q = f"%{query.strip()}%"

    stock_rows = await pool.fetch(
        "SELECT symbol, display_name, score FROM discover_stock_snapshots "
        "WHERE symbol ILIKE $1 OR display_name ILIKE $1 "
        "ORDER BY score DESC NULLS LAST LIMIT $2",
        q,
        limit,
    )

    mf_rows = await pool.fetch(
        "SELECT scheme_code, scheme_name, score FROM discover_mutual_fund_snapshots "
        "WHERE scheme_name ILIKE $1 OR scheme_code ILIKE $1 "
        "ORDER BY score DESC NULLS LAST LIMIT $2",
        q,
        limit // 2,
    )

    results = []
    for r in stock_rows:
        results.append({
            "symbol": r["symbol"],
            "scheme_code": None,
            "name": r["display_name"],
            "type": "stock",
            "score": r["score"],
        })
    for r in mf_rows:
        results.append({
            "symbol": None,
            "scheme_code": r["scheme_code"],
            "name": r["scheme_name"],
            "type": "mf",
            "score": r["score"],
        })

    return results

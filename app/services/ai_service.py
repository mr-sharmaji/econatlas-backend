"""AI service — OpenRouter-backed LLM for narrative generation.

Uses free models (Nemotron 120B) for:
1. Stock story/narrative generation
2. Enriched notification content (market open/close)

All calls are non-blocking (httpx async). Failures are silent —
callers always get a fallback when AI is unavailable.
"""

import asyncio
import hashlib
import json
import logging
import os
import re
import time
from typing import Any

import httpx

logger = logging.getLogger(__name__)

_OPENROUTER_BASE = "https://openrouter.ai/api/v1/chat/completions"

# Model priority — try in order, first success wins
# Tested 12 free models; GPT-OSS has best quality (clean prose, no artifacts)
# Nemotron 120B is solid but sometimes echoes instructions
# Gemma 4 31B is fast but frequently rate-limited
_MODELS = [
    "openai/gpt-oss-120b:free",
    "openai/gpt-oss-20b:free",
    "nvidia/nemotron-3-super-120b-a12b:free",
    "google/gemma-4-31b-it:free",
]

_DEFAULT_TIMEOUT = 15.0  # seconds
_MAX_TOKENS = 300

# Simple in-memory cache: key → (response, timestamp)
_cache: dict[str, tuple[str, float]] = {}
_CACHE_TTL = 3600 * 6  # 6 hours


def _get_api_key() -> str | None:
    return os.environ.get("OPENROUTER_API_KEY")


def _cache_key(prompt: str, data_hash: str) -> str:
    raw = f"{prompt}:{data_hash}"
    return hashlib.md5(raw.encode()).hexdigest()


def _check_cache(key: str) -> str | None:
    entry = _cache.get(key)
    if entry is None:
        return None
    text, ts = entry
    if time.time() - ts > _CACHE_TTL:
        del _cache[key]
        return None
    return text


def _set_cache(key: str, text: str) -> None:
    # Evict oldest if cache grows too large
    if len(_cache) > 500:
        oldest_key = min(_cache, key=lambda k: _cache[k][1])
        del _cache[oldest_key]
    _cache[key] = (text, time.time())


def _convert_tables_to_bullets(text: str) -> str:
    """Convert markdown tables to compact bullet lists for mobile.

    Tables with 4+ columns are unreadable on phone screens (words
    wrap to 3-4 chars per column). Converts:
      | Symbol | Sector | Score | Change |
      |--------|--------|-------|--------|
      | TCS    | IT     | 78    | +1.2%  |
    To:
      - **TCS** — IT · Score 78 · +1.2%
    """
    lines = text.split("\n")
    result = []
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        # Detect table: starts with | and has multiple |
        if line.startswith("|") and line.count("|") >= 3:
            # Collect the full table
            table_lines = []
            while i < len(lines) and lines[i].strip().startswith("|"):
                table_lines.append(lines[i].strip())
                i += 1
            if len(table_lines) < 3:
                # Too short to be a real table, keep as-is
                result.extend(table_lines)
                continue
            # Parse header
            header = [c.strip() for c in table_lines[0].split("|")[1:-1]]
            # Skip separator row (|---|---|)
            data_start = 1
            if data_start < len(table_lines) and re.match(
                r"^\|[\s\-:|]+\|$", table_lines[data_start]
            ):
                data_start = 2
            # Convert each data row to a bullet
            for row_line in table_lines[data_start:]:
                cells = [c.strip() for c in row_line.split("|")[1:-1]]
                if not cells or all(not c for c in cells):
                    continue
                # First cell becomes bold label, rest joined with ·
                # Strip existing bold markers to avoid ****double****
                label = (cells[0] if cells else "").strip("*")
                details = []
                for j, cell in enumerate(cells[1:], 1):
                    if cell and j < len(header):
                        details.append(f"{header[j]} {cell}")
                    elif cell:
                        details.append(cell)
                if details:
                    result.append(f"- **{label}** — {' · '.join(details)}")
                else:
                    result.append(f"- **{label}**")
            result.append("")  # blank line after converted table
        else:
            result.append(lines[i])
            i += 1
    return "\n".join(result)


def _clean_response(raw: str) -> str:
    """Strip thinking artifacts and meta-commentary from LLM output.

    Some models (Nemotron) echo instructions like "We need to produce 2-3 sentences..."
    before the actual content. This strips that preamble.
    """
    text = raw.strip()

    # Remove <think>...</think> and <thinking>...</thinking> blocks
    text = re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL).strip()
    text = re.sub(r"<thinking>.*?</thinking>", "", text, flags=re.DOTALL).strip()

    # Remove leaked markdown thinking headings that weren't caught
    # by _normalize_thinking_markup (e.g. **Thinking:** or ### Thinking)
    text = re.sub(
        r"^\s*(?:#{1,6}\s*|\*{1,2})?[Tt]hinking(?:\*{1,2})?[:\s]*\n+",
        "", text,
    ).strip()

    # Convert markdown tables to bullet lists for mobile readability.
    # LLMs frequently emit tables despite prompt instructions. Tables
    # with 5+ columns are unreadable on phone screens (words wrap to
    # 3-4 chars per column). Convert each row to a compact bullet.
    text = _convert_tables_to_bullets(text)

    # Remove common preamble patterns from Nemotron-style models
    preamble_patterns = [
        r"^(?:We need to|I need to|Let me|Here's|Here is).*?[.!]\s*",
        r"^(?:Provide|Write|Produce|Create).*?[.:]\s*",
    ]
    for pattern in preamble_patterns:
        cleaned = re.sub(pattern, "", text, count=1).strip()
        # Only use cleaned version if substantial content remains
        if len(cleaned) > 20:
            text = cleaned
            break

    # Remove wrapping quotes if the model quoted its response
    if len(text) > 2 and text[0] == '"' and text[-1] == '"':
        text = text[1:-1].strip()

    return text


async def _call_llm(
    system_prompt: str,
    user_prompt: str,
    max_tokens: int = _MAX_TOKENS,
    temperature: float = 0.7,
) -> str | None:
    """Call OpenRouter LLM. Returns generated text or None on failure."""
    api_key = _get_api_key()
    if not api_key:
        logger.debug("AI service disabled — OPENROUTER_API_KEY not set")
        return None

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://econatlas.com",
        "X-Title": "EconAtlas",
    }

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]

    for model in _MODELS:
        try:
            async with httpx.AsyncClient(timeout=_DEFAULT_TIMEOUT) as client:
                resp = await client.post(
                    _OPENROUTER_BASE,
                    headers=headers,
                    json={
                        "model": model,
                        "max_tokens": max_tokens,
                        "temperature": temperature,
                        "messages": messages,
                    },
                )
                if resp.status_code == 429:
                    logger.debug("Rate limited on %s, trying next model", model)
                    continue
                resp.raise_for_status()
                data = resp.json()
                content = data["choices"][0]["message"].get("content")
                if not content:
                    logger.debug("Empty response from %s, trying next", model)
                    continue
                text = _clean_response(content)
                if not text:
                    continue
                logger.debug(
                    "AI response from %s (%d tokens)",
                    model,
                    data.get("usage", {}).get("total_tokens", 0),
                )
                return text
        except Exception:
            logger.debug("AI call failed for %s", model, exc_info=True)
            continue

    logger.warning("All AI models failed")
    return None


# ---------------------------------------------------------------------------
# Stock narrative
# ---------------------------------------------------------------------------

_STOCK_SYSTEM = """You are Artha, a concise Indian stock market analyst writing for retail investors.

Your input is a dense signal block covering 10 dimensions of the stock — action tag, valuation, quality, growth (3Y + YoY), balance sheet, ownership dynamics (FII/DII/promoter + pledge), technicals, per-layer score breakdown, analyst target and red flags. You will receive this data grouped by bucket with numeric values already formatted.

RULES
- Write exactly 2-3 short sentences (max 65 words total).
- Be specific — cite actual numbers from the data, not hand-wavy adjectives.
- Pick the 3-4 MOST MATERIAL facts across the buckets. Do not list one per bucket.
- Lead with the action-tag narrative if the confidence is high; otherwise lead with the strongest fundamental signal.
- Always mention ONE clear risk or counter-signal (red flag, pledge, FII selling, debt trend, momentum weakness, 52W near high, etc.). Never all-positive.
- No disclaimers, no "NFA", no "do your own research", no "consult an advisor".
- No bullet points, no headers, no markdown — flowing prose only.
- Plain English. Acronyms ROE/ROCE/FCF/FII/DII/PE are fine."""


def _fmt_pct(value: object, *, decimals: int = 1) -> str | None:
    """Format a raw number as a percent string. Handles both decimal
    (0.234 → "23.4%") and already-percentage (23.4 → "23.4%") inputs."""
    if value is None:
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    if abs(f) < 2:
        # Assume it's a decimal fraction.
        f *= 100
    return f"{f:.{decimals}f}%"


def _fmt_cr(value: object) -> str | None:
    """Format an INR crore value."""
    if value is None:
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    if abs(f) >= 1e5:
        return f"₹{f / 1e5:.1f}L Cr"
    return f"₹{f:,.0f} Cr"


def _fmt_num(value: object, *, decimals: int = 1, suffix: str = "") -> str | None:
    if value is None:
        return None
    try:
        return f"{float(value):.{decimals}f}{suffix}"
    except (TypeError, ValueError):
        return None


def _build_stock_prompt(stock_data: dict) -> str:
    """Build a multi-bucket prompt from the full stock snapshot.

    Emits ten grouped buckets — Artha is instructed to pick the 3-4
    most material facts across them, not one per bucket. Missing
    fields are silently dropped so the prompt length scales with
    data availability.
    """
    d = stock_data
    name = d.get("display_name") or d.get("symbol") or "this company"
    parts: list[str] = [f"Write a brief investment narrative for {name}."]
    parts.append("")
    parts.append("Sector: " + (d.get("sector") or "Unknown"))

    def _add_bucket(label: str, fields: list[tuple[str, str | None]]) -> None:
        rendered = [f"{lbl}={val}" for lbl, val in fields if val is not None and val != "None"]
        if rendered:
            parts.append(f"{label}: " + ", ".join(rendered))

    # ── 1. Action tag + confidence ─────────────────────────────
    _add_bucket("Action", [
        ("tag", d.get("action_tag")),
        ("confidence", d.get("score_confidence")),
        ("lynch", d.get("lynch_classification")),
        ("reasoning", str(d.get("action_tag_reasoning") or "")[:140] or None),
    ])

    # ── 2. Valuation ───────────────────────────────────────────
    target = d.get("analyst_target_mean")
    last = d.get("last_price")
    upside = None
    try:
        if target is not None and last is not None and float(last) > 0:
            upside = (float(target) - float(last)) / float(last) * 100
    except (TypeError, ValueError):
        pass
    _add_bucket("Valuation", [
        ("PE", _fmt_num(d.get("pe_ratio"), decimals=1, suffix="x")),
        ("P/B", _fmt_num(d.get("price_to_book"), decimals=1, suffix="x")),
        ("upside_vs_target",
         f"{upside:+.1f}%" if upside is not None else None),
        ("dividend_yield", _fmt_pct(d.get("dividend_yield"))),
    ])

    # ── 3. Quality / returns on capital ────────────────────────
    _add_bucket("Quality", [
        ("ROE", _fmt_pct(d.get("roe"))),
        ("ROCE", _fmt_pct(d.get("roce"))),
        ("gross_margin", _fmt_pct(d.get("gross_margins"))),
        ("operating_margin", _fmt_pct(d.get("operating_margins"))),
        ("profit_margin", _fmt_pct(d.get("profit_margins"))),
    ])

    # ── 4. Growth (3Y + YoY) ───────────────────────────────────
    _add_bucket("Growth", [
        ("revenue_cagr_3y", _fmt_pct(d.get("compounded_sales_growth_3y"))),
        ("profit_cagr_3y", _fmt_pct(d.get("compounded_profit_growth_3y"))),
        ("sales_yoy", _fmt_pct(d.get("sales_growth_yoy") or d.get("revenue_growth"))),
        ("profit_yoy", _fmt_pct(d.get("profit_growth_yoy") or d.get("earnings_growth"))),
    ])

    # ── 5. Balance sheet + cash ────────────────────────────────
    _add_bucket("Balance sheet", [
        ("D/E", _fmt_num(d.get("debt_to_equity"), decimals=2)),
        ("debt_direction", d.get("debt_direction")),
        ("total_debt", _fmt_cr(d.get("total_debt"))),
        ("total_cash", _fmt_cr(d.get("total_cash"))),
        ("FCF", _fmt_cr(d.get("free_cash_flow"))),
        ("payout_ratio", _fmt_pct(d.get("payout_ratio"))),
        ("market_cap", _fmt_cr(d.get("market_cap"))),
    ])

    # ── 6. Ownership dynamics ──────────────────────────────────
    _add_bucket("Ownership", [
        ("promoter", _fmt_pct(d.get("promoter_holding"))),
        ("promoter_change", _fmt_pct(d.get("promoter_holding_change"))),
        ("pledged_pct", _fmt_pct(d.get("pledged_promoter_pct"))),
        ("FII", _fmt_pct(d.get("fii_holding"))),
        ("FII_change", _fmt_pct(d.get("fii_holding_change"))),
        ("DII", _fmt_pct(d.get("dii_holding"))),
        ("DII_change", _fmt_pct(d.get("dii_holding_change"))),
    ])

    # ── 7. Technicals ──────────────────────────────────────────
    high52 = d.get("high_52w")
    low52 = d.get("low_52w")
    pos_52w = None
    try:
        if high52 and low52 and last is not None and float(high52) > float(low52):
            pos_52w = (float(last) - float(low52)) / (float(high52) - float(low52)) * 100
    except (TypeError, ValueError):
        pass
    _add_bucket("Technicals", [
        ("trend", d.get("trend_alignment")),
        ("breakout", d.get("breakout_signal")),
        ("beta", _fmt_num(d.get("beta"), decimals=2)),
        ("52w_position",
         f"{pos_52w:.0f}% of range" if pos_52w is not None else None),
        ("risk_reward", d.get("risk_reward_tag")),
    ])

    # ── 8. Score layer breakdown ───────────────────────────────
    _add_bucket("Score layers (0-100)", [
        ("overall", _fmt_num(d.get("score"), decimals=0)),
        ("quality", _fmt_num(d.get("score_quality"), decimals=0)),
        ("valuation", _fmt_num(d.get("score_valuation"), decimals=0)),
        ("growth", _fmt_num(d.get("score_growth"), decimals=0)),
        ("momentum", _fmt_num(d.get("score_momentum"), decimals=0)),
        ("smart_money", _fmt_num(d.get("score_smart_money") or d.get("score_institutional"), decimals=0)),
        ("risk", _fmt_num(d.get("score_risk"), decimals=0)),
        ("earnings_quality", _fmt_num(d.get("score_earnings_quality"), decimals=0)),
        ("financial_health", _fmt_num(d.get("score_financial_health"), decimals=0)),
        ("liquidity", _fmt_num(d.get("score_liquidity"), decimals=0)),
    ])

    # ── 9. Historical growth ranges (3Y/5Y/10Y) ────────────────
    gr = d.get("growth_ranges")
    if isinstance(gr, dict):
        gr_parts: list[str] = []
        for gr_key, gr_label in [
            ("compounded_sales_growth", "sales"),
            ("compounded_profit_growth", "profit"),
            ("return_on_equity", "ROE"),
        ]:
            gd = gr.get(gr_key)
            if isinstance(gd, dict):
                periods = []
                for period in ("10y", "5y", "3y"):
                    pv = gd.get(period)
                    if pv is not None:
                        periods.append(f"{period.upper()}:{pv}%")
                if periods:
                    gr_parts.append(f"{gr_label}({', '.join(periods)})")
        if gr_parts:
            parts.append("Growth history: " + " | ".join(gr_parts))

    # ── 10. Tags + red flags ───────────────────────────────────
    tags = d.get("tags_v2")
    if isinstance(tags, list):
        tag_names: list[str] = []
        for t in tags[:8]:
            if isinstance(t, dict):
                tg = t.get("tag")
                if tg:
                    tag_names.append(str(tg))
            elif isinstance(t, str):
                tag_names.append(t)
        if tag_names:
            parts.append("Tags: " + ", ".join(tag_names))

    # Red flags are embedded in tags_v2 as category='risk' entries
    # with severity='negative'. Extract and surface them separately
    # so the LLM's "one risk/counter-signal" rule has explicit
    # material to lean on.
    if isinstance(tags, list):
        risk_names: list[str] = []
        for t in tags:
            if not isinstance(t, dict):
                continue
            if str(t.get("category", "")).lower() != "risk":
                continue
            if str(t.get("severity", "")).lower() != "negative":
                continue
            tg = t.get("tag")
            if tg:
                risk_names.append(str(tg))
        if risk_names:
            parts.append("Red flags: " + ", ".join(risk_names[:5]))

    return "\n".join(parts)


async def generate_stock_narrative(stock_data: dict) -> str | None:
    """Generate a 2-3 sentence AI narrative for a stock.

    Returns the narrative text, or None if AI is unavailable.
    Uses caching to avoid redundant calls for the same stock data.

    Prompt versioning: the in-memory cache key is prefixed with a
    version tag so changing the prompt bucket set automatically
    invalidates every in-process cache entry. Existing DB-cached
    narratives are left alone on purpose — we let the 24-hour TTL
    in get_stock_story expire them naturally so rate-limited LLM
    calls spread out as users browse instead of spiking.
    """
    # Bump this whenever the bucket set / field list in
    # _build_stock_prompt changes materially.
    _PROMPT_VERSION = "v2-10buckets"

    symbol = stock_data.get("symbol", "")
    score = stock_data.get("score")
    data_hash = f"{_PROMPT_VERSION}:{symbol}:{score}"
    ck = _cache_key("stock_narrative", data_hash)

    cached = _check_cache(ck)
    if cached:
        return cached

    prompt = _build_stock_prompt(stock_data)
    result = await _call_llm(_STOCK_SYSTEM, prompt, max_tokens=200)
    if result:
        _set_cache(ck, result)
    return result


# ---------------------------------------------------------------------------
# Notification narrative (market open/close enrichment)
# ---------------------------------------------------------------------------
#
# Each notification type has:
#   1. A system prompt tailored to its domain (US tech vs India smallcaps
#      vs commodity spike need different language and emphasis)
#   2. A structured natural-language context builder that converts the raw
#      market_data dict into a human-readable summary (instead of shipping
#      raw JSON which leaks implementation details and confuses the LLM)
#   3. A (max_tokens, temperature) pair appropriate for the complexity
#      of the type
#
# Metrics: in-memory counters track per-type total / success / fallback /
# cumulative latency so we can see which types are actually exercising the
# LLM. Accessed via get_notification_ai_metrics() and exposed on /ops.
# Lost on restart — not persisted (in-memory was the explicit choice).

# Shared rules every type inherits (appended below each system prompt)
_NOTIFICATION_COMMON_RULES = """
- Write 1-2 short sentences, max 30 words total
- Be specific: use the actual numbers from the context
- No disclaimers, no "NFA", no "do your own research", no "consult an advisor"
- No generic phrases like "investors should watch", "markets remain volatile"
- Use Indian currency conventions: ₹ for Indian assets, $ for US/global
- Percentages: 1 decimal place with explicit sign (+1.2%, -0.3%)
- When trailing history or streaks are provided, weave them into the narrative naturally (e.g. "3rd consecutive green day", "FIIs buying for 2nd straight session", "worst week in a month") — but only if the streak/trend is notable (≥2 days)
- Only claim "best day in X", "largest drop in Y", or similar superlatives if the trailing data EXPLICITLY supports it — never fabricate superlatives
- Apply trailing context ONLY to the asset it belongs to. Do not attribute one asset's streak/history to a different asset in the same notification"""


_NOTIFICATION_PROMPTS: dict[str, str] = {
    # ── INDIA ──
    "india_close": """You are Artha, a concise Indian market commentator.
Task: summarize the India market close in a punchy 1-2 sentence notification body.
The three broad market-cap tiers are Nifty 50 (large-caps), Nifty Midcap 150 (midcaps),
and Nifty Smallcap 250 (smallcaps). Always reference AT LEAST TWO tiers when their
data is provided — the user wants to see cap-tier divergence at a glance. When the
tiers diverge (e.g. smallcaps up 1.7% but Nifty only +0.3%), lead with the divergence
story. Do NOT mention Sensex — the title already covers the headline index.""" + _NOTIFICATION_COMMON_RULES,

    "india_open": """You are Artha, a concise Indian market commentator.
Task: summarize the India market opening in a punchy 1-2 sentence body. Anchor on
Gift Nifty (which drives the opening signal) and overnight global cues that Indian
traders care about — US tech (NASDAQ), Nikkei trading live, and gold as a risk-off
gauge. Don't list everything; pick the cue that most shaped the open.""" + _NOTIFICATION_COMMON_RULES,

    # ── US / EUROPE / JAPAN ──
    "us_close": """You are a concise global-markets commentator for an Indian finance app.
Task: summarize the US market close in 1-2 sentences for Indian investors who follow
Wall Street as a next-day signal. Lead with S&P 500; call out tech divergence
(NASDAQ vs S&P) if present. If Gift Nifty is already trading with the news, end with
the implied NSE outlook. No mention of after-hours futures unless they're in context.""" + _NOTIFICATION_COMMON_RULES,

    "us_open": """You are a concise global-markets commentator for an Indian finance app.
Task: summarize the US market opening in 1-2 sentences. Lead with S&P 500 and call
out tech (NASDAQ) if it diverged. European close and crude-oil context matter for
the narrative. This fires at ~19:00 IST.""" + _NOTIFICATION_COMMON_RULES,

    "europe_close": """You are a concise European-markets commentator for an Indian finance app.
Task: summarize the European close in 1-2 sentences. Reference FTSE, DAX, and CAC
(the three main bourses) when available. Brent crude drives energy stocks — if Brent
moved >1%, call that out. This fires at ~20:30 IST.""" + _NOTIFICATION_COMMON_RULES,

    "europe_open": """You are a concise European-markets commentator for an Indian finance app.
Task: summarize the European market opening in 1-2 sentences. Asia cues (Nikkei,
Nifty) and Brent crude are the main drivers. This fires at ~13:30 IST while Indian
markets are mid-session.""" + _NOTIFICATION_COMMON_RULES,

    "japan_close": """You are a concise Japan-markets commentator for an Indian finance app.
Task: summarize the Nikkei/TOPIX close in 1-2 sentences. JPY strength/weakness is
the dominant macro variable for Japanese equities — always cite it when available
because yen direction inverts with Nikkei direction. This fires at ~11:30 IST.""" + _NOTIFICATION_COMMON_RULES,

    "japan_open": """You are a concise Japan-markets commentator for an Indian finance app.
Task: summarize the Nikkei/TOPIX opening in 1-2 sentences. Wall Street close from
the prior session is the primary overnight cue; JPY direction matters too. This
fires at ~05:30 IST.""" + _NOTIFICATION_COMMON_RULES,

    # ── INDIA-SPECIFIC SIGNALS ──
    "pre_market": """You are Artha, a concise Indian market commentator.
Task: write a 1-2 sentence pre-market summary at ~09:00 IST (15 minutes before
NSE opens). Gift Nifty is the dominant signal. Overnight US close is the
secondary driver; Asian session (Nikkei, Hang Seng) rounds it out. Give a clear
opening outlook (gap-up / positive / flat / gap-down). Do NOT mention Europe
— European markets are closed and don't open until 13:30 IST.""" + _NOTIFICATION_COMMON_RULES,

    "gift_nifty_move": """You are Artha, a concise Indian market commentator.
Task: Gift Nifty just moved >0.5% from the previous NSE close. Write a 1-2
sentence alert explaining what it signals for the NSE opening. Be direct:
"Gap-up signal" vs "negative signal" vs "weak handoff". Mention the absolute
percent move and the implied direction.""" + _NOTIFICATION_COMMON_RULES,

    "fii_dii": """You are Artha, a concise Indian market commentator.
Task: summarize the day's FII/DII net cash activity in 1-2 sentences. All values
are in INR Crores (Cr). The patterns you're looking for:
- BOTH buying → "institutional support"
- BOTH selling → "broad retreat"
- FII sell + DII buy → domestic absorbing foreign exit (note whether DII fully offset)
- FII buy + DII sell → rotation (rarer, usually indicates foreign re-entry)
Use ₹X,XXX Cr format with Indian comma convention.""" + _NOTIFICATION_COMMON_RULES,

    "commodity_spike": """You are Artha, a concise commodity-markets commentator for an Indian finance app.
Task: a commodity just moved ±2% or more. Write a 1-2 sentence alert. Include
both USD and INR price when both are provided (Indian users think in INR but
global prices quote USD). If you know the typical driver for that commodity
(e.g. gold on dollar weakness, crude on OPEC/inventory, natural gas on weather),
add one word of context — but don't fabricate specific reasons you don't have.""" + _NOTIFICATION_COMMON_RULES,
}


# Per-type (max_tokens, temperature). Defaults used when type is missing.
# Tighter tokens for simple alerts (commodity, gift_nifty); more headroom
# for multi-index summaries (india_close, us_close).
_NOTIFICATION_PARAMS: dict[str, tuple[int, float]] = {
    "india_close":     (120, 0.5),
    "india_open":      (100, 0.5),
    "us_close":        (100, 0.5),
    "us_open":         (100, 0.5),
    "europe_close":    (100, 0.5),
    "europe_open":     (100, 0.5),
    "japan_close":     (100, 0.5),
    "japan_open":      (100, 0.5),
    "pre_market":      (100, 0.5),
    "gift_nifty_move":  (70, 0.4),
    "fii_dii":         (100, 0.4),
    "commodity_spike":  (80, 0.5),
}
_DEFAULT_NOTIF_PARAMS = (80, 0.6)


def _fmt_pct(value: Any) -> str:
    """Format a number as a signed 1-decimal percentage."""
    try:
        v = float(value)
    except (TypeError, ValueError):
        return "—"
    sign = "+" if v >= 0 else ""
    return f"{sign}{v:.1f}%"


def _fmt_inr_cr(value: Any) -> str:
    """Format an INR Crores number with Indian comma convention."""
    try:
        v = float(value)
    except (TypeError, ValueError):
        return "—"
    abs_val = abs(v)
    # Indian comma: last 3 digits, then groups of 2
    int_part = int(abs_val)
    s = str(int_part)
    if len(s) > 3:
        last3 = s[-3:]
        rest = s[:-3]
        groups = []
        while len(rest) > 2:
            groups.insert(0, rest[-2:])
            rest = rest[:-2]
        if rest:
            groups.insert(0, rest)
        formatted = ",".join(groups) + "," + last3
    else:
        formatted = s
    sign = "-" if v < 0 else ""
    return f"{sign}₹{formatted} Cr"


def _fmt_trailing(trailing_data: dict, asset_label: str) -> str:
    """Format trailing context dict into a concise line for the LLM."""
    t = trailing_data.get("trailing", [])
    streak = trailing_data.get("streak", 0)
    week_pct = trailing_data.get("week_pct", 0)
    if not t:
        return ""
    pcts = ", ".join(f"{p:+.1f}%" for _, p in t)
    streak_dir = "green" if streak > 0 else "red"
    streak_abs = abs(streak)
    parts = [f"{asset_label} last {len(t)} sessions: {pcts}."]
    if streak_abs >= 2:
        parts.append(f"Streak: {streak_abs} consecutive {streak_dir} days.")
    parts.append(f"Week total: {week_pct:+.1f}%.")
    return " ".join(parts)


def _fmt_fii_dii_trailing(trailing_data: dict) -> str:
    """Format FII/DII trailing context."""
    fii_t = trailing_data.get("fii_trailing", [])
    dii_t = trailing_data.get("dii_trailing", [])
    fii_streak = trailing_data.get("fii_streak", 0)
    dii_streak = trailing_data.get("dii_streak", 0)
    fii_week = trailing_data.get("fii_week_total", 0)
    dii_week = trailing_data.get("dii_week_total", 0)
    if not fii_t:
        return ""
    fii_vals = ", ".join(f"{v:+,.0f}" for _, v in fii_t)
    dii_vals = ", ".join(f"{v:+,.0f}" for _, v in dii_t)
    parts = [f"Last {len(fii_t)} days (newest first): FII {fii_vals} Cr | DII {dii_vals} Cr."]
    fii_dir = "buying" if fii_streak > 0 else "selling"
    dii_dir = "buying" if dii_streak > 0 else "selling"
    if abs(fii_streak) >= 2:
        parts.append(f"FII streak: {fii_dir} {abs(fii_streak)} consecutive days.")
    if abs(dii_streak) >= 2:
        parts.append(f"DII streak: {dii_dir} {abs(dii_streak)} consecutive days.")
    parts.append(f"Week total: FII {fii_week:+,.0f} Cr, DII {dii_week:+,.0f} Cr.")
    return " ".join(parts)


def _build_india_close_context(d: dict) -> str:
    parts = [f"Nifty 50 closed {_fmt_pct(d.get('nifty_change_pct'))}"]
    midcap = d.get("midcap_change_pct")
    smallcap = d.get("smallcap_change_pct")
    if midcap is not None:
        parts.append(f"Nifty Midcap 150 {_fmt_pct(midcap)}")
    if smallcap is not None:
        parts.append(f"Nifty Smallcap 250 {_fmt_pct(smallcap)}")
    line1 = ", ".join(parts) + "."

    lines = [line1]

    top_sec = d.get("top_sector")
    top_pct = d.get("top_sector_pct")
    bot_sec = d.get("bottom_sector")
    bot_pct = d.get("bottom_sector_pct")
    if top_sec and top_pct is not None:
        lines.append(f"Top sector: {top_sec} {_fmt_pct(top_pct)}.")
    if bot_sec and bot_pct is not None:
        lines.append(f"Weakest sector: {bot_sec} {_fmt_pct(bot_pct)}.")

    trailing = d.get("nifty_trailing")
    if trailing:
        lines.append(_fmt_trailing(trailing, "Nifty"))

    return " ".join(lines)


def _build_india_open_context(d: dict) -> str:
    lines = [f"Nifty opens at {d.get('gift_nifty_price', 0):,.0f} ({_fmt_pct(d.get('gift_nifty_change_pct'))} from prev close)."]
    us_sp = d.get("us_sp500_pct")
    us_nas = d.get("us_nasdaq_pct")
    overnight = []
    if us_sp is not None:
        overnight.append(f"S&P 500 {_fmt_pct(us_sp)}")
    if us_nas is not None:
        overnight.append(f"NASDAQ {_fmt_pct(us_nas)}")
    if overnight:
        lines.append(f"Overnight US: {', '.join(overnight)}.")
    nikkei = d.get("nikkei_pct")
    if nikkei is not None:
        lines.append(f"Nikkei trading {_fmt_pct(nikkei)}.")
    gold = d.get("gold_pct")
    if gold is not None:
        lines.append(f"Gold {_fmt_pct(gold)}.")
    trailing = d.get("nifty_trailing")
    if trailing:
        lines.append(_fmt_trailing(trailing, "Nifty (prev sessions)"))
    return " ".join(lines)


def _build_us_close_context(d: dict) -> str:
    parts = [f"S&P 500 closed {_fmt_pct(d.get('sp500_change_pct'))}"]
    nas = d.get("nasdaq_change_pct")
    dow = d.get("dow_change_pct")
    if nas is not None:
        parts.append(f"NASDAQ {_fmt_pct(nas)}")
    if dow is not None:
        parts.append(f"Dow {_fmt_pct(dow)}")
    lines = [", ".join(parts) + "."]
    vix = d.get("cboe_vix")
    vix_pct = d.get("cboe_vix_pct")
    if vix is not None:
        vix_note = f"VIX at {vix:.1f}"
        if vix_pct is not None:
            vix_note += f" ({_fmt_pct(vix_pct)})"
        if vix > 25:
            vix_note += " — high fear"
        elif vix < 15:
            vix_note += " — complacency"
        lines.append(f"{vix_note}.")
    gift_pct = d.get("gift_nifty_change_pct")
    if gift_pct is not None:
        lines.append(f"Gift Nifty at {d.get('gift_nifty_price', 0):,.0f} ({_fmt_pct(gift_pct)} from Nifty close) — implies NSE opening signal.")
    trailing = d.get("sp500_trailing")
    if trailing:
        lines.append(_fmt_trailing(trailing, "S&P 500"))
    return " ".join(lines)


def _build_us_open_context(d: dict) -> str:
    parts = [f"S&P 500 opens {_fmt_pct(d.get('sp500_pct'))}"]
    nas = d.get("nasdaq_pct")
    dow = d.get("dow_pct")
    if nas is not None:
        parts.append(f"NASDAQ {_fmt_pct(nas)}")
    if dow is not None:
        parts.append(f"Dow {_fmt_pct(dow)}")
    lines = [", ".join(parts) + "."]
    eu_parts = []
    if d.get("ftse_pct") is not None:
        eu_parts.append(f"FTSE {_fmt_pct(d.get('ftse_pct'))}")
    if d.get("dax_pct") is not None:
        eu_parts.append(f"DAX {_fmt_pct(d.get('dax_pct'))}")
    if eu_parts:
        lines.append(f"Europe closed: {', '.join(eu_parts)}.")
    crude = d.get("crude_pct")
    if crude is not None:
        lines.append(f"Crude oil {_fmt_pct(crude)}.")
    return " ".join(lines)


def _build_europe_close_context(d: dict) -> str:
    parts = []
    if d.get("ftse_change_pct") is not None:
        parts.append(f"FTSE closed {_fmt_pct(d.get('ftse_change_pct'))}")
    if d.get("dax_change_pct") is not None:
        parts.append(f"DAX {_fmt_pct(d.get('dax_change_pct'))}")
    if d.get("cac_change_pct") is not None:
        parts.append(f"CAC {_fmt_pct(d.get('cac_change_pct'))}")
    lines = [", ".join(parts) + "."] if parts else []
    brent = d.get("brent_change_pct")
    if brent is not None:
        lines.append(f"Brent crude {_fmt_pct(brent)}.")
    trailing = d.get("ftse_trailing")
    if trailing:
        lines.append(_fmt_trailing(trailing, "FTSE"))
    return " ".join(lines)


def _build_europe_open_context(d: dict) -> str:
    parts = []
    if d.get("ftse_pct") is not None:
        parts.append(f"FTSE opens {_fmt_pct(d.get('ftse_pct'))}")
    if d.get("dax_pct") is not None:
        parts.append(f"DAX {_fmt_pct(d.get('dax_pct'))}")
    if d.get("cac_pct") is not None:
        parts.append(f"CAC {_fmt_pct(d.get('cac_pct'))}")
    lines = [", ".join(parts) + "."] if parts else []
    asia = []
    if d.get("nikkei_pct") is not None:
        asia.append(f"Nikkei {_fmt_pct(d.get('nikkei_pct'))}")
    if d.get("nifty_pct") is not None:
        asia.append(f"Nifty 50 {_fmt_pct(d.get('nifty_pct'))}")
    if asia:
        lines.append(f"Asia cues: {', '.join(asia)}.")
    brent = d.get("brent_pct")
    if brent is not None:
        lines.append(f"Brent crude {_fmt_pct(brent)}.")
    return " ".join(lines)


def _build_japan_close_context(d: dict) -> str:
    parts = [f"Nikkei closed {_fmt_pct(d.get('nikkei_change_pct'))}"]
    topix = d.get("topix_change_pct")
    if topix is not None:
        parts.append(f"TOPIX {_fmt_pct(topix)}")
    lines = [", ".join(parts) + "."]
    jpy = d.get("jpy_inr_price")
    jpy_pct = d.get("jpy_inr_change_pct")
    if jpy is not None and jpy_pct is not None:
        yen_dir = "strengthened" if jpy_pct > 0.1 else ("weakened" if jpy_pct < -0.1 else "flat")
        lines.append(f"JPY/INR at {jpy:.4f} ({_fmt_pct(jpy_pct)}) — yen {yen_dir}.")
    trailing = d.get("nikkei_trailing")
    if trailing:
        lines.append(_fmt_trailing(trailing, "Nikkei"))
    return " ".join(lines)


def _build_japan_open_context(d: dict) -> str:
    parts = [f"Nikkei opens {_fmt_pct(d.get('nikkei_pct'))}"]
    topix = d.get("topix_pct")
    if topix is not None:
        parts.append(f"TOPIX {_fmt_pct(topix)}")
    lines = [", ".join(parts) + "."]
    us = []
    if d.get("us_sp500_pct") is not None:
        us.append(f"S&P 500 {_fmt_pct(d.get('us_sp500_pct'))}")
    if d.get("us_nasdaq_pct") is not None:
        us.append(f"NASDAQ {_fmt_pct(d.get('us_nasdaq_pct'))}")
    if us:
        lines.append(f"Overnight US: {', '.join(us)}.")
    jpy_pct = d.get("jpy_inr_pct")
    if jpy_pct is not None:
        yen_dir = "strengthened" if jpy_pct > 0.1 else ("weakened" if jpy_pct < -0.1 else "flat")
        lines.append(f"Yen {yen_dir} overnight ({_fmt_pct(jpy_pct)}).")
    gold = d.get("gold_pct")
    if gold is not None:
        lines.append(f"Gold {_fmt_pct(gold)}.")
    return " ".join(lines)


def _build_pre_market_context(d: dict) -> str:
    lines = [f"Gift Nifty at {d.get('gift_nifty_price', 0):,.0f} ({_fmt_pct(d.get('gift_nifty_pct'))} from yesterday's Nifty close)."]
    outlook = d.get("outlook")
    if outlook:
        lines.append(f"Rule-based outlook: {outlook}.")
    us = d.get("us_change") or {}
    if isinstance(us, dict) and us:
        us_parts = [f"{name} {_fmt_pct(pct)}" for name, pct in us.items()]
        lines.append(f"Overnight US close: {', '.join(us_parts)}.")
    asia = d.get("asia_change") or {}
    if isinstance(asia, dict) and asia:
        asia_parts = [f"{name} {_fmt_pct(pct)}" for name, pct in asia.items()]
        lines.append(f"Asia live: {', '.join(asia_parts)}.")
    vix = d.get("india_vix")
    vix_pct = d.get("india_vix_pct")
    if vix is not None:
        vix_note = f"India VIX at {vix:.1f}"
        if vix_pct is not None:
            vix_note += f" ({_fmt_pct(vix_pct)})"
        if vix > 20:
            vix_note += " — elevated fear"
        elif vix < 13:
            vix_note += " — low volatility"
        lines.append(f"{vix_note}.")
    trailing = d.get("nifty_trailing")
    if trailing:
        lines.append(_fmt_trailing(trailing, "Nifty (prev sessions)"))
    return " ".join(lines)


def _build_gift_nifty_move_context(d: dict) -> str:
    return (
        f"Gift Nifty moved {_fmt_pct(d.get('change_pct'))} — "
        f"trading at {d.get('price', 0):,.0f}. "
        f"Signal direction: {d.get('direction', 'unknown')}."
    )


def _build_fii_dii_context(d: dict) -> str:
    fii = d.get("fii_net_cr", 0)
    dii = d.get("dii_net_cr", 0)
    net = d.get("net_cr", fii + dii)
    fii_verb = "bought" if fii >= 0 else "sold"
    dii_verb = "bought" if dii >= 0 else "sold"
    net_label = "inflow" if net >= 0 else "outflow"
    lines = [
        f"FII net cash: FIIs {fii_verb} {_fmt_inr_cr(fii)}. "
        f"DII net cash: DIIs {dii_verb} {_fmt_inr_cr(dii)}. "
        f"Combined net {net_label}: {_fmt_inr_cr(net)}."
    ]
    trailing = d.get("trailing")
    if trailing:
        lines.append(_fmt_fii_dii_trailing(trailing))
    return " ".join(lines)


def _build_commodity_spike_context(d: dict) -> str:
    asset = d.get("asset", "Commodity")
    pct = d.get("change_pct", 0)
    price_usd = d.get("price_usd", 0)
    unit = d.get("unit") or "unit"
    usd_str = f"${price_usd:,.2f}" if price_usd < 10000 else f"${price_usd:,.0f}"
    line = f"{asset} moved {_fmt_pct(pct)} — now at {usd_str}/{unit}."
    inr_price = d.get("price_inr")
    inr_unit = d.get("inr_unit")
    if inr_price is not None and inr_unit:
        line += f" (₹{inr_price:,.0f}/{inr_unit} for Indian buyers)."
    return line


_CONTEXT_BUILDERS: dict[str, Any] = {
    "india_close":     _build_india_close_context,
    "india_open":      _build_india_open_context,
    "us_close":        _build_us_close_context,
    "us_open":         _build_us_open_context,
    "europe_close":    _build_europe_close_context,
    "europe_open":     _build_europe_open_context,
    "japan_close":     _build_japan_close_context,
    "japan_open":      _build_japan_open_context,
    "pre_market":      _build_pre_market_context,
    "gift_nifty_move":  _build_gift_nifty_move_context,
    "fii_dii":         _build_fii_dii_context,
    "commodity_spike":  _build_commodity_spike_context,
}


# Fallback generic system prompt for unknown types (e.g. future additions
# that haven't been wired through _NOTIFICATION_PROMPTS yet).
_NOTIFICATION_SYSTEM_GENERIC = """You are a concise market commentator for an Indian finance app.
Task: write a short, data-driven notification body.""" + _NOTIFICATION_COMMON_RULES


# In-memory per-type metrics: lost on restart. Exposed via
# get_notification_ai_metrics() for /ops observability.
_notification_ai_metrics: dict[str, dict[str, float]] = {}


def _get_metric_bucket(notification_type: str) -> dict[str, float]:
    bucket = _notification_ai_metrics.get(notification_type)
    if bucket is None:
        bucket = {
            "total": 0.0,
            "success": 0.0,
            "fallback": 0.0,
            "latency_ms_sum": 0.0,
        }
        _notification_ai_metrics[notification_type] = bucket
    return bucket


def get_notification_ai_metrics() -> dict[str, dict[str, Any]]:
    """Return a snapshot of notification-AI metrics for /ops endpoint.

    Computes fallback_rate and avg_latency_ms on demand. Safe to call
    from a request handler — does not mutate state.
    """
    snapshot: dict[str, dict[str, Any]] = {}
    for ntype, bucket in _notification_ai_metrics.items():
        total = int(bucket["total"])
        success = int(bucket["success"])
        fallback = int(bucket["fallback"])
        latency_sum = bucket["latency_ms_sum"]
        snapshot[ntype] = {
            "total": total,
            "success": success,
            "fallback": fallback,
            "fallback_rate": round(fallback / total, 3) if total else 0.0,
            "avg_latency_ms": round(latency_sum / total, 1) if total else 0.0,
        }
    return snapshot


async def generate_notification_narrative(
    notification_type: str,
    market_data: dict[str, Any],
) -> str | None:
    """Generate a short AI-enhanced notification body.

    Routes to the per-type system prompt + context builder if the type is
    registered in _NOTIFICATION_PROMPTS; otherwise falls back to the generic
    prompt with a json.dumps of market_data. Updates the in-memory metric
    counters on every call so fallback rates can be observed via /ops.
    """
    bucket = _get_metric_bucket(notification_type)
    bucket["total"] += 1

    # Cache key incorporates type + payload hash (6-hour TTL, mostly useless
    # for notifications since each fires once per day, but kept for safety
    # in case the same notification is retried on a restart).
    data_hash = f"{notification_type}:{json.dumps(market_data, sort_keys=True, default=str)}"
    ck = _cache_key("notification", data_hash)
    cached = _check_cache(ck)
    if cached:
        bucket["success"] += 1
        return cached

    # Pick per-type prompt + params, fall back to generic for unknown types.
    system_prompt = _NOTIFICATION_PROMPTS.get(
        notification_type, _NOTIFICATION_SYSTEM_GENERIC,
    )
    max_tokens, temperature = _NOTIFICATION_PARAMS.get(
        notification_type, _DEFAULT_NOTIF_PARAMS,
    )

    # Build structured natural-language context. Fall back to json.dumps if
    # the type doesn't have a registered builder (preserves old behaviour).
    builder = _CONTEXT_BUILDERS.get(notification_type)
    if builder is not None:
        try:
            context = builder(market_data)
        except Exception:
            logger.warning(
                "notification_ai: context builder failed for %s, falling back to json",
                notification_type, exc_info=True,
            )
            context = json.dumps(market_data, default=str)
    else:
        context = json.dumps(market_data, default=str)

    user_prompt = f"Context:\n{context}\n\nWrite the notification body now."

    t0 = time.monotonic()
    result = await _call_llm(
        system_prompt, user_prompt,
        max_tokens=max_tokens,
        temperature=temperature,
    )
    elapsed_ms = (time.monotonic() - t0) * 1000
    bucket["latency_ms_sum"] += elapsed_ms

    if result:
        bucket["success"] += 1
        _set_cache(ck, result)
        logger.info(
            "notification_ai: type=%s model_success latency_ms=%.0f",
            notification_type, elapsed_ms,
        )
    else:
        bucket["fallback"] += 1
        logger.info(
            "notification_ai: type=%s FALLBACK latency_ms=%.0f (all models failed)",
            notification_type, elapsed_ms,
        )
    return result


# ---------------------------------------------------------------------------
# Market mood summary
# ---------------------------------------------------------------------------

_MOOD_SYSTEM = """You are a market mood analyst for an Indian finance app.
Rules:
- Write exactly 1-2 sentences (max 40 words)
- Summarize the overall market mood from the score distribution
- Use the numbers — be specific
- No disclaimers, no generic phrases
- Be opinionated — bullish, cautious, mixed, etc."""


async def generate_market_mood_summary(mood_data: dict[str, Any]) -> str | None:
    """Generate AI summary of market mood from score distribution.

    mood_data should include: avg_score, total, excellent, good, average, poor,
    above_good_pct.
    """
    data_hash = json.dumps(mood_data, sort_keys=True, default=str)
    ck = _cache_key("market_mood", data_hash)

    cached = _check_cache(ck)
    if cached:
        return cached

    prompt = (
        f"Market mood data:\n"
        f"- Average score: {mood_data.get('avg_score')}/100\n"
        f"- {mood_data.get('above_good_pct', 0)}% of stocks rated Good or above\n"
        f"- Distribution: {mood_data.get('excellent', 0)} excellent, "
        f"{mood_data.get('good', 0)} good, {mood_data.get('average', 0)} average, "
        f"{mood_data.get('poor', 0)} poor\n\n"
        f"Write a 1-2 sentence market mood summary."
    )
    result = await _call_llm(_MOOD_SYSTEM, prompt, max_tokens=80, temperature=0.5)
    if result:
        _set_cache(ck, result)
    return result


# ---------------------------------------------------------------------------
# Stock compare insight
# ---------------------------------------------------------------------------

_COMPARE_SYSTEM = """You are a concise Indian stock analyst.
Rules:
- Write exactly 1-2 sentences (max 50 words)
- Compare the stocks objectively using the data
- Highlight the key differentiator between them
- No disclaimers, no bullet points, just flowing prose"""


async def generate_compare_insight(stocks: list[dict]) -> str | None:
    """Generate a 1-2 sentence comparison insight for 2-3 stocks."""
    if len(stocks) < 2:
        return None

    symbols = [s.get("symbol", "?") for s in stocks]
    data_hash = ":".join(sorted(symbols))
    ck = _cache_key("compare", data_hash)

    cached = _check_cache(ck)
    if cached:
        return cached

    lines = []
    for s in stocks[:3]:
        parts = [f"{s.get('display_name', s.get('symbol', '?'))}:"]
        for key, label in [
            ("score", "Score"),
            ("pe_ratio", "PE"),
            ("roe", "ROE"),
            ("roce", "ROCE"),
            ("debt_to_equity", "D/E"),
            ("market_cap", "MCap(Cr)"),
            ("revenue_growth", "Rev Growth"),
        ]:
            v = s.get(key)
            if v is not None:
                parts.append(f"{label}={v}")
        lines.append(" ".join(parts))

    prompt = (
        f"Compare these stocks:\n" + "\n".join(lines) + "\n\n"
        f"Write a 1-2 sentence comparison verdict."
    )
    result = await _call_llm(_COMPARE_SYSTEM, prompt, max_tokens=100, temperature=0.5)
    if result:
        _set_cache(ck, result)
    return result


# ---------------------------------------------------------------------------
# Discover section AI subtitle
# ---------------------------------------------------------------------------

_SECTION_SYSTEM = """You are a market analyst for an Indian finance app.
Rules:
- Write exactly 1 sentence (max 20 words)
- Describe the theme of this stock list in an engaging way
- Use specific data if provided
- No disclaimers"""


async def generate_section_subtitle(
    section_key: str,
    section_title: str,
    stock_summaries: list[str],
) -> str | None:
    """Generate an AI subtitle for a discover home section.

    stock_summaries: list of brief stock descriptions like "TCS: Score 78, PE 28"
    """
    data_hash = f"{section_key}:{len(stock_summaries)}"
    ck = _cache_key("section", data_hash)

    cached = _check_cache(ck)
    if cached:
        return cached

    prompt = (
        f"Section: {section_title}\n"
        f"Top stocks: {', '.join(stock_summaries[:5])}\n\n"
        f"Write a 1-sentence engaging subtitle for this section."
    )
    result = await _call_llm(_SECTION_SYSTEM, prompt, max_tokens=50, temperature=0.6)
    if result:
        _set_cache(ck, result)
    return result

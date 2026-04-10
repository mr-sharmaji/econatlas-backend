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


def _clean_response(raw: str) -> str:
    """Strip thinking artifacts and meta-commentary from LLM output.

    Some models (Nemotron) echo instructions like "We need to produce 2-3 sentences..."
    before the actual content. This strips that preamble.
    """
    text = raw.strip()

    # Remove <think>...</think> blocks (some models use these)
    text = re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL).strip()

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

_STOCK_SYSTEM = """You are a concise Indian stock market analyst writing for retail investors.
Rules:
- Write exactly 2-3 short sentences (max 60 words total)
- Be specific — use the numbers provided
- No disclaimers, no "NFA", no "do your own research"
- No bullet points, no headers — just flowing prose
- Mention the company's key strength AND one risk/concern
- Use plain English, avoid jargon"""


def _build_stock_prompt(stock_data: dict) -> str:
    """Build a prompt from stock data dict."""
    parts = [f"Write a brief investment narrative for {stock_data.get('display_name', stock_data.get('symbol', 'this company'))}."]
    parts.append("Key data:")

    _fields = [
        ("sector", "Sector"),
        ("pe_ratio", "PE"),
        ("price_to_book", "P/B"),
        ("roe", "ROE"),
        ("roce", "ROCE"),
        ("operating_margins", "Operating Margin"),
        ("debt_to_equity", "D/E"),
        ("market_cap", "Market Cap (Cr)"),
        ("revenue_growth", "Revenue Growth YoY"),
        ("earnings_growth", "Earnings Growth YoY"),
        ("compounded_sales_growth_3y", "Revenue CAGR 3Y"),
        ("compounded_profit_growth_3y", "Profit CAGR 3Y"),
        ("dividend_yield", "Dividend Yield"),
        ("score", "EconAtlas Score (0-100)"),
    ]
    for key, label in _fields:
        v = stock_data.get(key)
        if v is not None:
            if key == "operating_margins" and isinstance(v, (int, float)) and abs(v) < 1:
                v = f"{v * 100:.1f}%"
            elif key == "revenue_growth" and isinstance(v, (int, float)) and abs(v) < 2:
                v = f"{v * 100:.1f}%"
            elif key == "earnings_growth" and isinstance(v, (int, float)) and abs(v) < 2:
                v = f"{v * 100:.1f}%"
            elif key in ("roe", "roce") and isinstance(v, (int, float)):
                v = f"{v:.1f}%"
            elif key == "market_cap" and isinstance(v, (int, float)):
                v = f"₹{v:,.0f} Cr"
            parts.append(f"- {label}: {v}")

    # Tags context
    tags = stock_data.get("tags_v2")
    if tags and isinstance(tags, list):
        tag_names = [t.get("tag", "") if isinstance(t, dict) else str(t) for t in tags[:5]]
        if tag_names:
            parts.append(f"- Tags: {', '.join(tag_names)}")

    # Growth ranges
    gr = stock_data.get("growth_ranges")
    if gr and isinstance(gr, dict):
        for gr_key, gr_label in [
            ("compounded_sales_growth", "Revenue CAGR"),
            ("compounded_profit_growth", "Profit CAGR"),
            ("return_on_equity", "ROE History"),
        ]:
            gr_data = gr.get(gr_key)
            if gr_data and isinstance(gr_data, dict):
                range_parts = []
                for period in ["10y", "5y", "3y"]:
                    pv = gr_data.get(period)
                    if pv is not None:
                        range_parts.append(f"{period.upper()}: {pv}%")
                if range_parts:
                    parts.append(f"- {gr_label}: {', '.join(range_parts)}")

    return "\n".join(parts)


async def generate_stock_narrative(stock_data: dict) -> str | None:
    """Generate a 2-3 sentence AI narrative for a stock.

    Returns the narrative text, or None if AI is unavailable.
    Uses caching to avoid redundant calls for the same stock data.
    """
    symbol = stock_data.get("symbol", "")
    score = stock_data.get("score")
    data_hash = f"{symbol}:{score}"
    ck = _cache_key("stock_narrative", data_hash)

    cached = _check_cache(ck)
    if cached:
        return cached

    prompt = _build_stock_prompt(stock_data)
    result = await _call_llm(_STOCK_SYSTEM, prompt, max_tokens=150)
    if result:
        _set_cache(ck, result)
    return result


# ---------------------------------------------------------------------------
# Notification narrative (market open/close enrichment)
# ---------------------------------------------------------------------------

_NOTIFICATION_SYSTEM = """You are a concise market commentator for an Indian finance app.
Rules:
- Write exactly 1-2 sentences (max 30 words)
- Use the data provided — be specific
- No generic phrases like "investors should watch" or "markets remain volatile"
- Capture the key theme of the session in a punchy, engaging way
- For India close notifications: the three broad indices are Nifty 50 (large-caps),
  Nifty Midcap 150 (midcaps), and Nifty Smallcap 250 (smallcaps). Reference at
  least TWO of these tiers when their data is provided — the user wants to see
  divergence across the market-cap spectrum, not just the large-cap headline.
  Do NOT mention Sensex in the body — the title already covers the headline indices.
- FII/DII values are in Indian Rupees Crores (Cr), not contracts
- Commodity prices: USD and INR values as provided"""


async def generate_notification_narrative(
    notification_type: str,
    market_data: dict[str, Any],
) -> str | None:
    """Generate a short AI-enhanced notification body.

    notification_type: "india_open", "india_close", "pre_market", etc.
    market_data: dict with relevant numbers (nifty_pct, sensex_pct, etc.)
    """
    data_hash = f"{notification_type}:{json.dumps(market_data, sort_keys=True, default=str)}"
    ck = _cache_key("notification", data_hash)

    cached = _check_cache(ck)
    if cached:
        return cached

    prompt = f"Notification type: {notification_type}\nMarket data: {json.dumps(market_data, default=str)}\n\nWrite a punchy 1-2 sentence notification body."
    result = await _call_llm(_NOTIFICATION_SYSTEM, prompt, max_tokens=80, temperature=0.6)
    if result:
        _set_cache(ck, result)
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

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
from datetime import datetime, timezone
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
MAX_TOKENS_CHAT = 800  # Larger than narrative — chat needs room

# Two model chains: fast for intent/tool detection, slow for the final
# answer that composes tool results into a polished response.
# Fast chain prioritises latency over quality — a 20B param model can
# easily decide "is this a stock lookup or a macro question?". Slow chain
# prioritises quality — 120B handles the nuanced composition better.
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

# ---------------------------------------------------------------------------
# System prompt — the brain of Artha
# ---------------------------------------------------------------------------
_ARTHA_SYSTEM = """You are **Artha** (अर्थ), an AI market analyst built into the EconAtlas app — an Indian finance app for retail investors.

## Your capabilities
You have access to tools that query live data. To use a tool, output exactly:
[TOOL:tool_name:{"param":"value"}]

Available tools:
- [TOOL:stock_lookup:{"symbol":"TCS"}] — Get full details for a stock
- [TOOL:stock_screen:{"query":"sector = 'Information Technology' AND roe > 20 AND score > 60", "limit":5}] — Screen stocks with SQL WHERE conditions. Available columns: symbol, display_name, sector, industry, last_price, percent_change, pe_ratio, roe, roce, debt_to_equity, price_to_book, market_cap, dividend_yield, score, revenue_growth, earnings_growth, operating_margins, profit_margins, beta, free_cash_flow, promoter_holding, fii_holding, dii_holding, compounded_sales_growth_3y, compounded_profit_growth_3y
- [TOOL:stock_compare:{"symbols":["TCS","INFY","WIPRO"]}] — Compare up to 3 stocks side-by-side
- [TOOL:mf_lookup:{"scheme_code":"119551"}] — Get mutual fund details
- [TOOL:mf_screen:{"query":"category = 'Equity' AND returns_1y > 15 AND score > 60", "limit":5}] — Screen mutual funds. Available columns: scheme_code, scheme_name, category, sub_category, nav, expense_ratio, aum_cr, returns_1y, returns_3y, returns_5y, sharpe, sortino, score, risk_level
- [TOOL:watchlist:{}] — Get user's watchlist stocks with current data
- [TOOL:market_status:{}] — All tracked indices (India, US, Europe, Japan), FX majors, key commodities, and market open/close status for each region
- [TOOL:ipo_list:{"status":"open"}] — List IPOs (open/upcoming/closed)
- [TOOL:news:{"entity":"Reliance"}] — Get latest news, optionally filtered by company/topic
- [TOOL:macro:{"indicator":"gdp_growth"}] — Get macro indicators (gdp_growth, inflation_cpi, repo_rate, usd_inr, fiscal_deficit, current_account, iip_growth)
- [TOOL:commodity:{}] — Get commodity prices (gold, silver, crude oil)
- [TOOL:crypto:{}] — Get crypto prices (BTC, ETH, etc.)
- [TOOL:tax:{"type":"ltcg","profit":500000,"purchase_year":"2020"}] — Calculate tax (ltcg, stcg, income_tax)

## Rules
1. Be concise — max 3-4 sentences for simple questions, longer for detailed analysis
2. Use specific numbers from tool results — never make up data
3. When mentioning stocks, ALWAYS use the tool first to get current data
4. For screener queries, translate the user's natural language into SQL WHERE conditions
5. Respond in English by default. You may match the user's style if they write in Hinglish.
6. No disclaimers like "NFA", "consult a financial advisor", "do your own research"
7. Use markdown formatting: **bold** for key numbers/names, bullet points (- ) for lists, line breaks between sections
8. Be opinionated — say if a stock looks strong, weak, overvalued, etc.
9. When showing stock results, output [CARD:SYMBOL] markers for each stock to display mini cards
10. For comparisons, show cards for all stocks and give a clear verdict
11. Max 5 stock/MF cards per response. If more results, mention the count.
12. You are knowledgeable about Indian markets, taxation, IPOs, mutual funds, and macroeconomics
13. When user asks about "my stocks" or "my watchlist", use the watchlist tool
14. Commodity prices are in USD and INR. FII/DII values are in Indian Rupees Crores (Cr).
15. IMPORTANT: At the END of every response, add exactly 5 follow-up suggestions in this format:
[SUGGESTIONS]
- Follow-up question 1
- Follow-up question 2
- Follow-up question 3
- Follow-up question 4
- Follow-up question 5
[/SUGGESTIONS]

STRICT RULES for follow-up suggestions:
- MAXIMUM 12 WORDS per suggestion — be substantive, not terse
- Write each as if the USER is asking YOU — first person from the user's perspective
- Natural, conversational, meaningful — not instructional
- Relevant and SPECIFIC to what was JUST discussed (reference actual names/numbers from the conversation)
- DO NOT start with instructional verbs ("Ask for...", "Inquire about...", "Check...", "Request...", "Get...", "See...", "Show me how...")
- DO write direct questions or commands the user would type

GOOD examples (≤12 words, first-person, substantive):
- "Compare TCS's fundamentals with Infosys and Wipro"
- "How have TCS returns performed over the last 5 years?"
- "Is TCS currently trading above or below its fair value?"
- "Any major TCS news or earnings announcements this week?"
- "What are the top 5 gainers on Nifty 50 right now?"
- "Which IT stocks have the best ROE and lowest debt?"
- "How is the Nifty Bank index performing versus Nifty 50 today?"
- "Which sectors led the market higher this week?"
- "What's the latest FII flow trend and its market impact?"

BAD examples (DO NOT write these):
- "Compare TCS"                                    ← too terse
- "Ask for the top gainers and losers in Nifty 50" ← instructional
- "Show a side-by-side comparison of Reliance and HDFC Bank fundamentals including ROE and PE" ← too long (15+ words)
- "Check the FII/DII flow trends"                  ← instructional
- "Get latest consensus EPS"                       ← instructional
"""

# ---------------------------------------------------------------------------
# Tool definitions for parsing and execution
# ---------------------------------------------------------------------------
_TOOL_PATTERN = re.compile(r'\[TOOL:(\w+):(.*?)\]', re.DOTALL)
_CARD_PATTERN = re.compile(r'\[CARD:(\S+)\]')
_SUGGESTIONS_PATTERN = re.compile(r'\[SUGGESTIONS\](.*?)\[/SUGGESTIONS\]', re.DOTALL)


async def _news_hybrid_search(pool, query: str, limit: int = 5) -> tuple[list[dict], str]:
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
            "WHERE COALESCE(title,'') % $1 OR COALESCE(summary,'') % $1 "
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
            "WHERE title ILIKE $1 OR summary ILIKE $1 OR body ILIKE $1 "
            "   OR primary_entity ILIKE $1 "
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
            symbols = [s.upper().strip() for s in params.get("symbols", [])][:3]
            if len(symbols) < 2:
                return {"error": "Need at least 2 symbols to compare"}
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
            # Hybrid semantic + trigram search on news_articles.
            #
            # Why: primary_entity is a TOPIC category ("market_news",
            # "dow_jones", "nifty_50", "sensex", "gold", ...) — NOT a
            # company ticker. Plain ILIKE also misses semantically
            # related articles (asking about "TCS" should match
            # "Tata Consultancy Services" and "Indian IT sector").
            #
            # Strategy:
            #   1. Try vector search (pgvector) if embedding is available
            #      for the query AND the news_articles.embedding column
            #      has been backfilled.
            #   2. Otherwise use pg_trgm similarity() ranking.
            #   3. Fall back to plain ILIKE if neither extension is installed.
            entity = (params.get("entity") or "").strip()
            if not entity:
                rows = await pool.fetch(
                    "SELECT title, summary, source, timestamp, url, primary_entity, impact "
                    "FROM news_articles ORDER BY timestamp DESC LIMIT 5",
                    timeout=5,
                )
                search_mode = "recent"
            else:
                rows, search_mode = await _news_hybrid_search(pool, entity, limit=5)

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
            indicator = params.get("indicator", "")
            if not indicator:
                return {"error": "No indicator specified"}
            rows = await pool.fetch(
                "SELECT indicator_name, country, value, timestamp "
                "FROM macro_indicators "
                "WHERE indicator_name ILIKE $1 "
                "ORDER BY timestamp DESC LIMIT 5",
                f"%{indicator}%",
            )
            return {
                "indicator": indicator,
                "data": [record_to_dict(r) for r in rows],
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
            # Basic tax info — delegate to LLM knowledge for now
            return {
                "note": "Tax calculation delegated to LLM knowledge. "
                "Indian LTCG: 12.5% above ₹1.25L exemption (equity held >1 year). "
                "STCG: 20% (equity held <1 year). "
                "New tax regime is default from FY 2024-25.",
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
                summary = {
                    k: (len(v) if isinstance(v, list) else "obj")
                    for k in ("stocks", "funds", "articles", "ipos", "indices_by_region", "data", "commodities", "crypto")
                    if k in result
                }
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
                "Here are the tool results. Now write your response using this data. "
                "Do NOT include any [TOOL:...] markers in your response. "
                "Be specific with the numbers. "
                "If a tool returned an error or empty data, be honest with the user: "
                "say 'I couldn't fetch X right now, want me to try again?' instead of "
                "inventing numbers. "
                "Remember to append the [SUGGESTIONS]...[/SUGGESTIONS] block with "
                "exactly 5 follow-up questions (max 12 words each)."
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

    async def _stream_final() -> AsyncGenerator[str, None]:
        """Yield raw token deltas from the appropriate model chain."""
        if compose_messages is not None:
            # Tools were used — compose with slow model
            async for delta in _stream_llm_response(
                api_key, compose_messages, chain=_SLOW_MODELS,
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

    # Accumulated raw text (including bracketed blocks). Used for final
    # DB save + post-stream suggestion extraction.
    raw_accum = ""
    # Cursor: character offset up to which we've already emitted safely.
    yielded_up_to = 0
    # True while we're inside a [SUGGESTIONS]...[/SUGGESTIONS] block.
    in_suggestions_block = False
    # Longest bracketed sentinel we might need to hold back at the
    # tail to detect a mid-chunk tag start.
    _HOLDBACK_LEN = len("[SUGGESTIONS]")
    _START_TAG = "[SUGGESTIONS]"
    _END_TAG = "[/SUGGESTIONS]"

    async for delta in _stream_final():
        if not delta:
            continue
        raw_accum += delta

        if in_suggestions_block:
            # Suppress all output until we see [/SUGGESTIONS].
            end_idx = raw_accum.find(_END_TAG, yielded_up_to)
            if end_idx >= 0:
                yielded_up_to = end_idx + len(_END_TAG)
                in_suggestions_block = False
                # Fall through — we might be able to emit text after the
                # closing tag in the same iteration.
            else:
                continue

        # Not inside a suppression block. Check if a new [SUGGESTIONS]
        # has appeared in the accumulated text.
        start_idx = raw_accum.find(_START_TAG, yielded_up_to)
        if start_idx >= 0:
            # Emit everything up to the start of the block
            if start_idx > yielded_up_to:
                safe = raw_accum[yielded_up_to:start_idx]
                if safe:
                    yield {"event": "token", "data": {"text": safe}}
            yielded_up_to = start_idx + len(_START_TAG)
            in_suggestions_block = True
            continue

        # No full start tag found — emit up to a safe point, but hold
        # back the last HOLDBACK_LEN chars in case they're a partial
        # tag that will complete in the next delta.
        safe_end = max(yielded_up_to, len(raw_accum) - _HOLDBACK_LEN)
        if safe_end > yielded_up_to:
            safe = raw_accum[yielded_up_to:safe_end]
            if safe:
                yield {"event": "token", "data": {"text": safe}}
            yielded_up_to = safe_end

    # Stream is done — flush any held-back tail that isn't inside a
    # suggestions block.
    if not in_suggestions_block and yielded_up_to < len(raw_accum):
        tail = raw_accum[yielded_up_to:]
        if tail:
            yield {"event": "token", "data": {"text": tail}}
        yielded_up_to = len(raw_accum)

    # Build the full response for DB save + suggestion extraction.
    final_response = raw_accum
    if not final_response:
        final_response = "I couldn't generate a response. Please try again."

    # Clean: strip tool markers, card markers.
    final_response = _clean_response(final_response)
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


async def _build_context(session_id: str, device_id: str) -> list[dict]:
    """Build LLM message context from session history."""
    messages = [{"role": "system", "content": _ARTHA_SYSTEM}]

    # Get last N messages from this session
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

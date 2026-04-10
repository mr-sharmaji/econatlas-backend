"""Artha AI chat service — orchestrates LLM, tools, sessions, and streaming.

Core flow:
1. User sends message → rate-limit check → save to DB
2. Build context (system prompt + last N messages + tool instructions)
3. Call LLM → parse tool markers → execute tools → re-call LLM with results
4. Stream response tokens via SSE
5. Save assistant message with stock/MF cards to DB
"""
from __future__ import annotations

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
STREAM_TIMEOUT = 30.0
MAX_TOKENS_CHAT = 800  # Larger than narrative — chat needs room

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
- [TOOL:market_status:{}] — Current market indices (Nifty, Sensex), market open/close status
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
15. IMPORTANT: At the END of every response, add exactly 4 follow-up suggestions in this format:
[SUGGESTIONS]
- Follow-up question 1
- Follow-up question 2
- Follow-up question 3
- Follow-up question 4
[/SUGGESTIONS]
Make follow-ups relevant to what was just discussed. Examples: if you discussed TCS, suggest "Compare TCS with Infosys", "Show TCS financials history", etc.
"""

# ---------------------------------------------------------------------------
# Tool definitions for parsing and execution
# ---------------------------------------------------------------------------
_TOOL_PATTERN = re.compile(r'\[TOOL:(\w+):(.*?)\]', re.DOTALL)
_CARD_PATTERN = re.compile(r'\[CARD:(\S+)\]')
_SUGGESTIONS_PATTERN = re.compile(r'\[SUGGESTIONS\](.*?)\[/SUGGESTIONS\]', re.DOTALL)


async def _execute_tool(
    tool_name: str,
    params: dict,
    device_id: str,
) -> dict[str, Any]:
    """Execute a tool and return results. Never raises — returns error dict on failure."""
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
            # Validate: only SELECT-safe WHERE conditions
            safe = _validate_screen_query(query_where)
            if not safe:
                return {"error": "Invalid query — only filtering conditions allowed"}
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
            safe = _validate_screen_query(query_where)
            if not safe:
                return {"error": "Invalid query"}
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
            prices = await get_latest_prices(instrument_type="index")
            # Asset names in DB are title-case: "Nifty 50", "Sensex", "Nifty Bank", etc.
            # Match case-insensitively so the tool is resilient to naming changes.
            _TARGET_INDICES = {
                "nifty 50", "sensex", "nifty bank", "nifty it",
                "nifty auto", "nifty pharma", "nifty metal",
                "nifty midcap 150", "nifty smallcap 250", "nifty 500",
                "gift nifty",
            }
            indices = {}
            for p in prices:
                name = str(p.get("asset") or "")
                if name.lower() not in _TARGET_INDICES:
                    continue
                indices[name] = {
                    "price": p.get("price"),
                    "previous_close": p.get("previous_close"),
                    "change_pct": p.get("change_percent"),
                    "timestamp": (
                        p["timestamp"].isoformat()
                        if p.get("timestamp") and hasattr(p["timestamp"], "isoformat")
                        else p.get("timestamp")
                    ),
                }
            status = _mkt_status(datetime.now(timezone.utc))
            return {
                "indices": indices,
                "india_open": bool(status.get("nse_open")),
                "us_open": bool(status.get("nyse_open")),
                "count": len(indices),
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
            # news_articles.primary_entity is a TOPIC category
            # ("market_news", "dow_jones", "nifty_50", "sensex", "gold",
            # "crude_oil", ...) — NOT a company ticker. So exact-match on
            # entity fails for company names like "TCS" or "Reliance".
            # Instead, search the title/summary/body text with ILIKE.
            entity = (params.get("entity") or "").strip()
            if not entity:
                # No filter — return the most recent articles
                rows = await pool.fetch(
                    "SELECT title, summary, source, timestamp, url, primary_entity, impact "
                    "FROM news_articles ORDER BY timestamp DESC LIMIT 5",
                    timeout=5,
                )
            else:
                pattern = f"%{entity}%"
                rows = await pool.fetch(
                    "SELECT title, summary, source, timestamp, url, primary_entity, impact "
                    "FROM news_articles "
                    "WHERE title ILIKE $1 OR summary ILIKE $1 OR body ILIKE $1 "
                    "   OR primary_entity ILIKE $1 "
                    "ORDER BY timestamp DESC LIMIT 5",
                    pattern,
                    timeout=5,
                )
            return {
                "query": entity or None,
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
            from app.services.market_service import get_latest_prices
            prices = await get_latest_prices(instrument_type="commodity")
            return {
                "commodities": [
                    {
                        "name": p.get("asset"),
                        "price": p.get("close") or p.get("last_price"),
                        "change_pct": p.get("change_percent"),
                    }
                    for p in prices[:10]
                ],
            }

        elif tool_name == "crypto":
            from app.services.market_service import get_latest_prices
            prices = await get_latest_prices(instrument_type="crypto")
            return {
                "crypto": [
                    {
                        "name": p.get("asset"),
                        "price": p.get("close") or p.get("last_price"),
                        "change_pct": p.get("change_percent"),
                    }
                    for p in prices[:10]
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
            return {"error": f"Unknown tool: {tool_name}"}

    except Exception as e:
        logger.warning("Tool %s failed: %s", tool_name, e, exc_info=True)
        return {"error": f"Tool failed: {str(e)[:100]}"}


def _validate_screen_query(query: str) -> bool:
    """Basic SQL injection prevention for screening queries.

    Only allows WHERE-clause-style conditions. Blocks dangerous keywords.
    """
    upper = query.upper().strip()
    # Block dangerous SQL keywords
    blocked = [
        "DROP", "DELETE", "INSERT", "UPDATE", "ALTER", "CREATE",
        "TRUNCATE", "GRANT", "REVOKE", "EXEC", "EXECUTE",
        "INTO", "UNION", "JOIN", ";", "--", "/*",
    ]
    for kw in blocked:
        if kw in upper:
            return False
    # Must look like a WHERE condition
    if not re.search(r'[=<>]', query):
        return False
    return True


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


# Cache for LLM-generated suggestions: {cache_key: (suggestions, timestamp)}
_suggestions_cache: dict[str, tuple[list[str], float]] = {}
_SUGGESTIONS_CACHE_TTL = 4 * 3600  # 4 hours


async def generate_suggestions(device_id: str | None = None) -> list[str]:
    """Generate dynamic LLM-powered suggested prompts with watchlist context.

    Caches per device_id (or global) for 4 hours to avoid excess LLM calls.
    Falls back to time-based static suggestions on LLM failure.
    """
    cache_key = device_id or "_global"
    now_ts = time.time()

    # Check cache
    if cache_key in _suggestions_cache:
        cached, ts = _suggestions_cache[cache_key]
        if now_ts - ts < _SUGGESTIONS_CACHE_TTL:
            return cached

    try:
        # Gather context for the LLM
        now = datetime.now(timezone.utc)
        ist_hour = (now.hour + 5) % 24 + (30 // 60)

        context_parts = []

        # Time context
        if ist_hour < 9:
            context_parts.append("Time: Pre-market (before 9:15 AM IST)")
        elif ist_hour < 15:
            context_parts.append("Time: Market hours (9:15 AM - 3:30 PM IST)")
        elif ist_hour < 20:
            context_parts.append("Time: Post-market (after 3:30 PM IST)")
        else:
            context_parts.append("Time: Evening")

        # Market context — market_service returns asset/price/change_percent
        try:
            from app.services.market_service import get_latest_prices
            prices = await get_latest_prices(instrument_type="index")
            nifty = None
            for p in (prices or []):
                if str(p.get("asset") or "").lower() == "nifty 50":
                    nifty = p
                    break
            if nifty and nifty.get("price") is not None:
                change = nifty.get("change_percent") or 0
                direction = "up" if change >= 0 else "down"
                context_parts.append(
                    f"Nifty 50: {nifty.get('price'):.0f} ({direction} {abs(change):.1f}%)"
                )
        except Exception:
            pass

        # Watchlist context (table is device_watchlists, column is asset)
        watchlist_symbols = []
        if device_id:
            try:
                pool = await get_pool()
                rows = await pool.fetch(
                    "SELECT asset FROM device_watchlists "
                    "WHERE device_id = $1 ORDER BY position ASC LIMIT 10",
                    device_id,
                )
                watchlist_symbols = [r["asset"] for r in rows if r.get("asset")]
                if watchlist_symbols:
                    context_parts.append(f"User's watchlist: {', '.join(watchlist_symbols[:8])}")
            except Exception:
                pass

        # Top movers for context
        try:
            pool = await get_pool()
            top_mover = await pool.fetchrow(
                "SELECT symbol, percent_change FROM discover_stock_snapshots "
                "WHERE percent_change IS NOT NULL ORDER BY percent_change DESC LIMIT 1"
            )
            if top_mover:
                context_parts.append(
                    f"Today's top gainer: {top_mover['symbol']} ({top_mover['percent_change']:+.1f}%)"
                )
        except Exception:
            pass

        context_str = "\n".join(context_parts) if context_parts else "No additional context."

        # Call LLM
        api_key = _get_api_key()
        if not api_key:
            return _static_suggestions(ist_hour)

        prompt_messages = [
            {
                "role": "system",
                "content": (
                    "You generate exactly 6 short suggested prompts in English for an "
                    "Indian market AI chatbot called Artha. "
                    "Each prompt should be a natural question a retail investor would ask. "
                    "Make them diverse: mix stocks, MFs, macro, IPOs, tax, commodities. "
                    "Make them contextual to the current time and market conditions. "
                    "If the user has a watchlist, include 1-2 prompts about their stocks. "
                    "Output ONLY the 6 English prompts, one per line, no numbering, no bullets."
                ),
            },
            {
                "role": "user",
                "content": (
                    f"Current context:\n{context_str}\n\n"
                    "Generate 6 suggested prompts in English:"
                ),
            },
        ]

        result = await _call_llm_blocking(api_key, prompt_messages, max_tokens=200)
        if result:
            lines = [
                line.strip().lstrip("0123456789.-) ")
                for line in result.strip().split("\n")
                if line.strip() and len(line.strip()) > 5
            ][:6]
            if len(lines) >= 4:
                _suggestions_cache[cache_key] = (lines, now_ts)
                return lines

        # Fallback
        return _static_suggestions(ist_hour)

    except Exception as e:
        logger.warning("Failed to generate LLM suggestions: %s", e)
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
    api_key = _get_api_key()
    if not api_key:
        yield {"event": "error", "data": {"message": "AI service is temporarily unavailable.", "retry": True}}
        return

    # Save user message
    await save_message(session_id, "user", user_message)
    await increment_rate_limit(device_id)

    # Build conversation context
    messages = await _build_context(session_id, device_id)

    yield {"event": "thinking", "data": {"status": "Artha is thinking..."}}

    # Phase 1: Initial LLM call (may contain tool markers)
    initial_response = await _call_llm_blocking(api_key, messages)
    if not initial_response:
        yield {"event": "error", "data": {"message": "Artha is taking a break. Try again in a few minutes.", "retry": True}}
        return

    # Phase 2: Parse and execute tools
    tool_markers = _TOOL_PATTERN.findall(initial_response)
    tool_results = {}
    stock_cards = []
    mf_cards = []
    tools_used = []

    if tool_markers:
        yield {"event": "thinking", "data": {"status": "Querying data..."}}

        for tool_name, params_str in tool_markers:
            try:
                params = json.loads(params_str)
            except json.JSONDecodeError:
                params = {}
            result = await _execute_tool(tool_name, params, device_id)
            tool_results[tool_name] = result
            tools_used.append({"tool": tool_name, "params": params})

            # Extract stock cards from results
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

            # Extract MF cards
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

        # Phase 3: Re-call LLM with tool results injected
        tool_context = "\n\n--- Tool Results ---\n"
        for tn, tr in tool_results.items():
            tool_context += f"\n[{tn}]: {json.dumps(tr, default=str)[:2000]}\n"

        messages.append({"role": "assistant", "content": initial_response})
        messages.append({
            "role": "user",
            "content": f"Here are the tool results. Now write your response using this data. "
                       f"Do NOT include any [TOOL:...] markers in your response. "
                       f"Be specific with the numbers.{tool_context}",
        })

        yield {"event": "thinking", "data": {"status": "Composing response..."}}
        final_response = await _call_llm_blocking(api_key, messages)
        if not final_response:
            # Fall back to initial response with tool markers stripped
            final_response = _TOOL_PATTERN.sub("", initial_response).strip()
            if not final_response:
                final_response = "I found the data but couldn't generate a summary. Please try again."
    else:
        final_response = initial_response

    # Clean response
    final_response = _clean_response(final_response)
    final_response = _TOOL_PATTERN.sub("", final_response).strip()
    final_response = _CARD_PATTERN.sub("", final_response).strip()

    # Extract follow-up suggestions before streaming
    follow_ups = []
    suggestions_match = _SUGGESTIONS_PATTERN.search(final_response)
    if suggestions_match:
        raw = suggestions_match.group(1).strip()
        follow_ups = [
            line.lstrip("- ").strip()
            for line in raw.split("\n")
            if line.strip() and line.strip() != "-"
        ][:4]
        final_response = _SUGGESTIONS_PATTERN.sub("", final_response).strip()

    # Stream tokens (simulate word-by-word for now — real streaming when model supports it)
    words = final_response.split()
    buffer = ""
    for i, word in enumerate(words):
        buffer += word + " "
        # Send in chunks of ~3-5 words for smooth UX
        if len(buffer.split()) >= 4 or i == len(words) - 1:
            yield {"event": "token", "data": {"text": buffer}}
            buffer = ""

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
    msg_id = await save_message(
        session_id,
        "assistant",
        final_response,
        stock_cards=stock_cards if stock_cards else None,
        mf_cards=mf_cards if mf_cards else None,
        tool_calls=tools_used if tools_used else None,
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


async def _call_llm_blocking(api_key: str, messages: list[dict], max_tokens: int | None = None) -> str | None:
    """Call OpenRouter LLM (blocking, not streaming). Returns text or None."""
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://econatlas.com",
        "X-Title": "EconAtlas Artha",
    }

    for model in _MODELS:
        try:
            async with httpx.AsyncClient(timeout=STREAM_TIMEOUT) as client:
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
                    logger.debug("Rate limited on %s, trying next", model)
                    continue
                resp.raise_for_status()
                data = resp.json()
                content = data["choices"][0]["message"].get("content")
                if not content:
                    continue
                text = content.strip()
                if not text:
                    continue
                logger.debug("Artha LLM response from %s", model)
                return text
        except Exception:
            logger.debug("Artha LLM call failed for %s", model, exc_info=True)
            continue

    logger.warning("All AI models failed for Artha chat")
    return None


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

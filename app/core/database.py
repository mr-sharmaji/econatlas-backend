"""PostgreSQL connection pool and lifecycle."""
from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from uuid import UUID

import asyncpg

from app.core.config import get_settings

logger = logging.getLogger(__name__)


def parse_ts(ts: str | datetime | None):
    """Return a timezone-aware datetime for asyncpg. Accepts None, datetime, or ISO str."""
    if ts is None:
        return None
    if isinstance(ts, datetime):
        return ts
    if isinstance(ts, str):
        s = ts.replace("Z", "+00:00")
        return datetime.fromisoformat(s)
    return ts

_pool: asyncpg.Pool | None = None

# Project root (parent of app/)
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
_INIT_SQL_PATH = _PROJECT_ROOT / "sql" / "init.sql"


def _strip_sql_line_comments(sql: str) -> str:
    """Drop full-line '--' comments so statement splitting stays deterministic."""
    lines: list[str] = []
    for line in sql.splitlines():
        if line.lstrip().startswith("--"):
            continue
        lines.append(line)
    return "\n".join(lines)


def record_to_dict(record: asyncpg.Record) -> dict:
    """Convert asyncpg Record to a JSON-serializable dict (id/dates as str)."""
    out = {}
    for k, v in zip(record.keys(), record.values()):
        if v is None:
            out[k] = None
        elif isinstance(v, UUID):
            out[k] = str(v)
        elif hasattr(v, "isoformat"):
            out[k] = v.isoformat()
        else:
            out[k] = v
    return out


async def get_pool() -> asyncpg.Pool:
    """Return the application's connection pool. Must be called after startup."""
    if _pool is None:
        raise RuntimeError("Database pool not initialized; app may not have started.")
    return _pool


async def ensure_vector_registered(conn: asyncpg.Connection) -> bool:
    """Register pgvector's VECTOR type on a connection. Idempotent.

    Must be called explicitly before running any query that binds or
    returns a VECTOR value on a given connection — we don't use the
    pool's init hook because the hook runs before the CREATE EXTENSION
    migration, creating a chicken-and-egg problem.

    Returns True on success, False if the extension isn't installed or
    the pgvector Python driver is missing (callers should fall back to
    non-vector search in that case).
    """
    try:
        from pgvector.asyncpg import register_vector
        await register_vector(conn)
        return True
    except Exception:
        return False


async def init_pool() -> asyncpg.Pool:
    """Create the connection pool and run schema if present. Called during app lifespan."""
    global _pool
    settings = get_settings()
    _pool = await asyncpg.create_pool(
        settings.database_url,
        min_size=1,
        max_size=10,
        command_timeout=60,
    )
    logger.info("Database pool created")
    async with _pool.acquire() as conn:
        if _INIT_SQL_PATH.exists():
            sql = _strip_sql_line_comments(_INIT_SQL_PATH.read_text())
            for raw in sql.split(";"):
                stmt = raw.strip()
                if not stmt:
                    continue
                up = stmt.upper()
                # Defer this one until after duplicate cleanup below.
                if "IDX_MARKET_PRICES_INTRADAY_ASSET_TYPE_TS_UNIQUE" in up:
                    continue
                if "IDX_MARKET_PRICES_INTRADAY_ASSET_TYPE_SOURCE_TS" in up:
                    continue
                if "IDX_MARKET_PRICES_INTRADAY_ASSET_TYPE_SOURCE_TS_PROVIDER_UNIQUE" in up:
                    continue
                if "IDX_IPO_SNAPSHOTS_ARCHIVED_AT" in up:
                    continue
                if up.startswith("CREATE") or up.startswith("ALTER") or up.startswith("DROP"):
                    await conn.execute(stmt)
            logger.info("Schema init executed from sql/init.sql")
        # --- Extensions ---
        # pgvector: required for semantic search on news_articles.
        # pg_trgm: required for fuzzy trigram matching on text columns.
        # Both are idempotent — safe to run on existing databases.
        try:
            await conn.execute("CREATE EXTENSION IF NOT EXISTS vector")
            logger.info("pgvector extension ensured")
        except Exception as e:
            logger.warning(
                "Failed to create 'vector' extension (semantic news search "
                "will fall back to trigram-only): %s", e
            )
        try:
            await conn.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
            logger.info("pg_trgm extension ensured")
        except Exception as e:
            logger.warning("Failed to create 'pg_trgm' extension: %s", e)
        # Ensure idempotent-insert indexes exist (for ON CONFLICT).
        await conn.execute(
            'CREATE UNIQUE INDEX IF NOT EXISTS idx_market_prices_asset_type_ts '
            'ON market_prices (asset, instrument_type, "timestamp")'
        )
        await conn.execute(
            'CREATE UNIQUE INDEX IF NOT EXISTS idx_macro_indicators_name_country_ts '
            'ON macro_indicators (indicator_name, country, "timestamp")'
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS macro_forecasts (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                indicator_name TEXT NOT NULL,
                country TEXT NOT NULL,
                forecast_year INTEGER NOT NULL,
                value DOUBLE PRECISION NOT NULL,
                source TEXT,
                fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        await conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_macro_forecasts_key "
            "ON macro_forecasts (indicator_name, country, forecast_year)"
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS economic_calendar (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                event_name TEXT NOT NULL,
                institution TEXT NOT NULL,
                event_date DATE NOT NULL,
                country TEXT NOT NULL,
                event_type TEXT NOT NULL,
                description TEXT,
                source TEXT
            )
            """
        )
        await conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_economic_calendar_name_date "
            "ON economic_calendar (event_name, event_date)"
        )
        await conn.execute(
            "ALTER TABLE economic_calendar ADD COLUMN IF NOT EXISTS importance TEXT"
        )
        await conn.execute(
            "ALTER TABLE economic_calendar ADD COLUMN IF NOT EXISTS previous DOUBLE PRECISION"
        )
        await conn.execute(
            "ALTER TABLE economic_calendar ADD COLUMN IF NOT EXISTS consensus DOUBLE PRECISION"
        )
        await conn.execute(
            "ALTER TABLE economic_calendar ADD COLUMN IF NOT EXISTS actual DOUBLE PRECISION"
        )
        await conn.execute(
            "ALTER TABLE economic_calendar ADD COLUMN IF NOT EXISTS surprise DOUBLE PRECISION"
        )
        await conn.execute(
            "ALTER TABLE economic_calendar ADD COLUMN IF NOT EXISTS status TEXT"
        )
        await conn.execute(
            "ALTER TABLE economic_calendar ADD COLUMN IF NOT EXISTS revised_at TIMESTAMPTZ"
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS device_watchlists (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                device_id TEXT NOT NULL,
                asset TEXT NOT NULL,
                position INTEGER NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        await conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_device_watchlists_device_asset_unique "
            "ON device_watchlists (device_id, asset)"
        )
        await conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_device_watchlists_device_position_unique "
            "ON device_watchlists (device_id, position)"
        )
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_device_watchlists_device_position "
            "ON device_watchlists (device_id, position ASC)"
        )
        # Discover stock scoring: Volatility + Growth + 3M change columns.
        await conn.execute(
            "ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS score_volatility DOUBLE PRECISION NOT NULL DEFAULT 0"
        )
        await conn.execute(
            "ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS score_growth DOUBLE PRECISION NOT NULL DEFAULT 0"
        )
        await conn.execute(
            "ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS percent_change_3m DOUBLE PRECISION"
        )
        await conn.execute(
            "ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS percent_change_1w DOUBLE PRECISION"
        )
        # MF dual ranking: sub-category rank columns
        await conn.execute(
            "ALTER TABLE discover_mutual_fund_snapshots ADD COLUMN IF NOT EXISTS sub_category_rank INTEGER"
        )
        await conn.execute(
            "ALTER TABLE discover_mutual_fund_snapshots ADD COLUMN IF NOT EXISTS sub_category_total INTEGER"
        )
        # IPO snapshot backward-compatible columns for Closed tab and retention.
        await conn.execute(
            "ALTER TABLE ipo_snapshots ADD COLUMN IF NOT EXISTS listing_price DOUBLE PRECISION"
        )
        await conn.execute(
            "ALTER TABLE ipo_snapshots ADD COLUMN IF NOT EXISTS listing_gain_pct DOUBLE PRECISION"
        )
        await conn.execute(
            "ALTER TABLE ipo_snapshots ADD COLUMN IF NOT EXISTS outcome_state TEXT"
        )
        await conn.execute(
            "ALTER TABLE ipo_snapshots ADD COLUMN IF NOT EXISTS archived_at TIMESTAMPTZ"
        )
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_ipo_snapshots_archived_at ON ipo_snapshots (archived_at)"
        )
        # Tax sync metadata columns (backward-compatible migration).
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS tax_config_versions (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                version TEXT NOT NULL UNIQUE,
                default_fy TEXT NOT NULL,
                disclaimer TEXT NOT NULL,
                supported_fy JSONB NOT NULL,
                helper_points JSONB NOT NULL DEFAULT '{"hub":[],"income_tax":[],"capital_gains":[],"advance_tax":[],"tds":[]}'::jsonb,
                rounding_policy JSONB NOT NULL,
                rules_by_fy JSONB NOT NULL,
                content_hash TEXT NOT NULL,
                source TEXT,
                source_mode TEXT,
                is_active BOOLEAN NOT NULL DEFAULT FALSE,
                archived_at TIMESTAMPTZ,
                last_validation_status TEXT,
                last_validation_reason TEXT,
                last_sync_attempt_at TIMESTAMPTZ,
                last_sync_success_at TIMESTAMPTZ,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        await conn.execute(
            "ALTER TABLE tax_config_versions ADD COLUMN IF NOT EXISTS source_mode TEXT"
        )
        await conn.execute(
            "ALTER TABLE tax_config_versions ADD COLUMN IF NOT EXISTS helper_points JSONB"
        )
        await conn.execute(
            """
            UPDATE tax_config_versions
            SET helper_points = '{"hub":[],"income_tax":[],"capital_gains":[],"advance_tax":[],"tds":[]}'::jsonb
            WHERE helper_points IS NULL
            """
        )
        await conn.execute(
            "ALTER TABLE tax_config_versions ALTER COLUMN helper_points SET DEFAULT '{\"hub\":[],\"income_tax\":[],\"capital_gains\":[],\"advance_tax\":[],\"tds\":[]}'::jsonb"
        )
        await conn.execute(
            "ALTER TABLE tax_config_versions ALTER COLUMN helper_points SET NOT NULL"
        )
        await conn.execute(
            "ALTER TABLE tax_config_versions ADD COLUMN IF NOT EXISTS archived_at TIMESTAMPTZ"
        )
        await conn.execute(
            "ALTER TABLE tax_config_versions ADD COLUMN IF NOT EXISTS last_validation_status TEXT"
        )
        await conn.execute(
            "ALTER TABLE tax_config_versions ADD COLUMN IF NOT EXISTS last_validation_reason TEXT"
        )
        await conn.execute(
            "ALTER TABLE tax_config_versions ADD COLUMN IF NOT EXISTS last_sync_attempt_at TIMESTAMPTZ"
        )
        await conn.execute(
            "ALTER TABLE tax_config_versions ADD COLUMN IF NOT EXISTS last_sync_success_at TIMESTAMPTZ"
        )
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_tax_config_versions_active ON tax_config_versions (is_active) WHERE is_active = TRUE"
        )
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_tax_config_versions_archived_at ON tax_config_versions (archived_at)"
        )
        await conn.execute(
            "DROP TABLE IF EXISTS tax_validation_cases"
        )
        # Intraday canonical tick metadata columns (backward-compatible migration).
        await conn.execute(
            'ALTER TABLE market_prices_intraday ADD COLUMN IF NOT EXISTS source_timestamp TIMESTAMPTZ'
        )
        await conn.execute(
            'ALTER TABLE market_prices_intraday ADD COLUMN IF NOT EXISTS ingested_at TIMESTAMPTZ'
        )
        await conn.execute(
            "ALTER TABLE market_prices_intraday ADD COLUMN IF NOT EXISTS provider TEXT"
        )
        await conn.execute(
            "ALTER TABLE market_prices_intraday ADD COLUMN IF NOT EXISTS provider_priority INTEGER"
        )
        await conn.execute(
            "ALTER TABLE market_prices_intraday ADD COLUMN IF NOT EXISTS confidence_level DOUBLE PRECISION"
        )
        await conn.execute(
            "ALTER TABLE market_prices_intraday ADD COLUMN IF NOT EXISTS is_fallback BOOLEAN"
        )
        await conn.execute(
            "ALTER TABLE market_prices_intraday ADD COLUMN IF NOT EXISTS quality TEXT"
        )
        await conn.execute(
            "ALTER TABLE market_prices_intraday ADD COLUMN IF NOT EXISTS is_predictive BOOLEAN"
        )
        await conn.execute(
            "ALTER TABLE market_prices_intraday ADD COLUMN IF NOT EXISTS session_source TEXT"
        )
        # Backfill NULLs in batches to avoid startup timeout on large tables
        try:
            await conn.execute(
                """
                UPDATE market_prices_intraday
                SET source_timestamp = COALESCE(source_timestamp, "timestamp"),
                    ingested_at = COALESCE(ingested_at, NOW()),
                    provider = COALESCE(provider, 'unknown'),
                    provider_priority = COALESCE(provider_priority, 99),
                    is_fallback = COALESCE(is_fallback, FALSE),
                    is_predictive = COALESCE(is_predictive, FALSE)
                WHERE ctid = ANY(ARRAY(
                    SELECT ctid FROM market_prices_intraday
                    WHERE source_timestamp IS NULL
                       OR ingested_at IS NULL
                       OR provider IS NULL
                       OR provider_priority IS NULL
                       OR is_fallback IS NULL
                       OR is_predictive IS NULL
                    LIMIT 50000
                ))
                """,
                timeout=30,
            )
        except Exception:
            logger.warning("Intraday backfill skipped (timeout or no rows) — will retry next startup")
        await conn.execute('ALTER TABLE market_prices_intraday ALTER COLUMN source_timestamp SET NOT NULL')
        await conn.execute('ALTER TABLE market_prices_intraday ALTER COLUMN ingested_at SET NOT NULL')
        await conn.execute("ALTER TABLE market_prices_intraday ALTER COLUMN provider SET DEFAULT 'unknown'")
        await conn.execute("ALTER TABLE market_prices_intraday ALTER COLUMN provider_priority SET DEFAULT 99")
        await conn.execute("ALTER TABLE market_prices_intraday ALTER COLUMN is_fallback SET DEFAULT FALSE")
        await conn.execute("ALTER TABLE market_prices_intraday ALTER COLUMN is_predictive SET DEFAULT FALSE")
        await conn.execute('ALTER TABLE market_prices_intraday ALTER COLUMN provider SET NOT NULL')
        await conn.execute('ALTER TABLE market_prices_intraday ALTER COLUMN provider_priority SET NOT NULL')
        await conn.execute('ALTER TABLE market_prices_intraday ALTER COLUMN is_fallback SET NOT NULL')
        await conn.execute('ALTER TABLE market_prices_intraday ALTER COLUMN is_predictive SET NOT NULL')

        # Drop old uniqueness constraint to allow multi-provider ticks at same source timestamp.
        await conn.execute('DROP INDEX IF EXISTS idx_market_prices_intraday_asset_type_ts_unique')

        # Keep one row per canonical key before enforcing uniqueness.
        await conn.execute(
            """
            DELETE FROM market_prices_intraday a
            USING market_prices_intraday b
            WHERE a.asset = b.asset
              AND a.instrument_type = b.instrument_type
              AND a.source_timestamp = b.source_timestamp
              AND a.provider = b.provider
              AND a.ctid < b.ctid
            """,
            timeout=300,
        )
        await conn.execute(
            'CREATE INDEX IF NOT EXISTS idx_market_prices_intraday_asset_type_source_ts '
            'ON market_prices_intraday (asset, instrument_type, source_timestamp DESC)'
        )
        await conn.execute(
            'CREATE UNIQUE INDEX IF NOT EXISTS idx_market_prices_intraday_asset_type_source_ts_provider_unique '
            'ON market_prices_intraday (asset, instrument_type, source_timestamp, provider)'
        )
        # Job dead-letter queue table.
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS job_dead_letters (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                job_name TEXT NOT NULL,
                error_message TEXT NOT NULL,
                traceback TEXT,
                retry_count INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'dead',
                failed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                retried_at TIMESTAMPTZ,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_job_dead_letters_status ON job_dead_letters (status)"
        )
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_job_dead_letters_job_name ON job_dead_letters (job_name)"
        )
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_job_dead_letters_failed_at ON job_dead_letters (failed_at DESC)"
        )
        # MF holdings / portfolio columns
        await conn.execute(
            "ALTER TABLE discover_mutual_fund_snapshots ADD COLUMN IF NOT EXISTS top_holdings JSONB"
        )
        await conn.execute(
            "ALTER TABLE discover_mutual_fund_snapshots ADD COLUMN IF NOT EXISTS sector_allocation JSONB"
        )
        await conn.execute(
            "ALTER TABLE discover_mutual_fund_snapshots ADD COLUMN IF NOT EXISTS asset_allocation JSONB"
        )
        await conn.execute(
            "ALTER TABLE discover_mutual_fund_snapshots ADD COLUMN IF NOT EXISTS holdings_as_of DATE"
        )
        # --- Artha AI Chat tables ---
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS chat_sessions (
                id TEXT PRIMARY KEY,
                device_id TEXT NOT NULL,
                title TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_chat_sessions_device ON chat_sessions (device_id)"
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS chat_messages (
                id TEXT PRIMARY KEY,
                session_id TEXT NOT NULL REFERENCES chat_sessions(id) ON DELETE CASCADE,
                role TEXT NOT NULL,
                content TEXT NOT NULL,
                thinking_text TEXT,
                tool_calls JSONB,
                stock_cards JSONB,
                mf_cards JSONB,
                feedback INTEGER,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_chat_messages_session ON chat_messages (session_id)"
        )
        await conn.execute(
            "ALTER TABLE chat_messages ADD COLUMN IF NOT EXISTS thinking_text TEXT"
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS chat_rate_limits (
                device_id TEXT NOT NULL,
                date DATE NOT NULL DEFAULT CURRENT_DATE,
                count INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (device_id, date)
            )
            """
        )
        # --- notification_log: store FCM delivery identifiers for diagnostics ---
        # Lets us look up "was this push actually accepted by FCM, and what
        # was the message_id?" in cases where users report missed notifications.
        await conn.execute(
            "ALTER TABLE notification_log ADD COLUMN IF NOT EXISTS fcm_message_id TEXT"
        )
        await conn.execute(
            "ALTER TABLE notification_log ADD COLUMN IF NOT EXISTS fcm_topic TEXT"
        )
        # --- news_articles: clean up legacy duplicate URLs ---
        # A handful of rows (~10) were inserted with duplicate urls
        # before the unique index on url was in force. Those duplicates
        # prevent UPDATE operations on affected rows (index maintenance
        # fires a UniqueViolationError for the sibling row). The fix is
        # a one-time cleanup: for each duplicate URL keep the newest
        # row and delete the rest. Idempotent — re-running is a no-op.
        try:
            deleted = await conn.execute(
                """
                WITH dups AS (
                    SELECT id,
                           ROW_NUMBER() OVER (
                               PARTITION BY url ORDER BY timestamp DESC, id DESC
                           ) AS rn
                    FROM news_articles
                    WHERE url IS NOT NULL
                )
                DELETE FROM news_articles
                WHERE id IN (SELECT id FROM dups WHERE rn > 1)
                """
            )
            # conn.execute returns a status string like "DELETE 10"
            if deleted and deleted.startswith("DELETE"):
                n = int(deleted.split()[-1]) if deleted.split()[-1].isdigit() else 0
                if n > 0:
                    logger.info(
                        "news_articles: deleted %d legacy duplicate-URL rows", n,
                    )
        except Exception as e:
            logger.warning("news_articles: dup-URL cleanup failed: %s", e)
        # --- news_articles: semantic + fuzzy search infrastructure ---
        # 384-dim embedding from BAAI/bge-small-en-v1.5 (fastembed default).
        # Nullable — backfill happens async via /ops/jobs/trigger/news_embed.
        # The column and indexes only install if pgvector/pg_trgm are present;
        # otherwise the block is skipped gracefully so the app still boots on
        # a plain postgres instance (news tool falls back to ILIKE).
        try:
            await conn.execute(
                "ALTER TABLE news_articles ADD COLUMN IF NOT EXISTS embedding vector(384)"
            )
            # HNSW index is much faster than ivfflat for our query volume and
            # doesn't need training. cosine ops match the distance function
            # used by bge-small-en-v1.5.
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_news_articles_embedding_hnsw "
                "ON news_articles USING hnsw (embedding vector_cosine_ops)"
            )
            logger.info("news_articles.embedding column + HNSW index ensured")
        except Exception as e:
            logger.warning(
                "Failed to create news_articles.embedding (pgvector not "
                "available?): %s — semantic search disabled, trigram still works",
                e,
            )
        # Trigram GIN indexes for fuzzy text matching on title/summary.
        try:
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_news_articles_title_trgm "
                "ON news_articles USING gin (title gin_trgm_ops)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_news_articles_summary_trgm "
                "ON news_articles USING gin (summary gin_trgm_ops)"
            )
            logger.info("news_articles trigram indexes ensured")
        except Exception as e:
            logger.warning("Failed to create news trigram indexes: %s", e)
        # --- Artha semantic layer: embeddings on stock narratives + events ---
        # Discover stock snapshots get two embedding columns (one per narrative
        # column) so Artha can do semantic lookups against the pre-computed
        # ai_narrative / why_narrative text when the user asks open-ended
        # questions ("which IT stocks look defensive right now?").
        try:
            await conn.execute(
                "ALTER TABLE discover_stock_snapshots "
                "ADD COLUMN IF NOT EXISTS ai_narrative_embedding vector(384)"
            )
            await conn.execute(
                "ALTER TABLE discover_stock_snapshots "
                "ADD COLUMN IF NOT EXISTS why_narrative_embedding vector(384)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_dss_ai_narrative_emb "
                "ON discover_stock_snapshots USING hnsw "
                "(ai_narrative_embedding vector_cosine_ops)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_dss_why_narrative_emb "
                "ON discover_stock_snapshots USING hnsw "
                "(why_narrative_embedding vector_cosine_ops)"
            )
            logger.info("discover_stock_snapshots narrative embedding columns + HNSW indexes ensured")
        except Exception as e:
            logger.warning(
                "Failed to create discover_stock_snapshots embedding columns "
                "(pgvector missing?): %s", e,
            )
        # economic_events — 176K+ news_market_linked_signal rows. Embedding
        # the event_type + entity + impact triple lets Artha answer
        # "what macro events were triggered by RBI decisions last quarter?"
        try:
            await conn.execute(
                "ALTER TABLE economic_events "
                "ADD COLUMN IF NOT EXISTS event_embedding vector(384)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_economic_events_embedding_hnsw "
                "ON economic_events USING hnsw (event_embedding vector_cosine_ops)"
            )
            logger.info("economic_events embedding column + HNSW index ensured")
        except Exception as e:
            logger.warning("Failed to create economic_events embedding: %s", e)
        # artha_educational_concepts — seeded at startup with ~40 baseline
        # finance/investing concepts (P/E, ROCE, SIP, NPS, etc.) so Artha
        # can answer meta/educational queries without hitting an LLM tool.
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS artha_educational_concepts (
                id BIGSERIAL PRIMARY KEY,
                slug TEXT UNIQUE NOT NULL,
                title TEXT NOT NULL,
                body TEXT NOT NULL,
                category TEXT,
                embedding vector(384),
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        try:
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_edu_concepts_embedding_hnsw "
                "ON artha_educational_concepts USING hnsw "
                "(embedding vector_cosine_ops)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_edu_concepts_category "
                "ON artha_educational_concepts (category)"
            )
        except Exception as e:
            logger.warning("Failed to create edu concepts index: %s", e)
        # chat_tool_invocations — lightweight observability for tool-usage
        # patterns, refusals, and tool-level latency. Queryable via raw SQL,
        # no dashboard endpoint (ship it later if needed).
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS chat_tool_invocations (
                id BIGSERIAL PRIMARY KEY,
                session_id TEXT,
                message_id TEXT,
                tool_name TEXT NOT NULL,
                params JSONB,
                success BOOLEAN NOT NULL DEFAULT TRUE,
                refused BOOLEAN NOT NULL DEFAULT FALSE,
                result_size INTEGER,
                latency_ms INTEGER,
                error_message TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_tool_inv_session "
            "ON chat_tool_invocations (session_id)"
        )
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_tool_inv_tool "
            "ON chat_tool_invocations (tool_name, created_at DESC)"
        )
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_tool_inv_refused "
            "ON chat_tool_invocations (refused, created_at DESC) "
            "WHERE refused = TRUE"
        )
        # --- chat_messages.tool_calls column for inline debug-on-read ---
        # Stores a compact trace of every tool call made while composing
        # the assistant message: name, params, result_size, success,
        # latency_ms. Exposed via the /chat/sessions/{id} endpoint so the
        # UI can show "what tools actually fired" without hitting the
        # chat_tool_invocations table.
        await conn.execute(
            "ALTER TABLE chat_messages ADD COLUMN IF NOT EXISTS tool_calls JSONB"
        )
        # --- device_preferences: per-device user settings ---
        # verbosity      — concise/balanced/detailed, controls Artha's
        #                  output length + max_tokens per response
        # risk_tolerance — conservative/balanced/aggressive, shapes
        #                  recommendation defaults + warning emphasis
        # language       — en / hinglish, picks the composer voice
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS device_preferences (
                device_id TEXT PRIMARY KEY,
                verbosity TEXT NOT NULL DEFAULT 'balanced',
                risk_tolerance TEXT NOT NULL DEFAULT 'balanced',
                language TEXT NOT NULL DEFAULT 'en',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        # --- fixed_income_rates: curated live rates for FDs / PPF / G-Sec / etc. ---
        # Populated by a scheduled scraper (to be added later). Artha's
        # `fixed_income` tool reads this table for "best FD rates",
        # "G-Sec yields", "tax-free bonds" etc. The rate_source column
        # identifies the bank/issuer so we can attribute correctly.
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS fixed_income_rates (
                id BIGSERIAL PRIMARY KEY,
                instrument_type TEXT NOT NULL,
                rate_source TEXT NOT NULL,
                tenor TEXT,
                rate_pct DOUBLE PRECISION NOT NULL,
                min_amount DOUBLE PRECISION,
                tax_status TEXT,
                lock_in_years DOUBLE PRECISION,
                notes TEXT,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (instrument_type, rate_source, tenor)
            )
            """
        )
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_fixed_income_type "
            "ON fixed_income_rates (instrument_type, rate_pct DESC)"
        )
        # --- discover_stock_snapshots: max-drawdown columns for risk metrics ---
        # Populated at snapshot time from price history. Lets
        # factor_decomposition / portfolio_risk tools compute aggregate
        # worst-case drops without re-querying price history each call.
        await conn.execute(
            "ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS max_drawdown_1y DOUBLE PRECISION"
        )
        await conn.execute(
            "ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS max_drawdown_3y DOUBLE PRECISION"
        )
        await conn.execute(
            "ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS dividend_consistency_years INTEGER"
        )
        # --- ipo_snapshots: type classification for REIT/InvIT disambiguation ---
        # Avoids Artha comparing SM REIT unit prices (\u20b910L per SEBI rule)
        # to regular equity IPO prices as if they were the same category.
        await conn.execute(
            "ALTER TABLE ipo_snapshots ADD COLUMN IF NOT EXISTS ipo_type TEXT"
        )
        logger.info("Idempotent indexes ensured")
    return _pool


async def close_pool() -> None:
    """Close the connection pool. Called during app shutdown."""
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None
        logger.info("Database pool closed")

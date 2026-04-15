-- EconAtlas backend schema (PostgreSQL)

-- Extensions required for semantic + fuzzy search.
-- Requires the pgvector/pgvector:pg16-alpine image (or any Postgres with
-- pgvector installed). pg_trgm is a standard contrib extension shipped
-- with every Postgres distribution.
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Market and commodity prices
CREATE TABLE IF NOT EXISTS market_prices (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    asset TEXT NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    "timestamp" TIMESTAMPTZ NOT NULL,
    source TEXT,
    instrument_type TEXT,
    unit TEXT,
    change_percent DOUBLE PRECISION,
    previous_close DOUBLE PRECISION
);

CREATE INDEX IF NOT EXISTS idx_market_prices_timestamp ON market_prices ("timestamp" DESC);
CREATE INDEX IF NOT EXISTS idx_market_prices_asset ON market_prices (asset);
CREATE INDEX IF NOT EXISTS idx_market_prices_instrument_type ON market_prices (instrument_type);
-- Prevent duplicate (asset, instrument_type, date) so backfill re-runs are safe
CREATE UNIQUE INDEX IF NOT EXISTS idx_market_prices_asset_type_ts ON market_prices (asset, instrument_type, "timestamp");

CREATE TABLE IF NOT EXISTS market_prices_intraday (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    asset TEXT NOT NULL,
    instrument_type TEXT NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    "timestamp" TIMESTAMPTZ NOT NULL,
    source_timestamp TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    provider TEXT NOT NULL DEFAULT 'unknown',
    provider_priority INTEGER NOT NULL DEFAULT 99,
    confidence_level DOUBLE PRECISION,
    is_fallback BOOLEAN NOT NULL DEFAULT FALSE,
    quality TEXT,
    is_predictive BOOLEAN NOT NULL DEFAULT FALSE,
    session_source TEXT
);
CREATE INDEX IF NOT EXISTS idx_market_prices_intraday_asset_type_ts ON market_prices_intraday (asset, instrument_type, "timestamp" DESC);
CREATE INDEX IF NOT EXISTS idx_market_prices_intraday_asset_type_source_ts
ON market_prices_intraday (asset, instrument_type, source_timestamp DESC);
CREATE UNIQUE INDEX IF NOT EXISTS idx_market_prices_intraday_asset_type_source_ts_provider_unique
ON market_prices_intraday (asset, instrument_type, source_timestamp, provider);

-- Macro indicators
CREATE TABLE IF NOT EXISTS macro_indicators (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    indicator_name TEXT NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    country TEXT NOT NULL,
    "timestamp" TIMESTAMPTZ NOT NULL,
    unit TEXT,
    source TEXT
);

CREATE INDEX IF NOT EXISTS idx_macro_indicators_timestamp ON macro_indicators ("timestamp" DESC);
CREATE INDEX IF NOT EXISTS idx_macro_indicators_country ON macro_indicators (country);
-- Prevent duplicate (indicator_name, country, date) so backfill re-runs are safe
CREATE UNIQUE INDEX IF NOT EXISTS idx_macro_indicators_name_country_ts ON macro_indicators (indicator_name, country, "timestamp");

-- News articles
CREATE TABLE IF NOT EXISTS news_articles (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    title TEXT NOT NULL,
    summary TEXT,
    body TEXT,
    "timestamp" TIMESTAMPTZ NOT NULL,
    source TEXT,
    url TEXT,
    primary_entity TEXT,
    impact TEXT,
    confidence DOUBLE PRECISION,
    CONSTRAINT news_articles_url_key UNIQUE (url)
);

CREATE INDEX IF NOT EXISTS idx_news_articles_timestamp ON news_articles ("timestamp" DESC);
CREATE INDEX IF NOT EXISTS idx_news_articles_primary_entity ON news_articles (primary_entity);
CREATE INDEX IF NOT EXISTS idx_news_articles_source ON news_articles (source);

-- Economic events
CREATE TABLE IF NOT EXISTS economic_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_type TEXT NOT NULL,
    entity TEXT NOT NULL,
    impact TEXT NOT NULL,
    confidence DOUBLE PRECISION NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_economic_events_created_at ON economic_events (created_at DESC);

-- Stock future-prospects evidence store
CREATE TABLE IF NOT EXISTS stock_future_prospect_documents (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    symbol TEXT NOT NULL,
    display_name TEXT NOT NULL,
    source_kind TEXT NOT NULL,
    source_name TEXT,
    source_url TEXT,
    source_title TEXT,
    document_key TEXT NOT NULL,
    document_type TEXT NOT NULL,
    source_published_at TIMESTAMPTZ,
    recency_bucket TEXT,
    extracted_fields JSONB NOT NULL DEFAULT '{}'::jsonb,
    extraction_confidence DOUBLE PRECISION,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_stock_future_docs_key
ON stock_future_prospect_documents (document_key);
CREATE INDEX IF NOT EXISTS idx_stock_future_docs_symbol_published
ON stock_future_prospect_documents (symbol, source_published_at DESC);
CREATE INDEX IF NOT EXISTS idx_stock_future_docs_kind
ON stock_future_prospect_documents (source_kind, source_published_at DESC);

CREATE TABLE IF NOT EXISTS stock_future_prospect_passages (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    document_id UUID NOT NULL REFERENCES stock_future_prospect_documents(id) ON DELETE CASCADE,
    symbol TEXT NOT NULL,
    passage_index INTEGER NOT NULL,
    passage_type TEXT,
    passage_text TEXT NOT NULL,
    source_published_at TIMESTAMPTZ,
    embedding_status TEXT NOT NULL DEFAULT 'pending',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_stock_future_passages_doc_idx
ON stock_future_prospect_passages (document_id, passage_index);
CREATE INDEX IF NOT EXISTS idx_stock_future_passages_symbol_published
ON stock_future_prospect_passages (symbol, source_published_at DESC);
CREATE INDEX IF NOT EXISTS idx_stock_future_passages_embedding_status
ON stock_future_prospect_passages (embedding_status, created_at DESC);

-- Devices (for future push notifications)
CREATE TABLE IF NOT EXISTS devices (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id TEXT,
    device_token TEXT,
    platform TEXT
);

-- Device-scoped watchlists (no auth v1)
CREATE TABLE IF NOT EXISTS device_watchlists (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    device_id TEXT NOT NULL,
    asset TEXT NOT NULL,
    position INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_device_watchlists_device_asset_unique
ON device_watchlists (device_id, asset);
CREATE UNIQUE INDEX IF NOT EXISTS idx_device_watchlists_device_position_unique
ON device_watchlists (device_id, position);
CREATE INDEX IF NOT EXISTS idx_device_watchlists_device_position
ON device_watchlists (device_id, position ASC);

-- Stock snapshots for Brief tab (country-level movers/active/sector pulse)
CREATE TABLE IF NOT EXISTS stock_snapshots (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    market TEXT NOT NULL, -- IN | US
    symbol TEXT NOT NULL,
    display_name TEXT NOT NULL,
    sector TEXT,
    last_price DOUBLE PRECISION NOT NULL,
    point_change DOUBLE PRECISION,
    percent_change DOUBLE PRECISION,
    volume BIGINT,
    traded_value DOUBLE PRECISION,
    source_timestamp TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_stock_snapshots_market_symbol_unique
ON stock_snapshots (market, symbol);
CREATE INDEX IF NOT EXISTS idx_stock_snapshots_market_pct
ON stock_snapshots (market, percent_change DESC);
CREATE INDEX IF NOT EXISTS idx_stock_snapshots_market_active
ON stock_snapshots (market, traded_value DESC);
CREATE INDEX IF NOT EXISTS idx_stock_snapshots_market_source_ts
ON stock_snapshots (market, source_timestamp DESC);

-- Discover stock snapshots (India-focused screener)
CREATE TABLE IF NOT EXISTS discover_stock_snapshots (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    market TEXT NOT NULL DEFAULT 'IN',
    symbol TEXT NOT NULL,
    display_name TEXT NOT NULL,
    sector TEXT,
    last_price DOUBLE PRECISION NOT NULL,
    point_change DOUBLE PRECISION,
    percent_change DOUBLE PRECISION,
    volume BIGINT,
    traded_value DOUBLE PRECISION,
    pe_ratio DOUBLE PRECISION,
    roe DOUBLE PRECISION,
    roce DOUBLE PRECISION,
    debt_to_equity DOUBLE PRECISION,
    price_to_book DOUBLE PRECISION,
    eps DOUBLE PRECISION,
    score DOUBLE PRECISION NOT NULL DEFAULT 0,
    score_momentum DOUBLE PRECISION NOT NULL DEFAULT 0,
    score_liquidity DOUBLE PRECISION NOT NULL DEFAULT 0,
    score_fundamentals DOUBLE PRECISION NOT NULL DEFAULT 0,
    score_breakdown JSONB NOT NULL DEFAULT '{}'::jsonb,
    tags JSONB NOT NULL DEFAULT '[]'::jsonb,
    tags_v2 JSONB NOT NULL DEFAULT '[]'::jsonb,   -- structured tags: [{tag, category, severity, priority, confidence, explanation, expires_at}]
    source_status TEXT NOT NULL DEFAULT 'limited', -- primary | fallback | limited
    source_timestamp TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    primary_source TEXT,
    secondary_source TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_discover_stock_snapshots_symbol_unique
ON discover_stock_snapshots (symbol);
CREATE INDEX IF NOT EXISTS idx_discover_stock_snapshots_score
ON discover_stock_snapshots (score DESC);
CREATE INDEX IF NOT EXISTS idx_discover_stock_snapshots_sector
ON discover_stock_snapshots (sector);
CREATE INDEX IF NOT EXISTS idx_discover_stock_snapshots_source_ts
ON discover_stock_snapshots (source_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_discover_stock_snapshots_status
ON discover_stock_snapshots (source_status);

-- Discover mutual fund snapshots (India direct plans)
CREATE TABLE IF NOT EXISTS discover_mutual_fund_snapshots (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    scheme_code TEXT NOT NULL,
    scheme_name TEXT NOT NULL,
    amc TEXT,
    category TEXT,
    sub_category TEXT,
    plan_type TEXT NOT NULL DEFAULT 'direct', -- direct | regular
    option_type TEXT,
    nav DOUBLE PRECISION NOT NULL,
    nav_date DATE,
    expense_ratio DOUBLE PRECISION,
    aum_cr DOUBLE PRECISION,
    risk_level TEXT,
    returns_1y DOUBLE PRECISION,
    returns_3y DOUBLE PRECISION,
    returns_5y DOUBLE PRECISION,
    std_dev DOUBLE PRECISION,
    sharpe DOUBLE PRECISION,
    sortino DOUBLE PRECISION,
    score DOUBLE PRECISION NOT NULL DEFAULT 0,
    score_return DOUBLE PRECISION NOT NULL DEFAULT 0,
    score_risk DOUBLE PRECISION NOT NULL DEFAULT 0,
    score_cost DOUBLE PRECISION NOT NULL DEFAULT 0,
    score_consistency DOUBLE PRECISION NOT NULL DEFAULT 0,
    score_breakdown JSONB NOT NULL DEFAULT '{}'::jsonb,
    tags JSONB NOT NULL DEFAULT '[]'::jsonb,
    tags_v2 JSONB NOT NULL DEFAULT '[]'::jsonb,   -- structured tags: [{tag, category, severity, priority, confidence, explanation, expires_at}]
    source_status TEXT NOT NULL DEFAULT 'limited', -- primary | fallback | limited
    source_timestamp TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    primary_source TEXT,
    secondary_source TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_discover_mutual_fund_scheme_code_unique
ON discover_mutual_fund_snapshots (scheme_code);
CREATE INDEX IF NOT EXISTS idx_discover_mutual_fund_score
ON discover_mutual_fund_snapshots (score DESC);
CREATE INDEX IF NOT EXISTS idx_discover_mutual_fund_category
ON discover_mutual_fund_snapshots (category);
CREATE INDEX IF NOT EXISTS idx_discover_mutual_fund_plan_type
ON discover_mutual_fund_snapshots (plan_type);
CREATE INDEX IF NOT EXISTS idx_discover_mutual_fund_source_ts
ON discover_mutual_fund_snapshots (source_timestamp DESC);

-- IPO snapshots for Overview IPO card
CREATE TABLE IF NOT EXISTS ipo_snapshots (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    symbol TEXT NOT NULL,
    company_name TEXT NOT NULL,
    market TEXT NOT NULL DEFAULT 'IN',
    status TEXT NOT NULL, -- open | upcoming | closed
    ipo_type TEXT NOT NULL, -- mainboard | sme
    issue_size_cr DOUBLE PRECISION,
    price_band TEXT,
    gmp_percent DOUBLE PRECISION,
    subscription_multiple DOUBLE PRECISION,
    listing_price DOUBLE PRECISION,
    listing_gain_pct DOUBLE PRECISION,
    outcome_state TEXT, -- listed | awaiting_listing_data
    open_date DATE,
    close_date DATE,
    listing_date DATE,
    source_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    archived_at TIMESTAMPTZ,
    source TEXT,
    notes TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_ipo_snapshots_symbol_unique
ON ipo_snapshots (symbol);
CREATE INDEX IF NOT EXISTS idx_ipo_snapshots_status_dates
ON ipo_snapshots (status, open_date ASC, close_date ASC);
CREATE INDEX IF NOT EXISTS idx_ipo_snapshots_archived_at
ON ipo_snapshots (archived_at);

-- Device-scoped IPO alert selections
CREATE TABLE IF NOT EXISTS device_ipo_alerts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    device_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_device_ipo_alerts_device_symbol_unique
ON device_ipo_alerts (device_id, symbol);
CREATE INDEX IF NOT EXISTS idx_device_ipo_alerts_device
ON device_ipo_alerts (device_id);

-- User feedback submissions from app settings
CREATE TABLE IF NOT EXISTS feedback_submissions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    device_id TEXT NOT NULL,
    category TEXT NOT NULL,
    message TEXT NOT NULL,
    app_version TEXT,
    platform TEXT,
    status TEXT NOT NULL DEFAULT 'received',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_feedback_submissions_created_at
ON feedback_submissions (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_feedback_submissions_category
ON feedback_submissions (category);

-- Tax configuration (DB-backed; no hardcoded backend rule constants)
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
);

CREATE INDEX IF NOT EXISTS idx_tax_config_versions_active
ON tax_config_versions (is_active)
WHERE is_active = TRUE;

CREATE INDEX IF NOT EXISTS idx_tax_config_versions_archived_at
ON tax_config_versions (archived_at);

DROP TABLE IF EXISTS tax_validation_cases;

-- ================================================================
-- Discover enrichment: additional stock columns
-- ================================================================
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS high_52w DOUBLE PRECISION;
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS low_52w DOUBLE PRECISION;
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS market_cap DOUBLE PRECISION;
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS dividend_yield DOUBLE PRECISION;
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS score_volatility DOUBLE PRECISION NOT NULL DEFAULT 0;
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS score_growth DOUBLE PRECISION NOT NULL DEFAULT 0;
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS percent_change_3m DOUBLE PRECISION;
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS percent_change_1w DOUBLE PRECISION;

-- Discover enrichment: additional MF columns
ALTER TABLE discover_mutual_fund_snapshots ADD COLUMN IF NOT EXISTS category_rank INTEGER;
ALTER TABLE discover_mutual_fund_snapshots ADD COLUMN IF NOT EXISTS category_total INTEGER;
ALTER TABLE discover_mutual_fund_snapshots ADD COLUMN IF NOT EXISTS sub_category_rank INTEGER;
ALTER TABLE discover_mutual_fund_snapshots ADD COLUMN IF NOT EXISTS sub_category_total INTEGER;
ALTER TABLE discover_mutual_fund_snapshots ADD COLUMN IF NOT EXISTS fund_age_years DOUBLE PRECISION;

-- Historical stock prices for charts
CREATE TABLE IF NOT EXISTS discover_stock_price_history (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    symbol TEXT NOT NULL,
    trade_date DATE NOT NULL,
    close DOUBLE PRECISION NOT NULL,
    volume BIGINT,
    source TEXT,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_discover_stock_price_history_uniq
ON discover_stock_price_history (symbol, trade_date);
CREATE INDEX IF NOT EXISTS idx_discover_stock_price_history_lookup
ON discover_stock_price_history (symbol, trade_date DESC);

-- Historical MF NAV for charts
CREATE TABLE IF NOT EXISTS discover_mf_nav_history (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    scheme_code TEXT NOT NULL,
    nav_date DATE NOT NULL,
    nav DOUBLE PRECISION NOT NULL,
    source TEXT,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_discover_mf_nav_history_uniq
ON discover_mf_nav_history (scheme_code, nav_date);
CREATE INDEX IF NOT EXISTS idx_discover_mf_nav_history_lookup
ON discover_mf_nav_history (scheme_code, nav_date DESC);

-- ================================================================
-- Discover enrichment: additional stock columns (v0.2.3)
-- ================================================================
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS percent_change_1y DOUBLE PRECISION;
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS percent_change_3y DOUBLE PRECISION;
-- Allow NULL for score_volatility/score_growth (NULL = no data, not neutral 50)
ALTER TABLE discover_stock_snapshots ALTER COLUMN score_volatility DROP NOT NULL;
ALTER TABLE discover_stock_snapshots ALTER COLUMN score_volatility DROP DEFAULT;
ALTER TABLE discover_stock_snapshots ALTER COLUMN score_growth DROP NOT NULL;
ALTER TABLE discover_stock_snapshots ALTER COLUMN score_growth DROP DEFAULT;

-- ================================================================
-- Discover enrichment: additional MF columns (v0.2.3)
-- ================================================================
ALTER TABLE discover_mutual_fund_snapshots ADD COLUMN IF NOT EXISTS max_drawdown DOUBLE PRECISION;
ALTER TABLE discover_mutual_fund_snapshots ADD COLUMN IF NOT EXISTS rolling_return_consistency DOUBLE PRECISION;
ALTER TABLE discover_mutual_fund_snapshots ADD COLUMN IF NOT EXISTS alpha DOUBLE PRECISION;
ALTER TABLE discover_mutual_fund_snapshots ADD COLUMN IF NOT EXISTS beta DOUBLE PRECISION;
ALTER TABLE discover_mutual_fund_snapshots ADD COLUMN IF NOT EXISTS score_alpha DOUBLE PRECISION;
ALTER TABLE discover_mutual_fund_snapshots ADD COLUMN IF NOT EXISTS score_beta DOUBLE PRECISION;

-- ================================================================
-- Discover enrichment: stock fundamentals + shareholding + analyst (v0.2.4)
-- ================================================================
-- Shareholding (from Screener.in)
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS promoter_holding DOUBLE PRECISION;
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS fii_holding DOUBLE PRECISION;
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS dii_holding DOUBLE PRECISION;
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS government_holding DOUBLE PRECISION;
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS public_holding DOUBLE PRECISION;
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS num_shareholders BIGINT;
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS promoter_holding_change DOUBLE PRECISION;
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS fii_holding_change DOUBLE PRECISION;
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS dii_holding_change DOUBLE PRECISION;
-- Yahoo Finance exclusive fundamentals
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS beta DOUBLE PRECISION;
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS free_cash_flow DOUBLE PRECISION;
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS operating_cash_flow DOUBLE PRECISION;
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS total_cash DOUBLE PRECISION;
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS total_debt DOUBLE PRECISION;
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS total_revenue DOUBLE PRECISION;
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS gross_margins DOUBLE PRECISION;
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS operating_margins DOUBLE PRECISION;
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS profit_margins DOUBLE PRECISION;
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS revenue_growth DOUBLE PRECISION;
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS earnings_growth DOUBLE PRECISION;
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS forward_pe DOUBLE PRECISION;
-- Analyst data (from Yahoo Finance)
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS analyst_target_mean DOUBLE PRECISION;
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS analyst_count INTEGER;
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS analyst_recommendation TEXT;
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS analyst_recommendation_mean DOUBLE PRECISION;
-- New score components
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS score_ownership DOUBLE PRECISION;
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS score_financial_health DOUBLE PRECISION;
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS score_analyst DOUBLE PRECISION;
-- Industry sub-sector (from Screener.in)
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS industry TEXT;
-- Payout ratio (from Yahoo Finance)
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS payout_ratio DOUBLE PRECISION;

-- Dead-letter queue for failed background jobs
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
);

CREATE INDEX IF NOT EXISTS idx_job_dead_letters_status
ON job_dead_letters (status);
CREATE INDEX IF NOT EXISTS idx_job_dead_letters_job_name
ON job_dead_letters (job_name);
CREATE INDEX IF NOT EXISTS idx_job_dead_letters_failed_at
ON job_dead_letters (failed_at DESC);

-- ================================================================
-- Scoring v0.3: pledged shares + score history (expert panel rework)
-- ================================================================
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS pledged_promoter_pct DOUBLE PRECISION;

CREATE TABLE IF NOT EXISTS discover_stock_score_history (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    symbol TEXT NOT NULL,
    score DOUBLE PRECISION NOT NULL,
    scored_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_stock_score_history_symbol_scored
ON discover_stock_score_history (symbol, scored_at DESC);

-- ================================================================
-- Scoring v0.4: P&L, Balance Sheet, Cash Flow, Shareholder Trends
-- ================================================================

-- P&L derived signals (from Screener.in annual data)
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS sales_growth_yoy DOUBLE PRECISION;
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS profit_growth_yoy DOUBLE PRECISION;
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS opm_change DOUBLE PRECISION;
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS interest_coverage DOUBLE PRECISION;
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS compounded_sales_growth_3y DOUBLE PRECISION;
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS compounded_profit_growth_3y DOUBLE PRECISION;

-- Balance sheet derived signals
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS total_assets DOUBLE PRECISION;
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS asset_growth_yoy DOUBLE PRECISION;
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS reserves_growth DOUBLE PRECISION;
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS debt_direction DOUBLE PRECISION;
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS cwip DOUBLE PRECISION;

-- Cash flow (from Screener.in — fixes Yahoo OCF gap: 72 → ~2200 stocks)
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS cash_from_operations DOUBLE PRECISION;
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS cash_from_investing DOUBLE PRECISION;
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS cash_from_financing DOUBLE PRECISION;

-- Shareholder trends (QoQ and YoY)
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS num_shareholders_change_qoq DOUBLE PRECISION;
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS num_shareholders_change_yoy DOUBLE PRECISION;

-- Synthetic forward PE (computed from profit_growth_yoy when Yahoo forward PE missing)
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS synthetic_forward_pe DOUBLE PRECISION;

-- New score components
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS score_valuation DOUBLE PRECISION;
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS score_earnings_quality DOUBLE PRECISION;
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS score_smart_money DOUBLE PRECISION;

-- v0.5: Complete Screener financial tables (full YoY history as JSONB)
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS pl_annual JSONB;
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS bs_annual JSONB;
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS cf_annual JSONB;

-- v0.5.1: Allow score columns to be NULL so incremental (unscored) upserts
-- can use COALESCE to preserve existing scores without hitting NOT NULL.
ALTER TABLE discover_stock_snapshots ALTER COLUMN score DROP NOT NULL;
ALTER TABLE discover_stock_snapshots ALTER COLUMN score DROP DEFAULT;
ALTER TABLE discover_stock_snapshots ALTER COLUMN score_momentum DROP NOT NULL;
ALTER TABLE discover_stock_snapshots ALTER COLUMN score_momentum DROP DEFAULT;
ALTER TABLE discover_stock_snapshots ALTER COLUMN score_liquidity DROP NOT NULL;
ALTER TABLE discover_stock_snapshots ALTER COLUMN score_liquidity DROP DEFAULT;
ALTER TABLE discover_stock_snapshots ALTER COLUMN score_fundamentals DROP NOT NULL;
ALTER TABLE discover_stock_snapshots ALTER COLUMN score_fundamentals DROP DEFAULT;
ALTER TABLE discover_stock_snapshots ALTER COLUMN score_breakdown DROP NOT NULL;
ALTER TABLE discover_stock_snapshots ALTER COLUMN score_breakdown DROP DEFAULT;
ALTER TABLE discover_stock_snapshots ALTER COLUMN tags DROP NOT NULL;
ALTER TABLE discover_stock_snapshots ALTER COLUMN tags DROP DEFAULT;

-- v0.6: Shareholding quarterly history (12 quarters from Screener)
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS shareholding_quarterly JSONB;

-- v0.7: Scoring model overhaul — 6-layer model
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS percent_change_5y DOUBLE PRECISION;
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS sector_percentile DOUBLE PRECISION;
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS lynch_classification TEXT;
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS score_quality DOUBLE PRECISION;
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS score_institutional DOUBLE PRECISION;
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS score_risk DOUBLE PRECISION;

-- ================================================================
-- v0.8: MF scoring model overhaul — 5-layer model
-- ================================================================
ALTER TABLE discover_mutual_fund_snapshots ADD COLUMN IF NOT EXISTS score_performance DOUBLE PRECISION;
ALTER TABLE discover_mutual_fund_snapshots ADD COLUMN IF NOT EXISTS score_category_fit DOUBLE PRECISION;
ALTER TABLE discover_mutual_fund_snapshots ADD COLUMN IF NOT EXISTS sub_category_percentile DOUBLE PRECISION;
ALTER TABLE discover_mutual_fund_snapshots ADD COLUMN IF NOT EXISTS fund_classification TEXT;
-- Allow NULL for score (NULL = unrated, no data)
ALTER TABLE discover_mutual_fund_snapshots ALTER COLUMN score DROP NOT NULL;
ALTER TABLE discover_mutual_fund_snapshots ALTER COLUMN score DROP DEFAULT;
ALTER TABLE discover_mutual_fund_snapshots ALTER COLUMN score_return DROP NOT NULL;
ALTER TABLE discover_mutual_fund_snapshots ALTER COLUMN score_return DROP DEFAULT;
ALTER TABLE discover_mutual_fund_snapshots ALTER COLUMN score_risk DROP NOT NULL;
ALTER TABLE discover_mutual_fund_snapshots ALTER COLUMN score_risk DROP DEFAULT;
ALTER TABLE discover_mutual_fund_snapshots ALTER COLUMN score_cost DROP NOT NULL;
ALTER TABLE discover_mutual_fund_snapshots ALTER COLUMN score_cost DROP DEFAULT;
ALTER TABLE discover_mutual_fund_snapshots ALTER COLUMN score_consistency DROP NOT NULL;
ALTER TABLE discover_mutual_fund_snapshots ALTER COLUMN score_consistency DROP DEFAULT;
ALTER TABLE discover_mutual_fund_snapshots ALTER COLUMN score_breakdown DROP NOT NULL;
ALTER TABLE discover_mutual_fund_snapshots ALTER COLUMN score_breakdown DROP DEFAULT;
ALTER TABLE discover_mutual_fund_snapshots ALTER COLUMN tags DROP NOT NULL;
ALTER TABLE discover_mutual_fund_snapshots ALTER COLUMN tags DROP DEFAULT;

-- ================================================================
-- v0.9: Technical score & action tag for stocks
-- ================================================================
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS technical_score DOUBLE PRECISION;
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS rsi_14 DOUBLE PRECISION;
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS action_tag TEXT;
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS action_tag_reasoning TEXT;
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS score_confidence TEXT;
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS trend_alignment TEXT;
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS breakout_signal TEXT;

-- ================================================================
-- v1.0: Enhanced technical signals & risk-reward
-- ================================================================
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS entry_exit_signal TEXT;
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS risk_reward_ratio DOUBLE PRECISION;
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS risk_reward_tag TEXT;

-- v1.1: Growth ranges (10Y, 5Y, 3Y, TTM/1Y for sales, profit, price CAGR, ROE)
ALTER TABLE discover_stock_snapshots ADD COLUMN IF NOT EXISTS growth_ranges JSONB;

-- ================================================================
-- v1.2: MF holdings / portfolio data
-- ================================================================
ALTER TABLE discover_mutual_fund_snapshots ADD COLUMN IF NOT EXISTS top_holdings JSONB;
ALTER TABLE discover_mutual_fund_snapshots ADD COLUMN IF NOT EXISTS sector_allocation JSONB;
ALTER TABLE discover_mutual_fund_snapshots ADD COLUMN IF NOT EXISTS asset_allocation JSONB;
ALTER TABLE discover_mutual_fund_snapshots ADD COLUMN IF NOT EXISTS holdings_as_of DATE;

-- ================================================================
-- Notification dedup log (prevents duplicate notifications after deploys/restarts)
-- ================================================================
CREATE TABLE IF NOT EXISTS notification_log (
    id SERIAL PRIMARY KEY,
    notification_type TEXT NOT NULL,      -- e.g. 'market_close_us', 'market_open_india', 'gift_nifty_alert', 'fii_dii', 'pre_market', 'post_market', 'commodity_spike_gold'
    sent_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    title TEXT,
    dedup_key TEXT NOT NULL,              -- e.g. '2026-03-25_market_close_us' for daily dedup, or '2026-03-25_commodity_spike_gold_4' for band-based dedup
    fcm_message_id TEXT,                  -- FCM response message_id (e.g. 'projects/x/messages/1968...')
    fcm_topic TEXT                        -- FCM topic the notification was sent to (e.g. 'market_alerts')
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_notification_log_dedup ON notification_log (dedup_key);

-- ================================================================
-- Device FCM tokens for per-device push notifications
-- ================================================================
CREATE TABLE IF NOT EXISTS device_tokens (
    device_id TEXT NOT NULL,
    fcm_token TEXT NOT NULL,
    platform TEXT DEFAULT 'android',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (device_id)
);
CREATE INDEX IF NOT EXISTS idx_device_tokens_fcm ON device_tokens(fcm_token);

-- ================================================================
-- v1.3: Market scores for story/verdict endpoint
-- ================================================================
CREATE TABLE IF NOT EXISTS market_scores (
    asset TEXT NOT NULL,
    instrument_type TEXT NOT NULL,
    score_trend DOUBLE PRECISION,
    score_volatility DOUBLE PRECISION,
    score_momentum DOUBLE PRECISION,
    verdict TEXT,
    action_tag TEXT,
    action_tag_reasoning TEXT,
    driver_tags JSONB DEFAULT '[]'::JSONB,
    type_extras JSONB,
    computed_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (asset, instrument_type)
);

-- Broker trade charges (scraped from official pricing pages)
CREATE TABLE IF NOT EXISTS broker_charges (
    broker TEXT NOT NULL,
    segment TEXT NOT NULL,
    brokerage_mode TEXT NOT NULL DEFAULT 'flat',
    brokerage_pct DOUBLE PRECISION DEFAULT 0,
    brokerage_cap DOUBLE PRECISION DEFAULT 0,
    brokerage_flat DOUBLE PRECISION DEFAULT 0,
    min_charge DOUBLE PRECISION DEFAULT 0,
    dp_charge DOUBLE PRECISION DEFAULT 0,
    dp_includes_gst BOOLEAN DEFAULT FALSE,
    tagline TEXT,
    amc_yearly DOUBLE PRECISION DEFAULT 0,
    account_opening_fee DOUBLE PRECISION DEFAULT 0,
    call_trade_fee DOUBLE PRECISION DEFAULT 0,
    source_url TEXT,
    scraped_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (broker, segment)
);

-- Statutory/regulatory charges (same for all brokers)
CREATE TABLE IF NOT EXISTS statutory_charges (
    segment TEXT NOT NULL,
    exchange TEXT NOT NULL,
    stt_buy_rate DOUBLE PRECISION DEFAULT 0,
    stt_sell_rate DOUBLE PRECISION DEFAULT 0,
    exchange_txn_rate DOUBLE PRECISION DEFAULT 0,
    stamp_duty_buy_rate DOUBLE PRECISION DEFAULT 0,
    ipft_rate DOUBLE PRECISION DEFAULT 0,
    sebi_fee_rate DOUBLE PRECISION DEFAULT 0.000001,
    gst_rate DOUBLE PRECISION DEFAULT 0.18,
    effective_from DATE,
    source_url TEXT,
    scraped_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (segment, exchange)
);

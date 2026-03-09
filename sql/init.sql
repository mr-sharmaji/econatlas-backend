-- EconAtlas backend schema (PostgreSQL)

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

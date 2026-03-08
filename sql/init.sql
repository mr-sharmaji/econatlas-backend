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
    "timestamp" TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_market_prices_intraday_asset_type_ts ON market_prices_intraday (asset, instrument_type, "timestamp" DESC);

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

from functools import lru_cache

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str

    app_name: str = "EconAtlas"
    debug: bool = False
    log_level: str = "INFO"

    market_interval_minutes: int = 1
    commodity_interval_minutes: int = 1
    # When set, market/commodity jobs run every N seconds (default 30 for live accuracy). Set to 0 or omit to use *_interval_minutes.
    market_interval_seconds: int | None = 30
    commodity_interval_seconds: int | None = 30
    crypto_interval_minutes: int = 1
    crypto_interval_seconds: int | None = 30
    macro_interval_minutes: int = 1
    news_interval_minutes: int = 30
    brief_interval_minutes: int = 5
    discover_stock_interval_minutes: int = 60
    discover_stock_daily_hour_ist: int = 16
    discover_stock_daily_minute_ist: int = 0
    discover_stock_daily_days: str = "mon-fri"
    discover_stock_retry_enabled: bool = True
    discover_stock_retry_hour_ist: int = 16
    discover_stock_retry_minute_ist: int = 20
    discover_mutual_fund_interval_minutes: int = 60
    discover_mf_daily_hour_ist: int = 22
    discover_mf_daily_minute_ist: int = 0
    discover_mf_daily_days: str = "mon-fri"
    ipo_interval_minutes: int = 5
    ipo_stale_threshold_seconds: int = 900
    tax_sync_enabled: bool = True
    tax_sync_interval_minutes: int = 1440
    tax_sync_timeout_seconds: int = 30

    # Cache GET /market/status for this many seconds (reduces calendar lookups; status only changes at session boundaries).
    market_status_cache_seconds: int = 30
    # Automated India session detection (NSE/BSE-equivalent) for special sessions.
    india_session_auto_enabled: bool = True
    india_session_cache_seconds: int = 60
    india_session_timeout_seconds: int = 10
    india_session_primary_url: str = "https://www.nseindia.com/api/marketStatus"

    # Discover data source URLs
    discover_stock_primary_url: str = "https://www.screener.in"
    discover_stock_fallback_url: str = "https://www.nseindia.com"
    discover_stock_nse_timeout_seconds: int = 4
    discover_stock_nse_cooldown_seconds: int = 300
    discover_stock_screener_timeout_seconds: int = 8
    discover_stock_fundamentals_limit: int = 600
    discover_stock_universe_url: str = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"
    discover_stock_bhavcopy_url_template: str = "https://archives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_{yyyymmdd}_F_0000.csv.zip"
    discover_stock_bhavcopy_lookback_days: int = 7
    discover_stock_universe_cache_ttl_seconds: int = 21600
    discover_stock_missing_quote_retry_limit: int = 400
    discover_mf_primary_url: str = "https://www.etmoney.com"
    discover_mf_fallback_url: str = "https://www.amfiindia.com/spages/NAVAll.txt"

    # Tick freshness thresholds by instrument policy.
    # Session assets: indices + bond yields.
    session_live_max_age_seconds: int = 300
    # Rolling assets: currencies + commodities.
    rolling_live_max_age_seconds: int = 900
    # Legacy single-threshold knob. If explicitly provided and typed thresholds
    # are not set, this value is used for both paths.
    live_max_age_seconds: int | None = None
    stale_threshold_seconds_market: int = 600
    stale_threshold_seconds_rolling_24h: int = 900
    # Promote fallback index providers when primary index ticks are older than this.
    # Keeps delayed free feeds responsive during open sessions.
    index_fallback_promote_seconds: int = 120
    # Allow a short post-close window to capture final close snapshots from
    # free providers and persist them to intraday.
    session_post_close_grace_seconds: int = 7200
    # Free feeds for Japan indices are often delayed; allow a wider stale window so
    # "live delayed" data is not incorrectly marked as stale.
    stale_threshold_seconds_tse_session: int = 1800

    # Ops logs endpoint settings
    ops_logs_enabled: bool = True
    ops_log_buffer_size: int = 5000
    ops_logs_token: str | None = None

    # Gift Nifty default sessions in IST.
    gift_nifty_session1_open: str = "06:30"
    gift_nifty_session1_close: str = "15:40"
    gift_nifty_session2_open: str = "16:35"
    gift_nifty_session2_close: str = "02:45"
    # Optional JSON overrides for special sessions (e.g. Diwali/muhurat).
    # Example: {"2026-11-12":[["18:00","19:00"]]}
    gift_nifty_special_sessions_json: str | None = None

    @staticmethod
    def _positive_seconds(value: int | None, default: int) -> int:
        try:
            parsed = int(value) if value is not None else int(default)
        except (TypeError, ValueError):
            parsed = int(default)
        return max(1, parsed)

    def effective_session_live_max_age_seconds(self) -> int:
        if "session_live_max_age_seconds" in self.model_fields_set:
            return self._positive_seconds(self.session_live_max_age_seconds, 300)
        if self.live_max_age_seconds is not None:
            return self._positive_seconds(self.live_max_age_seconds, 300)
        return self._positive_seconds(self.session_live_max_age_seconds, 300)

    def effective_rolling_live_max_age_seconds(self) -> int:
        if "rolling_live_max_age_seconds" in self.model_fields_set:
            return self._positive_seconds(self.rolling_live_max_age_seconds, 900)
        if self.live_max_age_seconds is not None:
            return self._positive_seconds(self.live_max_age_seconds, 900)
        return self._positive_seconds(self.rolling_live_max_age_seconds, 900)

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


@lru_cache
def get_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]

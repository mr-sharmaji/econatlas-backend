from functools import lru_cache

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str

    app_name: str = "EconAtlas"
    debug: bool = False

    market_interval_minutes: int = 1
    commodity_interval_minutes: int = 1
    # When set, market/commodity jobs run every N seconds (default 30 for live accuracy). Set to 0 or omit to use *_interval_minutes.
    market_interval_seconds: int | None = 30
    commodity_interval_seconds: int | None = 30
    macro_interval_minutes: int = 1
    news_interval_minutes: int = 30

    # Cache GET /market/status for this many seconds (reduces calendar lookups; status only changes at session boundaries).
    market_status_cache_seconds: int = 30

    # Tick freshness/staleness thresholds
    stale_threshold_seconds_market: int = 600
    stale_threshold_seconds_rolling_24h: int = 900

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

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


@lru_cache
def get_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]

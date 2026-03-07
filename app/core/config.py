from functools import lru_cache

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    supabase_url: str
    supabase_key: str
    supabase_service_key: str

    app_name: str = "EconAtlas"
    debug: bool = False

    market_interval_minutes: int = 1
    commodity_interval_minutes: int = 1
    macro_interval_minutes: int = 1
    news_interval_minutes: int = 30

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


@lru_cache
def get_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]

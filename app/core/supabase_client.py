from supabase import Client, create_client

from app.core.config import get_settings

_client: Client | None = None


def get_supabase() -> Client:
    """Return a singleton Supabase client authenticated with the service key."""
    global _client
    if _client is None:
        settings = get_settings()
        _client = create_client(settings.supabase_url, settings.supabase_service_key)
    return _client

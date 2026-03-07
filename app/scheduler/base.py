from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Set

import requests

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 15.0
DEFAULT_RETRIES = 3
DEFAULT_BACKOFF = 1.5
USER_AGENT = "Mozilla/5.0 (compatible; EconAtlasScraper/1.0; +https://econatlas.local)"


class BaseScraper:
    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})

    def _get_json(
        self,
        url: str,
        params: Dict[str, Any] | None = None,
        timeout: float = DEFAULT_TIMEOUT,
        retries: int = DEFAULT_RETRIES,
    ) -> Dict[str, Any]:
        last_exc: Exception | None = None
        for attempt in range(retries + 1):
            try:
                response = self.session.get(url, params=params, timeout=timeout)
                response.raise_for_status()
                return response.json()
            except (requests.RequestException, ValueError) as exc:
                last_exc = exc
                if _is_non_retryable(exc):
                    break
                if attempt < retries:
                    time.sleep(DEFAULT_BACKOFF * (2 ** attempt))
        raise last_exc or RuntimeError(f"JSON fetch failed: {url}")

    def _get_text(
        self,
        url: str,
        params: Dict[str, Any] | None = None,
        timeout: float = DEFAULT_TIMEOUT,
        retries: int = DEFAULT_RETRIES,
    ) -> str:
        last_exc: Exception | None = None
        for attempt in range(retries + 1):
            try:
                response = self.session.get(url, params=params, timeout=timeout)
                response.raise_for_status()
                return response.text
            except requests.RequestException as exc:
                last_exc = exc
                if _is_non_retryable(exc):
                    break
                if attempt < retries:
                    time.sleep(DEFAULT_BACKOFF * (2 ** attempt))
        raise last_exc or RuntimeError(f"Text fetch failed: {url}")

    @staticmethod
    def utc_now() -> datetime:
        return datetime.now(tz=timezone.utc)


def _is_non_retryable(exc: Exception) -> bool:
    if not isinstance(exc, requests.RequestException):
        return False
    response = getattr(exc, "response", None)
    if response is None:
        return False
    return 400 <= response.status_code < 500 and response.status_code != 429

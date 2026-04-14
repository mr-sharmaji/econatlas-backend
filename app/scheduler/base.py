from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Set

import requests

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 15.0
DEFAULT_RETRIES = 3
DEFAULT_BACKOFF = 1.5
USER_AGENT = "Mozilla/5.0 (compatible; EconAtlasScraper/1.0; +https://econatlas.local)"


class BaseScraper:
    # ── Per-host rate-limit backoff (shared across all subclass instances) ──
    _RATE_BACKOFF_SEC = 60
    _rate_backoff_until: dict[str, float] = {}
    _rate_backoff_lock = threading.Lock()

    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})

    @classmethod
    def _check_rate_backoff(cls, host: str) -> None:
        """Block the caller until the host's backoff window expires."""
        while True:
            with cls._rate_backoff_lock:
                until = cls._rate_backoff_until.get(host, 0.0)
            remaining = until - time.monotonic()
            if remaining <= 0:
                return
            time.sleep(min(2.0, remaining))

    @classmethod
    def _mark_rate_limited(cls, host: str) -> None:
        """Record that this host rate-limited us."""
        with cls._rate_backoff_lock:
            cls._rate_backoff_until[host] = (
                time.monotonic() + cls._RATE_BACKOFF_SEC
            )
        logger.warning("rate_limit: backing off %s for %d s", host, cls._RATE_BACKOFF_SEC)
        # Bump the Prometheus counter so Grafana can chart per-host
        # throttle frequency. Imported lazily to avoid a circular
        # import at scraper-module load time.
        try:
            from app.core.metrics import SCRAPER_RATE_LIMITED
            SCRAPER_RATE_LIMITED.labels(host=host).inc()
        except Exception:
            pass

    def _parallel_map(
        self,
        host: str,
        workers: int,
        per_call_delay: float,
        items: list,
        fetch_fn,
    ) -> list:
        """Run fetch_fn(item) for every item with bounded concurrency.

        Thread-pool based with per-host rate-limit backoff. Each worker:
          1. Checks the host-level backoff state before its call.
          2. Calls fetch_fn(item). If it returns a tuple starting with
             "__RATE_LIMITED__", marks the host as rate-limited.
          3. Sleeps for per_call_delay to smooth bursts.

        Returns results in same order as items. Exceptions in one worker
        don't abort others — substituted with None.
        """
        import traceback
        if not items:
            return []
        results: list = [None] * len(items)
        errors = 0
        errors_lock = threading.Lock()

        def _worker(idx: int, item) -> None:
            nonlocal errors
            self._check_rate_backoff(host)
            try:
                out = fetch_fn(item)
                if isinstance(out, tuple) and out and out[0] == "__RATE_LIMITED__":
                    self._mark_rate_limited(host)
                    return
                results[idx] = out
            except requests.HTTPError as http_err:
                resp = getattr(http_err, "response", None)
                if resp is not None and resp.status_code in (429, 503):
                    self._mark_rate_limited(host)
                    return
                with errors_lock:
                    errors += 1
                    if errors <= 3:
                        logger.error(
                            "parallel_map(%s) raised http %s:\n%s",
                            host, getattr(resp, "status_code", "?"),
                            traceback.format_exc(),
                        )
            except Exception:
                with errors_lock:
                    errors += 1
                    if errors <= 3:
                        logger.error(
                            "parallel_map(%s) raised:\n%s",
                            host, traceback.format_exc(),
                        )
            finally:
                if per_call_delay > 0:
                    time.sleep(per_call_delay)

        with ThreadPoolExecutor(
            max_workers=workers,
            thread_name_prefix=f"par-{host.split('.')[0]}",
        ) as pool:
            futures = [pool.submit(_worker, i, x) for i, x in enumerate(items)]
            for f in as_completed(futures):
                try:
                    f.result()
                except Exception:
                    pass
        logger.info(
            "parallel_map(%s) done — %d items, %d errors",
            host, len(items), errors,
        )
        return results

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

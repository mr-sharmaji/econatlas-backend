"""Crypto price scraper — fetches live crypto quotes via CoinGecko (primary) and Yahoo Finance (fallback).

Crypto markets are 24/7 so no trading calendar filtering is applied.
"""
from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional

from app.core.database import parse_ts
from app.core.config import get_settings
from app.scheduler.base import BaseScraper
from app.scheduler.job_executors import get_job_executor
from app.scheduler.provider_router import QuoteProvider
from app.services import market_service

logger = logging.getLogger(__name__)

COINGECKO_SIMPLE_PRICE_URL = "https://api.coingecko.com/api/v3/simple/price"
YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"


def _yahoo_chart_url(symbol: str) -> str:
    proxy = os.environ.get("INTRADAY_YAHOO_PROXY_URL", "").strip()
    if proxy:
        return proxy.rstrip("/") + f"/v8/finance/chart/{symbol}"
    return YAHOO_CHART_URL.format(symbol=symbol)

# CoinGecko ID → (asset name, unit)
COINGECKO_IDS: dict[str, tuple[str, str]] = {
    "bitcoin": ("bitcoin", "usd"),
    "ethereum": ("ethereum", "usd"),
    "binancecoin": ("bnb", "usd"),
    "solana": ("solana", "usd"),
    "ripple": ("xrp", "usd"),
    "cardano": ("cardano", "usd"),
    "dogecoin": ("dogecoin", "usd"),
    "polkadot": ("polkadot", "usd"),
    "avalanche-2": ("avalanche", "usd"),
    "chainlink": ("chainlink", "usd"),
}

# Yahoo symbol → (asset name, unit) — used as fallback
YAHOO_CRYPTO_SYMBOLS: dict[str, tuple[str, str]] = {
    "BTC-USD": ("bitcoin", "usd"),
    "ETH-USD": ("ethereum", "usd"),
    "BNB-USD": ("bnb", "usd"),
    "SOL-USD": ("solana", "usd"),
    "XRP-USD": ("xrp", "usd"),
    "ADA-USD": ("cardano", "usd"),
    "DOGE-USD": ("dogecoin", "usd"),
    "DOT-USD": ("polkadot", "usd"),
    "AVAX-USD": ("avalanche", "usd"),
    "LINK-USD": ("chainlink", "usd"),
}


class CryptoScraper(BaseScraper, QuoteProvider):

    def _fetch_coingecko(self) -> List[Dict]:
        items = []
        ids_str = ",".join(COINGECKO_IDS.keys())
        try:
            data = self._get_json(
                COINGECKO_SIMPLE_PRICE_URL,
                params={
                    "ids": ids_str,
                    "vs_currencies": "usd",
                    "include_24hr_change": "true",
                    "include_last_updated_at": "true",
                },
            )
        except Exception:
            logger.warning("CoinGecko fetch failed", exc_info=True)
            return items

        now = datetime.now(timezone.utc)
        for cg_id, (asset, unit) in COINGECKO_IDS.items():
            entry = data.get(cg_id)
            if not entry:
                continue
            price = entry.get("usd")
            if price is None:
                continue
            pct = entry.get("usd_24h_change")
            if pct is not None:
                pct = round(float(pct), 2)
            last_updated = entry.get("last_updated_at")
            try:
                source_ts = datetime.fromtimestamp(int(last_updated), tz=timezone.utc) if last_updated else now
            except (TypeError, ValueError, OSError):
                source_ts = now
            # Derive previous close from 24h change
            prev_close = None
            if pct is not None and pct != -100:
                prev_close = round(float(price) / (1 + pct / 100), 6)
            items.append({
                "asset": asset,
                "price": float(price),
                "unit": unit,
                "source": "coingecko_simple_price",
                "change_percent": pct,
                "previous_close": prev_close,
                "source_timestamp": source_ts.isoformat(),
                "provider": "coingecko",
                "provider_priority": 1,
                "confidence_level": 0.95,
                "is_fallback": False,
                "quality": "primary",
            })
        logger.debug("CoinGecko crypto fetch complete: %d/%d", len(items), len(COINGECKO_IDS))
        return items

    def _fetch_yahoo_fallback(self, primary_rows: List[Dict]) -> List[Dict]:
        now = datetime.now(timezone.utc)
        live_max_age = max(60, int(get_settings().effective_rolling_live_max_age_seconds()))
        by_asset = {str(r.get("asset") or ""): r for r in primary_rows}
        out: list[dict] = []

        for symbol, (asset, unit) in YAHOO_CRYPTO_SYMBOLS.items():
            primary = by_asset.get(asset)
            needs_fallback = primary is None
            if primary is not None:
                p_ts = parse_ts(primary.get("source_timestamp"))
                if p_ts is not None:
                    if p_ts.tzinfo is None:
                        p_ts = p_ts.replace(tzinfo=timezone.utc)
                    needs_fallback = (now - p_ts).total_seconds() > live_max_age
            if not needs_fallback:
                continue

            try:
                payload = self._get_json(_yahoo_chart_url(symbol))
                result = payload.get("chart", {}).get("result", [])
                if not result:
                    continue
                meta = result[0].get("meta", {})
                price = meta.get("regularMarketPrice") or meta.get("previousClose")
                if price is None:
                    continue
                raw_ts = meta.get("regularMarketTime")
                try:
                    source_ts = datetime.fromtimestamp(int(raw_ts), tz=timezone.utc) if raw_ts is not None else now
                except (TypeError, ValueError, OSError):
                    source_ts = now
                prev_raw = meta.get("regularMarketPreviousClose") or meta.get("previousClose") or meta.get("chartPreviousClose")
                prev_usd = float(prev_raw) if prev_raw is not None else None
                pct = None
                if prev_usd is not None and prev_usd > 0:
                    pct = round(((float(price) - prev_usd) / prev_usd) * 100, 2)
                out.append({
                    "asset": asset,
                    "price": float(price),
                    "unit": unit,
                    "source": "yahoo_chart_api",
                    "change_percent": pct,
                    "previous_close": prev_usd,
                    "source_timestamp": source_ts.isoformat(),
                    "provider": "yahoo",
                    "provider_priority": 3,
                    "confidence_level": 0.85,
                    "is_fallback": True,
                    "quality": "fallback",
                })
            except Exception:
                logger.debug("Crypto Yahoo fallback failed for %s", symbol, exc_info=True)
        return out

    @staticmethod
    def _select_best_quotes(rows: list[dict]) -> list[dict]:
        best: dict[str, dict] = {}
        for row in rows:
            asset = str(row.get("asset") or "")
            if not asset:
                continue
            prev = best.get(asset)
            if prev is None:
                best[asset] = row
                continue
            cur_prio = int(row.get("provider_priority") or 99)
            prev_prio = int(prev.get("provider_priority") or 99)
            if cur_prio < prev_prio:
                best[asset] = row
                continue
            if cur_prio > prev_prio:
                continue
            cur_ts = parse_ts(row.get("source_timestamp"))
            prev_ts = parse_ts(prev.get("source_timestamp"))
            if cur_ts is None or prev_ts is None:
                continue
            if cur_ts.tzinfo is None:
                cur_ts = cur_ts.replace(tzinfo=timezone.utc)
            if prev_ts.tzinfo is None:
                prev_ts = prev_ts.replace(tzinfo=timezone.utc)
            if cur_ts > prev_ts:
                best[asset] = row
        return list(best.values())

    def fetch_quotes(self) -> List[Dict]:
        try:
            primary_rows = self._fetch_coingecko()
        except Exception:
            logger.exception("Crypto CoinGecko fetch failed")
            primary_rows = []
        all_rows = list(primary_rows)
        try:
            fallback_rows = self._fetch_yahoo_fallback(primary_rows)
            if fallback_rows:
                logger.info("Crypto fallback quotes added: %d", len(fallback_rows))
                all_rows.extend(fallback_rows)
        except Exception:
            logger.debug("Crypto fallback scan failed", exc_info=True)
        return self._select_best_quotes(all_rows)

    def fetch_all(self) -> List[Dict]:
        return self.fetch_quotes()


_scraper = CryptoScraper()

_PRICE_CHANGE_TOLERANCE = 1e-9


def _num_changed(a, b, tolerance: float = _PRICE_CHANGE_TOLERANCE) -> bool:
    if a is None and b is None:
        return False
    if a is None or b is None:
        return True
    try:
        return abs(float(a) - float(b)) > tolerance
    except (TypeError, ValueError):
        return True


def _daily_row_changed(new_row: dict, latest: dict | None) -> bool:
    if latest is None:
        return True
    return (
        _num_changed(new_row.get("price"), latest.get("price"))
        or _num_changed(new_row.get("previous_close"), latest.get("previous_close"))
        or _num_changed(new_row.get("change_percent"), latest.get("change_percent"))
    )


def _fetch_crypto_rows_sync() -> tuple[List[Dict], bool]:
    """Sync scrape; run in thread executor. Returns (rows, always_open=True).
    Crypto is 24/7 so calendar_open is always True."""
    now = _scraper.utc_now()
    items = _scraper.fetch_all()
    logger.debug("Fetched crypto rows sync: now=%s raw_items=%d", now.isoformat(), len(items))
    # Crypto is 24/7 — use today's date at midnight UTC as the daily key.
    ts = datetime(now.year, now.month, now.day, 0, 0, 0, tzinfo=timezone.utc).isoformat()
    rows = [
        {
            "asset": it["asset"],
            "price": it["price"],
            "timestamp": ts,
            "source": it.get("source"),
            "instrument_type": "crypto",
            "unit": it.get("unit"),
            "change_percent": it.get("change_percent"),
            "previous_close": it.get("previous_close"),
            "source_timestamp": it.get("source_timestamp"),
            "provider": it.get("provider"),
            "provider_priority": it.get("provider_priority"),
            "confidence_level": it.get("confidence_level"),
            "is_fallback": it.get("is_fallback"),
            "quality": it.get("quality"),
        }
        for it in items
    ]
    logger.debug("Prepared crypto rows for persistence: %d", len(rows))
    return (rows, True)


def build_crypto_intraday_rows_for_open(crypto_rows: list[dict]) -> list[dict]:
    """Build intraday rows for 24H chart using provider source timestamps."""
    rows = []
    for r in crypto_rows:
        source_dt = parse_ts(r.get("source_timestamp")) if r.get("source_timestamp") else None
        if source_dt is None:
            source_dt = datetime.now(timezone.utc)
        ts_rounded = market_service._round_to_minute(source_dt).isoformat()
        rows.append({
            "asset": r["asset"],
            "instrument_type": "crypto",
            "price": r["price"],
            "timestamp": ts_rounded,
            "source_timestamp": ts_rounded,
            "provider": r.get("provider") or "unknown",
            "provider_priority": r.get("provider_priority") or 99,
            "confidence_level": r.get("confidence_level"),
            "is_fallback": bool(r.get("is_fallback")) if r.get("is_fallback") is not None else False,
            "quality": r.get("quality"),
        })
    logger.debug("Built crypto intraday rows: input=%d output=%d", len(crypto_rows), len(rows))
    return rows


def build_crypto_intraday_rows_last_session_yahoo(
    crypto_rows: list[dict], trading_date: list | set,
) -> list[dict]:
    """Build full minute-level intraday rows for last sessions using Yahoo 1m chart data."""
    from app.scheduler.market_job import _fetch_yahoo_1m_bars

    target_dates = set(trading_date)
    if not target_dates:
        return []

    rows_out = []
    for symbol, (asset_name, _unit) in YAHOO_CRYPTO_SYMBOLS.items():
        bars, currency = _fetch_yahoo_1m_bars(symbol, range_period="7d")
        for dt, close in bars:
            if dt.date() not in target_dates:
                continue
            ts_rounded = market_service._round_to_minute(dt).isoformat()
            rows_out.append({
                "asset": asset_name,
                "instrument_type": "crypto",
                "price": float(close),
                "timestamp": ts_rounded,
                "source_timestamp": ts_rounded,
                "provider": "yahoo_1m",
                "provider_priority": 1,
                "confidence_level": 0.95,
                "is_fallback": False,
                "quality": "primary",
            })
    return rows_out


async def run_crypto_job() -> None:
    try:
        logger.debug("Crypto job cycle started")
        loop = asyncio.get_event_loop()
        fetched_rows, _ = await loop.run_in_executor(
            get_job_executor("crypto"),
            _fetch_crypto_rows_sync,
        )
        logger.debug("Crypto job fetched_rows=%d", len(fetched_rows))
        if not fetched_rows:
            logger.debug("Crypto job exiting early: no fetched rows")
            return
        # Crypto is 24/7 — always upsert daily rows.
        updated = await market_service.insert_prices_batch_upsert_daily(fetched_rows)
        logger.debug("Crypto job daily rows written=%d", updated)
        intraday_rows = build_crypto_intraday_rows_for_open(fetched_rows)
        if intraday_rows:
            n = await market_service.insert_intraday_batch(intraday_rows)
            logger.info("Crypto job: %d daily upserted, %d intraday", updated, n)
        elif updated == 0:
            logger.info("Crypto job: no daily or intraday rows written")
        else:
            logger.info("Crypto job complete: %d rows upserted (daily)", updated)
        logger.debug("Crypto job cycle completed")
    except Exception:
        logger.exception("Crypto job failed")

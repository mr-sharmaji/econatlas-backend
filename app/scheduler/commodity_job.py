from __future__ import annotations

import asyncio
import logging
import random
import re
import time
from datetime import date, datetime, timezone, timedelta
from typing import Dict, List, Optional

import requests

from app.core.database import parse_ts
from app.core.config import get_settings
from app.scheduler.base import BaseScraper
from app.scheduler.job_executors import get_job_executor
from app.scheduler.provider_router import QuoteProvider
from app.scheduler.trading_calendar import get_trading_date, is_trading_day_commodities, NYSE
from app.services import market_service

logger = logging.getLogger(__name__)

YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
FX_USD_BASE_URL = "https://open.er-api.com/v6/latest/USD"
GOOGLE_FINANCE_QUOTE_URL = "https://www.google.com/finance/quote/{code}"

SYMBOLS = {
    "GC=F": ("gold", "usd_per_troy_ounce"),
    "SI=F": ("silver", "usd_per_troy_ounce"),
    "PL=F": ("platinum", "usd_per_troy_ounce"),
    "PA=F": ("palladium", "usd_per_troy_ounce"),
    "CL=F": ("crude oil", "usd_per_barrel"),
    "NG=F": ("natural gas", "usd_per_mmbtu"),
    "HG=F": ("copper", "usd_per_pound"),
    "ZW=F": ("wheat", "usd_per_bushel"),
    "ZC=F": ("corn", "usd_per_bushel"),
    "ZS=F": ("soybeans", "usd_per_bushel"),
    "ZR=F": ("rice", "usd_per_hundredweight"),
    "ZO=F": ("oats", "usd_per_bushel"),
    "CT=F": ("cotton", "usd_per_pound"),
    "SB=F": ("sugar", "usd_per_pound"),
    "KC=F": ("coffee", "usd_per_pound"),
    "CC=F": ("cocoa", "usd_per_metric_ton"),
    "ALI=F": ("aluminum", "usd_per_pound"),
    "BZ=F": ("brent crude", "usd_per_barrel"),
    "RB=F": ("gasoline", "usd_per_gallon"),
    "HO=F": ("heating oil", "usd_per_gallon"),
}

GOOGLE_COMMODITY_FALLBACKS = {
    "GC=F": {"code": "GCW00:COMEX", "token": '"GCW00","COMEX"'},
    "SI=F": {"code": "SIW00:COMEX", "token": '"SIW00","COMEX"'},
    "PL=F": {"code": "PLW00:NYMEX", "token": '"PLW00","NYMEX"'},
    "PA=F": {"code": "PAW00:NYMEX", "token": '"PAW00","NYMEX"'},
    "CL=F": {"code": "CLW00:NYMEX", "token": '"CLW00","NYMEX"'},
    "NG=F": {"code": "NGW00:NYMEX", "token": '"NGW00","NYMEX"'},
    "HG=F": {"code": "HGW00:COMEX", "token": '"HGW00","COMEX"'},
}
COMMODITY_FALLBACK_MAX_CLOCK_SKEW_SECONDS = 180


def _pick_previous_close(meta: dict) -> tuple[float | None, str | None]:
    """Pick previous close with market-convention priority."""
    candidates = (
        ("regularMarketPreviousClose", meta.get("regularMarketPreviousClose")),
        ("previousClose", meta.get("previousClose")),
        ("chartPreviousClose", meta.get("chartPreviousClose")),
    )
    for key, value in candidates:
        if value is None:
            continue
        try:
            return float(value), key
        except (TypeError, ValueError):
            continue
    return None, None


class CommodityScraper(BaseScraper, QuoteProvider):

    def __init__(self) -> None:
        super().__init__()
        self._cached_fx: Optional[Dict[str, float]] = None

    def _get_usd_rates(self) -> Dict[str, float]:
        if self._cached_fx:
            return self._cached_fx
        data = self._get_json(FX_USD_BASE_URL)
        rates = data.get("rates", {})
        self._cached_fx = {str(k).upper(): float(v) for k, v in rates.items()}
        return self._cached_fx

    def _to_usd(self, value: float, currency: str) -> tuple[float, float]:
        currency = currency.upper()
        if currency == "USD":
            return value, 1.0
        # USX = US cents (used by Yahoo for grains, softs like wheat, corn, cotton)
        if currency == "USX":
            return value / 100.0, 0.01
        # GBX = British pence
        if currency == "GBX":
            return value / 100.0, 0.01
        rates = self._get_usd_rates()
        if currency not in rates or rates[currency] <= 0:
            raise ValueError(f"Missing FX rate for {currency}")
        return value / rates[currency], 1.0 / rates[currency]

    def _fetch_yahoo(self) -> List[Dict]:
        items = []
        logger.debug("Fetching commodity quotes for %d symbols", len(SYMBOLS))
        for symbol, (asset, unit) in SYMBOLS.items():
            try:
                payload = self._get_json(YAHOO_CHART_URL.format(symbol=symbol))
                result = payload.get("chart", {}).get("result", [])
                if not result:
                    continue
                meta = result[0].get("meta", {})
                price = meta.get("regularMarketPrice") or meta.get("previousClose")
                if price is None:
                    continue
                raw_ts = meta.get("regularMarketTime")
                try:
                    source_ts = datetime.fromtimestamp(int(raw_ts), tz=timezone.utc) if raw_ts is not None else datetime.now(timezone.utc)
                except (TypeError, ValueError, OSError):
                    source_ts = datetime.now(timezone.utc)
                currency = str(meta.get("currency", "USD"))
                usd_val, _fx = self._to_usd(float(price), currency)
                prev_raw, prev_key = _pick_previous_close(meta)
                if prev_key == "chartPreviousClose":
                    logger.debug("Using chartPreviousClose fallback for %s", symbol)
                prev_usd = None
                pct = None
                if prev_raw is not None:
                    try:
                        prev_usd, _ = self._to_usd(prev_raw, currency)
                        if prev_usd > 0:
                            pct = round(((usd_val - prev_usd) / prev_usd) * 100, 2)
                    except (ValueError, ZeroDivisionError):
                        pass
                items.append({
                    "asset": asset,
                    "price": usd_val,
                    "unit": unit,
                    "source": "yahoo_chart_api",
                    "change_percent": pct,
                    "previous_close": prev_usd,
                    "source_timestamp": source_ts.isoformat(),
                    "provider": "yahoo",
                    "provider_priority": 1,
                    "confidence_level": 0.95,
                    "is_fallback": False,
                    "quality": "primary",
                })
            except Exception:
                logger.warning("Commodity fetch failed for %s", symbol, exc_info=True)
        logger.debug("Commodity fetch complete: %d/%d symbols", len(items), len(SYMBOLS))
        return items

    @staticmethod
    def _parse_google_quote(page_html: str, token: str) -> tuple[float, float | None, float | None, datetime] | None:
        pattern = re.compile(
            rf'\["/[^"]+",\[{re.escape(token)}\][\s\S]{{0,520}}?'
            r"\[\s*(?P<price>-?\d+(?:\.\d+)?)\s*,\s*(?P<chg>-?\d+(?:\.\d+)?)\s*,\s*(?P<pct>-?\d+(?:\.\d+)?)\s*,\s*\d+\s*,\s*\d+\s*,\s*\d+\s*\]"
            r"[\s\S]{0,260}?\[(?P<ts>\d{10})\]",
            re.IGNORECASE,
        )
        m = pattern.search(page_html)
        if not m:
            return None
        try:
            price = float(m.group("price"))
            chg = float(m.group("chg"))
            pct = float(m.group("pct"))
            ts = datetime.fromtimestamp(int(m.group("ts")), tz=timezone.utc)
            block = page_html[m.start():m.end() + 180]
            m_prev = re.search(r"\]\s*,\s*null\s*,\s*(-?\d+(?:\.\d+)?)\s*,", block)
            prev = float(m_prev.group(1)) if m_prev else round(price - chg, 6)
            return (price, prev, pct, ts)
        except (TypeError, ValueError, OSError):
            return None

    def _fetch_google_fallbacks(self, yahoo_rows: List[Dict]) -> List[Dict]:
        now = datetime.now(timezone.utc)
        live_max_age_seconds = max(60, int(get_settings().effective_rolling_live_max_age_seconds()))
        by_asset = {str(r.get("asset") or ""): r for r in yahoo_rows}
        out: list[dict] = []

        for symbol, (asset, unit) in SYMBOLS.items():
            primary = by_asset.get(asset)
            needs_fallback = primary is None
            if primary is not None:
                p_ts = parse_ts(primary.get("source_timestamp"))
                if p_ts is not None:
                    if p_ts.tzinfo is None:
                        p_ts = p_ts.replace(tzinfo=timezone.utc)
                    needs_fallback = (now - p_ts).total_seconds() > live_max_age_seconds
            if not needs_fallback:
                continue

            cfg = GOOGLE_COMMODITY_FALLBACKS.get(symbol)
            if cfg is None:
                continue
            try:
                html_text = self._get_text(GOOGLE_FINANCE_QUOTE_URL.format(code=cfg["code"]))
                parsed = self._parse_google_quote(html_text, cfg["token"])
                if not parsed:
                    continue
                price, prev, pct, ts = parsed
                if ts > (now + timedelta(seconds=COMMODITY_FALLBACK_MAX_CLOCK_SKEW_SECONDS)):
                    logger.debug("Commodity fallback skipped (future timestamp): asset=%s ts=%s", asset, ts.isoformat())
                    continue
                if (now - ts).total_seconds() > 24 * 3600:
                    logger.debug("Commodity fallback skipped (too old): asset=%s ts=%s", asset, ts.isoformat())
                    continue
                out.append(
                    {
                        "asset": asset,
                        "price": float(price),
                        "unit": unit,
                        "source": "google_finance_html",
                        "change_percent": round(float(pct), 2) if pct is not None else None,
                        "previous_close": float(prev) if prev is not None else None,
                        "source_timestamp": ts.isoformat(),
                        "provider": "google_finance",
                        "provider_priority": 4,
                        "confidence_level": 0.8,
                        "is_fallback": True,
                        "quality": "fallback",
                    }
                )
            except Exception:
                logger.debug("Commodity Google fallback fetch failed for %s", asset, exc_info=True)
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

    def _promote_delayed_primary_with_fallback(self, selected: list[dict], all_rows: list[dict]) -> list[dict]:
        now = datetime.now(timezone.utc)
        live_max_age_seconds = max(60, int(get_settings().effective_rolling_live_max_age_seconds()))
        fallback_by_asset: dict[str, dict] = {}
        for row in all_rows:
            if str(row.get("provider") or "") == "yahoo":
                continue
            asset = str(row.get("asset") or "")
            if not asset:
                continue
            prev = fallback_by_asset.get(asset)
            if prev is None:
                fallback_by_asset[asset] = row
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
                fallback_by_asset[asset] = row

        out: list[dict] = []
        for row in selected:
            if str(row.get("provider") or "") != "yahoo":
                out.append(row)
                continue
            asset = str(row.get("asset") or "")
            tick_ts = parse_ts(row.get("source_timestamp"))
            if tick_ts is None:
                out.append(row)
                continue
            if tick_ts.tzinfo is None:
                tick_ts = tick_ts.replace(tzinfo=timezone.utc)
            age_seconds = (now - tick_ts).total_seconds()
            if age_seconds <= live_max_age_seconds:
                out.append(row)
                continue

            fb = fallback_by_asset.get(asset)
            if fb is None:
                out.append(row)
                continue
            fb_ts = parse_ts(fb.get("source_timestamp"))
            if fb_ts is None:
                out.append(row)
                continue
            if fb_ts.tzinfo is None:
                fb_ts = fb_ts.replace(tzinfo=timezone.utc)
            if fb_ts <= tick_ts:
                out.append(row)
                continue
            logger.info(
                "Promoted delayed commodity to fallback: asset=%s age_seconds=%.1f primary_ts=%s fallback_provider=%s fallback_ts=%s",
                asset,
                age_seconds,
                tick_ts.isoformat(),
                fb.get("provider"),
                fb_ts.isoformat(),
            )
            out.append(fb)
        return out

    def fetch_quotes(self) -> List[Dict]:
        try:
            yahoo_rows = self._fetch_yahoo()
        except Exception:
            logger.exception("Commodity Yahoo fetch failed")
            yahoo_rows = []
        all_rows = list(yahoo_rows)
        try:
            fallback_rows = self._fetch_google_fallbacks(yahoo_rows)
            if fallback_rows:
                logger.info("Commodity fallback quotes added: %d", len(fallback_rows))
                all_rows.extend(fallback_rows)
        except Exception:
            logger.debug("Commodity fallback scan failed", exc_info=True)
        selected = self._select_best_quotes(all_rows)
        selected = self._promote_delayed_primary_with_fallback(selected, all_rows)
        return selected

    def fetch_all(self) -> List[Dict]:
        return self.fetch_quotes()


_scraper = CommodityScraper()

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


def _fetch_commodity_rows_sync() -> tuple[List[Dict], bool]:
    """Sync scrape; run in thread executor. Returns (rows, calendar_says_trading_day).
    Timestamp uses NYSE trading date so Monday's close is not stored as Tuesday."""
    now = _scraper.utc_now()
    calendar_open = is_trading_day_commodities(now)
    items = _scraper.fetch_all()
    logger.debug(
        "Fetched commodity rows sync: now=%s calendar_open=%s raw_items=%d",
        now.isoformat(),
        calendar_open,
        len(items),
    )
    trading_date = get_trading_date(now, NYSE)
    ts = datetime(trading_date.year, trading_date.month, trading_date.day, 0, 0, 0, tzinfo=timezone.utc).isoformat()
    rows = [
        {
            "asset": it["asset"],
            "price": it["price"],
            "timestamp": ts,
            "source": it.get("source"),
            "instrument_type": "commodity",
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
    logger.debug("Prepared commodity rows for persistence: %d", len(rows))
    return (rows, calendar_open)


_PRICE_CHANGE_TOLERANCE = 1e-9


def build_commodity_intraday_rows_for_open(commodity_rows: list[dict]) -> list[dict]:
    """Build intraday rows for 24H chart using provider source timestamps."""
    rows = []
    for r in commodity_rows:
        source_dt = parse_ts(r.get("source_timestamp")) if r.get("source_timestamp") else None
        if source_dt is None:
            source_dt = datetime.now(timezone.utc)
        ts_rounded = market_service._round_to_minute(source_dt).isoformat()
        rows.append({
            "asset": r["asset"],
            "instrument_type": "commodity",
            "price": r["price"],
            "timestamp": ts_rounded,
            "source_timestamp": ts_rounded,
            "provider": r.get("provider") or "unknown",
            "provider_priority": r.get("provider_priority") or 99,
            "confidence_level": r.get("confidence_level"),
            "is_fallback": bool(r.get("is_fallback")) if r.get("is_fallback") is not None else False,
            "quality": r.get("quality"),
        })
    logger.debug("Built commodity intraday rows: input=%d output=%d", len(commodity_rows), len(rows))
    return rows


def build_commodity_intraday_rows_last_session_yahoo(
    commodity_rows: list[dict], trading_date: date | list[date] | set[date]
) -> list[dict]:
    """Build full minute-level intraday rows for last session using Yahoo 1m chart data.
    Fetches 1m bars per symbol, converts close to USD, filters to target trading date(s)."""
    from app.scheduler.market_job import _fetch_yahoo_1m_bars

    if isinstance(trading_date, date):
        target_dates = {trading_date}
    else:
        target_dates = set(trading_date)
    if not target_dates:
        return []

    rows_out = []
    for symbol, (asset_name, _unit) in SYMBOLS.items():
        bars, currency = _fetch_yahoo_1m_bars(symbol, range_period="7d")
        for dt, close in bars:
            if dt.date() not in target_dates:
                continue
            try:
                usd_price, _ = _scraper._to_usd(float(close), currency)
            except (ValueError, TypeError):
                continue
            ts_rounded = market_service._round_to_minute(dt).isoformat()
            rows_out.append({
                "asset": asset_name,
                "instrument_type": "commodity",
                "price": usd_price,
                "timestamp": ts_rounded,
                "source_timestamp": ts_rounded,
                "provider": "yahoo_1m",
                "provider_priority": 1,
                "confidence_level": 0.95,
                "is_fallback": False,
                "quality": "primary",
            })
    return rows_out


TE_FERTILIZERS = {
    "/commodity/urea": ("urea", "usd_per_metric_ton"),
    "/commodity/dap-fertilizer": ("dap fertilizer", "usd_per_metric_ton"),
    "/commodity/potash": ("potash", "usd_per_metric_ton"),
    "/commodity/tsp-fertilizer": ("tsp fertilizer", "usd_per_metric_ton"),
    "/commodity/ammonia": ("ammonia", "usd_per_metric_ton"),
}

_TE_BASE_URL = "https://tradingeconomics.com"
_TE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}


def _fetch_fertilizer_prices() -> List[Dict]:
    """Scrape fertilizer prices from Index Mundi commodity index page via Playwright.
    Single page load returns all fertilizer prices in a rendered table."""
    # Map Index Mundi names → our asset names and units
    IM_MAP = {
        "Urea": ("urea", "usd_per_metric_ton"),
        "DAP fertilizer": ("dap fertilizer", "usd_per_metric_ton"),
        "Potassium Chloride": ("potash", "usd_per_metric_ton"),
        "Triple Superphosphate": ("tsp fertilizer", "usd_per_metric_ton"),
    }
    items: List[Dict] = []
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        logger.warning("Playwright not installed; skipping fertilizer scraping")
        return items
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto("https://www.indexmundi.com/commodities/", timeout=40000)
            page.wait_for_timeout(8000)
            table_data = page.evaluate("""() => {
                const rows = [];
                document.querySelectorAll('table tr').forEach(r => {
                    const cells = Array.from(r.querySelectorAll('td'));
                    if (cells.length >= 2) rows.push([cells[0].innerText.trim(), cells[1].innerText.trim()]);
                });
                return rows;
            }""")
            browser.close()
        now_iso = datetime.now(timezone.utc).isoformat()
        for name, price_str in table_data:
            if name not in IM_MAP:
                continue
            try:
                price = float(price_str.replace(",", ""))
            except (ValueError, TypeError):
                continue
            if price <= 0:
                continue
            asset, unit = IM_MAP[name]
            items.append({
                "asset": asset,
                "price": price,
                "unit": unit,
                "instrument_type": "commodity",
                "source": "indexmundi_scrape",
                "source_timestamp": now_iso,
            })
            logger.info("IndexMundi fertilizer: %s = %.2f", asset, price)
    except Exception:
        logger.warning("IndexMundi fertilizer scraping failed", exc_info=True)
    return items


async def run_commodity_job() -> None:
    try:
        logger.debug("Commodity job cycle started")
        loop = asyncio.get_event_loop()
        fetched_rows, calendar_says_open = await loop.run_in_executor(
            get_job_executor("commodity"),
            _fetch_commodity_rows_sync,
        )
        logger.debug("Commodity job fetched_rows=%d calendar_says_open=%s", len(fetched_rows), calendar_says_open)
        if not fetched_rows:
            logger.debug("Commodity job exiting early: no fetched rows")
            return
        rows = fetched_rows
        if not calendar_says_open:
            before = len(rows)
            pairs = [(r["asset"], "commodity") for r in rows]
            latest = await market_service.get_latest_daily_snapshot_per_asset_type(pairs)
            rows = [
                r for r in rows
                if _daily_row_changed(r, latest.get((r["asset"], "commodity")))
            ]
            logger.debug("Commodity job filtered unchanged rows while calendar closed: %d -> %d", before, len(rows))
        updated = 0
        if rows:
            updated = await market_service.insert_prices_batch_upsert_daily(rows)
        logger.debug("Commodity job daily rows written=%d", updated)
        intraday_rows = build_commodity_intraday_rows_for_open(fetched_rows)
        if intraday_rows:
            n = await market_service.insert_intraday_batch(intraday_rows)
            logger.info("Commodity job: %d daily upserted, %d intraday", updated, n)
            logger.debug("Commodity job intraday rows attempted=%d inserted_or_updated=%d", len(intraday_rows), n)
        elif updated == 0:
            logger.info("Commodity job: no daily or intraday rows written")
        else:
            logger.info("Commodity job complete: %d rows upserted (daily)", updated)
        logger.debug("Commodity job cycle completed")
    except Exception:
        logger.exception("Commodity job failed")


async def run_fertilizer_job() -> None:
    """Separate job for TE fertilizer scraping — runs every few hours, not every 30s."""
    try:
        logger.info("Fertilizer job started")
        loop = asyncio.get_event_loop()
        te_rows_raw = await loop.run_in_executor(
            get_job_executor("fertilizer"),
            _fetch_fertilizer_prices,
        )
        if not te_rows_raw:
            logger.info("Fertilizer job: no rows fetched")
            return
        now = datetime.now(timezone.utc)
        trading_date = get_trading_date(now, NYSE)
        ts = datetime(trading_date.year, trading_date.month, trading_date.day, 0, 0, 0, tzinfo=timezone.utc).isoformat()
        te_rows = [
            {
                "asset": it["asset"],
                "price": it["price"],
                "timestamp": ts,
                "source": it.get("source"),
                "instrument_type": "commodity",
                "unit": it.get("unit"),
                "change_percent": None,
                "previous_close": None,
            }
            for it in te_rows_raw
        ]
        updated = await market_service.insert_prices_batch_upsert_daily(te_rows)
        logger.info("Fertilizer job: %d rows upserted", updated)
    except Exception:
        logger.exception("Fertilizer job failed")

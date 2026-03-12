from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from html import unescape

import requests

from app.core.config import get_settings
from app.scheduler.base import BaseScraper
from app.scheduler.brief_job import INDIA_STOCKS
from app.scheduler.job_executors import get_job_executor
from app.services import discover_service

logger = logging.getLogger(__name__)

NSE_HOME_URL = "https://www.nseindia.com"
NSE_QUOTE_URL = "https://www.nseindia.com/api/quote-equity"
NSE_INDEX_URL = "https://www.nseindia.com/api/equity-stockIndices"
YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"


@dataclass(frozen=True)
class DiscoverStockDef:
    nse_symbol: str
    yahoo_symbol: str
    display_name: str
    sector: str
    fundamentals_enabled: bool = True


def _build_universe() -> tuple[DiscoverStockDef, ...]:
    rows: list[DiscoverStockDef] = []
    seen: set[str] = set()
    for item in INDIA_STOCKS:
        y_symbol = item.symbol
        n_symbol = y_symbol.replace(".NS", "").strip().upper()
        if not n_symbol or n_symbol in seen:
            continue
        seen.add(n_symbol)
        rows.append(
            DiscoverStockDef(
                nse_symbol=n_symbol,
                yahoo_symbol=y_symbol,
                display_name=item.display_name,
                sector=item.sector,
                fundamentals_enabled=True,
            )
        )
    return tuple(rows)


UNIVERSE = _build_universe()

# Static NIFTY 500 seed symbols to keep stock coverage expanded even if
# NSE index-constituent calls are temporarily blocked.
NIFTY500_SEED_SYMBOLS: tuple[str, ...] = (
    "ATGL",
    "M&M",
    "JINDALSAW",
    "NTPCGREEN",
    "DOMS",
    "ETERNAL",
    "DIXON",
    "JSWENERGY",
    "TVSMOTOR",
    "BSE",
    "GUJGASLTD",
    "NATIONALUM",
    "ASHOKLEY",
    "VEDL",
    "HAPPSTMNDS",
    "INDIGO",
    "GODREJCP",
    "BEL",
    "MCX",
    "MAXHEALTH",
    "MUTHOOTFIN",
    "AMBER",
    "KAYNES",
    "MAZDOCK",
    "VBL",
    "COFORGE",
    "SUZLON",
    "CUMMINSIND",
    "HFCL",
    "NETWEB",
    "BHEL",
    "IDEA",
    "HINDCOPPER",
    "KPRMILL",
    "SWIGGY",
    "HAL",
    "ABB",
    "POLYCAB",
    "RECLTD",
    "CHOLAFIN",
    "WAAREEENER",
    "BAJAJ-AUTO",
    "PFC",
    "POWERINDIA",
    "TEJASNET",
    "TRENT",
    "IOC",
    "NLCINDIA",
    "CHENNPETRO",
    "SRF",
    "HINDPETRO",
    "MARICO",
    "TMPV",
    "CANBK",
    "FACT",
    "INDHOTEL",
    "MOTHERSON",
    "OIL",
    "NAUKRI",
    "AUROPHARMA",
    "PERSISTENT",
    "APLAPOLLO",
    "CESC",
    "PETRONET",
    "ASTRAL",
    "KEC",
    "CGPOWER",
    "UNIONBANK",
    "HINDZINC",
    "AMBUJACEM",
    "MRPL",
    "GAIL",
    "INDUSTOWER",
    "JINDALSTEL",
    "BANKBARODA",
    "DATAPATTNS",
    "ANGELONE",
    "KEI",
    "GLENMARK",
    "IDFCFIRSTB",
    "BHARATFORG",
    "GVT&D",
    "FEDERALBNK",
    "SOLARINDS",
    "HDFCAMC",
    "PAYTM",
    "LUPIN",
    "FORTIS",
    "MGL",
    "CEATLTD",
    "CDSL",
    "RVNL",
    "FORCEMOT",
    "JUBLFOOD",
    "SCI",
    "CONCOR",
    "RPOWER",
    "POLICYBZR",
    "PGEL",
    "DLF",
    "GSPL",
    "BDL",
    "INOXWIND",
    "IRCTC",
    "YESBANK",
    "NMDC",
    "COLPAL",
    "AUBANK",
    "GMRAIRPORT",
    "SUPREMEIND",
    "GESHIP",
    "PNB",
    "RADICO",
    "REDINGTON",
    "J&KBANK",
    "VMM",
    "SAIL",
    "TORNTPHARM",
    "GRSE",
    "IRFC",
    "APARINDS",
    "AARTIIND",
    "MFSL",
    "DMART",
    "TORNTPOWER",
    "LAURUSLABS",
    "PREMIERENE",
    "PPLPHARMA",
    "LICHSGFIN",
    "MANAPPURAM",
    "NHPC",
    "KALYANKJIL",
)


class DiscoverStockScraper(BaseScraper):
    def __init__(self) -> None:
        super().__init__()
        self.settings = get_settings()
        self._nse_ready = False
        self._nse_disabled_until: datetime | None = None
        self._nse_timeout = max(1, int(getattr(self.settings, "discover_stock_nse_timeout_seconds", 4)))
        self._nse_cooldown = max(30, int(getattr(self.settings, "discover_stock_nse_cooldown_seconds", 300)))
        self._screener_timeout = max(2, int(getattr(self.settings, "discover_stock_screener_timeout_seconds", 8)))
        default_target = max(len(UNIVERSE), 150)
        configured_target = int(getattr(self.settings, "discover_stock_universe_target_size", default_target))
        self._universe_target_size = max(len(UNIVERSE), min(configured_target, 500))

    def _nse_on_cooldown(self) -> bool:
        if self._nse_disabled_until is None:
            return False
        if datetime.now(timezone.utc) >= self._nse_disabled_until:
            self._nse_disabled_until = None
            return False
        return True

    def _activate_nse_cooldown(self, *, reason: str) -> None:
        if self._nse_on_cooldown():
            return
        self._nse_ready = False
        self._nse_disabled_until = datetime.now(timezone.utc) + timedelta(seconds=self._nse_cooldown)
        logger.warning("NSE quote path disabled for %ds (%s); using Yahoo fallback", self._nse_cooldown, reason)

    def _ensure_nse_session(self) -> None:
        if self._nse_ready:
            return
        if self._nse_on_cooldown():
            raise RuntimeError("NSE session cooldown active")
        headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": NSE_HOME_URL,
        }
        try:
            self.session.get(NSE_HOME_URL, headers=headers, timeout=self._nse_timeout)
            self._nse_ready = True
        except Exception:
            self._activate_nse_cooldown(reason="session bootstrap failed")
            raise

    def _fetch_nse_quote(self, symbol: str) -> dict | None:
        if self._nse_on_cooldown():
            return None
        try:
            self._ensure_nse_session()
            headers = {
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": f"{NSE_HOME_URL}/get-quotes/equity?symbol={symbol}",
                "X-Requested-With": "XMLHttpRequest",
            }
            resp = self.session.get(
                NSE_QUOTE_URL,
                params={"symbol": symbol},
                headers=headers,
                timeout=self._nse_timeout,
            )
            resp.raise_for_status()
            payload = resp.json()
            info = payload.get("priceInfo") or {}
            sec = payload.get("securityWiseDP") or {}
            meta = payload.get("metadata") or {}

            last = info.get("lastPrice")
            if last is None:
                return None
            last_price = float(str(last).replace(",", ""))

            point_change = info.get("change")
            pct_change = info.get("pChange")
            volume = sec.get("quantityTraded") or info.get("totalTradedVolume")
            traded_value = sec.get("valueTraded") or info.get("totalTradedValue")

            ts_text = meta.get("lastUpdateTime")
            source_ts = datetime.now(timezone.utc)
            if ts_text:
                # NSE format example: 12-Mar-2026 15:30:00
                try:
                    parsed = datetime.strptime(str(ts_text), "%d-%b-%Y %H:%M:%S")
                    source_ts = parsed.replace(tzinfo=timezone.utc)
                except ValueError:
                    pass

            return {
                "last_price": last_price,
                "point_change": float(str(point_change).replace(",", "")) if point_change is not None else None,
                "percent_change": float(str(pct_change).replace(",", "")) if pct_change is not None else None,
                "volume": int(float(str(volume).replace(",", ""))) if volume is not None else None,
                "traded_value": float(str(traded_value).replace(",", "")) if traded_value is not None else None,
                "source_timestamp": source_ts,
                "source": "nse_quote_api",
            }
        except Exception:
            self._activate_nse_cooldown(reason=f"quote fetch failed for {symbol}")
            logger.debug("NSE quote fetch failed for %s", symbol, exc_info=True)
            return None

    def _fetch_nifty500_constituents(self) -> list[DiscoverStockDef]:
        if self._universe_target_size <= len(UNIVERSE) or self._nse_on_cooldown():
            return []
        try:
            self._ensure_nse_session()
            headers = {
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": f"{NSE_HOME_URL}/market-data/live-equity-market?symbol=NIFTY%20500",
                "X-Requested-With": "XMLHttpRequest",
            }
            resp = self.session.get(
                NSE_INDEX_URL,
                params={"index": "NIFTY 500"},
                headers=headers,
                timeout=self._nse_timeout,
            )
            resp.raise_for_status()
            payload = resp.json()
            rows = payload.get("data") or []
            out: list[DiscoverStockDef] = []
            for row in rows:
                symbol = str(row.get("symbol") or "").strip().upper()
                if not symbol or symbol == "NIFTY 500":
                    continue
                meta = row.get("meta") or {}
                display_name = str(meta.get("companyName") or symbol).strip() or symbol
                sector = str(meta.get("industry") or "Diversified").strip() or "Diversified"
                out.append(
                    DiscoverStockDef(
                        nse_symbol=symbol,
                        yahoo_symbol=f"{symbol}.NS",
                        display_name=display_name,
                        sector=sector,
                        fundamentals_enabled=False,
                    )
                )
            return out
        except Exception:
            logger.debug("Failed to fetch NSE NIFTY 500 constituents", exc_info=True)
            return []

    def _build_effective_universe(self) -> tuple[DiscoverStockDef, ...]:
        rows = list(UNIVERSE)
        seen = {item.nse_symbol for item in rows}
        for symbol in NIFTY500_SEED_SYMBOLS:
            if len(rows) >= self._universe_target_size:
                break
            normalized = str(symbol or "").strip().upper()
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            rows.append(
                DiscoverStockDef(
                    nse_symbol=normalized,
                    yahoo_symbol=f"{normalized}.NS",
                    display_name=normalized,
                    sector="Diversified",
                    fundamentals_enabled=False,
                )
            )
        extras = self._fetch_nifty500_constituents()
        for item in extras:
            if len(rows) >= self._universe_target_size:
                break
            if item.nse_symbol in seen:
                continue
            seen.add(item.nse_symbol)
            rows.append(item)
        return tuple(rows)

    def _fetch_yahoo_quote(self, yahoo_symbol: str) -> dict | None:
        try:
            payload = self._get_json(YAHOO_CHART_URL.format(symbol=yahoo_symbol))
            result = payload.get("chart", {}).get("result", [])
            if not result:
                return None
            meta = result[0].get("meta", {})
            price_raw = meta.get("regularMarketPrice")
            prev_raw = meta.get("regularMarketPreviousClose") or meta.get("previousClose")
            if price_raw is None:
                return None

            price = float(price_raw)
            prev = float(prev_raw) if prev_raw is not None else None
            point = round(price - prev, 2) if prev is not None else None
            pct = round(((price - prev) / prev) * 100, 2) if prev not in (None, 0) else None

            vol_raw = meta.get("regularMarketVolume")
            volume = int(vol_raw) if vol_raw is not None else None
            traded_value = round(price * volume, 2) if volume is not None else None

            ts_raw = meta.get("regularMarketTime")
            if ts_raw is not None:
                try:
                    source_ts = datetime.fromtimestamp(int(ts_raw), tz=timezone.utc)
                except Exception:
                    source_ts = datetime.now(timezone.utc)
            else:
                source_ts = datetime.now(timezone.utc)

            return {
                "last_price": price,
                "point_change": point,
                "percent_change": pct,
                "volume": volume,
                "traded_value": traded_value,
                "source_timestamp": source_ts,
                "source": "yahoo_finance_api",
            }
        except Exception:
            logger.debug("Yahoo quote fetch failed for %s", yahoo_symbol, exc_info=True)
            return None

    def _extract_labeled_number(self, text: str, labels: list[str]) -> float | None:
        for label in labels:
            patt = rf"{re.escape(label)}\s*[:\-]?\s*([\-]?[0-9][0-9,]*(?:\.[0-9]+)?)"
            match = re.search(patt, text, flags=re.IGNORECASE)
            if not match:
                continue
            raw = match.group(1).replace(",", "").strip()
            try:
                return float(raw)
            except ValueError:
                continue
        return None

    def _fetch_screener_fundamentals(self, nse_symbol: str) -> tuple[dict, str]:
        base = self.settings.discover_stock_primary_url.rstrip("/")
        candidates = [
            f"{base}/company/{nse_symbol}/consolidated/",
            f"{base}/company/{nse_symbol}/",
        ]
        for url in candidates:
            try:
                html = self._get_text(url, timeout=self._screener_timeout)
                text = unescape(re.sub(r"<[^>]+>", " ", html))
                text = re.sub(r"\s+", " ", text)

                fundamentals = {
                    "pe_ratio": self._extract_labeled_number(text, ["Stock P/E", "P/E"]),
                    "roe": self._extract_labeled_number(text, ["ROE", "Return on equity"]),
                    "roce": self._extract_labeled_number(text, ["ROCE", "Return on capital employed"]),
                    "debt_to_equity": self._extract_labeled_number(text, ["Debt to equity", "Debt to Equity"]),
                    "price_to_book": self._extract_labeled_number(text, ["Price to book value", "P/B"]),
                    "eps": self._extract_labeled_number(text, ["EPS", "Earnings Per Share"]),
                }
                return fundamentals, "screener_in"
            except Exception:
                logger.debug("Screener fundamentals fetch failed for %s url=%s", nse_symbol, url, exc_info=True)
                continue
        return {
            "pe_ratio": None,
            "roe": None,
            "roce": None,
            "debt_to_equity": None,
            "price_to_book": None,
            "eps": None,
        }, "unavailable"

    @staticmethod
    def _clamp(value: float, lo: float = 0.0, hi: float = 100.0) -> float:
        return max(lo, min(hi, value))

    def _score_fundamentals(self, row: dict) -> tuple[float, int]:
        parts: list[float] = []
        metrics_used = 0

        pe = row.get("pe_ratio")
        if pe is not None and pe > 0:
            parts.append(self._clamp(100 - ((pe / 80) * 100)))
            metrics_used += 1

        roe = row.get("roe")
        if roe is not None:
            parts.append(self._clamp(roe * 5))
            metrics_used += 1

        roce = row.get("roce")
        if roce is not None:
            parts.append(self._clamp(roce * 4.5))
            metrics_used += 1

        dte = row.get("debt_to_equity")
        if dte is not None:
            parts.append(self._clamp(100 - (dte * 50)))
            metrics_used += 1

        pb = row.get("price_to_book")
        if pb is not None and pb > 0:
            parts.append(self._clamp(100 - ((pb / 15) * 100)))
            metrics_used += 1

        if not parts:
            return 50.0, metrics_used
        return round(sum(parts) / len(parts), 2), metrics_used

    def _compute_scores(self, rows: list[dict]) -> list[dict]:
        max_tv = max((float(r.get("traded_value") or 0.0) for r in rows), default=0.0)
        max_vol = max((float(r.get("volume") or 0.0) for r in rows), default=0.0)

        out: list[dict] = []
        for row in rows:
            pct = float(row.get("percent_change") or 0.0)
            tv = float(row.get("traded_value") or 0.0)
            vol = float(row.get("volume") or 0.0)

            momentum = self._clamp(50 + (pct * 10))
            if abs(pct) >= 2:
                momentum = self._clamp(momentum + 10)

            tv_norm = (tv / max_tv) if max_tv > 0 else 0.0
            vol_norm = (vol / max_vol) if max_vol > 0 else 0.0
            liquidity = self._clamp(((tv_norm + vol_norm) / 2.0) * 100)

            fundamentals, metrics_used = self._score_fundamentals(row)
            combined_signal = round((momentum + liquidity) / 2.0, 2)
            if metrics_used == 0:
                total = combined_signal
            else:
                total = (combined_signal * 0.5) + (fundamentals * 0.5)

            source_status = str(row.get("source_status") or "limited")
            if metrics_used == 0 and source_status == "primary":
                source_status = "fallback"

            tags: list[str] = []
            if pct >= 2:
                tags.append("momentum_up")
            elif pct <= -2:
                tags.append("momentum_down")
            if liquidity >= 70:
                tags.append("high_liquidity")
            if fundamentals >= 70:
                tags.append("strong_fundamentals")
            if source_status != "primary":
                tags.append("limited_data")
            if not tags:
                tags.append("balanced")

            enriched = {
                **row,
                "score": round(self._clamp(total), 2),
                "score_momentum": round(momentum, 2),
                "score_liquidity": round(liquidity, 2),
                "score_fundamentals": round(fundamentals, 2),
                "score_breakdown": {
                    "momentum": round(momentum, 2),
                    "liquidity": round(liquidity, 2),
                    "fundamentals": round(fundamentals, 2),
                    "combined_signal": round(combined_signal, 2),
                },
                "tags": tags,
                "source_status": source_status,
            }
            out.append(enriched)

        out.sort(key=lambda item: (float(item.get("score") or 0.0), float(item.get("percent_change") or 0.0)), reverse=True)
        return out

    def _fetch_one(self, stock: DiscoverStockDef) -> dict | None:
        quote = self._fetch_nse_quote(stock.nse_symbol)
        quote_source = "nse_quote_api"
        if quote is None:
            quote = self._fetch_yahoo_quote(stock.yahoo_symbol)
            quote_source = "yahoo_finance_api"
        if quote is None:
            return None

        if bool(getattr(stock, "fundamentals_enabled", True)):
            fundamentals, fundamentals_source = self._fetch_screener_fundamentals(stock.nse_symbol)
        else:
            fundamentals = {
                "pe_ratio": None,
                "roe": None,
                "roce": None,
                "debt_to_equity": None,
                "price_to_book": None,
                "eps": None,
            }
            fundamentals_source = "unavailable"
        fundamentals_count = sum(1 for value in fundamentals.values() if value is not None)

        # Source health is driven primarily by fundamentals coverage from Screener.
        # Quote source can vary (NSE/Yahoo) without reducing to fallback when core
        # fundamentals are available.
        source_status = "primary" if (fundamentals_source == "screener_in" and fundamentals_count >= 2) else "fallback"
        if fundamentals_count == 0 and quote_source != "nse_quote_api":
            source_status = "limited"

        return {
            "market": "IN",
            "symbol": stock.nse_symbol,
            "display_name": stock.display_name,
            "sector": stock.sector,
            "last_price": quote["last_price"],
            "point_change": quote.get("point_change"),
            "percent_change": quote.get("percent_change"),
            "volume": quote.get("volume"),
            "traded_value": quote.get("traded_value"),
            "pe_ratio": fundamentals.get("pe_ratio"),
            "roe": fundamentals.get("roe"),
            "roce": fundamentals.get("roce"),
            "debt_to_equity": fundamentals.get("debt_to_equity"),
            "price_to_book": fundamentals.get("price_to_book"),
            "eps": fundamentals.get("eps"),
            "source_status": source_status,
            "source_timestamp": quote.get("source_timestamp") or datetime.now(timezone.utc),
            "primary_source": fundamentals_source,
            "secondary_source": quote_source,
        }

    def fetch_all(self) -> list[dict]:
        universe = self._build_effective_universe()
        raw_rows: list[dict] = []
        for stock in universe:
            item = self._fetch_one(stock)
            if item is not None:
                raw_rows.append(item)
        return self._compute_scores(raw_rows)


_scraper = DiscoverStockScraper()


def _fetch_discover_stock_rows_sync() -> list[dict]:
    return _scraper.fetch_all()


async def run_discover_stock_job() -> None:
    try:
        loop = asyncio.get_event_loop()
        rows = await loop.run_in_executor(
            get_job_executor("discover-stock"),
            _fetch_discover_stock_rows_sync,
        )
        count = await discover_service.upsert_discover_stock_snapshots(rows)
        logger.info("Discover stock job complete: %d snapshots upserted", count)
    except requests.RequestException:
        logger.exception("Discover stock job failed due to network exception")
    except Exception:
        logger.exception("Discover stock job failed")

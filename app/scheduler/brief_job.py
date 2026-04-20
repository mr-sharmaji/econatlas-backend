from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable

import requests

from app.scheduler.base import BaseScraper
from app.scheduler.job_executors import get_job_executor
from app.services import brief_service

logger = logging.getLogger(__name__)

YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"


def _yahoo_chart_url(symbol: str) -> str:
    proxy = os.environ.get("INTRADAY_YAHOO_PROXY_URL", "").strip()
    if proxy:
        return proxy.rstrip("/") + f"/v8/finance/chart/{symbol}"
    return YAHOO_CHART_URL.format(symbol=symbol)


@dataclass(frozen=True)
class StockDef:
    symbol: str
    display_name: str
    sector: str


# Curated liquid universes (expandable). v1 keeps high-liquidity names for reliable snapshots.
INDIA_STOCKS: tuple[StockDef, ...] = (
    StockDef("RELIANCE.NS", "Reliance Industries", "Energy"),
    StockDef("TCS.NS", "TCS", "IT"),
    StockDef("HDFCBANK.NS", "HDFC Bank", "Financials"),
    StockDef("ICICIBANK.NS", "ICICI Bank", "Financials"),
    StockDef("INFY.NS", "Infosys", "IT"),
    StockDef("BHARTIARTL.NS", "Bharti Airtel", "Telecom"),
    StockDef("ITC.NS", "ITC", "Consumer"),
    StockDef("SBIN.NS", "State Bank of India", "Financials"),
    StockDef("LT.NS", "Larsen & Toubro", "Industrials"),
    StockDef("KOTAKBANK.NS", "Kotak Mahindra Bank", "Financials"),
    StockDef("HINDUNILVR.NS", "Hindustan Unilever", "Consumer"),
    StockDef("AXISBANK.NS", "Axis Bank", "Financials"),
    StockDef("BAJFINANCE.NS", "Bajaj Finance", "Financials"),
    StockDef("ASIANPAINT.NS", "Asian Paints", "Materials"),
    StockDef("MARUTI.NS", "Maruti Suzuki", "Auto"),
    StockDef("TITAN.NS", "Titan", "Consumer"),
    StockDef("SUNPHARMA.NS", "Sun Pharma", "Healthcare"),
    StockDef("ULTRACEMCO.NS", "UltraTech Cement", "Materials"),
    StockDef("HCLTECH.NS", "HCL Tech", "IT"),
    StockDef("NTPC.NS", "NTPC", "Utilities"),
    StockDef("POWERGRID.NS", "Power Grid", "Utilities"),
    StockDef("ONGC.NS", "ONGC", "Energy"),
    StockDef("COALINDIA.NS", "Coal India", "Energy"),
    StockDef("ADANIENT.NS", "Adani Enterprises", "Industrials"),
    StockDef("ADANIPORTS.NS", "Adani Ports", "Industrials"),
    StockDef("WIPRO.NS", "Wipro", "IT"),
    StockDef("TECHM.NS", "Tech Mahindra", "IT"),
    StockDef("M&M.NS", "Mahindra & Mahindra", "Auto"),
    StockDef("TATAPOWER.NS", "Tata Power", "Utilities"),
    StockDef("TATASTEEL.NS", "Tata Steel", "Materials"),
    StockDef("JSWSTEEL.NS", "JSW Steel", "Materials"),
    StockDef("GRASIM.NS", "Grasim", "Materials"),
    StockDef("INDUSINDBK.NS", "IndusInd Bank", "Financials"),
    StockDef("BAJAJFINSV.NS", "Bajaj Finserv", "Financials"),
    StockDef("DRREDDY.NS", "Dr Reddy's", "Healthcare"),
    StockDef("APOLLOHOSP.NS", "Apollo Hospitals", "Healthcare"),
    StockDef("DIVISLAB.NS", "Divi's Labs", "Healthcare"),
    StockDef("NESTLEIND.NS", "Nestle India", "Consumer"),
    StockDef("HDFCLIFE.NS", "HDFC Life", "Financials"),
    StockDef("SBILIFE.NS", "SBI Life", "Financials"),
    StockDef("PIDILITIND.NS", "Pidilite", "Materials"),
    StockDef("EICHERMOT.NS", "Eicher Motors", "Auto"),
    StockDef("BRITANNIA.NS", "Britannia", "Consumer"),
    StockDef("HEROMOTOCO.NS", "Hero MotoCorp", "Auto"),
    StockDef("CIPLA.NS", "Cipla", "Healthcare"),
    StockDef("BPCL.NS", "BPCL", "Energy"),
    StockDef("SHRIRAMFIN.NS", "Shriram Finance", "Financials"),
    StockDef("ADANIGREEN.NS", "Adani Green", "Utilities"),
    StockDef("ADANIPOWER.NS", "Adani Power", "Utilities"),
    StockDef("JIOFIN.NS", "Jio Financial", "Financials"),
)

US_STOCKS: tuple[StockDef, ...] = (
    StockDef("AAPL", "Apple", "Technology"),
    StockDef("MSFT", "Microsoft", "Technology"),
    StockDef("NVDA", "NVIDIA", "Technology"),
    StockDef("AMZN", "Amazon", "Consumer"),
    StockDef("GOOGL", "Alphabet A", "Technology"),
    StockDef("META", "Meta", "Technology"),
    StockDef("TSLA", "Tesla", "Auto"),
    StockDef("BRK-B", "Berkshire Hathaway", "Financials"),
    StockDef("JPM", "JPMorgan Chase", "Financials"),
    StockDef("V", "Visa", "Financials"),
    StockDef("MA", "Mastercard", "Financials"),
    StockDef("XOM", "Exxon Mobil", "Energy"),
    StockDef("LLY", "Eli Lilly", "Healthcare"),
    StockDef("UNH", "UnitedHealth", "Healthcare"),
    StockDef("JNJ", "Johnson & Johnson", "Healthcare"),
    StockDef("PG", "Procter & Gamble", "Consumer"),
    StockDef("HD", "Home Depot", "Consumer"),
    StockDef("KO", "Coca-Cola", "Consumer"),
    StockDef("PEP", "PepsiCo", "Consumer"),
    StockDef("AVGO", "Broadcom", "Technology"),
    StockDef("COST", "Costco", "Consumer"),
    StockDef("BAC", "Bank of America", "Financials"),
    StockDef("WFC", "Wells Fargo", "Financials"),
    StockDef("AMD", "AMD", "Technology"),
    StockDef("NFLX", "Netflix", "Technology"),
    StockDef("ADBE", "Adobe", "Technology"),
    StockDef("CRM", "Salesforce", "Technology"),
    StockDef("INTC", "Intel", "Technology"),
    StockDef("QCOM", "Qualcomm", "Technology"),
    StockDef("CSCO", "Cisco", "Technology"),
    StockDef("ORCL", "Oracle", "Technology"),
    StockDef("IBM", "IBM", "Technology"),
    StockDef("T", "AT&T", "Telecom"),
    StockDef("VZ", "Verizon", "Telecom"),
    StockDef("PFE", "Pfizer", "Healthcare"),
    StockDef("MRK", "Merck", "Healthcare"),
    StockDef("ABBV", "AbbVie", "Healthcare"),
    StockDef("CVX", "Chevron", "Energy"),
    StockDef("COP", "ConocoPhillips", "Energy"),
    StockDef("MCD", "McDonald's", "Consumer"),
    StockDef("NKE", "Nike", "Consumer"),
    StockDef("WMT", "Walmart", "Consumer"),
    StockDef("DIS", "Disney", "Communication"),
    StockDef("UBER", "Uber", "Technology"),
    StockDef("PYPL", "PayPal", "Financials"),
    StockDef("SHOP", "Shopify", "Technology"),
    StockDef("AMAT", "Applied Materials", "Technology"),
    StockDef("TXN", "Texas Instruments", "Technology"),
    StockDef("MU", "Micron", "Technology"),
    StockDef("PANW", "Palo Alto Networks", "Technology"),
)


class BriefStockScraper(BaseScraper):

    def _fetch_one(self, item: StockDef, market: str) -> dict | None:
        try:
            payload = self._get_json(_yahoo_chart_url(item.symbol))
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
            volume_raw = meta.get("regularMarketVolume")
            volume = int(volume_raw) if volume_raw is not None else None
            traded_value = round(price * volume, 2) if volume is not None else None
            ts_raw = meta.get("regularMarketTime")
            if ts_raw is not None:
                try:
                    source_ts = datetime.fromtimestamp(int(ts_raw), tz=timezone.utc)
                except (TypeError, ValueError, OSError):
                    source_ts = datetime.now(timezone.utc)
            else:
                source_ts = datetime.now(timezone.utc)

            display = str(meta.get("shortName") or item.display_name or item.symbol)
            return {
                "market": market,
                "symbol": item.symbol,
                "display_name": display,
                "sector": item.sector,
                "last_price": price,
                "point_change": point,
                "percent_change": pct,
                "volume": volume,
                "traded_value": traded_value,
                "source_timestamp": source_ts.isoformat(),
                "source": "yahoo_finance_api",
            }
        except requests.RequestException:
            logger.debug("Brief snapshot fetch failed: market=%s symbol=%s", market, item.symbol, exc_info=True)
            return None
        except Exception:
            logger.debug("Brief snapshot parse failed: market=%s symbol=%s", market, item.symbol, exc_info=True)
            return None

    def _fetch_market(self, market: str, universe: Iterable[StockDef]) -> list[dict]:
        out: list[dict] = []
        for item in universe:
            row = self._fetch_one(item, market)
            if row is not None:
                out.append(row)
        return out

    def fetch_all(self) -> list[dict]:
        india_rows = self._fetch_market("IN", INDIA_STOCKS)
        logger.debug("Brief snapshot fetch complete: IN=%d", len(india_rows))
        return india_rows


_scraper = BriefStockScraper()


def _fetch_brief_rows_sync() -> list[dict]:
    return _scraper.fetch_all()


async def run_brief_job() -> None:
    try:
        loop = asyncio.get_event_loop()
        rows = await loop.run_in_executor(
            get_job_executor("brief"),
            _fetch_brief_rows_sync,
        )
        count = await brief_service.upsert_stock_snapshots(rows)
        logger.info("Brief stock job complete: %d snapshots upserted", count)
    except Exception:
        logger.exception("Brief stock job failed")

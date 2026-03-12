from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone
import os

os.environ.setdefault(
    "DATABASE_URL",
    "postgresql://econatlas:econatlas@localhost:5432/econatlas",
)

from app.scheduler.discover_mutual_fund_job import DiscoverMutualFundScraper
from app.scheduler.discover_stock_job import DiscoverStockScraper


class DiscoverStockJobTests(unittest.TestCase):
    def test_compute_scores_reweights_when_fundamentals_missing(self) -> None:
        scraper = DiscoverStockScraper()
        rows = [
            {
                "symbol": "AAA",
                "display_name": "AAA Ltd",
                "market": "IN",
                "sector": "IT",
                "last_price": 100,
                "point_change": 2,
                "percent_change": 2.0,
                "volume": 100000,
                "traded_value": 5000000,
                "pe_ratio": None,
                "roe": None,
                "roce": None,
                "debt_to_equity": None,
                "price_to_book": None,
                "eps": None,
                "source_status": "primary",
                "source_timestamp": datetime.now(timezone.utc),
                "primary_source": "screener_in",
                "secondary_source": "nse_quote_api",
            },
            {
                "symbol": "BBB",
                "display_name": "BBB Ltd",
                "market": "IN",
                "sector": "IT",
                "last_price": 100,
                "point_change": 1,
                "percent_change": 1.0,
                "volume": 120000,
                "traded_value": 7000000,
                "pe_ratio": 18,
                "roe": 16,
                "roce": 18,
                "debt_to_equity": 0.4,
                "price_to_book": 3,
                "eps": 10,
                "source_status": "primary",
                "source_timestamp": datetime.now(timezone.utc),
                "primary_source": "screener_in",
                "secondary_source": "nse_quote_api",
            },
        ]

        scored = scraper._compute_scores(rows)
        lookup = {r["symbol"]: r for r in scored}

        self.assertIn("AAA", lookup)
        self.assertIn("BBB", lookup)
        self.assertEqual("fallback", lookup["AAA"]["source_status"])
        self.assertGreaterEqual(lookup["AAA"]["score"], 0)
        self.assertLessEqual(lookup["AAA"]["score"], 100)
        self.assertIn("limited_data", lookup["AAA"]["tags"])

    def test_fetch_nse_quote_skips_when_cooldown_is_active(self) -> None:
        scraper = DiscoverStockScraper()
        scraper._nse_disabled_until = datetime.now(timezone.utc) + timedelta(seconds=120)
        scraper._ensure_nse_session = lambda: self.fail("NSE session should not be attempted during cooldown")
        self.assertIsNone(scraper._fetch_nse_quote("INFY"))

    def test_fetch_one_keeps_primary_when_screener_fundamentals_available(self) -> None:
        scraper = DiscoverStockScraper()
        stock = type(
            "Stock",
            (),
            {"nse_symbol": "INFY", "yahoo_symbol": "INFY.NS", "display_name": "Infosys", "sector": "IT"},
        )()

        scraper._fetch_nse_quote = lambda symbol: None
        scraper._fetch_yahoo_quote = lambda symbol: {
            "last_price": 100.0,
            "point_change": 1.0,
            "percent_change": 1.0,
            "volume": 1000,
            "traded_value": 100000.0,
            "source_timestamp": datetime.now(timezone.utc),
            "source": "yahoo_finance_api",
        }
        scraper._fetch_screener_fundamentals = lambda symbol: (
            {
                "pe_ratio": 20.0,
                "roe": 18.0,
                "roce": 20.0,
                "debt_to_equity": None,
                "price_to_book": None,
                "eps": None,
            },
            "screener_in",
        )

        row = scraper._fetch_one(stock)
        self.assertIsNotNone(row)
        self.assertEqual("primary", row["source_status"])


class DiscoverMutualFundJobTests(unittest.TestCase):
    def test_compute_scores_assigns_fallback_when_advanced_missing(self) -> None:
        scraper = DiscoverMutualFundScraper()
        rows = [
            {
                "scheme_code": "1001",
                "scheme_name": "Fund A Direct Plan Growth",
                "category": "Large Cap",
                "plan_type": "direct",
                "nav": 120.0,
                "source_status": "primary",
                "source_timestamp": datetime.now(timezone.utc),
                "returns_1y": None,
                "returns_3y": None,
                "returns_5y": None,
                "expense_ratio": None,
                "risk_level": None,
                "std_dev": None,
                "sharpe": None,
                "sortino": None,
            },
            {
                "scheme_code": "1002",
                "scheme_name": "Fund B Direct Plan Growth",
                "category": "Large Cap",
                "plan_type": "direct",
                "nav": 140.0,
                "source_status": "primary",
                "source_timestamp": datetime.now(timezone.utc),
                "returns_1y": 10.0,
                "returns_3y": 13.0,
                "returns_5y": 15.0,
                "expense_ratio": 0.8,
                "risk_level": "Moderate",
                "std_dev": 7.0,
                "sharpe": 1.0,
                "sortino": 1.2,
            },
        ]
        scored = scraper._compute_scores(rows)
        lookup = {r["scheme_code"]: r for r in scored}

        self.assertEqual("fallback", lookup["1001"]["source_status"])
        self.assertGreaterEqual(lookup["1002"]["score"], 0)
        self.assertLessEqual(lookup["1002"]["score"], 100)


if __name__ == "__main__":
    unittest.main()

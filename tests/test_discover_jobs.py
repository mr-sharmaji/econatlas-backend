from __future__ import annotations

import unittest
import io
import zipfile
from datetime import datetime, timedelta, timezone
import os

os.environ.setdefault(
    "DATABASE_URL",
    "postgresql://econatlas:econatlas@localhost:5432/econatlas",
)

from app.scheduler.discover_mutual_fund_job import DiscoverMutualFundScraper
from app.scheduler.discover_stock_job import CORE_UNIVERSE, DiscoverStockScraper


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

    def test_fetch_nse_master_universe_parses_equity_series(self) -> None:
        scraper = DiscoverStockScraper()
        scraper._get_text = lambda *_args, **_kwargs: (
            "SYMBOL,NAME OF COMPANY, SERIES\n"
            "ABC,ABC LIMITED,EQ\n"
            "XYZ,XYZ LIMITED,BE\n"
            "GBOND,GOLD BOND,GB\n"
        )
        parsed = scraper._fetch_nse_master_universe()
        symbols = {row.nse_symbol for row in parsed}
        self.assertIn("ABC", symbols)
        self.assertIn("XYZ", symbols)
        self.assertNotIn("GBOND", symbols)

    def test_build_effective_universe_falls_back_to_core_on_fetch_failure(self) -> None:
        scraper = DiscoverStockScraper()
        scraper._fetch_nse_master_universe = lambda: (_ for _ in ()).throw(RuntimeError("boom"))
        parsed = scraper._build_effective_universe()
        self.assertEqual(len(parsed), len(CORE_UNIVERSE))

    def test_fetch_latest_bhavcopy_quotes_parses_zip_payload(self) -> None:
        scraper = DiscoverStockScraper()
        csv_text = (
            "TradDt,BizDt,Sgmt,Src,FinInstrmTp,FinInstrmId,ISIN,TckrSymb,SctySrs,XpryDt,FininstrmActlXpryDt,StrkPric,OptnTp,FinInstrmNm,OpnPric,HghPric,LwPric,ClsPric,LastPric,PrvsClsgPric,UndrlygPric,SttlmPric,OpnIntrst,ChngInOpnIntrst,TtlTradgVol,TtlTrfVal,TtlNbOfTxsExctd,SsnId,NewBrdLotQty,Rmks,Rsvd1,Rsvd2,Rsvd3,Rsvd4\n"
            "2026-03-12,2026-03-12,CM,NSE,STK,1,INE000A01011,ABC,EQ,,,,,ABC LTD,10,11,9,10.5,10.5,10,10.5,,,1000,10500,10,F1,1,,,,,\n"
            "2026-03-12,2026-03-12,CM,NSE,STK,2,INE000A01012,GBOND,GB,,,,,GOLD BOND,100,101,99,100,100,99,100,,,10,1000,1,F1,1,,,,,\n"
        )

        payload = io.BytesIO()
        with zipfile.ZipFile(payload, "w", zipfile.ZIP_DEFLATED) as archive:
            archive.writestr("bhavcopy.csv", csv_text)
        payload_bytes = payload.getvalue()

        class _Resp:
            status_code = 200
            content = payload_bytes

            def raise_for_status(self) -> None:
                return None

        scraper.session.get = lambda *_args, **_kwargs: _Resp()
        scraper._bhavcopy_lookback_days = 0
        quotes, source_ts = scraper._fetch_latest_bhavcopy_quotes()
        self.assertIn("ABC", quotes)
        self.assertNotIn("GBOND", quotes)
        self.assertIsNotNone(source_ts)


class DiscoverMutualFundJobTests(unittest.TestCase):
    def test_parse_amfi_fallback_uses_scheme_category_not_amc_line(self) -> None:
        scraper = DiscoverMutualFundScraper()
        scraper._get_text = lambda *_args, **_kwargs: (
            "Scheme Code;ISIN Div Payout/ ISIN Growth;ISIN Div Reinvestment;Scheme Name;Net Asset Value;Date\n"
            "Open Ended Schemes(Debt Scheme - Banking and PSU Fund)\n"
            "Aditya Birla Sun Life Mutual Fund\n"
            "119551;INF209KA12Z1;INF209KA13Z9;Aditya Birla Sun Life Banking & PSU Debt Fund  - DIRECT - IDCW;111.1072;11-Mar-2026\n"
        )
        rows = scraper._parse_amfi_fallback()
        self.assertIn("119551", rows)
        row = rows["119551"]
        self.assertEqual("Debt", row["category"])
        self.assertEqual("Banking and PSU Fund", row["sub_category"])
        self.assertEqual("Aditya Birla Sun Life Mutual Fund", row["amc"])

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

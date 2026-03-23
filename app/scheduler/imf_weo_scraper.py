"""IMF World Economic Outlook forecast fetcher.

Fetches GDP growth and inflation projections for India and US
from the IMF DataMapper API.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, List

import requests

from app.scheduler.job_executors import get_job_executor
from app.services import macro_service

logger = logging.getLogger(__name__)

_IMF_API = "https://www.imf.org/external/datamapper/api/v1"

# IMF indicator codes
_INDICATORS: Dict[str, str] = {
    "gdp_growth": "NGDP_RPCH",
    "inflation": "PCPIPCH",
}

# IMF country codes (different from our 2-letter codes)
_COUNTRY_MAP: Dict[str, str] = {
    "IN": "IND",
    "US": "USA",
}

# Reverse mapping for storage
_COUNTRY_REVERSE = {v: k for k, v in _COUNTRY_MAP.items()}


def _fetch_imf_forecasts_sync() -> List[Dict]:
    """Fetch IMF WEO forecasts. Runs synchronously in thread executor."""
    now = datetime.now(timezone.utc)
    current_year = now.year
    periods = ",".join(str(y) for y in range(current_year - 1, current_year + 4))

    results: List[Dict] = []

    for indicator_name, imf_code in _INDICATORS.items():
        countries = "/".join(_COUNTRY_MAP.values())
        url = f"{_IMF_API}/{imf_code}/{countries}?periods={periods}"

        try:
            resp = requests.get(url, timeout=30)
            if resp.status_code != 200:
                logger.warning("IMF API returned %d for %s", resp.status_code, imf_code)
                continue

            data = resp.json()
            values = data.get("values", {}).get(imf_code, {})

            for imf_country, year_data in values.items():
                our_country = _COUNTRY_REVERSE.get(imf_country)
                if not our_country:
                    continue

                for year_str, value in year_data.items():
                    try:
                        year = int(year_str)
                        val = float(value)
                        results.append({
                            "indicator_name": indicator_name,
                            "country": our_country,
                            "forecast_year": year,
                            "value": round(val, 2),
                            "source": "imf_weo",
                        })
                    except (ValueError, TypeError):
                        continue

            logger.info("IMF %s: fetched %d country-year pairs", indicator_name, len(values))

        except Exception:
            logger.exception("IMF fetch failed for %s", imf_code)

    logger.info("IMF WEO complete: %d forecasts", len(results))
    return results


async def run_imf_forecast_job() -> None:
    """Async entry point for the IMF forecast job."""
    try:
        loop = asyncio.get_event_loop()
        forecasts = await loop.run_in_executor(
            get_job_executor("imf-forecast"),
            _fetch_imf_forecasts_sync,
        )
        if forecasts:
            count = await macro_service.upsert_forecasts(forecasts)
            logger.info("IMF forecast job: %d forecasts upserted", count)
        else:
            logger.info("IMF forecast job: no data fetched")
    except Exception:
        logger.exception("IMF forecast job failed")

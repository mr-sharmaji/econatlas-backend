from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable


@dataclass
class QuoteTick:
    asset: str
    instrument_type: str
    price: float
    unit: str
    source: str
    provider: str
    provider_priority: int
    confidence_level: float
    source_timestamp: datetime
    previous_close: float | None = None
    change_percent: float | None = None
    is_fallback: bool = False
    quality: str | None = None
    is_predictive: bool = False
    session_source: str | None = None


class QuoteProvider(ABC):
    """Provider adapter for market quotes."""

    @abstractmethod
    def fetch_quotes(self) -> list[QuoteTick]:
        raise NotImplementedError


def select_best_quotes(ticks: Iterable[QuoteTick]) -> list[QuoteTick]:
    """Select best quote per (asset, instrument_type) by provider priority then recency.
    Freshness override: for index quotes only, allow newer fallback ticks to replace
    stale primary ticks when the timestamp gap is significant."""
    freshness_override = timedelta(minutes=8)
    best: dict[tuple[str, str], QuoteTick] = {}
    for tick in ticks:
        key = (tick.asset, tick.instrument_type)
        prev = best.get(key)
        if prev is None:
            best[key] = tick
            continue
        if tick.provider_priority < prev.provider_priority:
            best[key] = tick
            continue
        prev_ts = prev.source_timestamp
        cur_ts = tick.source_timestamp
        if prev_ts.tzinfo is None:
            prev_ts = prev_ts.replace(tzinfo=timezone.utc)
        if cur_ts.tzinfo is None:
            cur_ts = cur_ts.replace(tzinfo=timezone.utc)
        if (
            tick.instrument_type == "index"
            and tick.provider_priority > prev.provider_priority
            and (cur_ts - prev_ts) >= freshness_override
        ):
            # Prefer significantly fresher index fallback over delayed primary feed.
            best[key] = tick
            continue
        if tick.provider_priority == prev.provider_priority:
            if cur_ts > prev_ts:
                best[key] = tick
    return list(best.values())

from __future__ import annotations

import math
from datetime import UTC, date, datetime, timedelta

from app.core.database import get_pool, parse_ts, record_to_dict

TABLE = "macro_indicators"

COUNTRY_ORDER = ["IN", "US", "EU", "JP"]
COUNTRY_LABELS: dict[str, str] = {
    "IN": "India",
    "US": "United States",
    "EU": "Europe",
    "JP": "Japan",
}

INFLATION_TARGETS: dict[str, float] = {
    "IN": 4.0,
    "US": 2.0,
    "EU": 2.0,
    "JP": 2.0,
}

NEUTRAL_POLICY_RATE: dict[str, float] = {
    "IN": 6.0,
    "US": 2.5,
    "EU": 2.0,
    "JP": 1.0,
}

PERCENT_UNITS = {"percent", "percent_yoy", "percent_gdp"}

INDICATOR_METADATA: list[dict] = [
    {
        "indicator_name": "gdp_growth",
        "display_name": "GDP Growth",
        "helper_text": "GDP Growth shows how fast the economy is expanding versus last year. Higher, stable growth usually means stronger demand and business activity.",
        "unit": "percent",
        "frequency": "quarterly",
        "source": "fred_api/imf_weo",
        "update_cadence": "quarterly",
        "chart_type": "line",
        "thresholds": {"slowdown": 2.0, "strong": 6.0},
    },
    {
        "indicator_name": "inflation",
        "display_name": "Inflation (CPI)",
        "helper_text": "Inflation tracks changes in consumer prices. Sustained inflation above target can reduce purchasing power and keep policy rates elevated.",
        "unit": "percent_yoy",
        "frequency": "monthly",
        "source": "fred_api",
        "update_cadence": "monthly",
        "chart_type": "line",
        "thresholds": {"target_us_eu_jp": 2.0, "target_in": 4.0},
    },
    {
        "indicator_name": "core_inflation",
        "display_name": "Core Inflation",
        "helper_text": "Core inflation excludes volatile food and energy items and is often used to judge persistent price pressure in the economy.",
        "unit": "percent_yoy",
        "frequency": "monthly",
        "source": "fred_api/trading_economics",
        "update_cadence": "monthly",
        "chart_type": "line",
        "thresholds": {"elevated": 4.0, "sticky": 5.0},
    },
    {
        "indicator_name": "food_inflation",
        "display_name": "Food Inflation",
        "helper_text": "Food inflation measures changes in food prices, which directly impact household budgets and near-term inflation expectations.",
        "unit": "percent_yoy",
        "frequency": "monthly",
        "source": "trading_economics",
        "update_cadence": "monthly",
        "chart_type": "line",
        "thresholds": {"elevated": 6.0, "high": 8.0},
    },
    {
        "indicator_name": "unemployment",
        "display_name": "Unemployment",
        "helper_text": "Unemployment reflects labor market health. Lower unemployment typically indicates stronger hiring and consumer income support.",
        "unit": "percent",
        "frequency": "monthly",
        "source": "fred_api/world_bank",
        "update_cadence": "monthly",
        "chart_type": "line",
        "thresholds": {"moderate": 4.5, "high": 6.0},
    },
    {
        "indicator_name": "repo_rate",
        "display_name": "Policy Rate",
        "helper_text": "Policy Rate is the central bank's benchmark interest rate. It influences borrowing costs, liquidity conditions, and market risk appetite.",
        "unit": "percent",
        "frequency": "monthly",
        "source": "fred_api",
        "update_cadence": "event-driven",
        "chart_type": "line",
        "thresholds": {"restrictive_gap": 1.5},
    },
    {
        "indicator_name": "pmi_manufacturing",
        "display_name": "PMI Manufacturing",
        "helper_text": "PMI Manufacturing is a forward-looking business survey. Readings above 50 indicate expansion, while below 50 indicate contraction.",
        "unit": "index",
        "frequency": "monthly",
        "source": "trading_economics",
        "update_cadence": "monthly",
        "chart_type": "line",
        "thresholds": {"expansion": 50.0, "strong": 55.0},
    },
    {
        "indicator_name": "pmi_services",
        "display_name": "PMI Services",
        "helper_text": "PMI Services tracks activity momentum in the services sector. Values above 50 signal expansion in demand and output.",
        "unit": "index",
        "frequency": "monthly",
        "source": "trading_economics",
        "update_cadence": "monthly",
        "chart_type": "line",
        "thresholds": {"expansion": 50.0, "strong": 55.0},
    },
    {
        "indicator_name": "iip",
        "display_name": "Industrial Production",
        "helper_text": "Industrial Production measures output from manufacturing, mining, and utilities. It is a key read on real economy momentum.",
        "unit": "percent_yoy",
        "frequency": "monthly",
        "source": "trading_economics/fred_api",
        "update_cadence": "monthly",
        "chart_type": "line",
        "thresholds": {"contraction": 0.0, "strong": 5.0},
    },
    {
        "indicator_name": "trade_balance",
        "display_name": "Trade Balance",
        "helper_text": "Trade Balance is exports minus imports. Persistent deficits can pressure the currency, while improving balance supports external stability.",
        "unit": "usd_mn",
        "frequency": "monthly",
        "source": "trading_economics",
        "update_cadence": "monthly",
        "chart_type": "bar",
        "thresholds": {"deficit": 0.0},
    },
    {
        "indicator_name": "current_account_deficit",
        "display_name": "Current Account",
        "helper_text": "Current Account summarizes trade, income, and transfer flows with the rest of the world. Large deficits can increase external funding risk.",
        "unit": "usd_bn",
        "frequency": "quarterly",
        "source": "trading_economics",
        "update_cadence": "quarterly",
        "chart_type": "bar",
        "thresholds": {"deficit": 0.0},
    },
    {
        "indicator_name": "fiscal_deficit",
        "display_name": "Fiscal Deficit",
        "helper_text": "Fiscal Deficit shows how much government spending exceeds revenue. Wider deficits may raise borrowing needs and bond supply pressure.",
        "unit": "percent_gdp",
        "frequency": "quarterly",
        "source": "trading_economics",
        "update_cadence": "quarterly",
        "chart_type": "bar",
        "thresholds": {"wide": 5.0},
    },
    {
        "indicator_name": "bank_credit_growth",
        "display_name": "Bank Credit Growth",
        "helper_text": "Bank Credit Growth indicates lending momentum in the financial system. Rising growth often signals stronger investment and consumption demand.",
        "unit": "percent",
        "frequency": "monthly",
        "source": "trading_economics",
        "update_cadence": "monthly",
        "chart_type": "line",
        "thresholds": {"weak": 5.0, "strong": 12.0},
    },
    {
        "indicator_name": "forex_reserves",
        "display_name": "FX Reserves",
        "helper_text": "FX Reserves are foreign currency assets held by the central bank. Higher reserves improve resilience against external shocks.",
        "unit": "usd_mn",
        "frequency": "weekly",
        "source": "trading_economics",
        "update_cadence": "weekly",
        "chart_type": "area",
        "thresholds": {},
    },
    {
        "indicator_name": "fii_net_cash",
        "display_name": "FII Net Cash",
        "helper_text": "FII Net Cash captures daily net equity flows from foreign institutional investors. It can influence short-term market liquidity and sentiment.",
        "unit": "inr_cr",
        "frequency": "daily",
        "source": "nse_fiidii_api",
        "update_cadence": "daily",
        "chart_type": "bar",
        "thresholds": {"buying": 0.0},
    },
    {
        "indicator_name": "dii_net_cash",
        "display_name": "DII Net Cash",
        "helper_text": "DII Net Cash captures daily net equity flows from domestic institutions. It often offsets or amplifies foreign flow impact.",
        "unit": "inr_cr",
        "frequency": "daily",
        "source": "nse_fiidii_api",
        "update_cadence": "daily",
        "chart_type": "bar",
        "thresholds": {"buying": 0.0},
    },
    {
        "indicator_name": "bond_yield_10y",
        "display_name": "10Y Yield",
        "helper_text": "10Y Yield is the benchmark long-term sovereign borrowing cost. It is a core reference for valuation and macro risk pricing.",
        "unit": "percent",
        "frequency": "intraday",
        "source": "market_feed",
        "update_cadence": "live",
        "chart_type": "line",
        "thresholds": {},
    },
    {
        "indicator_name": "bond_yield_2y",
        "display_name": "2Y Yield",
        "helper_text": "2Y Yield reflects near-term policy expectations and front-end rate conditions in fixed-income markets.",
        "unit": "percent",
        "frequency": "intraday",
        "source": "market_feed",
        "update_cadence": "live",
        "chart_type": "line",
        "thresholds": {},
    },
]

DEFAULT_LINKAGE_ASSETS: dict[str, list[str]] = {
    "IN": ["Nifty 50", "India 10Y Bond Yield", "USD/INR", "gold", "crude oil"],
    "US": ["S&P500", "US 10Y Treasury Yield", "US 2Y Treasury Yield", "gold", "bitcoin"],
    "EU": ["DAX", "Euro Stoxx 50", "Germany 10Y Bond Yield", "EUR/INR", "gold"],
    "JP": ["Nikkei 225", "Japan 10Y Bond Yield", "JPY/INR", "gold", "crude oil"],
}


def _as_dt(ts: object) -> datetime | None:
    if ts is None:
        return None
    if isinstance(ts, datetime):
        return ts if ts.tzinfo is not None else ts.replace(tzinfo=UTC)
    try:
        parsed = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)
    except ValueError:
        return None


def _clamp(value: float | None, low: float = -1.0, high: float = 1.0) -> float | None:
    if value is None or not math.isfinite(value):
        return None
    return max(low, min(high, value))


def _to_float(value: object) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _country_name(code: str) -> str:
    return COUNTRY_LABELS.get(code, code)


def _format_metric_value(value: float, unit: str) -> str:
    if unit in PERCENT_UNITS:
        return f"{value:.2f}%"
    if unit == "index":
        return f"{value:.1f}"
    if unit == "usd_mn":
        return f"${value:,.0f}M"
    if unit == "usd_bn":
        return f"${value:,.2f}B"
    if unit == "inr_cr":
        return f"₹{value:,.0f} Cr"
    return f"{value:.2f}"


def _format_metric_change(delta: float, unit: str) -> str:
    sign = "+" if delta >= 0 else ""
    if unit in PERCENT_UNITS:
        return f"{sign}{delta:.2f} pp"
    return f"{sign}{delta:.2f}"


def _metric_signal_text(
    *,
    indicator: str,
    value: float,
    country: str,
    thresholds: dict[str, float],
) -> str:
    if indicator == "inflation":
        target = thresholds.get("target_in", 4.0) if country == "IN" else thresholds.get("target_us_eu_jp", 2.0)
        gap = value - target
        if gap <= 0:
            return f"At or below target ({target:.1f}%)."
        if gap <= 1.5:
            return f"Above target by {gap:.2f} pp."
        return f"Well above target by {gap:.2f} pp; policy pressure can stay elevated."

    if indicator == "core_inflation":
        elevated = thresholds.get("elevated", 4.0)
        sticky = thresholds.get("sticky", 5.0)
        if value >= sticky:
            return f"Sticky and elevated (≥{sticky:.1f}%)."
        if value >= elevated:
            return f"Elevated versus preferred range (≥{elevated:.1f}%)."
        return "Relatively contained."

    if indicator == "food_inflation":
        elevated = thresholds.get("elevated", 6.0)
        high = thresholds.get("high", 8.0)
        if value >= high:
            return f"High food price pressure (≥{high:.1f}%)."
        if value >= elevated:
            return f"Elevated food price pressure (≥{elevated:.1f}%)."
        return "Food price pressure is manageable."

    if indicator == "gdp_growth":
        slowdown = thresholds.get("slowdown", 2.0)
        strong = thresholds.get("strong", 6.0)
        if value >= strong:
            return f"Strong growth momentum (≥{strong:.1f}%)."
        if value <= slowdown:
            return f"Soft growth zone (≤{slowdown:.1f}%)."
        return "Growth is in a moderate range."

    if indicator == "unemployment":
        moderate = thresholds.get("moderate", 4.5)
        high = thresholds.get("high", 6.0)
        if value >= high:
            return f"Labor market is weak (≥{high:.1f}%)."
        if value >= moderate:
            return f"Labor market is moderately soft (≥{moderate:.1f}%)."
        return "Labor market conditions look healthy."

    if indicator == "repo_rate":
        neutral = NEUTRAL_POLICY_RATE.get(country, 2.5)
        restrictive_gap = thresholds.get("restrictive_gap", 1.5)
        if value >= neutral + restrictive_gap:
            return f"Policy looks restrictive vs neutral ({neutral:.1f}%)."
        return f"Policy is near neutral zone ({neutral:.1f}%)."

    if indicator.startswith("pmi_"):
        expansion = thresholds.get("expansion", 50.0)
        strong = thresholds.get("strong", 55.0)
        if value >= strong:
            return f"Strong expansion signal (≥{strong:.1f})."
        if value >= expansion:
            return f"Expansionary signal (≥{expansion:.1f})."
        return f"Contractionary signal (<{expansion:.1f})."

    if indicator in {"iip", "bank_credit_growth"}:
        weak = thresholds.get("weak", 0.0)
        strong = thresholds.get("strong", 5.0)
        if value >= strong:
            return "Momentum is strong."
        if value >= weak:
            return "Momentum is positive but moderate."
        return "Momentum is weak."

    if indicator in {"trade_balance", "current_account_deficit"}:
        if value >= 0:
            return "External balance is in surplus."
        return "External balance is in deficit."

    if indicator == "fiscal_deficit":
        wide = thresholds.get("wide", 5.0)
        if value > wide:
            return f"Deficit is wider than preferred (>{wide:.1f}%)."
        return "Deficit is within a manageable range."

    return "Use this with trend and upcoming events for context."


def _compose_dynamic_helper_text(
    *,
    country: str,
    item: dict,
    latest_row: dict | None,
    previous_row: dict | None,
) -> str:
    base = str(item.get("helper_text") or "").strip()
    indicator = str(item.get("indicator_name") or "")
    display_name = str(item.get("display_name") or indicator or "Metric")
    unit = str(item.get("unit") or "")
    thresholds = item.get("thresholds") or {}
    thresholds = thresholds if isinstance(thresholds, dict) else {}

    latest_value = _to_float(None if latest_row is None else latest_row.get("value"))
    if latest_value is None:
        if base:
            return (
                f"{base}\n\n"
                f"{_country_name(country)} latest: not available.\n"
                f"Signal: Waiting for a fresh {display_name} release."
            )
        return (
            f"{_country_name(country)} latest {display_name}: not available.\n"
            f"Signal: Waiting for a fresh release."
        )

    latest_ts = _as_dt(None if latest_row is None else latest_row.get("timestamp"))
    previous_value = _to_float(None if previous_row is None else previous_row.get("value"))
    signal = _metric_signal_text(
        indicator=indicator,
        value=latest_value,
        country=country,
        thresholds=thresholds,
    )

    latest_line = f"{_country_name(country)} latest: {_format_metric_value(latest_value, unit)}"
    if latest_ts is not None:
        latest_line = f"{latest_line} ({latest_ts.date().isoformat()})"

    change_line = "Change vs previous: No prior release."
    if previous_value is not None:
        delta = latest_value - previous_value
        change_line = f"Change vs previous: {_format_metric_change(delta, unit)}"

    body = "\n".join(
        [
            latest_line,
            change_line,
            f"Signal: {signal}",
        ]
    )
    if not base:
        return body
    return f"{base}\n\n{body}"


async def _latest_two_rows_by_indicator(
    *,
    country: str,
    indicator_names: list[str],
) -> dict[str, list[dict]]:
    if not indicator_names:
        return {}

    pool = await get_pool()
    rows = await pool.fetch(
        f"""
        SELECT indicator_name, value, "timestamp"
        FROM (
            SELECT
                indicator_name,
                value,
                "timestamp",
                ROW_NUMBER() OVER (
                    PARTITION BY indicator_name
                    ORDER BY "timestamp" DESC
                ) AS rn
            FROM {TABLE}
            WHERE country = $1
              AND indicator_name = ANY($2::text[])
        ) ranked
        WHERE rn <= 2
        ORDER BY indicator_name ASC, rn ASC
        """,
        country,
        indicator_names,
    )

    grouped: dict[str, list[dict]] = {}
    for row in rows:
        indicator = str(row["indicator_name"])
        grouped.setdefault(indicator, []).append(
            {
                "value": row["value"],
                "timestamp": row["timestamp"],
            }
        )
    return grouped


def _latest_by_indicator(rows: list[dict], country: str) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for row in rows:
        if row.get("country") != country:
            continue
        name = str(row.get("indicator_name") or "")
        current = out.get(name)
        if current is None:
            out[name] = row
            continue
        current_ts = _as_dt(current.get("timestamp"))
        row_ts = _as_dt(row.get("timestamp"))
        if current_ts is None or (row_ts is not None and row_ts > current_ts):
            out[name] = row
    return out


def _pearson(xs: list[float], ys: list[float]) -> float | None:
    n = min(len(xs), len(ys))
    if n < 3:
        return None
    x = xs[:n]
    y = ys[:n]
    x_mean = sum(x) / n
    y_mean = sum(y) / n
    cov = sum((a - x_mean) * (b - y_mean) for a, b in zip(x, y))
    var_x = sum((a - x_mean) ** 2 for a in x)
    var_y = sum((b - y_mean) ** 2 for b in y)
    if var_x <= 0 or var_y <= 0:
        return None
    return cov / math.sqrt(var_x * var_y)


def _regime_label(growth_score: float | None, inflation_score: float | None) -> str:
    g = growth_score or 0.0
    i = inflation_score or 0.0
    if g >= 0 and i > 0.25:
        return "Overheating"
    if g >= 0 and i <= 0.25:
        return "Expansion"
    if g < 0 and i > 0.25:
        return "Stagflation Risk"
    return "Disinflation Slowdown"


def _risk_label(score: float) -> str:
    if score >= 75:
        return "High"
    if score >= 50:
        return "Elevated"
    if score >= 30:
        return "Moderate"
    return "Contained"


async def get_indicators(
    country: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> list[dict]:
    """Fetch macro-economic indicators, optionally filtered by country."""
    pool = await get_pool()
    if country:
        rows = await pool.fetch(
            f"SELECT * FROM {TABLE} WHERE country = $1 ORDER BY timestamp DESC LIMIT $2 OFFSET $3",
            country,
            limit,
            offset,
        )
    else:
        rows = await pool.fetch(
            f"SELECT * FROM {TABLE} ORDER BY timestamp DESC LIMIT $1 OFFSET $2",
            limit,
            offset,
        )
    return [record_to_dict(r) for r in rows]


async def get_indicators_latest(country: str | None = None) -> list[dict]:
    """Return the latest value per (indicator_name, country) for list views."""
    pool = await get_pool()
    if country:
        rows = await pool.fetch(
            f"""
            SELECT * FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY indicator_name, country ORDER BY timestamp DESC) AS rn
                FROM {TABLE} WHERE country = $1
            ) sub WHERE rn = 1
            ORDER BY indicator_name, country
            """,
            country,
        )
    else:
        rows = await pool.fetch(
            f"""
            SELECT * FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY indicator_name, country ORDER BY timestamp DESC) AS rn
                FROM {TABLE}
            ) sub WHERE rn = 1
            ORDER BY indicator_name, country
            """
        )
    return [record_to_dict(r) for r in rows]


async def insert_indicator(payload: dict) -> dict | None:
    """Insert a macro-economic indicator row. Idempotent: ON CONFLICT DO NOTHING.
    Returns the created row, or None if row already existed (duplicate key)."""
    pool = await get_pool()
    row = await pool.fetchrow(
        f"""
        INSERT INTO {TABLE} (indicator_name, value, country, timestamp, unit, source)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (indicator_name, country, "timestamp") DO NOTHING
        RETURNING *
        """,
        payload["indicator_name"],
        payload["value"],
        payload["country"],
        parse_ts(payload["timestamp"]),
        payload.get("unit"),
        payload.get("source"),
    )
    if row is None:
        return None
    return record_to_dict(row)


async def insert_indicators_batch_upsert_source_timestamp(rows: list[dict]) -> int:
    """Insert or update macro rows using provider/source timestamps.
    Uniqueness is (indicator_name, country, timestamp)."""
    if not rows:
        return 0
    pool = await get_pool()
    count = 0
    async with pool.acquire() as conn:
        for r in rows:
            await conn.execute(
                f"""
                INSERT INTO {TABLE} (indicator_name, value, country, timestamp, unit, source)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (indicator_name, country, "timestamp")
                DO UPDATE SET
                    value = EXCLUDED.value,
                    unit = COALESCE(EXCLUDED.unit, {TABLE}.unit),
                    source = EXCLUDED.source
                """,
                r.get("indicator_name"),
                r.get("value"),
                r.get("country"),
                parse_ts(r.get("timestamp")),
                r.get("unit"),
                r.get("source"),
            )
            count += 1
    return count


async def insert_indicators_batch_upsert_daily(rows: list[dict]) -> int:
    """Backward-compatible alias.
    Prefer insert_indicators_batch_upsert_source_timestamp for new code."""
    return await insert_indicators_batch_upsert_source_timestamp(rows)


async def delete_rows_newer_than_source_timestamps(
    rows: list[dict],
    sources: set[str] | None = None,
) -> int:
    """Delete legacy rows that are newer than provider/source timestamps.
    Useful when migrating from synthetic daily timestamps to source timestamps."""
    if not rows:
        return 0
    source_filter = {s.lower() for s in (sources or set())}
    candidates: set[tuple[str, str, str, str]] = set()
    for r in rows:
        indicator = r.get("indicator_name")
        country = r.get("country")
        source = str(r.get("source") or "").lower()
        ts = r.get("timestamp")
        if not indicator or not country or not ts:
            continue
        if source_filter and source not in source_filter:
            continue
        candidates.add((str(indicator), str(country), source, str(ts)))

    if not candidates:
        return 0

    pool = await get_pool()
    deleted = 0
    async with pool.acquire() as conn:
        for indicator, country, source, ts in candidates:
            status = await conn.execute(
                f"""
                DELETE FROM {TABLE}
                WHERE indicator_name = $1
                  AND country = $2
                  AND LOWER(COALESCE(source, '')) = $3
                  AND "timestamp" > $4
                """,
                indicator,
                country,
                source,
                parse_ts(ts),
            )
            try:
                deleted += int(str(status).split()[-1])
            except Exception:
                continue
    return deleted


async def get_existing_indicator(indicator_name: str, country: str, timestamp) -> dict | None:
    """Fetch existing row by (indicator_name, country, timestamp) for 200 response on conflict."""
    pool = await get_pool()
    row = await pool.fetchrow(
        f"SELECT * FROM {TABLE} WHERE indicator_name = $1 AND country = $2 AND timestamp = $3",
        indicator_name,
        country,
        parse_ts(timestamp),
    )
    if row is None:
        return None
    return record_to_dict(row)


async def get_institutional_flows_overview(*, sessions: int = 7) -> dict:
    """Return latest FII/DII flows summary and short combined trend for Overview."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        trend_rows = await conn.fetch(
            f"""
            WITH daily_latest AS (
                SELECT DISTINCT ON (indicator_name, session_date)
                    indicator_name,
                    session_date,
                    value,
                    "timestamp",
                    EXTRACT(HOUR FROM ("timestamp" AT TIME ZONE 'Asia/Kolkata'))::int AS local_hour
                FROM (
                    SELECT indicator_name,
                           value,
                           "timestamp",
                           (("timestamp" AT TIME ZONE 'Asia/Kolkata')::date) AS session_date
                    FROM {TABLE}
                    WHERE country = 'IN'
                      AND indicator_name = ANY($1::text[])
                ) raw
                ORDER BY
                    indicator_name,
                    session_date,
                    CASE
                        WHEN EXTRACT(HOUR FROM ("timestamp" AT TIME ZONE 'Asia/Kolkata')) BETWEEN 9 AND 18
                            THEN 0
                        ELSE 1
                    END,
                    "timestamp" DESC
            ),
            rollup AS (
                SELECT
                    session_date,
                    MAX(CASE WHEN indicator_name = 'fii_net_cash' THEN value END) AS fii_value,
                    MAX(CASE WHEN indicator_name = 'dii_net_cash' THEN value END) AS dii_value,
                    MAX("timestamp") AS as_of,
                    MAX(CASE WHEN local_hour BETWEEN 9 AND 18 THEN 1 ELSE 0 END) AS has_daytime_source
                FROM daily_latest
                GROUP BY session_date
                HAVING
                    -- Exclude synthetic early-morning "today" rows that can appear before NSE publishes the day.
                    session_date < (NOW() AT TIME ZONE 'Asia/Kolkata')::date
                    OR MAX(CASE WHEN local_hour BETWEEN 9 AND 18 THEN 1 ELSE 0 END) = 1
                ORDER BY session_date DESC
                LIMIT $2
            )
            SELECT
                session_date,
                fii_value,
                dii_value,
                COALESCE(fii_value, 0) + COALESCE(dii_value, 0) AS combined_value,
                as_of
            FROM rollup
            ORDER BY session_date ASC
            """,
            ["fii_net_cash", "dii_net_cash"],
            sessions,
        )

    trend: list[dict] = []
    for row in trend_rows:
        trend.append(
            {
                "session_date": row["session_date"],
                "fii_value": float(row["fii_value"]) if row["fii_value"] is not None else None,
                "dii_value": float(row["dii_value"]) if row["dii_value"] is not None else None,
                "combined_value": float(row["combined_value"]),
                "as_of": row["as_of"],
            }
        )

    latest_point = trend[-1] if trend else None
    fii_value = latest_point["fii_value"] if latest_point else None
    dii_value = latest_point["dii_value"] if latest_point else None
    as_of = latest_point["as_of"] if latest_point else None

    combined_value = None
    if fii_value is not None or dii_value is not None:
        combined_value = float(fii_value or 0.0) + float(dii_value or 0.0)

    return {
        "as_of": as_of,
        "fii_value": fii_value,
        "dii_value": dii_value,
        "combined_value": combined_value,
        "trend": trend,
    }


# ── Forecasts (IMF WEO) ──────────────────────────────────────────────

async def upsert_forecasts(rows: list[dict]) -> int:
    """Upsert IMF forecast rows into macro_forecasts table."""
    pool = await get_pool()
    count = 0
    async with pool.acquire() as conn:
        for row in rows:
            await conn.execute(
                """
                INSERT INTO macro_forecasts
                    (indicator_name, country, forecast_year, value, source, fetched_at)
                VALUES ($1, $2, $3, $4, $5, NOW())
                ON CONFLICT (indicator_name, country, forecast_year)
                DO UPDATE SET value = EXCLUDED.value, source = EXCLUDED.source, fetched_at = NOW()
                """,
                row["indicator_name"],
                row["country"],
                row["forecast_year"],
                row["value"],
                row.get("source", "imf_weo"),
            )
            count += 1
    return count


async def get_forecasts(
    country: str | None = None,
    indicator: str | None = None,
) -> list[dict]:
    """Fetch forecasts, optionally filtered by country/indicator."""
    pool = await get_pool()
    conds = []
    args = []
    if country:
        args.append(country)
        conds.append(f"country = ${len(args)}")
    if indicator:
        args.append(indicator)
        conds.append(f"indicator_name = ${len(args)}")
    where = f"WHERE {' AND '.join(conds)}" if conds else ""
    rows = await pool.fetch(
        f"SELECT * FROM macro_forecasts {where} ORDER BY indicator_name, country, forecast_year",
        *args,
    )
    return [record_to_dict(r) for r in rows]


# ── Economic Calendar ──────────────────────────────────────────────────

async def upsert_calendar_events(events: list[dict]) -> int:
    """Upsert economic calendar events."""
    pool = await get_pool()
    count = 0
    async with pool.acquire() as conn:
        for e in events:
            importance = e.get("importance")
            if importance is None:
                importance = "high" if str(e.get("event_type") or "").lower() == "rate_decision" else "medium"
            await conn.execute(
                """
                INSERT INTO economic_calendar
                    (event_name, institution, event_date, country, event_type, description, source, importance, previous, consensus, actual, surprise, status, revised_at)
                VALUES ($1, $2, $3::date, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
                ON CONFLICT (event_name, event_date)
                DO UPDATE SET institution = EXCLUDED.institution,
                             country = EXCLUDED.country,
                             event_type = EXCLUDED.event_type,
                             description = EXCLUDED.description,
                             source = EXCLUDED.source,
                             importance = COALESCE(EXCLUDED.importance, economic_calendar.importance),
                             previous = COALESCE(EXCLUDED.previous, economic_calendar.previous),
                             consensus = COALESCE(EXCLUDED.consensus, economic_calendar.consensus),
                             actual = COALESCE(EXCLUDED.actual, economic_calendar.actual),
                             surprise = COALESCE(EXCLUDED.surprise, economic_calendar.surprise),
                             status = COALESCE(EXCLUDED.status, economic_calendar.status),
                             revised_at = COALESCE(EXCLUDED.revised_at, economic_calendar.revised_at)
                """,
                e["event_name"],
                e["institution"],
                e["event_date"],
                e["country"],
                e["event_type"],
                e.get("description"),
                e.get("source"),
                importance,
                e.get("previous"),
                e.get("consensus"),
                e.get("actual"),
                e.get("surprise"),
                e.get("status") or "scheduled",
                parse_ts(e.get("revised_at")),
            )
            count += 1
    return count


async def get_upcoming_events(
    days_ahead: int = 90,
    country: str | None = None,
    include_past: bool = False,
) -> list[dict]:
    """Fetch calendar events with optional include_past support."""
    pool = await get_pool()
    lower_bound_sql = "event_date >= CURRENT_DATE" if not include_past else "event_date >= CURRENT_DATE - make_interval(days => 30)"
    if country:
        rows = await pool.fetch(
            f"""
            SELECT * FROM economic_calendar
            WHERE {lower_bound_sql}
              AND event_date <= CURRENT_DATE + make_interval(days => $1)
              AND country = $2
            ORDER BY event_date ASC
            """,
            days_ahead, country,
        )
    else:
        rows = await pool.fetch(
            f"""
            SELECT * FROM economic_calendar
            WHERE {lower_bound_sql}
              AND event_date <= CURRENT_DATE + make_interval(days => $1)
            ORDER BY event_date ASC
            """,
            days_ahead,
        )
    today = date.today()
    out: list[dict] = []
    for row in rows:
        item = record_to_dict(row)
        event_date_raw = item.get("event_date")
        try:
            event_date = date.fromisoformat(str(event_date_raw))
        except ValueError:
            event_date = today
        status = str(item.get("status") or "").lower()
        if not status:
            if item.get("revised_at"):
                status = "revised"
            elif item.get("actual") is not None or event_date < today:
                status = "released"
            else:
                status = "scheduled"
        item["status"] = status
        if not item.get("importance"):
            item["importance"] = "high" if str(item.get("event_type") or "").lower() == "rate_decision" else "medium"
        out.append(item)
    return out


async def get_metadata(country: str | None = None) -> list[dict]:
    items = [dict(item) for item in INDICATOR_METADATA]
    normalized_country = (country or "").strip().upper()
    if not normalized_country:
        return items

    indicator_names = [str(item.get("indicator_name") or "") for item in items]
    latest_rows = await _latest_two_rows_by_indicator(
        country=normalized_country,
        indicator_names=indicator_names,
    )

    for item in items:
        indicator = str(item.get("indicator_name") or "")
        points = latest_rows.get(indicator, [])
        latest = points[0] if points else None
        previous = points[1] if len(points) > 1 else None
        item["helper_text"] = _compose_dynamic_helper_text(
            country=normalized_country,
            item=item,
            latest_row=latest,
            previous_row=previous,
        )
    return items


async def get_regime(country: str | None = None) -> dict:
    latest_rows = await get_indicators_latest(country=country)
    countries = sorted({str(r.get("country")) for r in latest_rows if r.get("country")}, key=lambda c: COUNTRY_ORDER.index(c) if c in COUNTRY_ORDER else 99)
    now = datetime.now(UTC)
    items: list[dict] = []
    as_of: datetime | None = None
    for c in countries:
        latest = _latest_by_indicator(latest_rows, c)
        growth = latest.get("gdp_growth", {}).get("value")
        inflation = latest.get("inflation", {}).get("value")
        policy = latest.get("repo_rate", {}).get("value")
        unemployment = latest.get("unemployment", {}).get("value")
        try:
            growth_v = float(growth) if growth is not None else None
        except (TypeError, ValueError):
            growth_v = None
        try:
            inflation_v = float(inflation) if inflation is not None else None
        except (TypeError, ValueError):
            inflation_v = None
        try:
            policy_v = float(policy) if policy is not None else None
        except (TypeError, ValueError):
            policy_v = None
        try:
            unemployment_v = float(unemployment) if unemployment is not None else None
        except (TypeError, ValueError):
            unemployment_v = None

        growth_score = _clamp(None if growth_v is None else (growth_v - 2.5) / 4.0)
        inflation_target = INFLATION_TARGETS.get(c, 2.0)
        inflation_score = _clamp(None if inflation_v is None else (inflation_v - inflation_target) / 3.0)
        neutral_policy = NEUTRAL_POLICY_RATE.get(c, 2.5)
        policy_score = _clamp(None if policy_v is None else (policy_v - neutral_policy) / 2.0)

        timestamps = []
        for k in ("gdp_growth", "inflation", "repo_rate", "unemployment"):
            ts = _as_dt(latest.get(k, {}).get("timestamp"))
            if ts is not None:
                timestamps.append(ts)
        freshness_hours = None
        if timestamps:
            newest = max(timestamps)
            oldest = min(timestamps)
            freshness_hours = round((now - oldest).total_seconds() / 3600.0, 2)
            as_of = newest if as_of is None else max(as_of, newest)

        availability = sum(v is not None for v in (growth_v, inflation_v, policy_v, unemployment_v)) / 4.0
        freshness_factor = 1.0
        if freshness_hours is not None:
            freshness_factor = 1.0 / (1.0 + (freshness_hours / 720.0))
        confidence = max(0.1, min(1.0, availability * freshness_factor))
        regime = _regime_label(growth_score, inflation_score)
        metrics = {}
        if growth_v is not None:
            metrics["gdp_growth"] = round(growth_v, 3)
        if inflation_v is not None:
            metrics["inflation"] = round(inflation_v, 3)
        if policy_v is not None:
            metrics["repo_rate"] = round(policy_v, 3)
        if unemployment_v is not None:
            metrics["unemployment"] = round(unemployment_v, 3)

        items.append(
            {
                "country": c,
                "growth_score": growth_score,
                "inflation_score": inflation_score,
                "policy_score": policy_score,
                "regime_label": regime,
                "confidence": round(confidence, 3),
                "freshness_hours": freshness_hours,
                "metrics": metrics,
            }
        )
    return {"as_of": as_of, "countries": items}


async def get_linkages(
    *,
    country: str = "IN",
    indicator_name: str = "inflation",
    window_days: int = 365,
    assets: list[str] | None = None,
) -> dict:
    pool = await get_pool()
    selected_assets = assets or DEFAULT_LINKAGE_ASSETS.get(country, DEFAULT_LINKAGE_ASSETS["IN"])
    start_ts = datetime.now(UTC) - timedelta(days=max(30, window_days))
    macro_rows = await pool.fetch(
        f"""
        SELECT DISTINCT ON (("timestamp"::date))
            ("timestamp"::date) AS d,
            value
        FROM {TABLE}
        WHERE country = $1
          AND indicator_name = $2
          AND "timestamp" >= $3
        ORDER BY ("timestamp"::date), "timestamp" DESC
        """,
        country,
        indicator_name,
        start_ts,
    )
    macro_series: dict[date, float] = {r["d"]: float(r["value"]) for r in macro_rows}
    if not macro_series:
        return {
            "country": country,
            "indicator_name": indicator_name,
            "window_days": window_days,
            "as_of": None,
            "series": [],
        }

    out_series: list[dict] = []
    latest_as_of: datetime | None = None
    for asset in selected_assets:
        asset_rows = await pool.fetch(
            """
            SELECT DISTINCT ON (("timestamp"::date))
                ("timestamp"::date) AS d,
                price,
                "timestamp"
            FROM market_prices
            WHERE asset = $1
              AND "timestamp" >= $2
            ORDER BY ("timestamp"::date), "timestamp" DESC
            """,
            asset,
            start_ts,
        )
        paired = []
        for row in asset_rows:
            d = row["d"]
            if d not in macro_series:
                continue
            paired.append(
                {
                    "date": d,
                    "macro_value": macro_series[d],
                    "asset_value": float(row["price"]),
                }
            )
            row_ts = _as_dt(row["timestamp"])
            if row_ts is not None:
                latest_as_of = row_ts if latest_as_of is None else max(latest_as_of, row_ts)
        if len(paired) < 8:
            continue
        paired.sort(key=lambda p: p["date"])
        xs = [p["macro_value"] for p in paired]
        ys = [p["asset_value"] for p in paired]
        corr = _pearson(xs, ys)
        out_series.append(
            {
                "asset": asset,
                "correlation": round(corr, 4) if corr is not None else None,
                "point_count": len(paired),
                "points": paired[-120:],
            }
        )

    return {
        "country": country,
        "indicator_name": indicator_name,
        "window_days": window_days,
        "as_of": latest_as_of,
        "series": out_series,
    }


async def get_summary(country: str | None = None) -> dict:
    regime_payload = await get_regime(country=country)
    countries = regime_payload.get("countries", [])
    if not countries:
        return {"as_of": regime_payload.get("as_of"), "countries": []}
    all_events = await get_upcoming_events(days_ahead=180, include_past=False)
    grouped_events: dict[str, list[dict]] = {}
    for event in all_events:
        c = str(event.get("country") or "")
        if not c:
            continue
        grouped_events.setdefault(c, []).append(event)

    out: list[dict] = []
    for item in countries:
        c = str(item.get("country"))
        metrics = item.get("metrics", {})
        growth = float(metrics.get("gdp_growth", 0.0))
        inflation = float(metrics.get("inflation", 0.0))
        policy = float(metrics.get("repo_rate", 0.0))
        infl_gap = max(0.0, inflation - INFLATION_TARGETS.get(c, 2.0))
        policy_gap = max(0.0, policy - NEUTRAL_POLICY_RATE.get(c, 2.5))
        growth_drag = max(0.0, 1.5 - growth)
        freshness_penalty = 0.0
        freshness_h = item.get("freshness_hours")
        if isinstance(freshness_h, (int, float)) and freshness_h > 24 * 45:
            freshness_penalty = 12.0
        risk_score = max(0.0, min(100.0, (infl_gap * 18.0) + (policy_gap * 10.0) + (growth_drag * 16.0) + freshness_penalty))
        watchouts: list[str] = []
        if infl_gap > 1.0:
            watchouts.append("Inflation above target")
        if growth_drag > 0.7:
            watchouts.append("Growth momentum softening")
        if policy_gap > 1.5:
            watchouts.append("Policy stance restrictive")
        if not watchouts:
            watchouts.append("Macro backdrop relatively balanced")

        next_event_name = None
        next_event_date = None
        events_for_country = grouped_events.get(c, [])
        if events_for_country:
            events_for_country.sort(key=lambda e: str(e.get("event_date")))
            next_event_name = events_for_country[0].get("event_name")
            next_event_date = events_for_country[0].get("event_date")

        out.append(
            {
                "country": c,
                "now_title": str(item.get("regime_label") or "Macro Regime"),
                "now_subtitle": f"Growth {growth:.1f}% · Inflation {inflation:.1f}% · Policy {policy:.1f}%",
                "risk_score": round(risk_score, 1),
                "risk_label": _risk_label(risk_score),
                "freshness_hours": item.get("freshness_hours"),
                "next_event_name": next_event_name,
                "next_event_date": next_event_date,
                "watchouts": watchouts,
            }
        )
    return {"as_of": regime_payload.get("as_of"), "countries": out}

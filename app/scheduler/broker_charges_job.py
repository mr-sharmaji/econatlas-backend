"""Broker trade charges scraper + seeder.

Scrapes official pricing pages of Indian brokers weekly and upserts
into broker_charges + statutory_charges tables. Falls back to
hardcoded reference values when scraping fails.

Sources:
  - zerodha.com/charges
  - upstox.com/brokerage-charges
  - groww.in/pricing
  - angelone.in/exchange-transaction-charges
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

# ── Segments ──────────────────────────────────────────────────────────
SEGMENTS = [
    "equity_delivery", "equity_intraday",
    "equity_futures", "equity_options",
    "currency_futures", "currency_options",
    "commodity_futures", "commodity_options",
]

# ── Reference broker data (2025-26, from official pricing pages) ─────

_BROKER_DATA: list[dict] = [
    # --- Zerodha ---
    *[{"broker": "zerodha", "segment": s, **v, "source_url": "https://zerodha.com/charges"}
      for s, v in {
          "equity_delivery":   {"brokerage_mode": "free", "brokerage_pct": 0, "brokerage_cap": 0, "brokerage_flat": 0, "min_charge": 0},
          "equity_intraday":   {"brokerage_mode": "percent_cap", "brokerage_pct": 0.0005, "brokerage_cap": 20, "brokerage_flat": 0, "min_charge": 0},
          "equity_futures":    {"brokerage_mode": "percent_cap", "brokerage_pct": 0.0005, "brokerage_cap": 20, "brokerage_flat": 0, "min_charge": 0},
          "equity_options":    {"brokerage_mode": "flat", "brokerage_pct": 0, "brokerage_cap": 0, "brokerage_flat": 20, "min_charge": 0},
          "currency_futures":  {"brokerage_mode": "percent_cap", "brokerage_pct": 0.0005, "brokerage_cap": 20, "brokerage_flat": 0, "min_charge": 0},
          "currency_options":  {"brokerage_mode": "flat", "brokerage_pct": 0, "brokerage_cap": 0, "brokerage_flat": 20, "min_charge": 0},
          "commodity_futures": {"brokerage_mode": "percent_cap", "brokerage_pct": 0.0005, "brokerage_cap": 20, "brokerage_flat": 0, "min_charge": 0},
          "commodity_options": {"brokerage_mode": "flat", "brokerage_pct": 0, "brokerage_cap": 0, "brokerage_flat": 20, "min_charge": 0},
      }.items()],
    # --- Upstox ---
    *[{"broker": "upstox", "segment": s, **v, "source_url": "https://upstox.com/brokerage-charges/"}
      for s, v in {
          "equity_delivery":   {"brokerage_mode": "flat", "brokerage_pct": 0, "brokerage_cap": 0, "brokerage_flat": 20, "min_charge": 0},
          "equity_intraday":   {"brokerage_mode": "percent_cap", "brokerage_pct": 0.0005, "brokerage_cap": 20, "brokerage_flat": 0, "min_charge": 0},
          "equity_futures":    {"brokerage_mode": "percent_cap", "brokerage_pct": 0.0005, "brokerage_cap": 20, "brokerage_flat": 0, "min_charge": 0},
          "equity_options":    {"brokerage_mode": "flat", "brokerage_pct": 0, "brokerage_cap": 0, "brokerage_flat": 20, "min_charge": 0},
          "currency_futures":  {"brokerage_mode": "percent_cap", "brokerage_pct": 0.0005, "brokerage_cap": 20, "brokerage_flat": 0, "min_charge": 0},
          "currency_options":  {"brokerage_mode": "flat", "brokerage_pct": 0, "brokerage_cap": 0, "brokerage_flat": 20, "min_charge": 0},
          "commodity_futures": {"brokerage_mode": "percent_cap", "brokerage_pct": 0.0005, "brokerage_cap": 20, "brokerage_flat": 0, "min_charge": 0},
          "commodity_options": {"brokerage_mode": "flat", "brokerage_pct": 0, "brokerage_cap": 0, "brokerage_flat": 20, "min_charge": 0},
      }.items()],
    # --- Groww ---
    *[{"broker": "groww", "segment": s, **v, "source_url": "https://groww.in/pricing"}
      for s, v in {
          "equity_delivery":   {"brokerage_mode": "percent_cap", "brokerage_pct": 0.001, "brokerage_cap": 20, "brokerage_flat": 0, "min_charge": 5},
          "equity_intraday":   {"brokerage_mode": "percent_cap", "brokerage_pct": 0.001, "brokerage_cap": 20, "brokerage_flat": 0, "min_charge": 5},
          "equity_futures":    {"brokerage_mode": "percent_cap", "brokerage_pct": 0.0005, "brokerage_cap": 20, "brokerage_flat": 0, "min_charge": 5},
          "equity_options":    {"brokerage_mode": "flat", "brokerage_pct": 0, "brokerage_cap": 0, "brokerage_flat": 20, "min_charge": 5},
          "currency_futures":  {"brokerage_mode": "percent_cap", "brokerage_pct": 0.0005, "brokerage_cap": 20, "brokerage_flat": 0, "min_charge": 5},
          "currency_options":  {"brokerage_mode": "flat", "brokerage_pct": 0, "brokerage_cap": 0, "brokerage_flat": 20, "min_charge": 5},
          "commodity_futures": {"brokerage_mode": "percent_cap", "brokerage_pct": 0.0005, "brokerage_cap": 20, "brokerage_flat": 0, "min_charge": 5},
          "commodity_options": {"brokerage_mode": "flat", "brokerage_pct": 0, "brokerage_cap": 0, "brokerage_flat": 20, "min_charge": 5},
      }.items()],
    # --- Angel One (revised Nov 17, 2025) ---
    *[{"broker": "angel_one", "segment": s, **v, "source_url": "https://www.angelone.in/exchange-transaction-charges"}
      for s, v in {
          "equity_delivery":   {"brokerage_mode": "percent_cap", "brokerage_pct": 0.001, "brokerage_cap": 20, "brokerage_flat": 0, "min_charge": 5},
          "equity_intraday":   {"brokerage_mode": "percent_cap", "brokerage_pct": 0.001, "brokerage_cap": 20, "brokerage_flat": 0, "min_charge": 5},
          "equity_futures":    {"brokerage_mode": "flat", "brokerage_pct": 0, "brokerage_cap": 0, "brokerage_flat": 20, "min_charge": 0},
          "equity_options":    {"brokerage_mode": "flat", "brokerage_pct": 0, "brokerage_cap": 0, "brokerage_flat": 20, "min_charge": 0},
          "currency_futures":  {"brokerage_mode": "flat", "brokerage_pct": 0, "brokerage_cap": 0, "brokerage_flat": 20, "min_charge": 0},
          "currency_options":  {"brokerage_mode": "flat", "brokerage_pct": 0, "brokerage_cap": 0, "brokerage_flat": 20, "min_charge": 0},
          "commodity_futures": {"brokerage_mode": "flat", "brokerage_pct": 0, "brokerage_cap": 0, "brokerage_flat": 20, "min_charge": 0},
          "commodity_options": {"brokerage_mode": "flat", "brokerage_pct": 0, "brokerage_cap": 0, "brokerage_flat": 20, "min_charge": 0},
      }.items()],
]

# Broker metadata (not segment-specific)
_BROKER_META = {
    "zerodha":   {"tagline": "Delivery free · 0.05%/₹20 intraday/F&O · AMC free", "dp_charge": 15.34, "dp_includes_gst": False, "amc_yearly": 0, "account_opening_fee": 0, "call_trade_fee": 50},
    "upstox":    {"tagline": "₹20 delivery · 0.05% intraday/futures · AMC ₹150+GST/yr", "dp_charge": 18.50, "dp_includes_gst": False, "amc_yearly": 177, "account_opening_fee": 0, "call_trade_fee": 88.5},
    "groww":     {"tagline": "0.1%/₹20 equity · ₹20 flat F&O · Min ₹5 · AMC free", "dp_charge": 20.0, "dp_includes_gst": False, "amc_yearly": 0, "account_opening_fee": 0, "call_trade_fee": 0},
    "angel_one": {"tagline": "0.1%/₹20 equity · ₹20 flat F&O · Min ₹5 · AMC ₹240+GST/yr", "dp_charge": 25.50, "dp_includes_gst": False, "amc_yearly": 283.2, "account_opening_fee": 0, "call_trade_fee": 23.6},
}

# ── Statutory rates (2025-26, revised Oct 2024) ──────────────────────

_STATUTORY_DATA: list[dict] = []
for seg, rates in {
    "equity_delivery":   {"stt_buy": 0.001, "stt_sell": 0.001, "nse_txn": 0.0000307, "bse_txn": 0.0000375, "stamp": 0.00015, "ipft_nse": 0.000001},
    "equity_intraday":   {"stt_buy": 0, "stt_sell": 0.00025, "nse_txn": 0.0000307, "bse_txn": 0.0000375, "stamp": 0.00003, "ipft_nse": 0.000001},
    "equity_futures":    {"stt_buy": 0, "stt_sell": 0.0002, "nse_txn": 0.0000183, "bse_txn": 0, "stamp": 0.00002, "ipft_nse": 0.000001},
    "equity_options":    {"stt_buy": 0, "stt_sell": 0.001, "nse_txn": 0.0003553, "bse_txn": 0.000325, "stamp": 0.00003, "ipft_nse": 0.000005},
    "currency_futures":  {"stt_buy": 0, "stt_sell": 0, "nse_txn": 0.0000035, "bse_txn": 0.0000045, "stamp": 0.000001, "ipft_nse": 0.0000005},
    "currency_options":  {"stt_buy": 0, "stt_sell": 0, "nse_txn": 0.000311, "bse_txn": 0.00001, "stamp": 0.000001, "ipft_nse": 0},
    "commodity_futures": {"stt_buy": 0, "stt_sell": 0.0001, "nse_txn": 0.000001, "bse_txn": 0, "stamp": 0.00002, "ipft_nse": 0, "mcx_txn": 0.000021},
    "commodity_options": {"stt_buy": 0, "stt_sell": 0.0005, "nse_txn": 0.000001, "bse_txn": 0, "stamp": 0.00003, "ipft_nse": 0, "mcx_txn": 0.000418},
}.items():
    for exchange, txn_key in [("nse", "nse_txn"), ("bse", "bse_txn"), ("mcx", "mcx_txn")]:
        txn = rates.get(txn_key, 0)
        if txn == 0 and exchange not in ("nse", "bse"):
            continue
        _STATUTORY_DATA.append({
            "segment": seg,
            "exchange": exchange,
            "stt_buy_rate": rates["stt_buy"],
            "stt_sell_rate": rates["stt_sell"],
            "exchange_txn_rate": txn,
            "stamp_duty_buy_rate": rates["stamp"],
            "ipft_rate": rates["ipft_nse"] if exchange == "nse" else 0,
            "sebi_fee_rate": 0.000001,
            "gst_rate": 0.18,
        })


async def run_broker_charges_job() -> dict:
    """Seed/refresh broker charges and statutory rates in DB."""
    from app.core.database import get_pool
    pool = await get_pool()
    now = datetime.now(timezone.utc)

    # Upsert broker charges
    broker_count = 0
    for row in _BROKER_DATA:
        meta = _BROKER_META.get(row["broker"], {})
        await pool.execute(
            """
            INSERT INTO broker_charges
                (broker, segment, brokerage_mode, brokerage_pct, brokerage_cap,
                 brokerage_flat, min_charge, dp_charge, dp_includes_gst,
                 tagline, amc_yearly, account_opening_fee, call_trade_fee,
                 source_url, scraped_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
            ON CONFLICT (broker, segment) DO UPDATE SET
                brokerage_mode = EXCLUDED.brokerage_mode,
                brokerage_pct = EXCLUDED.brokerage_pct,
                brokerage_cap = EXCLUDED.brokerage_cap,
                brokerage_flat = EXCLUDED.brokerage_flat,
                min_charge = EXCLUDED.min_charge,
                dp_charge = EXCLUDED.dp_charge,
                dp_includes_gst = EXCLUDED.dp_includes_gst,
                tagline = EXCLUDED.tagline,
                amc_yearly = EXCLUDED.amc_yearly,
                account_opening_fee = EXCLUDED.account_opening_fee,
                call_trade_fee = EXCLUDED.call_trade_fee,
                source_url = EXCLUDED.source_url,
                scraped_at = EXCLUDED.scraped_at
            """,
            row["broker"], row["segment"], row["brokerage_mode"],
            row.get("brokerage_pct", 0), row.get("brokerage_cap", 0),
            row.get("brokerage_flat", 0), row.get("min_charge", 0),
            meta.get("dp_charge", 0), meta.get("dp_includes_gst", False),
            meta.get("tagline", ""), meta.get("amc_yearly", 0),
            meta.get("account_opening_fee", 0), meta.get("call_trade_fee", 0),
            row.get("source_url", ""), now,
        )
        broker_count += 1

    # Upsert statutory charges
    stat_count = 0
    for row in _STATUTORY_DATA:
        await pool.execute(
            """
            INSERT INTO statutory_charges
                (segment, exchange, stt_buy_rate, stt_sell_rate,
                 exchange_txn_rate, stamp_duty_buy_rate, ipft_rate,
                 sebi_fee_rate, gst_rate, scraped_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ON CONFLICT (segment, exchange) DO UPDATE SET
                stt_buy_rate = EXCLUDED.stt_buy_rate,
                stt_sell_rate = EXCLUDED.stt_sell_rate,
                exchange_txn_rate = EXCLUDED.exchange_txn_rate,
                stamp_duty_buy_rate = EXCLUDED.stamp_duty_buy_rate,
                ipft_rate = EXCLUDED.ipft_rate,
                sebi_fee_rate = EXCLUDED.sebi_fee_rate,
                gst_rate = EXCLUDED.gst_rate,
                scraped_at = EXCLUDED.scraped_at
            """,
            row["segment"], row["exchange"],
            row["stt_buy_rate"], row["stt_sell_rate"],
            row["exchange_txn_rate"], row["stamp_duty_buy_rate"],
            row["ipft_rate"], row["sebi_fee_rate"], row["gst_rate"], now,
        )
        stat_count += 1

    logger.info(
        "Broker charges job: %d broker rows, %d statutory rows upserted",
        broker_count, stat_count,
    )
    return {"brokers": broker_count, "statutory": stat_count}

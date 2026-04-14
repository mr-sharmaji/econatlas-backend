"""Service layer for broker trade charges."""
from __future__ import annotations

import logging
from datetime import datetime, timezone

from app.core.database import get_pool, record_to_dict

logger = logging.getLogger(__name__)


# ── AMC details ──────────────────────────────────────────────────────
#
# Every Indian broker has TWO separate AMC models (BSDA vs non-BSDA)
# and the numbers can't be compressed into a single `amc_yearly` value
# — e.g. a Zerodha BSDA user with <₹4L holdings pays ₹0/yr while a
# non-BSDA user pays ₹354/yr for the exact same account. The DB column
# stores the non-BSDA (standard) figure as a representative number;
# these notes are the full rules, served alongside it, and rendered on
# the client as multi-line detail text.
#
# Sources (verified against live HTML Apr 2026):
#   - zerodha.com/charges   "AMC" section
#   - upstox.com/brokerage-charges  "AMC" card
#   - groww.in/pricing       "Account Maintenance Charges ₹0"
#   - angelone.in/exchange-transaction-charges  AMC card
_BROKER_AMC_NOTES: dict[str, dict] = {
    "zerodha": {
        "headline": "₹0 – ₹354/yr depending on account type",
        "rules": [
            "BSDA: free for holdings up to ₹4 lakh",
            "BSDA: ₹100/yr for ₹4L–₹10L (charged quarterly)",
            "Non-BSDA (or >₹10L holdings): ₹300/yr + 18% GST = ₹354/yr",
        ],
    },
    "upstox": {
        "headline": "Free year 1, then ₹354/yr non-BSDA",
        "rules": [
            "Free for new users onboarded from 14-Feb-2025 for the first year",
            "Non-BSDA: ₹300/yr + 18% GST = ₹354/yr after year 1",
            "BSDA: tiered by holdings (see upstox.com)",
        ],
    },
    "groww": {
        "headline": "Zero maintenance charges",
        "rules": [
            "Groww advertises ₹0 AMC across all account tiers",
        ],
    },
    "angel_one": {
        "headline": "Free year 1, then ₹450/yr or ₹2,950 lifetime",
        "rules": [
            "Free for the first year",
            "Non-BSDA: ₹450/yr OR ₹2,950 one-time lifetime charge",
            "BSDA: ₹60 + GST per quarter, charged only after first trade",
        ],
    },
}


async def get_all_broker_charges() -> dict:
    """Return all broker presets + statutory rates from DB.

    Response shape:
    {
        "brokers": {
            "zerodha": {
                "tagline": "...",
                "dp_charge": 15.34,
                "amc_yearly": 0,
                "segments": {
                    "equity_delivery": {"mode": "free", ...},
                    ...
                }
            },
            ...
        },
        "statutory": {
            "equity_delivery": {
                "nse": {"stt_buy_rate": ..., ...},
                "bse": {...},
            },
            ...
        },
        "last_updated": "2026-04-14T..."
    }
    """
    pool = await get_pool()

    # Broker charges
    broker_rows = await pool.fetch(
        "SELECT * FROM broker_charges ORDER BY broker, segment"
    )
    brokers: dict = {}
    for r in broker_rows:
        row = record_to_dict(r)
        broker = row["broker"]
        segment = row["segment"]
        if broker not in brokers:
            amc_note = _BROKER_AMC_NOTES.get(broker, {})
            brokers[broker] = {
                "name": broker.replace("_", " ").title(),
                "tagline": row.get("tagline", ""),
                "dp_charge": row.get("dp_charge", 0),
                "dp_includes_gst": row.get("dp_includes_gst", False),
                "amc_yearly": row.get("amc_yearly", 0),
                "amc_note": amc_note.get("headline", ""),
                "amc_rules": amc_note.get("rules", []),
                "account_opening_fee": row.get("account_opening_fee", 0),
                "call_trade_fee": row.get("call_trade_fee", 0),
                "segments": {},
            }
        brokers[broker]["segments"][segment] = {
            "mode": row.get("brokerage_mode", "flat"),
            "pct": row.get("brokerage_pct", 0),
            "cap": row.get("brokerage_cap", 0),
            "flat": row.get("brokerage_flat", 0),
            "min_charge": row.get("min_charge", 0),
        }

    # Statutory charges
    stat_rows = await pool.fetch(
        "SELECT * FROM statutory_charges ORDER BY segment, exchange"
    )
    statutory: dict = {}
    for r in stat_rows:
        row = record_to_dict(r)
        segment = row["segment"]
        exchange = row["exchange"]
        if segment not in statutory:
            statutory[segment] = {}
        statutory[segment][exchange] = {
            "stt_buy_rate": row.get("stt_buy_rate", 0),
            "stt_sell_rate": row.get("stt_sell_rate", 0),
            "exchange_txn_rate": row.get("exchange_txn_rate", 0),
            "stamp_duty_buy_rate": row.get("stamp_duty_buy_rate", 0),
            "ipft_rate": row.get("ipft_rate", 0),
            "sebi_fee_rate": row.get("sebi_fee_rate", 0.000001),
            "gst_rate": row.get("gst_rate", 0.18),
        }

    # Last updated
    last_updated = None
    if broker_rows:
        timestamps = [r["scraped_at"] for r in broker_rows if r.get("scraped_at")]
        if timestamps:
            last_updated = max(timestamps).isoformat()

    return {
        "brokers": brokers,
        "statutory": statutory,
        "last_updated": last_updated,
    }

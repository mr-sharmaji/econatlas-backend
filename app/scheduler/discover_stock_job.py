from __future__ import annotations

import asyncio
import csv
import io
import json
import logging
import math
import re
import statistics
import time as time_mod
import zipfile
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from html import unescape
from zoneinfo import ZoneInfo

import requests

from app.core.config import get_settings
from app.scheduler.base import BaseScraper
from app.scheduler.brief_job import INDIA_STOCKS
from app.scheduler.job_executors import get_job_executor
from app.services import discover_service

logger = logging.getLogger(__name__)

NSE_HOME_URL = "https://www.nseindia.com"
NSE_QUOTE_URL = "https://www.nseindia.com/api/quote-equity"
NSE_EQUITY_MASTER_URL = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"
NSE_BHAVCOPY_URL_TMPL = "https://archives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_{yyyymmdd}_F_0000.csv.zip"
NSE_STOCK_SERIES = {"EQ", "BE", "BZ"}
IST = ZoneInfo("Asia/Kolkata")
YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"


# ---------------------------------------------------------------------------
# Screener.in Broad Sector → clean sector name mapping
# ---------------------------------------------------------------------------
_SCREENER_BROAD_SECTOR_MAP: dict[str, str] = {
    "energy": "Energy",
    "information technology": "IT",
    "financial services": "Financials",
    "fast moving consumer goods": "FMCG",
    "consumer discretionary": "Consumer Discretionary",
    "consumer staples": "FMCG",
    "healthcare": "Healthcare",
    "pharmaceuticals": "Healthcare",
    "industrials": "Industrials",
    "capital goods": "Industrials",
    "construction": "Industrials",
    "construction materials": "Industrials",
    "materials": "Materials",
    "chemicals": "Chemicals",
    "metals & mining": "Materials",
    "telecommunication": "Telecom",
    "real estate": "Real Estate",
    "media entertainment & publication": "Media & Entertainment",
    "media": "Media & Entertainment",
    "services": "Services",
    "utilities": "Utilities",
    "automobile and auto components": "Auto",
    "automobile": "Auto",
    "diversified": "Diversified",
    "textiles": "Textiles",
    "forest materials": "Materials",
    "consumer services": "Services",
    "oil gas & consumable fuels": "Energy",
    "power": "Energy",
    "commodities": "Commodities",
    "realty": "Real Estate",
}

# Legacy keyword-based fallback for sector classification
_SECTOR_MAPPING: dict[str, str] = {
    # Energy
    "oil": "Energy", "gas": "Energy", "petroleum": "Energy", "crude": "Energy",
    "energy": "Energy", "power": "Energy", "renewable": "Energy",
    "electric utilities": "Utilities", "utilities": "Utilities",
    # IT
    "information technology": "IT", "software": "IT", "it ": "IT",
    "computer": "IT", "digital": "IT",
    # Financials
    "bank": "Financials", "finance": "Financials", "insurance": "Financials",
    "nbfc": "Financials", "financial": "Financials", "credit": "Financials",
    # Healthcare
    "pharma": "Healthcare", "healthcare": "Healthcare", "hospital": "Healthcare",
    "drug": "Healthcare", "medical": "Healthcare", "biotech": "Healthcare",
    # Consumer
    "fmcg": "FMCG", "consumer": "Consumer Discretionary", "retail": "Consumer Discretionary",
    "food": "FMCG", "beverage": "FMCG", "textile": "Textiles",
    "apparel": "Consumer Discretionary", "personal care": "FMCG",
    # Auto
    "auto": "Auto", "automobile": "Auto", "vehicle": "Auto",
    "tyre": "Auto", "tire": "Auto",
    # Industrials
    "capital goods": "Industrials", "industrial": "Industrials",
    "engineering": "Industrials", "construction": "Industrials",
    "infrastructure": "Industrials", "cement": "Industrials",
    "defence": "Industrials", "defense": "Industrials",
    # Materials / Chemicals
    "metals": "Materials", "steel": "Materials", "aluminium": "Materials",
    "mining": "Materials", "chemicals": "Chemicals", "paper": "Materials",
    "fertilizer": "Chemicals", "plastic": "Chemicals",
    # Telecom
    "telecom": "Telecom", "communication": "Telecom",
    # Real Estate
    "real estate": "Real Estate", "realty": "Real Estate", "housing": "Real Estate",
    # Media
    "media": "Media & Entertainment", "entertainment": "Media & Entertainment",
}

# Expanded curated sector mapping for common stocks not in INDIA_STOCKS
_EXTRA_SECTOR_MAP: dict[str, str] = {
    "ATGL": "Energy", "NTPCGREEN": "Energy", "JSWENERGY": "Energy",
    "ADANIENERGY": "Energy", "CESC": "Energy", "NHPC": "Energy",
    "TORNTPOWER": "Utilities", "SJVN": "Energy",
    "DOMS": "Consumer Discretionary", "JINDALSAW": "Materials", "JINDALSTEL": "Materials",
    "TATASTEEL": "Materials", "HINDALCO": "Materials", "VEDL": "Materials",
    "NMDC": "Materials", "COALINDIA": "Energy",
    "DABUR": "FMCG", "GODREJCP": "FMCG", "MARICO": "FMCG",
    "PIDILITIND": "FMCG", "COLPAL": "FMCG", "BRITANNIA": "FMCG",
    "PAGEIND": "Consumer Discretionary", "VBL": "FMCG", "TRENT": "Consumer Discretionary",
    "IRCTC": "Consumer Discretionary", "ZOMATO": "Consumer Discretionary", "NYKAA": "Consumer Discretionary",
    "DMART": "Consumer Discretionary", "TITAN": "Consumer Discretionary",
    "SBICARD": "Financials", "CHOLAFIN": "Financials", "MUTHOOTFIN": "Financials",
    "MANAPPURAM": "Financials", "PEL": "Financials", "CANFINHOME": "Financials",
    "ICICIGI": "Financials", "SBILIFE": "Financials", "HDFCLIFE": "Financials",
    "MAXHEALTH": "Healthcare", "FORTIS": "Healthcare", "LALPATHLAB": "Healthcare",
    "METROPOLIS": "Healthcare", "AUROPHARMA": "Healthcare", "ALKEM": "Healthcare",
    "LAURUSLABS": "Healthcare", "GLENMARK": "Healthcare", "IPCALAB": "Healthcare",
    "MPHASIS": "IT", "COFORGE": "IT", "LTTS": "IT", "PERSISTENT": "IT",
    "MFSL": "Financials", "NAUKRI": "IT", "PAYTM": "IT",
    "MOTHERSON": "Auto", "BALKRISIND": "Auto", "ASHOKLEY": "Auto",
    "ESCORTS": "Auto", "TVSMTR": "Auto", "TIINDIA": "Auto",
    "GODREJPROP": "Real Estate", "DLF": "Real Estate", "OBEROIRLTY": "Real Estate",
    "PRESTIGE": "Real Estate", "PHOENIXLTD": "Real Estate",
    "INDUSTOWER": "Telecom", "TATACOMM": "Telecom",
    "ABB": "Industrials", "SIEMENS": "Industrials", "HAVELLS": "Industrials",
    "POLYCAB": "Industrials", "VOLTAS": "Industrials", "CGPOWER": "Industrials",
    "BEL": "Industrials", "HAL": "Industrials", "CONCOR": "Industrials",
    "IRFC": "Financials", "PFC": "Financials", "RECLTD": "Financials",
    "ULTRACEMCO": "Industrials", "AMBUJACEM": "Industrials", "SHREECEM": "Industrials",
    "DELHIVERY": "Industrials", "PIIND": "Chemicals", "AARTI": "Chemicals",
    "DEEPAKNTR": "Chemicals", "UPL": "Chemicals", "SRF": "Chemicals",
    "PVRINOX": "Media & Entertainment", "SUNTV": "Media & Entertainment", "ZEEL": "Media & Entertainment",
    # Additional sector mappings to reduce "Other"
    "JUBLFOOD": "Consumer Discretionary", "MCDOWELL": "FMCG", "UBL": "FMCG",
    "TATACONSUM": "FMCG", "EMAMILTD": "FMCG", "JYOTHYLAB": "FMCG",
    "RADICO": "FMCG", "BATAINDIA": "Consumer Discretionary", "RELAXO": "Consumer Discretionary",
    "WHIRLPOOL": "Consumer Discretionary", "BLUESTARLT": "Consumer Discretionary", "CROMPTON": "Consumer Discretionary",
    "KAJARIACER": "Materials", "CENTURYTEX": "Textiles", "GRASIM": "Materials",
    "FLUOROCHEM": "Chemicals", "CLEAN": "Chemicals", "NAVINFLUOR": "Chemicals",
    "SUMICHEM": "Chemicals", "BASF": "Chemicals", "TATACHEM": "Chemicals",
    "FINEORG": "Chemicals", "ALKYLAMINE": "Chemicals",
    "HINDPETRO": "Energy", "BPCL": "Energy", "IOC": "Energy",
    "GAIL": "Energy", "PETRONET": "Energy", "ONGC": "Energy",
    "ADANIGREEN": "Energy", "TATAPOWER": "Energy", "ADANIPOWER": "Energy",
    "KPITTECH": "IT", "ZENSAR": "IT", "BIRLASOFT": "IT",
    "TATAELXSI": "IT", "INTELLECT": "IT", "HAPPSTMNDS": "IT",
    "ROUTE": "IT", "MASTEK": "IT", "CYIENT": "IT",
    "ICICIPRULI": "Financials", "BAJFINANCE": "Financials", "BAJAJFINSV": "Financials",
    "LICHSGFIN": "Financials", "M&MFIN": "Financials", "SHRIRAMFIN": "Financials",
    "SUNDARMFIN": "Financials", "CANARAHSBK": "Financials", "FEDERALBNK": "Financials",
    "BANDHANBNK": "Financials", "RBLBANK": "Financials", "IDFC": "Financials",
    "IDFCFIRSTB": "Financials", "INDUSINDBK": "Financials",
    "APOLLOHOSP": "Healthcare", "SYNGENE": "Healthcare", "BIOCON": "Healthcare",
    "NATCOPHARMA": "Healthcare", "TORNTPHARM": "Healthcare",
    "APLLTD": "Healthcare", "GRANULES": "Healthcare",
    "EXIDEIND": "Auto", "AMARAJABAT": "Auto", "SONACOMS": "Auto",
    "SAMVARDHNA": "Auto", "ENDURANCE": "Auto", "BHARATFORG": "Industrials",
    "CUMMINSIND": "Industrials", "THERMAX": "Industrials", "LTIM": "IT",
    "KAYNES": "Industrials", "AFFLE": "IT", "MAPMY": "IT",
    "RVNL": "Industrials", "IRCON": "Industrials", "NCC": "Industrials",
    "NBCC": "Industrials", "KECINTL": "Industrials", "KALPATPOWR": "Industrials",
    "AIAENG": "Industrials", "GRINFRA": "Industrials",
    "BSE": "Financials", "MCX": "Financials",
    "CDSL": "Financials", "CAMS": "Financials", "KFIN": "Financials",
    "BRIGADE": "Real Estate", "SOBHA": "Real Estate", "MAHLIFE": "Real Estate",
    "LODHA": "Real Estate", "RAYMOND": "Consumer Discretionary",
    "TTML": "Telecom", "VODAFONE": "Telecom",
    "GPPL": "Utilities", "POWERGRID": "Utilities",
    "IEX": "Utilities",
}


def _map_screener_sector(raw: str) -> str:
    """Map a raw Screener.in sector/industry string to a broad sector category."""
    lowered = raw.strip().lower()
    for keyword, sector in _SECTOR_MAPPING.items():
        if keyword in lowered:
            return sector
    return raw.strip().title()  # Use the raw value title-cased as fallback


# ── 6-Layer Scoring Model: Sector Weight Profiles ──
_SECTOR_LAYER_WEIGHTS: dict[str, dict[str, float]] = {
    "DEFAULT":                {"quality": 0.30, "valuation": 0.25, "growth": 0.20, "momentum": 0.10, "institutional": 0.10, "risk": 0.05},
    "Financials":             {"quality": 0.35, "valuation": 0.20, "growth": 0.15, "momentum": 0.10, "institutional": 0.15, "risk": 0.05},
    "IT":                     {"quality": 0.35, "valuation": 0.20, "growth": 0.25, "momentum": 0.10, "institutional": 0.05, "risk": 0.05},
    "Healthcare":             {"quality": 0.25, "valuation": 0.20, "growth": 0.25, "momentum": 0.10, "institutional": 0.10, "risk": 0.10},
    "Real Estate":            {"quality": 0.20, "valuation": 0.35, "growth": 0.15, "momentum": 0.15, "institutional": 0.10, "risk": 0.05},
    "Industrials":            {"quality": 0.25, "valuation": 0.20, "growth": 0.30, "momentum": 0.10, "institutional": 0.10, "risk": 0.05},
    "FMCG":                   {"quality": 0.35, "valuation": 0.25, "growth": 0.15, "momentum": 0.05, "institutional": 0.10, "risk": 0.10},
    "Auto":                   {"quality": 0.25, "valuation": 0.25, "growth": 0.20, "momentum": 0.15, "institutional": 0.10, "risk": 0.05},
    "Utilities":              {"quality": 0.30, "valuation": 0.30, "growth": 0.10, "momentum": 0.05, "institutional": 0.15, "risk": 0.10},
    "Energy":                 {"quality": 0.20, "valuation": 0.30, "growth": 0.15, "momentum": 0.20, "institutional": 0.10, "risk": 0.05},
    "Materials":              {"quality": 0.20, "valuation": 0.30, "growth": 0.15, "momentum": 0.20, "institutional": 0.10, "risk": 0.05},
    "Chemicals":              {"quality": 0.25, "valuation": 0.25, "growth": 0.25, "momentum": 0.10, "institutional": 0.10, "risk": 0.05},
    "Telecom":                {"quality": 0.25, "valuation": 0.20, "growth": 0.25, "momentum": 0.10, "institutional": 0.10, "risk": 0.10},
    "Consumer Discretionary": {"quality": 0.30, "valuation": 0.25, "growth": 0.20, "momentum": 0.10, "institutional": 0.10, "risk": 0.05},
    "Textiles":               {"quality": 0.25, "valuation": 0.25, "growth": 0.20, "momentum": 0.15, "institutional": 0.10, "risk": 0.05},
    "Services":               {"quality": 0.30, "valuation": 0.25, "growth": 0.20, "momentum": 0.10, "institutional": 0.10, "risk": 0.05},
    "Media & Entertainment":  {"quality": 0.25, "valuation": 0.25, "growth": 0.25, "momentum": 0.10, "institutional": 0.10, "risk": 0.05},
    "Diversified":            {"quality": 0.30, "valuation": 0.25, "growth": 0.20, "momentum": 0.10, "institutional": 0.10, "risk": 0.05},
    "Commodities":            {"quality": 0.20, "valuation": 0.25, "growth": 0.15, "momentum": 0.25, "institutional": 0.10, "risk": 0.05},
}

# Sub-metric weights within Quality layer, per sector
_SECTOR_QUALITY_WEIGHTS: dict[str, dict[str, float]] = {
    "DEFAULT":      {"roe": 0.25, "roce": 0.20, "op_margin": 0.15, "fcf_yield": 0.15, "ocf_consistency": 0.10, "margin_stability": 0.10, "net_cash": 0.05},
    "Financials":   {"roe": 0.35, "nim_proxy": 0.25, "profit_consistency": 0.20, "interest_to_rev": 0.10, "accrual_quality": 0.10},
    "IT":           {"op_margin": 0.30, "margin_stability": 0.25, "fcf_yield": 0.20, "net_cash": 0.15, "profit_consistency": 0.10},
    "Healthcare":   {"roce": 0.20, "op_margin": 0.20, "cwip_to_assets": 0.20, "fcf_yield": 0.15, "profit_consistency": 0.15, "margin_stability": 0.10},
    "Real Estate":  {"roe": 0.25, "net_cash": 0.25, "profit_consistency": 0.20, "op_margin": 0.15, "fcf_yield": 0.15},
    "Industrials":  {"roce": 0.25, "op_margin": 0.20, "cwip_to_assets": 0.20, "fcf_yield": 0.15, "profit_consistency": 0.10, "margin_stability": 0.10},
    "FMCG":         {"margin_stability": 0.25, "gross_margin": 0.20, "profit_consistency": 0.20, "roe": 0.15, "fcf_yield": 0.10, "ocf_consistency": 0.10},
    "Auto":         {"roce": 0.25, "op_margin": 0.20, "cwip_to_assets": 0.15, "margin_stability": 0.15, "fcf_yield": 0.15, "profit_consistency": 0.10},
    "Utilities":    {"roe": 0.25, "profit_consistency": 0.25, "ocf_consistency": 0.20, "op_margin": 0.15, "fcf_yield": 0.15},
    "Energy":       {"roce": 0.25, "op_margin": 0.20, "fcf_yield": 0.20, "margin_stability": 0.15, "net_cash": 0.10, "ocf_consistency": 0.10},
    "Materials":    {"roce": 0.25, "op_margin": 0.20, "fcf_yield": 0.20, "margin_stability": 0.15, "net_cash": 0.10, "ocf_consistency": 0.10},
    "Chemicals":    {"roce": 0.25, "op_margin": 0.20, "cwip_to_assets": 0.15, "fcf_yield": 0.15, "margin_stability": 0.15, "profit_consistency": 0.10},
    "Consumer Discretionary": {"roe": 0.20, "roce": 0.20, "op_margin": 0.20, "fcf_yield": 0.15, "margin_stability": 0.15, "profit_consistency": 0.10},
    "Telecom":      {"roce": 0.25, "op_margin": 0.25, "fcf_yield": 0.20, "profit_consistency": 0.15, "net_cash": 0.15},
    "Textiles":     {"roce": 0.25, "op_margin": 0.25, "margin_stability": 0.20, "fcf_yield": 0.15, "profit_consistency": 0.15},
    "Services":     {"roe": 0.25, "op_margin": 0.25, "fcf_yield": 0.20, "profit_consistency": 0.15, "ocf_consistency": 0.15},
    "Media & Entertainment": {"roe": 0.20, "op_margin": 0.25, "fcf_yield": 0.20, "profit_consistency": 0.20, "margin_stability": 0.15},
    "Diversified":  {"roe": 0.25, "roce": 0.20, "op_margin": 0.15, "fcf_yield": 0.15, "profit_consistency": 0.15, "net_cash": 0.10},
    "Commodities":  {"roce": 0.25, "op_margin": 0.25, "fcf_yield": 0.20, "margin_stability": 0.15, "net_cash": 0.15},
}

# Sub-metric weights within Valuation layer, per sector
_SECTOR_VALUATION_WEIGHTS: dict[str, dict[str, float]] = {
    "DEFAULT":      {"peg": 0.35, "pe_relative": 0.25, "pb_relative": 0.20, "forward_pe": 0.10, "div_yield": 0.10},
    "Financials":   {"peg": 0.25, "pb_relative": 0.35, "pe_relative": 0.20, "div_yield": 0.20},
    "IT":           {"peg": 0.40, "pe_relative": 0.25, "forward_pe": 0.20, "div_yield": 0.15},
    "Real Estate":  {"pb_relative": 0.50, "peg": 0.20, "pe_relative": 0.10, "div_yield": 0.10, "forward_pe": 0.10},
    "Utilities":    {"div_yield": 0.35, "peg": 0.25, "pe_relative": 0.20, "pb_relative": 0.20},
    "Energy":       {"pb_relative": 0.30, "peg": 0.25, "pe_relative": 0.20, "div_yield": 0.15, "forward_pe": 0.10},
    "Materials":    {"pb_relative": 0.30, "peg": 0.25, "pe_relative": 0.20, "div_yield": 0.15, "forward_pe": 0.10},
    "Healthcare":   {"peg": 0.40, "pe_relative": 0.25, "forward_pe": 0.15, "pb_relative": 0.10, "div_yield": 0.10},
    "Industrials":  {"peg": 0.30, "pe_relative": 0.25, "pb_relative": 0.20, "forward_pe": 0.15, "div_yield": 0.10},
    "FMCG":         {"peg": 0.30, "pe_relative": 0.30, "forward_pe": 0.15, "div_yield": 0.15, "pb_relative": 0.10},
    "Auto":         {"peg": 0.30, "pe_relative": 0.25, "pb_relative": 0.20, "forward_pe": 0.15, "div_yield": 0.10},
    "Chemicals":    {"peg": 0.35, "pe_relative": 0.25, "pb_relative": 0.15, "forward_pe": 0.15, "div_yield": 0.10},
    "Telecom":      {"peg": 0.30, "pe_relative": 0.20, "pb_relative": 0.20, "forward_pe": 0.15, "div_yield": 0.15},
    "Consumer Discretionary": {"peg": 0.35, "pe_relative": 0.25, "pb_relative": 0.15, "forward_pe": 0.15, "div_yield": 0.10},
    "Textiles":     {"peg": 0.25, "pe_relative": 0.25, "pb_relative": 0.25, "forward_pe": 0.10, "div_yield": 0.15},
    "Services":     {"peg": 0.35, "pe_relative": 0.25, "forward_pe": 0.15, "pb_relative": 0.15, "div_yield": 0.10},
    "Media & Entertainment": {"peg": 0.35, "pe_relative": 0.25, "forward_pe": 0.15, "pb_relative": 0.15, "div_yield": 0.10},
    "Diversified":  {"peg": 0.30, "pe_relative": 0.25, "pb_relative": 0.20, "forward_pe": 0.10, "div_yield": 0.15},
    "Commodities":  {"pb_relative": 0.30, "peg": 0.25, "pe_relative": 0.20, "div_yield": 0.15, "forward_pe": 0.10},
}

# Sub-metric weights within Growth layer, per sector
_SECTOR_GROWTH_WEIGHTS: dict[str, dict[str, float]] = {
    "DEFAULT":      {"revenue_cagr": 0.30, "profit_cagr": 0.30, "consistency": 0.25, "compounding_bonus": 0.15},
    "Financials":   {"revenue_cagr": 0.20, "profit_cagr": 0.35, "consistency": 0.30, "compounding_bonus": 0.15},
    "IT":           {"revenue_cagr": 0.30, "profit_cagr": 0.30, "consistency": 0.20, "compounding_bonus": 0.20},
    "Healthcare":   {"revenue_cagr": 0.30, "profit_cagr": 0.25, "consistency": 0.25, "compounding_bonus": 0.20},
    "Real Estate":  {"revenue_cagr": 0.25, "profit_cagr": 0.30, "consistency": 0.35, "compounding_bonus": 0.10},
    "Industrials":  {"revenue_cagr": 0.25, "profit_cagr": 0.25, "consistency": 0.35, "compounding_bonus": 0.15},
    "FMCG":         {"revenue_cagr": 0.35, "profit_cagr": 0.25, "consistency": 0.30, "compounding_bonus": 0.10},
    "Auto":         {"revenue_cagr": 0.30, "profit_cagr": 0.30, "consistency": 0.20, "compounding_bonus": 0.20},
    "Utilities":    {"revenue_cagr": 0.20, "profit_cagr": 0.25, "consistency": 0.40, "compounding_bonus": 0.15},
    "Energy":       {"revenue_cagr": 0.25, "profit_cagr": 0.30, "consistency": 0.30, "compounding_bonus": 0.15},
    "Materials":    {"revenue_cagr": 0.25, "profit_cagr": 0.30, "consistency": 0.30, "compounding_bonus": 0.15},
    "Chemicals":    {"revenue_cagr": 0.30, "profit_cagr": 0.30, "consistency": 0.20, "compounding_bonus": 0.20},
    "Telecom":      {"revenue_cagr": 0.30, "profit_cagr": 0.30, "consistency": 0.25, "compounding_bonus": 0.15},
    "Consumer Discretionary": {"revenue_cagr": 0.30, "profit_cagr": 0.30, "consistency": 0.25, "compounding_bonus": 0.15},
    "Textiles":     {"revenue_cagr": 0.30, "profit_cagr": 0.25, "consistency": 0.30, "compounding_bonus": 0.15},
    "Services":     {"revenue_cagr": 0.30, "profit_cagr": 0.30, "consistency": 0.25, "compounding_bonus": 0.15},
    "Media & Entertainment": {"revenue_cagr": 0.30, "profit_cagr": 0.30, "consistency": 0.20, "compounding_bonus": 0.20},
    "Diversified":  {"revenue_cagr": 0.30, "profit_cagr": 0.30, "consistency": 0.25, "compounding_bonus": 0.15},
    "Commodities":  {"revenue_cagr": 0.25, "profit_cagr": 0.25, "consistency": 0.30, "compounding_bonus": 0.20},
}

# Cyclical sectors (for Lynch classification and scoring adjustments)
_CYCLICAL_SECTORS = frozenset({"Materials", "Energy", "Chemicals", "Real Estate", "Auto", "Textiles", "Commodities"})


@dataclass(frozen=True)
class DiscoverStockDef:
    nse_symbol: str
    yahoo_symbol: str
    display_name: str
    sector: str
    fundamentals_enabled: bool = True


def _build_core_universe() -> tuple[DiscoverStockDef, ...]:
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


CORE_UNIVERSE = _build_core_universe()


# ---------------------------------------------------------------------------
# Yahoo Finance v10 quoteSummary via curl_cffi (browser impersonation)
# ---------------------------------------------------------------------------

class YahooFinanceSession:
    """Yahoo v10 quoteSummary via curl_cffi with crumb caching."""

    def __init__(self, crumb_ttl: int = 600, timeout: int = 10):
        self._session = None
        self._crumb: str | None = None
        self._crumb_ts: float = 0.0
        self._crumb_ttl = crumb_ttl
        self._yahoo_timeout = max(2, timeout)

    def _ensure_session(self) -> None:
        if self._session and self._crumb and time_mod.time() - self._crumb_ts < self._crumb_ttl:
            return
        # Try curl_cffi first (bypasses TLS fingerprinting), fall back to requests
        session = None
        try:
            from curl_cffi import requests as cffi_requests
            session = cffi_requests.Session(impersonate="chrome")
            logger.info("Yahoo v10: using curl_cffi session")
        except Exception as exc:
            logger.warning("curl_cffi unavailable (%s), falling back to requests", exc)
            import requests as std_requests
            session = std_requests.Session()
            session.headers.update({
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            })
        self._session = session
        try:
            self._session.get("https://fc.yahoo.com", timeout=self._yahoo_timeout)
        except Exception:
            pass
        time_mod.sleep(1)
        r = self._session.get("https://query2.finance.yahoo.com/v1/test/getcrumb", timeout=self._yahoo_timeout)
        crumb = r.text.strip()
        if "Too Many" in crumb or "error" in crumb.lower() or len(crumb) < 5:
            raise RuntimeError(f"Yahoo crumb failed: {crumb!r}")
        self._crumb = crumb
        self._crumb_ts = time_mod.time()
        logger.info("Yahoo v10: crumb obtained successfully (len=%d)", len(crumb))

    def get_stock_data(self, nse_symbol: str) -> dict:
        """Fetch comprehensive stock data from Yahoo v10 quoteSummary."""
        self._ensure_session()
        modules = "defaultKeyStatistics,financialData,summaryDetail,recommendationTrend"
        url = (
            f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/"
            f"{nse_symbol}.NS?modules={modules}&crumb={self._crumb}"
        )
        r = self._session.get(url, timeout=self._yahoo_timeout)
        data = r.json()
        result = data.get("quoteSummary", {}).get("result", [{}])[0]
        ks = result.get("defaultKeyStatistics", {})
        fd = result.get("financialData", {})
        sd = result.get("summaryDetail", {})
        rt = result.get("recommendationTrend", {}).get("trend", [])

        def _r(d: dict, k: str):
            v = d.get(k, {})
            return v.get("raw") if isinstance(v, dict) else None

        # Analyst consensus
        current_reco = rt[0] if rt else {}

        return {
            # Fundamentals (fallback for Screener gaps)
            "pe_ratio": _r(sd, "trailingPE"),
            "forward_pe": _r(sd, "forwardPE") or _r(ks, "forwardPE"),
            "price_to_book": _r(ks, "priceToBook"),
            "eps": _r(ks, "trailingEps"),
            "forward_eps": _r(ks, "forwardEps"),
            "debt_to_equity": (
                (_r(fd, "debtToEquity") or 0) / 100.0
                if _r(fd, "debtToEquity") is not None else None
            ),
            "dividend_yield": (
                (_r(sd, "dividendYield") or 0) * 100
                if _r(sd, "dividendYield") is not None else None
            ),
            "market_cap": _r(sd, "marketCap"),
            "high_52w": _r(sd, "fiftyTwoWeekHigh"),
            "low_52w": _r(sd, "fiftyTwoWeekLow"),
            # Yahoo-exclusive: Financial Health
            "beta": _r(ks, "beta") or _r(sd, "beta"),
            "free_cash_flow": _r(fd, "freeCashflow"),
            "operating_cash_flow": _r(fd, "operatingCashflow"),
            "total_cash": _r(fd, "totalCash"),
            "total_debt": _r(fd, "totalDebt"),
            "total_revenue": _r(fd, "totalRevenue"),
            "gross_margins": _r(fd, "grossMargins"),
            "operating_margins": _r(fd, "operatingMargins"),
            "profit_margins": _r(fd, "profitMargins"),
            "ebitda_margins": _r(fd, "ebitdaMargins"),
            # Yahoo-exclusive: Growth
            "revenue_growth": _r(fd, "revenueGrowth"),
            "earnings_growth": _r(fd, "earningsGrowth"),
            "earnings_quarterly_growth": _r(ks, "earningsQuarterlyGrowth"),
            # Yahoo-exclusive: Analyst
            "analyst_target_mean": _r(fd, "targetMeanPrice"),
            "analyst_target_median": _r(fd, "targetMedianPrice"),
            "analyst_target_high": _r(fd, "targetHighPrice"),
            "analyst_target_low": _r(fd, "targetLowPrice"),
            "analyst_count": _r(fd, "numberOfAnalystOpinions"),
            "analyst_recommendation": fd.get("recommendationKey"),
            "analyst_recommendation_mean": _r(fd, "recommendationMean"),
            "analyst_strong_buy": current_reco.get("strongBuy", 0),
            "analyst_buy": current_reco.get("buy", 0),
            "analyst_hold": current_reco.get("hold", 0),
            "analyst_sell": (current_reco.get("sell", 0) or 0) + (current_reco.get("strongSell", 0) or 0),
            # Ownership (fallback if Screener missing)
            "held_percent_insiders": _r(ks, "heldPercentInsiders"),
            "held_percent_institutions": _r(ks, "heldPercentInstitutions"),
            # Valuation
            "enterprise_value": _r(ks, "enterpriseValue"),
            "ev_to_ebitda": _r(ks, "enterpriseToEbitda"),
            "ev_to_revenue": _r(ks, "enterpriseToRevenue"),
            "price_to_sales": _r(sd, "priceToSalesTrailing12Months"),
            "payout_ratio": _r(sd, "payoutRatio"),
            # Moving averages
            "fifty_day_avg": _r(sd, "fiftyDayAverage"),
            "two_hundred_day_avg": _r(sd, "twoHundredDayAverage"),
        }


class DiscoverStockScraper(BaseScraper):
    def __init__(self) -> None:
        super().__init__()
        self.settings = get_settings()
        self._nse_ready = False
        self._nse_disabled_until: datetime | None = None
        self._nse_timeout = max(1, int(getattr(self.settings, "discover_stock_nse_timeout_seconds", 4)))
        self._nse_cooldown = max(30, int(getattr(self.settings, "discover_stock_nse_cooldown_seconds", 300)))
        self._screener_timeout = max(2, int(getattr(self.settings, "discover_stock_screener_timeout_seconds", 10)))
        self._screener_max_retries = max(1, int(getattr(self.settings, "discover_stock_screener_max_retries", 3)))
        self._screener_retry_delay = max(0.5, float(getattr(self.settings, "discover_stock_screener_retry_delay", 5.0)))
        self._screener_batch_delay = max(0.0, float(getattr(self.settings, "discover_stock_screener_batch_delay", 0.5)))
        self._yahoo_batch_delay = max(0.0, float(getattr(self.settings, "discover_stock_yahoo_batch_delay", 0.5)))
        self._yahoo_crumb_ttl = max(60, int(getattr(self.settings, "discover_stock_yahoo_crumb_ttl", 600)))
        self._yahoo_timeout = max(2, int(getattr(self.settings, "discover_stock_yahoo_timeout_seconds", 10)))
        self._fundamentals_limit = max(
            len(CORE_UNIVERSE),
            int(getattr(self.settings, "discover_stock_fundamentals_limit", 5000)),
        )
        self._bhavcopy_lookback_days = max(
            1,
            int(getattr(self.settings, "discover_stock_bhavcopy_lookback_days", 7)),
        )
        self._universe_cache_ttl_seconds = max(
            300,
            int(getattr(self.settings, "discover_stock_universe_cache_ttl_seconds", 21600)),
        )
        self._missing_quote_retry_limit = max(
            0,
            int(getattr(self.settings, "discover_stock_missing_quote_retry_limit", 400)),
        )
        self._core_symbol_map = {row.nse_symbol: row for row in CORE_UNIVERSE}
        self._universe_cache: tuple[DiscoverStockDef, ...] | None = None
        self._universe_cache_at: datetime | None = None
        # Yahoo v10 session (lazy init)
        self._yahoo_session: YahooFinanceSession | None = None

    def _get_yahoo_session(self) -> YahooFinanceSession:
        if self._yahoo_session is None:
            self._yahoo_session = YahooFinanceSession(crumb_ttl=self._yahoo_crumb_ttl, timeout=self._yahoo_timeout)
        return self._yahoo_session

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

    @staticmethod
    def _parse_float(value: object) -> float | None:
        try:
            if value is None:
                return None
            text = str(value).replace(",", "").strip()
            if not text:
                return None
            return float(text)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _parse_int(value: object) -> int | None:
        try:
            if value is None:
                return None
            text = str(value).replace(",", "").strip()
            if not text:
                return None
            return int(float(text))
        except (TypeError, ValueError):
            return None

    def _fetch_nse_master_universe(self) -> tuple[DiscoverStockDef, ...]:
        url = str(getattr(self.settings, "discover_stock_universe_url", NSE_EQUITY_MASTER_URL)).strip() or NSE_EQUITY_MASTER_URL
        text = self._get_text(url, timeout=20)
        reader = csv.DictReader(io.StringIO(text))
        out: list[DiscoverStockDef] = []
        seen: set[str] = set()
        for row in reader:
            symbol = str(row.get("SYMBOL") or "").strip().upper()
            if not symbol or symbol in seen:
                continue
            series = str(row.get(" SERIES") or row.get("SERIES") or "").strip().upper()
            if series and series not in NSE_STOCK_SERIES:
                continue

            display_name = str(row.get("NAME OF COMPANY") or symbol).strip() or symbol
            core = self._core_symbol_map.get(symbol)
            # Determine sector: curated > extra map > "Other"
            if core:
                sector = core.sector
            else:
                sector = _EXTRA_SECTOR_MAP.get(symbol, "Other")
                if sector == "Other":
                    logger.debug("Stock %s mapped to 'Other' sector", symbol)
            out.append(
                DiscoverStockDef(
                    nse_symbol=symbol,
                    yahoo_symbol=f"{symbol}.NS",
                    display_name=core.display_name if core else display_name,
                    sector=sector,
                    fundamentals_enabled=core is not None,
                )
            )
            seen.add(symbol)
        return tuple(out)

    def _build_effective_universe(self) -> tuple[DiscoverStockDef, ...]:
        now = datetime.now(timezone.utc)
        if self._universe_cache and self._universe_cache_at is not None:
            age = (now - self._universe_cache_at).total_seconds()
            if age <= self._universe_cache_ttl_seconds:
                return self._universe_cache
        try:
            universe = self._fetch_nse_master_universe()
            if universe:
                self._universe_cache = universe
                self._universe_cache_at = now
                return universe
        except Exception:
            logger.debug("Failed to build full NSE universe from master file", exc_info=True)
        if self._universe_cache:
            return self._universe_cache
        return CORE_UNIVERSE

    def _select_fundamentals_symbols(
        self,
        universe: tuple[DiscoverStockDef, ...],
        quotes_by_symbol: dict[str, dict],
    ) -> set[str]:
        prioritized: list[tuple[int, float, int, str]] = []
        for stock in universe:
            quote = quotes_by_symbol.get(stock.nse_symbol) or {}
            prioritized.append(
                (
                    0 if stock.nse_symbol in self._core_symbol_map else 1,
                    -float(quote.get("traded_value") or 0.0),
                    -int(quote.get("volume") or 0),
                    stock.nse_symbol,
                )
            )

        selected: set[str] = set()
        for _, _, _, symbol in sorted(prioritized):
            selected.add(symbol)
            if len(selected) >= self._fundamentals_limit:
                break

        selected.update(self._core_symbol_map.keys())
        return selected

    def _fetch_latest_bhavcopy_quotes(self) -> tuple[dict[str, dict], datetime | None]:
        base_url = str(getattr(self.settings, "discover_stock_bhavcopy_url_template", NSE_BHAVCOPY_URL_TMPL)).strip()
        if not base_url:
            base_url = NSE_BHAVCOPY_URL_TMPL

        now_ist = datetime.now(IST)
        for day_offset in range(self._bhavcopy_lookback_days + 1):
            d = (now_ist - timedelta(days=day_offset)).date()
            yyyymmdd = d.strftime("%Y%m%d")
            url = base_url.format(yyyymmdd=yyyymmdd)
            try:
                resp = self.session.get(url, timeout=20)
                if resp.status_code == 404:
                    continue
                resp.raise_for_status()
                with zipfile.ZipFile(io.BytesIO(resp.content)) as archive:
                    csv_names = [name for name in archive.namelist() if name.lower().endswith(".csv")]
                    if not csv_names:
                        continue
                    payload = archive.read(csv_names[0]).decode("utf-8", errors="ignore")
                rows = csv.DictReader(io.StringIO(payload))
            except Exception:
                logger.debug("Failed to load NSE bhavcopy url=%s", url, exc_info=True)
                continue

            source_ts = datetime.combine(d, time(hour=16, minute=0), tzinfo=IST).astimezone(timezone.utc)
            out: dict[str, dict] = {}
            for row in rows:
                symbol = str(row.get("TckrSymb") or "").strip().upper()
                if not symbol:
                    continue
                series = str(row.get("SctySrs") or "").strip().upper()
                if series not in NSE_STOCK_SERIES:
                    continue
                last_price = self._parse_float(row.get("ClsPric"))
                prev_close = self._parse_float(row.get("PrvsClsgPric"))
                if last_price is None or last_price <= 0:
                    continue
                point_change = None
                pct_change = None
                if prev_close is not None and prev_close != 0:
                    point_change = round(last_price - prev_close, 2)
                    pct_change = round(((last_price - prev_close) / prev_close) * 100.0, 2)

                out[symbol] = {
                    "last_price": last_price,
                    "point_change": point_change,
                    "percent_change": pct_change,
                    "volume": self._parse_int(row.get("TtlTradgVol")),
                    "traded_value": self._parse_float(row.get("TtlTrfVal")),
                    "source_timestamp": source_ts,
                    "source": "nse_bhavcopy",
                }
            if out:
                return out, source_ts
        return {}, None

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
            patt = rf"{re.escape(label)}\s*(?:in\s+)?[:\-]?\s*(?:[₹]|Rs\.?\s*)?\s*([\-]?[0-9][0-9,]*(?:\.[0-9]+)?)"
            match = re.search(patt, text, flags=re.IGNORECASE)
            if not match:
                continue
            raw = match.group(1).replace(",", "").strip()
            try:
                return float(raw)
            except ValueError:
                continue
        return None

    # ------------------------------------------------------------------
    # Generic Screener table row extractor (reused across P&L, BS, CF)
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_table_row_numbers(
        chunk: str, label: str, *, signed: bool = True
    ) -> list[float]:
        """Extract ALL numeric cell values from a Screener table row.

        Args:
            chunk: HTML slice containing the relevant table section.
            label: Row label text to search for (e.g. "Sales", "Borrowings").
            signed: If True, allows negative numbers (for cash-flow rows).

        Returns:
            List of floats in column order (oldest → latest).  Empty if not found.
        """
        idx = chunk.find(label)
        if idx < 0:
            return []
        close_td = chunk.find("</td>", idx)
        if close_td < 0:
            return []
        after_start = close_td + 5
        end_tr = chunk.find("</tr>", after_start)
        row_slice = chunk[after_start: end_tr] if end_tr > 0 else chunk[after_start: after_start + 3000]
        pat = r'<td[^>]*>\s*([\-]?[\d,]+(?:\.\d+)?)\s*</td>' if signed else r'<td[^>]*>\s*([\d,]+(?:\.\d+)?)\s*</td>'
        raw = re.findall(pat, row_slice)
        nums: list[float] = []
        for r in raw:
            try:
                nums.append(float(r.replace(",", "")))
            except ValueError:
                pass
        return nums

    # ------------------------------------------------------------------
    # Full table extractor (complete YoY history as JSONB)
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_full_table(html: str, section_id: str) -> dict | None:
        """Extract complete table from a Screener.in section as JSONB-ready dict.

        Returns dict with:
          "years": ["Mar 2014", ..., "Mar 2025"],
          "sales": [389178, 328013, ...],
          "expenses": [358244, ...],
          ...
        Returns None if the section is not found.
        """
        match = re.search(rf'id="{section_id}"', html)
        if not match:
            return None
        chunk = html[match.start(): match.start() + 30000]

        # Extract year headers from <thead>
        thead = re.search(r'<thead>(.*?)</thead>', chunk, re.DOTALL)
        years = (
            re.findall(r'>\s*((?:Mar|Jun|Sep|Dec)\s+\d{4})\s*<', thead.group(1))
            if thead else []
        )
        if not years:
            return None

        result: dict = {"years": years}

        # Extract each <tr> in <tbody>
        tbody_match = re.search(r'<tbody>(.*?)</tbody>', chunk, re.DOTALL)
        if not tbody_match:
            return result
        tbody = tbody_match.group(1)
        rows = re.findall(r'<tr[^>]*>(.*?)</tr>', tbody, re.DOTALL)

        for tr in rows:
            tds = re.findall(r'<td[^>]*>(.*?)</td>', tr, re.DOTALL)
            if len(tds) < 2:
                continue
            # First td = label
            label_raw = re.sub(r'<[^>]+>', '', tds[0]).strip()
            label = label_raw.replace('\xa0', ' ').replace('&nbsp;', ' ').rstrip(' +').strip()
            if not label:
                continue
            # Normalize to snake_case key
            key = label.lower().replace(' ', '_').replace('%', 'pct').replace('.', '')
            key = re.sub(r'[^a-z0-9_]', '', key)
            # Clean common suffixes from Screener HTML
            key = re.sub(r'nbsp$', '', key)
            # Remaining tds = values (one per year)
            values: list[float | None] = []
            for td in tds[1:]:
                text = re.sub(r'<[^>]+>', '', td).strip().replace(',', '')
                if text == '' or text == '-':
                    values.append(None)
                elif '%' in text:
                    m = re.search(r'([\-]?\d+)', text)
                    values.append(float(m.group(1)) if m else None)
                else:
                    try:
                        values.append(float(text))
                    except ValueError:
                        values.append(None)
            result[key] = values

        return result

    # ------------------------------------------------------------------
    # Historical metrics from JSONB tables (pl_annual, bs_annual, cf_annual, shareholding_quarterly)
    # ------------------------------------------------------------------

    @staticmethod
    def _compute_historical_metrics(row: dict) -> dict:
        """Mine JSONB annual tables for multi-year trend signals.

        Returns dict of derived metrics (prefixed with nothing — caller adds _hist_).
        Gracefully returns {} when JSONB data is absent.
        """
        import statistics

        out: dict = {}

        def _ensure_dict(val: object) -> dict | None:
            if val is None:
                return None
            if isinstance(val, dict):
                return val
            if isinstance(val, str):
                try:
                    parsed = json.loads(val)
                    if isinstance(parsed, dict):
                        return parsed
                except (ValueError, TypeError):
                    pass
            return None

        pl = _ensure_dict(row.get("pl_annual"))
        bs = _ensure_dict(row.get("bs_annual"))
        cf = _ensure_dict(row.get("cf_annual"))
        sh = _ensure_dict(row.get("shareholding_quarterly"))

        def _tail(lst: list | None, n: int) -> list:
            """Last n non-None values from list."""
            if not lst:
                return []
            return [v for v in lst[-n:] if v is not None]

        def _cagr(values: list, years: int) -> float | None:
            """CAGR from first to last value over N years."""
            if len(values) < 2 or years <= 0:
                return None
            start, end = values[0], values[-1]
            if start is None or end is None or start <= 0:
                return None
            ratio = end / start
            if ratio <= 0:
                return None  # negative ratio → complex power, skip
            try:
                return ratio ** (1.0 / years) - 1.0
            except (ZeroDivisionError, ValueError, OverflowError):
                return None

        def _consistency(values: list) -> int:
            """Count years with positive YoY growth (last 5 years)."""
            vals = _tail(values, 6)  # need 6 to get 5 YoY deltas
            if len(vals) < 2:
                return 0
            count = 0
            for i in range(1, len(vals)):
                if vals[i] is not None and vals[i - 1] is not None and vals[i - 1] > 0:
                    if vals[i] > vals[i - 1]:
                        count += 1
            return count

        # ── From pl_annual ──
        if pl and isinstance(pl, dict):
            net_profits = pl.get("net_profit", [])
            sales = pl.get("sales", [])
            opm_vals = pl.get("opm_pct") or pl.get("opm_pct", [])
            eps_vals = pl.get("eps_in_rs") or pl.get("eps", [])

            # Profit growth 3Y CAGR
            tail_np = _tail(net_profits, 4)
            if len(tail_np) >= 2:
                cagr = _cagr(tail_np, len(tail_np) - 1)
                if cagr is not None:
                    out["profit_growth_3y_cagr"] = cagr

            # Consistency (last 5 years)
            out["profit_growth_consistency"] = _consistency(net_profits)
            out["sales_growth_consistency"] = _consistency(sales)

            # OPM trend 3Y (latest − 3yr ago)
            opm_tail = _tail(opm_vals, 4)
            if len(opm_tail) >= 2:
                out["opm_trend_3y"] = opm_tail[-1] - opm_tail[0]

            # OPM std 5Y
            opm_5y = _tail(opm_vals, 5)
            if len(opm_5y) >= 3:
                try:
                    out["opm_std_5y"] = statistics.stdev(opm_5y)
                except statistics.StatisticsError:
                    pass

            # 3-year average ROE: net_profit / equity from bs_annual
            if bs and isinstance(bs, dict):
                equity_key = None
                for k in ("shareholders_equity", "shareholder_equity", "share_capital", "equity_capital"):
                    if k in bs:
                        equity_key = k
                        break
                # Try reserves + equity capital as fallback
                reserves = bs.get("reserves", [])
                eq_cap = bs.get(equity_key, []) if equity_key else []

                # Compute equity as reserves if shareholders_equity not found
                nps_3 = _tail(net_profits, 3)
                if reserves and len(reserves) >= 3:
                    eqs_3 = _tail(reserves, 3)
                    valid = [(n, e) for n, e in zip(nps_3, eqs_3)
                             if n is not None and e is not None and e > 0]
                    if len(valid) >= 2:
                        out["avg_roe_3y"] = sum(n / e for n, e in valid) / len(valid) * 100

            # 3-year average ROCE: operating_profit / (total_assets - current_liabilities)
            # Simplified: operating_profit / total_assets as proxy
            if bs and isinstance(bs, dict):
                op_profits = pl.get("operating_profit", [])
                total_assets = bs.get("total_assets", [])
                ops_3 = _tail(op_profits, 3)
                assets_3 = _tail(total_assets, 3)
                valid = [(o, a) for o, a in zip(ops_3, assets_3)
                         if o is not None and a is not None and a > 0]
                if len(valid) >= 2:
                    out["avg_roce_3y"] = sum(o / a for o, a in valid) / len(valid) * 100

            # EPS CAGR for synthetic forward PE
            eps_tail = [v for v in (eps_vals or [])[-4:] if v is not None and v > 0]
            if len(eps_tail) >= 2:
                cagr = _cagr(eps_tail, len(eps_tail) - 1)
                if cagr is not None and cagr > -0.5:
                    out["eps_cagr_3y"] = cagr
                    forward_eps = eps_tail[-1] * (1 + max(cagr, -0.3))
                    if forward_eps > 0:
                        out["improved_forward_eps"] = forward_eps

        # ── From bs_annual ──
        if bs and isinstance(bs, dict):
            # Debt trajectory
            borrowings = bs.get("borrowings", [])
            b_tail = _tail(borrowings, 4)
            if len(b_tail) >= 2 and b_tail[0] > 0:
                out["debt_trajectory"] = (b_tail[-1] - b_tail[0]) / b_tail[0]

            # Reserves CAGR 3Y
            reserves = bs.get("reserves", [])
            r_tail = _tail(reserves, 4)
            if len(r_tail) >= 2:
                cagr = _cagr(r_tail, len(r_tail) - 1)
                if cagr is not None:
                    out["reserves_cagr_3y"] = cagr

        # ── From cf_annual ──
        if cf and isinstance(cf, dict):
            ocf_key = None
            for k in ("cash_from_operating_activity", "cash_from_operations",
                       "cash_from_operating_activities"):
                if k in cf:
                    ocf_key = k
                    break
            ocf_vals = cf.get(ocf_key, []) if ocf_key else []

            # OCF positive years (last 5)
            ocf_5 = _tail(ocf_vals, 5)
            if ocf_5:
                out["ocf_positive_years"] = sum(1 for v in ocf_5 if v > 0)

            # Cumulative accrual ratio: (sum 3yr NP − sum 3yr OCF) / avg total_assets
            if pl and isinstance(pl, dict) and bs and isinstance(bs, dict):
                np_3 = _tail(pl.get("net_profit", []), 3)
                ocf_3 = _tail(ocf_vals, 3)
                ta_3 = _tail(bs.get("total_assets", []), 3)
                if len(np_3) >= 2 and len(ocf_3) >= 2 and len(ta_3) >= 1:
                    sum_np = sum(v for v in np_3 if v is not None)
                    sum_ocf = sum(v for v in ocf_3 if v is not None)
                    avg_ta = sum(v for v in ta_3 if v is not None) / max(len(ta_3), 1)
                    if avg_ta > 0:
                        out["cumulative_accrual_ratio"] = (sum_np - sum_ocf) / avg_ta

        # ── From shareholding_quarterly ──
        if sh and isinstance(sh, dict):
            for cat_key, out_key in [
                ("promoters", "promoter_trend_4q"),
                ("fiis", "fii_trend_4q"),
                ("diis", "dii_trend_4q"),
            ]:
                vals = sh.get(cat_key, [])
                t = _tail(vals, 5)  # 5 quarters → 4-quarter change
                if len(t) >= 2:
                    out[out_key] = t[-1] - t[0]

            # FII trend direction (monotonicity over 4 quarters)
            fii_vals = _tail(sh.get("fiis", []), 5)
            if len(fii_vals) >= 3:
                diffs = [fii_vals[i] - fii_vals[i - 1] for i in range(1, len(fii_vals))]
                pos = sum(1 for d in diffs if d > 0)
                neg = sum(1 for d in diffs if d < 0)
                if pos >= len(diffs) * 0.75:
                    out["fii_trend_direction"] = "increasing"
                elif neg >= len(diffs) * 0.75:
                    out["fii_trend_direction"] = "decreasing"
                else:
                    out["fii_trend_direction"] = "stable"

        # ── New derived metrics ──

        # 5Y revenue CAGR
        if pl and isinstance(pl, dict):
            sales = pl.get("sales", [])
            tail_sales_5y = _tail(sales, 6)
            if len(tail_sales_5y) >= 2:
                cagr = _cagr(tail_sales_5y, len(tail_sales_5y) - 1)
                if cagr is not None:
                    out["5y_revenue_cagr"] = cagr

            # 5Y profit CAGR
            net_profits = pl.get("net_profit", [])
            tail_np_5y = _tail(net_profits, 6)
            if len(tail_np_5y) >= 2:
                cagr = _cagr(tail_np_5y, len(tail_np_5y) - 1)
                if cagr is not None:
                    out["5y_profit_cagr"] = cagr

        # 5Y ROE stability (stddev)
        if pl and bs and isinstance(pl, dict) and isinstance(bs, dict):
            np_5 = _tail(pl.get("net_profit", []), 5)
            res_5 = _tail(bs.get("reserves", []), 5)
            if len(np_5) >= 3 and len(res_5) >= 3:
                roes = [n/r*100 for n, r in zip(np_5, res_5) if n is not None and r is not None and r > 0]
                if len(roes) >= 3:
                    try:
                        out["5y_roe_stability"] = statistics.stdev(roes)
                    except statistics.StatisticsError:
                        pass

        # CWIP to assets ratio
        if bs and isinstance(bs, dict):
            cwip_vals = bs.get("cwip", [])
            ta_vals = bs.get("total_assets", [])
            if cwip_vals and ta_vals:
                c = _tail(cwip_vals, 1)
                a = _tail(ta_vals, 1)
                if c and a and a[0] > 0:
                    out["cwip_to_assets"] = c[0] / a[0]

        # Interest to revenue (bank lending efficiency)
        if pl and isinstance(pl, dict):
            interest_vals = pl.get("interest", [])
            sales_vals = pl.get("sales", [])
            if interest_vals and sales_vals:
                i = _tail(interest_vals, 1)
                s = _tail(sales_vals, 1)
                if i and s and s[0] > 0:
                    out["interest_to_revenue"] = i[0] / s[0]

        # Incremental ROE
        if pl and bs and isinstance(pl, dict) and isinstance(bs, dict):
            np_vals = pl.get("net_profit", [])
            res_vals = bs.get("reserves", [])
            np_t = _tail(np_vals, 2)
            res_t = _tail(res_vals, 2)
            if len(np_t) >= 2 and len(res_t) >= 2:
                d_np = np_t[-1] - np_t[0]
                d_res = res_t[-1] - res_t[0]
                if d_res > 0:
                    out["incremental_roe"] = (d_np / d_res) * 100

        # Negative FCF streak (consecutive recent years with OCF < 0)
        if cf and isinstance(cf, dict):
            ocf_key = None
            for k in ("cash_from_operating_activity", "cash_from_operations", "cash_from_operating_activities"):
                if k in cf:
                    ocf_key = k
                    break
            if ocf_key:
                ocf_vals_streak = _tail(cf.get(ocf_key, []), 5)
                streak = 0
                for v in reversed(ocf_vals_streak):
                    if v is not None and v < 0:
                        streak += 1
                    else:
                        break
                out["negative_fcf_streak"] = streak

        # Low ROE streak (consecutive recent years with ROE < 5%)
        if pl and bs and isinstance(pl, dict) and isinstance(bs, dict):
            np_streak = _tail(pl.get("net_profit", []), 5)
            res_streak = _tail(bs.get("reserves", []), 5)
            streak = 0
            for n, r in zip(reversed(np_streak), reversed(res_streak)):
                if n is not None and r is not None and r > 0:
                    roe_val = (n / r) * 100
                    if roe_val < 5:
                        streak += 1
                    else:
                        break
                else:
                    break
            out["low_roe_streak"] = streak

        return out

    # ------------------------------------------------------------------
    # P&L extractor
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_profit_loss(html: str) -> dict:
        """Extract key P&L signals from Screener.in annual Profit & Loss table.

        Returns dict with: sales_growth_yoy, profit_growth_yoy, opm_change,
        interest_coverage, eps_latest, eps_prev.
        """
        result: dict = {}
        pl_match = re.search(r'id="profit-loss"', html)
        if not pl_match:
            return result
        pl_chunk = html[pl_match.start(): pl_match.start() + 25000]

        _ext = DiscoverStockScraper._extract_table_row_numbers

        sales = _ext(pl_chunk, "Sales")
        op_profit = _ext(pl_chunk, "Operating Profit")
        net_profit = _ext(pl_chunk, "Net Profit")
        interest = _ext(pl_chunk, "Interest")
        eps_row = _ext(pl_chunk, "EPS in Rs")

        # Sales YoY growth (latest vs previous year)
        if len(sales) >= 2 and sales[-2] and sales[-2] > 0:
            result["sales_growth_yoy"] = round((sales[-1] / sales[-2]) - 1, 4)

        # Net Profit YoY growth
        if len(net_profit) >= 2 and net_profit[-2] and net_profit[-2] > 0:
            result["profit_growth_yoy"] = round((net_profit[-1] / net_profit[-2]) - 1, 4)

        # Operating Profit Margin change (latest OPM - previous OPM)
        if len(op_profit) >= 2 and len(sales) >= 2 and sales[-1] > 0 and sales[-2] > 0:
            opm_latest = op_profit[-1] / sales[-1]
            opm_prev = op_profit[-2] / sales[-2]
            result["opm_change"] = round(opm_latest - opm_prev, 4)

        # Interest coverage = Operating Profit / Interest
        if len(op_profit) >= 1 and len(interest) >= 1 and interest[-1] and interest[-1] > 0:
            result["interest_coverage"] = round(op_profit[-1] / interest[-1], 2)

        # EPS latest and previous (for synthetic forward PE)
        if len(eps_row) >= 1:
            result["eps_latest"] = eps_row[-1]
        if len(eps_row) >= 2:
            result["eps_prev"] = eps_row[-2]

        # Store raw latest values for downstream signals
        if len(net_profit) >= 1:
            result["_net_profit_latest"] = net_profit[-1]
        if len(sales) >= 1:
            result["_sales_latest"] = sales[-1]

        return result

    # ------------------------------------------------------------------
    # Balance Sheet extractor (enhanced — replaces old D/E-only version)
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_balance_sheet(html: str) -> dict:
        """Extract balance sheet signals from Screener.in.

        Returns dict with: debt_to_equity, total_assets, asset_growth_yoy,
        reserves_growth, debt_direction, cwip.
        """
        result: dict = {}
        bs_match = re.search(r'id="balance-sheet"', html)
        if not bs_match:
            return result
        bs_chunk = html[bs_match.start(): bs_match.start() + 25000]

        _ext = DiscoverStockScraper._extract_table_row_numbers

        borrowings = _ext(bs_chunk, "Borrowings")
        equity_capital = _ext(bs_chunk, "Equity Capital", signed=False)
        reserves = _ext(bs_chunk, "Reserves")
        total_assets = _ext(bs_chunk, "Total Assets", signed=False)
        cwip = _ext(bs_chunk, "CWIP", signed=False)
        investments = _ext(bs_chunk, "Investments")

        # D/E from latest period (backward-compatible)
        if borrowings:
            latest_borrow = borrowings[-1]
            latest_eq = (equity_capital[-1] if equity_capital else 0) + (reserves[-1] if reserves else 0)
            if latest_eq > 0:
                result["debt_to_equity"] = round(latest_borrow / latest_eq, 2)

        # Total Assets (latest, in Cr)
        if total_assets:
            result["total_assets"] = total_assets[-1]
            # Asset growth YoY
            if len(total_assets) >= 2 and total_assets[-2] > 0:
                result["asset_growth_yoy"] = round((total_assets[-1] / total_assets[-2]) - 1, 4)

        # Reserves growth (internal value creation)
        if len(reserves) >= 2 and reserves[-2] > 0:
            result["reserves_growth"] = round((reserves[-1] / reserves[-2]) - 1, 4)

        # Debt direction (positive = debt increasing, negative = deleveraging)
        if len(borrowings) >= 2 and borrowings[-2] > 0:
            result["debt_direction"] = round((borrowings[-1] / borrowings[-2]) - 1, 4)
        elif len(borrowings) >= 1:
            # If only one period or prev was 0, just store latest
            result["debt_direction"] = 0.0 if borrowings[-1] == 0 else None

        # CWIP (Capital Work in Progress — future growth pipeline)
        if cwip:
            result["cwip"] = cwip[-1]

        return result

    @staticmethod
    def _extract_balance_sheet_de(html: str) -> float | None:
        """Backward-compatible D/E extraction (delegates to full extractor)."""
        bs_data = DiscoverStockScraper._extract_balance_sheet(html)
        return bs_data.get("debt_to_equity")

    # ------------------------------------------------------------------
    # Cash Flow extractor
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_cash_flow(html: str) -> dict:
        """Extract cash flow signals from Screener.in Cash Flow section.

        Returns dict with: cash_from_operations, cash_from_investing,
        cash_from_financing.  Values are in Cr (latest annual period).
        """
        result: dict = {}
        cf_match = re.search(r'id="cash-flow"', html)
        if not cf_match:
            return result
        cf_chunk = html[cf_match.start(): cf_match.start() + 15000]

        _ext = DiscoverStockScraper._extract_table_row_numbers

        cfo = _ext(cf_chunk, "Cash from Operating Activity")
        cfi = _ext(cf_chunk, "Cash from Investing Activity")
        cff = _ext(cf_chunk, "Cash from Financing Activity")

        if cfo:
            result["cash_from_operations"] = cfo[-1]
        if cfi:
            result["cash_from_investing"] = cfi[-1]
        if cff:
            result["cash_from_financing"] = cff[-1]

        return result

    # ------------------------------------------------------------------
    # Compounded Growth extractor
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_compounded_growth(html: str) -> dict:
        """Extract pre-computed compounded growth rates from Screener.in.

        Screener shows: Compounded Sales Growth, Compounded Profit Growth,
        Stock Price CAGR, Return on Equity — each with 10Y, 5Y, 3Y, TTM/1Y.

        Returns dict with: compounded_sales_growth_3y, compounded_profit_growth_3y,
        stock_price_cagr_3y, roe_3y_avg.
        """
        result: dict = {}

        def _extract_growth_value(section_label: str, period_label: str) -> float | None:
            # Find the section
            idx = html.find(section_label)
            if idx < 0:
                return None
            # Search within a reasonable window after the label
            window = html[idx: idx + 800]
            # Screener HTML: <td>3 Years:</td>\n<td>7%</td>
            # Match across the </td><td> boundary
            pat = re.compile(
                period_label + r'[:\s]*</td>\s*<td[^>]*>\s*([\-]?\d+)\s*%',
                re.IGNORECASE,
            )
            m = pat.search(window)
            if m:
                try:
                    return float(m.group(1))
                except ValueError:
                    return None
            return None

        csg_3y = _extract_growth_value("Compounded Sales Growth", "3 Years")
        cpg_3y = _extract_growth_value("Compounded Profit Growth", "3 Years")

        if csg_3y is not None:
            result["compounded_sales_growth_3y"] = csg_3y
        if cpg_3y is not None:
            result["compounded_profit_growth_3y"] = cpg_3y

        return result

    @staticmethod
    def _extract_shareholding(html: str) -> dict:
        """Extract shareholding data from Screener.in <section id="shareholding"> table.

        Returns dict with: promoter_holding, fii_holding, dii_holding,
        government_holding, public_holding, num_shareholders,
        and *_prev variants for QoQ change computation.
        """
        result: dict = {}
        sh_match = re.search(r'id="shareholding"', html)
        if not sh_match:
            return result
        sh_chunk = html[sh_match.start(): sh_match.start() + 15000]

        def _extract_row(label: str) -> tuple[float | None, float | None]:
            """Extract the latest and previous quarter values for a shareholding row."""
            idx = sh_chunk.find(label)
            if idx < 0:
                return None, None
            close_td = sh_chunk.find("</td>", idx)
            if close_td < 0:
                return None, None
            after_start = close_td + 5
            end_tr = sh_chunk.find("</tr>", after_start)
            row_slice = sh_chunk[after_start: end_tr] if end_tr > 0 else sh_chunk[after_start: after_start + 2000]
            nums = re.findall(r'<td[^>]*>\s*([\d,]+(?:\.\d+)?)\s*%?\s*</td>', row_slice)
            latest = None
            prev = None
            if nums:
                try:
                    latest = float(nums[-1].replace(",", ""))
                except ValueError:
                    pass
                if len(nums) >= 2:
                    try:
                        prev = float(nums[-2].replace(",", ""))
                    except ValueError:
                        pass
            return latest, prev

        promoter, promoter_prev = _extract_row("Promoters")
        if promoter is not None:
            result["promoter_holding"] = promoter
            if promoter_prev is not None:
                result["promoter_holding_change"] = round(promoter - promoter_prev, 2)

        fii, fii_prev = _extract_row("FIIs")
        if fii is None:
            fii, fii_prev = _extract_row("Foreign Institutions")
        if fii is not None:
            result["fii_holding"] = fii
            if fii_prev is not None:
                result["fii_holding_change"] = round(fii - fii_prev, 2)

        dii, dii_prev = _extract_row("DIIs")
        if dii is None:
            dii, dii_prev = _extract_row("Domestic Institutions")
        if dii is not None:
            result["dii_holding"] = dii
            if dii_prev is not None:
                result["dii_holding_change"] = round(dii - dii_prev, 2)

        gov, _ = _extract_row("Government")
        if gov is not None:
            result["government_holding"] = gov

        pub, _ = _extract_row("Public")
        if pub is not None:
            result["public_holding"] = pub

        # Number of shareholders — extract all available quarters for QoQ + YoY
        _ext_all = DiscoverStockScraper._extract_table_row_numbers
        ns_all = _ext_all(sh_chunk, "No. of Shareholders", signed=False)
        if ns_all:
            result["num_shareholders"] = int(ns_all[-1])
            # QoQ change (latest vs previous quarter)
            if len(ns_all) >= 2 and ns_all[-2] > 0:
                result["num_shareholders_change_qoq"] = round(
                    (ns_all[-1] / ns_all[-2] - 1) * 100, 2
                )
            # YoY change (latest vs 4 quarters ago)
            if len(ns_all) >= 5 and ns_all[-5] > 0:
                result["num_shareholders_change_yoy"] = round(
                    (ns_all[-1] / ns_all[-5] - 1) * 100, 2
                )

        # Pledged promoter shares (critical risk signal)
        pledged, _ = _extract_row("Pledged")
        if pledged is None:
            pledged, _ = _extract_row("Pledge")
        if pledged is not None:
            result["pledged_promoter_pct"] = pledged

        return result

    def _fetch_screener_fundamentals(self, nse_symbol: str) -> tuple[dict, str]:
        """Fetch fundamentals from Screener.in with retry logic and shareholding extraction."""
        base = self.settings.discover_stock_primary_url.rstrip("/")
        candidates = [
            f"{base}/company/{nse_symbol}/consolidated/",
            f"{base}/company/{nse_symbol}/",
        ]
        for url in candidates:
            for attempt in range(self._screener_max_retries):
                try:
                    html = self._get_text(url, timeout=self._screener_timeout)
                    text = unescape(re.sub(r"<[^>]+>", " ", html))
                    text = re.sub(r"\s+", " ", text)

                    book_value = self._extract_labeled_number(text, ["Book Value"])
                    current_price = self._extract_labeled_number(text, ["Current Price"])

                    debt_to_equity = self._extract_balance_sheet_de(html)

                    fundamentals: dict = {
                        "pe_ratio": self._extract_labeled_number(text, ["Stock P/E", "P/E"]),
                        "roe": self._extract_labeled_number(text, ["ROE", "Return on equity"]),
                        "roce": self._extract_labeled_number(text, ["ROCE", "Return on capital employed"]),
                        "debt_to_equity": debt_to_equity,
                        "price_to_book": (
                            round(current_price / book_value, 2)
                            if current_price and book_value and book_value > 0
                            else None
                        ),
                        "eps": self._extract_labeled_number(text, ["EPS", "Earnings Per Share"]),
                        "market_cap": self._extract_labeled_number(text, ["Market Cap"]),
                        "dividend_yield": self._extract_labeled_number(text, ["Dividend Yield"]),
                    }

                    # --- Extract sector/industry from HTML attributes (NOT flattened text) ---
                    broad_sector_match = re.search(r'title="Broad Sector">([^<]+)</a>', html)
                    industry_match = re.search(r'title="Industry">([^<]+)</a>', html)

                    if broad_sector_match:
                        fundamentals["_screener_broad_sector"] = unescape(broad_sector_match.group(1).strip())
                    if industry_match:
                        fundamentals["_screener_industry"] = unescape(industry_match.group(1).strip())

                    # 52-week High / Low
                    hl_match = re.search(
                        r"High\s*/\s*Low\s*[₹Rs.\s]*([\d,]+(?:\.\d+)?)\s*/\s*[₹Rs.\s]*([\d,]+(?:\.\d+)?)",
                        text,
                        flags=re.IGNORECASE,
                    )
                    if hl_match:
                        try:
                            fundamentals["high_52w"] = float(hl_match.group(1).replace(",", ""))
                        except ValueError:
                            fundamentals["high_52w"] = None
                        try:
                            fundamentals["low_52w"] = float(hl_match.group(2).replace(",", ""))
                        except ValueError:
                            fundamentals["low_52w"] = None
                    else:
                        fundamentals["high_52w"] = None
                        fundamentals["low_52w"] = None

                    # --- Extract shareholding from HTML ---
                    shareholding = self._extract_shareholding(html)
                    sh_fields = sum(1 for v in shareholding.values() if v is not None)
                    if sh_fields > 0:
                        logger.debug(
                            "Screener shareholding for %s: %d fields (promoter=%.1f%%)",
                            nse_symbol, sh_fields,
                            shareholding.get("promoter_holding") or 0,
                        )
                    fundamentals.update(shareholding)

                    # --- Extract P&L signals from annual table ---
                    pl_data = self._extract_profit_loss(html)
                    if pl_data:
                        fundamentals.update(pl_data)

                    # --- Extract Balance Sheet signals ---
                    bs_data = self._extract_balance_sheet(html)
                    if bs_data:
                        # Don't overwrite D/E if already set from old extractor
                        if "debt_to_equity" in bs_data and fundamentals.get("debt_to_equity") is None:
                            fundamentals["debt_to_equity"] = bs_data.pop("debt_to_equity")
                        else:
                            bs_data.pop("debt_to_equity", None)
                        fundamentals.update(bs_data)

                    # --- Extract Cash Flow signals ---
                    cf_data = self._extract_cash_flow(html)
                    if cf_data:
                        fundamentals.update(cf_data)

                    # --- Extract Compounded Growth metrics ---
                    cg_data = self._extract_compounded_growth(html)
                    if cg_data:
                        fundamentals.update(cg_data)

                    # --- Extract full historical tables (JSONB) ---
                    pl_full = self._extract_full_table(html, "profit-loss")
                    if pl_full:
                        fundamentals["pl_annual"] = pl_full
                    bs_full = self._extract_full_table(html, "balance-sheet")
                    if bs_full:
                        fundamentals["bs_annual"] = bs_full
                    cf_full = self._extract_full_table(html, "cash-flow")
                    if cf_full:
                        fundamentals["cf_annual"] = cf_full
                    sh_full = self._extract_full_table(html, "shareholding")
                    if sh_full:
                        fundamentals["shareholding_quarterly"] = sh_full
                        logger.debug(
                            "Shareholding JSONB OK for %s: %d keys, years=%s",
                            nse_symbol, len(sh_full), sh_full.get("years", [])[:3],
                        )
                    else:
                        # Debug: why did _extract_full_table return None?
                        _sh_match = re.search(r'id="shareholding"', html)
                        logger.warning(
                            "Shareholding JSONB MISSING for %s: html_len=%d, section_found=%s",
                            nse_symbol, len(html), _sh_match is not None,
                        )

                    return fundamentals, "screener_in"
                except requests.exceptions.HTTPError as e:
                    status = e.response.status_code if e.response is not None else 0
                    if status in (429, 503) and attempt < self._screener_max_retries - 1:
                        wait = self._screener_retry_delay * (attempt + 1)
                        logger.warning("Screener %d for %s; retry in %.0fs (%d/%d)", status, nse_symbol, wait, attempt + 1, self._screener_max_retries)
                        time_mod.sleep(wait)
                        continue
                    break
                except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
                    if attempt < self._screener_max_retries - 1:
                        time_mod.sleep(self._screener_retry_delay)
                        continue
                    break
                except Exception:
                    logger.debug("Screener fundamentals fetch failed for %s url=%s", nse_symbol, url, exc_info=True)
                    break
        return {
            "pe_ratio": None, "roe": None, "roce": None,
            "debt_to_equity": None, "price_to_book": None,
            "eps": None, "market_cap": None, "high_52w": None,
            "low_52w": None, "dividend_yield": None,
        }, "unavailable"

    @staticmethod
    def _clamp(value: float, lo: float = 0.0, hi: float = 100.0) -> float:
        return max(lo, min(hi, value))

    @staticmethod
    def _percentile_rank(values: list[float], target: float) -> float:
        if not values:
            return 50.0
        eps = 1e-9
        below = sum(1 for v in values if v < (target - eps))
        equal = sum(1 for v in values if abs(v - target) <= eps)
        return ((below + (equal * 0.5)) / len(values)) * 100.0

    @staticmethod
    def _quantile(values: list[float], q: float) -> float:
        if not values:
            return 0.0
        clipped_q = max(0.0, min(1.0, q))
        s = sorted(values)
        if len(s) == 1:
            return s[0]
        pos = (len(s) - 1) * clipped_q
        lo = int(pos)
        hi = min(lo + 1, len(s) - 1)
        if lo == hi:
            return s[lo]
        frac = pos - lo
        return s[lo] + ((s[hi] - s[lo]) * frac)

    @staticmethod
    def _shrink_to_neutral(
        score: float,
        coverage: float,
        *,
        neutral: float = 42.0,
        min_factor: float = 0.35,
    ) -> float:
        c = max(0.0, min(1.0, coverage))
        factor = min_factor + ((1.0 - min_factor) * c)
        return neutral + ((score - neutral) * factor)

    @staticmethod
    def _median(values: list[float]) -> float:
        if not values:
            return 0.0
        s = sorted(values)
        n = len(s)
        mid = n // 2
        return (s[mid] + s[mid - 1]) / 2.0 if n % 2 == 0 else s[mid]

    def _score_52w_position(
        self,
        price: float | None,
        high_52w: float | None,
        low_52w: float | None,
    ) -> float | None:
        if price is None or high_52w is None or low_52w is None:
            return None
        if high_52w <= low_52w or price <= 0:
            return None
        position = (price - low_52w) / (high_52w - low_52w)
        position = max(0.0, min(1.0, position))
        return round(self._clamp(position * 95 + 5), 2)

    @staticmethod
    def _adjust_volatility_for_cap(
        vol_score: float,
        market_cap: float | None,
    ) -> float:
        if market_cap is None or market_cap <= 0:
            return vol_score
        if market_cap >= 20_000:
            return vol_score
        if market_cap >= 5_000:
            return min(100.0, vol_score * 1.08)
        return min(100.0, vol_score * 1.15)

    def _score_quality(
        self,
        row: dict,
        sector: str,
        sector_medians: dict[str, dict[str, float]],
    ) -> tuple[float, int]:
        """Unified quality score merging fundamentals, financial health, and earnings quality.

        Sector-specific sub-metric weights eliminate double-counting.
        """
        parts: dict[str, float] = {}
        metrics_used = 0

        is_financial = sector == "Financials"
        weights = _SECTOR_QUALITY_WEIGHTS.get(sector, _SECTOR_QUALITY_WEIGHTS["DEFAULT"])

        # ROE (blended current + 3Y historical)
        roe = row.get("roe")
        hist_roe = row.get("_hist_avg_roe_3y")
        if hist_roe is not None and roe is not None:
            roe = roe * 0.6 + hist_roe * 0.4
        if roe is not None:
            roe = max(-50.0, min(roe, 100.0))
            roe_score = self._clamp(roe * 4.5)
            # DuPont discount
            roce = row.get("roce")
            if roce is not None and roce > 0 and roe > roce * 1.5:
                roe_score *= 0.85
            parts["roe"] = roe_score
            metrics_used += 1

        # ROCE (blended current + 3Y historical)
        roce = row.get("roce")
        hist_roce = row.get("_hist_avg_roce_3y")
        if hist_roce is not None and roce is not None:
            roce = roce * 0.6 + hist_roce * 0.4
        if roce is not None:
            roce = max(-50.0, min(roce, 100.0))
            parts["roce"] = self._clamp(roce * 4.0)
            metrics_used += 1

        # Operating margin
        op_margin = row.get("operating_margins")
        if op_margin is not None:
            parts["op_margin"] = self._clamp(op_margin * 200 + 20)
            metrics_used += 1

        # NIM proxy for financials
        if is_financial and op_margin is not None:
            parts["nim_proxy"] = self._clamp(op_margin * 200 + 30)

        # Gross margin (FMCG brand moat proxy)
        gross = row.get("gross_margins")
        if gross is not None:
            parts["gross_margin"] = self._clamp(gross * 150 + 10)

        # FCF yield
        fcf = row.get("free_cash_flow")
        mcap = row.get("market_cap")
        if fcf is not None and mcap and mcap > 0:
            fcf_yield = (fcf / (mcap * 1e7)) * 100
            parts["fcf_yield"] = self._clamp(fcf_yield * 8 + 30)
            metrics_used += 1

        # Net cash position
        cash = row.get("total_cash")
        debt = row.get("total_debt")
        if cash is not None and debt is not None and mcap and mcap > 0:
            net_cash_pct = ((cash - debt) / (mcap * 1e7)) * 100
            parts["net_cash"] = self._clamp(50 + net_cash_pct * 3)

        # Margin stability (low std = good)
        opm_std = row.get("_hist_opm_std_5y")
        if opm_std is not None:
            parts["margin_stability"] = self._clamp(90 - opm_std * 5)

        # OCF consistency
        ocf_years = row.get("_hist_ocf_positive_years")
        if ocf_years is not None:
            parts["ocf_consistency"] = self._clamp(ocf_years / 5.0 * 100)

        # Profit consistency
        profit_cons = row.get("_hist_profit_growth_consistency", 0)
        sales_cons = row.get("_hist_sales_growth_consistency", 0)
        if profit_cons > 0 or sales_cons > 0:
            parts["profit_consistency"] = self._clamp((profit_cons + sales_cons) / 10.0 * 100)

        # Accrual quality (CFO/profit ratio)
        cfo = row.get("cash_from_operations")
        net_profit = row.get("_net_profit_latest")
        if cfo is not None and net_profit is not None and net_profit > 0:
            cfo_ratio = cfo / net_profit
            parts["accrual_quality"] = self._clamp(cfo_ratio * 50 + 20)

        # Interest coverage
        int_cov = row.get("interest_coverage")
        if int_cov is not None:
            parts["interest_coverage"] = self._clamp(int_cov * 12)

        # Interest to revenue (bank-specific)
        int_rev = row.get("_hist_interest_to_revenue")
        if int_rev is not None and is_financial:
            parts["interest_to_rev"] = self._clamp(int_rev * 200 + 20)

        # CWIP to assets (pharma/industrials R&D/capex proxy)
        cwip_ratio = row.get("_hist_cwip_to_assets")
        if cwip_ratio is not None:
            # Higher CWIP = more investment; good for pharma/industrials
            if sector in ("Healthcare", "Industrials", "Auto", "Chemicals"):
                parts["cwip_to_assets"] = self._clamp(cwip_ratio * 300 + 30)
            else:
                parts["cwip_to_assets"] = self._clamp(50 + cwip_ratio * 100)

        # Incremental ROE
        inc_roe = row.get("_hist_incremental_roe")
        if inc_roe is not None:
            parts["incremental_roe"] = self._clamp(inc_roe * 2 + 20)

        if not parts:
            return 50.0, metrics_used

        # Weighted score using sector-specific weights
        total_w = 0.0
        weighted_sum = 0.0
        for key, score_val in parts.items():
            w = weights.get(key, 0.05)
            weighted_sum += score_val * w
            total_w += w
        result = weighted_sum / total_w if total_w > 0 else 50.0

        # Negative EPS penalty
        eps = row.get("eps")
        if eps is not None and eps < 0:
            result = min(result, 40.0)

        return round(result, 2), metrics_used

    def _score_institutional(self, row: dict) -> tuple[float | None, dict]:
        """Merged ownership + smart money: static levels + flows + sustained trends."""
        parts: dict[str, float] = {}

        # Promoter level (static)
        promoter = row.get("promoter_holding")
        if promoter is not None:
            if 50 <= promoter <= 75:
                parts["promoter_level"] = 80.0
            elif promoter > 75:
                parts["promoter_level"] = 60.0
            elif promoter >= 30:
                parts["promoter_level"] = 65.0
            else:
                parts["promoter_level"] = 40.0

        # FII level + flow + sustained
        fii = row.get("fii_holding")
        if fii is not None:
            parts["fii_level"] = self._clamp(fii * 3 + 20)

        fii_chg = row.get("fii_holding_change")
        if fii_chg is not None:
            parts["fii_flow"] = self._clamp(50 + fii_chg * 20)

        fii_4q = row.get("_hist_fii_trend_4q")
        if fii_4q is not None:
            parts["fii_sustained"] = self._clamp(50 + fii_4q * 10)

        # DII level + flow
        dii = row.get("dii_holding")
        if dii is not None:
            parts["dii_level"] = self._clamp(dii * 3 + 15)

        dii_chg = row.get("dii_holding_change")
        if dii_chg is not None:
            parts["dii_flow"] = self._clamp(50 + dii_chg * 20)

        # Promoter flow + sustained
        prom_chg = row.get("promoter_holding_change")
        if prom_chg is not None:
            parts["promoter_flow"] = self._clamp(50 + prom_chg * 25)

        promoter_4q = row.get("_hist_promoter_trend_4q")
        if promoter_4q is not None:
            parts["promoter_sustained"] = self._clamp(50 + promoter_4q * 12)

        # Shareholder trend
        sh_yoy = row.get("num_shareholders_change_yoy")
        if sh_yoy is not None:
            if sh_yoy > 100:
                parts["shareholder_trend"] = 35.0
            elif sh_yoy > 50:
                parts["shareholder_trend"] = 45.0
            elif sh_yoy > 0:
                parts["shareholder_trend"] = 60.0
            elif sh_yoy > -20:
                parts["shareholder_trend"] = 55.0
            else:
                parts["shareholder_trend"] = 40.0

        if not parts:
            return None, {}

        weights = {
            "promoter_level": 0.20, "fii_level": 0.15, "fii_flow": 0.15,
            "fii_sustained": 0.10, "dii_level": 0.10, "dii_flow": 0.10,
            "promoter_flow": 0.05, "promoter_sustained": 0.05, "shareholder_trend": 0.10,
        }
        total_w = sum(weights.get(k, 0.05) for k in parts)
        score = sum(parts[k] * weights.get(k, 0.05) for k in parts) / total_w

        # Pledging penalty
        pledged = row.get("pledged_promoter_pct")
        if pledged is not None:
            if pledged > 40:
                score = max(0, score - 30)
            elif pledged > 20:
                score = max(0, score - 15)

        # Low free-float penalty
        public = row.get("public_holding")
        if public is not None and public < 15:
            score = max(0, score - 10)

        return round(score, 2), parts

    def _score_valuation_v2(
        self,
        row: dict,
        sector: str,
        sector_medians: dict[str, dict[str, float]],
        industry_medians: dict[str, dict[str, float]],
        all_div_yields: list[float],
    ) -> tuple[float | None, dict, float | None]:
        """Deduplicated valuation score with PEG ratio. Returns (score, parts, peg_ratio)."""
        parts: dict[str, float] = {}
        peg_ratio = None
        weights = _SECTOR_VALUATION_WEIGHTS.get(sector, _SECTOR_VALUATION_WEIGHTS["DEFAULT"])

        pe_val = row.get("pe_ratio")
        pb_val = row.get("price_to_book")
        dy_val = row.get("dividend_yield")
        ind = str(row.get("industry") or "").strip()
        medians = sector_medians.get(sector, {})

        # PEG ratio
        earnings_growth = row.get("earnings_growth")
        eg_cagr = row.get("_hist_profit_growth_3y_cagr")
        growth_rate = None
        if earnings_growth is not None and earnings_growth > 0:
            growth_rate = earnings_growth * 100
        elif eg_cagr is not None and eg_cagr > 0:
            growth_rate = eg_cagr * 100

        if pe_val is not None and pe_val > 0 and growth_rate is not None:
            peg_ratio = pe_val / max(growth_rate, 5.0)
            parts["peg"] = self._clamp(100 - peg_ratio * 25)

        # PE relative to sector/industry median
        if pe_val is not None and pe_val > 0:
            ind_pe = (industry_medians.get(ind, {}).get("pe") if ind else None)
            ref_pe = ind_pe or medians.get("pe", 25.0)
            ratio = pe_val / max(ref_pe, 1.0)
            parts["pe_relative"] = self._clamp(100 - ratio * 50)

        # Forward PE discount
        improved_fwd_eps = row.get("_hist_improved_forward_eps")
        lp = row.get("last_price")
        if improved_fwd_eps and lp and lp > 0:
            row["synthetic_forward_pe"] = lp / improved_fwd_eps
        fpe = row.get("forward_pe") or row.get("synthetic_forward_pe")
        if fpe is not None and pe_val is not None and pe_val > 0 and fpe > 0:
            discount = 1 - (fpe / pe_val)
            parts["forward_pe"] = self._clamp(50 + discount * 200)

        # P/B relative
        if pb_val is not None and pb_val > 0:
            ind_pb = (industry_medians.get(ind, {}).get("pb") if ind else None)
            ref_pb = ind_pb or medians.get("pb", 4.0)
            ratio = pb_val / max(ref_pb, 0.5)
            parts["pb_relative"] = self._clamp(100 - ratio * 40)

        # Dividend yield percentile
        if dy_val is not None and dy_val > 0 and all_div_yields:
            parts["div_yield"] = self._percentile_rank(all_div_yields, dy_val)

        # Penalise loss-making companies: if EPS is negative and P/E is
        # unavailable, the score relies only on P/B / dividend yield which
        # can be misleadingly high.  Apply a penalty so "cheap" ≠ "good".
        eps = row.get("eps")
        if eps is not None and eps < 0 and pe_val is None:
            parts["loss_penalty"] = 20.0

        if not parts:
            return None, {}, None

        total_w = sum(weights.get(k, 0.10) for k in parts)
        score = sum(parts[k] * weights.get(k, 0.10) for k in parts) / total_w
        return round(score, 2), parts, peg_ratio

    def _score_growth_v2(
        self,
        row: dict,
        sector: str,
    ) -> tuple[float | None, dict]:
        """Growth score with 5Y compounding and sector-specific weights."""
        parts: dict[str, float] = {}

        # Revenue CAGR: blend 3Y and 5Y
        csg_3y = row.get("compounded_sales_growth_3y")
        sg_yoy = row.get("sales_growth_yoy")
        rg = row.get("revenue_growth")
        rev_cagr_5y = row.get("_hist_5y_revenue_cagr")

        sales_signal_3y = None
        if csg_3y is not None:
            sales_signal_3y = csg_3y / 100.0
        elif sg_yoy is not None:
            sales_signal_3y = sg_yoy
        elif rg is not None:
            sales_signal_3y = rg

        rev_parts: list[tuple[float, float]] = []
        if sales_signal_3y is not None:
            capped = min(sales_signal_3y, 0.50)
            rev_parts.append((self._clamp(50 + capped * 200), 0.50))
        if rev_cagr_5y is not None:
            capped = min(rev_cagr_5y, 0.50)
            rev_parts.append((self._clamp(50 + capped * 200), 0.50))
        if rev_parts:
            rw = sum(w for _, w in rev_parts)
            parts["revenue_cagr"] = self._clamp(sum(s * w / rw for s, w in rev_parts))

        # Profit CAGR: blend 3Y and 5Y
        profit_cagr_3y = row.get("_hist_profit_growth_3y_cagr")
        profit_cagr_5y = row.get("_hist_5y_profit_cagr")

        prof_parts: list[tuple[float, float]] = []
        if profit_cagr_3y is not None and isinstance(profit_cagr_3y, (int, float)):
            capped = min(profit_cagr_3y, 0.50)
            prof_parts.append((self._clamp(50 + capped * 200), 0.50))
        if profit_cagr_5y is not None:
            capped = min(profit_cagr_5y, 0.50)
            prof_parts.append((self._clamp(50 + capped * 200), 0.50))
        if prof_parts:
            pw = sum(w for _, w in prof_parts)
            parts["profit_cagr"] = self._clamp(sum(s * w / pw for s, w in prof_parts))

        # Consistency
        sales_cons = row.get("_hist_sales_growth_consistency", 0)
        profit_cons = row.get("_hist_profit_growth_consistency", 0)
        if sales_cons > 0 or profit_cons > 0:
            parts["consistency"] = self._clamp((sales_cons + profit_cons) / 10.0 * 100)

        # 5Y compounding bonus
        if rev_cagr_5y is not None and profit_cagr_5y is not None:
            if rev_cagr_5y > 0.15 and profit_cagr_5y > 0.15:
                parts["compounding_bonus"] = 85.0
            elif rev_cagr_5y > 0.10 and profit_cagr_5y > 0.10:
                parts["compounding_bonus"] = 70.0

        if parts:
            w = _SECTOR_GROWTH_WEIGHTS.get(sector, _SECTOR_GROWTH_WEIGHTS["DEFAULT"])
            tw = sum(w.get(k, 0.10) for k in parts)
            score = sum(parts[k] * w.get(k, 0.10) for k in parts) / tw
            return round(score, 2), parts

        # No fundamental growth data available — return None instead of
        # proxying via price returns (which would double-count with momentum layer)
        return None, {}

    def _score_momentum_v2(
        self,
        short_term_momentum: float,
        pos_52w: float | None,
        pct_3m: float | None,
        pct_1y: float | None,
        all_pct_3m: list[float],
        all_pct_1y: list[float],
    ) -> float:
        """Short/medium-term momentum (3M and 1Y only).

        3Y/5Y returns removed — those are long-term capital appreciation,
        already captured by the Growth and Valuation layers.
        """
        multi_parts: list[tuple[float, float]] = []
        if pct_3m is not None and all_pct_3m:
            multi_parts.append((self._percentile_rank(all_pct_3m, pct_3m), 0.55))
        if pct_1y is not None and all_pct_1y:
            multi_parts.append((self._percentile_rank(all_pct_1y, pct_1y), 0.45))

        if multi_parts:
            mp_w = sum(w for _, w in multi_parts)
            multi_score = self._clamp(sum(s * w / mp_w for s, w in multi_parts))
            if pos_52w is not None:
                # short-term 10%, 52W position 15%, multi-period 75%
                return self._clamp(short_term_momentum * 0.10 + pos_52w * 0.15 + multi_score * 0.75)
            return self._clamp(short_term_momentum * 0.15 + multi_score * 0.85)
        elif pos_52w is not None:
            return self._clamp(short_term_momentum * 0.60 + pos_52w * 0.40)
        return short_term_momentum

    def _score_risk(
        self,
        row: dict,
        liquidity: float,
        volatility_score: float | None,
    ) -> tuple[float | None, dict]:
        """Gate-based risk score: liquidity, volatility, pledging, free-float, EPS."""
        parts: dict[str, float] = {}

        # Liquidity gate
        traded_val = row.get("traded_value")
        if traded_val is not None:
            if traded_val < 10_00_000:
                parts["liquidity"] = 30.0
            elif traded_val < 1_00_00_000:
                parts["liquidity"] = 50.0
            else:
                parts["liquidity"] = min(liquidity, 100.0)
        else:
            parts["liquidity"] = min(liquidity, 80.0)

        # Volatility (inverse = stability premium)
        if volatility_score is not None:
            parts["volatility"] = volatility_score

        # Pledging risk
        pledged = row.get("pledged_promoter_pct")
        if pledged is not None:
            if pledged > 40:
                parts["pledging"] = 20.0
            elif pledged > 20:
                parts["pledging"] = 50.0
            else:
                parts["pledging"] = 80.0

        # Free-float
        public = row.get("public_holding")
        if public is not None:
            if public < 15:
                parts["free_float"] = 40.0
            elif public < 25:
                parts["free_float"] = 60.0
            else:
                parts["free_float"] = 80.0

        # Negative EPS
        eps = row.get("eps")
        if eps is not None:
            parts["eps_sign"] = 80.0 if eps > 0 else 30.0

        if not parts:
            return None, {}

        weights = {
            "liquidity": 0.25, "volatility": 0.25, "pledging": 0.20,
            "free_float": 0.15, "eps_sign": 0.15,
        }
        total_w = sum(weights.get(k, 0.10) for k in parts)
        score = sum(parts[k] * weights.get(k, 0.10) for k in parts) / total_w
        return round(score, 2), parts

    def _apply_quality_gates(self, row: dict) -> float | None:
        """Hard score caps for structural red flags. Returns max cap or None."""
        caps: list[float] = []

        # Negative FCF for 3+ consecutive years
        fcf_streak = row.get("_hist_negative_fcf_streak", 0)
        if fcf_streak >= 3:
            caps.append(40.0)

        # ROE < 5% for 3+ years
        roe_streak = row.get("_hist_low_roe_streak", 0)
        if roe_streak >= 3:
            caps.append(45.0)

        # Extreme pledging
        pledged = row.get("pledged_promoter_pct")
        if pledged is not None and pledged > 40:
            caps.append(30.0)

        # Negative EPS + negative OCF → hard cap at 35
        eps = row.get("eps")
        ocf = row.get("cash_from_operations") or row.get("operating_cash_flow")
        if eps is not None and eps < 0 and ocf is not None and ocf < 0:
            caps.append(35.0)

        # Negative EPS alone (loss-making) → cap at 55
        if eps is not None and eps < 0:
            caps.append(55.0)

        return min(caps) if caps else None

    def _classify_lynch(self, row: dict, sector: str) -> str:
        """Peter Lynch stock classification."""
        mcap = row.get("market_cap") or 0
        rev_cagr = row.get("_hist_5y_revenue_cagr") or row.get("_hist_profit_growth_3y_cagr")
        profit_cagr = row.get("_hist_5y_profit_cagr") or row.get("_hist_profit_growth_3y_cagr")
        opm_std = row.get("_hist_opm_std_5y")
        pb = row.get("price_to_book")
        net_profit_cons = row.get("_hist_profit_growth_consistency", 0)

        # Turnaround: was unprofitable, now profitable
        np_latest = row.get("_net_profit_latest")
        profit_cagr_3y = row.get("_hist_profit_growth_3y_cagr")
        eps = row.get("eps")
        if profit_cagr_3y is not None and profit_cagr_3y > 0.3 and net_profit_cons <= 2:
            if eps is not None and eps > 0:
                return "turnaround"

        # Asset play: P/B < 0.7 or net cash heavy
        cash = row.get("total_cash")
        debt = row.get("total_debt")
        if pb is not None and pb < 0.7:
            return "asset_play"
        if cash and debt and mcap and mcap > 0:
            net_cash_pct = ((cash - debt) / (mcap * 1e7)) * 100
            if net_cash_pct > 30:
                return "asset_play"

        # Fast grower
        if rev_cagr is not None and profit_cagr is not None:
            if rev_cagr > 0.15 and profit_cagr > 0.15:
                return "fast_grower"

        # Cyclical
        if sector in _CYCLICAL_SECTORS and opm_std is not None and opm_std > 5:
            return "cyclical"

        # Slow grower
        if rev_cagr is not None and rev_cagr < 0.05 and mcap >= 20000:
            return "slow_grower"

        # Loss-making companies should not default to stalwart
        if eps is not None and eps < 0:
            return "speculative"

        # Stalwart (default for profitable companies with moderate growth)
        return "stalwart"

    # ── Technical Score & Action Tag ──

    @staticmethod
    def _compute_ema(prices: list[float], period: int) -> list[float]:
        """Compute exponential moving average for a price series."""
        if not prices or period <= 0:
            return []
        multiplier = 2.0 / (period + 1)
        ema = [prices[0]]
        for i in range(1, len(prices)):
            ema.append(prices[i] * multiplier + ema[-1] * (1.0 - multiplier))
        return ema

    @staticmethod
    def _compute_sma(prices: list[float], period: int) -> float | None:
        """Simple moving average of the last `period` values."""
        if len(prices) < period:
            return None
        return sum(prices[-period:]) / period

    def _compute_technical_score(
        self,
        history: list[dict],
        sector: str,
        row: dict,
    ) -> tuple[float | None, dict]:
        """Compute technical confirmation score (0-100) from price history.

        Returns (score, details_dict) or (None, {}) if insufficient data.
        history: list of {"date", "close", "volume"} sorted by date ascending.
        """
        if len(history) < 30:
            return None, {}

        closes = [h["close"] for h in history]
        volumes = [h["volume"] for h in history]
        current_price = closes[-1]

        details: dict[str, float] = {}

        # ── RSI-14 ──
        rsi_score = None
        rsi_value = None
        if len(closes) >= 15:
            gains, losses = [], []
            for i in range(1, len(closes)):
                delta = closes[i] - closes[i - 1]
                gains.append(max(delta, 0))
                losses.append(max(-delta, 0))

            period = 14
            if len(gains) >= period:
                # Seed from first 14 periods (SMA)
                avg_gain = sum(gains[:period]) / period
                avg_loss = sum(losses[:period]) / period
                # Wilder's smoothing through all remaining periods
                for i in range(period, len(gains)):
                    avg_gain = (avg_gain * (period - 1) + gains[i]) / period
                    avg_loss = (avg_loss * (period - 1) + losses[i]) / period

                if avg_loss == 0:
                    rsi_value = 100.0
                else:
                    rs = avg_gain / avg_loss
                    rsi_value = 100.0 - (100.0 / (1.0 + rs))

                details["rsi_14"] = round(rsi_value, 2)

                # Score RSI: oversold is bullish, overbought is bearish
                if rsi_value < 30:
                    rsi_score = self._clamp(70 + (30 - rsi_value) * 1.0)
                elif rsi_value > 70:
                    rsi_score = self._clamp(30 - (rsi_value - 70) * 1.0)
                else:
                    rsi_score = self._clamp(50 + (rsi_value - 50) * 1.5)

        # ── MACD ──
        macd_score = None
        if len(closes) >= 35:
            ema_12 = self._compute_ema(closes, 12)
            ema_26 = self._compute_ema(closes, 26)
            macd_line = [ema_12[i] - ema_26[i] for i in range(len(closes))]
            signal_line = self._compute_ema(macd_line[25:], 9)  # 9-EMA of MACD after 26 warmup

            if signal_line:
                macd_val = macd_line[-1]
                signal_val = signal_line[-1]
                histogram = macd_val - signal_val

                details["macd"] = round(macd_val, 4)
                details["macd_signal"] = round(signal_val, 4)
                details["macd_histogram"] = round(histogram, 4)

                if macd_val > signal_val and macd_val > 0:
                    macd_score = 82.0  # strong bullish
                elif macd_val > signal_val and macd_val <= 0:
                    macd_score = 62.0  # bullish crossover from weakness
                elif macd_val <= signal_val and macd_val > 0:
                    macd_score = 38.0  # bearish crossover from strength
                else:
                    macd_score = 18.0  # strong bearish

                # Histogram acceleration bonus
                if len(macd_line) >= 2 and len(signal_line) >= 2:
                    prev_hist = macd_line[-2] - signal_line[-2]
                    if abs(histogram) > abs(prev_hist) and histogram > 0:
                        macd_score = min(macd_score + 8.0, 95.0)
                    elif abs(histogram) > abs(prev_hist) and histogram < 0:
                        macd_score = max(macd_score - 8.0, 5.0)

        # ── Volume Trend ──
        vol_score = None
        avg_vol_20 = 0.0
        vol_up = False
        vol_surge = False
        if len(volumes) >= 50 and len(closes) >= 20:
            recent_vol = volumes[-20:]
            medium_vol = volumes[-50:]
            avg_vol_20 = sum(recent_vol) / 20 if any(v > 0 for v in recent_vol) else 0
            avg_vol_50 = sum(medium_vol) / 50 if any(v > 0 for v in medium_vol) else 0

            price_trend_20d = (closes[-1] - closes[-20]) / closes[-20] if closes[-20] > 0 else 0
            price_up = price_trend_20d > 0.01
            vol_up = avg_vol_20 > avg_vol_50 * 1.05 if avg_vol_50 > 0 else False

            # Volume surge: last day's volume vs 20-day average
            last_vol = volumes[-1] if volumes[-1] else 0
            vol_surge = (last_vol > avg_vol_20 * 1.5) if avg_vol_20 > 0 else False

            if price_up and vol_up:
                vol_score = 80.0  # confirmed advance
            elif price_up and not vol_up:
                vol_score = 45.0  # weak advance
            elif not price_up and vol_up:
                vol_score = 25.0  # confirmed decline
            else:
                vol_score = 55.0  # declining on low interest

            details["vol_ratio_20_50"] = round(avg_vol_20 / max(avg_vol_50, 1), 2)

        # ── Moving Average Trend ──
        ma_score = None
        ema_20 = self._compute_ema(closes, 20)[-1] if len(closes) >= 20 else None
        sma_50 = self._compute_sma(closes, 50)
        sma_200 = self._compute_sma(closes, 200)

        if ema_20 is not None and sma_50 is not None:
            if sma_200 is not None:
                # Full MA alignment scoring
                if current_price > ema_20 > sma_50 > sma_200:
                    ma_score = 90.0  # all aligned bullish
                elif current_price > sma_50 > sma_200:
                    ma_score = 72.0  # above key MAs
                elif current_price > sma_50 and sma_50 < sma_200:
                    ma_score = 52.0  # mixed — above 50 but death cross
                elif current_price < sma_50 and sma_50 > sma_200:
                    ma_score = 38.0  # below 50 but golden cross intact
                elif current_price < sma_50 < sma_200:
                    ma_score = 20.0  # weak — below both
                elif current_price < ema_20 < sma_50 < sma_200:
                    ma_score = 10.0  # all aligned bearish
                else:
                    ma_score = 50.0  # neutral/mixed

                # Golden/death cross recency bonus
                if len(closes) >= 220:
                    sma_50_prev = self._compute_sma(closes[:-20], 50)
                    sma_200_prev = self._compute_sma(closes[:-20], 200)
                    if sma_50_prev and sma_200_prev:
                        if sma_50_prev < sma_200_prev and sma_50 > sma_200:
                            ma_score = min(ma_score + 10.0, 95.0)  # recent golden cross
                        elif sma_50_prev > sma_200_prev and sma_50 < sma_200:
                            ma_score = max(ma_score - 10.0, 5.0)  # recent death cross
            else:
                # No 200-DMA available — use price vs 20/50 only
                if current_price > ema_20 > sma_50:
                    ma_score = 75.0
                elif current_price > sma_50:
                    ma_score = 60.0
                elif current_price < ema_20 < sma_50:
                    ma_score = 25.0
                else:
                    ma_score = 45.0

        # ── Support/Resistance & Breakout/Breakdown Detection ──
        sr_score = None
        breakout_signal = "none"
        high_52w = row.get("high_52w")
        low_52w = row.get("low_52w")

        # 20-day high/low for short-term breakout detection
        high_20d = max(closes[-20:]) if len(closes) >= 20 else None
        low_20d = min(closes[-20:]) if len(closes) >= 20 else None

        if high_52w and low_52w and current_price > 0 and high_52w > low_52w:
            dist_high_pct = (high_52w - current_price) / current_price * 100
            dist_low_pct = (current_price - low_52w) / current_price * 100

            # BREAKOUT: price at or above 52W high
            if dist_high_pct <= 0:
                if vol_up or vol_surge:
                    breakout_signal = "breakout"
                    sr_score = 85.0
                else:
                    breakout_signal = "approaching_breakout"
                    sr_score = 65.0
            # APPROACHING BREAKOUT: within 3% of 52W high
            elif dist_high_pct < 3:
                if vol_up or vol_surge:
                    breakout_signal = "approaching_breakout"
                    sr_score = 75.0
                else:
                    breakout_signal = "resistance"
                    sr_score = 40.0
            # NEAR RESISTANCE: within 5% of 52W high
            elif dist_high_pct < 5:
                if vol_up:
                    breakout_signal = "approaching_breakout"
                    sr_score = 70.0
                else:
                    breakout_signal = "resistance"
                    sr_score = 35.0
            # BREAKDOWN: price at or below 52W low
            elif dist_low_pct <= 0:
                if vol_up or vol_surge:
                    breakout_signal = "breakdown"
                    sr_score = 10.0
                else:
                    breakout_signal = "approaching_breakdown"
                    sr_score = 25.0
            # APPROACHING BREAKDOWN: within 3% of 52W low
            elif dist_low_pct < 3:
                breakout_signal = "approaching_breakdown"
                sr_score = 20.0 if vol_up else 30.0
            # NEAR SUPPORT: within 5% of 52W low
            elif dist_low_pct < 5:
                breakout_signal = "support"
                sr_score = 60.0
            # MID-RANGE
            elif dist_high_pct < 15:
                sr_score = 60.0
            elif dist_low_pct < 15:
                sr_score = 40.0
            else:
                sr_score = 50.0

            details["dist_to_52w_high_pct"] = round(dist_high_pct, 2)
            details["dist_to_52w_low_pct"] = round(dist_low_pct, 2)

        # Short-term 20-day breakout (if no 52W signal detected)
        if breakout_signal == "none" and high_20d and current_price >= high_20d and (vol_up or vol_surge):
            breakout_signal = "approaching_breakout"

        details["breakout_signal"] = breakout_signal

        # ── Weighted composite with sector adjustments ──
        base_weights = {
            "rsi": 0.25, "macd": 0.20, "volume": 0.20, "ma": 0.20, "sr": 0.15,
        }

        # Sector-conditional adjustments
        if sector in ("Energy", "Materials", "Commodities"):
            base_weights["macd"] += 0.05
            base_weights["rsi"] -= 0.05
        elif sector in ("FMCG", "Utilities"):
            base_weights["volume"] -= 0.05
            base_weights["sr"] += 0.05
        elif sector in ("IT", "Healthcare"):
            base_weights["rsi"] += 0.05
            base_weights["macd"] -= 0.05

        components = {}
        if rsi_score is not None:
            components["rsi"] = rsi_score
        if macd_score is not None:
            components["macd"] = macd_score
        if vol_score is not None:
            components["volume"] = vol_score
        if ma_score is not None:
            components["ma"] = ma_score
        if sr_score is not None:
            components["sr"] = sr_score

        if not components:
            return None, {}

        total_w = sum(base_weights.get(k, 0.10) for k in components)
        tech_score = sum(components[k] * base_weights.get(k, 0.10) for k in components) / total_w
        tech_score = round(self._clamp(tech_score), 2)

        details["technical_score"] = tech_score
        if rsi_value is not None:
            details["rsi_14"] = round(rsi_value, 2)

        return tech_score, details

    @staticmethod
    def _compute_action_tag(
        score: float,
        tech_score: float | None,
        quality_score: float,
        momentum_score: float,
        quality_cap: float | None,
        data_quality: str,
        *,
        quality_sub: float | None = None,
        valuation_sub: float | None = None,
        growth_sub: float | None = None,
        institutional_sub: float | None = None,
        risk_sub: float | None = None,
        tech_details: dict | None = None,
        breakout_signal: str = "none",
    ) -> tuple[str, str]:
        """Assign a user-facing action tag based on fundamentals + technicals.

        Returns (tag, reasoning).
        """
        tech_details = tech_details or {}

        # Identify top 2 contributing layers
        layer_scores = [
            ("Quality", quality_score),
            ("Valuation", valuation_sub),
            ("Growth", growth_sub),
            ("Momentum", momentum_score),
            ("Institutional", institutional_sub),
            ("Risk", risk_sub),
        ]
        valid_layers = [(n, s) for n, s in layer_scores if s is not None]
        valid_layers.sort(key=lambda x: x[1], reverse=True)
        top_layers = valid_layers[:2]
        top_str = " and ".join(f"{n} ({s:.0f})" for n, s in top_layers) if top_layers else "limited data"

        # Build tech summary components
        rsi_val = tech_details.get("rsi_14")
        macd_hist = tech_details.get("macd_histogram")

        rsi_overbought = rsi_val is not None and rsi_val > 70
        rsi_oversold = rsi_val is not None and rsi_val < 30

        # Positive tech signals (for "confirm" sentence)
        positive_parts: list[str] = []
        # Caution signals (for separate caveat)
        caution_parts: list[str] = []

        if rsi_val is not None:
            if rsi_overbought:
                caution_parts.append(f"RSI at {rsi_val:.0f} indicates overbought conditions")
            elif rsi_oversold:
                caution_parts.append(f"RSI at {rsi_val:.0f} indicates oversold conditions")
            else:
                positive_parts.append(f"RSI at {rsi_val:.0f}")

        macd_val = tech_details.get("macd")
        macd_signal = tech_details.get("macd_signal")
        if macd_val is not None and macd_signal is not None:
            if macd_val > macd_signal and macd_val > 0:
                positive_parts.append("strong bullish MACD")
            elif macd_val > macd_signal and macd_val <= 0:
                positive_parts.append("MACD crossover (recovery starting)")
            elif macd_val <= macd_signal and macd_val > 0:
                caution_parts.append("MACD weakening from bullish territory")
            else:
                caution_parts.append("bearish MACD")
        elif macd_hist is not None:
            # Fallback if only histogram is available
            if macd_hist > 0:
                positive_parts.append("bullish MACD")
            else:
                caution_parts.append("bearish MACD")

        tech_positive = ", ".join(positive_parts) if positive_parts else None
        tech_caution = ", ".join(caution_parts) if caution_parts else None
        # Fallback flat summary for simpler reason strings
        all_parts = positive_parts + caution_parts
        tech_summary = ", ".join(all_parts) if all_parts else "limited technical data"

        # Trend alignment for reasoning text
        trend = DiscoverStockScraper._compute_trend_alignment(score, tech_score)
        tech_confirms = {"aligned": "confirms", "divergent": "shows divergence at"}.get(
            trend or "", "conflicts with"
        )

        # 1. Quality gate triggered
        if quality_cap is not None and score <= quality_cap:
            if quality_cap <= 35:
                return ("Avoid",
                        f"Avoid — Structural red flags cap the score at {quality_cap:.0f}. "
                        f"Fundamental weakness is confirmed regardless of technical position.")
            if quality_cap <= 45:
                return ("Deteriorating",
                        f"Deteriorating — Quality gates limit the score to {quality_cap:.0f}. "
                        f"Core fundamentals show persistent weakness.")
            if quality_cap <= 55:
                return ("Underperformer",
                        f"Underperformer — Quality constraints cap the score at {quality_cap:.0f}. "
                        f"Fundamentals show weakness in key areas but not structural failure. "
                        f"{top_str}.")

        # 2. No technical data
        if tech_score is None:
            if score >= 70:
                return ("Outperformer",
                        f"Outperformer — Strong fundamentals (score {score:.0f}) driven by {top_str}. "
                        f"Technical data unavailable for confirmation.")
            if score >= 50:
                return ("Hold",
                        f"Hold — Decent fundamentals (score {score:.0f}) but technical data unavailable. "
                        f"Primary drivers: {top_str}.")
            if quality_score < 40:
                return ("Deteriorating",
                        f"Deteriorating — Weak fundamentals (score {score:.0f}, quality {quality_score:.0f}). "
                        f"No technical data to suggest otherwise.")
            return ("Watchlist",
                    f"Watchlist — Score {score:.0f} with {top_str} as drivers. "
                    f"Technical data unavailable; monitor for clarity.")

        # 3. Data quality downgrade — limited data caps at Hold
        if data_quality == "limited":
            return ("Hold — Low Data",
                    f"Hold — Limited data quality restricts confidence. "
                    f"Score {score:.0f}, tech {tech_score:.0f}. "
                    f"More data needed before a stronger signal.")

        # 4. Full data — decision tree
        tag: str
        reason: str

        if score >= 75 and tech_score >= 60:
            tag = "Strong Outperformer"
            reason = f"Strong Outperformer — {top_str} are the primary drivers. "
            if tech_positive:
                reason += f"{tech_positive} {tech_confirms} the fundamental strength. "
            if tech_caution:
                reason += f"However, {tech_caution} — monitor for short-term pullback risk. "
            if not tech_positive and not tech_caution:
                reason += "Reflects both long-term quality and favorable positioning. "

        elif score >= 60 and tech_score >= 45:
            tag = "Outperformer"
            reason = f"Outperformer — Solid fundamentals (score {score:.0f}) led by {top_str}. "
            if tech_positive:
                reason += f"{tech_positive} {tech_confirms} at tech score {tech_score:.0f}. "
            if tech_caution:
                reason += f"Note: {tech_caution}. "

        elif score >= 50 and tech_score >= 50:
            tag = "Accumulate"
            reason = f"Accumulate — Fundamentals ({score:.0f}) and technicals ({tech_score:.0f}) are both moderately positive. {top_str} lead the score. "
            if tech_caution:
                reason += f"Watch: {tech_caution}. "
            else:
                reason += "Suitable for gradual position building. "

        elif 45 <= score < 55 and 40 <= tech_score < 50:
            tag = "Neutral"
            reason = f"Neutral — Fundamentals ({score:.0f}) and technicals ({tech_score:.0f}) are both middling. {top_str} lead. "
            if tech_caution:
                reason += f"{tech_caution}. "
            elif tech_positive:
                reason += f"{tech_positive}. "
            reason += "No directional edge."

        elif score >= 55 and tech_score < 40:
            tag = "Watchlist"
            reason = f"Watchlist — Fundamentals are solid ({score:.0f}) led by {top_str}, but technicals are weak ({tech_score:.0f}). "
            if tech_caution:
                reason += f"{tech_caution}. "
            reason += "Technical weakness may present a better entry point if fundamentals hold."

        elif score < 45 and tech_score >= 65:
            tag = "Momentum Only"
            reason = f"Momentum Only — Price momentum (tech {tech_score:.0f}) is not supported by fundamentals ({score:.0f}). "
            if tech_positive:
                reason += f"{tech_positive} drive short-term strength. "
            if tech_caution:
                reason += f"However, {tech_caution}. "
            reason += "High risk — momentum may not sustain without quality backing."

        elif score < 35:
            tag = "Avoid"
            reason = f"Avoid — Weak fundamentals ({score:.0f}) with {top_str}. "
            reason += f"Technical position ({tech_score:.0f}) does not offset structural weakness."

        elif score < 50 and tech_score < 40:
            tag = "Deteriorating"
            reason = f"Deteriorating — Both fundamentals ({score:.0f}) and technicals ({tech_score:.0f}) are weak. "
            if tech_caution:
                reason += f"{tech_caution} confirms the downward pressure. "
            elif tech_positive:
                reason += f"{tech_positive} offer limited bright spots. "
            reason += f"{top_str}."

        else:
            tag = "Hold"
            reason = f"Hold — Mixed signals with score {score:.0f} and tech {tech_score:.0f}. {top_str} are the main drivers. "
            if tech_positive:
                reason += f"{tech_positive} provide some support. "
            if tech_caution:
                reason += f"However, {tech_caution}. "
            reason += "No strong case for action in either direction."

        # 5. Breakout/Breakdown modifier — upgrades or downgrades the tag
        if breakout_signal == "breakout":
            upgrades = {
                "Outperformer": "Strong Outperformer",
                "Accumulate": "Outperformer",
                "Hold": "Accumulate",
                "Neutral": "Accumulate",
                "Watchlist": "Hold",
            }
            if tag in upgrades:
                old_tag = tag
                tag = upgrades[tag]
                reason += f" Upgraded from {old_tag} due to confirmed 52W breakout with volume."

        elif breakout_signal == "breakdown":
            downgrades = {
                "Strong Outperformer": "Outperformer",
                "Outperformer": "Hold",
                "Accumulate": "Watchlist",
                "Hold": "Deteriorating",
                "Neutral": "Deteriorating",
            }
            if tag in downgrades:
                old_tag = tag
                tag = downgrades[tag]
                reason += f" Downgraded from {old_tag} due to 52W breakdown."

        elif breakout_signal == "approaching_breakout":
            reason += " Approaching 52W high — potential breakout if volume confirms."

        elif breakout_signal == "approaching_breakdown":
            reason += " Approaching 52W low — monitor for breakdown risk."

        return tag, reason

    @staticmethod
    def _compute_trend_alignment(
        score: float,
        tech_score: float | None,
    ) -> str | None:
        """Determine if fundamentals and technicals are aligned, divergent, or conflicting."""
        if tech_score is None:
            return None
        # Cross-zone conflict: one strong positive, other strong negative
        if (score >= 70 and tech_score < 30) or (score < 35 and tech_score >= 70):
            return "conflicting"
        abs_diff = abs(score - tech_score)
        if abs_diff >= 35:
            return "conflicting"
        if abs_diff >= 18:
            return "divergent"
        return "aligned"

    @staticmethod
    def _compute_score_confidence(
        score: float,
        tech_score: float | None,
        data_quality: str,
        metrics_used: int,
    ) -> str:
        """Determine confidence level based on data quality and fundamental-technical agreement."""
        if data_quality == "limited":
            return "low"
        if tech_score is None:
            if score >= 65 and metrics_used >= 4 and data_quality == "full":
                return "medium"
            return "low"
        abs_diff = abs(score - tech_score)
        if abs_diff < 15 and data_quality == "full" and metrics_used >= 3:
            # Strong agreement with good data
            if (score >= 65 and tech_score >= 55) or (score < 40 and tech_score < 40):
                return "high"
            return "medium"
        if abs_diff < 25 and data_quality == "full" and metrics_used >= 3:
            return "medium"
        return "low"

    @staticmethod
    def _detect_market_regime(
        nifty_price: float | None = None,
        nifty_200dma: float | None = None,
    ) -> str:
        """Regime detection based on Nifty 50 vs its 200-DMA.

        Returns 'bull' if Nifty is >3% above 200-DMA, 'bear' if >3% below,
        'neutral' otherwise. Falls back to 'neutral' if data unavailable.

        nifty_price and nifty_200dma are pre-fetched async and passed in.
        """
        if nifty_price is None or nifty_200dma is None or nifty_200dma <= 0:
            return "neutral"
        deviation = (nifty_price - nifty_200dma) / nifty_200dma
        if deviation > 0.03:
            return "bull"
        elif deviation < -0.03:
            return "bear"
        return "neutral"

    @staticmethod
    def _generate_tags(
        row: dict,
        *,
        quality_score: float,
        valuation_score: float | None,
        growth_score: float | None,
        momentum_score: float,
        institutional_score: float | None,
        risk_score: float | None,
        lynch_classification: str,
        market_regime: str,
        sector: str,
        pct_change_5y: float | None,
        peg_ratio: float | None,
        paper_profits: bool = False,
        sector_pe_median: float = 25.0,
    ) -> list[str]:
        """Generate prioritized tags for the 6-layer model."""
        # (priority, tag_name)
        tagged: list[tuple[int, str]] = []

        mcap = row.get("market_cap")
        pe = row.get("pe_ratio")
        roe = row.get("roe")
        roce = row.get("roce")
        dte = row.get("debt_to_equity")
        eps = row.get("eps")
        dy = row.get("dividend_yield")
        promoter = row.get("promoter_holding")
        fii = row.get("fii_holding")
        dii = row.get("dii_holding")
        public = row.get("public_holding")
        pledged = row.get("pledged_promoter_pct")
        fii_chg = row.get("fii_holding_change")
        dii_chg = row.get("dii_holding_change")
        prom_chg = row.get("promoter_holding_change")
        rg = row.get("revenue_growth")
        eg = row.get("earnings_growth")
        opm_trend = row.get("_hist_opm_trend_3y")
        debt_traj = row.get("_hist_debt_trajectory")
        ocf_years = row.get("_hist_ocf_positive_years")
        profit_cons = row.get("_hist_profit_growth_consistency", 0)
        sales_cons = row.get("_hist_sales_growth_consistency", 0)
        cwip_to_assets = row.get("_hist_cwip_to_assets")
        opm_std = row.get("_hist_opm_std_5y")
        inc_roe = row.get("_hist_incremental_roe")
        rev_cagr_5y = row.get("_hist_5y_revenue_cagr")
        prof_cagr_5y = row.get("_hist_5y_profit_cagr")
        fcf = row.get("free_cash_flow")
        total_cash = row.get("total_cash")
        total_debt = row.get("total_debt")
        rec_mean = row.get("analyst_recommendation_mean")
        analyst_count = row.get("analyst_count")
        target = row.get("analyst_target_mean")
        price = row.get("last_price")
        ma_50 = row.get("fifty_day_avg")
        ma_200 = row.get("two_hundred_day_avg")
        beta_val = row.get("beta")

        # Priority 1: Market Cap
        if mcap is not None:
            if mcap >= 20000:
                tagged.append((1, "Large Cap"))
            elif mcap >= 5000:
                tagged.append((1, "Mid Cap"))
            else:
                tagged.append((1, "Small Cap"))

        # Priority 2: Lynch classification
        lynch_tag_map = {
            "turnaround": "Turnaround Story",
            "fast_grower": "Fast Grower",
            "stalwart": "Stalwart",
            "cyclical": "Cyclical Play",
            "asset_play": "Asset Play",
            "slow_grower": "Slow Grower",
        }
        if lynch_classification in lynch_tag_map:
            tagged.append((2, lynch_tag_map[lynch_classification]))

        # Priority 2: Quality
        if quality_score >= 75:
            tagged.append((2, "High Quality"))
        if paper_profits:
            tagged.append((2, "Paper Profits"))
        if pledged is not None and pledged > 20:
            tagged.append((2, "High Pledge Risk"))
        if sales_cons >= 4 and profit_cons >= 4:
            tagged.append((2, "Consistent Compounder"))

        # Priority 3: Compounding
        if rev_cagr_5y is not None and rev_cagr_5y > 0.15 and prof_cagr_5y is not None and prof_cagr_5y > 0.15:
            tagged.append((3, "Decade Compounder"))
        elif (rev_cagr_5y is not None and rev_cagr_5y > 0.15) or (prof_cagr_5y is not None and prof_cagr_5y > 0.15):
            tagged.append((3, "5Y Wealth Creator"))

        # Priority 3: Quality signals
        if dte is not None and dte == 0 and eps is not None and eps > 0:
            tagged.append((3, "Debt Free"))
        if fcf is not None and mcap and mcap > 0:
            fcf_yield = (fcf / (mcap * 1e7)) * 100
            if fcf_yield > 5:
                tagged.append((3, "FCF Machine"))
        if total_cash is not None and total_debt is not None and mcap and mcap > 0:
            net_cash_pct = ((total_cash - total_debt) / (mcap * 1e7)) * 100
            if net_cash_pct > 10:
                tagged.append((3, "Cash Rich"))

        # Priority 3: Valuation
        if peg_ratio is not None and peg_ratio < 0.8 and quality_score > 50:
            tagged.append((3, "PEG Bargain"))
        if pe is not None and pe > 0 and pe < sector_pe_median * 0.7 and quality_score > 55:
            tagged.append((3, "Value Pick"))
        if rec_mean is not None and rec_mean <= 1.5 and analyst_count is not None and analyst_count >= 10:
            tagged.append((3, "Analyst Strong Buy"))
        if inc_roe is not None and inc_roe > 20:
            tagged.append((3, "Capital Allocator"))

        # Priority 4: Growth & Valuation
        if peg_ratio is not None and 0.8 <= peg_ratio <= 1.2 and growth_score is not None and growth_score > 60:
            tagged.append((4, "Growth at Fair Price"))
        if peg_ratio is not None and peg_ratio > 2.5:
            tagged.append((4, "Richly Valued"))
        elif pe is not None and pe > sector_pe_median * 1.8:
            tagged.append((4, "Richly Valued"))
        if rg is not None and rg >= 0.15:
            tagged.append((4, "Growth Stock"))
        elif eg is not None and eg >= 0.20:
            tagged.append((4, "Growth Stock"))
        if dy is not None and dy >= 2.0:
            tagged.append((4, "High Dividend"))
        if target and price and price > 0 and analyst_count and analyst_count >= 5:
            upside = ((target - price) / price) * 100
            if upside >= 25:
                tagged.append((4, "Analyst Undervalued"))

        # Priority 4: Quality trends
        if opm_trend is not None and opm_trend >= 3:
            tagged.append((4, "Margin Expansion"))
        if debt_traj is not None and debt_traj < -0.15:
            tagged.append((4, "Deleveraging"))
        if ocf_years is not None and ocf_years >= 4:
            tagged.append((4, "Strong Cash Flow"))

        # Priority 4: Divergence
        if momentum_score >= 65 and quality_score < 40:
            tagged.append((4, "Momentum Without Quality"))
        if quality_score >= 65 and momentum_score < 35:
            tagged.append((4, "Quality Weak Momentum"))

        # Priority 4: Sector-specific
        if sector == "Financials" and opm_trend is not None and opm_trend > 0:
            tagged.append((4, "NIM Expander"))
        if sector == "Healthcare" and cwip_to_assets is not None and cwip_to_assets > 0.10:
            tagged.append((4, "R&D Intensive"))
        if sector in ("Industrials", "Auto") and cwip_to_assets is not None and cwip_to_assets > 0.15:
            tagged.append((4, "Capacity Expansion"))
        if sector == "FMCG" and opm_std is not None and opm_std < 2:
            tagged.append((4, "Margin Fortress"))

        # Priority 5: Regime
        if market_regime == "bear":
            if quality_score > 75 and risk_score is not None and risk_score > 70:
                tagged.append((5, "Defensive Pick"))
            if beta_val is not None and beta_val < 0.7 and quality_score > 70:
                tagged.append((5, "Bear Market Resilient"))

        # Priority 5: Ownership
        if promoter is not None and promoter >= 55:
            tagged.append((5, "High Promoter"))
        if fii is not None and fii >= 20:
            tagged.append((5, "FII Favorite"))
        if dii is not None and dii >= 25:
            tagged.append((5, "DII Backed"))
        if prom_chg is not None and prom_chg > 0.5:
            tagged.append((5, "Promoter Buying"))
        if fii_chg is not None and fii_chg > 0.5:
            tagged.append((5, "FII Buying"))
        if dii_chg is not None and dii_chg > 0.5:
            tagged.append((5, "DII Buying"))

        # Priority 5: Technicals
        if ma_50 and ma_200 and price:
            if ma_50 > ma_200 and price > ma_50:
                tagged.append((5, "Bullish Trend"))
            elif ma_50 < ma_200 and price < ma_50:
                tagged.append((5, "Bearish Trend"))

        if public is not None and public < 25:
            tagged.append((5, "Low Free Float"))

        # Priority 6
        if eps is not None and eps < 0:
            tagged.append((6, "Negative EPS"))

        # Sort by priority, deduplicate, limit to 15
        seen: set[str] = set()
        result: list[str] = []
        for _, tag in sorted(tagged, key=lambda x: x[0]):
            if tag not in seen:
                seen.add(tag)
                result.append(tag)
            if len(result) >= 15:
                break
        return result

    @staticmethod
    def _generate_why_narrative(
        score: float,
        row: dict,
        *,
        quality_score: float,
        valuation_score: float | None,
        growth_score: float | None,
        momentum_score: float,
        institutional_score: float | None,
        risk_score: float | None,
        lynch_classification: str,
        sector: str,
        sector_percentile: float | None,
        peg_ratio: float | None,
        pct_change_5y: float | None,
        market_regime: str,
        paper_profits: bool = False,
    ) -> str:
        """Generate rich, insightful 2-3 sentence narrative."""
        # Lynch type opener
        lynch_labels = {
            "fast_grower": "A fast-growing",
            "stalwart": "A steady",
            "slow_grower": "A mature",
            "cyclical": "A cyclical",
            "turnaround": "A turnaround",
            "asset_play": "An asset-rich",
        }
        opener = lynch_labels.get(lynch_classification, "A")
        sector_label = sector if sector != "Other" else "company"

        strengths: list[str] = []
        risks: list[str] = []

        # Key metrics
        roe = row.get("roe")
        roce = row.get("roce")
        rev_cagr_5y = row.get("_hist_5y_revenue_cagr")
        prof_cagr_5y = row.get("_hist_5y_profit_cagr")
        opm_std = row.get("_hist_opm_std_5y")
        opm_chg = row.get("opm_change")
        dte = row.get("debt_to_equity")
        fii_dir = row.get("_hist_fii_trend_direction")
        pledged = row.get("pledged_promoter_pct")
        eps = row.get("eps")
        rg = row.get("revenue_growth")
        cfo = row.get("cash_from_operations")

        # Strengths
        if prof_cagr_5y is not None and prof_cagr_5y > 0.15:
            strengths.append(f"5Y profit CAGR of {prof_cagr_5y*100:.0f}%")
        elif roe is not None and roe >= 18:
            strengths.append(f"ROE of {roe:.0f}%")
        if opm_std is not None and opm_std < 3:
            strengths.append("stable margins")
        elif roce is not None and roce >= 20:
            strengths.append(f"ROCE of {roce:.0f}%")
        if dte is not None and dte == 0:
            strengths.append("debt-free")
        elif dte is not None and dte <= 0.3 and dte > 0:
            strengths.append("low debt")
        if rg is not None and rg >= 0.15:
            strengths.append(f"revenue growing {rg*100:.0f}%")

        # Valuation context
        val_text = ""
        if peg_ratio is not None:
            if peg_ratio < 1.0:
                val_text = f"PEG {peg_ratio:.1f} — undervalued for its growth"
            elif peg_ratio <= 1.5:
                val_text = f"PEG {peg_ratio:.1f} — fairly valued"
            elif peg_ratio <= 2.5:
                val_text = f"PEG {peg_ratio:.1f} — growth priced in"
            else:
                val_text = f"PEG {peg_ratio:.1f} — premium valuation"

        # Smart money
        smart_text = ""
        if fii_dir == "increasing":
            smart_text = "FII accumulating"
        elif fii_dir == "decreasing":
            smart_text = "FII reducing exposure"

        # Risks
        if paper_profits:
            risks.append("negative cash flow despite reported profits")
        if pledged is not None and pledged > 20:
            risks.append(f"{pledged:.0f}% promoter shares pledged")
        if opm_chg is not None and opm_chg < -3:
            risks.append(f"margins contracting (OPM {opm_chg:+.1f}% YoY)")
        if eps is not None and eps < 0:
            risks.append("loss-making")
        if dte is not None and dte > 2.0:
            risks.append(f"high leverage (D/E {dte:.1f})")

        # Build narrative
        strength_str = " with " + " and ".join(strengths[:2]) if strengths else ""
        parts = [f"{opener} {sector_label} play{strength_str}."]

        if val_text:
            parts.append(f"{val_text}.")
        if smart_text:
            parts.append(f"{smart_text}.")
        if risks:
            parts.append(f"Key risk: {risks[0]}.")

        # Sector percentile context
        if sector_percentile is not None:
            if sector_percentile >= 80:
                parts.append(f"Top {100-sector_percentile:.0f}% in {sector}.")
            elif sector_percentile <= 20:
                parts.append(f"Bottom {sector_percentile:.0f}% in {sector}.")

        # Cap at 3 sentences total
        narrative = " ".join(parts[:4])
        return narrative

    def _compute_scores(
        self,
        rows: list[dict],
        *,
        volatility_data: dict[str, dict] | None = None,
        nifty_price: float | None = None,
        nifty_200dma: float | None = None,
        price_history: dict[str, list[dict]] | None = None,
    ) -> list[dict]:
        if not rows:
            return []

        vol_data = volatility_data or {}
        ph_data = price_history or {}

        # ── Market regime detection ──
        market_regime = self._detect_market_regime(nifty_price, nifty_200dma)

        # ── Sanitize numeric fields (Screener.in can return strings) ──
        _NUMERIC_FIELDS = {
            "pe_ratio", "price_to_book", "debt_to_equity", "roe", "roce",
            "eps", "dividend_yield", "last_price", "percent_change",
            "volume", "traded_value", "high_52w", "low_52w", "market_cap",
            "beta", "forward_pe", "gross_margins", "operating_margins",
            "profit_margins", "revenue_growth", "earnings_growth",
            "promoter_holding", "fii_holding", "dii_holding",
            "total_cash", "total_debt", "total_revenue",
            "free_cash_flow", "operating_cash_flow", "payout_ratio",
            "sales_growth_yoy", "profit_growth_yoy", "opm_change",
            "interest_coverage", "compounded_sales_growth_3y", "compounded_profit_growth_3y",
            "total_assets", "asset_growth_yoy", "reserves_growth", "debt_direction",
            "cash_from_operations", "cash_from_investing", "cash_from_financing",
            "num_shareholders_change_qoq", "num_shareholders_change_yoy",
        }
        for r in rows:
            for field in _NUMERIC_FIELDS:
                v = r.get(field)
                if v is not None and not isinstance(v, (int, float)):
                    try:
                        r[field] = float(v)
                    except (ValueError, TypeError):
                        r[field] = None

        # ── Derive missing fields from available data ──
        _derived_pe = 0
        _derived_pb = 0
        _derived_roce = 0
        _defaulted_fii = 0
        _defaulted_dii = 0
        for r in rows:
            price = r.get("last_price")
            # PE = price / EPS when PE is missing but both inputs exist
            if r.get("pe_ratio") is None and price and price > 0:
                eps = r.get("eps")
                if eps is not None and eps > 0:
                    r["pe_ratio"] = round(price / eps, 2)
                    _derived_pe += 1
            # P/B = price / (EPS / ROE) when P/B is missing
            if r.get("price_to_book") is None and price and price > 0:
                eps = r.get("eps")
                roe = r.get("roe")
                if eps is not None and eps > 0 and roe is not None and roe > 0:
                    book_value = eps / (roe / 100.0)
                    if book_value > 0:
                        r["price_to_book"] = round(price / book_value, 2)
                        _derived_pb += 1
            # ROCE approximation
            if r.get("roce") is None:
                roe = r.get("roe")
                if roe is not None:
                    dte = r.get("debt_to_equity")
                    if dte is not None and dte >= 0:
                        r["roce"] = round(roe * (1 + dte), 2)
                    else:
                        r["roce"] = round(roe, 2)
                    _derived_roce += 1
            # Default FII/DII to 0 when promoter data exists
            if r.get("promoter_holding") is not None:
                if r.get("fii_holding") is None:
                    r["fii_holding"] = 0.0
                    _defaulted_fii += 1
                if r.get("dii_holding") is None:
                    r["dii_holding"] = 0.0
                    _defaulted_dii += 1

        if _derived_pe or _derived_pb or _derived_roce or _defaulted_fii or _defaulted_dii:
            logger.info(
                "Derived fields: PE=%d, P/B=%d, ROCE=%d, FII→0=%d, DII→0=%d",
                _derived_pe, _derived_pb, _derived_roce, _defaulted_fii, _defaulted_dii,
            )

        # ── Pre-compute sector medians for PE, PB, and D/E ──
        sector_pe: dict[str, list[float]] = {}
        sector_pb: dict[str, list[float]] = {}
        sector_de: dict[str, list[float]] = {}
        def _safe_float(v: object) -> float | None:
            if v is None:
                return None
            try:
                return float(v)
            except (ValueError, TypeError):
                return None

        for r in rows:
            sector = str(r.get("sector") or "Other")
            pe = _safe_float(r.get("pe_ratio"))
            if pe is not None and pe > 0:
                sector_pe.setdefault(sector, []).append(pe)
            pb = _safe_float(r.get("price_to_book"))
            if pb is not None and pb > 0:
                sector_pb.setdefault(sector, []).append(pb)
            de = _safe_float(r.get("debt_to_equity"))
            if de is not None and de >= 0:
                sector_de.setdefault(sector, []).append(de)

        all_pe = [v for vals in sector_pe.values() for v in vals]
        all_pb = [v for vals in sector_pb.values() for v in vals]
        all_de = [v for vals in sector_de.values() for v in vals]
        overall_pe_med = statistics.median(all_pe) if len(all_pe) >= 3 else 25.0
        overall_pb_med = statistics.median(all_pb) if len(all_pb) >= 3 else 4.0
        overall_de_med = statistics.median(all_de) if len(all_de) >= 3 else 1.0

        sector_medians: dict[str, dict[str, float]] = {}
        all_sectors = set(str(r.get("sector") or "Other") for r in rows)
        for sector in all_sectors:
            pe_vals = sector_pe.get(sector, [])
            pb_vals = sector_pb.get(sector, [])
            de_vals = sector_de.get(sector, [])
            sector_medians[sector] = {
                "pe": statistics.median(pe_vals) if len(pe_vals) >= 3 else overall_pe_med,
                "pb": statistics.median(pb_vals) if len(pb_vals) >= 3 else overall_pb_med,
                "de": statistics.median(de_vals) if len(de_vals) >= 3 else overall_de_med,
            }

        # ── Pre-compute INDUSTRY-level medians ──
        industry_pe: dict[str, list[float]] = {}
        industry_pb: dict[str, list[float]] = {}
        for r in rows:
            ind = str(r.get("industry") or "").strip()
            if not ind:
                continue
            pe = _safe_float(r.get("pe_ratio"))
            if pe is not None and 0 < pe < 500:
                industry_pe.setdefault(ind, []).append(pe)
            pb = _safe_float(r.get("price_to_book"))
            if pb is not None and pb > 0:
                industry_pb.setdefault(ind, []).append(pb)

        industry_medians: dict[str, dict[str, float]] = {}
        for ind in set(str(r.get("industry") or "").strip() for r in rows if r.get("industry")):
            pe_vals = industry_pe.get(ind, [])
            pb_vals = industry_pb.get(ind, [])
            industry_medians[ind] = {
                "pe": statistics.median(pe_vals) if len(pe_vals) >= 5 else None,
                "pb": statistics.median(pb_vals) if len(pb_vals) >= 5 else None,
            }

        # ── Pre-compute dividend yield percentiles ──
        all_div_yields: list[float] = []
        for r in rows:
            dy = _safe_float(r.get("dividend_yield"))
            if dy is not None and dy > 0:
                all_div_yields.append(dy)

        # ── Compute synthetic forward PE where Yahoo forward PE is missing ──
        _synth_fpe_count = 0
        for r in rows:
            if r.get("forward_pe") is None:
                pe = _safe_float(r.get("pe_ratio"))
                pg = _safe_float(r.get("profit_growth_yoy")) or _safe_float(r.get("earnings_growth"))
                if pe is not None and pe > 0 and pg is not None:
                    capped_pg = max(pg, -0.50)
                    denom = 1 + capped_pg
                    if denom > 0.2:
                        r["synthetic_forward_pe"] = round(pe / denom, 2)
                        _synth_fpe_count += 1
        if _synth_fpe_count:
            logger.info("Computed synthetic forward PE for %d stocks", _synth_fpe_count)

        # ── Pre-compute robust percentile data for daily momentum (winsorized) ──
        all_pcts_raw = [float(r.get("percent_change") or 0.0) for r in rows]
        if all_pcts_raw:
            if len(all_pcts_raw) >= 8:
                pct_lo = self._quantile(all_pcts_raw, 0.05)
                pct_hi = self._quantile(all_pcts_raw, 0.95)
            else:
                pct_lo = min(all_pcts_raw)
                pct_hi = max(all_pcts_raw)
        else:
            pct_lo = -5.0
            pct_hi = 5.0
        if pct_lo > pct_hi:
            pct_lo, pct_hi = pct_hi, pct_lo
        all_pcts = [max(pct_lo, min(pct_hi, v)) for v in all_pcts_raw]

        # ── Pre-compute multi-day momentum percentile data ──
        all_momentum_5d: list[float] = []
        all_momentum_20d: list[float] = []
        momentum_5d_by_sym: dict[str, float] = {}
        momentum_20d_by_sym: dict[str, float] = {}
        for r in rows:
            sym = str(r.get("symbol") or "")
            vd = vol_data.get(sym)
            if vd:
                m5 = vd.get("momentum_5d")
                if m5 is not None:
                    all_momentum_5d.append(m5)
                    momentum_5d_by_sym[sym] = m5
                m20 = vd.get("momentum_20d")
                if m20 is not None:
                    all_momentum_20d.append(m20)
                    momentum_20d_by_sym[sym] = m20

        # ── Pre-compute 52W position scores ──
        pos_52w_by_sym: dict[str, float] = {}
        all_pos_52w: list[float] = []
        for r in rows:
            sym = str(r.get("symbol") or "")
            price = r.get("last_price")
            high_52w = r.get("high_52w")
            low_52w = r.get("low_52w")
            pos = self._score_52w_position(price, high_52w, low_52w)
            if pos is not None:
                pos_52w_by_sym[sym] = pos
                all_pos_52w.append(pos)

        # ── Pre-compute multi-day liquidity data ──
        all_avg_vol_5d: list[float] = []
        all_avg_vol_20d: list[float] = []
        avg_vol_5d_by_sym: dict[str, float] = {}
        avg_vol_20d_by_sym: dict[str, float] = {}
        for r in rows:
            sym = str(r.get("symbol") or "")
            vd = vol_data.get(sym)
            if vd:
                v5 = vd.get("avg_vol_5d")
                if v5 is not None and v5 > 0:
                    log_v5 = math.log1p(v5)
                    all_avg_vol_5d.append(log_v5)
                    avg_vol_5d_by_sym[sym] = log_v5
                v20 = vd.get("avg_vol_20d")
                if v20 is not None and v20 > 0:
                    log_v20 = math.log1p(v20)
                    all_avg_vol_20d.append(log_v20)
                    avg_vol_20d_by_sym[sym] = log_v20

        all_tv_logs = [
            math.log1p(max(0.0, float(r.get("traded_value") or 0.0)))
            for r in rows
        ]
        all_vol_logs = [
            math.log1p(max(0.0, float(r.get("volume") or 0.0)))
            for r in rows
        ]

        # ── Pre-compute volatility ──
        all_std_devs: list[float] = []
        for r in rows:
            sym = str(r.get("symbol") or "")
            vd = vol_data.get(sym)
            if vd and vd.get("std_dev") is not None:
                all_std_devs.append(vd["std_dev"])

        # ── Pre-compute multi-period growth data (including 5Y) ──
        all_pct_3m: list[float] = []
        all_pct_1y: list[float] = []
        all_pct_3y: list[float] = []
        all_pct_5y: list[float] = []
        pct_3m_by_sym: dict[str, float] = {}
        pct_1y_by_sym: dict[str, float] = {}
        pct_3y_by_sym: dict[str, float] = {}
        pct_5y_by_sym: dict[str, float] = {}
        for r in rows:
            sym = str(r.get("symbol") or "")
            vd = vol_data.get(sym)
            if not vd:
                continue
            p3m = vd.get("pct_change_3m")
            if p3m is not None:
                pct_3m_by_sym[sym] = p3m
                all_pct_3m.append(p3m)
            p1y = vd.get("pct_change_1y")
            if p1y is not None:
                pct_1y_by_sym[sym] = p1y
                all_pct_1y.append(p1y)
            p3y = vd.get("pct_change_3y")
            if p3y is not None:
                pct_3y_by_sym[sym] = p3y
                all_pct_3y.append(p3y)
            p5y = vd.get("pct_change_5y")
            if p5y is not None:
                pct_5y_by_sym[sym] = p5y
                all_pct_5y.append(p5y)

        # Track sector scores for sector_leader tag and sector percentile
        sector_best: dict[str, list[tuple[float, str]]] = {}

        out: list[dict] = []
        for row in rows:
            symbol = str(row.get("symbol") or "")
            sector = str(row.get("sector") or "Other")

            # Mine JSONB annual tables for multi-year trend signals
            historical_metrics = self._compute_historical_metrics(row)
            for _hk, _hv in historical_metrics.items():
                row[f"_hist_{_hk}"] = _hv

            pct_raw = float(row.get("percent_change") or 0.0)
            pct = max(pct_lo, min(pct_hi, pct_raw))
            tv_log = math.log1p(max(0.0, float(row.get("traded_value") or 0.0)))
            vol_log = math.log1p(max(0.0, float(row.get("volume") or 0.0)))

            # ── Short-term momentum (5d/20d/daily) ──
            daily_momentum = self._clamp(self._percentile_rank(all_pcts, pct))
            has_m5 = symbol in momentum_5d_by_sym and all_momentum_5d
            has_m20 = symbol in momentum_20d_by_sym and all_momentum_20d
            if has_m5 and has_m20:
                m5_pctile = self._percentile_rank(all_momentum_5d, momentum_5d_by_sym[symbol])
                m20_pctile = self._percentile_rank(all_momentum_20d, momentum_20d_by_sym[symbol])
                short_term_momentum = self._clamp(m5_pctile * 0.50 + m20_pctile * 0.30 + daily_momentum * 0.20)
            elif has_m5:
                m5_pctile = self._percentile_rank(all_momentum_5d, momentum_5d_by_sym[symbol])
                short_term_momentum = self._clamp(m5_pctile * 0.65 + daily_momentum * 0.35)
            else:
                short_term_momentum = daily_momentum

            # ── Per-stock return data ──
            pct_change_3m = pct_3m_by_sym.get(symbol)
            pct_change_1y = pct_1y_by_sym.get(symbol)
            pct_change_3y = pct_3y_by_sym.get(symbol)
            pct_change_5y = pct_5y_by_sym.get(symbol)

            # ── Momentum v2 (short/medium-term only — 3M and 1Y) ──
            momentum = self._score_momentum_v2(
                short_term_momentum,
                pos_52w_by_sym.get(symbol),
                pct_change_3m, pct_change_1y,
                all_pct_3m, all_pct_1y,
            )

            # ── Liquidity ──
            has_v5 = symbol in avg_vol_5d_by_sym and all_avg_vol_5d
            has_v20 = symbol in avg_vol_20d_by_sym and all_avg_vol_20d
            if has_v5 and has_v20:
                v5_pctile = self._percentile_rank(all_avg_vol_5d, avg_vol_5d_by_sym[symbol])
                v20_pctile = self._percentile_rank(all_avg_vol_20d, avg_vol_20d_by_sym[symbol])
                liquidity = self._clamp(v5_pctile * 0.70 + v20_pctile * 0.30)
            elif has_v5:
                v5_pctile = self._percentile_rank(all_avg_vol_5d, avg_vol_5d_by_sym[symbol])
                liquidity = self._clamp(v5_pctile)
            else:
                tv_percentile = self._percentile_rank(all_tv_logs, tv_log)
                vol_percentile = self._percentile_rank(all_vol_logs, vol_log)
                liquidity = self._clamp((tv_percentile * 0.60) + (vol_percentile * 0.40))

            # ── Quality (merged fundamentals + financial health + earnings quality) ──
            quality_score, metrics_used = self._score_quality(row, sector, sector_medians)
            if metrics_used > 0:
                coverage = metrics_used / 5.0
                quality_score = self._clamp(
                    self._shrink_to_neutral(quality_score, coverage),
                )

            # ── OCF earnings quality gate ──
            paper_profits = False
            _ocf = row.get("cash_from_operations") or row.get("operating_cash_flow")
            _eps_val = row.get("eps")
            if _ocf is not None and _eps_val is not None and _eps_val > 0 and _ocf < 0:
                quality_score = min(quality_score, 45.0)
                paper_profits = True

            # ── Volatility (lower std_dev -> higher score = stability premium) ──
            vd = vol_data.get(symbol)
            volatility_score: float | None = None
            if vd:
                sd = vd.get("std_dev")
                if sd is not None and all_std_devs:
                    raw_pctile = self._percentile_rank(all_std_devs, sd)
                    vol_raw = self._clamp(100.0 - raw_pctile)
                    vol_raw = self._adjust_volatility_for_cap(vol_raw, row.get("market_cap"))
                    beta = row.get("beta")
                    if beta is not None:
                        beta_score = self._clamp(100 - beta * 35)
                        volatility_score = self._clamp(vol_raw * 0.70 + beta_score * 0.30)
                    else:
                        volatility_score = vol_raw
                    traded_val = row.get("traded_value")
                    if traded_val is not None and traded_val < 10_00_000:
                        volatility_score = min(volatility_score, 50.0)

            # ── Valuation v2 (with PEG ratio) ──
            valuation_score, _val_parts, peg_ratio = self._score_valuation_v2(
                row, sector, sector_medians, industry_medians, all_div_yields,
            )

            # ── Growth v2 (with 5Y compounding) ──
            growth_score, _growth_parts = self._score_growth_v2(row, sector)

            # ── Institutional (merged ownership + smart money) ──
            institutional_score, _inst_parts = self._score_institutional(row)

            # ── Risk ──
            risk_score, _risk_parts = self._score_risk(row, liquidity, volatility_score)

            # ── Lynch classification ──
            lynch_class = self._classify_lynch(row, sector)

            # ── Quality gates ──
            quality_cap = self._apply_quality_gates(row)

            # ── 6-Layer weighted total ──
            layer_weights = _SECTOR_LAYER_WEIGHTS.get(sector, _SECTOR_LAYER_WEIGHTS["DEFAULT"])

            scores_map: dict[str, float | None] = {
                "quality": quality_score,
                "valuation": valuation_score,
                "growth": growth_score,
                "momentum": momentum,
                "institutional": institutional_score,
                "risk": risk_score,
            }

            # Dynamic reweighting: exclude unavailable components
            # For growth, use a penalty default (25) instead of skipping —
            # missing growth data should not be rewarded via weight redistribution
            available_weights: dict[str, float] = {}
            for k, w in layer_weights.items():
                if scores_map.get(k) is not None:
                    available_weights[k] = w
                elif k == "growth":
                    scores_map[k] = 25.0
                    growth_score = 25.0
                    available_weights[k] = w
                elif k == "quality" and metrics_used > 0:
                    available_weights[k] = w
                elif k == "momentum":
                    available_weights[k] = w

            if not available_weights:
                combined_signal = (momentum + liquidity) / 2.0
                total = self._clamp(combined_signal, lo=20.0, hi=80.0)
            else:
                total_w = sum(available_weights.values())
                total = sum(
                    (scores_map.get(k) or 50.0) * (w / total_w)
                    for k, w in available_weights.items()
                )

            # Shrink quality influence when coverage is low
            if metrics_used > 0 and metrics_used < 4:
                signal_only = (momentum * 0.6 + liquidity * 0.4)
                blend_factor = metrics_used / 5.0
                total = (total * blend_factor) + (signal_only * (1.0 - blend_factor))

            source_status = str(row.get("source_status") or "limited").strip().lower()
            if metrics_used == 0 and source_status == "primary":
                source_status = "fallback"
            status_penalty = 0.0
            if source_status == "fallback":
                status_penalty = 5.0
            elif source_status == "limited":
                status_penalty = 12.0

            score = round(self._clamp(total - status_penalty), 2)

            # ── Confidence caps ──
            data_quality = "full"
            total_data_metrics = metrics_used
            if institutional_score is not None:
                total_data_metrics += 1
            if valuation_score is not None:
                total_data_metrics += 1

            if total_data_metrics == 0:
                score = min(score, 65.0)
                data_quality = "limited"
            elif total_data_metrics <= 2:
                score = min(score, 75.0)
                data_quality = "partial"
            elif total_data_metrics <= 4:
                data_quality = "partial"

            # Apply quality gates (hard caps)
            if quality_cap is not None:
                score = min(score, quality_cap)

            # ── Auto-tags (structured v2) ──
            med_pe = sector_medians.get(sector, {}).get("pe", 25.0)
            from app.services.tag_engine import generate_stock_tags
            tags_v2 = generate_stock_tags(
                row,
                quality_score=quality_score,
                valuation_score=valuation_score,
                growth_score=growth_score,
                momentum_score=momentum,
                institutional_score=institutional_score,
                risk_score=risk_score,
                lynch_classification=lynch_class,
                market_regime=market_regime,
                sector=sector,
                pct_change_5y=pct_change_5y,
                peg_ratio=peg_ratio,
                paper_profits=paper_profits,
                sector_pe_median=med_pe,
            )

            # Track sector leaders (top 3)
            sector_best.setdefault(sector, []).append((score, symbol))

            pct_change_1w = vd.get("pct_change_1w") if vd else None

            # Generate human-readable narrative (sector_percentile filled in post-pass)
            why_narrative = self._generate_why_narrative(
                score, row,
                quality_score=quality_score,
                valuation_score=valuation_score,
                growth_score=growth_score,
                momentum_score=momentum,
                institutional_score=institutional_score,
                risk_score=risk_score,
                lynch_classification=lynch_class,
                sector=sector,
                sector_percentile=None,
                peg_ratio=peg_ratio,
                pct_change_5y=pct_change_5y,
                market_regime=market_regime,
                paper_profits=paper_profits,
            )

            # ── Technical score (from price history) ──
            sym_history = ph_data.get(symbol, [])
            tech_score, tech_details = self._compute_technical_score(sym_history, sector, row)

            # ── Action tag ──
            # ── Breakout signal, trend alignment, confidence ──
            breakout_signal = tech_details.get("breakout_signal", "none")
            trend_alignment = self._compute_trend_alignment(score, tech_score)
            score_confidence = self._compute_score_confidence(
                score, tech_score, data_quality, metrics_used,
            )

            action_tag, action_tag_reasoning = self._compute_action_tag(
                score, tech_score,
                quality_score, momentum,
                quality_cap, data_quality,
                quality_sub=quality_score,
                valuation_sub=valuation_score,
                growth_sub=growth_score,
                institutional_sub=institutional_score,
                risk_sub=risk_score,
                tech_details=tech_details,
                breakout_signal=breakout_signal,
            )

            enriched = {
                **row,
                "score": score,
                "score_quality": round(quality_score, 2),
                "score_valuation": round(valuation_score, 2) if valuation_score is not None else None,
                "score_growth": round(growth_score, 2) if growth_score is not None else None,
                "score_momentum": round(momentum, 2),
                "score_institutional": round(institutional_score, 2) if institutional_score is not None else None,
                "score_risk": round(risk_score, 2) if risk_score is not None else None,
                "sector_percentile": None,  # filled in post-pass
                "lynch_classification": lynch_class,
                "percent_change_3m": pct_change_3m,
                "percent_change_1w": pct_change_1w,
                "percent_change_1y": pct_change_1y,
                "percent_change_3y": pct_change_3y,
                "percent_change_5y": pct_change_5y,
                "technical_score": tech_score,
                "rsi_14": tech_details.get("rsi_14"),
                "action_tag": action_tag,
                "action_tag_reasoning": action_tag_reasoning,
                "score_confidence": score_confidence,
                "trend_alignment": trend_alignment,
                "breakout_signal": breakout_signal,
                "score_breakdown": {
                    "quality": round(quality_score, 2),
                    "valuation": round(valuation_score, 2) if valuation_score is not None else None,
                    "growth": round(growth_score, 2) if growth_score is not None else None,
                    "momentum": round(momentum, 2),
                    "institutional": round(institutional_score, 2) if institutional_score is not None else None,
                    "risk": round(risk_score, 2) if risk_score is not None else None,
                    "technical_score": tech_score,
                    "rsi_14": tech_details.get("rsi_14"),
                    "action_tag": action_tag,
                    "action_tag_reasoning": action_tag_reasoning,
                    "score_confidence": score_confidence,
                    "trend_alignment": trend_alignment,
                    "breakout_signal": breakout_signal,
                    "52w_position": pos_52w_by_sym.get(symbol),
                    "combined_signal": round((momentum + liquidity) / 2.0, 2),
                    "quality_coverage": f"{metrics_used}/5",
                    "data_quality": data_quality,
                    "peg_ratio": round(peg_ratio, 2) if peg_ratio is not None else None,
                    "lynch_classification": lynch_class,
                    "market_regime": market_regime,
                    "why_narrative": why_narrative,
                },
                "tags_v2": tags_v2,
                "source_status": source_status,
            }
            out.append(enriched)

        # ── Post-pass: sector percentiles and sector leaders ──
        sector_scores: dict[str, list[float]] = {}
        for enriched in out:
            sec = str(enriched.get("sector") or "Other")
            sector_scores.setdefault(sec, []).append(enriched["score"])

        for enriched in out:
            sec = str(enriched.get("sector") or "Other")
            sec_scores = sector_scores.get(sec, [])
            if sec_scores:
                enriched["sector_percentile"] = round(
                    self._percentile_rank(sec_scores, enriched["score"]), 1
                )

        # Add sector_leader tag to top 3 scorers in each sector
        sector_leaders: set[str] = set()
        for sector, scores_list in sector_best.items():
            scores_list.sort(key=lambda x: -x[0])
            for rank_score, sym in scores_list[:3]:
                sector_leaders.add(sym)

        for enriched in out:
            symbol = str(enriched.get("symbol") or "")
            if symbol in sector_leaders:
                tv2 = enriched.get("tags_v2", [])
                if not any(t.get("tag") == "Sector Leader" for t in tv2):
                    tv2.insert(0, {
                        "tag": "Sector Leader",
                        "category": "classification",
                        "severity": "positive",
                        "priority": 0,
                        "confidence": 1.0,
                        "explanation": "Top 3 scorer in sector",
                        "expires_at": None,
                    })

        out.sort(
            key=lambda item: (
                -float(item.get("score") or 0.0),
                -float(item.get("percent_change") or 0.0),
                str(item.get("symbol") or ""),
            )
        )
        return out

    def _build_snapshot_row(
        self,
        stock: DiscoverStockDef,
        quote: dict,
        quote_source: str,
        *,
        fundamentals_enabled: bool | None = None,
    ) -> dict:
        use_fundamentals = bool(getattr(stock, "fundamentals_enabled", True))
        if fundamentals_enabled is not None:
            use_fundamentals = fundamentals_enabled

        if use_fundamentals:
            fundamentals, fundamentals_source = self._fetch_screener_fundamentals(stock.nse_symbol)
            time_mod.sleep(self._screener_batch_delay)

            # Yahoo v10 for EVERY stock: fills gaps + adds exclusive data
            try:
                yahoo_session = self._get_yahoo_session()
                yahoo = yahoo_session.get_stock_data(stock.nse_symbol)
                time_mod.sleep(self._yahoo_batch_delay)

                yahoo_fields_filled = 0

                # Fill missing Screener fields from Yahoo
                for field in ("pe_ratio", "price_to_book", "eps", "debt_to_equity",
                              "market_cap", "high_52w", "low_52w", "dividend_yield"):
                    if fundamentals.get(field) is None and yahoo.get(field) is not None:
                        fundamentals[field] = yahoo[field]
                        yahoo_fields_filled += 1

                # Add Yahoo-exclusive fields (always overwrite with Yahoo data)
                for field in ("beta", "free_cash_flow", "operating_cash_flow", "total_cash",
                              "total_debt", "total_revenue", "gross_margins", "operating_margins",
                              "profit_margins", "revenue_growth", "earnings_growth",
                              "forward_pe",
                              "analyst_target_mean", "analyst_count", "analyst_recommendation",
                              "analyst_recommendation_mean", "analyst_strong_buy", "analyst_buy",
                              "analyst_hold", "analyst_sell",
                              "payout_ratio", "fifty_day_avg", "two_hundred_day_avg"):
                    if yahoo.get(field) is not None:
                        fundamentals[field] = yahoo[field]
                        yahoo_fields_filled += 1

                if fundamentals_source == "unavailable":
                    fundamentals_source = "yahoo_fundamentals"
                elif fundamentals_source == "screener_in":
                    fundamentals_source = "screener_in+yahoo"

                logger.debug(
                    "Yahoo v10 OK for %s: %d fields enriched → source=%s",
                    stock.nse_symbol, yahoo_fields_filled, fundamentals_source,
                )
            except Exception as exc:
                logger.warning("Yahoo v10 failed for %s: %s", stock.nse_symbol, exc)
        else:
            fundamentals = {
                "pe_ratio": None, "roe": None, "roce": None,
                "debt_to_equity": None, "price_to_book": None,
                "eps": None, "market_cap": None, "high_52w": None,
                "low_52w": None, "dividend_yield": None,
            }
            fundamentals_source = "unavailable"

        fundamentals_count = sum(1 for k, v in fundamentals.items() if v is not None and not k.startswith("_"))

        source_status = "primary" if (fundamentals_source in ("screener_in", "screener_in+yahoo") and fundamentals_count >= 2) else "fallback"
        if fundamentals_count == 0 and quote_source not in {"nse_quote_api", "nse_bhavcopy"}:
            source_status = "limited"

        # Sector resolution: curated > Screener Broad Sector > existing > "Other"
        sector = stock.sector
        broad_sector_raw = fundamentals.pop("_screener_broad_sector", None)
        industry_raw = fundamentals.pop("_screener_industry", None)
        # Remove legacy key if present
        fundamentals.pop("_screener_sector", None)

        if stock.nse_symbol in _EXTRA_SECTOR_MAP:
            sector = _EXTRA_SECTOR_MAP[stock.nse_symbol]
        elif broad_sector_raw:
            mapped = _SCREENER_BROAD_SECTOR_MAP.get(broad_sector_raw.lower())
            if mapped:
                sector = mapped
            else:
                sector = _map_screener_sector(broad_sector_raw)
        elif sector in ("Diversified", "Other") and industry_raw:
            sector = _map_screener_sector(industry_raw)

        return {
            "market": "IN",
            "symbol": stock.nse_symbol,
            "display_name": stock.display_name,
            "sector": sector,
            "industry": industry_raw,
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
            "market_cap": fundamentals.get("market_cap"),
            "high_52w": fundamentals.get("high_52w"),
            "low_52w": fundamentals.get("low_52w"),
            "dividend_yield": fundamentals.get("dividend_yield"),
            # Shareholding
            "promoter_holding": fundamentals.get("promoter_holding"),
            "fii_holding": fundamentals.get("fii_holding"),
            "dii_holding": fundamentals.get("dii_holding"),
            "government_holding": fundamentals.get("government_holding"),
            "public_holding": fundamentals.get("public_holding"),
            "num_shareholders": fundamentals.get("num_shareholders"),
            "promoter_holding_change": fundamentals.get("promoter_holding_change"),
            "fii_holding_change": fundamentals.get("fii_holding_change"),
            "dii_holding_change": fundamentals.get("dii_holding_change"),
            # Yahoo-exclusive fundamentals
            "beta": fundamentals.get("beta"),
            "free_cash_flow": fundamentals.get("free_cash_flow"),
            "operating_cash_flow": fundamentals.get("operating_cash_flow"),
            "total_cash": fundamentals.get("total_cash"),
            "total_debt": fundamentals.get("total_debt"),
            "total_revenue": fundamentals.get("total_revenue"),
            "gross_margins": fundamentals.get("gross_margins"),
            "operating_margins": fundamentals.get("operating_margins"),
            "profit_margins": fundamentals.get("profit_margins"),
            "revenue_growth": fundamentals.get("revenue_growth"),
            "earnings_growth": fundamentals.get("earnings_growth"),
            "forward_pe": fundamentals.get("forward_pe"),
            # Analyst data
            "analyst_target_mean": fundamentals.get("analyst_target_mean"),
            "analyst_count": fundamentals.get("analyst_count"),
            "analyst_recommendation": fundamentals.get("analyst_recommendation"),
            "analyst_recommendation_mean": fundamentals.get("analyst_recommendation_mean"),
            "analyst_strong_buy": fundamentals.get("analyst_strong_buy"),
            "analyst_buy": fundamentals.get("analyst_buy"),
            "analyst_hold": fundamentals.get("analyst_hold"),
            "analyst_sell": fundamentals.get("analyst_sell"),
            # Technical
            "fifty_day_avg": fundamentals.get("fifty_day_avg"),
            "two_hundred_day_avg": fundamentals.get("two_hundred_day_avg"),
            "payout_ratio": fundamentals.get("payout_ratio"),
            "pledged_promoter_pct": fundamentals.get("pledged_promoter_pct"),
            # P&L derived signals (Screener.in)
            "sales_growth_yoy": fundamentals.get("sales_growth_yoy"),
            "profit_growth_yoy": fundamentals.get("profit_growth_yoy"),
            "opm_change": fundamentals.get("opm_change"),
            "interest_coverage": fundamentals.get("interest_coverage"),
            "compounded_sales_growth_3y": fundamentals.get("compounded_sales_growth_3y"),
            "compounded_profit_growth_3y": fundamentals.get("compounded_profit_growth_3y"),
            # Balance sheet derived signals (Screener.in)
            "total_assets": fundamentals.get("total_assets"),
            "asset_growth_yoy": fundamentals.get("asset_growth_yoy"),
            "reserves_growth": fundamentals.get("reserves_growth"),
            "debt_direction": fundamentals.get("debt_direction"),
            "cwip": fundamentals.get("cwip"),
            # Cash flow signals (Screener.in — fixes Yahoo OCF gap)
            "cash_from_operations": fundamentals.get("cash_from_operations"),
            "cash_from_investing": fundamentals.get("cash_from_investing"),
            "cash_from_financing": fundamentals.get("cash_from_financing"),
            # Full historical tables (JSONB)
            "pl_annual": fundamentals.get("pl_annual"),
            "bs_annual": fundamentals.get("bs_annual"),
            "cf_annual": fundamentals.get("cf_annual"),
            "shareholding_quarterly": fundamentals.get("shareholding_quarterly"),
            # Shareholder trends
            "num_shareholders_change_qoq": fundamentals.get("num_shareholders_change_qoq"),
            "num_shareholders_change_yoy": fundamentals.get("num_shareholders_change_yoy"),
            # Metadata
            "source_status": source_status,
            "source_timestamp": quote.get("source_timestamp") or datetime.now(timezone.utc),
            "primary_source": fundamentals_source,
            "secondary_source": quote_source,
        }

    def _fetch_one(self, stock: DiscoverStockDef) -> dict | None:
        quote = self._fetch_nse_quote(stock.nse_symbol)
        quote_source = "nse_quote_api"
        if quote is None:
            quote = self._fetch_yahoo_quote(stock.yahoo_symbol)
            quote_source = "yahoo_finance_api"
        if quote is None:
            return None
        return self._build_snapshot_row(stock, quote, quote_source)

    def fetch_raw_rows(self, on_batch: "Callable[[list[dict]], None] | None" = None, batch_size: int = 50) -> list[dict]:
        """Fetch quotes + fundamentals (sync I/O). Does NOT score.

        If *on_batch* is provided, it is called every *batch_size* rows with
        the latest batch for incremental DB visibility.
        """
        universe = self._build_effective_universe()
        bulk_quotes, _ = self._fetch_latest_bhavcopy_quotes()
        raw_rows: list[dict] = []
        # Counters for progress / diagnostic logging
        _total = len(universe)
        _processed = 0
        _yahoo_ok = 0
        _yahoo_fail = 0
        _yahoo_skip = 0
        _screener_ok = 0
        _screener_fail = 0
        _t_start = time_mod.time()

        _aborted = False
        _pending_batch: list[dict] = []
        _batch_count = 0

        def _flush_batch(force: bool = False) -> None:
            nonlocal _batch_count
            if on_batch is None or (not force and len(_pending_batch) < batch_size):
                return
            if _pending_batch:
                batch = list(_pending_batch)
                _pending_batch.clear()
                _batch_count += 1
                try:
                    on_batch(batch)
                except Exception as exc:
                    logger.warning("Incremental upsert batch %d failed: %s", _batch_count, exc)

        def _log_progress(force: bool = False) -> None:
            nonlocal _processed, _aborted
            if not force and _processed % 100 != 0:
                return
            # Check for abort every 100 stocks
            if not _aborted and _processed % 100 == 0 and _check_abort():
                logger.warning("ABORT requested — stopping stock fetch at %d/%d", _processed, _total)
                _aborted = True
            elapsed = time_mod.time() - _t_start
            rate = _processed / elapsed if elapsed > 0 else 0
            eta = (_total - _processed) / rate if rate > 0 else 0
            logger.info(
                "Stock progress: %d/%d (%.0f%%) | yahoo ok=%d fail=%d skip=%d | "
                "screener ok=%d fail=%d | %.1f stocks/min | ETA %.0fm",
                _processed, _total, (_processed / max(_total, 1)) * 100,
                _yahoo_ok, _yahoo_fail, _yahoo_skip,
                _screener_ok, _screener_fail,
                rate * 60, eta / 60,
            )

        if bulk_quotes:
            fundamentals_symbols = self._select_fundamentals_symbols(universe, bulk_quotes)
            logger.info(
                "Bhavcopy loaded: %d quotes for %d universe stocks, "
                "fundamentals enabled for %d symbols",
                len(bulk_quotes), _total, len(fundamentals_symbols),
            )
            missing: list[DiscoverStockDef] = []
            for stock in universe:
                if _aborted:
                    break
                quote = bulk_quotes.get(stock.nse_symbol)
                if quote is None:
                    missing.append(stock)
                    continue
                row = self._build_snapshot_row(
                    stock,
                    quote,
                    "nse_bhavcopy",
                    fundamentals_enabled=stock.nse_symbol in fundamentals_symbols,
                )
                raw_rows.append(row)
                _pending_batch.append(row)
                _processed += 1
                # Track Yahoo / Screener stats from primary_source
                src = row.get("primary_source", "")
                if "yahoo" in src:
                    _yahoo_ok += 1
                if "screener" in src:
                    _screener_ok += 1
                if src == "screener_in" and "yahoo" not in src:
                    _yahoo_fail += 1
                if src == "unavailable":
                    _yahoo_skip += 1
                _flush_batch()
                _log_progress()
            if not _aborted and missing and self._missing_quote_retry_limit > 0:
                retry_batch = missing[: self._missing_quote_retry_limit]
                logger.warning(
                    "Bhavcopy missing %d/%d symbols; retrying fallback quotes for %d symbols",
                    len(missing),
                    len(universe),
                    len(retry_batch),
                )
                for stock in retry_batch:
                    if _aborted:
                        break
                    item = self._fetch_one(stock)
                    if item is not None:
                        raw_rows.append(item)
                        _pending_batch.append(item)
                    _processed += 1
                    _flush_batch()
                    _log_progress()
        else:
            logger.warning("Bhavcopy unavailable — using fallback quote path for all symbols")
            fallback_universe = CORE_UNIVERSE if len(universe) > len(CORE_UNIVERSE) else universe
            _total = len(fallback_universe)
            for stock in fallback_universe:
                if _aborted:
                    break
                item = self._fetch_one(stock)
                if item is not None:
                    raw_rows.append(item)
                    _pending_batch.append(item)
                _processed += 1
                _flush_batch()
                _log_progress()
            if len(raw_rows) < len(fallback_universe):
                logger.warning(
                    "Fallback quote path updated %d/%d symbols",
                    len(raw_rows),
                    len(fallback_universe),
                )
        _flush_batch(force=True)  # flush remaining rows
        _log_progress(force=True)
        elapsed_total = time_mod.time() - _t_start
        status_word = "ABORTED" if _aborted else "complete"
        logger.info(
            "Stock fetch %s: %d rows in %.1fm | yahoo ok=%d fail=%d skip=%d | "
            "screener ok=%d fail=%d | incremental batches=%d",
            status_word, len(raw_rows), elapsed_total / 60,
            _yahoo_ok, _yahoo_fail, _yahoo_skip,
            _screener_ok, _screener_fail, _batch_count,
        )
        return raw_rows

    def fetch_all(self, *, volatility_data: dict[str, dict] | None = None) -> list[dict]:
        raw_rows = self.fetch_raw_rows()
        return self._compute_scores(raw_rows, volatility_data=volatility_data)


_scraper = DiscoverStockScraper()


def _check_abort(job_name: str = "discover_stock") -> bool:
    """Check Redis for an abort flag. Returns True if abort requested."""
    try:
        import redis as _redis

        settings = get_settings()
        r = _redis.from_url(settings.redis_url, decode_responses=True)
        val = r.get(f"job:abort:{job_name}")
        if val:
            r.delete(f"job:abort:{job_name}")
            return True
    except Exception:
        pass
    return False


def _fetch_discover_stock_raw_sync() -> list[dict]:
    return _scraper.fetch_raw_rows()


_UPSERT_BATCH_SIZE = 200
_INCREMENTAL_BATCH_SIZE = 50


async def run_discover_stock_job() -> None:
    try:
        job_t0 = time_mod.time()

        # 1. Pre-fetch volatility data from PostgreSQL (async).
        volatility_data = await discover_service.get_bulk_stock_volatility_data()
        logger.info(
            "Discover stock: volatility_data has %d symbols (sample keys: %s)",
            len(volatility_data),
            list(volatility_data.keys())[:5] if volatility_data else "EMPTY",
        )

        # 2. Fetch quotes + fundamentals (sync network I/O in executor).
        #    Incremental upsert every 50 rows for early DB visibility (unscored).
        loop = asyncio.get_event_loop()

        # Keys that only exist after scoring — strip from incremental upserts
        # so we don't overwrite existing scores with 0/None.
        _SCORE_KEYS = {
            "score", "score_quality", "score_valuation", "score_growth",
            "score_momentum", "score_institutional", "score_risk",
            "score_breakdown", "tags_v2", "why_ranked",
            "sector_percentile", "lynch_classification",
            "technical_score", "rsi_14", "action_tag", "action_tag_reasoning",
        }

        def _incremental_upsert(batch: list[dict]) -> None:
            """Called from sync thread every 50 rows — upserts raw (unscored) data."""
            # Strip score fields so we don't overwrite existing scores with 0/None
            clean_batch = [{k: v for k, v in row.items() if k not in _SCORE_KEYS} for row in batch]
            future = asyncio.run_coroutine_threadsafe(
                discover_service.upsert_discover_stock_snapshots(clean_batch),
                loop,
            )
            try:
                count = future.result(timeout=30)
                logger.info("Incremental upsert: %d rows written to DB", count)
            except Exception as exc:
                logger.warning("Incremental upsert failed: %s", exc)

        raw_rows = await loop.run_in_executor(
            get_job_executor("discover-stock"),
            lambda: _scraper.fetch_raw_rows(
                on_batch=_incremental_upsert,
                batch_size=_INCREMENTAL_BATCH_SIZE,
            ),
        )
        fetch_elapsed = time_mod.time() - job_t0
        logger.info(
            "Discover stock: fetched %d raw rows in %.1fm",
            len(raw_rows), fetch_elapsed / 60,
        )

        # Log data source distribution
        source_counts: dict[str, int] = {}
        for r in raw_rows:
            src = r.get("primary_source", "unknown")
            source_counts[src] = source_counts.get(src, 0) + 1
        logger.info("Discover stock: source distribution: %s", source_counts)

        # Log sector distribution
        sector_counts: dict[str, int] = {}
        for r in raw_rows:
            sec = r.get("sector", "Other")
            sector_counts[sec] = sector_counts.get(sec, 0) + 1
        other_count = sector_counts.get("Other", 0)
        logger.info(
            "Discover stock: %d sectors, 'Other'=%d/%d (%.1f%%)",
            len(sector_counts), other_count, len(raw_rows),
            (other_count / max(len(raw_rows), 1)) * 100,
        )

        # 2b. Pre-fetch Nifty 50 regime data and price history for technical scoring.
        from app.core.database import get_pool as _get_pool
        _pool = await _get_pool()
        nifty_price, nifty_200dma = None, None
        try:
            nifty_row = await _pool.fetchrow(
                """SELECT price FROM market_prices
                   WHERE asset = 'Nifty 50' AND instrument_type = 'index'
                   ORDER BY "timestamp" DESC LIMIT 1"""
            )
            if nifty_row:
                nifty_price = float(nifty_row["price"])
            nifty_200dma_row = await _pool.fetchrow(
                """SELECT AVG(close) AS dma200
                   FROM (
                       SELECT close FROM discover_stock_price_history
                       WHERE symbol = 'NIFTY 50'
                       ORDER BY trade_date DESC LIMIT 200
                   ) sub"""
            )
            if nifty_200dma_row and nifty_200dma_row["dma200"]:
                nifty_200dma = float(nifty_200dma_row["dma200"])
        except Exception:
            logger.warning("Failed to fetch Nifty regime data — defaulting to neutral")

        # Fetch price history for technical score computation
        price_history: dict[str, list[dict]] = {}
        try:
            ph_rows = await _pool.fetch(
                """SELECT symbol, trade_date, close, volume
                   FROM discover_stock_price_history
                   WHERE trade_date >= CURRENT_DATE - INTERVAL '450 days'
                   ORDER BY symbol, trade_date"""
            )
            for ph_row in ph_rows:
                sym = ph_row["symbol"]
                if sym not in price_history:
                    price_history[sym] = []
                price_history[sym].append({
                    "date": ph_row["trade_date"],
                    "close": float(ph_row["close"]),
                    "volume": int(ph_row["volume"]) if ph_row["volume"] else 0,
                })
            logger.info("Discover stock: loaded price history for %d symbols", len(price_history))
        except Exception:
            logger.warning("Failed to fetch price history for technical scoring")

        # 3. Score with 6-layer model (CPU-bound, fast).
        score_t0 = time_mod.time()
        rows = _scraper._compute_scores(
            raw_rows,
            volatility_data=volatility_data,
            nifty_price=nifty_price,
            nifty_200dma=nifty_200dma,
            price_history=price_history,
        )
        logger.info(
            "Discover stock: scored %d rows in %.1fs",
            len(rows), time_mod.time() - score_t0,
        )

        # Log sample scores
        if rows:
            sample = rows[0]
            logger.info(
                "Discover stock: sample scored row %s → score=%.2f "
                "qual=%.1f val=%s gro=%s mom=%.1f inst=%s risk=%s lynch=%s tags_v2=%s",
                sample.get("symbol"),
                sample.get("score", 0),
                sample.get("score_quality", 0),
                sample.get("score_valuation"),
                sample.get("score_growth"),
                sample.get("score_momentum", 0),
                sample.get("score_institutional"),
                sample.get("score_risk"),
                sample.get("lynch_classification"),
                [t.get("tag") for t in sample.get("tags_v2", [])][:5],
            )

        # 4. Upsert in batches for incremental visibility + fault tolerance.
        upsert_t0 = time_mod.time()
        total_upserted = 0
        for batch_start in range(0, len(rows), _UPSERT_BATCH_SIZE):
            batch = rows[batch_start : batch_start + _UPSERT_BATCH_SIZE]
            count = await discover_service.upsert_discover_stock_snapshots(batch)
            total_upserted += count
            logger.info(
                "Discover stock: upserted batch %d–%d (%d rows, %d total so far)",
                batch_start, batch_start + len(batch) - 1, count, total_upserted,
            )

        total_elapsed = time_mod.time() - job_t0
        logger.info(
            "Discover stock job complete: %d snapshots upserted in %.1fm "
            "(fetch=%.1fm, score=%.1fs, upsert=%.1fs)",
            total_upserted, total_elapsed / 60,
            fetch_elapsed / 60,
            time_mod.time() - score_t0,  # includes upsert time too but close enough
            time_mod.time() - upsert_t0,
        )
    except requests.RequestException:
        logger.exception("Discover stock job failed due to network exception")
    except Exception:
        logger.exception("Discover stock job failed")


async def rescore_discover_stocks() -> dict:
    """Read all stock rows from DB, re-compute scores, and write back.

    No network fetching — purely DB read → score → DB write.
    """
    t0 = time_mod.time()

    # 1. Read all raw rows from the DB
    from app.core.database import get_pool

    pool = await get_pool()
    async with pool.acquire() as conn:
        db_rows = await conn.fetch(
            f"SELECT * FROM {discover_service.STOCK_TABLE} WHERE market = 'IN'"
        )
    raw_rows = [dict(r) for r in db_rows]
    read_elapsed = time_mod.time() - t0
    logger.info("Rescore: read %d rows from DB in %.1fs", len(raw_rows), read_elapsed)

    if not raw_rows:
        return {"status": "empty", "rows": 0}

    # 2. Data quality audit — track missing fields
    _KEY_FIELDS = {
        "price": ["last_price", "percent_change", "volume"],
        "fundamentals": ["pe_ratio", "roe", "roce", "debt_to_equity", "eps", "price_to_book"],
        "yahoo": ["beta", "gross_margins", "operating_margins", "profit_margins",
                   "forward_pe", "revenue_growth", "earnings_growth",
                   "total_debt", "total_revenue", "total_cash"],
        "shareholding": ["promoter_holding", "fii_holding", "dii_holding", "public_holding"],
        "analyst": ["analyst_count", "analyst_target_mean", "analyst_recommendation"],
        "meta": ["sector", "industry", "market_cap", "high_52w", "low_52w"],
    }
    missing_stats: dict[str, dict[str, int]] = {}
    stocks_missing_all: dict[str, list[str]] = {}  # group → symbols with ALL fields missing

    for group, fields in _KEY_FIELDS.items():
        field_counts: dict[str, int] = {}
        all_missing_syms: list[str] = []
        for row in raw_rows:
            missing_in_group = 0
            for f in fields:
                if row.get(f) is None:
                    field_counts[f] = field_counts.get(f, 0) + 1
                    missing_in_group += 1
            if missing_in_group == len(fields):
                all_missing_syms.append(str(row.get("symbol", "?")))
        if field_counts:
            missing_stats[group] = field_counts
        if all_missing_syms:
            stocks_missing_all[group] = all_missing_syms

    total = len(raw_rows)
    for group, counts in missing_stats.items():
        parts = ", ".join(f"{f}={c}/{total}" for f, c in sorted(counts.items(), key=lambda x: -x[1]))
        logger.info("Rescore data gaps [%s]: %s", group, parts)

    for group, syms in stocks_missing_all.items():
        logger.warning(
            "Rescore: %d stocks missing ALL %s fields (first 10): %s",
            len(syms), group, syms[:10],
        )

    # Coverage summary
    has_yahoo = sum(1 for r in raw_rows if r.get("beta") is not None)
    has_shareholding = sum(1 for r in raw_rows if r.get("promoter_holding") is not None)
    has_analyst = sum(1 for r in raw_rows if r.get("analyst_count") is not None)
    has_fundamentals = sum(1 for r in raw_rows if r.get("pe_ratio") is not None)
    has_industry = sum(1 for r in raw_rows if r.get("industry") is not None)
    logger.info(
        "Rescore coverage: %d total | fundamentals=%d (%.0f%%) | yahoo=%d (%.0f%%) | "
        "shareholding=%d (%.0f%%) | analyst=%d (%.0f%%) | industry=%d (%.0f%%)",
        total,
        has_fundamentals, has_fundamentals / total * 100,
        has_yahoo, has_yahoo / total * 100,
        has_shareholding, has_shareholding / total * 100,
        has_analyst, has_analyst / total * 100,
        has_industry, has_industry / total * 100,
    )

    # 3. Pre-fetch volatility data
    volatility_data = await discover_service.get_bulk_stock_volatility_data()
    logger.info("Rescore: volatility data for %d symbols", len(volatility_data))

    # 3b. Pre-fetch Nifty regime data and price history
    nifty_price, nifty_200dma = None, None
    try:
        nifty_row = await pool.fetchrow(
            """SELECT price FROM market_prices
               WHERE asset = 'Nifty 50' AND instrument_type = 'index'
               ORDER BY "timestamp" DESC LIMIT 1"""
        )
        if nifty_row:
            nifty_price = float(nifty_row["price"])
        nifty_200dma_row = await pool.fetchrow(
            """SELECT AVG(close) AS dma200
               FROM (
                   SELECT close FROM discover_stock_price_history
                   WHERE symbol = 'NIFTY 50'
                   ORDER BY trade_date DESC LIMIT 200
               ) sub"""
        )
        if nifty_200dma_row and nifty_200dma_row["dma200"]:
            nifty_200dma = float(nifty_200dma_row["dma200"])
    except Exception:
        logger.warning("Rescore: failed to fetch Nifty regime data")

    price_history: dict[str, list[dict]] = {}
    try:
        ph_rows = await pool.fetch(
            """SELECT symbol, trade_date, close, volume
               FROM discover_stock_price_history
               WHERE trade_date >= CURRENT_DATE - INTERVAL '450 days'
               ORDER BY symbol, trade_date"""
        )
        for ph_row in ph_rows:
            sym = ph_row["symbol"]
            if sym not in price_history:
                price_history[sym] = []
            price_history[sym].append({
                "date": ph_row["trade_date"],
                "close": float(ph_row["close"]),
                "volume": int(ph_row["volume"]) if ph_row["volume"] else 0,
            })
        logger.info("Rescore: loaded price history for %d symbols", len(price_history))
    except Exception:
        logger.warning("Rescore: failed to fetch price history")

    # 4. Score
    score_t0 = time_mod.time()
    scored_rows = _scraper._compute_scores(
        raw_rows,
        volatility_data=volatility_data,
        nifty_price=nifty_price,
        nifty_200dma=nifty_200dma,
        price_history=price_history,
    )
    score_elapsed = time_mod.time() - score_t0
    logger.info("Rescore: scored %d rows in %.1fs", len(scored_rows), score_elapsed)

    # Score distribution
    scores = [r.get("score", 0) for r in scored_rows if r.get("score") is not None]
    if scores:
        scores.sort()
        p25 = scores[len(scores) // 4]
        p50 = scores[len(scores) // 2]
        p75 = scores[3 * len(scores) // 4]
        tiers = {"Strong": 0, "Good": 0, "Average": 0, "Weak": 0}
        for s in scores:
            if s >= 75:
                tiers["Strong"] += 1
            elif s >= 50:
                tiers["Good"] += 1
            elif s >= 25:
                tiers["Average"] += 1
            else:
                tiers["Weak"] += 1
        logger.info(
            "Rescore scores: min=%.1f p25=%.1f p50=%.1f p75=%.1f max=%.1f | "
            "Strong=%d Good=%d Average=%d Weak=%d",
            scores[0], p25, p50, p75, scores[-1],
            tiers["Strong"], tiers["Good"], tiers["Average"], tiers["Weak"],
        )

    # 5. Upsert scored rows back
    upsert_t0 = time_mod.time()
    total_upserted = 0
    for batch_start in range(0, len(scored_rows), _UPSERT_BATCH_SIZE):
        batch = scored_rows[batch_start: batch_start + _UPSERT_BATCH_SIZE]
        count = await discover_service.upsert_discover_stock_snapshots(batch)
        total_upserted += count

    # Insert score history snapshot for trend tracking
    try:
        history_count = await discover_service.insert_score_history(scored_rows)
        await discover_service.prune_score_history(days=30)
        logger.info("Score history: inserted %d snapshots, pruned >30d", history_count)
    except Exception:
        logger.exception("Score history insert failed (non-fatal)")

    total_elapsed = time_mod.time() - t0
    logger.info(
        "Rescore complete: %d rows in %.1fs (read=%.1fs, score=%.1fs, upsert=%.1fs)",
        total_upserted, total_elapsed, read_elapsed, score_elapsed, time_mod.time() - upsert_t0,
    )

    return {
        "status": "completed",
        "rows_scored": len(scored_rows),
        "rows_upserted": total_upserted,
        "elapsed_seconds": round(total_elapsed, 1),
        "coverage": {
            "total": total,
            "fundamentals": has_fundamentals,
            "yahoo": has_yahoo,
            "shareholding": has_shareholding,
            "analyst": has_analyst,
            "industry": has_industry,
        },
        "missing_all": {group: len(syms) for group, syms in stocks_missing_all.items()},
        "score_distribution": tiers if scores else {},
    }

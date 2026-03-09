from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class AssetCatalogItem:
    asset: str
    instrument_type: str
    symbol: str
    region: str
    exchange: str
    session_policy: str
    priority_rank: int
    tradable_type: str
    unit: str
    default_watchlist: bool = False
    benchmark_asset: str | None = None


ASSET_CATALOG: tuple[AssetCatalogItem, ...] = (
    # India indices
    AssetCatalogItem("Nifty 50", "index", "^NSEI", "India", "NSE", "session", 10, "index", "points", True, "Nifty 50"),
    AssetCatalogItem("Nifty 500", "index", "^CRSLDX", "India", "NSE", "session", 20, "index", "points", True, "Nifty 50"),
    AssetCatalogItem("Sensex", "index", "^BSESN", "India", "NSE", "session", 30, "index", "points", False, "Nifty 50"),
    AssetCatalogItem("Nifty Bank", "index", "^NSEBANK", "India", "NSE", "session", 40, "index", "points", False, "Nifty 50"),
    AssetCatalogItem("Nifty IT", "index", "^CNXIT", "India", "NSE", "session", 50, "index", "points", False, "Nifty 50"),
    AssetCatalogItem("Nifty Midcap 150", "index", "NIFTYMIDCAP150.NS", "India", "NSE", "session", 60, "index", "points", False, "Nifty 50"),
    AssetCatalogItem("Nifty Smallcap 250", "index", "NIFTYSMLCAP250.NS", "India", "NSE", "session", 70, "index", "points", False, "Nifty 50"),
    AssetCatalogItem("Nifty Auto", "index", "^CNXAUTO", "India", "NSE", "session", 80, "index", "points", False, "Nifty 50"),
    AssetCatalogItem("Nifty Pharma", "index", "^CNXPHARMA", "India", "NSE", "session", 90, "index", "points", False, "Nifty 50"),
    AssetCatalogItem("Nifty Metal", "index", "^CNXMETAL", "India", "NSE", "session", 100, "index", "points", False, "Nifty 50"),
    AssetCatalogItem("India VIX", "index", "^INDIAVIX", "India", "NSE", "session", 105, "volatility_index", "points", False, "Nifty 50"),
    AssetCatalogItem("Gift Nifty", "index", "GIFTNIFTY", "India", "NSE", "predictive", 110, "predictive_index", "points", True, "Nifty 50"),
    # US indices and sector proxies
    AssetCatalogItem("S&P500", "index", "^GSPC", "US", "NYSE", "session", 200, "index", "points", False, "S&P500"),
    AssetCatalogItem("NASDAQ", "index", "^IXIC", "US", "NYSE", "session", 210, "index", "points", True, "S&P500"),
    AssetCatalogItem("Dow Jones", "index", "^DJI", "US", "NYSE", "session", 220, "index", "points", False, "S&P500"),
    AssetCatalogItem("CBOE VIX", "index", "^VIX", "US", "NYSE", "session", 230, "volatility_index", "points", False, "S&P500"),
    AssetCatalogItem("S&P 500 Tech", "index", "XLK", "US", "NYSE", "session", 240, "sector_proxy", "points", False, "S&P500"),
    AssetCatalogItem("S&P 500 Financials", "index", "XLF", "US", "NYSE", "session", 250, "sector_proxy", "points", False, "S&P500"),
    AssetCatalogItem("S&P 500 Energy", "index", "XLE", "US", "NYSE", "session", 260, "sector_proxy", "points", False, "S&P500"),
    # Europe
    AssetCatalogItem("FTSE 100", "index", "^FTSE", "Europe", "LSE", "session", 300, "index", "points", False, "Euro Stoxx 50"),
    AssetCatalogItem("DAX", "index", "^GDAXI", "Europe", "XETRA", "session", 310, "index", "points", False, "Euro Stoxx 50"),
    AssetCatalogItem("CAC 40", "index", "^FCHI", "Europe", "EURONEXT", "session", 320, "index", "points", False, "Euro Stoxx 50"),
    AssetCatalogItem("Euro Stoxx 50", "index", "^STOXX50E", "Europe", "EURONEXT", "session", 330, "index", "points", False, "Euro Stoxx 50"),
    # Japan
    AssetCatalogItem("Nikkei 225", "index", "^N225", "Japan", "TSE", "session", 400, "index", "points", False, "Nikkei 225"),
    AssetCatalogItem("TOPIX", "index", "^TOPX", "Japan", "TSE", "session", 410, "index", "points", False, "Nikkei 225"),
    # FX
    AssetCatalogItem("USD/INR", "currency", "USDINR=X", "FX", "NYSE", "rolling_24h", 500, "fx", "inr", True, "USD/INR"),
    AssetCatalogItem("EUR/INR", "currency", "EURINR=X", "FX", "NYSE", "rolling_24h", 510, "fx", "inr", False, "USD/INR"),
    AssetCatalogItem("GBP/INR", "currency", "GBPINR=X", "FX", "NYSE", "rolling_24h", 520, "fx", "inr", False, "USD/INR"),
    AssetCatalogItem("JPY/INR", "currency", "JPYINR=X", "FX", "NYSE", "rolling_24h", 530, "fx", "inr", False, "USD/INR"),
    # Commodities
    AssetCatalogItem("gold", "commodity", "GC=F", "Commodities", "NYSE", "rolling_24h", 600, "commodity", "usd_per_troy_ounce", True, "gold"),
    AssetCatalogItem("silver", "commodity", "SI=F", "Commodities", "NYSE", "rolling_24h", 610, "commodity", "usd_per_troy_ounce", True, "gold"),
    AssetCatalogItem("copper", "commodity", "HG=F", "Commodities", "NYSE", "rolling_24h", 620, "commodity", "usd_per_pound", False, "gold"),
    AssetCatalogItem("crude oil", "commodity", "CL=F", "Commodities", "NYSE", "rolling_24h", 630, "commodity", "usd_per_barrel", False, "gold"),
    AssetCatalogItem("natural gas", "commodity", "NG=F", "Commodities", "NYSE", "rolling_24h", 640, "commodity", "usd_per_mmbtu", False, "gold"),
    # Bonds
    AssetCatalogItem("India 10Y Bond Yield", "bond_yield", "INDIRLTLT01STM", "India", "NSE", "session", 700, "bond_yield", "percent", False, "India 10Y Bond Yield"),
    AssetCatalogItem("US 10Y Treasury Yield", "bond_yield", "DGS10", "US", "NYSE", "session", 710, "bond_yield", "percent", False, "US 10Y Treasury Yield"),
    AssetCatalogItem("US 2Y Treasury Yield", "bond_yield", "DGS2", "US", "NYSE", "session", 720, "bond_yield", "percent", False, "US 10Y Treasury Yield"),
)

_ASSET_LOOKUP = {item.asset: item for item in ASSET_CATALOG}


def get_asset_meta(asset: str) -> AssetCatalogItem | None:
    return _ASSET_LOOKUP.get(asset)


def list_asset_catalog(
    *,
    region: str | None = None,
    instrument_type: str | None = None,
) -> list[AssetCatalogItem]:
    items = list(ASSET_CATALOG)
    if region:
        region_norm = region.strip().lower()
        items = [i for i in items if i.region.lower() == region_norm]
    if instrument_type:
        inst_norm = instrument_type.strip().lower()
        items = [i for i in items if i.instrument_type.lower() == inst_norm]
    return sorted(items, key=lambda i: (i.priority_rank, i.asset))


def default_watchlist_assets() -> list[str]:
    defaults = [i.asset for i in ASSET_CATALOG if i.default_watchlist]
    return defaults or [
        "Nifty 50",
        "Nifty 500",
        "NASDAQ",
        "Gift Nifty",
        "USD/INR",
        "gold",
        "silver",
    ]


def benchmark_for_region(region: str) -> str | None:
    region_norm = region.strip().lower()
    if region_norm == "india":
        return "Nifty 50"
    if region_norm == "us":
        return "S&P500"
    if region_norm == "europe":
        return "Euro Stoxx 50"
    if region_norm == "japan":
        return "Nikkei 225"
    if region_norm == "fx":
        return "USD/INR"
    if region_norm == "commodities":
        return "gold"
    return None

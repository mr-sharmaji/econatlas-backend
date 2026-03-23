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
    AssetCatalogItem("Nifty 500", "index", "^CRSLDX", "India", "NSE", "session", 20, "index", "points", False, "Nifty 50"),
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
    AssetCatalogItem("NASDAQ", "index", "^IXIC", "US", "NYSE", "session", 210, "index", "points", False, "S&P500"),
    AssetCatalogItem("Nasdaq 100", "index", "^NDX", "US", "NYSE", "session", 215, "index", "points", True, "S&P500"),
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
    AssetCatalogItem("AUD/INR", "currency", "AUDINR=X", "FX", "NYSE", "rolling_24h", 540, "fx", "inr", False, "USD/INR"),
    AssetCatalogItem("CAD/INR", "currency", "CADINR=X", "FX", "NYSE", "rolling_24h", 550, "fx", "inr", False, "USD/INR"),
    AssetCatalogItem("CHF/INR", "currency", "CHFINR=X", "FX", "NYSE", "rolling_24h", 560, "fx", "inr", False, "USD/INR"),
    AssetCatalogItem("CNY/INR", "currency", "CNYINR=X", "FX", "NYSE", "rolling_24h", 570, "fx", "inr", False, "USD/INR"),
    AssetCatalogItem("SGD/INR", "currency", "SGDINR=X", "FX", "NYSE", "rolling_24h", 580, "fx", "inr", False, "USD/INR"),
    AssetCatalogItem("HKD/INR", "currency", "HKDINR=X", "FX", "NYSE", "rolling_24h", 590, "fx", "inr", False, "USD/INR"),
    AssetCatalogItem("KRW/INR", "currency", "KRWINR=X", "FX", "NYSE", "rolling_24h", 600, "fx", "inr", False, "USD/INR"),
    AssetCatalogItem("AED/INR", "currency", "AEDINR=X", "FX", "NYSE", "rolling_24h", 610, "fx", "inr", False, "USD/INR"),
    AssetCatalogItem("NZD/INR", "currency", "NZDINR=X", "FX", "NYSE", "rolling_24h", 620, "fx", "inr", False, "USD/INR"),
    AssetCatalogItem("SAR/INR", "currency", "SARINR=X", "FX", "NYSE", "rolling_24h", 630, "fx", "inr", False, "USD/INR"),
    AssetCatalogItem("THB/INR", "currency", "THBINR=X", "FX", "NYSE", "rolling_24h", 640, "fx", "inr", False, "USD/INR"),
    AssetCatalogItem("MYR/INR", "currency", "MYRINR=X", "FX", "NYSE", "rolling_24h", 650, "fx", "inr", False, "USD/INR"),
    AssetCatalogItem("IDR/INR", "currency", "IDRINR=X", "FX", "NYSE", "rolling_24h", 660, "fx", "inr", False, "USD/INR"),
    AssetCatalogItem("PHP/INR", "currency", "PHPINR=X", "FX", "NYSE", "rolling_24h", 670, "fx", "inr", False, "USD/INR"),
    AssetCatalogItem("ZAR/INR", "currency", "ZARINR=X", "FX", "NYSE", "rolling_24h", 680, "fx", "inr", False, "USD/INR"),
    AssetCatalogItem("BRL/INR", "currency", "BRLINR=X", "FX", "NYSE", "rolling_24h", 690, "fx", "inr", False, "USD/INR"),
    AssetCatalogItem("MXN/INR", "currency", "MXNINR=X", "FX", "NYSE", "rolling_24h", 700, "fx", "inr", False, "USD/INR"),
    AssetCatalogItem("QAR/INR", "currency", "QARINR=X", "FX", "NYSE", "rolling_24h", 710, "fx", "inr", False, "USD/INR"),
    AssetCatalogItem("KWD/INR", "currency", "KWDINR=X", "FX", "NYSE", "rolling_24h", 715, "fx", "inr", False, "USD/INR"),
    AssetCatalogItem("BHD/INR", "currency", "BHDINR=X", "FX", "NYSE", "rolling_24h", 720, "fx", "inr", False, "USD/INR"),
    AssetCatalogItem("OMR/INR", "currency", "OMRINR=X", "FX", "NYSE", "rolling_24h", 725, "fx", "inr", False, "USD/INR"),
    AssetCatalogItem("ILS/INR", "currency", "ILSINR=X", "FX", "NYSE", "rolling_24h", 730, "fx", "inr", False, "USD/INR"),
    AssetCatalogItem("SEK/INR", "currency", "SEKINR=X", "FX", "NYSE", "rolling_24h", 735, "fx", "inr", False, "USD/INR"),
    AssetCatalogItem("NOK/INR", "currency", "NOKINR=X", "FX", "NYSE", "rolling_24h", 740, "fx", "inr", False, "USD/INR"),
    AssetCatalogItem("DKK/INR", "currency", "DKKINR=X", "FX", "NYSE", "rolling_24h", 745, "fx", "inr", False, "USD/INR"),
    AssetCatalogItem("PLN/INR", "currency", "PLNINR=X", "FX", "NYSE", "rolling_24h", 750, "fx", "inr", False, "USD/INR"),
    AssetCatalogItem("TRY/INR", "currency", "TRYINR=X", "FX", "NYSE", "rolling_24h", 755, "fx", "inr", False, "USD/INR"),
    AssetCatalogItem("TWD/INR", "currency", "TWDINR=X", "FX", "NYSE", "rolling_24h", 760, "fx", "inr", False, "USD/INR"),
    AssetCatalogItem("VND/INR", "currency", "VNDINR=X", "FX", "NYSE", "rolling_24h", 765, "fx", "inr", False, "USD/INR"),
    AssetCatalogItem("BDT/INR", "currency", "BDTINR=X", "FX", "NYSE", "rolling_24h", 770, "fx", "inr", False, "USD/INR"),
    AssetCatalogItem("LKR/INR", "currency", "LKRINR=X", "FX", "NYSE", "rolling_24h", 775, "fx", "inr", False, "USD/INR"),
    AssetCatalogItem("PKR/INR", "currency", "PKRINR=X", "FX", "NYSE", "rolling_24h", 780, "fx", "inr", False, "USD/INR"),
    AssetCatalogItem("NPR/INR", "currency", "NPRINR=X", "FX", "NYSE", "rolling_24h", 785, "fx", "inr", False, "USD/INR"),
    # Commodities
    AssetCatalogItem("gold", "commodity", "GC=F", "Commodities", "NYSE", "rolling_24h", 800, "commodity", "usd_per_troy_ounce", True, "gold"),
    AssetCatalogItem("silver", "commodity", "SI=F", "Commodities", "NYSE", "rolling_24h", 810, "commodity", "usd_per_troy_ounce", True, "gold"),
    AssetCatalogItem("platinum", "commodity", "PL=F", "Commodities", "NYSE", "rolling_24h", 820, "commodity", "usd_per_troy_ounce", False, "gold"),
    AssetCatalogItem("palladium", "commodity", "PA=F", "Commodities", "NYSE", "rolling_24h", 830, "commodity", "usd_per_troy_ounce", False, "gold"),
    AssetCatalogItem("copper", "commodity", "HG=F", "Commodities", "NYSE", "rolling_24h", 840, "commodity", "usd_per_pound", False, "gold"),
    AssetCatalogItem("crude oil", "commodity", "CL=F", "Commodities", "NYSE", "rolling_24h", 850, "commodity", "usd_per_barrel", True, "gold"),
    AssetCatalogItem("natural gas", "commodity", "NG=F", "Commodities", "NYSE", "rolling_24h", 860, "commodity", "usd_per_mmbtu", False, "gold"),
    # Agriculture
    AssetCatalogItem("wheat", "commodity", "ZW=F", "Commodities", "CBOT", "rolling_24h", 870, "commodity", "usd_per_bushel", False, "gold"),
    AssetCatalogItem("corn", "commodity", "ZC=F", "Commodities", "CBOT", "rolling_24h", 880, "commodity", "usd_per_bushel", False, "gold"),
    AssetCatalogItem("soybeans", "commodity", "ZS=F", "Commodities", "CBOT", "rolling_24h", 890, "commodity", "usd_per_bushel", False, "gold"),
    AssetCatalogItem("rice", "commodity", "ZR=F", "Commodities", "CBOT", "rolling_24h", 900, "commodity", "usd_per_hundredweight", False, "gold"),
    AssetCatalogItem("oats", "commodity", "ZO=F", "Commodities", "CBOT", "rolling_24h", 910, "commodity", "usd_per_bushel", False, "gold"),
    # Softs
    AssetCatalogItem("cotton", "commodity", "CT=F", "Commodities", "ICE", "rolling_24h", 920, "commodity", "usd_per_pound", False, "gold"),
    AssetCatalogItem("sugar", "commodity", "SB=F", "Commodities", "ICE", "rolling_24h", 930, "commodity", "usd_per_pound", False, "gold"),
    AssetCatalogItem("coffee", "commodity", "KC=F", "Commodities", "ICE", "rolling_24h", 940, "commodity", "usd_per_pound", False, "gold"),
    AssetCatalogItem("cocoa", "commodity", "CC=F", "Commodities", "ICE", "rolling_24h", 950, "commodity", "usd_per_metric_ton", False, "gold"),
    # Industrial Metal
    AssetCatalogItem("aluminum", "commodity", "ALI=F", "Commodities", "COMEX", "rolling_24h", 960, "commodity", "usd_per_pound", False, "gold"),
    # Energy
    AssetCatalogItem("brent crude", "commodity", "BZ=F", "Commodities", "ICE", "rolling_24h", 970, "commodity", "usd_per_barrel", False, "gold"),
    AssetCatalogItem("gasoline", "commodity", "RB=F", "Commodities", "NYMEX", "rolling_24h", 980, "commodity", "usd_per_gallon", False, "gold"),
    AssetCatalogItem("heating oil", "commodity", "HO=F", "Commodities", "NYMEX", "rolling_24h", 990, "commodity", "usd_per_gallon", False, "gold"),
    # Fertilizers (TE source, no Yahoo symbol)
    AssetCatalogItem("urea", "commodity", "TE:urea", "Commodities", "OTC", "rolling_24h", 1000, "commodity", "usd_per_metric_ton", False, "gold"),
    AssetCatalogItem("dap fertilizer", "commodity", "TE:dap", "Commodities", "OTC", "rolling_24h", 1010, "commodity", "usd_per_metric_ton", False, "gold"),
    AssetCatalogItem("potash", "commodity", "TE:potash", "Commodities", "OTC", "rolling_24h", 1020, "commodity", "usd_per_metric_ton", False, "gold"),
    AssetCatalogItem("tsp fertilizer", "commodity", "TE:tsp", "Commodities", "OTC", "rolling_24h", 1030, "commodity", "usd_per_metric_ton", False, "gold"),
    # Crypto
    AssetCatalogItem("bitcoin", "crypto", "BTC-USD", "Crypto", "GLOBAL", "rolling_24h", 900, "crypto", "usd", True, "bitcoin"),
    AssetCatalogItem("ethereum", "crypto", "ETH-USD", "Crypto", "GLOBAL", "rolling_24h", 910, "crypto", "usd", True, "bitcoin"),
    AssetCatalogItem("bnb", "crypto", "BNB-USD", "Crypto", "GLOBAL", "rolling_24h", 920, "crypto", "usd", False, "bitcoin"),
    AssetCatalogItem("solana", "crypto", "SOL-USD", "Crypto", "GLOBAL", "rolling_24h", 930, "crypto", "usd", False, "bitcoin"),
    AssetCatalogItem("xrp", "crypto", "XRP-USD", "Crypto", "GLOBAL", "rolling_24h", 940, "crypto", "usd", False, "bitcoin"),
    AssetCatalogItem("cardano", "crypto", "ADA-USD", "Crypto", "GLOBAL", "rolling_24h", 950, "crypto", "usd", False, "bitcoin"),
    AssetCatalogItem("dogecoin", "crypto", "DOGE-USD", "Crypto", "GLOBAL", "rolling_24h", 960, "crypto", "usd", False, "bitcoin"),
    AssetCatalogItem("polkadot", "crypto", "DOT-USD", "Crypto", "GLOBAL", "rolling_24h", 970, "crypto", "usd", False, "bitcoin"),
    AssetCatalogItem("avalanche", "crypto", "AVAX-USD", "Crypto", "GLOBAL", "rolling_24h", 980, "crypto", "usd", False, "bitcoin"),
    AssetCatalogItem("chainlink", "crypto", "LINK-USD", "Crypto", "GLOBAL", "rolling_24h", 990, "crypto", "usd", False, "bitcoin"),
    # Bonds
    AssetCatalogItem("India 10Y Bond Yield", "bond_yield", "INDIRLTLT01STM", "India", "NSE", "session", 800, "bond_yield", "percent", False, "India 10Y Bond Yield"),
    AssetCatalogItem("US 10Y Treasury Yield", "bond_yield", "DGS10", "US", "NYSE", "session", 810, "bond_yield", "percent", False, "US 10Y Treasury Yield"),
    AssetCatalogItem("US 2Y Treasury Yield", "bond_yield", "DGS2", "US", "NYSE", "session", 820, "bond_yield", "percent", False, "US 10Y Treasury Yield"),
    AssetCatalogItem("Germany 10Y Bond Yield", "bond_yield", "IRLTLT01DEM156N", "Europe", "XETRA", "session", 830, "bond_yield", "percent", False, "Germany 10Y Bond Yield"),
    AssetCatalogItem("Japan 10Y Bond Yield", "bond_yield", "IRLTLT01JPM156N", "Japan", "TSE", "session", 840, "bond_yield", "percent", False, "Japan 10Y Bond Yield"),
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
        "Nasdaq 100",
        "Gift Nifty",
        "USD/INR",
        "gold",
        "silver",
        "crude oil",
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
    if region_norm == "crypto":
        return "bitcoin"
    return None

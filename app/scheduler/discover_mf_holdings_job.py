from __future__ import annotations

import asyncio
import json
import logging
import random
import re
import time
from datetime import datetime, timezone
from html import unescape
from typing import Any

import requests

from app.core.config import get_settings
from app.core.database import get_pool
from app.scheduler.base import BaseScraper
from app.scheduler.job_executors import get_job_executor

logger = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────────────
_MF_TABLE = "discover_mutual_fund_snapshots"
_RATE_LIMIT_MIN = 0.3
_RATE_LIMIT_MAX = 0.5
_MAX_FUNDS_PER_RUN = 1800
_BATCH_SIZE = 50

# Keys to search for in __NEXT_DATA__ when looking for holdings data
_HOLDINGS_KEYS = frozenset({
    "holdings",
    "topHoldings",
    "top_holdings",
    "sectorAllocation",
    "sector_allocation",
    "assetAllocation",
    "asset_allocation",
    "portfolioHoldings",
    "stockAllocation",
    "stock_allocation",
    "portfolio",
})


class DiscoverMfHoldingsScraper(BaseScraper):
    """Scrapes mutual fund holdings data from ETMoney detail pages."""

    def __init__(self) -> None:
        super().__init__()
        self.settings = get_settings()

    # ── JSON / HTML extraction helpers ─────────────────────────────

    @staticmethod
    def _extract_next_data(html: str) -> dict | None:
        """Extract the __NEXT_DATA__ JSON blob from an ETMoney page."""
        match = re.search(
            r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
            html,
            flags=re.DOTALL,
        )
        if not match:
            return None
        try:
            parsed = json.loads(match.group(1).strip())
            return parsed if isinstance(parsed, dict) else None
        except (json.JSONDecodeError, ValueError):
            return None

    def _walk(self, obj: Any):
        """Recursively yield all dict nodes in a nested structure."""
        if isinstance(obj, dict):
            yield obj
            for value in obj.values():
                yield from self._walk(value)
        elif isinstance(obj, list):
            for item in obj:
                yield from self._walk(item)

    def _to_float(self, value: Any) -> float | None:
        try:
            if value is None:
                return None
            if isinstance(value, str):
                text = value.replace(",", "").replace("%", "").strip()
                if not text:
                    return None
                return float(text)
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _strip_tags(text: str) -> str:
        raw = re.sub(r"<[^>]+>", " ", text or "", flags=re.IGNORECASE)
        return re.sub(r"\s+", " ", unescape(raw)).strip()

    # ── Holdings parsing from compSchemeDTO (ET Money JS variable) ──

    def _extract_comp_scheme_dto(self, html: str) -> dict[str, Any] | None:
        """Extract portfolio data from ET Money's compSchemeDTO JS variable.

        ET Money embeds fund data in:  var compSchemeDTO = {...};
        This contains mfPortfolioData with asset allocation and
        mfCompanyDTOMap/mfSectorDTOMap with holdings and sector data.
        """
        result: dict[str, Any] = {
            "top_holdings": None,
            "sector_allocation": None,
            "asset_allocation": None,
        }

        # Extract compSchemeDTO JSON
        match = re.search(
            r'var\s+compSchemeDTO\s*=\s*(\{.*?\});\s*\n',
            html,
            flags=re.DOTALL,
        )
        if not match:
            return None

        try:
            data = json.loads(match.group(1))
        except (json.JSONDecodeError, ValueError):
            return None

        # Walk the JSON tree looking for portfolio data
        for node in self._walk(data):
            # Asset allocation from mfPortfolioData
            if result["asset_allocation"] is None:
                port = node.get("mfPortfolioData")
                if isinstance(port, dict) and (port.get("equity") is not None or port.get("debt") is not None):
                    equity = self._to_float(port.get("equity")) or 0
                    debt = self._to_float(port.get("debt")) or 0
                    others = self._to_float(port.get("others")) or 0
                    commodities = self._to_float(port.get("commodities")) or 0
                    cash = others + commodities if commodities == 0 else 0
                    other_pct = commodities + (others if commodities > 0 else 0)
                    total = equity + debt + cash + other_pct
                    if total > 0:
                        result["asset_allocation"] = {
                            "equity_pct": round(equity, 2),
                            "debt_pct": round(debt, 2),
                            "cash_pct": round(cash, 2),
                            "other_pct": round(other_pct, 2),
                        }

        # Extract holdings from mfCompanyDTOMapJson JS variable
        company_match = re.search(
            r'var\s+mfCompanyDTOMapJson\s*=\s*(\{.*?\});\s*\n',
            html,
            flags=re.DOTALL,
        )
        if company_match:
            try:
                companies = json.loads(company_match.group(1))
                if isinstance(companies, dict) and companies:
                    holdings = []
                    for company_id, info in companies.items():
                        if isinstance(info, dict):
                            name = info.get("companyName") or info.get("company") or info.get("name") or info.get("shortName")
                            pct = self._to_float(info.get("corpusPer") or info.get("percentage") or info.get("weightage"))
                            sector = info.get("sectorName") or info.get("sector") or info.get("industry")
                            if name and pct is not None and pct > 0:
                                h = {"name": str(name).strip(), "percentage": round(pct, 2)}
                                if sector:
                                    h["sector"] = str(sector).strip()
                                holdings.append(h)
                    if holdings:
                        holdings.sort(key=lambda x: x.get("percentage", 0), reverse=True)
                        result["top_holdings"] = holdings[:25]
            except (json.JSONDecodeError, ValueError):
                pass

        # Extract sector allocation from mfSectorDTOMapJson
        sector_match = re.search(
            r'var\s+mfSectorDTOMapJson\s*=\s*(\{.*?\});\s*\n',
            html,
            flags=re.DOTALL,
        )
        if sector_match:
            try:
                sectors = json.loads(sector_match.group(1))
                if isinstance(sectors, dict) and sectors:
                    sector_alloc = []
                    for sector_id, info in sectors.items():
                        if isinstance(info, dict):
                            name = info.get("sectorName") or info.get("name")
                            pct = self._to_float(info.get("corpusPer") or info.get("percentage"))
                            if name and pct is not None and pct > 0:
                                sector_alloc.append({
                                    "sector": str(name).strip(),
                                    "percentage": round(pct, 2),
                                })
                    if sector_alloc:
                        sector_alloc.sort(key=lambda x: x.get("percentage", 0), reverse=True)
                        result["sector_allocation"] = sector_alloc
            except (json.JSONDecodeError, ValueError):
                pass

        has_any = any(v is not None for v in result.values())
        return result if has_any else None

    # ── Holdings parsing from __NEXT_DATA__ ────────────────────────

    def _extract_holdings_from_next_data(
        self, next_data: dict
    ) -> dict[str, Any]:
        """Walk __NEXT_DATA__ looking for holdings, sector, and asset allocation."""
        result: dict[str, Any] = {
            "top_holdings": None,
            "sector_allocation": None,
            "asset_allocation": None,
        }

        for node in self._walk(next_data):
            # --- Top Holdings ---
            if result["top_holdings"] is None:
                holdings_raw = (
                    node.get("topHoldings")
                    or node.get("top_holdings")
                    or node.get("holdings")
                    or node.get("portfolioHoldings")
                    or node.get("stockAllocation")
                    or node.get("stock_allocation")
                )
                if isinstance(holdings_raw, list) and len(holdings_raw) > 0:
                    parsed = self._parse_top_holdings(holdings_raw)
                    if parsed:
                        result["top_holdings"] = parsed

            # --- Sector Allocation ---
            if result["sector_allocation"] is None:
                sector_raw = (
                    node.get("sectorAllocation")
                    or node.get("sector_allocation")
                    or node.get("sectorWiseAllocation")
                )
                if isinstance(sector_raw, list) and len(sector_raw) > 0:
                    parsed = self._parse_sector_allocation(sector_raw)
                    if parsed:
                        result["sector_allocation"] = parsed

            # --- Asset Allocation ---
            if result["asset_allocation"] is None:
                asset_raw = (
                    node.get("assetAllocation")
                    or node.get("asset_allocation")
                    or node.get("assetComposition")
                )
                if isinstance(asset_raw, dict) and asset_raw:
                    parsed = self._parse_asset_allocation_from_dict(asset_raw)
                    if parsed:
                        result["asset_allocation"] = parsed
                elif isinstance(asset_raw, list) and len(asset_raw) > 0:
                    parsed = self._parse_asset_allocation_from_list(asset_raw)
                    if parsed:
                        result["asset_allocation"] = parsed

            # Also look for a nested "portfolio" object that may contain all three
            portfolio = node.get("portfolio")
            if isinstance(portfolio, dict):
                if result["top_holdings"] is None:
                    h = portfolio.get("holdings") or portfolio.get("topHoldings")
                    if isinstance(h, list) and h:
                        parsed = self._parse_top_holdings(h)
                        if parsed:
                            result["top_holdings"] = parsed
                if result["sector_allocation"] is None:
                    s = portfolio.get("sectorAllocation") or portfolio.get("sectors")
                    if isinstance(s, list) and s:
                        parsed = self._parse_sector_allocation(s)
                        if parsed:
                            result["sector_allocation"] = parsed
                if result["asset_allocation"] is None:
                    a = portfolio.get("assetAllocation") or portfolio.get("assets")
                    if isinstance(a, dict) and a:
                        parsed = self._parse_asset_allocation_from_dict(a)
                        if parsed:
                            result["asset_allocation"] = parsed

            # Early exit if all found
            if all(v is not None for v in result.values()):
                break

        return result

    def _parse_top_holdings(self, items: list) -> list[dict] | None:
        """Parse a list of holding objects into [{name, percentage, sector}]."""
        holdings: list[dict] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            name = (
                item.get("name")
                or item.get("companyName")
                or item.get("company_name")
                or item.get("stockName")
                or item.get("stock_name")
                or item.get("holdingName")
                or item.get("instrument_name")
                or item.get("instrumentName")
            )
            if not name or not str(name).strip():
                continue

            pct = self._to_float(
                item.get("percentage")
                or item.get("weightage")
                or item.get("weight")
                or item.get("corpus_per")
                or item.get("corpusPer")
                or item.get("allocation")
                or item.get("value")
            )

            sector = (
                item.get("sector")
                or item.get("sectorName")
                or item.get("sector_name")
                or item.get("industry")
            )

            holding: dict[str, Any] = {"name": str(name).strip()}
            if pct is not None:
                holding["percentage"] = round(pct, 2)
            if sector and str(sector).strip():
                holding["sector"] = str(sector).strip()
            holdings.append(holding)

        # Keep top 25 max
        if not holdings:
            return None
        return holdings[:25]

    def _parse_sector_allocation(self, items: list) -> list[dict] | None:
        """Parse sector allocation list into [{sector, percentage}]."""
        sectors: list[dict] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            sector = (
                item.get("sector")
                or item.get("sectorName")
                or item.get("sector_name")
                or item.get("name")
                or item.get("label")
            )
            if not sector or not str(sector).strip():
                continue

            pct = self._to_float(
                item.get("percentage")
                or item.get("weightage")
                or item.get("weight")
                or item.get("allocation")
                or item.get("value")
                or item.get("corpus_per")
            )

            entry: dict[str, Any] = {"sector": str(sector).strip()}
            if pct is not None:
                entry["percentage"] = round(pct, 2)
            sectors.append(entry)

        return sectors if sectors else None

    def _parse_asset_allocation_from_dict(self, obj: dict) -> dict | None:
        """Parse asset allocation from a dict with equity/debt/cash keys."""
        equity = self._to_float(
            obj.get("equity") or obj.get("equityPer") or obj.get("equity_per")
        )
        debt = self._to_float(
            obj.get("debt") or obj.get("debtPer") or obj.get("debt_per")
        )
        cash = self._to_float(
            obj.get("cash") or obj.get("cashPer") or obj.get("cash_per")
            or obj.get("moneyMarket") or obj.get("money_market")
        )
        other = self._to_float(
            obj.get("other") or obj.get("otherPer") or obj.get("other_per")
            or obj.get("others")
        )
        if equity is None and debt is None and cash is None and other is None:
            return None
        return {
            "equity_pct": round(equity, 2) if equity is not None else None,
            "debt_pct": round(debt, 2) if debt is not None else None,
            "cash_pct": round(cash, 2) if cash is not None else None,
            "other_pct": round(other, 2) if other is not None else None,
        }

    def _parse_asset_allocation_from_list(self, items: list) -> dict | None:
        """Parse asset allocation from a list of {type/name, percentage} objects."""
        mapping: dict[str, float | None] = {
            "equity_pct": None,
            "debt_pct": None,
            "cash_pct": None,
            "other_pct": None,
        }
        for item in items:
            if not isinstance(item, dict):
                continue
            label = str(
                item.get("type") or item.get("name") or item.get("assetType")
                or item.get("asset_type") or item.get("label") or ""
            ).strip().lower()
            pct = self._to_float(
                item.get("percentage") or item.get("weightage")
                or item.get("weight") or item.get("value") or item.get("allocation")
            )
            if not label:
                continue
            if "equity" in label or "stock" in label:
                mapping["equity_pct"] = round(pct, 2) if pct is not None else None
            elif "debt" in label or "bond" in label or "fixed" in label:
                mapping["debt_pct"] = round(pct, 2) if pct is not None else None
            elif "cash" in label or "money" in label or "liquid" in label:
                mapping["cash_pct"] = round(pct, 2) if pct is not None else None
            elif "other" in label or "commodity" in label or "gold" in label or "reit" in label:
                mapping["other_pct"] = round(pct, 2) if pct is not None else None

        if all(v is None for v in mapping.values()):
            return None
        return mapping

    # ── HTML table fallback parsing ────────────────────────────────

    def _parse_holdings_from_html(self, html: str) -> dict[str, Any]:
        """Fallback: try to extract holdings from HTML tables/sections."""
        result: dict[str, Any] = {
            "top_holdings": None,
            "sector_allocation": None,
            "asset_allocation": None,
        }

        # Look for top holdings in HTML tables
        holdings_section = re.search(
            r'(?:Top\s+Holdings?|Portfolio\s+Holdings?|Stock\s+Holdings?)'
            r'.*?<table[^>]*>(.*?)</table>',
            html,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if holdings_section:
            table_html = holdings_section.group(1)
            holdings = self._parse_html_table_holdings(table_html)
            if holdings:
                result["top_holdings"] = holdings

        # Look for sector allocation in HTML tables
        sector_section = re.search(
            r'(?:Sector\s+Allocation|Sector\s+Wise|Sector\s+Exposure)'
            r'.*?<table[^>]*>(.*?)</table>',
            html,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if sector_section:
            table_html = sector_section.group(1)
            sectors = self._parse_html_table_sectors(table_html)
            if sectors:
                result["sector_allocation"] = sectors

        # Look for asset allocation in HTML
        asset_section = re.search(
            r'(?:Asset\s+Allocation|Asset\s+Composition)'
            r'.*?<(?:table|div)[^>]*>(.*?)</(?:table|div)>',
            html,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if asset_section:
            raw_text = self._strip_tags(asset_section.group(1)).lower()
            asset = self._extract_asset_allocation_from_text(raw_text)
            if asset:
                result["asset_allocation"] = asset

        return result

    def _parse_html_table_holdings(self, table_html: str) -> list[dict] | None:
        """Parse holdings from an HTML table body."""
        rows = re.findall(r"<tr[^>]*>(.*?)</tr>", table_html, flags=re.DOTALL | re.IGNORECASE)
        holdings: list[dict] = []
        for row_html in rows:
            cells = re.findall(r"<td[^>]*>(.*?)</td>", row_html, flags=re.DOTALL | re.IGNORECASE)
            if len(cells) < 2:
                continue
            name = self._strip_tags(cells[0])
            if not name or name.lower() in ("name", "stock", "holding", "company"):
                continue  # skip header row
            pct = self._to_float(self._strip_tags(cells[1]))
            holding: dict[str, Any] = {"name": name}
            if pct is not None:
                holding["percentage"] = round(pct, 2)
            if len(cells) >= 3:
                sector = self._strip_tags(cells[2])
                if sector and sector.lower() not in ("sector", "industry", ""):
                    holding["sector"] = sector
            holdings.append(holding)
        return holdings[:25] if holdings else None

    def _parse_html_table_sectors(self, table_html: str) -> list[dict] | None:
        """Parse sector allocation from an HTML table."""
        rows = re.findall(r"<tr[^>]*>(.*?)</tr>", table_html, flags=re.DOTALL | re.IGNORECASE)
        sectors: list[dict] = []
        for row_html in rows:
            cells = re.findall(r"<td[^>]*>(.*?)</td>", row_html, flags=re.DOTALL | re.IGNORECASE)
            if len(cells) < 2:
                continue
            sector = self._strip_tags(cells[0])
            if not sector or sector.lower() in ("sector", "name", ""):
                continue
            pct = self._to_float(self._strip_tags(cells[1]))
            entry: dict[str, Any] = {"sector": sector}
            if pct is not None:
                entry["percentage"] = round(pct, 2)
            sectors.append(entry)
        return sectors if sectors else None

    def _extract_asset_allocation_from_text(self, text: str) -> dict | None:
        """Extract asset allocation percentages from raw text."""
        mapping: dict[str, float | None] = {
            "equity_pct": None,
            "debt_pct": None,
            "cash_pct": None,
            "other_pct": None,
        }
        for label_key, patterns in [
            ("equity_pct", [r"equity[:\s]+?([\d.]+)\s*%?"]),
            ("debt_pct", [r"debt[:\s]+?([\d.]+)\s*%?", r"fixed\s*income[:\s]+?([\d.]+)\s*%?"]),
            ("cash_pct", [r"cash[:\s]+?([\d.]+)\s*%?", r"money\s*market[:\s]+?([\d.]+)\s*%?"]),
            ("other_pct", [r"other[s]?[:\s]+?([\d.]+)\s*%?"]),
        ]:
            for pattern in patterns:
                match = re.search(pattern, text, flags=re.IGNORECASE)
                if match:
                    val = self._to_float(match.group(1))
                    if val is not None:
                        mapping[label_key] = round(val, 2)
                    break

        if all(v is None for v in mapping.values()):
            return None
        return mapping

    # ── ETMoney URL discovery ──────────────────────────────────────

    def _discover_detail_links(self) -> dict[str, str]:
        """Crawl ETMoney explore/category pages to build a map of
        scheme_name_key -> relative detail link (e.g. /mutual-funds/slug/123).
        """
        base = self.settings.discover_mf_primary_url.rstrip("/")
        detail_links: dict[str, str] = {}  # normalized_name -> rel_link

        explore_urls = [
            f"{base}/mutual-funds/explore",
            f"{base}/mutual-funds",
        ]

        all_rel_links: set[str] = set()

        for url in explore_urls:
            try:
                html = self._get_text(url, timeout=16, retries=1)
            except Exception:
                logger.debug("Holdings: explore fetch failed url=%s", url, exc_info=True)
                continue

            # Collect category page links
            category_links = sorted(
                {
                    link
                    for link in re.findall(
                        r'href="(/mutual-funds/(?:equity|debt|hybrid|other|solution-oriented)/[^"]+/\d+)"',
                        html,
                    )
                    if link
                }
            )

            # Collect detail links from explore page
            detail_matches = re.findall(
                r'href="(/mutual-funds/(?!compare|explore|featured|filter|fund-houses|equity|debt|hybrid|other|solution-oriented)[^"]+/\d+)"',
                html,
                flags=re.IGNORECASE,
            )
            all_rel_links.update(unescape(m.strip()) for m in detail_matches if m)

            # Visit category pages to find more detail links
            for rel_link in category_links:
                cat_url = f"{base}{rel_link}"
                try:
                    cat_html = self._get_text(cat_url, timeout=14, retries=0)
                except Exception:
                    logger.debug("Holdings: category fetch failed url=%s", cat_url, exc_info=True)
                    continue
                cat_detail_matches = re.findall(
                    r'href="(/mutual-funds/(?!compare|explore|featured|filter|fund-houses|equity|debt|hybrid|other|solution-oriented)[^"]+/\d+)"',
                    cat_html,
                    flags=re.IGNORECASE,
                )
                all_rel_links.update(unescape(m.strip()) for m in cat_detail_matches if m)
                time.sleep(random.uniform(_RATE_LIMIT_MIN, _RATE_LIMIT_MAX))

        # Build name -> link mapping from the slugs
        for rel_link in all_rel_links:
            # Extract slug: /mutual-funds/<slug>/<id>
            parts = [p for p in rel_link.strip().split("/") if p]
            if len(parts) < 3:
                continue
            slug = parts[-2].strip().lower()
            if not slug:
                continue
            # Normalize slug to a name key (replace hyphens with spaces, lowercase)
            name_key = slug.replace("-", " ").strip()
            if name_key:
                detail_links[name_key] = rel_link

        logger.info(
            "Holdings: discovered %d detail links from ETMoney explore pages",
            len(detail_links),
        )
        return detail_links

    @staticmethod
    def _normalize_name_for_matching(name: str) -> str:
        """Normalize a fund name for fuzzy matching against URL slugs."""
        text = name.lower().strip()
        # Remove common suffixes and noise
        text = re.sub(r"\s*-\s*(direct|regular)\s*(plan)?\s*$", "", text, flags=re.IGNORECASE)
        text = re.sub(r"\s*-\s*(growth|idcw|dividend)\s*(option)?\s*$", "", text, flags=re.IGNORECASE)
        # Remove special characters, keep spaces
        text = re.sub(r"[^a-z0-9\s]", " ", text)
        text = re.sub(r"\s+", " ", text).strip()
        return text

    def _match_scheme_to_link(
        self,
        scheme_name: str,
        detail_links: dict[str, str],
    ) -> str | None:
        """Try to match a scheme_name to an ETMoney detail link."""
        norm_name = self._normalize_name_for_matching(scheme_name)
        if not norm_name:
            return None

        # Exact match
        if norm_name in detail_links:
            return detail_links[norm_name]

        # Try matching by checking if the slug is contained in the name or vice versa
        name_tokens = set(norm_name.split())
        best_match: str | None = None
        best_overlap = 0

        for slug_key, rel_link in detail_links.items():
            slug_tokens = set(slug_key.split())
            overlap = len(name_tokens & slug_tokens)
            # Require at least 3 overlapping tokens or 60% overlap
            min_len = min(len(name_tokens), len(slug_tokens))
            if min_len == 0:
                continue
            overlap_ratio = overlap / min_len
            if overlap >= 3 and overlap_ratio >= 0.6 and overlap > best_overlap:
                best_overlap = overlap
                best_match = rel_link

        return best_match

    # ── Core scraping logic ────────────────────────────────────────

    def fetch_holdings_for_fund(
        self,
        detail_url: str,
    ) -> dict[str, Any]:
        """Fetch and parse holdings data from a single ETMoney detail page.

        Returns a dict with keys: top_holdings, sector_allocation,
        asset_allocation (any may be None).
        """
        empty: dict[str, Any] = {
            "top_holdings": None,
            "sector_allocation": None,
            "asset_allocation": None,
        }

        try:
            html = self._get_text(detail_url, timeout=10, retries=1)
        except Exception:
            logger.debug(
                "Holdings: failed to fetch detail page url=%s",
                detail_url,
                exc_info=True,
            )
            return empty

        result = dict(empty)

        # Primary: extract from compSchemeDTO JS variable (ET Money's data format)
        comp_data = self._extract_comp_scheme_dto(html)
        if comp_data:
            for key in ("top_holdings", "sector_allocation", "asset_allocation"):
                if comp_data.get(key) is not None:
                    result[key] = comp_data[key]

        # Secondary: extract from __NEXT_DATA__ (some pages)
        if any(result[k] is None for k in ("top_holdings", "sector_allocation", "asset_allocation")):
            next_data = self._extract_next_data(html)
            if next_data:
                nd_result = self._extract_holdings_from_next_data(next_data)
                for key in ("top_holdings", "sector_allocation", "asset_allocation"):
                    if result[key] is None and nd_result.get(key) is not None:
                        result[key] = nd_result[key]

        return result

    # ── MoneyControl holdings extraction ───────────────────────────

    @staticmethod
    def _make_mc_slug(scheme_name: str) -> str:
        """Convert scheme name to MoneyControl URL slug."""
        name = scheme_name
        # Strip all plan/growth/option suffixes
        name = re.sub(
            r'\s*[-–]\s*(Direct\s+Plan|Growth\s+Option|Growth|Direct|Plan)\s*',
            ' ', name, flags=re.I,
        )
        name = re.sub(r'\s*\(formerly.*?\)', '', name, flags=re.I)
        name = re.sub(r'\s*\(erstwhile.*?\)', '', name, flags=re.I)
        name = name.strip(' -')
        slug = name.lower()
        slug = re.sub(r'[^a-z0-9\s-]', '', slug)
        slug = re.sub(r'\s+', '-', slug.strip())
        slug = re.sub(r'-+', '-', slug)
        slug += "-direct-plan-growth"
        return slug

    _MC_UA = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    )

    def _fetch_mc_holdings(self, scheme_name: str) -> dict[str, Any]:
        """Fetch stock-level holdings from MoneyControl portfolio page."""
        empty = {"top_holdings": None, "sector_allocation": None}
        slug = self._make_mc_slug(scheme_name)
        url = f"https://www.moneycontrol.com/mutual-funds/{slug}/portfolio-holdings/"

        try:
            resp = requests.get(url, headers={"User-Agent": self._MC_UA}, timeout=12)
            if resp.status_code != 200:
                return empty
            html = resp.text
        except Exception:
            return empty

        if len(html) < 50000:
            return empty

        # Extract stock holdings from table rows
        pattern = re.compile(
            r'stockpricequote/([^/"]+)/[^"]*"[^>]*>'
            r'([^<]+)</a>'
            r'.*?</td>\s*'
            r'<td>([^<]*?)</td>\s*'
            r'<td>[\d,.]+</td>\s*'
            r'<td>([\d.]+)%</td>',
            re.DOTALL,
        )

        all_matches = list(pattern.finditer(html))
        if not all_matches:
            return empty

        # Take first half only (MC shows two monthly tables)
        half = len(all_matches) // 2 if len(all_matches) > 10 else len(all_matches)
        holdings: list[dict] = []
        seen: set[str] = set()

        for m in all_matches[:half]:
            sector_url = m.group(1).replace("-", " ").title()
            name = m.group(2).strip()
            sector_text = m.group(3).strip()
            pct = self._to_float(m.group(4))

            sector = sector_text if sector_text and not re.match(r'^[\d.]+$', sector_text) else sector_url
            sector = sector.replace("&amp;", "&")

            if name and name not in seen and pct is not None and pct > 0:
                seen.add(name)
                holdings.append({
                    "name": name,
                    "percentage": round(pct, 2),
                    "sector": sector,
                })

        if not holdings:
            return empty

        holdings.sort(key=lambda x: x["percentage"], reverse=True)
        top_holdings = holdings[:25]

        # Aggregate sector allocation
        sectors: dict[str, float] = {}
        for h in holdings:
            s = h["sector"]
            sectors[s] = round(sectors.get(s, 0) + h["percentage"], 2)
        sector_alloc = [
            {"sector": s, "percentage": p}
            for s, p in sorted(sectors.items(), key=lambda x: -x[1])
        ]

        return {
            "top_holdings": top_holdings,
            "sector_allocation": sector_alloc,
        }

    def fetch_all(self, funds: list[dict], *, source: str = "all") -> list[dict]:
        """Fetch holdings for a list of funds.

        Each fund dict should have: scheme_code, scheme_name.
        source: "all" (both), "etmoney" (ET Money only), "moneycontrol" (MC only)
        Returns list of dicts with scheme_code + holdings data.

        This method runs synchronously (called from a thread executor).
        """
        # Discover ETMoney detail links
        detail_links = self._discover_detail_links() if source in ("all", "etmoney") else {}
        base = self.settings.discover_mf_primary_url.rstrip("/")

        results: list[dict] = []
        matched = 0
        fetched = 0
        with_holdings = 0

        if source == "moneycontrol":
            # Skip ET Money pass entirely — jump to MC
            logger.info("Skipping ET Money pass (source=moneycontrol)")
            # Go straight to MoneyControl for all funds
            mc_candidates = list(funds[:_MAX_FUNDS_PER_RUN])
            mc_success = 0
            for j, fund in enumerate(mc_candidates):
                mc_data = self._fetch_mc_holdings(fund["scheme_name"])
                if mc_data.get("top_holdings"):
                    mc_success += 1
                    results.append({
                        "scheme_code": fund["scheme_code"],
                        "top_holdings": mc_data["top_holdings"],
                        "sector_allocation": mc_data.get("sector_allocation"),
                        "asset_allocation": None,
                    })
                if (j + 1) % 50 == 0:
                    logger.info(
                        "MoneyControl holdings: %d/%d processed, %d with stock data",
                        j + 1, len(mc_candidates), mc_success,
                    )
                time.sleep(random.uniform(_RATE_LIMIT_MIN, _RATE_LIMIT_MAX))
            logger.info(
                "MoneyControl pass complete: %d candidates, %d with stock-level holdings",
                len(mc_candidates), mc_success,
            )
            return results

        for i, fund in enumerate(funds[:_MAX_FUNDS_PER_RUN]):
            scheme_code = fund["scheme_code"]
            scheme_name = fund["scheme_name"]

            rel_link = self._match_scheme_to_link(scheme_name, detail_links)
            if not rel_link:
                logger.debug(
                    "Holdings: no ETMoney link match for scheme=%s name=%s",
                    scheme_code,
                    scheme_name,
                )
                continue

            matched += 1
            detail_url = f"{base}{rel_link}"
            holdings_data = self.fetch_holdings_for_fund(detail_url)
            fetched += 1

            has_data = any(
                holdings_data[k] is not None
                for k in ("top_holdings", "sector_allocation", "asset_allocation")
            )

            if has_data:
                with_holdings += 1
                results.append({
                    "scheme_code": scheme_code,
                    "top_holdings": holdings_data["top_holdings"],
                    "sector_allocation": holdings_data["sector_allocation"],
                    "asset_allocation": holdings_data["asset_allocation"],
                })

            if (i + 1) % 50 == 0:
                logger.info(
                    "Holdings progress: %d/%d processed, %d matched, %d fetched, %d with data",
                    i + 1,
                    min(len(funds), _MAX_FUNDS_PER_RUN),
                    matched,
                    fetched,
                    with_holdings,
                )

            # Rate limiting
            time.sleep(random.uniform(_RATE_LIMIT_MIN, _RATE_LIMIT_MAX))

        logger.info(
            "ET Money holdings fetch complete: %d funds, %d matched, %d fetched, %d with data",
            min(len(funds), _MAX_FUNDS_PER_RUN),
            matched,
            fetched,
            with_holdings,
        )

        # Second pass: MoneyControl for funds missing top_holdings
        codes_with_holdings = {r["scheme_code"] for r in results if r.get("top_holdings")}
        mc_candidates = [
            f for f in funds[:_MAX_FUNDS_PER_RUN]
            if f["scheme_code"] not in codes_with_holdings
        ]
        mc_success = 0
        for j, fund in enumerate(mc_candidates):
            mc_data = self._fetch_mc_holdings(fund["scheme_name"])
            if mc_data.get("top_holdings"):
                mc_success += 1
                # Find existing result to merge, or create new
                existing = next((r for r in results if r["scheme_code"] == fund["scheme_code"]), None)
                if existing:
                    existing["top_holdings"] = mc_data["top_holdings"]
                    if mc_data.get("sector_allocation"):
                        existing["sector_allocation"] = mc_data["sector_allocation"]
                else:
                    results.append({
                        "scheme_code": fund["scheme_code"],
                        "top_holdings": mc_data["top_holdings"],
                        "sector_allocation": mc_data.get("sector_allocation"),
                        "asset_allocation": None,
                    })

            if (j + 1) % 50 == 0:
                logger.info(
                    "MoneyControl holdings: %d/%d processed, %d with stock data",
                    j + 1, len(mc_candidates), mc_success,
                )
            time.sleep(random.uniform(_RATE_LIMIT_MIN, _RATE_LIMIT_MAX))

        logger.info(
            "MoneyControl pass complete: %d candidates, %d with stock-level holdings",
            len(mc_candidates), mc_success,
        )

        return results


# ── Database operations ────────────────────────────────────────────

async def _fetch_funds_needing_refresh(pool) -> list[dict]:
    """Fetch scheme_codes and names that need holdings refresh."""
    rows = await pool.fetch(f"""
        SELECT scheme_code, scheme_name
        FROM {_MF_TABLE}
        WHERE (holdings_as_of IS NULL
           OR holdings_as_of < CURRENT_DATE - INTERVAL '7 days'
           OR top_holdings IS NULL)
          AND nav_date >= CURRENT_DATE - INTERVAL '90 days'
          AND LOWER(COALESCE(plan_type, 'direct')) = 'direct'
          AND COALESCE(option_type, '') NOT ILIKE '%%idcw%%'
          AND scheme_name NOT ILIKE '%%income distribution%%'
          AND scheme_name NOT ILIKE '%%idcw%%'
          AND scheme_name NOT ILIKE '%%icdw%%'
          AND scheme_name NOT ILIKE '%%idwc%%'
          AND scheme_name NOT ILIKE '%%fmp%%'
          AND scheme_name NOT ILIKE '%%fixed maturity%%'
          AND scheme_name NOT ILIKE '%%close ended%%'
          AND scheme_name NOT ILIKE '%%closed ended%%'
          AND scheme_name NOT ILIKE '%%capital protection%%'
          AND scheme_name NOT ILIKE '%%unclaimed%%'
          AND scheme_name NOT ILIKE '%%bonus%%'
          AND category != 'Income'
        ORDER BY
            holdings_as_of IS NULL DESC,
            score DESC NULLS LAST,
            holdings_as_of ASC
        LIMIT $1
    """, _MAX_FUNDS_PER_RUN)
    return [dict(r) for r in rows]


async def _persist_holdings(pool, results: list[dict]) -> int:
    """Persist holdings data to the database in batches."""
    if not results:
        return 0

    updated = 0
    for i in range(0, len(results), _BATCH_SIZE):
        batch = results[i : i + _BATCH_SIZE]
        async with pool.acquire() as conn:
            async with conn.transaction():
                for row in batch:
                    top_holdings_json = (
                        json.dumps(row["top_holdings"])
                        if row["top_holdings"] is not None
                        else None
                    )
                    sector_json = (
                        json.dumps(row["sector_allocation"])
                        if row["sector_allocation"] is not None
                        else None
                    )
                    asset_json = (
                        json.dumps(row["asset_allocation"])
                        if row["asset_allocation"] is not None
                        else None
                    )

                    result = await conn.execute(f"""
                        UPDATE {_MF_TABLE}
                        SET top_holdings = COALESCE($2::jsonb, top_holdings),
                            sector_allocation = COALESCE($3::jsonb, sector_allocation),
                            asset_allocation = COALESCE($4::jsonb, asset_allocation),
                            holdings_as_of = CURRENT_DATE
                        WHERE scheme_code = $1
                    """, row["scheme_code"], top_holdings_json, sector_json, asset_json)

                    if result and result.endswith("1"):
                        updated += 1

        logger.info(
            "Holdings persist batch %d-%d: %d updated so far",
            i + 1,
            min(i + _BATCH_SIZE, len(results)),
            updated,
        )

    return updated


# ── Sync wrapper for thread executor ───────────────────────────────

_scraper = DiscoverMfHoldingsScraper()


def _fetch_holdings_sync(funds: list[dict], source: str = "all") -> list[dict]:
    """Synchronous entry point for the thread executor."""
    return _scraper.fetch_all(funds, source=source)


# ── Public async entry points ──────────────────────────────────────

async def _run_holdings(source: str = "all") -> None:
    """Core holdings job logic with source filter."""
    pool = await get_pool()
    funds = await _fetch_funds_needing_refresh(pool)
    if not funds:
        logger.info("Holdings job (%s): no funds need refresh", source)
        return

    logger.info("Holdings job (%s): %d funds to process", source, len(funds))

    loop = asyncio.get_event_loop()
    results = await loop.run_in_executor(
        get_job_executor("discover-mf-holdings"),
        _fetch_holdings_sync,
        funds,
        source,
    )

    if not results:
        logger.info("Holdings job (%s): no data found", source)
        return

    updated = await _persist_holdings(pool, results)
    logger.info(
        "Holdings job (%s) complete: %d funds, %d with data, %d updated",
        source, len(funds), len(results), updated,
    )


async def run_discover_mf_holdings_job() -> None:
    """Full holdings job: ET Money + MoneyControl."""
    try:
        await _run_holdings("all")
    except Exception:
        logger.exception("Holdings job failed")


async def run_discover_mf_holdings_etmoney_job() -> None:
    """ET Money only: asset allocation."""
    try:
        await _run_holdings("etmoney")
    except Exception:
        logger.exception("Holdings job (etmoney) failed")


async def run_discover_mf_holdings_moneycontrol_job() -> None:
    """MoneyControl only: stock-level holdings + sector allocation."""
    try:
        await _run_holdings("moneycontrol")
    except Exception:
        logger.exception("Holdings job (moneycontrol) failed")

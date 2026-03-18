from __future__ import annotations

import asyncio
import json
import logging
import math
import re
from collections import deque
from datetime import datetime, timezone
from html import unescape
from typing import Any

import requests

from app.core.config import get_settings
from app.core.database import get_pool
from app.scheduler.base import BaseScraper
from app.scheduler.job_executors import get_job_executor
from app.services import discover_service

logger = logging.getLogger(__name__)


class DiscoverMutualFundScraper(BaseScraper):
    def __init__(self) -> None:
        super().__init__()
        self.settings = get_settings()

    def _walk(self, obj: Any):
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

    def _normalize_risk(self, value: Any) -> str | None:
        text = str(value or "").strip().lower()
        if not text:
            return None
        mapping = {
            "low": "Low",
            "moderately low": "Moderately Low",
            "moderate": "Moderate",
            "moderately high": "Moderately High",
            "high": "High",
            "very high": "Very High",
        }
        for key, label in mapping.items():
            if key in text:
                return label
        return text.title()

    def _infer_risk_from_category(self, category: str | None, sub_category: str | None) -> str | None:
        text = f"{category or ''} {sub_category or ''}".strip().lower()
        if not text:
            return None
        if any(token in text for token in ("overnight", "liquid", "ultra short", "money market", "arbitrage")):
            return "Low"
        if any(
            token in text
            for token in (
                "gilt",
                "banking and psu",
                "corporate bond",
                "short duration",
                "low duration",
                "floater",
                "retirement",
            )
        ):
            return "Moderately Low"
        if any(
            token in text
            for token in (
                "small cap",
                "sectoral",
                "thematic",
                "focused",
                "international",
                "aggressive hybrid",
            )
        ):
            return "High"
        if any(token in text for token in ("mid cap", "multi cap", "flexi cap", "elss", "value", "contra")):
            return "Moderately High"
        if any(token in text for token in ("large cap", "index", "balanced hybrid", "dynamic asset allocation")):
            return "Moderate"
        if "equity" in text:
            return "Moderately High"
        if "hybrid" in text:
            return "Moderate"
        if "debt" in text:
            return "Moderately Low"
        return None

    @staticmethod
    def _extract_json_object_after_marker(text: str, marker: str) -> dict[str, Any] | None:
        idx = text.find(marker)
        if idx < 0:
            return None
        start = text.find("{", idx)
        if start < 0:
            return None

        depth = 0
        in_string = False
        escaped = False
        for pos in range(start, len(text)):
            ch = text[pos]
            if in_string:
                if escaped:
                    escaped = False
                elif ch == "\\":
                    escaped = True
                elif ch == '"':
                    in_string = False
                continue
            if ch == '"':
                in_string = True
                continue
            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    raw = text[start : pos + 1]
                    try:
                        parsed = json.loads(raw)
                        if isinstance(parsed, dict):
                            return parsed
                    except json.JSONDecodeError:
                        return None
                    return None
        return None

    @staticmethod
    def _extract_attr(tag_text: str, attr_name: str) -> str | None:
        pattern = rf'{re.escape(attr_name)}\s*=\s*"([^"]*)"'
        match = re.search(pattern, tag_text, flags=re.IGNORECASE)
        if not match:
            return None
        return unescape(match.group(1)).strip() or None

    def _normalize_fund_name_key(self, text: str) -> str:
        value = unescape(str(text or "")).strip().lower()
        if not value:
            return ""
        value = value.replace("&", " and ")
        value = re.sub(r"\([^)]*\)", " ", value)
        value = re.sub(
            r"\b(direct|regular|plan|growth|idcw|dividend|payout|reinvestment|reinvest|option|bonus|daily|weekly|monthly|quarterly|annual|retail)\b",
            " ",
            value,
            flags=re.IGNORECASE,
        )
        value = re.sub(r"[-_/]", " ", value)
        value = re.sub(r"\s+", " ", value).strip()
        return value

    def _normalize_heading_labels(self, main: str | None, sub: str | None) -> tuple[str | None, str | None]:
        main_value = (main or "").strip()
        sub_value = (sub or "").strip()
        text = f"{main_value} {sub_value}".lower()

        if "equity scheme" in text or "elss" in text or "large cap" in text or "small cap" in text:
            return "Equity", sub_value or main_value or None
        if "hybrid scheme" in text or "hybrid" in text or "arbitrage" in text:
            return "Hybrid", sub_value or main_value or None
        if "debt scheme" in text or "gilt" in text or "duration" in text or "money market" in text:
            return "Debt", sub_value or main_value or None
        if "index" in text or "etf" in text:
            return "Index", sub_value or main_value or None
        if "solution oriented" in text:
            return "Solution Oriented", sub_value or None
        if "other scheme" in text:
            if "index" in text or "etf" in text:
                return "Index", sub_value or None
            return "Other", sub_value or None

        compact_main = re.sub(r"\s*schemes?\s*", " ", main_value, flags=re.IGNORECASE).strip()
        return compact_main or None, sub_value or None

    def _extract_etmoney_json_blobs(self, html: str) -> list[dict]:
        blobs: list[dict] = []
        patterns = [
            r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
            r"window\.__INITIAL_STATE__\s*=\s*(\{.*?\});",
            r"window\.__PRELOADED_STATE__\s*=\s*(\{.*?\});",
        ]
        for patt in patterns:
            for match in re.finditer(patt, html, flags=re.DOTALL):
                raw = match.group(1).strip()
                try:
                    parsed = json.loads(raw)
                    if isinstance(parsed, dict):
                        blobs.append(parsed)
                except json.JSONDecodeError:
                    continue
        return blobs

    @staticmethod
    def _strip_tags(text: str) -> str:
        raw = re.sub(r"<[^>]+>", " ", text or "", flags=re.IGNORECASE)
        return re.sub(r"\s+", " ", unescape(raw)).strip()

    @staticmethod
    def _extract_number(text: str) -> float | None:
        if not text:
            return None
        match = re.search(r"-?\d[\d,]*(?:\.\d+)?", text)
        if not match:
            return None
        try:
            return float(match.group(0).replace(",", ""))
        except (TypeError, ValueError):
            return None

    def _parse_aum_cr(self, text: str | None) -> float | None:
        if not text:
            return None
        raw = self._strip_tags(text)
        value = self._extract_number(raw)
        if value is None:
            return None
        lowered = raw.lower()
        if "lakh" in lowered:
            value = value / 100.0
        elif "million" in lowered:
            value = value / 10.0
        elif "billion" in lowered:
            value = value * 100.0
        return round(value, 2)

    def _parse_age_years(self, text: str | None) -> float | None:
        if not text:
            return None
        raw = self._strip_tags(text).lower()
        years = 0.0
        months = 0.0
        y = re.search(r"(\d+(?:\.\d+)?)\s*(?:yr|yrs|year|years)\b", raw)
        if y:
            years = float(y.group(1))
        m = re.search(r"(\d+(?:\.\d+)?)\s*(?:m|mon|month|months)\b", raw)
        if m:
            months = float(m.group(1))
        total = years + (months / 12.0)
        if total > 0:
            return round(total, 1)
        direct_year = self._extract_number(raw)
        if direct_year is not None and ("year" in raw or "yr" in raw):
            return round(direct_year, 1)
        return None

    @staticmethod
    def _slug_from_detail_link(link: str) -> str:
        parts = [p for p in str(link or "").strip().split("/") if p]
        if len(parts) < 2:
            return ""
        return parts[-2].strip().lower()

    @staticmethod
    def _extract_table_value(html: str, title_pattern: str) -> str | None:
        patt = (
            rf'<strong class="mfSceme-key-title">\s*{title_pattern}\s*</strong>'
            r"\s*</td>\s*<td class=\"item\">\s*(.*?)</td>"
        )
        match = re.search(patt, html, flags=re.IGNORECASE | re.DOTALL)
        if not match:
            return None
        return match.group(1)

    @staticmethod
    def _extract_etmoney_detail_links(html: str) -> set[str]:
        links = re.findall(
            r'href="(/mutual-funds/(?!compare|explore|featured|filter|fund-houses|equity|debt|hybrid|other|solution-oriented)[^"]+/\d+)"',
            html,
            flags=re.IGNORECASE,
        )
        return {unescape(link.strip()) for link in links if link and link.strip()}

    @staticmethod
    def _extract_counterpart_link(html: str) -> str | None:
        for match in re.finditer(
            r'class="counterpart-plan-link"[^>]*href="([^"]+)"',
            html,
            flags=re.IGNORECASE,
        ):
            href = unescape(match.group(1)).strip()
            if href.startswith("/mutual-funds/") and re.search(r"/\d+$", href):
                return href
        return None

    def _parse_etmoney_detail_page(self, html: str, rel_link: str) -> dict | None:
        slug = self._slug_from_detail_link(rel_link)
        if not slug:
            return None

        expense_raw = self._extract_table_value(html, r"Expense\s*ratio")
        expense = self._to_float(self._strip_tags(expense_raw or ""))
        if expense is None:
            regex_exp = re.search(r'"expenseRatio"\s*:\s*"([^"]+)"', html, flags=re.IGNORECASE)
            if regex_exp:
                expense = self._to_float(regex_exp.group(1))

        aum_raw = self._extract_table_value(html, r"[^<]*AUM[^<]*")
        if not aum_raw:
            regex_aum = re.search(r'"aum"\s*:\s*"([^"]+)"', html, flags=re.IGNORECASE)
            if regex_aum:
                aum_raw = regex_aum.group(1)
        aum_cr = self._parse_aum_cr(aum_raw)

        risk_raw = self._extract_table_value(html, r"Risk")
        risk_level = self._normalize_risk(self._strip_tags(risk_raw or ""))

        age_raw = self._extract_table_value(html, r"Age")
        if not age_raw:
            regex_age = re.search(r'"fundAge"\s*:\s*"([^"]+)"', html, flags=re.IGNORECASE)
            if regex_age:
                age_raw = regex_age.group(1)
        fund_age_years = self._parse_age_years(age_raw)

        report_card = self._extract_json_object_after_marker(html, '"mfReportCardData":') or {}
        std_dev = self._to_float(report_card.get("standardDeviation") or report_card.get("stdDev"))
        sharpe = self._to_float(report_card.get("sharpeRatio") or report_card.get("sharpe"))
        sortino = self._to_float(report_card.get("sortinoRatio") or report_card.get("sortino"))
        if std_dev is None:
            std_match = re.search(r'"standardDeviation"\s*:\s*(-?\d+(?:\.\d+)?)', html, flags=re.IGNORECASE)
            if std_match:
                std_dev = self._to_float(std_match.group(1))
        if sharpe is None:
            sh_match = re.search(r'"sharpeRatio"\s*:\s*(-?\d+(?:\.\d+)?)', html, flags=re.IGNORECASE)
            if sh_match:
                sharpe = self._to_float(sh_match.group(1))
        if sortino is None:
            so_match = re.search(r'"sortinoRatio"\s*:\s*(-?\d+(?:\.\d+)?)', html, flags=re.IGNORECASE)
            if so_match:
                sortino = self._to_float(so_match.group(1))

        option_type = None
        slug_low = slug.lower()
        if "growth" in slug_low:
            option_type = "Growth"
        elif "idcw" in slug_low or "dividend" in slug_low:
            option_type = "IDCW"

        title_match = re.search(r"<h1[^>]*>(.*?)</h1>", html, flags=re.IGNORECASE | re.DOTALL)
        display_name = self._strip_tags(title_match.group(1)) if title_match else slug.replace("-", " ")
        name_key = self._normalize_fund_name_key(display_name)
        if not name_key:
            return None

        # Extract fund managers from JSON
        fund_managers = None
        try:
            fm_match = re.search(r'"fundManagers"\s*:\s*(\[.*?\])', html, flags=re.DOTALL)
            if fm_match:
                import json as _json
                fm_list = _json.loads(fm_match.group(1))
                if isinstance(fm_list, list) and fm_list:
                    fund_managers = [
                        {"name": fm.get("name", ""), "experience": fm.get("experience", "")}
                        for fm in fm_list
                        if fm.get("name")
                    ]
                    if not fund_managers:
                        fund_managers = None
        except Exception:
            pass

        return {
            "_name_key": name_key,
            "_is_direct": "direct" in slug_low,
            "_slug": slug,
            "scheme_name": display_name,
            "option_type": option_type,
            "expense_ratio": expense,
            "aum_cr": aum_cr,
            "risk_level": risk_level,
            "std_dev": std_dev,
            "sharpe": sharpe,
            "sortino": sortino,
            "fund_age_years": fund_age_years,
            "fund_managers": fund_managers,
            "source_status": "primary",
            "source_timestamp": datetime.now(timezone.utc),
            "primary_source": "etmoney_web",
            "secondary_source": "amfi_nav_file",
        }

    @staticmethod
    def _detail_completeness(row: dict) -> tuple[int, int]:
        metrics = (
            "expense_ratio",
            "aum_cr",
            "risk_level",
            "std_dev",
            "sharpe",
            "sortino",
            "fund_age_years",
        )
        coverage = sum(1 for key in metrics if row.get(key) is not None)
        growth_bias = 1 if str(row.get("option_type") or "").lower() == "growth" else 0
        return coverage, growth_bias

    def _parse_etmoney_detail_pages(
        self,
        detail_links: set[str],
        *,
        max_pages: int = 5000,
    ) -> dict[str, dict]:
        if not detail_links:
            return {}
        base = self.settings.discover_mf_primary_url.rstrip("/")
        queue: deque[str] = deque(sorted(detail_links))
        queued = set(queue)
        seen: set[str] = set()
        out: dict[str, dict] = {}

        while queue and len(seen) < max_pages:
            rel_link = queue.popleft()
            queued.discard(rel_link)
            if rel_link in seen:
                continue
            seen.add(rel_link)

            url = f"{base}{rel_link}"
            try:
                html = self._get_text(url, timeout=8, retries=0)
            except Exception:
                logger.debug("ET Money detail fetch failed url=%s", url, exc_info=True)
                continue

            counterpart = self._extract_counterpart_link(html)
            if (
                counterpart
                and "direct" in counterpart.lower()
                and counterpart not in seen
                and counterpart not in queued
            ):
                queue.appendleft(counterpart)
                queued.add(counterpart)

            detail = self._parse_etmoney_detail_page(html, rel_link)
            if not detail or not detail.get("_is_direct"):
                continue
            key = str(detail.get("_name_key") or "").strip()
            if not key:
                continue

            prior = out.get(key)
            if prior is None or self._detail_completeness(detail) > self._detail_completeness(prior):
                out[key] = detail

        if queue:
            logger.info("ET Money detail enrichment hit max_pages=%d; remaining=%d", max_pages, len(queue))
        logger.info(
            "ET Money detail enrichment scanned=%d direct_rows=%d seed_links=%d",
            len(seen),
            len(out),
            len(detail_links),
        )
        return out

    def _parse_etmoney_candidates(self) -> dict[str, dict]:
        base = self.settings.discover_mf_primary_url.rstrip("/")
        urls = [
            f"{base}/mutual-funds",
            f"{base}/mutual-funds/best-mutual-funds",
            f"{base}/mutual-funds/direct-plans",
        ]
        out: dict[str, dict] = {}
        for url in urls:
            try:
                html = self._get_text(url, timeout=14)
                blobs = self._extract_etmoney_json_blobs(html)
                for blob in blobs:
                    for obj in self._walk(blob):
                        name = (
                            obj.get("schemeName")
                            or obj.get("fundName")
                            or obj.get("name")
                            or obj.get("scheme_name")
                        )
                        if not name:
                            continue
                        name_text = str(name).strip()
                        if not name_text or "direct" not in name_text.lower():
                            continue

                        code = (
                            obj.get("schemeCode")
                            or obj.get("amfiCode")
                            or obj.get("scheme_code")
                            or obj.get("code")
                            or obj.get("id")
                        )
                        if code is None:
                            continue
                        code_text = str(code).strip()
                        if not code_text:
                            continue

                        nav = self._to_float(
                            obj.get("nav")
                            or obj.get("latestNav")
                            or obj.get("currentNav")
                            or obj.get("latest_nav")
                        )
                        if nav is None or nav <= 0:
                            continue

                        category = (
                            obj.get("category")
                            or obj.get("categoryName")
                            or obj.get("category_name")
                        )
                        sub_category = (
                            obj.get("subCategory")
                            or obj.get("subcategory")
                            or obj.get("sub_category")
                        )
                        amc = obj.get("amc") or obj.get("amcName") or obj.get("fundHouse")
                        option_type = obj.get("optionType") or obj.get("planOption")

                        row = {
                            "scheme_code": code_text,
                            "scheme_name": name_text,
                            "amc": str(amc).strip() if amc else None,
                            "category": str(category).strip() if category else None,
                            "sub_category": str(sub_category).strip() if sub_category else None,
                            "plan_type": "direct",
                            "option_type": str(option_type).strip() if option_type else None,
                            "nav": nav,
                            "nav_date": obj.get("navDate") or obj.get("nav_date"),
                            "expense_ratio": self._to_float(obj.get("expenseRatio") or obj.get("expense_ratio")),
                            "aum_cr": self._to_float(obj.get("aum") or obj.get("aumCr") or obj.get("aum_cr")),
                            "risk_level": self._normalize_risk(obj.get("risk") or obj.get("riskLevel") or obj.get("risk_profile")),
                            "returns_1y": self._to_float(obj.get("return1Y") or obj.get("returns1y") or obj.get("return_1y")),
                            "returns_3y": self._to_float(obj.get("return3Y") or obj.get("returns3y") or obj.get("return_3y")),
                            "returns_5y": self._to_float(obj.get("return5Y") or obj.get("returns5y") or obj.get("return_5y")),
                            "std_dev": self._to_float(obj.get("stdDev") or obj.get("standardDeviation")),
                            "sharpe": self._to_float(obj.get("sharpe") or obj.get("sharpeRatio")),
                            "sortino": self._to_float(obj.get("sortino") or obj.get("sortinoRatio")),
                            "source_status": "primary",
                            "source_timestamp": datetime.now(timezone.utc),
                            "primary_source": "etmoney_web",
                            "secondary_source": "amfi_nav_fallback",
                        }
                        out[code_text] = row
            except Exception:
                logger.debug("ET Money scrape failed url=%s", url, exc_info=True)
                continue
        return out

    def _split_category(self, text: str) -> tuple[str | None, str | None]:
        raw = text.strip()
        if not raw:
            return None, None
        compact = re.sub(r"\s+", " ", raw)
        main = compact
        sub = None
        if "(" in compact and ")" in compact:
            inside = compact[compact.find("(") + 1 : compact.rfind(")")]
            if "-" in inside:
                left, right = inside.split("-", 1)
                main = left.strip()
                sub = right.strip()
            else:
                main = inside.strip()
        return self._normalize_heading_labels(main, sub)

    def _parse_etmoney_category_pages(self) -> tuple[dict[str, dict], set[str]]:
        out: dict[str, dict] = {}
        detail_links: set[str] = set()
        base = self.settings.discover_mf_primary_url.rstrip("/")
        try:
            explore_html = self._get_text(f"{base}/mutual-funds/explore", timeout=16)
        except Exception:
            logger.debug("ET Money explore fetch failed", exc_info=True)
            return out, detail_links

        category_links = sorted(
            {
                link
                for link in re.findall(r'href="(/mutual-funds/(?:equity|debt|hybrid|other|solution-oriented|featured|fund-houses)/[^"]+/\d+)"', explore_html)
                if link
            }
        )
        detail_links.update(self._extract_etmoney_detail_links(explore_html))

        for rel_link in category_links:
            url = f"{base}{rel_link}"
            try:
                html = self._get_text(url, timeout=14)
            except Exception:
                logger.debug("ET Money category fetch failed url=%s", url, exc_info=True)
                continue
            detail_links.update(self._extract_etmoney_detail_links(html))

            category_payload = self._extract_json_object_after_marker(html, "var category =")
            category = None
            sub_category = None
            if category_payload:
                primary = str(category_payload.get("primary") or "").strip().lower()
                if primary == "equity":
                    category = "Equity"
                elif primary == "debt":
                    category = "Debt"
                elif primary == "hybrid":
                    category = "Hybrid"
                elif primary:
                    category = str(primary).title()
                sub_category = str(category_payload.get("displayName") or "").strip() or None

            return_map = self._extract_json_object_after_marker(html, "var mfSchemeCalculatorReturnMap =") or {}
            for match in re.finditer(r'<li[^>]*class="[^"]*mfFund-list[^"]*"[^>]*>', html, flags=re.IGNORECASE):
                tag = match.group(0)
                sid = self._extract_attr(tag, "data-sid")
                fund_name = self._extract_attr(tag, "data-fundname")
                if not sid or not fund_name:
                    continue

                metrics = return_map.get(str(sid)) if isinstance(return_map, dict) else None
                xirr = metrics.get("xirrDurationWise") if isinstance(metrics, dict) else {}
                if not isinstance(xirr, dict):
                    xirr = {}

                def _xirr_pct(key: str) -> float | None:
                    value = self._to_float(xirr.get(key))
                    if value is None:
                        return None
                    return round(value * 100.0, 2)

                risk = self._infer_risk_from_category(category, sub_category)
                out[self._normalize_fund_name_key(fund_name)] = {
                    "et_scheme_id": str(sid),
                    "scheme_name": fund_name,
                    "category": category,
                    "sub_category": sub_category,
                    "risk_level": risk,
                    "returns_1y": _xirr_pct("365"),
                    "returns_3y": _xirr_pct("1095"),
                    "returns_5y": _xirr_pct("1825"),
                    "source_status": "primary",
                    "source_timestamp": datetime.now(timezone.utc),
                    "primary_source": "etmoney_web",
                    "secondary_source": "amfi_nav_file",
                }
        return out, detail_links

    def _parse_amfi_fallback(self) -> dict[str, dict]:
        out: dict[str, dict] = {}
        try:
            text = self._get_text(self.settings.discover_mf_fallback_url, timeout=20)
        except Exception:
            logger.debug("AMFI fallback fetch failed", exc_info=True)
            return out

        current_category = ""
        current_amc = None
        for raw_line in text.splitlines():
            line = raw_line.strip()
            if not line:
                continue
            if ";" not in line:
                header = line.lower()
                if re.search(r"schemes?\s*\(", line, flags=re.IGNORECASE):
                    current_category = line
                elif "mutual fund" in header:
                    current_amc = line
                continue
            parts = [p.strip() for p in line.split(";")]
            if len(parts) < 6:
                continue
            scheme_code = parts[0]
            scheme_name = parts[3]
            nav_raw = parts[4]
            nav_date_raw = parts[5]
            if not scheme_code or not scheme_name:
                continue
            if "direct" not in scheme_name.lower():
                continue
            nav = self._to_float(nav_raw)
            if nav is None or nav <= 0:
                continue

            category, sub_category = self._split_category(current_category)
            option_type = None
            name_lower = scheme_name.lower()
            if "growth" in name_lower:
                option_type = "Growth"
            elif "idcw" in name_lower or "dividend" in name_lower:
                option_type = "IDCW"

            amc = current_amc
            if not amc and " - " in scheme_name:
                amc = scheme_name.split(" - ", 1)[0].strip()
            inferred_risk = self._infer_risk_from_category(category, sub_category)
            out[scheme_code] = {
                "scheme_code": scheme_code,
                "scheme_name": scheme_name,
                "amc": amc,
                "category": category,
                "sub_category": sub_category,
                "plan_type": "direct",
                "option_type": option_type,
                "nav": nav,
                "nav_date": nav_date_raw,
                "expense_ratio": None,
                "aum_cr": None,
                "risk_level": inferred_risk,
                "returns_1y": None,
                "returns_3y": None,
                "returns_5y": None,
                "std_dev": None,
                "sharpe": None,
                "sortino": None,
                "source_status": "fallback",
                "source_timestamp": datetime.now(timezone.utc),
                "primary_source": "amfi_nav_file",
                "secondary_source": "etmoney_web",
            }
        return out

    @staticmethod
    def _percentile_rank(values: list[float], target: float) -> float:
        """Return 0-100 percentile rank of *target* within *values*."""
        if not values:
            return 50.0
        eps = 1e-9
        below = sum(1 for v in values if v < (target - eps))
        equal = sum(1 for v in values if abs(v - target) <= eps)
        return ((below + (equal * 0.5)) / len(values)) * 100.0

    @staticmethod
    def _shrink_to_neutral(
        score: float,
        coverage: float,
        *,
        neutral: float = 50.0,
        min_factor: float = 0.35,
    ) -> float:
        c = max(0.0, min(1.0, coverage))
        factor = min_factor + ((1.0 - min_factor) * c)
        return neutral + ((score - neutral) * factor)

    # ── Sub-category normalization map ────────────────────────────────
    _SUBCATEGORY_NORMALIZE_MAP: dict[str, str] = {
        # Equity — exact and variant forms
        "large cap fund": "Large Cap", "large cap": "Large Cap",
        "mid cap fund": "Mid Cap", "mid cap": "Mid Cap",
        "small cap fund": "Small Cap", "small cap": "Small Cap",
        "flexi cap fund": "Flexi Cap", "flexi cap": "Flexi Cap",
        "multi cap fund": "Multi Cap", "multi cap": "Multi Cap",
        "focused fund": "Focused", "value fund": "Value",
        "contra fund": "Contra", "dividend yield fund": "Dividend Yield",
        "large & mid cap fund": "Large & Mid Cap", "large & midcap": "Large & Mid Cap",
        "elss": "ELSS",
        # Index
        "index funds": "Index", "large cap index": "Index", "mid cap index": "Index",
        "small cap index": "Index", "multi cap index": "Index",
        "large & midcap index": "Index", "international index": "Index",
        "other equity index": "Index", "long debt index": "Index", "other etfs": "Index",
        # Sectoral/Thematic
        "sectoral/ thematic": "Sectoral", "sectoral-banking": "Sectoral",
        "sectoral-pharma": "Sectoral", "sectoral-technology": "Sectoral",
        "sectoral-infrastructure": "Sectoral", "thematic": "Thematic",
        "thematic-consumption": "Thematic", "thematic-esg": "Thematic",
        "thematic-mnc": "Thematic", "thematic-psu": "Thematic",
        "energy": "Sectoral", "international": "International",
        # Debt
        "liquid fund": "Liquid", "liquid": "Liquid",
        "money market fund": "Money Market", "money market": "Money Market",
        "overnight fund": "Overnight", "overnight": "Overnight",
        "ultra short duration fund": "Ultra Short Duration", "ultra short duration": "Ultra Short Duration",
        "short duration fund": "Short Duration", "short duration": "Short Duration",
        "low duration fund": "Low Duration", "low duration": "Low Duration",
        "medium duration fund": "Medium Duration", "medium duration": "Medium Duration",
        "medium to long duration fund": "Medium to Long Duration", "medium to long duration": "Medium to Long Duration",
        "long duration fund": "Long Duration", "long duration": "Long Duration",
        "corporate bond fund": "Corporate Bond", "corporate bond": "Corporate Bond",
        "banking and psu fund": "Banking & PSU", "banking and psu": "Banking & PSU",
        "credit risk fund": "Credit Risk", "credit risk": "Credit Risk",
        "gilt fund": "Gilt", "gilt": "Gilt",
        "gilt fund with 10 year constant duration": "Gilt", "gilt with 10 year constant duration": "Gilt",
        "floater fund": "Floater", "floater": "Floater",
        "dynamic bond": "Dynamic Bond", "target maturity": "Target Maturity",
        "fmp": "Target Maturity", "tax efficient income": "Dynamic Bond",
        # Hybrid
        "aggressive hybrid fund": "Aggressive Hybrid", "aggressive hybrid": "Aggressive Hybrid",
        "balanced hybrid fund": "Balanced Hybrid", "balanced hybrid": "Balanced Hybrid",
        "conservative hybrid fund": "Conservative Hybrid", "conservative hybrid": "Conservative Hybrid",
        "dynamic asset allocation or balanced advantage": "Dynamic Asset Allocation",
        "dynamic asset allocation": "Dynamic Asset Allocation",
        "arbitrage fund": "Arbitrage", "arbitrage": "Arbitrage",
        "multi asset allocation": "Multi-Asset", "equity savings": "Equity Savings",
        # Special
        "fof domestic": "FoF Domestic", "fof overseas": "FoF Overseas",
        "strategy": "Value",
        "retirement fund": "Retirement", "retirement solutions": "Retirement",
        "children\u2019s fund": "Children", "children solutions": "Children",
        "children's fund": "Children",
    }

    # ── Sub-category-specific layer weights ────────────────────────────
    _SUBCATEGORY_LAYER_WEIGHTS: dict[str, dict[str, float]] = {
        # 6 layers: performance, consistency, risk, cost, category_fit, beta
        # Beta weight is 0 for debt funds (not relevant), 0.10 for equity/hybrid
        # Dynamic rebalancing handles missing beta gracefully
        "DEFAULT":        {"performance": 0.22, "consistency": 0.22, "risk": 0.18, "cost": 0.13, "category_fit": 0.15, "beta": 0.10},
        # Equity
        "Large Cap":      {"performance": 0.22, "consistency": 0.22, "risk": 0.13, "cost": 0.13, "category_fit": 0.18, "beta": 0.12},
        "Mid Cap":        {"performance": 0.25, "consistency": 0.18, "risk": 0.18, "cost": 0.13, "category_fit": 0.14, "beta": 0.12},
        "Small Cap":      {"performance": 0.25, "consistency": 0.18, "risk": 0.13, "cost": 0.13, "category_fit": 0.18, "beta": 0.13},
        "Flexi Cap":      {"performance": 0.22, "consistency": 0.22, "risk": 0.18, "cost": 0.13, "category_fit": 0.15, "beta": 0.10},
        "Multi Cap":      {"performance": 0.22, "consistency": 0.22, "risk": 0.18, "cost": 0.13, "category_fit": 0.15, "beta": 0.10},
        "ELSS":           {"performance": 0.22, "consistency": 0.22, "risk": 0.18, "cost": 0.13, "category_fit": 0.15, "beta": 0.10},
        "Index":          {"performance": 0.18, "consistency": 0.18, "risk": 0.10, "cost": 0.28, "category_fit": 0.18, "beta": 0.08},
        "Sectoral":       {"performance": 0.25, "consistency": 0.13, "risk": 0.18, "cost": 0.13, "category_fit": 0.18, "beta": 0.13},
        "Thematic":       {"performance": 0.25, "consistency": 0.13, "risk": 0.18, "cost": 0.13, "category_fit": 0.18, "beta": 0.13},
        "Focused":        {"performance": 0.22, "consistency": 0.22, "risk": 0.18, "cost": 0.13, "category_fit": 0.15, "beta": 0.10},
        "Contra":         {"performance": 0.22, "consistency": 0.22, "risk": 0.18, "cost": 0.13, "category_fit": 0.15, "beta": 0.10},
        "Value":          {"performance": 0.22, "consistency": 0.22, "risk": 0.18, "cost": 0.13, "category_fit": 0.15, "beta": 0.10},
        "Dividend Yield": {"performance": 0.22, "consistency": 0.22, "risk": 0.18, "cost": 0.13, "category_fit": 0.15, "beta": 0.10},
        "Large & Mid Cap": {"performance": 0.22, "consistency": 0.22, "risk": 0.18, "cost": 0.13, "category_fit": 0.15, "beta": 0.10},
        "International":  {"performance": 0.27, "consistency": 0.18, "risk": 0.18, "cost": 0.13, "category_fit": 0.14, "beta": 0.10},
        # Debt — no beta (weight = 0)
        "Liquid":         {"performance": 0.15, "consistency": 0.20, "risk": 0.25, "cost": 0.30, "category_fit": 0.10, "beta": 0.0},
        "Money Market":   {"performance": 0.15, "consistency": 0.20, "risk": 0.25, "cost": 0.30, "category_fit": 0.10, "beta": 0.0},
        "Overnight":      {"performance": 0.10, "consistency": 0.15, "risk": 0.25, "cost": 0.35, "category_fit": 0.15, "beta": 0.0},
        "Gilt":           {"performance": 0.20, "consistency": 0.25, "risk": 0.25, "cost": 0.20, "category_fit": 0.10, "beta": 0.0},
        "Corporate Bond": {"performance": 0.20, "consistency": 0.20, "risk": 0.25, "cost": 0.20, "category_fit": 0.15, "beta": 0.0},
        "Credit Risk":    {"performance": 0.20, "consistency": 0.15, "risk": 0.30, "cost": 0.15, "category_fit": 0.20, "beta": 0.0},
        "Short Duration": {"performance": 0.20, "consistency": 0.20, "risk": 0.25, "cost": 0.20, "category_fit": 0.15, "beta": 0.0},
        "Medium Duration": {"performance": 0.20, "consistency": 0.20, "risk": 0.25, "cost": 0.20, "category_fit": 0.15, "beta": 0.0},
        "Long Duration":  {"performance": 0.25, "consistency": 0.20, "risk": 0.25, "cost": 0.15, "category_fit": 0.15, "beta": 0.0},
        "Ultra Short Duration": {"performance": 0.15, "consistency": 0.20, "risk": 0.25, "cost": 0.25, "category_fit": 0.15, "beta": 0.0},
        "Low Duration":   {"performance": 0.15, "consistency": 0.20, "risk": 0.25, "cost": 0.25, "category_fit": 0.15, "beta": 0.0},
        "Banking & PSU":  {"performance": 0.20, "consistency": 0.20, "risk": 0.25, "cost": 0.20, "category_fit": 0.15, "beta": 0.0},
        "Floater":        {"performance": 0.20, "consistency": 0.20, "risk": 0.25, "cost": 0.20, "category_fit": 0.15, "beta": 0.0},
        "Dynamic Bond":   {"performance": 0.20, "consistency": 0.25, "risk": 0.25, "cost": 0.15, "category_fit": 0.15, "beta": 0.0},
        "Target Maturity": {"performance": 0.15, "consistency": 0.25, "risk": 0.25, "cost": 0.20, "category_fit": 0.15, "beta": 0.0},
        "Medium to Long Duration": {"performance": 0.20, "consistency": 0.20, "risk": 0.25, "cost": 0.20, "category_fit": 0.15, "beta": 0.0},
        # Hybrid — moderate beta weight
        "Aggressive Hybrid":      {"performance": 0.22, "consistency": 0.18, "risk": 0.18, "cost": 0.13, "category_fit": 0.18, "beta": 0.11},
        "Balanced Hybrid":        {"performance": 0.22, "consistency": 0.22, "risk": 0.18, "cost": 0.13, "category_fit": 0.15, "beta": 0.10},
        "Conservative Hybrid":    {"performance": 0.20, "consistency": 0.25, "risk": 0.25, "cost": 0.15, "category_fit": 0.15, "beta": 0.0},
        "Dynamic Asset Allocation": {"performance": 0.20, "consistency": 0.22, "risk": 0.22, "cost": 0.13, "category_fit": 0.13, "beta": 0.10},
        "Arbitrage":              {"performance": 0.15, "consistency": 0.20, "risk": 0.20, "cost": 0.30, "category_fit": 0.15, "beta": 0.0},
        "Multi-Asset":            {"performance": 0.22, "consistency": 0.22, "risk": 0.18, "cost": 0.13, "category_fit": 0.15, "beta": 0.10},
        "Equity Savings":         {"performance": 0.20, "consistency": 0.22, "risk": 0.22, "cost": 0.13, "category_fit": 0.15, "beta": 0.08},
        # Special
        "FoF Domestic":   {"performance": 0.22, "consistency": 0.22, "risk": 0.18, "cost": 0.18, "category_fit": 0.10, "beta": 0.10},
        "FoF Overseas":   {"performance": 0.27, "consistency": 0.18, "risk": 0.18, "cost": 0.13, "category_fit": 0.14, "beta": 0.10},
        "Retirement":     {"performance": 0.18, "consistency": 0.27, "risk": 0.18, "cost": 0.13, "category_fit": 0.14, "beta": 0.10},
        "Children":       {"performance": 0.18, "consistency": 0.27, "risk": 0.18, "cost": 0.13, "category_fit": 0.14, "beta": 0.10},
    }

    @staticmethod
    def _median(values: list[float]) -> float:
        if not values:
            return 0.0
        s = sorted(values)
        n = len(s)
        mid = n // 2
        return (s[mid - 1] + s[mid]) / 2.0 if n % 2 == 0 else s[mid]

    @classmethod
    def _resolve_sub_category(cls, row: dict) -> str:
        """Normalize raw sub_category to canonical form for weight lookup."""
        sub = (str(row.get("sub_category") or "")).strip()
        if sub:
            normalized = cls._SUBCATEGORY_NORMALIZE_MAP.get(sub.lower())
            if normalized:
                return normalized
        # Infer from scheme name for empty/unrecognized sub_category
        name = (str(row.get("scheme_name") or "")).lower()
        if "fmp" in name or "fixed maturity" in name or "fixed term" in name:
            return "Target Maturity"
        if "capital protection" in name:
            return "Conservative Hybrid"
        cat = (str(row.get("category") or "")).strip().lower()
        if cat == "solution oriented":
            if "retirement" in name:
                return "Retirement"
            if "child" in name:
                return "Children"
        return "DEFAULT"

    def _peer_percentile(
        self,
        value: float | None,
        sub_peers: list[float],
        cat_peers: list[float],
        global_peers: list[float],
    ) -> float | None:
        """Percentile rank with sub-category → category → global fallback."""
        if value is None:
            return None
        if len(sub_peers) >= 5:
            return self._percentile_rank(sub_peers, value)
        if len(cat_peers) >= 8:
            return self._percentile_rank(cat_peers, value)
        if sub_peers:
            return self._percentile_rank(sub_peers, value)
        if cat_peers:
            return self._percentile_rank(cat_peers, value)
        if global_peers:
            return self._percentile_rank(global_peers, value)
        return None

    def _score_performance(self, row: dict, peer_sets: dict) -> float | None:
        """Percentile-ranked returns within sub-category peers. Blend: 50% 3Y + 30% 5Y + 20% 1Y."""
        sub_cat = self._resolve_sub_category(row)
        cat = str(row.get("category") or "Other")

        parts: list[tuple[float, float]] = []
        for key, weight in [("returns_3y", 0.50), ("returns_5y", 0.30), ("returns_1y", 0.20)]:
            val = self._to_float(row.get(key))
            pctl = self._peer_percentile(
                val,
                peer_sets.get(f"sub_{key}", {}).get(sub_cat, []),
                peer_sets.get(f"cat_{key}", {}).get(cat, []),
                peer_sets.get(f"global_{key}", []),
            )
            if pctl is not None:
                parts.append((pctl, weight))

        if not parts:
            return None
        total_w = sum(w for _, w in parts)
        return sum(s * w for s, w in parts) / total_w

    def _score_consistency(self, row: dict, peer_sets: dict) -> float | None:
        """Sortino percentile (60%) + rolling return consistency (40%). Drops Sharpe."""
        sub_cat = self._resolve_sub_category(row)
        cat = str(row.get("category") or "Other")
        parts: list[tuple[float, float]] = []

        # Sortino — higher is better
        sortino = self._to_float(row.get("sortino"))
        sortino_pctl = self._peer_percentile(
            sortino,
            peer_sets.get("sub_sortino", {}).get(sub_cat, []),
            peer_sets.get("cat_sortino", {}).get(cat, []),
            peer_sets.get("global_sortino", []),
        )
        if sortino_pctl is not None:
            parts.append((sortino_pctl, 0.60))

        # Rolling return consistency — lower std = better (inverted percentile)
        rolling = self._to_float(row.get("rolling_return_consistency"))
        rolling_pctl = self._peer_percentile(
            rolling,
            peer_sets.get("sub_rolling", {}).get(sub_cat, []),
            peer_sets.get("cat_rolling", {}).get(cat, []),
            peer_sets.get("global_rolling", []),
        )
        if rolling_pctl is not None:
            # Invert: lower rolling consistency std = better score
            parts.append((100.0 - rolling_pctl, 0.40))

        if not parts:
            return None
        total_w = sum(w for _, w in parts)
        return sum(s * w for s, w in parts) / total_w

    def _score_risk(self, row: dict, peer_sets: dict) -> float | None:
        """Max drawdown percentile (60%) + categorical risk level (40%)."""
        sub_cat = self._resolve_sub_category(row)
        cat = str(row.get("category") or "Other")
        parts: list[tuple[float, float]] = []

        # Max drawdown — lower (closer to 0) is better → inverted percentile
        dd = self._to_float(row.get("max_drawdown"))
        dd_pctl = self._peer_percentile(
            dd,
            peer_sets.get("sub_drawdown", {}).get(sub_cat, []),
            peer_sets.get("cat_drawdown", {}).get(cat, []),
            peer_sets.get("global_drawdown", []),
        )
        if dd_pctl is not None:
            # Drawdown is negative; lower absolute = better. Since _percentile_rank
            # gives higher rank for higher values, and drawdown is negative (more
            # negative = worse), a fund with dd=-5% ranks higher than dd=-30%.
            # So higher percentile = less drawdown = better. No inversion needed.
            parts.append((dd_pctl, 0.60))

        # Categorical risk level
        risk_level = str(row.get("risk_level") or "").strip().lower()
        _RISK_MAP = {
            "low": 90.0, "moderately low": 75.0, "moderate": 60.0,
            "moderately high": 40.0, "high": 25.0, "very high": 15.0,
        }
        for key, score in _RISK_MAP.items():
            if key in risk_level:
                parts.append((score, 0.40))
                break

        if not parts:
            return None
        total_w = sum(w for _, w in parts)
        return sum(s * w for s, w in parts) / total_w

    def _score_beta(self, row: dict, peer_sets: dict) -> float | None:
        """Score beta (market sensitivity) — lower beta = higher score.

        Beta ~1 means fund moves with the market. Lower beta means less
        market-correlated risk. For equity funds, moderately low beta is
        ideal; for debt funds, beta should be near zero.
        """
        beta = self._to_float(row.get("beta"))
        if beta is None:
            return None

        sub_cat = self._resolve_sub_category(row)
        cat = str(row.get("category") or "Other")

        # Percentile rank within peers (lower beta = higher percentile = better)
        beta_pctl = self._peer_percentile(
            beta,
            peer_sets.get("sub_beta", {}).get(sub_cat, []),
            peer_sets.get("cat_beta", {}).get(cat, []),
            peer_sets.get("global_beta", []),
        )
        if beta_pctl is not None:
            # Invert: lower beta should score higher
            return 100.0 - beta_pctl

        # Absolute scoring fallback when no peers available
        fund_type = self._determine_fund_type(row)
        if fund_type == "debt":
            # Debt funds: beta should be near 0
            if beta <= 0.1:
                return 90.0
            elif beta <= 0.3:
                return 70.0
            elif beta <= 0.5:
                return 50.0
            else:
                return max(10.0, 50.0 - (beta - 0.5) * 80)
        else:
            # Equity/hybrid: beta ~0.7-0.9 is ideal, >1.3 is risky
            if beta <= 0.5:
                return 85.0
            elif beta <= 0.8:
                return 80.0
            elif beta <= 1.0:
                return 65.0
            elif beta <= 1.2:
                return 50.0
            else:
                return max(10.0, 50.0 - (beta - 1.2) * 60)

    def _score_category_fit(self, row: dict, peer_sets: dict) -> float | None:
        """Sub-category-specific quality assessment."""
        sub_cat = self._resolve_sub_category(row)
        cat = str(row.get("category") or "Other")
        fund_type = self._determine_fund_type(row)

        def _pctl(metric: str, val: float | None) -> float | None:
            return self._peer_percentile(
                val,
                peer_sets.get(f"sub_{metric}", {}).get(sub_cat, []),
                peer_sets.get(f"cat_{metric}", {}).get(cat, []),
                peer_sets.get(f"global_{metric}", []),
            )

        std = self._to_float(row.get("std_dev"))
        dd = self._to_float(row.get("max_drawdown"))
        sortino = self._to_float(row.get("sortino"))
        expense = self._to_float(row.get("expense_ratio"))
        ret1y = self._to_float(row.get("returns_1y"))
        ret3y = self._to_float(row.get("returns_3y"))
        age = self._to_float(row.get("fund_age_years"))

        std_pctl = _pctl("std_dev", std)
        dd_pctl = _pctl("drawdown", dd)
        sortino_pctl = _pctl("sortino", sortino)
        ret1y_pctl = _pctl("returns_1y", ret1y)
        ret3y_pctl = _pctl("returns_3y", ret3y)

        parts: list[tuple[float, float]] = []

        if sub_cat == "Large Cap":
            # Consistency premium: low std + low drawdown
            if std_pctl is not None:
                parts.append((100.0 - std_pctl, 0.50))  # lower std = better
            if dd_pctl is not None:
                parts.append((dd_pctl, 0.50))
        elif sub_cat in ("Small Cap", "Mid Cap"):
            # Alpha generation: returns vs peers + drawdown control
            if ret3y_pctl is not None:
                parts.append((ret3y_pctl, 0.60))
            if dd_pctl is not None:
                parts.append((dd_pctl, 0.40))
        elif sub_cat == "Index":
            # Tracking error proxy: expense ratio (lower=better) + std_dev vs peers
            if expense is not None:
                cost_fit = max(0.0, min(100.0, 100.0 - (expense * 60.0)))
                parts.append((cost_fit, 0.60))
            if std_pctl is not None:
                parts.append((100.0 - std_pctl, 0.40))
        elif sub_cat in ("Sectoral", "Thematic"):
            # Momentum delivery
            if ret1y_pctl is not None:
                parts.append((ret1y_pctl, 0.70))
            if ret3y_pctl is not None:
                parts.append((ret3y_pctl, 0.30))
        elif sub_cat == "ELSS":
            if ret3y_pctl is not None:
                parts.append((ret3y_pctl, 0.50))
            if std_pctl is not None:
                parts.append((100.0 - std_pctl, 0.30))
            if age is not None:
                age_score = min(100.0, age * 10.0)
                parts.append((age_score, 0.20))
        elif sub_cat == "Arbitrage":
            # Near-zero drawdown + positive returns
            if dd is not None:
                dd_fit = max(0.0, min(100.0, 100.0 + (dd * 10.0)))  # dd near 0 → ~100
                parts.append((dd_fit, 0.60))
            if ret1y is not None:
                ret_fit = 80.0 if ret1y > 0 else 20.0
                parts.append((ret_fit, 0.40))
        elif fund_type == "debt":
            # Credit quality + stability
            risk_level = str(row.get("risk_level") or "").strip().lower()
            risk_fit = {"low": 90.0, "moderately low": 75.0, "moderate": 60.0,
                        "moderately high": 40.0, "high": 25.0}.get(risk_level, 50.0)
            parts.append((risk_fit, 0.50))
            if std_pctl is not None:
                parts.append((100.0 - std_pctl, 0.50))
        elif fund_type == "hybrid":
            # Downside protection
            if dd_pctl is not None:
                parts.append((dd_pctl, 0.50))
            if sortino_pctl is not None:
                parts.append((sortino_pctl, 0.50))
        else:
            # Default equity: returns vs peers + stability
            if ret3y_pctl is not None:
                parts.append((ret3y_pctl, 0.50))
            if std_pctl is not None:
                parts.append((100.0 - std_pctl, 0.50))

        if not parts:
            return None
        total_w = sum(w for _, w in parts)
        return sum(s * w for s, w in parts) / total_w

    @staticmethod
    def _cost_score(expense_ratio: float | None) -> float:
        """Continuous cost score instead of tiered."""
        if expense_ratio is None:
            return 50.0
        return max(0.0, min(100.0, 100.0 - (expense_ratio * 45.0)))

    @staticmethod
    def _determine_fund_type(row: dict) -> str:
        """Classify fund as 'equity', 'debt', or 'hybrid' from category/sub_category.

        Handles 8 live AMFI categories: Equity, Debt, Income (=debt), Hybrid,
        Index, Other (=FoFs), Growth (=closed-ended equity), Solution Oriented.
        """
        cat = (str(row.get("category") or "")).strip().lower()
        sub = (str(row.get("sub_category") or "")).strip().lower()
        combined = f"{cat} {sub}"

        # Direct category matches (handles Income, Growth, Index, Solution Oriented, Other)
        if cat == "income":
            return "debt"
        if cat == "growth":
            return "equity"
        if cat == "solution oriented":
            return "hybrid"
        if cat == "other":
            # FoFs — check sub_category
            if any(k in sub for k in ("fof overseas", "international")):
                return "equity"
            return "equity"  # most FoFs are equity-oriented
        if cat == "index":
            if any(k in sub for k in ("long debt", "gilt")):
                return "debt"
            return "equity"

        if any(k in combined for k in ("equity", "elss", "large cap", "mid cap", "small cap",
                                        "flexi", "multi cap", "sectoral", "thematic",
                                        "focused", "contra", "value", "dividend yield")):
            return "equity"
        if any(k in combined for k in ("debt", "liquid", "money market", "overnight",
                                        "gilt", "corporate bond", "credit risk",
                                        "banking", "short duration", "medium duration",
                                        "long duration", "ultra short", "low duration",
                                        "floater", "fixed maturity")):
            return "debt"
        if any(k in combined for k in ("hybrid", "balanced", "aggressive", "conservative",
                                        "dynamic asset", "multi asset", "arbitrage",
                                        "equity savings")):
            return "hybrid"
        # Heuristic: if risk_level is Low/Moderately Low → likely debt
        risk = str(row.get("risk_level") or "").strip().lower()
        if risk in ("low", "moderately low"):
            return "debt"
        if risk in ("high", "moderately high"):
            return "equity"
        return "equity"  # default to equity

    def _generate_mf_tags(self, row: dict, sub_cat: str, sub_cat_scores: list[float],
                          sub_cat_expenses: list[float], sub_cat_avg_ret3y: float | None) -> list[str]:
        """Priority-ranked tag system. Max 10 tags per fund."""
        tags: list[str] = []
        score = row.get("_final_score")
        ret3 = self._to_float(row.get("returns_3y"))
        ret5 = self._to_float(row.get("returns_5y"))
        ret1 = self._to_float(row.get("returns_1y"))
        expense = self._to_float(row.get("expense_ratio"))
        sharpe = self._to_float(row.get("sharpe"))
        std_dev = self._to_float(row.get("std_dev"))
        rolling = self._to_float(row.get("rolling_return_consistency"))
        aum = self._to_float(row.get("aum_cr"))
        age = self._to_float(row.get("fund_age_years"))
        risk = str(row.get("risk_level") or "").strip().lower()

        # Pri 1: Sub-category leader
        if score is not None and sub_cat_scores:
            pctl = self._percentile_rank(sub_cat_scores, score)
            if pctl >= 90:
                label = f"{sub_cat} Leader" if sub_cat != "DEFAULT" else "Category Leader"
                tags.append(label)

        # Pri 1: Sub-category label
        if sub_cat not in ("DEFAULT",):
            tags.append(sub_cat)

        # Pri 2: Consistent compounder
        if rolling is not None and rolling < 5.0 and std_dev is not None and std_dev < 15.0:
            tags.append("Consistent Compounder")

        # Pri 2: Low cost leader
        if expense is not None and sub_cat_expenses:
            cost_pctl = self._percentile_rank(sub_cat_expenses, expense)
            if cost_pctl <= 20:
                tags.append("Low Cost Leader")

        # Pri 3
        if ret3 is not None and sub_cat_avg_ret3y is not None and ret3 > sub_cat_avg_ret3y + 3.0:
            tags.append("Strong Alpha")
        if sharpe is not None and sharpe > 1.5:
            tags.append("High Sharpe")
        if age is not None and age > 10:
            tags.append("Decade Veteran")
        if ret3 is not None and ret3 >= 12:
            tags.append("Strong Returns")
        if ret5 is not None and ret5 >= 12:
            tags.append("5Y Consistent")

        # Pri 4: Risk/warning tags
        if risk in ("high", "very high"):
            tags.append("High Risk")
        if age is not None and age < 3:
            tags.append("New Fund")
        if aum is not None and aum < 25:
            tags.append("Very Small Fund")
        elif aum is not None and aum < 50:
            tags.append("Small Fund")
        if expense is not None and expense > 2.0:
            tags.append("High Cost")
        if ret1 is not None and ret1 < 0:
            tags.append("Negative Returns")

        return tags[:10]

    def _compute_scores(self, rows: list[dict]) -> list[dict]:
        """5-layer scoring model with sub-category-specific weights."""
        if not rows:
            return []

        # ── Pre-compute peer sets by sub_category, category, and global ──
        _METRICS = ("returns_1y", "returns_3y", "returns_5y", "sortino",
                     "std_dev", "rolling_return_consistency", "max_drawdown", "beta")
        peer_sets: dict[str, Any] = {}
        for metric in _METRICS:
            peer_sets[f"sub_{metric}"] = {}
            peer_sets[f"cat_{metric}"] = {}
            peer_sets[f"global_{metric}"] = []

        # Also track expenses per sub-category for tags
        sub_expenses: dict[str, list[float]] = {}
        sub_ret3y: dict[str, list[float]] = {}
        # Track sub_sortino separately since it's named differently in peer_sets
        # (already covered above via _METRICS)

        for r in rows:
            sub_cat = self._resolve_sub_category(r)
            cat = str(r.get("category") or "Other")
            for metric in _METRICS:
                val = self._to_float(r.get(metric))
                if val is not None:
                    peer_sets[f"sub_{metric}"].setdefault(sub_cat, []).append(val)
                    peer_sets[f"cat_{metric}"].setdefault(cat, []).append(val)
                    peer_sets[f"global_{metric}"].append(val)
            exp = self._to_float(r.get("expense_ratio"))
            if exp is not None:
                sub_expenses.setdefault(sub_cat, []).append(exp)
            r3 = self._to_float(r.get("returns_3y"))
            if r3 is not None:
                sub_ret3y.setdefault(sub_cat, []).append(r3)

        # Also need sortino peers under the name used by _score_consistency
        peer_sets["sub_sortino"] = peer_sets.get("sub_sortino", {})
        peer_sets["cat_sortino"] = peer_sets.get("cat_sortino", {})
        peer_sets["global_sortino"] = peer_sets.get("global_sortino", [])
        peer_sets["sub_rolling"] = peer_sets.get("sub_rolling_return_consistency", {})
        peer_sets["cat_rolling"] = peer_sets.get("cat_rolling_return_consistency", {})
        peer_sets["global_rolling"] = peer_sets.get("global_rolling_return_consistency", [])
        peer_sets["sub_drawdown"] = peer_sets.get("sub_max_drawdown", {})
        peer_sets["cat_drawdown"] = peer_sets.get("cat_max_drawdown", {})
        peer_sets["global_drawdown"] = peer_sets.get("global_max_drawdown", [])

        out: list[dict] = []
        # Collect scores per sub-category for post-scoring percentile ranking
        sub_cat_scored: dict[str, list[tuple[int, float]]] = {}

        for idx, row in enumerate(rows):
            sub_cat = self._resolve_sub_category(row)
            fund_type = self._determine_fund_type(row)

            # ── Null score for no-data funds ──
            has_returns = any(self._to_float(row.get(k)) is not None
                              for k in ("returns_1y", "returns_3y", "returns_5y"))
            has_risk = (self._to_float(row.get("std_dev")) is not None
                        or bool(str(row.get("risk_level") or "").strip())
                        or self._to_float(row.get("max_drawdown")) is not None)
            has_expense = self._to_float(row.get("expense_ratio")) is not None

            if not has_returns and not has_risk and not has_expense:
                out.append({
                    **row,
                    "fund_type": fund_type,
                    "fund_classification": sub_cat,
                    "score": None,
                    "score_return": None,
                    "score_risk": None,
                    "score_cost": None,
                    "score_consistency": None,
                    "score_performance": None,
                    "score_category_fit": None,
                    "alpha": None, "beta": None,
                    "score_alpha": None, "score_beta": None,
                    "sub_category_percentile": None,
                    "score_breakdown": {},
                    "source_status": "limited",
                    "tags_v2": [{"tag": "Unrated", "category": "classification", "severity": "neutral", "priority": 99, "confidence": None, "explanation": "Insufficient data for scoring", "expires_at": None}],
                })
                continue

            # ── Compute 6 layer scores ──
            performance = self._score_performance(row, peer_sets)
            consistency = self._score_consistency(row, peer_sets)
            risk_score = self._score_risk(row, peer_sets)
            cost_score = self._cost_score(self._to_float(row.get("expense_ratio")))
            category_fit = self._score_category_fit(row, peer_sets)
            beta_score = self._score_beta(row, peer_sets)

            # ── Look up sub-category weights ──
            weights = self._SUBCATEGORY_LAYER_WEIGHTS.get(
                sub_cat, self._SUBCATEGORY_LAYER_WEIGHTS["DEFAULT"]
            )

            # ── Dynamic weight rebalancing for unavailable layers ──
            layer_scores: dict[str, tuple[float, float | None]] = {
                "performance":  (weights["performance"], performance),
                "consistency":  (weights["consistency"], consistency),
                "risk":         (weights["risk"], risk_score),
                "cost":         (weights["cost"], cost_score if has_expense else None),
                "category_fit": (weights["category_fit"], category_fit),
                "beta":         (weights["beta"], beta_score),
            }

            parts: list[tuple[float, float]] = []
            for _layer, (w, s) in layer_scores.items():
                if s is not None:
                    parts.append((s, w))

            if parts:
                total_w = sum(w for _, w in parts)
                score = sum(s * w for s, w in parts) / total_w
            else:
                score = 50.0

            # ── Coverage shrinkage ──
            coverage_signals = [
                has_returns,
                has_risk,
                has_expense,
                self._to_float(row.get("sortino")) is not None,
                self._to_float(row.get("rolling_return_consistency")) is not None,
            ]
            coverage = sum(1 for sig in coverage_signals if sig) / len(coverage_signals)
            score = self._shrink_to_neutral(score, coverage)

            # ── Risk flags (hard caps) instead of multiplicative AUM penalty ──
            aum = self._to_float(row.get("aum_cr"))
            age = self._to_float(row.get("fund_age_years"))
            expense = self._to_float(row.get("expense_ratio"))

            if age is not None and age < 3:
                score = min(score, 65.0)
            if aum is not None and aum < 25:
                score = min(score, 55.0)
            elif aum is not None and aum < 50:
                score = min(score, 65.0)
            if expense is not None and expense > 2.0:
                score = min(score, 70.0)

            # ── Source status ──
            status = str(row.get("source_status") or "limited").strip().lower()
            has_meta = has_expense
            if has_returns and has_risk and has_meta:
                status = "primary"
            elif has_returns or has_risk:
                if status == "limited":
                    status = "fallback"
            elif status == "primary":
                status = "fallback"

            status_penalty = 0.0
            if status == "fallback":
                status_penalty = 5.0
            elif status == "limited":
                status_penalty = 12.0

            final_score = round(max(0.0, min(100.0, score - status_penalty)), 2)

            # Store for sub-category percentile post-processing
            sub_cat_scored.setdefault(sub_cat, []).append((idx, final_score))

            # Compute legacy alpha/beta values for backward compat
            ret3y = self._to_float(row.get("returns_3y"))
            sub_avg = None
            sub_rets = sub_ret3y.get(sub_cat)
            if sub_rets:
                sub_avg = sum(sub_rets) / len(sub_rets)
            alpha_value = round(ret3y - sub_avg, 2) if ret3y is not None and sub_avg is not None else None

            out.append({
                **row,
                "fund_type": fund_type,
                "fund_classification": sub_cat,
                "_final_score": final_score,
                "score": final_score,
                "score_return": round(performance, 2) if performance is not None else None,
                "score_risk": round(risk_score, 2) if risk_score is not None else None,
                "score_cost": round(cost_score, 2),
                "score_consistency": round(consistency, 2) if consistency is not None else None,
                "score_performance": round(performance, 2) if performance is not None else None,
                "score_category_fit": round(category_fit, 2) if category_fit is not None else None,
                "alpha": alpha_value,
                "score_alpha": None,  # alpha not a scoring layer
                "score_beta": round(beta_score, 2) if beta_score is not None else None,
                "score_breakdown": {
                    "performance_score": round(performance, 2) if performance is not None else None,
                    "consistency_score": round(consistency, 2) if consistency is not None else None,
                    "risk_score": round(risk_score, 2) if risk_score is not None else None,
                    "cost_score": round(cost_score, 2),
                    "category_fit_score": round(category_fit, 2) if category_fit is not None else None,
                    "beta_score": round(beta_score, 2) if beta_score is not None else None,
                    # Legacy keys for backward compat
                    "return_score": round(performance, 2) if performance is not None else None,
                },
                "source_status": status,
                "tags_v2": [],  # populated below after percentile computation
            })

        # ── Post-scoring: sub-category percentile + tags ──
        for sub_cat, entries in sub_cat_scored.items():
            scores_in_sub = [s for _, s in entries]
            sub_exps = sub_expenses.get(sub_cat, [])
            sub_avg = None
            sub_rets = sub_ret3y.get(sub_cat)
            if sub_rets:
                sub_avg = sum(sub_rets) / len(sub_rets)

            for out_idx, final_score in entries:
                pctl = self._percentile_rank(scores_in_sub, final_score)
                out[out_idx]["sub_category_percentile"] = round(pctl, 1)
                from app.services.tag_engine import generate_mf_tags
                mf_tags_v2 = generate_mf_tags(
                    out[out_idx], sub_cat, scores_in_sub, sub_exps, sub_avg
                )
                out[out_idx]["tags_v2"] = mf_tags_v2
                # Clean up internal field
                out[out_idx].pop("_final_score", None)

        # Also handle unrated funds (they don't appear in sub_cat_scored)
        for item in out:
            if item.get("score") is None:
                item.setdefault("sub_category_percentile", None)

        out.sort(
            key=lambda item: (
                -(float(item["score"]) if item.get("score") is not None else -1.0),
                -float(item.get("returns_3y") or -9999.0),
                str(item.get("scheme_code") or ""),
            )
        )
        return out

    def _enrich_from_mfapi(self, rows: dict[str, dict]) -> None:
        """Enrich fund_age_years, returns, and risk metrics from mfapi.in for funds missing data.

        Processes all funds that need enrichment (no cap). Rate-limited at 0.5s/call.
        """
        import math
        import time

        needs_enrichment = [
            code for code, row in rows.items()
            if row.get("fund_age_years") is None
            or row.get("max_drawdown") is None
            or row.get("rolling_return_consistency") is None
            or row.get("returns_1y") is None
            or (row.get("std_dev") is None and row.get("returns_1y") is not None)
        ]
        if not needs_enrichment:
            return

        to_enrich = needs_enrichment
        logger.info("mfapi.in enrichment: %d funds need data", len(to_enrich))

        # Load Nifty 50 benchmark NAVs from mfapi.in for beta computation
        benchmark_daily_returns: dict[str, float] = {}
        try:
            bench_resp = requests.get("https://api.mfapi.in/mf/120505", timeout=15)
            if bench_resp.status_code == 200:
                bench_data = bench_resp.json().get("data", [])
                bench_data.reverse()  # Oldest first
                for j in range(1, len(bench_data)):
                    try:
                        prev_nav = float(bench_data[j - 1].get("nav", 0))
                        curr_nav = float(bench_data[j].get("nav", 0))
                        if prev_nav > 0:
                            dr = (curr_nav / prev_nav) - 1.0
                            benchmark_daily_returns[bench_data[j].get("date", "")] = dr
                    except (TypeError, ValueError):
                        continue
                logger.info("Benchmark (Nifty 50) daily returns loaded from mfapi.in: %d days", len(benchmark_daily_returns))
        except Exception as e:
            logger.warning("Failed to load Nifty 50 benchmark: %s", e)

        enriched = 0
        for i, code in enumerate(to_enrich):
            try:
                resp = requests.get(f"https://api.mfapi.in/mf/{code}", timeout=10)
                if resp.status_code != 200:
                    continue
                data = resp.json()
                meta = data.get("meta", {})
                nav_data = data.get("data", [])

                row = rows[code]

                # Fund age from inception date or oldest NAV
                if row.get("fund_age_years") is None:
                    inception_date = None
                    # Try scheme_start_date from meta (dict with "date" key, or string)
                    ssd = meta.get("scheme_start_date")
                    inception_str = None
                    if isinstance(ssd, dict):
                        inception_str = ssd.get("date")
                    elif isinstance(ssd, str):
                        inception_str = ssd
                    if inception_str and "-" in str(inception_str) and len(str(inception_str)) == 10:
                        try:
                            parts = str(inception_str).split("-")
                            if len(parts[0]) == 2:  # DD-MM-YYYY
                                inception_date = datetime(int(parts[2]), int(parts[1]), int(parts[0]),
                                                          tzinfo=timezone.utc)
                            else:  # YYYY-MM-DD
                                inception_date = datetime(int(parts[0]), int(parts[1]), int(parts[2]),
                                                          tzinfo=timezone.utc)
                        except (ValueError, IndexError):
                            pass
                    # Fallback: oldest NAV date (mfapi returns newest-first)
                    if inception_date is None and nav_data:
                        oldest_pt = nav_data[-1]
                        oldest_str = oldest_pt.get("date", "") if isinstance(oldest_pt, dict) else ""
                        if oldest_str and "-" in oldest_str:
                            try:
                                parts = oldest_str.split("-")
                                if len(parts) == 3 and len(parts[0]) == 2:  # DD-MM-YYYY
                                    inception_date = datetime(int(parts[2]), int(parts[1]), int(parts[0]),
                                                              tzinfo=timezone.utc)
                            except (ValueError, IndexError):
                                pass
                    if inception_date is not None:
                        age_days = (datetime.now(timezone.utc) - inception_date).days
                        row["fund_age_years"] = round(age_days / 365.25, 1)

                # AMC from meta if missing
                if not row.get("amc") and meta.get("fund_house"):
                    row["amc"] = meta["fund_house"]

                # Category from meta if missing
                if not row.get("category") and meta.get("scheme_category"):
                    row["category"] = meta["scheme_category"]

                # ── Compute risk metrics + CAGR returns from NAV history ──
                if nav_data and len(nav_data) >= 30:
                    try:
                        # Parse all NAVs with dates (most recent first in mfapi)
                        nav_pairs: list[tuple[str, float]] = []
                        navs_short: list[float] = []
                        for pt in nav_data:
                            try:
                                nav_val = float(pt.get("nav", 0))
                                if nav_val <= 0:
                                    continue
                                nav_pairs.append((pt.get("date", ""), nav_val))
                                if len(navs_short) < 365:
                                    navs_short.append(nav_val)
                            except (TypeError, ValueError):
                                continue
                        nav_pairs.reverse()  # Oldest first
                        navs_short.reverse()  # Oldest first

                        # ── CAGR returns from NAV history (replaces XIRR) ──
                        if len(nav_pairs) >= 2:
                            latest_nav = nav_pairs[-1][1]
                            total_days = len(nav_pairs)

                            # 1Y CAGR (≥200 trading days)
                            if total_days >= 200:
                                idx_1y = max(0, total_days - 252)
                                nav_1y = nav_pairs[idx_1y][1]
                                years_1y = (total_days - idx_1y) / 252.0
                                if nav_1y > 0 and years_1y > 0:
                                    cagr_1y = round(((latest_nav / nav_1y) ** (1.0 / years_1y) - 1.0) * 100, 2)
                                    row["returns_1y"] = cagr_1y

                            # 3Y CAGR (≥600 trading days)
                            if total_days >= 600:
                                idx_3y = max(0, total_days - 756)
                                nav_3y = nav_pairs[idx_3y][1]
                                years_3y = (total_days - idx_3y) / 252.0
                                if nav_3y > 0 and years_3y > 0:
                                    cagr_3y = round(((latest_nav / nav_3y) ** (1.0 / years_3y) - 1.0) * 100, 2)
                                    row["returns_3y"] = cagr_3y

                            # 5Y CAGR (≥1000 trading days)
                            if total_days >= 1000:
                                idx_5y = max(0, total_days - 1260)
                                nav_5y = nav_pairs[idx_5y][1]
                                years_5y = (total_days - idx_5y) / 252.0
                                if nav_5y > 0 and years_5y > 0:
                                    cagr_5y = round(((latest_nav / nav_5y) ** (1.0 / years_5y) - 1.0) * 100, 2)
                                    row["returns_5y"] = cagr_5y

                        # ── Maximum Drawdown ──
                        all_navs = [p[1] for p in nav_pairs]
                        if len(all_navs) >= 60:
                            peak = all_navs[0]
                            max_dd = 0.0
                            for nav_val in all_navs:
                                if nav_val > peak:
                                    peak = nav_val
                                dd = (peak - nav_val) / peak * 100
                                if dd > max_dd:
                                    max_dd = dd
                            row["max_drawdown"] = round(max_dd, 2)

                        # ── Rolling Return Consistency (std dev of rolling 1Y returns) ──
                        if len(nav_pairs) >= 504:  # Need 2+ years
                            rolling_returns: list[float] = []
                            for ri in range(252, len(nav_pairs)):
                                base_nav = nav_pairs[ri - 252][1]
                                if base_nav > 0:
                                    ret = (nav_pairs[ri][1] / base_nav - 1.0) * 100
                                    rolling_returns.append(ret)
                            if len(rolling_returns) >= 4:
                                mean_rr = sum(rolling_returns) / len(rolling_returns)
                                var_rr = sum((r - mean_rr) ** 2 for r in rolling_returns) / len(rolling_returns)
                                row["rolling_return_consistency"] = round(math.sqrt(var_rr), 2)

                        # ── Std dev, Sharpe, Sortino from NAV history if missing ──
                        if len(navs_short) >= 30 and row.get("std_dev") is None:
                            daily_returns = [(navs_short[j] / navs_short[j - 1]) - 1.0
                                             for j in range(1, len(navs_short))
                                             if navs_short[j - 1] > 0]

                            if daily_returns:
                                mean_ret = sum(daily_returns) / len(daily_returns)
                                variance = sum((r - mean_ret) ** 2 for r in daily_returns) / len(daily_returns)
                                std_daily = math.sqrt(variance) if variance > 0 else 0
                                std_annual = std_daily * math.sqrt(252)
                                row["std_dev"] = round(std_annual * 100, 2)  # as percentage

                                # Annualized return
                                total_return = (navs_short[-1] / navs_short[0]) - 1.0 if navs_short[0] > 0 else 0
                                n_years = len(navs_short) / 252.0
                                ann_return = ((1 + total_return) ** (1.0 / n_years) - 1.0) * 100 if n_years > 0 else 0

                                risk_free = 6.5  # approx Indian risk-free rate
                                if std_annual > 0:
                                    row["sharpe"] = round((ann_return - risk_free) / (std_annual * 100), 2)

                                    # Sortino: downside deviation only (BUG FIX: divide by len(downside))
                                    downside = [r for r in daily_returns if r < 0]
                                    if downside:
                                        down_var = sum(r ** 2 for r in downside) / len(downside)
                                        down_dev = math.sqrt(down_var) * math.sqrt(252) * 100
                                        if down_dev > 0:
                                            row["sortino"] = round((ann_return - risk_free) / down_dev, 2)

                        # ── Beta vs Nifty 50 benchmark ──
                        if benchmark_daily_returns and len(nav_pairs) >= 60:
                            fund_dates = [p[0] for p in nav_pairs]
                            fund_navs_map = {p[0]: p[1] for p in nav_pairs}
                            paired_fund: list[float] = []
                            paired_bench: list[float] = []
                            for j in range(1, len(fund_dates)):
                                dt = fund_dates[j]
                                dt_prev = fund_dates[j - 1]
                                if dt in benchmark_daily_returns and fund_navs_map.get(dt_prev, 0) > 0:
                                    fr = (fund_navs_map[dt] / fund_navs_map[dt_prev]) - 1.0
                                    br = benchmark_daily_returns[dt]
                                    paired_fund.append(fr)
                                    paired_bench.append(br)
                            if len(paired_fund) >= 30:
                                mean_f = sum(paired_fund) / len(paired_fund)
                                mean_b = sum(paired_bench) / len(paired_bench)
                                cov = sum((f - mean_f) * (b - mean_b) for f, b in zip(paired_fund, paired_bench)) / len(paired_fund)
                                var_b = sum((b - mean_b) ** 2 for b in paired_bench) / len(paired_bench)
                                if var_b > 0:
                                    row["beta"] = round(cov / var_b, 2)

                    except Exception:
                        pass  # Skip on any calculation error

                enriched += 1
                if (i + 1) % 25 == 0:
                    logger.info("mfapi.in enrichment progress: %d / %d", i + 1, len(to_enrich))

            except Exception:
                logger.debug("mfapi.in fetch failed for %s", code)

            time.sleep(0.5)  # Rate limit

        logger.info("mfapi.in enrichment complete: %d funds enriched", enriched)

    def fetch_all(self) -> list[dict]:
        amfi_rows = self._parse_amfi_fallback()
        et_rows = self._parse_etmoney_candidates()
        et_rows_by_name, et_detail_links = self._parse_etmoney_category_pages()
        et_detail_rows_by_name = self._parse_etmoney_detail_pages(et_detail_links)

        merged: dict[str, dict] = {}
        for code, row in amfi_rows.items():
            merged[code] = dict(row)

        for code, row in et_rows.items():
            existing = merged.get(code)
            if existing is None:
                merged[code] = dict(row)
                continue
            # Keep AMFI NAV as hard fallback truth, enrich with ET Money advanced metrics.
            merged[code] = {
                **existing,
                **row,
                "nav": existing.get("nav") or row.get("nav"),
                "nav_date": existing.get("nav_date") or row.get("nav_date"),
                "source_status": "primary",
                "primary_source": "etmoney_web",
                "secondary_source": "amfi_nav_file",
            }

        amfi_name_index: dict[str, list[str]] = {}
        for code, row in merged.items():
            key = self._normalize_fund_name_key(str(row.get("scheme_name") or ""))
            if not key:
                continue
            amfi_name_index.setdefault(key, []).append(code)

        def _candidate_codes_for_key(name_key: str) -> list[str]:
            candidate_codes = amfi_name_index.get(name_key) or []
            if candidate_codes:
                return candidate_codes
            return [
                code
                for known_key, codes in amfi_name_index.items()
                if known_key and (known_key in name_key or name_key in known_key)
                for code in codes
            ]

        def _candidate_priority(code: str) -> tuple[int, int]:
            name = str(merged.get(code, {}).get("scheme_name") or "").lower()
            growth_bias = 2 if "growth" in name else 0
            direct_bias = 1 if "direct" in name else 0
            return (growth_bias + direct_bias, -len(name))

        for key, et_row in et_rows_by_name.items():
            if not key:
                continue
            candidate_codes = _candidate_codes_for_key(key)
            if not candidate_codes:
                continue

            best_code = sorted(candidate_codes, key=_candidate_priority, reverse=True)[0]
            base_row = dict(merged[best_code])

            if et_row.get("category"):
                base_row["category"] = et_row.get("category")
            if et_row.get("sub_category"):
                base_row["sub_category"] = et_row.get("sub_category")
            for metric_key in ("returns_1y", "returns_3y", "returns_5y", "risk_level"):
                if et_row.get(metric_key) is not None:
                    base_row[metric_key] = et_row.get(metric_key)

            base_row["source_status"] = "primary"
            base_row["source_timestamp"] = datetime.now(timezone.utc)
            base_row["primary_source"] = "etmoney_web"
            base_row["secondary_source"] = "amfi_nav_file"
            merged[best_code] = base_row

        for key, detail_row in et_detail_rows_by_name.items():
            if not key:
                continue
            candidate_codes = _candidate_codes_for_key(key)
            if not candidate_codes:
                continue
            best_code = sorted(candidate_codes, key=_candidate_priority, reverse=True)[0]
            base_row = dict(merged[best_code])

            for metric_key in (
                "expense_ratio",
                "aum_cr",
                "risk_level",
                "std_dev",
                "sharpe",
                "sortino",
                "fund_age_years",
            ):
                if detail_row.get(metric_key) is not None:
                    base_row[metric_key] = detail_row.get(metric_key)
            if detail_row.get("option_type") and not base_row.get("option_type"):
                base_row["option_type"] = detail_row.get("option_type")
            if detail_row.get("fund_managers"):
                base_row["fund_managers"] = detail_row["fund_managers"]

            base_row["source_status"] = "primary"
            base_row["source_timestamp"] = datetime.now(timezone.utc)
            base_row["primary_source"] = "etmoney_web"
            base_row["secondary_source"] = "amfi_nav_file"
            merged[best_code] = base_row

        # Enrich missing data from mfapi.in (fund_age, risk metrics) — only active funds
        direct_merged = {}
        for code, row in merged.items():
            if str(row.get("plan_type") or "").lower() != "direct":
                continue
            name = (row.get("scheme_name") or "").lower()
            opt = (row.get("option_type") or "").lower()
            # Skip non-growth, IDCW, dead, and other filtered variants
            if "idcw" in name or "idcw" in opt or "income distribution" in name:
                continue
            if any(kw in name for kw in ["fmp", "fixed maturity", "close ended", "closed ended",
                                          "capital protection", "fixed term", "unclaimed",
                                          "bonus", "payout", "icdw", "idwc", "weekly",
                                          "daily", "linked insurance", "interval fund",
                                          "p f option", "- monthly", "- quarterly",
                                          "- half yearly", "- annual"]):
                continue
            direct_merged[code] = row
        logger.info("Active direct funds for mfapi enrichment: %d", len(direct_merged))
        self._enrich_from_mfapi(direct_merged)
        merged.update(direct_merged)

        rows = [row for row in merged.values() if str(row.get("plan_type") or "").lower() == "direct"]

        # ── Fix misclassified index fund sub_categories ──────────────────
        for row in rows:
            name_lower = (row.get("scheme_name") or "").lower()
            sub = (row.get("sub_category") or "").lower()
            classification = (row.get("fund_classification") or "").lower()

            # Only correct index funds with generic/wrong sub_category
            if classification != "index" and "index" not in sub:
                continue

            # Rule-based correction using index name keywords
            # Order matters: more specific patterns first
            if "nifty 500" in name_lower or "bse 500" in name_lower:
                row["sub_category"] = "Multi Cap Index"
            elif "largemidcap" in name_lower or "large midcap" in name_lower or "large mid cap" in name_lower or "nifty 250" in name_lower:
                row["sub_category"] = "Large & MidCap Index"
            elif "midsmallcap" in name_lower or "mid small" in name_lower:
                row["sub_category"] = "Mid Cap Index"
            elif ("smallcap" in name_lower or "small cap" in name_lower) and "mid" not in name_lower:
                row["sub_category"] = "Small Cap Index"
            elif ("midcap" in name_lower or "mid cap" in name_lower) and "small" not in name_lower and "large" not in name_lower:
                row["sub_category"] = "Mid Cap Index"
            elif "nifty 200" in name_lower:
                row["sub_category"] = "Large & MidCap Index"
            elif ("nifty 50 " in name_lower or "nifty50" in name_lower or "sensex" in name_lower or "bse 100" in name_lower) and "nifty 500" not in name_lower:
                row["sub_category"] = "Large Cap Index"
            elif "nifty next 50" in name_lower:
                row["sub_category"] = "Large Cap Index"
            elif "total market" in name_lower:
                row["sub_category"] = "Multi Cap Index"

        return self._compute_scores(rows)


_scraper = DiscoverMutualFundScraper()


def _fetch_discover_mf_rows_sync() -> list[dict]:
    return _scraper.fetch_all()


async def run_discover_mutual_fund_job() -> None:
    try:
        loop = asyncio.get_event_loop()
        rows = await loop.run_in_executor(
            get_job_executor("discover-mf"),
            _fetch_discover_mf_rows_sync,
        )
        count = await discover_service.upsert_discover_mutual_fund_snapshots(rows)
        logger.info("Discover mutual fund job complete: %d snapshots upserted", count)

        # After upsert, compute dual ranking: sub-category + category
        pool = await get_pool()

        # Only rank active funds
        _active_filter = """
                    WHERE nav_date >= CURRENT_DATE - INTERVAL '90 days'
                      AND LOWER(COALESCE(plan_type, 'direct')) = 'direct'
                      AND COALESCE(option_type, '') NOT ILIKE '%idcw%'
                      AND scheme_name NOT ILIKE '%fmp%'
                      AND scheme_name NOT ILIKE '%fixed maturity%'
                      AND scheme_name NOT ILIKE '%close ended%'
                      AND scheme_name NOT ILIKE '%closed ended%'
                      AND scheme_name NOT ILIKE '%interval%fund%'
                      AND scheme_name NOT ILIKE '%capital protection%'
                      AND scheme_name NOT ILIKE '%fixed term%'
                      AND scheme_name NOT ILIKE '%idcw%'
                      AND scheme_name NOT ILIKE '%income distribution%'
                      AND scheme_name NOT ILIKE '%unclaimed%'
                      AND scheme_name NOT ILIKE '%bonus%'
                      AND scheme_name NOT ILIKE '%payout%'
                      AND scheme_name NOT ILIKE '%- monthly%'
                      AND scheme_name NOT ILIKE '%- quarterly%'
                      AND scheme_name NOT ILIKE '%- half yearly%'
                      AND scheme_name NOT ILIKE '%- annual%'
                      AND scheme_name NOT ILIKE '%icdw%'
                      AND scheme_name NOT ILIKE '%idwc%'
                      AND scheme_name NOT ILIKE '%p f option%'
                      AND scheme_name NOT ILIKE '%weekly%'
                      AND scheme_name NOT ILIKE '%daily%'
                      AND scheme_name NOT ILIKE '%linked insurance%'
        """

        # 1. Sub-category rank (granular: Large Cap, Mid Cap, Corporate Bond, etc.)
        try:
            sub_r = await pool.execute(f"""
                UPDATE discover_mutual_fund_snapshots AS t
                SET sub_category_rank = sub.rnk, sub_category_total = sub.total
                FROM (
                    SELECT scheme_code,
                           DENSE_RANK() OVER (
                               PARTITION BY COALESCE(NULLIF(fund_classification, ''), NULLIF(sub_category, ''), NULLIF(category, ''), 'Other')
                               ORDER BY score DESC
                           ) AS rnk,
                           COUNT(*) OVER (
                               PARTITION BY COALESCE(NULLIF(fund_classification, ''), NULLIF(sub_category, ''), NULLIF(category, ''), 'Other')
                           ) AS total
                    FROM discover_mutual_fund_snapshots
                    {_active_filter}
                ) sub
                WHERE t.scheme_code = sub.scheme_code
            """)
            logger.info("MF Job: sub_category_rank updated: %s", sub_r)
        except Exception:
            logger.exception("MF Job: sub_category_rank update FAILED")

        # 2. Category rank (broader: Equity, Debt, Hybrid, etc.)
        await pool.execute(f"""
            UPDATE discover_mutual_fund_snapshots AS t
            SET category_rank = sub.rnk, category_total = sub.total
            FROM (
                SELECT scheme_code,
                       DENSE_RANK() OVER (
                           PARTITION BY COALESCE(NULLIF(category, ''), 'Other')
                           ORDER BY score DESC
                       ) AS rnk,
                       COUNT(*) OVER (
                           PARTITION BY COALESCE(NULLIF(category, ''), 'Other')
                       ) AS total
                FROM discover_mutual_fund_snapshots
                {_active_filter}
            ) sub
            WHERE t.scheme_code = sub.scheme_code
        """)
    except requests.RequestException:
        logger.exception("Discover mutual fund job failed due to network exception")
    except Exception:
        logger.exception("Discover mutual fund job failed")


# ── Module-level rescore function ──────────────────────────────────
_scraper = DiscoverMutualFundScraper()
_UPSERT_BATCH_SIZE = 200


async def rescore_discover_mutual_funds() -> dict:
    """Read all MF rows from DB, re-compute scores, and write back.

    No network fetching — purely DB read → score → DB write.
    """
    import time as time_mod

    t0 = time_mod.time()

    pool = await get_pool()
    async with pool.acquire() as conn:
        db_rows = await conn.fetch(
            f"SELECT * FROM {discover_service.MF_TABLE}"
        )
    raw_rows = [dict(r) for r in db_rows]
    read_elapsed = time_mod.time() - t0
    logger.info("MF Rescore: read %d rows from DB in %.1fs", len(raw_rows), read_elapsed)

    if not raw_rows:
        return {"status": "empty", "rows": 0}

    # Coverage summary
    total = len(raw_rows)
    has_returns = sum(1 for r in raw_rows if r.get("returns_3y") is not None)
    has_risk = sum(1 for r in raw_rows if r.get("std_dev") is not None or r.get("risk_level"))
    has_expense = sum(1 for r in raw_rows if r.get("expense_ratio") is not None)
    has_sortino = sum(1 for r in raw_rows if r.get("sortino") is not None)
    logger.info(
        "MF Rescore coverage: %d total | returns=%d (%.0f%%) | risk=%d (%.0f%%) | "
        "expense=%d (%.0f%%) | sortino=%d (%.0f%%)",
        total,
        has_returns, has_returns / total * 100,
        has_risk, has_risk / total * 100,
        has_expense, has_expense / total * 100,
        has_sortino, has_sortino / total * 100,
    )

    # Fix misclassified index fund sub_categories before scoring
    for row in raw_rows:
        name_lower = (row.get("scheme_name") or "").lower()
        sub = (row.get("sub_category") or "").lower()
        classification = (row.get("fund_classification") or "").lower()
        if classification != "index" and "index" not in sub:
            continue
        if "nifty 500" in name_lower or "bse 500" in name_lower:
            row["sub_category"] = "Multi Cap Index"
        elif "largemidcap" in name_lower or "large midcap" in name_lower or "large mid cap" in name_lower or "nifty 250" in name_lower:
            row["sub_category"] = "Large & MidCap Index"
        elif "midsmallcap" in name_lower or "mid small" in name_lower:
            row["sub_category"] = "Mid Cap Index"
        elif ("smallcap" in name_lower or "small cap" in name_lower) and "mid" not in name_lower:
            row["sub_category"] = "Small Cap Index"
        elif ("midcap" in name_lower or "mid cap" in name_lower) and "small" not in name_lower and "large" not in name_lower:
            row["sub_category"] = "Mid Cap Index"
        elif "nifty 200" in name_lower:
            row["sub_category"] = "Large & MidCap Index"
        elif ("nifty 50 " in name_lower or "nifty50" in name_lower or "sensex" in name_lower or "bse 100" in name_lower) and "nifty 500" not in name_lower:
            row["sub_category"] = "Large Cap Index"
        elif "nifty next 50" in name_lower:
            row["sub_category"] = "Large Cap Index"
        elif "total market" in name_lower:
            row["sub_category"] = "Multi Cap Index"

    # Score
    score_t0 = time_mod.time()
    scored_rows = _scraper._compute_scores(raw_rows)
    score_elapsed = time_mod.time() - score_t0
    logger.info("MF Rescore: scored %d rows in %.1fs", len(scored_rows), score_elapsed)

    # Score distribution
    scores = [r.get("score") for r in scored_rows if r.get("score") is not None]
    unrated = sum(1 for r in scored_rows if r.get("score") is None)
    tiers: dict[str, int] = {}
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
            "MF Rescore scores: min=%.1f p25=%.1f p50=%.1f p75=%.1f max=%.1f | "
            "Strong=%d Good=%d Average=%d Weak=%d | Unrated=%d",
            scores[0], p25, p50, p75, scores[-1],
            tiers["Strong"], tiers["Good"], tiers["Average"], tiers["Weak"], unrated,
        )

    # Upsert
    upsert_t0 = time_mod.time()
    total_upserted = 0
    for batch_start in range(0, len(scored_rows), _UPSERT_BATCH_SIZE):
        batch = scored_rows[batch_start: batch_start + _UPSERT_BATCH_SIZE]
        count = await discover_service.upsert_discover_mutual_fund_snapshots(batch)
        total_upserted += count

    total_elapsed = time_mod.time() - t0
    logger.info(
        "MF Rescore complete: %d rows in %.1fs (read=%.1fs, score=%.1fs, upsert=%.1fs)",
        total_upserted, total_elapsed, read_elapsed, score_elapsed, time_mod.time() - upsert_t0,
    )

    # Recompute dual ranking after rescore (only active funds)
    import time as _t
    rank_t0 = _t.time()
    _active_rank_filter = """
                WHERE nav_date >= CURRENT_DATE - INTERVAL '90 days'
                  AND LOWER(COALESCE(plan_type, 'direct')) = 'direct'
                  AND COALESCE(option_type, '') NOT ILIKE '%idcw%'
                  AND scheme_name NOT ILIKE '%fmp%'
                  AND scheme_name NOT ILIKE '%fixed maturity%'
                  AND scheme_name NOT ILIKE '%close ended%'
                  AND scheme_name NOT ILIKE '%closed ended%'
                  AND scheme_name NOT ILIKE '%interval%fund%'
                  AND scheme_name NOT ILIKE '%capital protection%'
                  AND scheme_name NOT ILIKE '%fixed term%'
    """
    try:
        sub_result = await pool.execute(f"""
            UPDATE discover_mutual_fund_snapshots AS t
            SET sub_category_rank = sub.rnk, sub_category_total = sub.total
            FROM (
                SELECT scheme_code,
                       DENSE_RANK() OVER (
                           PARTITION BY COALESCE(NULLIF(fund_classification, ''), NULLIF(sub_category, ''), NULLIF(category, ''), 'Other')
                           ORDER BY score DESC
                       ) AS rnk,
                       COUNT(*) OVER (
                           PARTITION BY COALESCE(NULLIF(fund_classification, ''), NULLIF(sub_category, ''), NULLIF(category, ''), 'Other')
                       ) AS total
                FROM discover_mutual_fund_snapshots
                {_active_rank_filter}
            ) sub
            WHERE t.scheme_code = sub.scheme_code
        """)
        logger.info("MF Rescore: sub_category_rank updated: %s", sub_result)
    except Exception:
        logger.exception("MF Rescore: sub_category_rank update FAILED")
    try:
        cat_result = await pool.execute(f"""
            UPDATE discover_mutual_fund_snapshots AS t
            SET category_rank = sub.rnk, category_total = sub.total
            FROM (
                SELECT scheme_code,
                       DENSE_RANK() OVER (
                           PARTITION BY COALESCE(NULLIF(category, ''), 'Other')
                           ORDER BY score DESC
                       ) AS rnk,
                       COUNT(*) OVER (
                           PARTITION BY COALESCE(NULLIF(category, ''), 'Other')
                       ) AS total
                FROM discover_mutual_fund_snapshots
                {_active_rank_filter}
            ) sub
            WHERE t.scheme_code = sub.scheme_code
        """)
        logger.info("MF Rescore: category_rank updated: %s", cat_result)
    except Exception:
        logger.exception("MF Rescore: category_rank update FAILED")
    logger.info("MF Rescore: rankings recomputed in %.1fs", _t.time() - rank_t0)

    # Sub-category distribution
    sub_cat_counts: dict[str, int] = {}
    for r in scored_rows:
        sc = r.get("fund_classification", "DEFAULT")
        sub_cat_counts[sc] = sub_cat_counts.get(sc, 0) + 1

    return {
        "status": "completed",
        "rows_scored": len(scores),
        "rows_unrated": unrated,
        "rows_upserted": total_upserted,
        "elapsed_seconds": round(total_elapsed, 1),
        "coverage": {
            "total": total,
            "returns": has_returns,
            "risk": has_risk,
            "expense": has_expense,
            "sortino": has_sortino,
        },
        "score_distribution": tiers,
        "sub_category_distribution": dict(sorted(sub_cat_counts.items(), key=lambda x: -x[1])[:20]),
    }

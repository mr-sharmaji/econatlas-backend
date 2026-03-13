from __future__ import annotations

import asyncio
import json
import logging
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
        max_pages: int = 3200,
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
                for link in re.findall(r'href="(/mutual-funds/(?:equity|debt|hybrid|other|solution-oriented)/[^"]+/\d+)"', explore_html)
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

    def _risk_score(
        self,
        risk_level: str | None,
        std_dev: float | None,
        category_std_devs: list[float],
        global_std_devs: list[float],
    ) -> float:
        """Blended risk score with peer-relative std-dev percentile when data is sufficient."""
        mapping = {
            "low": 85.0,
            "moderately low": 75.0,
            "moderate": 65.0,
            "moderately high": 45.0,
            "high": 35.0,
            "very high": 20.0,
        }
        categorical: float | None = None
        if risk_level:
            lowered = risk_level.strip().lower()
            for key, score in mapping.items():
                if key in lowered:
                    categorical = score
                    break

        continuous: float | None = None
        if std_dev is not None:
            peer_values: list[float] = []
            if len(category_std_devs) >= 5:
                peer_values = category_std_devs
            elif len(global_std_devs) >= 8:
                peer_values = global_std_devs
            elif category_std_devs:
                peer_values = category_std_devs
            elif global_std_devs:
                peer_values = global_std_devs

            if peer_values:
                continuous = max(0.0, min(100.0, 100.0 - self._percentile_rank(peer_values, std_dev)))
            else:
                continuous = max(0.0, min(100.0, 100.0 - (std_dev * 5.0)))

        if categorical is not None and continuous is not None:
            return categorical * 0.55 + continuous * 0.45
        if categorical is not None:
            return categorical
        if continuous is not None:
            return continuous
        return 50.0

    @staticmethod
    def _cost_score(expense_ratio: float | None) -> float:
        """Continuous cost score instead of tiered."""
        if expense_ratio is None:
            return 50.0
        return max(0.0, min(100.0, 100.0 - (expense_ratio * 45.0)))

    def _consistency_score(
        self,
        sharpe: float | None,
        sortino: float | None,
        category_sharpes: list[float],
        category_sortinos: list[float],
        global_sharpes: list[float],
        global_sortinos: list[float],
    ) -> float:
        """Percentile-rank Sharpe/Sortino using category peers with global fallback."""
        values: list[float] = []
        if sharpe is not None:
            if len(category_sharpes) >= 5:
                values.append(self._percentile_rank(category_sharpes, sharpe))
            elif len(global_sharpes) >= 8:
                values.append(self._percentile_rank(global_sharpes, sharpe))
            elif category_sharpes:
                values.append(self._percentile_rank(category_sharpes, sharpe))
            elif global_sharpes:
                values.append(self._percentile_rank(global_sharpes, sharpe))
            else:
                values.append(max(0.0, min(100.0, sharpe * 35.0)))
        if sortino is not None:
            if len(category_sortinos) >= 5:
                values.append(self._percentile_rank(category_sortinos, sortino))
            elif len(global_sortinos) >= 8:
                values.append(self._percentile_rank(global_sortinos, sortino))
            elif category_sortinos:
                values.append(self._percentile_rank(category_sortinos, sortino))
            elif global_sortinos:
                values.append(self._percentile_rank(global_sortinos, sortino))
            else:
                values.append(max(0.0, min(100.0, sortino * 28.0)))
        if not values:
            return 50.0
        return sum(values) / len(values)

    def _return_score(self, rows: list[dict], current: dict) -> float:
        """Blended return score using percentile peers (category-first, global fallback)."""
        category = str(current.get("category") or "Other")
        bucket = [r for r in rows if str(r.get("category") or "Other") == category]

        def _peer_percentile(key: str) -> float | None:
            cur = self._to_float(current.get(key))
            if cur is None:
                return None
            category_values = [self._to_float(r.get(key)) for r in bucket]
            cat_numeric = [v for v in category_values if v is not None]
            all_values = [self._to_float(r.get(key)) for r in rows]
            global_numeric = [v for v in all_values if v is not None]

            peer_values: list[float] = []
            if len(cat_numeric) >= 5:
                peer_values = cat_numeric
            elif len(global_numeric) >= 8:
                peer_values = global_numeric
            elif cat_numeric:
                peer_values = cat_numeric
            elif global_numeric:
                peer_values = global_numeric

            if peer_values:
                return self._percentile_rank(peer_values, cur)
            return max(0.0, min(100.0, 30.0 + (cur * 3.0)))

        r3 = _peer_percentile("returns_3y")
        r5 = _peer_percentile("returns_5y")
        r1 = _peer_percentile("returns_1y")

        # Weighted blend based on what's available
        parts: list[tuple[float, float]] = []  # (score, weight)
        if r3 is not None:
            parts.append((r3, 0.50))
        if r5 is not None:
            parts.append((r5, 0.30))
        if r1 is not None:
            parts.append((r1, 0.20))

        if not parts:
            return 50.0
        total_w = sum(w for _, w in parts)
        return sum(s * w for s, w in parts) / total_w

    @staticmethod
    def _determine_fund_type(row: dict) -> str:
        """Classify fund as 'equity', 'debt', or 'hybrid' from category/sub_category."""
        cat = (str(row.get("category") or "")).strip().lower()
        sub = (str(row.get("sub_category") or "")).strip().lower()
        combined = f"{cat} {sub}"
        if any(k in combined for k in ("equity", "elss", "large cap", "mid cap", "small cap",
                                        "flexi", "multi cap", "index", "sectoral", "thematic",
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

    def _alpha_score(self, row: dict, cat_avg_returns: dict[str, float]) -> float | None:
        """Alpha proxy: fund 3Y return minus category average 3Y return."""
        ret3 = self._to_float(row.get("returns_3y"))
        if ret3 is None:
            return None
        cat = str(row.get("category") or "Other")
        avg = cat_avg_returns.get(cat)
        if avg is None:
            return None
        # Alpha = excess return; 0 alpha = 50, positive = higher
        alpha = ret3 - avg
        return max(0.0, min(100.0, 50.0 + (alpha * 5.0)))

    def _beta_score(self, row: dict, cat_std_devs: dict[str, list[float]], global_std_devs: list[float]) -> float | None:
        """Beta proxy: fund std_dev relative to category median std_dev. Lower beta = higher score."""
        std = self._to_float(row.get("std_dev"))
        if std is None:
            return None
        cat = str(row.get("category") or "Other")
        peers = cat_std_devs.get(cat, [])
        if len(peers) < 3:
            peers = global_std_devs
        if not peers:
            return None
        median_std = self._median(peers)
        if median_std <= 0:
            return 50.0
        beta_ratio = std / median_std
        # beta_ratio ~1 = 50, lower = better
        return max(0.0, min(100.0, 100.0 - (beta_ratio * 50.0)))

    def _credit_quality_score(self, row: dict) -> float | None:
        """Credit quality proxy for debt funds: based on risk_level + category keywords."""
        risk = str(row.get("risk_level") or "").strip().lower()
        sub = str(row.get("sub_category") or "").strip().lower()
        cat = str(row.get("category") or "").strip().lower()
        combined = f"{cat} {sub}"

        # AAA-heavy categories
        if any(k in combined for k in ("gilt", "overnight", "liquid", "money market")):
            base = 90.0
        elif any(k in combined for k in ("corporate bond", "banking")):
            base = 75.0
        elif "credit risk" in combined:
            base = 40.0
        elif any(k in combined for k in ("short duration", "medium duration", "low duration")):
            base = 70.0
        else:
            base = 60.0

        # Adjust by risk level
        risk_adj = {"low": 10, "moderately low": 5, "moderate": 0,
                    "moderately high": -10, "high": -15}.get(risk, 0)
        return max(0.0, min(100.0, base + risk_adj))

    def _duration_score(self, row: dict) -> float | None:
        """Duration proxy for debt: lower volatility (std_dev) = better for debt funds."""
        std = self._to_float(row.get("std_dev"))
        if std is None:
            return None
        # For debt, lower std_dev = higher score
        return max(0.0, min(100.0, 100.0 - (std * 12.0)))

    def _compute_scores(self, rows: list[dict]) -> list[dict]:
        if not rows:
            return []

        # Pre-compute category/global peer sets for percentile-based scoring.
        cat_sharpes: dict[str, list[float]] = {}
        cat_sortinos: dict[str, list[float]] = {}
        cat_std_devs: dict[str, list[float]] = {}
        global_sharpes: list[float] = []
        global_sortinos: list[float] = []
        global_std_devs: list[float] = []
        cat_returns_3y: dict[str, list[float]] = {}
        for r in rows:
            cat = str(r.get("category") or "Other")
            s = self._to_float(r.get("sharpe"))
            if s is not None:
                cat_sharpes.setdefault(cat, []).append(s)
                global_sharpes.append(s)
            t = self._to_float(r.get("sortino"))
            if t is not None:
                cat_sortinos.setdefault(cat, []).append(t)
                global_sortinos.append(t)
            d = self._to_float(r.get("std_dev"))
            if d is not None:
                cat_std_devs.setdefault(cat, []).append(d)
                global_std_devs.append(d)
            r3 = self._to_float(r.get("returns_3y"))
            if r3 is not None:
                cat_returns_3y.setdefault(cat, []).append(r3)

        cat_avg_returns: dict[str, float] = {
            cat: sum(vals) / len(vals) for cat, vals in cat_returns_3y.items() if vals
        }

        out: list[dict] = []
        for row in rows:
            cat = str(row.get("category") or "Other")
            fund_type = self._determine_fund_type(row)
            return_score = self._return_score(rows, row)
            risk_score = self._risk_score(
                risk_level=str(row.get("risk_level") or "").strip() or None,
                std_dev=self._to_float(row.get("std_dev")),
                category_std_devs=cat_std_devs.get(cat, []),
                global_std_devs=global_std_devs,
            )
            cost_score = self._cost_score(self._to_float(row.get("expense_ratio")))
            consistency_score = self._consistency_score(
                self._to_float(row.get("sharpe")),
                self._to_float(row.get("sortino")),
                cat_sharpes.get(cat, []),
                cat_sortinos.get(cat, []),
                global_sharpes,
                global_sortinos,
            )

            # Type-specific extra scores
            alpha_score = self._alpha_score(row, cat_avg_returns)
            beta_score = self._beta_score(row, cat_std_devs, global_std_devs)
            credit_quality_score = self._credit_quality_score(row)
            duration_score = self._duration_score(row)

            return_available = any(
                self._to_float(row.get(k)) is not None
                for k in ("returns_1y", "returns_3y", "returns_5y")
            )
            risk_available = bool(str(row.get("risk_level") or "").strip()) or self._to_float(row.get("std_dev")) is not None
            cost_available = self._to_float(row.get("expense_ratio")) is not None
            consistency_available = (
                self._to_float(row.get("sharpe")) is not None
                or self._to_float(row.get("sortino")) is not None
            )

            # Type-specific weight distributions
            if fund_type == "equity":
                base_weights: dict[str, tuple[float, float | None, bool]] = {
                    # (weight, score, available)
                    "return":      (0.30, return_score, return_available),
                    "risk":        (0.15, risk_score, risk_available),
                    "cost":        (0.10, cost_score, cost_available),
                    "consistency": (0.20, consistency_score, consistency_available),
                    "alpha":       (0.15, alpha_score, alpha_score is not None),
                    "beta":        (0.10, beta_score, beta_score is not None),
                }
            elif fund_type == "debt":
                base_weights = {
                    "return":         (0.15, return_score, return_available),
                    "risk":           (0.25, risk_score, risk_available),
                    "cost":           (0.20, cost_score, cost_available),
                    "consistency":    (0.15, consistency_score, consistency_available),
                    "credit_quality": (0.15, credit_quality_score, credit_quality_score is not None),
                    "duration":       (0.10, duration_score, duration_score is not None),
                }
            else:
                # Hybrid: blend equity and debt approach
                base_weights = {
                    "return":         (0.25, return_score, return_available),
                    "risk":           (0.20, risk_score, risk_available),
                    "cost":           (0.15, cost_score, cost_available),
                    "consistency":    (0.20, consistency_score, consistency_available),
                    "alpha":          (0.10, alpha_score, alpha_score is not None),
                    "credit_quality": (0.10, credit_quality_score, credit_quality_score is not None),
                }

            parts: list[tuple[float, float]] = []
            for _, (w, s, avail) in base_weights.items():
                if avail and s is not None:
                    parts.append((s, w))

            if parts:
                total_w = sum(w for _, w in parts)
                score = sum(s * w for s, w in parts) / total_w
            else:
                score = 50.0

            advanced_coverage_signals = [
                self._to_float(row.get("returns_3y")) is not None,
                self._to_float(row.get("returns_5y")) is not None,
                self._to_float(row.get("expense_ratio")) is not None,
                risk_available,
                consistency_available,
            ]
            coverage = sum(1 for signal in advanced_coverage_signals if signal) / len(advanced_coverage_signals)
            score = self._shrink_to_neutral(score, coverage)

            # Tiered AUM penalty: strongest for very small funds.
            aum = self._to_float(row.get("aum_cr"))
            if aum is not None and aum < 100:
                if aum < 25:
                    score *= 0.82
                elif aum < 50:
                    score *= 0.88
                else:
                    score *= 0.94
            elif aum is not None and aum < 250:
                score *= 0.97

            status = str(row.get("source_status") or "limited").strip().lower()
            has_advanced = any(
                row.get(k) is not None
                for k in ("returns_3y", "expense_ratio", "risk_level", "sharpe", "sortino")
            )
            if status == "primary" and not has_advanced:
                status = "fallback"
            status_penalty = 0.0
            if status == "fallback":
                status_penalty = 7.0
            elif status == "limited":
                status_penalty = 15.0

            tags: list[str] = []
            ret3 = self._to_float(row.get("returns_3y"))
            if ret3 is not None and ret3 >= 12:
                tags.append("strong_returns")
            ret5 = self._to_float(row.get("returns_5y"))
            if ret5 is not None and ret5 >= 12:
                tags.append("consistent_performer")
            if cost_score >= 80:
                tags.append("low_cost")
            if risk_score >= 75:
                tags.append("lower_risk")
            if aum is not None and aum < 100:
                tags.append("small_fund")
            if status != "primary":
                tags.append("limited_data")
            if not tags:
                tags.append("balanced")

            out.append(
                {
                    **row,
                    "fund_type": fund_type,
                    "score": round(max(0.0, min(100.0, score - status_penalty)), 2),
                    "score_return": round(return_score, 2),
                    "score_risk": round(risk_score, 2),
                    "score_cost": round(cost_score, 2),
                    "score_consistency": round(consistency_score, 2),
                    "score_breakdown": {
                        "return_score": round(return_score, 2),
                        "risk_score": round(risk_score, 2),
                        "cost_score": round(cost_score, 2),
                        "consistency_score": round(consistency_score, 2),
                    },
                    "source_status": status,
                    "tags": tags,
                }
            )

        out.sort(
            key=lambda item: (
                -float(item.get("score") or 0.0),
                -float(item.get("returns_3y") or -9999.0),
                str(item.get("scheme_code") or ""),
            )
        )
        return out

    def _enrich_from_mfapi(self, rows: dict[str, dict], *, max_enrich: int = 200) -> None:
        """Enrich fund_age_years and risk metrics from mfapi.in for funds missing data.

        Only enriches up to max_enrich funds to avoid rate-limiting. Prioritises
        funds with missing fund_age_years, std_dev, sharpe, or sortino.

        NOTE: expense_ratio and aum_cr are NOT available from any free public API
        (mfapi.in, AMFI NAV file, ET Money, Groww, etc. all lack these fields or
        block server-side requests). A premium data source or browser-based scraping
        would be needed to populate these fields.
        """
        import math
        import time

        needs_enrichment = [
            code for code, row in rows.items()
            if row.get("fund_age_years") is None
            or (row.get("std_dev") is None and row.get("returns_1y") is not None)
        ]
        if not needs_enrichment:
            return

        # Limit to avoid excessive API calls
        to_enrich = needs_enrichment[:max_enrich]
        logger.info("mfapi.in enrichment: %d / %d funds need data", len(to_enrich), len(needs_enrichment))

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

                # Compute std_dev, sharpe, sortino from NAV history if missing
                if nav_data and len(nav_data) >= 30 and row.get("std_dev") is None:
                    try:
                        # Parse NAVs (most recent first in mfapi)
                        navs = []
                        for pt in nav_data[:365]:  # Last ~1 year
                            try:
                                navs.append(float(pt.get("nav", 0)))
                            except (TypeError, ValueError):
                                continue
                        navs.reverse()  # Oldest first

                        if len(navs) >= 30:
                            # Daily returns
                            daily_returns = [(navs[j] / navs[j - 1]) - 1.0
                                             for j in range(1, len(navs))
                                             if navs[j - 1] > 0]

                            if daily_returns:
                                mean_ret = sum(daily_returns) / len(daily_returns)
                                variance = sum((r - mean_ret) ** 2 for r in daily_returns) / len(daily_returns)
                                std_daily = math.sqrt(variance) if variance > 0 else 0
                                std_annual = std_daily * math.sqrt(252)
                                row["std_dev"] = round(std_annual * 100, 2)  # as percentage

                                # Annualized return
                                total_return = (navs[-1] / navs[0]) - 1.0 if navs[0] > 0 else 0
                                n_years = len(navs) / 252.0
                                ann_return = ((1 + total_return) ** (1.0 / n_years) - 1.0) * 100 if n_years > 0 else 0

                                risk_free = 6.5  # approx Indian risk-free rate
                                if std_annual > 0:
                                    row["sharpe"] = round((ann_return - risk_free) / (std_annual * 100), 2)

                                    # Sortino: downside deviation only
                                    downside = [r for r in daily_returns if r < 0]
                                    if downside:
                                        down_var = sum(r ** 2 for r in downside) / len(daily_returns)
                                        down_dev = math.sqrt(down_var) * math.sqrt(252) * 100
                                        if down_dev > 0:
                                            row["sortino"] = round((ann_return - risk_free) / down_dev, 2)
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

            base_row["source_status"] = "primary"
            base_row["source_timestamp"] = datetime.now(timezone.utc)
            base_row["primary_source"] = "etmoney_web"
            base_row["secondary_source"] = "amfi_nav_file"
            merged[best_code] = base_row

        # Enrich missing data from mfapi.in (fund_age, risk metrics)
        direct_codes = {code for code, row in merged.items()
                        if str(row.get("plan_type") or "").lower() == "direct"}
        direct_merged = {code: row for code, row in merged.items() if code in direct_codes}
        self._enrich_from_mfapi(direct_merged)
        merged.update(direct_merged)

        rows = [row for row in merged.values() if str(row.get("plan_type") or "").lower() == "direct"]
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

        # After upsert, compute category rank
        pool = await get_pool()
        await pool.execute("""
            UPDATE discover_mutual_fund_snapshots AS t
            SET category_rank = sub.rnk, category_total = sub.total
            FROM (
                SELECT scheme_code,
                       DENSE_RANK() OVER (
                           PARTITION BY COALESCE(NULLIF(sub_category, ''), NULLIF(category, ''), 'Other')
                           ORDER BY score DESC
                       ) AS rnk,
                       COUNT(*) OVER (
                           PARTITION BY COALESCE(NULLIF(sub_category, ''), NULLIF(category, ''), 'Other')
                       ) AS total
                FROM discover_mutual_fund_snapshots
            ) sub
            WHERE t.scheme_code = sub.scheme_code
        """)
    except requests.RequestException:
        logger.exception("Discover mutual fund job failed due to network exception")
    except Exception:
        logger.exception("Discover mutual fund job failed")

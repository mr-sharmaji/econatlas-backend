from __future__ import annotations

import asyncio
import json
import logging
import re
from datetime import datetime, timezone
from html import unescape
from typing import Any

import requests

from app.core.config import get_settings
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

    def _parse_etmoney_category_pages(self) -> dict[str, dict]:
        out: dict[str, dict] = {}
        base = self.settings.discover_mf_primary_url.rstrip("/")
        try:
            explore_html = self._get_text(f"{base}/mutual-funds/explore", timeout=16)
        except Exception:
            logger.debug("ET Money explore fetch failed", exc_info=True)
            return out

        category_links = sorted(
            {
                link
                for link in re.findall(r'href="(/mutual-funds/(?:equity|debt|hybrid|other|solution-oriented)/[^"]+/\d+)"', explore_html)
                if link
            }
        )

        for rel_link in category_links:
            url = f"{base}{rel_link}"
            try:
                html = self._get_text(url, timeout=14)
            except Exception:
                logger.debug("ET Money category fetch failed url=%s", url, exc_info=True)
                continue

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
        return out

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
    def _risk_score(risk_level: str | None, std_dev: float | None) -> float:
        mapping = {
            "low": 85.0,
            "moderately low": 75.0,
            "moderate": 65.0,
            "moderately high": 45.0,
            "high": 35.0,
            "very high": 20.0,
        }
        if risk_level:
            lowered = risk_level.strip().lower()
            for key, score in mapping.items():
                if key in lowered:
                    return score
        if std_dev is not None:
            return max(0.0, min(100.0, 100.0 - (std_dev * 5.0)))
        return 50.0

    @staticmethod
    def _cost_score(expense_ratio: float | None) -> float:
        if expense_ratio is None:
            return 50.0
        if expense_ratio <= 0.5:
            return 95.0
        if expense_ratio <= 1.0:
            return 80.0
        if expense_ratio <= 1.5:
            return 60.0
        if expense_ratio <= 2.0:
            return 40.0
        return 20.0

    @staticmethod
    def _consistency_score(sharpe: float | None, sortino: float | None) -> float:
        values: list[float] = []
        if sharpe is not None:
            values.append(max(0.0, min(100.0, sharpe * 35.0)))
        if sortino is not None:
            values.append(max(0.0, min(100.0, sortino * 28.0)))
        if not values:
            return 50.0
        return sum(values) / len(values)

    def _return_score(self, rows: list[dict], current: dict) -> float:
        category = str(current.get("category") or "Other")
        bucket = [r for r in rows if str(r.get("category") or "Other") == category]
        values = [self._to_float(r.get("returns_3y")) for r in bucket]
        numeric = [v for v in values if v is not None]
        cur = self._to_float(current.get("returns_3y"))
        if cur is None:
            cur = self._to_float(current.get("returns_1y"))
            if cur is None:
                return 50.0
            # 1Y proxy gets lower conviction.
            return max(0.0, min(100.0, 30.0 + (cur * 3.0)))
        if not numeric:
            return max(0.0, min(100.0, 30.0 + (cur * 3.0)))
        lo = min(numeric)
        hi = max(numeric)
        if hi - lo < 1e-6:
            return 65.0
        return max(0.0, min(100.0, ((cur - lo) / (hi - lo)) * 100.0))

    def _compute_scores(self, rows: list[dict]) -> list[dict]:
        out: list[dict] = []
        for row in rows:
            return_score = self._return_score(rows, row)
            risk_score = self._risk_score(
                risk_level=str(row.get("risk_level") or "").strip() or None,
                std_dev=self._to_float(row.get("std_dev")),
            )
            cost_score = self._cost_score(self._to_float(row.get("expense_ratio")))
            consistency_score = self._consistency_score(
                self._to_float(row.get("sharpe")),
                self._to_float(row.get("sortino")),
            )
            score = (
                (return_score * 0.40)
                + (risk_score * 0.25)
                + (cost_score * 0.20)
                + (consistency_score * 0.15)
            )

            status = str(row.get("source_status") or "limited")
            has_advanced = any(
                row.get(k) is not None
                for k in ("returns_3y", "expense_ratio", "risk_level", "sharpe", "sortino")
            )
            if status == "primary" and not has_advanced:
                status = "fallback"

            tags: list[str] = []
            ret3 = self._to_float(row.get("returns_3y"))
            if ret3 is not None and ret3 >= 12:
                tags.append("strong_returns")
            if cost_score >= 80:
                tags.append("low_cost")
            if risk_score >= 75:
                tags.append("lower_risk")
            if status != "primary":
                tags.append("limited_data")
            if not tags:
                tags.append("balanced")

            out.append(
                {
                    **row,
                    "score": round(max(0.0, min(100.0, score)), 2),
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

        out.sort(key=lambda item: (float(item.get("score") or 0.0), float(item.get("returns_3y") or -9999.0)), reverse=True)
        return out

    def fetch_all(self) -> list[dict]:
        amfi_rows = self._parse_amfi_fallback()
        et_rows = self._parse_etmoney_candidates()
        et_rows_by_name = self._parse_etmoney_category_pages()

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

        for key, et_row in et_rows_by_name.items():
            if not key:
                continue
            candidate_codes = amfi_name_index.get(key) or []
            if not candidate_codes:
                candidate_codes = [
                    code
                    for known_key, codes in amfi_name_index.items()
                    if known_key and (known_key in key or key in known_key)
                    for code in codes
                ]
            if not candidate_codes:
                continue

            def _candidate_priority(code: str) -> tuple[int, int]:
                name = str(merged.get(code, {}).get("scheme_name") or "").lower()
                growth_bias = 2 if "growth" in name else 0
                direct_bias = 1 if "direct" in name else 0
                return (growth_bias + direct_bias, -len(name))

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
    except requests.RequestException:
        logger.exception("Discover mutual fund job failed due to network exception")
    except Exception:
        logger.exception("Discover mutual fund job failed")

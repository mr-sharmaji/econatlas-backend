from __future__ import annotations

import asyncio
import html
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import httpx

from app.services import tax_fy, tax_service

_RUPEE_SENTINEL = 1.0e18


@dataclass(frozen=True)
class OfficialTaxSourceUrls:
    income_changes_2025: str = "https://cleartax.in/s/income-tax-changes-from-april-2025"
    income_changes_2024: str = "https://cleartax.in/s/income-tax-changes-from-april-2024"
    tax_calculator: str = "https://cleartax.in/paytax/taxcalculator"
    capital_gains: str = "https://cleartax.in/s/capital-gains-income"
    tds_rate_chart: str = "https://cleartax.in/s/tds-rate-chart"
    advance_tax: str = "https://cleartax.in/s/advance-tax"


OFFICIAL_TAX_SOURCE_URLS = OfficialTaxSourceUrls()

_SOURCE_FALLBACK_URLS: dict[str, list[str]] = {
    "capital_gains": [
        "https://cleartax.in/s/long-term-capital-gains-on-shares",
    ],
}

_REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}


@dataclass
class OfficialTaxBundle:
    source_name: str
    source_urls: dict[str, str]
    config_payload: dict[str, Any]


def _clean_text(raw_html: str) -> str:
    text = raw_html
    text = re.sub(r"(?is)<script.*?>.*?</script>", " ", text)
    text = re.sub(r"(?is)<style.*?>.*?</style>", " ", text)
    text = re.sub(r"(?is)<!--.*?-->", " ", text)
    text = re.sub(r"(?is)</(p|div|tr|li|table|section|h[1-6]|br|td|th)>", "\n", text)
    text = re.sub(r"(?is)<[^>]+>", " ", text)
    text = html.unescape(text).replace("\xa0", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"[ \t]+\n", "\n", text)
    text = re.sub(r"\n{2,}", "\n", text)
    return text.strip()


def _to_number(raw_value: str) -> float:
    cleaned = re.sub(r"[^0-9.]", "", raw_value)
    if not cleaned:
        raise ValueError(f"Unable to parse numeric value from: {raw_value}")
    return float(cleaned)


def _parse_indian_amount(raw_value: str) -> float:
    value = _to_number(raw_value)
    lower = raw_value.lower()
    if "crore" in lower:
        return value * 10_000_000.0
    if "lakh" in lower or re.search(r"[0-9]\s*l\b", lower) or re.search(r"[0-9]l\b", lower):
        return value * 100_000.0
    return value


def _rate_fraction(raw_value: str) -> float:
    if re.search(r"\bnil\b", raw_value, re.IGNORECASE):
        return 0.0
    value = _to_number(raw_value)
    # Source pages provide percentage points (e.g. 1, 10, 12.5), not fractions.
    return value / 100.0 if value >= 1 else value


def _format_fy_id(start_year: int) -> str:
    return tax_fy.fy_id_from_start_year(start_year)


def _parse_slab_upper(label: str) -> float:
    if re.search(r"\babove\b", label, re.IGNORECASE):
        return _RUPEE_SENTINEL
    matches = re.findall(
        r"([0-9]+(?:\.[0-9]+)?)\s*(lakh|lakhs|crore|crores)?",
        label.lower(),
    )
    if not matches:
        raise ValueError(f"Unable to parse slab label: {label}")
    number, unit = matches[-1]
    return _parse_indian_amount(f"{number} {unit or ''}".strip())


def _build_slab(label: str, rate_text: str) -> dict[str, float]:
    return {
        "upper_limit": _parse_slab_upper(label),
        "rate": _rate_fraction(rate_text),
    }


def _dedupe_sorted_slabs(slabs: list[dict[str, float]]) -> list[dict[str, float]]:
    ordered = sorted(slabs, key=lambda row: float(row["upper_limit"]))
    deduped: list[dict[str, float]] = []
    seen: set[float] = set()
    for row in ordered:
        upper = float(row["upper_limit"])
        if upper in seen:
            continue
        seen.add(upper)
        deduped.append({"upper_limit": upper, "rate": float(row["rate"])})
    return deduped


def _extract_new_slabs_fy_2025_and_2024(changes_2025_text: str) -> tuple[list[dict[str, float]], list[dict[str, float]]]:
    start = changes_2025_text.find("Income Tax Slabs for FY 2025-26")
    end = changes_2025_text.find("2. Increased Rebate Under Section 87A")
    chunk = changes_2025_text[start:end] if start >= 0 and end > start else changes_2025_text

    row_pattern = re.compile(
        r"(Up to Rs\.?\s*[0-9.,]+\s*lakhs?|"
        r"Rs\.?\s*[0-9.,]+\s*lakhs?\s*to\s*Rs\.?\s*[0-9.,]+\s*lakhs?|"
        r"Above Rs\.?\s*[0-9.,]+\s*lakhs?)\s*"
        r"(Nil|[0-9.]+%)\s*"
        r"(Up to Rs\.?\s*[0-9.,]+\s*lakhs?|"
        r"Rs\.?\s*[0-9.,]+\s*lakhs?\s*to\s*Rs\.?\s*[0-9.,]+\s*lakhs?|"
        r"Above Rs\.?\s*[0-9.,]+\s*lakhs?)\s*"
        r"(Nil|[0-9.]+%)",
        re.IGNORECASE,
    )
    rows = row_pattern.findall(chunk)
    if len(rows) < 6:
        raise ValueError("Unable to parse FY2025-26/FY2024-25 new-regime slab comparison table.")

    fy2025_slabs = [_build_slab(row[0], row[1]) for row in rows]
    fy2024_slabs = [_build_slab(row[2], row[3]) for row in rows]

    max_upper_2025 = max(item["upper_limit"] for item in fy2025_slabs)
    if max_upper_2025 < _RUPEE_SENTINEL:
        above_values = re.findall(r"Above\s+Rs\.?\s*([0-9.,]+)\s*lakhs?", chunk, re.IGNORECASE)
        max_above = 0.0
        for raw in above_values:
            max_above = max(max_above, _parse_indian_amount(f"{raw} lakh"))
        if max_above > max_upper_2025:
            fy2025_slabs.append({"upper_limit": _RUPEE_SENTINEL, "rate": 0.30})

    fy2025_slabs = _dedupe_sorted_slabs(fy2025_slabs)
    fy2024_slabs = _dedupe_sorted_slabs(fy2024_slabs)
    if fy2025_slabs[-1]["upper_limit"] < _RUPEE_SENTINEL:
        fy2025_slabs.append({"upper_limit": _RUPEE_SENTINEL, "rate": 0.30})
    if fy2024_slabs[-1]["upper_limit"] < _RUPEE_SENTINEL:
        fy2024_slabs.append({"upper_limit": _RUPEE_SENTINEL, "rate": 0.30})

    return fy2025_slabs, fy2024_slabs


def _extract_taxcalc_latest_new_slabs(taxcalc_text: str) -> tuple[str, str, list[dict[str, float]]]:
    fy_match = re.search(
        r"Financial year FY\s*([0-9]{4})-([0-9]{4}).*?Latest FY\s*([0-9]{4})-([0-9]{4})",
        taxcalc_text,
        re.IGNORECASE | re.DOTALL,
    )
    if not fy_match:
        raise ValueError("Unable to parse financial-year selector from tax calculator source.")
    selected_fy_start = int(fy_match.group(1))
    latest_fy_start = int(fy_match.group(3))
    selected_fy = _format_fy_id(selected_fy_start)
    latest_fy = _format_fy_id(latest_fy_start)

    start = taxcalc_text.find("Income Tax Slab Rates")
    end = taxcalc_text.find("Old Regime Slab Rates", start if start >= 0 else 0)
    chunk = taxcalc_text[start:end] if start >= 0 and end > start else taxcalc_text
    rows = re.findall(
        r"(Up to\s*[0-9.]+\s*lakh|"
        r"[0-9.]+\s*lakh\s*to\s*[0-9.]+\s*lakh|"
        r"Above\s*[0-9.]+\s*lakh)\s*"
        r"(NIL|[0-9.]+%)",
        chunk,
        re.IGNORECASE,
    )
    if len(rows) < 7:
        raise ValueError("Unable to parse latest new-regime slab table from tax calculator source.")
    slabs = _dedupe_sorted_slabs([_build_slab(label, rate) for label, rate in rows])
    if slabs[-1]["upper_limit"] < _RUPEE_SENTINEL:
        slabs.append({"upper_limit": _RUPEE_SENTINEL, "rate": 0.30})
    return selected_fy, latest_fy, slabs


def _extract_old_slabs_and_basic_exemption(taxcalc_text: str) -> tuple[list[dict[str, float]], dict[str, float]]:
    below60_match = re.search(
        r"Individuals less than 60 Years of Age\s+Income Slabs\s+Income Tax Rates\s+"
        r"Up to Rs\.\s*([0-9.]+)\s*lakh\s+NIL",
        taxcalc_text,
        re.IGNORECASE | re.DOTALL,
    )
    senior_match = re.search(
        r"Resident Individuals Aged 60-80 Years\s+Income Slabs\s+Income Tax Rates\s+"
        r"Up to Rs\.\s*([0-9.]+)\s*lakh\s+NIL",
        taxcalc_text,
        re.IGNORECASE | re.DOTALL,
    )
    super_match = re.search(
        r"Resident Individuals Aged more than 80 Years\s+Income Slabs\s+Income Tax Rates\s+"
        r"Up to Rs\.\s*([0-9.]+)\s*lakh\s+NIL",
        taxcalc_text,
        re.IGNORECASE | re.DOTALL,
    )
    if not (below60_match and senior_match and super_match):
        raise ValueError("Unable to parse old-regime basic exemption table.")

    old_mid1 = re.search(
        r"Up to Rs\.\s*2\.5\s*lakh\s+NIL\s+Rs\.\s*2\.5\s*lakh\s*-\s*Rs\.\s*5\s*lakh\s+([0-9.]+)%",
        taxcalc_text,
        re.IGNORECASE | re.DOTALL,
    )
    old_mid2 = re.search(
        r"Rs\.\s*2\.5\s*lakh\s*-\s*Rs\.\s*5\s*lakh\s+[0-9.]+%\s+"
        r"Rs\.\s*5\s*lakh\s*-\s*Rs\.\s*10\s*lakh\s+([0-9.]+)%",
        taxcalc_text,
        re.IGNORECASE | re.DOTALL,
    )
    old_top = re.search(
        r"Rs\.\s*5\s*lakh\s*-\s*Rs\.\s*10\s*lakh\s+[0-9.]+%\s+"
        r"Above Rs\.\s*10\s*lakh\s+([0-9.]+)%",
        taxcalc_text,
        re.IGNORECASE | re.DOTALL,
    )
    if not (old_mid1 and old_mid2 and old_top):
        raise ValueError("Unable to parse old-regime slab rates.")

    old_slabs = [
        {"upper_limit": _parse_indian_amount("5 lakh"), "rate": _rate_fraction(old_mid1.group(1))},
        {"upper_limit": _parse_indian_amount("10 lakh"), "rate": _rate_fraction(old_mid2.group(1))},
        {"upper_limit": _RUPEE_SENTINEL, "rate": _rate_fraction(old_top.group(1))},
    ]
    old_basic = {
        "below60": _parse_indian_amount(f"{below60_match.group(1)} lakh"),
        "age60to80": _parse_indian_amount(f"{senior_match.group(1)} lakh"),
        "above80": _parse_indian_amount(f"{super_match.group(1)} lakh"),
    }
    return old_slabs, old_basic


def _extract_rebate_and_standard_deduction(
    changes_2024_text: str,
    changes_2025_text: str,
    taxcalc_text: str,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, float]]:
    prev_old_match = re.search(
        r"old tax regime, the applicable rebate limit is Rs\.?\s*([0-9,]+)\s*"
        r"for incomes up to Rs\.?\s*([0-9,]+)\s*lakhs",
        changes_2024_text,
        re.IGNORECASE,
    )
    prev_new_match = re.search(
        r"new tax regime, this rebate limit has increased to Rs\.?\s*([0-9,]+)\s*"
        r"if the taxable income is less than or equal to Rs\.?\s*([0-9,]+)\s*lakhs",
        changes_2024_text,
        re.IGNORECASE,
    )
    if not (prev_old_match and prev_new_match):
        raise ValueError("Unable to parse FY2024-25 rebate values.")

    current_rebate_change = re.search(
        r"increased to from Rs\.?\s*([0-9,]+)\s*to\s*Rs\.?\s*([0-9,]+)",
        changes_2025_text,
        re.IGNORECASE,
    )
    current_rebate_threshold = re.search(
        r"tax free income of up to Rs\.?\s*([0-9,]+)\s*Lakhs",
        changes_2025_text,
        re.IGNORECASE,
    )
    current_old_rebate = re.search(
        r"Old Tax Regime remains the same i\.e\., Rs\.?\s*([0-9,]+)",
        changes_2025_text,
        re.IGNORECASE,
    )
    if not (current_rebate_change and current_rebate_threshold and current_old_rebate):
        raise ValueError("Unable to parse FY2025-26 rebate values.")

    std_match = re.search(
        r"standard deduction under the old regime is Rs\.?\s*([0-9,]+).*?"
        r"new regime, the limit has been increased to Rs\.?\s*([0-9,]+)",
        changes_2024_text,
        re.IGNORECASE | re.DOTALL,
    )
    if not std_match:
        std_match = re.search(
            r"standard deduction of Rs\.?\s*([0-9,]+).*?old regime, and Rs\.?\s*([0-9,]+).*?new regime",
            taxcalc_text,
            re.IGNORECASE | re.DOTALL,
        )
    if not std_match:
        raise ValueError("Unable to parse standard deduction values.")

    old_rebate = {
        "threshold": _parse_indian_amount(f"{prev_old_match.group(2)} lakh"),
        "max_rebate": _to_number(prev_old_match.group(1)),
        "resident_only": True,
        "marginal_relief": False,
    }
    prev_new_rebate = {
        "threshold": _parse_indian_amount(f"{prev_new_match.group(2)} lakh"),
        "max_rebate": _to_number(prev_new_match.group(1)),
        "resident_only": True,
        "marginal_relief": True,
    }
    current_new_rebate = {
        "threshold": _parse_indian_amount(f"{current_rebate_threshold.group(1)} lakh"),
        "max_rebate": _to_number(current_rebate_change.group(2)),
        "resident_only": True,
        "marginal_relief": True,
    }
    old_rebate["max_rebate"] = _to_number(current_old_rebate.group(1))

    standard_deduction = {
        "old": _to_number(std_match.group(1)),
        "new": _to_number(std_match.group(2)),
    }
    return old_rebate, prev_new_rebate, current_new_rebate, standard_deduction


def _extract_cess_and_surcharge(taxcalc_text: str) -> tuple[float, list[dict[str, float]], list[dict[str, float]]]:
    cess_match = re.search(
        r"Health and Education cess at the rate of\s*([0-9.]+)%",
        taxcalc_text,
        re.IGNORECASE,
    )
    if not cess_match:
        raise ValueError("Unable to parse cess rate.")

    surcharge_rows: list[tuple[float, float]] = []
    for rate in (10, 15, 25, 37):
        match = re.search(
            rf"{rate}% of Income tax if total income > Rs\.\s*([0-9.]+)\s*(lakh|crore)",
            taxcalc_text,
            re.IGNORECASE,
        )
        if not match:
            raise ValueError(f"Unable to parse surcharge threshold for rate {rate}%.")
        amount = _parse_indian_amount(f"{match.group(1)} {match.group(2)}")
        surcharge_rows.append((amount, float(rate) / 100.0))

    surcharge_rows = sorted(surcharge_rows, key=lambda row: row[0])
    old_surcharge = [
        {"threshold": threshold, "rate": rate}
        for threshold, rate in surcharge_rows
    ]

    new_top_cap = bool(
        re.search(
            r"highest surcharge rate of 37% has been reduced to 25% under the new tax regime",
            taxcalc_text,
            re.IGNORECASE,
        )
    )
    new_surcharge: list[dict[str, float]] = []
    for row in old_surcharge:
        capped_rate = min(row["rate"], 0.25) if new_top_cap else row["rate"]
        new_surcharge.append({"threshold": row["threshold"], "rate": capped_rate})

    return _rate_fraction(cess_match.group(1)), old_surcharge, new_surcharge


def _extract_capital_gains_rules(capital_text: str, *, top_slab_rate: float) -> dict[str, Any]:
    stcg_match = re.search(
        r"short-term capital gains are taxed at\s*([0-9.]+)%\s*or at applicable slab rates",
        capital_text,
        re.IGNORECASE,
    )
    if not stcg_match:
        raise ValueError("Unable to parse STCG rate from capital-gains source.")

    ltcg_match = re.search(
        r"([0-9.]+)%\s*\(Rs\.\s*([0-9.,]+)\s*(L|lakh)\s*exemption on equity\)",
        capital_text,
        re.IGNORECASE,
    )
    if not ltcg_match:
        ltcg_match = re.search(
            r"([0-9.]+)%\s*over and above Rs\s*([0-9.,]+)\s*(L|lakh)",
            capital_text,
            re.IGNORECASE,
        )
    if not ltcg_match:
        raise ValueError("Unable to parse LTCG rate/exemption from capital-gains source.")

    holding_match = re.search(
        r"Up to\s*([0-9]+)\s*months\s*\(equity\)\s*or\s*([0-9]+)\s*months\s*\(others\)",
        capital_text,
        re.IGNORECASE,
    )
    if not holding_match:
        raise ValueError("Unable to parse holding-period table from capital-gains source.")

    debt_is_short = bool(
        re.search(
            r"treated as short-term capital gains under section\s*50AA",
            capital_text,
            re.IGNORECASE,
        )
    )

    equity_holding = int(holding_match.group(1))
    ltcg_rate = _rate_fraction(ltcg_match.group(1))
    exemption = _parse_indian_amount(f"{ltcg_match.group(2)} {ltcg_match.group(3)}")
    debt_holding = 9_999 if debt_is_short else int(holding_match.group(2))

    return {
        "assets": {
            "equity": {
                "holding_period_months": equity_holding,
                "stcg_rate": _rate_fraction(stcg_match.group(1)),
                "ltcg_rate": ltcg_rate,
                "ltcg_exemption": exemption,
                "section": "111A/112A",
            },
            "debt_mf": {
                "holding_period_months": debt_holding,
                "stcg_rate": top_slab_rate,
                "ltcg_rate": top_slab_rate,
                "ltcg_exemption": 0.0,
                "section": "50AA (deemed STCG; estimator uses top slab rate)",
            },
        }
    }


def _extract_tds_rules(
    tds_text: str,
    changes_2025_text: str,
    *,
    fy_start_year: int,
) -> dict[str, Any]:
    current_194a = re.search(
        r"194A\s+on bank/post office deposits\s+Rs\.?\s*([0-9,]+)\s+([0-9.]+)%",
        tds_text,
        re.IGNORECASE | re.DOTALL,
    )
    current_194c = re.search(
        r"194C\s+Payment to contractors or sub-contractors\s+Rs\.?\s*([0-9,]+).*?"
        r"([0-9.]+)%\s*for individuals and HUF",
        tds_text,
        re.IGNORECASE | re.DOTALL,
    )
    current_194j = re.search(
        r"194J\(b\)\s+Fees\s*[\u2013\-]\s*All other Professional Services\s+Rs\.?\s*([0-9,]+)\s+([0-9.]+)%",
        tds_text,
        re.IGNORECASE | re.DOTALL,
    )
    if not (current_194a and current_194c and current_194j):
        raise ValueError("Unable to parse required TDS sections from TDS chart source.")

    threshold_194a_change = re.search(
        r"194A - Interest other than Interest on securities.*?"
        r"\(ii\)\s*([0-9,]+).*?"
        r"\(ii\)\s*([0-9,]+)",
        changes_2025_text,
        re.IGNORECASE | re.DOTALL,
    )
    threshold_194j_change = re.search(
        r"194J - Fee for professional or technical services\s*([0-9,]+)\s*([0-9,]+)",
        changes_2025_text,
        re.IGNORECASE | re.DOTALL,
    )

    if threshold_194a_change and threshold_194j_change:
        before_194a = _to_number(threshold_194a_change.group(1))
        after_194a = _to_number(threshold_194a_change.group(2))
        before_194j = _to_number(threshold_194j_change.group(1))
        after_194j = _to_number(threshold_194j_change.group(2))
    else:
        before_194a = after_194a = _to_number(current_194a.group(1))
        before_194j = after_194j = _to_number(current_194j.group(1))

    use_after = fy_start_year >= 2025
    threshold_194a = after_194a if use_after else before_194a
    threshold_194j = after_194j if use_after else before_194j

    return {
        "sections": [
            {
                "section": "194A",
                "label": "Interest",
                "rate": _rate_fraction(current_194a.group(2)),
                "threshold": threshold_194a,
                "resident_only": True,
            },
            {
                "section": "194C",
                "label": "Contract Payment",
                "rate": _rate_fraction(current_194c.group(2)),
                "threshold": _to_number(current_194c.group(1)),
                "resident_only": True,
            },
            {
                "section": "194J",
                "label": "Professional Fee",
                "rate": _rate_fraction(current_194j.group(2)),
                "threshold": threshold_194j,
                "resident_only": True,
            },
        ]
    }


def _extract_advance_tax_rules(advance_text: str) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    labels = ["First", "Second", "Third", "Fourth"]
    short = ["Q1", "Q2", "Q3", "Q4"]
    for long_label, short_label in zip(labels, short, strict=True):
        match = re.search(
            rf"{long_label}\s+Instalment\s+On or before\s*([0-9]{{1,2}})(?:st|nd|rd|th)?\s*"
            rf"([A-Za-z]+)\s*[0-9]{{4}}\s+([0-9]{{1,3}})% of tax liability",
            advance_text,
            re.IGNORECASE | re.DOTALL,
        )
        if not match:
            raise ValueError(f"Unable to parse advance-tax installment: {long_label}")
        day = int(match.group(1))
        month = match.group(2).strip()[:3].title()
        percent = _to_number(match.group(3))
        rows.append(
            {
                "label": short_label,
                "due_date": f"{day:02d} {month}",
                "cumulative_percent": percent,
            }
        )

    interest_match = re.search(
        r"interest at\s*([0-9.]+)%\s*per month is payable",
        advance_text,
        re.IGNORECASE,
    )
    if not interest_match:
        raise ValueError("Unable to parse section 234C interest rate from advance-tax source.")

    return {
        "installments": rows,
        "interest_rate_234c": _rate_fraction(interest_match.group(1)),
    }


def _pick_nearest_ruleset(mapping: dict[str, Any], fy_id: str) -> Any:
    if fy_id in mapping:
        return mapping[fy_id]
    target = tax_fy.fy_start_year(fy_id)
    entries = sorted((tax_fy.fy_start_year(key), key) for key in mapping)
    if not entries:
        raise ValueError("No rule mapping available.")
    if target <= entries[0][0]:
        return mapping[entries[0][1]]
    if target >= entries[-1][0]:
        return mapping[entries[-1][1]]
    lower = max((item for item in entries if item[0] <= target), key=lambda item: item[0])
    upper = min((item for item in entries if item[0] >= target), key=lambda item: item[0])
    if abs(lower[0] - target) <= abs(upper[0] - target):
        return mapping[lower[1]]
    return mapping[upper[1]]


def _format_indian_rupees(value: float) -> str:
    rounded = int(round(float(value)))
    negative = rounded < 0
    digits = str(abs(rounded))
    if len(digits) > 3:
        head = digits[:-3]
        tail = digits[-3:]
        groups: list[str] = []
        while len(head) > 2:
            groups.insert(0, head[-2:])
            head = head[:-2]
        if head:
            groups.insert(0, head)
        digits = ",".join([*groups, tail])
    return f"{'-' if negative else ''}₹{digits}"


def _pct_label(rate: float) -> str:
    value = float(rate) * 100.0
    if abs(value - round(value)) < 1e-6:
        return f"{int(round(value))}%"
    return f"{value:.2f}%"


def _build_helper_points(
    *,
    default_fy: str,
    rules_by_fy: dict[str, dict[str, Any]],
) -> dict[str, list[str]]:
    rule_set = rules_by_fy.get(default_fy) or next(iter(rules_by_fy.values()))
    income = rule_set["income_tax"]
    capital = rule_set["capital_gains"]
    advance = rule_set["advance_tax"]
    tds = rule_set["tds"]

    income_new_rebate = income["rebate"]["new"]
    income_old_rebate = income["rebate"]["old"]
    income_new_std = income["standard_deduction"]["new"]
    income_old_std = income["standard_deduction"]["old"]

    equity = capital["assets"]["equity"]
    debt = capital["assets"]["debt_mf"]

    installments = list(advance["installments"])
    tds_sections = list(tds["sections"])

    first_installment = installments[0] if installments else None
    final_installment = installments[-1] if installments else None
    main_tds = tds_sections[0] if tds_sections else None

    return {
        "hub": [
            f"Rules auto-sync from ClearTax for {default_fy}.",
            "Pick your calculator based on salary, gains, advance tax, or TDS estimate.",
            "Results are estimates for planning and should be validated before filing.",
        ],
        "income_tax": [
            f"New regime rebate applies up to {_format_indian_rupees(income_new_rebate['threshold'])}; old regime rebate up to {_format_indian_rupees(income_old_rebate['threshold'])}.",
            f"Standard deduction: new {_format_indian_rupees(income_new_std)}, old {_format_indian_rupees(income_old_std)}.",
            "Switch regime to compare tax quickly before final filing.",
        ],
        "capital_gains": [
            f"Equity gains use {_pct_label(equity['stcg_rate'])} (STCG) and {_pct_label(equity['ltcg_rate'])} (LTCG) with {_format_indian_rupees(equity['ltcg_exemption'])} LTCG exemption.",
            f"Use holding period to classify short vs long term (equity cutoff: {equity['holding_period_months']} months).",
            f"Debt MF is estimated at top slab {_pct_label(debt['stcg_rate'])} under section 50AA treatment.",
        ],
        "advance_tax": [
            "Enter total annual tax and tax already paid to see current shortfall.",
            (
                f"Advance tax schedule runs from {first_installment['label']} ({first_installment['due_date']})"
                f" to {final_installment['label']} ({final_installment['due_date']})."
                if first_installment and final_installment
                else "Advance tax schedule follows quarterly cumulative targets."
            ),
            "Paying by the next due milestone helps avoid section 234C interest.",
        ],
        "tds": [
            "Use this to estimate net amount you may receive after TDS deduction.",
            (
                f"Typical section {main_tds['section']} uses {_pct_label(main_tds['rate'])} above {_format_indian_rupees(main_tds['threshold'])}."
                if main_tds
                else "TDS rate and threshold depend on the selected section."
            ),
            "Select the closest section to your payment type for a better estimate.",
        ],
    }


async def _fetch_sources(
    *,
    source_urls: dict[str, str],
    timeout_seconds: int,
) -> tuple[dict[str, str], dict[str, str]]:
    timeout = httpx.Timeout(float(timeout_seconds))
    resolved_urls: dict[str, str] = {}
    raw_html: dict[str, str] = {}
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True, headers=_REQUEST_HEADERS) as client:
        for key, primary_url in source_urls.items():
            candidate_urls = [primary_url, *(_SOURCE_FALLBACK_URLS.get(key) or [])]
            last_error: Exception | None = None
            for candidate_url in candidate_urls:
                for attempt in range(3):
                    try:
                        response = await client.get(candidate_url)
                        response.raise_for_status()
                        raw_html[key] = response.text
                        resolved_urls[key] = candidate_url
                        break
                    except Exception as exc:
                        last_error = exc
                        if attempt < 2:
                            await asyncio.sleep(1.0 + attempt)
                if key in raw_html:
                    break
            if key not in raw_html:
                raise RuntimeError(f"Failed to fetch source '{key}': {last_error}")
    return resolved_urls, raw_html


async def fetch_official_tax_bundle(
    *,
    timeout_seconds: int = 30,
    now_utc: datetime | None = None,
) -> OfficialTaxBundle:
    now = now_utc or datetime.now(timezone.utc)

    source_urls = {
        "income_changes_2025": OFFICIAL_TAX_SOURCE_URLS.income_changes_2025,
        "income_changes_2024": OFFICIAL_TAX_SOURCE_URLS.income_changes_2024,
        "tax_calculator": OFFICIAL_TAX_SOURCE_URLS.tax_calculator,
        "capital_gains": OFFICIAL_TAX_SOURCE_URLS.capital_gains,
        "tds_rate_chart": OFFICIAL_TAX_SOURCE_URLS.tds_rate_chart,
        "advance_tax": OFFICIAL_TAX_SOURCE_URLS.advance_tax,
    }

    resolved_urls, raw_html = await _fetch_sources(
        source_urls=source_urls,
        timeout_seconds=timeout_seconds,
    )

    changes_2025_text = _clean_text(raw_html["income_changes_2025"])
    changes_2024_text = _clean_text(raw_html["income_changes_2024"])
    taxcalc_text = _clean_text(raw_html["tax_calculator"])
    capital_text = _clean_text(raw_html["capital_gains"])
    tds_text = _clean_text(raw_html["tds_rate_chart"])
    advance_text = _clean_text(raw_html["advance_tax"])

    fy2025_new_slabs, fy2024_new_slabs = _extract_new_slabs_fy_2025_and_2024(changes_2025_text)
    selected_taxcalc_fy, latest_taxcalc_fy, selected_new_slabs = _extract_taxcalc_latest_new_slabs(
        taxcalc_text
    )

    old_slabs, old_basic_exemption = _extract_old_slabs_and_basic_exemption(taxcalc_text)
    old_rebate, prev_new_rebate, current_new_rebate, standard_deduction = _extract_rebate_and_standard_deduction(
        changes_2024_text,
        changes_2025_text,
        taxcalc_text,
    )
    cess_rate, old_surcharge, new_surcharge = _extract_cess_and_surcharge(taxcalc_text)

    top_old_rate = max(float(row["rate"]) for row in old_slabs)
    capital_rules = _extract_capital_gains_rules(capital_text, top_slab_rate=top_old_rate)
    advance_rules = _extract_advance_tax_rules(advance_text)

    new_slabs_by_fy: dict[str, list[dict[str, float]]] = {
        _format_fy_id(2024): fy2024_new_slabs,
        _format_fy_id(2025): fy2025_new_slabs,
        selected_taxcalc_fy: selected_new_slabs,
    }
    new_rebate_by_fy: dict[str, dict[str, Any]] = {
        _format_fy_id(2024): prev_new_rebate,
        _format_fy_id(2025): current_new_rebate,
        selected_taxcalc_fy: current_new_rebate,
    }

    source_fys = sorted(
        {selected_taxcalc_fy, latest_taxcalc_fy},
        key=tax_fy.fy_start_year,
    )
    if not source_fys:
        raise ValueError("No FY keys parsed from ClearTax sources.")

    rules_by_fy: dict[str, dict[str, Any]] = {}
    for fy_id in source_fys:
        fy_start_year = tax_fy.fy_start_year(fy_id)
        new_slabs = _pick_nearest_ruleset(new_slabs_by_fy, fy_id)
        new_rebate = _pick_nearest_ruleset(new_rebate_by_fy, fy_id)
        tds_rules = _extract_tds_rules(tds_text, changes_2025_text, fy_start_year=fy_start_year)
        rules_by_fy[fy_id] = {
            "income_tax": {
                "standard_deduction": {
                    "old": float(standard_deduction["old"]),
                    "new": float(standard_deduction["new"]),
                },
                "old_basic_exemption": {
                    "below60": float(old_basic_exemption["below60"]),
                    "age60to80": float(old_basic_exemption["age60to80"]),
                    "above80": float(old_basic_exemption["above80"]),
                },
                "old_slabs": old_slabs,
                "new_slabs": new_slabs,
                "rebate": {
                    "old": old_rebate,
                    "new": new_rebate,
                },
                "surcharge": {
                    "old": old_surcharge,
                    "new": new_surcharge,
                },
                "cess_rate": cess_rate,
            },
            "capital_gains": capital_rules,
            "advance_tax": advance_rules,
            "tds": tds_rules,
        }

    supported_fy = [
        {"id": fy_id, "label": tax_fy.fy_label_from_start_year(tax_fy.fy_start_year(fy_id))}
        for fy_id in source_fys
    ]
    default_fy = selected_taxcalc_fy if selected_taxcalc_fy in rules_by_fy else source_fys[-1]
    helper_points = _build_helper_points(
        default_fy=default_fy,
        rules_by_fy=rules_by_fy,
    )

    version = f"cleartax-{now.strftime('%Y%m%d%H%M')}"
    config_payload = {
        "version": version,
        "hash": "",
        "supported_fy": supported_fy,
        "default_fy": default_fy,
        "disclaimer": (
            "Rules sourced from ClearTax tax content pages. "
            "Debt mutual fund gains are treated under section 50AA with top-slab estimator."
        ),
        "helper_points": helper_points,
        "rounding_policy": {
            "currency_scale": 2,
            "percentage_scale": 2,
            "tax_rounding": "nearest_rupee",
        },
        "rules_by_fy": rules_by_fy,
    }
    config_payload["hash"] = tax_service.compute_config_hash(config_payload)

    return OfficialTaxBundle(
        source_name="cleartax_web",
        source_urls=resolved_urls,
        config_payload=config_payload,
    )

from __future__ import annotations

import re
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")
_FY_PATTERN = re.compile(r"^FY(\d{4})-(\d{2})$")


def fy_start_year(fy_id: str) -> int:
    match = _FY_PATTERN.match(str(fy_id).strip())
    if not match:
        raise ValueError(f"Invalid FY id: {fy_id}")
    start = int(match.group(1))
    end_two = int(match.group(2))
    if (start + 1) % 100 != end_two:
        raise ValueError(f"Invalid FY id: {fy_id}")
    return start


def fy_id_from_start_year(start_year: int) -> str:
    return f"FY{start_year}-{(start_year + 1) % 100:02d}"


def fy_label_from_start_year(start_year: int) -> str:
    return (
        f"FY {start_year}-{(start_year + 1) % 100:02d} "
        f"(AY {start_year + 1}-{(start_year + 2) % 100:02d})"
    )


def current_fy_start_year(*, now_utc: datetime | None = None) -> int:
    now = now_utc or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    now_ist = now.astimezone(IST)
    return now_ist.year if now_ist.month >= 4 else now_ist.year - 1


def fy_window_ids(*, now_utc: datetime | None = None) -> tuple[str, str, str]:
    current_start = current_fy_start_year(now_utc=now_utc)
    prev = fy_id_from_start_year(current_start - 1)
    current = fy_id_from_start_year(current_start)
    nxt = fy_id_from_start_year(current_start + 1)
    return (prev, current, nxt)


def trim_payload_to_window(
    payload: dict,
    *,
    now_utc: datetime | None = None,
    require_all_window_fys: bool = False,
) -> dict:
    window = fy_window_ids(now_utc=now_utc)
    window_set = set(window)
    rules_by_fy = payload.get("rules_by_fy") or {}
    if not isinstance(rules_by_fy, dict):
        raise ValueError("rules_by_fy must be an object.")

    missing = [fy for fy in window if fy not in rules_by_fy]
    if require_all_window_fys and missing:
        raise ValueError(f"Missing FY rules for window: {', '.join(missing)}")

    selected_fys = [fy for fy in window if fy in rules_by_fy]
    if not selected_fys:
        raise ValueError("No FY rules available in canonical 3-FY window.")

    supported_raw = payload.get("supported_fy") or []
    if not isinstance(supported_raw, list):
        supported_raw = []
    supported_lookup: dict[str, str] = {}
    for item in supported_raw:
        if not isinstance(item, dict):
            continue
        fy_id = str(item.get("id") or "").strip()
        label = str(item.get("label") or "").strip()
        if fy_id:
            supported_lookup[fy_id] = label or fy_label_from_start_year(fy_start_year(fy_id))

    supported_fy = []
    for fy in selected_fys:
        if fy in supported_lookup:
            label = supported_lookup[fy]
        else:
            label = fy_label_from_start_year(fy_start_year(fy))
        supported_fy.append({"id": fy, "label": label})

    current = window[1]
    default_fy = current if current in selected_fys else selected_fys[-1]

    trimmed = dict(payload)
    trimmed["supported_fy"] = supported_fy
    trimmed["default_fy"] = default_fy
    trimmed["rules_by_fy"] = {fy: rules_by_fy[fy] for fy in selected_fys}
    return trimmed

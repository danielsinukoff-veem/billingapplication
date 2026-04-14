from __future__ import annotations

from calendar import monthrange
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import sys


ROOT_DIR = Path(__file__).resolve().parents[1]
DB_PATH = ROOT_DIR / "server" / "data" / "shared_workspace.db"
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from server.storage import SharedWorkspaceStore


def norm(value: Any) -> str:
    return str(value or "").strip().lower()


def normalize_month_key(value: Any) -> str:
    return str(value or "").strip()[:7]


def normalize_iso_date(value: Any) -> str:
    text = str(value or "").strip()
    return text[:10] if len(text) >= 10 else ""


def parse_due_days_from_pay_by(value: Any) -> int:
    text = str(value or "").strip().lower()
    if not text:
        return 0
    digits = "".join(ch for ch in text if ch.isdigit())
    return int(digits) if digits else 0


def infer_billing_day_from_timing_text(value: Any) -> int:
    text = str(value or "").strip().lower()
    if not text:
        return 0
    if "end of month" in text:
        return 31
    if "begining of month" in text or "beginning of month" in text or "start of month" in text:
        return 1
    if "first week" in text or "1st week" in text:
        return 7
    if "second week" in text or "2nd week" in text:
        return 14
    if "third week" in text or "3rd week" in text:
        return 21
    if "fourth week" in text or "4th week" in text:
        return 28
    for suffix in ("st", "nd", "rd", "th"):
        marker = suffix
        for day in range(1, 32):
            if f"{day}{marker}" in text:
                return min(day, 31)
    if "first" in text:
        return 1
    if "second" in text:
        return 2
    if "third" in text:
        return 3
    if "fourth" in text:
        return 4
    return 0


def get_partner_billing_config(snapshot: dict[str, Any], partner: str) -> dict[str, Any]:
    for row in snapshot.get("pBilling", []) or []:
        if norm(row.get("partner")) == norm(partner):
            return row
    return {}


def get_billing_day(snapshot: dict[str, Any], partner: str) -> int:
    config = get_partner_billing_config(snapshot, partner)
    try:
        day = int(float(config.get("billingDay") or 0))
    except (TypeError, ValueError):
        return 0
    if day <= 0:
        return 0
    return min(day, 31)


def get_inferred_billing_day(snapshot: dict[str, Any], partner: str) -> int:
    explicit = get_billing_day(snapshot, partner)
    if explicit:
        return explicit
    config = get_partner_billing_config(snapshot, partner)
    preferred = infer_billing_day_from_timing_text(config.get("preferredBillingTiming"))
    if preferred:
        return preferred
    note = infer_billing_day_from_timing_text(config.get("note"))
    if note:
        return note
    return 1


def next_month_key(period: str) -> str:
    normalized = normalize_month_key(period)
    if not normalized:
        return ""
    year, month = normalized.split("-")
    year_num = int(year)
    month_num = int(month)
    if month_num == 12:
        return f"{year_num + 1}-01"
    return f"{year_num}-{str(month_num + 1).zfill(2)}"


def get_expected_invoice_send_date(snapshot: dict[str, Any], partner: str, period: str) -> str:
    billing_day = get_inferred_billing_day(snapshot, partner)
    if not billing_day:
        return ""
    send_month = next_month_key(period)
    if not send_month:
        return ""
    year_num, month_num = map(int, send_month.split("-"))
    day = min(billing_day, monthrange(year_num, month_num)[1])
    return f"{year_num}-{str(month_num).zfill(2)}-{str(day).zfill(2)}"


def infer_invoice_tracking_invoice_date(snapshot: dict[str, Any], partner: str, period: str, kind: str) -> str:
    normalized_period = normalize_month_key(period)
    if not normalized_period:
        return ""
    expected_send_date = get_expected_invoice_send_date(snapshot, partner, normalized_period)
    if expected_send_date:
        return expected_send_date
    next_period = next_month_key(normalized_period)
    if not next_period:
        return ""
    year_num, month_num = map(int, next_period.split("-"))
    return f"{year_num}-{str(month_num).zfill(2)}-01"


def main() -> int:
    store = SharedWorkspaceStore(DB_PATH)
    workspace = store.get_workspace()
    snapshot = workspace.get("snapshot")
    if not isinstance(snapshot, dict):
        print("No shared workbook snapshot found.")
        return 1

    rows = snapshot.get("pInvoices") or []
    changed = 0
    for row in rows:
        has_payment = bool(row.get("paid")) or float(row.get("amountPaid") or 0) > 0
        if not has_payment or normalize_iso_date(row.get("invoiceDate")):
            continue
        inferred = infer_invoice_tracking_invoice_date(
            snapshot,
            str(row.get("partner") or ""),
            str(row.get("period") or ""),
            str(row.get("kind") or "receivable"),
        )
        if not inferred:
            continue
        row["invoiceDate"] = inferred
        changed += 1

    if not changed:
        print("No invoice dates needed backfill.")
        return 0

    snapshot["pInvoices"] = rows
    snapshot["_saved"] = datetime.now(timezone.utc).isoformat()
    saved_at = store.save_snapshot(snapshot)
    print(f"Backfilled {changed} invoice dates. Saved at {saved_at}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

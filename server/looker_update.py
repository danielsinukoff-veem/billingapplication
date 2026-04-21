"""Archival reference Looker update helpers.

The production Looker import flow should be driven by n8n and the AWS-hosted
API/workflow stack.
"""

from __future__ import annotations

import base64
import csv
import importlib.util
import io
import re
import sys
import tempfile
import uuid
from collections import defaultdict
from datetime import date, datetime, timedelta
from functools import lru_cache
from pathlib import Path
from typing import Any


ROOT_DIR = Path(__file__).resolve().parents[1]
GENERATOR_PATH = ROOT_DIR / "tools" / "generate_looker_import.py"

LOOKER_FILE_TYPES = {
    "partner_offline_billing": {
        "label": "Partner Offline Billing 2026-04-12T2001.xlsx",
        "defaultSuffix": ".csv",
    },
    "partner_offline_billing_reversals": {
        "label": "Partner Offline Billing (Reversals) 2026-04-12T2009.xlsx",
        "defaultSuffix": ".csv",
    },
    "all_registered_accounts": {
        "label": "All Registered Accounts.xlsx / all_registered_accounts.csv",
        "defaultSuffix": ".csv",
    },
    "all_registered_accounts_offline": {
        "label": "All Registered Accounts - Offline Billing 2026-04-12T1940.xlsx",
        "defaultSuffix": ".csv",
    },
    "all_registered_accounts_rev_share": {
        "label": "All Registered Accounts - Rev Share 2026-04-12T1943.xlsx",
        "defaultSuffix": ".csv",
    },
    "vba_accounts": {
        "label": "VBA ACCOUNTS 2026-04-12T2034.xlsx",
        "defaultSuffix": ".csv",
    },
    "vba_transactions": {
        "label": "CC_Citi VBA Txns 2026-04-12T1949.xlsx",
        "defaultSuffix": ".csv",
    },
    "vba_transactions_cc": {
        "label": "CC/Citi VBA Txns (CC).csv",
        "defaultSuffix": ".csv",
    },
    "vba_transactions_citi": {
        "label": "CC/Citi VBA Txns (Citi).csv",
        "defaultSuffix": ".csv",
    },
    "partner_rev_share_v2": {
        "label": "Partner Rev Share V2.xlsx / partner_revenue_share_v2.csv",
        "defaultSuffix": ".csv",
    },
    "partner_revenue_share": {
        "label": "Partner Revenue Share.xlsx / partner_revenue_share.csv",
        "defaultSuffix": ".csv",
    },
    "revenue_share_report": {
        "label": "Revenue Share Report 2026-04-12T2020.xlsx",
        "defaultSuffix": ".csv",
    },
    "partner_revenue_reversal": {
        "label": "Partner Revenue Reversal.xlsx / partner_revenue_reversal.csv",
        "defaultSuffix": ".csv",
    },
    "rev_share_reversals": {
        "label": "Rev Share Reversals 2026-04-12T2020.xlsx",
        "defaultSuffix": ".csv",
    },
    "partner_revenue_summary": {
        "label": "Partner Revenue Summary.xlsx / partner_revenue_summary.csv",
        "defaultSuffix": ".csv",
    },
    "all_stampli_credit_complete": {
        "label": "All Stampli Credit Complete.xlsx / .csv",
        "defaultSuffix": ".csv",
    },
    "stampli_fx_revenue_share": {
        "label": "Stampli FX Revenue Share 2026-04-12T2033.xlsx",
        "defaultSuffix": ".csv",
    },
    "stampli_fx_revenue_reversal": {
        "label": "Stampli FX Revenue Reversal 2026-04-12T2033.xlsx",
        "defaultSuffix": ".csv",
    },
}

LOOKER_IMPORT_ORDER = [
    "partner_offline_billing",
    "partner_offline_billing_reversals",
    "all_registered_accounts_offline",
    "vba_accounts",
    "vba_transactions_cc",
    "vba_transactions_citi",
    "vba_transactions",
    "revenue_share_report",
    "rev_share_reversals",
    "all_registered_accounts_rev_share",
    "stampli_fx_revenue_share",
    "stampli_fx_revenue_reversal",
    "all_registered_accounts",
    "partner_rev_share_v2",
    "partner_revenue_share",
    "partner_revenue_reversal",
    "partner_revenue_summary",
    "all_stampli_credit_complete",
]

LVA_CONTEXT_FILE_TYPES = {
    "partner_offline_billing",
    "all_registered_accounts",
    "all_registered_accounts_offline",
    "all_registered_accounts_rev_share",
    "vba_accounts",
    "vba_transactions_cc",
    "vba_transactions_citi",
    "vba_transactions",
}

STAMPLI_FX_CONTEXT_FILE_TYPES = {
    "stampli_fx_revenue_share",
    "stampli_fx_revenue_reversal",
}

FULL_HISTORY_FILE_TYPES = {
    "partner_offline_billing",
    "partner_offline_billing_reversals",
    "all_registered_accounts",
    "all_registered_accounts_offline",
    "all_registered_accounts_rev_share",
    "vba_accounts",
    "vba_transactions",
    "vba_transactions_cc",
    "vba_transactions_citi",
    "partner_rev_share_v2",
    "partner_revenue_share",
    "revenue_share_report",
    "partner_revenue_reversal",
    "rev_share_reversals",
    "partner_revenue_summary",
    "all_stampli_credit_complete",
    "stampli_fx_revenue_share",
    "stampli_fx_revenue_reversal",
}

SECTION_CHANGE_LABELS = {
    "ltxn": "Transactions",
    "lrev": "Reversals",
    "lva": "Virtual Accounts",
    "lrs": "Revenue Share",
    "lfxp": "Stampli FX Payout",
}

SECTION_CHANGE_FIELDS = {
    "ltxn": ("txnCount", "totalVolume", "customerRevenue", "estRevenue", "directInvoiceAmount"),
    "lrev": ("txnCount", "totalVolume", "customerRevenue"),
    "lva": ("totalActiveAccounts", "newAccountsOpened", "dormantAccounts", "newBusinessSetups", "settlementCount", "closedAccounts"),
    "lrs": ("partnerRevenueShare", "revenueOwed", "monthlyMinimumRevenue", "netRevenue", "summaryLineAmount"),
    "lfxp": ("txnCount", "shareAmount", "volume", "paymentUsdEquivalentAmount"),
}

MAX_PERSISTED_DETAIL_ROWS_FOR_ALL_TIME_IMPORT = 5000
MAX_PERSISTED_DETAIL_ROWS_PER_IMPORT = 10000
MAX_PERSISTED_DETAIL_ROWS_TOTAL = 50000


@lru_cache(maxsize=1)
def load_generator_module():
    spec = importlib.util.spec_from_file_location("billing_looker_generator", GENERATOR_PATH)
    if not spec or not spec.loader:
        raise RuntimeError("Could not load Looker import generator module.")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)  # type: ignore[attr-defined]
    return module


def _first_data_line(raw_text: str) -> str:
    for line in raw_text.splitlines():
        if line.strip():
            return line
    return ""


def normalize_pasted_table(raw_text: str) -> str:
    cleaned = raw_text.replace("\ufeff", "").strip()
    if not cleaned:
        return ""
    sample_line = _first_data_line(cleaned)
    delimiter = "\t" if "\t" in sample_line else ","
    reader = csv.reader(io.StringIO(cleaned), delimiter=delimiter)
    output = io.StringIO()
    writer = csv.writer(output)
    for row in reader:
        if not row:
            continue
        writer.writerow(row)
    return output.getvalue()


def decode_upload_bytes(payload: dict[str, Any], file_type: str) -> tuple[bytes, str]:
    file_name = str(payload.get("fileName") or "").strip()
    file_b64 = str(payload.get("fileBase64") or "").strip()
    pasted_text = str(payload.get("pastedText") or "")
    file_meta = LOOKER_FILE_TYPES[file_type]
    suffix = Path(file_name).suffix or file_meta["defaultSuffix"]

    if file_b64:
        return base64.b64decode(file_b64), suffix
    if pasted_text.strip():
        normalized = normalize_pasted_table(pasted_text)
        if not normalized:
            raise ValueError("No tabular data was detected in the pasted content.")
        return normalized.encode("utf-8"), ".csv"
    raise ValueError("Provide either a file upload or pasted tabular data.")


def build_temp_path(file_type: str, suffix: str) -> Path:
    temp_dir = Path(tempfile.mkdtemp(prefix="billing-looker-import-"))
    return temp_dir / f"{file_type}{suffix}"


def serialize_dates(values: list[date]) -> list[str]:
    return [value.isoformat() for value in sorted(set(values))]


def serialize_offline_context(account_activity: dict[str, list[date]], settlement_days: dict[tuple[str, str], set[date]]) -> dict[str, Any]:
    return {
        "accountActivity": {account_id: serialize_dates(days) for account_id, days in account_activity.items()},
        "settlementDays": {f"{partner}|{period}": serialize_dates(list(days)) for (partner, period), days in settlement_days.items()},
    }


def deserialize_offline_context(context: dict[str, Any]) -> tuple[dict[str, list[date]], dict[tuple[str, str], set[date]]]:
    account_activity: dict[str, list[date]] = {}
    settlement_days: dict[tuple[str, str], set[date]] = {}
    for account_id, day_values in (context.get("accountActivity") or {}).items():
        account_activity[str(account_id)] = [date.fromisoformat(str(day)) for day in day_values or [] if day]
    for key, day_values in (context.get("settlementDays") or {}).items():
        partner, _, period = str(key).partition("|")
        if not partner or not period:
            continue
        settlement_days[(partner, period)] = {date.fromisoformat(str(day)) for day in day_values or [] if day}
    return account_activity, settlement_days


def merge_account_activity(
    existing: dict[str, list[date]],
    incoming: dict[str, list[date]],
) -> dict[str, list[date]]:
    merged: dict[str, list[date]] = {
        str(account_id): sorted(set(days or []))
        for account_id, days in (existing or {}).items()
    }
    for account_id, days in (incoming or {}).items():
        key = str(account_id or "").strip()
        if not key:
            continue
        merged[key] = sorted(set([*(merged.get(key) or []), *(days or [])]))
    return merged


def context_registered_account_rows(context: dict[str, Any]) -> list[dict[str, Any]]:
    rows = context.get("registeredAccountRows") or []
    if not isinstance(rows, list):
        return []
    return [dict(row) for row in rows if isinstance(row, dict)]


def month_key(value: date) -> str:
    return value.strftime("%Y-%m")


def enumerate_period_range(start_period: str, end_period: str) -> list[str]:
    if not start_period or not end_period:
        return []
    start_year, start_month = (int(part) for part in start_period.split("-", 1))
    end_year, end_month = (int(part) for part in end_period.split("-", 1))
    periods: list[str] = []
    year = start_year
    month = start_month
    while (year, month) <= (end_year, end_month):
        periods.append(f"{year:04d}-{month:02d}")
        if month == 12:
            year += 1
            month = 1
        else:
            month += 1
    return periods


def derive_virtual_account_periods(
    module,
    register_rows: list[dict[str, Any]],
    account_activity: dict[str, list[date]],
    settlement_days: dict[tuple[str, str], set[date]],
    fallback_period: str,
) -> list[str]:
    explicit_period = str(fallback_period or "").strip()
    if explicit_period:
        return [explicit_period]

    dates: list[date] = []
    for row in register_rows or []:
        join_date = module.parse_dateish_from_row(
            row,
            "Join Date Time",
            patterns=("joindatetime",),
        )
        if join_date:
            dates.append(join_date)
    for day_values in (account_activity or {}).values():
        dates.extend(day for day in (day_values or []) if isinstance(day, date))
    for day_values in (settlement_days or {}).values():
        dates.extend(day for day in (day_values or set()) if isinstance(day, date))
    if not dates:
        return []
    start_period = month_key(min(dates))
    end_period = month_key(max([*dates, date.today()]))
    return enumerate_period_range(start_period, end_period)


def filter_rows_for_summary(rows: list[dict[str, Any]]) -> bool:
    if not rows:
        return False
    keys = set(rows[0].keys())
    return "Partner Net Revenue Share" in keys or "Revenue Owed" in keys or "Monthly Minimum Revenue" in keys


def partner_period_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        key = f'{row.get("partner", "")}|{row.get("period", "")}'
        counts[key] = counts.get(key, 0) + 1
    return counts


DATE_RANGE_PATTERN = re.compile(r"(\d{4}-\d{2}-\d{2})\s+to\s+(\d{4}-\d{2}-\d{2})")


def parse_iso_day(value: Any) -> str | None:
    if isinstance(value, date):
        return value.isoformat()
    raw = str(value or "").strip()
    if not raw:
        return None
    candidate = raw[:10]
    try:
        return date.fromisoformat(candidate).isoformat()
    except ValueError:
        return None


def period_bounds(period: str | None) -> tuple[str | None, str | None]:
    raw = str(period or "").strip()
    if not raw or len(raw) < 7:
        return None, None
    try:
        year = int(raw[:4])
        month = int(raw[5:7])
        period_start = date(year, month, 1)
        if month == 12:
            period_end = date(year, 12, 31)
        else:
            period_end = date.fromordinal(date(year, month + 1, 1).toordinal() - 1)
        return period_start.isoformat(), period_end.isoformat()
    except ValueError:
        return None, None


def infer_looker_data_coverage(
    period: str | None,
    sections: dict[str, list[dict[str, Any]]] | None,
    detail_rows: list[dict[str, Any]] | None,
    context_update: dict[str, Any] | None,
    source_metadata: dict[str, Any] | None = None,
    source_rows: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    collected_dates: list[str] = []

    for row in detail_rows or []:
        for key, value in row.items():
            key_text = str(key).lower()
            if "date" not in key_text and "time" not in key_text:
                continue
            iso_day = parse_iso_day(value)
            if iso_day:
                collected_dates.append(iso_day)

    for rows in (sections or {}).values():
        for row in rows or []:
            for key, value in row.items():
                key_text = str(key).lower()
                if "date" not in key_text and "time" not in key_text:
                    continue
                iso_day = parse_iso_day(value)
                if iso_day:
                    collected_dates.append(iso_day)

    for row in source_rows or []:
        for key, value in row.items():
            key_text = str(key).lower()
            if "date" not in key_text and "time" not in key_text:
                continue
            iso_day = parse_iso_day(value)
            if iso_day:
                collected_dates.append(iso_day)

    offline_context = (context_update or {}).get("offlineContext")
    if isinstance(offline_context, dict):
        for collection in ("accountActivity", "settlementDays"):
            values = offline_context.get(collection) or {}
            if not isinstance(values, dict):
                continue
            for days in values.values():
                for day_value in days or []:
                    iso_day = parse_iso_day(day_value)
                    if iso_day:
                        collected_dates.append(iso_day)

    if collected_dates:
        return {
            "rangeStart": min(collected_dates),
            "currentThrough": max(collected_dates),
            "source": "detail_dates",
            "exact": True,
        }

    period_start, period_end = period_bounds(period)
    if period_start or period_end:
        return {
            "coverageMonth": str(period or ""),
            "source": "billing_period",
            "exact": False,
        }

    return {}


def parse_history_window_days(value: Any) -> int:
    try:
        days = int(value or 0)
    except (TypeError, ValueError):
        return 0
    return max(days, 0)


def history_window_cutoff(history_window_days: int) -> date | None:
    if history_window_days <= 0:
        return None
    return date.today() - timedelta(days=max(history_window_days - 1, 0))


def history_window_periods(history_window_days: int) -> list[str]:
    cutoff = history_window_cutoff(history_window_days)
    if cutoff is None:
        return []
    return enumerate_period_range(month_key(cutoff), month_key(date.today()))


def normalize_tracking_key(value: Any) -> str:
    return "".join(char.lower() for char in str(value or "") if char.isalnum())


def normalize_period_value(value: Any) -> str:
    return str(load_generator_module().month_key(value) or "").strip()


def row_anchor_period(row: dict[str, Any]) -> str:
    for key in ("period", "refundPeriod", "creditCompleteMonth", "billingMonth", "summaryMonth"):
        period = normalize_period_value(row.get(key))
        if period:
            return period
    for key, value in row.items():
        normalized_key = normalize_tracking_key(key)
        if "period" not in normalized_key and "month" not in normalized_key:
            continue
        period = normalize_period_value(value)
        if period:
            return period
    return ""


def row_anchor_date(row: dict[str, Any]) -> date | None:
    module = load_generator_module()
    best_score = -1
    best_value: date | None = None
    for key, value in row.items():
        normalized_key = normalize_tracking_key(key)
        score = -1
        if "refundcomplete" in normalized_key:
            score = 100
        elif "creditcomplete" in normalized_key:
            score = 90
        elif "completedat" in normalized_key or "completiondate" in normalized_key:
            score = 80
        elif "joindate" in normalized_key:
            score = 70
        elif "timecreated" in normalized_key:
            score = 60
        elif "submissiondate" in normalized_key:
            score = 50
        elif "date" in normalized_key or "time" in normalized_key:
            score = 10
        if score < 0:
            continue
        parsed = module.parse_dateish(value)
        if parsed and score > best_score:
            best_score = score
            best_value = parsed
    return best_value


def row_is_within_history_window(
    row: dict[str, Any],
    cutoff_date: date | None,
    cutoff_period: str | None,
) -> bool:
    if cutoff_date is None and not cutoff_period:
        return True
    anchor_date = row_anchor_date(row)
    if anchor_date is not None and cutoff_date is not None:
        return anchor_date >= cutoff_date
    anchor_period = row_anchor_period(row)
    if anchor_period and cutoff_period:
        return anchor_period >= cutoff_period
    return True


def filter_rows_to_history_window(rows: list[dict[str, Any]], history_window_days: int) -> list[dict[str, Any]]:
    if history_window_days <= 0:
        return list(rows or [])
    cutoff = history_window_cutoff(history_window_days)
    cutoff_period = month_key(cutoff) if cutoff else ""
    return [
        dict(row)
        for row in (rows or [])
        if isinstance(row, dict) and row_is_within_history_window(row, cutoff, cutoff_period)
    ]


def trim_lookup_to_history_window(lookup: dict[str, Any], history_window_days: int) -> dict[str, Any]:
    if history_window_days <= 0:
        return dict(lookup or {})
    cutoff = history_window_cutoff(history_window_days)
    cutoff_period = month_key(cutoff) if cutoff else ""
    trimmed: dict[str, Any] = {}
    for key, value in (lookup or {}).items():
        period = normalize_period_value(value)
        if period and cutoff_period and period < cutoff_period:
            continue
        trimmed[str(key)] = value
    return trimmed


def apply_history_window_to_result(result: dict[str, Any], history_window_days: int) -> dict[str, Any]:
    history_window_days = parse_history_window_days(history_window_days)
    if history_window_days <= 0:
        result["historyWindowDays"] = 0
        result["windowPeriods"] = []
        return result

    sections = dict(result.get("sections") or {})
    detail_rows = list(result.get("detailRows") or [])
    context_update = dict(result.get("contextUpdate") or {})
    source_rows = list(result.get("sourceRows") or [])
    stats = dict(result.get("stats") or {})
    warnings = list(result.get("warnings") or [])

    filtered_sections = {
        section: filter_rows_to_history_window(rows or [], history_window_days)
        for section, rows in sections.items()
    }
    filtered_detail_rows = filter_rows_to_history_window(detail_rows, history_window_days)
    filtered_source_rows = filter_rows_to_history_window(source_rows, history_window_days)

    if isinstance(context_update.get("stampliFxShareRows"), list):
        context_update["stampliFxShareRows"] = filter_rows_to_history_window(context_update.get("stampliFxShareRows") or [], history_window_days)
    if isinstance(context_update.get("stampliFxReversalRows"), list):
        context_update["stampliFxReversalRows"] = filter_rows_to_history_window(context_update.get("stampliFxReversalRows") or [], history_window_days)
    if isinstance(context_update.get("stampliCreditCompleteLookup"), dict):
        context_update["stampliCreditCompleteLookup"] = trim_lookup_to_history_window(context_update.get("stampliCreditCompleteLookup") or {}, history_window_days)

    section_counts_before = dict(stats.get("sectionCounts") or {})
    detail_count_before = len(detail_rows)
    section_counts_after = {section: len(rows) for section, rows in filtered_sections.items()}
    detail_count_after = len(filtered_detail_rows)
    if section_counts_before != section_counts_after or detail_count_before != detail_count_after:
        warnings.append(
            f"Applied rolling {history_window_days}-day import window before save. "
            f"Section rows changed from {sum(section_counts_before.values())} to {sum(section_counts_after.values())} "
            f"and detail rows from {detail_count_before} to {detail_count_after}."
        )

    window_periods = history_window_periods(history_window_days)
    result.update({
        "sections": filtered_sections,
        "detailRows": filtered_detail_rows,
        "contextUpdate": context_update,
        "sourceRows": filtered_source_rows,
        "warnings": warnings,
        "historyWindowDays": history_window_days,
        "windowPeriods": window_periods,
        "dataCoverage": infer_looker_data_coverage(
            str(result.get("period") or ""),
            filtered_sections,
            filtered_detail_rows,
            context_update,
            dict(result.get("sourceMetadata") or {}),
            source_rows=filtered_source_rows,
        ),
        "stats": {
            **stats,
            "historyWindowDays": history_window_days,
            "sectionCounts": section_counts_after,
            "detailCounts": partner_period_counts(filtered_detail_rows),
            "detailRowsPersisted": detail_count_after,
        },
    })
    return result


def normalize_number(value: Any) -> int | float | None:
    if value in (None, "", False):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if abs(number - round(number)) < 1e-9:
        return int(round(number))
    return round(number, 2)


def json_safe_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: json_safe_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [json_safe_value(item) for item in value]
    return value


def compact_metric_map(values: dict[str, Any]) -> dict[str, Any]:
    compacted: dict[str, Any] = {}
    for key, value in values.items():
        if key in {"partner", "period"}:
            continue
        if key == "rows":
            compacted[key] = int(value or 0)
            continue
        if value not in (None, 0, 0.0):
            compacted[key] = value
    if "rows" not in compacted:
        compacted["rows"] = int(values.get("rows") or 0)
    return compacted


def summarize_section_groups(rows: list[dict[str, Any]], section: str) -> dict[tuple[str, str], dict[str, Any]]:
    fields = SECTION_CHANGE_FIELDS.get(section, ())
    grouped: dict[tuple[str, str], dict[str, Any]] = defaultdict(lambda: {"rows": 0})
    for row in rows or []:
        partner = str(row.get("partnerGroup") or row.get("partner") or "").strip()
        period = str(row.get("period") or row.get("refundPeriod") or row.get("creditCompleteMonth") or "").strip()
        if not partner and not period:
            continue
        key = (partner, period)
        entry = grouped[key]
        entry["partner"] = partner
        entry["period"] = period
        entry["rows"] = int(entry.get("rows") or 0) + 1
        for field in fields:
            number = normalize_number(row.get(field))
            if number is None:
                continue
            entry[field] = normalize_number((entry.get(field) or 0) + number)
    return dict(grouped)


def build_section_replace_predicate(
    section: str,
    file_type: str,
    period: str,
    incoming_rows: list[dict[str, Any]],
):
    rows = incoming_rows or []
    target_periods = {
        str(
            row.get("period")
            or row.get("refundPeriod")
            or row.get("creditCompleteMonth")
            or ""
        ).strip()
        for row in rows
        if str(
            row.get("period")
            or row.get("refundPeriod")
            or row.get("creditCompleteMonth")
            or ""
        ).strip()
    }
    if period:
        target_periods.add(period)

    def period_matches(row: dict[str, Any]) -> bool:
        row_period = str(
            row.get("period")
            or row.get("refundPeriod")
            or row.get("creditCompleteMonth")
            or ""
        ).strip()
        if not target_periods:
            return False
        return row_period in target_periods

    if section == "ltxn":
        if file_type == "partner_offline_billing":
            return lambda row: period_matches(row) and not row.get("revenueBasis") and not row.get("directInvoiceSource")
        if file_type == "all_stampli_credit_complete":
            return lambda row: period_matches(row) and row.get("directInvoiceSource") == "stampli_credit_complete_billing"
        if file_type in {"partner_rev_share_v2", "partner_revenue_share", "revenue_share_report"}:
            return lambda row: period_matches(row) and bool(row.get("revenueBasis"))
        return lambda row: period_matches(row)

    if section == "lrev":
        return lambda row: period_matches(row)

    if section == "lva":
        partners = {row.get("partner") for row in rows if row.get("partner")}
        return lambda row: period_matches(row) and (not partners or row.get("partner") in partners)

    if section == "lrs":
        if file_type == "partner_revenue_summary":
            return lambda row: period_matches(row) and revenue_source_key(row) == "billing_summary"
        partner_source_pairs = {
            (row.get("partner"), revenue_source_key(row))
            for row in rows
            if row.get("partner")
        }
        return lambda row: period_matches(row) and (
            not partner_source_pairs
            or (row.get("partner"), revenue_source_key(row)) in partner_source_pairs
        )

    if section == "lfxp":
        partners = {row.get("partner") for row in rows if row.get("partner")}
        return lambda row: period_matches(row) and (not partners or row.get("partner") in partners)

    return lambda row: False


def build_section_change_summary(
    before_snapshot: dict[str, Any],
    after_snapshot: dict[str, Any],
    section: str,
    file_type: str,
    period: str,
    incoming_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    predicate = build_section_replace_predicate(section, file_type, period, incoming_rows)
    before_rows = [row for row in (before_snapshot.get(section) or []) if predicate(row)]
    after_rows = [row for row in (after_snapshot.get(section) or []) if predicate(row)]
    before_groups = summarize_section_groups(before_rows, section)
    after_groups = summarize_section_groups(after_rows, section)
    changed_groups: list[dict[str, Any]] = []
    for key in sorted(set(before_groups) | set(after_groups), key=lambda item: (item[0].lower(), item[1])):
        before_values = before_groups.get(key, {"partner": key[0], "period": key[1], "rows": 0})
        after_values = after_groups.get(key, {"partner": key[0], "period": key[1], "rows": 0})
        if compact_metric_map(before_values) == compact_metric_map(after_values):
            continue
        delta: dict[str, Any] = {}
        for field in {"rows", *SECTION_CHANGE_FIELDS.get(section, ())}:
            before_number = normalize_number(before_values.get(field) or 0) or 0
            after_number = normalize_number(after_values.get(field) or 0) or 0
            if before_number == after_number:
                continue
            delta[field] = normalize_number(after_number - before_number)
        changed_groups.append({
            "partner": key[0],
            "period": key[1],
            "before": compact_metric_map(before_values),
            "after": compact_metric_map(after_values),
            "delta": delta,
        })
    return {
        "section": section,
        "label": SECTION_CHANGE_LABELS.get(section, section),
        "changedGroupCount": len(changed_groups),
        "changedGroups": changed_groups,
    }


def build_looker_import_change_summary(
    before_snapshot: dict[str, Any],
    after_snapshot: dict[str, Any],
    result: dict[str, Any],
) -> dict[str, Any]:
    section_summaries = [
        build_section_change_summary(
            before_snapshot,
            after_snapshot,
            section,
            str(result.get("fileType") or ""),
            str(result.get("period") or ""),
            rows or [],
        )
        for section, rows in (result.get("sections") or {}).items()
    ]
    section_summaries = [summary for summary in section_summaries if summary.get("changedGroupCount")]
    changed_partner_periods = {
        (group.get("partner", ""), group.get("period", ""))
        for summary in section_summaries
        for group in (summary.get("changedGroups") or [])
    }
    changed_partners = sorted({partner for partner, _ in changed_partner_periods if partner})
    changed_periods = sorted({period for _, period in changed_partner_periods if period})
    return {
        "totalChangedGroups": len(changed_partner_periods),
        "partnerCount": len(changed_partners),
        "periodCount": len(changed_periods),
        "changedPartners": changed_partners,
        "changedPeriods": changed_periods,
        "sections": section_summaries,
    }


def aggregate_run_change_summary(files: list[dict[str, Any]]) -> dict[str, Any]:
    changed_partner_periods: set[tuple[str, str]] = set()
    changed_partners: set[str] = set()
    changed_periods: set[str] = set()
    changed_file_types: set[str] = set()
    for file_record in files or []:
        change_summary = file_record.get("changeSummary") or {}
        if not change_summary.get("totalChangedGroups"):
            continue
        changed_file_types.add(str(file_record.get("fileType") or ""))
        for section in change_summary.get("sections") or []:
            for group in section.get("changedGroups") or []:
                partner = str(group.get("partner") or "")
                period = str(group.get("period") or "")
                changed_partner_periods.add((partner, period))
                if partner:
                    changed_partners.add(partner)
                if period:
                    changed_periods.add(period)
    return {
        "totalChangedGroups": len(changed_partner_periods),
        "partnerCount": len(changed_partners),
        "periodCount": len(changed_periods),
        "changedFileCount": len(changed_file_types),
        "changedPartners": sorted(changed_partners),
        "changedPeriods": sorted(changed_periods),
    }


def file_type_uses_full_history(file_type: str) -> bool:
    return file_type in FULL_HISTORY_FILE_TYPES


def parse_looker_file(payload: dict[str, Any]) -> dict[str, Any]:
    file_type = str(payload.get("fileType") or "").strip()
    period = str(payload.get("period") or "").strip()
    history_window_days = parse_history_window_days(payload.get("historyWindowDays"))
    selected_period = period or None
    effective_period = None if file_type_uses_full_history(file_type) else selected_period
    context = payload.get("context") or {}
    if file_type not in LOOKER_FILE_TYPES:
        raise ValueError("Unsupported Looker file type.")

    module = load_generator_module()
    raw_bytes, suffix = decode_upload_bytes(payload, file_type)
    path = build_temp_path(file_type, suffix)
    path.write_bytes(raw_bytes)

    sections: dict[str, list[dict[str, Any]]] = {}
    detail_rows: list[dict[str, Any]] = []
    warnings: list[str] = []
    context_update: dict[str, Any] = {}
    source_rows: list[dict[str, Any]] = []
    stats: dict[str, Any] = {"fileType": file_type, "period": period, "fileLabel": LOOKER_FILE_TYPES[file_type]["label"]}

    if file_type == "partner_offline_billing":
        ltxn, meta, account_activity, settlement_days, periods_seen, parsed_detail_rows = module.build_offline_transactions(path, effective_period)
        sections["ltxn"] = ltxn
        detail_rows = parsed_detail_rows
        context_update["offlineContext"] = serialize_offline_context(account_activity, settlement_days)
        stats["periodsSeen"] = periods_seen
        stats["paymentIdsProcessed"] = meta.get("paymentIdsProcessed", 0)
        stats["paymentIdsImported"] = meta.get("paymentIdsImported", 0)
        if meta.get("unmatchedPaymentIds"):
            warnings.append(f'{meta["unmatchedPaymentIds"]} payment IDs could not be matched to a partner.')

    elif file_type == "partner_offline_billing_reversals":
        registered_rows = context_registered_account_rows(context)
        account_partner_lookup: dict[str, str] = {}
        for row in registered_rows:
            account_id = str(row.get("Account Id") or row.get("ACCOUNT_ID") or "").strip()
            partner = str(
                row.get("Partner Name")
                or row.get("Partner Group Source")
                or row.get("Partner Group With Bank")
                or ""
            ).strip()
            if account_id and partner:
                account_partner_lookup[account_id] = partner
        lrev, meta, periods_seen, parsed_detail_rows = module.build_offline_reversals(
            path,
            effective_period,
            account_partner_lookup=account_partner_lookup,
        )
        sections["lrev"] = lrev
        detail_rows = parsed_detail_rows
        stats["periodsSeen"] = periods_seen
        stats["reversalRows"] = len(lrev)
        if meta.get("unmatchedExamples"):
            warnings.append("Some reversal rows could not be matched to a partner.")

    elif file_type in {"all_registered_accounts", "all_registered_accounts_offline", "all_registered_accounts_rev_share", "vba_accounts"}:
        rows = module.read_table(path)
        source_rows = rows
        offline_context = context.get("offlineContext") or {}
        account_activity, settlement_days = deserialize_offline_context(offline_context)
        existing_registered_rows = context_registered_account_rows(context)
        normalized_rows = module.normalize_registered_account_rows(
            rows,
            prefer_time_created=(file_type == "vba_accounts"),
        )
        merged_registered_rows = module.merge_registered_account_rows(existing_registered_rows, normalized_rows)
        context_update["registeredAccountRows"] = json_safe_value(merged_registered_rows)
        if not account_activity and not settlement_days:
            warnings.append("Partner Offline Billing context was not supplied, so dormant accounts and settlement counts may be incomplete.")
        target_periods = derive_virtual_account_periods(module, merged_registered_rows, account_activity, settlement_days, "" if effective_period is None else period)
        lva = module.build_virtual_account_usage(merged_registered_rows, account_activity, settlement_days, target_periods)
        sections["lva"] = lva if effective_period is None else [row for row in lva if row.get("period") == period]
        stats["registeredRows"] = len(rows)
        stats["registeredAccountsLoaded"] = len(merged_registered_rows)

    elif file_type in {"vba_transactions", "vba_transactions_cc", "vba_transactions_citi"}:
        rows = module.read_table(path)
        source_rows = rows
        offline_context = context.get("offlineContext") or {}
        account_activity, settlement_days = deserialize_offline_context(offline_context)
        vba_activity = module.build_vba_transaction_activity(rows)
        merged_activity = merge_account_activity(account_activity, vba_activity)
        context_update["offlineContext"] = serialize_offline_context(merged_activity, settlement_days)
        registered_rows = context_registered_account_rows(context)
        if not registered_rows:
            warnings.append("Registered account looks have not been imported yet, so VBA dormancy could not be recalculated.")
        else:
            target_periods = derive_virtual_account_periods(module, registered_rows, merged_activity, settlement_days, "" if effective_period is None else period)
            lva = module.build_virtual_account_usage(registered_rows, merged_activity, settlement_days, target_periods)
            sections["lva"] = lva if effective_period is None else [row for row in lva if row.get("period") == period]
        stats["vbaTransactionRows"] = len(rows)

    elif file_type in {"partner_rev_share_v2", "partner_revenue_share", "revenue_share_report"}:
        rows = module.read_table(path)
        source_rows = rows
        ltxn, parsed_detail_rows = module.build_revenue_detail_transactions(rows, effective_period)
        sections["ltxn"] = ltxn
        detail_rows = parsed_detail_rows
        if filter_rows_for_summary(rows):
            sections["lrs"] = module.build_revenue_share_summary(
                rows,
                effective_period,
                allow_billing_month_fallback=(file_type != "revenue_share_report"),
            )
        stats["revenueRows"] = len(rows)

    elif file_type in {"partner_revenue_reversal", "rev_share_reversals"}:
        rows = module.read_table(path)
        source_rows = rows
        sections["lrs"] = module.build_revenue_reversal_summary(rows, effective_period)
        stats["reversalRows"] = len(rows)

    elif file_type == "partner_revenue_summary":
        rows = module.read_table(path)
        source_rows = rows
        sections["lrs"] = module.build_revenue_share_summary(rows, effective_period)
        stats["summaryRows"] = len(rows)

    elif file_type == "all_stampli_credit_complete":
        ltxn, periods_seen, parsed_detail_rows, meta = module.build_stampli_direct_billing(path, None, None, effective_period)
        sections["ltxn"] = ltxn
        detail_rows = parsed_detail_rows
        context_update["stampliCreditCompleteLookup"] = module.build_stampli_credit_complete_lookup(path)
        stats["periodsSeen"] = periods_seen
        stats["paymentIdsImported"] = meta.get("paymentIdsImported", 0)

    elif file_type in {"stampli_fx_revenue_share", "stampli_fx_revenue_reversal"}:
        rows = module.read_table(path)
        source_rows = rows
        share_rows = context.get("stampliFxShareRows") or []
        reversal_rows = context.get("stampliFxReversalRows") or []
        if file_type == "stampli_fx_revenue_share":
            share_rows = rows
            context_update["stampliFxShareRows"] = json_safe_value(rows)
        else:
            reversal_rows = rows
            context_update["stampliFxReversalRows"] = json_safe_value(rows)
        credit_complete_lookup = context.get("stampliCreditCompleteLookup") or {}
        has_bucketing_date = any(
            row.get("Credit Complete Date") not in (None, "")
            or row.get("Transaction Lookup Dates Credit Complete Timestamp Time") not in (None, "")
            or row.get("Transaction Lookup Dates Credit Complete Timestamp Date") not in (None, "")
            for row in share_rows
        )
        if not credit_complete_lookup and not has_bucketing_date:
            warnings.append("All Stampli Credit Complete has not been imported in this session yet, so FX rows cannot be bucketed to a billing month accurately.")
        lfxp, parsed_detail_rows = module.build_stampli_fx_partner_payouts(
            [],
            [],
            share_rows=share_rows,
            reversal_rows=reversal_rows,
            period=effective_period,
            credit_complete_lookup=credit_complete_lookup,
        )
        sections["lfxp"] = lfxp if effective_period is None else [row for row in lfxp if row.get("period") == period]
        detail_rows = parsed_detail_rows
        stats["shareRowsLoaded"] = len(share_rows)
        stats["reversalRowsLoaded"] = len(reversal_rows)

    else:
        raise ValueError("That Looker file type is not wired yet.")

    result = {
        "fileType": file_type,
        "fileLabel": LOOKER_FILE_TYPES[file_type]["label"],
        "period": period,
        "sections": sections,
        "detailRows": detail_rows,
        "contextUpdate": context_update,
        "warnings": warnings,
        "dataCoverage": infer_looker_data_coverage(effective_period, sections, detail_rows, context_update, source_rows=source_rows),
        "historyWindowDays": history_window_days,
        "sourceRows": source_rows,
        "stats": {
            **stats,
            "historyWindowDays": history_window_days,
            "sectionCounts": {section: len(rows) for section, rows in sections.items()},
            "detailCounts": partner_period_counts(detail_rows),
        },
    }
    if history_window_days and file_type_uses_full_history(file_type):
        result = apply_history_window_to_result(result, history_window_days)
    return result


def ensure_row_ids(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for row in rows or []:
        if row.get("id"):
            output.append(dict(row))
            continue
        output.append({"id": uuid.uuid4().hex[:6], **row})
    return output


def replace_rows(
    existing_rows: list[dict[str, Any]],
    incoming_rows: list[dict[str, Any]],
    should_remove,
) -> list[dict[str, Any]]:
    preserved = [row for row in (existing_rows or []) if not should_remove(row)]
    return [*preserved, *ensure_row_ids(incoming_rows or [])]


def revenue_source_key(row: dict[str, Any]) -> str:
    return str(row.get("revenueSource") or "summary")


def apply_looker_section_update(
    snapshot: dict[str, Any],
    section: str,
    file_type: str,
    period: str,
    incoming_rows: list[dict[str, Any]],
) -> None:
    rows = incoming_rows or []
    predicate = build_section_replace_predicate(section, file_type, period, rows)
    if section in {"ltxn", "lrev", "lva", "lrs", "lfxp"}:
        snapshot[section] = replace_rows(snapshot.get(section, []), rows, predicate)


def merge_looker_import_context(snapshot: dict[str, Any], context_update: dict[str, Any] | None) -> None:
    if not isinstance(context_update, dict) or not context_update:
        return
    snapshot["lookerImportContext"] = {
        **(snapshot.get("lookerImportContext") or {}),
        **context_update,
    }


def merge_looker_detail_overrides(snapshot: dict[str, Any], detail_rows: list[dict[str, Any]] | None, period: str) -> None:
    rows = detail_rows or []
    if not rows:
        return
    incoming_sources = {
        row.get("detailSource") or row.get("detailCategory") or "uploaded_looker_detail"
        for row in rows
    }
    existing_rows = snapshot.get("lookerImportedDetailRows") or []
    if not period and len(rows) > MAX_PERSISTED_DETAIL_ROWS_FOR_ALL_TIME_IMPORT:
        snapshot["lookerImportedDetailRows"] = [
            row
            for row in existing_rows
            if (row.get("detailSource") or row.get("detailCategory") or "uploaded_looker_detail") not in incoming_sources
        ]
        return
    target_periods = {
        str(row.get("period") or "").strip()
        for row in rows
        if str(row.get("period") or "").strip()
    }
    if period:
        target_periods.add(period)
    retained_existing_rows = [
        row
        for row in existing_rows
        if not (
            str(row.get("period") or "").strip() in target_periods
            and (row.get("detailSource") or row.get("detailCategory") or "uploaded_looker_detail") in incoming_sources
        )
    ]
    if len(rows) > MAX_PERSISTED_DETAIL_ROWS_PER_IMPORT:
        snapshot["lookerImportedDetailRows"] = compact_persisted_detail_rows(retained_existing_rows)
        return
    snapshot["lookerImportedDetailRows"] = compact_persisted_detail_rows([
        *retained_existing_rows,
        *rows,
    ])


def compact_persisted_detail_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if len(rows) <= MAX_PERSISTED_DETAIL_ROWS_TOTAL:
        return rows
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        key = (
            str(row.get("period") or "").strip(),
            str(row.get("detailSource") or row.get("detailCategory") or "uploaded_looker_detail").strip(),
        )
        grouped[key].append(row)
    ordered_keys = sorted(grouped.keys(), reverse=True)
    kept: list[dict[str, Any]] = []
    for key in ordered_keys:
        group = grouped[key]
        if kept and len(kept) + len(group) > MAX_PERSISTED_DETAIL_ROWS_TOTAL:
            continue
        if not kept and len(group) > MAX_PERSISTED_DETAIL_ROWS_TOTAL:
            kept.extend(group[:MAX_PERSISTED_DETAIL_ROWS_TOTAL])
            break
        kept.extend(group)
        if len(kept) >= MAX_PERSISTED_DETAIL_ROWS_TOTAL:
            break
    return kept


def rederive_virtual_account_section(snapshot: dict[str, Any], period: str) -> None:
    module = load_generator_module()
    context = snapshot.get("lookerImportContext") or {}
    registered_rows = context_registered_account_rows(context)
    if not registered_rows:
        return
    offline_context = context.get("offlineContext") or {}
    if not isinstance(offline_context, dict):
        offline_context = {}
    account_activity, settlement_days = deserialize_offline_context(offline_context)
    lva_rows = module.build_virtual_account_usage(registered_rows, account_activity, settlement_days, [period])
    period_rows = [row for row in lva_rows if row.get("period") == period]
    snapshot["lva"] = replace_rows(
        snapshot.get("lva", []),
        period_rows,
        lambda row: row.get("period") == period,
    )


def rederive_all_virtual_account_sections(snapshot: dict[str, Any]) -> None:
    module = load_generator_module()
    context = snapshot.get("lookerImportContext") or {}
    registered_rows = context_registered_account_rows(context)
    if not registered_rows:
        return
    offline_context = context.get("offlineContext") or {}
    if not isinstance(offline_context, dict):
        offline_context = {}
    account_activity, settlement_days = deserialize_offline_context(offline_context)
    periods = derive_virtual_account_periods(module, registered_rows, account_activity, settlement_days, "")
    snapshot["lva"] = module.build_virtual_account_usage(registered_rows, account_activity, settlement_days, periods)


def rederive_virtual_account_sections_for_periods(snapshot: dict[str, Any], periods: list[str]) -> None:
    target_periods = [period for period in periods if str(period or "").strip()]
    if not target_periods:
        return
    module = load_generator_module()
    context = snapshot.get("lookerImportContext") or {}
    registered_rows = context_registered_account_rows(context)
    if not registered_rows:
        return
    offline_context = context.get("offlineContext") or {}
    if not isinstance(offline_context, dict):
        offline_context = {}
    account_activity, settlement_days = deserialize_offline_context(offline_context)
    lva_rows = module.build_virtual_account_usage(registered_rows, account_activity, settlement_days, target_periods)
    snapshot["lva"] = replace_rows(
        snapshot.get("lva", []),
        [row for row in lva_rows if row.get("period") in target_periods],
        lambda row: row.get("period") in target_periods,
    )


def rederive_stampli_fx_section(snapshot: dict[str, Any], period: str) -> None:
    module = load_generator_module()
    context = snapshot.get("lookerImportContext") or {}
    share_rows = context.get("stampliFxShareRows") or []
    reversal_rows = context.get("stampliFxReversalRows") or []
    if not share_rows and not reversal_rows:
        return
    credit_complete_lookup = context.get("stampliCreditCompleteLookup") or {}
    lfxp_rows, detail_rows = module.build_stampli_fx_partner_payouts(
        [],
        [period],
        share_rows=share_rows,
        reversal_rows=reversal_rows,
        period=period,
        credit_complete_lookup=credit_complete_lookup,
    )
    period_rows = [row for row in lfxp_rows if row.get("period") == period]
    snapshot["lfxp"] = replace_rows(
        snapshot.get("lfxp", []),
        period_rows,
        lambda row: row.get("period") == period,
    )
    merge_looker_detail_overrides(snapshot, detail_rows, period)


def rederive_all_stampli_fx_sections(snapshot: dict[str, Any]) -> None:
    module = load_generator_module()
    context = snapshot.get("lookerImportContext") or {}
    share_rows = context.get("stampliFxShareRows") or []
    reversal_rows = context.get("stampliFxReversalRows") or []
    if not share_rows and not reversal_rows:
        return
    credit_complete_lookup = context.get("stampliCreditCompleteLookup") or {}
    lfxp_rows, detail_rows = module.build_stampli_fx_partner_payouts(
        [],
        [],
        share_rows=share_rows,
        reversal_rows=reversal_rows,
        period=None,
        credit_complete_lookup=credit_complete_lookup,
    )
    snapshot["lfxp"] = lfxp_rows
    merge_looker_detail_overrides(snapshot, detail_rows, "")


def rederive_stampli_fx_sections_for_periods(snapshot: dict[str, Any], periods: list[str]) -> None:
    target_periods = [period for period in periods if str(period or "").strip()]
    if not target_periods:
        return
    module = load_generator_module()
    context = snapshot.get("lookerImportContext") or {}
    share_rows = context.get("stampliFxShareRows") or []
    reversal_rows = context.get("stampliFxReversalRows") or []
    if not share_rows and not reversal_rows:
        return
    credit_complete_lookup = context.get("stampliCreditCompleteLookup") or {}
    lfxp_rows, detail_rows = module.build_stampli_fx_partner_payouts(
        [],
        target_periods,
        share_rows=share_rows,
        reversal_rows=reversal_rows,
        period=None,
        credit_complete_lookup=credit_complete_lookup,
    )
    snapshot["lfxp"] = replace_rows(
        snapshot.get("lfxp", []),
        [row for row in lfxp_rows if row.get("period") in target_periods],
        lambda row: row.get("period") in target_periods,
    )
    merge_looker_detail_overrides(
        snapshot,
        [row for row in detail_rows if str(row.get("period") or "").strip() in set(target_periods)],
        "",
    )


def apply_looker_import_result(snapshot: dict[str, Any], result: dict[str, Any]) -> dict[str, Any]:
    updated_snapshot = dict(snapshot or {})
    warnings = list(result.get("warnings") or [])
    history_window_days = parse_history_window_days(result.get("historyWindowDays"))
    window_periods = [str(period).strip() for period in (result.get("windowPeriods") or []) if str(period).strip()]
    for section, rows in (result.get("sections") or {}).items():
        predicate = build_section_replace_predicate(
            section,
            str(result.get("fileType") or ""),
            str(result.get("period") or ""),
            rows or [],
        )
        existing_rows = [row for row in (updated_snapshot.get(section) or []) if predicate(row)]
        if not rows and existing_rows:
            warnings.append(
                f"{SECTION_CHANGE_LABELS.get(section, section)} import returned 0 rows for {result.get('period')}, so {len(existing_rows)} existing stored row(s) were preserved."
            )
            continue
        apply_looker_section_update(
            updated_snapshot,
            section,
            str(result.get("fileType") or ""),
            str(result.get("period") or ""),
            rows or [],
        )
    merge_looker_import_context(updated_snapshot, result.get("contextUpdate"))
    merge_looker_detail_overrides(
        updated_snapshot,
        result.get("detailRows") or [],
        str(result.get("period") or ""),
    )
    period = str(result.get("period") or "")
    file_type = str(result.get("fileType") or "")
    if period and file_type in LVA_CONTEXT_FILE_TYPES:
        if history_window_days and window_periods:
            rederive_virtual_account_sections_for_periods(updated_snapshot, window_periods)
        elif file_type_uses_full_history(file_type):
            rederive_all_virtual_account_sections(updated_snapshot)
        else:
            rederive_virtual_account_section(updated_snapshot, period)
    if period and file_type in STAMPLI_FX_CONTEXT_FILE_TYPES:
        if history_window_days and window_periods:
            rederive_stampli_fx_sections_for_periods(updated_snapshot, window_periods)
        elif file_type_uses_full_history(file_type):
            rederive_all_stampli_fx_sections(updated_snapshot)
        else:
            rederive_stampli_fx_section(updated_snapshot, period)
    result["warnings"] = warnings
    return updated_snapshot


def update_looker_import_audit(
    snapshot: dict[str, Any],
    result: dict[str, Any],
    run_id: str,
    saved_at: str,
    source: str = "server",
) -> None:
    audit = snapshot.get("lookerImportAudit")
    if not isinstance(audit, dict):
        audit = {}

    by_file_type = dict(audit.get("byFileType") or {})
    record = {
        "fileType": str(result.get("fileType") or ""),
        "fileLabel": str(result.get("fileLabel") or ""),
        "period": str(result.get("period") or ""),
        "savedAt": saved_at,
        "source": source,
        "warnings": list(result.get("warnings") or []),
        "sectionCounts": dict((result.get("stats") or {}).get("sectionCounts") or {}),
        "stats": dict(result.get("stats") or {}),
        "changeSummary": dict(result.get("changeSummary") or {}),
        "sourceMetadata": dict(result.get("sourceMetadata") or {}),
        "dataCoverage": dict(result.get("dataCoverage") or {}) or infer_looker_data_coverage(
            str(result.get("period") or ""),
            dict(result.get("sections") or {}),
            list(result.get("detailRows") or []),
            dict(result.get("contextUpdate") or {}),
            dict(result.get("sourceMetadata") or {}),
        ),
    }
    by_file_type[record["fileType"]] = record

    latest_run = audit.get("latestRun")
    if not isinstance(latest_run, dict) or str(latest_run.get("runId") or "") != run_id:
        latest_run = {
            "runId": run_id,
            "period": record["period"],
            "savedAt": saved_at,
            "source": source,
            "files": [],
        }

    files = [
        file_record
        for file_record in (latest_run.get("files") or [])
        if str(file_record.get("fileType") or "") != record["fileType"]
    ]
    files.append(record)
    order_index = {file_type: idx for idx, file_type in enumerate(LOOKER_IMPORT_ORDER)}
    files.sort(key=lambda file_record: (order_index.get(str(file_record.get("fileType") or ""), 999), str(file_record.get("fileLabel") or "").lower()))

    latest_run.update({
        "period": record["period"],
        "savedAt": saved_at,
        "source": source,
        "files": files,
        "changeSummary": aggregate_run_change_summary(files),
    })

    runs_seed = audit.get("runs") or []
    if not isinstance(runs_seed, list):
        runs_seed = []
    if audit.get("latestRun") and isinstance(audit.get("latestRun"), dict):
        legacy_latest = audit.get("latestRun")
        if not any(str(entry.get("runId") or "") == str(legacy_latest.get("runId") or "") for entry in runs_seed if isinstance(entry, dict)):
            runs_seed = [legacy_latest, *runs_seed]
    runs = [
        entry
        for entry in runs_seed
        if isinstance(entry, dict) and str(entry.get("runId") or "") != str(run_id)
    ]
    runs.append(latest_run)
    runs.sort(key=lambda entry: str(entry.get("savedAt") or ""), reverse=True)

    latest_run_by_channel = {"manual": None, "workflow": None}
    for entry in runs:
        run_id = str(entry.get("runId") or "").lower()
        source_name = str(entry.get("source") or "").lower()
        if source_name == "manual" or run_id.startswith("manual-"):
            channel = "manual"
        elif run_id.startswith("n8n-"):
            channel = "workflow"
        else:
            channel = None
        if channel and latest_run_by_channel[channel] is None:
            latest_run_by_channel[channel] = entry

    snapshot["lookerImportAudit"] = {
        "byFileType": by_file_type,
        "latestRun": latest_run,
        "runs": runs,
        "latestRunByChannel": latest_run_by_channel,
    }

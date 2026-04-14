from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import tempfile
import uuid
from datetime import date, datetime
from pathlib import Path

ROOT = Path("/Users/danielsinukoff/Documents/billing-workbook")
SERVER_DIR = ROOT / "server"
if str(ROOT / "tools") not in sys.path:
    sys.path.insert(0, str(ROOT / "tools"))
if str(SERVER_DIR) not in sys.path:
    sys.path.insert(0, str(SERVER_DIR))

from pull_looker_and_push import ReportSpec, fetch_report_bytes, load_config, login_to_looker, ordered_reports
from storage import SharedWorkspaceStore

PARTNER_ALIASES = {
    "remittances hub": "Remittanceshub",
    "remittanceshub": "Remittanceshub",
    "yeepay": "Yeepay",
    "yee pay": "Yeepay",
    "nsave": "Nsave",
    "graph finance": "Graph Finance",
    "oval tech(graph)": "Graph Finance",
    "altpaynet": "Altpay",
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Backfill all historical partner revenue summary rows from the direct Looker Look."
    )
    parser.add_argument("--config-file", required=True, help="Path to looker-direct-reports.json")
    parser.add_argument("--db-path", default=str(ROOT / "server" / "data" / "shared_workspace.db"), help="Shared workspace sqlite path")
    parser.add_argument("--file-type", default="partner_revenue_summary", help="Configured Looker fileType to backfill")
    parser.add_argument("--probe-period", default="2026-03", help="A valid billing month used only for Looker auth/filter plumbing")
    parser.add_argument("--report-timeout", type=int, default=600, help="Looker export timeout in seconds")
    return parser


def select_report(config: dict[str, object], file_type: str) -> ReportSpec:
    for report in ordered_reports(config.get("reports") or []):
        if report.file_type == file_type:
            return report
    raise SystemExit(f"Could not find fileType '{file_type}' in config.")


def ensure_ids(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    output: list[dict[str, object]] = []
    for row in rows:
        if row.get("id"):
            output.append(dict(row))
        else:
            output.append({"id": uuid.uuid4().hex[:6], **row})
    return output


def text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value).strip()


def month_key(value: object) -> str:
    if value is None or value == "":
        return ""
    if isinstance(value, datetime):
        return value.strftime("%Y-%m")
    if isinstance(value, date):
        return value.strftime("%Y-%m")
    raw = text(value)
    if not raw:
        return ""
    for fmt in ("%Y-%m", "%Y-%m-%d", "%b-%y", "%m/%d/%Y", "%m/%d/%y"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m")
        except ValueError:
            continue
    if len(raw) >= 7 and raw[4] == "-":
        return raw[:7]
    return raw


def money(value: object) -> float:
    if value in (None, ""):
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    raw = text(value).replace("$", "").replace(",", "")
    return float(raw) if raw else 0.0


def normalize_partner_name(raw_partner: object) -> str:
    cleaned = text(raw_partner)
    if not cleaned:
        return ""
    alias = PARTNER_ALIASES.get(cleaned.lower())
    if alias:
        return alias
    return cleaned


def normalize_revenue_partner(raw_partner: object) -> str:
    cleaned = text(raw_partner)
    if not cleaned:
        return ""
    return normalize_partner_name(cleaned.split("|")[0].strip())


def normalize_header(value: object) -> str:
    return "".join(char.lower() for char in text(value) if char.isalnum())


def row_value_by_patterns(row: dict[str, object], *patterns: str) -> object:
    normalized_patterns = tuple(pattern.lower() for pattern in patterns if pattern)
    best_value: object = None
    best_score = -1
    for key, value in row.items():
        normalized_key = normalize_header(key)
        for pattern in normalized_patterns:
            score = -1
            if normalized_key == pattern:
                score = 4
            elif normalized_key.endswith(pattern):
                score = 3
            elif f"{pattern}id" in normalized_key:
                score = 2
            elif pattern in normalized_key:
                score = 1
            if score > best_score:
                best_score = score
                best_value = value
    return best_value


def read_csv_rows(path: Path) -> list[dict[str, object]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [row for row in csv.DictReader(handle) if any(value not in (None, "") for value in row.values())]


def build_revenue_share_summary(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    output: list[dict[str, object]] = []
    for row in rows:
        month = month_key(row.get("Credit Complete Timestamp Month") or row.get("Credit Complete Timestamp Date"))
        if month:
            partner = normalize_revenue_partner(row.get("Partner Group Source") or row.get("Partner Group With Bank"))
            if not partner:
                continue
            output.append(
                {
                    "period": month,
                    "partner": partner,
                    "netRevenue": money(row.get("Net Revenue")),
                    "partnerRevenueShare": money(row.get("Partner Net Revenue Share")),
                    "revenueOwed": money(row.get("Revenue Owed")),
                    "monthlyMinimumRevenue": money(row.get("Monthly Minimum Revenue")),
                    "revenueSource": "summary",
                }
            )
            continue

        summary_month = month_key(row_value_by_patterns(row, "billingmonthmonth", "billingmonth", "billingmo"))
        if not summary_month:
            continue
        partner = normalize_revenue_partner(row_value_by_patterns(row, "partnername", "partner"))
        if not partner:
            continue
        total_amount_raw = row_value_by_patterns(row, "totalamountfee", "totalamount", "totalamo")
        if total_amount_raw in (None, ""):
            continue
        total_amount = abs(money(total_amount_raw))
        billing_type = text(row_value_by_patterns(row, "billingtype", "billingty")) or "Billing Summary"
        computation = text(row_value_by_patterns(row, "computationmemo", "computation", "computati", "memo"))
        normalized_context = f"{billing_type} {computation}".lower()
        direction = "pay" if any(
            token in normalized_context
            for token in ("revsharepayout", "revshare payout", "partner net revenue share", "we pay", "veem owes", "payout")
        ) else "charge"
        count = 0.0
        unit_amount = 0.0
        match = re.search(r"([-+]?[0-9][0-9,]*(?:\.[0-9]+)?)\s*\*\s*\$?([-+]?[0-9][0-9,]*(?:\.[0-9]+)?)", computation.replace("%", ""))
        if match:
            count = money(match.group(1))
            unit_amount = money(match.group(2))
        is_minimum_row = "minimum" in normalized_context
        output.append(
            {
                "period": summary_month,
                "partner": partner,
                "netRevenue": 0.0,
                "partnerRevenueShare": total_amount if direction == "pay" else 0.0,
                "revenueOwed": total_amount if direction == "charge" else 0.0,
                "monthlyMinimumRevenue": total_amount if direction == "charge" and is_minimum_row else 0.0,
                "revenueSource": "billing_summary",
                "summaryDirection": direction,
                "summaryBillingType": billing_type,
                "summaryLabel": billing_type.strip() or "Billing Summary",
                "summaryComputation": computation,
                "summaryCount": round(count, 2),
                "summaryUnitAmount": round(unit_amount, 6),
                "summaryLineAmount": round(total_amount, 2),
            }
        )
    return sorted(
        output,
        key=lambda row: (
            str(row["partner"]),
            str(row["period"]),
            str(row.get("summaryLabel") or row.get("revenueSource") or ""),
            str(row.get("summaryComputation") or ""),
        ),
    )


def main() -> int:
    args = build_parser().parse_args()
    config_path = Path(args.config_file).expanduser().resolve()
    config = load_config(config_path)
    report = select_report(config, args.file_type)
    base_url = str(config.get("baseUrl") or "").strip()
    api_version = str(config.get("apiVersion") or "4.0").strip()
    force_production = bool(config.get("forceProduction", True))
    client_id = str(config.get("clientID") or "").strip()
    client_secret = str(config.get("clientSecret") or "").strip()
    if not base_url or not client_id or not client_secret:
        raise SystemExit("Looker baseUrl, clientID, and clientSecret are required.")

    unfiltered_report = ReportSpec(
        file_type=report.file_type,
        file_name=report.file_name,
        dashboard_id=report.dashboard_id,
        report_name=report.report_name,
        tile_id=report.tile_id,
        look_id=report.look_id,
        period_filter_key=None,
        period_filter_mode=None,
    )

    access_token = login_to_looker(base_url, api_version, client_id, client_secret)
    file_bytes, source_metadata = fetch_report_bytes(
        base_url,
        api_version,
        access_token,
        unfiltered_report,
        force_production,
        args.probe_period,
        timeout=max(1, int(args.report_timeout)),
    )

    suffix = "." + report.file_name.rsplit(".", 1)[-1].lower()
    with tempfile.NamedTemporaryFile(prefix="direct_revenue_summary_", suffix=suffix, delete=False) as handle:
        handle.write(file_bytes)
        temp_path = Path(handle.name)

    rows = read_csv_rows(temp_path)
    lrs_rows = build_revenue_share_summary(rows)
    temp_path.unlink(missing_ok=True)

    store = SharedWorkspaceStore(Path(args.db_path))
    workspace = store.get_workspace()
    snapshot = dict(workspace.get("snapshot") or {})
    existing_lrs = snapshot.get("lrs") or []
    preserved_lrs = [row for row in existing_lrs if str(row.get("revenueSource") or "") != "billing_summary"]
    snapshot["lrs"] = [*preserved_lrs, *ensure_ids(lrs_rows)]
    snapshot["lookerImportAudit"] = {
        **(snapshot.get("lookerImportAudit") or {}),
        "historicalRevenueSummaryBackfill": {
            "savedFromLookId": str(report.look_id or ""),
            "sourceMetadata": source_metadata,
            "rowsImported": len(lrs_rows),
        },
    }
    saved_at = store.save_snapshot(snapshot)

    print(
        json.dumps(
            {
                "savedAt": saved_at,
                "rowsImported": len(lrs_rows),
                "periodsImported": sorted({str(row.get("period") or "") for row in lrs_rows if row.get("period")}),
                "sourceMetadata": source_metadata,
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())

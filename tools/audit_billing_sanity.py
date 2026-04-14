from __future__ import annotations

import argparse
import json
import re
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from server.invoice_engine import calculate_invoice_for_period
from tools.generate_looker_import import infer_partner, iter_table_rows, month_key, row_value_first


PRECOLLECTED_PATTERN = re.compile(
    r"Pre-collected revenue from transaction-time charges: \$([0-9,]+(?:\.[0-9]+)?)"
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Audit imported billing outputs for suspicious zero-generated-revenue minimums.")
    parser.add_argument(
        "--db-path",
        default="/Users/danielsinukoff/Documents/billing-workbook/server/data/shared_workspace.db",
        help="Path to the shared workbook SQLite database.",
    )
    parser.add_argument(
        "--offline-file",
        default="/Users/danielsinukoff/Documents/billing-workbook/Historical Partner Data/Partner Offline Billing 2026-04-13T0052.xlsx",
        help="Offline billing export used to cross-check source activity.",
    )
    parser.add_argument(
        "--periods",
        nargs="+",
        default=["2026-01", "2026-02", "2026-03", "2026-04"],
        help="Billing periods to audit.",
    )
    return parser


def load_snapshot(db_path: Path) -> dict[str, Any]:
    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute("select snapshot_json from workbook_snapshots").fetchone()
    finally:
        conn.close()
    if not row or not row[0]:
        raise RuntimeError("No shared workbook snapshot found.")
    return json.loads(row[0])


def parse_precollected_amount(notes: list[str]) -> float:
    for note in notes or []:
        match = PRECOLLECTED_PATTERN.search(str(note))
        if match:
            return float(match.group(1).replace(",", ""))
    return 0.0


def anomaly_candidates(snapshot: dict[str, Any], periods: list[str]) -> list[dict[str, Any]]:
    partners = sorted({row.get("partner") for row in snapshot.get("pBilling", []) if row.get("partner")})
    findings: list[dict[str, Any]] = []
    for partner in partners:
        for period in periods:
            invoice = calculate_invoice_for_period(snapshot, partner, period)
            lines = invoice.get("lines") or []
            minimum_line = next(
                (line for line in lines if line.get("cat") == "Minimum" and line.get("active")),
                None,
            )
            if not minimum_line:
                continue
            replaced_amount = round(
                sum(
                    float(line.get("amount") or 0)
                    for line in lines
                    if line.get("dir") == "charge" and line.get("active") is False
                ),
                2,
            )
            precollected = parse_precollected_amount(list(invoice.get("notes") or []))
            if replaced_amount == 0 and precollected == 0:
                findings.append(
                    {
                        "partner": partner,
                        "period": period,
                        "minimumAmount": round(float(minimum_line.get("amount") or 0), 2),
                        "notes": list(invoice.get("notes") or []),
                    }
                )
    return findings


def source_activity_by_partner_period(offline_file: Path, periods: list[str]) -> dict[tuple[str, str], int]:
    target_periods = set(periods)
    counts: dict[tuple[str, str], int] = defaultdict(int)
    for row in iter_table_rows(offline_file):
        credit_complete_value = row_value_first(
            row,
            "Credit Complete Date",
            "Credit Complete Month",
            "Transaction Lookup Dates Credit Complete Timestamp Time",
            patterns=(
                "creditcompletedate",
                "creditcompletemonth",
                "creditcompletetimestampdate",
                "creditcompletetimestampmonth",
                "creditcompletetimestamptime",
            ),
        )
        period = month_key(credit_complete_value)
        if period not in target_periods:
            continue
        partner = infer_partner(row)
        if not partner:
            continue
        counts[(partner, period)] += 1
    return counts


def main() -> int:
    args = build_parser().parse_args()
    snapshot = load_snapshot(Path(args.db_path).expanduser().resolve())
    periods = [str(period).strip() for period in args.periods if str(period).strip()]
    findings = anomaly_candidates(snapshot, periods)
    source_counts = source_activity_by_partner_period(Path(args.offline_file).expanduser().resolve(), periods)

    report: list[dict[str, Any]] = []
    for finding in findings:
        key = (finding["partner"], finding["period"])
        report.append(
            {
                **finding,
                "offlineSourceRowCount": int(source_counts.get(key, 0)),
                "sourceMismatch": bool(source_counts.get(key, 0)),
            }
        )

    report.sort(key=lambda item: (item["partner"].lower(), item["period"]))
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

from __future__ import annotations

import argparse
import base64
import json
import sys
from dataclasses import asdict
from pathlib import Path

from pull_looker_and_push import (
    ReportSpec,
    fetch_report_bytes,
    load_config,
    login_to_looker,
    ordered_reports,
    post_import,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Backfill direct Looker revenue-summary rows across a month range."
    )
    parser.add_argument("--config-file", required=True, help="Path to looker-direct-reports.json")
    parser.add_argument("--billing-api-base-url", required=True, help="Billing API base URL")
    parser.add_argument("--billing-api-token", default="", help="Optional billing API bearer token")
    parser.add_argument("--start-period", required=True, help="Start billing month in YYYY-MM")
    parser.add_argument("--end-period", required=True, help="End billing month in YYYY-MM")
    parser.add_argument("--file-type", default="partner_revenue_summary", help="Configured Looker fileType to backfill")
    parser.add_argument("--report-timeout", type=int, default=600, help="Per-report timeout in seconds")
    return parser


def enumerate_periods(start_period: str, end_period: str) -> list[str]:
    start_year, start_month = map(int, start_period.split("-"))
    end_year, end_month = map(int, end_period.split("-"))
    periods: list[str] = []
    year, month = start_year, start_month
    while (year, month) <= (end_year, end_month):
        periods.append(f"{year:04d}-{month:02d}")
        month += 1
        if month > 12:
            month = 1
            year += 1
    return periods


def find_report(config_path: Path, file_type: str) -> tuple[dict, ReportSpec]:
    config = load_config(config_path)
    for report in ordered_reports(config.get("reports") or []):
        if report.file_type == file_type:
            return config, report
    raise SystemExit(f"Could not find fileType '{file_type}' in {config_path}")


def main() -> int:
    args = build_parser().parse_args()
    config_path = Path(args.config_file).expanduser().resolve()
    config, spec = find_report(config_path, args.file_type)

    base_url = str(config.get("baseUrl") or "").strip()
    api_version = str(config.get("apiVersion") or "4.0").strip()
    force_production = bool(config.get("forceProduction", True))
    client_id = str(config.get("clientID") or "").strip()
    client_secret = str(config.get("clientSecret") or "").strip()
    if not base_url or not client_id or not client_secret:
        raise SystemExit("Looker baseUrl, clientID, and clientSecret are required.")

    periods = enumerate_periods(args.start_period, args.end_period)
    access_token = login_to_looker(base_url, api_version, client_id, client_secret)

    summary: dict[str, object] = {
        "fileType": spec.file_type,
        "reportName": spec.report_name,
        "startPeriod": args.start_period,
        "endPeriod": args.end_period,
        "results": [],
    }

    for period in periods:
        result: dict[str, object] = {"period": period}
        try:
            file_bytes, source_metadata = fetch_report_bytes(
                base_url,
                api_version,
                access_token,
                spec,
                force_production,
                period,
                timeout=max(1, int(args.report_timeout)),
            )
            response = post_import(
                args.billing_api_base_url,
                args.billing_api_token,
                {
                    "fileType": spec.file_type,
                    "period": period,
                    "fileName": spec.file_name,
                    "fileBase64": base64.b64encode(file_bytes).decode("ascii"),
                    "sourceMetadata": source_metadata,
                },
            )
            result.update(
                {
                    "status": "imported",
                    "savedAt": response.get("savedAt"),
                    "warnings": response.get("warnings") or [],
                    "sectionCounts": (response.get("stats") or {}).get("sectionCounts") or {},
                    "sourceMetadata": source_metadata,
                }
            )
        except Exception as exc:  # noqa: BLE001
            result.update({"status": "error", "error": str(exc)})
        summary["results"].append(result)

    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())

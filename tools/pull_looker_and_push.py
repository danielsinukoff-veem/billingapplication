from __future__ import annotations

import argparse
import base64
import calendar
import json
import os
import re
import socket
import ssl
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from urllib import error, parse, request

try:
    import certifi  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    certifi = None


IMPORT_ORDER = [
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


@dataclass
class ReportSpec:
    file_type: str
    file_name: str
    dashboard_id: int | None
    report_name: str
    tile_id: int | None = None
    look_id: str | None = None
    period_filter_key: str | None = None
    period_filter_mode: str | None = None
    report_timeout: int | None = None
    history_window_days: int | None = None


def normalize(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip()).lower()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Pull configured Looker dashboard reports and push them into the billing workbook API.")
    parser.add_argument("--config-file", required=True, help="JSON config file with Looker report definitions.")
    parser.add_argument("--period", default=date.today().strftime("%Y-%m"), help="Billing period in YYYY-MM.")
    parser.add_argument("--billing-api-base-url", required=True, help="Billing API base URL, for example http://127.0.0.1:4174")
    parser.add_argument("--billing-api-token", default="", help="Optional bearer token for the billing API.")
    parser.add_argument("--looker-client-id", default="", help="Override Looker client ID. Falls back to config or env.")
    parser.add_argument("--looker-client-secret", default="", help="Override Looker client secret. Falls back to config or env.")
    parser.add_argument("--report-timeout", type=int, default=240, help="Per-report Looker export timeout in seconds, including async query polling.")
    parser.add_argument("--billing-api-timeout", type=int, default=1800, help="Timeout in seconds for the billing API save step after a report has been fetched.")
    parser.add_argument(
        "--history-window-days",
        type=int,
        default=0,
        help="Optional rolling window for imports. When set, the billing API trims imported rows to the last N days before saving.",
    )
    parser.add_argument("--run-id", default="", help="Optional shared workflow run ID so multiple fileType imports group into one logical run.")
    parser.add_argument(
        "--file-type",
        action="append",
        default=[],
        help="Optional configured fileType to import. Repeat to import multiple specific reports.",
    )
    parser.add_argument(
        "--debug-save-report-dir",
        default="",
        help="Optional directory to save fetched report bytes for debugging before posting to the billing API.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Resolve the report mapping without downloading or importing.")
    parser.add_argument("--fail-on-warnings", action="store_true", help="Exit non-zero if the billing API returns warnings.")
    return parser


def load_config(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def ordered_reports(report_defs: list[dict[str, Any]]) -> list[ReportSpec]:
    typed = [
        ReportSpec(
            file_type=str(item["fileType"]),
            file_name=str(item["fileName"]),
            dashboard_id=int(item["dashboardId"]) if item.get("dashboardId") not in (None, "", "null") else None,
            report_name=str(item.get("reportName") or item["fileName"]),
            tile_id=int(item["tileId"]) if item.get("tileId") not in (None, "", "null") else None,
            look_id=str(item["lookId"]) if item.get("lookId") not in (None, "", "null") else None,
            period_filter_key=str(item["periodFilterKey"]) if item.get("periodFilterKey") not in (None, "", "null") else None,
            period_filter_mode=str(item.get("periodFilterMode") or "") or None,
            report_timeout=int(item["reportTimeout"]) if item.get("reportTimeout") not in (None, "", "null") else None,
            history_window_days=int(item["historyWindowDays"]) if item.get("historyWindowDays") not in (None, "", "null") else None,
        )
        for item in report_defs
    ]
    order_index = {file_type: idx for idx, file_type in enumerate(IMPORT_ORDER)}
    return sorted(typed, key=lambda item: (order_index.get(item.file_type, 999), item.dashboard_id or 0, item.report_name.lower()))


def build_ssl_context() -> ssl.SSLContext:
    explicit_bundle = str(Path(__file__).resolve().parent.parent / "certs" / "veem-looker-ca.pem")
    env_bundle = str(Path(os.environ["LOOKER_CA_BUNDLE"]).expanduser()) if os.environ.get("LOOKER_CA_BUNDLE") else ""
    cafile = ""
    for candidate in (env_bundle, explicit_bundle):
        if candidate and Path(candidate).exists():
            cafile = candidate
            break
    if not cafile and certifi is not None:
        try:
            cafile = certifi.where()
        except Exception:
            cafile = ""
    return ssl.create_default_context(cafile=cafile or None)


SSL_CONTEXT = build_ssl_context()


def http_json(method: str, url: str, headers: dict[str, str] | None = None, body: bytes | None = None, timeout: int = 120) -> dict[str, Any] | list[Any]:
    req = request.Request(url, headers=headers or {}, data=body, method=method)
    try:
        with request.urlopen(req, timeout=timeout, context=SSL_CONTEXT if url.lower().startswith("https://") else None) as response:
            raw = response.read().decode("utf-8")
    except error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code} from {url}: {detail}") from exc
    except (error.URLError, TimeoutError, socket.timeout) as exc:
        raise RuntimeError(f"Could not reach {url}: {exc}") from exc
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Expected JSON from {url}, got: {raw[:500]}") from exc


def http_bytes(method: str, url: str, headers: dict[str, str] | None = None, timeout: int = 300) -> bytes:
    req = request.Request(url, headers=headers or {}, method=method)
    try:
        with request.urlopen(req, timeout=timeout, context=SSL_CONTEXT if url.lower().startswith("https://") else None) as response:
            return response.read()
    except error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code} from {url}: {detail}") from exc
    except (error.URLError, TimeoutError, socket.timeout) as exc:
        raise RuntimeError(f"Could not reach {url}: {exc}") from exc


def login_to_looker(base_url: str, api_version: str, client_id: str, client_secret: str) -> str:
    login_url = f"{base_url.rstrip('/')}/api/{api_version}/login"
    body = parse.urlencode({"client_id": client_id, "client_secret": client_secret}).encode("utf-8")
    payload = http_json("POST", login_url, headers={"Content-Type": "application/x-www-form-urlencoded", "Accept": "application/json"}, body=body)
    if not isinstance(payload, dict) or not payload.get("access_token"):
        raise RuntimeError("Looker login did not return an access token.")
    return str(payload["access_token"])


def build_looker_headers(access_token: str) -> dict[str, str]:
    return {
        "Accept": "application/json",
        "Authorization": f"token {access_token}",
    }


def dashboard_elements(base_url: str, api_version: str, access_token: str, dashboard_id: int, force_production: bool) -> list[dict[str, Any]]:
    params = {"apply_formatting": "false"}
    if force_production:
        params["force_production"] = "true"
    url = f"{base_url.rstrip('/')}/api/{api_version}/dashboards/{dashboard_id}/dashboard_elements?{parse.urlencode(params)}"
    payload = http_json("GET", url, headers=build_looker_headers(access_token))
    if not isinstance(payload, list):
        raise RuntimeError(f"Unexpected dashboard element payload for dashboard {dashboard_id}")
    return payload


def get_query(base_url: str, api_version: str, access_token: str, query_id: str) -> dict[str, Any]:
    encoded_query_id = parse.quote(str(query_id), safe="")
    payload = http_json(
        "GET",
        f"{base_url.rstrip('/')}/api/{api_version}/queries/{encoded_query_id}",
        headers=build_looker_headers(access_token),
    )
    if not isinstance(payload, dict):
        raise RuntimeError(f"Unexpected query payload for query {query_id}")
    return payload


def resolve_report_element(elements: list[dict[str, Any]], spec: ReportSpec) -> dict[str, Any]:
    if spec.tile_id is not None:
        for element in elements:
            if int(element.get("id") or 0) == spec.tile_id:
                return element
        raise RuntimeError(f"Could not find tileId {spec.tile_id} on dashboard {spec.dashboard_id} for {spec.report_name}")

    target = normalize(spec.report_name)
    matches = [
        element
        for element in elements
        if normalize(element.get("title")) == target
        or normalize((element.get("look") or {}).get("title")) == target
    ]
    if len(matches) == 1:
        return matches[0]
    if not matches:
        available = sorted({
            str(element.get("title") or (element.get("look") or {}).get("title") or "").strip()
            for element in elements
            if str(element.get("title") or (element.get("look") or {}).get("title") or "").strip()
        })
        raise RuntimeError(
            f"Could not find report '{spec.report_name}' on dashboard {spec.dashboard_id}. Available titles: {available}"
        )
    raise RuntimeError(f"Report title '{spec.report_name}' matched multiple dashboard elements on dashboard {spec.dashboard_id}")


def period_month_value(period: str) -> str:
    return period.replace("-", "/")


def period_date_range_value(period: str) -> str:
    year = int(period[:4])
    month = int(period[5:7])
    last_day = calendar.monthrange(year, month)[1]
    return f"{period}-01 to {period}-{last_day:02d}"


def build_period_filter_value(period: str, mode: str | None) -> str:
    normalized = (mode or "month").strip().lower()
    if normalized in {"month", "month_value"}:
        return period_month_value(period)
    if normalized in {"date_range", "month_range"}:
        return period_date_range_value(period)
    if normalized == "raw":
        return period
    raise RuntimeError(f"Unsupported Looker period filter mode: {mode}")


def create_filtered_query(
    base_url: str,
    api_version: str,
    access_token: str,
    base_query: dict[str, Any],
    filter_key: str,
    filter_value: str,
) -> str:
    return create_export_query(
        base_url=base_url,
        api_version=api_version,
        access_token=access_token,
        base_query=base_query,
        extra_filters={filter_key: filter_value},
    )


def create_export_query(
    base_url: str,
    api_version: str,
    access_token: str,
    base_query: dict[str, Any],
    extra_filters: dict[str, Any] | None = None,
) -> str:
    merged_filters = dict(base_query.get("filters") or {})
    if extra_filters:
        merged_filters.update(extra_filters)
    payload = {
        "model": base_query.get("model"),
        "view": base_query.get("view"),
        "fields": base_query.get("fields"),
        "filters": merged_filters,
        "filter_expression": base_query.get("filter_expression"),
        "sorts": base_query.get("sorts"),
        # Saved Looks often carry UI row limits like 500 or 5000. For billing
        # imports we want the full downloadable result set, so override the
        # query limit to Looker's "all results" sentinel value.
        "limit": "-1",
        "column_limit": base_query.get("column_limit"),
        "total": base_query.get("total"),
        "row_total": base_query.get("row_total"),
        "subtotals": base_query.get("subtotals"),
        "vis_config": base_query.get("vis_config"),
        "filter_config": None,
        "query_timezone": base_query.get("query_timezone"),
        "dynamic_fields": base_query.get("dynamic_fields"),
    }
    created = http_json(
        "POST",
        f"{base_url.rstrip('/')}/api/{api_version}/queries",
        headers={
            **build_looker_headers(access_token),
            "Content-Type": "application/json",
        },
        body=json.dumps(payload).encode("utf-8"),
        timeout=120,
    )
    if not isinstance(created, dict) or not created.get("id"):
        raise RuntimeError("Failed to create Looker export query.")
    return str(created["id"])


def create_query_task(
    base_url: str,
    api_version: str,
    access_token: str,
    query_id: str,
    result_format: str,
    force_production: bool,
) -> str:
    payload = {
        "query_id": str(query_id),
        "result_format": result_format,
        "source": "billing-workbook-direct-api",
    }
    if force_production:
        payload["force_production"] = True
    task = http_json(
        "POST",
        f"{base_url.rstrip('/')}/api/{api_version}/query_tasks",
        headers={
            **build_looker_headers(access_token),
            "Content-Type": "application/json",
        },
        body=json.dumps(payload).encode("utf-8"),
        timeout=120,
    )
    if not isinstance(task, dict) or not task.get("id"):
        raise RuntimeError(f"Failed to create Looker query task for query {query_id}")
    return str(task["id"])


def wait_for_query_task_results(
    base_url: str,
    api_version: str,
    access_token: str,
    task_id: str,
    timeout: int,
) -> bytes:
    url = f"{base_url.rstrip('/')}/api/{api_version}/query_tasks/{parse.quote(str(task_id), safe='')}/results"
    deadline = time.monotonic() + max(1, timeout)
    transient_404_count = 0
    while time.monotonic() < deadline:
        req = request.Request(
            url,
            headers={"Authorization": f"token {access_token}", "Accept": "*/*"},
            method="GET",
        )
        try:
            with request.urlopen(
                req,
                timeout=min(60, max(5, timeout)),
                context=SSL_CONTEXT if url.lower().startswith("https://") else None,
            ) as response:
                if response.status == 202:
                    time.sleep(2)
                    continue
                if response.status != 200:
                    raise RuntimeError(f"Unexpected Looker query task status {response.status} for task {task_id}")
                return response.read()
        except error.HTTPError as exc:
            if exc.code == 202:
                time.sleep(2)
                continue
            if exc.code == 404:
                transient_404_count += 1
                # Looker occasionally returns a temporary 404 before the async
                # result file is fully available. Treat that the same way we
                # treat 202 and keep polling until the timeout is reached.
                time.sleep(min(5, 1 + transient_404_count))
                continue
            detail = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"HTTP {exc.code} from {url}: {detail}") from exc
        except (error.URLError, TimeoutError, socket.timeout) as exc:
            raise RuntimeError(f"Could not reach {url}: {exc}") from exc
    raise RuntimeError(f"Timed out waiting for Looker query task {task_id} after {timeout} seconds")


def fetch_report_bytes(base_url: str, api_version: str, access_token: str, spec: ReportSpec, force_production: bool, period: str, timeout: int = 300) -> tuple[bytes, dict[str, Any]]:
    result_format = spec.file_name.rsplit(".", 1)[-1].lower()
    source_metadata = {
        "dashboardId": spec.dashboard_id,
        "requestedTileId": spec.tile_id,
        "reportName": spec.report_name,
        "fileName": spec.file_name,
        "resultFormat": result_format,
        "fetchedAt": datetime.now(timezone.utc).isoformat(),
    }

    resolved_query_id: str | None = None

    if spec.look_id:
        source_metadata["lookId"] = str(spec.look_id)
        source_metadata["resolvedTitle"] = spec.report_name
        look = http_json(
            "GET",
            f"{base_url.rstrip('/')}/api/{api_version}/looks/{parse.quote(str(spec.look_id), safe='')}",
            headers=build_looker_headers(access_token),
            timeout=120,
        )
        if not isinstance(look, dict) or not look.get("query_id"):
            raise RuntimeError(f"Look {spec.look_id} did not return a query_id")
        resolved_query_id = str(look["query_id"])

    elif spec.dashboard_id is None:
        raise RuntimeError(f"Report '{spec.report_name}' is missing both lookId and dashboardId.")
    else:
        elements = dashboard_elements(base_url, api_version, access_token, spec.dashboard_id, force_production)
        element = resolve_report_element(elements, spec)
        source_metadata["resolvedTileId"] = element.get("id")
        source_metadata["resolvedTitle"] = element.get("title") or (element.get("look") or {}).get("title") or spec.report_name

        query_id = element.get("query_id") or (element.get("look") or {}).get("query_id")
        if query_id:
            resolved_query_id = str(query_id)
        else:
            look_id = (element.get("look") or {}).get("id")
            if not look_id:
                raise RuntimeError(f"Dashboard element for '{spec.report_name}' has neither query_id nor look.id")
            source_metadata["lookId"] = str(look_id)
            look = http_json(
                "GET",
                f"{base_url.rstrip('/')}/api/{api_version}/looks/{parse.quote(str(look_id), safe='')}",
                headers=build_looker_headers(access_token),
                timeout=120,
            )
            if not isinstance(look, dict) or not look.get("query_id"):
                raise RuntimeError(f"Look {look_id} did not return a query_id")
            resolved_query_id = str(look["query_id"])

    if not resolved_query_id:
        raise RuntimeError(f"Could not resolve a Looker query id for '{spec.report_name}'")

    source_metadata["baseQueryId"] = str(resolved_query_id)
    base_query = get_query(base_url, api_version, access_token, resolved_query_id)
    effective_query_id = resolved_query_id
    source_metadata["baseQueryLimit"] = base_query.get("limit")
    source_metadata["baseQueryColumnLimit"] = base_query.get("column_limit")
    if spec.period_filter_key:
        filter_value = build_period_filter_value(period, spec.period_filter_mode)
        effective_query_id = create_filtered_query(
            base_url,
            api_version,
            access_token,
            base_query,
            spec.period_filter_key,
            filter_value,
        )
        source_metadata["periodFilterKey"] = spec.period_filter_key
        source_metadata["periodFilterMode"] = spec.period_filter_mode or "month"
        source_metadata["periodFilterValue"] = filter_value
        source_metadata["exportQueryLimit"] = "-1"
    else:
        effective_query_id = create_export_query(
            base_url=base_url,
            api_version=api_version,
            access_token=access_token,
            base_query=base_query,
        )
        source_metadata["exportQueryLimit"] = "-1"
    source_metadata["queryId"] = str(effective_query_id)
    task_id = create_query_task(base_url, api_version, access_token, effective_query_id, result_format, force_production)
    source_metadata["queryTaskId"] = task_id
    file_bytes = wait_for_query_task_results(base_url, api_version, access_token, task_id, timeout)
    source_metadata["byteCount"] = len(file_bytes)
    return file_bytes, source_metadata


def post_import(
    billing_api_base_url: str,
    billing_api_token: str,
    payload: dict[str, Any],
    timeout: int,
) -> dict[str, Any]:
    body = json.dumps(payload).encode("utf-8")
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    if billing_api_token:
        headers["Authorization"] = f"Bearer {billing_api_token}"
    req = request.Request(
        billing_api_base_url.rstrip("/") + "/api/looker/import-and-save",
        data=body,
        headers=headers,
        method="POST",
    )
    try:
        with request.urlopen(req, timeout=max(1, timeout)) as response:
            raw = response.read().decode("utf-8")
    except error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code}: {detail}") from exc
    except error.URLError as exc:
        raise RuntimeError(f"Could not reach billing API: {exc}") from exc
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Billing API returned non-JSON output: {raw}") from exc


def main() -> int:
    args = build_parser().parse_args()
    config_path = Path(args.config_file).expanduser().resolve()
    config = load_config(config_path)

    base_url = str(config.get("baseUrl") or "").strip()
    api_version = str(config.get("apiVersion") or "4.0").strip()
    force_production = bool(config.get("forceProduction", True))
    client_id = args.looker_client_id or str(config.get("clientID") or "") or ""
    client_secret = args.looker_client_secret or str(config.get("clientSecret") or "") or ""
    if not base_url or not client_id or not client_secret:
        raise SystemExit("Looker baseUrl, clientID, and clientSecret are required.")

    reports = ordered_reports(config.get("reports") or [])
    requested_file_types = [normalize(file_type) for file_type in args.file_type or [] if str(file_type).strip()]
    if requested_file_types:
        reports = [report for report in reports if normalize(report.file_type) in requested_file_types]
    if not reports:
        if requested_file_types:
            raise SystemExit(f"No configured Looker reports matched --file-type values: {args.file_type}")
        raise SystemExit("No Looker reports are configured.")

    access_token = login_to_looker(base_url, api_version, client_id, client_secret)

    warnings_found = False
    errors_found = False
    run_id = str(args.run_id or "").strip() or f"n8n-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:8]}"
    summary: dict[str, Any] = {
        "configFile": str(config_path),
        "period": args.period,
        "runId": run_id,
        "baseUrl": base_url,
        "apiVersion": api_version,
        "forceProduction": force_production,
        "dryRun": bool(args.dry_run),
        "expectedReportCount": len(reports),
        "reports": [],
    }

    for spec in reports:
        started_at = time.monotonic()
        entry: dict[str, Any] = {
            "fileType": spec.file_type,
            "fileName": spec.file_name,
            "dashboardId": spec.dashboard_id,
            "tileId": spec.tile_id,
            "lookId": spec.look_id,
            "reportName": spec.report_name,
        }
        if args.dry_run:
            entry["status"] = "configured"
            summary["reports"].append(entry)
            continue

        try:
            print(f"[looker-import] starting {spec.file_type} ({spec.report_name})", file=sys.stderr, flush=True)
            effective_timeout = max(1, int(spec.report_timeout or args.report_timeout))
            file_bytes, source_metadata = fetch_report_bytes(
                base_url,
                api_version,
                access_token,
                spec,
                force_production,
                args.period,
                timeout=effective_timeout,
            )
            if args.debug_save_report_dir:
                debug_dir = Path(args.debug_save_report_dir).expanduser().resolve()
                debug_dir.mkdir(parents=True, exist_ok=True)
                debug_path = debug_dir / f"{spec.file_type}.{spec.file_name.rsplit('.', 1)[-1].lower()}"
                debug_path.write_bytes(file_bytes)
                entry["debugSavedTo"] = str(debug_path)
            response = post_import(
                args.billing_api_base_url,
                args.billing_api_token,
                {
                    "fileType": spec.file_type,
                    "period": args.period,
                    "historyWindowDays": max(0, int(spec.history_window_days or args.history_window_days or 0)),
                    "runId": run_id,
                    "fileName": spec.file_name,
                    "fileBase64": base64.b64encode(file_bytes).decode("ascii"),
                    "sourceMetadata": source_metadata,
                },
                timeout=int(args.billing_api_timeout),
            )
            warnings = response.get("warnings") or []
            if warnings:
                warnings_found = True
            entry.update(
                {
                    "status": "imported",
                    "savedAt": response.get("savedAt"),
                    "warnings": warnings,
                    "stats": response.get("stats") or {},
                    "changeSummary": response.get("changeSummary") or {},
                    "sourceMetadata": source_metadata,
                    "sectionKeys": sorted((response.get("sections") or {}).keys()),
                    "byteCount": len(file_bytes),
                }
            )
            print(
                f"[looker-import] completed {spec.file_type} in {time.monotonic() - started_at:.1f}s",
                file=sys.stderr,
                flush=True,
            )
        except Exception as exc:
            errors_found = True
            warnings_found = True
            entry.update(
                {
                    "status": "error",
                    "error": str(exc),
                }
            )
            print(
                f"[looker-import] failed {spec.file_type} after {time.monotonic() - started_at:.1f}s: {exc}",
                file=sys.stderr,
                flush=True,
            )
        summary["reports"].append(entry)

    summary["importedReportCount"] = len([entry for entry in summary["reports"] if entry.get("status") == "imported"])
    summary["errorReportCount"] = len([entry for entry in summary["reports"] if entry.get("status") == "error"])
    print(json.dumps(summary, indent=2))
    if errors_found:
        return 2
    if warnings_found and args.fail_on_warnings:
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""Archival reference Looker sync helper.

The production Looker import flow should be driven by n8n and the AWS-hosted
API/workflow stack.
"""

from __future__ import annotations

import base64
import importlib.util
import sys
import uuid
from datetime import date, datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any

if __package__ in (None, ""):
    sys.path.append(str(Path(__file__).resolve().parents[1]))
    from server.looker_update import (  # type: ignore
        apply_looker_import_result,
        build_looker_import_change_summary,
        parse_looker_file,
        update_looker_import_audit,
    )
else:
    from .looker_update import (
        apply_looker_import_result,
        build_looker_import_change_summary,
        parse_looker_file,
        update_looker_import_audit,
    )


ROOT_DIR = Path(__file__).resolve().parents[1]
DIRECT_PULLER_PATH = ROOT_DIR / "tools" / "pull_looker_and_push.py"
DEFAULT_CONFIG_PATH = ROOT_DIR / "docs" / "looker-direct-reports.json"


@lru_cache(maxsize=1)
def load_direct_puller_module():
    spec = importlib.util.spec_from_file_location("billing_looker_direct_puller", DIRECT_PULLER_PATH)
    if not spec or not spec.loader:
        raise RuntimeError("Could not load the direct Looker puller module.")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)  # type: ignore[attr-defined]
    return module


def _today_period() -> str:
    return date.today().strftime("%Y-%m")


def _load_config_from_payload(payload: dict[str, Any]) -> tuple[dict[str, Any], str]:
    if isinstance(payload.get("config"), dict):
        return dict(payload.get("config") or {}), "<inline>"
    config_path = Path(str(payload.get("configFile") or DEFAULT_CONFIG_PATH)).expanduser().resolve()
    module = load_direct_puller_module()
    return module.load_config(config_path), str(config_path)


def _normalize_requested_file_types(payload: dict[str, Any]) -> set[str]:
    values = payload.get("reportFileTypes") or []
    if not isinstance(values, list):
        raise ValueError("reportFileTypes must be an array of Looker file types when provided.")
    requested = {str(value).strip() for value in values if str(value).strip()}
    return requested


def run_direct_looker_sync(
    snapshot: dict[str, Any],
    payload: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], bool]:
    module = load_direct_puller_module()
    config, config_source = _load_config_from_payload(payload)

    period = str(payload.get("period") or _today_period()).strip()
    if not period:
        raise ValueError("A billing period is required.")

    requested_file_types = _normalize_requested_file_types(payload)
    reports = module.ordered_reports(config.get("reports") or [])
    if requested_file_types:
        configured = {report.file_type for report in reports}
        missing = sorted(requested_file_types - configured)
        if missing:
            raise ValueError(f"Requested reportFileTypes are not configured: {missing}")
        reports = [report for report in reports if report.file_type in requested_file_types]
    if not reports:
        raise ValueError("No Looker reports are configured for direct sync.")

    base_url = str(payload.get("lookerBaseUrl") or config.get("baseUrl") or "").strip()
    api_version = str(payload.get("lookerApiVersion") or config.get("apiVersion") or "4.0").strip()
    force_production = bool(payload["forceProduction"]) if "forceProduction" in payload else bool(config.get("forceProduction", True))
    client_id = str(payload.get("lookerClientId") or config.get("clientID") or "").strip()
    client_secret = str(payload.get("lookerClientSecret") or config.get("clientSecret") or "").strip()
    dry_run = bool(payload.get("dryRun"))
    report_timeout = max(1, int(payload.get("reportTimeout") or 600))
    run_id = str(payload.get("runId") or f"direct-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:8]}")

    if not dry_run and (not base_url or not client_id or not client_secret):
        raise ValueError("Looker baseUrl, clientID, and clientSecret are required for direct sync.")

    access_token = None
    if not dry_run:
        access_token = module.login_to_looker(base_url, api_version, client_id, client_secret)

    working_snapshot = dict(snapshot or {})
    imported_any = False
    warnings_found = False
    errors_found = False
    summary: dict[str, Any] = {
        "configFile": config_source,
        "period": period,
        "runId": run_id,
        "baseUrl": base_url,
        "apiVersion": api_version,
        "forceProduction": force_production,
        "dryRun": dry_run,
        "reports": [],
    }

    for spec in reports:
        entry: dict[str, Any] = {
            "fileType": spec.file_type,
            "fileName": spec.file_name,
            "dashboardId": spec.dashboard_id,
            "tileId": spec.tile_id,
            "lookId": spec.look_id,
            "reportName": spec.report_name,
            "historyWindowDays": int(payload.get("historyWindowDays") or spec.history_window_days or 0),
        }
        if dry_run:
            entry["status"] = "configured"
            summary["reports"].append(entry)
            continue

        try:
            file_bytes, source_metadata = module.fetch_report_bytes(
                base_url,
                api_version,
                access_token,
                spec,
                force_production,
                period,
                timeout=report_timeout,
            )
            parse_payload = {
                "fileType": spec.file_type,
                "period": period,
                "historyWindowDays": int(payload.get("historyWindowDays") or spec.history_window_days or 0),
                "fileName": spec.file_name,
                "fileBase64": base64.b64encode(file_bytes).decode("ascii"),
                "sourceMetadata": source_metadata,
                "context": dict(working_snapshot.get("lookerImportContext") or {}),
            }
            result = parse_looker_file(parse_payload)
            next_snapshot = apply_looker_import_result(working_snapshot, result)
            result["changeSummary"] = build_looker_import_change_summary(working_snapshot, next_snapshot, result)
            if source_metadata:
                result["sourceMetadata"] = source_metadata
            audit_stamp = datetime.now(timezone.utc).isoformat()
            next_snapshot["_saved"] = audit_stamp
            update_looker_import_audit(next_snapshot, result, run_id, audit_stamp, "server-direct")
            working_snapshot = next_snapshot
            imported_any = True
            warnings = list(result.get("warnings") or [])
            if warnings:
                warnings_found = True
            entry.update(
                {
                    "status": "imported",
                    "savedAt": audit_stamp,
                    "warnings": warnings,
                    "stats": result.get("stats") or {},
                    "changeSummary": result.get("changeSummary") or {},
                    "sourceMetadata": source_metadata,
                    "sectionKeys": sorted((result.get("sections") or {}).keys()),
                    "byteCount": len(file_bytes),
                }
            )
        except Exception as error:
            errors_found = True
            warnings_found = True
            entry.update({"status": "error", "error": str(error)})

        summary["reports"].append(entry)

    summary["warningCount"] = sum(len(report.get("warnings") or []) for report in summary["reports"])
    summary["errorCount"] = sum(1 for report in summary["reports"] if report.get("status") == "error")
    summary["importedCount"] = sum(1 for report in summary["reports"] if report.get("status") == "imported")
    summary["configuredCount"] = len(summary["reports"])
    summary["hasWarnings"] = warnings_found
    summary["hasErrors"] = errors_found
    summary["source"] = "server-direct"

    return working_snapshot, summary, imported_any

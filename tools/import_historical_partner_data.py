from __future__ import annotations

import argparse
import base64
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from server.looker_update import (
    LOOKER_IMPORT_ORDER,
    apply_looker_import_result,
    parse_looker_file,
    update_looker_import_audit,
)
from server.storage import SharedWorkspaceStore


FILE_TYPE_PREFIXES = {
    "partner_offline_billing": ["Partner Offline Billing "],
    "partner_offline_billing_reversals": ["Partner Offline Billing (Reversals) "],
    "all_registered_accounts_offline": ["All Registered Accounts - Offline Billing "],
    "all_registered_accounts_rev_share": ["All Registered Accounts - Rev Share "],
    "vba_accounts": ["VBA ACCOUNTS "],
    "vba_transactions": ["CC_Citi VBA Txns "],
    "revenue_share_report": ["Revenue Share Report "],
    "rev_share_reversals": ["Rev Share Reversals "],
    "stampli_fx_revenue_share": ["Stampli FX Revenue Share "],
    "stampli_fx_revenue_reversal": ["Stampli FX Revenue Reversal "],
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Import all historical Looker baseline files into the shared workbook.")
    parser.add_argument(
        "--input-dir",
        default="/Users/danielsinukoff/Documents/billing-workbook/Historical Partner Data",
        help="Directory containing historical Looker export files.",
    )
    parser.add_argument(
        "--db-path",
        default="/Users/danielsinukoff/Documents/billing-workbook/server/data/shared_workspace.db",
        help="Shared workbook SQLite database path.",
    )
    parser.add_argument(
        "--run-id",
        default="",
        help="Optional run id for audit grouping.",
    )
    return parser


def find_file_type(file_name: str) -> str:
    ordered = sorted(
        ((file_type, prefix) for file_type, prefixes in FILE_TYPE_PREFIXES.items() for prefix in prefixes),
        key=lambda item: len(item[1]),
        reverse=True,
    )
    for file_type, prefix in ordered:
        if file_name.startswith(prefix):
            return file_type
    raise RuntimeError(f"Unmapped historical file: {file_name}")


def collect_files(input_dir: Path) -> list[tuple[str, Path]]:
    found: dict[str, Path] = {}
    for path in sorted(input_dir.iterdir()):
        if not path.is_file():
            continue
        if path.name.startswith("."):
            continue
        if path.name.startswith("~$"):
            continue
        file_type = find_file_type(path.name)
        found[file_type] = path
    missing = [file_type for file_type in FILE_TYPE_PREFIXES if file_type not in found]
    if missing:
        raise RuntimeError(f"Historical input directory is missing files for: {', '.join(missing)}")
    order_index = {file_type: idx for idx, file_type in enumerate(LOOKER_IMPORT_ORDER)}
    return sorted(found.items(), key=lambda item: order_index.get(item[0], 999))


def build_payload(file_type: str, path: Path, run_id: str) -> dict[str, Any]:
    return {
        "fileType": file_type,
        "fileName": path.name,
        "fileBase64": base64.b64encode(path.read_bytes()).decode("ascii"),
        "period": "",
        "runId": run_id,
        "sourceMetadata": {
            "historicalBaseline": True,
            "sourceFileName": path.name,
            "sourcePath": str(path),
        },
    }


def main() -> int:
    args = build_parser().parse_args()
    input_dir = Path(args.input_dir).expanduser().resolve()
    db_path = Path(args.db_path).expanduser().resolve()
    store = SharedWorkspaceStore(db_path)
    workspace = store.get_workspace()
    snapshot = workspace.get("snapshot")
    if not isinstance(snapshot, dict):
        raise RuntimeError("No shared workbook snapshot found.")

    run_id = str(args.run_id or "").strip() or f"manual-baseline-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    files = collect_files(input_dir)
    saved_at = datetime.now(timezone.utc).isoformat()
    results: list[dict[str, Any]] = []

    for file_type, path in files:
        payload = build_payload(file_type, path, run_id)
        result = parse_looker_file(payload)
        snapshot = apply_looker_import_result(snapshot, result)
        update_looker_import_audit(snapshot, result, run_id, saved_at, source="manual")
        results.append(
            {
                "fileType": file_type,
                "fileName": path.name,
                "sectionCounts": dict((result.get("stats") or {}).get("sectionCounts") or {}),
                "warnings": list(result.get("warnings") or []),
            }
        )

    snapshot["_saved"] = saved_at
    store.save_snapshot(snapshot)

    print(f"Imported {len(results)} historical files into the shared workbook.")
    for item in results:
        print(f"{item['fileType']}: {item['sectionCounts']}")
        for warning in item["warnings"]:
            print(f"  warning: {warning}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

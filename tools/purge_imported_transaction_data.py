from __future__ import annotations

import argparse
import json
import secrets
import sys
from datetime import datetime, timezone
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
SERVER_DIR = ROOT_DIR / "server"
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
if str(SERVER_DIR) not in sys.path:
    sys.path.insert(0, str(SERVER_DIR))

from storage import SharedWorkspaceStore  # noqa: E402


PURGE_LIST_KEYS = (
    "ltxn",
    "lrev",
    "lva",
    "lrs",
    "lfxp",
    "lookerImportedDetailRows",
)

PURGE_DICT_KEYS = (
    "lookerImportAudit",
    "lookerImportContext",
)

PURGE_OPTIONAL_KEYS = (
    "lookerImportedFiles",
    "lookerImportRuns",
)


def snapshot_counts(snapshot: dict[str, object]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for key in PURGE_LIST_KEYS:
        value = snapshot.get(key)
        counts[key] = len(value) if isinstance(value, list) else 0
    for key in PURGE_DICT_KEYS:
        value = snapshot.get(key)
        counts[key] = len(value) if isinstance(value, dict) else 0
    return counts


def build_access_log_entry(before: dict[str, int]) -> dict[str, object]:
    detail = (
        "Purged imported transaction data and Looker audit/context for clean cutover. "
        f"Cleared ltxn={before['ltxn']}, lrev={before['lrev']}, lva={before['lva']}, "
        f"lrs={before['lrs']}, lfxp={before['lfxp']}, detailRows={before['lookerImportedDetailRows']}."
    )
    return {
        "id": secrets.token_hex(3),
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "actorRole": "admin",
        "actorName": "VeemAdmin",
        "action": "purge_imported_transaction_data",
        "detail": detail,
        "tab": "data-upload",
        "category": "change",
        "section": "imported-data-cutover",
    }


def purge_snapshot(snapshot: dict[str, object]) -> tuple[dict[str, object], dict[str, int], dict[str, int]]:
    next_snapshot = json.loads(json.dumps(snapshot))
    before = snapshot_counts(next_snapshot)

    for key in PURGE_LIST_KEYS:
        next_snapshot[key] = []
    for key in PURGE_DICT_KEYS:
        next_snapshot[key] = {}
    for key in PURGE_OPTIONAL_KEYS:
        if key in next_snapshot:
            next_snapshot[key] = []

    access_logs = next_snapshot.get("accessLogs")
    if not isinstance(access_logs, list):
        access_logs = []
    access_logs.append(build_access_log_entry(before))
    next_snapshot["accessLogs"] = access_logs[-250:]

    version_value = next_snapshot.get("_version")
    if isinstance(version_value, int):
        next_snapshot["_version"] = version_value + 1
    else:
        next_snapshot["_version"] = 1
    next_snapshot["_saved"] = datetime.now(timezone.utc).isoformat()

    after = snapshot_counts(next_snapshot)
    return next_snapshot, before, after


def main() -> int:
    parser = argparse.ArgumentParser(description="Purge imported transaction data from the shared billing workbook while keeping contract/cost/config data intact.")
    parser.add_argument(
        "--db-path",
        default=str(ROOT_DIR / "server" / "data" / "shared_workspace.db"),
        help="Path to the shared workspace SQLite database.",
    )
    args = parser.parse_args()

    store = SharedWorkspaceStore(Path(args.db_path))
    workspace = store.get_workspace()
    snapshot = workspace.get("snapshot")
    if not isinstance(snapshot, dict):
        raise SystemExit("No shared workbook snapshot found.")

    next_snapshot, before, after = purge_snapshot(snapshot)
    saved_at = store.save_snapshot(next_snapshot)

    print(json.dumps({
        "savedAt": saved_at,
        "before": before,
        "after": after,
        "purgedOptionalKeys": list(PURGE_OPTIONAL_KEYS),
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

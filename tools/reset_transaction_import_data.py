from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    import sys

    ROOT = Path(__file__).resolve().parents[1]
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    from server.storage import SharedWorkspaceStore  # type: ignore
else:
    from ..server.storage import SharedWorkspaceStore


CLEAR_TO_EMPTY_LIST = {
    "ltxn",
    "lrev",
    "lrs",
    "lfxp",
    "lva",
    "lookerImportedDetailRows",
}

CLEAR_TO_EMPTY_DICT = {
    "lookerImportAudit",
    "lookerImportContext",
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Clear imported transaction/Looker data while preserving contract and cost data.")
    parser.add_argument(
        "--db-path",
        default=str(Path(__file__).resolve().parents[1] / "server" / "data" / "shared_workspace.db"),
        help="Path to the shared workspace sqlite database.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Preview what would be cleared without saving.")
    return parser


def snapshot_counts(snapshot: dict[str, Any]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for key in sorted(CLEAR_TO_EMPTY_LIST | CLEAR_TO_EMPTY_DICT):
        value = snapshot.get(key)
        if isinstance(value, list):
            counts[key] = len(value)
        elif isinstance(value, dict):
            counts[key] = len(value)
        else:
            counts[key] = 0
    return counts


def main() -> int:
    args = build_parser().parse_args()
    store = SharedWorkspaceStore(Path(args.db_path))
    workspace = store.get_workspace()
    snapshot = workspace.get("snapshot")
    if not isinstance(snapshot, dict):
        raise SystemExit("No shared workbook snapshot found.")

    before = snapshot_counts(snapshot)
    next_snapshot = dict(snapshot)
    for key in CLEAR_TO_EMPTY_LIST:
        next_snapshot[key] = []
    for key in CLEAR_TO_EMPTY_DICT:
        next_snapshot[key] = {}

    after = snapshot_counts(next_snapshot)
    saved_at = None
    if not args.dry_run:
        saved_at = store.save_snapshot(next_snapshot)

    print(json.dumps({
        "dryRun": bool(args.dry_run),
        "savedAt": saved_at,
        "cleared": {
            key: {
                "before": before[key],
                "after": after[key],
            }
            for key in sorted(before)
        },
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

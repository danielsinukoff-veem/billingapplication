#!/usr/bin/env python3
"""Export the SQLite shared-workspace snapshot to data/current-workbook.json,
applying the v36 migration in-process so the resulting file is the exact
artifact a deploy should upload to S3 (bucket key: data/current-workbook.json).

This is the bridge from local dev DB → S3 publish:

  SQLite (server/data/shared_workspace.db)
     │
     ▼  this script
  data/current-workbook.json  (post-migration, _saved bumped, untrusted rows purged)
     │
     ▼  AWS upload (separate step, requires bucket write creds)
  s3://veem-qa-billing-data/data/current-workbook.json
     │
     ▼  CloudFront (https://billing.qa-us-west-2.veem.com/data/current-workbook.json)
  client app

Usage:
  python3 tools/export-snapshot-for-deploy.py            # in-place rewrite of data/current-workbook.json
  python3 tools/export-snapshot-for-deploy.py --out path/to/file.json
  python3 tools/export-snapshot-for-deploy.py --no-migrate   # skip in-process migration (client will run it)
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB = ROOT / "server" / "data" / "shared_workspace.db"
DEFAULT_OUT = ROOT / "data" / "current-workbook.json"

UNTRUSTED_DIRECT_INVOICE_SOURCES = {
    "stampli_credit_complete_billing",
    "stampli_direct_billing",
    "stampli_usd_abroad_revenue",
    "stampli_usd_abroad_reversal",
}

TARGET_VERSION = 36


def load_snapshot_from_db(db_path: Path) -> dict:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "select snapshot_json, updated_at from workbook_snapshots"
    ).fetchone()
    if row is None:
        raise SystemExit(f"No snapshot in DB at {db_path}")
    snap = json.loads(row["snapshot_json"])
    print(f"Loaded snapshot: db_updated_at={row['updated_at']}, "
          f"_saved={snap.get('_saved')}, _version={snap.get('_version')}")
    return snap


def apply_migration(snap: dict) -> dict:
    """Mirror the JS migrateSnapshot for the parts that affect at-rest data:
    strip untrusted Stampli rows from ltxn, bump _version. Anything else
    that's strictly UI/render-only we leave to the client at load time."""
    ltxn = snap.get("ltxn") or []
    before = len(ltxn)
    snap["ltxn"] = [
        r for r in ltxn
        if r.get("directInvoiceSource") not in UNTRUSTED_DIRECT_INVOICE_SOURCES
    ]
    purged = before - len(snap["ltxn"])
    if purged:
        print(f"Purged {purged} untrusted Stampli ltxn rows")
    snap["_version"] = TARGET_VERSION
    return snap


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--no-migrate", action="store_true",
                        help="Skip the in-process migration; client will run it on load")
    parser.add_argument("--no-bump-saved", action="store_true",
                        help="Don't update the _saved timestamp")
    args = parser.parse_args()

    snap = load_snapshot_from_db(args.db)
    if not args.no_migrate:
        snap = apply_migration(snap)
    if not args.no_bump_saved:
        snap["_saved"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        print(f"Set _saved={snap['_saved']}")

    payload = {
        "workspace": {"label": "Veem Billing Workspace"},
        "user": {},
        "snapshot": snap,
    }

    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    size = args.out.stat().st_size
    print(f"Wrote {args.out} ({size:,} bytes)")
    print(f"Final: _version={snap.get('_version')}, _saved={snap.get('_saved')}")
    print(f"Counts: pBilling={len(snap.get('pBilling') or [])}, "
          f"ltxn={len(snap.get('ltxn') or [])}, "
          f"mins={len(snap.get('mins') or [])}, "
          f"impl={len(snap.get('impl') or [])}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

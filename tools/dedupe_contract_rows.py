from __future__ import annotations

import json
import sqlite3
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path


DB_PATH = Path("/Users/danielsinukoff/Documents/billing-workbook/server/data/shared_workspace.db")


SECTION_SIGNATURES = {
    "off": ["txnType", "speedFlag", "payerFunding", "payeeFunding", "payerCcy", "payeeCcy", "payerCountry", "payeeCountry", "processingMethod", "minAmt", "maxAmt", "fee"],
    "vol": ["txnType", "speedFlag", "payerFunding", "payeeFunding", "payeeCardType", "ccyGroup", "minVol", "maxVol", "rate", "note"],
    "fxRates": ["payerCorridor", "payerCcy", "payeeCorridor", "payeeCcy", "minTxnSize", "maxTxnSize", "minVol", "maxVol", "rate", "note"],
    "cap": ["productType", "capType", "amount"],
    "mins": ["minAmount", "minVol", "maxVol", "implFeeOffset"],
    "revf": ["payerFunding", "feePerReversal"],
    "plat": ["monthlyFee"],
    "impl": ["feeType", "feeAmount", "applyAgainstMin", "note"],
    "vaFees": ["feeType", "minAccounts", "maxAccounts", "discount", "feePerAccount", "note"],
    "surch": ["surchargeType", "rate", "minVol", "maxVol", "note"],
}


def normalize_date(value: object) -> str:
    text = str(value or "").strip()[:10]
    return text if len(text) == 10 else ""


def compare_dates(left: object, right: object) -> int:
    a = normalize_date(left)
    b = normalize_date(right)
    if a == b:
        return 0
    if not a:
        return -1
    if not b:
        return 1
    return 1 if a > b else -1


def row_signature(row: dict, fields: list[str]) -> tuple:
    return tuple((field, json.dumps(row.get(field), sort_keys=True)) for field in fields)


def dedupe_rows(rows: list[dict], signature_fields: list[str]) -> tuple[list[dict], int]:
    groups: dict[tuple, list[tuple[int, dict]]] = defaultdict(list)
    for index, row in enumerate(rows):
        key = (str(row.get("partner", "")).strip().lower(), row_signature(row, signature_fields))
        groups[key].append((index, row))

    cleaned: list[tuple[int, dict]] = []
    removed = 0
    for items in groups.values():
        if len(items) == 1:
            cleaned.append(items[0])
            continue
        removed += len(items) - 1
        chosen = sorted(
            items,
            key=lambda item: (
                compare_dates(item[1].get("startDate"), ""),
                normalize_date(item[1].get("startDate")),
                item[0],
            ),
            reverse=True,
        )[0]
        cleaned.append(chosen)

    cleaned.sort(key=lambda item: item[0])
    return [row for _, row in cleaned], removed


def main() -> int:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    row = cur.execute("SELECT id, snapshot_json FROM workbook_snapshots ORDER BY id DESC LIMIT 1").fetchone()
    if not row:
        raise SystemExit("No workbook snapshot found.")
    snapshot_id, snapshot_json = row
    snapshot = json.loads(snapshot_json)

    removed_total = 0
    section_summary: dict[str, int] = {}
    for section, fields in SECTION_SIGNATURES.items():
        cleaned, removed = dedupe_rows(snapshot.get(section, []), fields)
        snapshot[section] = cleaned
        if removed:
            section_summary[section] = removed
            removed_total += removed

    snapshot["_saved"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    cur.execute(
        "UPDATE workbook_snapshots SET snapshot_json = ?, updated_at = current_timestamp WHERE id = ?",
        (json.dumps(snapshot), snapshot_id),
    )
    conn.commit()
    conn.close()

    print(json.dumps({
        "snapshotId": snapshot_id,
        "rowsRemoved": removed_total,
        "sections": section_summary,
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

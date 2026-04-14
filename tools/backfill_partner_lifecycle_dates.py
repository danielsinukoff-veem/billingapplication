from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.contract_parse import parse_contract_text
from server.storage import SharedWorkspaceStore

DB_PATH = ROOT / "server" / "data" / "shared_workspace.db"
EXTRACTED_TEXT_DIR = ROOT / "reports" / "contract_audit" / "extracted_text"
REPORT_PATH = ROOT / "reports" / "contract_audit" / "partner_lifecycle_dates_report.json"

PARTNER_SOURCE = {
    "Altpay": "Altpay.txt",
    "Athena": "Athena.txt",
    "Blindpay": "Blindpay.txt",
    "Capi": "Capi.txt",
    "Cellpay": "Cellpay.txt",
    "Clearshift": "Clearshift.txt",
    "Everflow": "Everflow.txt",
    "Graph Finance": "Graph.txt",
    "Halorecruiting": "Halo_Recruiting.txt",
    "LianLian": "Lian_Lian.txt",
    "Maplewave": "Maplewave.txt",
    "Nomad": "Nomad.txt",
    "Nsave": "NSave_Addendum.txt",
    "Nuvion": "Nuvion.txt",
    "Oson": "Oson.txt",
    "Q2": "Q2.txt",
    "Repay": "RePay.txt",
    "Remittanceshub": "RemittancesHub.txt",
    "Shepherd": "Shepherd_Somewhere.txt",
    "Skydo": "Skydo.txt",
    "Stampli": "Stampli.txt",
    "TripleA": "Triple_A.txt",
    "Whish": "Whish.txt",
    "Yeepay": "Yeepay.txt",
}

SOURCE_KEYS = ["off", "vol", "fxRates", "cap", "rs", "mins", "plat", "revf", "impl", "vaFees", "surch"]


def norm(value) -> str:
    return str(value or "").strip().lower()


def clean_date(value) -> str:
    return str(value or "").strip()[:10]


def parse_contract_effective_date(partner: str) -> str:
    source_name = PARTNER_SOURCE.get(partner)
    if not source_name:
        return ""
    source_path = EXTRACTED_TEXT_DIR / source_name
    if not source_path.exists():
        return ""
    parsed = parse_contract_text({"text": source_path.read_text(errors="replace"), "fileName": source_name.replace(".txt", ".pdf")})
    return clean_date(parsed.get("effectiveDate"))


def earliest_partner_start(snapshot: dict, partner: str) -> str:
    candidates: list[str] = []
    for key in SOURCE_KEYS:
        for row in snapshot.get(key, []):
            if norm(row.get("partner")) != norm(partner):
                continue
            start_date = clean_date(row.get("startDate"))
            if start_date:
                candidates.append(start_date)
    return min(candidates) if candidates else ""


def implementation_go_live(snapshot: dict, partner: str) -> str:
    for row in snapshot.get("impl", []):
        if norm(row.get("partner")) == norm(partner) and row.get("feeType") == "Implementation":
            go_live = clean_date(row.get("goLiveDate"))
            if go_live:
                return go_live
    return ""


def main() -> None:
    store = SharedWorkspaceStore(DB_PATH)
    workspace = store.get_workspace()
    snapshot = workspace.get("snapshot") or {}

    updated_rows = []
    p_billing = []
    for row in snapshot.get("pBilling", []):
        partner = row.get("partner")
        contract_start = clean_date(row.get("contractStartDate")) or parse_contract_effective_date(partner) or earliest_partner_start(snapshot, partner)
        go_live = clean_date(row.get("goLiveDate")) or implementation_go_live(snapshot, partner)
        next_row = {
            **row,
            "contractStartDate": contract_start,
            "goLiveDate": go_live,
        }
        p_billing.append(next_row)
        updated_rows.append({
            "partner": partner,
            "contractStartDate": contract_start,
            "goLiveDate": go_live,
        })

    snapshot["pBilling"] = p_billing
    snapshot["impl"] = [
        {
            **row,
            "startDate": clean_date(row.get("startDate")) or (clean_date(next((billing.get("contractStartDate") for billing in p_billing if billing.get("partner") == row.get("partner")), "")) if row.get("feeType") == "Implementation" else clean_date(row.get("startDate"))),
        }
        for row in snapshot.get("impl", [])
    ]

    saved_at = store.save_snapshot(snapshot)
    REPORT_PATH.write_text(json.dumps({
        "savedAt": saved_at,
        "updatedCount": len(updated_rows),
        "rows": updated_rows,
    }, indent=2))
    print(json.dumps({"savedAt": saved_at, "updatedCount": len(updated_rows), "reportPath": str(REPORT_PATH)}, indent=2))


if __name__ == "__main__":
    main()

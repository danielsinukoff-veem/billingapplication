from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.storage import SharedWorkspaceStore

DB_PATH = ROOT / "server" / "data" / "shared_workspace.db"
REPORT_PATH = ROOT / "reports" / "contract_audit" / "partner_hubspot_status_report.json"


HUBSPOT_ROWS = [
    {"hubspotName": "AltPay", "partner": "Altpay", "integrationStatus": "Integration Underway (Partners Onboarding)", "goLiveDate": ""},
    {"hubspotName": "Athena Bitcoin", "partner": "Athena", "integrationStatus": "", "goLiveDate": ""},
    {"hubspotName": "BlindPay", "partner": "Blindpay", "integrationStatus": "Live (Partners Onboarding)", "goLiveDate": "2026-02-18"},
    {"hubspotName": "Capi", "partner": "Capi", "integrationStatus": "On Hold (Partners Onboarding)", "goLiveDate": ""},
    {"hubspotName": "CellPay", "partner": "Cellpay", "integrationStatus": "Sales Complete (Partners Onboarding)", "goLiveDate": ""},
    {"hubspotName": "Clearshift", "partner": "Clearshift", "integrationStatus": "Integration Underway (Partners Onboarding)", "goLiveDate": ""},
    {"hubspotName": "Graph", "partner": "Graph Finance", "integrationStatus": "Integration Underway (Partners Onboarding)", "goLiveDate": ""},
    {"hubspotName": "LianLian", "partner": "LianLian", "integrationStatus": "Live (Partners Onboarding)", "goLiveDate": "2025-12-01"},
    {"hubspotName": "Maple Wave", "partner": "Maplewave", "integrationStatus": "Live (Partners Onboarding)", "goLiveDate": "2026-01-15"},
    {"hubspotName": "Nomad Global", "partner": "Nomad", "integrationStatus": "Integration Underway (Partners Onboarding)", "goLiveDate": ""},
    {"hubspotName": "Nsave", "partner": "Nsave", "integrationStatus": "Live (Partners Onboarding)", "goLiveDate": "2025-05-15"},
    {"hubspotName": "Nuvion (Flutterwave)", "partner": "Nuvion", "integrationStatus": "Live (Partners Onboarding)", "goLiveDate": "2025-12-19"},
    {"hubspotName": "Remittances Hub", "partner": "Remittanceshub", "integrationStatus": "Live (Partners Onboarding)", "goLiveDate": "2025-11-24"},
    {"hubspotName": "Repay", "partner": "Repay", "integrationStatus": "Integration Underway (Partners Onboarding)", "goLiveDate": ""},
    {"hubspotName": "Skydo", "partner": "Skydo", "integrationStatus": "Live (Partners Onboarding)", "goLiveDate": "2025-05-20"},
    {"hubspotName": "TripleA", "partner": "TripleA", "integrationStatus": "Live (Partners Onboarding)", "goLiveDate": ""},
    {"hubspotName": "YeePay", "partner": "Yeepay", "integrationStatus": "Live (Partners Onboarding)", "goLiveDate": "2025-12-01"},
]

UNMATCHED_HUBSPOT_ROWS = [
    "Blackhawk",
    "Finmo",
    "Goldstack",
    "Jazz",
    "Lightnet",
    "M-DAQ",
    "Multigate",
    "NIBSS",
    "Nium",
    "Ohent Pay - New Deal",
    "Vigipay",
]


def main() -> None:
    store = SharedWorkspaceStore(DB_PATH)
    workspace = store.get_workspace()
    snapshot = workspace.get("snapshot") or {}
    rows = snapshot.get("pBilling") or []
    by_partner = {str(row.get("partner") or "").strip().lower(): row for row in rows}

    updated = []
    for item in HUBSPOT_ROWS:
        row = by_partner.get(item["partner"].strip().lower())
        if not row:
            continue
        row["integrationStatus"] = item["integrationStatus"]
        row["goLiveDate"] = item["goLiveDate"]
        updated.append({
            "partner": item["partner"],
            "integrationStatus": item["integrationStatus"],
            "goLiveDate": item["goLiveDate"],
        })

    snapshot["pBilling"] = rows
    snapshot["_version"] = max(int(snapshot.get("_version") or 0), 30)
    snapshot["_saved"] = datetime.now(timezone.utc).isoformat()
    saved_at = store.save_snapshot(snapshot)

    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(json.dumps({
        "savedAt": saved_at,
        "updatedCount": len(updated),
        "updatedPartners": updated,
        "unmatchedHubspotRows": UNMATCHED_HUBSPOT_ROWS,
    }, indent=2), encoding="utf-8")
    print(json.dumps({"savedAt": saved_at, "updatedCount": len(updated), "report": str(REPORT_PATH)}, indent=2))


if __name__ == "__main__":
    main()

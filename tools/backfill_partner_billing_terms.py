from __future__ import annotations

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.contract_parse import parse_contract_text
from server.storage import SharedWorkspaceStore

DB_PATH = ROOT / "server" / "data" / "shared_workspace.db"
EXTRACTED_TEXT_DIR = ROOT / "reports" / "contract_audit" / "extracted_text"
REPORT_DIR = ROOT / "reports" / "contract_audit"

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

MANUAL_FREQ_OVERRIDES = {
    "Athena": "Monthly",
    "Capi": "Monthly",
    "LianLian": "Monthly",
    "Maplewave": "Monthly",
    "Nsave": "Monthly",
    "Oson": "Monthly",
    "TripleA": "Monthly",
    "Whish": "Monthly",
}

MANUAL_PAYBY_OVERRIDES = {
    "Maplewave": "Issued monthly in arrears",
}


def uid_for_partner(partner: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", partner.lower()).strip("_")
    return f"pb_{slug}"


def parse_due_days(pay_by: str) -> int:
    text = str(pay_by or "").strip()
    if not text:
        return 0
    due_match = re.search(r"due\s+in\s+(\d+)\s+days?", text, re.IGNORECASE)
    if due_match:
        return int(due_match.group(1))
    net_match = re.search(r"\bnet\s*(\d+)\b", text, re.IGNORECASE)
    if net_match:
        return int(net_match.group(1))
    days_match = re.search(r"\b(\d+)\s*days?\b", text, re.IGNORECASE)
    if days_match:
        return int(days_match.group(1))
    return 0


def load_contract_terms(partner: str, source_name: str) -> tuple[dict[str, str | int], list[str]]:
    source_path = EXTRACTED_TEXT_DIR / source_name
    if not source_path.exists():
        return {}, [f"Missing extracted text: {source_name}"]

    raw_text = source_path.read_text(errors="replace")
    parsed = parse_contract_text({"text": raw_text, "fileName": source_name.replace(".txt", ".pdf")})
    billing_terms = parsed.get("billingTerms") or {}
    billing_freq = str(billing_terms.get("billingFreq") or "").strip()
    pay_by = str(billing_terms.get("payBy") or "").strip()

    if not billing_freq and partner in MANUAL_FREQ_OVERRIDES:
        billing_freq = MANUAL_FREQ_OVERRIDES[partner]
    if not pay_by and partner in MANUAL_PAYBY_OVERRIDES:
        pay_by = MANUAL_PAYBY_OVERRIDES[partner]

    due_days = parse_due_days(pay_by)

    result = {
        "billingFreq": billing_freq,
        "payBy": pay_by,
        "dueDays": due_days,
    }

    warnings = list(parsed.get("warnings") or [])
    return result, warnings


def build_next_row(partner: str, existing: dict | None, derived: dict[str, str | int], source_name: str, warnings: list[str]) -> dict:
    existing = existing or {}
    billing_freq = str(derived.get("billingFreq") or existing.get("billingFreq") or "").strip()
    pay_by = str(derived.get("payBy") or existing.get("payBy") or "").strip()
    due_days = int(derived.get("dueDays") or existing.get("dueDays") or 0)
    note_parts = [f"Source: {source_name}"]
    if warnings:
        note_parts.append("Warnings: " + " | ".join(warnings[:2]))
    if not derived.get("payBy"):
        note_parts.append("No explicit due term extracted.")
    return {
        "id": existing.get("id") or uid_for_partner(partner),
        "partner": partner,
        "billingFreq": billing_freq,
        "payBy": pay_by,
        "dueDays": due_days,
        "billingDay": existing.get("billingDay") or "",
        "note": " ".join(note_parts).strip(),
    }


def main() -> None:
    store = SharedWorkspaceStore(DB_PATH)
    workspace = store.get_workspace()
    snapshot = workspace.get("snapshot") or {}
    partners = sorted(snapshot.get("ps") or [])
    existing_rows = snapshot.get("pBilling") or []
    existing_by_partner = {row.get("partner"): row for row in existing_rows if row.get("partner")}

    updated_rows: list[dict] = []
    unresolved: list[dict] = []
    touched = set()

    for partner in partners:
        source_name = PARTNER_SOURCE.get(partner)
        if not source_name:
            unresolved.append({"partner": partner, "reason": "No mapped contract file"})
            continue

        derived, warnings = load_contract_terms(partner, source_name)
        if not any([derived.get("billingFreq"), derived.get("payBy"), derived.get("dueDays")]):
            if partner not in MANUAL_FREQ_OVERRIDES and partner not in MANUAL_PAYBY_OVERRIDES:
                unresolved.append({"partner": partner, "reason": f"No billing terms extracted from {source_name}"})
                continue
        updated_rows.append(build_next_row(partner, existing_by_partner.get(partner), derived, source_name, warnings))
        touched.add(partner)

    preserved_rows = [row for row in existing_rows if row.get("partner") not in touched]
    snapshot["pBilling"] = sorted(preserved_rows + updated_rows, key=lambda row: str(row.get("partner") or "").lower())
    saved_at = store.save_snapshot(snapshot)

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    report_path = REPORT_DIR / "partner_billing_terms_report.json"
    report_path.write_text(
        json.dumps(
            {
                "savedAt": saved_at,
                "updatedCount": len(updated_rows),
                "updatedPartners": updated_rows,
                "unresolved": unresolved,
            },
            indent=2,
        )
    )

    print(json.dumps({"savedAt": saved_at, "updatedCount": len(updated_rows), "unresolvedCount": len(unresolved)}, indent=2))
    if unresolved:
        print("Unresolved:")
        for row in unresolved:
            print(f"- {row['partner']}: {row['reason']}")


if __name__ == "__main__":
    main()

from __future__ import annotations

import json
import random
import re
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
SERVER_DIR = ROOT / "server"
if str(SERVER_DIR) not in sys.path:
    sys.path.insert(0, str(SERVER_DIR))

from contract_parse import parse_contract_text  # noqa: E402
from storage import SharedWorkspaceStore  # noqa: E402


EXTRACTED_TEXT_DIR = ROOT / "reports" / "contract_audit" / "extracted_text"
DB_PATH = ROOT / "server" / "data" / "shared_workspace.db"
REPORT_PATH = ROOT / "reports" / "contract_audit" / "missing_contract_partners_report.json"


def uid() -> str:
    return "".join(random.choice("abcdefghijklmnopqrstuvwxyz0123456789") for _ in range(6))


def norm(value: Any) -> str:
    return str(value or "").strip().lower()


def normalize_iso_date(value: Any) -> str:
    text = str(value or "").strip()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        return text
    return ""


def previous_day(iso_date: str) -> str:
    if not iso_date:
        return ""
    from datetime import date, timedelta

    year, month, day = (int(part) for part in iso_date.split("-"))
    return (date(year, month, day) - timedelta(days=1)).isoformat()


def row_signature(row: dict[str, Any], fields: list[str]) -> str:
    return "|".join(norm(row.get(field)) for field in fields)


def parse_due_days(pay_by: str) -> int:
    text = str(pay_by or "").strip()
    if not text:
        return 0
    due_match = re.search(r"due\s+in\s+(\d+)\s+days?", text, re.I)
    if due_match:
        return int(due_match.group(1))
    net_match = re.search(r"\bnet\s*(\d+)\b", text, re.I)
    if net_match:
        return int(net_match.group(1))
    days_match = re.search(r"\b(\d+)\s*days?\b", text, re.I)
    if days_match:
        return int(days_match.group(1))
    return 0


def detect_per_tier_marginal_pricing(raw_text: str) -> bool:
    text = re.sub(r"\s+", " ", str(raw_text or "").lower()).strip()
    if not text:
        return False
    return (
        "tiered pricing is applied on a per-tier basis" in text
        and (
            "only the incremental volume above that threshold is priced at the applicable tier rate" in text
            or "all prior volume remains priced at the applicable lower-tier rates" in text
        )
    )


def merge_contract_rows_by_effective_date(
    existing_rows: list[dict[str, Any]],
    new_rows: list[dict[str, Any]],
    *,
    effective_date: str,
    matches,
    start_key: str = "startDate",
    end_key: str = "endDate",
) -> list[dict[str, Any]]:
    if not new_rows:
        return existing_rows
    cutoff = previous_day(effective_date)
    merged_existing: list[dict[str, Any]] = []
    for existing in existing_rows:
        if not any(matches(existing, new_row) for new_row in new_rows):
            merged_existing.append(existing)
            continue
        row_start = normalize_iso_date(existing.get(start_key))
        row_end = normalize_iso_date(existing.get(end_key))
        if row_start and row_start == effective_date:
            continue
        if row_start and row_start > effective_date:
            merged_existing.append(existing)
            continue
        if row_end and row_end < effective_date:
            merged_existing.append(existing)
            continue
        if not cutoff:
            merged_existing.append(existing)
            continue
        updated = dict(existing)
        updated[end_key] = cutoff
        merged_existing.append(updated)
    return merged_existing + new_rows


PARTNER_SPECS = [
    {
        "partner": "BHN",
        "file": "BHN.txt",
        "effectiveDate": "2025-08-04",
        "billingFreq": "Monthly",
        "payBy": "Due in 30 days",
        "billingDay": "",
        "integrationStatus": "Integration Underway (Partners Onboarding)",
        "notYetLive": True,
        "preferredBillingTiming": "",
        "contractDueText": "",
    },
    {
        "partner": "Factura",
        "file": "Factura.txt",
        "effectiveDate": "2024-02-14",
        "billingFreq": "Quarterly",
        "payBy": "Quarterly within 15 days from last day of quarter",
        "billingDay": "",
        "integrationStatus": "",
        "notYetLive": False,
        "preferredBillingTiming": "",
        "contractDueText": "",
    },
    {
        "partner": "Goldstack",
        "file": "Goldstack.txt",
        "effectiveDate": "2026-01-12",
        "billingFreq": "Monthly",
        "payBy": "Due in 7 days",
        "billingDay": "",
        "integrationStatus": "Integration Underway (Partners Onboarding)",
        "notYetLive": True,
        "preferredBillingTiming": "",
        "contractDueText": "",
    },
    {
        "partner": "Jazz Cash",
        "file": "Jazz_Cash.txt",
        "effectiveDate": "2025-12-30",
        "billingFreq": "Monthly",
        "payBy": "",
        "billingDay": "",
        "integrationStatus": "Integration Underway (Partners Onboarding)",
        "notYetLive": True,
        "preferredBillingTiming": "",
        "contractDueText": "",
    },
    {
        "partner": "LightNet",
        "file": "Lightnet.txt",
        "effectiveDate": "2025-04-22",
        "billingFreq": "Monthly",
        "payBy": "Due in 7 days",
        "billingDay": "",
        "integrationStatus": "On Hold (Partners Onboarding)",
        "notYetLive": True,
        "preferredBillingTiming": "",
        "contractDueText": "",
    },
    {
        "partner": "MultiGate",
        "file": "Multigate.txt",
        "effectiveDate": "2025-10-22",
        "billingFreq": "Monthly",
        "payBy": "Due in 7 days",
        "billingDay": "",
        "integrationStatus": "Integration Underway (Partners Onboarding)",
        "notYetLive": True,
        "preferredBillingTiming": "",
        "contractDueText": "",
    },
    {
        "partner": "NIBSS ( TurboTech)",
        "file": "NIBSS.txt",
        "effectiveDate": "2025-03-01",
        "billingFreq": "Monthly",
        "payBy": "",
        "billingDay": "",
        "integrationStatus": "Closed Lost (Partners Onboarding)",
        "notYetLive": True,
        "preferredBillingTiming": "",
        "contractDueText": "",
    },
    {
        "partner": "Nium",
        "file": "Nium.txt",
        "effectiveDate": "2025-10-23",
        "billingFreq": "Monthly",
        "payBy": "Due in 7 days",
        "billingDay": "",
        "integrationStatus": "Integration Underway (Partners Onboarding)",
        "notYetLive": True,
        "preferredBillingTiming": "",
        "contractDueText": "",
    },
    {
        "partner": "OhentPay",
        "file": "OhentPay.txt",
        "effectiveDate": "2026-02-27",
        "billingFreq": "Monthly",
        "payBy": "Due in 7 days",
        "billingDay": "",
        "integrationStatus": "Integration Underway (Partners Onboarding)",
        "notYetLive": True,
        "preferredBillingTiming": "",
        "contractDueText": "",
    },
    {
        "partner": "VG Pay",
        "file": "VG_Pay.txt",
        "effectiveDate": "2026-02-01",
        "billingFreq": "Monthly",
        "payBy": "Due in 7 days",
        "billingDay": "",
        "integrationStatus": "Integration Underway (Partners Onboarding)",
        "notYetLive": True,
        "preferredBillingTiming": "",
        "contractDueText": "",
    },
]


def apply_manual_contract_overrides(partner: str, parsed: dict[str, Any]) -> dict[str, Any]:
    patched = dict(parsed)
    patched["warnings"] = list(parsed.get("warnings") or [])
    patched["implFees"] = list(parsed.get("implFees") or [])
    if partner == "MultiGate" and not any(norm(row.get("feeType")) == "implementation" for row in patched["implFees"]):
        patched["implFees"].append({
            "feeType": "Implementation",
            "feeAmount": 7500.0,
            "note": "Initial Commitment Payment billable at the Effective Date; non-refundable platform activation fee."
        })
    if partner == "VG Pay" and not any(norm(row.get("feeType")) == "implementation" for row in patched["implFees"]):
        patched["implFees"].append({
            "feeType": "Implementation",
            "feeAmount": 5000.0,
            "note": "$500 service credit applies against future transaction or platform fees."
        })
    if partner == "Factura":
        patched["warnings"] = [
            warning
            for warning in patched["warnings"]
            if "No pricing rows were confidently extracted" not in warning
        ]
        patched["warnings"].append(
            "Main contract pricing is a $20 monthly subscription fee per Veem account plus referral/rebate terms. The app still needs a referred-business account count source to calculate this automatically."
        )
    return patched


def add_partner_billing(snapshot: dict[str, Any], spec: dict[str, Any], parsed: dict[str, Any], effective_date: str) -> None:
    billing_rows = snapshot.setdefault("pBilling", [])
    note_parts = [f"Source: {spec['file']}"]
    warnings = parsed.get("warnings") or []
    if warnings:
        note_parts.append("Warnings: " + " ".join(warnings))
    row = {
        "id": f"pb_{spec['partner'].lower().replace(' ', '_').replace('(', '').replace(')', '').replace('-', '_')}",
        "partner": spec["partner"],
        "billingFreq": spec.get("billingFreq") or parsed.get("billingTerms", {}).get("billingFreq") or "Monthly",
        "payBy": spec.get("payBy") or parsed.get("billingTerms", {}).get("payBy") or "",
        "dueDays": parse_due_days(spec.get("payBy") or parsed.get("billingTerms", {}).get("payBy") or ""),
        "billingDay": spec.get("billingDay", ""),
        "contractDueText": spec.get("contractDueText", ""),
        "preferredBillingTiming": spec.get("preferredBillingTiming", ""),
        "contactEmails": spec.get("contactEmails", ""),
        "note": " ".join(note_parts).strip(),
        "contractStartDate": effective_date,
        "goLiveDate": spec.get("goLiveDate", ""),
        "notYetLive": bool(spec.get("notYetLive", False)),
        "integrationStatus": spec.get("integrationStatus", ""),
    }
    replaced = False
    for idx, existing in enumerate(billing_rows):
        if norm(existing.get("partner")) == norm(spec["partner"]):
            billing_rows[idx] = {**existing, **row}
            replaced = True
            break
    if not replaced:
        billing_rows.append(row)


def import_offline(snapshot: dict[str, Any], partner: str, parsed: dict[str, Any], effective_date: str) -> int:
    rows = [
        {
            "id": uid(),
            "partner": partner,
            "txnType": row.get("txnType") or "Domestic",
            "speedFlag": row.get("speedFlag") or "Standard",
            "minAmt": row.get("minAmt") or 0,
            "maxAmt": row.get("maxAmt") or 10000000,
            "payerFunding": "",
            "payeeFunding": "",
            "fee": row["fee"],
            "payerCcy": row.get("payerCcy") or "USD",
            "payeeCcy": row.get("payeeCcy") or "USD",
            "payerCountry": row.get("payerCountry") or "",
            "payeeCountry": row.get("payeeCountry") or "",
            "payerCountryGroup": row.get("payerCountryGroup") or "",
            "payeeCountryGroup": row.get("payeeCountryGroup") or "",
            "processingMethod": row.get("processingMethod") or "",
            "startDate": effective_date,
            "endDate": "",
            "note": row.get("note") or "",
        }
        for row in (parsed.get("offlineRates") or [])
    ]
    snapshot["off"] = merge_contract_rows_by_effective_date(
        list(snapshot.get("off") or []),
        rows,
        effective_date=effective_date,
        matches=lambda existing, incoming: norm(existing.get("partner")) == norm(partner)
        and row_signature(existing, ["txnType", "speedFlag", "payerFunding", "payeeFunding", "payerCcy", "payeeCcy", "payerCountry", "payeeCountry", "payerCountryGroup", "payeeCountryGroup", "processingMethod"])
        == row_signature(incoming, ["txnType", "speedFlag", "payerFunding", "payeeFunding", "payerCcy", "payeeCcy", "payerCountry", "payeeCountry", "payerCountryGroup", "payeeCountryGroup", "processingMethod"]),
    )
    return len(rows)


def import_volume(snapshot: dict[str, Any], partner: str, parsed: dict[str, Any], effective_date: str) -> int:
    rows = [
        {
            "id": uid(),
            "partner": partner,
            "txnType": row.get("txnType") or "",
            "speedFlag": row.get("speedFlag") or "",
            "rate": row["rate"],
            "payerFunding": row.get("payerFunding") or "",
            "payeeFunding": row.get("payeeFunding") or "",
            "payeeCardType": row.get("payeeCardType") or "",
            "ccyGroup": row.get("ccyGroup") or "",
            "minVol": row.get("minVol") or 0,
            "maxVol": row.get("maxVol") or 1e9,
            "startDate": effective_date,
            "endDate": "",
            "note": row.get("note") or "",
        }
        for row in (parsed.get("volumeRates") or [])
        if row.get("txnType") != "FX" and not row.get("ccyGroup")
    ]
    snapshot["vol"] = merge_contract_rows_by_effective_date(
        list(snapshot.get("vol") or []),
        rows,
        effective_date=effective_date,
        matches=lambda existing, incoming: norm(existing.get("partner")) == norm(partner)
        and row_signature(existing, ["txnType", "speedFlag", "payerFunding", "payeeFunding", "payeeCardType", "ccyGroup"])
        == row_signature(incoming, ["txnType", "speedFlag", "payerFunding", "payeeFunding", "payeeCardType", "ccyGroup"]),
    )
    return len(rows)


def import_fx(snapshot: dict[str, Any], partner: str, parsed: dict[str, Any], effective_date: str) -> int:
    parsed_fx = list(parsed.get("fxRates") or [])
    parsed_fx.extend(
        row
        for row in (parsed.get("volumeRates") or [])
        if row.get("txnType") == "FX" or row.get("ccyGroup")
    )
    rows: list[dict[str, Any]] = []
    for row in parsed_fx:
        group = row.get("ccyGroup") or ""
        payee_corridor = ""
        payee_ccy = ""
        if group in {"MAJORS", "Major"}:
            payee_corridor = "Major"
        elif group in {"MINORS", "Minor"}:
            payee_corridor = "Minor"
        elif group in {"TERTIARY", "Tertiary"}:
            payee_corridor = "Tertiary"
        elif group:
            payee_ccy = group
        rows.append(
            {
                "id": uid(),
                "partner": partner,
                "payerCorridor": row.get("payerCorridor") or "",
                "payerCcy": row.get("payerCcy") or "",
                "payeeCorridor": row.get("payeeCorridor") or payee_corridor,
                "payeeCcy": row.get("payeeCcy") or payee_ccy,
                "minTxnSize": row.get("minTxnSize") or 0,
                "maxTxnSize": row.get("maxTxnSize") or 1e9,
                "minVol": row.get("minVol") or 0,
                "maxVol": row.get("maxVol") or 1e9,
                "rate": row["rate"],
                "startDate": effective_date,
                "endDate": "",
                "note": row.get("note") or "",
            }
        )
    snapshot["fxRates"] = merge_contract_rows_by_effective_date(
        list(snapshot.get("fxRates") or []),
        rows,
        effective_date=effective_date,
        matches=lambda existing, incoming: norm(existing.get("partner")) == norm(partner)
        and row_signature(existing, ["payerCorridor", "payerCcy", "payeeCorridor", "payeeCcy"])
        == row_signature(incoming, ["payerCorridor", "payerCcy", "payeeCorridor", "payeeCcy"]),
    )
    return len(rows)


def import_minimums(snapshot: dict[str, Any], partner: str, parsed: dict[str, Any], effective_date: str) -> int:
    rows = [
        {
            "id": uid(),
            "partner": partner,
            "startDate": effective_date,
            "endDate": "",
            "minAmount": row["minAmount"],
            "minVol": row.get("minVol") or 0,
            "maxVol": row.get("maxVol") or 1e9,
            "implFeeOffset": False,
        }
        for row in (parsed.get("minimums") or [])
    ]
    snapshot["mins"] = merge_contract_rows_by_effective_date(
        list(snapshot.get("mins") or []),
        rows,
        effective_date=effective_date,
        matches=lambda existing, incoming: norm(existing.get("partner")) == norm(partner),
    )
    return len(rows)


def import_fee_caps(snapshot: dict[str, Any], partner: str, parsed: dict[str, Any], effective_date: str) -> int:
    rows = [
        {
            "id": uid(),
            "partner": partner,
            "productType": row.get("productType") or "",
            "capType": row.get("capType") or "Max Fee",
            "amount": row.get("capAmount") or row.get("amount") or 0,
            "startDate": effective_date,
            "endDate": "",
        }
        for row in (parsed.get("feeCaps") or [])
    ]
    snapshot["cap"] = merge_contract_rows_by_effective_date(
        list(snapshot.get("cap") or []),
        rows,
        effective_date=effective_date,
        matches=lambda existing, incoming: norm(existing.get("partner")) == norm(partner)
        and row_signature(existing, ["productType", "capType"]) == row_signature(incoming, ["productType", "capType"]),
    )
    return len(rows)


def import_reversal_fees(snapshot: dict[str, Any], partner: str, parsed: dict[str, Any], effective_date: str) -> int:
    rows = [
        {
            "id": uid(),
            "partner": partner,
            "payerFunding": row.get("payerFunding") or "",
            "feePerReversal": row["feePerReversal"],
            "startDate": effective_date,
            "endDate": "",
        }
        for row in (parsed.get("reversalFees") or [])
    ]
    snapshot["revf"] = merge_contract_rows_by_effective_date(
        list(snapshot.get("revf") or []),
        rows,
        effective_date=effective_date,
        matches=lambda existing, incoming: norm(existing.get("partner")) == norm(partner)
        and row_signature(existing, ["payerFunding"]) == row_signature(incoming, ["payerFunding"]),
    )
    return len(rows)


def import_platform_fees(snapshot: dict[str, Any], partner: str, parsed: dict[str, Any], effective_date: str) -> int:
    rows = [
        {
            "id": uid(),
            "partner": partner,
            "monthlyFee": row["monthlyFee"],
            "startDate": effective_date,
            "endDate": "",
        }
        for row in (parsed.get("platformFees") or [])
        if row.get("monthlyFee", 0) > 0
    ]
    snapshot["plat"] = merge_contract_rows_by_effective_date(
        list(snapshot.get("plat") or []),
        rows,
        effective_date=effective_date,
        matches=lambda existing, incoming: norm(existing.get("partner")) == norm(partner),
    )
    return len(rows)


def import_impl(snapshot: dict[str, Any], partner: str, parsed: dict[str, Any], effective_date: str, go_live_date: str) -> int:
    rows = [
        {
            "id": uid(),
            "partner": partner,
            "feeType": row.get("feeType") or "Implementation",
            "feeAmount": row["feeAmount"],
            "goLiveDate": go_live_date or "",
            "startDate": effective_date,
            "endDate": "",
            "applyAgainstMin": False,
            "note": row.get("note") or "",
        }
        for row in (parsed.get("implFees") or [])
    ]
    snapshot["impl"] = merge_contract_rows_by_effective_date(
        list(snapshot.get("impl") or []),
        rows,
        effective_date=effective_date,
        matches=lambda existing, incoming: norm(existing.get("partner")) == norm(partner)
        and row_signature(existing, ["feeType", "note"]) == row_signature(incoming, ["feeType", "note"]),
    )
    return len(rows)


def import_virtual_account_fees(snapshot: dict[str, Any], partner: str, parsed: dict[str, Any], effective_date: str) -> int:
    rows = [
        {
            "id": uid(),
            "partner": partner,
            "feeType": row["feeType"],
            "minAccounts": row["minAccounts"],
            "maxAccounts": row["maxAccounts"],
            "discount": row.get("discount") or 0,
            "feePerAccount": row["feePerAccount"],
            "startDate": effective_date,
            "endDate": "",
            "note": row.get("note") or "",
        }
        for row in (parsed.get("virtualAccountFees") or [])
    ]
    snapshot["vaFees"] = merge_contract_rows_by_effective_date(
        list(snapshot.get("vaFees") or []),
        rows,
        effective_date=effective_date,
        matches=lambda existing, incoming: norm(existing.get("partner")) == norm(partner)
        and row_signature(existing, ["feeType", "minAccounts", "maxAccounts", "note"])
        == row_signature(incoming, ["feeType", "minAccounts", "maxAccounts", "note"]),
    )
    return len(rows)


def import_surcharges(snapshot: dict[str, Any], partner: str, parsed: dict[str, Any], effective_date: str) -> int:
    rows = [
        {
            "id": uid(),
            "partner": partner,
            "surchargeType": row.get("surchargeType") or "Same Currency",
            "rate": row["rate"],
            "minVol": row.get("minVol") or 0,
            "maxVol": row.get("maxVol") or 1e9,
            "startDate": effective_date,
            "endDate": "",
            "note": row.get("note") or "",
        }
        for row in (parsed.get("surcharges") or [])
    ]
    snapshot["surch"] = merge_contract_rows_by_effective_date(
        list(snapshot.get("surch") or []),
        rows,
        effective_date=effective_date,
        matches=lambda existing, incoming: norm(existing.get("partner")) == norm(partner)
        and row_signature(existing, ["surchargeType"]) == row_signature(incoming, ["surchargeType"]),
    )
    return len(rows)


def main() -> int:
    store = SharedWorkspaceStore(DB_PATH)
    workspace = store.get_workspace()
    snapshot = workspace.get("snapshot") or {}
    report: list[dict[str, Any]] = []

    snapshot.setdefault("ps", [])
    snapshot.setdefault("pConfig", {})

    for spec in PARTNER_SPECS:
        text = (EXTRACTED_TEXT_DIR / spec["file"]).read_text()
        parsed = apply_manual_contract_overrides(spec["partner"], parse_contract_text({"text": text, "fileName": spec["file"]}))
        partner = spec["partner"]
        effective_date = normalize_iso_date(parsed.get("effectiveDate")) or spec["effectiveDate"]
        if partner not in snapshot["ps"]:
            snapshot["ps"].append(partner)
        add_partner_billing(snapshot, spec, parsed, effective_date)

        if detect_per_tier_marginal_pricing(text):
            snapshot["pConfig"][partner] = True

        counts = {
            "offlineRates": import_offline(snapshot, partner, parsed, effective_date),
            "volumeRates": import_volume(snapshot, partner, parsed, effective_date),
            "fxRates": import_fx(snapshot, partner, parsed, effective_date),
            "feeCaps": import_fee_caps(snapshot, partner, parsed, effective_date),
            "minimums": import_minimums(snapshot, partner, parsed, effective_date),
            "reversalFees": import_reversal_fees(snapshot, partner, parsed, effective_date),
            "platformFees": import_platform_fees(snapshot, partner, parsed, effective_date),
            "implFees": import_impl(snapshot, partner, parsed, effective_date, spec.get("goLiveDate", "")),
            "virtualAccountFees": import_virtual_account_fees(snapshot, partner, parsed, effective_date),
            "surcharges": import_surcharges(snapshot, partner, parsed, effective_date),
        }
        report.append(
            {
                "partner": partner,
                "file": spec["file"],
                "effectiveDate": effective_date,
                "billingFreq": spec.get("billingFreq") or parsed.get("billingTerms", {}).get("billingFreq") or "Monthly",
                "payBy": spec.get("payBy") or parsed.get("billingTerms", {}).get("payBy") or "",
                "notYetLive": bool(spec.get("notYetLive", False)),
                "integrationStatus": spec.get("integrationStatus", ""),
                "incrementalPricing": bool(snapshot.get("pConfig", {}).get(partner)),
                "warnings": parsed.get("warnings") or [],
                "rowsAdded": counts,
            }
        )

    saved_at = store.save_snapshot(snapshot)
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(json.dumps({"savedAt": saved_at, "partners": report}, indent=2))
    print(json.dumps({"savedAt": saved_at, "partnersAdded": [row["partner"] for row in report]}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

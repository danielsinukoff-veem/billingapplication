from __future__ import annotations

import argparse
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


DB_PATH = ROOT / "server" / "data" / "shared_workspace.db"


SECTION_SIGNATURES = {
    "off": ["txnType", "speedFlag", "payerFunding", "payeeFunding", "payerCcy", "payeeCcy", "payerCountry", "payeeCountry", "payerCountryGroup", "payeeCountryGroup", "processingMethod", "minAmt", "maxAmt", "fee"],
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


def uid() -> str:
    return "".join(random.choice("abcdefghijklmnopqrstuvwxyz0123456789") for _ in range(6))


def norm(value: Any) -> str:
    return str(value or "").strip().lower()


def normalize_iso_date(value: Any) -> str:
    text = str(value or "").strip()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        return text
    return ""


def row_signature(row: dict[str, Any], fields: list[str]) -> tuple[str, ...]:
    return tuple(json.dumps(row.get(field), sort_keys=True) for field in fields)


def dedupe_rows_by_partner_signature(rows: list[dict[str, Any]], signature_fields: list[str], start_key: str = "startDate") -> list[dict[str, Any]]:
    indexed = []
    for index, row in enumerate(rows):
        indexed.append((index, row))

    grouped: dict[tuple[str, tuple[str, ...]], list[tuple[int, dict[str, Any]]]] = {}
    for item in indexed:
        index, row = item
        key = (norm(row.get("partner")), row_signature(row, signature_fields))
        grouped.setdefault(key, []).append((index, row))

    chosen: list[tuple[int, dict[str, Any]]] = []
    for items in grouped.values():
        if len(items) == 1:
            chosen.append(items[0])
            continue
        chosen.append(
            max(
                items,
                key=lambda item: (
                    normalize_iso_date(item[1].get(start_key)),
                    item[0],
                ),
            )
        )

    chosen.sort(key=lambda item: item[0])
    return [row for _, row in chosen]


def cleanup_duplicate_contract_rows(snapshot: dict[str, Any]) -> None:
    for section, fields in SECTION_SIGNATURES.items():
        snapshot[section] = dedupe_rows_by_partner_signature(list(snapshot.get(section) or []), fields)


def replace_partner_rows_for_effective_date(
    existing_rows: list[dict[str, Any]],
    new_rows: list[dict[str, Any]],
    *,
    partner: str,
    effective_date: str,
    start_key: str = "startDate",
    end_key: str = "endDate",
) -> list[dict[str, Any]]:
    target_date = normalize_iso_date(effective_date)
    partner_norm = norm(partner)
    cleaned: list[dict[str, Any]] = []
    for row in existing_rows:
        if norm(row.get("partner")) != partner_norm:
            cleaned.append(row)
            continue
        row_start = normalize_iso_date(row.get(start_key))
        row_end = normalize_iso_date(row.get(end_key))
        if target_date:
            if row_start == target_date:
                continue
            if not row_end and (not row_start or row_start >= target_date):
                continue
            cleaned.append(row)
            continue
        if not row_end:
            continue
        cleaned.append(row)
    return cleaned + new_rows


def get_partner_billing(snapshot: dict[str, Any], partner: str) -> dict[str, Any] | None:
    return next((row for row in (snapshot.get("pBilling") or []) if row.get("partner") == partner), None)


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


def upsert_partner_billing(snapshot: dict[str, Any], partner: str, updates: dict[str, Any]) -> None:
    rows = list(snapshot.get("pBilling") or [])
    updated = False
    for index, row in enumerate(rows):
        if row.get("partner") != partner:
            continue
        rows[index] = {**row, **updates, "partner": partner}
        updated = True
        break
    if not updated:
        rows.append({
            "partner": partner,
            "billingFreq": "Monthly",
            "payBy": "",
            "dueDays": 0,
            "billingDay": "",
            "contractStartDate": "",
            "goLiveDate": "",
            "notYetLive": False,
            "integrationStatus": "",
            "contactEmails": "",
            "contractDueText": "",
            "preferredBillingTiming": "",
            "note": "",
            **updates,
        })
    snapshot["pBilling"] = rows


def apply_graph_alias_override(partner: str, parsed: dict[str, Any]) -> dict[str, Any]:
    if partner != "Graph Finance":
        return parsed
    patched = dict(parsed)
    if not patched.get("partnerName"):
        patched["partnerName"] = "Oval"
    return patched


def build_offline_rows(partner: str, parsed: dict[str, Any], effective_date: str) -> list[dict[str, Any]]:
    return [{
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
        "note": row.get("note") or "",
        "startDate": effective_date,
        "endDate": "",
    } for row in (parsed.get("offlineRates") or [])]


def build_volume_rows(partner: str, parsed: dict[str, Any], effective_date: str) -> list[dict[str, Any]]:
    return [{
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
    } for row in (parsed.get("volumeRates") or []) if row.get("txnType") != "FX" and not row.get("ccyGroup")]


def build_fx_rows(partner: str, parsed: dict[str, Any], effective_date: str) -> list[dict[str, Any]]:
    parsed_fx = list(parsed.get("fxRates") or [])
    parsed_fx.extend(
        row for row in (parsed.get("volumeRates") or [])
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
        rows.append({
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
        })
    return rows


def build_fee_cap_rows(partner: str, parsed: dict[str, Any], effective_date: str) -> list[dict[str, Any]]:
    return [{
        "id": uid(),
        "partner": partner,
        "productType": row.get("productType") or "",
        "capType": row.get("capType") or "Max Fee",
        "amount": row.get("capAmount") or row.get("amount") or 0,
        "startDate": effective_date,
        "endDate": "",
    } for row in (parsed.get("feeCaps") or [])]


def build_minimum_rows(partner: str, parsed: dict[str, Any], effective_date: str) -> list[dict[str, Any]]:
    return [{
        "id": uid(),
        "partner": partner,
        "startDate": effective_date,
        "endDate": "",
        "minAmount": row["minAmount"],
        "minVol": row.get("minVol") or 0,
        "maxVol": row.get("maxVol") or 1e9,
        "implFeeOffset": False,
    } for row in (parsed.get("minimums") or [])]


def build_reversal_rows(partner: str, parsed: dict[str, Any], effective_date: str) -> list[dict[str, Any]]:
    return [{
        "id": uid(),
        "partner": partner,
        "payerFunding": row.get("payerFunding") or "",
        "feePerReversal": row["feePerReversal"],
        "startDate": effective_date,
        "endDate": "",
    } for row in (parsed.get("reversalFees") or [])]


def build_platform_rows(partner: str, parsed: dict[str, Any], effective_date: str) -> list[dict[str, Any]]:
    return [{
        "id": uid(),
        "partner": partner,
        "monthlyFee": row["monthlyFee"],
        "startDate": effective_date,
        "endDate": "",
    } for row in (parsed.get("platformFees") or []) if row.get("monthlyFee", 0) > 0]


def build_impl_rows(partner: str, parsed: dict[str, Any], effective_date: str, go_live_date: str) -> list[dict[str, Any]]:
    return [{
        "id": uid(),
        "partner": partner,
        "feeType": row.get("feeType") or "Implementation",
        "feeAmount": row["feeAmount"],
        "goLiveDate": go_live_date or "",
        "startDate": effective_date,
        "endDate": "",
        "applyAgainstMin": False,
        "note": row.get("note") or "",
    } for row in (parsed.get("implFees") or [])]


def build_va_rows(partner: str, parsed: dict[str, Any], effective_date: str) -> list[dict[str, Any]]:
    return [{
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
    } for row in (parsed.get("virtualAccountFees") or [])]


def build_surcharge_rows(partner: str, parsed: dict[str, Any], effective_date: str) -> list[dict[str, Any]]:
    return [{
        "id": uid(),
        "partner": partner,
        "surchargeType": row.get("surchargeType") or "Same Currency",
        "rate": row["rate"],
        "minVol": row.get("minVol") or 0,
        "maxVol": row.get("maxVol") or 1e9,
        "startDate": effective_date,
        "endDate": "",
        "note": row.get("note") or "",
    } for row in (parsed.get("surcharges") or [])]


def main() -> int:
    parser = argparse.ArgumentParser(description="Re-sync one partner's current contract rows into the live workbook.")
    parser.add_argument("--partner", required=True, help="Workbook partner name to update.")
    parser.add_argument("--text-file", required=True, help="Path to extracted contract text file.")
    parser.add_argument("--effective-date", default="", help="Optional effective date override (YYYY-MM-DD).")
    args = parser.parse_args()

    text_path = Path(args.text_file)
    raw_text = text_path.read_text(encoding="utf-8")
    parsed = apply_graph_alias_override(args.partner, parse_contract_text({"text": raw_text}))

    store = SharedWorkspaceStore(DB_PATH)
    workspace = store.get_workspace()
    snapshot = dict(workspace.get("snapshot") or {})

    billing = get_partner_billing(snapshot, args.partner) or {}
    effective_date = normalize_iso_date(args.effective_date) or normalize_iso_date(parsed.get("effectiveDate")) or normalize_iso_date(billing.get("contractStartDate"))

    if args.partner not in list(snapshot.get("ps") or []):
        snapshot["ps"] = [*(snapshot.get("ps") or []), args.partner]

    if detect_per_tier_marginal_pricing(raw_text):
        config = dict(snapshot.get("pConfig") or {})
        config[args.partner] = True
        snapshot["pConfig"] = config

    billing_updates = {
        "billingFreq": billing.get("billingFreq") or parsed.get("billingTerms", {}).get("billingFreq") or "Monthly",
        "payBy": billing.get("payBy") or parsed.get("billingTerms", {}).get("payBy") or "",
        "dueDays": parse_due_days(parsed.get("billingTerms", {}).get("payBy") or billing.get("payBy") or ""),
        "billingDay": billing.get("billingDay") or "",
        "contractStartDate": billing.get("contractStartDate") or effective_date,
        "goLiveDate": billing.get("goLiveDate") or "",
        "notYetLive": billing.get("notYetLive") if billing else False,
        "integrationStatus": billing.get("integrationStatus") or "",
        "note": billing.get("note") or "",
        "contactEmails": billing.get("contactEmails") or "",
        "contractDueText": billing.get("contractDueText") or "",
        "preferredBillingTiming": billing.get("preferredBillingTiming") or "",
    }
    upsert_partner_billing(snapshot, args.partner, billing_updates)

    section_rows = {
        "off": build_offline_rows(args.partner, parsed, effective_date),
        "vol": build_volume_rows(args.partner, parsed, effective_date),
        "fxRates": build_fx_rows(args.partner, parsed, effective_date),
        "cap": build_fee_cap_rows(args.partner, parsed, effective_date),
        "mins": build_minimum_rows(args.partner, parsed, effective_date),
        "revf": build_reversal_rows(args.partner, parsed, effective_date),
        "plat": build_platform_rows(args.partner, parsed, effective_date),
        "impl": build_impl_rows(args.partner, parsed, effective_date, billing_updates["goLiveDate"]),
        "vaFees": build_va_rows(args.partner, parsed, effective_date),
        "surch": build_surcharge_rows(args.partner, parsed, effective_date),
    }

    section_counts: dict[str, int] = {}
    for section, rows in section_rows.items():
        if not rows:
            continue
        snapshot[section] = replace_partner_rows_for_effective_date(
            list(snapshot.get(section) or []),
            rows,
            partner=args.partner,
            effective_date=effective_date,
        )
        section_counts[section] = len(rows)

    cleanup_duplicate_contract_rows(snapshot)
    saved_at = store.save_snapshot(snapshot)

    print(json.dumps({
        "partner": args.partner,
        "effectiveDate": effective_date,
        "savedAt": saved_at,
        "sections": section_counts,
        "warnings": parsed.get("warnings") or [],
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

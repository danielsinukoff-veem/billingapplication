from __future__ import annotations

from pathlib import Path
import sys

ROOT = Path("/Users/danielsinukoff/Documents/billing-workbook")
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.storage import SharedWorkspaceStore


DB_PATH = Path("/Users/danielsinukoff/Documents/billing-workbook/server/data/shared_workspace.db")


MONTHLY_MINIMUM_CREDITS = {
    "Altpay": {
        "feeAmount": 10000,
        "creditAmount": 10000,
        "creditWindowDays": 90,
        "creditMode": "Monthly Minimum",
        "applyAgainstMin": True,
        "note": "Credited against future monthly minimum fees if launch occurs within 90 days of effective date",
    },
    "Clearshift": {
        "feeAmount": 10000,
        "creditAmount": 10000,
        "creditWindowDays": 90,
        "creditMode": "Monthly Minimum",
        "applyAgainstMin": True,
        "note": "Credited against future monthly minimum fees if launch occurs within 90 days of effective date",
    },
    "Graph Finance": {
        "feeAmount": 10000,
        "creditAmount": 10000,
        "creditWindowDays": 90,
        "creditMode": "Monthly Minimum",
        "applyAgainstMin": True,
        "note": "Credited against future monthly minimum fees if launch occurs within 90 days of effective date",
    },
    "Goldstack": {
        "feeAmount": 10000,
        "creditAmount": 10000,
        "creditWindowDays": 90,
        "creditMode": "Monthly Minimum",
        "applyAgainstMin": True,
        "note": "Credited against future monthly minimum fees if launch occurs within 90 days of effective date",
    },
    "OhentPay": {
        "feeAmount": 10000,
        "creditAmount": 10000,
        "creditWindowDays": 90,
        "creditMode": "Monthly Minimum",
        "applyAgainstMin": True,
        "note": "Credited against future monthly minimum fees if launch occurs within 90 days of effective date",
    },
}

MONTHLY_SUBSCRIPTION_CREDITS = {
    "Capi": {
        "feeAmount": 10000,
        "startDate": "2025-04-01",
        "goLiveDate": "2025-05-01",
        "creditAmount": 10000,
        "creditWindowDays": 90,
        "creditMode": "Monthly Subscription",
        "applyAgainstMin": False,
        "note": "Refunded as an offset against future monthly subscription fees if launch occurs within 90 days of effective date",
    },
    "Nsave": {
        "feeAmount": 10000,
        "goLiveDate": "2025-05-01",
        "creditAmount": 10000,
        "creditWindowDays": 90,
        "creditMode": "Monthly Subscription",
        "applyAgainstMin": False,
        "note": "Refunded as an offset against future monthly subscription fees if launch occurs within 90 days of effective date",
    },
}


def norm(value: object) -> str:
    return "".join(str(value or "").lower().split())


def next_impl_id(snapshot: dict) -> str:
    existing = {str(row.get("id") or "") for row in snapshot.get("impl", [])}
    index = 1
    while True:
        candidate = f"impl-credit-{index}"
        if candidate not in existing:
            return candidate
        index += 1


def main() -> int:
    store = SharedWorkspaceStore(DB_PATH)
    workspace = store.get_workspace()
    snapshot = workspace.get("snapshot") or {}
    snapshot.setdefault("impl", [])
    snapshot.setdefault("pBilling", [])

    changed = 0
    all_updates = {**MONTHLY_MINIMUM_CREDITS, **MONTHLY_SUBSCRIPTION_CREDITS}
    for partner, updates in all_updates.items():
        matching_rows = [
            row for row in snapshot["impl"]
            if norm(row.get("partner")) == norm(partner) and str(row.get("feeType") or "") == "Implementation"
        ]
        if matching_rows:
            for row in matching_rows:
                before = (
                    bool(row.get("applyAgainstMin")),
                    str(row.get("creditMode") or ""),
                    float(row.get("creditAmount") or 0),
                    int(float(row.get("creditWindowDays") or 0)),
                    str(row.get("startDate") or ""),
                    str(row.get("goLiveDate") or ""),
                    str(row.get("note") or ""),
                )
                row.update(updates)
                after = (
                    bool(row.get("applyAgainstMin")),
                    str(row.get("creditMode") or ""),
                    float(row.get("creditAmount") or 0),
                    int(float(row.get("creditWindowDays") or 0)),
                    str(row.get("startDate") or ""),
                    str(row.get("goLiveDate") or ""),
                    str(row.get("note") or ""),
                )
                if before != after:
                    changed += 1
        else:
            p_billing = next((row for row in snapshot["pBilling"] if norm(row.get("partner")) == norm(partner)), {})
            snapshot["impl"].append({
                "id": next_impl_id(snapshot),
                "partner": partner,
                "feeType": "Implementation",
                "feeAmount": updates["feeAmount"],
                "startDate": str(updates.get("startDate") or p_billing.get("contractStartDate") or ""),
                "endDate": "",
                "goLiveDate": str(updates.get("goLiveDate") or p_billing.get("goLiveDate") or ""),
                **updates,
            })
            changed += 1

    if changed:
        store.save_snapshot(snapshot)
    print({"changedRows": changed})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

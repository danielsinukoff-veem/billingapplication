"""Archival reference checker engine.

The production checker is intended to run as an AWS/n8n reconciliation
workflow. This module remains only as a local reference implementation.
"""

from __future__ import annotations

from collections import OrderedDict, defaultdict
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any

from .invoice_engine import (
    activity_row_key,
    apply_fee_caps,
    calculate_active_invoice_totals,
    calculate_rev_share_cost,
    compare_periods,
    enumerate_periods,
    fmt,
    fmt_pct,
    format_period_boundary,
    format_period_label,
    get_corridor,
    get_implementation_billing_date,
    get_implementation_credit_amount,
    get_implementation_credit_start_period,
    get_partner_contract_start_date,
    get_partner_go_live_date,
    implementation_credit_label,
    in_range,
    is_calendar_year_end_period,
    is_partner_active_for_period,
    is_partner_not_yet_live,
    is_recurring_billing_live_for_period,
    is_year_end_account_setup_row,
    norm,
    normalize_implementation_credit_mode,
    normalize_month_key,
    group_invoice_lines,
    rev_share_direction,
    rev_share_scope_matches,
    to_float,
    txn_matches_pricing_row,
    get_product_type,
)


def _source_rows(snapshot: dict[str, Any], key: str, partner: str, period: str | None = None) -> list[dict[str, Any]]:
    rows = []
    for row in snapshot.get(key, []) or []:
        if norm(row.get("partner")) != norm(partner):
            continue
        if period is not None and normalize_month_key(row.get("period")) != normalize_month_key(period):
            continue
        rows.append(deepcopy(row))
    return rows


def _source_periods(snapshot: dict[str, Any], partner: str | None = None) -> list[str]:
    periods: set[str] = set()
    keys = ("ltxn", "lrev", "lrs", "lfxp", "lva")
    for key in keys:
        for row in snapshot.get(key, []) or []:
            if partner and norm(row.get("partner")) != norm(partner):
                continue
            period = normalize_month_key(row.get("period") or row.get("refundPeriod") or row.get("creditCompleteMonth"))
            if period:
                periods.add(period)
    for row in snapshot.get("impl", []) or []:
        if partner and norm(row.get("partner")) != norm(partner):
            continue
        billing_date = get_implementation_billing_date(snapshot, str(row.get("partner") or ""), row)
        if billing_date:
            periods.add(normalize_month_key(billing_date))
    return sorted(periods)


def _select_partners(snapshot: dict[str, Any], partner: str | None = None) -> list[str]:
    if partner:
        return [partner]
    partners = {str(row.get("partner") or "").strip() for key in ("pBilling", "ltxn", "lrev", "lrs", "lfxp", "lva", "impl") for row in snapshot.get(key, []) or [] if str(row.get("partner") or "").strip()}
    return sorted(partners)


def _select_periods(
    snapshot: dict[str, Any],
    partner: str | None,
    periods: list[str] | None = None,
    start_period: str | None = None,
    end_period: str | None = None,
) -> list[str]:
    if periods:
        cleaned = [normalize_month_key(period) for period in periods if normalize_month_key(period)]
        return list(dict.fromkeys(cleaned))
    if start_period:
        return enumerate_periods(start_period, end_period or start_period)
    inferred = _source_periods(snapshot, partner)
    return inferred


def _bucket_key(cat: str, direction: str) -> str:
    return f"{cat}|{direction}"


def _summarize_lines(lines: list[dict[str, Any]]) -> dict[str, Any]:
    totals = calculate_active_invoice_totals(lines)
    buckets: dict[str, float] = defaultdict(float)
    counts: dict[str, int] = defaultdict(int)
    for line in lines:
        if line.get("active") is False:
            continue
        key = _bucket_key(str(line.get("cat") or ""), str(line.get("dir") or ""))
        amount = to_float(line.get("amount"))
        buckets[key] = round(buckets.get(key, 0.0) + amount, 2)
        counts[key] += 1
    return {
        "chg": round(totals["chg"], 2),
        "pay": round(totals["pay"], 2),
        "offset": round(totals["offset"], 2),
        "net": round(totals["chg"] - totals["pay"], 2),
        "buckets": dict(sorted(buckets.items())),
        "counts": dict(sorted(counts.items())),
        "lineCount": len([line for line in lines if line.get("active") is not False]),
    }


def _compare_buckets(
    maker: dict[str, Any],
    checker: dict[str, Any],
    *,
    epsilon: float = 0.01,
) -> list[dict[str, Any]]:
    keys = sorted(set((maker.get("buckets") or {}).keys()) | set((checker.get("buckets") or {}).keys()))
    diffs: list[dict[str, Any]] = []
    for key in keys:
        maker_amount = round(to_float((maker.get("buckets") or {}).get(key)), 2)
        checker_amount = round(to_float((checker.get("buckets") or {}).get(key)), 2)
        delta = round(checker_amount - maker_amount, 2)
        if abs(delta) <= epsilon:
            continue
        diffs.append(
            {
                "bucket": key,
                "maker": maker_amount,
                "checker": checker_amount,
                "delta": delta,
            }
        )
    return diffs


def _source_stats(snapshot: dict[str, Any], partner: str, period: str) -> dict[str, Any]:
    stats = {
        "ltxn_rows": 0,
        "lrev_rows": 0,
        "lrs_rows": 0,
        "lfxp_rows": 0,
        "lva_rows": 0,
        "impl_rows": 0,
    }
    for key in ("ltxn", "lrev", "lrs", "lfxp", "lva"):
        stats[f"{key}_rows"] = len(_source_rows(snapshot, key, partner, period))
    for row in snapshot.get("impl", []) or []:
        if norm(row.get("partner")) != norm(partner):
            continue
        billing_date = get_implementation_billing_date(snapshot, partner, row)
        if normalize_month_key(billing_date) == normalize_month_key(period):
            stats["impl_rows"] += 1
    return stats


def _append_line(
    lines: list[dict[str, Any]],
    *,
    cat: str,
    direction: str,
    amount: float,
    active: bool = True,
    minimum_eligible: bool = False,
    implementation_credit_eligible: str = "",
    activity_rows: list[dict[str, Any]] | None = None,
    group_label: str = "",
    note: str = "",
) -> None:
    lines.append(
        {
            "id": f"line-{len(lines)}",
            "cat": cat,
            "dir": direction,
            "amount": round(to_float(amount), 2),
            "active": active,
            "minimumEligible": minimum_eligible,
            "implementationCreditEligible": implementation_credit_eligible,
            "activityRows": activity_rows or [],
            "groupLabel": group_label or cat,
            "desc": note or group_label or cat,
            "groupKey": f"{cat}|{direction}|{group_label or cat}",
        }
    )


def calculate_checker_invoice_for_period(
    snapshot: dict[str, Any],
    partner: str,
    period: str,
    *,
    skip_implementation_credits: bool = False,
) -> dict[str, Any]:
    if not is_partner_active_for_period(snapshot, partner, period):
        return {
            "partner": partner,
            "period": period,
            "periodStart": period,
            "periodEnd": period,
            "periodLabel": format_period_label(period),
            "periodDateRange": f"{format_period_boundary(period, 'start')} - {format_period_boundary(period, 'end')}",
            "lines": [],
            "groups": [],
            "notes": [f"Partner marked inactive for {format_period_label(period)}. Billing was skipped for this month."],
            "inactivePeriod": True,
            "chg": 0.0,
            "pay": 0.0,
            "net": 0.0,
            "dir": "Partner Owes Us",
        }

    lines: list[dict[str, Any]] = []
    notes: list[str] = []
    txns = _source_rows(snapshot, "ltxn", partner, period)
    revs = _source_rows(snapshot, "lrev", partner, period)
    rev_share_summaries = _source_rows(snapshot, "lrs", partner, period)
    fx_partner_payout_rows = _source_rows(snapshot, "lfxp", partner, period)
    va_rows = _source_rows(snapshot, "lva", partner, period)
    rev_share_rows = [
        deepcopy(row)
        for row in snapshot.get("rs", []) or []
        if norm(row.get("partner")) == norm(partner) and in_range(f"{period}-15", row.get("startDate"), row.get("endDate"))
    ]
    recurring_billing_active = is_recurring_billing_live_for_period(snapshot, partner, period)
    period_volume = sum(to_float(row.get("totalVolume")) for row in txns)
    if not recurring_billing_active:
        txns = []
        revs = []
        rev_share_summaries = []
        fx_partner_payout_rows = []
        va_rows = []
        rev_share_rows = []
        period_volume = 0.0

    minimum_row = next(
        (
            row
            for row in snapshot.get("mins", []) or []
            if recurring_billing_active
            and norm(row.get("partner")) == norm(partner)
            and in_range(f"{period}-15", row.get("startDate"), row.get("endDate"))
            and period_volume >= to_float(row.get("minVol"))
            and period_volume <= to_float(row.get("maxVol"))
        ),
        None,
    )
    summary_minimum_amount = max([to_float(row.get("monthlyMinimumRevenue")) for row in rev_share_summaries] or [0.0])
    effective_minimum_amount = to_float(minimum_row.get("minAmount")) if minimum_row and to_float(minimum_row.get("minAmount")) > 0 else summary_minimum_amount
    summary_charge_rows = [row for row in rev_share_summaries if to_float(row.get("revenueOwed")) > 0]
    summary_pay_rows = [row for row in rev_share_summaries if to_float(row.get("partnerRevenueShare")) > 0]
    authoritative_payout_summary = bool(summary_pay_rows) and not rev_share_rows and not fx_partner_payout_rows
    authoritative_recurring_charge_summary = any(str(row.get("revenueSource") or "") == "billing_summary" for row in summary_charge_rows)
    pre_collected_revenue_total = sum(to_float(row.get("estRevenue")) for row in txns)
    fx_markup_activity_rows = [
        row
        for row in txns
        if (row.get("txnType") == "FX" or (row.get("payerCcy") == "USD" and row.get("payeeCcy") and row.get("payeeCcy") != "USD"))
        and row.get("processingMethod") == "Wire"
    ]

    def apply_pre_collected_revenue_offsets() -> float:
        if not recurring_billing_active or authoritative_recurring_charge_summary:
            return 0.0
        remaining_by_activity_key = OrderedDict(
            (activity_row_key(row), round(to_float(row.get("estRevenue")), 2))
            for row in txns
            if to_float(row.get("estRevenue")) > 0
        )
        if not remaining_by_activity_key:
            return 0.0
        used_total = 0.0
        for line in lines:
            if line.get("dir") != "charge" or line.get("minimumEligible") is not True or line.get("active") is False:
                continue
            activity_rows = line.get("activityRows") or []
            if not activity_rows:
                continue
            available = round(sum(remaining_by_activity_key.get(activity_row_key(row), 0.0) for row in activity_rows), 2)
            amount = to_float(line.get("amount"))
            if available <= 0 or amount <= 0:
                continue
            credit = round(min(amount, available), 2)
            remaining_credit = credit
            for row in activity_rows:
                if remaining_credit <= 0:
                    break
                key = activity_row_key(row)
                remaining = round(remaining_by_activity_key.get(key, 0.0), 2)
                if remaining <= 0:
                    continue
                applied = round(min(remaining, remaining_credit), 2)
                remaining_by_activity_key[key] = round(remaining - applied, 2)
                remaining_credit = round(remaining_credit - applied, 2)
            used_total = round(used_total + credit, 2)
            if credit >= amount - 0.01:
                line["active"] = False
                line["note"] = f"Already charged at transaction time via Est Revenue {fmt(credit)}"
            else:
                line["amount"] = round(amount - credit, 2)
        return used_total

    def apply_monthly_minimum_rule() -> None:
        if not recurring_billing_active or effective_minimum_amount <= 0:
            return
        if authoritative_recurring_charge_summary:
            return
        eligible_lines = [line for line in lines if line.get("dir") == "charge" and line.get("minimumEligible") and line.get("active") is not False]
        invoiced_generated_revenue = sum(to_float(line.get("amount")) for line in eligible_lines)
        generated_revenue = invoiced_generated_revenue + pre_collected_revenue_total
        if generated_revenue < effective_minimum_amount:
            for line in eligible_lines:
                line["active"] = False
            _append_line(
                lines,
                cat="Minimum",
                direction="charge",
                amount=effective_minimum_amount,
                minimum_eligible=True,
                implementation_credit_eligible="monthly_minimum",
                group_label="Monthly minimum",
                note=f"Monthly minimum fee for period ({fmt(effective_minimum_amount)})",
            )
        else:
            _append_line(
                lines,
                cat="Minimum",
                direction="charge",
                amount=effective_minimum_amount,
                active=False,
                group_label="Monthly minimum",
                note=f"Monthly minimum fee for period ({fmt(effective_minimum_amount)})",
            )

    def summarize_implementation_credit_base() -> dict[str, float]:
        totals: dict[str, float] = {}
        for line in lines:
            if line.get("dir") != "charge" or line.get("active") is False:
                continue
            mode = str(line.get("implementationCreditEligible") or "")
            if not mode:
                continue
            totals[mode] = round(totals.get(mode, 0.0) + to_float(line.get("amount")), 2)
        return totals

    if not authoritative_recurring_charge_summary:
        for txn in txns:
            direct_invoice_amount = to_float(txn.get("directInvoiceAmount"))
            if direct_invoice_amount != 0:
                _append_line(
                    lines,
                    cat="Offline",
                    direction="charge",
                    amount=direct_invoice_amount,
                    minimum_eligible=True,
                    activity_rows=[txn],
                    group_label=str(txn.get("txnType") or "Offline"),
                    note="Imported direct invoice amount",
                )
                continue
            for row in snapshot.get("off", []) or []:
                if norm(row.get("partner")) != norm(partner):
                    continue
                if not (
                    txn_matches_pricing_row(row, txn)
                    and to_float(txn.get("minAmt")) >= to_float(row.get("minAmt"))
                    and to_float(txn.get("maxAmt")) <= to_float(row.get("maxAmt"))
                    and in_range(f"{period}-15", row.get("startDate"), row.get("endDate"))
                ):
                    continue
                amount = to_float(row.get("fee")) * to_float(txn.get("txnCount"))
                _append_line(
                    lines,
                    cat="Offline",
                    direction="charge",
                    amount=amount,
                    minimum_eligible=True,
                    activity_rows=[txn],
                    group_label=str(txn.get("txnType") or "Offline"),
                    note=f"{int(to_float(txn.get('txnCount')))} x {fmt(row.get('fee'))}",
                )

    volume_rows = [row for row in snapshot.get("vol", []) or [] if norm(row.get("partner")) == norm(partner) and in_range(f"{period}-15", row.get("startDate"), row.get("endDate"))]
    if not authoritative_recurring_charge_summary and volume_rows:
        grouped_volume: OrderedDict[str, list[dict[str, Any]]] = OrderedDict()
        for row in volume_rows:
            grouped_volume.setdefault("|".join([
                str(row.get("txnType") or ""),
                str(row.get("speedFlag") or ""),
                str(row.get("payerFunding") or ""),
                str(row.get("payeeFunding") or ""),
                str(row.get("payeeCardType") or ""),
                str(row.get("ccyGroup") or ""),
            ]), []).append(row)
        for tiers in grouped_volume.values():
            tiers = sorted(tiers, key=lambda item: to_float(item.get("minVol")))
            base_rate = tiers[0]
            matching_txns = [txn for txn in txns if txn_matches_pricing_row(base_rate, txn)]
            if not matching_txns:
                continue
            combined_volume = sum(to_float(txn.get("totalVolume")) for txn in matching_txns)
            combined_txn_count = sum(to_float(txn.get("txnCount")) for txn in matching_txns)
            if not combined_volume:
                continue
            if len(tiers) > 1 and any(to_float(row.get("minVol")) != to_float(tiers[0].get("minVol")) for row in tiers):
                remaining = combined_volume
                total_fee = 0.0
                for tier in tiers:
                    if remaining <= 0:
                        break
                    band_size = to_float(tier.get("maxVol")) - to_float(tier.get("minVol")) + 1
                    volume_in_band = min(remaining, band_size)
                    total_fee += to_float(tier.get("rate")) * volume_in_band
                    remaining -= volume_in_band
                if total_fee > 0:
                    product_type = get_product_type(matching_txns[0], base_rate)
                    fee_per_txn = total_fee / combined_txn_count if combined_txn_count > 0 else 0
                    adjusted = apply_fee_caps(snapshot, partner, product_type, fee_per_txn, combined_txn_count, period)
                    amount = adjusted["total"] if adjusted["capped"] else total_fee
                    _append_line(
                        lines,
                        cat="Volume",
                        direction="charge",
                        amount=amount,
                        minimum_eligible=True,
                        activity_rows=matching_txns,
                        group_label=str(base_rate.get("txnType") or "Volume"),
                        note="Incremental volume band",
                    )
            else:
                tier = next((row for row in tiers if combined_volume >= to_float(row.get("minVol")) and combined_volume <= to_float(row.get("maxVol"))), None)
                if not tier:
                    continue
                product_type = get_product_type(matching_txns[0], tier)
                amount = 0.0
                for txn in matching_txns:
                    raw_amount = to_float(tier.get("rate")) * to_float(txn.get("totalVolume"))
                    fee_per_txn = raw_amount / to_float(txn.get("txnCount")) if to_float(txn.get("txnCount")) > 0 else 0
                    adjusted = apply_fee_caps(snapshot, partner, product_type, fee_per_txn, to_float(txn.get("txnCount")), period)
                    amount += adjusted["total"] if adjusted["capped"] else raw_amount
                _append_line(
                    lines,
                    cat="Volume",
                    direction="charge",
                    amount=amount,
                    minimum_eligible=True,
                    activity_rows=matching_txns,
                    group_label=str(tier.get("txnType") or "Volume"),
                    note="Volume tier",
                )

    partner_surcharges = [row for row in snapshot.get("surch", []) or [] if norm(row.get("partner")) == norm(partner) and in_range(f"{period}-15", row.get("startDate"), row.get("endDate"))]
    if not authoritative_recurring_charge_summary and partner_surcharges:
        grouped_surcharges: OrderedDict[str, list[dict[str, Any]]] = OrderedDict()
        for row in partner_surcharges:
            grouped_surcharges_key = str(row.get("surchargeType") or "")
            grouped_surcharges.setdefault(grouped_surcharges_key, []).append(row)
        matching_txns = [txn for txn in txns if to_float(txn.get("totalVolume")) > 0]
        combined_volume = sum(to_float(txn.get("totalVolume")) for txn in matching_txns)
        if matching_txns and combined_volume > 0:
            for surcharge_type, tiers in grouped_surcharges.items():
                tiers = sorted(tiers, key=lambda item: to_float(item.get("minVol")))
                if len(tiers) > 1 and any(to_float(row.get("minVol")) != to_float(tiers[0].get("minVol")) for row in tiers):
                    remaining = combined_volume
                    total_fee = 0.0
                    for tier in tiers:
                        if remaining <= 0:
                            break
                        band_size = to_float(tier.get("maxVol")) - to_float(tier.get("minVol")) + 1
                        volume_in_band = min(remaining, band_size)
                        total_fee += to_float(tier.get("rate")) * volume_in_band
                        remaining -= volume_in_band
                    if total_fee > 0:
                        _append_line(
                            lines,
                            cat="Surcharge",
                            direction="charge",
                            amount=total_fee,
                            minimum_eligible=True,
                            activity_rows=matching_txns,
                            group_label=surcharge_type,
                            note=f"{surcharge_type} incremental surcharge",
                        )
                else:
                    tier = next((row for row in tiers if combined_volume >= to_float(row.get("minVol")) and combined_volume <= to_float(row.get("maxVol"))), None)
                    if not tier:
                        continue
                    amount = to_float(tier.get("rate")) * combined_volume
                    _append_line(
                        lines,
                        cat="Surcharge",
                        direction="charge",
                        amount=amount,
                        minimum_eligible=True,
                        activity_rows=matching_txns,
                        group_label=surcharge_type,
                        note=f"{surcharge_type} surcharge",
                    )

    if authoritative_payout_summary:
        for summary in summary_pay_rows:
            if str(summary.get("revenueSource") or "") == "billing_summary":
                _append_line(
                    lines,
                    cat="Rev Share",
                    direction="pay",
                    amount=to_float(summary.get("partnerRevenueShare")),
                    group_label="Partner payout",
                    note="Billing summary payout",
                )
            else:
                _append_line(
                    lines,
                    cat="Rev Share",
                    direction="pay",
                    amount=to_float(summary.get("partnerRevenueShare")),
                    group_label="Partner payout",
                    note="Revenue report payout",
                )

    for summary in summary_charge_rows:
        if str(summary.get("revenueSource") or "") == "billing_summary":
            summary_category = "Platform" if "subscription" in norm(summary.get("summaryBillingType") or summary.get("summaryLabel")) else "Minimum" if "minimum" in norm(summary.get("summaryBillingType") or summary.get("summaryLabel")) else "Revenue"
            _append_line(
                lines,
                cat=summary_category,
                direction="charge",
                amount=to_float(summary.get("revenueOwed")),
                minimum_eligible=True,
                implementation_credit_eligible="monthly_minimum" if summary_category == "Minimum" else "monthly_subscription" if summary_category == "Platform" else "",
                group_label=str(summary.get("summaryLabel") or summary.get("summaryBillingType") or "Revenue"),
                note="Billing summary charge",
            )
        else:
            _append_line(
                lines,
                cat="Revenue",
                direction="charge",
                amount=to_float(summary.get("revenueOwed")),
                minimum_eligible=True,
                group_label="Partner-generated revenue",
                note="Revenue summary charge",
            )

    generated_revenue_by_activity: dict[str, float] = {}
    for line in lines:
        if line.get("dir") != "charge" or line.get("active") is False:
            continue
        if line.get("cat") not in {"Offline", "Volume", "FX", "Surcharge", "Revenue", "Txn Count"}:
            continue
        activity_rows = line.get("activityRows") or []
        amount = to_float(line.get("amount"))
        if amount <= 0 or not activity_rows:
            continue
        allocation_weights: list[float] = []
        if line.get("cat") in {"Volume", "FX", "Surcharge"}:
            allocation_weights = [to_float(row.get("totalVolume")) for row in activity_rows]
        elif line.get("cat") in {"Offline", "Txn Count"}:
            allocation_weights = [to_float(row.get("txnCount")) for row in activity_rows]
        total_weight = sum(weight for weight in allocation_weights if weight > 0)
        for index, row in enumerate(activity_rows):
            key = activity_row_key(row)
            if total_weight > 0 and index < len(allocation_weights) and allocation_weights[index] > 0:
                allocated = round(amount * (allocation_weights[index] / total_weight), 2)
            else:
                allocated = round(amount / len(activity_rows), 2)
            generated_revenue_by_activity[key] = round(generated_revenue_by_activity.get(key, 0.0) + allocated, 2)
    for txn in txns:
        txn["generatedRevenueSupport"] = round(generated_revenue_by_activity.get(activity_row_key(txn), 0.0), 2)

    if not authoritative_payout_summary and rev_share_rows:
        rev_share_lines: list[dict[str, Any]] = []
        for share in rev_share_rows:
            if norm(share.get("txnType")) == "payin":
                continue
            for txn in txns:
                if rev_share_direction(txn) != "Out":
                    continue
                if not rev_share_scope_matches(share, txn):
                    continue
                total_cost = calculate_rev_share_cost(snapshot, txn, period)
                est_revenue = to_float(txn.get("estRevenue"))
                imported_revenue = to_float(txn.get("customerRevenue"))
                generated_revenue = generated_revenue_by_activity.get(activity_row_key(txn), 0.0)
                if est_revenue > 0:
                    source_revenue = est_revenue
                elif imported_revenue > 0:
                    source_revenue = imported_revenue
                else:
                    source_revenue = generated_revenue
                revenue_base = max(source_revenue - total_cost, 0.0)
                payback = to_float(share.get("revSharePct")) * revenue_base
                if payback > 0:
                    rev_share_lines.append(
                        {
                            "id": f"line-{len(lines) + len(rev_share_lines)}",
                            "cat": "Rev Share",
                            "dir": "pay",
                            "amount": payback,
                            "active": True,
                            "minimumEligible": False,
                            "implementationCreditEligible": "",
                            "activityRows": [txn],
                            "groupLabel": "Partner rev-share payout",
                            "desc": "Partner rev-share payout",
                            "groupKey": "Rev Share|pay|Partner rev-share payout",
                        }
                    )
        lines.extend(rev_share_lines)

    if fx_partner_payout_rows:
        for row in fx_partner_payout_rows:
            share_activity_summary_row = {
                "partner": partner,
                "period": period,
                "txnType": "FX",
                "speedFlag": "Standard",
                "processingMethod": "Wire",
                "payerFunding": "Bank",
                "payeeFunding": "Bank",
                "payerCcy": "USD",
                "payeeCcy": "",
                "payerCountry": "",
                "payeeCountry": "",
                "txnCount": row.get("shareTxnCount") or row.get("txnCount"),
                "totalVolume": row.get("shareTotalMidMarketUsd") or row.get("shareTotalUsdDebited") or row.get("totalMidMarketUsd"),
            }
            reversal_activity_summary_row = {
                "partner": partner,
                "period": period,
                "txnType": "FX Reversal",
                "speedFlag": "Standard",
                "processingMethod": "Wire",
                "payerFunding": "Bank",
                "payeeFunding": "Bank",
                "payerCcy": "USD",
                "payeeCcy": "",
                "payerCountry": "",
                "payeeCountry": "",
                "txnCount": row.get("reversalTxnCount") or 0,
                "totalVolume": row.get("reversalTotalMidMarketUsd") or row.get("reversalTotalUsdDebited") or 0,
            }
            share_amount = to_float(row.get("shareAmount"))
            reversal_amount = to_float(row.get("reversalAmount"))
            net_payout = to_float(row.get("partnerPayout"))
            if share_amount <= 0 and net_payout > 0:
                share_amount = net_payout
            if reversal_amount <= 0 and net_payout < 0:
                reversal_amount = abs(net_payout)
            if share_amount > 0:
                _append_line(
                    lines,
                    cat="Rev Share",
                    direction="pay",
                    amount=share_amount,
                    group_label="FX partner markup payout",
                    activity_rows=[share_activity_summary_row],
                    note="FX payout",
                )
            if reversal_amount > 0:
                _append_line(
                    lines,
                    cat="Rev Share",
                    direction="pay",
                    amount=-reversal_amount,
                    group_label="FX partner markup reversal",
                    activity_rows=[reversal_activity_summary_row],
                    note="FX reversal",
                )

    partner_fx_rates = [row for row in snapshot.get("fxRates", []) or [] if norm(row.get("partner")) == norm(partner) and in_range(f"{period}-15", row.get("startDate"), row.get("endDate"))]
    if not authoritative_recurring_charge_summary and partner_fx_rates:
        for txn in [row for row in txns if row.get("payerCcy") != row.get("payeeCcy")]:
            avg_size = to_float(txn.get("avgTxnSize")) or (to_float(txn.get("totalVolume")) / to_float(txn.get("txnCount")) if to_float(txn.get("txnCount")) > 0 else 0)
            payee_corridor = get_corridor(str(txn.get("payeeCcy") or ""))
            payer_corridor = get_corridor(str(txn.get("payerCcy") or ""))
            matches: list[dict[str, Any]] = []
            for row in partner_fx_rates:
                payee_ok = row.get("payeeCcy") == txn.get("payeeCcy") if row.get("payeeCcy") else (not row.get("payeeCorridor") or row.get("payeeCorridor") == payee_corridor)
                if not payee_ok:
                    continue
                if not row.get("payerCcy") and not row.get("payerCorridor"):
                    payer_ok = True
                else:
                    payer_ok = row.get("payerCcy") == txn.get("payerCcy") if row.get("payerCcy") else row.get("payerCorridor") == payer_corridor
                if not payer_ok:
                    continue
                size_ok = avg_size >= to_float(row.get("minTxnSize")) and avg_size <= to_float(row.get("maxTxnSize"))
                if size_ok:
                    matches.append(row)
            if not matches:
                continue
            specific = [row for row in matches if row.get("payeeCcy") == txn.get("payeeCcy")]
            pool = specific or matches
            tiers = sorted(pool, key=lambda item: to_float(item.get("minVol")))
            if len(tiers) > 1 and any(to_float(row.get("minVol")) != to_float(tiers[0].get("minVol")) for row in tiers):
                remaining = to_float(txn.get("totalVolume"))
                total_fee = 0.0
                for tier in tiers:
                    if remaining <= 0:
                        break
                    band_size = to_float(tier.get("maxVol")) - to_float(tier.get("minVol")) + 1
                    volume_in_band = min(remaining, band_size)
                    total_fee += to_float(tier.get("rate")) * volume_in_band
                    remaining -= volume_in_band
                if total_fee > 0:
                    _append_line(
                        lines,
                        cat="FX",
                        direction="charge",
                        amount=total_fee,
                        minimum_eligible=True,
                        activity_rows=[txn],
                        group_label=f"{txn.get('payerCcy')}→{txn.get('payeeCcy')}",
                        note="Incremental FX charge",
                    )
            else:
                best = next((row for row in pool if to_float(txn.get("totalVolume")) >= to_float(row.get("minVol")) and to_float(txn.get("totalVolume")) <= to_float(row.get("maxVol"))), pool[0])
                amount = to_float(best.get("rate")) * to_float(txn.get("totalVolume"))
                _append_line(
                    lines,
                    cat="FX",
                    direction="charge",
                    amount=amount,
                    minimum_eligible=True,
                    activity_rows=[txn],
                    group_label=f"{txn.get('payerCcy')}→{txn.get('payeeCcy')}",
                    note="FX rate charge",
                )

    partner_reversal_fees = [row for row in snapshot.get("revf", []) or [] if norm(row.get("partner")) == norm(partner) and in_range(f"{period}-15", row.get("startDate"), row.get("endDate"))]
    if not authoritative_recurring_charge_summary:
        for row in revs:
            match = next((fee for fee in partner_reversal_fees if not fee.get("payerFunding") or fee.get("payerFunding") == row.get("payerFunding")), None)
            if match:
                amount = to_float(match.get("feePerReversal")) * to_float(row.get("reversalCount"))
                _append_line(
                    lines,
                    cat="Reversal",
                    direction="charge",
                    amount=amount,
                    minimum_eligible=True,
                    activity_rows=[row],
                    group_label=f"{row.get('payerFunding') or 'All'} reversals",
                    note="Reversal fee",
                )

    platform_fee = next((row for row in snapshot.get("plat", []) or [] if recurring_billing_active and norm(row.get("partner")) == norm(partner) and in_range(f"{period}-15", row.get("startDate"), row.get("endDate"))), None)
    if platform_fee and not any("subscription" in norm(summary.get("summaryBillingType") or summary.get("summaryLabel")) for summary in summary_charge_rows):
        _append_line(
            lines,
            cat="Platform",
            direction="charge",
            amount=to_float(platform_fee.get("monthlyFee")),
            implementation_credit_eligible="monthly_subscription",
            group_label="Monthly platform fee",
            note="Platform fee",
        )

    partner_va_fees = [row for row in snapshot.get("vaFees", []) or [] if norm(row.get("partner")) == norm(partner) and in_range(f"{period}-15", row.get("startDate"), row.get("endDate"))]
    account_setup_rows = [
        row for row in snapshot.get("impl", []) or []
        if norm(row.get("partner")) == norm(partner)
        and row.get("feeType") == "Account Setup"
        and in_range(f"{period}-15", row.get("startDate") or row.get("goLiveDate"), row.get("endDate"))
    ]
    daily_settlement_rows = [
        row for row in snapshot.get("impl", []) or []
        if norm(row.get("partner")) == norm(partner)
        and row.get("feeType") == "Daily Settlement"
        and in_range(f"{period}-15", row.get("startDate") or row.get("goLiveDate"), row.get("endDate"))
    ]
    configured_rows = [
        row for row in (list(snapshot.get("off", []) or []) + list(snapshot.get("vol", []) or []) + list(snapshot.get("fxRates", []) or []) + list(snapshot.get("surch", []) or []))
        if norm(row.get("partner")) == norm(partner) and in_range(f"{period}-15", row.get("startDate"), row.get("endDate"))
    ]
    va_data = next((row for row in va_rows if row.get("partner") == partner and normalize_month_key(row.get("period")) == normalize_month_key(period)), None)
    if recurring_billing_active and not authoritative_recurring_charge_summary and va_data:
        def find_tier(fee_type: str, count: float) -> dict[str, Any] | None:
            return next((row for row in partner_va_fees if row.get("feeType") == fee_type and count >= to_float(row.get("minAccounts")) and count <= to_float(row.get("maxAccounts"))), None)

        if to_float(va_data.get("newAccountsOpened")) > 0:
            tier = find_tier("Account Opening", to_float(va_data.get("newAccountsOpened")))
            if tier:
                _append_line(
                    lines,
                    cat="Virtual Acct",
                    direction="charge",
                    amount=to_float(tier.get("feePerAccount")) * to_float(va_data.get("newAccountsOpened")),
                    minimum_eligible=True,
                    group_label="Account Opening",
                    note="Virtual account opening",
                )
        if to_float(va_data.get("totalActiveAccounts")) > 0:
            tier = find_tier("Monthly Active", to_float(va_data.get("totalActiveAccounts")))
            if tier:
                _append_line(
                    lines,
                    cat="Virtual Acct",
                    direction="charge",
                    amount=to_float(tier.get("feePerAccount")) * to_float(va_data.get("totalActiveAccounts")),
                    minimum_eligible=True,
                    group_label="Monthly Active",
                    note="Virtual account active",
                )
        if to_float(va_data.get("dormantAccounts")) > 0:
            tier = find_tier("Dormancy", to_float(va_data.get("dormantAccounts")))
            if tier:
                _append_line(
                    lines,
                    cat="Virtual Acct",
                    direction="charge",
                    amount=to_float(tier.get("feePerAccount")) * to_float(va_data.get("dormantAccounts")),
                    minimum_eligible=True,
                    group_label="Dormancy",
                    note="Virtual account dormancy",
                )
        if to_float(va_data.get("closedAccounts")) > 0:
            tier = find_tier("Account Closing", to_float(va_data.get("closedAccounts")))
            if tier:
                _append_line(
                    lines,
                    cat="Virtual Acct",
                    direction="charge",
                    amount=to_float(tier.get("feePerAccount")) * to_float(va_data.get("closedAccounts")),
                    minimum_eligible=True,
                    group_label="Account Closing",
                    note="Virtual account closing",
                )
        annual_business_setup = next((row for row in account_setup_rows if is_year_end_account_setup_row(row) and "per business" in norm(row.get("note"))), None)
        annual_individual_setup = next((row for row in account_setup_rows if is_year_end_account_setup_row(row) and "per individual" in norm(row.get("note"))), None)
        standard_setup_fee = next((row for row in account_setup_rows if not is_year_end_account_setup_row(row)), None)
        if is_calendar_year_end_period(period) and annual_business_setup and to_float(va_data.get("totalBusinessAccounts")) > 0:
            _append_line(
                lines,
                cat="Account Setup",
                direction="charge",
                amount=to_float(annual_business_setup.get("feeAmount")) * to_float(va_data.get("totalBusinessAccounts")),
                minimum_eligible=True,
                group_label="Year-end business account setup",
                note="Year-end business setup",
            )
        if is_calendar_year_end_period(period) and annual_individual_setup and to_float(va_data.get("totalIndividualAccounts")) > 0:
            _append_line(
                lines,
                cat="Account Setup",
                direction="charge",
                amount=to_float(annual_individual_setup.get("feeAmount")) * to_float(va_data.get("totalIndividualAccounts")),
                minimum_eligible=True,
                group_label="Year-end individual account setup",
                note="Year-end individual setup",
            )
        if to_float(va_data.get("newBusinessSetups")) > 0 and standard_setup_fee:
            _append_line(
                lines,
                cat="Account Setup",
                direction="charge",
                amount=to_float(standard_setup_fee.get("feeAmount")) * to_float(va_data.get("newBusinessSetups")),
                minimum_eligible=True,
                group_label="Account Setup",
                note="Account setup",
            )
        if to_float(va_data.get("settlementCount")) > 0:
            settlement_fee = daily_settlement_rows[0] if daily_settlement_rows else None
            if settlement_fee:
                _append_line(
                    lines,
                    cat="Settlement",
                    direction="charge",
                    amount=to_float(settlement_fee.get("feeAmount")) * to_float(va_data.get("settlementCount")),
                    minimum_eligible=True,
                    group_label="Daily Settlement",
                    note="Daily settlement",
                )

    if not authoritative_recurring_charge_summary:
        if partner_reversal_fees and not revs:
            notes.append("Reversal fees are configured for this partner, but no reversal upload was imported for this period. Reversal charges may be missing.")
        if (partner_va_fees or account_setup_rows or daily_settlement_rows) and not va_data:
            notes.append("Virtual-account, account-setup, or settlement fees are configured for this partner, but no account-usage upload was imported for this period. Those charges may be missing.")
        if configured_rows and not txns:
            notes.append("Transaction-priced fees are configured for this partner, but no transaction upload was imported for this period. Offline, volume, FX, and surcharge charges may be missing.")

    if pre_collected_revenue_total > 0:
        notes.append(
            f"Pre-collected revenue from transaction-time charges: {fmt(pre_collected_revenue_total)}."
        )

    pre_collected_revenue_used = apply_pre_collected_revenue_offsets()
    apply_monthly_minimum_rule()

    if not recurring_billing_active:
        go_live_date = get_partner_go_live_date(snapshot, partner)
        if is_partner_not_yet_live(snapshot, partner):
            if go_live_date:
                notes.append(f"Partner is marked not yet live. Only implementation bills until go-live is confirmed. Target go-live date: {go_live_date}.")
            else:
                notes.append("Partner is marked not yet live. Only implementation bills during integration until a go-live date is set.")
        elif go_live_date:
            notes.append(f"Recurring monthly billing begins at go-live date {go_live_date}.")

    impl_fee = next(
        (
            row for row in snapshot.get("impl", []) or []
            if norm(row.get("partner")) == norm(partner)
            and row.get("feeType") == "Implementation"
            and normalize_month_key(get_implementation_billing_date(snapshot, partner, row)) == normalize_month_key(period)
        ),
        None,
    )
    if impl_fee:
        _append_line(
            lines,
            cat="Impl Fee",
            direction="charge",
            amount=to_float(impl_fee.get("feeAmount")),
            group_label="Implementation fee",
            note="Implementation fee",
        )

    def apply_implementation_credits() -> None:
        if skip_implementation_credits:
            return
        implementation_rows = [row for row in snapshot.get("impl", []) or [] if norm(row.get("partner")) == norm(partner) and row.get("feeType") == "Implementation"]
        if not implementation_rows:
            return
        current_base_by_mode = summarize_implementation_credit_base()
        for row in implementation_rows:
            mode = normalize_implementation_credit_mode(row)
            credit_amount = get_implementation_credit_amount(row)
            start_period = get_implementation_credit_start_period(snapshot, partner, row)
            if not mode or credit_amount <= 0 or not start_period or compare_periods(period, start_period) < 0:
                continue
            target_base = to_float(current_base_by_mode.get(mode))
            if target_base <= 0:
                continue
            prior_periods = enumerate_periods(start_period, period)[:-1]
            previously_applied = 0.0
            for prior_period in prior_periods:
                if previously_applied >= credit_amount:
                    break
                prior_invoice = calculate_checker_invoice_for_period(snapshot, partner, prior_period, skip_implementation_credits=True)
                prior_base = to_float((prior_invoice.get("implementationCreditBaseByMode") or {}).get(mode))
                if prior_base <= 0:
                    continue
                previously_applied = round(previously_applied + min(prior_base, credit_amount - previously_applied), 2)
            remaining_credit = round(credit_amount - previously_applied, 2)
            if remaining_credit <= 0:
                continue
            applied_credit = round(min(remaining_credit, target_base), 2)
            if applied_credit <= 0:
                continue
            _append_line(
                lines,
                cat="Impl Credit",
                direction="offset",
                amount=applied_credit,
                group_label="Implementation credit",
                note=f"Implementation fee credit vs {implementation_credit_label(mode)}",
            )

    apply_implementation_credits()

    totals = _summarize_lines(lines)
    implementation_credit_base_by_mode = summarize_implementation_credit_base()
    groups = group_invoice_lines(lines)
    period_label = format_period_label(period)
    period_date_range = f"{format_period_boundary(period, 'start')} - {format_period_boundary(period, 'end')}"
    return {
        "partner": partner,
        "period": period,
        "periodStart": period,
        "periodEnd": period,
        "periodLabel": period_label,
        "periodDateRange": period_date_range,
        "lines": lines,
        "groups": groups,
        "notes": notes,
        "implementationCreditBaseByMode": implementation_credit_base_by_mode,
        "checkerSourceStats": _source_stats(snapshot, partner, period),
        "chg": totals["chg"],
        "pay": totals["pay"],
        "net": totals["net"],
        "dir": "Partner Owes Us" if totals["net"] >= 0 else "We Owe Partner",
        "checkerBuckets": totals["buckets"],
    }


def build_billing_checker_report(
    snapshot: dict[str, Any],
    *,
    partner: str | None = None,
    periods: list[str] | None = None,
    start_period: str | None = None,
    end_period: str | None = None,
    epsilon: float = 0.01,
) -> dict[str, Any]:
    partners = _select_partners(snapshot, partner)
    selected_periods = _select_periods(snapshot, partner, periods=periods, start_period=start_period, end_period=end_period)
    from .invoice_engine import calculate_invoice_for_period

    runs: list[dict[str, Any]] = []
    for partner_name in partners:
        for period in selected_periods:
            maker_invoice = calculate_invoice_for_period(snapshot, partner_name, period)
            checker_invoice = calculate_checker_invoice_for_period(snapshot, partner_name, period)
            maker_summary = _summarize_lines(maker_invoice.get("lines", []))
            checker_summary = _summarize_lines(checker_invoice.get("lines", []))
            diffs = _compare_buckets(maker_summary, checker_summary, epsilon=epsilon)
            total_deltas = {
                "chg": round(checker_summary["chg"] - maker_summary["chg"], 2),
                "pay": round(checker_summary["pay"] - maker_summary["pay"], 2),
                "net": round(checker_summary["net"] - maker_summary["net"], 2),
            }
            passed = not diffs and all(abs(value) <= epsilon for value in total_deltas.values())
            runs.append(
                {
                    "partner": partner_name,
                    "period": period,
                    "passed": passed,
                    "maker": {
                        "chg": round(to_float(maker_invoice.get("chg")), 2),
                        "pay": round(to_float(maker_invoice.get("pay")), 2),
                        "net": round(to_float(maker_invoice.get("net")), 2),
                        "lineCount": maker_summary["lineCount"],
                        "buckets": maker_summary["buckets"],
                    },
                    "checker": {
                        "chg": round(to_float(checker_invoice.get("chg")), 2),
                        "pay": round(to_float(checker_invoice.get("pay")), 2),
                        "net": round(to_float(checker_invoice.get("net")), 2),
                        "lineCount": checker_summary["lineCount"],
                        "buckets": checker_summary["buckets"],
                    },
                    "diffs": diffs,
                    "totalDeltas": total_deltas,
                    "sourceStats": checker_invoice.get("checkerSourceStats") or _source_stats(snapshot, partner_name, period),
                    "notes": checker_invoice.get("notes") or [],
                }
            )
    total_passed = sum(1 for run in runs if run["passed"])
    return {
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "partnerFilter": partner,
        "periodFilter": {
            "periods": selected_periods,
            "startPeriod": start_period,
            "endPeriod": end_period,
        },
        "runCount": len(runs),
        "passedCount": total_passed,
        "failedCount": len(runs) - total_passed,
        "runs": runs,
    }

"""Archival reference automation engine.

This module is retained for historical context only. Production automation
should run in AWS and n8n.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any
from urllib.parse import urlencode

if __package__ in (None, ""):
    from invoice_engine import calculate_invoice  # type: ignore
else:
    from .invoice_engine import calculate_invoice


AUTOMATION_POLICY = {
    "internalInvoicePrepDays": [10, 5],
    "partnerReminderDaysBeforeDue": [3],
    "partnerReminderDaysAfterDue": [0, 7, 30],
    "defaultLookaheadDays": 45,
    "staleInvoiceActionGraceDays": 14,
}


def round_currency(value: Any) -> float:
    try:
        return round(float(value or 0), 2)
    except (TypeError, ValueError):
        return 0.0


def normalize_iso_date(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    try:
        return date.fromisoformat(text[:10]).isoformat()
    except ValueError:
        return ""


def parse_iso_date(value: Any) -> date | None:
    normalized = normalize_iso_date(value)
    if not normalized:
        return None
    return date.fromisoformat(normalized)


def normalize_month_key(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if len(text) >= 7 and text[4] == "-":
        return text[:7]
    return ""


def compare_periods(a: str, b: str) -> int:
    return (a > b) - (a < b)


def days_in_month(year: int, month: int) -> int:
    if month == 12:
        next_month = date(year + 1, 1, 1)
    else:
        next_month = date(year, month + 1, 1)
    return (next_month - date(year, month, 1)).days


def parse_due_days_from_pay_by(pay_by: Any) -> int:
    import re

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


def split_contact_emails(value: Any) -> list[str]:
    import re

    parts = [
        item.strip()
        for item in re.split(r"[,\n;]+", str(value or ""))
        if item and item.strip()
    ]
    deduped: list[str] = []
    seen: set[str] = set()
    for part in parts:
        key = part.lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(part)
    return deduped


def normalize_billing_frequency(value: Any) -> str:
    return str(value or "").strip().lower()


def requires_manual_schedule(value: Any) -> bool:
    text = normalize_billing_frequency(value)
    return "quarter" in text


def format_period_label(period: str) -> str:
    period = normalize_month_key(period)
    if not period:
        return ""
    year, month = period.split("-")
    month_name = date(int(year), int(month), 1).strftime("%B")
    return f"{month_name} {year}"


def format_iso_date(value: str) -> str:
    parsed = parse_iso_date(value)
    return parsed.strftime("%B %-d, %Y") if parsed else ""


def add_days_to_iso_date(value: str, offset_days: int) -> str:
    parsed = parse_iso_date(value)
    if not parsed:
        return ""
    return (parsed + timedelta(days=offset_days)).isoformat()


def get_partner_billing_config(snapshot: dict[str, Any], partner: str) -> dict[str, Any] | None:
    key = str(partner or "").strip().lower()
    for row in snapshot.get("pBilling", []) or []:
        if str(row.get("partner") or "").strip().lower() == key:
            return row
    return None


def get_billing_due_days(snapshot: dict[str, Any], partner: str) -> int:
    config = get_partner_billing_config(snapshot, partner) or {}
    explicit = int(float(config.get("dueDays") or 0))
    if explicit > 0:
        return explicit
    return parse_due_days_from_pay_by(config.get("payBy"))


def get_billing_day(snapshot: dict[str, Any], partner: str) -> int:
    config = get_partner_billing_config(snapshot, partner) or {}
    try:
        value = int(float(config.get("billingDay") or 0))
    except (TypeError, ValueError):
        value = 0
    if value <= 0:
        return 0
    return min(value, 31)


def next_month_key(period: str) -> str:
    normalized = normalize_month_key(period)
    if not normalized:
        return ""
    year, month = [int(part) for part in normalized.split("-")]
    if month == 12:
        return f"{year + 1}-01"
    return f"{year}-{str(month + 1).zfill(2)}"


def get_expected_invoice_send_date(snapshot: dict[str, Any], partner: str, period: str) -> str:
    billing_day = get_billing_day(snapshot, partner)
    if billing_day <= 0:
        return ""
    send_month = next_month_key(period)
    if not send_month:
        return ""
    year, month = [int(part) for part in send_month.split("-")]
    day = min(billing_day, days_in_month(year, month))
    return f"{year}-{str(month).zfill(2)}-{str(day).zfill(2)}"


def get_invoice_due_date(snapshot: dict[str, Any], partner: str, period: str, invoice_date: str) -> str:
    due_days = get_billing_due_days(snapshot, partner)
    if due_days <= 0 or not invoice_date:
        return ""
    return add_days_to_iso_date(invoice_date, due_days)


def get_all_invoice_periods(snapshot: dict[str, Any]) -> list[str]:
    periods: set[str] = set()
    for key in ("ltxn", "lrev", "lva", "lrs", "lfxp"):
        for row in snapshot.get(key, []) or []:
            period = normalize_month_key(row.get("period"))
            if period:
                periods.add(period)
    for row in snapshot.get("pInvoices", []) or []:
        period = normalize_month_key(row.get("period"))
        if period:
            periods.add(period)
    return sorted(periods)


def get_invoice_tracking_record(snapshot: dict[str, Any], partner: str, period: str) -> dict[str, Any] | None:
    partner_key = str(partner or "").strip().lower()
    period_key = normalize_month_key(period)
    for row in snapshot.get("pInvoices", []) or []:
        if str(row.get("kind") or "receivable") != "receivable":
            continue
        if str(row.get("partner") or "").strip().lower() != partner_key:
            continue
        if normalize_month_key(row.get("period")) != period_key:
            continue
        return row
    return None


def build_receivable_entry(
    snapshot: dict[str, Any],
    partner: str,
    period: str,
    invoice_cache: dict[tuple[str, str], dict[str, Any]],
) -> dict[str, Any]:
    cache_key = (partner, period)
    if cache_key not in invoice_cache:
        invoice_cache[cache_key] = calculate_invoice(snapshot, partner, period, period)
    invoice = invoice_cache[cache_key]
    record = get_invoice_tracking_record(snapshot, partner, period) or {}
    amount_due = round_currency(record.get("amountDueOverride") or max(round_currency(invoice.get("net")), 0))
    amount_paid = round_currency(record.get("amountPaid"))
    invoice_date = normalize_iso_date(record.get("invoiceDate"))
    expected_send_date = get_expected_invoice_send_date(snapshot, partner, period)
    scheduled_invoice_date = invoice_date or expected_send_date
    due_date = normalize_iso_date(record.get("dueDateOverride")) or get_invoice_due_date(snapshot, partner, period, scheduled_invoice_date)
    balance = round_currency(max(amount_due - amount_paid, 0))
    return {
        "partner": partner,
        "period": period,
        "invoice": invoice,
        "record": record,
        "amountDue": amount_due,
        "amountPaid": amount_paid,
        "balance": balance,
        "invoiceDate": invoice_date,
        "expectedSendDate": expected_send_date,
        "scheduledInvoiceDate": scheduled_invoice_date,
        "dueDate": due_date,
    }


def build_email_subject(item_type: str, partner: str, period: str, due_date: str) -> str:
    period_label = format_period_label(period)
    if item_type == "invoice_send":
        return f"Invoice for {period_label} - {partner}"
    if item_type == "payment_reminder_due_soon":
        return f"Reminder: {partner} invoice due {format_iso_date(due_date)}"
    if item_type == "payment_reminder_due_today":
        return f"Due today: {partner} invoice for {period_label}"
    if item_type == "payment_reminder_overdue":
        return f"Past due reminder: {partner} invoice for {period_label}"
    if item_type == "late_fee_notice":
        return f"Late fee now applies: {partner} invoice for {period_label}"
    if item_type == "service_suspension_warning":
        return f"Service suspension threshold reached: {partner}"
    return f"Billing action for {partner}"


def build_email_body(
    item_type: str,
    partner: str,
    period: str,
    amount_due: float,
    balance: float,
    due_date: str,
    late_fee_terms: str,
    late_fee_percent_monthly: float,
) -> str:
    period_label = format_period_label(period)
    intro = [
        f"Partner: {partner}",
        f"Invoice period: {period_label}",
        f"Amount due: ${amount_due:,.2f}",
        f"Outstanding balance: ${balance:,.2f}",
    ]
    if due_date:
        intro.append(f"Due date: {format_iso_date(due_date)}")
    if item_type == "invoice_send":
        intro.append("Action: send invoice email with the current invoice draft attached when hosted email delivery is enabled.")
    elif item_type == "payment_reminder_due_soon":
        intro.append("Action: send a due-soon reminder to the billing contacts.")
    elif item_type == "payment_reminder_due_today":
        intro.append("Action: send a payment-due-today reminder to the billing contacts.")
    elif item_type == "payment_reminder_overdue":
        intro.append("Action: send an overdue reminder to the billing contacts.")
    elif item_type == "late_fee_notice":
        if late_fee_percent_monthly > 0:
            intro.append(f"Late fee: {late_fee_percent_monthly:.2f}% monthly now applies.")
        if late_fee_terms:
            intro.append(f"Late fee terms: {late_fee_terms}")
    elif item_type == "service_suspension_warning":
        intro.append("Action: notify billing ops that the service-suspension threshold has been reached.")
    return "\n".join(intro)


def build_automation_outbox(
    snapshot: dict[str, Any],
    as_of: str | None = None,
    lookahead_days: int = 45,
    operator_email: str = "billing.ops@veem.local",
    base_url: str = "",
) -> dict[str, Any]:
    as_of_date = parse_iso_date(as_of) or date.today()
    horizon = as_of_date + timedelta(days=max(int(lookahead_days or 0), 0))
    stale_invoice_action_cutoff = as_of_date - timedelta(days=AUTOMATION_POLICY["staleInvoiceActionGraceDays"])
    invoice_cache: dict[tuple[str, str], dict[str, Any]] = {}
    partners = sorted({str(partner) for partner in snapshot.get("ps", []) or []})
    periods = get_all_invoice_periods(snapshot)
    items: list[dict[str, Any]] = []
    issues: list[dict[str, Any]] = []

    def make_item(
        *,
        audience: str,
        item_type: str,
        scheduled_for: str,
        partner: str,
        period: str,
        recipients: list[str],
        entry: dict[str, Any],
        config: dict[str, Any],
        detail: str,
    ) -> None:
        scheduled_date = parse_iso_date(scheduled_for)
        if not scheduled_date:
            return
        if scheduled_date > horizon:
            return
        if item_type in {"invoice_prep", "invoice_send"} and scheduled_date < stale_invoice_action_cutoff:
            return
        status = "due_today" if scheduled_date == as_of_date else "overdue" if scheduled_date < as_of_date else "upcoming"
        late_fee_rate = round_currency(config.get("lateFeePercentMonthly"))
        late_fee_terms = str(config.get("lateFeeTerms") or "").strip()
        item = {
            "id": f"{partner}|{period}|{item_type}|{scheduled_for}|{audience}",
            "audience": audience,
            "type": item_type,
            "scheduledFor": scheduled_for,
            "status": status,
            "detail": detail,
            "partner": partner,
            "period": period,
            "periodLabel": format_period_label(period),
            "recipients": recipients,
            "amountDue": entry["amountDue"],
            "amountPaid": entry["amountPaid"],
            "balance": entry["balance"],
            "invoiceDate": entry["invoiceDate"],
            "expectedSendDate": entry["expectedSendDate"],
            "dueDate": entry["dueDate"],
            "lateFeePercentMonthly": late_fee_rate,
            "lateFeeStartDays": int(float(config.get("lateFeeStartDays") or 0)),
            "serviceSuspensionDays": int(float(config.get("serviceSuspensionDays") or 0)),
            "lateFeeTerms": late_fee_terms,
            "subject": build_email_subject(item_type, partner, period, entry["dueDate"]),
            "bodyText": build_email_body(
                item_type,
                partner,
                period,
                entry["amountDue"],
                entry["balance"],
                entry["dueDate"],
                late_fee_terms,
                late_fee_rate,
            ),
            "invoiceDraftPath": f"/api/invoices/draft?{urlencode({'partner': partner, 'startPeriod': period, 'endPeriod': period})}",
            "invoiceDraftUrl": f"{base_url}/api/invoices/draft?{urlencode({'partner': partner, 'startPeriod': period, 'endPeriod': period})}" if base_url else "",
        }
        items.append(item)

    for partner in partners:
        config = get_partner_billing_config(snapshot, partner) or {}
        recipients = split_contact_emails(config.get("contactEmails"))
        manual_schedule = requires_manual_schedule(config.get("billingFreq"))
        for period in periods:
            entry = build_receivable_entry(snapshot, partner, period, invoice_cache)
            if entry["amountDue"] <= 0:
                continue
            if entry["balance"] <= 0:
                continue

            if not recipients:
                issues.append({
                    "partner": partner,
                    "period": period,
                    "severity": "warning",
                    "code": "missing_contact_emails",
                    "message": "No partner contact emails are configured, so invoice and reminder emails cannot be sent automatically.",
                })

            if manual_schedule and not entry["invoiceDate"]:
                issues.append({
                    "partner": partner,
                    "period": period,
                    "severity": "info",
                    "code": "manual_schedule_required",
                    "message": "This billing frequency requires manual invoice timing unless an invoice date is explicitly entered.",
                })
            elif not entry["expectedSendDate"] and not entry["invoiceDate"]:
                issues.append({
                    "partner": partner,
                    "period": period,
                    "severity": "warning",
                    "code": "missing_billing_day",
                    "message": "No billing day is configured, so the app cannot schedule an invoice send date automatically.",
                })

            if not entry["dueDate"]:
                issues.append({
                    "partner": partner,
                    "period": period,
                    "severity": "warning",
                    "code": "missing_due_terms",
                    "message": "No due days are configured, so payment reminders and late-fee timing cannot be scheduled automatically.",
                })

            late_fee_rate = round_currency(config.get("lateFeePercentMonthly"))
            late_fee_start_days = int(float(config.get("lateFeeStartDays") or 0))
            service_suspension_days = int(float(config.get("serviceSuspensionDays") or 0))
            late_fee_terms = str(config.get("lateFeeTerms") or "").strip()
            if late_fee_terms and late_fee_rate <= 0:
                issues.append({
                    "partner": partner,
                    "period": period,
                    "severity": "info",
                    "code": "late_fee_rate_missing",
                    "message": "Late-fee terms text exists, but no monthly late-fee percentage is configured yet.",
                })

            if not manual_schedule and not entry["invoiceDate"] and entry["expectedSendDate"]:
                for lead_days in AUTOMATION_POLICY["internalInvoicePrepDays"]:
                    send_on = add_days_to_iso_date(entry["expectedSendDate"], -lead_days)
                    make_item(
                        audience="internal",
                        item_type="invoice_prep",
                        scheduled_for=send_on,
                        partner=partner,
                        period=period,
                        recipients=[operator_email] if operator_email else [],
                        entry=entry,
                        config=config,
                        detail=f"Billing ops reminder {lead_days} day{'s' if lead_days != 1 else ''} before the expected invoice send date.",
                    )
                if recipients:
                    make_item(
                        audience="partner",
                        item_type="invoice_send",
                        scheduled_for=entry["expectedSendDate"],
                        partner=partner,
                        period=period,
                        recipients=recipients,
                        entry=entry,
                        config=config,
                        detail="Send the invoice email to the partner billing contacts.",
                    )

            if entry["balance"] > 0 and entry["dueDate"] and recipients:
                for days_before in AUTOMATION_POLICY["partnerReminderDaysBeforeDue"]:
                    make_item(
                        audience="partner",
                        item_type="payment_reminder_due_soon",
                        scheduled_for=add_days_to_iso_date(entry["dueDate"], -days_before),
                        partner=partner,
                        period=period,
                        recipients=recipients,
                        entry=entry,
                        config=config,
                        detail=f"Partner reminder {days_before} day{'s' if days_before != 1 else ''} before the due date.",
                    )
                for days_after in AUTOMATION_POLICY["partnerReminderDaysAfterDue"]:
                    reminder_type = "payment_reminder_due_today" if days_after == 0 else "payment_reminder_overdue"
                    detail = "Partner reminder on the due date." if days_after == 0 else f"Partner overdue reminder {days_after} day{'s' if days_after != 1 else ''} after the due date."
                    make_item(
                        audience="partner",
                        item_type=reminder_type,
                        scheduled_for=add_days_to_iso_date(entry["dueDate"], days_after),
                        partner=partner,
                        period=period,
                        recipients=recipients,
                        entry=entry,
                        config=config,
                        detail=detail,
                    )
                if late_fee_rate > 0:
                    make_item(
                        audience="partner",
                        item_type="late_fee_notice",
                        scheduled_for=add_days_to_iso_date(entry["dueDate"], late_fee_start_days),
                        partner=partner,
                        period=period,
                        recipients=recipients,
                        entry=entry,
                        config=config,
                        detail="Notify the partner that late-fee terms now apply to the unpaid balance.",
                    )
                if service_suspension_days > 0:
                    make_item(
                        audience="internal",
                        item_type="service_suspension_warning",
                        scheduled_for=add_days_to_iso_date(entry["dueDate"], service_suspension_days),
                        partner=partner,
                        period=period,
                        recipients=[operator_email] if operator_email else [],
                        entry=entry,
                        config=config,
                        detail="Warn billing ops that the contract's suspension threshold has been reached.",
                    )

    items.sort(key=lambda item: (item["scheduledFor"], item["partner"], item["period"], item["type"]))
    issues.sort(key=lambda issue: (issue["partner"], issue["period"], issue["code"]))
    return {
        "generatedAt": datetime.utcnow().isoformat() + "Z",
        "asOfDate": as_of_date.isoformat(),
        "lookaheadDays": int(lookahead_days),
        "policy": AUTOMATION_POLICY,
        "items": items,
        "issues": issues,
        "summary": {
            "totalItems": len(items),
            "dueToday": sum(1 for item in items if item["status"] == "due_today"),
            "overdue": sum(1 for item in items if item["status"] == "overdue"),
            "upcoming": sum(1 for item in items if item["status"] == "upcoming"),
            "partnerEmails": sum(1 for item in items if item["audience"] == "partner"),
            "internalReminders": sum(1 for item in items if item["audience"] == "internal"),
            "issues": len(issues),
        },
    }

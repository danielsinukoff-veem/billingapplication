"""Archival reference contract parsing helpers.

Production contract parsing is moving to an S3 + n8n workflow.
"""

from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any


MONTHS = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
}

DUE_DAY_WORDS = {
    "one": 1,
    "two": 2,
    "three": 3,
    "four": 4,
    "five": 5,
    "six": 6,
    "seven": 7,
    "eight": 8,
    "nine": 9,
    "ten": 10,
    "fourteen": 14,
    "fifteen": 15,
    "thirty": 30,
    "forty-five": 45,
    "forty five": 45,
}

GENERIC_PARTNER_WORDS = {
    "inc",
    "inc.",
    "llc",
    "ltd",
    "ltd.",
    "limited",
    "private",
    "payments",
    "payment",
    "technology",
    "technologies",
    "corp",
    "corp.",
    "corporation",
    "company",
    "co",
    "co.",
    "holdings",
    "holding",
    "services",
}

SECTION_STOP_MARKERS = [
    "next day ach",
    "same day ach",
    "instant bank transfer",
    "credit card payments",
    "debit card payments",
    "instant deposit to card",
    "push-to-debit",
    "other fees",
    "cross border",
    "cross-border",
    "foreign exchange",
    "majors",
    "minors",
    "tertiary",
    "named virtual accounts",
    "usd virtual account fees",
    "monthly minimum fees",
    "minimum monthly revenue commitment",
    "monthly fees",
    "billing & payment timing",
    "billing and payment timing",
    "one time account setup fee",
    "daily settlement fee",
    "card processing",
    "schedule b",
    "schedule c",
    "schedule d",
    "exhibit b",
    "tier 1 and tier 2 support responsibilities",
]

DEFAULT_LOCAL_PAYMENT_BANDS = [
    (0, 999),
    (1_000, 4_999),
    (5_000, 19_999),
    (20_000, int(1e9)),
]

LOCAL_PAYMENT_CURRENCY_RULES = {
    "CAD": {"payerCcy": "CAD", "payeeCcy": "CAD", "payeeCountryGroup": "CA", "note": "Canada local payment leg"},
    "GBP": {"payerCcy": "GBP", "payeeCcy": "GBP", "payeeCountryGroup": "UK", "note": "UK local payment leg"},
    "EUR": {"payerCcy": "EUR", "payeeCcy": "EUR", "payeeCountryGroup": "EEA", "note": "EEA local payment leg"},
    "AUD": {"payerCcy": "AUD", "payeeCcy": "AUD", "payeeCountryGroup": "AU", "note": "Australia local payment leg"},
}


def empty_contract_result() -> dict[str, Any]:
    return {
        "partnerName": "",
        "effectiveDate": "",
        "implementationFee": 0,
        "offlineRates": [],
        "volumeRates": [],
        "feeCaps": [],
        "minimums": [],
        "reversalFees": [],
        "platformFees": [],
        "implFees": [],
        "virtualAccountFees": [],
        "surcharges": [],
        "otherFees": [],
        "revShareTiers": [],
        "revShareFees": [],
        "billingTerms": {
            "payBy": "",
            "billingFreq": "",
        },
        "warnings": [],
    }


def parse_contract_text(payload: dict[str, Any]) -> dict[str, Any]:
    raw_text = str(payload.get("text") or payload.get("rawText") or payload.get("contractText") or "").strip()
    file_name = str(payload.get("fileName") or "").strip()
    if not raw_text:
        raise ValueError("Paste contract text or upload a PDF first.")

    maybe_json = try_parse_json_blob(raw_text)
    if maybe_json is not None:
        maybe_json.setdefault("warnings", [])
        return maybe_json

    full_text = normalize_text(raw_text)
    pricing_text = extract_pricing_text(full_text)

    result = empty_contract_result()
    warnings: list[str] = []

    result["partnerName"] = extract_partner_name(full_text, file_name)
    result["effectiveDate"] = extract_effective_date(full_text)
    result["billingTerms"] = extract_billing_terms("\n\n".join(part for part in [full_text, pricing_text] if part))

    parse_impl_fees(pricing_text, result)
    parse_platform_fees(pricing_text, result)
    parse_offline_rates(pricing_text, result)
    parse_volume_rates(pricing_text, result)
    parse_fee_caps(pricing_text, result)
    parse_reversal_fees(pricing_text, result)
    parse_virtual_account_fees(pricing_text, result)
    parse_surcharges(pricing_text, result)
    parse_minimums(pricing_text, result, warnings)
    parse_other_fees(pricing_text, result)
    parse_rev_share_terms(pricing_text, result, warnings)

    if detect_fx_formula(pricing_text):
        warnings.append("Detected an FX markup or settlement formula that cannot be converted into fixed rate rows automatically.")
    if "ic++" in pricing_text.lower() or "interchange plus plus" in pricing_text.lower():
        warnings.append("Detected IC++ / pass-through card pricing. Review card pricing manually after import.")
    if not any(result[key] for key in ["offlineRates", "volumeRates", "feeCaps", "minimums", "reversalFees", "platformFees", "implFees", "virtualAccountFees", "surcharges", "otherFees", "revShareTiers", "revShareFees"]):
        warnings.append("No pricing rows were confidently extracted. Review the contract text and complete any missing details manually.")

    result["warnings"] = dedupe_list(warnings)
    return result


def try_parse_json_blob(text: str) -> dict[str, Any] | None:
    stripped = text.strip().replace("```json", "").replace("```", "").strip()
    start = stripped.find("{")
    end = stripped.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return None
    candidate = stripped[start : end + 1]
    try:
        parsed = json.loads(candidate)
    except json.JSONDecodeError:
        return None
    if not isinstance(parsed, dict):
        return None
    return parsed


def normalize_text(text: str) -> str:
    replacements = {
        "\r": "\n",
        "\u2013": "-",
        "\u2014": "-",
        "\u2012": "-",
        "\u2011": "-",
        "\u2212": "-",
        "\u2018": "'",
        "\u2019": "'",
        "\u201c": '"',
        "\u201d": '"',
        "\u2022": "\n",
        "\uf0b7": "\n",
        "\xa0": " ",
    }
    clean = text
    for old, new in replacements.items():
        clean = clean.replace(old, new)
    clean = re.sub(r"[\x00-\x08\x0b-\x1f\x7f]", " ", clean)

    lines: list[str] = []
    for raw_line in clean.splitlines():
        line = re.sub(r"\s+", " ", raw_line).strip()
        if not line:
            if lines and lines[-1]:
                lines.append("")
            continue
        lowered = line.lower()
        if lowered.startswith("docusign envelope id"):
            continue
        if re.fullmatch(r"\d{1,2}", line):
            continue
        lines.append(line)

    collapsed = "\n".join(lines)
    collapsed = re.sub(r"\n{3,}", "\n\n", collapsed).strip()
    return collapsed


def extract_pricing_text(full_text: str) -> str:
    lowered = full_text.lower()
    pricing_schedule = lowered.find("pricing schedule")
    if pricing_schedule != -1:
        start = pricing_schedule
    else:
        start_candidates = [lowered.rfind(marker) for marker in ["schedule a", "exhibit a"] if lowered.rfind(marker) != -1]
        if not start_candidates:
            return full_text
        start = max(start_candidates)
    end = len(full_text)
    for marker in ["schedule b", "schedule c", "schedule d", "exhibit b", "support responsibilities", "customer onboarding elements", "payment processing operational matters"]:
        idx = lowered.find(marker, start + 1)
        if idx != -1 and idx < end:
            end = idx
    return full_text[start:end].strip()


def extract_partner_name(full_text: str, file_name: str) -> str:
    patterns = [
        r"\(\"company\"\)\s+and\s+(.+?)(?:,\s*located at|,\s*a company| and its affiliates|\s*\(\"partner\"|\s*\(“partner”|$)",
        r"between\s+veem inc\.[\s\S]*?\(\"company\"\)\s+and\s+(.+?)(?:,\s*located at|,\s*a company| and its affiliates|\s*\(\"partner\"|\s*\(“partner”|$)",
        r"\band\s+(.+?)\s+and\s+its affiliates\s+\(\"partner\"\)",
        r"\band\s+(.+?)\s+and\s+its affiliates\s+\(“partner”\)",
        r"\band\s+(.+?)\s+\(\"partner\"\)",
        r"\band\s+(.+?)\s+\(“partner”\)",
    ]
    for pattern in patterns:
        match = re.search(pattern, full_text, re.IGNORECASE | re.DOTALL)
        if match:
            return shorten_partner_name(match.group(1))
    file_stem = Path(file_name).stem if file_name else ""
    return shorten_partner_name(file_stem)


def shorten_partner_name(raw_name: str) -> str:
    clean = re.sub(r"_+", " ", str(raw_name or "")).strip(" ,.-")
    clean = re.sub(r"\s+", " ", clean)
    if not clean:
        return ""
    clean = re.sub(r"\band its affiliates\b.*$", "", clean, flags=re.IGNORECASE).strip(" ,.-")
    clean = re.sub(r"\b(dba|fka)\b.*$", "", clean, flags=re.IGNORECASE).strip(" ,.-")
    tokens = [token for token in re.split(r"\s+", clean) if token]
    filtered: list[str] = []
    for token in tokens:
        normalized = token.strip(",.").lower()
        if normalized in GENERIC_PARTNER_WORDS and filtered:
            break
        filtered.append(token.strip(","))
    if not filtered:
        filtered = tokens[:2]
    if len(filtered) >= 2 and filtered[1].strip(",.").lower() not in GENERIC_PARTNER_WORDS:
        return " ".join(filtered[:2]).strip()
    return filtered[0].strip()


def extract_effective_date(full_text: str) -> str:
    head = full_text[:2500]
    patterns = [
        r"(?:entered into|made)\s+as of\s+([A-Za-z]+)\s+(\d{1,2}|_+)?\s*,?\s*(\d{4})",
        r"effective date\)?\s*(?:is|:)?\s*([A-Za-z]+)\s+(\d{1,2}|_+)?\s*,?\s*(\d{4})",
        r"as of\s+([A-Za-z]+)\s+(\d{1,2}|_+)?\s*,?\s*(\d{4})",
    ]
    for pattern in patterns:
        match = re.search(pattern, head, re.IGNORECASE)
        if not match:
            continue
        month = MONTHS.get(match.group(1).lower())
        year = int(match.group(3))
        day_token = (match.group(2) or "").strip("_ ,")
        day = int(day_token) if day_token.isdigit() else 1
        return f"{year:04d}-{month:02d}-{day:02d}" if month else ""
    fallback = re.search(r"\b(\d{1,2})/(\d{1,2})/(\d{4})\b", head)
    if fallback:
        month = int(fallback.group(1))
        day = int(fallback.group(2))
        year = int(fallback.group(3))
        return f"{year:04d}-{month:02d}-{day:02d}"
    return ""


def extract_billing_terms(text: str) -> dict[str, str]:
    billing_freq = ""
    pay_by = ""
    lowered = text.lower()

    if re.search(r"\bpayable quarterly\b", lowered) or "last day of the relevant quarter" in lowered:
        billing_freq = "Quarterly"
    elif re.search(r"\bpayable monthly\b", lowered) or re.search(r"\bbill(?:ing)? .*?\bmonthly\b", lowered) or "invoice the partner for fees incurred on a monthly basis" in lowered or "invoices issued monthly in arrears" in lowered or "at the beginning of each month" in lowered or "each month, company will bill partner" in lowered or "on a monthly basis, company and partner will settle the net payment owed" in lowered or "will pay to partner revenue sharing due on a monthly basis" in lowered or "issue invoice regarding all transactions made in the previous month" in lowered:
        billing_freq = "Monthly"

    quarter_match = re.search(r"payable\s+quarterly\s+within\s+([a-z0-9()\s-]{1,32}?)\s+days?\s+from\s+the\s+last\s+day\s+of\s+the\s+(?:relevant\s+)?quarter", lowered)
    month_match = re.search(r"payable\s+monthly\s+within\s+([a-z0-9()\s-]{1,32}?)\s+days?\s+from\s+the\s+last\s+day\s+of\s+the\s+calendar\s+month", lowered)
    invoice_match = re.search(r"(?:due|payable)\s+within\s+(.{1,32}?)\s+days?\s+(?:of|from)\s+(?:invoice date|the date of such invoice|receipt|partner'?s receipt of an invoice)", lowered)
    receipt_match = re.search(r"payable(?:\s+by\s+the\s+partner)?\s+within\s+([a-z0-9()\s-]{1,32}?)\s+days?\s+of\s+receipt", lowered)
    fees_match = re.search(r"fees payable within\s+(.{1,32}?)\s+days?", lowered)
    generic_invoice_match = re.search(r"partner shall pay such invoices within\s+(.{1,32}?)\s+days?\s+from\s+the\s+date\s+of\s+such\s+invoice", lowered)

    if quarter_match:
        days = parse_due_day_phrase(quarter_match.group(1))
        if days:
            pay_by = f"Quarterly within {days} days from last day of quarter"
    elif month_match:
        days = parse_due_day_phrase(month_match.group(1))
        if days:
            pay_by = f"Monthly within {days} days from last day of month"
    elif receipt_match:
        days = parse_due_day_phrase(receipt_match.group(1))
        if days:
            pay_by = f"Due in {days} days"
    elif invoice_match:
        days = parse_due_day_phrase(invoice_match.group(1))
        if days:
            pay_by = f"Due in {days} days"
    elif fees_match:
        days = parse_due_day_phrase(fees_match.group(1))
        if days:
            pay_by = f"Due in {days} days"
    elif generic_invoice_match:
        days = parse_due_day_phrase(generic_invoice_match.group(1))
        if days:
            pay_by = f"Due in {days} days"
    elif "before the 15th" in lowered:
        pay_by = "Before 15th (Net 30)"
    elif "settle the net payment owed to either party" in lowered:
        pay_by = "Monthly settlement / setoff"

    return {
        "payBy": pay_by,
        "billingFreq": billing_freq,
    }


def parse_due_day_phrase(raw_phrase: str) -> int | None:
    phrase = str(raw_phrase or "").strip().lower()
    if not phrase:
        return None
    digit_match = re.search(r"\b(\d+)\b", phrase)
    if digit_match:
        return int(digit_match.group(1))
    normalized = phrase.replace("‑", "-").replace("–", "-").replace("—", "-")
    for token, days in sorted(DUE_DAY_WORDS.items(), key=lambda item: len(item[0]), reverse=True):
        if token in normalized:
            return days
    return None


def detect_fx_formula(text: str) -> bool:
    lowered = text.lower()
    return "mid-market fx rate" in lowered and "partner's mark-up" in lowered


def parse_impl_fees(text: str, result: dict[str, Any]) -> None:
    implementation = search_money(text, r"implementation fee(?: of)?\s*\$([\d,]+(?:\.\d+)?)")
    if implementation is not None:
        result["implementationFee"] = implementation
        impl_row = {"feeType": "Implementation", "feeAmount": implementation, "note": ""}
        lowered = text.lower()
        def extract_credit_window_days() -> int:
            month_words = {"one": 30, "two": 60, "three": 90, "four": 120, "five": 150, "six": 180}
            for word, days in month_words.items():
                if re.search(rf"(?:prior|within)\s+(?:to\s+)?{word}\s+months?", text, re.IGNORECASE):
                    return days
            months_match = re.search(r"(?:prior|within)\s+(?:to\s+)?(\d+)\s+months?", text, re.IGNORECASE)
            if months_match:
                return int(months_match.group(1)) * 30
            days_match = re.search(r"(?:prior|within)\s+(?:to\s+)?(\d+)\s+days?", text, re.IGNORECASE)
            if days_match:
                return int(days_match.group(1))
            return 0

        has_future_fee_credit = any(
            phrase in lowered
            for phrase in (
                "credited against future fees",
                "credit against future fees",
                "credited against future fee",
                "credit against future fee",
                "offset against future fees",
                "offset against future fee",
                "offset against future monthly subscription fees",
                "offset against future monthly subscription fee",
            )
        )
        has_monthly_subscription_credit = "monthly subscription fee" in lowered or "monthly subscription fees" in lowered
        has_monthly_minimum_credit = any(
            phrase in lowered
            for phrase in (
                "minimum monthly revenue commitment",
                "monthly minimum revenue commitment",
                "monthly minimum revenue",
                "monthly minimum fee",
                "monthly minimum fees",
                "minimum fee schedule",
                "minimum monthly fees",
            )
        )
        if has_future_fee_credit:
            credit_window_days = extract_credit_window_days()
            if has_monthly_subscription_credit:
                impl_row.update({
                    "creditMode": "Monthly Subscription",
                    "creditAmount": implementation,
                    "creditWindowDays": credit_window_days,
                    "note": "Refunded as an offset against future monthly subscription fees"
                    + (f" if launch occurs within {credit_window_days} days of effective date" if credit_window_days else ""),
                })
            elif has_monthly_minimum_credit:
                impl_row.update({
                    "applyAgainstMin": True,
                    "creditMode": "Monthly Minimum",
                    "creditAmount": implementation,
                    "creditWindowDays": credit_window_days,
                    "note": "Credited against future monthly minimum fees"
                    + (f" if launch occurs within {credit_window_days} days of effective date" if credit_window_days else ""),
                })
        result["implFees"].append(impl_row)

    yearly_account_setup_block = extract_block(text, "yearly account setup fee")
    generic_account_setup_block = extract_block(text, "account setup fee")
    account_setup_block = yearly_account_setup_block or generic_account_setup_block
    is_yearly_account_setup = bool(yearly_account_setup_block)
    if account_setup_block:
        per_business = search_money(account_setup_block, r"per business\s*\$([\d,]+(?:\.\d+)?)")
        if per_business is not None:
            note = "Per business · Year-end active accounts" if is_yearly_account_setup else "Per business"
            result["implFees"].append({"feeType": "Account Setup", "feeAmount": per_business, "note": note})
        per_individual = search_money(account_setup_block, r"per individual\s*\$([\d,]+(?:\.\d+)?)")
        if per_individual is not None:
            note = "Per individual · Year-end active accounts" if is_yearly_account_setup else "Per individual"
            result["implFees"].append({"feeType": "Account Setup", "feeAmount": per_individual, "note": note})

    daily_settlement = search_money(text, r"daily settlement fee:.*?price\s*\$([\d,]+(?:\.\d+)?)")
    if daily_settlement is None:
        daily_settlement = search_money(text, r"daily settlement fee.*?\$([\d,]+(?:\.\d+)?)")
    if daily_settlement is not None:
        result["implFees"].append({"feeType": "Daily Settlement", "feeAmount": daily_settlement, "note": ""})

    result["implFees"] = dedupe_dicts(result["implFees"], ("feeType", "feeAmount", "creditMode", "creditAmount", "creditWindowDays", "note"))


def parse_platform_fees(text: str, result: dict[str, Any]) -> None:
    matches = re.finditer(r"(?:monthly platform fee|platform fee)\s*(?:of|:)?\s*\$([\d,]+(?:\.\d+)?)", text, re.IGNORECASE)
    for match in matches:
        fee = parse_money(match.group(1))
        if fee is None:
            continue
        result["platformFees"].append({"monthlyFee": fee, "note": ""})
    result["platformFees"] = dedupe_dicts(result["platformFees"], ("monthlyFee", "note"))


def parse_offline_rates(text: str, result: dict[str, Any]) -> None:
    add_offline_block_rates(text, result, marker="next day ach", txn_type="Domestic", speed_flag="Standard", processing_method="ACH")
    add_offline_block_rates(text, result, marker="same day ach", txn_type="Domestic", speed_flag="FasterACH", processing_method="ACH")
    add_offline_block_rates(text, result, marker="swift wire fees", txn_type="USD Abroad", speed_flag="Standard", processing_method="Wire")
    add_local_payment_leg_rates(text, result)

    transaction_block = extract_transaction_fees_block(text)
    if transaction_block:
        standard_ach_line = search_money(transaction_block, r"(?:^|\n)ach\s+\$([\d,]+(?:\.\d+)?)")
        if standard_ach_line is not None:
            push_offline_rate(result, "Domestic", "Standard", standard_ach_line, processing_method="ACH")

        same_day_ach_line = search_money(transaction_block, r"(?:^|\n)same day ach\s+\$([\d,]+(?:\.\d+)?)")
        if same_day_ach_line is not None:
            push_offline_rate(result, "Domestic", "FasterACH", same_day_ach_line, processing_method="ACH")

        cross_border_usd_line = search_money(transaction_block, r"cross border usd payment\s*\(wire\)\s*\$([\d,]+(?:\.\d+)?)")
        if cross_border_usd_line is not None:
            push_offline_rate(result, "USD Abroad", "Standard", cross_border_usd_line, processing_method="Wire", note="Cross Border USD Payment")

    ach_match = re.search(r"us:\s*ach\s*\$([\d,]+(?:\.\d+)?)", text, re.IGNORECASE)
    if ach_match:
        push_offline_rate(result, "Domestic", "Standard", parse_money(ach_match.group(1)), processing_method="ACH")

    cross_border_usd = re.search(r"cross-border usd payments:.*?\$([\d,]+(?:\.\d+)?)\s*fee per transaction", text, re.IGNORECASE | re.DOTALL)
    if cross_border_usd:
        push_offline_rate(result, "USD Abroad", "Standard", parse_money(cross_border_usd.group(1)), processing_method="Wire", note="Cross-Border USD Payments")

    settlement_usd_match = re.search(r"cross border:.*?settlement in \$us.*?(?:pricing is tiered based on the number of monthly wire transactions\.)?\s*\$([\d,]+(?:\.\d+)?)", text, re.IGNORECASE | re.DOTALL)
    if settlement_usd_match:
        push_offline_rate(result, "USD Abroad", "Standard", parse_money(settlement_usd_match.group(1)), processing_method="Wire", note="Settlement in USD")

    same_currency_wire_block = extract_block(text, "cross border same currency transactions")
    if same_currency_wire_block:
        per_txn_money = re.search(r"\b(?:n/a\s+)?usd\s+([\d,]+(?:\.\d+)?)\b", same_currency_wire_block, re.IGNORECASE)
        if per_txn_money:
            push_offline_rate(result, "USD Abroad", "Standard", parse_money(per_txn_money.group(1)), processing_method="Wire", note="Cross Border Same Currency")

    domestic_wire_international = re.search(r"domestic wire\s+international wire\s+\$([\d,]+(?:\.\d+)?)\s+\$([\d,]+(?:\.\d+)?)", text, re.IGNORECASE)
    if domestic_wire_international:
        push_offline_rate(result, "Domestic", "Standard", parse_money(domestic_wire_international.group(1)), processing_method="Wire", note="Domestic Wire")
        push_offline_rate(result, "FX", "Standard", parse_money(domestic_wire_international.group(2)), processing_method="Wire", note="International Wire")

    standard_ach = search_money(text, r"standard ach\s*:\s*company will charge partner\s*\$([\d,]+(?:\.\d+)?)")
    if standard_ach is not None:
        push_offline_rate(result, "Domestic", "Standard", standard_ach, processing_method="ACH")

    same_day_ach = search_money(text, r"same-day ach\s*:\s*company will charge partner\s*\$([\d,]+(?:\.\d+)?)")
    if same_day_ach is not None:
        push_offline_rate(result, "Domestic", "FasterACH", same_day_ach, processing_method="ACH")

    result["offlineRates"] = dedupe_dicts(
        result["offlineRates"],
        ("txnType", "speedFlag", "minAmt", "maxAmt", "fee", "payerCcy", "payeeCcy", "payerCountry", "payeeCountry", "payerCountryGroup", "payeeCountryGroup", "processingMethod", "note"),
    )


def add_offline_block_rates(text: str, result: dict[str, Any], marker: str, txn_type: str, speed_flag: str, processing_method: str) -> None:
    block = extract_block(text, marker)
    if not block:
        return
    rows = parse_count_tier_rows(block)
    if rows:
        for min_amt, max_amt, fee in rows:
            push_offline_rate(result, txn_type, speed_flag, fee, min_amt=min_amt, max_amt=max_amt, processing_method=processing_method)
        return
    fee = search_money(block, r"\$([\d,]+(?:\.\d+)?)")
    if fee is not None:
        push_offline_rate(result, txn_type, speed_flag, fee, processing_method=processing_method)


def add_local_payment_leg_rates(text: str, result: dict[str, Any]) -> None:
    block = extract_block(text, "local payment leg in canada, uk, eu and australia", window=2400, ignored_stops={"cross border", "cross-border"})
    if not block:
        return
    add_local_payment_leg_section(block, result, heading="next day settlement", speed_flag="Standard")
    add_local_payment_leg_section(block, result, heading="same day settlement", speed_flag="FasterACH")


def add_local_payment_leg_section(block: str, result: dict[str, Any], *, heading: str, speed_flag: str) -> None:
    lower_block = block.lower()
    heading_index = lower_block.find(heading.lower())
    if heading_index == -1:
        return
    end = len(block)
    for stop in ["same day settlement", "cross border", "cross-border"]:
        if stop == heading.lower():
            continue
        idx = lower_block.find(stop, heading_index + len(heading))
        if idx != -1 and idx < end:
            end = idx
    section = block[heading_index:end]
    header_match = re.search(r"cad\s+gbp\s+euro\s+aud", section, re.IGNORECASE)
    if not header_match:
        return
    body = section[header_match.end():]
    matrix_lines: list[list[float]] = []
    for line in body.splitlines():
        clean = line.strip()
        if not clean:
            continue
        amounts = re.findall(r"[$£€]\s*([\d,]+(?:\.\d+)?)", clean)
        if len(amounts) == 4:
            matrix_lines.append([parse_money(amount) for amount in amounts])
    if not matrix_lines:
        return
    for idx, fees in enumerate(matrix_lines[: len(DEFAULT_LOCAL_PAYMENT_BANDS)]):
        min_amt, max_amt = DEFAULT_LOCAL_PAYMENT_BANDS[idx]
        for ccy, fee in zip(("CAD", "GBP", "EUR", "AUD"), fees):
            rule = LOCAL_PAYMENT_CURRENCY_RULES[ccy]
            push_offline_rate(
                result,
                "Domestic",
                speed_flag,
                fee,
                min_amt=min_amt,
                max_amt=max_amt,
                payer_ccy=rule["payerCcy"],
                payee_ccy=rule["payeeCcy"],
                payee_country_group=rule["payeeCountryGroup"],
                note=rule["note"],
            )


def parse_volume_rates(text: str, result: dict[str, Any]) -> None:
    add_percentage_block_rates(text, result, marker="instant bank transfer", txn_type="Domestic", speed_flag="RTP")
    add_percentage_block_rates(text, result, marker="credit card payments", txn_type="Domestic", payer_funding="Card", payee_card_type="Credit")
    add_percentage_block_rates(text, result, marker="debit card payments", txn_type="Domestic", payer_funding="Card", payee_card_type="Debit")
    add_percentage_block_rates(text, result, marker="instant deposit to card", txn_type="", payee_funding="Card", payee_card_type="Debit")
    add_percentage_block_rates(text, result, marker="push-to-debit", txn_type="", payee_funding="Card", payee_card_type="Debit")

    transaction_block = extract_transaction_fees_block(text)
    if transaction_block:
        instant_bank_rate = search_percent(transaction_block, r"instant bank(?:\s*\(rtp\))?\s+([\d.,]+)%")
        if instant_bank_rate is not None:
            result["volumeRates"].append({
                "txnType": "Domestic",
                "speedFlag": "RTP",
                "rate": instant_bank_rate,
                "payerFunding": "",
                "payeeFunding": "",
                "payeeCardType": "",
                "ccyGroup": "",
                "minVol": 0,
                "maxVol": int(1e9),
                "note": "Instant Bank (RTP)",
            })

        push_to_debit_rate = search_percent(transaction_block, r"push to debit card\s+([\d.,]+)%")
        if push_to_debit_rate is not None:
            result["volumeRates"].append({
                "txnType": "",
                "speedFlag": "",
                "rate": push_to_debit_rate,
                "payerFunding": "",
                "payeeFunding": "Card",
                "payeeCardType": "Debit",
                "ccyGroup": "",
                "minVol": 0,
                "maxVol": int(1e9),
                "note": "Push to Debit Card",
            })

    fx_block = extract_block(text, "foreign exchange", ignored_stops={"majors", "minors", "tertiary"})
    if fx_block:
        add_fx_group_rates(fx_block, result, "majors", "MAJORS")
        add_fx_group_rates(fx_block, result, "minors", "MINORS")
        add_fx_group_rates(fx_block, result, "tertiary", "TERTIARY")

    cross_border_fx_block = extract_block(text, "cross border fx", ignored_stops={"majors", "minors", "tertiary"})
    if cross_border_fx_block:
        add_fx_group_rates(cross_border_fx_block, result, "majors", "MAJORS")
        add_fx_group_rates(cross_border_fx_block, result, "minors", "MINORS")
        add_fx_group_rates(cross_border_fx_block, result, "tertiary", "TERTIARY")
        if not any(row for row in result["volumeRates"] if row.get("txnType") == "FX"):
            for min_vol, max_vol, rate in parse_volume_tier_rows(cross_border_fx_block):
                result["volumeRates"].append({
                    "txnType": "FX",
                    "speedFlag": "",
                    "rate": rate,
                    "payerFunding": "",
                    "payeeFunding": "",
                    "payeeCardType": "",
                    "ccyGroup": "",
                    "minVol": min_vol,
                    "maxVol": max_vol,
                    "note": "Cross Border FX",
                })

    if re.search(r"us:\s*instant bank transfer:\s*([\d.,]+)%\s+of transaction amount\s+\(\$([\d,]+(?:\.\d+)?)\s*min\)", text, re.IGNORECASE):
        match = re.search(r"us:\s*instant bank transfer:\s*([\d.,]+)%\s+of transaction amount\s+\(\$([\d,]+(?:\.\d+)?)\s*min\)", text, re.IGNORECASE)
        result["volumeRates"].append({
            "txnType": "Domestic",
            "speedFlag": "RTP",
            "rate": parse_percent(match.group(1)),
            "payerFunding": "",
            "payeeFunding": "",
            "payeeCardType": "",
            "ccyGroup": "",
            "minVol": 0,
            "maxVol": int(1e9),
            "note": "US Instant Bank Transfer",
        })

    result["volumeRates"] = dedupe_dicts(
        result["volumeRates"],
        ("txnType", "speedFlag", "rate", "payerFunding", "payeeFunding", "payeeCardType", "ccyGroup", "minVol", "maxVol", "note"),
    )


def add_percentage_block_rates(
    text: str,
    result: dict[str, Any],
    marker: str,
    txn_type: str,
    speed_flag: str = "",
    payer_funding: str = "",
    payee_funding: str = "",
    payee_card_type: str = "",
) -> None:
    block = extract_block(text, marker)
    if not block:
        return
    rows = parse_volume_tier_rows(block)
    if rows:
        for min_vol, max_vol, rate in rows:
            result["volumeRates"].append({
                "txnType": txn_type,
                "speedFlag": speed_flag,
                "rate": rate,
                "payerFunding": payer_funding,
                "payeeFunding": payee_funding,
                "payeeCardType": payee_card_type,
                "ccyGroup": "",
                "minVol": min_vol,
                "maxVol": max_vol,
                "note": clean_heading_note(marker),
            })
        return
    flat_rate = search_percent(block, r"([\d.,]+)%")
    if flat_rate is not None:
        result["volumeRates"].append({
            "txnType": txn_type,
            "speedFlag": speed_flag,
            "rate": flat_rate,
            "payerFunding": payer_funding,
            "payeeFunding": payee_funding,
            "payeeCardType": payee_card_type,
            "ccyGroup": "",
            "minVol": 0,
            "maxVol": int(1e9),
            "note": clean_heading_note(marker),
        })


def add_fx_group_rates(block: str, result: dict[str, Any], marker: str, group_name: str) -> None:
    group_block = extract_heading_block(
        block,
        marker,
        stop_markers=["majors", "minors", "tertiary", "named virtual accounts", "monthly minimum fees", "minimum monthly revenue commitment", "monthly fees", "billing & payment timing", "billing and payment timing"],
    )
    if not group_block:
        return
    rows = parse_volume_tier_rows(group_block)
    if rows:
        for min_vol, max_vol, rate in rows:
            result["volumeRates"].append({
                "txnType": "FX",
                "speedFlag": "",
                "rate": rate,
                "payerFunding": "",
                "payeeFunding": "",
                "payeeCardType": "",
                "ccyGroup": group_name,
                "minVol": min_vol,
                "maxVol": max_vol,
                "note": marker.title(),
            })
        return
    flat_rate = search_percent(group_block, r"([\d.,]+)%")
    if flat_rate is not None:
        result["volumeRates"].append({
            "txnType": "FX",
            "speedFlag": "",
            "rate": flat_rate,
            "payerFunding": "",
            "payeeFunding": "",
            "payeeCardType": "",
            "ccyGroup": group_name,
            "minVol": 0,
            "maxVol": int(1e9),
            "note": marker.title(),
        })


def parse_fee_caps(text: str, result: dict[str, Any]) -> None:
    rtp_block = extract_block(text, "instant bank transfer")
    if rtp_block:
        max_cap = search_money(rtp_block, r"cap:\s*\$([\d,]+(?:\.\d+)?)")
        if max_cap is not None:
            result["feeCaps"].append({"productType": "RTP", "capType": "Max Fee", "amount": max_cap})
        min_cap = search_money(rtp_block, r"\(\$([\d,]+(?:\.\d+)?)\s*min\)")
        if min_cap is not None:
            result["feeCaps"].append({"productType": "RTP", "capType": "Min Fee", "amount": min_cap})

    transaction_block = extract_transaction_fees_block(text)
    if transaction_block:
        instant_bank_cap = search_money(transaction_block, r"instant bank(?:\s*\(rtp\))?\s+[\d.,]+%\s*,\s*\$([\d,]+(?:\.\d+)?)\s*cap")
        if instant_bank_cap is not None:
            result["feeCaps"].append({"productType": "RTP", "capType": "Max Fee", "amount": instant_bank_cap})

    if "cross border fx" in text.lower():
        fx_min = search_money(extract_block(text, "cross border fx"), r"\(\$([\d,]+(?:\.\d+)?)\s*min\)")
        if fx_min is not None:
            result["feeCaps"].append({"productType": "FX Majors", "capType": "Min Fee", "amount": fx_min})

    result["feeCaps"] = dedupe_dicts(result["feeCaps"], ("productType", "capType", "amount"))


def parse_reversal_fees(text: str, result: dict[str, Any]) -> None:
    chargeback_fee = search_money(text, r"chargeback fee:\s*\$([\d,]+(?:\.\d+)?)")
    if chargeback_fee is not None:
        result["reversalFees"].append({"payerFunding": "Card", "feePerReversal": chargeback_fee, "note": "Chargeback Fee"})

    nsf_fee = search_money(text, r"nsfs?\s*/\s*reversals?:\s*\$([\d,]+(?:\.\d+)?)")
    if nsf_fee is not None:
        result["reversalFees"].append({"payerFunding": "", "feePerReversal": nsf_fee, "note": "NSFs / Reversals"})

    transaction_block = extract_transaction_fees_block(text)
    if transaction_block:
        nsf_line = search_money(transaction_block, r"(?:^|\n)nsf\s+\$([\d,]+(?:\.\d+)?)")
        if nsf_line is not None:
            result["reversalFees"].append({"payerFunding": "", "feePerReversal": nsf_line, "note": "NSF"})

        reversal_line = search_money(transaction_block, r"(?:^|\n)reversals?\s+\$([\d,]+(?:\.\d+)?)")
        if reversal_line is not None:
            result["reversalFees"].append({"payerFunding": "", "feePerReversal": reversal_line, "note": "Reversals"})

    result["reversalFees"] = dedupe_dicts(result["reversalFees"], ("payerFunding", "feePerReversal", "note"))


def parse_virtual_account_fees(text: str, result: dict[str, Any]) -> None:
    block_candidates = [
        extract_block(text, "named virtual accounts"),
        extract_block(text, "usd virtual account fees"),
    ]
    blocks: list[str] = []
    for candidate in block_candidates:
        normalized = (candidate or "").strip()
        if normalized and normalized not in blocks:
            blocks.append(normalized)
    if not blocks:
        return

    for block in blocks:
        if re.search(r"account opening fee\s+dormancy fee\s*/?\s*month", block, re.IGNORECASE):
            amounts = re.findall(r"\$([\d,]+(?:\.\d+)?)", block)
            if len(amounts) >= 2:
                result["virtualAccountFees"].append({
                    "feeType": "Account Opening",
                    "minAccounts": 1,
                    "maxAccounts": int(1e9),
                    "discount": 0,
                    "feePerAccount": parse_money(amounts[0]),
                    "note": "",
                })
                result["virtualAccountFees"].append({
                    "feeType": "Dormancy",
                    "minAccounts": 1,
                    "maxAccounts": int(1e9),
                    "discount": 0,
                    "feePerAccount": parse_money(amounts[1]),
                    "note": "",
                })

        tier_rows = re.finditer(
            r"(?m)^\s*\d+\s+(?:([\d,]+)\s*-\s*([\d,]+)|([\d,]+)\+)\s+([\d.]+)%\s+\$([\d,]+(?:\.\d+)?)\s+\$([\d,]+(?:\.\d+)?)\s*$",
            block,
        )
        for match in tier_rows:
            range_min = match.group(1)
            range_max = match.group(2)
            plus_min = match.group(3)
            if plus_min is not None:
                min_accounts = int(plus_min.replace(",", ""))
                max_accounts = int(1e9)
            else:
                min_accounts = int(range_min.replace(",", ""))
                max_accounts = int(range_max.replace(",", ""))
            discount = float(match.group(4)) / 100.0
            opening_fee = parse_money(match.group(5))
            dormancy_fee = parse_money(match.group(6))
            result["virtualAccountFees"].append({
                "feeType": "Account Opening",
                "minAccounts": min_accounts,
                "maxAccounts": max_accounts,
                "discount": discount,
                "feePerAccount": opening_fee,
                "note": "",
            })
            result["virtualAccountFees"].append({
                "feeType": "Dormancy",
                "minAccounts": min_accounts,
                "maxAccounts": max_accounts,
                "discount": 0,
                "feePerAccount": dormancy_fee,
                "note": "",
            })

        account_opening = search_money(block, r"account opening fee\s*\$([\d,]+(?:\.\d+)?)")
        if account_opening is not None:
            result["virtualAccountFees"].append({
                "feeType": "Account Opening",
                "minAccounts": 1,
                "maxAccounts": int(1e9),
                "discount": 0,
                "feePerAccount": account_opening,
                "note": "",
            })
        monthly_active = search_money(block, r"monthly active account fee\s*\$([\d,]+(?:\.\d+)?)")
        if monthly_active is not None:
            result["virtualAccountFees"].append({
                "feeType": "Monthly Active",
                "minAccounts": 1,
                "maxAccounts": int(1e9),
                "discount": 0,
                "feePerAccount": monthly_active,
                "note": "",
            })
        dormancy = search_money(block, r"dormancy fee(?: /\s*month)?\s*\$([\d,]+(?:\.\d+)?)")
        if dormancy is not None:
            result["virtualAccountFees"].append({
                "feeType": "Dormancy",
                "minAccounts": 1,
                "maxAccounts": int(1e9),
                "discount": 0,
                "feePerAccount": dormancy,
                "note": "",
            })
        closing = search_money(block, r"account closing fee\s*\$([\d,]+(?:\.\d+)?)")
        if closing is not None:
            result["virtualAccountFees"].append({
                "feeType": "Account Closing",
                "minAccounts": 1,
                "maxAccounts": int(1e9),
                "discount": 0,
                "feePerAccount": closing,
                "note": "",
            })

    result["virtualAccountFees"] = dedupe_dicts(
        result["virtualAccountFees"],
        ("feeType", "minAccounts", "maxAccounts", "discount", "feePerAccount", "note"),
    )


def parse_surcharges(text: str, result: dict[str, Any]) -> None:
    block = extract_block(text, "same currency transaction surcharge")
    if not block:
        return
    rows = parse_volume_tier_rows(block, allow_untiered=True)
    for min_vol, max_vol, rate in rows:
        result["surcharges"].append({
            "surchargeType": "Same Currency",
            "rate": rate,
            "minVol": min_vol,
            "maxVol": max_vol,
            "note": "",
        })
    result["surcharges"] = dedupe_dicts(result["surcharges"], ("surchargeType", "rate", "minVol", "maxVol", "note"))


def parse_other_fees(text: str, result: dict[str, Any]) -> None:
    transaction_block = extract_transaction_fees_block(text)
    if transaction_block:
        fixed_fee_patterns = [
            ("1099 Preparation & Electronic Delivery", r"1099 preparation\s*&\s*electronic delivery\s*\$([\d,]+(?:\.\d+)?)", ""),
            ("1099 Print & Mail", r"1099 print\s*&?\s*mail cost(?:\s*\(incremental cost\))?\s*\$([\d,]+(?:\.\d+)?)", "Incremental mailing cost"),
            ("1042S Preparation & Electronic Delivery", r"1042s preparation\s*&\s*electronic delivery\s*\$([\d,]+(?:\.\d+)?)", ""),
            ("1042S Print & Mail", r"1042s print\s*&\s*mail(?:\s*\(incremental cost\))?.*?\+\s*\$([\d,]+(?:\.\d+)?)", "Actual mailing cost plus markup"),
        ]
        for fee_type, pattern, note in fixed_fee_patterns:
            amount = search_money(transaction_block, pattern)
            if amount is None:
                continue
            result["otherFees"].append({
                "feeType": fee_type,
                "rate": None,
                "amount": amount,
                "note": note,
            })

    fee_patterns = [
        ("Android Pay Transaction Volume Fee", r"android pay transaction volume fee[\s\S]{0,220}?([\d.,]+)%\s+of android pay transaction\s+volume", "Android Pay transaction volume"),
        ("Apple Pay Transaction Volume Fee", r"apple pay transaction volume fee[\s\S]{0,220}?([\d.,]+)%\s+of apple pay transaction\s+volume", "Apple Pay transaction volume"),
        ("International Transaction Percentage Fee", r"international transaction percentage fee[\s\S]{0,220}?([\d.,]+)%\s+of settled international transaction\s+volume", "Settled international transaction volume"),
    ]
    for fee_type, pattern, note in fee_patterns:
        rate = search_percent(text, pattern)
        if rate is None:
            continue
        result["revShareFees"].append({
            "feeType": fee_type,
            "rate": rate,
            "amount": None,
            "note": note,
        })

    attempted_fee = search_money(text, r"international attempted transaction fee[\s\S]{0,180}?\$([\d,]+(?:\.\d+)?)\s+per attempted transaction")
    if attempted_fee is not None:
        result["revShareFees"].append({
            "feeType": "International Attempted Transaction Fee",
            "rate": None,
            "amount": attempted_fee,
            "note": "Per attempted transaction",
        })

    settled_rate = search_percent(text, r"international settled transaction fee\s+flat fee\s*\(([\d.,]+)%\)")
    if settled_rate is not None:
        result["revShareFees"].append({
            "feeType": "International Settled Transaction Fee",
            "rate": settled_rate,
            "amount": None,
            "note": "Inferred from sample fee calculation",
        })

    result["otherFees"] = dedupe_dicts(result["otherFees"], ("feeType", "rate", "amount", "note"))
    result["revShareFees"] = dedupe_dicts(result["revShareFees"], ("feeType", "rate", "amount", "note"))


def parse_rev_share_terms(text: str, result: dict[str, Any], warnings: list[str]) -> None:
    rev_share_block = extract_block(text, "rev share:")
    if not rev_share_block:
        return

    tier_rows = parse_percent_tier_rows(rev_share_block)
    for min_vol, max_vol, rate in tier_rows:
        result["revShareTiers"].append({
            "minVol": min_vol,
            "maxVol": max_vol,
            "revSharePct": rate,
            "note": "Virtual Debit Card Interchange Revenue Share",
        })

    if "per settled transaction (flat fee)" in rev_share_block.lower():
        warnings.append("Detected an international settled transaction fee with no explicit numeric amount in the schedule. Review that card fee manually.")

    result["revShareTiers"] = dedupe_dicts(result["revShareTiers"], ("minVol", "maxVol", "revSharePct", "note"))


def parse_minimums(text: str, result: dict[str, Any], warnings: list[str]) -> None:
    block = extract_block(text, "minimum monthly revenue commitment") or extract_block(text, "monthly minimum fees") or extract_block(text, "monthly fees")
    if not block:
        return

    rows = parse_minimum_rows(block)
    if rows:
        result["minimums"].extend(rows)
        result["minimums"] = dedupe_dicts(result["minimums"], ("minAmount", "minVol", "maxVol", "note"))
        return

    if re.search(r"period minimum (?:fees|monthly revenue)", block, re.IGNORECASE) or re.search(r"month\s+\d", block, re.IGNORECASE):
        warnings.append("Detected a period-based or ramping monthly minimum schedule. Review minimum rows manually after import.")
        flat_amounts = re.findall(r"\$\s*([\d,]+(?:\.\d+)?)", block)
        if flat_amounts and not result["minimums"]:
            result["minimums"].append({
                "minAmount": parse_money(flat_amounts[-1]),
                "minVol": 0,
                "maxVol": int(1e9),
                "note": "Latest ramp minimum",
            })
        return

    flat_zero = search_money(block, r"\$([\d,]+(?:\.\d+)?)")
    if flat_zero is not None:
        result["minimums"].append({
            "minAmount": flat_zero,
            "minVol": 0,
            "maxVol": int(1e9),
            "note": "",
        })
        result["minimums"] = dedupe_dicts(result["minimums"], ("minAmount", "minVol", "maxVol", "note"))


def parse_count_tier_rows(block: str) -> list[tuple[int, int, float]]:
    rows: list[tuple[int, int, float]] = []
    for line in block.splitlines():
        clean = line.strip()
        if not clean:
            continue
        range_match = re.match(r"^\d+\s+([\d,]+)\s*-\s*([\d,]+)\s+\$([\d,]+(?:\.\d+)?)$", clean)
        plus_match = re.match(r"^\d+\s+([\d,]+)\+\s+\$([\d,]+(?:\.\d+)?)$", clean)
        if range_match:
            rows.append((
                int(range_match.group(1).replace(",", "")),
                int(range_match.group(2).replace(",", "")),
                parse_money(range_match.group(3)),
            ))
        elif plus_match:
            rows.append((
                int(plus_match.group(1).replace(",", "")),
                int(1e9),
                parse_money(plus_match.group(2)),
            ))
    return rows


def parse_volume_tier_rows(block: str, allow_untiered: bool = False) -> list[tuple[int, int, float]]:
    rows: list[tuple[int, int, float]] = []
    patterns = [
        r"^\s*(?:tier\s+)?\d+\s+(?:usd\s+)?\$?([\d.,]+[mk]?)\s*-\s*(?:usd\s+)?\$?([\d.,]+[mk]?)\s+([\d.,]+)%",
        r"^\s*(?:tier\s+)?\d+\s+(?:usd\s+)?\$?([\d.,]+[mk]?)\+\s+([\d.,]+)%",
        r"^\s*\$?([\d.,]+[mk]?)\s*-\s*\$?([\d.,]+[mk]?)\s+([\d.,]+)%\s*(?:\(|$)",
        r"^\s*\$?([\d.,]+[mk]?)\+\s+([\d.,]+)%\s*(?:\(|$)",
    ]
    for line in block.splitlines():
        clean = line.strip().lower().replace("–", "-")
        if not clean:
            continue
        matched = False
        for idx, pattern in enumerate(patterns):
            match = re.match(pattern, clean, re.IGNORECASE)
            if not match:
                continue
            if idx in (0, 2):
                min_vol = parse_volume_number(match.group(1))
                max_vol = parse_volume_number(match.group(2))
                rate = parse_percent(match.group(3))
            else:
                min_vol = parse_volume_number(match.group(1))
                max_vol = int(1e9)
                rate = parse_percent(match.group(2))
            rows.append((min_vol, max_vol, rate))
            matched = True
            break
        if matched:
            continue
    if rows or not allow_untiered:
        return rows
    flat = search_percent(block, r"([\d.,]+)%")
    if flat is None:
        return []
    return [(0, int(1e9), flat)]


def parse_percent_tier_rows(block: str) -> list[tuple[int, int, float]]:
    rows: list[tuple[int, int, float]] = []
    patterns = [
        r"^\s*(?:tier\s+)?\d+\s+\$?([\d.,]+[mk]?)\s*-\s*\$?([\d.,]+[mk]?)\s+([\d.,]+)%",
        r"^\s*(?:tier\s+)?\d+\s+\$?([\d.,]+[mk]?)\+\s+([\d.,]+)%",
    ]
    for line in block.splitlines():
        clean = line.strip().lower().replace("–", "-")
        if not clean:
            continue
        for idx, pattern in enumerate(patterns):
            match = re.match(pattern, clean, re.IGNORECASE)
            if not match:
                continue
            if idx == 0:
                rows.append((
                    parse_volume_number(match.group(1)),
                    parse_volume_number(match.group(2)),
                    parse_percent(match.group(3)),
                ))
            else:
                rows.append((
                    parse_volume_number(match.group(1)),
                    int(1e9),
                    parse_percent(match.group(2)),
                ))
            break
    return rows


def parse_minimum_rows(block: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line in block.splitlines():
        clean = line.strip().replace("–", "-")
        if not clean:
            continue
        range_match = re.match(r"^\d+\s+\$?([\d.,]+[mk]?)\s*-\s*\$?([\d.,]+[mk]?)\s+\$([\d,]+(?:\.\d+)?)$", clean, re.IGNORECASE)
        plus_match = re.match(r"^\d+\s+\$?([\d.,]+[mk]?)\+\s+\$([\d,]+(?:\.\d+)?)$", clean, re.IGNORECASE)
        n_a_match = re.match(r"^(?:\d+\s+)?n/?a\s+(?:usd\s+)?\$?([\d,]+(?:\.\d+)?)$", clean, re.IGNORECASE)
        if range_match:
            rows.append({
                "minAmount": parse_money(range_match.group(3)),
                "minVol": parse_volume_number(range_match.group(1)),
                "maxVol": parse_volume_number(range_match.group(2)),
                "note": "",
            })
        elif plus_match:
            rows.append({
                "minAmount": parse_money(plus_match.group(2)),
                "minVol": parse_volume_number(plus_match.group(1)),
                "maxVol": int(1e9),
                "note": "",
            })
        elif n_a_match:
            rows.append({
                "minAmount": parse_money(n_a_match.group(1)),
                "minVol": 0,
                "maxVol": int(1e9),
                "note": "",
            })
    return rows


def extract_block(text: str, marker: str, window: int = 3200, ignored_stops: set[str] | None = None) -> str:
    lowered = text.lower()
    start = lowered.find(marker.lower())
    if start == -1:
        return ""
    end = min(len(text), start + window)
    ignored = {item.lower() for item in (ignored_stops or set())}
    for stop in SECTION_STOP_MARKERS:
        if stop in ignored:
            continue
        if stop == marker.lower() or stop in marker.lower() or marker.lower() in stop:
            continue
        idx = lowered.find(stop, start + len(marker))
        if idx != -1 and idx < end:
            end = idx
    return text[start:end].strip()


def extract_transaction_fees_block(text: str) -> str:
    lowered = text.lower()
    start = lowered.find("transaction fees")
    if start == -1:
        return ""
    end = len(text)
    for marker in ["foreign exchange fees", "foreign exchange", "volume range virtual debit card interchange revenue share", "schedule b", "exhibit b"]:
        idx = lowered.find(marker, start + 1)
        if idx != -1 and idx < end:
            end = idx
    return text[start:end].strip()


def extract_heading_block(text: str, marker: str, *, stop_markers: list[str], window: int = 3200) -> str:
    if marker.lower() == "majors":
        heading_pattern = r"(^|\n)majors?(?: currencies)?:?\b"
    elif marker.lower() == "minors":
        heading_pattern = r"(^|\n)minors\b"
    elif marker.lower() == "tertiary":
        heading_pattern = r"(^|\n)tertiary\b"
    else:
        heading_pattern = rf"(^|\n){re.escape(marker)}\b"
    match = re.search(heading_pattern, text, re.IGNORECASE)
    if not match:
        return ""
    start = match.start()
    end = min(len(text), start + window)
    for stop in stop_markers:
        if stop.lower() == marker.lower():
            continue
        stop_match = re.search(rf"(^|\n){re.escape(stop)}\b", text[start + len(marker) :], re.IGNORECASE)
        if stop_match:
            candidate = start + len(marker) + stop_match.start()
            if candidate < end:
                end = candidate
    return text[start:end].strip()


def push_offline_rate(
    result: dict[str, Any],
    txn_type: str,
    speed_flag: str,
    fee: float | None,
    *,
    min_amt: int = 0,
    max_amt: int = int(1e9),
    payer_ccy: str = "USD",
    payee_ccy: str | None = None,
    payer_country: str = "",
    payee_country: str = "",
    payer_country_group: str = "",
    payee_country_group: str = "",
    processing_method: str = "",
    note: str = "",
) -> None:
    if fee is None:
        return
    result["offlineRates"].append({
        "txnType": txn_type,
        "speedFlag": speed_flag,
        "minAmt": min_amt,
        "maxAmt": max_amt,
        "fee": fee,
        "payerCcy": payer_ccy,
        "payeeCcy": payee_ccy if payee_ccy is not None else ("USD" if txn_type != "FX" else ""),
        "payerCountry": payer_country,
        "payeeCountry": payee_country,
        "payerCountryGroup": payer_country_group,
        "payeeCountryGroup": payee_country_group,
        "processingMethod": processing_method,
        "note": note,
    })


def search_money(text: str, pattern: str) -> float | None:
    match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
    return parse_money(match.group(1)) if match else None


def search_percent(text: str, pattern: str) -> float | None:
    match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
    return parse_percent(match.group(1)) if match else None


def parse_money(value: str | float | int | None) -> float | None:
    if value is None:
        return None
    clean = str(value).strip().replace("$", "").replace(",", "")
    if not clean:
        return None
    try:
        return float(clean)
    except ValueError:
        return None


def parse_percent(value: str | float | int | None) -> float | None:
    if value is None:
        return None
    clean = str(value).strip().replace("%", "").replace(",", "")
    if not clean:
        return None
    try:
        return float(clean) / 100.0
    except ValueError:
        return None


def parse_volume_number(value: str) -> int:
    clean = str(value).strip().lower().replace("$", "").replace(",", "")
    multiplier = 1
    if clean.endswith("m"):
        multiplier = 1_000_000
        clean = clean[:-1]
    elif clean.endswith("k"):
        multiplier = 1_000
        clean = clean[:-1]
    number = float(clean or 0)
    return int(round(number * multiplier))


def clean_heading_note(marker: str) -> str:
    return re.sub(r"\s+", " ", marker).strip().title()


def dedupe_dicts(rows: list[dict[str, Any]], keys: tuple[str, ...]) -> list[dict[str, Any]]:
    seen: set[tuple[Any, ...]] = set()
    deduped: list[dict[str, Any]] = []
    for row in rows:
        signature = tuple(row.get(key) for key in keys)
        if signature in seen:
            continue
        seen.add(signature)
        deduped.append(row)
    return deduped


def dedupe_list(values: list[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for value in values:
        clean = str(value).strip()
        if not clean or clean in seen:
            continue
        seen.add(clean)
        deduped.append(clean)
    return deduped

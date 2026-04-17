"""Archival reference invoice engine.

The long-term billing calculation target is AWS + S3 + n8n, with the browser
serving as the maker UI and the workflow layer owning the heavy computation.
"""

from __future__ import annotations

from collections import OrderedDict
from copy import deepcopy
from datetime import date, datetime
from typing import Any


MAJORS = "AUD,CAD,CHF,CNY,DKK,EUR,GBP,HKD,JPY,NOK,NZD,PHP,SEK,SGD,USD"
MINORS = "AED,BBD,BDT,BGN,BHD,BMD,BND,BRL,BSD,BWP,BZD,CRC,CZK,DOP,DZD,EGP,ETB,FJD,GHS,GTQ,GYD,HTG,HUF,IDR,ILS,INR,ISK,JMD,JOD,KES,KWD,KYD,KZT,LBP,LKR,MAD,MOP,MUR,MWK,MXN,MZN,NGN,OMR,PEN,PGK,PKR,PLN,QAR,RON,RUB,RWF,SAR,SBD,THB,TND,TOP,TRY,TTD,TZS,UGX,UYU,VND,VUV,WST,XAF,XCD,XOF,ZAR,ZMW"
TERTIARY = "ALL,AMD,ANG,AOA,ARS,AWG,AZN,BAM,BIF,BOB,BTN,BYN,CDF,CLP,COP,CVE,DJF,ERN,FKP,GEL,GIP,GMD,GNF,HNL,KGS,KHR,KMF,KRW,LAK,LRD,LSL,LYD,MDL,MGA,MKD,MMK,MNT,MRU,MVR,MYR,NAD,NIO,NPR,PAB,PYG,RSD,SCR,SHP,SLE,SRD,SSP,STN,SVC,SZL,TJS,TMT,TWD,UAH,UZS,VES,XPF,YER,ZWD"
MAJOR_CCYS = set(MAJORS.split(","))
MINOR_CCYS = set(MINORS.split(","))
TERTIARY_CCYS = set(TERTIARY.split(","))
EEA_COUNTRY_TOKENS = {
    "at", "austria", "be", "belgium", "bg", "bulgaria", "hr", "croatia", "cy", "cyprus",
    "cz", "czechrepublic", "dk", "denmark", "ee", "estonia", "fi", "finland", "fr", "france",
    "de", "germany", "gr", "greece", "hu", "hungary", "is", "iceland", "ie", "ireland",
    "it", "italy", "lv", "latvia", "li", "liechtenstein", "lt", "lithuania", "lu", "luxembourg",
    "mt", "malta", "nl", "netherlands", "no", "norway", "pl", "poland", "pt", "portugal",
    "ro", "romania", "sk", "slovakia", "si", "slovenia", "es", "spain", "se", "sweden",
}
COUNTRY_GROUP_TOKENS = {
    "CA": {"ca", "canada"},
    "UK": {"uk", "gb", "gbr", "unitedkingdom", "greatbritain", "england", "scotland", "wales", "northernireland"},
    "AU": {"au", "aus", "australia"},
    "US": {"us", "usa", "unitedstates", "unitedstatesofamerica"},
    "EEA": EEA_COUNTRY_TOKENS,
}


def norm(value: Any) -> str:
    return str(value or "").strip().lower()


def to_float(value: Any) -> float:
    if value in (None, ""):
        return 0.0
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        text = str(value).strip().replace("$", "").replace(",", "")
        if text.startswith("(") and text.endswith(")"):
            text = f"-{text[1:-1]}"
        return float(text or 0)


def fmt(n: Any) -> str:
    return f"${to_float(n):,.2f}"


def fmt_pct(n: Any) -> str:
    return f"{to_float(n) * 100:.4f}%"


def format_period_label(period: str) -> str:
    year, month = [int(part) for part in str(period).split("-", 1)]
    return datetime(year, month, 1).strftime("%B %Y")


def format_period_boundary(period: str, boundary: str) -> str:
    year, month = [int(part) for part in str(period).split("-", 1)]
    if boundary == "end":
        if month == 12:
            dt = datetime(year + 1, 1, 1) - datetime.resolution
        else:
            dt = datetime(year, month + 1, 1) - datetime.resolution
    else:
        dt = datetime(year, month, 1)
    return f"{dt.strftime('%B')} {dt.day}, {dt.year}"


def parse_date(value: Any) -> date | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text[:10]).date()
    except ValueError:
        return None


def compare_periods(a: str, b: str) -> int:
    a_text = str(a or "")
    b_text = str(b or "")
    if a_text < b_text:
        return -1
    if a_text > b_text:
        return 1
    return 0


def normalize_month_key(value: Any) -> str:
    return str(value or "").strip()[:7]


def period_matches_schedule(period: str, start: Any, end: Any) -> bool:
    month = normalize_month_key(period)
    range_start = normalize_month_key(start)
    range_end = normalize_month_key(end)
    if range_start and compare_periods(month, range_start) < 0:
        return False
    if range_end and compare_periods(month, range_end) > 0:
        return False
    return True


def is_partner_active_for_period(snapshot: dict[str, Any], partner: str, period: str) -> bool:
    rows = [
        row for row in snapshot.get("pActive", [])
        if norm(row.get("partner")) == norm(partner) and period_matches_schedule(period, row.get("startPeriod"), row.get("endPeriod"))
    ]
    if not rows:
        return True
    if any(norm(row.get("status") or "Active") == "inactive" for row in rows):
        return False
    return True


def get_partner_contract_start_date(snapshot: dict[str, Any], partner: str) -> str:
    config = next((row for row in snapshot.get("pBilling", []) if norm(row.get("partner")) == norm(partner)), None)
    explicit = str(config.get("contractStartDate") or "").strip() if config else ""
    if explicit:
        return explicit[:10]
    candidates: list[str] = []
    for key in ["off", "vol", "fxRates", "cap", "rs", "mins", "plat", "revf", "impl", "vaFees", "surch"]:
        for row in snapshot.get(key, []):
            if norm(row.get("partner")) != norm(partner):
                continue
            start_date = str(row.get("startDate") or "").strip()
            if start_date:
                candidates.append(start_date[:10])
    return min(candidates) if candidates else ""


def get_partner_go_live_date(snapshot: dict[str, Any], partner: str) -> str:
    config = next((row for row in snapshot.get("pBilling", []) if norm(row.get("partner")) == norm(partner)), None)
    if config and "goLiveDate" in config:
        explicit = str(config.get("goLiveDate") or "").strip()[:10]
        if explicit:
            return explicit
    impl_row = next((row for row in snapshot.get("impl", []) if norm(row.get("partner")) == norm(partner) and row.get("feeType") == "Implementation" and str(row.get("goLiveDate") or "").strip()), None)
    return str(impl_row.get("goLiveDate") or "").strip()[:10] if impl_row else ""


def is_partner_not_yet_live(snapshot: dict[str, Any], partner: str) -> bool:
    config = next((row for row in snapshot.get("pBilling", []) if norm(row.get("partner")) == norm(partner)), None)
    return bool(config.get("notYetLive")) if config else False


def partner_has_imported_activity_through_period(snapshot: dict[str, Any], partner: str, period: str) -> bool:
    target_period = normalize_month_key(period)
    if not target_period:
        return False
    for key in ("ltxn", "lrev", "lrs", "lfxp", "lva"):
        for row in snapshot.get(key, []):
            if norm(row.get("partner")) != norm(partner):
                continue
            row_period = normalize_month_key(
                row.get("period")
                or row.get("refundPeriod")
                or row.get("creditCompleteMonth")
                or ""
            )
            if row_period and compare_periods(row_period, target_period) <= 0:
                return True
    return False


def is_recurring_billing_live_for_period(snapshot: dict[str, Any], partner: str, period: str) -> bool:
    config = next((row for row in snapshot.get("pBilling", []) if norm(row.get("partner")) == norm(partner)), None)
    explicit_go_live_month = normalize_month_key(str(config.get("goLiveDate") or "").strip()) if config else ""
    if config and bool(config.get("notYetLive")):
        # Treat parser-derived go-live hints on implementation rows as planning metadata.
        # Recurring billing should not start until the partner billing profile is marked
        # live or given an explicit go-live date.
        if explicit_go_live_month:
            return compare_periods(normalize_month_key(period), explicit_go_live_month) >= 0
        return False
    go_live_month = normalize_month_key(get_partner_go_live_date(snapshot, partner))
    if not go_live_month:
        return True
    return compare_periods(normalize_month_key(period), go_live_month) >= 0


def get_implementation_billing_date(snapshot: dict[str, Any], partner: str, row: dict[str, Any]) -> str:
    explicit = str(row.get("billingDate") or "").strip()[:10]
    if explicit:
        return explicit
    return str(row.get("startDate") or "").strip()[:10] or get_partner_contract_start_date(snapshot, partner) or str(row.get("goLiveDate") or "").strip()[:10]


def normalize_implementation_credit_mode(row: dict[str, Any]) -> str:
    raw = str(row.get("creditMode") or "").strip().lower().replace(" ", "_")
    if raw:
        return raw
    if row.get("applyAgainstMin"):
        return "monthly_minimum"
    return ""


def get_implementation_credit_amount(row: dict[str, Any]) -> float:
    explicit = to_float(row.get("creditAmount"))
    if explicit > 0:
        return explicit
    return to_float(row.get("feeAmount")) if normalize_implementation_credit_mode(row) else 0.0


def get_implementation_credit_window_days(row: dict[str, Any]) -> int:
    return int(to_float(row.get("creditWindowDays")))


def get_implementation_credit_start_period(snapshot: dict[str, Any], partner: str, row: dict[str, Any]) -> str:
    mode = normalize_implementation_credit_mode(row)
    credit_amount = get_implementation_credit_amount(row)
    if not mode or credit_amount <= 0:
        return ""
    go_live_date = str(get_partner_go_live_date(snapshot, partner) or row.get("goLiveDate") or "").strip()[:10]
    if not go_live_date:
        return ""
    billing_date = str(get_implementation_billing_date(snapshot, partner, row) or "").strip()[:10]
    credit_window_days = get_implementation_credit_window_days(row)
    if credit_window_days > 0 and billing_date:
        billing = parse_date(billing_date)
        go_live = parse_date(go_live_date)
        if billing and go_live and (go_live - billing).days > credit_window_days:
            return ""
    return normalize_month_key(go_live_date)


def implementation_credit_label(mode: str) -> str:
    if mode == "monthly_minimum":
        return "monthly minimum"
    if mode == "monthly_subscription":
        return "monthly subscription"
    return "future fees"


def normalize_period_range(start_period: str, end_period: str | None = None) -> tuple[str, str]:
    start_text = str(start_period or "")
    end_text = str(end_period or start_text or "")
    if compare_periods(start_text, end_text) > 0:
        start_text, end_text = end_text, start_text
    return start_text, end_text


def enumerate_periods(start_period: str, end_period: str | None = None) -> list[str]:
    range_start, range_end = normalize_period_range(start_period, end_period)
    start_year, start_month = [int(part) for part in range_start.split("-", 1)]
    end_year, end_month = [int(part) for part in range_end.split("-", 1)]
    periods: list[str] = []
    year = start_year
    month = start_month
    while (year, month) <= (end_year, end_month):
        periods.append(f"{year:04d}-{month:02d}")
        month += 1
        if month > 12:
            month = 1
            year += 1
    return periods


def in_range(day: str, start: Any, end: Any) -> bool:
    if not day or not start:
        return True
    day_value = parse_date(day)
    start_value = parse_date(start)
    if not day_value or not start_value:
        return True
    if day_value < start_value:
        return False
    end_value = parse_date(end)
    if end_value and day_value > end_value:
        return False
    return True


def optional_match(rule_value: Any, actual_value: Any) -> bool:
    return not str(rule_value or "").strip() or norm(rule_value) == norm(actual_value)


def normalize_country_token(value: Any) -> str:
    return "".join(char for char in norm(value) if char.isalnum())


def country_in_group(country_value: Any, group_value: Any) -> bool:
    token = normalize_country_token(country_value)
    group = str(group_value or "").strip().upper()
    if not token or not group:
        return False
    return token in COUNTRY_GROUP_TOKENS.get(group, {normalize_country_token(group)})


def optional_country_match(rule_country: Any, rule_group: Any, actual_country: Any) -> bool:
    if str(rule_country or "").strip():
        return optional_match(rule_country, actual_country)
    if str(rule_group or "").strip():
        return country_in_group(actual_country, rule_group)
    return True


def txn_matches_pricing_row(rule: dict[str, Any], txn: dict[str, Any]) -> bool:
    return (
        optional_match(rule.get("txnType"), txn.get("txnType"))
        and optional_match(rule.get("speedFlag"), txn.get("speedFlag"))
        and optional_match(rule.get("payerFunding"), txn.get("payerFunding"))
        and optional_match(rule.get("payeeFunding"), txn.get("payeeFunding"))
        and optional_match(rule.get("payeeCardType"), txn.get("payeeCardType"))
        and optional_match(rule.get("payerCcy"), txn.get("payerCcy"))
        and optional_match(rule.get("payeeCcy"), txn.get("payeeCcy"))
        and optional_country_match(rule.get("payerCountry"), rule.get("payerCountryGroup"), txn.get("payerCountry"))
        and optional_country_match(rule.get("payeeCountry"), rule.get("payeeCountryGroup"), txn.get("payeeCountry"))
        and optional_match(rule.get("processingMethod"), txn.get("processingMethod"))
    )


def is_calendar_year_end_period(period: str) -> bool:
    return normalize_month_key(period).endswith("-12")


def is_year_end_account_setup_row(row: dict[str, Any]) -> bool:
    return "year-end active" in norm(row.get("note"))


def get_corridor(ccy: str) -> str:
    ccy = str(ccy or "").upper().strip()
    if ccy in MAJOR_CCYS:
        return "Major"
    if ccy in MINOR_CCYS:
        return "Minor"
    if ccy in TERTIARY_CCYS:
        return "Tertiary"
    return ""


def is_cross_border_transaction(txn: dict[str, Any]) -> bool:
    txn_type = norm(txn.get("txnType"))
    if txn_type in {"fx", "usd abroad", "payout"}:
        return True
    payer_country = str(txn.get("payerCountry") or "").upper().strip()
    payee_country = str(txn.get("payeeCountry") or "").upper().strip()
    if payer_country and payee_country and payer_country != payee_country:
        return True
    payer_ccy = str(txn.get("payerCcy") or "").upper().strip()
    payee_ccy = str(txn.get("payeeCcy") or "").upper().strip()
    return bool(payer_ccy and payee_ccy and payer_ccy != payee_ccy)


def rev_share_direction(txn: dict[str, Any]) -> str:
    return "In" if norm(txn.get("txnType")) == "payin" else "Out"


def rev_share_scope_matches(share: dict[str, Any], txn: dict[str, Any]) -> bool:
    share_type = norm(share.get("txnType"))
    txn_type = norm(txn.get("txnType"))
    if share_type == "payin":
        type_match = txn_type == "payin"
    elif share_type == "payout":
        type_match = txn_type != "payin" and rev_share_direction(txn) == "Out"
    else:
        type_match = optional_match(share.get("txnType"), txn.get("txnType"))
    return type_match and optional_match(share.get("speedFlag"), txn.get("speedFlag"))


def txn_average_size(txn: dict[str, Any]) -> float:
    avg_size = to_float(txn.get("avgTxnSize"))
    if avg_size > 0:
        return avg_size
    txn_count = to_float(txn.get("txnCount"))
    total_volume = to_float(txn.get("totalVolume"))
    return total_volume / txn_count if txn_count > 0 else 0.0


def rev_share_cost_tokens(txn: dict[str, Any]) -> list[str]:
    txn_type = norm(txn.get("txnType"))
    processing_method = norm(txn.get("processingMethod"))
    speed_flag = norm(txn.get("speedFlag"))
    if txn_type == "fx":
        return ["wire transfer - fx", "wire transfer", "wire"]
    if txn_type == "usd abroad":
        return ["wire transfer - usd", "wire transfer", "wire"]
    if txn_type == "payout" or is_cross_border_transaction(txn):
        return ["wire transfer", "wire"]
    if speed_flag == "rtp" or processing_method == "rtp":
        return ["instant payments", "rtp"]
    if processing_method in {"ach", "nacha", "eft", "sepa"}:
        return (["ach - same day"] if speed_flag == "fasterach" else []) + ["ach", "sepa", "nacha", "eft"]
    if processing_method in {"card", "wallet", "push"}:
        return [processing_method]
    return [processing_method] if processing_method else ["ach"]


def _find_rev_share_cost_row(
    snapshot: dict[str, Any],
    txn: dict[str, Any],
    period: str,
    tokens: list[str],
    *,
    volume_band: bool = False,
) -> dict[str, Any] | None:
    direction = rev_share_direction(txn)
    cross_border = is_cross_border_transaction(txn)
    partner = str(txn.get("partner") or "")
    avg_txn_size = txn_average_size(txn)
    total_volume = to_float(txn.get("totalVolume"))
    tokens = [norm(token) for token in tokens if token]
    candidates: list[tuple[int, dict[str, Any]]] = []
    for cost in snapshot.get("pCosts", []):
        if cost.get("direction") != direction:
            continue
        if cost.get("partner") and cost.get("partner") != partner:
            continue
        if (cost.get("feeType") or "Per Item") != "Per Item":
            continue
        if norm(cost.get("paymentOrChargeback") or "Payment") not in {"payment", ""}:
            continue
        if not in_range(f"{period}-15", cost.get("startDate"), cost.get("endDate")):
            continue
        txn_name = norm(cost.get("txnName"))
        if not any(token in txn_name for token in tokens):
            continue
        min_amt = to_float(cost.get("minAmt"))
        max_amt = to_float(cost.get("maxAmt"))
        band_value = total_volume if volume_band else avg_txn_size
        if max_amt > 0 and band_value > 0 and not (band_value >= min_amt and band_value <= max_amt):
            continue
        corridor = norm(cost.get("corridorType"))
        score = 0
        if cross_border:
            if "cross border" in corridor:
                score += 50
            elif "cross-border" in corridor:
                score += 50
            elif "domestic/cross border" in corridor or "domestic/cross-border" in corridor:
                score += 35
            elif corridor == "domestic":
                continue
        else:
            if corridor == "domestic":
                score += 50
            elif "domestic" in corridor:
                score += 35
            elif "cross border" in corridor or "cross-border" in corridor:
                continue
        token_scores = [30 - index * 4 for index, token in enumerate(tokens) if token in txn_name]
        if token_scores:
            score += max(token_scores)
        if max_amt > 0 and band_value > 0:
            score += 10
        candidates.append((score, cost))
    if not candidates:
        return None
    candidates.sort(key=lambda item: (item[0], to_float(item[1].get("fee"))), reverse=True)
    return candidates[0][1]


def find_rev_share_cost_row(snapshot: dict[str, Any], txn: dict[str, Any], period: str) -> dict[str, Any] | None:
    return _find_rev_share_cost_row(snapshot, txn, period, rev_share_cost_tokens(txn))


def calculate_rev_share_cost(snapshot: dict[str, Any], txn: dict[str, Any], period: str) -> float:
    txn_count = to_float(txn.get("txnCount"))
    total_volume = to_float(txn.get("totalVolume"))
    total_cost = 0.0
    seen_cost_keys: set[tuple[str, str, str]] = set()

    def add_cost(row: dict[str, Any] | None, amount: float) -> None:
        nonlocal total_cost
        if not row or amount <= 0:
            return
        key = (
            str(row.get("txnName") or ""),
            str(row.get("corridorType") or ""),
            str(row.get("fee") or ""),
        )
        if key in seen_cost_keys:
            return
        seen_cost_keys.add(key)
        total_cost += amount

    primary_row = find_rev_share_cost_row(snapshot, txn, period)
    if primary_row:
        add_cost(primary_row, to_float(primary_row.get("fee")) * txn_count)

    txn_type = norm(txn.get("txnType"))
    payee_ccy = str(txn.get("payeeCcy") or "").upper().strip()
    if txn_type == "fx" and total_volume > 0:
        conversion_row = _find_rev_share_cost_row(
            snapshot,
            txn,
            period,
            ["conversion volume fee"],
            volume_band=True,
        )
        if conversion_row:
            add_cost(conversion_row, to_float(conversion_row.get("fee")) * total_volume)
    if txn_type in {"fx", "usd abroad"} and payee_ccy:
        local_payment_row = _find_rev_share_cost_row(
            snapshot,
            txn,
            period,
            [f"local payment fee ({payee_ccy.lower()})"],
        )
        if local_payment_row:
            add_cost(local_payment_row, to_float(local_payment_row.get("fee")) * txn_count)
    return round(total_cost, 2)


def activity_row_key(row: dict[str, Any]) -> str:
    parts = [
        row.get("period"),
        row.get("partner"),
        row.get("txnType"),
        row.get("speedFlag"),
        row.get("processingMethod"),
        row.get("payerFunding"),
        row.get("payeeFunding"),
        row.get("payerCcy"),
        row.get("payeeCcy"),
        row.get("txnCount"),
        row.get("totalVolume"),
        row.get("customerRevenue"),
        row.get("estRevenue"),
        row.get("avgTxnSize"),
        row.get("revenueBasis"),
    ]
    return "|".join(str(part or "") for part in parts)


def summarize_invoice_group(group: dict[str, Any]) -> str:
    parts: list[str] = []
    if group["activityRows"]:
        parts.append(f'{group["activityRowCount"]} imported row{"s" if group["activityRowCount"] != 1 else ""}')
        if group["activityTxnCount"] > 0:
            parts.append(f'{int(group["activityTxnCount"]):,} txns')
        if group["activityVolume"] > 0:
            parts.append(f'{fmt(group["activityVolume"])} volume')
    if len(group["lines"]) > 1:
        parts.append(f'{len(group["lines"])} calc lines')
    if not parts:
        parts.append(group["lines"][0]["desc"] if group["lines"] else "")
    return " · ".join(parts)


def calculate_active_invoice_totals(lines: list[dict[str, Any]]) -> dict[str, float]:
    totals = {"chg": 0.0, "pay": 0.0, "offset": 0.0}
    for line in lines:
        if line.get("active") is False:
            continue
        amount = to_float(line.get("amount"))
        direction = line.get("dir")
        if direction == "charge":
            totals["chg"] += amount
        elif direction == "pay":
            totals["pay"] += amount
        elif direction == "offset":
            totals["offset"] += amount
            totals["pay"] += amount
    return totals


def group_invoice_lines(lines: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: list[dict[str, Any]] = []
    mapping: OrderedDict[str, dict[str, Any]] = OrderedDict()
    for index, line in enumerate(lines):
        key = line.get("groupKey") or f'{line.get("cat")}|{line.get("dir")}|{line.get("desc")}'
        group = mapping.get(key)
        if group is None:
            group = {
                "id": f"invoice-group-{index}",
                "key": key,
                "cat": line.get("cat"),
                "dir": line.get("dir"),
                "label": line.get("groupLabel") or line.get("desc"),
                "lines": [],
                "charge": 0.0,
                "pay": 0.0,
                "offset": 0.0,
                "displayCharge": 0.0,
                "displayPay": 0.0,
                "displayOffset": 0.0,
                "activityRows": [],
                "activityRowCount": 0,
                "activityTxnCount": 0.0,
                "activityVolume": 0.0,
                "isInactive": False,
                "hasInactiveLines": False,
            }
            mapping[key] = group
            groups.append(group)
        group["lines"].append(line)
        amount = to_float(line.get("amount"))
        if line.get("dir") == "charge":
            group["displayCharge"] += amount
            if line.get("active") is not False:
                group["charge"] += amount
        elif line.get("dir") == "pay":
            group["displayPay"] += amount
            if line.get("active") is not False:
                group["pay"] += amount
        elif line.get("dir") == "offset":
            group["displayOffset"] += amount
            if line.get("active") is not False:
                group["offset"] += amount

    for group in groups:
        group["isInactive"] = all(line.get("active") is False for line in group["lines"])
        group["hasInactiveLines"] = any(line.get("active") is False for line in group["lines"])
        activity_map: OrderedDict[str, dict[str, Any]] = OrderedDict()
        for line in group["lines"]:
            for row in line.get("activityRows") or []:
                activity_map[activity_row_key(row)] = row
        group["activityRows"] = list(activity_map.values())
        group["activityRowCount"] = len(group["activityRows"])
        group["activityTxnCount"] = sum(to_float(row.get("txnCount")) for row in group["activityRows"])
        group["activityVolume"] = sum(to_float(row.get("totalVolume")) for row in group["activityRows"])
        group["summary"] = summarize_invoice_group(group)
    return groups


def get_product_type(txn: dict[str, Any], rate: dict[str, Any]) -> str:
    if rate and rate.get("ccyGroup") == "GBP" and not rate.get("txnType"):
        return "GBP 0.7%"
    if rate and rate.get("speedFlag") == "RTP":
        return "RTP"
    if rate and rate.get("speedFlag") == "FasterACH":
        return "FasterACH"
    if txn.get("speedFlag") == "RTP" or (rate and rate.get("speedFlag") == "RTP"):
        return "RTP"
    if txn.get("speedFlag") == "FasterACH" or (rate and rate.get("speedFlag") == "FasterACH"):
        return "FasterACH"
    if txn.get("processingMethod") == "Wire" or (rate and rate.get("txnType") == "FX" and rate.get("processingMethod") == "Wire"):
        return "Wire"
    if rate and rate.get("payerFunding") == "Card" and rate.get("payeeCardType") == "Credit" and rate.get("txnType") == "FX":
        return "Card Credit FX"
    if rate and rate.get("payerFunding") == "Card" and rate.get("payeeCardType") == "Credit":
        return "Card Credit Domestic"
    if rate and rate.get("payerFunding") == "Card" and rate.get("payeeCardType") == "Debit" and rate.get("txnType") == "FX":
        return "Card Debit FX"
    if rate and rate.get("payerFunding") == "Card" and rate.get("payeeCardType") == "Debit":
        return "Card Debit Domestic"
    if rate and rate.get("payeeFunding") == "Card" and rate.get("payeeCardType") == "Debit":
        return "Push-to-Debit"
    if rate and rate.get("txnType") == "FX":
        ccy_group = rate.get("ccyGroup")
        if ccy_group == MAJORS or get_corridor(ccy_group) == "Major":
            return "FX Majors"
        if ccy_group == MINORS or get_corridor(ccy_group) == "Minor":
            return "FX Minors"
        if ccy_group == TERTIARY or get_corridor(ccy_group) == "Tertiary":
            return "FX Tertiary"
        return "FX Majors"
    return "ACH"


def apply_fee_caps(snapshot: dict[str, Any], partner: str, product_type: str, fee_per_txn: float, txn_count: float, period: str) -> dict[str, Any]:
    caps = sorted(
        [
            row for row in snapshot.get("cap", [])
            if row.get("partner") == partner
            and row.get("productType") == product_type
            and in_range(f"{period}-15", row.get("startDate"), row.get("endDate"))
        ],
        key=lambda row: str(row.get("startDate") or ""),
        reverse=True,
    )
    max_cap = next((row for row in caps if row.get("capType") == "Max Fee"), None)
    min_cap = next((row for row in caps if row.get("capType") == "Min Fee"), None)
    adjusted = fee_per_txn
    note = ""
    if max_cap and adjusted > to_float(max_cap.get("amount")):
        adjusted = to_float(max_cap.get("amount"))
        note = f" MAX@{fmt(adjusted)}/txn"
    if min_cap and adjusted < to_float(min_cap.get("amount")):
        adjusted = to_float(min_cap.get("amount"))
        note = f" MIN@{fmt(adjusted)}/txn"
    return {
        "adjFee": adjusted,
        "total": adjusted * txn_count,
        "capNote": note,
        "capped": bool(note),
    }


def calculate_invoice_for_period(snapshot: dict[str, Any], partner: str, period: str, *, skip_implementation_credits: bool = False, suppress_notes: bool = False) -> dict[str, Any]:
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
    billing_profile = next((row for row in snapshot.get("pBilling", []) if norm(row.get("partner")) == norm(partner)), None)
    billing_profile_note = str((billing_profile or {}).get("note") or "").strip()
    txns = [deepcopy(row) for row in snapshot.get("ltxn", []) if row.get("partner") == partner and row.get("period") == period]
    revs = [deepcopy(row) for row in snapshot.get("lrev", []) if row.get("partner") == partner and row.get("period") == period]
    rev_share_summaries = [deepcopy(row) for row in snapshot.get("lrs", []) if row.get("partner") == partner and row.get("period") == period]
    fx_partner_payout_rows = [deepcopy(row) for row in snapshot.get("lfxp", []) if row.get("partner") == partner and row.get("period") == period]
    rev_share_rows = [deepcopy(row) for row in snapshot.get("rs", []) if row.get("partner") == partner and in_range(f"{period}-15", row.get("startDate"), row.get("endDate"))]
    is_incremental = bool((snapshot.get("pConfig") or {}).get(partner))
    period_volume = sum(to_float(row.get("totalVolume")) for row in txns)
    recurring_billing_active = is_recurring_billing_live_for_period(snapshot, partner, period)
    if not recurring_billing_active:
        txns = []
        revs = []
        rev_share_summaries = []
        fx_partner_payout_rows = []
        rev_share_rows = []
        fx_markup_activity_rows = []
        period_volume = 0.0
    minimum_row = next((row for row in snapshot.get("mins", []) if recurring_billing_active and row.get("partner") == partner and in_range(f"{period}-15", row.get("startDate"), row.get("endDate")) and period_volume >= to_float(row.get("minVol")) and period_volume <= to_float(row.get("maxVol"))), None)
    summary_minimum_amount = max([to_float(row.get("monthlyMinimumRevenue")) for row in rev_share_summaries] or [0.0])
    effective_minimum_amount = to_float(minimum_row.get("minAmount")) if minimum_row and to_float(minimum_row.get("minAmount")) > 0 else summary_minimum_amount
    fx_markup_activity_rows = [row for row in txns if (row.get("txnType") == "FX" or (row.get("payerCcy") == "USD" and row.get("payeeCcy") and row.get("payeeCcy") != "USD")) and row.get("processingMethod") == "Wire"]
    summary_charge_rows = [row for row in rev_share_summaries if to_float(row.get("revenueOwed")) > 0]
    summary_pay_rows = [row for row in rev_share_summaries if to_float(row.get("partnerRevenueShare")) > 0]
    authoritative_payout_summary = bool(summary_pay_rows) and not rev_share_rows and not fx_partner_payout_rows
    authoritative_recurring_charge_summary = any(str(row.get("revenueSource") or "") == "billing_summary" for row in summary_charge_rows)
    pre_collected_revenue_total = sum(to_float(row.get("estRevenue")) for row in txns)
    has_dedicated_stampli_usd_abroad = partner == "Stampli" and any(row.get("directInvoiceSource") == "stampli_direct_billing" for row in txns)

    def append_line(**kwargs: Any) -> None:
        activity_rows = kwargs.pop("activityRows", []) or []
        group_label = kwargs.pop("groupLabel", "") or kwargs.get("desc", "")
        group_key = kwargs.pop("groupKey", "") or f'{kwargs.get("cat")}|{kwargs.get("dir")}|{group_label}'
        active = kwargs.pop("active", True)
        minimum_eligible = kwargs.pop("minimumEligible", False)
        line = {
            "id": f"line-{len(lines)}",
            **kwargs,
            "active": active,
            "minimumEligible": minimum_eligible,
            "groupLabel": group_label,
            "groupKey": group_key,
            "activityRows": activity_rows,
        }
        lines.append(line)

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
                line["inactiveReason"] = f"Already charged at transaction time via Est Revenue {fmt(credit)}"
            else:
                line["amount"] = round(amount - credit, 2)
                line["desc"] = f"{line.get('desc')} (less {fmt(credit)} already charged)"
        return used_total

    def apply_monthly_minimum_rule() -> None:
        nonlocal lines
        if not recurring_billing_active or effective_minimum_amount <= 0:
            return
        if authoritative_recurring_charge_summary:
            return
        eligible_lines = [line for line in lines if line.get("dir") == "charge" and line.get("minimumEligible") and line.get("active") is not False]
        invoiced_generated_revenue = sum(to_float(line.get("amount")) for line in eligible_lines)
        generated_revenue = invoiced_generated_revenue + pre_collected_revenue_total
        minimum_desc = f"Monthly minimum fee for period ({fmt(effective_minimum_amount)})"
        if generated_revenue < effective_minimum_amount:
            for line in eligible_lines:
                line["active"] = False
                line["inactiveReason"] = f"Replaced by monthly minimum {fmt(effective_minimum_amount)}"
            append_line(
                cat="Minimum",
                desc=(
                    f"{minimum_desc} replaces {fmt(invoiced_generated_revenue)} invoiced revenue + {fmt(pre_collected_revenue_total)} pre-collected revenue"
                    if pre_collected_revenue_total > 0
                    else f"{minimum_desc} replaces {fmt(generated_revenue)} generated revenue"
                ),
                amount=effective_minimum_amount,
                dir="charge",
                groupLabel="Monthly minimum",
                implementationCreditEligible="monthly_minimum",
            )
        else:
            append_line(
                cat="Minimum",
                desc=minimum_desc,
                amount=effective_minimum_amount,
                dir="charge",
                groupLabel="Monthly minimum",
                active=False,
            )
            lines[-1]["inactiveReason"] = (
                f"Not applicable because invoiced revenue {fmt(invoiced_generated_revenue)} + pre-collected revenue {fmt(pre_collected_revenue_total)} exceeds minimum"
                if pre_collected_revenue_total > 0
                else f"Not applicable because generated revenue {fmt(generated_revenue)} exceeds minimum"
            )

    def volume_group_signature(row: dict[str, Any]) -> str:
        return "|".join([
            str(row.get("txnType") or ""),
            str(row.get("speedFlag") or ""),
            str(row.get("payerFunding") or ""),
            str(row.get("payeeFunding") or ""),
            str(row.get("payeeCardType") or ""),
            str(row.get("ccyGroup") or ""),
        ])

    def build_rate_group_label(activity_rows: list[dict[str, Any]], rate_row: dict[str, Any]) -> str:
        note_label = str(rate_row.get("note") or "").strip()
        if note_label:
            return note_label
        txn_types = list({str(row.get("txnType") or "").strip() for row in activity_rows if str(row.get("txnType") or "").strip()})
        speed_flags = list({str(row.get("speedFlag") or "").strip() for row in activity_rows if str(row.get("speedFlag") or "").strip()})
        if len(txn_types) == 1:
            return " ".join(filter(None, [txn_types[0], speed_flags[0] if speed_flags else str(rate_row.get("speedFlag") or "")])).strip()
        return get_product_type(activity_rows[0] if activity_rows else {}, rate_row)

    def summary_charge_category(summary: dict[str, Any]) -> str:
        normalized = str(summary.get("summaryBillingType") or summary.get("summaryLabel") or "").lower()
        if "subscription" in normalized or "platform" in normalized:
            return "Platform"
        if "reversal" in normalized:
            return "Reversal"
        if "volume" in normalized:
            return "Volume"
        if "txn" in normalized or "count" in normalized:
            return "Txn Count"
        if "minimum" in normalized:
            return "Minimum"
        return "Revenue"

    def summary_line_label(summary: dict[str, Any], fallback: str) -> str:
        return str(summary.get("summaryLabel") or summary.get("summaryBillingType") or fallback).strip() or fallback

    def is_subscription_summary(summary: dict[str, Any]) -> bool:
        normalized = str(summary.get("summaryBillingType") or summary.get("summaryLabel") or "").lower()
        return "subscription" in normalized

    def is_subscription_component_summary(summary: dict[str, Any]) -> bool:
        normalized = str(summary.get("summaryBillingType") or summary.get("summaryLabel") or "").strip().lower().replace(" ", "_")
        return normalized in {"txn_count", "volume"}

    subscription_summary_rows = [row for row in summary_charge_rows if is_subscription_summary(row)]
    has_combined_subscription_summary = bool(subscription_summary_rows) and (
        any("+" in str(row.get("summaryComputation") or "") for row in subscription_summary_rows)
        or any(is_subscription_component_summary(row) for row in summary_charge_rows)
    )

    if not authoritative_recurring_charge_summary:
        for txn in txns:
            direct_invoice_amount = to_float(txn.get("directInvoiceAmount"))
            if has_dedicated_stampli_usd_abroad and txn.get("txnType") == "USD Abroad" and not txn.get("directInvoiceSource"):
                continue
            if txn.get("directInvoiceSource") == "stampli_usd_abroad_reversal" and direct_invoice_amount == 0:
                for row in snapshot.get("off", []):
                    if row.get("partner") != partner:
                        continue
                    if not (
                        txn_matches_pricing_row(row, txn)
                        and to_float(txn.get("minAmt")) >= to_float(row.get("minAmt"))
                        and to_float(txn.get("maxAmt")) <= to_float(row.get("maxAmt"))
                        and in_range(f"{period}-15", row.get("startDate"), row.get("endDate"))
                    ):
                        continue
                    amount = to_float(row.get("fee")) * to_float(txn.get("txnCount"))
                    label = " ".join(filter(None, [str(txn.get("txnType") or ""), str(txn.get("speedFlag") or ""), str(txn.get("processingMethod") or "")])).strip()
                append_line(
                    cat="Offline",
                    desc=f"{label} reversal adjustment ({int(to_float(txn.get('txnCount')))}x{fmt(row.get('fee'))})",
                    amount=-amount,
                    dir="charge",
                    groupLabel=f"{label} reversal",
                    activityRows=[txn],
                )
                continue
            if direct_invoice_amount != 0:
                direct_rate = abs(direct_invoice_amount) / to_float(txn.get("txnCount")) if to_float(txn.get("txnCount")) > 0 else abs(to_float(txn.get("directInvoiceRate")))
                label = " ".join(filter(None, [str(txn.get("txnType") or ""), str(txn.get("speedFlag") or ""), str(txn.get("processingMethod") or "")])).strip()
                append_line(
                    cat="Offline",
                    desc=(
                        f"{label} reversal adjustment ({int(to_float(txn.get('txnCount')))}x{fmt(direct_rate)} imported)"
                        if direct_invoice_amount < 0
                        else f"{label} ({int(to_float(txn.get('txnCount')))}x{fmt(direct_rate)} imported)"
                    ),
                    amount=direct_invoice_amount,
                    dir="charge",
                    groupLabel=label,
                    minimumEligible=True,
                    activityRows=[txn],
                )
                continue
            for row in snapshot.get("off", []):
                if row.get("partner") != partner:
                    continue
                if not (
                    txn_matches_pricing_row(row, txn)
                    and to_float(txn.get("minAmt")) >= to_float(row.get("minAmt"))
                    and to_float(txn.get("maxAmt")) <= to_float(row.get("maxAmt"))
                    and in_range(f"{period}-15", row.get("startDate"), row.get("endDate"))
                ):
                    continue
                amount = to_float(row.get("fee")) * to_float(txn.get("txnCount"))
                label = " ".join(filter(None, [str(txn.get("txnType") or ""), str(txn.get("speedFlag") or ""), str(txn.get("processingMethod") or "")])).strip()
                append_line(
                    cat="Offline",
                    desc=f"{label} ({int(to_float(txn.get('txnCount')))}x{fmt(row.get('fee'))})",
                    amount=amount,
                    dir="charge",
                    groupLabel=label,
                    minimumEligible=True,
                    activityRows=[txn],
                )

    volume_rows = [row for row in snapshot.get("vol", []) if row.get("partner") == partner and in_range(f"{period}-15", row.get("startDate"), row.get("endDate"))]
    if not authoritative_recurring_charge_summary and volume_rows:
        grouped_volume: OrderedDict[str, list[dict[str, Any]]] = OrderedDict()
        for row in volume_rows:
            grouped_volume.setdefault(volume_group_signature(row), []).append(row)
        for tiers in grouped_volume.values():
            tiers = sorted(tiers, key=lambda item: to_float(item.get("minVol")))
            base_rate = tiers[0]
            matching_txns = [
                txn for txn in txns
                if txn_matches_pricing_row(base_rate, txn)
            ]
            if not matching_txns:
                continue
            combined_volume = sum(to_float(txn.get("totalVolume")) for txn in matching_txns)
            combined_txn_count = sum(to_float(txn.get("txnCount")) for txn in matching_txns)
            label = build_rate_group_label(matching_txns, base_rate)
            if is_incremental and len(tiers) > 1:
                remaining = combined_volume
                total_fee = 0.0
                parts: list[str] = []
                for tier in tiers:
                    if remaining <= 0:
                        break
                    band_size = to_float(tier.get("maxVol")) - to_float(tier.get("minVol")) + 1
                    volume_in_band = min(remaining, band_size)
                    total_fee += to_float(tier.get("rate")) * volume_in_band
                    parts.append(f"{fmt_pct(tier.get('rate'))}x{fmt(volume_in_band)}")
                    remaining -= volume_in_band
                if total_fee > 0:
                    product_type = get_product_type(matching_txns[0], base_rate)
                    fee_per_txn = total_fee / combined_txn_count if combined_txn_count > 0 else 0
                    adjusted = apply_fee_caps(snapshot, partner, product_type, fee_per_txn, combined_txn_count, period)
                    amount = adjusted["total"] if adjusted["capped"] else total_fee
                    append_line(
                        cat="Volume",
                        desc=f"{label} incremental [{' + '.join(parts)}]{adjusted['capNote']}",
                        amount=amount,
                        dir="charge",
                        groupLabel=label,
                        minimumEligible=True,
                        activityRows=matching_txns,
                    )
            else:
                tier = next((row for row in tiers if combined_volume >= to_float(row.get("minVol")) and combined_volume <= to_float(row.get("maxVol"))), None)
                if not tier:
                    continue
                product_type = get_product_type(matching_txns[0], tier)
                cap_notes: list[str] = []
                amount = 0.0
                for txn in matching_txns:
                    raw_amount = to_float(tier.get("rate")) * to_float(txn.get("totalVolume"))
                    fee_per_txn = (raw_amount / to_float(txn.get("txnCount"))) if to_float(txn.get("txnCount")) > 0 else 0
                    adjusted = apply_fee_caps(snapshot, partner, product_type, fee_per_txn, to_float(txn.get("txnCount")), period)
                    if adjusted["capNote"] and adjusted["capNote"] not in cap_notes:
                        cap_notes.append(adjusted["capNote"])
                    amount += adjusted["total"] if adjusted["capped"] else raw_amount
                note = str(tier.get("note") or "").strip()
                cap_note = "".join(cap_notes)
                desc = f"{label} {note} ({fmt_pct(tier.get('rate'))}x{fmt(combined_volume)}{cap_note})".strip()
                append_line(
                    cat="Volume",
                    desc=" ".join(desc.split()),
                    amount=amount,
                    dir="charge",
                    groupLabel=label,
                    minimumEligible=True,
                    activityRows=matching_txns,
                )

    partner_surcharges = [row for row in snapshot.get("surch", []) if row.get("partner") == partner and in_range(f"{period}-15", row.get("startDate"), row.get("endDate"))]
    if not authoritative_recurring_charge_summary and partner_surcharges:
        surch_groups: OrderedDict[str, list[dict[str, Any]]] = OrderedDict()
        for row in partner_surcharges:
            surch_groups.setdefault(str(row.get("surchargeType") or ""), []).append(row)
        matching_txns = [txn for txn in txns if to_float(txn.get("totalVolume")) > 0]
        combined_volume = sum(to_float(txn.get("totalVolume")) for txn in matching_txns)
        if matching_txns and combined_volume > 0:
            for surcharge_type, tiers in surch_groups.items():
                tiers = sorted(tiers, key=lambda item: to_float(item.get("minVol")))
                if is_incremental and len(tiers) > 1:
                    remaining = combined_volume
                    total_fee = 0.0
                    parts: list[str] = []
                    for tier in tiers:
                        if remaining <= 0:
                            break
                        band_size = to_float(tier.get("maxVol")) - to_float(tier.get("minVol")) + 1
                        volume_in_band = min(remaining, band_size)
                        total_fee += to_float(tier.get("rate")) * volume_in_band
                        parts.append(f"{fmt_pct(tier.get('rate'))}x{fmt(volume_in_band)}")
                        remaining -= volume_in_band
                    if total_fee > 0:
                        append_line(
                            cat="Surcharge",
                            desc=f"{surcharge_type} incremental [{' + '.join(parts)}]",
                            amount=total_fee,
                            dir="charge",
                            groupLabel=surcharge_type,
                            minimumEligible=True,
                            activityRows=matching_txns,
                        )
                else:
                    tier = next((row for row in tiers if combined_volume >= to_float(row.get("minVol")) and combined_volume <= to_float(row.get("maxVol"))), None)
                    if not tier:
                        continue
                    amount = to_float(tier.get("rate")) * combined_volume
                    desc = f"{tier.get('surchargeType')} {tier.get('note') or ''} ({fmt_pct(tier.get('rate'))}x{fmt(combined_volume)})".strip()
                    append_line(
                        cat="Surcharge",
                        desc=" ".join(desc.split()),
                        amount=amount,
                        dir="charge",
                        groupLabel=str(tier.get("surchargeType") or ""),
                        minimumEligible=True,
                        activityRows=matching_txns,
                    )

    if authoritative_payout_summary:
        for summary in summary_pay_rows:
            if str(summary.get("revenueSource") or "") == "billing_summary":
                label = summary_line_label(summary, "Partner payout")
                append_line(
                    cat="Rev Share",
                    desc=str(summary.get("summaryComputation") or label),
                    amount=to_float(summary.get("partnerRevenueShare")),
                    dir="pay",
                    groupLabel=label,
                )
            else:
                append_line(
                    cat="Rev Share",
                    desc=f"Partner rev-share payout from revenue report (net revenue {fmt(summary.get('netRevenue'))})",
                    amount=to_float(summary.get("partnerRevenueShare")),
                    dir="pay",
                    groupLabel="Partner rev-share payout",
                )
    for summary in summary_charge_rows:
        if has_combined_subscription_summary and is_subscription_component_summary(summary):
            continue
        if str(summary.get("revenueSource") or "") == "billing_summary":
            label = summary_line_label(summary, "Partner-generated revenue")
            summary_category = summary_charge_category(summary)
            append_line(
                cat=summary_category,
                desc=str(summary.get("summaryComputation") or label),
                amount=to_float(summary.get("revenueOwed")),
                dir="charge",
                groupLabel=label,
                minimumEligible=True,
                implementationCreditEligible=(
                    "monthly_minimum"
                    if summary_category == "Minimum"
                    else "monthly_subscription"
                    if summary_category == "Platform"
                    else ""
                ),
            )
        else:
            min_note = f", minimum {fmt(summary.get('monthlyMinimumRevenue'))}" if to_float(summary.get("monthlyMinimumRevenue")) > 0 else ""
            append_line(
                cat="Revenue",
                desc=f"Partner-generated revenue from revenue report ({fmt(summary.get('revenueOwed'))} owed{min_note})",
                amount=to_float(summary.get("revenueOwed")),
                dir="charge",
                groupLabel="Partner-generated revenue",
                minimumEligible=True,
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
        rev_share_lines = []
        rev_share_match_count = 0
        rev_share_revenue_count = 0
        for share in rev_share_rows:
            if norm(share.get("txnType")) == "payin":
                continue
            for txn in txns:
                if rev_share_direction(txn) != "Out":
                    continue
                if not rev_share_scope_matches(share, txn):
                    continue
                rev_share_match_count += 1
                total_cost = calculate_rev_share_cost(snapshot, txn, period)
                est_revenue = to_float(txn.get("estRevenue"))
                imported_revenue = to_float(txn.get("customerRevenue"))
                generated_revenue = generated_revenue_by_activity.get(activity_row_key(txn), 0.0)
                if est_revenue > 0:
                    source_revenue = est_revenue
                    revenue_source_label = "est revenue"
                elif imported_revenue > 0:
                    source_revenue = imported_revenue
                    revenue_source_label = "imported revenue"
                else:
                    source_revenue = generated_revenue
                    revenue_source_label = "contract-generated revenue"
                revenue_base = max(source_revenue - total_cost, 0.0)
                if source_revenue > 0:
                    rev_share_revenue_count += 1
                payback = to_float(share.get("revSharePct")) * revenue_base
                if payback > 0:
                    scope = " ".join(filter(None, [str(share.get("txnType") or ("Payin" if rev_share_direction(txn) == "In" else "Payout") or "All"), str(share.get("speedFlag") or txn.get("speedFlag") or "")])).strip()
                    desc = f"{scope}: {fmt_pct(share.get('revSharePct'))}x({fmt(source_revenue)} {revenue_source_label}-{fmt(total_cost)} cost)"
                    rev_share_lines.append({
                        "id": f"line-{len(lines) + len(rev_share_lines)}",
                        "cat": "Rev Share",
                        "desc": desc,
                        "amount": payback,
                        "dir": "pay",
                        "groupLabel": scope or "Partner rev-share payout",
                        "groupKey": f"Rev Share|pay|{scope or 'Partner rev-share payout'}",
                        "activityRows": [txn],
                        "active": True,
                        "minimumEligible": False,
                    })
        if not rev_share_lines:
            scopes = list({(" ".join(filter(None, [str(share.get("txnType") or "All"), str(share.get("speedFlag") or "")])).strip()) for share in rev_share_rows if (" ".join(filter(None, [str(share.get("txnType") or "All"), str(share.get("speedFlag") or "")])).strip())})
            scope_label = ", ".join(scopes) if scopes else "configured rev-share"
            if not rev_share_match_count:
                notes.append(f"Revenue share is configured for {scope_label}, but no matching transactions were imported for {partner} in {period}.")
            elif not rev_share_revenue_count:
                notes.append(f"Revenue share is configured for {scope_label}, but neither Est Revenue, the imported revenue fields, nor the contract-derived transaction charges produced a revenue base for the partner payout.")
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
                append_line(
                    cat="Rev Share",
                    desc=f"FX partner markup payout ({int(to_float(row.get('shareTxnCount') or row.get('txnCount')))} payout txns, markup {fmt(share_amount)}{f', net after reversals {fmt(net_payout)}' if reversal_amount > 0 and net_payout > 0 else ''})",
                    amount=share_amount,
                    dir="pay",
                    groupLabel="FX partner markup payout",
                    activityRows=[share_activity_summary_row],
                )
            if reversal_amount > 0:
                append_line(
                    cat="Rev Share",
                    desc=f"FX partner markup reversal adjustment ({int(to_float(row.get('reversalTxnCount') or 0))} reversal txns, reversed {fmt(reversal_amount)}{f', net balance {fmt(net_payout)}' if share_amount > 0 else ''})",
                    amount=-reversal_amount,
                    dir="pay",
                    groupLabel="FX partner markup reversal",
                    activityRows=[reversal_activity_summary_row],
                )
            if row.get("note"):
                notes.append(f"Stampli FX payout: {row.get('note')}")
    elif partner == "Stampli" and recurring_billing_active:
        if not fx_markup_activity_rows:
            notes.append(f"No Stampli FX transactions were imported for {period}. The supplied data only contains Domestic and USD Abroad rows, so the FX partner-markup payout remains $0.00.")
        else:
            notes.append(f"Stampli FX transactions were imported for {period}, but no FX partner-markup payout summary was derived from the raw payment detail.")

    partner_fx_rates = [row for row in snapshot.get("fxRates", []) if row.get("partner") == partner and in_range(f"{period}-15", row.get("startDate"), row.get("endDate"))]
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
            if is_incremental and len(tiers) > 1 and any(to_float(row.get("minVol")) != to_float(tiers[0].get("minVol")) for row in tiers):
                remaining = to_float(txn.get("totalVolume"))
                total_fee = 0.0
                parts: list[str] = []
                for tier in tiers:
                    if remaining <= 0:
                        break
                    band_size = to_float(tier.get("maxVol")) - to_float(tier.get("minVol")) + 1
                    volume_in_band = min(remaining, band_size)
                    total_fee += to_float(tier.get("rate")) * volume_in_band
                    parts.append(f"{fmt_pct(tier.get('rate'))}x{fmt(volume_in_band)}")
                    remaining -= volume_in_band
                if total_fee > 0:
                    label = f"{txn.get('payerCcy')}→{txn.get('payeeCcy')}"
                    append_line(
                        cat="FX",
                        desc=f"{label} incremental [{' + '.join(parts)}]",
                        amount=total_fee,
                        dir="charge",
                        groupLabel=label,
                        minimumEligible=True,
                        activityRows=[txn],
                    )
            else:
                best = next((row for row in pool if to_float(txn.get("totalVolume")) >= to_float(row.get("minVol")) and to_float(txn.get("totalVolume")) <= to_float(row.get("maxVol"))), pool[0])
                amount = to_float(best.get("rate")) * to_float(txn.get("totalVolume"))
                label = f"{txn.get('payerCcy')}→{txn.get('payeeCcy')}"
                append_line(
                    cat="FX",
                    desc=f"{label} @ {(to_float(best.get('rate')) * 100):.4f}% (avg txn {fmt(avg_size)}) x {fmt(txn.get('totalVolume'))}",
                    amount=amount,
                    dir="charge",
                    groupLabel=label,
                    minimumEligible=True,
                    activityRows=[txn],
                )

    partner_reversal_fees = [row for row in snapshot.get("revf", []) if row.get("partner") == partner and in_range(f"{period}-15", row.get("startDate"), row.get("endDate"))]
    if not authoritative_recurring_charge_summary:
        for row in revs:
            match = next((fee for fee in partner_reversal_fees if not fee.get("payerFunding") or fee.get("payerFunding") == row.get("payerFunding")), None)
            if match:
                amount = to_float(match.get("feePerReversal")) * to_float(row.get("reversalCount"))
                append_line(
                    cat="Reversal",
                    desc=f"{row.get('payerFunding') or 'All'} {int(to_float(row.get('reversalCount')))}x{fmt(match.get('feePerReversal'))}",
                    amount=amount,
                    dir="charge",
                    groupLabel=f"{row.get('payerFunding') or 'All'} reversals",
                    minimumEligible=True,
                    activityRows=[row],
                )

    platform_fee = next((row for row in snapshot.get("plat", []) if recurring_billing_active and row.get("partner") == partner and in_range(f"{period}-15", row.get("startDate"), row.get("endDate"))), None)
    if platform_fee:
        if not has_combined_subscription_summary:
            append_line(
                cat="Platform",
                desc="Monthly subscription",
                amount=to_float(platform_fee.get("monthlyFee")),
                dir="charge",
                groupLabel="Monthly platform fee",
                implementationCreditEligible="monthly_subscription",
            )

    configured_transaction_fee_rows = [
        row for row in (
            list(snapshot.get("off", []))
            + list(snapshot.get("vol", []))
            + list(snapshot.get("fxRates", []))
            + list(snapshot.get("surch", []))
        )
        if row.get("partner") == partner and in_range(f"{period}-15", row.get("startDate"), row.get("endDate"))
    ]
    partner_va_fees = [row for row in snapshot.get("vaFees", []) if row.get("partner") == partner and in_range(f"{period}-15", row.get("startDate"), row.get("endDate"))]
    account_setup_rows = [
        row for row in snapshot.get("impl", [])
        if row.get("partner") == partner
        and row.get("feeType") == "Account Setup"
        and in_range(f"{period}-15", row.get("startDate") or row.get("goLiveDate"), row.get("endDate"))
    ]
    daily_settlement_rows = [
        row for row in snapshot.get("impl", [])
        if row.get("partner") == partner
        and row.get("feeType") == "Daily Settlement"
        and in_range(f"{period}-15", row.get("startDate") or row.get("goLiveDate"), row.get("endDate"))
    ]
    va_data = next((row for row in snapshot.get("lva", []) if row.get("partner") == partner and row.get("period") == period), None)
    if recurring_billing_active and not authoritative_recurring_charge_summary and va_data:
        def find_tier(fee_type: str, count: float) -> dict[str, Any] | None:
            return next((row for row in partner_va_fees if row.get("feeType") == fee_type and count >= to_float(row.get("minAccounts")) and count <= to_float(row.get("maxAccounts"))), None)
        if to_float(va_data.get("newAccountsOpened")) > 0:
            tier = find_tier("Account Opening", to_float(va_data.get("newAccountsOpened")))
            if tier:
                amount = to_float(tier.get("feePerAccount")) * to_float(va_data.get("newAccountsOpened"))
                append_line(cat="Virtual Acct", desc=f"Account Opening: {int(to_float(va_data.get('newAccountsOpened')))} accts x {fmt(tier.get('feePerAccount'))}", amount=amount, dir="charge", groupLabel="Account Opening", minimumEligible=True)
        if recurring_billing_active and to_float(va_data.get("totalActiveAccounts")) > 0:
            tier = find_tier("Monthly Active", to_float(va_data.get("totalActiveAccounts")))
            if tier:
                amount = to_float(tier.get("feePerAccount")) * to_float(va_data.get("totalActiveAccounts"))
                append_line(cat="Virtual Acct", desc=f"Monthly Active: {int(to_float(va_data.get('totalActiveAccounts')))} accts x {fmt(tier.get('feePerAccount'))}/mo", amount=amount, dir="charge", groupLabel="Monthly Active", minimumEligible=True)
        if recurring_billing_active and to_float(va_data.get("dormantAccounts")) > 0:
            tier = find_tier("Dormancy", to_float(va_data.get("dormantAccounts")))
            if tier:
                amount = to_float(tier.get("feePerAccount")) * to_float(va_data.get("dormantAccounts"))
                append_line(cat="Virtual Acct", desc=f"Dormancy: {int(to_float(va_data.get('dormantAccounts')))} accts x {fmt(tier.get('feePerAccount'))}/mo", amount=amount, dir="charge", groupLabel="Dormancy", minimumEligible=True)
        if to_float(va_data.get("closedAccounts")) > 0:
            tier = find_tier("Account Closing", to_float(va_data.get("closedAccounts")))
            if tier:
                amount = to_float(tier.get("feePerAccount")) * to_float(va_data.get("closedAccounts"))
                append_line(cat="Virtual Acct", desc=f"Account Closing: {int(to_float(va_data.get('closedAccounts')))} accts x {fmt(tier.get('feePerAccount'))}", amount=amount, dir="charge", groupLabel="Account Closing", minimumEligible=True)
        annual_business_setup = next((row for row in account_setup_rows if is_year_end_account_setup_row(row) and "per business" in norm(row.get("note"))), None)
        annual_individual_setup = next((row for row in account_setup_rows if is_year_end_account_setup_row(row) and "per individual" in norm(row.get("note"))), None)
        standard_setup_fee = next((row for row in account_setup_rows if not is_year_end_account_setup_row(row)), None)
        if is_calendar_year_end_period(period) and annual_business_setup and to_float(va_data.get("totalBusinessAccounts")) > 0:
            amount = to_float(annual_business_setup.get("feeAmount")) * to_float(va_data.get("totalBusinessAccounts"))
            append_line(cat="Account Setup", desc=f"Year-end business accounts: {int(to_float(va_data.get('totalBusinessAccounts')))} x {fmt(annual_business_setup.get('feeAmount'))}", amount=amount, dir="charge", groupLabel="Year-end business account setup", minimumEligible=True)
        if is_calendar_year_end_period(period) and annual_individual_setup and to_float(va_data.get("totalIndividualAccounts")) > 0:
            amount = to_float(annual_individual_setup.get("feeAmount")) * to_float(va_data.get("totalIndividualAccounts"))
            append_line(cat="Account Setup", desc=f"Year-end individual accounts: {int(to_float(va_data.get('totalIndividualAccounts')))} x {fmt(annual_individual_setup.get('feeAmount'))}", amount=amount, dir="charge", groupLabel="Year-end individual account setup", minimumEligible=True)
        if to_float(va_data.get("newBusinessSetups")) > 0 and standard_setup_fee:
            amount = to_float(standard_setup_fee.get("feeAmount")) * to_float(va_data.get("newBusinessSetups"))
            append_line(cat="Account Setup", desc=f"{int(to_float(va_data.get('newBusinessSetups')))} biz x {fmt(standard_setup_fee.get('feeAmount'))}", amount=amount, dir="charge", groupLabel="Account Setup", minimumEligible=True)
        if to_float(va_data.get("settlementCount")) > 0:
            settlement_fee = daily_settlement_rows[0] if daily_settlement_rows else None
            if settlement_fee:
                amount = to_float(settlement_fee.get("feeAmount")) * to_float(va_data.get("settlementCount"))
                append_line(cat="Settlement", desc=f"{int(to_float(va_data.get('settlementCount')))} sweeps x {fmt(settlement_fee.get('feeAmount'))}", amount=amount, dir="charge", groupLabel="Daily Settlement", minimumEligible=True)

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

    def apply_implementation_credits() -> None:
        if skip_implementation_credits:
            return
        implementation_rows = [row for row in snapshot.get("impl", []) if row.get("partner") == partner and row.get("feeType") == "Implementation"]
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
                prior_invoice = calculate_invoice_for_period(snapshot, partner, prior_period, skip_implementation_credits=True, suppress_notes=True)
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
            append_line(
                cat="Impl Credit",
                desc=f"Implementation fee credit vs {implementation_credit_label(mode)}",
                amount=applied_credit,
                dir="offset",
                groupLabel="Implementation credit",
            )

    pre_collected_revenue_used = apply_pre_collected_revenue_offsets()

    if recurring_billing_active and not authoritative_recurring_charge_summary:
        if configured_transaction_fee_rows and not txns:
            notes.append("Transaction-priced fees are configured for this partner, but no transaction upload was imported for this period. Offline, volume, FX, and surcharge charges may be missing.")
        if partner_reversal_fees and not revs:
            notes.append("Reversal fees are configured for this partner, but no reversal upload was imported for this period. Reversal charges may be missing.")
        if (partner_va_fees or account_setup_rows or daily_settlement_rows) and not va_data:
            notes.append("Virtual-account, account-setup, or settlement fees are configured for this partner, but no account-usage upload was imported for this period. Those charges may be missing.")
    if pre_collected_revenue_used > 0 or pre_collected_revenue_total > 0:
        notes.append(f"Pre-collected revenue from transaction-time charges: {fmt(pre_collected_revenue_total)}. {fmt(pre_collected_revenue_used)} was excluded from this invoice to avoid double charging and still counts toward monthly minimum calculations.")

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

    impl_fee = next((
        row for row in snapshot.get("impl", [])
        if row.get("partner") == partner
        and row.get("feeType") == "Implementation"
        and normalize_month_key(get_implementation_billing_date(snapshot, partner, row)) == period
    ), None)
    if impl_fee:
        append_line(cat="Impl Fee", desc="Implementation fee", amount=to_float(impl_fee.get("feeAmount")), dir="charge", groupLabel="Implementation fee")

    apply_implementation_credits()

    implementation_credit_base_by_mode = summarize_implementation_credit_base()

    totals = calculate_active_invoice_totals(lines)
    groups = group_invoice_lines(lines)
    chg = totals["chg"]
    pay = totals["pay"]
    net = chg - pay
    if (
        not suppress_notes
        and chg == 0
        and pay == 0
        and "referred-business account count source" in norm(billing_profile_note)
    ):
        notes.append(
            "This contract bills only approved referred businesses. The current workflow imports total account counts, "
            "but not the referred-business account source required to calculate this fee, so the invoice remains $0.00."
        )
    return {
        "partner": partner,
        "period": period,
        "periodStart": period,
        "periodEnd": period,
        "periodLabel": format_period_label(period),
        "periodDateRange": f"{format_period_boundary(period, 'start')} - {format_period_boundary(period, 'end')}",
        "lines": lines,
        "groups": groups,
        "notes": [] if suppress_notes else notes,
        "implementationCreditBaseByMode": implementation_credit_base_by_mode,
        "chg": chg,
        "pay": pay,
        "net": net,
        "dir": "Partner Owes Us" if net >= 0 else "We Owe Partner",
    }


def calculate_invoice(snapshot: dict[str, Any], partner: str, start_period: str, end_period: str | None = None) -> dict[str, Any]:
    range_start, range_end = normalize_period_range(start_period, end_period)
    periods = enumerate_periods(range_start, range_end)
    monthly_invoices = [calculate_invoice_for_period(snapshot, partner, period) for period in periods]

    lines: list[dict[str, Any]] = []
    notes: list[str] = []
    for invoice in monthly_invoices:
        source_period = str(invoice.get("periodStart") or invoice.get("period") or "")
        for line in invoice.get("lines", []):
            cloned = deepcopy(line)
            cloned["id"] = f"line-{len(lines)}"
            cloned["sourcePeriod"] = source_period
            cloned["activityRows"] = [{**row, "period": row.get("period") or source_period} for row in (line.get("activityRows") or [])]
            lines.append(cloned)
        for note in invoice.get("notes", []):
            if invoice.get("inactivePeriod") and range_start != range_end:
                continue
            if range_start == range_end:
                notes.append(str(note))
            else:
                notes.append(f"{format_period_label(source_period)}: {note}")

    totals = calculate_active_invoice_totals(lines)
    groups = group_invoice_lines(lines)
    chg = totals["chg"]
    pay = totals["pay"]
    net = chg - pay
    period_key = range_start if range_start == range_end else f"{range_start}_to_{range_end}"
    period_label = format_period_label(range_start) if range_start == range_end else f"{format_period_label(range_start)} - {format_period_label(range_end)}"
    period_date_range = f"{format_period_boundary(range_start, 'start')} - {format_period_boundary(range_end, 'end')}"
    return {
        "partner": partner,
        "period": period_key,
        "periodStart": range_start,
        "periodEnd": range_end,
        "periodLabel": period_label,
        "periodDateRange": period_date_range,
        "lines": lines,
        "groups": groups,
        "notes": notes,
        "chg": chg,
        "pay": pay,
        "net": net,
        "dir": "Partner Owes Us" if net >= 0 else "We Owe Partner",
    }

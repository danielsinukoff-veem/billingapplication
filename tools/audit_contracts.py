#!/usr/bin/env python3

from __future__ import annotations

import json
import math
import os
import re
import shutil
import subprocess
import sys
import csv
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any


WORKSPACE = Path("/Users/danielsinukoff/Documents/billing-workbook")
DATA_JS = WORKSPACE / "data.js"
SWIFT_EXTRACTOR = WORKSPACE / "tools" / "extract_pdf_text.swift"
REPORT_DIR = WORKSPACE / "reports" / "contract_audit"
TEXT_EXPORT_DIR = REPORT_DIR / "extracted_text"
MODULE_CACHE = Path("/tmp/swift-module-cache")

FILE_PARTNER_ALIASES = {
    "ajhanareferral": "AJ Hana Referral",
    "altpay": "Altpay",
    "athena": "Athena",
    "bhn": "BHN",
    "blindpay": "Blindpay",
    "capi": "Capi",
    "cellpay": "Cellpay",
    "clearshift": "Clearshift",
    "everflow": "Everflow",
    "factura": "Factura",
    "goldstack": "Goldstack",
    "graph": "Graph Finance",
    "graphfinance": "Graph Finance",
    "halorecruiting": "Halorecruiting",
    "jazzcash": "Jazz Cash",
    "lianlian": "LianLian",
    "lightnet": "Lightnet",
    "mdaq": "M-DAQ",
    "maplewave": "Maplewave",
    "multigate": "Multigate",
    "nibss": "NIBSS",
    "nsave": "Nsave",
    "nium": "Nium",
    "nomad": "Nomad",
    "nuvion": "Nuvion",
    "ohentpay": "OhentPay",
    "oson": "Oson",
    "q2": "Q2",
    "repay": "Repay",
    "remittanceshub": "Remittanceshub",
    "skydo": "Skydo",
    "stampli": "Stampli",
    "triplea": "TripleA",
    "vgpay": "VG Pay",
    "whish": "Whish",
    "yeepay": "Yeepay",
}

PARTNER_DISPLAY_ALIASES = {
    "graphfinance": "Graph Finance",
    "halorecruiting": "Halo Recruiting",
    "lianlian": "Lian Lian",
    "remittanceshub": "RemittancesHub",
    "repay": "RePay",
    "triplea": "Triple A",
    "nsave": "Nsave",
}

PRICING_KEYWORDS = [
    "schedule a",
    "pricing",
    "economics",
    "fees",
    "fee schedule",
    "monthly minimum",
    "implementation",
    "reversal",
    "virtual account",
    "subscription",
    "same day",
    "rtp",
    "ach",
    "wire",
]

KEYWORD_SYNONYMS = {
    "Standard": ["standard", "next day", "next-day"],
    "FasterACH": ["fasterach", "same day", "same-day", "same day ach", "instant bank transfer"],
    "RTP": ["rtp", "instant", "real time payments", "real-time payments", "instant bank transfer"],
    "Wire": ["wire", "swift"],
    "Domestic": ["domestic", "usd domestic", "us domestic", "same currency"],
    "USD Abroad": ["usd abroad", "international usd", "usd wire", "cross-border usd"],
    "FX": ["fx", "foreign exchange", "cross-border", "cross border"],
    "CAD Domestic": ["cad domestic", "canada domestic", "eft"],
    "GBP Domestic": ["gbp domestic", "uk domestic", "faster payments"],
    "EUR Domestic": ["eur domestic", "europe domestic", "sepa"],
    "AUD Domestic": ["aud domestic", "australia domestic"],
    "Account Opening": ["account opening", "opening fee"],
    "Monthly Active": ["monthly active", "monthly fee per active account"],
    "Dormancy": ["dormancy", "inactive account"],
    "Account Closing": ["account closing", "closing fee"],
    "Platform": ["platform", "subscription", "monthly platform"],
}


def simplify_name(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", text.lower())


def trim_filename(path: str) -> str:
    name = Path(path).stem
    name = re.sub(r"\b(addendum|amendment|referral)\b", "", name, flags=re.I)
    return simplify_name(name)


def load_workbook_defaults() -> dict[str, Any]:
    script = f"""
ObjC.import('Foundation');
const path = {json.dumps(str(DATA_JS))};
const raw = ObjC.unwrap($.NSString.stringWithContentsOfFileEncodingError(path, $.NSUTF8StringEncoding, null));
const sanitized = raw.replace(/^export\\s+/mg, '');
eval(sanitized);
console.log(JSON.stringify(createInitialWorkbookData()));
"""
    result = subprocess.run(
        ["/usr/bin/osascript", "-l", "JavaScript", "-e", script],
        capture_output=True,
        text=True,
        check=True,
    )
    payload = result.stdout.strip() or result.stderr.strip()
    decoder = json.JSONDecoder()
    data, _ = decoder.raw_decode(payload)
    return data


def extract_contract_texts(pdf_folder: Path) -> list[dict[str, Any]]:
    MODULE_CACHE.mkdir(parents=True, exist_ok=True)
    result = subprocess.run(
        [
            "/usr/bin/swift",
            "-module-cache-path",
            str(MODULE_CACHE),
            str(SWIFT_EXTRACTOR),
            str(pdf_folder),
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(result.stdout)


def money_variants(amount: float) -> set[str]:
    variants = set()
    decimals = f"{amount:,.2f}"
    variants.add(f"${decimals}")
    variants.add(f"${decimals.replace(',', '')}")
    variants.add(decimals)
    variants.add(decimals.replace(",", ""))
    if abs(amount - round(amount)) < 1e-9:
        integer = f"{int(round(amount)):,}"
        variants.add(integer)
        variants.add(integer.replace(",", ""))
        variants.add(f"${integer}")
        variants.add(f"${integer.replace(',', '')}")
        variants.add(f"{int(round(amount))}.00")
    trimmed = decimals.rstrip("0").rstrip(".")
    variants.add(trimmed)
    variants.add(f"${trimmed}")
    return {item.lower() for item in variants if item}


def pct_variants(rate: float) -> set[str]:
    pct = rate * 100
    bps = rate * 10000
    candidates = set()
    for places in (6, 5, 4, 3):
        raw = f"{rate:.{places}f}".rstrip("0").rstrip(".")
        if raw:
            candidates.add(raw)
            if raw.startswith("0."):
                candidates.add(raw[1:])
    for places in (4, 3, 2, 1):
        fixed = f"{pct:.{places}f}"
        candidates.add(f"{fixed}%")
        trimmed = fixed.rstrip("0").rstrip(".")
        if trimmed:
            candidates.add(f"{trimmed}%")
    for places in (4, 3, 2, 1):
        val = f"{pct:.{places}f}".rstrip("0").rstrip(".")
        if val:
            candidates.add(f"{val}%")
    if abs(bps - round(bps)) < 1e-9:
        candidates.add(f"{int(round(bps))} bps")
        candidates.add(f"{int(round(bps))}bps")
    if pct >= 1 and abs(pct - round(pct)) < 1e-9:
        candidates.add(f"{int(round(pct))}%")
    return {item.lower() for item in candidates if item}


def normalize_text_for_search(text: str) -> tuple[str, str]:
    lowered = text.lower()
    compact = re.sub(r"\s+", " ", lowered)
    no_commas = compact.replace(",", "")
    return compact, no_commas


def search_variants(text: str, no_commas_text: str, variants: set[str]) -> bool:
    for variant in variants:
        if variant in text:
            return True
        if variant.replace(",", "") in no_commas_text:
            return True
    return False


def choose_contract_partner(contract_name: str, workbook_partners: set[str]) -> str:
    simplified = trim_filename(contract_name)
    alias = FILE_PARTNER_ALIASES.get(simplified)
    if alias:
        return alias
    if simplified in workbook_partners:
        return PARTNER_DISPLAY_ALIASES.get(simplified, next((p for p in workbook_partners if p == simplified), simplified))
    for partner in workbook_partners:
        if simplify_name(partner) == simplified:
            return partner
    return Path(contract_name).stem


def partner_match_key(name: str) -> str:
    simplified = simplify_name(name)
    return FILE_PARTNER_ALIASES.get(simplified, PARTNER_DISPLAY_ALIASES.get(simplified, name))


def snippet_windows(text: str, terms: list[str], window: int = 5, after: int = 20) -> list[str]:
    lines = [line.strip() for line in text.splitlines()]
    lines = [line for line in lines if line]
    snippets: list[str] = []
    seen: set[int] = set()
    for idx, line in enumerate(lines):
        lowered = line.lower()
        if any(term in lowered for term in terms):
            start = max(0, idx - window)
            if start in seen:
                continue
            seen.add(start)
            end = min(len(lines), idx + after)
            block = "\n".join(lines[start:end])
            snippets.append(block)
    return snippets[:8]


def describe_offline(row: dict[str, Any]) -> str:
    parts = [row.get("txnType", ""), row.get("speedFlag", "")]
    if row.get("processingMethod"):
        parts.append(row["processingMethod"])
    parts = [item for item in parts if item]
    return " / ".join(parts) or "Offline"


def describe_volume(row: dict[str, Any]) -> str:
    bits = [row.get("txnType") or "*", row.get("speedFlag") or "*"]
    if row.get("payerFunding"):
        bits.append(f"payer:{row['payerFunding']}")
    if row.get("payeeFunding"):
        bits.append(f"payee:{row['payeeFunding']}")
    if row.get("payeeCardType"):
        bits.append(f"card:{row['payeeCardType']}")
    if row.get("ccyGroup"):
        bits.append(f"ccy:{row['ccyGroup']}")
    return " ".join(bits)


def primary_keywords(row: dict[str, Any], category: str) -> list[str]:
    words: list[str] = []
    for key in ("txnType", "speedFlag", "processingMethod", "feeType", "surchargeType", "productType", "payerFunding", "payeeFunding", "payeeCardType", "note"):
        value = row.get(key)
        if not value:
            continue
        text = str(value)
        words.append(text.lower())
        words.extend(KEYWORD_SYNONYMS.get(text, []))
    if category == "FX":
        for key in ("payeeCorridor", "payeeCcy", "payerCorridor", "payerCcy"):
            value = row.get(key)
            if value:
                words.append(str(value).lower())
    return [word for word in words if word]


def workbook_rows_for_partner(data: dict[str, Any], partner: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    for row in data["off"]:
        if row["partner"] != partner:
            continue
        rows.append({
            "category": "Offline",
            "label": describe_offline(row),
            "value": row["fee"],
            "value_kind": "money",
            "keywords": primary_keywords(row, "Offline"),
            "raw": row,
        })

    for row in data["vol"]:
        if row["partner"] != partner:
            continue
        rows.append({
            "category": "Volume",
            "label": describe_volume(row),
            "value": row["rate"],
            "value_kind": "rate",
            "keywords": primary_keywords(row, "Volume"),
            "raw": row,
        })

    for row in data["fxRates"]:
        if row["partner"] != partner:
            continue
        rows.append({
            "category": "FX",
            "label": f"{row.get('payerCcy') or row.get('payerCorridor') or '*'}->{row.get('payeeCcy') or row.get('payeeCorridor') or '*'}",
            "value": row["rate"],
            "value_kind": "rate",
            "keywords": primary_keywords(row, "FX"),
            "raw": row,
        })

    for row in data["cap"]:
        if row["partner"] != partner:
            continue
        rows.append({
            "category": "Fee Cap",
            "label": f"{row['productType']} {row['capType']}",
            "value": row["amount"],
            "value_kind": "money",
            "keywords": primary_keywords(row, "Fee Cap"),
            "raw": row,
        })

    for row in data["rs"]:
        if row["partner"] != partner:
            continue
        rows.append({
            "category": "Rev Share",
            "label": row["txnType"],
            "value": row["revSharePct"],
            "value_kind": "rate",
            "keywords": primary_keywords(row, "Rev Share"),
            "raw": row,
        })

    for row in data["mins"]:
        if row["partner"] != partner:
            continue
        rows.append({
            "category": "Minimum",
            "label": f"Monthly minimum {row['minVol']:.0f}-{row['maxVol']:.0f}",
            "value": row["minAmount"],
            "value_kind": "money",
            "keywords": ["minimum", "monthly minimum"],
            "raw": row,
        })

    for row in data["plat"]:
        if row["partner"] != partner:
            continue
        rows.append({
            "category": "Platform",
            "label": "Monthly platform fee",
            "value": row["monthlyFee"],
            "value_kind": "money",
            "keywords": ["platform", "subscription", "monthly"],
            "raw": row,
        })

    for row in data["revf"]:
        if row["partner"] != partner:
            continue
        rows.append({
            "category": "Reversal",
            "label": f"{row.get('payerFunding') or 'All'} reversal fee",
            "value": row["feePerReversal"],
            "value_kind": "money",
            "keywords": primary_keywords(row, "Reversal") + ["reversal", "return"],
            "raw": row,
        })

    for row in data["impl"]:
        if row["partner"] != partner:
            continue
        rows.append({
            "category": "Implementation",
            "label": row["feeType"],
            "value": row["feeAmount"],
            "value_kind": "money",
            "keywords": primary_keywords(row, "Implementation") + ["implementation", "setup", "settlement"],
            "raw": row,
        })

    for row in data["vaFees"]:
        if row["partner"] != partner:
            continue
        rows.append({
            "category": "Virtual Account",
            "label": f"{row['feeType']} {row['minAccounts']}-{row['maxAccounts']}",
            "value": row["feePerAccount"],
            "value_kind": "money",
            "keywords": primary_keywords(row, "Virtual Account") + ["virtual account", "account"],
            "raw": row,
        })

    for row in data["surch"]:
        if row["partner"] != partner:
            continue
        rows.append({
            "category": "Surcharge",
            "label": row["surchargeType"],
            "value": row["rate"],
            "value_kind": "rate",
            "keywords": primary_keywords(row, "Surcharge") + ["surcharge"],
            "raw": row,
        })

    return rows


def evaluate_partner(contract_docs: list[dict[str, Any]], workbook_rows: list[dict[str, Any]]) -> dict[str, Any]:
    combined_text = "\n\n".join(doc["text"] for doc in contract_docs)
    compact_text, no_commas_text = normalize_text_for_search(combined_text)
    pricing_snippets = snippet_windows(combined_text, PRICING_KEYWORDS)

    row_results = []
    matched = 0
    missing = 0
    for row in workbook_rows:
        variants = money_variants(float(row["value"])) if row["value_kind"] == "money" else pct_variants(float(row["value"]))
        numeric_hit = search_variants(compact_text, no_commas_text, variants)
        keyword_hit = True
        if row["keywords"]:
            keyword_hit = any(keyword in compact_text for keyword in row["keywords"])
        hit = numeric_hit and keyword_hit
        row_results.append({
            "category": row["category"],
            "label": row["label"],
            "value_kind": row["value_kind"],
            "value": row["value"],
            "numeric_hit": numeric_hit,
            "keyword_hit": keyword_hit,
            "hit": hit,
            "variants": sorted(variants),
        })
        if hit:
            matched += 1
        else:
            missing += 1

    score = round((matched / len(workbook_rows)) * 100, 1) if workbook_rows else 0.0
    suspicious = [row for row in row_results if not row["hit"]][:20]
    return {
        "pricing_snippets": pricing_snippets,
        "row_results": row_results,
        "matched_rows": matched,
        "missing_rows": missing,
        "score": score,
        "has_pricing_snippets": bool(pricing_snippets),
        "contract_text_length": len(combined_text),
        "suspicious_rows": suspicious,
    }


def category_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = defaultdict(int)
    for row in rows:
        counts[row["category"]] += 1
    return dict(sorted(counts.items()))


def html_report(summary: dict[str, Any]) -> str:
    partner_cards = []
    for partner in summary["partners"]:
        row = partner["audit"]
        suspicious = "".join(
            f"<li><strong>{item['category']}</strong> · {item['label']} · {format_value(item['value_kind'], item['value'])}</li>"
            for item in row["suspicious_rows"][:8]
        ) or "<li>No suspicious rows in the top sample.</li>"
        snippets = "".join(
            f"<pre>{escape_html(snippet)}</pre>"
            for snippet in row["pricing_snippets"][:3]
        ) or "<p>No pricing anchors were found automatically.</p>"
        files = ", ".join(doc["name"] for doc in partner["contracts"]) or "No matching PDF"
        counts = ", ".join(f"{k}: {v}" for k, v in partner["category_counts"].items()) or "No workbook rows"
        partner_cards.append(
            f"""
            <section class="card">
              <div class="header">
                <div>
                  <h2>{escape_html(partner['partner'])}</h2>
                  <p>Contracts: {escape_html(files)}</p>
                  <p>Workbook rows: {escape_html(counts)}</p>
                </div>
                <div class="score {'good' if row['score'] >= 80 else 'warn' if row['score'] >= 50 else 'bad'}">{row['score']}%</div>
              </div>
              <div class="metrics">
                <span>Matched rows: {row['matched_rows']}</span>
                <span>Missing rows: {row['missing_rows']}</span>
                <span>Pricing snippets: {'yes' if row['has_pricing_snippets'] else 'no'}</span>
                <span>Text chars: {row['contract_text_length']:,}</span>
              </div>
              <h3>Likely Missing Or Mismatched Workbook Rows</h3>
              <ul>{suspicious}</ul>
              <h3>Pricing Snippets</h3>
              {snippets}
            </section>
            """
        )

    unmatched_pdfs = "".join(f"<li>{escape_html(name)}</li>" for name in summary["unmatched_pdfs"]) or "<li>None</li>"
    unmatched_partners = "".join(f"<li>{escape_html(name)}</li>" for name in summary["unmatched_workbook_partners"]) or "<li>None</li>"

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Contract Audit Report</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, sans-serif; margin: 0; background: #f7f2ea; color: #2f251b; }}
    main {{ width: min(1200px, calc(100vw - 32px)); margin: 0 auto; padding: 24px 0 48px; }}
    .hero {{ background: linear-gradient(135deg, #0f5a52, #204a7a); color: #fff8ef; padding: 28px 0; }}
    .hero .inner {{ width: min(1200px, calc(100vw - 32px)); margin: 0 auto; }}
    .stats {{ display: flex; gap: 12px; flex-wrap: wrap; margin-top: 14px; }}
    .pill {{ background: rgba(255,255,255,0.14); border: 1px solid rgba(255,255,255,0.12); border-radius: 999px; padding: 8px 12px; font-size: 14px; }}
    .grid {{ display: grid; gap: 16px; }}
    .card {{ background: rgba(255,252,247,0.95); border: 1px solid rgba(66,50,31,0.12); border-radius: 16px; padding: 18px; box-shadow: 0 14px 32px rgba(33,24,15,0.08); }}
    .header {{ display: flex; justify-content: space-between; gap: 16px; align-items: start; }}
    .header h2 {{ margin: 0 0 8px; }}
    .header p {{ margin: 4px 0; color: #72614e; }}
    .score {{ min-width: 84px; text-align: center; font-size: 24px; font-weight: 800; padding: 10px 12px; border-radius: 14px; }}
    .score.good {{ background: #dff4e7; color: #1e6340; }}
    .score.warn {{ background: #f8ebc6; color: #7c5a10; }}
    .score.bad {{ background: #f9ddd8; color: #8b3828; }}
    .metrics {{ display: flex; gap: 12px; flex-wrap: wrap; margin: 12px 0 18px; color: #5d4d3d; font-size: 14px; }}
    h3 {{ margin: 18px 0 8px; font-size: 16px; }}
    pre {{ white-space: pre-wrap; background: #f4ebde; padding: 12px; border-radius: 12px; border: 1px solid rgba(66,50,31,0.08); overflow-x: auto; }}
    .double {{ display: grid; grid-template-columns: repeat(2, minmax(0,1fr)); gap: 16px; }}
    ul {{ margin: 0; padding-left: 20px; }}
    @media (max-width: 900px) {{ .double {{ grid-template-columns: 1fr; }} .header {{ flex-direction: column; }} }}
  </style>
</head>
<body>
  <div class="hero">
    <div class="inner">
      <h1>Contract Audit Report</h1>
      <p>Generated {escape_html(summary['generated_at'])}</p>
      <div class="stats">
        <span class="pill">PDFs: {summary['pdf_count']}</span>
        <span class="pill">Partners audited: {summary['audited_partner_count']}</span>
        <span class="pill">Average score: {summary['average_score']}%</span>
        <span class="pill">Low-confidence partners: {summary['low_confidence_count']}</span>
      </div>
    </div>
  </div>
  <main class="grid">
    <section class="double">
      <div class="card">
        <h2>Unmatched PDFs</h2>
        <ul>{unmatched_pdfs}</ul>
      </div>
      <div class="card">
        <h2>Workbook Partners Without PDFs</h2>
        <ul>{unmatched_partners}</ul>
      </div>
    </section>
    {''.join(partner_cards)}
  </main>
</body>
</html>"""


def escape_html(value: str) -> str:
    return (
        str(value)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def format_value(kind: str, value: Any) -> str:
    if kind == "money":
        return f"${float(value):,.2f}"
    return f"{float(value) * 100:.4f}%"


def main() -> int:
    if len(sys.argv) != 2:
        print("Usage: audit_contracts.py <pdf-folder>", file=sys.stderr)
        return 1

    pdf_folder = Path(sys.argv[1]).expanduser()
    if not pdf_folder.exists():
        print(f"Folder not found: {pdf_folder}", file=sys.stderr)
        return 1

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    TEXT_EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    workbook = load_workbook_defaults()
    contracts = extract_contract_texts(pdf_folder)

    for contract in contracts:
        safe_name = re.sub(r"[^A-Za-z0-9._-]+", "_", Path(contract["name"]).stem).strip("_") or "contract"
        (TEXT_EXPORT_DIR / f"{safe_name}.txt").write_text(contract["text"])

    workbook_partners = set(workbook["ps"])
    contracts_by_partner: dict[str, list[dict[str, Any]]] = defaultdict(list)
    unmatched_pdfs: list[str] = []

    for contract in contracts:
        partner = choose_contract_partner(contract["name"], workbook_partners)
        if partner in workbook_partners:
            contracts_by_partner[partner].append(contract)
        else:
            unmatched_pdfs.append(contract["name"])

    partner_results = []
    matched_contract_partners = set(contracts_by_partner.keys())
    for partner in sorted(matched_contract_partners):
        rows = workbook_rows_for_partner(workbook, partner)
        audit = evaluate_partner(contracts_by_partner[partner], rows)
        partner_results.append({
            "partner": partner,
            "contracts": [{"name": doc["name"], "page_count": doc["pageCount"], "path": doc["path"]} for doc in contracts_by_partner[partner]],
            "category_counts": category_counts(rows),
            "audit": audit,
        })

    average_score = round(sum(item["audit"]["score"] for item in partner_results) / len(partner_results), 1) if partner_results else 0.0
    low_confidence = [item for item in partner_results if item["audit"]["score"] < 60 or not item["audit"]["has_pricing_snippets"]]

    summary = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "pdf_count": len(contracts),
        "audited_partner_count": len(partner_results),
        "average_score": average_score,
        "low_confidence_count": len(low_confidence),
        "unmatched_pdfs": sorted(unmatched_pdfs),
        "unmatched_workbook_partners": sorted(set(workbook_partners) - matched_contract_partners),
        "partners": partner_results,
    }

    (REPORT_DIR / "contract_audit_report.json").write_text(json.dumps(summary, indent=2))
    (REPORT_DIR / "contract_audit_report.html").write_text(html_report(summary))

    concise = [
        {
            "partner": item["partner"],
            "score": item["audit"]["score"],
            "missing_rows": item["audit"]["missing_rows"],
            "snippets": item["audit"]["has_pricing_snippets"],
            "contracts": [doc["name"] for doc in item["contracts"]],
        }
        for item in sorted(partner_results, key=lambda row: (row["audit"]["score"], -row["audit"]["missing_rows"]))
    ]
    (REPORT_DIR / "contract_audit_summary.json").write_text(json.dumps(concise, indent=2))

    with (REPORT_DIR / "contract_audit_summary.csv").open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["partner", "score", "missing_rows", "snippets", "contracts"])
        writer.writeheader()
        for row in concise:
            writer.writerow({
                "partner": row["partner"],
                "score": row["score"],
                "missing_rows": row["missing_rows"],
                "snippets": row["snippets"],
                "contracts": "; ".join(row["contracts"]),
            })

    print(f"Wrote report to {REPORT_DIR / 'contract_audit_report.html'}")
    print(f"Wrote JSON to {REPORT_DIR / 'contract_audit_report.json'}")
    print(f"Wrote CSV to {REPORT_DIR / 'contract_audit_summary.csv'}")
    print(f"Wrote extracted text files to {TEXT_EXPORT_DIR}")
    print("Lowest-confidence partners:")
    for item in concise[:10]:
        print(f"  {item['partner']}: score={item['score']} missing_rows={item['missing_rows']} snippets={item['snippets']} files={', '.join(item['contracts'])}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

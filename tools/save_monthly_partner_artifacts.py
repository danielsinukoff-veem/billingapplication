from __future__ import annotations

import argparse
import csv
import html
import json
import os
import shutil
import subprocess
import sys
import tempfile
from collections import OrderedDict
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

if __package__ in (None, ""):
    sys.path.append(str(Path(__file__).resolve().parents[1]))
    from server.automation_engine import (  # type: ignore
        build_receivable_entry,
        format_iso_date,
        format_period_label,
        get_all_invoice_periods,
        get_expected_invoice_send_date,
        get_invoice_due_date,
        normalize_month_key,
        round_currency,
    )
    from server.invoice_engine import calculate_active_invoice_totals, calculate_invoice  # type: ignore
    from server.storage import SharedWorkspaceStore  # type: ignore
else:
    from ..server.automation_engine import (
        build_receivable_entry,
        format_iso_date,
        format_period_label,
        get_all_invoice_periods,
        get_expected_invoice_send_date,
        get_invoice_due_date,
        normalize_month_key,
        round_currency,
    )
    from ..server.invoice_engine import calculate_active_invoice_totals, calculate_invoice
    from ..server.storage import SharedWorkspaceStore


ROOT_DIR = Path(__file__).resolve().parents[1]
DB_PATH = ROOT_DIR / "server" / "data" / "shared_workspace.db"
PARTNER_CONTRACTS_DIR = ROOT_DIR / "Partner Contracts"
CHROME_CANDIDATES = [
    Path("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"),
    Path("/Applications/Chromium.app/Contents/MacOS/Chromium"),
]
PARTNER_FOLDER_ALIASES = {
    "graph finance": "Graph AI",
    "halo recruiting": "Halorecruiting",
    "lightnet": "Lightnet",
    "nibss (turbotech)": "NIBBS",
    "remittanceshub": "Remittanceshub",
    "yeepay": "YeePay",
}


def norm(value: Any) -> str:
    return "".join(char for char in str(value or "").strip().lower() if char.isalnum())


def slug(value: Any) -> str:
    base = "".join(char if char.isalnum() else "-" for char in str(value or "").strip())
    while "--" in base:
        base = base.replace("--", "-")
    return base.strip("-") or "artifact"


def previous_month_key(today: date | None = None) -> str:
    current = today or date.today()
    first_of_month = current.replace(day=1)
    previous = first_of_month - timedelta(days=1)
    return previous.strftime("%Y-%m")


def resolve_partner_folder(partner: str) -> Path:
    explicit = PARTNER_FOLDER_ALIASES.get(str(partner or "").strip().lower())
    if explicit:
        return PARTNER_CONTRACTS_DIR / explicit
    target = norm(partner)
    for entry in PARTNER_CONTRACTS_DIR.iterdir():
        if not entry.is_dir():
            continue
        if norm(entry.name) == target:
            return entry
    raise FileNotFoundError(f"Could not find partner folder for {partner}")


def get_chrome_binary() -> Path:
    for candidate in CHROME_CANDIDATES:
        if candidate.exists():
            return candidate
    raise FileNotFoundError("Google Chrome was not found. Install Chrome or update CHROME_CANDIDATES.")


def build_invoice_documents(invoice: dict[str, Any]) -> list[dict[str, Any]]:
    documents: list[dict[str, Any]] = []
    for kind in ("receivable", "payable"):
        source_lines = [
            line for line in invoice.get("lines", [])
            if (kind == "receivable" and line.get("dir") in {"charge", "offset"})
            or (kind == "payable" and line.get("dir") == "pay")
        ]
        totals = calculate_active_invoice_totals(source_lines)
        amount_due = round_currency(max(totals["chg"] - totals["offset"], 0)) if kind == "receivable" else round_currency(max(totals["pay"], 0))
        groups = [
            group for group in invoice.get("groups", [])
            if (
                kind == "receivable"
                and (group.get("charge", 0) > 0 or group.get("offset", 0) > 0 or group.get("displayCharge", 0) > 0 or group.get("displayOffset", 0) > 0)
            ) or (
                kind == "payable"
                and (group.get("pay", 0) > 0 or group.get("displayPay", 0) > 0)
            )
        ]
        has_visible = any(
            (
                kind == "receivable"
                and (group.get("charge", 0) > 0 or group.get("offset", 0) > 0 or group.get("displayCharge", 0) > 0 or group.get("displayOffset", 0) > 0)
            ) or (
                kind == "payable"
                and (group.get("pay", 0) > 0 or group.get("displayPay", 0) > 0)
            )
            for group in groups
        )
        if not has_visible and amount_due <= 0:
            continue
        documents.append(
            {
                "kind": kind,
                "title": "AR Invoice" if kind == "receivable" else "AP Invoice",
                "amountLabel": f"{invoice.get('partner')} Owes" if kind == "receivable" else "Veem Owes",
                "amountDue": amount_due,
                "chargeTotal": round_currency(totals["chg"]),
                "creditTotal": round_currency(totals["offset"]),
                "payTotal": round_currency(totals["pay"]),
                "groups": groups,
                "lines": source_lines,
            }
        )
    return documents


def infer_invoice_date(snapshot: dict[str, Any], partner: str, period: str, kind: str) -> str:
    for row in snapshot.get("pInvoices", []) or []:
        if str(row.get("partner") or "").strip().lower() != str(partner or "").strip().lower():
            continue
        if normalize_month_key(row.get("period")) != normalize_month_key(period):
            continue
        if str(row.get("kind") or "receivable") != kind:
            continue
        invoice_date = str(row.get("invoiceDate") or "").strip()[:10]
        if invoice_date:
            return invoice_date
    expected = get_expected_invoice_send_date(snapshot, partner, period)
    if expected:
        return expected
    next_month = datetime.strptime(f"{period}-01", "%Y-%m-%d").date().replace(day=1)
    if next_month.month == 12:
        return date(next_month.year + 1, 1, 1).isoformat()
    return date(next_month.year, next_month.month + 1, 1).isoformat()


def get_due_date(snapshot: dict[str, Any], partner: str, period: str, kind: str, invoice_date: str) -> str:
    if kind != "receivable":
        return ""
    for row in snapshot.get("pInvoices", []) or []:
        if str(row.get("partner") or "").strip().lower() != str(partner or "").strip().lower():
            continue
        if normalize_month_key(row.get("period")) != normalize_month_key(period):
            continue
        if str(row.get("kind") or "receivable") != kind:
            continue
        override = str(row.get("dueDateOverride") or "").strip()[:10]
        if override:
            return override
    return get_invoice_due_date(snapshot, partner, period, invoice_date)


def money(value: Any) -> str:
    return f"${round_currency(value):,.2f}"


def render_invoice_html(snapshot: dict[str, Any], invoice: dict[str, Any], document: dict[str, Any]) -> str:
    partner = str(invoice.get("partner") or "")
    period = str(invoice.get("period") or "")
    invoice_date = infer_invoice_date(snapshot, partner, period, document["kind"])
    due_date = get_due_date(snapshot, partner, period, document["kind"], invoice_date)
    rows = []
    for group in document["groups"]:
        amount = group.get("charge", 0) - group.get("offset", 0) if document["kind"] == "receivable" else group.get("pay", 0)
        if round_currency(amount) <= 0 and round_currency(group.get("displayCharge", 0) + group.get("displayPay", 0)) <= 0:
            continue
        rows.append(
            "<tr>"
            f"<td>{html.escape(str(group.get('cat') or ''))}</td>"
            f"<td>{html.escape(str(group.get('label') or ''))}</td>"
            f"<td>{html.escape(str(group.get('summary') or ''))}</td>"
            f"<td style='text-align:right'>{money(amount)}</td>"
            "</tr>"
        )
    notes = "".join(f"<li>{html.escape(str(note))}</li>" for note in invoice.get("notes", []) or [])
    terms = f"Net {max((datetime.fromisoformat(due_date) - datetime.fromisoformat(invoice_date)).days, 0)}" if invoice_date and due_date else ""
    return f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <style>
    body {{ font-family: Arial, sans-serif; color: #1f2937; margin: 32px; }}
    .header {{ display:flex; justify-content:space-between; gap:24px; margin-bottom:24px; }}
    .title {{ font-size:32px; font-weight:700; margin:0 0 6px; }}
    .meta td {{ padding:4px 0 4px 16px; vertical-align:top; }}
    .meta td:first-child {{ padding-left:0; font-weight:700; color:#374151; }}
    table.line {{ width:100%; border-collapse:collapse; margin-top:16px; }}
    table.line th {{ text-align:left; background:#f3ead8; padding:10px; border-bottom:1px solid #ddd; }}
    table.line td {{ padding:10px; border-bottom:1px solid #e5e7eb; }}
    .totals {{ margin-top:18px; width:320px; margin-left:auto; }}
    .totals-row {{ display:flex; justify-content:space-between; padding:6px 0; }}
    .grand {{ font-size:20px; font-weight:700; border-top:2px solid #111827; padding-top:10px; }}
    .notes {{ margin-top:22px; }}
  </style>
</head>
<body>
  <div class="header">
    <div>
      <div class="title">{html.escape(document['title'])}</div>
      <div><strong>Partner:</strong> {html.escape(partner)}</div>
      <div><strong>Period:</strong> {html.escape(invoice.get('periodLabel') or format_period_label(period))}</div>
      <div><strong>Range:</strong> {html.escape(str(invoice.get('periodDateRange') or ''))}</div>
    </div>
    <table class="meta">
      <tr><td>Invoice Date</td><td>{html.escape(format_iso_date(invoice_date)) if invoice_date else '—'}</td></tr>
      <tr><td>Due Date</td><td>{html.escape(format_iso_date(due_date)) if due_date else '—'}</td></tr>
      <tr><td>Terms</td><td>{html.escape(terms or '—')}</td></tr>
      <tr><td>{html.escape(document['amountLabel'])}</td><td>{money(document['amountDue'])}</td></tr>
    </table>
  </div>
  <table class="line">
    <thead>
      <tr>
        <th style="width:18%">Category</th>
        <th style="width:34%">Description</th>
        <th style="width:28%">Activity</th>
        <th style="width:20%; text-align:right">Amount</th>
      </tr>
    </thead>
    <tbody>
      {''.join(rows) or '<tr><td colspan="4">No invoice lines generated.</td></tr>'}
    </tbody>
  </table>
  <div class="totals">
    {f"<div class='totals-row'><span>Credits</span><span>{money(document['creditTotal'])}</span></div>" if document['kind'] == 'receivable' and round_currency(document['creditTotal']) > 0 else ""}
    <div class="totals-row grand"><span>Total</span><span>{money(document['amountDue'])}</span></div>
  </div>
  {f"<div class='notes'><strong>Notes</strong><ul>{notes}</ul></div>" if notes else ""}
</body>
</html>"""


def build_soa_entries(snapshot: dict[str, Any], partner: str, as_of: date) -> list[dict[str, Any]]:
    invoice_cache: dict[tuple[str, str], dict[str, Any]] = {}
    entries = []
    for period in get_all_invoice_periods(snapshot):
        entry = build_receivable_entry(snapshot, partner, period, invoice_cache)
        if round_currency(entry.get("amountDue")) <= 0 or round_currency(entry.get("balance")) <= 0:
            continue
        due_date = str(entry.get("dueDate") or "")
        if not due_date:
            continue
        try:
            due = datetime.fromisoformat(due_date).date()
        except ValueError:
            continue
        if due >= as_of:
            continue
        entries.append(entry)
    entries.sort(key=lambda item: item["period"])
    return entries


def render_soa_html(snapshot: dict[str, Any], partner: str, entries: list[dict[str, Any]], as_of: date) -> str:
    rows = []
    total_due = 0.0
    total_paid = 0.0
    total_balance = 0.0
    for entry in entries:
        amount_due = round_currency(entry.get("amountDue"))
        amount_paid = round_currency(entry.get("amountPaid"))
        balance = round_currency(entry.get("balance"))
        total_due += amount_due
        total_paid += amount_paid
        total_balance += balance
        rows.append(
            "<tr>"
            f"<td>{html.escape(format_period_label(entry['period']))}</td>"
            f"<td>{html.escape(format_iso_date(entry.get('invoiceDate') or entry.get('expectedSendDate') or ''))}</td>"
            f"<td>{html.escape(format_iso_date(entry.get('dueDate') or ''))}</td>"
            f"<td style='text-align:right'>{money(amount_due)}</td>"
            f"<td style='text-align:right'>{money(amount_paid)}</td>"
            f"<td style='text-align:right'>{money(balance)}</td>"
            "</tr>"
        )
    return f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <style>
    body {{ font-family: Arial, sans-serif; color: #1f2937; margin: 32px; }}
    .title {{ font-size:28px; font-weight:700; margin:0 0 8px; }}
    table {{ width:100%; border-collapse:collapse; margin-top:18px; }}
    th {{ text-align:left; background:#f3ead8; padding:10px; border-bottom:1px solid #ddd; }}
    td {{ padding:10px; border-bottom:1px solid #e5e7eb; }}
    .totals {{ margin-top:18px; width:320px; margin-left:auto; }}
    .totals-row {{ display:flex; justify-content:space-between; padding:6px 0; }}
    .grand {{ font-size:20px; font-weight:700; border-top:2px solid #111827; padding-top:10px; }}
  </style>
</head>
<body>
  <div class="title">Statement of Account</div>
  <div><strong>Partner:</strong> {html.escape(partner)}</div>
  <div><strong>As of:</strong> {html.escape(as_of.strftime('%B %-d, %Y'))}</div>
  <table>
    <thead>
      <tr>
        <th>Period</th>
        <th>Invoice Date</th>
        <th>Due Date</th>
        <th style='text-align:right'>Amount Due</th>
        <th style='text-align:right'>Paid</th>
        <th style='text-align:right'>Outstanding</th>
      </tr>
    </thead>
    <tbody>
      {''.join(rows) or '<tr><td colspan="6">No overdue receivable balances.</td></tr>'}
    </tbody>
  </table>
  <div class="totals">
    <div class="totals-row"><span>Total Due</span><span>{money(total_due)}</span></div>
    <div class="totals-row"><span>Total Paid</span><span>{money(total_paid)}</span></div>
    <div class="totals-row grand"><span>Total Outstanding</span><span>{money(total_balance)}</span></div>
  </div>
</body>
</html>"""


def render_pdf_from_html(html_text: str, output_path: Path) -> None:
    chrome = get_chrome_binary()
    profile_dir = Path(tempfile.mkdtemp(prefix="billing-artifact-pdf-profile-"))
    with tempfile.NamedTemporaryFile("w", suffix=".html", delete=False, encoding="utf-8") as handle:
        handle.write(html_text)
        temp_path = Path(handle.name)
    try:
        process = subprocess.Popen(
            [
                str(chrome),
                "--headless=new",
                "--disable-gpu",
                f"--user-data-dir={profile_dir}",
                "--no-first-run",
                "--no-default-browser-check",
                "--no-pdf-header-footer",
                f"--print-to-pdf={output_path}",
                temp_path.as_uri(),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        last_size = -1
        stable_ticks = 0
        for _ in range(240):
            if process.poll() is not None:
                if output_path.exists() and output_path.stat().st_size > 0:
                    return
                stdout, stderr = process.communicate(timeout=1)
                raise RuntimeError((stderr or stdout or f"Chrome PDF export failed with exit code {process.returncode}").strip())
            if output_path.exists():
                size = output_path.stat().st_size
                if size > 0 and size == last_size:
                    stable_ticks += 1
                else:
                    stable_ticks = 0
                last_size = size
                if size > 0 and stable_ticks >= 2:
                    process.terminate()
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                    return
            import time
            time.sleep(0.5)
        process.kill()
        raise RuntimeError(f"Timed out waiting for PDF export: {output_path}")
    finally:
        try:
            temp_path.unlink(missing_ok=True)
        except OSError:
            pass
        try:
            shutil.rmtree(profile_dir, ignore_errors=True)
        except OSError:
            pass


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    headers = sorted({key for row in rows for key in row.keys()})
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def collect_partner_transaction_rows(snapshot: dict[str, Any], partner: str, period: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in snapshot.get("lookerImportedDetailRows", []) or []:
        if str(row.get("partner") or "").strip().lower() != str(partner or "").strip().lower():
            continue
        if normalize_month_key(row.get("period")) != normalize_month_key(period):
            continue
        rows.append({"sourceSection": "detail", **row})
    for section in ("ltxn", "lrev", "lva", "lrs", "lfxp"):
        for row in snapshot.get(section, []) or []:
            if str(row.get("partner") or "").strip().lower() != str(partner or "").strip().lower():
                continue
            if normalize_month_key(row.get("period")) != normalize_month_key(period):
                continue
            rows.append({"sourceSection": section, **row})
    deduped: OrderedDict[str, dict[str, Any]] = OrderedDict()
    for row in rows:
        key = json.dumps(row, sort_keys=True, default=str)
        deduped[key] = row
    return list(deduped.values())


def ensure_subfolders(folder: Path) -> dict[str, Path]:
    targets = {
        "invoice": folder / "Invoice",
        "soa": folder / "SOA",
        "transactions": folder / "Transactions",
    }
    for path in targets.values():
        path.mkdir(parents=True, exist_ok=True)
    return targets


def save_partner_artifacts(snapshot: dict[str, Any], partner: str, period: str, as_of: date, overwrite: bool = False, dry_run: bool = False) -> dict[str, Any]:
    partner_folder = resolve_partner_folder(partner)
    targets = ensure_subfolders(partner_folder)
    invoice = calculate_invoice(snapshot, partner, period, period)
    documents = build_invoice_documents(invoice)
    saved = {"partner": partner, "period": period, "documents": [], "soa": None, "transactionsCsv": None, "manifest": None}

    for document in documents:
        filename = f"{partner} {period} {'AR' if document['kind'] == 'receivable' else 'AP'} Invoice.pdf"
        pdf_path = targets["invoice"] / filename
        if pdf_path.exists() and not overwrite:
            saved["documents"].append(str(pdf_path))
            continue
        if not dry_run:
            render_pdf_from_html(render_invoice_html(snapshot, invoice, document), pdf_path)
        saved["documents"].append(str(pdf_path))

    transaction_rows = collect_partner_transaction_rows(snapshot, partner, period)
    csv_rows = transaction_rows or [{
        "partner": partner,
        "period": period,
        "notice": "No imported transaction rows were available for this partner and period at export time.",
    }]
    csv_path = targets["transactions"] / f"{partner} {period} Transactions.csv"
    if not csv_path.exists() or overwrite:
        if not dry_run:
            write_csv(csv_path, csv_rows)
    saved["transactionsCsv"] = str(csv_path)

    soa_entries = build_soa_entries(snapshot, partner, as_of)
    if soa_entries:
        soa_path = targets["soa"] / f"{partner} SOA as of {as_of.isoformat()}.pdf"
        if not soa_path.exists() or overwrite:
            if not dry_run:
                render_pdf_from_html(render_soa_html(snapshot, partner, soa_entries, as_of), soa_path)
        saved["soa"] = str(soa_path)

    manifest_path = targets["invoice"] / f"{partner} {period} Artifact Manifest.json"
    if not manifest_path.exists() or overwrite:
        manifest = {
            "partner": partner,
            "period": period,
            "generatedAt": datetime.now(UTC).isoformat(),
            "snapshotSavedAt": snapshot.get("_saved"),
            "documents": saved["documents"],
            "soa": saved["soa"],
            "transactionsCsv": saved["transactionsCsv"],
        }
        if not dry_run:
            manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    saved["manifest"] = str(manifest_path)
    return saved


def load_snapshot() -> dict[str, Any]:
    store = SharedWorkspaceStore(DB_PATH)
    workspace = store.get_workspace()
    snapshot = workspace.get("snapshot")
    if not isinstance(snapshot, dict):
        raise RuntimeError("No shared workbook snapshot is available yet.")
    return snapshot


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Save monthly invoice, SOA, and transaction artifacts into partner contract folders.")
    parser.add_argument("--period", default="", help="Billing period in YYYY-MM format. Defaults to the previous completed month.")
    parser.add_argument("--partner", default="", help="Optional single partner to export.")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing saved artifacts for the period.")
    parser.add_argument("--dry-run", action="store_true", help="Print what would be saved without writing files.")
    parser.add_argument("--as-of", default="", help="As-of date for SOA evaluation in YYYY-MM-DD format. Defaults to today.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    period = normalize_month_key(args.period) or previous_month_key()
    as_of = datetime.strptime(args.as_of, "%Y-%m-%d").date() if args.as_of else date.today()
    snapshot = load_snapshot()
    partners = [args.partner] if args.partner else sorted({str(partner) for partner in snapshot.get("ps", []) or []})
    results = []
    errors = []
    for partner in partners:
        try:
            result = save_partner_artifacts(snapshot, partner, period, as_of, overwrite=args.overwrite, dry_run=args.dry_run)
            if result["documents"] or result["transactionsCsv"] or result["soa"]:
                results.append(result)
        except Exception as error:  # pragma: no cover - operational guard
            errors.append({"partner": partner, "error": str(error)})
    print(json.dumps({"period": period, "asOf": as_of.isoformat(), "results": results, "errors": errors}, indent=2))
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())

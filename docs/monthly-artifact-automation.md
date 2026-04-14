# Monthly Billing Artifact Automation

This automation saves audit-ready billing artifacts into each partner folder under:

- `Partner Contracts/<Partner>/Invoice`
- `Partner Contracts/<Partner>/SOA`
- `Partner Contracts/<Partner>/Transactions`

It uses the shared workbook snapshot in:

- `/Users/danielsinukoff/Documents/billing-workbook/server/data/shared_workspace.db`

## What it saves

For the previous completed billing month, the generator creates:

- `AR` invoice PDF when a receivable invoice exists
- `AP` invoice PDF when a payable invoice exists
- `SOA` PDF when the partner has overdue receivable balances
- a transaction CSV for the month
- a JSON artifact manifest for audit traceability

## Static audit behavior

The generator does **not** overwrite existing files unless `--overwrite` is passed.
That keeps the saved PDF/CSV artifacts static even if later data changes appear in the workbook.

## Command

```bash
python3 /Users/danielsinukoff/Documents/billing-workbook/tools/save_monthly_partner_artifacts.py
```

Useful options:

```bash
python3 /Users/danielsinukoff/Documents/billing-workbook/tools/save_monthly_partner_artifacts.py --period 2026-03
python3 /Users/danielsinukoff/Documents/billing-workbook/tools/save_monthly_partner_artifacts.py --partner Stampli --period 2026-02
python3 /Users/danielsinukoff/Documents/billing-workbook/tools/save_monthly_partner_artifacts.py --dry-run
python3 /Users/danielsinukoff/Documents/billing-workbook/tools/save_monthly_partner_artifacts.py --overwrite
```

## Current implementation notes

- The script reads the shared snapshot directly from SQLite, so it does not require the API to be up just to save artifacts.
- PDF generation uses headless Google Chrome on macOS.
- Transaction CSVs come from imported detail rows when available and fall back to the saved workbook transaction sections otherwise.

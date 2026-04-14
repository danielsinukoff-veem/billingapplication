# Partner Billing Workbook

A zero-build browser app for partner billing configuration, invoice calculation, workbook backup, and contract import/verification.

## Run locally

From this folder:

```bash
python3 -m http.server 4173
```

Then open [http://localhost:4173](http://localhost:4173).

## Notes

- Workbook data autosaves to browser `localStorage`.
- `Export Backup` downloads the full workbook as JSON.
- `Import Backup` restores a saved workbook JSON file.
- Contract parsing in this static build expects structured JSON. If you only have raw contract text, use `Copy Extraction Prompt`, run that prompt in an LLM, and paste the returned JSON into the app.

## Shared-app scaffolding

This repo now includes the first migration scaffolding for a company-shared version of the app:

- Architecture plan: `/Users/danielsinukoff/Documents/billing-workbook/docs/shared-app-architecture.md`
- API contract: `/Users/danielsinukoff/Documents/billing-workbook/docs/shared-api-contract.md`
- Schema outline: `/Users/danielsinukoff/Documents/billing-workbook/docs/shared-app-schema.sql`
- Supabase migrations:
  - `/Users/danielsinukoff/Documents/billing-workbook/supabase/migrations/20260316_0001_core_schema.sql`
  - `/Users/danielsinukoff/Documents/billing-workbook/supabase/migrations/20260316_0002_rls.sql`
- Frontend shared-backend client:
  - `/Users/danielsinukoff/Documents/billing-workbook/shared-backend.js`
  - `/Users/danielsinukoff/Documents/billing-workbook/shared-config.js`

By default the app still runs in local mode. To point it at a shared backend later, configure `window.BILLING_APP_CONFIG` with a real `apiBaseUrl` and enable the shared workbook / remote invoice flags.

For a local reference shared deployment using the included Python API + SQLite store:

```bash
python3 /Users/danielsinukoff/Documents/billing-workbook/server/app.py --port 4174
```

Then open [http://127.0.0.1:4174](http://127.0.0.1:4174). The first load seeds the shared workbook snapshot from the app defaults, and subsequent saves/invoice reads go through the shared API.

For a Supabase-backed shared deployment, see:

- `/Users/danielsinukoff/Documents/billing-workbook/docs/supabase-backend-setup.md`
- `/Users/danielsinukoff/Documents/billing-workbook/server/.env.example`

## Batch contract audit

To compare the workbook defaults against a folder of partner PDFs:

```bash
python3 /Users/danielsinukoff/Documents/billing-workbook/tools/audit_contracts.py /Users/danielsinukoff/Desktop/Partner
```

Outputs are written to:

- `/Users/danielsinukoff/Documents/billing-workbook/reports/contract_audit/contract_audit_report.html`
- `/Users/danielsinukoff/Documents/billing-workbook/reports/contract_audit/contract_audit_report.json`
- `/Users/danielsinukoff/Documents/billing-workbook/reports/contract_audit/contract_audit_summary.csv`
- `/Users/danielsinukoff/Documents/billing-workbook/reports/contract_audit/extracted_text/`

The audit is heuristic. It is best used to surface likely mismatches and low-confidence contracts for review, not as a final legal signoff.

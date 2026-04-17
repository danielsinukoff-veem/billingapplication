# Partner Billing Workbook

A zero-build browser app for partner billing configuration, invoice calculation, workbook backup, and contract import/verification.

## Run locally

The frontend can be served as a static app:

```bash
python3 -m http.server 4173
```

Then open [http://localhost:4173](http://localhost:4173).

For the legacy shared backend reference, you can still run:

```bash
python3 /Users/danielsinukoff/Documents/billing-workbook/server/app.py --port 4174
```

Then open [http://127.0.0.1:4174](http://127.0.0.1:4174). The target deployment is AWS + S3 + n8n, so this backend is now only a reference implementation.

## Notes

- Workbook data autosaves to browser `localStorage`.
- `Export Backup` downloads the full workbook as JSON.
- `Import Backup` restores a saved workbook JSON file.
- Contract parsing in this static build expects structured JSON. If you only have raw contract text, use `Copy Extraction Prompt`, run that prompt in an LLM, and paste the returned JSON into the app.

## Conversion target

The target architecture is now AWS + S3 + n8n, with the browser app acting as the main user interface and the scheduled parsing/reconciliation work moving out of local Python processes.

Recommended handoff docs:

- AWS/S3/n8n handoff: `/Users/danielsinukoff/Documents/billing-workbook/docs/aws-s3-n8n-handoff.md`
- Shared API contract: `/Users/danielsinukoff/Documents/billing-workbook/docs/shared-api-contract.md`
- Shared data shape: `/Users/danielsinukoff/Documents/billing-workbook/docs/shared-app-schema.sql`
- n8n workflow examples:
  - `/Users/danielsinukoff/Documents/billing-workbook/docs/n8n-looker-direct.workflow.json`
  - `/Users/danielsinukoff/Documents/billing-workbook/docs/n8n-looker-cloud.workflow.json`
  - `/Users/danielsinukoff/Documents/billing-workbook/docs/n8n-billing-automation.workflow.json`
- Current n8n data input flow:
  - `/Users/danielsinukoff/Documents/billing-workbook/docs/n8n-looker-ingestion.md`
  - `/Users/danielsinukoff/Documents/billing-workbook/docs/n8n-looker-direct.workflow.json`
  - `/Users/danielsinukoff/Documents/billing-workbook/docs/n8n-looker-cloud.workflow.json`
- AWS contract ingestion starter:
  - `/Users/danielsinukoff/Documents/billing-workbook/docs/eng-handoff-aws-n8n/README.md`
  - `/Users/danielsinukoff/Documents/billing-workbook/docs/eng-handoff-aws-n8n/contract-s3-ingestion.workflow.json`
  - `/Users/danielsinukoff/Documents/billing-workbook/docs/eng-handoff-aws-n8n/contract-s3-ingestion.md`
- Frontend shared-backend client:
  - `/Users/danielsinukoff/Documents/billing-workbook/shared-backend.js`
  - `/Users/danielsinukoff/Documents/billing-workbook/shared-config.js`

The current Python reference server and the older backend notes are kept only as transitional references. They are not the target production shape.
The `server/` Python folder is archival reference code only.

## Retained utility scripts

These are the remaining reusable scripts in `tools/`:

- `audit_contracts.py`
- `audit_billing_sanity.py`
- `generate_looker_import.py`
- `import_historical_partner_data.py`
- `pull_looker_and_push.py`
- `push_looker_batch.py`
- `extract_pdf_text.swift`
- `ocr_pdf.swift`

For the current local reference server, you can still run:

```bash
python3 /Users/danielsinukoff/Documents/billing-workbook/server/app.py --port 4174
```

Then open [http://127.0.0.1:4174](http://127.0.0.1:4174). That remains useful for local experiments, but the long-term plan is to replace the server-side Python pieces with AWS-hosted services and n8n workflows.

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

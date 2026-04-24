# Partner Billing Workbook

A frontend-first browser app for partner billing configuration, invoice calculation, workbook backup, and contract review.

## Run locally

The frontend can be served as a static app:

```bash
python3 -m http.server 4173
```

Then open [http://localhost:4173](http://localhost:4173).

## Shared local + cloud mode

If you want your local browser app to work against the same shared workbook as the hosted cloud app:

1. Copy [app-config.local.example.js](/Users/danielsinukoff/Documents/billing-workbook/app-config.local.example.js) to `app-config.local.js`
2. Point it at the shared S3 `current-workbook.json` object and history prefix
3. Add local-only browser AWS credentials in `aws-credentials.local.js`

Those two local override files are ignored by git. The hosted app will keep using [app-config.js](/Users/danielsinukoff/Documents/billing-workbook/app-config.js), while your local app can read and write the same shared workbook without changing the cloud bundle.

## Notes

- The hosted frontend now reads its shared seed workbook from [data/current-workbook.json](/Users/danielsinukoff/Documents/billing-workbook/data/current-workbook.json) by default.
- Runtime integration settings live in [app-config.js](/Users/danielsinukoff/Documents/billing-workbook/app-config.js).
- Workbook data autosaves to browser `localStorage`, and can be written directly to AWS object storage when `workbookWriteUrl` plus the Cognito/SigV4 auth settings are configured.
- `Export Backup` downloads the full workbook as JSON.
- `Import Backup` restores a saved workbook JSON file.
- Contract parsing in this static build can always accept structured JSON directly. PDF extraction, raw-text parsing, and manual Looker imports are enabled only when their matching automation URLs are configured.

## Conversion target

The target architecture is AWS + S3 + n8n, with the browser app acting as the main user interface and n8n handling file processing and writes back to hosted JSON artifacts.

Recommended handoff docs:

- AWS/S3/n8n handoff: `/Users/danielsinukoff/Documents/billing-workbook/docs/aws-s3-n8n-handoff.md`
- Frontend runtime config:
  - `/Users/danielsinukoff/Documents/billing-workbook/app-config.js`
  - `/Users/danielsinukoff/Documents/billing-workbook/shared-backend.js`
  - `/Users/danielsinukoff/Documents/billing-workbook/shared-config.js`

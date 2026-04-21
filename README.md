# Partner Billing Workbook

A frontend-first browser app for partner billing configuration, invoice calculation, workbook backup, and contract review.

## Run locally

The frontend can be served as a static app:

```bash
python3 -m http.server 4173
```

Then open [http://localhost:4173](http://localhost:4173).

## Notes

- The hosted frontend now reads its shared seed workbook from [data/current-workbook.json](/Users/danielsinukoff/Documents/billing-workbook/data/current-workbook.json) by default.
- Runtime integration settings live in [app-config.js](/Users/danielsinukoff/Documents/billing-workbook/app-config.js).
- Workbook data autosaves to browser `localStorage`, and can be written directly to AWS object storage when `workbookWriteUrl` is configured.
- `Export Backup` downloads the full workbook as JSON.
- `Import Backup` restores a saved workbook JSON file.
- Contract parsing in this static build can always accept structured JSON directly. PDF extraction, raw-text parsing, manual Looker imports, and checker runs are enabled only when their matching webhook URLs are configured.

## Conversion target

The target architecture is AWS + S3 + n8n, with the browser app acting as the main user interface and n8n handling file processing, checker runs, and writes back to hosted JSON artifacts.

Recommended handoff docs:

- AWS/S3/n8n handoff: `/Users/danielsinukoff/Documents/billing-workbook/docs/aws-s3-n8n-handoff.md`
- Frontend runtime config:
  - `/Users/danielsinukoff/Documents/billing-workbook/app-config.js`
  - `/Users/danielsinukoff/Documents/billing-workbook/shared-backend.js`
  - `/Users/danielsinukoff/Documents/billing-workbook/shared-config.js`

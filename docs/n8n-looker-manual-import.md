# n8n Looker Manual Import

This workflow backs the app's `Data Upload` manual import button.

## Importable Workflow

Use:

```text
docs/n8n-looker-manual-import.workflow.json
```

After import, select the same AWS S3 credential used by the Looker cloud sync workflow on:

- `Download Current Workbook`
- `Upload Current Workbook`
- `Upload Workbook History`
- `Upload Manual Import Summary`

The Excel path also uses n8n's built-in `Extract From File` node. No extra credential is required on:

- `Extract XLS Rows`
- `Extract XLSX Rows`

Publish/activate the workflow so this production webhook is live:

```text
https://veem.app.n8n.cloud/webhook/billing-looker-manual-import
```

## What It Does

The hosted frontend posts one manual Looker upload to n8n. n8n accepts pasted table text, CSV, XLS, or XLSX. Excel files are normalized into CSV-shaped rows first, then n8n reads `data/current-workbook.json`, applies the same JavaScript import runtime used by the scheduled Looker cloud sync, writes the updated workbook back to S3, writes timestamped workbook history, and returns the import result to the browser.

Like the scheduled Looker sync, manual imports keep raw detail rows off by default so `current-workbook.json` does not become too large. Send `includeDetailRows: true` only for a small targeted test import.

For Excel imports, use the first worksheet as the Looker export table and keep the header row intact.

# n8n QA Status API

This workflow is the no-Engineering workaround for showing QA checker results in the hosted app while the site bucket is not writable from n8n.

## Importable Workflow

Use this file:

```text
docs/n8n-qa-status-api.workflow.json
```

After import, select the same AWS S3 credential used by the working QA checker on:

```text
Download Latest QA Summary
```

Publish/activate the workflow. The app config already points to:

```text
https://veem.app.n8n.cloud/webhook/billing-qa-status
```

## What It Does

The workflow reads:

```text
s3://veem-qa-billing-data/data/qa/latest-checker-summary.json
```

and returns:

```json
{
  "ok": true,
  "qaCheckerLatest": { "...": "latest checker report" },
  "report": { "...": "same checker report" }
}
```

This lets the frontend update the QA Checker panel without writing to:

```text
s3://veem-qa-billing-fe-site/data/current-workbook.json
```

## Test

After the QA checker has run successfully, open:

```text
https://veem.app.n8n.cloud/webhook/billing-qa-status
```

Expected result: JSON with `ok: true` and a populated `qaCheckerLatest.runId`.

Then reload the billing app or click `Refresh QA Status` in the QA Checker panel.

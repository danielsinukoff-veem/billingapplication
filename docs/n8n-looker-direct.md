# n8n Looker Direct Import

This is the recommended setup for the billing workbook.

Instead of:

- exporting Looker files to Google Sheets
- storing them in Drive
- then having `n8n` re-read them

this workflow does:

1. `n8n` starts a run
2. the workflow calls a local helper script
3. the helper script authenticates to Looker API
4. it resolves each dashboard report by `dashboardId` and `reportName`
5. it exports the raw report as `csv` or `xlsx`
6. it sends the result directly to `POST /api/looker/import-and-save`

## Included files

- Direct workflow: [n8n-looker-direct.workflow.json](/Users/danielsinukoff/Documents/billing-workbook/docs/n8n-looker-direct.workflow.json)
- Looker report mapping: [looker-direct-reports.json](/Users/danielsinukoff/Documents/billing-workbook/docs/looker-direct-reports.json)
- Helper script: [pull_looker_and_push.py](/Users/danielsinukoff/Documents/billing-workbook/tools/pull_looker_and_push.py)

## Why this is better than Google Sheets

- no spreadsheet type coercion
- no extra storage/sync layer
- no manual file handling
- cleaner provenance from Looker straight into the billing app
- easier retries and alerting

## What to configure in n8n

The workflow reads these env vars:

- `BILLING_API_BASE_URL`
- `BILLING_API_TOKEN` (optional)

The current direct config file already includes:

- `clientID`
- `clientSecret`

So for the current setup, you only need to provide the billing API values in `n8n` unless you later want to move the Looker credentials back out of the file.

The workflow also points to:

- [looker-direct-reports.json](/Users/danielsinukoff/Documents/billing-workbook/docs/looker-direct-reports.json)

You can edit that file when dashboard IDs, tile IDs, or report names change.

## Current report mapping

The current config covers:

- `Partner Offline Billing`
- `Partner Offline Billing (Reversals)`
- `All Registered Accounts`
- `Partner Revenue Share V2`
- `Partner Revenue Share`
- `Partner Revenue Reversal`
- `Stampli FX Revenue Share`
- `Stampli FX Revenue Reversal`

## Tile IDs

Your current config uses `dashboardId + reportName` matching because you did not provide `tileId`.

That works, but if you later provide `tileId`, that is even better because it is more stable than matching by title.

## Important security note

Do not store Looker secrets in workflow JSON.

Use:

- n8n environment variables
- or n8n credentials/secrets

The current config file includes the Looker credentials so the workflow can be run quickly, but this should be treated as a temporary convenience setup. The safer long-term setup is still to rotate and move them into `n8n` secrets or env vars.

## Recommended next step

Use [n8n-looker-direct.workflow.json](/Users/danielsinukoff/Documents/billing-workbook/docs/n8n-looker-direct.workflow.json) as the production candidate.

Keep [n8n-looker-batch-import.workflow.json](/Users/danielsinukoff/Documents/billing-workbook/docs/n8n-looker-batch-import.workflow.json) only as a fallback if you ever need folder-based imports.

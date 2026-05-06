# n8n Rebuild From JSONs

Use this when rebuilding the Partner Billing n8n project from scratch.

## Retired Workflows

Older monolithic Looker workflow exports were removed from this repo. Rebuild only from the active JSONs listed below.

## Create These Workflows

Create each workflow by importing one JSON file. Do not drag all JSONs into one workflow.

### Looker Data Imports

Import every file in numeric order from:

```text
docs/n8n-looker-cloud-split/
```

This creates 14 separate workflows:

```text
01 Partner Offline Billing Month -3
02 Partner Offline Billing Month -2
03 Partner Offline Billing Month -1
04 Partner Offline Billing Current Month
05 Partner Offline Billing Reversals
06 All Registered Accounts - Offline Billing
07 VBA Accounts
08 CC/Citi VBA Txns (CC)
09 CC/Citi VBA Txns (Citi)
10 Revenue Share Report
11 Rev Share Reversals
12 All Registered Accounts - Rev Share
13 Stampli FX Revenue Share
14 Stampli FX Revenue Reversal
```

For each Looker workflow:

1. Open the workflow after import.
2. Select the AWS credential on `Download Current Workbook`, `Upload Current Workbook`, `Upload Workbook History`, and `Upload Sync Summary`.
3. Open `Build Run Context`.
4. Confirm `lookerClientId` and `lookerClientSecret` are real values, not `SET_LOOKER_CLIENT_ID` / `SET_LOOKER_CLIENT_SECRET`.
5. Confirm the workflow has no red credential warnings.
6. Publish/activate the workflow.

These workflows are already staggered every 30 minutes from `00:00` through `06:30`.

### QA Checker

Import:

```text
docs/n8n-qa-checker.workflow.json
```

Set AWS credentials on:

```text
Download Current Workbook
Upload Workbook QA Status
Download Verified Workbook QA Status
Upload Latest QA Summary
Upload Latest QA Exceptions CSV
```

Publish/activate it. The generated schedule is `07:30`, after the split Looker workflows.

### QA Status API

Import:

```text
docs/n8n-qa-status-api.workflow.json
```

Set AWS credentials on:

```text
Download Latest QA Summary
```

Publish/activate it. Production webhook path:

```text
billing-qa-status
```

The app reads this endpoint for QA status.

### Manual Looker Upload

Import:

```text
docs/n8n-looker-manual-import.workflow.json
```

Set AWS credentials on:

```text
Download Current Workbook
Upload Current Workbook
Upload Workbook History
Upload Manual Import Summary
```

No credentials are needed for `Extract XLS Rows` or `Extract XLSX Rows`.

Publish/activate it. Production webhook path:

```text
billing-looker-manual-import
```

### Contract Automation

Import:

```text
docs/n8n-contract-automation.workflow.json
```

This workflow has two webhook paths:

```text
billing-contract-extract
billing-contract-parse
```

No AWS credential is required. Publish/activate it after import.

### HubSpot Partner Sync

Import:

```text
docs/n8n-hubspot-partner-sync.workflow.json
```

Set the HubSpot credential on:

```text
HubSpot Search Companies
HubSpot Search Associated Contacts
```

Publish/activate it. Production webhook path:

```text
billing-hubspot-partner-sync
```

## Delete From n8n Cloud

Delete or archive older undated versions in n8n Cloud, especially:

```text
Billing Workbook Looker Cloud Sync
Billing Workbook QA Checker
Billing QA Status API
Billing Workbook Looker Manual Import
Billing Workbook Contract Automation
Billing Workbook HubSpot Partner Sync
```

Keep the workflows ending in:

```text
Updated 2026-04-30
```

## Manual Test Order

After import and credential setup:

1. Run Looker workflows `01` through `14` manually in numeric order.
2. Run `Billing Workbook QA Checker - Updated 2026-04-30`.
3. Open `https://veem.app.n8n.cloud/webhook/billing-qa-status` and confirm it returns the latest QA run.
4. Refresh the billing app and confirm the QA checker box updates.

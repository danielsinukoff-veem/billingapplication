# n8n Looker Cloud Split Workflows

The old `Billing Workbook Looker Cloud Sync` workflow was too large for n8n Cloud to render reliably. It has been replaced by smaller single-report workflows.

## Import These Files

Import every JSON file in:

```text
docs/n8n-looker-cloud-split/
```

Do not re-import an older undated `Billing Workbook Looker Cloud Sync` workflow. Delete the old large workflow from n8n after the split workflows are imported.

## Run Order

The split files are numbered and should run in that order:

```text
01-partner-offline-billing-month-minus-3.workflow.json
02-partner-offline-billing-month-minus-2.workflow.json
03-partner-offline-billing-month-minus-1.workflow.json
04-partner-offline-billing-current-month.workflow.json
05-partner-offline-billing-reversals.workflow.json
06-all-registered-accounts-offline.workflow.json
07-vba-accounts.workflow.json
08-vba-transactions-cc.workflow.json
09-vba-transactions-citi.workflow.json
10-revenue-share-report.workflow.json
11-rev-share-reversals.workflow.json
12-all-registered-accounts-rev-share.workflow.json
13-stampli-fx-revenue-share.workflow.json
14-stampli-fx-revenue-reversal.workflow.json
```

Each workflow reads `s3://veem-qa-billing-data/data/current-workbook.json`, applies one Looker report, and writes the workbook back to the same key.

## Schedules

The generated workflows include staggered daily schedules so they do not overwrite each other by running at the same time:

```text
01 00:00
02 00:30
03 01:00
04 01:30
05 02:00
06 02:30
07 03:00
08 03:30
09 04:00
10 04:30
11 05:00
12 05:30
13 06:00
14 06:30
```

Keep them inactive until AWS and Looker credentials are selected in each imported workflow. Once credentials are selected, publish/activate the children.

Schedule the QA checker for `07:30` or later so it runs after workflow `14` finishes.

## QA Checker

The split Looker workflows do not trigger QA after every report. Keep the QA checker as its own separate workflow and schedule it after the final Looker child has completed.

Use:

```text
docs/n8n-qa-checker.workflow.json
docs/n8n-qa-status-api.workflow.json
```

## Why This Is Safer

The largest children are now 20 nodes. n8n no longer has to load a 118-node canvas, which is what caused the connection-lost behavior in the editor.

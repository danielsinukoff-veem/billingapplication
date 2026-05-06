# n8n QA Checker

This is the first checker layer for the billing QA workflow. It is deterministic and runs against the same `current-workbook.json` that the hosted app reads.

## Importable Workflow

Use this file:

```text
docs/n8n-qa-checker.workflow.json
```

Import it into n8n and select the same AWS S3 credential used by the Looker cloud sync workflow on these nodes:

- `Download Current Workbook`
- `Upload Workbook QA Status`
- `Download Verified Workbook QA Status`
- `Upload Latest QA Summary`
- `Upload Latest QA Exceptions CSV`

Also import and publish the read-only status workflow:

```text
docs/n8n-qa-status-api.workflow.json
```

Select the same AWS S3 credential on `Download Latest QA Summary`, then activate the workflow so this URL is live:

```text
https://veem.app.n8n.cloud/webhook/billing-qa-status
```

## What It Checks

The first layer currently checks:

- active/configured partners with no imported rows for the checked month
- transaction rows with volume/count but no fee amount
- transaction rows that have no matching configured fee/revenue-share rows
- malformed currency codes
- reversal rows where the default `$2.50` fee applies because no contract-specific reversal fee exists
- Stampli FX validation mismatches or missing source fields
- Looker import warnings already captured in the workbook audit
- stale workbook timestamps
- duplicate row IDs within the checked period

This does not use an LLM and does not call Snowflake yet. Those are later layers.

## Outputs

The workflow writes:

```text
s3://veem-qa-billing-data/data/current-workbook.json      # source workbook updated with snapshot.qaCheckerLatest
s3://veem-qa-billing-data/data/qa/latest-checker-summary.json
s3://veem-qa-billing-data/data/qa/latest-checker-exceptions.csv
```

The exceptions CSV is finance-facing. It includes the issue, suggested action,
straight-path solution, transaction identifiers, transaction count, volume,
currency/speed/processing fields, and fee-config match counts where available.
The hosted UI can dismiss individual rows or clear a run locally; those
dismissals are browser-local review state and do not mutate the source checker
result in S3.

The checker no longer writes `veem-qa-billing-fe-site/data/current-workbook.json`. That site-bucket write is blocked by IAM today, so the hosted app reads the checker status from the n8n status API instead. The status API pulls:

```text
s3://veem-qa-billing-data/data/qa/latest-checker-summary.json
```

and returns it to the frontend at `BILLING_APP_CONFIG.qaCheckerSummaryUrl`.

Timestamped QA history uploads are intentionally not included in this workflow yet because the current n8n IAM policy does not allow `s3:PutObject` to `history/qa/*`. If Finance later needs retained checker history, ask DevOps to allow writes to:

```text
arn:aws:s3:::veem-qa-billing-data/history/qa/*
```

## Recommended n8n Setup

The checker can run three ways:

- Manual test from `Manual Trigger`
- Scheduled run from `Daily Schedule`
- Production webhook call at:

```text
https://veem.app.n8n.cloud/webhook/billing-qa-checker
```

Best setup after the Looker workflow is stable:

1. Keep this as a separate workflow named `Billing Workbook QA Checker`.
2. Publish/activate it so the production webhook is live.
3. Import the split Looker workflows from `docs/n8n-looker-cloud-split/`.
4. Publish/activate the split workflows only after AWS and Looker credentials are selected.
5. Run the split Looker workflows in numeric order, or use their staggered schedules.
6. Run the QA checker after the final Looker child workflow completes. The generated schedule is `07:30`.
7. If calling the checker webhook manually, post this JSON:

```json
{
  "period": "={{ $('Build Run Context').all()[0].json.period }}",
  "runId": "={{ $('Build Run Context').all()[0].json.runId }}",
  "bucketName": "veem-qa-billing-data",
  "workbookKey": "data/current-workbook.json"
}
```

The checker intentionally reads `data/current-workbook.json` from S3 after the Looker workflow writes it. That keeps the checker separate from the large Looker import tree and avoids passing the large workbook payload through the webhook.

After a successful run, verify this URL contains a `snapshot.qaCheckerLatest` object before testing the UI:

```text
https://veem.app.n8n.cloud/webhook/billing-qa-status
```

If the checker workflow succeeds but this endpoint does not return the latest run, inspect the separate `Billing QA Status API` workflow first. If the endpoint returns the latest run but the app still shows `QA Checker · Not run`, redeploy the frontend bundle/config that includes `qaCheckerSummaryUrl`.

## Looker Workflow Stability

The old Looker cloud workflow was much larger than the checker workflow and caused n8n Cloud connection loss while rendering the editor canvas. Do not re-import the old large workflow. Use the split workflow files instead:

```text
docs/n8n-looker-cloud-split/
```

Recommended operating pattern:

1. Delete the older undated `Billing Workbook Looker Cloud Sync` workflow from n8n.
2. Import the numbered split workflow JSON files.
3. Re-select the AWS and Looker credentials on each child workflow if n8n asks.
4. Publish/activate the child workflows.
5. Let the staggered schedules run them in order, or run them manually in numeric order.
6. Use the n8n `Executions` tab to review each smaller run output.

## Local Test

Run this from the repo root:

```bash
node tools/run_qa_checker.mjs \
  --workbook data/current-workbook.json \
  --period 2026-03 \
  --out reports/qa-checker-2026-03.json \
  --csv reports/qa-checker-2026-03.csv \
  --fail-on-critical false
```

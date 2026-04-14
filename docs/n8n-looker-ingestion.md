# n8n Looker Ingestion

This app now supports a server-side Looker ingestion route for live automation:

- `POST /api/looker/import-and-save`

Use this route from `n8n` for unattended imports. It parses the file, replaces the matching month in the shared workbook, saves the new snapshot, and returns counts/warnings.

Included assets:

- Recommended direct Looker workflow: [n8n-looker-direct.workflow.json](/Users/danielsinukoff/Documents/billing-workbook/docs/n8n-looker-direct.workflow.json)
- Recommended direct Looker guide: [n8n-looker-direct.md](/Users/danielsinukoff/Documents/billing-workbook/docs/n8n-looker-direct.md)
- Direct Looker report map: [looker-direct-reports.json](/Users/danielsinukoff/Documents/billing-workbook/docs/looker-direct-reports.json)
- Ready-to-import n8n workflow: [n8n-looker-batch-import.workflow.json](/Users/danielsinukoff/Documents/billing-workbook/docs/n8n-looker-batch-import.workflow.json)
- Batch helper script used by that workflow: [push_looker_batch.py](/Users/danielsinukoff/Documents/billing-workbook/tools/push_looker_batch.py)

## What n8n needs

- The app API URL
  - local example: `http://127.0.0.1:4174`
- Optional bearer token if the server is started with `BILLING_API_TOKEN`
- A way to identify:
  - `fileType`
  - `period` in `YYYY-MM`
  - `fileName`
  - `fileBase64`

## Recommended workflow shape

1. Trigger on a new Looker export file or scheduled export batch.
2. Normalize the file metadata.
3. Map the file to the correct `fileType`.
4. Set the billing `period`.
5. Send the file to `POST /api/looker/import-and-save`.
6. If `warnings.length > 0`, alert Slack/email.
7. Continue to the next dependent file in order.

The included workflow takes a simpler route:

1. `n8n` starts with a `Manual Trigger`
2. it builds config values
3. it runs the helper script
4. the script scans the folder, orders the files, posts them to the API, and returns a JSON summary

That makes the workflow much more stable than trying to juggle binary file handling and dependency ordering directly inside n8n.

## HTTP Request node

Use a JSON `POST` request.

### URL

```text
{{$env.BILLING_API_BASE_URL}}/api/looker/import-and-save
```

### Headers

```text
Content-Type: application/json
Accept: application/json
Authorization: Bearer {{$env.BILLING_API_TOKEN}}
```

Omit the `Authorization` header if the API is running without token auth.

### Body

Assuming the binary file property is named `data`:

```json
{
  "fileType": "={{$json.fileType}}",
  "period": "={{$json.period}}",
  "fileName": "={{$binary.data.fileName || $json.fileName}}",
  "fileBase64": "={{$binary.data.data}}"
}
```

If you are sending pasted CSV text instead of a file:

```json
{
  "fileType": "={{$json.fileType}}",
  "period": "={{$json.period}}",
  "pastedText": "={{$json.csvText}}"
}
```

## Response shape

The route returns:

- `sections`
- `detailRows`
- `contextUpdate`
- `warnings`
- `stats`
- `savedAt`
- `source = "server"`

Use `warnings` and `stats` for logging and alerting.

## File type mapping

| Looker export | `fileType` | What it updates | Dependency notes |
| --- | --- | --- | --- |
| `Partner Offline Billing.xlsx` / `.csv` | `partner_offline_billing` | `ltxn`, `lookerImportedDetailRows`, saved `offlineContext` | Should run before `all_registered_accounts` |
| `partner_offline_billing_(reversals).xlsx` / `.csv` | `partner_offline_billing_reversals` | `lrev`, `lookerImportedDetailRows` | Independent |
| `All Registered Accounts.xlsx` / `.csv` | `all_registered_accounts` | `lva` | Uses saved `offlineContext` from `partner_offline_billing` |
| `Partner Rev Share V2.xlsx` / `partner_revenue_share_v2.csv` | `partner_rev_share_v2` | `ltxn`, sometimes `lrs`, `lookerImportedDetailRows` | Independent |
| `Partner Revenue Share.xlsx` / `partner_revenue_share.csv` | `partner_revenue_share` | `ltxn`, sometimes `lrs`, `lookerImportedDetailRows` | Independent |
| `Partner Revenue Reversal.xlsx` / `partner_revenue_reversal.csv` | `partner_revenue_reversal` | `lrs` reversal adjustments | Independent |
| `Partner Revenue Summary.xlsx` / `partner_revenue_summary.csv` | `partner_revenue_summary` | `lrs` | Independent |
| `All Stampli Credit Complete.xlsx` / `.csv` | `all_stampli_credit_complete` | `ltxn`, `lookerImportedDetailRows`, saved `stampliCreditCompleteLookup` | Should run before Stampli FX imports |
| `stampli_fx_revenue_share.xlsx` / `.csv` | `stampli_fx_revenue_share` | `lfxp`, saved Stampli FX share cache, `lookerImportedDetailRows` | Uses saved `stampliCreditCompleteLookup` |
| `stampli_fx_revenue_reversal.xlsx` / `.csv` | `stampli_fx_revenue_reversal` | `lfxp`, saved Stampli FX reversal cache, `lookerImportedDetailRows` | Uses saved `stampliCreditCompleteLookup` |

## Recommended run order

If you are importing a full batch, use this order:

1. `partner_offline_billing`
2. `partner_offline_billing_reversals`
3. `all_registered_accounts`
4. `partner_rev_share_v2`
5. `partner_revenue_share`
6. `partner_revenue_reversal`
7. `partner_revenue_summary`
8. `all_stampli_credit_complete`
9. `stampli_fx_revenue_share`
10. `stampli_fx_revenue_reversal`

The server automatically reuses saved parsing context between calls, so `n8n` does not need to manually carry context from one request to the next.

## Period rules

The route always needs a billing `period` in `YYYY-MM`.

Examples:

- January 2026 -> `2026-01`
- February 2026 -> `2026-02`
- March 2026 -> `2026-03`

If a Looker export contains multiple months, you can still use it by sending the same file multiple times with different `period` values. The parser will filter/bucket rows for the requested month.

## Suggested filename matching

Use a `Set` or `Code` node to map files like this:

| Filename contains | `fileType` |
| --- | --- |
| `Partner Offline Billing` | `partner_offline_billing` |
| `partner_offline_billing_(reversals)` | `partner_offline_billing_reversals` |
| `All Registered Accounts` | `all_registered_accounts` |
| `Partner Rev Share V2` | `partner_rev_share_v2` |
| `Partner Revenue Share` | `partner_revenue_share` |
| `Partner Revenue Reversal` | `partner_revenue_reversal` |
| `Partner Revenue Summary` | `partner_revenue_summary` |
| `All Stampli Credit Complete` | `all_stampli_credit_complete` |
| `stampli_fx_revenue_share` | `stampli_fx_revenue_share` |
| `stampli_fx_revenue_reversal` | `stampli_fx_revenue_reversal` |

## Suggested n8n Code node

This example assumes the current item has a file name in `json.fileName` and a target month in `json.period`.

```javascript
const name = String($json.fileName || "");

const rules = [
  [/partner_offline_billing_\\(reversals\\)/i, "partner_offline_billing_reversals"],
  [/partner offline billing/i, "partner_offline_billing"],
  [/all registered accounts/i, "all_registered_accounts"],
  [/partner rev share v2/i, "partner_rev_share_v2"],
  [/partner revenue share/i, "partner_revenue_share"],
  [/partner revenue summary/i, "partner_revenue_summary"],
  [/all stampli credit complete/i, "all_stampli_credit_complete"],
  [/stampli_fx_revenue_share/i, "stampli_fx_revenue_share"],
  [/stampli_fx_revenue_reversal/i, "stampli_fx_revenue_reversal"],
];

const match = rules.find(([pattern]) => pattern.test(name));
if (!match) {
  throw new Error(`Unsupported Looker export: ${name}`);
}

return {
  json: {
    ...$json,
    fileType: match[1],
  },
  binary: $binary,
};
```

## Failure handling

Treat these cases as alerts:

- non-200 response
- `warnings.length > 0`
- empty `sections` when the file should contain rows
- missing `savedAt`

Good alert payload:

- file name
- `fileType`
- `period`
- `warnings`
- `stats`
- response body

## Application-side setup checklist

- Host the API somewhere reachable from `n8n`
- Set `BILLING_API_BASE_URL`
- Optionally set `BILLING_API_TOKEN`
- Start the billing API server
- Point `n8n` to `POST /api/looker/import-and-save`

## Workflow import

Import [n8n-looker-batch-import.workflow.json](/Users/danielsinukoff/Documents/billing-workbook/docs/n8n-looker-batch-import.workflow.json) into `n8n`.

Then update only these values in the `Build Config` node if needed:

- `sourceDir`
- `apiBaseUrl`
- `apiToken`
- `period`

If you want it scheduled instead of manual:

- replace `Manual Trigger` with a `Schedule Trigger`
- keep the rest of the workflow unchanged

## Verified behavior

The route has been verified locally to:

- parse a Looker payload
- save the shared workbook snapshot
- return `savedAt`
- preserve server-side import context across requests

# n8n Cloud Looker Import

This is the cloud-safe version of the billing workbook Looker workflow.

Use:

- [/Users/danielsinukoff/Documents/billing-workbook/docs/n8n-looker-cloud.workflow.json](/Users/danielsinukoff/Documents/billing-workbook/docs/n8n-looker-cloud.workflow.json)

It does not use `Execute Command`, local scripts, or local filesystem paths. Instead, it calls the billing API route:

- `POST /api/looker/direct-sync`

The billing API does the full Looker login, report export, parsing, and shared workbook save on the server side.

## What To Change In n8n Cloud

Open the `Build Config` node and set:

- `billingApiBaseUrl`
- `billingApiToken` if the billing API requires bearer auth

Optional fields in `Build Config`:

- `reportFileTypes`
  - leave empty to sync all configured reports
  - or set specific file types like `partner_revenue_summary`
- `configFile`
  - leave blank to use the server default config
- `lookerClientId`
- `lookerClientSecret`
  - only needed if you do not want the billing API to use the server-side config

## Important Requirement

n8n Cloud cannot call:

- `http://127.0.0.1:4174`
- your laptop
- a Docker container on your laptop

So `billingApiBaseUrl` must be a URL reachable from n8n Cloud, for example:

- `https://billing-api.company.com`

For temporary testing, you can use a secure tunnel to your local billing API, but the stable long-term setup is a hosted billing API.

## Recommended Request Body

The workflow sends:

```json
{
  "period": "2026-04",
  "dryRun": false,
  "reportTimeout": 600
}
```

You can also narrow it to one file type while testing:

```json
{
  "period": "2026-04",
  "dryRun": false,
  "reportTimeout": 600,
  "reportFileTypes": ["partner_revenue_summary"]
}
```

## Dry Run

To test the route without pulling Looker data yet, set:

```json
{
  "period": "2026-04",
  "dryRun": true
}
```

That returns the configured report list without downloading reports or saving the workbook.

## Result

The workflow returns:

- `configuredCount`
- `importedCount`
- `errorCount`
- `warningCount`
- one status row per report

If a report fails, the workflow still returns the per-report error message so you can see which Looker export broke.

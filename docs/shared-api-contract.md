# Shared API Contract

This is the first API slice for moving the billing workbook from browser-local state to a shared company app.

The current reference server still implements this contract locally, but the target production shape is AWS + S3 + n8n. Treat this contract as the interface the frontend expects, regardless of which backend service is behind it.

## Base assumptions

- Auth happens before these routes are reached.
- The API uses the current user session to determine org and role.
- All responses are JSON.

## `GET /api/bootstrap`

Returns the initial shared workbook payload for the signed-in user.

### Response

```json
{
  "workspace": {
    "label": "Finance Shared Workspace",
    "mode": "shared"
  },
  "user": {
    "email": "user@company.com",
    "role": "billing_ops"
  },
  "snapshot": {
    "_version": 17,
    "_saved": "2026-03-16T22:00:00.000Z",
    "ps": [],
    "pConfig": {},
    "off": [],
    "vol": [],
    "fxRates": [],
    "cap": [],
    "rs": [],
    "mins": [],
    "plat": [],
    "revf": [],
    "impl": [],
    "vaFees": [],
    "surch": [],
    "pCosts": [],
    "ltxn": [],
    "lrev": [],
    "lva": [],
    "lrs": [],
    "lfxp": []
  }
}
```

## `PUT /api/workbook`

Replaces the shared workbook snapshot for the org.

This is the first migration step only. In the long run, pricing and import data should become more granular endpoints rather than whole-snapshot writes.

The backend should persist this in a shared system-of-record row keyed by organization, whether that is an AWS-hosted API, an AWS database, or another centralized service.

### Request

```json
{
  "snapshot": {
    "_version": 17,
    "_saved": "2026-03-16T22:00:00.000Z",
    "ps": [],
    "pConfig": {},
    "off": [],
    "vol": [],
    "fxRates": [],
    "cap": [],
    "rs": [],
    "mins": [],
    "plat": [],
    "revf": [],
    "impl": [],
    "vaFees": [],
    "surch": [],
    "pCosts": [],
    "ltxn": [],
    "lrev": [],
    "lva": [],
    "lrs": [],
    "lfxp": []
  }
}
```

### Response

```json
{
  "savedAt": "2026-03-16T22:01:00.000Z"
}
```

## `GET /api/invoices/draft?partner=Stampli&startPeriod=2026-02&endPeriod=2026-02`

Returns a server-calculated invoice draft using the active shared workbook data and imported facts. For a single month, set `startPeriod` and `endPeriod` to the same month.

### Response

```json
{
  "invoice": {
    "partner": "Stampli",
    "period": "2026-02",
    "periodStart": "2026-02",
    "periodEnd": "2026-02",
    "periodLabel": "February 2026",
    "periodDateRange": "February 1, 2026 - February 28, 2026",
    "lines": [],
    "groups": [],
    "notes": [],
    "chg": 14957,
    "pay": 47407.89,
    "net": -32450.89,
    "dir": "We Owe Partner"
  },
  "generatedAt": "2026-03-16T22:05:00.000Z",
  "source": "server"
}
```

## `POST /api/looker/import`

Parses one uploaded or pasted Looker export and returns the normalized billing sections, detail rows, context updates, and warnings. This does not save anything to the shared workbook.

This route is useful for browser-driven review flows where the client wants to preview the parsed results before deciding to persist them.

### Request

```json
{
  "fileType": "partner_offline_billing",
  "period": "2026-03",
  "fileName": "Partner Offline Billing.xlsx",
  "fileBase64": "<base64 file contents>",
  "context": {}
}
```

or

```json
{
  "fileType": "partner_revenue_summary",
  "period": "2026-03",
  "pastedText": "Credit Complete Timestamp Month,Partner Group Source,Net Revenue,Partner Net Revenue Share,Revenue Owed,Monthly Minimum Revenue\n...",
  "context": {}
}
```

### Response

```json
{
  "fileType": "partner_revenue_summary",
  "fileLabel": "Partner Revenue Summary.xlsx / partner_revenue_summary.csv",
  "period": "2026-03",
  "sections": {
    "lrs": []
  },
  "detailRows": [],
  "contextUpdate": {},
  "warnings": [],
  "stats": {
    "fileType": "partner_revenue_summary",
    "period": "2026-03",
    "fileLabel": "Partner Revenue Summary.xlsx / partner_revenue_summary.csv",
    "summaryRows": 0,
    "sectionCounts": {
      "lrs": 0
    },
    "detailCounts": {}
  }
}
```

## `POST /api/looker/import-and-save`

Parses one uploaded or pasted Looker export, applies the same month-replacement rules used by the browser workbook, and saves the updated shared snapshot. This is the recommended route for n8n and other machine-to-machine ingestion flows.

For dependent imports, the server automatically merges the request `context` with the previously saved `lookerImportContext` in the shared snapshot. That means n8n does not need to manually round-trip parsing context between calls unless it wants to override values during the same run.

### Request

```json
{
  "fileType": "partner_revenue_summary",
  "period": "2026-03",
  "pastedText": "Credit Complete Timestamp Month,Partner Group Source,Net Revenue,Partner Net Revenue Share,Revenue Owed,Monthly Minimum Revenue\n...",
  "context": {}
}
```

### Response

```json
{
  "fileType": "partner_revenue_summary",
  "fileLabel": "Partner Revenue Summary.xlsx / partner_revenue_summary.csv",
  "period": "2026-03",
  "sections": {
    "lrs": []
  },
  "detailRows": [],
  "contextUpdate": {},
  "warnings": [],
  "stats": {
    "fileType": "partner_revenue_summary",
    "period": "2026-03",
    "fileLabel": "Partner Revenue Summary.xlsx / partner_revenue_summary.csv",
    "summaryRows": 0,
    "sectionCounts": {
      "lrs": 0
    },
    "detailCounts": {}
  },
  "savedAt": "2026-03-26 02:26:53",
  "source": "server"
}
```

### Notes For n8n

- Use `import-and-save` instead of `import` for unattended live feeds.
- The server automatically reuses saved parsing context between calls.
- You can still pass `context` to override or supplement the saved values for a specific run.
- Recommended order:
  1. `partner_offline_billing`
  2. `partner_offline_billing_reversals`
  3. `all_registered_accounts`
  4. `partner_rev_share_v2` or `partner_revenue_share`
  5. `partner_revenue_summary`
  6. `all_stampli_credit_complete`
  7. `stampli_fx_revenue_share`
  8. `stampli_fx_revenue_reversal`
- If the server is started with `BILLING_API_TOKEN`, send `Authorization: Bearer <token>`.

## `POST /api/checker/run`

Runs the maker/checker reconciliation workflow against the shared snapshot and returns a report of matching and mismatching invoice buckets.

### Request

```json
{
  "partner": "Stampli",
  "period": "2026-02",
  "epsilon": 0.01
}
```

or

```json
{
  "startPeriod": "2026-01",
  "endPeriod": "2026-04",
  "periods": ["2026-01", "2026-02", "2026-03", "2026-04"],
  "epsilon": 0.01
}
```

### Response

```json
{
  "generatedAt": "2026-04-17T18:00:00Z",
  "partnerFilter": "Stampli",
  "periodFilter": {
    "periods": ["2026-02"],
    "startPeriod": "2026-02",
    "endPeriod": "2026-02"
  },
  "runCount": 1,
  "passedCount": 1,
  "failedCount": 0,
  "runs": [
    {
      "partner": "Stampli",
      "period": "2026-02",
      "passed": true,
      "maker": {
        "chg": 0,
        "pay": 0,
        "net": 0,
        "lineCount": 0,
        "buckets": {}
      },
      "checker": {
        "chg": 0,
        "pay": 0,
        "net": 0,
        "lineCount": 0,
        "buckets": {}
      },
      "totalDeltas": {
        "chg": 0,
        "pay": 0,
        "net": 0
      },
      "diffs": [],
      "sourceStats": {
        "ltxn_rows": 0,
        "lrev_rows": 0,
        "lrs_rows": 0,
        "lfxp_rows": 0,
        "lva_rows": 0,
        "impl_rows": 0
      },
      "notes": []
    }
  ]
}
```

## `POST /api/looker/direct-sync`

Triggers the full direct Looker pull on the billing API server itself. This is the recommended route for `n8n Cloud`, because the workflow only needs `HTTP Request` and does not need `Execute Command` or local filesystem access.

The server:

1. logs into Looker
2. exports each configured report
3. parses it with the same billing import logic
4. applies month-replacement rules
5. saves the shared workbook snapshot

### Request

```json
{
  "period": "2026-03",
  "dryRun": false,
  "reportTimeout": 600
}
```

Optional request fields:

- `reportFileTypes`
- `configFile`
- `lookerClientId`
- `lookerClientSecret`
- `forceProduction`
- `runId`

### Response

```json
{
  "configFile": "/app/docs/looker-direct-reports.json",
  "period": "2026-03",
  "runId": "direct-20260326T120000Z-ab12cd34",
  "baseUrl": "https://veem.looker.com",
  "apiVersion": "4.0",
  "forceProduction": true,
  "dryRun": false,
  "reports": [
    {
      "fileType": "partner_revenue_summary",
      "fileName": "partner_revenue_summary.csv",
      "reportName": "Partner Revenue Summary",
      "status": "imported",
      "savedAt": "2026-03-26T12:00:03.000Z",
      "warnings": [],
      "stats": {},
      "changeSummary": {},
      "sectionKeys": ["lrs"],
      "byteCount": 12345
    }
  ],
  "warningCount": 0,
  "errorCount": 0,
  "importedCount": 1,
  "configuredCount": 1,
  "hasWarnings": false,
  "hasErrors": false,
  "source": "server-direct",
  "savedAt": "2026-03-26T12:00:03.000Z"
}
```

## Errors

Use standard JSON errors:

```json
{
  "error": "Forbidden"
}
```

or

```json
{
  "error": "Invoice inputs are incomplete for partner Everflow in 2026-02"
}
```

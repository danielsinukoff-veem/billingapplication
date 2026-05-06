# AWS, S3, and n8n Handoff

This is the target operating model for the billing app.

## What we are trying to achieve

We want the app to stay mostly front end driven while moving the heavy work out of the browser and out of the local Python backend. The frontend should remain the place where Finance and Billing users review data, kick off workflows, and approve results. The actual parsing, scheduled imports, reconciliation, and reminder automation should happen in AWS and n8n.

## Current state

- The browser app owns the UI and the editable billing workbook experience.
- The hosted runtime is now a static frontend that reads current state from same-directory JSON files.
- Optional integrations are driven by runtime config, Cognito-authenticated AWS object writes, signer-based private links, and n8n automation URLs rather than a custom app backend.

## Target state

- Frontend: static web app hosted in AWS.
- File storage: S3.
- Workflow orchestration: n8n.
- Shared data source: versioned JSON artifacts in S3 that both the frontend and n8n use.
- Contract parsing: n8n job that reads contract files from S3, extracts terms, and writes structured output back to S3.
- Looker ingestion: n8n job on a schedule that writes refreshed workbook data back to S3.
- Checker: a separate reconciliation workflow that recalculates billing and flags mismatches before invoices are released.
- Frontend config: the browser reads same-directory runtime config from `app-config.js`, so AWS can point the app at S3 JSON files, direct-write object URLs, and n8n automation URLs without rebuilding the frontend.

## Frontend runtime model

1. The app reads `app-config.js` at load time.
2. The app reads the current workbook snapshot from `data/current-workbook.json` or another S3 JSON path configured in `app-config.js`.
3. User edits continue locally in the browser unless either `workbookWriteUrl` is configured for direct writes or `workbookWriteBridgeUrl` plus `workbookWriteKey` is configured for Lambda bridge writes.
4. For invoice delivery and shared workbook saves, the frontend can call Lambda Function URLs as a serverless AWS bridge. The bridge endpoint writes only explicitly allowed S3 keys or prefixes; direct browser S3 writes remain a fallback only when Cognito or temporary AWS credentials are configured.
5. Partner package pages use stable random URLs, while the invoice and transaction file buttons mint fresh signed download URLs that expire after 60 minutes.
6. The frontend reloads the refreshed S3 artifacts on the next load or refresh.

## Current repo components that matter

- Frontend entry points:
  - `/Users/danielsinukoff/Documents/billing-workbook/index.html`
  - `/Users/danielsinukoff/Documents/billing-workbook/app.js`
  - `/Users/danielsinukoff/Documents/billing-workbook/styles.css`
- Runtime config:
  - `/Users/danielsinukoff/Documents/billing-workbook/app-config.js`
  - `/Users/danielsinukoff/Documents/billing-workbook/shared-backend.js`
  - `/Users/danielsinukoff/Documents/billing-workbook/shared-config.js`
- Seed workbook data:
  - `/Users/danielsinukoff/Documents/billing-workbook/data/current-workbook.json`

## Required storage and automation slots

- `workbookWriteUrl`
- `workbookWriteBridgeUrl`
- `workbookWriteKey`
- `workbookHistoryWriteBaseUrl`
- `workbookHistoryKeyPrefix`
- `invoiceArtifactWriteUrl`
- `invoiceArtifactWriteBaseUrl`
- `authMethod`
- `awsRegion`
- `cognitoUserPoolId`
- `cognitoUserPoolClientId`
- `cognitoIdentityPoolId`
- `cognitoHostedUiDomain`
- `cognitoRedirectUrl`
- `privateInvoiceLinkWriteBaseUrl`
- `privateInvoiceLinkReadBaseUrl`
- `privateInvoiceLinkSignerUrl`
- `privateInvoiceDownloadUrlTtlSeconds`
- `privateInvoiceLinkExpiresInDays`
- `privateInvoiceDownloadRetentionDays`
- `invoiceDraftUrl`
- `automationOutboxUrl`
- `lookerImportWebhookUrl`
- `contractParseWebhookUrl`
- `contractExtractWebhookUrl`
It should independently pull raw source data, run the comparisons, and fail closed on any mismatch.

## Frontend-safe shared workbook writes

When the app is deployed without browser Cognito auth, shared workbook edits should use the existing Lambda bridge instead of public S3 credentials.

Recommended QA runtime config:

```js
workbookReadUrl: "./data/current-workbook.json",
workbookWriteUrl: "",
workbookWriteBridgeUrl: "https://2cm57gp365lualmea7uydm2wci0cjtgq.lambda-url.us-west-2.on.aws/",
workbookWriteKey: "data/current-workbook.json",
workbookHistoryWriteBaseUrl: "",
workbookHistoryKeyPrefix: "history/workbook/",
```

Backend/Lambda allow-list required:

- Allow `action: "write"` and `action: "presign"` for `data/current-workbook.json`.
- Allow `action: "write"` and `action: "presign"` for `history/workbook/*`.
- Keep broad S3 writes blocked. Do not allow arbitrary bucket keys from the browser.
- Keep the existing bearer-token check and CORS origin restriction for `https://billing.qa-us-west-2.veem.com`.

## What should stay in the repo

- Frontend UI files
- UI state and rendering logic
- Workflow definitions for n8n
- Shared API contract documentation
- Billing calculation rules, but only if they are moved into a shared reusable module that the AWS workflow can call

## What should stop being the production dependency

- Local Python server processes
- Local filesystem contract parsing
- Local Downloads-folder based imports
- Any workflow that requires someone to run a script on their own machine to keep billing moving

## Working principle

The browser should be the maker interface. AWS and n8n should own the scheduled work, parsing, reconciliation, and release steps.

## Partner download retention

- The frontend defaults `privateInvoiceDownloadUrlTtlSeconds` to `3600`, so each actual PDF/CSV download URL expires 60 minutes after the partner clicks a download button.
- The stable partner-facing package URL is random and should be retained long enough for normal partner access. The frontend defaults `privateInvoiceLinkExpiresInDays` and `privateInvoiceDownloadRetentionDays` to `180` and writes those values into the package manifest.
- AWS must enforce deletion with an S3 Lifecycle rule on `partner-downloads/`. The frontend can write the expiry metadata, but bucket cleanup is an S3 policy responsibility.
- Do not apply the short partner-download lifecycle to `artifacts/invoices/`; those timestamped invoice artifacts are the finance/legal audit copies and should follow the required retention policy.

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
3. User edits continue locally in the browser unless `workbookWriteUrl` is configured.
4. For invoice delivery, the frontend can call Lambda Function URLs as a serverless AWS bridge: one endpoint writes timestamped invoice artifacts to S3 and the signer endpoint returns the private partner download URL. Direct browser S3 writes remain a fallback only when Cognito or temporary AWS credentials are configured.
5. The frontend reloads the refreshed S3 artifacts on the next load or refresh.

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
- `workbookHistoryWriteBaseUrl`
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
- `invoiceDraftUrl`
- `automationOutboxUrl`
- `lookerImportWebhookUrl`
- `contractParseWebhookUrl`
- `contractExtractWebhookUrl`
It should independently pull raw source data, run the comparisons, and fail closed on any mismatch.

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

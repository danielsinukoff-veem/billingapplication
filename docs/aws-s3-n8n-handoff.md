# AWS, S3, and n8n Handoff

This is the target operating model for the billing app.

## What we are trying to achieve

We want the app to stay mostly front end driven while moving the heavy work out of the browser and out of the local Python backend. The frontend should remain the place where Finance and Billing users review data, kick off workflows, and approve results. The actual parsing, scheduled imports, reconciliation, and reminder automation should happen in AWS and n8n.

## Current state

- The browser app still owns the UI and most user interaction.
- The repo still contains archival Python reference code for local/shared development.
- Contract parsing, invoice math, checker logic, and Looker sync have server-side reference implementations today.
- The app still has local-first assumptions in a few places, especially around imports and shared state.

## Target state

- Frontend: static web app hosted in AWS.
- File storage: S3.
- Workflow orchestration: n8n.
- Shared data source: an AWS-hosted system of record that the frontend and n8n both read from and write to.
- Contract parsing: n8n job that reads contract files from S3, extracts terms, and writes structured output back to the system of record.
- Looker ingestion: n8n job on a schedule, similar to the current Looker pull flow.
- Checker: a separate reconciliation workflow that recalculates billing and flags mismatches before invoices are released.
- Frontend config: the browser reads `window.BILLING_APP_CONFIG.apiBaseUrl` at runtime, so AWS can inject the production API origin without editing the app code.

## Features affected by removing the Python backend

1. Contract ingestion and parsing
- The local contract extraction and parsing code should no longer be the production path.
- Contracts should live in a designated S3 folder.
- n8n should pick up new files, parse them, and store the structured contract terms centrally.

2. Invoice generation
- The browser should not depend on Python to build invoice drafts.
- Invoice math should move to either the AWS-hosted API layer or an n8n-backed calculation workflow that writes results back to the shared store.

3. Checker / maker-checker control
- The checker should be an independent workflow, not just a browser-only calculation.
- It should recompute the invoice from the same source data and output an exception report before billing is approved.

4. Looker import
- Scheduled Looker sync should run through n8n, not a local script on a developer machine.
- The browser should only display the result of the last successful sync.

5. Audit history and approvals
- Finalized invoice snapshots, reminders, and approval state need to live centrally so they can be reviewed later.
- The browser should never be the only place where billing history exists.

## Recommended first conversion slice

1. Keep the current frontend UI in place.
2. Decide on the AWS system of record for shared billing data.
3. Move contract files into S3 and use the starter n8n contract parsing workflow in `/Users/danielsinukoff/Documents/billing-workbook/docs/eng-handoff-aws-n8n/contract-s3-ingestion.workflow.json`.
4. Move Looker ingestion into n8n on a schedule.
5. Replace the Python checker with an AWS/n8n reconciliation workflow.
6. Repoint the frontend to the new AWS endpoints once they exist.

## Starter n8n package

- `/Users/danielsinukoff/Documents/billing-workbook/docs/eng-handoff-aws-n8n/README.md`
- `/Users/danielsinukoff/Documents/billing-workbook/docs/eng-handoff-aws-n8n/contract-s3-ingestion.md`
- `/Users/danielsinukoff/Documents/billing-workbook/docs/eng-handoff-aws-n8n/contract-s3-ingestion.workflow.json`
- Current n8n data input flow:
  - `/Users/danielsinukoff/Documents/billing-workbook/docs/n8n-looker-ingestion.md`
  - `/Users/danielsinukoff/Documents/billing-workbook/docs/n8n-looker-direct.workflow.json`
  - `/Users/danielsinukoff/Documents/billing-workbook/docs/n8n-looker-cloud.workflow.json`

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

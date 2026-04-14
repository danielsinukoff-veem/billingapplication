# Shared Billing App Architecture

## Goal

Move the current billing workbook from a browser-only tool into a secure, shared internal app where:

- everyone sees the same partner config, imports, and invoices
- invoice calculations run from one server-side source of truth
- uploads, contract checks, and finalized invoices are auditable
- no one relies on `localStorage`, localhost, or local Downloads folders

## Current App Shape

The current app is a static browser app with state stored in `localStorage`.

- UI and calculator: [app.js](/Users/danielsinukoff/Documents/billing-workbook/app.js)
- Seed/workbook data: [data.js](/Users/danielsinukoff/Documents/billing-workbook/data.js)
- Imported transaction bundle: [looker-import.js](/Users/danielsinukoff/Documents/billing-workbook/looker-import.js)
- Import pipeline: [tools/generate_looker_import.py](/Users/danielsinukoff/Documents/billing-workbook/tools/generate_looker_import.py)
- Server-side invoice report logic: [tools/generate_invoice_report.js](/Users/danielsinukoff/Documents/billing-workbook/tools/generate_invoice_report.js)

The current persisted data buckets are:

- `ps`: partners
- `pConfig`: partner pricing mode flags
- `off`: offline rates
- `vol`: volume rates
- `fxRates`: FX pricing
- `cap`: fee caps
- `rs`: rev-share contract terms
- `mins`: monthly minimums
- `plat`: platform fees
- `revf`: reversal fees
- `impl`: implementation/setup fees
- `vaFees`: virtual account fee tables
- `surch`: surcharges
- `pCosts`: provider costs
- `ltxn`: imported transaction aggregates
- `lrev`: imported reversal aggregates
- `lva`: imported virtual-account usage
- `lrs`: imported rev-share summary rows
- `lfxp`: imported FX partner payout rows

## Recommended Target Stack

### Chosen stack

- Frontend hosting: Cloudflare Pages
- App perimeter: Cloudflare Access
- Auth, database, storage: Supabase
- Background jobs / server logic: Supabase Edge Functions or a small Node/Python worker service
- Source control + CI: GitHub

### Why this fits this app

- The UI is already a static web app, so Pages is a low-friction first host.
- The app needs central Postgres tables, file storage, auth, and auditability, which Supabase fits well.
- Cloudflare Access gives a simple first security layer for an internal-only app.
- The current Python import pipeline can be reused instead of rewritten immediately.

### Official docs

- Cloudflare Pages: <https://developers.cloudflare.com/pages/framework-guides/deploy-anything/>
- Cloudflare Access: <https://developers.cloudflare.com/cloudflare-one/access-controls/applications/http-apps/self-hosted-public-app/>
- Supabase Row Level Security: <https://supabase.com/docs/guides/database/postgres/row-level-security>
- Supabase API security: <https://supabase.com/docs/guides/api/securing-your-api>
- Supabase SAML SSO: <https://supabase.com/docs/guides/auth/enterprise-sso/auth-sso-saml>

## Security Model

### Authentication

- Require company SSO only.
- Restrict access to company email domain at Cloudflare Access.
- Require SSO again at the application layer via Supabase Auth.

### Authorization

Use app roles, not just “logged in / not logged in”.

- `admin`
  - manage users, roles, environments, imports, contract sources
- `billing_ops`
  - edit partner config, upload source files, generate invoices, finalize invoices
- `finance_approver`
  - review diffs, approve/finalize invoices, export signed outputs
- `readonly`
  - view contracts, imports, and invoices only

### Data protection

- Store uploaded reports and contracts in private object storage
- Never read billing files from local machine paths in production
- Log every config edit and every invoice regeneration
- Snapshot finalized invoices so later config changes do not rewrite history
- Enable database backups and point-in-time recovery

## Core Domain Rules To Preserve

These should live in shared server-side calculation code, not only in the browser:

- billing period is based on `Credit Complete`
- reversals use `Refund Completed`
- duplicate `Payment ID` rows count once unless business rules explicitly say otherwise
- monthly minimum is an either/or rule against minimum-eligible revenue
- platform fees are additive and do not get replaced by monthly minimum logic
- finalized invoices must calculate from a saved snapshot of config + imported facts

## System Design

### 1. Frontend

Keep the current UI shape first. Replace browser persistence with API reads and writes.

Frontend responsibilities:

- authenticate user
- show partner config tables
- upload source files
- trigger import jobs
- render invoice drafts and finalized snapshots
- export CSV/PDF from server-produced results

### 2. API / Server layer

Add a backend API that owns:

- partner config CRUD
- import job creation
- canonical transaction normalization
- invoice calculation
- invoice snapshot finalization
- audit logging

Recommended first API groups:

- `/auth/me`
- `/partners`
- `/pricing/*`
- `/imports`
- `/imports/:id/files`
- `/imports/:id/run`
- `/invoices`
- `/invoices/:id`
- `/invoices/:id/finalize`
- `/contracts`
- `/contracts/:id/verify`

### 3. Storage model

Use a two-layer data design:

- staging layer
  - raw uploaded files
  - parsed raw rows
  - row-level diagnostics
- canonical layer
  - normalized transactions, reversals, virtual-account usage, rev-share summaries, FX partner payouts

That lets you:

- preserve original source files for audit
- inspect parsing mistakes
- fix mapping logic without losing source evidence

### 4. Calculation engine

Extract the calculation rules from [app.js](/Users/danielsinukoff/Documents/billing-workbook/app.js) into a shared module that runs on the server.

That server calculator should be the only source of truth for:

- invoice draft generation
- PDF export
- CSV export
- API responses

The browser should only render returned results.

## Data Model

See the schema outline in [shared-app-schema.sql](/Users/danielsinukoff/Documents/billing-workbook/docs/shared-app-schema.sql).

The high-level entity groups are:

- org and access
  - organizations
  - app_users
  - user_roles
- commercial config
  - partners
  - partner_aliases
  - partner_config
  - pricing term tables
- source files and imports
  - workbook_snapshots
  - import_runs
  - import_files
  - staging_source_rows
- canonical billing facts
  - billing_transactions
  - billing_reversals
  - virtual_account_usage
  - revenue_share_summaries
  - fx_partner_payouts
- contract source of truth
  - contracts
  - contract_extractions
  - contract_verifications
- invoice history
  - invoices
  - invoice_lines
  - invoice_line_activity_links
- audit
  - audit_log

## Import Workflow

### Production workflow

1. User uploads source files for a month
2. Backend stores files privately
3. Import job parses rows into staging
4. Normalizer maps rows into canonical billing facts
5. Validation rules run
6. Any warnings are attached to the import run
7. Invoice drafts recalculate from canonical facts

### Validation checks to keep

- duplicate `Payment ID` detection
- partner mapping failures
- missing `Credit Complete` date
- missing `Refund Completed` date on reversals
- conflicts between attached invoice-support files and canonical imports
- contract/config mismatches

## Invoice Workflow

### Draft

- generated on demand from selected partner + period
- uses current active contract config
- shows active vs inactive monthly-minimum lines
- shows notes and source warnings

### Finalized

When AP or Finance approves a month:

- freeze all calculation inputs into a snapshot
- store rendered invoice lines and totals
- store linked raw activity IDs
- store generated PDF
- mark invoice as `finalized`

After finalization, the displayed invoice should come from snapshot tables, not live recalculation.

## Recommended Deployment Topology

### Environment split

- `dev`
  - sandbox auth
  - test DB
  - test storage bucket
- `staging`
  - production-like config
  - smaller user group
  - pre-release invoice validation
- `prod`
  - real users
  - restricted uploads
  - backups and audit retention

### Git + deploy flow

1. GitHub repo becomes source of truth
2. PRs required for calculator and config logic changes
3. CI runs import tests and invoice snapshot tests
4. Merges deploy to staging
5. Approved promotion deploys to prod

## Migration Plan

### Phase 1. Freeze the domain model

- keep current UI
- document all existing pricing and invoice rules
- lock canonical business rules:
  - `Credit Complete`
  - `Refund Completed`
  - `Payment ID` dedupe
  - monthly minimum behavior

### Phase 2. Stand up shared backend

- create Supabase project
- create schema
- add SSO and roles
- move workbook state out of `localStorage`

### Phase 3. Move imports server-side

- upload files through the app
- run parsing and normalization centrally
- persist canonical facts in Postgres

### Phase 4. Move invoice calculation server-side

- extract calculator from `app.js`
- serve invoice drafts from API
- generate PDFs on server

### Phase 5. Finalization and audit

- add draft / approved / finalized statuses
- add immutable invoice snapshots
- add audit log views

## First Implementation Slice

The lowest-risk first delivery is:

1. Host current UI behind SSO
2. Replace `localStorage` with shared Postgres-backed API
3. Upload and store files centrally
4. Reuse the existing import scripts server-side
5. Reuse the current invoice math on the server

That gets the team in sync without forcing a full frontend rewrite first.

## Open Decisions

- Company IdP: Google Workspace, Okta, or Microsoft Entra?
- Whether Cloudflare Access should be mandatory in front of the app
- Where to run heavy imports:
  - Supabase Edge Functions
  - container worker
  - scheduled GitHub Action against a secure runner
- Approval workflow:
  - one approver or dual approval
- Whether pricing config changes require approval before becoming active

## What I Would Build Next

1. Create the Postgres schema and RLS policies
2. Add auth and roles
3. Add shared API wrappers for current data buckets
4. Move one workflow first:
   - partner config reads/writes
5. Then move one import workflow:
   - monthly billing file upload and normalization
6. Then move invoice generation and PDF finalization

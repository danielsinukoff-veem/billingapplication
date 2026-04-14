# Supabase Backend Setup

This repo now supports two backend modes behind the same `/api/*` contract:

- `sqlite`: local development reference mode
- `supabase`: shared production-style mode backed by Supabase PostgREST

## Environment

Set these environment variables for Supabase mode:

```bash
export BILLING_BACKEND=supabase
export SUPABASE_URL="https://your-project-ref.supabase.co"
export SUPABASE_SERVICE_ROLE_KEY="your-service-role-key"
export BILLING_ORG_SLUG="veem-billing"
export BILLING_ORG_NAME="Veem Billing Workspace"
export BILLING_DEFAULT_USER_EMAIL="billing.ops@company.com"
export BILLING_DEFAULT_USER_ROLE="billing_ops"
```

Optional:

```bash
export BILLING_SERVER_HOST="0.0.0.0"
```

## Apply the database schema

Run the existing migrations in:

- `/Users/danielsinukoff/Documents/billing-workbook/supabase/migrations/20260316_0001_core_schema.sql`
- `/Users/danielsinukoff/Documents/billing-workbook/supabase/migrations/20260316_0002_rls.sql`

## Start the API

```bash
python3 /Users/danielsinukoff/Documents/billing-workbook/server/app.py --port 4174 --backend supabase
```

The server will:

- ensure the configured organization exists
- ensure the configured bootstrap user exists
- ensure that user has the configured role
- store the workbook snapshot in `workbook_snapshots`
- append audit rows to `audit_log`

## Current production shape

The frontend still talks only to:

- `GET /api/bootstrap`
- `PUT /api/workbook`
- `GET /api/invoices/draft`

The server calculates draft invoices from the shared snapshot in memory. This is the right first step for getting the company onto one shared source of truth without rewriting the UI first.

## Next hardening steps

- Replace the placeholder bootstrap identity with real SSO session mapping.
- Move from whole-snapshot persistence toward granular tables for rates, imports, and invoices.
- Persist finalized invoices and invoice lines as immutable snapshots.
- Run this API behind company-controlled hosting and TLS.

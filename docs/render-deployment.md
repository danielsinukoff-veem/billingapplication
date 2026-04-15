# Render Deployment

This app can run as a single Python web service that serves both:

- the billing UI
- the `/api/*` backend

That makes Render the fastest first production host from the current codebase.

## Recommended production shape

- Host: Render web service
- Data store: Supabase
- Source control: GitHub

## Files added for deployment

- [`render.yaml`](/Users/danielsinukoff/Documents/billing-workbook/render.yaml)
- [`requirements.txt`](/Users/danielsinukoff/Documents/billing-workbook/requirements.txt)
- [`runtime.txt`](/Users/danielsinukoff/Documents/billing-workbook/runtime.txt)

## Before deploying

### 1. Create a Supabase project

You need a Supabase project because this app should not use local SQLite in production.

Required values:

- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`

### 2. Apply the schema

Run these migration files in Supabase SQL Editor or your preferred migration flow:

- [`20260316_0001_core_schema.sql`](/Users/danielsinukoff/Documents/billing-workbook/supabase/migrations/20260316_0001_core_schema.sql)
- [`20260316_0002_rls.sql`](/Users/danielsinukoff/Documents/billing-workbook/supabase/migrations/20260316_0002_rls.sql)

### 3. Connect the GitHub repo in Render

Use this repo:

- [https://github.com/danielsinukoff-veem/billingapplication](https://github.com/danielsinukoff-veem/billingapplication)

## Deploy with Render

### Option A: Blueprint

1. In Render, choose `New +`
2. Choose `Blueprint`
3. Select the GitHub repo
4. Render will detect [`render.yaml`](/Users/danielsinukoff/Documents/billing-workbook/render.yaml)
5. Fill in:
   - `SUPABASE_URL`
   - `SUPABASE_SERVICE_ROLE_KEY`
6. Deploy

### Option B: Manual web service

If you prefer not to use the blueprint:

- Runtime: `Python`
- Build command: `pip install -r requirements.txt`
- Start command: `python3 server/app.py`

Environment variables:

- `BILLING_BACKEND=supabase`
- `SUPABASE_URL=...`
- `SUPABASE_SERVICE_ROLE_KEY=...`
- `BILLING_ORG_SLUG=veem-billing`
- `BILLING_ORG_NAME=Veem Billing Workspace`
- `BILLING_DEFAULT_USER_EMAIL=billing.ops@veem.local`
- `BILLING_DEFAULT_USER_ROLE=billing_ops`
- optional: `BILLING_API_TOKEN=...`

## Notes

- Render injects `PORT`, and the server now honors that automatically.
- The app serves the frontend and backend from the same process, so you only need one service.
- Contract PDF extraction falls back to `pypdf` automatically on Linux hosts where Swift is unavailable.
- For internal-only access, put the service behind your company access layer or add SSO in front of it.

## First smoke test after deploy

Check:

- `/api/health`
- `/api/bootstrap`

Then open the root app URL and confirm:

- the app loads
- the shared workspace bootstrap returns
- invoice draft generation works for at least one known partner/month

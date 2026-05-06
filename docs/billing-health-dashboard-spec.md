# Billing Health Dashboard — build spec

Goal: a single page in the billing app (or a static HTML companion) that
turns red when the n8n pipeline writes bad/incomplete data, so we never
again ship a stale or partially-populated snapshot to S3 without
noticing. The dashboard reads the same `current-workbook.json` the SPA
already loads — no new infrastructure, no Lambda.

---

## What it needs to surface (one card per check)

Each check is a function that takes the loaded `state` snapshot and
returns `{ status: "green" | "yellow" | "red", value, threshold, link, narrative }`.
The card renders the status as a colored bar, the current value, the
threshold for going yellow/red, and a one-line narrative explaining what
the user should do.

### 1. Snapshot freshness
- **Green**: `_saved` within the last 36 hours.
- **Yellow**: 36–72 hours.
- **Red**: > 72 hours.
- Why: we expect n8n to run daily; >72h usually means the schedule is broken.
- Compute: `(Date.now() - Date.parse(state._saved)) / 3600000`.

### 2. Snapshot version
- **Green**: `_version === STORAGE_VERSION` (currently 36).
- **Red**: anything older.
- Why: an older version means migrations ran client-side but never persisted; the next sync will write a v34 blob over our v36 fixes.

### 3. Stampli untrusted-source rows
- **Green**: 0 ltxn rows where `directInvoiceSource ∈ {stampli_credit_complete_billing, stampli_direct_billing, stampli_usd_abroad_revenue, stampli_usd_abroad_reversal}`.
- **Red**: any.
- Why: these were the bad Looks dropped on 2026-04-21; a non-zero count means they came back.

### 4. Pre-collected revenue coverage (the "Whish-style minimum risk")
- **Green**: ≥ 80% of ltxn rows for partners with `mins` config have `estRevenue > 0`.
- **Yellow**: 50–80%.
- **Red**: < 50%.
- Why: if estRevenue is missing on minimum-eligible partners, the calc will trigger the minimum incorrectly. This was the core bug we shipped today's fix for.

### 5. Reversal data presence (the "Blindpay $3,967 gap")
- **Green**: every partner with a non-empty `revf` config has either (a) at least one ltxn row for the period with negative `directInvoiceAmount`, OR (b) an `lrev` row for the period with `reversalCount > 0`.
- **Yellow**: 1–2 partners missing reversal data.
- **Red**: ≥ 3 partners missing.
- Why: every "Reversal fees are configured for this partner, but no reversal upload was imported" note is a billing gap.

### 6. notYetLive vs imported activity
- **Green**: no partners flagged `notYetLive=true` have ltxn rows with non-zero `customerRevenue` for the current period.
- **Red**: any.
- Why: this catches HubSpot drift (the Nomad case — flagged Integration Underway but actually live and transacting).

### 7. Implementation-fee coverage
- **Green**: every partner whose `pBilling.notYetLive=true` has either (a) an `impl` row with `feeAmount > 0`, or (b) `pBilling.implementationCharged=true` flag.
- **Yellow**: 1 partner missing.
- **Red**: ≥ 2 missing.
- Why: implementation fees should bill at signing — if the row's missing, we won't bill them when the partner goes live.

### 8. Contract-config completeness
- **Green**: every partner in `state.ps` has at least one row in at least one of `off`, `vol`, `mins`, `plat`, `rs`, `revf`.
- **Red**: any partner has nothing.
- Why: the VG Pay case — workbook lists the partner but has no fee schedule; any future PDF would have nothing to back it.

### 9. PDF-to-calc validator (the QA loop)
This is heavier and runs separately, but the dashboard surfaces the
last results:
- **Green**: ≥ 90% PASS or NEAR_PASS.
- **Yellow**: 70–89%.
- **Red**: < 70%.
- Pulls from `/tmp/batch-validate-results.json` (or wherever the n8n QA
  Checker dumps its output — `n8n-qa-checker.workflow.json` already exists).
- Each FAIL row is clickable → drills into a partner detail panel.

### 10. Per-feed row counts vs expected
A simple bar chart of `ltxn`, `lrev`, `lrs`, `lva`, `lfxp` row counts
with the previous 7 days as a sparkline.
- **Yellow**: any feed dropped >25% week-over-week.
- **Red**: any feed at zero rows.
- Why: catches "the Revenue Share Report Look returned 0 rows today" type
  pipeline failures the moment they happen.

---

## Where it lives

Two options, in increasing order of effort:

### Option A — page inside the existing SPA (minimal effort)
- Add a top-level route (`#/health`) in `app.js` that renders the cards.
- Each card is a pure function of `state`; no new fetches.
- Surfaces only when `pBilling.role === "admin"` (or whatever role gate the app is using).
- Effort: ~half a day. Reads the data the app already has loaded.

### Option B — static page hosted alongside the SPA
- New file `health.html` in the same S3 bucket.
- Loads `current-workbook.json` directly via Cognito (same as the SPA).
- Renders the same cards via vanilla JS.
- Pros: visible without logging into the billing UI; can be linked from a Slack channel topic.
- Cons: duplicates state-loading code.
- Effort: 1 day.

Recommendation: build Option A first. If you want it visible to people
who don't use the billing app daily, Option B is a small add-on.

---

## Wiring the n8n QA checker into this

You already have `docs/n8n-qa-checker.workflow.json` and a related
`n8n-qa-checker.md`. The dashboard's check #9 is the place where its
output surfaces. Two specific changes to the QA checker workflow to
make it useful here:

1. After it runs the validator, write the results JSON to S3 at
   `data/qa-results.json` (alongside `current-workbook.json`).
2. Have the SPA fetch that JSON when rendering the dashboard's check #9.

That way the dashboard reflects the most recent batch validation
without re-running the calc in the browser.

---

## Alerting (next step after the dashboard ships)

Once the dashboard is up, wire each red check to a Slack ping. The
n8n flow can hit a webhook every time the QA checker runs and post a
single message:

> ⚠️ Billing snapshot QA — 2 red, 1 yellow
> • [RED] Pre-collected revenue coverage: 32% (threshold 50%)
> • [RED] Reversal data: 4 partners missing
> • [YELLOW] PDF/calc match: 73% (threshold 90%)
> https://billing.qa-us-west-2.veem.com/#/health

Each line links back to the dashboard. Total cost: ~10 minutes of n8n
node config. The point isn't to fix problems automatically — it's to
make sure nobody ever ships a snapshot that fails one of these checks
without seeing it first.

---

## Definition of "done" for this dashboard

- Cards 1–8 land in the SPA, color-coded, thresholds wired.
- Card 9 reads `qa-results.json` written by the n8n QA checker.
- One Slack alert per red status from the n8n run.
- Documented at `docs/billing-health-dashboard-spec.md` (this file) so the
  next person to add a check knows the pattern.

That's it — every check in the file is a 10–30 line pure function over
`state`, and every red result is a thing we already burned cycles on
in this session. Building it once means we never burn those cycles
again.

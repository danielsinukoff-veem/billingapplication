# Billing QA action follow-up — 2026-04-28

What was applied today, what changed, and what's still on the table.

## Applied locally (in `server/data/shared_workspace.db`)

### 1. Nomad — flipped to live
- `pBilling.notYetLive` → **false**
- `pBilling.goLiveDate` → **2026-03-01** (anchored on first imported activity)
- `pBilling.subEntities` → **`["Nomad Global", "Nomad Shop"]`** (UI hint only)

**Validator outcome:** Nomad 2026-03 AR now **PASSES** at $10,000 (matches PDF). The $10K minimum is correctly applied.

**Sub-entity caveat:** the `subEntities` field is a UI hint that the partner has two entity views (Nomad Global, Nomad Shop). Actual split rollup requires the Looker side to tag each row with which entity it belongs to. Current Looker output groups everything under "Nomad" — Veem can build the alias mapping (we already added `nomad global` and `nomad shop` to PARTNER_PATTERNS, both → `"Nomad"`). The UI work to render two cards from the single dataset is a separate task and needs Looker source change first.

### 2. Multigate — implementation fee + credit pool
Added one impl row, exactly per the contract Schedule of Fees:

```json
{
  "partner": "Multigate",
  "feeType": "Implementation",
  "feeAmount": 7500,                       // bills once at Commencement
  "goLiveDate": "2026-04-01",              // Month-1 anchor
  "creditMode": "monthly_minimum",
  "creditAmount": 5000,                    // banked to offset future minimums
  "applyAgainstMin": true,
  "note": "Initial Commitment Payment: $7,500 one-time at Commencement (2026-04-01). $5,000 banked as service credit against future monthly minimums (Month-4 onward = 2026-07+); $2,500 non-refundable platform activation fee Veem retains."
}
```

Also flipped `implFeeOffset: true` on all four Multigate min rows (the $5K/$7.5K/$10K/$12.5K tiers) so the credit pool actually drains them.

**Expected billing pattern**:
| Period | What bills | Why |
|---|---:|---|
| 2026-04 | $7,500 | Commencement Implementation Fee |
| 2026-05, 06 | $0 | Months 2 and 3 — no min, no txns |
| 2026-07 | $0 | Month 4 min ($5K tier) fully offset by $5K credit pool |
| 2026-08+ | $5K+ minimum | Credit pool exhausted; min charges normally |

**Net** Veem keeps $2,500 of the original $7,500.

### 3. VG Pay — alias backfill (partner stays not-yet-live)
- `pBilling.aliases` → **`["Vigipay", "Venture Garden", "Venture Garden Pay"]`**
- No other config changes — confirmed not yet live, only original implementation fees charged at signing apply.

### 4. Cellpay / Repay — confirmed already correct
Both `notYetLive=true`. No PDF should issue beyond the original implementation fees charged at signing.

---

## Patched in `docs/n8n-looker-cloud.workflow.json` (+ backup `.bak.json`)

### Aliases added to `PARTNER_PATTERNS` (16 nodes touched)
```js
["vigipay", "VG Pay"],
["venture garden", "VG Pay"],
["venture garden pay", "VG Pay"],
["nomad global", "Nomad"],
["nomad shop", "Nomad"],
["nomadshop", "Nomad"],
```

### `extractEstRevenue` fallback chain (16 nodes touched)
The original looked only at `Est Revenue` / `Estimated Revenue`. Looker base Looks (e.g. Look 6876 — Partner Offline Billing) don't expose an "Est Revenue" column, so this returned `0` for every offline-billing row. We now fall back through the revenue columns Looker DOES expose, in order of preference:

1. `Est Revenue` / `Estimated Revenue` (preferred — explicit Looker measure)
2. `Net Revenue`
3. `Fixed Fee`
4. `Partner Net Revenue Share`
5. `Revenue Owed`
6. Pattern match on normalized variants

This is a **belt-and-suspenders fix in the workflow code** so the calc engine can see pre-collected revenue immediately. The cleaner long-term fix is to add `Est Revenue` as a Looker measure on Look 6876 — the workflow then prefers it over the fallback.

**Expected outcome** after the next n8n run: partners like Whish whose calcs were triggering the monthly minimum because no pre-collected revenue was visible should now show the partial revenue and (depending on amount) skip the minimum.

---

## Still requires action

### A. Looker-side: add measures to feeds
The workflow code now handles missing `Est Revenue` gracefully, but the cleanest source of truth is:
- **Look 6876 — Partner Offline Billing**: add an "Est Revenue" measure (Veem revenue collected at txn time per row).
- **Reversal Look** (Look ID currently returns only `period, partner, payerFunding, reversalCount`): add `Total Reversal Amount` and `Reversal Fee Charged` columns so the calc has per-period numbers without needing to multiply count × contract rate.

Without these, the workflow has to derive these from other columns or the calc has to compute reversals as `lrev.reversalCount × pBilling-derived $/reversal`. That second option is doable but lives in the calc engine, not the n8n flow.

### B. Calc-engine option: count-based reversal charging
If we don't want to wait for Looker to expose a per-reversal feed, add a `revf`-driven path:
```
reversalCharge = lrev.reversalCount × revf.feePerReversal  (where revf row exists for the partner+period)
```
This would close the Blindpay $3,967 gap, Nuvion $1,414, Remittanceshub $770, Yeepay $13, Skydo $121 — every "Reversal fees are configured for this partner, but no reversal upload was imported" note.

**Code location to add this:** `app.js` around line 5664 (the `minimumRow = recurringBillingActive` block) — after the offline/volume/VA charges run, fold in `reversalCount × revf.feePerReversal` if `lrev` has a reversal-count row for the period and `revf` has a per-reversal price.

I haven't applied this fix yet because it needs the workbook to surface a "fee per reversal" field on the `revf` config, which today only stores `partnerSharePercent` / `direction` — there's no $/reversal column. Adding it is a 1-day task: extend the `revf` schema, surface it in the partner-detail UI, and wire it into the calc.

### C. AP rev-share rate verification (Everflow / Shepherd / Halo / Q2)
Back-calculating from the PDFs vs imported `customerRevenue`:

| Partner | PDF | customerRevenue | Implied rate | Workbook rs |
|---|---:|---:|---:|---:|
| Everflow | $5,304.59 | $10,627.62 | **49.9%** | 40% |
| Shepherd | $1,863.35 | $4,795.85 | **38.9%** | 30% |
| Halorecruiting | $54.50 | $214.44 | **25.4%** | 20% |
| Q2 | $22.26 | $46.24 | **48.1%** | 40% |

Each PDF rate is consistently ~10 percentage points higher than the workbook config — that pattern usually means **the contract rate applies to gross customer revenue (no cost subtraction)**, while the calc applies it to `(customerRevenue − cost)`. To verify, read the actual Everflow / Shepherd / Halo / Q2 partner agreements and either:
1. Bump the workbook `rs.revSharePct` to match the PDF rate (and keep the cost subtraction), OR
2. Add a `costMode: "none"` flag on the `rs` row that tells the calc to apply the rate to gross revenue.

The PDF-vs-calc gap (~$1,950 Everflow, ~$946 Shepherd, ~$31 Halo, ~$11 Q2) is fully explained by this single config question. Once resolved, those four FAILs flip to PASS.

### D. Remaining open PDFs that should be voided / re-issued
- **Stampli 2026-03 AR ($58,976) and AP ($52,950)** — issued from the dropped USD Abroad Look. Calc says AR=$8,000, AP=$14,873.86. Re-issue.
- **Cellpay $5K, Repay $5K, VG Pay $5K** — partners not yet live, calc says $0. Void or re-issue as $0.
- **Multigate $5K** — was an early implementation bill that didn't match the contract's $7,500 commencement structure. Now the workbook has the proper impl row firing in 2026-04. Either void the $5K and re-issue $7,500 in 2026-04, or treat the $5K as a partial payment toward the $7,500 commencement.

### E. Whish $1,678 over PDF
The PDF was $8,322; calc fires the $10K minimum because no pre-collected revenue is visible. After the next n8n run with the new `extractEstRevenue` fallback, Whish should show the actual Net Revenue from offline billing and (if it exceeds $10K) skip the minimum, or (if less) show a partial-credit.

If Whish PDFs continue to differ from the calc, the contract may have a separate clause (partial-month proration, manual credit, etc.) that needs to be encoded in the workbook.

---

## How to deploy the changes

1. Re-export the workbook from SQLite → JSON:
   ```bash
   python3 tools/export-snapshot-for-deploy.py
   ```
   Produces `data/current-workbook.json` with the Nomad/Multigate/VG-Pay updates and the v36 migration applied.

2. Upload to S3 (needs QA-bucket-writer credentials I don't have):
   ```bash
   aws s3 cp data/current-workbook.json s3://veem-qa-billing-data/data/current-workbook.json \
     --content-type application/json --cache-control "no-cache" --region us-west-2
   ```

3. Replace the n8n workflow with the patched version:
   - File: `docs/n8n-looker-cloud.workflow.json`
   - Backup of the previous version: `docs/n8n-looker-cloud.workflow.workflow.bak.json`
   - Import the new version into n8n. Trigger a manual run to verify estRevenue appears on the next snapshot.

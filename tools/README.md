# Utility Scripts

This folder now contains only reusable utilities that still support the billing app handoff or migration work.

## Kept on purpose

- `audit_contracts.py`
  - contract QA against a folder of PDFs
- `audit_billing_sanity.py`
  - billing/checker sanity checks
- `generate_looker_import.py`
  - Looker import generation used by the reference server
- `import_historical_partner_data.py`
  - bootstrap helper for historical partner data
- `pull_looker_and_push.py`
  - current Looker import helper used by n8n and the reference server
- `push_looker_batch.py`
  - batch Looker import helper used by n8n
- `extract_pdf_text.swift`
  - PDF text extraction helper
- `ocr_pdf.swift`
  - OCR fallback helper

## Removed

One-off repair scripts, backfills, debugging helpers, and monthly artifact exporters were removed from the repository to keep the handoff focused on the production path.

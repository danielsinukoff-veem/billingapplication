# n8n Contract Automation

This workflow turns the app's `Import Contract` tab into a browser-safe contract automation path. The frontend never stores LLM, OCR, or document parser credentials; it only calls n8n production webhooks.

## Importable Workflow

Use this file:

```text
docs/n8n-contract-automation.workflow.json
```

Import it into n8n and activate/publish the workflow.

The website is configured to call:

```text
https://veem.app.n8n.cloud/webhook/billing-contract-extract
https://veem.app.n8n.cloud/webhook/billing-contract-parse
```

## What Works Immediately

- Text files load in the browser without n8n extraction.
- Pasted contract text can be parsed through the `billing-contract-parse` webhook.
- The included parser extracts common contract terms deterministically: partner name, effective date, billing timing, monthly minimums, reversal fees, platform fees, implementation fees, flat transaction fees, FX markup/spread rows, and revenue-share notes.
- Parsed rows are returned in the exact shape the app already expects for the contract import preview.

## PDF Extraction Node

The `billing-contract-extract` path uses n8n's built-in `Extract From File` node with operation `pdf`.

```text
Contract Extract Webhook
Normalize Extract Request
Build PDF Binary
Extract PDF Text
Normalize PDF Text Response
```

`Extract PDF Text` is the n8n node. It is configured as:

```text
Node type: n8n-nodes-base.extractFromFile
Operation: Extract From PDF
Input binary field: data
```

The app receives this response from `Normalize PDF Text Response`:

```json
{
  "fileName": "Partner Contract.pdf",
  "text": "Full extracted contract text...",
  "charCount": 12345,
  "warnings": []
}
```

Text files still load directly in the browser and do not need this webhook. Scanned/image-only PDFs may return empty text; those require replacing `Extract PDF Text` with an OCR or document parser node/service.

## Optional LLM Parser Upgrade

The deterministic parser is intentionally conservative. If Finance wants stronger coverage, replace this node:

```text
Deterministic Contract Parser
```

with an OpenAI, Claude, or other LLM node. The replacement node must return a JSON object with this shape:

```json
{
  "partnerName": "Stampli",
  "effectiveDate": "2026-03-01",
  "billingTerms": {
    "billingFreq": "Monthly",
    "payBy": "7th Business Day"
  },
  "offlineRates": [],
  "volumeRates": [],
  "fxRates": [],
  "feeCaps": [],
  "minimums": [],
  "reversalFees": [],
  "platformFees": [],
  "implFees": [],
  "virtualAccountFees": [],
  "surcharges": [],
  "otherFees": [],
  "revShareTiers": [],
  "revShareFees": [],
  "warnings": []
}
```

The most important importable rows are:

- `offlineRates`: `{ "txnType": "Domestic", "speedFlag": "Standard", "minAmt": 0, "maxAmt": 1000000000, "payerCcy": "USD", "payeeCcy": "USD", "fee": 2.5, "note": "" }`
- `volumeRates`: `{ "txnType": "", "speedFlag": "", "minVol": 0, "maxVol": 1000000000, "rate": 0.0025, "note": "" }`
- `fxRates`: `{ "payeeCorridor": "Major", "payeeCcy": "", "minVol": 0, "maxVol": 1000000000, "rate": 0.002, "note": "" }`
- `minimums`: `{ "minAmount": 1000, "minVol": 0, "maxVol": 1000000000, "note": "" }`
- `reversalFees`: `{ "payerFunding": "", "feePerReversal": 2.5, "note": "" }`
- `platformFees`: `{ "monthlyFee": 1000, "note": "" }`
- `implFees`: `{ "feeType": "Implementation", "feeAmount": 5000, "creditMode": "", "creditAmount": 0, "creditWindowDays": 0, "note": "" }`

Revenue-share fields currently display for review but are not automatically imported into the workbook tables.

## Testing

In n8n:

1. Import `docs/n8n-contract-automation.workflow.json`.
2. Run `Manual Parse Test`.
3. Confirm `Deterministic Contract Parser` returns `partnerName`, `minimums`, `reversalFees`, and `fxRates`.
4. Run `Manual Extract Test`.
5. Confirm `Normalize PDF Text Response` returns `text` and `charCount`.
6. Activate/publish the workflow.

In the app:

1. Open `Import Contract`.
2. Paste contract text into the text box.
3. Click `Parse Pricing`.
4. Review the extracted terms before importing.

PDF upload will require the real n8n extraction node described above before it can parse PDF bytes.

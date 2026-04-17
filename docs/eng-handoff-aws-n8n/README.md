# AWS / S3 / n8n Contract Ingestion Handoff

This folder is the starter package for the new contract ingestion path.

## What this flow does

- Watches the S3 contract drop folder on a schedule.
- Lists new contract files.
- Downloads each file.
- Extracts or prepares the contract text.
- Sends the normalized contract payload to the billing API for storage.

## What engineering needs to wire up

- S3 bucket name.
- Incoming and processed contract prefixes.
- AWS credentials for the n8n S3 node.
- Billing API base URL and token.
- PDF extraction step for PDF-heavy contracts, if needed.

## Contract folder convention

Recommended S3 layout:

- `contracts/incoming/`
- `contracts/processed/`
- `contracts/errors/`

## Current behavior

The starter workflow is intentionally simple:

- it is scheduled
- it uses S3 as the contract source
- it posts the parsed payload to the billing API

For PDF contracts, replace the raw-text decode step with n8n's file extraction node or AWS Textract before the final ingest call.

## Related docs

- [AWS, S3, and n8n handoff](/Users/danielsinukoff/Documents/billing-workbook/docs/aws-s3-n8n-handoff.md)
- [Shared API contract](/Users/danielsinukoff/Documents/billing-workbook/docs/shared-api-contract.md)
- Current n8n data input flow:
  - [n8n Looker ingestion guide](/Users/danielsinukoff/Documents/billing-workbook/docs/n8n-looker-ingestion.md)
  - [n8n direct workflow](/Users/danielsinukoff/Documents/billing-workbook/docs/n8n-looker-direct.workflow.json)
  - [n8n cloud workflow](/Users/danielsinukoff/Documents/billing-workbook/docs/n8n-looker-cloud.workflow.json)

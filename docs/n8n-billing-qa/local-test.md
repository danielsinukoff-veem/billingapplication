# Billing QA Local Test

This workflow is the runnable local version of the checker.

## What it does

- parses a sample contract twice using OpenAI
- compares the two parse outputs
- recalculates billing from the parsed terms
- compares sample billing-feed rows to sample Snowflake rows
- flags zero-fee transactions and suggests the fallback rate

## What it does not do yet

- it does not use a real Claude credential
- it does not call a real Snowflake connector
- it does not read S3 yet

Those pieces are still part of the production handoff package. This local harness exists so you can test the checker shape immediately in n8n.

## Import files

- [local-test.workflow.json](/Users/danielsinukoff/Documents/billing-workbook/docs/n8n-billing-qa/local-test.workflow.json)
- [README.md](/Users/danielsinukoff/Documents/billing-workbook/docs/n8n-billing-qa/README.md)

## Before you run it

1. Set `OPENAI_API_KEY` in your local n8n environment.
2. Import the workflow into n8n.
3. If you want to change the model, edit the `Build Local QA Context` node:
   - `primaryModel`
   - `shadowModel`
4. Run the workflow manually.

## What to expect

The sample data intentionally includes:

- a transaction feed mismatch between the billing app sample and the Snowflake sample
- a zero-fee transaction with a missing currency mapping

So the local harness should return a report with exceptions, not a clean pass.

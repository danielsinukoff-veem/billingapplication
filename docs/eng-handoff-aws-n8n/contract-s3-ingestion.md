# Contract S3 Ingestion Flow

This workflow is the first AWS / S3 / n8n contract parsing starter.

## Goal

Move contract parsing out of the app and into a scheduled n8n workflow that reads files from S3 and writes normalized contract data back to the billing system.

## Steps

1. Finance or Ops drops a contract file into the S3 incoming folder.
2. n8n runs on a schedule and lists new files.
3. n8n downloads each file.
4. n8n extracts or prepares the contract text.
5. n8n sends the parsed contract payload to the billing API.
6. The file should then be moved to a processed folder or error folder, depending on the ingest result.

## Expected S3 folders

- `contracts/incoming/`
- `contracts/processed/`
- `contracts/errors/`

## Notes for engineering

- PDF files will need a real extraction step before the ingest POST.
- Text-based contracts can be parsed directly by the starter flow.
- The billing API endpoint should be injected by deployment config, not hardcoded to localhost.

## Output

The ingest payload should contain:

- source bucket
- source key
- file name
- contract text or extracted text reference
- partner hint
- run id
- ingest status

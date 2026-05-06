# n8n API Deploy

Use this if you want Codex to create/update the n8n workflows instead of manually dragging JSON files into n8n Cloud.

## What This Automates

The deploy helper imports the active workflow JSONs from this repo:

```text
docs/n8n-looker-cloud-split/*.workflow.json
docs/n8n-qa-checker.workflow.json
docs/n8n-qa-status-api.workflow.json
docs/n8n-looker-manual-import.workflow.json
docs/n8n-contract-automation.workflow.json
docs/n8n-hubspot-partner-sync.workflow.json
```

Retired monolithic Looker workflow exports were removed from this repo, so the deploy helper only imports the active workflow paths listed above.

By default it creates new workflows as inactive. Existing workflows found by exact name are updated in place, so do not run `--apply` against a live workflow unless you are ready for that workflow definition to change.

If `N8N_PROJECT_ID` is set, new workflows target that n8n project. Existing workflows found by exact name are still updated in place. n8n's public API may not expose folder placement inside a project, so newly created workflows may need to be moved into the `Partner Billing` folder manually if n8n does not place them there automatically.

## What Is Still Manual

n8n credentials are encrypted and workspace-specific. The JSON exports do not contain usable AWS credential IDs, and the HubSpot workflow only contains placeholders.

After API deploy, open each workflow and assign credentials once:

```text
AWS S3 nodes -> AWS (IAM) account
HubSpot nodes -> HubSpot OAuth2 API
```

After credentials are assigned, publish/activate the workflows.

## Required n8n API Key

Create or copy an n8n API key in n8n Cloud, then run:

```bash
cd /Users/danielsinukoff/Documents/billing-workbook
N8N_API_KEY="paste_api_key_here" node tools/deploy_n8n_workflows.mjs --apply
```

Or save it locally in an ignored file:

```bash
cd /Users/danielsinukoff/Documents/billing-workbook
printf 'N8N_API_KEY=%s\n' 'paste_api_key_here' > .env.n8n
printf 'N8N_PROJECT_ID=%s\n' 'rFYV4PhQ7kkEZpEt' >> .env.n8n
node tools/deploy_n8n_workflows.mjs --apply
```

To check what it will do before making changes:

```bash
cd /Users/danielsinukoff/Documents/billing-workbook
N8N_API_KEY="paste_api_key_here" node tools/deploy_n8n_workflows.mjs --dry-run
```

To also delete older duplicate/retired workflow copies:

```bash
cd /Users/danielsinukoff/Documents/billing-workbook
N8N_API_KEY="paste_api_key_here" node tools/deploy_n8n_workflows.mjs --apply --delete-old
```

Do not use `--activate` until credentials have been assigned or injected.

## Optional Credential Injection

If you know the n8n credential IDs, you can inject them during deploy:

```bash
export N8N_CREDENTIAL_MAP='{
  "n8n-nodes-base.hubspot": {
    "hubspotOAuth2Api": { "id": "hubspot_credential_id", "name": "HubSpot OAuth2 API" }
  },
  "n8n-nodes-base.awsS3": {
    "aws": { "id": "aws_credential_id", "name": "AWS (IAM) account" }
  }
}'

N8N_API_KEY="paste_api_key_here" node tools/deploy_n8n_workflows.mjs --apply
```

Only use the AWS credential map if the exported credential type key matches the n8n S3 credential in your workspace.

## Recommended Flow

1. Run `--dry-run`.
2. Run `--apply`.
3. Open the new workflows and assign AWS/HubSpot credentials.
4. Run one workflow manually to confirm the credential works.
5. Publish/activate.
6. Run `--apply --delete-old` only after the new workflows are verified.

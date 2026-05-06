# n8n HubSpot Partner Sync

The app should not store HubSpot credentials. HubSpot API auth stays inside n8n, and the frontend calls one n8n production webhook.

## Importable Workflow

Use this file:

```text
docs/n8n-hubspot-partner-sync.workflow.json
```

Import it into n8n, then open both native HubSpot nodes and select/map your existing `HubSpot OAuth2 API` credential:

- `HubSpot Search Companies`
- `HubSpot Search Associated Contacts`

This avoids the generic HTTP Request credential restriction:

```text
This credential is configured to prevent use within an HTTP Request node
```

Do not replace these with HTTP Request nodes unless Eng enables the credential for that node type. Do not set the HubSpot node `Resource` field to `{{ $json.hubspotResource }}`; n8n does not handle that as a reliable dynamic dispatch field. The imported workflow uses fixed native HubSpot resources/operations instead.

The website is configured to call:

```text
https://veem.app.n8n.cloud/webhook/billing-hubspot-partner-sync
```

After import, activate/publish the n8n workflow so that production webhook URL is live.

## Testing In n8n

The `HubSpot Sync Webhook` node waits by design. It will show `Listening for test event` until the website, Postman, or curl POSTs to the test URL.

For a quick n8n-only test:

1. Do not execute the `HubSpot Sync Webhook` node directly.
2. Open `Manual Test Trigger`.
3. Click `Execute step` or run the workflow from that manual trigger path.
4. Confirm the run reaches `Build Frontend Response` and returns a `summary` plus `partners` array.

For a website test:

1. Activate/publish the workflow.
2. Use the production URL, not the test URL.
3. In the app, open `Partner View` -> `Billing and Invoice Tracking` -> `HubSpot Live Sync`.
4. Click `Sync This Partner`.

## Frontend Config

Set this in the deployed runtime config:

```js
window.BILLING_APP_CONFIG = {
  hubspotSyncWebhookUrl: "https://veem.app.n8n.cloud/webhook/billing-hubspot-partner-sync",
  enableHubSpotSync: true
};
```

`hubspotPartnerSyncWebhookUrl` is also accepted as an alias, but `hubspotSyncWebhookUrl` is the canonical key.

If `BILLING_APP_CONFIG.bearerToken` or `apiToken` is present, the app sends it as `Authorization: Bearer <token>` to the n8n webhook.

## Where It Appears

Open `Partner View`, select a partner, then open `Billing and Invoice Tracking`. The `HubSpot Live Sync` panel has:

- `Sync This Partner`
- `Sync All Partners`

Both actions are admin-only. After n8n returns updates, the app merges the allowed fields into `pBilling` and persists through the existing shared workbook write bridge.

## Request Sent To n8n

```json
{
  "action": "sync_partner",
  "requestedAt": "2026-04-27T20:00:00.000Z",
  "source": "partner-billing-workbook",
  "workspace": {
    "version": 36,
    "label": "Veem Billing Workspace",
    "savedAt": "2026-04-27T20:00:00.000Z"
  },
  "partners": [
    {
      "partner": "Stampli",
      "hubspotCompanyId": "19013930538",
      "hubspotCompanyName": "Stampli",
      "hubspotDomain": "stampli.com",
      "followUpHubspotUrl": "https://app.hubspot.com/contacts/23392895/record/0-2/19013930538",
      "partnerLegalName": "Stampli",
      "partnerBillingAddress": "127 Dalma Dr\nMountain View, CA 94041\nUnited States",
      "contactEmails": "billing@stampli.com",
      "hubspotContactEmails": "finance@stampli.com, billing@stampli.com"
    }
  ]
}
```

`action` is `sync_partner` for one partner and `sync_all_partners` for the full workbook list.

## Expected n8n Response

Return one of these top-level arrays: `partners`, `updates`, `partnerProfiles`, `profiles`, or `pBilling`.

```json
{
  "summary": {
    "matched": 1,
    "unmatched": 0
  },
  "partners": [
    {
      "partner": "Stampli",
      "patch": {
        "hubspotCompanyId": "19013930538",
        "hubspotCompanyName": "Stampli",
        "hubspotDomain": "stampli.com",
        "followUpHubspotUrl": "https://app.hubspot.com/contacts/23392895/record/0-2/19013930538",
        "partnerLegalName": "Stampli",
        "partnerBillingAddress": "127 Dalma Dr\nMountain View, CA 94041\nUnited States",
        "contactEmails": "billing@stampli.com, finance@stampli.com",
        "hubspotContactEmails": [
          "billing@stampli.com",
          "finance@stampli.com"
        ],
        "hubspotAddressCandidates": [
          "127 Dalma Dr\nMountain View, CA 94041\nUnited States"
        ],
        "integrationStatus": "Live",
        "goLiveDate": "2026-01-01",
        "notYetLive": false,
        "followUpStatus": "Current",
        "followUpReason": "",
        "followUpNextDate": "2026-05-07",
        "followUpNotes": "Synced from HubSpot."
      }
    }
  ]
}
```

The `patch` wrapper is optional. Flat partner rows with these same fields are accepted.

The included workflow returns this shape automatically. It searches HubSpot companies, then searches contacts associated with the matched company, then maps those results to this response contract.

## Allowed Fields

The frontend only applies these fields:

- `partnerLegalName`
- `partnerBillingAddress`
- `contactEmails`
- `hubspotContactEmails`
- `hubspotAddressCandidates`
- `hubspotCompanyId`
- `hubspotCompanyName`
- `hubspotDomain`
- `followUpHubspotUrl`
- `integrationStatus`
- `goLiveDate`
- `notYetLive`
- `followUpStatus`
- `followUpReason`
- `followUpNextDate`
- `followUpNotes`

Common aliases are accepted, including `companyId`, `companyName`, `domain`, `hubspotUrl`, `emails`, `billingAddress`, `legalName`, `liveDate`, `status`, and `notes`.

## CORS

The n8n production webhook must allow:

- `https://billing.qa-us-west-2.veem.com`
- Any local testing origin if needed

For `file://` testing, the browser may send `Origin: null`. Prefer testing from the QA URL.

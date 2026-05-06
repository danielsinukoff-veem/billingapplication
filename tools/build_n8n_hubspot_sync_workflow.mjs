import fs from "node:fs";
import path from "node:path";

const outputPath = path.resolve("docs/n8n-hubspot-partner-sync.workflow.json");

const normalizeFrontendRequestCode = String.raw`
const first = $input.first();
const raw = first?.json || {};
const body = raw.body && typeof raw.body === "object" ? raw.body : raw;
const partners = Array.isArray(body.partners) ? body.partners : [];

function clean(value) {
  return String(value || "").trim();
}

function unique(values) {
  return [...new Set(values.map(clean).filter(Boolean))];
}

function normalizeDomain(value) {
  const text = clean(value).toLowerCase();
  if (!text) return "";
  try {
    const host = new URL(text.includes("://") ? text : "https://" + text).hostname;
    return host.replace(/^www\./, "");
  } catch {
    return text.replace(/^https?:\/\//, "").replace(/^www\./, "").split(/[/?#]/)[0];
  }
}

function domainsFromEmails(value) {
  return clean(value)
    .split(/[,\s;]+/)
    .map((entry) => entry.split("@")[1] || "")
    .map(normalizeDomain)
    .filter(Boolean);
}

if (!partners.length) {
  throw new Error("HubSpot sync request did not include a partners array.");
}

return partners.map((entry, index) => {
  const partner = clean(entry.partner || entry.partnerName || entry.name);
  if (!partner) throw new Error("Partner sync item " + (index + 1) + " did not include a partner name.");
  const partnerLegalName = clean(entry.partnerLegalName) || partner;
  const domainCandidates = unique([
    normalizeDomain(entry.hubspotDomain || entry.domain),
    ...domainsFromEmails(entry.contactEmails),
    ...domainsFromEmails(entry.hubspotContactEmails)
  ]);
  return {
    json: {
      action: clean(body.action) || "sync_all_partners",
      requestedAt: clean(body.requestedAt) || new Date().toISOString(),
      source: clean(body.source) || "partner-billing-workbook",
      workspace: body.workspace || {},
      partner,
      partnerLegalName,
      searchName: partnerLegalName,
      hubspotCompanyId: clean(entry.hubspotCompanyId || entry.companyId),
      domainCandidates,
      frontendContext: entry
    }
  };
});
`;

const prepareCompanySearchCode = String.raw`
const companyProperties = [
  "name",
  "domain",
  "hs_additional_domains",
  "address",
  "address2",
  "city",
  "state",
  "zip",
  "country",
  "phone",
  "lifecyclestage",
  "hubspot_owner_id",
  "integration_status",
  "partner_status",
  "go_live_date",
  "live_date",
  "partner_live_date",
  "billing_email",
  "ap_email",
  "finance_email"
];

function clean(value) {
  return String(value || "").trim();
}

return $input.all().map((item) => {
  const ctx = item.json || {};
  const firstDomain = Array.isArray(ctx.domainCandidates) ? clean(ctx.domainCandidates[0]).toLowerCase() : "";

  return {
    json: {
      ...ctx,
      companySearchDomain: firstDomain || "__missing-domain__.invalid",
      companySearchProperties: companyProperties
    }
  };
});
`;

const buildContactSearchRequestsCode = String.raw`
const requests = $("Prepare Company Search").all();
const companyResponses = $input.all();

function clean(value) {
  return String(value || "").trim();
}

function pairedIndex(item, fallbackIndex) {
  const paired = item?.pairedItem;
  if (Array.isArray(paired)) return Number(paired[0]?.item ?? fallbackIndex);
  if (paired && typeof paired === "object") return Number(paired.item ?? fallbackIndex);
  return fallbackIndex;
}

function propValue(props, key) {
  const raw = props?.[key];
  if (raw == null) return "";
  if (typeof raw === "object") {
    if (raw.value != null) return clean(raw.value);
    if (Array.isArray(raw.versions) && raw.versions[0]?.value != null) return clean(raw.versions[0].value);
  }
  return clean(raw);
}

function normalizeCompany(company) {
  const props = company?.properties || {};
  return {
    ...company,
    id: clean(company?.id || company?.companyId || propValue(props, "hs_object_id")),
    properties: props
  };
}

function norm(value) {
  return clean(value).toLowerCase().replace(/[^a-z0-9]+/g, "");
}

function scoreCompany(ctx, company) {
  const props = company?.properties || {};
  let score = 0;
  const companyId = clean(company?.id || company?.companyId || propValue(props, "hs_object_id"));
  if (ctx.hubspotCompanyId && companyId === String(ctx.hubspotCompanyId)) score += 100;
  const domains = Array.isArray(ctx.domainCandidates) ? ctx.domainCandidates : [];
  if (domains.includes(propValue(props, "domain").toLowerCase())) score += 40;
  if (domains.some((domain) => propValue(props, "hs_additional_domains").toLowerCase().includes(domain))) score += 25;
  if (norm(propValue(props, "name")) === norm(ctx.searchName || ctx.partner)) score += 30;
  if (norm(propValue(props, "name")).includes(norm(ctx.partner)) || norm(ctx.partner).includes(norm(propValue(props, "name")))) score += 15;
  return score;
}

function pickBestCompany(ctx, results) {
  let best = null;
  let bestScore = -1;
  for (const company of results) {
    const score = scoreCompany(ctx, company);
    if (score > bestScore) {
      best = company;
      bestScore = score;
    }
  }
  return { company: best, score: bestScore };
}

const responsesByRequestIndex = new Map();
companyResponses.forEach((item, fallbackIndex) => {
  const response = normalizeCompany(item.json || {});
  const hasProperties = response.properties && Object.keys(response.properties).length > 0;
  if (!response.id && !response.companyId && !hasProperties) return;
  const index = pairedIndex(item, fallbackIndex);
  if (!responsesByRequestIndex.has(index)) responsesByRequestIndex.set(index, []);
  responsesByRequestIndex.get(index).push(response);
});

return requests.map((requestItem, index) => {
  const ctx = requestItem.json || {};
  const results = responsesByRequestIndex.get(index) || [];
  const picked = pickBestCompany(ctx, results);
  const company = picked.company || null;
  const companyId = clean(company?.id || company?.companyId || propValue(company?.properties, "hs_object_id"));

  return {
    json: {
      ...ctx,
      company,
      companyId,
      companyMatchScore: picked.score,
      companySearchResultCount: results.length,
      contactCompanyIdForSearch: companyId || "__no_company_match__"
    }
  };
});
`;

const buildFrontendResponseCode = String.raw`
const config = $("Config").all()[0]?.json || {};
const contexts = $("Build Contact Search Requests").all();
const contactResponses = $input.all();

function clean(value) {
  return String(value || "").trim();
}

function pairedIndex(item, fallbackIndex) {
  const paired = item?.pairedItem;
  if (Array.isArray(paired)) return Number(paired[0]?.item ?? fallbackIndex);
  if (paired && typeof paired === "object") return Number(paired.item ?? fallbackIndex);
  return fallbackIndex;
}

function unique(values) {
  return [...new Set(values.map(clean).filter(Boolean))];
}

function propValue(props, key) {
  const raw = props?.[key];
  if (raw == null) return "";
  if (typeof raw === "object") {
    if (raw.value != null) return clean(raw.value);
    if (Array.isArray(raw.versions) && raw.versions[0]?.value != null) return clean(raw.versions[0].value);
  }
  return clean(raw);
}

function firstProp(props, keys) {
  for (const key of keys) {
    const value = propValue(props, key);
    if (value) return value;
  }
  return "";
}

function normalizeDate(value) {
  const text = clean(value);
  if (!text) return "";
  if (/^\d{4}-\d{2}-\d{2}/.test(text)) return text.slice(0, 10);
  if (/^\d{13}$/.test(text)) return new Date(Number(text)).toISOString().slice(0, 10);
  if (/^\d{10}$/.test(text)) return new Date(Number(text) * 1000).toISOString().slice(0, 10);
  return "";
}

function buildAddress(props) {
  const line1 = firstProp(props, ["address"]);
  const line2 = firstProp(props, ["address2"]);
  const city = firstProp(props, ["city"]);
  const state = firstProp(props, ["state"]);
  const zip = firstProp(props, ["zip"]);
  const country = firstProp(props, ["country"]);
  const cityLine = [city, state, zip].filter(Boolean).join(", ").replace(", " + zip, " " + zip);
  return [line1, line2, cityLine, country].filter(Boolean).join("\n");
}

function hubspotRecordUrl(portalId, companyId) {
  if (!portalId || !companyId) return "";
  return "https://app.hubspot.com/contacts/" + portalId + "/record/0-2/" + companyId;
}

const contactsByContextIndex = new Map();
contactResponses.forEach((item, fallbackIndex) => {
  const contact = item.json || {};
  if (!contact.id && !contact.properties) return;
  const index = pairedIndex(item, fallbackIndex);
  if (!contactsByContextIndex.has(index)) contactsByContextIndex.set(index, []);
  contactsByContextIndex.get(index).push(contact);
});

const partners = contexts.map((contextItem, index) => {
  const ctx = contextItem.json || {};
  const company = ctx.company || null;
  const props = company?.properties || {};
  const companyId = clean(ctx.companyId || company?.id || company?.companyId || propValue(props, "hs_object_id"));
  const contactResults = contactsByContextIndex.get(index) || [];
  const contactEmails = unique(contactResults.map((contact) => propValue(contact?.properties, "email")));
  const companyEmails = unique([
    firstProp(props, ["billing_email"]),
    firstProp(props, ["ap_email"]),
    firstProp(props, ["finance_email"])
  ]);
  const emails = unique([...companyEmails, ...contactEmails]);
  const address = buildAddress(props);
  const status = firstProp(props, ["integration_status", "partner_status", "lifecyclestage"]);
  const goLiveDate = normalizeDate(firstProp(props, ["go_live_date", "live_date", "partner_live_date"]));
  const existing = ctx.frontendContext || {};
  const patch = {
    hubspotCompanyId: companyId,
    hubspotCompanyName: firstProp(props, ["name"]) || clean(ctx.partnerLegalName || ctx.partner),
    hubspotDomain: firstProp(props, ["domain"]) || (Array.isArray(ctx.domainCandidates) ? ctx.domainCandidates[0] || "" : ""),
    followUpHubspotUrl: clean(existing.followUpHubspotUrl) || hubspotRecordUrl(config.hubspotPortalId, companyId),
    hubspotContactEmails: emails.join(", "),
    hubspotAddressCandidates: address,
    integrationStatus: status,
    followUpNotes: companyId
      ? "HubSpot sync matched company " + companyId + " with " + emails.length + " contact email(s)."
      : "HubSpot sync did not find a company match."
  };
  if (!clean(existing.partnerBillingAddress) && address) patch.partnerBillingAddress = address;
  if (!clean(existing.partnerLegalName) && patch.hubspotCompanyName) patch.partnerLegalName = patch.hubspotCompanyName;
  if (!clean(existing.contactEmails) && emails.length) patch.contactEmails = emails.join(", ");
  if (goLiveDate) patch.goLiveDate = goLiveDate;
  if (status && /not\s*live|not\s*yet|implementation|onboarding/i.test(status)) patch.notYetLive = true;
  if (status && /live/i.test(status) && !/not/i.test(status)) patch.notYetLive = false;

  Object.keys(patch).forEach((key) => {
    if (patch[key] === "" || patch[key] == null) delete patch[key];
  });

  return {
    partner: ctx.partner,
    patch,
    meta: {
      companySearchResultCount: ctx.companySearchResultCount || 0,
      companyMatchScore: ctx.companyMatchScore || 0,
      contactCount: contactResults.length
    }
  };
});

const matched = partners.filter((row) => row.patch.hubspotCompanyId).length;
const emailCount = partners.reduce((sum, row) => {
  return sum + (row.patch.hubspotContactEmails ? row.patch.hubspotContactEmails.split(",").map(clean).filter(Boolean).length : 0);
}, 0);

return [
  {
    json: {
      summary: {
        matched,
        unmatched: partners.length - matched,
        partners: partners.length,
        contactEmails: emailCount,
        syncedAt: new Date().toISOString(),
        mode: config.manualTest ? "manual_test" : "webhook"
      },
      manualTest: !!config.manualTest,
      partners
    }
  }
];
`;

const sampleFrontendRequestCode = String.raw`
return [
  {
    json: {
      manualTest: true,
      action: "sync_partner",
      requestedAt: new Date().toISOString(),
      source: "partner-billing-workbook-manual-test",
      workspace: {
        version: 36,
        label: "Manual n8n test"
      },
      partners: [
        {
          partner: "Stampli",
          hubspotCompanyName: "Stampli",
          hubspotDomain: "stampli.com",
          partnerLegalName: "Stampli",
          contactEmails: "billing@stampli.com"
        }
      ]
    }
  }
];
`;

function codeNode(id, name, position, jsCode) {
  return {
    parameters: {
      mode: "runOnceForAllItems",
      language: "javaScript",
      jsCode
    },
    id,
    name,
    type: "n8n-nodes-base.code",
    typeVersion: 2,
    position
  };
}

function hubspotNode(id, name, position, parameters, options = {}) {
  return {
    parameters: {
      authentication: "oAuth2",
      ...parameters
    },
    id,
    name,
    type: "n8n-nodes-base.hubspot",
    typeVersion: 2.2,
    position,
    ...options,
    credentials: {
      hubspotOAuth2Api: {
        id: "REPLACE_WITH_YOUR_HUBSPOT_CREDENTIAL_ID",
        name: "HubSpot OAuth2 API"
      }
    }
  };
}

const companyProperties = [
  "name",
  "domain",
  "hs_additional_domains",
  "address",
  "address2",
  "city",
  "state",
  "zip",
  "country",
  "phone",
  "lifecyclestage",
  "hubspot_owner_id",
  "integration_status",
  "partner_status",
  "go_live_date",
  "live_date",
  "partner_live_date",
  "billing_email",
  "ap_email",
  "finance_email"
];

const contactProperties = [
  "email",
  "firstname",
  "lastname",
  "phone",
  "jobtitle",
  "hs_object_id"
];

const workflow = {
  name: "Billing Workbook HubSpot Partner Sync - Updated 2026-04-30",
  active: false,
  isArchived: false,
  nodes: [
    {
      parameters: {
        httpMethod: "POST",
        path: "billing-hubspot-partner-sync",
        responseMode: "lastNode",
        options: {
          allowedOrigins: "https://billing.qa-us-west-2.veem.com"
        }
      },
      id: "hubspot-sync-webhook",
      name: "HubSpot Sync Webhook",
      type: "n8n-nodes-base.webhook",
      typeVersion: 2,
      position: [180, 260],
      webhookId: "billing-hubspot-partner-sync"
    },
    {
      parameters: {},
      id: "manual-test-trigger",
      name: "Manual Test Trigger",
      type: "n8n-nodes-base.manualTrigger",
      typeVersion: 1,
      position: [180, 500]
    },
    codeNode("sample-frontend-request", "Sample Frontend Request", [460, 500], sampleFrontendRequestCode),
    codeNode(
      "config",
      "Config",
      [460, 260],
      'const input = $input.first()?.json || {};\nreturn [{ json: { ...input, hubspotPortalId: "23392895" } }];'
    ),
    codeNode("normalize-frontend-request", "Normalize Frontend Request", [740, 260], normalizeFrontendRequestCode),
    codeNode("prepare-company-search", "Prepare Company Search", [1040, 260], prepareCompanySearchCode),
    hubspotNode(
      "hubspot-search-companies",
      "HubSpot Search Companies",
      [1360, 260],
      {
        resource: "company",
        operation: "searchByDomain",
        domain: "={{ $json.companySearchDomain }}",
        returnAll: false,
        limit: 5,
        options: {
          properties: companyProperties
        }
      },
      { alwaysOutputData: true }
    ),
    codeNode("build-contact-search-requests", "Build Contact Search Requests", [1680, 260], buildContactSearchRequestsCode),
    hubspotNode(
      "hubspot-search-associated-contacts",
      "HubSpot Search Associated Contacts",
      [2000, 260],
      {
        resource: "contact",
        operation: "search",
        returnAll: false,
        limit: 100,
        filterGroupsUi: {
          filterGroupsValues: [
            {
              filtersUi: {
                filterValues: [
                  {
                    propertyName: "associations.company|string",
                    operator: "EQ",
                    value: "={{ $json.contactCompanyIdForSearch }}"
                  }
                ]
              }
            }
          ]
        },
        additionalFields: {
          properties: contactProperties
        }
      },
      { alwaysOutputData: true }
    ),
    codeNode("build-frontend-response", "Build Frontend Response", [2320, 260], buildFrontendResponseCode),
    {
      parameters: {
        content: "For n8n-only testing, run Manual Test Trigger, not HubSpot Sync Webhook. The Webhook node intentionally waits until the website POSTs to it. For production, activate the workflow and use the production webhook URL.",
        height: 220,
        width: 460,
        color: 5
      },
      id: "setup-note",
      name: "Setup Note",
      type: "n8n-nodes-base.stickyNote",
      typeVersion: 1,
      position: [1020, -40]
    }
  ],
  connections: {
    "HubSpot Sync Webhook": {
      main: [[{ node: "Config", type: "main", index: 0 }]]
    },
    "Manual Test Trigger": {
      main: [[{ node: "Sample Frontend Request", type: "main", index: 0 }]]
    },
    "Sample Frontend Request": {
      main: [[{ node: "Config", type: "main", index: 0 }]]
    },
    Config: {
      main: [[{ node: "Normalize Frontend Request", type: "main", index: 0 }]]
    },
    "Normalize Frontend Request": {
      main: [[{ node: "Prepare Company Search", type: "main", index: 0 }]]
    },
    "Prepare Company Search": {
      main: [[{ node: "HubSpot Search Companies", type: "main", index: 0 }]]
    },
    "HubSpot Search Companies": {
      main: [[{ node: "Build Contact Search Requests", type: "main", index: 0 }]]
    },
    "Build Contact Search Requests": {
      main: [[{ node: "HubSpot Search Associated Contacts", type: "main", index: 0 }]]
    },
    "HubSpot Search Associated Contacts": {
      main: [[{ node: "Build Frontend Response", type: "main", index: 0 }]]
    }
  },
  pinData: {},
  settings: {
    executionOrder: "v1"
  },
  tags: []
};

fs.writeFileSync(outputPath, JSON.stringify(workflow, null, 2) + "\n");
console.log("wrote " + outputPath);

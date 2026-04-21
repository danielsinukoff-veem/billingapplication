import { billingAppConfig } from "./shared-config.js?v=20260421b";

function normalizeConfigUrl(value) {
  return String(value || "").trim();
}

function readWindowOverrides() {
  if (typeof window === "undefined" || !window.BILLING_APP_CONFIG || typeof window.BILLING_APP_CONFIG !== "object") {
    return {};
  }
  return window.BILLING_APP_CONFIG;
}

export function getSharedBackendConfig() {
  const overrides = readWindowOverrides();
  const merged = {
    ...billingAppConfig,
    ...overrides
  };
  return {
    ...merged,
    bootstrapUrl: normalizeConfigUrl(merged.bootstrapUrl),
    workbookReadUrl: normalizeConfigUrl(merged.workbookReadUrl),
    workbookWriteWebhookUrl: normalizeConfigUrl(merged.workbookWriteWebhookUrl),
    invoiceDraftUrl: normalizeConfigUrl(merged.invoiceDraftUrl),
    automationOutboxUrl: normalizeConfigUrl(merged.automationOutboxUrl),
    checkerWebhookUrl: normalizeConfigUrl(merged.checkerWebhookUrl),
    lookerImportWebhookUrl: normalizeConfigUrl(merged.lookerImportWebhookUrl),
    contractParseWebhookUrl: normalizeConfigUrl(merged.contractParseWebhookUrl),
    contractExtractWebhookUrl: normalizeConfigUrl(merged.contractExtractWebhookUrl)
  };
}

function resolveUrl(value, params = null) {
  if (!value) return "";
  const base = typeof window !== "undefined" && window.location ? window.location.href : "http://localhost/";
  const url = new URL(value, base);
  if (params) {
    Object.entries(params).forEach(([key, val]) => {
      if (val != null && val !== "") url.searchParams.set(key, val);
    });
  }
  return url.toString();
}

function getSharedWorkbookReadUrl() {
  const config = getSharedBackendConfig();
  return config.workbookReadUrl || config.bootstrapUrl;
}

function getSharedWorkbookWriteUrl() {
  return getSharedBackendConfig().workbookWriteWebhookUrl;
}

export function isSharedWorkbookEnabled() {
  const config = getSharedBackendConfig();
  return !!(config.enableSharedWorkbook && getSharedWorkbookReadUrl());
}

export function isSharedWorkbookWriteEnabled() {
  const config = getSharedBackendConfig();
  return !!(config.enableSharedWorkbook && config.workbookWriteWebhookUrl);
}

export function isRemoteInvoiceReadEnabled() {
  const config = getSharedBackendConfig();
  return !!(config.enableRemoteInvoiceReads && config.invoiceDraftUrl);
}

export function isBillingCheckerEnabled() {
  const config = getSharedBackendConfig();
  return !!(config.enableCheckerRuns && config.checkerWebhookUrl);
}

export function isLookerImportEnabled() {
  const config = getSharedBackendConfig();
  return !!(config.enableLookerImports && config.lookerImportWebhookUrl);
}

export function isContractParseEnabled() {
  const config = getSharedBackendConfig();
  return !!(config.enableContractAutomation && config.contractParseWebhookUrl);
}

export function isContractExtractEnabled() {
  const config = getSharedBackendConfig();
  return !!(config.enableContractAutomation && config.contractExtractWebhookUrl);
}

export function getWorkspaceLabel() {
  const config = getSharedBackendConfig();
  if (isSharedWorkbookEnabled()) return config.workspaceLabel || "Shared workspace";
  return config.workspaceLabel || "Local workspace";
}

function sleep(ms) {
  return new Promise((resolve) => window.setTimeout(resolve, ms));
}

function buildHeaders(extra = {}) {
  const config = getSharedBackendConfig();
  const headers = {
    Accept: "application/json",
    ...extra
  };
  if (config.apiToken) headers.Authorization = `Bearer ${config.apiToken}`;
  return headers;
}

async function parseApiResponse(response, fallbackMessage) {
  const text = await response.text();
  let payload = null;
  try {
    payload = text ? JSON.parse(text) : null;
  } catch (error) {
    payload = null;
  }
  if (!response.ok) {
    const message = payload?.error || payload?.message || text || fallbackMessage;
    throw new Error(message);
  }
  return payload;
}

function normalizeBootstrapPayload(payload) {
  if (!payload) return payload;
  if (payload.snapshot && payload.snapshot._version) {
    return {
      workspace: payload.workspace || { label: payload.workspaceLabel || getWorkspaceLabel() },
      user: payload.user || {},
      snapshot: payload.snapshot
    };
  }
  if (payload._version) {
    return {
      workspace: { label: payload.workspaceLabel || getWorkspaceLabel() },
      user: {},
      snapshot: payload
    };
  }
  throw new Error("Shared workbook file did not include a valid snapshot.");
}

async function fetchConfiguredJson(url, fallbackMessage, { params = null, retries = 0, retryDelayMs = 350 } = {}) {
  let lastError = null;
  for (let attempt = 0; attempt <= retries; attempt += 1) {
    try {
      const payload = await parseApiResponse(
        await fetch(resolveUrl(url, params), {
          method: "GET",
          headers: buildHeaders(),
          cache: "no-store"
        }),
        fallbackMessage
      );
      return payload;
    } catch (error) {
      lastError = error;
      if (attempt >= retries) break;
      await sleep(retryDelayMs * (attempt + 1));
    }
  }
  throw lastError || new Error(fallbackMessage);
}

async function postConfiguredJson(url, body, fallbackMessage) {
  return parseApiResponse(
    await fetch(resolveUrl(url), {
      method: "POST",
      headers: buildHeaders({ "Content-Type": "application/json" }),
      body: JSON.stringify(body || {})
    }),
    fallbackMessage
  );
}

export async function loadSharedBootstrap({ retries = 4, retryDelayMs = 350 } = {}) {
  const staticUrl = getSharedWorkbookReadUrl();
  if (!staticUrl) {
    throw new Error("Shared workbook file is not configured. Set BILLING_APP_CONFIG.bootstrapUrl or workbookReadUrl.");
  }
  const payload = await fetchConfiguredJson(staticUrl, "Could not load the shared workbook file.", {
    params: { ts: Date.now() },
    retries,
    retryDelayMs
  });
  return normalizeBootstrapPayload(payload);
}

export async function saveSharedWorkbookSnapshot(snapshot) {
  const writeUrl = getSharedWorkbookWriteUrl();
  if (!writeUrl) {
    throw new Error("Shared workbook saving is not configured. Set BILLING_APP_CONFIG.workbookWriteWebhookUrl.");
  }
  const payload = await postConfiguredJson(
    writeUrl,
    {
      snapshot,
      savedAt: snapshot?._saved || new Date().toISOString(),
      workspaceLabel: getWorkspaceLabel(),
      mode: "workbook_save"
    },
    "Could not save the shared workbook."
  );
  return payload || { savedAt: snapshot?._saved || new Date().toISOString() };
}

export async function fetchSharedDraftInvoice(partner, startPeriod, endPeriod = startPeriod) {
  const config = getSharedBackendConfig();
  if (!config.invoiceDraftUrl) {
    throw new Error("Remote invoice reads are not configured.");
  }
  return fetchConfiguredJson(
    config.invoiceDraftUrl,
    "Could not load the server-generated invoice.",
    { params: { partner, startPeriod, endPeriod } }
  );
}

export async function fetchBillingAutomationOutbox(asOf = "", lookaheadDays = 45) {
  const config = getSharedBackendConfig();
  if (!config.automationOutboxUrl) {
    throw new Error("Billing automation outbox is not configured.");
  }
  return fetchConfiguredJson(
    config.automationOutboxUrl,
    "Could not load the billing automation outbox.",
    { params: { asOf, lookaheadDays } }
  );
}

export async function fetchBillingCheckerReport(payload) {
  const config = getSharedBackendConfig();
  if (!config.checkerWebhookUrl) {
    throw new Error("Billing checker is not configured.");
  }
  return postConfiguredJson(config.checkerWebhookUrl, payload, "Could not run the billing checker.");
}

export async function importLookerFileAndSave(payload) {
  const config = getSharedBackendConfig();
  if (!config.lookerImportWebhookUrl) {
    throw new Error("Looker import automation is not configured.");
  }
  return postConfiguredJson(config.lookerImportWebhookUrl, payload, "Could not import and save the Looker file.");
}

export async function parseContractText(payload) {
  const config = getSharedBackendConfig();
  if (!config.contractParseWebhookUrl) {
    throw new Error("Contract parsing automation is not configured. Paste structured JSON or connect BILLING_APP_CONFIG.contractParseWebhookUrl.");
  }
  return postConfiguredJson(config.contractParseWebhookUrl, payload, "Could not parse the contract text.");
}

export async function extractContractText(payload) {
  const config = getSharedBackendConfig();
  if (!config.contractExtractWebhookUrl) {
    throw new Error("Contract extraction automation is not configured. Connect BILLING_APP_CONFIG.contractExtractWebhookUrl to enable PDF extraction.");
  }
  return postConfiguredJson(config.contractExtractWebhookUrl, payload, "Could not extract the contract text.");
}

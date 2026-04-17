import { billingAppConfig } from "./shared-config.js?v=20260417b";

function trimSlash(value) {
  return String(value || "").replace(/\/+$/, "");
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
    apiBaseUrl: trimSlash(merged.apiBaseUrl)
  };
}

export function getSharedApiBaseUrl() {
  const config = getSharedBackendConfig();
  if (config.apiBaseUrl) return config.apiBaseUrl;
  if (typeof window !== "undefined" && window.location && window.location.origin) {
    return window.location.origin;
  }
  return "";
}

export function isSharedWorkbookEnabled() {
  const config = getSharedBackendConfig();
  return !!(config.enableSharedWorkbook && getSharedApiBaseUrl());
}

export function isRemoteInvoiceReadEnabled() {
  const config = getSharedBackendConfig();
  return !!(config.enableRemoteInvoiceReads && getSharedApiBaseUrl());
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

function buildUrl(path, params = null) {
  const baseUrl = getSharedApiBaseUrl();
  if (!baseUrl) throw new Error("Shared backend is not configured.");
  const url = new URL(path, baseUrl.endsWith("/") ? baseUrl : `${baseUrl}/`);
  if (params) {
    Object.entries(params).forEach(([key, value]) => {
      if (value != null && value !== "") url.searchParams.set(key, value);
    });
  }
  return url.toString();
}

export async function loadSharedBootstrap({ retries = 4, retryDelayMs = 350 } = {}) {
  let lastError = null;
  for (let attempt = 0; attempt <= retries; attempt += 1) {
    try {
      const payload = await parseApiResponse(
        await fetch(buildUrl("/api/bootstrap", { ts: Date.now() }), {
          method: "GET",
          headers: buildHeaders(),
          cache: "no-store"
        }),
        "Could not load shared workspace bootstrap."
      );
      return payload;
    } catch (error) {
      lastError = error;
      if (attempt >= retries) break;
      await sleep(retryDelayMs * (attempt + 1));
    }
  }
  throw lastError || new Error("Could not load shared workspace bootstrap.");
}

export async function saveSharedWorkbookSnapshot(snapshot) {
  const payload = await parseApiResponse(
    await fetch(buildUrl("/api/workbook"), {
      method: "PUT",
      headers: buildHeaders({ "Content-Type": "application/json" }),
      body: JSON.stringify({ snapshot })
    }),
    "Could not save the shared workbook."
  );
  return payload;
}

export async function fetchSharedDraftInvoice(partner, startPeriod, endPeriod = startPeriod) {
  const payload = await parseApiResponse(
    await fetch(buildUrl("/api/invoices/draft", { partner, startPeriod, endPeriod }), {
      method: "GET",
      headers: buildHeaders()
    }),
    "Could not load the server-generated invoice."
  );
  return payload;
}

export async function fetchBillingAutomationOutbox(asOf = "", lookaheadDays = 45) {
  const payload = await parseApiResponse(
    await fetch(buildUrl("/api/automation/outbox", { asOf, lookaheadDays }), {
      method: "GET",
      headers: buildHeaders()
    }),
    "Could not load the billing automation outbox."
  );
  return payload;
}

export async function fetchBillingCheckerReport(payload) {
  return parseApiResponse(
    await fetch(buildUrl("/api/checker/run"), {
      method: "POST",
      headers: buildHeaders({ "Content-Type": "application/json" }),
      body: JSON.stringify(payload || {})
    }),
    "Could not run the billing checker."
  );
}

export async function importLookerFileAndSave(payload) {
  return parseApiResponse(
    await fetch(buildUrl("/api/looker/import-and-save"), {
      method: "POST",
      headers: buildHeaders({ "Content-Type": "application/json" }),
      body: JSON.stringify(payload || {})
    }),
    "Could not import and save the Looker file."
  );
}

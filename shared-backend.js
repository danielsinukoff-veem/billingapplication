import { billingAppConfig } from "./shared-config.js?v=20260421c";

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
    workbookWriteUrl: normalizeConfigUrl(merged.workbookWriteUrl),
    workbookHistoryWriteBaseUrl: normalizeConfigUrl(merged.workbookHistoryWriteBaseUrl),
    invoiceDraftUrl: normalizeConfigUrl(merged.invoiceDraftUrl),
    invoiceArtifactWriteBaseUrl: normalizeConfigUrl(merged.invoiceArtifactWriteBaseUrl),
    privateInvoiceLinkWriteBaseUrl: normalizeConfigUrl(merged.privateInvoiceLinkWriteBaseUrl),
    privateInvoiceLinkReadBaseUrl: normalizeConfigUrl(merged.privateInvoiceLinkReadBaseUrl),
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
  return getSharedBackendConfig().workbookWriteUrl;
}

export function isSharedWorkbookEnabled() {
  const config = getSharedBackendConfig();
  return !!(config.enableSharedWorkbook && getSharedWorkbookReadUrl());
}

export function isSharedWorkbookWriteEnabled() {
  const config = getSharedBackendConfig();
  return !!(config.enableSharedWorkbook && config.workbookWriteUrl);
}

export function isRemoteInvoiceReadEnabled() {
  const config = getSharedBackendConfig();
  return !!(config.enableRemoteInvoiceReads && config.invoiceDraftUrl);
}

export function isInvoiceArtifactEnabled() {
  const config = getSharedBackendConfig();
  return !!(config.enableInvoiceArtifacts && config.invoiceArtifactWriteBaseUrl);
}

export function isPrivateInvoiceLinkEnabled() {
  const config = getSharedBackendConfig();
  return !!(config.enablePrivateInvoiceLinks && config.privateInvoiceLinkWriteBaseUrl);
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

function ensureTrailingSlash(value) {
  const trimmed = normalizeConfigUrl(value);
  if (!trimmed) return "";
  return trimmed.endsWith("/") ? trimmed : `${trimmed}/`;
}

function buildObjectUrl(baseUrl, objectKey) {
  return resolveUrl(`${ensureTrailingSlash(baseUrl)}${String(objectKey || "").replace(/^\/+/, "")}`);
}

function buildStorageHeaders(contentType) {
  const config = getSharedBackendConfig();
  const headers = {};
  if (contentType) headers["Content-Type"] = contentType;
  if (config.apiToken) headers.Authorization = `Bearer ${config.apiToken}`;
  return headers;
}

async function putConfiguredContent(url, content, contentType, fallbackMessage) {
  const response = await fetch(resolveUrl(url), {
    method: "PUT",
    headers: buildStorageHeaders(contentType),
    body: content
  });
  if (!response.ok) {
    const text = await response.text();
    throw new Error(text || fallbackMessage);
  }
  return {
    ok: true,
    url: resolveUrl(url),
    etag: response.headers.get("etag") || "",
    savedAt: new Date().toISOString()
  };
}

function createOpaqueToken(length = 24) {
  const chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
  const bytes = new Uint8Array(length);
  window.crypto.getRandomValues(bytes);
  return Array.from(bytes, (value) => chars[value % chars.length]).join("");
}

function buildPrivateInvoiceIndexHtml({ title, summaryLines, links }) {
  return `<!doctype html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>${title}</title>
    <style>
      body { font-family: system-ui, sans-serif; background: #faf7f2; color: #2f241d; margin: 0; padding: 32px; }
      .card { max-width: 760px; margin: 0 auto; background: white; border: 1px solid #e7ddd1; border-radius: 20px; padding: 28px; }
      h1 { margin: 0 0 12px; font-size: 28px; }
      p, li { line-height: 1.55; }
      .muted { color: #6d5d52; }
      ul { padding-left: 20px; }
      .downloads a { display: inline-block; margin: 8px 10px 0 0; padding: 10px 14px; border-radius: 999px; text-decoration: none; background: #0f5a52; color: white; }
    </style>
  </head>
  <body>
    <div class="card">
      <h1>${title}</h1>
      <div class="muted">
        ${summaryLines.map((line) => `<p>${line}</p>`).join("")}
      </div>
      <div class="downloads">
        ${links.map((item) => `<a href="${item.href}" target="_blank" rel="noopener">${item.label}</a>`).join("")}
      </div>
    </div>
  </body>
</html>`;
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
    throw new Error("Shared workbook saving is not configured. Set BILLING_APP_CONFIG.workbookWriteUrl.");
  }
  const savedAt = snapshot?._saved || new Date().toISOString();
  const serialized = JSON.stringify({
    workspaceLabel: getWorkspaceLabel(),
    snapshot,
    savedAt,
    mode: "workbook_save"
  }, null, 2);
  await putConfiguredContent(writeUrl, serialized, "application/json", "Could not save the shared workbook.");
  const config = getSharedBackendConfig();
  let historyUrl = "";
  if (config.workbookHistoryWriteBaseUrl) {
    const historyKey = `workbook-${savedAt.replaceAll(":", "-").replaceAll(".", "-")}.json`;
    historyUrl = buildObjectUrl(config.workbookHistoryWriteBaseUrl, historyKey);
    await putConfiguredContent(historyUrl, serialized, "application/json", "Could not save the workbook history copy.");
  }
  return { savedAt, url: resolveUrl(writeUrl), historyUrl };
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

export async function saveInvoiceArtifact(payload) {
  const config = getSharedBackendConfig();
  if (!config.invoiceArtifactWriteBaseUrl) {
    throw new Error("Invoice artifact saving is not configured.");
  }
  const artifactId = String(payload?.bundleKey || `invoice-${Date.now()}`);
  const basePath = ensureTrailingSlash(config.invoiceArtifactWriteBaseUrl);
  const manifestUrl = buildObjectUrl(basePath, `${artifactId}/manifest.json`);
  const transactionFileName = payload?.transactions?.fileName || "transactions.csv";
  const transactionsUrl = buildObjectUrl(basePath, `${artifactId}/${transactionFileName}`);
  const documentEntries = Array.isArray(payload?.documents) ? payload.documents : [];
  const savedDocuments = [];

  for (const doc of documentEntries) {
    const fileName = doc?.fileName || `${doc?.kind || "invoice"}.html`;
    const targetUrl = buildObjectUrl(basePath, `${artifactId}/${fileName}`);
    await putConfiguredContent(targetUrl, String(doc?.pdfHtml || ""), "text/html;charset=utf-8", "Could not save the invoice document.");
    savedDocuments.push({
      kind: doc?.kind || "",
      title: doc?.title || "",
      amountDue: Number(doc?.amountDue || 0),
      fileName,
      url: targetUrl
    });
  }

  await putConfiguredContent(
    transactionsUrl,
    String(payload?.transactions?.csvText || ""),
    "text/csv;charset=utf-8",
    "Could not save the transaction export."
  );

  const manifest = {
    artifactId,
    savedAt: payload?.generatedAt || new Date().toISOString(),
    partner: payload?.partner || "",
    period: payload?.period || "",
    periodStart: payload?.periodStart || "",
    periodEnd: payload?.periodEnd || "",
    periodLabel: payload?.periodLabel || "",
    summary: payload?.summary || {},
    workspace: payload?.workspace || {},
    actor: payload?.actor || {},
    warnings: Array.isArray(payload?.warnings) ? payload.warnings : [],
    documents: savedDocuments,
    transactions: {
      fileName: transactionFileName,
      rowCount: Number(payload?.transactions?.rowCount || 0),
      url: transactionsUrl
    }
  };
  await putConfiguredContent(
    manifestUrl,
    JSON.stringify(manifest, null, 2),
    "application/json",
    "Could not save the invoice manifest."
  );

  return {
    artifactId,
    savedAt: manifest.savedAt,
    manifestUrl,
    transactionsUrl,
    documents: savedDocuments,
    fileCount: savedDocuments.length + 2
  };
}

export async function generatePrivateInvoiceLink(payload) {
  const config = getSharedBackendConfig();
  if (!config.privateInvoiceLinkWriteBaseUrl) {
    throw new Error("Private invoice links are not configured.");
  }
  const token = payload?.token || createOpaqueToken(28);
  const writeBase = ensureTrailingSlash(config.privateInvoiceLinkWriteBaseUrl);
  const readBase = ensureTrailingSlash(config.privateInvoiceLinkReadBaseUrl || config.privateInvoiceLinkWriteBaseUrl);
  const savedAt = payload?.requestedAt || new Date().toISOString();
  const documentEntries = Array.isArray(payload?.invoiceArtifact?.documents) ? payload.invoiceArtifact.documents : [];
  const savedDocuments = [];

  for (const doc of documentEntries) {
    const fileName = doc?.fileName || `${doc?.kind || "invoice"}.html`;
    const writeUrl = buildObjectUrl(writeBase, `${token}/${fileName}`);
    const readUrl = buildObjectUrl(readBase, `${token}/${fileName}`);
    await putConfiguredContent(writeUrl, String(doc?.pdfHtml || ""), "text/html;charset=utf-8", "Could not save the private invoice document.");
    savedDocuments.push({
      kind: doc?.kind || "",
      title: doc?.title || "",
      amountDue: Number(doc?.amountDue || 0),
      fileName,
      url: readUrl
    });
  }

  const transactionFileName = payload?.invoiceArtifact?.transactions?.fileName || "transactions.csv";
  const transactionsWriteUrl = buildObjectUrl(writeBase, `${token}/${transactionFileName}`);
  const transactionsReadUrl = buildObjectUrl(readBase, `${token}/${transactionFileName}`);
  await putConfiguredContent(
    transactionsWriteUrl,
    String(payload?.invoiceArtifact?.transactions?.csvText || ""),
    "text/csv;charset=utf-8",
    "Could not save the private transaction export."
  );

  const manifest = {
    token,
    savedAt,
    partner: payload?.partner || payload?.invoiceArtifact?.partner || "",
    period: payload?.period || payload?.invoiceArtifact?.period || "",
    periodStart: payload?.periodStart || payload?.invoiceArtifact?.periodStart || "",
    periodEnd: payload?.periodEnd || payload?.invoiceArtifact?.periodEnd || "",
    summary: payload?.invoiceArtifact?.summary || {},
    archivedArtifact: payload?.archivedArtifact || null,
    documents: savedDocuments,
    transactions: {
      fileName: transactionFileName,
      rowCount: Number(payload?.invoiceArtifact?.transactions?.rowCount || 0),
      url: transactionsReadUrl
    }
  };
  const manifestWriteUrl = buildObjectUrl(writeBase, `${token}/manifest.json`);
  const manifestReadUrl = buildObjectUrl(readBase, `${token}/manifest.json`);
  await putConfiguredContent(
    manifestWriteUrl,
    JSON.stringify(manifest, null, 2),
    "application/json",
    "Could not save the private invoice manifest."
  );

  const summaryLines = [
    `${manifest.partner} · ${manifest.periodStart === manifest.periodEnd ? manifest.periodStart : `${manifest.periodStart} to ${manifest.periodEnd}`}`,
    `Generated ${savedAt}`,
    `This delivery package contains the invoice files and transaction detail export.`
  ];
  const indexHtml = buildPrivateInvoiceIndexHtml({
    title: `${manifest.partner} Billing Package`,
    summaryLines,
    links: [
      ...savedDocuments.map((doc) => ({ label: doc.kind === "receivable" ? "Download AR Invoice" : "Download AP Invoice", href: doc.url })),
      { label: "Download Transactions CSV", href: transactionsReadUrl },
      { label: "Download Manifest JSON", href: manifestReadUrl }
    ]
  });
  const indexWriteUrl = buildObjectUrl(writeBase, `${token}/index.html`);
  const indexReadUrl = buildObjectUrl(readBase, `${token}/index.html`);
  await putConfiguredContent(indexWriteUrl, indexHtml, "text/html;charset=utf-8", "Could not save the private invoice landing page.");

  return {
    token,
    savedAt,
    privateUrl: indexReadUrl,
    manifestUrl: manifestReadUrl,
    transactionsUrl: transactionsReadUrl,
    documents: savedDocuments,
    fileCount: savedDocuments.length + 3
  };
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

import { billingAppConfig } from "./shared-config.js";

const AUTH_REDIRECT_ERROR_CODE = "billing-auth-redirect";
const COGNITO_SESSION_STORAGE_KEY = "billing-workbook-cognito-session";
const COGNITO_PKCE_STORAGE_KEY = "billing-workbook-cognito-pkce";

const sharedWorkbookState = {
  etag: "",
  readUrl: ""
};

let awsBrowserModulesPromise = null;
let cognitoSessionPromise = null;
let awsCredentialProviderCache = {
  key: "",
  provider: null
};

function normalizeConfigUrl(value) {
  return String(value || "").trim();
}

function normalizeConfigList(value) {
  if (Array.isArray(value)) return value.map((item) => String(item || "").trim()).filter(Boolean);
  return String(value || "")
    .split(/[,\s]+/)
    .map((item) => item.trim())
    .filter(Boolean);
}

function readWindowOverrides() {
  if (typeof window === "undefined" || !window.BILLING_APP_CONFIG || typeof window.BILLING_APP_CONFIG !== "object") {
    return {};
  }
  return window.BILLING_APP_CONFIG;
}

function normalizeAuthMethod(value) {
  return String(value || "").trim().toLowerCase();
}

export function getSharedBackendConfig() {
  const overrides = readWindowOverrides();
  const merged = {
    ...billingAppConfig,
    ...overrides
  };
  return {
    ...merged,
    authMethod: normalizeAuthMethod(merged.authMethod),
    bootstrapUrl: normalizeConfigUrl(merged.bootstrapUrl),
    workbookReadUrl: normalizeConfigUrl(merged.workbookReadUrl),
    workbookWriteUrl: normalizeConfigUrl(merged.workbookWriteUrl),
    workbookHistoryWriteBaseUrl: normalizeConfigUrl(merged.workbookHistoryWriteBaseUrl),
    invoiceDraftUrl: normalizeConfigUrl(merged.invoiceDraftUrl),
    invoiceArtifactWriteBaseUrl: normalizeConfigUrl(merged.invoiceArtifactWriteBaseUrl),
    privateInvoiceLinkWriteBaseUrl: normalizeConfigUrl(merged.privateInvoiceLinkWriteBaseUrl),
    privateInvoiceLinkReadBaseUrl: normalizeConfigUrl(merged.privateInvoiceLinkReadBaseUrl),
    privateInvoiceLinkSignerUrl: normalizeConfigUrl(merged.privateInvoiceLinkSignerUrl),
    automationOutboxUrl: normalizeConfigUrl(merged.automationOutboxUrl),
    lookerImportWebhookUrl: normalizeConfigUrl(merged.lookerImportWebhookUrl),
    contractParseWebhookUrl: normalizeConfigUrl(merged.contractParseWebhookUrl),
    contractExtractWebhookUrl: normalizeConfigUrl(merged.contractExtractWebhookUrl),
    awsRegion: normalizeConfigUrl(merged.awsRegion),
    cognitoUserPoolId: normalizeConfigUrl(merged.cognitoUserPoolId),
    cognitoUserPoolClientId: normalizeConfigUrl(merged.cognitoUserPoolClientId),
    cognitoIdentityPoolId: normalizeConfigUrl(merged.cognitoIdentityPoolId),
    cognitoHostedUiDomain: normalizeConfigUrl(merged.cognitoHostedUiDomain),
    cognitoRedirectUrl: normalizeConfigUrl(merged.cognitoRedirectUrl),
    cognitoScopes: normalizeConfigList(merged.cognitoScopes).join(" ")
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

function isAwsStorageAuthMethod(config = getSharedBackendConfig()) {
  const method = normalizeAuthMethod(config.authMethod);
  return method === "aws-cognito"
    || method === "cognito-sigv4"
    || method === "aws-cognito-sigv4"
    || method === "sigv4-cognito"
    || method === "cognito-user-pool-identity-pool";
}

function shouldUseAwsCognito(config = getSharedBackendConfig()) {
  return isAwsStorageAuthMethod(config)
    && !!config.awsRegion
    && !!config.cognitoUserPoolId
    && !!config.cognitoUserPoolClientId
    && !!config.cognitoIdentityPoolId
    && !!config.cognitoHostedUiDomain;
}

function hasInjectedAwsCredentialObject() {
  const credentials = readWindowAwsCredentials();
  if (!credentials || typeof credentials.then === "function") return false;
  return !!(credentials.accessKeyId && credentials.secretAccessKey);
}

function shouldUseAwsStorageAuth(config = getSharedBackendConfig()) {
  return isAwsStorageAuthMethod(config)
    && (shouldUseAwsCognito(config) || hasInjectedAwsCredentialObject());
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
  return !!(config.enablePrivateInvoiceLinks && (config.privateInvoiceLinkSignerUrl || config.privateInvoiceLinkWriteBaseUrl));
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

export function isBillingAuthRedirectError(error) {
  return !!error && (error.code === AUTH_REDIRECT_ERROR_CODE || error.isAuthRedirect === true);
}

function sleep(ms) {
  return new Promise((resolve) => window.setTimeout(resolve, ms));
}

function readStoredJson(key) {
  if (typeof window === "undefined") return null;
  try {
    const raw = window.sessionStorage.getItem(key);
    return raw ? JSON.parse(raw) : null;
  } catch (error) {
    console.warn(`Could not read session storage key ${key}`, error);
    return null;
  }
}

function writeStoredJson(key, value) {
  if (typeof window === "undefined") return;
  try {
    window.sessionStorage.setItem(key, JSON.stringify(value));
  } catch (error) {
    console.warn(`Could not write session storage key ${key}`, error);
  }
}

function clearStoredJson(key) {
  if (typeof window === "undefined") return;
  try {
    window.sessionStorage.removeItem(key);
  } catch (error) {
    console.warn(`Could not clear session storage key ${key}`, error);
  }
}

function createAuthRedirectError() {
  const error = new Error("Redirecting to Cognito login.");
  error.code = AUTH_REDIRECT_ERROR_CODE;
  error.isAuthRedirect = true;
  return error;
}

function buildCognitoIssuer(region, userPoolId) {
  return `cognito-idp.${region}.amazonaws.com/${userPoolId}`;
}

function getCurrentRedirectUrl(config = getSharedBackendConfig()) {
  if (config.cognitoRedirectUrl) return config.cognitoRedirectUrl;
  if (typeof window === "undefined" || !window.location) return "";
  const url = new URL(window.location.href);
  url.searchParams.delete("code");
  url.searchParams.delete("state");
  url.searchParams.delete("error");
  url.searchParams.delete("error_description");
  return url.toString();
}

function getCognitoBaseUrl(config = getSharedBackendConfig()) {
  const domain = config.cognitoHostedUiDomain;
  if (!domain) return "";
  if (/^https?:\/\//i.test(domain)) return domain.replace(/\/+$/, "");
  return `https://${domain.replace(/^\/+/, "").replace(/\/+$/, "")}`;
}

function getCognitoCallbackParams() {
  if (typeof window === "undefined" || !window.location) return null;
  const url = new URL(window.location.href);
  const code = url.searchParams.get("code");
  const state = url.searchParams.get("state");
  const error = url.searchParams.get("error");
  const errorDescription = url.searchParams.get("error_description");
  if (!code && !error) return null;
  return { code, state, error, errorDescription };
}

function clearCognitoCallbackParams() {
  if (typeof window === "undefined" || !window.location || !window.history?.replaceState) return;
  const url = new URL(window.location.href);
  url.searchParams.delete("code");
  url.searchParams.delete("state");
  url.searchParams.delete("error");
  url.searchParams.delete("error_description");
  window.history.replaceState({}, document.title, url.toString());
}

function base64UrlEncode(bytes) {
  let binary = "";
  bytes.forEach((byte) => {
    binary += String.fromCharCode(byte);
  });
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
}

async function sha256Base64Url(text) {
  const data = new TextEncoder().encode(text);
  const digest = await window.crypto.subtle.digest("SHA-256", data);
  return base64UrlEncode(new Uint8Array(digest));
}

function createOpaqueToken(length = 24) {
  const chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
  const bytes = new Uint8Array(length);
  window.crypto.getRandomValues(bytes);
  return Array.from(bytes, (value) => chars[value % chars.length]).join("");
}

async function redirectToCognitoLogin() {
  const config = getSharedBackendConfig();
  if (!shouldUseAwsCognito(config)) {
    throw new Error("Cognito auth is not fully configured. Set region, Cognito IDs, and hosted UI domain.");
  }
  const verifier = createOpaqueToken(64);
  const challenge = await sha256Base64Url(verifier);
  const stateToken = createOpaqueToken(32);
  const redirectUri = getCurrentRedirectUrl(config);
  writeStoredJson(COGNITO_PKCE_STORAGE_KEY, {
    state: stateToken,
    verifier,
    redirectUri,
    requestedAt: Date.now()
  });
  const authorizeUrl = new URL(`${getCognitoBaseUrl(config)}/oauth2/authorize`);
  authorizeUrl.searchParams.set("client_id", config.cognitoUserPoolClientId);
  authorizeUrl.searchParams.set("response_type", "code");
  authorizeUrl.searchParams.set("redirect_uri", redirectUri);
  authorizeUrl.searchParams.set("scope", config.cognitoScopes || "openid email profile");
  authorizeUrl.searchParams.set("state", stateToken);
  authorizeUrl.searchParams.set("code_challenge_method", "S256");
  authorizeUrl.searchParams.set("code_challenge", challenge);
  window.location.assign(authorizeUrl.toString());
  throw createAuthRedirectError();
}

async function exchangeCognitoAuthCode(code, stateParam) {
  const config = getSharedBackendConfig();
  const pkce = readStoredJson(COGNITO_PKCE_STORAGE_KEY);
  if (!pkce?.verifier || !pkce?.state || pkce.state !== stateParam) {
    throw new Error("Cognito login callback state did not match the browser session.");
  }
  const tokenUrl = `${getCognitoBaseUrl(config)}/oauth2/token`;
  const body = new URLSearchParams({
    grant_type: "authorization_code",
    client_id: config.cognitoUserPoolClientId,
    code,
    redirect_uri: pkce.redirectUri || getCurrentRedirectUrl(config),
    code_verifier: pkce.verifier
  });
  const response = await fetch(tokenUrl, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: body.toString()
  });
  const text = await response.text();
  const payload = text ? JSON.parse(text) : {};
  if (!response.ok) {
    throw new Error(payload?.error_description || payload?.error || text || "Could not complete Cognito login.");
  }
  const session = {
    idToken: payload.id_token || "",
    accessToken: payload.access_token || "",
    refreshToken: payload.refresh_token || "",
    tokenType: payload.token_type || "Bearer",
    expiresAt: Date.now() + (Number(payload.expires_in || 3600) * 1000) - 60_000
  };
  writeStoredJson(COGNITO_SESSION_STORAGE_KEY, session);
  clearStoredJson(COGNITO_PKCE_STORAGE_KEY);
  clearCognitoCallbackParams();
  return session;
}

async function refreshCognitoSession(session) {
  if (!session?.refreshToken) return null;
  const config = getSharedBackendConfig();
  const tokenUrl = `${getCognitoBaseUrl(config)}/oauth2/token`;
  const body = new URLSearchParams({
    grant_type: "refresh_token",
    client_id: config.cognitoUserPoolClientId,
    refresh_token: session.refreshToken
  });
  const response = await fetch(tokenUrl, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: body.toString()
  });
  const text = await response.text();
  const payload = text ? JSON.parse(text) : {};
  if (!response.ok) {
    clearStoredJson(COGNITO_SESSION_STORAGE_KEY);
    throw new Error(payload?.error_description || payload?.error || text || "Could not refresh the Cognito session.");
  }
  const refreshed = {
    ...session,
    idToken: payload.id_token || session.idToken || "",
    accessToken: payload.access_token || session.accessToken || "",
    tokenType: payload.token_type || session.tokenType || "Bearer",
    expiresAt: Date.now() + (Number(payload.expires_in || 3600) * 1000) - 60_000
  };
  writeStoredJson(COGNITO_SESSION_STORAGE_KEY, refreshed);
  return refreshed;
}

async function getCognitoSession({ interactive = true } = {}) {
  if (!shouldUseAwsCognito()) return null;
  if (!cognitoSessionPromise) {
    cognitoSessionPromise = (async () => {
      const callback = getCognitoCallbackParams();
      if (callback?.error) {
        clearCognitoCallbackParams();
        throw new Error(callback.errorDescription || callback.error || "Cognito login failed.");
      }
      if (callback?.code) {
        return exchangeCognitoAuthCode(callback.code, callback.state || "");
      }
      const stored = readStoredJson(COGNITO_SESSION_STORAGE_KEY);
      if (stored?.idToken && Number(stored.expiresAt || 0) > Date.now()) {
        return stored;
      }
      if (stored?.refreshToken) {
        return refreshCognitoSession(stored);
      }
      if (interactive) {
        return redirectToCognitoLogin();
      }
      throw new Error("No Cognito browser session is available.");
    })().finally(() => {
      cognitoSessionPromise = null;
    });
  }
  return cognitoSessionPromise;
}

async function loadAwsBrowserModules() {
  if (!awsBrowserModulesPromise) {
    awsBrowserModulesPromise = Promise.all([
      import("https://esm.sh/aws4fetch@1.0.20?bundle"),
      import("https://esm.sh/@aws-sdk/credential-providers@3.922.0?bundle")
    ]).then(([aws4fetchModule, credentialProvidersModule]) => ({
      AwsClient: aws4fetchModule.AwsClient,
      fromCognitoIdentityPool: credentialProvidersModule.fromCognitoIdentityPool
    }));
  }
  return awsBrowserModulesPromise;
}

function readWindowAwsCredentials() {
  if (typeof window === "undefined") return null;
  if (typeof window.getBillingAppAwsCredentials === "function") {
    return window.getBillingAppAwsCredentials();
  }
  if (window.BILLING_APP_AWS_CREDENTIALS && typeof window.BILLING_APP_AWS_CREDENTIALS === "object") {
    return window.BILLING_APP_AWS_CREDENTIALS;
  }
  return null;
}

async function getAwsCredentialProvider() {
  const config = getSharedBackendConfig();
  const windowCredentials = await Promise.resolve(readWindowAwsCredentials());
  if (windowCredentials?.accessKeyId && windowCredentials?.secretAccessKey) {
    return async () => windowCredentials;
  }
  if (!shouldUseAwsCognito(config)) {
    throw new Error("AWS browser writes require Cognito auth configuration or injected temporary AWS credentials.");
  }
  const session = await getCognitoSession({ interactive: true });
  const providerKey = [
    config.awsRegion,
    config.cognitoIdentityPoolId,
    config.cognitoUserPoolId,
    session?.idToken || ""
  ].join("|");
  if (awsCredentialProviderCache.provider && awsCredentialProviderCache.key === providerKey) {
    return awsCredentialProviderCache.provider;
  }
  const { fromCognitoIdentityPool } = await loadAwsBrowserModules();
  const provider = fromCognitoIdentityPool({
    clientConfig: { region: config.awsRegion },
    identityPoolId: config.cognitoIdentityPoolId,
    logins: {
      [buildCognitoIssuer(config.awsRegion, config.cognitoUserPoolId)]: session.idToken
    }
  });
  awsCredentialProviderCache = { key: providerKey, provider };
  return provider;
}

async function resolveAwsCredentials() {
  const provider = await getAwsCredentialProvider();
  return provider();
}

async function signedStorageFetch(url, init = {}) {
  const config = getSharedBackendConfig();
  const { AwsClient } = await loadAwsBrowserModules();
  const credentials = await resolveAwsCredentials();
  const client = new AwsClient({
    accessKeyId: credentials.accessKeyId,
    secretAccessKey: credentials.secretAccessKey,
    sessionToken: credentials.sessionToken,
    region: config.awsRegion,
    service: "s3"
  });
  return client.fetch(url, init);
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

async function buildApiHeaders(extra = {}, { includeCognitoToken = false } = {}) {
  const headers = buildHeaders(extra);
  if (!headers.Authorization && includeCognitoToken && shouldUseAwsCognito()) {
    const session = await getCognitoSession({ interactive: true });
    const bearer = session?.accessToken || session?.idToken;
    if (bearer) headers.Authorization = `Bearer ${bearer}`;
  }
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

async function fetchConfiguredJson(url, fallbackMessage, {
  params = null,
  retries = 0,
  retryDelayMs = 350,
  includeCognitoToken = false
} = {}) {
  let lastError = null;
  for (let attempt = 0; attempt <= retries; attempt += 1) {
    try {
      const payload = await parseApiResponse(
        await fetch(resolveUrl(url, params), {
          method: "GET",
          headers: await buildApiHeaders({}, { includeCognitoToken }),
          cache: "no-store"
        }),
        fallbackMessage
      );
      return payload;
    } catch (error) {
      lastError = error;
      if (attempt >= retries || isBillingAuthRedirectError(error)) break;
      await sleep(retryDelayMs * (attempt + 1));
    }
  }
  throw lastError || new Error(fallbackMessage);
}

async function postConfiguredJson(url, body, fallbackMessage, { includeCognitoToken = false } = {}) {
  return parseApiResponse(
    await fetch(resolveUrl(url), {
      method: "POST",
      headers: await buildApiHeaders({ "Content-Type": "application/json" }, { includeCognitoToken }),
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

function escapeHtml(value) {
  return String(value ?? "").replace(/[&<>"']/g, (char) => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    '"': "&quot;",
    "'": "&#39;"
  })[char]);
}

function safeLinkHref(value) {
  const href = normalizeConfigUrl(value);
  if (!href) return "#";
  try {
    const base = typeof window !== "undefined" && window.location ? window.location.href : "https://billing.qa-us-west-2.veem.com/";
    const url = new URL(href, base);
    if (url.protocol !== "https:" && url.protocol !== "http:") return "#";
    return url.toString();
  } catch (error) {
    return "#";
  }
}

function buildStorageHeaders(contentType, extra = {}) {
  const config = getSharedBackendConfig();
  const headers = {
    ...extra
  };
  if (contentType) headers["Content-Type"] = contentType;
  if (!shouldUseAwsStorageAuth(config) && config.apiToken) {
    headers.Authorization = `Bearer ${config.apiToken}`;
  }
  return headers;
}

async function fetchStorage(url, init = {}) {
  const config = getSharedBackendConfig();
  const resolvedUrl = resolveUrl(url);
  if (shouldUseAwsStorageAuth(config)) {
    return signedStorageFetch(resolvedUrl, init);
  }
  return fetch(resolvedUrl, init);
}

async function fetchStorageJson(url, fallbackMessage, { params = null, retries = 0, retryDelayMs = 350 } = {}) {
  let lastError = null;
  let response = null;
  for (let attempt = 0; attempt <= retries; attempt += 1) {
    try {
      response = await fetchStorage(resolveUrl(url, params), {
        method: "GET",
        headers: buildStorageHeaders("", { Accept: "application/json" }),
        cache: "no-store"
      });
      if (!response.ok) {
        const text = await response.text();
        if (response.status === 401 || response.status === 403) {
          throw new Error(text || "Storage access was denied. Confirm Cognito auth, IAM scope, and S3 CORS settings.");
        }
        throw new Error(text || fallbackMessage);
      }
      const text = await response.text();
      let payload = null;
      try {
        payload = text ? JSON.parse(text) : null;
      } catch (error) {
        throw new Error("Storage response was not valid JSON.");
      }
      return {
        payload,
        etag: response.headers.get("etag") || "",
        url: resolveUrl(url, params)
      };
    } catch (error) {
      lastError = error;
      if (attempt >= retries || isBillingAuthRedirectError(error)) break;
      await sleep(retryDelayMs * (attempt + 1));
    }
  }
  throw lastError || new Error(fallbackMessage);
}

async function putConfiguredContent(url, content, contentType, fallbackMessage, { ifMatch = "", extraHeaders = {} } = {}) {
  const headers = buildStorageHeaders(contentType, extraHeaders);
  if (ifMatch) headers["If-Match"] = ifMatch;
  const response = await fetchStorage(resolveUrl(url), {
    method: "PUT",
    headers,
    body: content
  });
  if (!response.ok) {
    const text = await response.text();
    if (response.status === 412) {
      throw new Error("The shared workbook changed since you loaded it. Refresh the latest snapshot and try again.");
    }
    if (response.status === 401 || response.status === 403) {
      throw new Error(text || "Storage access was denied. Confirm Cognito auth, IAM scope, and S3 CORS settings.");
    }
    throw new Error(text || fallbackMessage);
  }
  return {
    ok: true,
    url: resolveUrl(url),
    etag: response.headers.get("etag") || "",
    savedAt: new Date().toISOString()
  };
}

function buildPrivateInvoiceIndexHtml({ title, summaryLines, links }) {
  const safeTitle = escapeHtml(title || "Partner Invoice Package");
  const safeSummary = (summaryLines || []).map((line) => `<p>${escapeHtml(line)}</p>`).join("");
  const safeLinks = (links || []).map((item) => {
    const href = escapeHtml(safeLinkHref(item?.href));
    const label = escapeHtml(item?.label || "Download");
    return `<a href="${href}" target="_blank" rel="noopener">${label}</a>`;
  }).join("");
  return `<!doctype html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>${safeTitle}</title>
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
      <h1>${safeTitle}</h1>
      <div class="muted">
        ${safeSummary}
      </div>
      <div class="downloads">
        ${safeLinks}
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
  const result = await fetchStorageJson(staticUrl, "Could not load the shared workbook file.", {
    params: { ts: Date.now() },
    retries,
    retryDelayMs
  });
  sharedWorkbookState.etag = result.etag || "";
  sharedWorkbookState.readUrl = result.url || resolveUrl(staticUrl);
  return normalizeBootstrapPayload(result.payload);
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
  const writeResult = await putConfiguredContent(
    writeUrl,
    serialized,
    "application/json",
    "Could not save the shared workbook.",
    { ifMatch: sharedWorkbookState.etag }
  );
  sharedWorkbookState.etag = writeResult.etag || "";
  const config = getSharedBackendConfig();
  let historyUrl = "";
  if (config.workbookHistoryWriteBaseUrl) {
    const historyKey = `workbook-${savedAt.replaceAll(":", "-").replaceAll(".", "-")}.json`;
    historyUrl = buildObjectUrl(config.workbookHistoryWriteBaseUrl, historyKey);
    await putConfiguredContent(historyUrl, serialized, "application/json", "Could not save the workbook history copy.");
  }
  return {
    savedAt,
    url: resolveUrl(writeUrl),
    historyUrl,
    etag: sharedWorkbookState.etag
  };
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
  if (config.privateInvoiceLinkSignerUrl) {
    return postConfiguredJson(
      config.privateInvoiceLinkSignerUrl,
      payload,
      "Could not generate the private invoice link.",
      { includeCognitoToken: true }
    );
  }
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
    "This delivery package contains the invoice files and transaction detail export."
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

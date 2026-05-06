import { billingAppConfig } from "./shared-config.js";

const AUTH_REDIRECT_ERROR_CODE = "billing-auth-redirect";
const COGNITO_SESSION_STORAGE_KEY = "billing-workbook-cognito-session";
const COGNITO_PKCE_STORAGE_KEY = "billing-workbook-cognito-pkce";

const sharedWorkbookState = {
  etag: "",
  readUrl: ""
};

const LAMBDA_FUNCTION_URL_SAFE_JSON_BYTES = 5_500_000;
const INVOICE_ARTIFACT_CHUNK_CHAR_LIMIT = 1_500_000;
const DEFAULT_PRIVATE_DOWNLOAD_URL_TTL_SECONDS = 60 * 60;
const DEFAULT_PRIVATE_LINK_RETENTION_DAYS = 180;
const QA_STATUS_FETCH_TIMEOUT_MS = 5000;

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

function normalizePositiveInteger(value, fallback) {
  const number = Number(value);
  if (!Number.isFinite(number) || number <= 0) return fallback;
  return Math.round(number);
}

function readWindowOverrides() {
  if (typeof window === "undefined") {
    return {};
  }
  const billingConfig = window.BILLING_APP_CONFIG && typeof window.BILLING_APP_CONFIG === "object"
    ? window.BILLING_APP_CONFIG
    : {};
  const veemConfig = window.VEEM_BILLING_FE_CONFIG && typeof window.VEEM_BILLING_FE_CONFIG === "object"
    ? window.VEEM_BILLING_FE_CONFIG
    : {};
  const token = billingConfig.apiToken
    || billingConfig.bearerToken
    || veemConfig.apiToken
    || veemConfig.bearerToken
    || "";
  return {
    ...billingConfig,
    ...veemConfig,
    apiToken: token,
    bearerToken: token
  };
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
  const hubspotSyncWebhookUrl = normalizeConfigUrl(merged.hubspotSyncWebhookUrl || merged.hubspotPartnerSyncWebhookUrl);
  return {
    ...merged,
    dataBucket: normalizeConfigUrl(merged.dataBucket),
    authMethod: normalizeAuthMethod(merged.authMethod),
    bootstrapUrl: normalizeConfigUrl(merged.bootstrapUrl),
    workbookReadUrl: normalizeConfigUrl(merged.workbookReadUrl),
    workbookWriteUrl: normalizeConfigUrl(merged.workbookWriteUrl),
    workbookWriteBridgeUrl: normalizeConfigUrl(merged.workbookWriteBridgeUrl),
    workbookWriteKey: normalizeConfigUrl(merged.workbookWriteKey),
    workbookHistoryWriteBaseUrl: normalizeConfigUrl(merged.workbookHistoryWriteBaseUrl),
    workbookHistoryKeyPrefix: normalizeConfigUrl(merged.workbookHistoryKeyPrefix),
    invoiceDraftUrl: normalizeConfigUrl(merged.invoiceDraftUrl),
    invoiceArtifactWriteUrl: normalizeConfigUrl(merged.invoiceArtifactWriteUrl),
    invoiceArtifactWriteBaseUrl: normalizeConfigUrl(merged.invoiceArtifactWriteBaseUrl),
    privateInvoiceLinkWriteBaseUrl: normalizeConfigUrl(merged.privateInvoiceLinkWriteBaseUrl),
    privateInvoiceLinkReadBaseUrl: normalizeConfigUrl(merged.privateInvoiceLinkReadBaseUrl),
    privateInvoiceLinkSignerUrl: normalizeConfigUrl(merged.privateInvoiceLinkSignerUrl),
    privateInvoiceDownloadUrlTtlSeconds: normalizePositiveInteger(
      merged.privateInvoiceDownloadUrlTtlSeconds || merged.privateInvoiceLinkDefaultTtl,
      DEFAULT_PRIVATE_DOWNLOAD_URL_TTL_SECONDS
    ),
    privateInvoiceLinkExpiresInDays: normalizePositiveInteger(
      merged.privateInvoiceLinkExpiresInDays || merged.privateInvoiceDownloadRetentionDays,
      DEFAULT_PRIVATE_LINK_RETENTION_DAYS
    ),
    privateInvoiceDownloadRetentionDays: normalizePositiveInteger(
      merged.privateInvoiceDownloadRetentionDays || merged.privateInvoiceLinkExpiresInDays,
      DEFAULT_PRIVATE_LINK_RETENTION_DAYS
    ),
    automationOutboxUrl: normalizeConfigUrl(merged.automationOutboxUrl),
    hubspotSyncWebhookUrl,
    hubspotPartnerSyncWebhookUrl: hubspotSyncWebhookUrl,
    qaCheckerSummaryUrl: normalizeConfigUrl(merged.qaCheckerSummaryUrl),
    lookerImportWebhookUrl: normalizeConfigUrl(merged.lookerImportWebhookUrl),
    contractParseWebhookUrl: normalizeConfigUrl(merged.contractParseWebhookUrl),
    contractExtractWebhookUrl: normalizeConfigUrl(merged.contractExtractWebhookUrl),
    apiToken: normalizeConfigUrl(merged.apiToken || merged.bearerToken),
    bearerToken: normalizeConfigUrl(merged.bearerToken || merged.apiToken),
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
  return !!(config.enableSharedWorkbook && (config.workbookWriteUrl || (config.workbookWriteBridgeUrl && config.workbookWriteKey)));
}

export function isRemoteInvoiceReadEnabled() {
  const config = getSharedBackendConfig();
  return !!(config.enableRemoteInvoiceReads && config.invoiceDraftUrl);
}

export function isInvoiceArtifactEnabled() {
  const config = getSharedBackendConfig();
  return !!(config.enableInvoiceArtifacts && (config.invoiceArtifactWriteUrl || config.invoiceArtifactWriteBaseUrl));
}

export function isPrivateInvoiceLinkEnabled() {
  const config = getSharedBackendConfig();
  return !!(config.enablePrivateInvoiceLinks && (config.privateInvoiceLinkSignerUrl || config.privateInvoiceLinkWriteBaseUrl));
}

export function isLookerImportEnabled() {
  const config = getSharedBackendConfig();
  return !!(config.enableLookerImports && config.lookerImportWebhookUrl);
}

export function isHubSpotSyncEnabled() {
  const config = getSharedBackendConfig();
  return !!(config.enableHubSpotSync !== false && config.hubspotSyncWebhookUrl);
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
  const authorization = buildBearerAuthorization(config.apiToken);
  if (authorization) headers.Authorization = authorization;
  return headers;
}

function buildBearerAuthorization(token) {
  const value = normalizeConfigUrl(token);
  if (!value) return "";
  return value.toLowerCase().startsWith("bearer ") ? value : `Bearer ${value}`;
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

// Soft cap under AWS Lambda Function URL's hard 6 MB request-payload limit.
// Anything larger than this uses a presigned PUT so the body goes straight to
// S3 and the Lambda only has to sign the URL -- Stampli's transactions.csv
// alone is ~25 MB, so the inline-JSON path can never work for that file.
const BRIDGE_INLINE_MAX_BODY_BYTES = 4 * 1024 * 1024;

function isArrayBufferLike(value) {
  return typeof ArrayBuffer !== "undefined" && value instanceof ArrayBuffer;
}

function isArrayBufferViewLike(value) {
  return typeof ArrayBuffer !== "undefined" && ArrayBuffer.isView(value);
}

function isBinaryUploadBody(value) {
  return value instanceof Uint8Array || isArrayBufferLike(value) || isArrayBufferViewLike(value);
}

function bodyToUploadBytes(value) {
  if (value instanceof Uint8Array) return value;
  if (isArrayBufferLike(value)) return new Uint8Array(value);
  if (isArrayBufferViewLike(value)) return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
  return new TextEncoder().encode(String(value ?? ""));
}

// Writes a single file to the Veem billing-fe data bucket via the n8n-bridge
// Lambda Function URL. Small bodies ride the inline `action: "write"` path
// (one round-trip). Larger bodies are PUT directly to S3 through a presigned
// URL minted by the bridge so the Lambda body never exceeds 6 MB.
async function writeArtifactFileViaBridge(bridgeUrl, key, body, contentType, options = {}) {
  const binaryBody = isBinaryUploadBody(body);
  const normalizedBody = binaryBody ? "" : String(body ?? "");
  const bytes = bodyToUploadBytes(body);
  const byteLength = bytes.byteLength;
  const contentTypeHeader = String(contentType);

  if (!binaryBody && byteLength <= BRIDGE_INLINE_MAX_BODY_BYTES) {
    const action = {
      action: "write",
      key: String(key),
      contentType: contentTypeHeader,
      encoding: "utf8",
      body: normalizedBody
    };
    // ifAbsent translates to S3 PutObject IfNoneMatch:"*" inside the
    // bridge so the call only succeeds when the key is missing. Used by
    // the boot-time seed-if-missing path; everywhere else this option is
    // left undefined and the call stays a regular overwrite save.
    if (options && options.ifAbsent === true) action.ifAbsent = true;
    return postConfiguredJson(
      bridgeUrl,
      action,
      `Could not save ${key} via the Veem Billing FE bridge.`
    );
  }

  const presign = await postConfiguredJson(
    bridgeUrl,
    {
      action: "presign",
      key: String(key),
      contentType: contentTypeHeader,
      contentLength: byteLength
    },
    `Could not presign upload for ${key}.`
  );
  const putUrl = typeof presign?.url === "string" ? presign.url : "";
  if (!putUrl) {
    throw new Error(`Bridge did not return a presigned URL for ${key}.`);
  }
  const requiredHeaders = (presign && typeof presign.requiredHeaders === "object" && presign.requiredHeaders) || {
    "Content-Type": contentTypeHeader,
    "Content-Length": String(byteLength),
    "x-amz-server-side-encryption": "AES256"
  };
  const response = await fetch(putUrl, {
    method: "PUT",
    headers: requiredHeaders,
    body: bytes
  });
  if (!response.ok) {
    const detail = await response.text().catch(() => "");
    throw new Error(
      `S3 rejected presigned upload for ${key}: ${response.status}${detail ? ` ${detail}` : ""}.`
    );
  }
  return { bucket: presign?.bucket || "", key: String(key) };
}

// Returns a CloudFront signed URL for an object under partner-downloads/. The
// signer Lambda expects only `{key, ttl}`; never forward the invoice artifact
// here, or the Function URL will reject the request with a 413.
async function signPartnerDownloadUrl(signerUrl, key, ttl) {
  const result = await postConfiguredJson(
    signerUrl,
    { key: String(key), ttl: normalizePositiveInteger(ttl, DEFAULT_PRIVATE_DOWNLOAD_URL_TTL_SECONDS) },
    `Could not sign ${key} via the Veem Billing FE signer.`
  );
  return String(
    result?.url
    || result?.signedUrl
    || result?.downloadUrl
    || result?.download_url
    || result?.privateUrl
    || ""
  ).trim();
}

function jsonByteLength(value) {
  return new TextEncoder().encode(JSON.stringify(value || {})).byteLength;
}

function textByteLength(value) {
  return new TextEncoder().encode(String(value ?? "")).byteLength;
}

function sizeLabel(bytes) {
  const value = Number(bytes || 0);
  if (value >= 1024 * 1024) return `${(value / (1024 * 1024)).toFixed(1)} MB`;
  if (value >= 1024) return `${(value / 1024).toFixed(1)} KB`;
  return `${value} bytes`;
}

function chunkText(value, maxChars = INVOICE_ARTIFACT_CHUNK_CHAR_LIMIT) {
  const text = String(value ?? "");
  if (!text) return [""];
  const chunks = [];
  for (let index = 0; index < text.length; index += maxChars) {
    chunks.push(text.slice(index, index + maxChars));
  }
  return chunks;
}

function ensureTrailingSlash(value) {
  const trimmed = normalizeConfigUrl(value);
  if (!trimmed) return "";
  return trimmed.endsWith("/") ? trimmed : `${trimmed}/`;
}

function buildObjectUrl(baseUrl, objectKey) {
  return resolveUrl(`${ensureTrailingSlash(baseUrl)}${String(objectKey || "").replace(/^\/+/, "")}`);
}

function addDaysIso(value, days) {
  const base = value ? new Date(value) : new Date();
  if (Number.isNaN(base.getTime())) return new Date().toISOString();
  base.setUTCDate(base.getUTCDate() + normalizePositiveInteger(days, DEFAULT_PRIVATE_LINK_RETENTION_DAYS));
  return base.toISOString();
}

function addSecondsIso(value, seconds) {
  const base = value ? new Date(value) : new Date();
  if (Number.isNaN(base.getTime())) return new Date().toISOString();
  base.setUTCSeconds(base.getUTCSeconds() + normalizePositiveInteger(seconds, DEFAULT_PRIVATE_DOWNLOAD_URL_TTL_SECONDS));
  return base.toISOString();
}

function buildPartnerDownloadReadUrl(readBaseUrl, token, fileName = "index.html") {
  const base = ensureTrailingSlash(readBaseUrl);
  if (!base || !token) return "";
  return buildObjectUrl(base, `${token}/${fileName}`);
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
  const authorization = buildBearerAuthorization(config.apiToken);
  if (!shouldUseAwsStorageAuth(config) && authorization) {
    headers.Authorization = authorization;
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

function buildPrivateInvoiceIndexHtml({ title, summaryLines, links, signerUrl = "", signedUrlTtlSeconds = DEFAULT_PRIVATE_DOWNLOAD_URL_TTL_SECONDS, linkExpiresAt = "" }) {
  const safeTitle = escapeHtml(title || "Partner Invoice Package");
  const safeSummary = (summaryLines || []).map((line) => `<p>${escapeHtml(line)}</p>`).join("");
  const normalizedTtlSeconds = normalizePositiveInteger(signedUrlTtlSeconds, DEFAULT_PRIVATE_DOWNLOAD_URL_TTL_SECONDS);
  const ttlMinutes = Math.max(1, Math.round(normalizedTtlSeconds / 60));
  const ttlLabel = ttlMinutes === 1 ? "1 minute" : `${ttlMinutes} minutes`;
  const normalizedLinks = (links || []).map((item, index) => ({
    index,
    label: String(item?.label || "Download"),
    fileName: String(item?.fileName || ""),
    key: String(item?.key || ""),
    href: safeLinkHref(item?.href)
  }));
  const safeLinks = normalizedLinks.map((item) => {
    const href = escapeHtml(item.href);
    const label = escapeHtml(item.label || "Download");
    const fileName = escapeHtml(item.fileName || "");
    if (signerUrl && item.key) {
      return `<button type="button" data-download-index="${escapeHtml(String(item.index))}">${label}</button>`;
    }
    return `<a href="${href}"${fileName ? ` download="${fileName}"` : ""}>${label}</a>`;
  }).join("");
  const safeDownloadsJson = JSON.stringify(normalizedLinks).replace(/</g, "\\u003c");
  const safeSignerUrlJson = JSON.stringify(String(signerUrl || "")).replace(/</g, "\\u003c");
  const safeTtlJson = JSON.stringify(normalizedTtlSeconds);
  const safeTtlLabelJson = JSON.stringify(ttlLabel).replace(/</g, "\\u003c");
  const safeExpiry = linkExpiresAt ? `<p>Partner link expires ${escapeHtml(linkExpiresAt)}.</p>` : "";
  const safeNotice = signerUrl
    ? `Each download is protected with a fresh signed URL that expires ${escapeHtml(ttlLabel)} after it is requested.`
    : "Use the links below to download the invoice files and transaction detail export.";
  return `<!doctype html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>${safeTitle}</title>
    <style>
      body { font-family: "Source Sans Pro", system-ui, sans-serif; background: linear-gradient(180deg, #ffffff 0%, #f2f8fa 100%); color: #212f45; margin: 0; padding: 32px; }
      .card { max-width: 760px; margin: 0 auto; background: white; border: 1px solid rgba(33, 47, 69, 0.14); border-radius: 20px; padding: 28px; box-shadow: 0 18px 42px rgba(33, 47, 69, 0.1); }
      h1 { margin: 0 0 12px; font-size: 28px; }
      p, li { line-height: 1.55; }
      .muted { color: #5e6b7f; }
      ul { padding-left: 20px; }
      .notice { margin: 18px 0; padding: 12px 14px; border-radius: 14px; background: #eaf8fa; color: #1c3969; }
      .downloads a,
      .downloads button { display: inline-block; margin: 8px 10px 0 0; padding: 10px 14px; border: 0; border-radius: 999px; text-decoration: none; background: linear-gradient(135deg, #007fe0 0%, #1c3969 100%); color: white; font: inherit; font-weight: 700; cursor: pointer; }
      .downloads button:disabled { cursor: not-allowed; opacity: 0.62; }
      .status { margin-top: 14px; color: #5e6b7f; min-height: 1.4em; }
      .status.error { color: #f00112; }
    </style>
  </head>
  <body>
    <div class="card">
      <h1>${safeTitle}</h1>
      <div class="muted">
        ${safeSummary}
        ${safeExpiry}
      </div>
      <div class="notice">${safeNotice}</div>
      <div class="downloads">
        ${safeLinks}
      </div>
      <div class="status" id="download-status" aria-live="polite"></div>
    </div>
    <script>
      const DOWNLOADS = ${safeDownloadsJson};
      const SIGNER_URL = ${safeSignerUrlJson};
      const SIGNED_URL_TTL_SECONDS = ${safeTtlJson};
      const SIGNED_URL_TTL_LABEL = ${safeTtlLabelJson};
      const STATUS = document.getElementById("download-status");
      let runtimeConfigPromise = null;

      function setStatus(message, isError = false) {
        if (!STATUS) return;
        STATUS.textContent = message || "";
        STATUS.className = isError ? "status error" : "status";
      }

      function loadScript(src) {
        return new Promise((resolve) => {
          const script = document.createElement("script");
          script.src = src;
          script.onload = () => resolve();
          script.onerror = () => resolve();
          document.head.appendChild(script);
        });
      }

      async function loadRuntimeConfig() {
        if (!runtimeConfigPromise) runtimeConfigPromise = loadScript("/app-config-runtime.js");
        await runtimeConfigPromise;
        return window.BILLING_APP_CONFIG || window.VEEM_BILLING_FE_CONFIG || {};
      }

      async function signDownloadUrl(key) {
        if (!SIGNER_URL) throw new Error("The private-link signer is not configured.");
        const config = await loadRuntimeConfig();
        const token = config.bearerToken || config.apiToken || "";
        const headers = { "Content-Type": "application/json", Accept: "application/json" };
        if (token) headers.Authorization = "Bearer " + token;
        const response = await fetch(SIGNER_URL, {
          method: "POST",
          headers,
          body: JSON.stringify({ key, ttl: SIGNED_URL_TTL_SECONDS })
        });
        const text = await response.text();
        let payload = {};
        try { payload = text ? JSON.parse(text) : {}; } catch (error) {}
        if (!response.ok) throw new Error(payload.error || payload.message || text || "Could not create the download URL.");
        const url = payload.url || payload.signedUrl || payload.downloadUrl || payload.download_url || "";
        if (!url) throw new Error("The signer did not return a download URL.");
        return url;
      }

      function clickDownload(url, fileName) {
        const anchor = document.createElement("a");
        anchor.href = url;
        if (fileName) anchor.download = fileName;
        anchor.rel = "noopener";
        document.body.appendChild(anchor);
        anchor.click();
        anchor.remove();
      }

      document.addEventListener("click", async (event) => {
        const button = event.target && event.target.closest ? event.target.closest("[data-download-index]") : null;
        if (!button) return;
        const item = DOWNLOADS[Number(button.dataset.downloadIndex)];
        if (!item) return;
        try {
          button.disabled = true;
          setStatus("Preparing " + item.fileName + "...");
          const url = item.key ? await signDownloadUrl(item.key) : item.href;
          clickDownload(url, item.fileName);
          setStatus(SIGNER_URL ? "Download URL created. It expires in " + SIGNED_URL_TTL_LABEL + "." : "Download started.");
        } catch (error) {
          setStatus(error && error.message ? error.message : "Could not create the download URL.", true);
        } finally {
          button.disabled = false;
        }
      });
    </script>
  </body>
</html>`;
}

// Stamps the data bucket with the bundled bootstrap payload the very
// first time this app loads in a fresh environment. The bridge handler
// honors ifAbsent by setting S3 PutObject IfNoneMatch:"*", so any
// browser arriving after the first seed gets a no-op (HTTP 200 with
// seeded:false). Gated by a localStorage flag so we do not POST 1 MB of
// bootstrap JSON on every page load -- one attempt per browser per
// WORKBOOK_SEED_TTL_MS.
const WORKBOOK_SEED_FLAG_KEY = "__veem_billing_workbook_seed_attempted";
const WORKBOOK_SEED_TTL_MS = 24 * 60 * 60 * 1000;

function workbookSeedAlreadyAttempted() {
  if (typeof localStorage === "undefined") return false;
  try {
    const last = parseInt(localStorage.getItem(WORKBOOK_SEED_FLAG_KEY) || "0", 10);
    if (!Number.isFinite(last) || last <= 0) return false;
    return Date.now() - last < WORKBOOK_SEED_TTL_MS;
  } catch {
    return false;
  }
}

function markWorkbookSeedAttempted() {
  if (typeof localStorage === "undefined") return;
  try { localStorage.setItem(WORKBOOK_SEED_FLAG_KEY, String(Date.now())); } catch {}
}

async function seedSharedWorkbookIfMissing(payload) {
  if (!payload || typeof payload !== "object") return;
  if (workbookSeedAlreadyAttempted()) return;
  const config = getSharedBackendConfig();
  const bridgeUrl = config.workbookWriteBridgeUrl;
  const writeKey = config.workbookWriteKey;
  if (!bridgeUrl || !writeKey) return;

  // Write the bootstrap payload as-is so n8n and other readers see the
  // same bytes the bundled site origin serves. After a real user save
  // lands, the bridge would refuse this seed via ifAbsent, so it stays
  // a one-time event.
  const serialized = JSON.stringify(payload, null, 2);
  try {
    const result = await writeArtifactFileViaBridge(
      bridgeUrl,
      writeKey,
      serialized,
      "application/json",
      { ifAbsent: true }
    );
    // Only suppress retries when the bridge gave a definitive answer:
    //   seeded:true  -> we just wrote it,
    //   seeded:false -> object already existed (412 PreconditionFailed).
    // Anything else (network blip, bridge 5xx, infra not yet deployed,
    // unexpected response shape) leaves the flag unset so the next page
    // load retries -- otherwise a single failed first attempt would lock
    // the browser out of the seed for 24 hours.
    if (result && (result.seeded === true || result.seeded === false)) {
      markWorkbookSeedAttempted();
    }
  } catch (err) {
    if (typeof console !== "undefined" && console.warn) {
      console.warn("[billing-fe] seed-if-missing failed:", err && err.message ? err.message : err);
    }
  }
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
  // Best-effort: if the data bucket is empty (fresh env, no save has
  // landed yet), upload the bundled bootstrap payload so n8n and any
  // other direct-S3 reader has something to read. Idempotent via
  // bridge's ifAbsent->IfNoneMatch.
  seedSharedWorkbookIfMissing(result.payload).catch(() => {});
  return normalizeBootstrapPayload(result.payload);
}

function normalizeQaCheckerReportPayload(payload) {
  if (!payload || typeof payload !== "object") return null;
  const candidates = [
    payload.qaCheckerLatest,
    payload.report,
    payload.data,
    payload.snapshot?.qaCheckerLatest,
    payload
  ];
  return candidates.find((candidate) => {
    if (!candidate || typeof candidate !== "object") return false;
    return !!(candidate.summary || candidate.status || candidate.issues || candidate.runId || candidate.generatedAt);
  }) || null;
}

export async function fetchLatestQaCheckerReport({ retries = 1, retryDelayMs = 350 } = {}) {
  const config = getSharedBackendConfig();
  if (!config.qaCheckerSummaryUrl) return null;
  let lastError = null;
  for (let attempt = 0; attempt <= retries; attempt += 1) {
    const controller = typeof AbortController !== "undefined" ? new AbortController() : null;
    const timeout = controller ? globalThis.setTimeout(() => controller.abort(), QA_STATUS_FETCH_TIMEOUT_MS) : null;
    try {
      const payload = await parseApiResponse(
        await fetch(resolveUrl(config.qaCheckerSummaryUrl, { ts: Date.now() }), {
          method: "GET",
          headers: { Accept: "application/json" },
          cache: "no-store",
          signal: controller?.signal
        }),
        "Could not load the latest QA checker result."
      );
      return normalizeQaCheckerReportPayload(payload);
    } catch (error) {
      lastError = error;
      if (attempt >= retries) break;
      await sleep(retryDelayMs * (attempt + 1));
    } finally {
      if (timeout) globalThis.clearTimeout(timeout);
    }
  }
  throw lastError || new Error("Could not load the latest QA checker result.");
}

export async function saveSharedWorkbookSnapshot(snapshot) {
  const config = getSharedBackendConfig();
  const writeUrl = getSharedWorkbookWriteUrl();
  const bridgeUrl = config.workbookWriteBridgeUrl;
  const writeKey = config.workbookWriteKey;
  if (!writeUrl && !(bridgeUrl && writeKey)) {
    throw new Error("Shared workbook saving is not configured. Set BILLING_APP_CONFIG.workbookWriteUrl or workbookWriteBridgeUrl/workbookWriteKey.");
  }
  const savedAt = snapshot?._saved || new Date().toISOString();
  const serialized = JSON.stringify({
    workspaceLabel: getWorkspaceLabel(),
    snapshot,
    savedAt,
    mode: "workbook_save"
  }, null, 2);
  if (bridgeUrl && writeKey) {
    await writeArtifactFileViaBridge(bridgeUrl, writeKey, serialized, "application/json");
    let historyUrl = "";
    const historyPrefix = String(config.workbookHistoryKeyPrefix || "").replace(/^\/+/, "").replace(/\/?$/, "/");
    if (historyPrefix) {
      const historyKey = `${historyPrefix}workbook-${savedAt.replaceAll(":", "-").replaceAll(".", "-")}.json`;
      await writeArtifactFileViaBridge(bridgeUrl, historyKey, serialized, "application/json");
      historyUrl = historyKey;
    }
    sharedWorkbookState.etag = "";
    return {
      savedAt,
      url: config.workbookReadUrl || writeKey,
      historyUrl,
      etag: ""
    };
  }
  const writeResult = await putConfiguredContent(
    writeUrl,
    serialized,
    "application/json",
    "Could not save the shared workbook.",
    { ifMatch: sharedWorkbookState.etag }
  );
  sharedWorkbookState.etag = writeResult.etag || "";
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

function buildInvoiceArtifactTarget(config) {
  return {
    bucket: config.dataBucket || "",
    prefix: "artifacts/invoices",
    privateDownloadPrefix: "partner-downloads",
    privateDownloadReadBaseUrl: config.privateInvoiceLinkReadBaseUrl || ""
  };
}

function getInvoiceDocumentHtmlFileName(doc) {
  return doc?.fileName || `${doc?.kind || "invoice"}.html`;
}

function getInvoiceDocumentPdfFileName(doc) {
  const htmlFileName = getInvoiceDocumentHtmlFileName(doc);
  return doc?.pdfFileName || htmlFileName.replace(/\.html?$/i, ".pdf") || `${doc?.kind || "invoice"}.pdf`;
}

function getInvoiceDocumentPdfBody(doc) {
  if (isBinaryUploadBody(doc?.pdfBytes)) return doc.pdfBytes;
  const base64 = String(doc?.pdfBase64 || "").trim();
  if (!base64 || typeof atob !== "function") return null;
  const binary = atob(base64);
  const bytes = new Uint8Array(binary.length);
  for (let index = 0; index < binary.length; index += 1) {
    bytes[index] = binary.charCodeAt(index);
  }
  return bytes;
}

function buildInvoiceArtifactDocumentManifest(doc) {
  const fileName = getInvoiceDocumentHtmlFileName(doc);
  const pdfFileName = getInvoiceDocumentPdfFileName(doc);
  return {
    kind: doc?.kind || "",
    title: doc?.title || "",
    amountDue: Number(doc?.amountDue || 0),
    fileName,
    htmlFileName: fileName,
    pdfFileName
  };
}

function buildInvoiceArtifactManifest(payload, { artifactId = "", documents = null, transactionsUrl = "" } = {}) {
  const transactionFileName = payload?.transactions?.fileName || "transactions.csv";
  const documentEntries = documents || (Array.isArray(payload?.documents) ? payload.documents.map(buildInvoiceArtifactDocumentManifest) : []);
  return {
    artifactId: artifactId || String(payload?.bundleKey || `invoice-${Date.now()}`),
    savedAt: payload?.generatedAt || new Date().toISOString(),
    generatedAt: payload?.generatedAt || "",
    bundleKey: payload?.bundleKey || "",
    partner: payload?.partner || "",
    period: payload?.period || "",
    periodStart: payload?.periodStart || "",
    periodEnd: payload?.periodEnd || "",
    periodKey: payload?.periodKey || "",
    periodLabel: payload?.periodLabel || "",
    periodDateRange: payload?.periodDateRange || "",
    summary: payload?.summary || {},
    workspace: payload?.workspace || {},
    actor: payload?.actor || {},
    invoiceSummary: payload?.invoiceSummary || {},
    warnings: Array.isArray(payload?.warnings) ? payload.warnings : [],
    documents: documentEntries,
    transactions: {
      fileName: transactionFileName,
      rowCount: Number(payload?.transactions?.rowCount || 0),
      url: transactionsUrl || ""
    }
  };
}

function buildInvoiceArtifactFiles(payload, artifactId) {
  const files = [];
  const documentEntries = Array.isArray(payload?.documents) ? payload.documents : [];
  documentEntries.forEach((doc) => {
    const fileName = getInvoiceDocumentHtmlFileName(doc);
    const content = String(doc?.pdfHtml || "");
    files.push({
      role: doc?.kind === "payable" ? "ap_invoice" : "ar_invoice",
      kind: doc?.kind || "",
      title: doc?.title || "",
      amountDue: Number(doc?.amountDue || 0),
      fileName,
      contentType: "text/html;charset=utf-8",
      content,
      byteLength: textByteLength(content)
    });
  });
  const transactionFileName = payload?.transactions?.fileName || `${artifactId || "invoice"}-transactions.csv`;
  const transactionContent = String(payload?.transactions?.csvText || "");
  files.push({
    role: "transactions",
    fileName: transactionFileName,
    contentType: "text/csv;charset=utf-8",
    content: transactionContent,
    byteLength: textByteLength(transactionContent),
    rowCount: Number(payload?.transactions?.rowCount || 0)
  });
  return files;
}

function stripInvoiceArtifactFileContents(payload) {
  if (!payload) return null;
  return {
    mode: payload.mode || "invoice_artifact",
    trigger: payload.trigger || "",
    generatedAt: payload.generatedAt || "",
    bundleKey: payload.bundleKey || "",
    partner: payload.partner || "",
    period: payload.period || "",
    periodStart: payload.periodStart || "",
    periodEnd: payload.periodEnd || "",
    periodKey: payload.periodKey || "",
    periodLabel: payload.periodLabel || "",
    periodDateRange: payload.periodDateRange || "",
    summary: payload.summary || {},
    workspace: payload.workspace || {},
    actor: payload.actor || {},
    invoiceSummary: payload.invoiceSummary || {},
    warnings: Array.isArray(payload.warnings) ? payload.warnings : [],
    documents: Array.isArray(payload.documents) ? payload.documents.map(buildInvoiceArtifactDocumentManifest) : [],
    transactions: {
      fileName: payload.transactions?.fileName || "transactions.csv",
      rowCount: Number(payload.transactions?.rowCount || 0)
    }
  };
}

async function postInvoiceArtifactInChunks(config, payload, artifactId, target, fallbackMessage) {
  const files = buildInvoiceArtifactFiles(payload, artifactId);
  const manifest = buildInvoiceArtifactManifest(payload, {
    artifactId,
    documents: files
      .filter((file) => file.role === "ar_invoice" || file.role === "ap_invoice")
      .map((file) => ({
        kind: file.kind,
        title: file.title,
        amountDue: file.amountDue,
        fileName: file.fileName
      }))
  });
  files.push({
    role: "manifest",
    fileName: "manifest.json",
    contentType: "application/json",
    content: JSON.stringify(manifest, null, 2),
    byteLength: textByteLength(JSON.stringify(manifest, null, 2))
  });
  const beginPayload = {
    mode: "invoice_artifact_upload_begin",
    action: "begin_invoice_artifact_upload",
    artifactId,
    target,
    manifest,
    files: files.map(({ content, ...file }) => file)
  };
  const beginSize = jsonByteLength(beginPayload);
  if (beginSize > LAMBDA_FUNCTION_URL_SAFE_JSON_BYTES) {
    throw new Error(`Invoice package metadata is ${sizeLabel(beginSize)}, above the Lambda Function URL safe payload size. The write endpoint must create S3 upload URLs or read source files server-side.`);
  }
  const beginResult = await postConfiguredJson(config.invoiceArtifactWriteUrl, beginPayload, fallbackMessage);
  const uploadId = String(beginResult?.uploadId || beginResult?.id || artifactId);

  for (const file of files) {
    const chunks = chunkText(file.content);
    for (let index = 0; index < chunks.length; index += 1) {
      const chunkPayload = {
        mode: "invoice_artifact_upload_chunk",
        action: "put_invoice_artifact_chunk",
        artifactId,
        uploadId,
        target,
        file: {
          role: file.role,
          kind: file.kind || "",
          title: file.title || "",
          amountDue: Number(file.amountDue || 0),
          fileName: file.fileName,
          contentType: file.contentType,
          byteLength: file.byteLength,
          rowCount: Number(file.rowCount || 0)
        },
        chunkIndex: index,
        chunkCount: chunks.length,
        chunk: chunks[index]
      };
      const chunkSize = jsonByteLength(chunkPayload);
      if (chunkSize > LAMBDA_FUNCTION_URL_SAFE_JSON_BYTES) {
        throw new Error(`Invoice package chunk ${file.fileName} #${index + 1} is ${sizeLabel(chunkSize)}, above the Lambda Function URL safe payload size.`);
      }
      await postConfiguredJson(config.invoiceArtifactWriteUrl, chunkPayload, fallbackMessage);
    }
  }

  const completePayload = {
    mode: "invoice_artifact_upload_complete",
    action: "complete_invoice_artifact_upload",
    artifactId,
    uploadId,
    target,
    manifest
  };
  const result = await postConfiguredJson(config.invoiceArtifactWriteUrl, completePayload, fallbackMessage);
  return {
    artifactId: result?.artifactId || result?.id || artifactId,
    savedAt: result?.savedAt || payload?.generatedAt || new Date().toISOString(),
    manifestUrl: result?.manifestUrl || result?.manifest?.url || "",
    transactionsUrl: result?.transactionsUrl || result?.transactions?.url || "",
    documents: Array.isArray(result?.documents) ? result.documents : [],
    fileCount: Number(result?.fileCount || result?.documents?.length || files.length),
    ...result
  };
}

export async function saveInvoiceArtifact(payload) {
  const config = getSharedBackendConfig();
  if (config.invoiceArtifactWriteUrl) {
    // The Veem billing-fe bridge Lambda is a single-file writer, not an
    // orchestrator: split the artifact client-side into one bridge call per
    // file so the payload of each call stays well below the AWS Function
    // URL 6 MB cap (the previous single mode-based POST shipped hundreds of
    // MB and came back as RequestEntityTooLargeException / 413).
    const artifactId = String(payload?.bundleKey || `invoice-${Date.now()}`);
    const artifactPrefix = `artifacts/invoices/${artifactId}`;
    const readBase = ensureTrailingSlash(config.invoiceArtifactWriteBaseUrl || "");
    const readUrlFor = (fileName) => readBase ? `${readBase}${artifactId}/${fileName}` : "";

    const documentEntries = Array.isArray(payload?.documents) ? payload.documents : [];
    const savedDocuments = [];
    for (const doc of documentEntries) {
      const fileName = getInvoiceDocumentHtmlFileName(doc);
      const pdfFileName = getInvoiceDocumentPdfFileName(doc);
      await writeArtifactFileViaBridge(
        config.invoiceArtifactWriteUrl,
        `${artifactPrefix}/${fileName}`,
        doc?.pdfHtml || "",
        "text/html;charset=utf-8"
      );
      const pdfBody = getInvoiceDocumentPdfBody(doc);
      if (pdfBody) {
        await writeArtifactFileViaBridge(
          config.invoiceArtifactWriteUrl,
          `${artifactPrefix}/${pdfFileName}`,
          pdfBody,
          "application/pdf"
        );
      }
      savedDocuments.push({
        kind: doc?.kind || "",
        title: doc?.title || "",
        amountDue: Number(doc?.amountDue || 0),
        fileName,
        url: readUrlFor(fileName),
        htmlFileName: fileName,
        htmlUrl: readUrlFor(fileName),
        pdfFileName,
        pdfUrl: pdfBody ? readUrlFor(pdfFileName) : ""
      });
    }

    const transactionFileName = payload?.transactions?.fileName || "transactions.csv";
    await writeArtifactFileViaBridge(
      config.invoiceArtifactWriteUrl,
      `${artifactPrefix}/${transactionFileName}`,
      payload?.transactions?.csvText || "",
      "text/csv;charset=utf-8"
    );
    const transactionsUrl = readUrlFor(transactionFileName);

    const savedAt = payload?.generatedAt || new Date().toISOString();
    const manifest = {
      artifactId,
      savedAt,
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
    await writeArtifactFileViaBridge(
      config.invoiceArtifactWriteUrl,
      `${artifactPrefix}/manifest.json`,
      JSON.stringify(manifest, null, 2),
      "application/json"
    );
    const manifestUrl = readUrlFor("manifest.json");

    return {
      artifactId,
      savedAt,
      manifestUrl,
      transactionsUrl,
      documents: savedDocuments,
      fileCount: savedDocuments.reduce((sum, doc) => sum + 1 + (doc.pdfUrl ? 1 : 0), 0) + 2
    };
  }
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
    const fileName = getInvoiceDocumentHtmlFileName(doc);
    const pdfFileName = getInvoiceDocumentPdfFileName(doc);
    const targetUrl = buildObjectUrl(basePath, `${artifactId}/${fileName}`);
    await putConfiguredContent(targetUrl, String(doc?.pdfHtml || ""), "text/html;charset=utf-8", "Could not save the invoice document.");
    const pdfBody = getInvoiceDocumentPdfBody(doc);
    let pdfUrl = "";
    if (pdfBody) {
      pdfUrl = buildObjectUrl(basePath, `${artifactId}/${pdfFileName}`);
      await putConfiguredContent(pdfUrl, pdfBody, "application/pdf", "Could not save the invoice PDF.");
    }
    savedDocuments.push({
      kind: doc?.kind || "",
      title: doc?.title || "",
      amountDue: Number(doc?.amountDue || 0),
      fileName,
      url: targetUrl,
      htmlFileName: fileName,
      htmlUrl: targetUrl,
      pdfFileName,
      pdfUrl
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
    fileCount: savedDocuments.reduce((sum, doc) => sum + 1 + (doc.pdfUrl ? 1 : 0), 0) + 2
  };
}

export async function generatePrivateInvoiceLink(payload) {
  const config = getSharedBackendConfig();

  // Preferred path for the Veem billing-fe stack: the bridge Lambda writes
  // each file under partner-downloads/<token>/ and the signer Lambda mints
  // one CloudFront signed URL per key. The signer only needs `{key, ttl}`;
  // never forward the artifact or document bodies or it will reject with a
  // 413. Signing each URL individually also means every link embedded in
  // the generated index.html works under CloudFront's trusted_key_groups.
  if (config.privateInvoiceLinkSignerUrl && config.invoiceArtifactWriteUrl) {
    const token = payload?.token || createOpaqueToken(28);
    const savedAt = payload?.requestedAt || new Date().toISOString();
    const writePrefix = `partner-downloads/${token}`;
    const signedUrlTtlSeconds = normalizePositiveInteger(
      payload?.downloadUrlTtlSeconds || payload?.ttl || config.privateInvoiceDownloadUrlTtlSeconds || config.privateInvoiceLinkDefaultTtl,
      DEFAULT_PRIVATE_DOWNLOAD_URL_TTL_SECONDS
    );
    const retentionDays = normalizePositiveInteger(
      payload?.partnerLinkExpiresInDays || config.privateInvoiceLinkExpiresInDays,
      DEFAULT_PRIVATE_LINK_RETENTION_DAYS
    );
    const linkExpiresAt = payload?.linkExpiresAt || addDaysIso(savedAt, retentionDays);

    const bridgeUrl = config.invoiceArtifactWriteUrl;
    const signerUrl = config.privateInvoiceLinkSignerUrl;

    const writeFile = async (fileName, body, contentType) => {
      const key = `${writePrefix}/${fileName}`;
      await writeArtifactFileViaBridge(bridgeUrl, key, body, contentType);
      return { fileName, key };
    };

    const documentEntries = Array.isArray(payload?.invoiceArtifact?.documents) ? payload.invoiceArtifact.documents : [];
    const savedDocuments = [];
    for (const doc of documentEntries) {
      const pdfBody = getInvoiceDocumentPdfBody(doc);
      const fileName = pdfBody ? getInvoiceDocumentPdfFileName(doc) : getInvoiceDocumentHtmlFileName(doc);
      const written = await writeFile(
        fileName,
        pdfBody || doc?.pdfHtml || "",
        pdfBody ? "application/pdf" : "text/html;charset=utf-8"
      );
      savedDocuments.push({
        kind: doc?.kind || "",
        title: doc?.title || "",
        amountDue: Number(doc?.amountDue || 0),
        fileName,
        key: written.key,
        url: buildPartnerDownloadReadUrl(config.privateInvoiceLinkReadBaseUrl, token, fileName)
      });
    }

    const transactionFileName = payload?.invoiceArtifact?.transactions?.fileName || "transactions.csv";
    const transactionsWrite = await writeFile(
      transactionFileName,
      payload?.invoiceArtifact?.transactions?.csvText || "",
      "text/csv;charset=utf-8"
    );
    const transactionsReadUrl = buildPartnerDownloadReadUrl(config.privateInvoiceLinkReadBaseUrl, token, transactionFileName);

    const manifest = {
      token,
      savedAt,
      linkExpiresAt,
      retentionDays,
      signedUrlTtlSeconds,
      signedUrlExpiresAt: addSecondsIso(savedAt, signedUrlTtlSeconds),
      partner: payload?.partner || payload?.invoiceArtifact?.partner || "",
      period: payload?.period || payload?.invoiceArtifact?.period || "",
      periodStart: payload?.periodStart || payload?.invoiceArtifact?.periodStart || "",
      periodEnd: payload?.periodEnd || payload?.invoiceArtifact?.periodEnd || "",
      summary: payload?.invoiceArtifact?.summary || {},
      archivedArtifact: payload?.archivedArtifact || null,
      documents: savedDocuments,
      transactions: {
        fileName: transactionFileName,
        key: transactionsWrite.key,
        rowCount: Number(payload?.invoiceArtifact?.transactions?.rowCount || 0),
        url: transactionsReadUrl
      }
    };
    const manifestWrite = await writeFile(
      "manifest.json",
      JSON.stringify(manifest, null, 2),
      "application/json"
    );
    const manifestReadUrl = buildPartnerDownloadReadUrl(config.privateInvoiceLinkReadBaseUrl, token, "manifest.json");

    const summaryLines = [
      `${manifest.partner} · ${manifest.periodStart === manifest.periodEnd ? manifest.periodStart : `${manifest.periodStart} to ${manifest.periodEnd}`}`,
      `Generated ${savedAt}`,
      "This delivery package contains the invoice files and transaction detail export."
    ];
    const indexHtml = buildPrivateInvoiceIndexHtml({
      title: `${manifest.partner} Billing Package`,
      summaryLines,
      links: [
        ...savedDocuments.map((doc) => ({
          label: doc.kind === "receivable" ? "Download AR Invoice PDF" : "Download AP Invoice PDF",
          key: doc.key,
          href: doc.url,
          fileName: doc.fileName
        })),
        { label: "Download Transactions CSV", key: transactionsWrite.key, href: transactionsReadUrl, fileName: transactionFileName }
      ],
      signerUrl,
      signedUrlTtlSeconds,
      linkExpiresAt
    });
    const indexWrite = await writeFile("index.html", indexHtml, "text/html;charset=utf-8");
    const signedIndexUrl = await signPartnerDownloadUrl(signerUrl, indexWrite.key, signedUrlTtlSeconds);
    const durableIndexUrl = buildPartnerDownloadReadUrl(config.privateInvoiceLinkReadBaseUrl, token, "index.html");

    return {
      token,
      savedAt,
      linkExpiresAt,
      retentionDays,
      signedUrlTtlSeconds,
      signedPrivateUrl: signedIndexUrl,
      privateUrl: signedIndexUrl || durableIndexUrl,
      manifestUrl: manifestReadUrl,
      transactionsUrl: transactionsReadUrl,
      documents: savedDocuments,
      fileCount: savedDocuments.length + 3
    };
  }

  if (config.privateInvoiceLinkSignerUrl) {
    const archivedArtifact = payload?.archivedArtifact || null;
    const savedAt = payload?.requestedAt || new Date().toISOString();
    const signedUrlTtlSeconds = normalizePositiveInteger(
      payload?.downloadUrlTtlSeconds || payload?.ttl || config.privateInvoiceDownloadUrlTtlSeconds || config.privateInvoiceLinkDefaultTtl,
      DEFAULT_PRIVATE_DOWNLOAD_URL_TTL_SECONDS
    );
    const retentionDays = normalizePositiveInteger(
      payload?.partnerLinkExpiresInDays || config.privateInvoiceLinkExpiresInDays,
      DEFAULT_PRIVATE_LINK_RETENTION_DAYS
    );
    const linkExpiresAt = payload?.linkExpiresAt || addDaysIso(savedAt, retentionDays);
    const requestPayload = {
      ...payload,
      mode: "invoice_private_link",
      action: "generate_private_invoice_link",
      requestedAt: savedAt,
      downloadUrlTtlSeconds: signedUrlTtlSeconds,
      partnerLinkExpiresInDays: retentionDays,
      linkExpiresAt,
      readBaseUrl: config.privateInvoiceLinkReadBaseUrl || "",
      artifactId: archivedArtifact?.artifactId || archivedArtifact?.id || payload?.invoiceArtifact?.bundleKey || "",
      archivedArtifact,
      invoiceArtifact: stripInvoiceArtifactFileContents(payload?.invoiceArtifact)
    };
    const requestSize = jsonByteLength(requestPayload);
    if (requestSize > LAMBDA_FUNCTION_URL_SAFE_JSON_BYTES) {
      throw new Error(`Private-link request is ${sizeLabel(requestSize)}, above the Lambda Function URL safe payload size. Save the invoice package first and send only the artifact ID/manifest URLs to the signer.`);
    }
    return postConfiguredJson(
      config.privateInvoiceLinkSignerUrl,
      requestPayload,
      "Could not generate the private invoice link."
    );
  }
  if (!config.privateInvoiceLinkWriteBaseUrl) {
    throw new Error("Private invoice links are not configured.");
  }

  const token = payload?.token || createOpaqueToken(28);
  const writeBase = ensureTrailingSlash(config.privateInvoiceLinkWriteBaseUrl);
  const readBase = ensureTrailingSlash(config.privateInvoiceLinkReadBaseUrl || config.privateInvoiceLinkWriteBaseUrl);
  const savedAt = payload?.requestedAt || new Date().toISOString();
  const signedUrlTtlSeconds = normalizePositiveInteger(
    payload?.downloadUrlTtlSeconds || payload?.ttl || config.privateInvoiceDownloadUrlTtlSeconds || config.privateInvoiceLinkDefaultTtl,
    DEFAULT_PRIVATE_DOWNLOAD_URL_TTL_SECONDS
  );
  const retentionDays = normalizePositiveInteger(
    payload?.partnerLinkExpiresInDays || config.privateInvoiceLinkExpiresInDays,
    DEFAULT_PRIVATE_LINK_RETENTION_DAYS
  );
  const linkExpiresAt = payload?.linkExpiresAt || addDaysIso(savedAt, retentionDays);
  const documentEntries = Array.isArray(payload?.invoiceArtifact?.documents) ? payload.invoiceArtifact.documents : [];
  const savedDocuments = [];

  for (const doc of documentEntries) {
    const pdfBody = getInvoiceDocumentPdfBody(doc);
    const fileName = pdfBody ? getInvoiceDocumentPdfFileName(doc) : getInvoiceDocumentHtmlFileName(doc);
    const writeUrl = buildObjectUrl(writeBase, `${token}/${fileName}`);
    const readUrl = buildObjectUrl(readBase, `${token}/${fileName}`);
    await putConfiguredContent(
      writeUrl,
      pdfBody || String(doc?.pdfHtml || ""),
      pdfBody ? "application/pdf" : "text/html;charset=utf-8",
      "Could not save the private invoice document."
    );
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
    linkExpiresAt,
    retentionDays,
    signedUrlTtlSeconds,
    signedUrlExpiresAt: addSecondsIso(savedAt, signedUrlTtlSeconds),
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
      ...savedDocuments.map((doc) => ({
        label: doc.kind === "receivable" ? "Download AR Invoice PDF" : "Download AP Invoice PDF",
        href: doc.url,
        fileName: doc.fileName
      })),
      { label: "Download Transactions CSV", href: transactionsReadUrl, fileName: transactionFileName }
    ],
    signedUrlTtlSeconds,
    linkExpiresAt
  });
  const indexWriteUrl = buildObjectUrl(writeBase, `${token}/index.html`);
  const indexReadUrl = buildObjectUrl(readBase, `${token}/index.html`);
  await putConfiguredContent(indexWriteUrl, indexHtml, "text/html;charset=utf-8", "Could not save the private invoice landing page.");

  return {
    token,
    savedAt,
    linkExpiresAt,
    retentionDays,
    signedUrlTtlSeconds,
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

export async function syncHubSpotPartnerProfiles(payload) {
  const config = getSharedBackendConfig();
  if (!config.hubspotSyncWebhookUrl) {
    throw new Error("HubSpot sync is not configured. Connect BILLING_APP_CONFIG.hubspotSyncWebhookUrl to an n8n production webhook.");
  }
  return postConfiguredJson(config.hubspotSyncWebhookUrl, payload, "Could not sync HubSpot partner profiles.");
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

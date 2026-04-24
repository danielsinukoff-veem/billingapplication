import {
  ALL_CCYS,
  CONTRACT_PROMPT,
  CORRIDORS,
  LOOKER_DETAIL_MANIFEST,
  LOOKER_IMPORT_PERIOD,
  MAJORS,
  MINORS,
  PRODUCT_TYPES,
  TERTIARY,
  createInitialWorkbookData,
  getCorridor
} from "./data.js";
import {
  fetchSharedDraftInvoice,
  generatePrivateInvoiceLink,
  getSharedBackendConfig,
  fetchBillingCheckerReport,
  extractContractText,
  getWorkspaceLabel,
  importLookerFileAndSave,
  isBillingAuthRedirectError,
  isBillingCheckerEnabled,
  isContractExtractEnabled,
  isInvoiceArtifactEnabled,
  isContractParseEnabled,
  isLookerImportEnabled,
  isPrivateInvoiceLinkEnabled,
  isRemoteInvoiceReadEnabled,
  isSharedWorkbookEnabled,
  isSharedWorkbookWriteEnabled,
  loadSharedBootstrap,
  parseContractText,
  saveInvoiceArtifact,
  saveSharedWorkbookSnapshot
} from "./shared-backend.js";

const STORAGE_KEY = "billing-workbook-data";
const ACCESS_SESSION_KEY = "billing-workbook-access-session";
const STORAGE_VERSION = 35;

// Feed markers for dedicated Stampli USD Abroad / credit-complete Looker feeds that
// were confirmed wrong on 2026-04-21 and must never contribute to the calc. The
// authoritative source for those transactions is partner_offline_billing (plus
// partner_offline_billing_reversals). See docs/calc-coverage-report-2026-04-21.md.
// Any ltxn row carrying one of these markers is purged on ingest, purged on
// snapshot migration, and defensively filtered out again at calc time.
const UNTRUSTED_DIRECT_INVOICE_SOURCES = new Set([
  "stampli_credit_complete_billing",
  "stampli_direct_billing",
  "stampli_usd_abroad_revenue",
  "stampli_usd_abroad_reversal"
]);
function isUntrustedDirectInvoiceRow(row) {
  return !!row && UNTRUSTED_DIRECT_INVOICE_SOURCES.has(row.directInvoiceSource);
}
function stripUntrustedDirectInvoiceRows(rows) {
  if (!Array.isArray(rows)) return rows;
  return rows.filter((row) => !isUntrustedDirectInvoiceRow(row));
}
const SAVE_DELAY_MS = 1200;
const ADMIN_USERNAME = "VeemAdmin";
const ADMIN_PASSWORD = "VeemBilling123$";
const DEFAULT_REVERSAL_FEE_PER_TXN = 2.5;
const DEFAULT_ADMIN_SETTINGS = Object.freeze({
  guestAllowedTabs: ["invoice", "partner", "rates", "looker", "costs", "import"],
  guestAccessCustomized: false
});

const root = document.getElementById("app");
let sharedWorkspaceRefreshPromise = null;
let sharedWorkspaceRetryTimer = null;

function scheduleSharedWorkspaceRetry(delayMs = 1500) {
  if (sharedWorkspaceRetryTimer) return;
  sharedWorkspaceRetryTimer = window.setTimeout(() => {
    sharedWorkspaceRetryTimer = null;
    void refreshSharedWorkspace({ showSuccessToast: false, showErrorToast: false });
  }, delayMs);
}

function renderFatalAppError(error) {
  if (isBillingAuthRedirectError(error)) return;
  const message = error instanceof Error ? error.message : String(error || "Unknown app error");
  const escaped = message
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
  console.error("Billing workbook fatal error", error);
  if (!root) return;
  root.innerHTML = `
    <div style="max-width:960px;margin:48px auto;padding:24px;border:1px solid #e4d8c7;border-radius:24px;background:#fffaf2;font-family:system-ui,sans-serif;color:#382f2a;">
      <h1 style="margin:0 0 12px;font-size:28px;">Billing app could not load</h1>
      <p style="margin:0 0 12px;font-size:16px;line-height:1.5;">A frontend error stopped the workbook from rendering. The message below is the live browser error so we can fix it quickly.</p>
      <pre style="margin:0;padding:16px;border-radius:16px;background:#f5eee1;white-space:pre-wrap;word-break:break-word;font-size:14px;">${escaped}</pre>
    </div>
  `;
}

const uid = () => Math.random().toString(36).slice(2, 8);
const fmt = (n) => (n == null || Number.isNaN(Number(n)) ? "$0.00" : "$" + Number(n).toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 }));
const fmtPct = (n) => (Number(n) * 100).toFixed(4) + "%";
const inRange = (d, s, e) => {
  if (!d || !s) return true;
  const dd = new Date(d);
  const ds = new Date(s);
  if (dd < ds) return false;
  if (e) {
    if (dd > new Date(e)) return false;
  }
  return true;
};
const norm = (value) => String(value ?? "").trim().toLowerCase();
const optionalMatch = (ruleValue, actualValue) => !ruleValue || norm(ruleValue) === norm(actualValue);
const EEA_COUNTRY_TOKENS = new Set([
  "at", "austria", "be", "belgium", "bg", "bulgaria", "hr", "croatia", "cy", "cyprus",
  "cz", "czechrepublic", "dk", "denmark", "ee", "estonia", "fi", "finland", "fr", "france",
  "de", "germany", "gr", "greece", "hu", "hungary", "is", "iceland", "ie", "ireland",
  "it", "italy", "lv", "latvia", "li", "liechtenstein", "lt", "lithuania", "lu", "luxembourg",
  "mt", "malta", "nl", "netherlands", "no", "norway", "pl", "poland", "pt", "portugal",
  "ro", "romania", "sk", "slovakia", "si", "slovenia", "es", "spain", "se", "sweden"
]);
const COUNTRY_GROUP_TOKENS = {
  CA: new Set(["ca", "canada"]),
  UK: new Set(["uk", "gb", "gbr", "unitedkingdom", "greatbritain", "england", "scotland", "wales", "northernireland"]),
  AU: new Set(["au", "aus", "australia"]),
  US: new Set(["us", "usa", "unitedstates", "unitedstatesofamerica"]),
  EEA: EEA_COUNTRY_TOKENS
};
const normalizeCountryToken = (value) => norm(value).replace(/[^a-z0-9]/g, "");
const countryInGroup = (actualValue, groupValue) => {
  const token = normalizeCountryToken(actualValue);
  const group = String(groupValue || "").trim().toUpperCase();
  if (!token || !group) return false;
  return (COUNTRY_GROUP_TOKENS[group] || new Set([normalizeCountryToken(group)])).has(token);
};
const optionalCountryMatch = (ruleCountry, ruleGroup, actualCountry) => {
  if (String(ruleCountry || "").trim()) return optionalMatch(ruleCountry, actualCountry);
  if (String(ruleGroup || "").trim()) return countryInGroup(actualCountry, ruleGroup);
  return true;
};
const isCrossBorderTransaction = (txn) => {
  const txnType = norm(txn?.txnType);
  if (["fx", "usd abroad", "payout"].includes(txnType)) return true;
  const payerCountry = String(txn?.payerCountry || "").toUpperCase().trim();
  const payeeCountry = String(txn?.payeeCountry || "").toUpperCase().trim();
  if (payerCountry && payeeCountry && payerCountry !== payeeCountry) return true;
  const payerCcy = String(txn?.payerCcy || "").toUpperCase().trim();
  const payeeCcy = String(txn?.payeeCcy || "").toUpperCase().trim();
  return Boolean(payerCcy && payeeCcy && payerCcy !== payeeCcy);
};
const isRevSharePayinTxn = (txn) => norm(txn?.txnType) === "payin";
const isRevSharePayoutTxn = (txn) => !isRevSharePayinTxn(txn);
const isFxConversionTransaction = (txn) => {
  const txnType = norm(txn?.txnType);
  if (txnType === "fx") return true;
  const payerCcy = String(txn?.payerCcy || "").toUpperCase().trim();
  const payeeCcy = String(txn?.payeeCcy || "").toUpperCase().trim();
  return Boolean(payerCcy && payeeCcy && payerCcy !== payeeCcy);
};
const getRevShareDirection = (txn) => norm(txn?.txnType) === "payin" ? "In" : "Out";
const revShareScopeMatches = (share, txn) => {
  const shareType = norm(share?.txnType);
  if (isRevSharePayinTxn(txn)) return false;
  let typeMatch;
  if (shareType === "payin") typeMatch = false;
  else if (shareType === "payout") typeMatch = isRevSharePayoutTxn(txn);
  else if (!shareType) typeMatch = isRevSharePayoutTxn(txn);
  else typeMatch = optionalMatch(share?.txnType, txn?.txnType);
  return typeMatch && optionalMatch(share?.speedFlag, txn?.speedFlag);
};
const txnAverageSize = (txn) => {
  const avgSize = Number(txn?.avgTxnSize || 0);
  if (avgSize > 0) return avgSize;
  const txnCount = Number(txn?.txnCount || 0);
  const totalVolume = Number(txn?.totalVolume || 0);
  return txnCount > 0 ? totalVolume / txnCount : 0;
};
const revShareCostTokens = (txn) => {
  const txnType = norm(txn?.txnType);
  const processingMethod = norm(txn?.processingMethod);
  const speedFlag = norm(txn?.speedFlag);
  if (txnType === "fx") return ["wire transfer - fx", "wire transfer", "wire"];
  if (txnType === "usd abroad") return ["wire transfer - usd", "wire transfer", "wire"];
  if (["payout"].includes(txnType) || isCrossBorderTransaction(txn)) return ["wire transfer", "wire"];
  if (speedFlag === "rtp" || processingMethod === "rtp") return ["instant payments", "rtp"];
  if (["ach", "nacha", "eft", "sepa"].includes(processingMethod)) return [...(speedFlag === "fasterach" ? ["ach - same day"] : []), "ach", "sepa", "nacha", "eft"];
  if (["card", "wallet", "push"].includes(processingMethod)) return [processingMethod];
  return processingMethod ? [processingMethod] : ["ach"];
};
const findRevShareCostRow = (costs, txn, period, tokens = revShareCostTokens(txn), { volumeBand = false } = {}) => {
  const direction = getRevShareDirection(txn);
  const crossBorder = isCrossBorderTransaction(txn);
  const avgTxnSize = txnAverageSize(txn);
  const totalVolume = Number(txn?.totalVolume || 0);
  const normalizedTokens = tokens.map((token) => norm(token)).filter(Boolean);
  const candidates = (costs || []).filter((cost) => {
    if (cost.direction !== direction) return false;
    if (cost.partner && cost.partner !== txn?.partner) return false;
    if ((cost.feeType || "Per Item") !== "Per Item") return false;
    if (!["payment", ""].includes(norm(cost.paymentOrChargeback || "Payment"))) return false;
    if (!inRange(`${period}-15`, cost.startDate, cost.endDate)) return false;
    const txnName = norm(cost.txnName);
    if (!normalizedTokens.some((token) => txnName.includes(token))) return false;
    const bandValue = volumeBand ? totalVolume : avgTxnSize;
    const minAmt = Number(cost.minAmt || 0);
    const maxAmt = Number(cost.maxAmt || 0);
    if (maxAmt > 0 && bandValue > 0 && !(bandValue >= minAmt && bandValue <= maxAmt)) return false;
    return true;
  }).map((cost) => {
    const txnName = norm(cost.txnName);
    const corridor = norm(cost.corridorType);
    let score = 0;
    if (crossBorder) {
      if (corridor.includes("cross border") || corridor.includes("cross-border")) score += 50;
      else if (corridor.includes("domestic/cross border") || corridor.includes("domestic/cross-border")) score += 35;
      else if (corridor === "domestic") score -= 100;
    } else if (corridor === "domestic") {
      score += 50;
    } else if (corridor.includes("domestic")) {
      score += 35;
    } else if (corridor.includes("cross border") || corridor.includes("cross-border")) {
      score -= 100;
    }
    normalizedTokens.forEach((token, index) => {
      if (txnName.includes(token)) score = Math.max(score, score + (30 - (index * 4)));
    });
    if (Number(cost.maxAmt || 0) > 0 && (volumeBand ? totalVolume : avgTxnSize) > 0) score += 10;
    return { score, cost };
  }).sort((a, b) => (b.score - a.score) || (Number(b.cost.fee || 0) - Number(a.cost.fee || 0)));
  return candidates.length ? candidates[0].cost : null;
};
const calculateRevShareTotalCost = (costs, txn, period) => {
  const txnCount = Number(txn?.txnCount || 0);
  const totalVolume = Number(txn?.totalVolume || 0);
  const primaryCostRow = findRevShareCostRow(costs, txn, period);
  let totalCost = 0;
  const seen = new Set();
  const addCost = (row, amount) => {
    if (!row || !(amount > 0)) return;
    const key = `${row.txnName || ""}|${row.corridorType || ""}|${row.fee || ""}`;
    if (seen.has(key)) return;
    seen.add(key);
    totalCost += amount;
  };
  if (primaryCostRow) {
    addCost(primaryCostRow, Number(primaryCostRow.fee || 0) * txnCount);
  }
  if (isFxConversionTransaction(txn) && totalVolume > 0) {
    const conversionCostRow = findRevShareCostRow(costs, txn, period, ["conversion volume fee"], { volumeBand: true });
    if (conversionCostRow) addCost(conversionCostRow, Number(conversionCostRow.fee || 0) * totalVolume);
  }
  const txnType = norm(txn?.txnType);
  const payeeCcy = String(txn?.payeeCcy || "").toUpperCase().trim();
  if (["fx", "usd abroad"].includes(txnType) && payeeCcy) {
    const localPaymentRow = findRevShareCostRow(costs, txn, period, [`local payment fee (${payeeCcy.toLowerCase()})`]);
    if (localPaymentRow) addCost(localPaymentRow, Number(localPaymentRow.fee || 0) * txnCount);
  }
  return {
    totalCost: roundCurrency(totalCost),
    primaryCostRow
  };
};
const txnMatchesPricingRow = (rule, txn) => optionalMatch(rule.txnType, txn.txnType)
  && optionalMatch(rule.speedFlag, txn.speedFlag)
  && optionalMatch(rule.payerFunding, txn.payerFunding)
  && optionalMatch(rule.payeeFunding, txn.payeeFunding)
  && optionalMatch(rule.payeeCardType, txn.payeeCardType)
  && optionalMatch(rule.payerCcy, txn.payerCcy)
  && optionalMatch(rule.payeeCcy, txn.payeeCcy)
  && optionalCountryMatch(rule.payerCountry, rule.payerCountryGroup, txn.payerCountry)
  && optionalCountryMatch(rule.payeeCountry, rule.payeeCountryGroup, txn.payeeCountry)
  && optionalMatch(rule.processingMethod, txn.processingMethod);
const isCalendarYearEndPeriod = (period) => String(period || "").endsWith("-12");
const html = (value) => String(value ?? "")
  .replaceAll("&", "&amp;")
  .replaceAll("<", "&lt;")
  .replaceAll(">", "&gt;")
  .replaceAll('"', "&quot;");

const DATA_KEYS = ["ps", "pConfig", "pArchived", "pActive", "pBilling", "pInvoices", "off", "vol", "fxRates", "cap", "rs", "mins", "plat", "revf", "impl", "vaFees", "surch", "pCosts", "ltxn", "lrev", "lva", "lrs", "lfxp", "lookerImportAudit", "accessLogs", "adminSettings"];
const WORKFLOW_LOOKER_FILE_OPTIONS = [
  { value: "all_registered_accounts_offline", label: "All Registered Accounts - Offline Billing" },
  { value: "all_registered_accounts_rev_share", label: "All Registered Accounts - Rev Share" },
  { value: "partner_offline_billing", label: "Partner Offline Billing" },
  { value: "partner_offline_billing_reversals", label: "Partner Offline Billing (Reversals)" },
  { value: "rev_share_reversals", label: "Rev Share Reversals" },
  { value: "revenue_share_report", label: "Revenue Share Report" },
  { value: "stampli_fx_revenue_reversal", label: "Stampli FX Revenue Reversal" },
  { value: "stampli_fx_revenue_share", label: "Stampli FX Revenue Share" },
  { value: "vba_accounts", label: "VBA ACCOUNTS" },
  { value: "vba_transactions_cc", label: "CC/Citi VBA Txns (Currency Cloud)" },
  { value: "vba_transactions_citi", label: "CC/Citi VBA Txns (Citi)" }
];
const MANUAL_LOOKER_FILE_OPTIONS = [
  { value: "all_registered_accounts_offline", label: "All Registered Accounts - Offline Billing 2026-04-12T1940.xlsx" },
  { value: "all_registered_accounts_rev_share", label: "All Registered Accounts - Rev Share 2026-04-12T1943.xlsx" },
  { value: "vba_transactions", label: "CC_Citi VBA Txns 2026-04-12T1949.xlsx" },
  { value: "partner_offline_billing", label: "Partner Offline Billing 2026-04-13T0052.xlsx" },
  { value: "partner_offline_billing_reversals", label: "Partner Offline Billing (Reversals) 2026-04-12T2009.xlsx" },
  { value: "rev_share_reversals", label: "Rev Share Reversals 2026-04-12T2020.xlsx" },
  { value: "revenue_share_report", label: "Revenue Share Report 2026-04-12T2020.xlsx" },
  { value: "stampli_fx_revenue_reversal", label: "Stampli FX Revenue Reversal 2026-04-12T2033.xlsx" },
  { value: "stampli_fx_revenue_share", label: "Stampli FX Revenue Share 2026-04-12T2033.xlsx" },
  { value: "vba_accounts", label: "VBA ACCOUNTS 2026-04-12T2034.xlsx" }
];
const LOOKER_FILE_ORDER_OPTIONS = [
  ...WORKFLOW_LOOKER_FILE_OPTIONS,
  ...MANUAL_LOOKER_FILE_OPTIONS.filter((option) => !WORKFLOW_LOOKER_FILE_OPTIONS.some((workflowOption) => workflowOption.value === option.value))
];
const LOOKER_FILE_OPTION_MAP = Object.fromEntries(LOOKER_FILE_ORDER_OPTIONS.map((option) => [option.value, option]));
const LOOKER_FILE_OPTION_AUDIT_MAP = {
  vba_transactions: ["vba_transactions_cc", "vba_transactions_citi"]
};
const LOOKER_SECTION_LABELS = {
  ltxn: { singular: "transaction row", plural: "transaction rows", title: "Transaction Data" },
  lrev: { singular: "reversal row", plural: "reversal rows", title: "Reversal Data" },
  lva: { singular: "virtual account row", plural: "virtual account rows", title: "Virtual Account / Setup / Settlement Data" },
  lrs: { singular: "revenue share row", plural: "revenue share rows", title: "Revenue Share Summary" },
  lfxp: { singular: "FX payout row", plural: "FX payout rows", title: "FX Partner Payout Data" }
};
const LOOKER_CHANGE_FIELD_LABELS = {
  rows: "rows",
  txnCount: "txns",
  totalVolume: "volume",
  volume: "volume",
  customerRevenue: "revenue",
  estRevenue: "est revenue",
  directInvoiceAmount: "invoice amount",
  totalActiveAccounts: "active accounts",
  totalBusinessAccounts: "business accounts",
  totalIndividualAccounts: "individual accounts",
  newAccountsOpened: "new accounts",
  dormantAccounts: "dormant accounts",
  newBusinessSetups: "business setups",
  settlementCount: "settlements",
  closedAccounts: "closed accounts",
  shareAmount: "partner payout",
  revenueAmount: "revenue amount",
  amountDue: "amount due",
  monthlyMinimum: "monthly minimum",
  grossRevenue: "gross revenue",
  paymentUsdEquivalentAmount: "usd equivalent"
};

const state = {
  ...createInitialWorkbookData(),
  tab: "invoice",
  sub: "offline",
  csub: "provider",
  sp: "",
  perStart: LOOKER_IMPORT_PERIOD,
  perEnd: LOOKER_IMPORT_PERIOD,
  useDateRange: false,
  billingSummaryPartner: "",
  pv: "",
  np: "",
  inv: null,
  checkerReport: null,
  checkerStatus: "idle",
  checkerError: "",
  invoiceArtifactStatus: "idle",
  invoiceArtifactError: "",
  invoiceArtifactRecord: null,
  privateInvoiceLinkStatus: "idle",
  privateInvoiceLinkError: "",
  privateInvoiceLinkResult: null,
  cf: "",
  fxSearch: "",
  cText: "",
  cStatus: "idle",
  cError: "",
  cParsed: null,
  cName: "",
  cVerifyPartner: "",
  cFileName: "",
  cPendingFile: null,
  cExtractStatus: "idle",
  cDetectedIncremental: false,
  cImported: false,
  cImportSummary: null,
  cImportBehavior: "override",
  confirmDel: false,
  cMode: "import",
  cDiff: null,
  cImportPlan: null,
  cSelectedImportRows: {},
  lastSaved: null,
  lastSavedAt: null,
  toast: null,
  openSections: {},
  tableRowsExpanded: {},
  pageTableRowsExpanded: {
    invoice: false,
    partner: false,
    rates: false,
    looker: false,
    costs: false,
    import: false,
    admin: false
  },
  invoiceExplorer: null,
  lookerImportType: MANUAL_LOOKER_FILE_OPTIONS[0].value,
  lookerImportPeriod: LOOKER_IMPORT_PERIOD,
  lookerImportText: "",
  lookerImportStatus: "idle",
  lookerImportError: "",
  lookerImportResult: null,
  lookerImportFileName: "",
  lookerImportPendingFile: null,
  lookerImportContext: {},
  lookerImportedDetailRows: [],
  lookerImportAudit: null,
  workspaceMode: isSharedWorkbookEnabled() ? "shared" : "local",
  workspaceLabel: getWorkspaceLabel(),
  workspaceRefreshing: false,
  lastSharedAutoRefreshAt: 0,
  currentUserRole: "",
  currentUserEmail: "",
  authSession: null,
  authView: "choice",
  authOverlayOpen: false,
  authError: "",
  authUsername: "",
  authPassword: "",
  guestNameDraft: "",
  adminView: "overview",
  adminLogFilter: "changes"
};

const saveTimer = { id: null };
const detailFileCache = new Map();
const monthlyInvoiceCache = new Map();

const allMainTabs = [
  { id: "invoice", label: "Generate Invoice" },
  { id: "partner", label: "Partner View" },
  { id: "rates", label: "Rate Config" },
  { id: "looker", label: "Data Upload" },
  { id: "costs", label: "Our Costs" },
  { id: "import", label: "Import Contract" },
  { id: "admin", label: "Admin Portal" }
];

const ADMIN_ONLY_ACTIONS = new Set([
  "reset-defaults",
  "refresh-shared-workspace",
  "export-backup",
  "import-backup",
  "set-admin-view"
]);

const rateTabs = [
  { id: "offline", label: "Offline" },
  { id: "volume", label: "Volume" },
  { id: "feecap", label: "Fee Caps" },
  { id: "surcharge", label: "Surcharges" },
  { id: "revshare", label: "Rev Share" },
  { id: "fx", label: "FX Rates" },
  { id: "minimum", label: "Min Rev" },
  { id: "platform", label: "Platform" },
  { id: "reversal", label: "Reversal" },
  { id: "impl", label: "Impl Fee" },
  { id: "vacct", label: "Virtual Accts" }
];

function buildDefaultAdminSettings(value = {}) {
  const safeGuestTabs = new Set(DEFAULT_ADMIN_SETTINGS.guestAllowedTabs);
  const guestAccessCustomized = !!value?.guestAccessCustomized;
  const guestAllowedTabs = !guestAccessCustomized
    ? [...DEFAULT_ADMIN_SETTINGS.guestAllowedTabs]
    : Array.isArray(value?.guestAllowedTabs)
      ? value.guestAllowedTabs.filter((tabId) => safeGuestTabs.has(tabId))
      : [...DEFAULT_ADMIN_SETTINGS.guestAllowedTabs];
  return {
    guestAllowedTabs: guestAllowedTabs.length ? guestAllowedTabs : [...DEFAULT_ADMIN_SETTINGS.guestAllowedTabs],
    guestAccessCustomized
  };
}

function getAdminSettings() {
  return buildDefaultAdminSettings(state.adminSettings);
}

function isAdminAuthenticated() {
  return !!state.authSession && state.authSession.role === "admin";
}

function isGuestAuthenticated() {
  return !!state.authSession && state.authSession.role === "guest";
}

function hasAccessSession() {
  return !!state.authSession;
}

function getSessionLabel() {
  if (!state.authSession) return "";
  return `${state.authSession.role === "admin" ? "Admin" : "Guest"}: ${state.authSession.name}`;
}

function isTabAccessible(tabId) {
  if (isAdminAuthenticated()) return true;
  return getAdminSettings().guestAllowedTabs.includes(tabId);
}

function getVisibleMainTabs() {
  return allMainTabs.filter((tab) => isTabAccessible(tab.id));
}

function readAccessSession() {
  try {
    const raw = sessionStorage.getItem(ACCESS_SESSION_KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    if (!parsed || !parsed.role || !parsed.name) return null;
    return parsed;
  } catch (error) {
    console.error("Could not read access session", error);
    return null;
  }
}

function persistAccessSession(session) {
  try {
    if (!session) {
      sessionStorage.removeItem(ACCESS_SESSION_KEY);
      return;
    }
    sessionStorage.setItem(ACCESS_SESSION_KEY, JSON.stringify(session));
  } catch (error) {
    console.error("Could not persist access session", error);
  }
}

function openAuthGate(view = "choice", { overlay = false } = {}) {
  state.authView = view;
  state.authOverlayOpen = overlay && hasAccessSession();
  state.authError = "";
  state.authUsername = view === "admin" ? state.authUsername : "";
  state.authPassword = "";
  if (view !== "guest") state.guestNameDraft = state.guestNameDraft || "";
}

function recordAccessActivity(action, detail, meta = {}, { persist = true, renderNow = false } = {}) {
  if (!state.authSession) return;
  const logs = Array.isArray(state.accessLogs) ? state.accessLogs : [];
  const entry = {
    id: uid(),
    timestamp: new Date().toISOString(),
    actorRole: state.authSession.role,
    actorName: state.authSession.name,
    action,
    detail,
    tab: state.tab,
    ...meta
  };
  state.accessLogs = [entry, ...logs].slice(0, 500);
  if (persist) scheduleSave();
  if (renderNow) render();
}

function formatLogValue(value) {
  if (value == null || value === "") return "blank";
  if (typeof value === "boolean") return value ? "Yes" : "No";
  if (typeof value === "number") return String(value);
  return String(value);
}

function describeSectionLabel(section) {
  const labels = {
    pBilling: "Partner Billing",
    pInvoices: "Invoice Tracking",
    pActive: "Partner Status",
    off: "Offline Rates",
    vol: "Volume Rates",
    fxRates: "FX Rates",
    cap: "Fee Caps",
    rs: "Rev Share",
    mins: "Minimum Revenue",
    plat: "Platform Fees",
    revf: "Reversal Fees",
    impl: "Implementation Fees",
    vaFees: "Virtual Account Fees",
    surch: "Surcharges",
    pCosts: "Provider Costs"
  };
  return labels[section] || section;
}

function describeSectionRow(section, row = {}) {
  const partner = row.partner ? `${row.partner}` : "";
  const bits = [];
  if (partner) bits.push(partner);
  if (row.feeType) bits.push(row.feeType);
  if (row.txnType) bits.push(row.txnType);
  if (row.speedFlag) bits.push(row.speedFlag);
  if (row.period) bits.push(row.period);
  if (row.kind) bits.push(row.kind);
  if (row.productType) bits.push(row.productType);
  if (row.surchargeType) bits.push(row.surchargeType);
  if (row.provider) bits.push(row.provider);
  if (row.status) bits.push(row.status);
  return bits.filter(Boolean).join(" · ") || row.id || "row";
}

function logWorkbookChange(action, detail, meta = {}) {
  recordAccessActivity(action, detail, { category: "change", ...meta }, { persist: true });
}

function beginGuestSession() {
  const name = String(state.guestNameDraft || "").trim();
  if (!name) {
    state.authError = "Guest name is required.";
    render();
    return;
  }
  state.authSession = {
    role: "guest",
    name,
    loggedInAt: new Date().toISOString()
  };
  persistAccessSession(state.authSession);
  state.authOverlayOpen = false;
  state.authView = "choice";
  state.authError = "";
  state.tab = isTabAccessible(state.tab) ? state.tab : "invoice";
  recordAccessActivity("guest_login", `${name} entered the workbook as a guest.`, { guestName: name, category: "access" });
  render();
}

function beginAdminSession() {
  const username = String(state.authUsername || "").trim();
  const password = String(state.authPassword || "");
  if (username !== ADMIN_USERNAME || password !== ADMIN_PASSWORD) {
    state.authError = "Username or password is incorrect.";
    render();
    return;
  }
  const switchingFromGuest = isGuestAuthenticated() ? state.authSession.name : "";
  state.authSession = {
    role: "admin",
    name: ADMIN_USERNAME,
    loggedInAt: new Date().toISOString()
  };
  persistAccessSession(state.authSession);
  state.authOverlayOpen = false;
  state.authView = "choice";
  state.authError = "";
  state.authPassword = "";
  state.tab = "admin";
  recordAccessActivity("admin_login", switchingFromGuest ? `Admin signed in from guest session ${switchingFromGuest}.` : "Admin signed in.", { username: ADMIN_USERNAME, category: "access" });
  render();
}

function logoutAccessSession() {
  if (state.authSession) {
    recordAccessActivity("logout", `${state.authSession.name} ended the session.`, { category: "access" }, { persist: true });
  }
  state.authSession = null;
  persistAccessSession(null);
  state.authOverlayOpen = false;
  state.authView = "choice";
  state.authError = "";
  state.authUsername = "";
  state.authPassword = "";
  state.guestNameDraft = "";
  state.tab = "invoice";
  render();
}

function promptAdminAccess(reason = "This action requires admin access.") {
  if (isAdminAuthenticated()) return true;
  if (state.authSession) {
    recordAccessActivity("admin_access_requested", reason, { blocked: true, category: "security" }, { persist: true });
  }
  state.authView = "admin";
  state.authOverlayOpen = hasAccessSession();
  state.authPassword = "";
  state.authError = reason;
  render();
  return false;
}

function canAdminEdit() {
  return isAdminAuthenticated();
}

function isAdminLockedTab(tabId) {
  return !isTabAccessible(tabId);
}

function setGuestTabAccess(tabId, allowed) {
  const configurableTabs = allMainTabs.filter((tab) => tab.id !== "admin").map((tab) => tab.id);
  if (!configurableTabs.includes(tabId)) return;
  const current = new Set(getAdminSettings().guestAllowedTabs);
  if (allowed) current.add(tabId);
  else current.delete(tabId);
  state.adminSettings = buildDefaultAdminSettings({ guestAllowedTabs: [...current], guestAccessCustomized: true });
  logWorkbookChange(
    "update_guest_tab_access",
    `${allowed ? "Unlocked" : "Locked"} ${allMainTabs.find((tab) => tab.id === tabId)?.label || tabId} for guest users.`,
    { section: "adminSettings", tabId, allowed }
  );
  persistAndRender();
}

function pCol() {
  return { key: "partner", label: "Partner", type: "select", opts: getPartnerOptions(), w: 130 };
}

const txCol = { key: "txnType", label: "Txn Type", type: "select", opts: ["Payout", "Payin", "Domestic", "USD Abroad", "FX", "CAD Domestic", "GBP Domestic", "EUR Domestic", "AUD Domestic", "Refund", "Transfer"], w: 120 };
const spCol = { key: "speedFlag", label: "Speed", type: "select", opts: ["Standard", "FasterACH", "Expedited", "RTP"], w: 110 };
const dsCol = { key: "startDate", label: "Start", w: 104 };
const deCol = { key: "endDate", label: "End", w: 104 };
const fpCol = { key: "payerFunding", label: "Payer Fund", type: "select", opts: ["", "Bank", "Credit", "Debit", "Wallet", "Cash"], w: 96 };
const feCol = { key: "payeeFunding", label: "Payee Fund", type: "select", opts: ["", "Bank", "Credit", "Debit", "Wallet", "Cash"], w: 96 };
const cpCol = { key: "payerCcy", label: "Payer Ccy", w: 84 };
const ceCol = { key: "payeeCcy", label: "Payee Ccy", w: 84 };
const kpCol = { key: "payerCountry", label: "Payer Ctry", w: 88 };
const keCol = { key: "payeeCountry", label: "Payee Ctry", w: 88 };
const pmCol = { key: "processingMethod", label: "Proc Method", type: "select", opts: ["", "ACH", "Wire", "RTP", "Card", "Check", "FedNow", "iACH", "SWIFT", "EFT"], w: 110 };
const mnCol = { key: "minAmt", label: "Min Amt", type: "number", w: 96 };
const mxCol = { key: "maxAmt", label: "Max Amt", type: "number", w: 96 };

function getTableConfigs() {
  return {
    pActive: [pCol(), { key: "status", label: "Status", type: "select", opts: ["Active", "Inactive"], w: 112 }, { key: "startPeriod", label: "Start Month", type: "month", w: 126 }, { key: "endPeriod", label: "End Month", type: "month", w: 126 }, { key: "note", label: "Note", w: 220 }],
    off: [pCol(), txCol, spCol, mnCol, mxCol, fpCol, feCol, { key: "fee", label: "Fee $", type: "number", w: 88, step: "0.01" }, cpCol, ceCol, kpCol, keCol, pmCol, dsCol, deCol],
    vol: [pCol(), txCol, spCol, { key: "rate", label: "Rate", type: "number", w: 88, step: "0.0001" }, { key: "payerFunding", label: "Payer", w: 70 }, { key: "payeeFunding", label: "Payee", w: 70 }, { key: "payeeCardType", label: "Card Type", w: 90 }, { key: "ccyGroup", label: "Ccy Group", w: 124 }, { key: "minVol", label: "Min Vol", type: "number", w: 96 }, { key: "maxVol", label: "Max Vol", type: "number", w: 96 }, dsCol, deCol, { key: "note", label: "Note", w: 140 }],
    cap: [pCol(), { key: "productType", label: "Product Type", type: "select", opts: PRODUCT_TYPES, w: 150 }, { key: "capType", label: "Cap Type", type: "select", opts: ["Min Fee", "Max Fee"], w: 110 }, { key: "amount", label: "$ Amount", type: "number", w: 90, step: "0.01" }, dsCol, deCol],
    rs: [pCol(), txCol, spCol, { key: "revSharePct", label: "Rev Share", type: "number", w: 92, step: "0.01" }, dsCol, deCol],
    mins: [pCol(), { key: "minAmount", label: "Min $", type: "number", w: 88 }, { key: "minVol", label: "Vol Lower", type: "number", w: 110 }, { key: "maxVol", label: "Vol Upper", type: "number", w: 110 }, { key: "implFeeOffset", label: "Impl Off", type: "bool", w: 78 }, dsCol, deCol],
    plat: [pCol(), { key: "monthlyFee", label: "Monthly $", type: "number", w: 96 }, dsCol, deCol],
    revf: [pCol(), { key: "payerFunding", label: "Payer Fund", type: "select", opts: ["", "Bank", "Wallet", "Card"], w: 96 }, { key: "feePerReversal", label: "$/Rev", type: "number", w: 82, step: "0.01" }, dsCol, deCol],
    impl: [
      pCol(),
      { key: "feeType", label: "Fee Type", type: "select", opts: ["Account Setup", "Daily Settlement", "Implementation", "Go-Live"], w: 148 },
      { key: "feeAmount", label: "Fee $", type: "number", w: 88 },
      dsCol,
      deCol,
      { key: "applyAgainstMin", label: "vs Min", type: "bool", w: 72 },
      { key: "creditMode", label: "Credit To", type: "select", opts: ["", "Monthly Minimum", "Monthly Subscription"], w: 160 },
      { key: "creditAmount", label: "Credit $", type: "number", w: 92 },
      { key: "creditWindowDays", label: "Launch ≤ Days", type: "number", w: 118 },
      { key: "note", label: "Note", w: 220 }
    ],
    vaFees: [pCol(), { key: "feeType", label: "Fee Type", type: "select", opts: ["Account Opening", "Monthly Active", "Dormancy", "Account Closing"], w: 148 }, { key: "minAccounts", label: "Min Accts", type: "number", w: 92 }, { key: "maxAccounts", label: "Max Accts", type: "number", w: 92 }, { key: "discount", label: "Discount", type: "number", w: 90 }, { key: "feePerAccount", label: "$/Acct", type: "number", w: 90 }, dsCol, deCol, { key: "note", label: "Note", w: 220 }],
    surch: [pCol(), { key: "surchargeType", label: "Surcharge Type", type: "select", opts: ["Same Currency", "Platform", "Card Surcharge", "Cross-Border"], w: 146 }, { key: "rate", label: "Rate", type: "number", w: 88, step: "0.0001" }, { key: "minVol", label: "Min Vol", type: "number", w: 96 }, { key: "maxVol", label: "Max Vol", type: "number", w: 96 }, dsCol, deCol, { key: "note", label: "Note", w: 150 }],
    fxRates: [pCol(), { key: "payerCorridor", label: "Payer Corridor", type: "select", opts: ["", ...CORRIDORS], w: 130 }, { key: "payerCcy", label: "Payer Ccy", w: 90 }, { key: "payeeCorridor", label: "Payee Corridor", type: "select", opts: ["", ...CORRIDORS], w: 130 }, { key: "payeeCcy", label: "Payee Ccy", w: 90 }, { key: "minTxnSize", label: "Min Txn $", type: "number", w: 96 }, { key: "maxTxnSize", label: "Max Txn $", type: "number", w: 96 }, { key: "minVol", label: "Min Vol", type: "number", w: 96 }, { key: "maxVol", label: "Max Vol", type: "number", w: 96 }, { key: "rate", label: "Rate", type: "number", w: 88, step: "0.0001" }, dsCol, deCol, { key: "note", label: "Note", w: 140 }],
    ltxn: [{ key: "period", label: "Period", w: 90 }, pCol(), txCol, spCol, mnCol, mxCol, fpCol, feCol, cpCol, ceCol, kpCol, keCol, pmCol, { key: "txnCount", label: "Txns", type: "number", w: 82 }, { key: "totalVolume", label: "Volume", type: "number", w: 110 }, { key: "customerRevenue", label: "Cust Rev", type: "number", w: 110 }, { key: "estRevenue", label: "Est Rev", type: "number", w: 110 }, { key: "avgTxnSize", label: "Avg Txn $", type: "number", w: 104 }],
    lrev: [{ key: "period", label: "Period", w: 90 }, pCol(), { key: "payerFunding", label: "Payer Fund", type: "select", opts: ["", "Bank", "Wallet", "Card"], w: 96 }, { key: "reversalCount", label: "Reversals", type: "number", w: 96 }],
    lva: [{ key: "period", label: "Period", w: 90 }, pCol(), { key: "newAccountsOpened", label: "New Accts", type: "number", w: 92 }, { key: "totalActiveAccounts", label: "Active", type: "number", w: 78 }, { key: "totalBusinessAccounts", label: "Biz Active", type: "number", w: 92 }, { key: "totalIndividualAccounts", label: "Ind Active", type: "number", w: 92 }, { key: "dormantAccounts", label: "Dormant", type: "number", w: 88 }, { key: "closedAccounts", label: "Closed", type: "number", w: 78 }, { key: "newBusinessSetups", label: "Biz Setup", type: "number", w: 92 }, { key: "settlementCount", label: "Settlements", type: "number", w: 102 }],
    lrs: [{ key: "period", label: "Period", w: 90 }, pCol(), { key: "netRevenue", label: "Net Rev", type: "number", w: 110 }, { key: "partnerRevenueShare", label: "Partner Share (We Pay)", type: "number", w: 140 }, { key: "revenueOwed", label: "Revenue Owed (They Pay)", type: "number", w: 150 }, { key: "monthlyMinimumRevenue", label: "Monthly Min Fee", type: "number", w: 130 }],
    pCosts: [{ key: "provider", label: "Provider", type: "select", opts: getProviderOptions(), w: 110 }, { key: "direction", label: "Dir", type: "select", opts: ["In", "Out"], w: 70 }, { key: "txnName", label: "Name", w: 240 }, { key: "corridorType", label: "Corridor", type: "select", opts: ["Domestic", "Cross Border", "Domestic/Cross Border"], w: 150 }, { key: "worldlink", label: "WL", type: "bool", w: 54 }, { key: "minAmt", label: "Min", type: "number", w: 88 }, { key: "maxAmt", label: "Max", type: "number", w: 88 }, { key: "varFixed", label: "V/F", type: "select", opts: ["Fixed", "Variable"], w: 92 }, { key: "fee", label: "Fee", type: "number", w: 88, step: "0.01" }, { key: "feeType", label: "Type", type: "select", opts: ["Per Item", "Monthly", "Monthly Minimum", "One-Time", "Hourly"], w: 130 }, { key: "paymentOrChargeback", label: "P/C", type: "select", opts: ["Payment", "Chargeback", "Other"], w: 124 }, dsCol, deCol, { key: "note", label: "Note", w: 240 }]
  };
}

function defaultValueForColumn(col) {
  if (col.type === "number") return 0;
  if (col.type === "bool") return false;
  return "";
}

function scheduleSave() {
  if (saveTimer.id) clearTimeout(saveTimer.id);
  saveTimer.id = window.setTimeout(() => {
    const payload = exportSnapshot();
    void persistSnapshot(payload)
      .then((savedAt) => {
        state.lastSavedAt = savedAt || payload._saved;
        state.lastSaved = new Date(savedAt || Date.now()).toLocaleTimeString();
        render();
      })
      .catch((error) => {
        console.error("Could not save workbook data", error);
        showToast("Save failed", "Could not sync the workbook. A local backup was kept in this browser.", "error");
      });
  }, SAVE_DELAY_MS);
}

function persistAndRender() {
  scheduleSave();
  render();
}

function showToast(title, message, tone = "info") {
  state.toast = { title, message, tone };
  render();
  window.setTimeout(() => {
    if (state.toast && state.toast.message === message) {
      state.toast = null;
      render();
    }
  }, 2800);
}

function cacheSnapshotLocally(snapshot) {
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(snapshot));
    return true;
  } catch (error) {
    console.warn("Could not cache workbook snapshot locally", error);
    return false;
  }
}

function clearCachedSnapshot() {
  try {
    localStorage.removeItem(STORAGE_KEY);
  } catch (error) {
    console.warn("Could not clear cached workbook snapshot", error);
  }
}

async function loadState() {
  if (isSharedWorkbookEnabled()) {
    try {
      const payload = await loadSharedBootstrap();
      let saved = payload?.snapshot;
      if (saved && saved._version) {
        const { snapshot, changed } = migrateSnapshot(saved);
        DATA_KEYS.forEach((key) => {
          if (snapshot[key] != null) state[key] = snapshot[key];
        });
        state.accessLogs = Array.isArray(state.accessLogs) ? state.accessLogs : [];
        state.adminSettings = buildDefaultAdminSettings(state.adminSettings);
        state.lastSavedAt = snapshot._saved || null;
        state.lastSaved = snapshot._saved ? new Date(snapshot._saved).toLocaleTimeString() : null;
        cacheSnapshotLocally(snapshot);
        const invoiceDateBackfilled = backfillMissingInvoiceTrackingDates({ log: false });
        if (changed || invoiceDateBackfilled) {
          const savedAt = await persistSnapshot(exportSnapshot());
          state.lastSavedAt = savedAt || state.lastSavedAt;
          state.lastSaved = savedAt ? new Date(savedAt).toLocaleTimeString() : state.lastSaved;
        }
      } else {
        const seededSnapshot = exportSnapshot();
        const savedAt = await persistSnapshot(seededSnapshot);
        state.lastSavedAt = savedAt || seededSnapshot._saved;
        state.lastSaved = savedAt ? new Date(savedAt).toLocaleTimeString() : null;
      }
      state.workspaceMode = "shared";
      state.workspaceLabel = payload?.workspace?.label || getWorkspaceLabel();
      state.currentUserRole = payload?.user?.role || "";
      state.currentUserEmail = payload?.user?.email || "";
      state.authSession = readAccessSession();
      return;
    } catch (error) {
      console.error("Could not load shared workspace bootstrap", error);
      state.workspaceMode = "local-fallback";
      state.workspaceLabel = `${getWorkspaceLabel()} (fallback)`;
      showToast("Shared workspace unavailable", "Falling back to this browser’s local workbook copy.", "warning");
    }
  }
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) return;
    const saved = JSON.parse(raw);
    if (!saved || !saved._version) return;
    const { snapshot, changed } = migrateSnapshot(saved);
    DATA_KEYS.forEach((key) => {
      if (snapshot[key] != null) state[key] = snapshot[key];
    });
    state.accessLogs = Array.isArray(state.accessLogs) ? state.accessLogs : [];
    state.adminSettings = buildDefaultAdminSettings(state.adminSettings);
    state.lastSavedAt = snapshot._saved || null;
    state.lastSaved = snapshot._saved ? new Date(snapshot._saved).toLocaleTimeString() : null;
    const invoiceDateBackfilled = backfillMissingInvoiceTrackingDates({ log: false });
    if (changed || invoiceDateBackfilled) {
      cacheSnapshotLocally(exportSnapshot());
    }
  } catch (error) {
    console.error("Could not load workbook data", error);
  }
  state.authSession = readAccessSession();
}

async function refreshSharedWorkspace({ showSuccessToast = true, showErrorToast = true, retries = 3, retryDelayMs = 500 } = {}) {
  if (!isSharedWorkbookEnabled()) return;
  if (sharedWorkspaceRefreshPromise) return sharedWorkspaceRefreshPromise;
  state.workspaceRefreshing = true;
  render();
  sharedWorkspaceRefreshPromise = (async () => {
    const hadUsableSharedSnapshot = state.workspaceMode === "shared" && !!(state.lastSavedAt || state.lastSaved || (Array.isArray(state.ps) && state.ps.length));
    let payload = null;
    try {
      payload = await loadSharedBootstrap({ retries, retryDelayMs });
    } catch (error) {
      console.error("Could not fetch shared workspace bootstrap", error);
      if (showErrorToast) {
        if (hadUsableSharedSnapshot) {
          showToast("Refresh delayed", "Keeping the current shared workbook snapshot while the server catches up.", "warning");
        } else {
          showToast("Refresh failed", "Could not reload the latest shared workbook snapshot.", "error");
        }
      }
      if (hadUsableSharedSnapshot) {
        scheduleSharedWorkspaceRetry();
      }
      return false;
    }

    try {
      const saved = payload?.snapshot;
      if (!saved || !saved._version) {
        throw new Error("Shared workspace bootstrap did not include a valid snapshot.");
      }
      const { snapshot } = migrateSnapshot(saved);
      DATA_KEYS.forEach((key) => {
        if (snapshot[key] != null) state[key] = snapshot[key];
      });
      state.accessLogs = Array.isArray(state.accessLogs) ? state.accessLogs : [];
      state.adminSettings = buildDefaultAdminSettings(state.adminSettings);
      state.lastSavedAt = snapshot._saved || null;
      state.lastSaved = snapshot._saved ? new Date(snapshot._saved).toLocaleTimeString() : null;
      state.workspaceMode = "shared";
      state.workspaceLabel = payload?.workspace?.label || getWorkspaceLabel();
      state.currentUserRole = payload?.user?.role || "";
      state.currentUserEmail = payload?.user?.email || "";
      state.invoiceExplorer = null;
      state.cImported = false;
      state.cImportSummary = null;
      state.cDiff = null;
      try {
        refreshContractImportPlan();
      } catch (error) {
        console.error("Could not refresh contract import plan during shared refresh", error);
        state.cImportPlan = null;
        state.cSelectedImportRows = {};
      }
      if (state.pv && !state.ps.includes(state.pv)) state.pv = "";
      if (state.sp && !state.ps.includes(state.sp)) {
        state.sp = "";
        state.inv = null;
      }
      cacheSnapshotLocally(snapshot);
      if (showSuccessToast) {
        showToast("Shared data refreshed", "Loaded the latest workbook snapshot from the shared source.", "success");
      }
      return true;
    } catch (error) {
      console.error("Could not apply refreshed shared workspace", error);
      if (showErrorToast) {
        if (hadUsableSharedSnapshot) {
          showToast("Refresh delayed", "Keeping the current shared workbook snapshot while the server catches up.", "warning");
        } else {
          showToast("Refresh failed", "Could not reload the latest shared workbook snapshot.", "error");
        }
      }
      if (hadUsableSharedSnapshot) {
        scheduleSharedWorkspaceRetry();
      }
      return false;
    } finally {
      state.workspaceRefreshing = false;
      sharedWorkspaceRefreshPromise = null;
      render();
    }
  })();
  return sharedWorkspaceRefreshPromise;
}

const SHARED_WORKSPACE_AUTO_REFRESH_MS = 30 * 1000;

async function maybeAutoRefreshSharedWorkspace({ force = false } = {}) {
  if (!isSharedWorkbookEnabled()) return;
  if (state.workspaceRefreshing) return;
  const now = Date.now();
  const lastRefreshAt = Number(state.lastSharedAutoRefreshAt || 0);
  if (!force && lastRefreshAt && now - lastRefreshAt < SHARED_WORKSPACE_AUTO_REFRESH_MS) return;
  const refreshed = await refreshSharedWorkspace({ showSuccessToast: false, showErrorToast: false });
  if (refreshed) {
    state.lastSharedAutoRefreshAt = now;
  }
}

function exportSnapshot() {
  const payload = { _version: STORAGE_VERSION, _saved: new Date().toISOString() };
  DATA_KEYS.forEach((key) => {
    payload[key] = state[key];
  });
  return payload;
}

async function persistSnapshot(payload) {
  if (isSharedWorkbookEnabled() && isSharedWorkbookWriteEnabled()) {
    try {
      const result = await saveSharedWorkbookSnapshot(payload);
      cacheSnapshotLocally(payload);
      return result?.savedAt || payload._saved;
    } catch (error) {
      cacheSnapshotLocally(payload);
      throw error;
    }
  }
  cacheSnapshotLocally(payload);
  return payload._saved;
}

function downloadJson(filename, data) {
  const blob = new Blob([JSON.stringify(data, null, 2)], { type: "application/json" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = filename;
  document.body.appendChild(link);
  link.click();
  link.remove();
  URL.revokeObjectURL(url);
}

function csvEscape(value) {
  const text = String(value ?? "");
  if (/[",\n]/.test(text)) {
    return `"${text.replaceAll('"', '""')}"`;
  }
  return text;
}

function rowsToCsvText(rows) {
  if (!rows.length) return "";
  const headers = Object.keys(rows[0]);
  return [
    headers.map(csvEscape).join(","),
    ...rows.map((row) => headers.map((header) => csvEscape(row[header])).join(","))
  ].join("\n");
}

function downloadCsv(filename, rows) {
  if (!rows.length) return;
  const blob = new Blob([rowsToCsvText(rows)], { type: "text/csv;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = filename;
  document.body.appendChild(link);
  link.click();
  link.remove();
  URL.revokeObjectURL(url);
}

function slugifyFilenamePart(value) {
  return String(value ?? "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "") || "export";
}

function formatPeriodLabel(period) {
  if (!period) return "";
  const [year, month] = String(period).split("-");
  if (!year || !month) return period;
  const date = new Date(Number(year), Number(month) - 1, 1);
  return date.toLocaleString("en-US", { month: "long", year: "numeric" });
}

function comparePeriods(a, b) {
  return String(a || "").localeCompare(String(b || ""));
}

function normalizeMonthKey(value) {
  return String(value || "").trim().slice(0, 7);
}

function isPartnerArchived(partner) {
  return (state.pArchived || []).some((name) => norm(name) === norm(partner));
}

function partnerOption(partner, { includeArchivedTag = true } = {}) {
  return {
    value: partner,
    label: includeArchivedTag && isPartnerArchived(partner) ? `${partner} (ARCHIVED)` : partner
  };
}

function getPartnerOptions({ includeArchived = true, includeArchivedTag = true } = {}) {
  return state.ps
    .filter((partner) => includeArchived || !isPartnerArchived(partner))
    .map((partner) => partnerOption(partner, { includeArchivedTag }));
}

function getProviderOptions() {
  const base = ["Citi", "SVB", "NAB", "Finexio", "CurrencyCloud"];
  const fromState = (state.pCosts || []).map((row) => row.provider).filter(Boolean);
  return [...new Set([...base, ...fromState])];
}

function periodMatchesSchedule(period, start, end) {
  const month = normalizeMonthKey(period);
  const rangeStart = normalizeMonthKey(start);
  const rangeEnd = normalizeMonthKey(end);
  if (rangeStart && comparePeriods(month, rangeStart) < 0) return false;
  if (rangeEnd && comparePeriods(month, rangeEnd) > 0) return false;
  return true;
}

function isPartnerActiveForPeriod(source, partner, period) {
  const rows = (source?.pActive || []).filter((row) => norm(row.partner) === norm(partner) && periodMatchesSchedule(period, row.startPeriod, row.endPeriod));
  if (!rows.length) return true;
  if (rows.some((row) => norm(row.status || "Active") === "inactive")) return false;
  return true;
}

function normalizePeriodRange(start, end) {
  const fallback = LOOKER_IMPORT_PERIOD;
  let rangeStart = String(start || fallback);
  let rangeEnd = String(end || rangeStart || fallback);
  if (comparePeriods(rangeStart, rangeEnd) > 0) {
    [rangeStart, rangeEnd] = [rangeEnd, rangeStart];
  }
  return { start: rangeStart, end: rangeEnd };
}

function enumeratePeriods(start, end) {
  const { start: rangeStart, end: rangeEnd } = normalizePeriodRange(start, end);
  const [startYear, startMonth] = rangeStart.split("-").map(Number);
  const [endYear, endMonth] = rangeEnd.split("-").map(Number);
  const months = [];
  let cursor = new Date(startYear, startMonth - 1, 1);
  const last = new Date(endYear, endMonth - 1, 1);
  while (cursor <= last) {
    months.push(`${cursor.getFullYear()}-${String(cursor.getMonth() + 1).padStart(2, "0")}`);
    cursor = new Date(cursor.getFullYear(), cursor.getMonth() + 1, 1);
  }
  return months;
}

function formatPeriodBoundary(period, boundary) {
  if (!period) return "";
  const [year, month] = String(period).split("-").map(Number);
  if (!year || !month) return String(period);
  const date = boundary === "end"
    ? new Date(year, month, 0)
    : new Date(year, month - 1, 1);
  return date.toLocaleDateString("en-US", { month: "long", day: "numeric", year: "numeric" });
}

function formatPeriodRangeLabel(start, end) {
  const normalized = normalizePeriodRange(start, end);
  if (normalized.start === normalized.end) return formatPeriodLabel(normalized.start);
  return `${formatPeriodLabel(normalized.start)} - ${formatPeriodLabel(normalized.end)}`;
}

function formatPeriodDateRange(start, end) {
  const normalized = normalizePeriodRange(start, end);
  return `${formatPeriodBoundary(normalized.start, "start")} - ${formatPeriodBoundary(normalized.end, "end")}`;
}

function buildInvoicePeriodKey(start, end) {
  const normalized = normalizePeriodRange(start, end);
  return normalized.start === normalized.end ? normalized.start : `${normalized.start}_to_${normalized.end}`;
}

function roundCurrency(value) {
  return Math.round(Number(value || 0) * 100) / 100;
}

function localDateFromIso(iso) {
  const text = normalizeIsoDate(iso);
  if (!text) return null;
  const [year, month, day] = text.split("-").map(Number);
  return new Date(year, month - 1, day);
}

function isoFromLocalDate(date) {
  if (!(date instanceof Date) || Number.isNaN(date.getTime())) return "";
  return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, "0")}-${String(date.getDate()).padStart(2, "0")}`;
}

function todayIsoDate() {
  return isoFromLocalDate(new Date());
}

function formatIsoDate(iso) {
  const date = localDateFromIso(iso);
  if (!date) return "";
  return date.toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" });
}

function formatDateTime(value) {
  if (!value) return "";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return String(value);
  return date.toLocaleString("en-US", {
    month: "long",
    day: "numeric",
    year: "numeric",
    hour: "numeric",
    minute: "2-digit"
  });
}

function formatDateTimeCompact(value) {
  if (!value) return "";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return String(value);
  return date.toLocaleString("en-US", {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit"
  });
}

function parseIsoDay(value) {
  if (!value) return "";
  const raw = String(value).trim();
  if (!raw) return "";
  const candidate = raw.slice(0, 10);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(candidate)) return "";
  const parsed = new Date(`${candidate}T00:00:00`);
  if (Number.isNaN(parsed.getTime())) return "";
  return candidate;
}

function getPeriodBoundaryIso(period, boundary) {
  if (!period) return "";
  const [year, month] = String(period).split("-").map(Number);
  if (!year || !month) return "";
  const date = boundary === "end"
    ? new Date(year, month, 0)
    : new Date(year, month - 1, 1);
  return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, "0")}-${String(date.getDate()).padStart(2, "0")}`;
}

function extractLookerFilterCoverage(sourceMetadata) {
  const raw = String(sourceMetadata?.periodFilterValue || "").trim();
  if (!raw) return null;
  const match = raw.match(/(\d{4}-\d{2}-\d{2})\s+to\s+(\d{4}-\d{2}-\d{2})/);
  if (!match) return null;
  return {
    rangeStart: match[1],
    currentThrough: match[2],
    source: "query_filter"
  };
}

function resolveLookerRecordCoverage(record) {
  const coverage = record?.dataCoverage && typeof record.dataCoverage === "object" ? record.dataCoverage : null;
  const explicitStart = parseIsoDay(coverage?.rangeStart);
  const explicitEnd = parseIsoDay(coverage?.currentThrough || coverage?.rangeEnd);
  if (explicitStart || explicitEnd) {
    return {
      rangeStart: explicitStart || explicitEnd,
      currentThrough: explicitEnd || explicitStart,
      source: coverage?.source || "stored",
      exact: coverage?.exact !== false
    };
  }
  const coverageMonth = String(coverage?.coverageMonth || record?.period || "").trim();
  if (coverageMonth) {
    return {
      coverageMonth,
      source: coverage?.source || "billing_period",
      exact: false
    };
  }
  return null;
}

function describeLookerRecordCoverage(record, { compact = false } = {}) {
  const coverage = resolveLookerRecordCoverage(record);
  if (!coverage) return "";
  if (coverage.exact !== false && coverage.currentThrough) {
    if (compact) return `Data through ${formatIsoDate(coverage.currentThrough)}`;
    if (coverage.rangeStart && coverage.rangeStart !== coverage.currentThrough) {
      return `Coverage ${formatIsoDate(coverage.rangeStart)} to ${formatIsoDate(coverage.currentThrough)}`;
    }
    return `Data through ${formatIsoDate(coverage.currentThrough)}`;
  }
  if (!coverage.coverageMonth) return "";
  const periodLabel = formatPeriodLabel(coverage.coverageMonth);
  return compact
    ? `${periodLabel} coverage month · exact through date unavailable`
    : `Coverage month ${periodLabel} · exact through date unavailable`;
}

function summarizeLookerRunCoverage(files, period) {
  const exactCoverageDates = [...new Set(
    (files || [])
      .map((fileRecord) => {
        const coverage = resolveLookerRecordCoverage(fileRecord);
        return coverage?.exact !== false ? coverage?.currentThrough || "" : "";
      })
      .filter(Boolean)
  )].sort();
  if (exactCoverageDates.length) {
    const latest = exactCoverageDates.at(-1);
    if (exactCoverageDates.length === 1) return `Data through ${formatIsoDate(latest)}`;
    return `Coverage varies by file · latest data through ${formatIsoDate(latest)}`;
  }
  const coverageMonth = String(period || "").trim();
  return coverageMonth ? `${formatPeriodLabel(coverageMonth)} coverage month · exact through date unavailable` : "";
}

function describeLookerSectionCount(section, count) {
  const meta = LOOKER_SECTION_LABELS[section];
  if (!meta) return `${count} ${section}`;
  return `${count} ${count === 1 ? meta.singular : meta.plural}`;
}

function summarizeLookerSectionCounts(sectionCounts) {
  return Object.entries(sectionCounts || {})
    .filter(([, count]) => Number(count) > 0)
    .map(([section, count]) => describeLookerSectionCount(section, Number(count)))
    .join(" · ");
}

function formatLookerMetricValue(field, value) {
  if (value == null || value === "") return "0";
  const numeric = Number(value);
  if (Number.isNaN(numeric)) return String(value);
  if (["rows", "txnCount", "totalActiveAccounts", "totalBusinessAccounts", "totalIndividualAccounts", "newAccountsOpened", "dormantAccounts", "newBusinessSetups", "settlementCount", "closedAccounts"].includes(field)) {
    return numeric.toLocaleString("en-US");
  }
  return fmt(numeric);
}

function summarizeLookerChangeSummary(changeSummary) {
  if (!changeSummary || typeof changeSummary !== "object") return "";
  const totalChangedGroups = Number(changeSummary.totalChangedGroups || 0);
  if (!totalChangedGroups) return "No partner-period values changed in this upload.";
  const parts = [
    `${totalChangedGroups.toLocaleString("en-US")} changed partner-period${totalChangedGroups === 1 ? "" : "s"}`
  ];
  const partnerCount = Number(changeSummary.partnerCount || 0);
  const changedFileCount = Number(changeSummary.changedFileCount || 0);
  if (partnerCount) parts.push(`${partnerCount.toLocaleString("en-US")} partner${partnerCount === 1 ? "" : "s"} touched`);
  if (changedFileCount) parts.push(`${changedFileCount.toLocaleString("en-US")} file${changedFileCount === 1 ? "" : "s"} with changes`);
  return parts.join(" · ");
}

function renderLookerChangeGroup(group) {
  const delta = group?.delta || {};
  const changes = Object.entries(delta)
    .map(([field, value]) => {
      const label = LOOKER_CHANGE_FIELD_LABELS[field] || field;
      const sign = Number(value) > 0 ? "+" : "";
      return `${label} ${sign}${formatLookerMetricValue(field, value)}`;
    });
  const changeText = changes.length ? changes.join(" · ") : "No numeric changes recorded.";
  return `<li><strong>${html(group.partner || "Unknown partner")}</strong>${group.period ? ` · ${html(formatPeriodLabel(group.period))}` : ""}<br><span>${html(changeText)}</span></li>`;
}

function renderLookerChangeSections(changeSummary) {
  const sections = (changeSummary?.sections || []).filter((section) => Number(section.changedGroupCount || 0) > 0);
  if (!sections.length) return "";
  return `
    <div class="upload-summary-changes">
      ${sections.map((section) => `
        <div class="upload-summary-change-block">
          <div class="upload-summary-change-title">${html(section.label)} · ${Number(section.changedGroupCount || 0).toLocaleString("en-US")} change${Number(section.changedGroupCount || 0) === 1 ? "" : "s"}</div>
          <ul class="bulleted-list upload-summary-change-list">
            ${(section.changedGroups || []).map((group) => renderLookerChangeGroup(group)).join("")}
          </ul>
        </div>
      `).join("")}
    </div>
  `;
}

function addDaysToIsoDate(iso, days) {
  const date = localDateFromIso(iso);
  if (!date || !Number.isFinite(Number(days))) return "";
  date.setDate(date.getDate() + Number(days));
  return isoFromLocalDate(date);
}

function diffDays(startIso, endIso) {
  const start = localDateFromIso(startIso);
  const end = localDateFromIso(endIso);
  if (!start || !end) return 0;
  return Math.floor((end.getTime() - start.getTime()) / 86400000);
}

function daysInMonth(year, month) {
  return new Date(year, month, 0).getDate();
}

function parseDueDaysFromPayBy(payBy) {
  const text = String(payBy || "").trim();
  if (!text) return 0;
  const dueMatch = text.match(/due\s+in\s+(\d+)\s+days?/i);
  if (dueMatch) return Number(dueMatch[1]);
  const netMatch = text.match(/\bnet\s*(\d+)\b/i);
  if (netMatch) return Number(netMatch[1]);
  const daysMatch = text.match(/\b(\d+)\s*days?\b/i);
  if (daysMatch) return Number(daysMatch[1]);
  return 0;
}

function getPartnerBillingConfig(partner) {
  return (state.pBilling || []).find((row) => norm(row.partner) === norm(partner)) || null;
}

function getPartnerContractStartDate(partner, source = state) {
  const config = (source.pBilling || []).find((row) => norm(row.partner) === norm(partner));
  const explicit = normalizeIsoDate(config?.contractStartDate);
  if (explicit) return explicit;
  const candidateDates = [];
  ["off", "vol", "fxRates", "cap", "rs", "mins", "plat", "revf", "impl", "vaFees", "surch"].forEach((key) => {
    (source[key] || []).forEach((row) => {
      if (norm(row.partner) !== norm(partner)) return;
      const startDate = normalizeIsoDate(row.startDate);
      if (startDate) candidateDates.push(startDate);
    });
  });
  candidateDates.sort(comparePeriods);
  return candidateDates[0] || "";
}

function getPartnerGoLiveDate(partner, source = state) {
  const config = (source.pBilling || []).find((row) => norm(row.partner) === norm(partner));
  if (config && Object.prototype.hasOwnProperty.call(config, "goLiveDate")) {
    const explicit = normalizeIsoDate(config.goLiveDate);
    if (explicit) return explicit;
  }
  const implRow = (source.impl || []).find((row) => norm(row.partner) === norm(partner) && row.feeType === "Implementation" && normalizeIsoDate(row.goLiveDate));
  return normalizeIsoDate(implRow?.goLiveDate) || "";
}

function isPartnerNotYetLive(partner, source = state) {
  const config = (source.pBilling || []).find((row) => norm(row.partner) === norm(partner));
  return !!config?.notYetLive;
}

function partnerHasImportedActivityThroughPeriod(partner, period, source = state) {
  const targetPeriod = normalizeMonthKey(period);
  if (!targetPeriod) return false;
  for (const key of ["ltxn", "lrev", "lrs", "lfxp", "lva"]) {
    for (const row of source[key] || []) {
      if (norm(row.partner) !== norm(partner)) continue;
      const rowPeriod = normalizeMonthKey(row.period || row.refundPeriod || row.creditCompleteMonth || "");
      if (rowPeriod && comparePeriods(rowPeriod, targetPeriod) <= 0) return true;
    }
  }
  return false;
}

function isRecurringBillingLiveForPeriod(partner, period, source = state) {
  // `notYetLive` on pBilling is authoritative: if set, the partner does NOT bill
  // recurring fees (minimums, subscriptions, rev-share, etc.), regardless of any
  // planned go-live date captured in the impl table. Impl rows are projections
  // from the sales handoff, not evidence the partner has actually started
  // transacting. Confirmed 2026-04-22 with user ("only if they're live"):
  // HubSpot Integration Status is the source of truth, synced into pBilling by
  // tools/apply-hubspot-partner-status.py. To mark a partner live, clear
  // notYetLive on their pBilling row (or set pBilling.goLiveDate explicitly).
  if (isPartnerNotYetLive(partner, source)) return false;
  const goLiveDate = getPartnerGoLiveDate(partner, source);
  const goLiveMonth = normalizeMonthKey(goLiveDate);
  if (!goLiveMonth) return true;
  return comparePeriods(normalizeMonthKey(period), goLiveMonth) >= 0;
}

function getImplementationBillingDate(partner, row, source = state) {
  return normalizeIsoDate(row?.startDate) || getPartnerContractStartDate(partner, source) || normalizeIsoDate(row?.goLiveDate);
}

function normalizeImplementationCreditMode(row) {
  const raw = String(row?.creditMode || "").trim().toLowerCase().replace(/\s+/g, "_");
  if (raw) return raw;
  if (row?.applyAgainstMin) return "monthly_minimum";
  return "";
}

function getImplementationCreditAmount(row) {
  const explicit = Number(row?.creditAmount || 0);
  if (explicit > 0) return explicit;
  return normalizeImplementationCreditMode(row) ? Number(row?.feeAmount || 0) : 0;
}

function getImplementationCreditWindowDays(row) {
  return Number(row?.creditWindowDays || 0);
}

function normalizeImplementationRow(row = {}) {
  return {
    creditMode: "",
    creditAmount: 0,
    creditWindowDays: 0,
    applyAgainstMin: false,
    note: "",
    ...row
  };
}

function getImplementationCreditStartPeriod(partner, row, source = state) {
  const mode = normalizeImplementationCreditMode(row);
  const creditAmount = getImplementationCreditAmount(row);
  if (!mode || !(creditAmount > 0)) return "";
  const goLiveDate = normalizeIsoDate(getPartnerGoLiveDate(partner, source) || row?.goLiveDate);
  if (!goLiveDate) return "";
  const billingDate = normalizeIsoDate(getImplementationBillingDate(partner, row, source));
  const creditWindowDays = getImplementationCreditWindowDays(row);
  if (creditWindowDays > 0 && billingDate) {
    const billing = localDateFromIso(billingDate);
    const goLive = localDateFromIso(goLiveDate);
    if (billing && goLive) {
      const diffDays = Math.round((goLive.getTime() - billing.getTime()) / 86400000);
      if (diffDays > creditWindowDays) return "";
    }
  }
  return normalizeMonthKey(goLiveDate);
}

function getImplementationCreditLabel(mode) {
  if (mode === "monthly_minimum") return "monthly minimum";
  if (mode === "monthly_subscription") return "monthly subscription";
  return "future fees";
}

function buildDefaultPartnerBilling(partner, existing = null) {
  return {
    id: existing?.id || uid(),
    partner,
    billingFreq: existing?.billingFreq || "Monthly",
    payBy: existing?.payBy || "",
    dueDays: Number(existing?.dueDays || 0),
    billingDay: existing?.billingDay || "",
    contractDueText: existing?.contractDueText || "",
    preferredBillingTiming: existing?.preferredBillingTiming || "",
    contactEmails: existing?.contactEmails || "",
    contractStartDate: normalizeIsoDate(existing?.contractStartDate) || getPartnerContractStartDate(partner),
    goLiveDate: normalizeIsoDate(existing?.goLiveDate) || getPartnerGoLiveDate(partner),
    notYetLive: !!existing?.notYetLive,
    integrationStatus: existing?.integrationStatus || "",
    lateFeePercentMonthly: Number(existing?.lateFeePercentMonthly || 0),
    lateFeeStartDays: Number(existing?.lateFeeStartDays || 0),
    serviceSuspensionDays: Number(existing?.serviceSuspensionDays || 0),
    lateFeeTerms: existing?.lateFeeTerms || "",
    note: existing?.note || ""
  };
}

function mergePartnerBillingDefaults(existingRows, defaultRows) {
  const existingByPartner = new Map((Array.isArray(existingRows) ? existingRows : []).map((row) => [norm(row.partner), row]));
  const merged = (Array.isArray(defaultRows) ? defaultRows : []).map((defaultRow) => {
    const existing = existingByPartner.get(norm(defaultRow.partner));
    existingByPartner.delete(norm(defaultRow.partner));
    if (!existing) return { ...defaultRow };
    return {
      ...existing,
      ...defaultRow,
      id: existing.id || defaultRow.id,
      partner: defaultRow.partner,
      billingFreq: defaultRow.billingFreq || existing.billingFreq || "Monthly",
      payBy: defaultRow.payBy || existing.payBy || "",
      dueDays: Number(defaultRow.dueDays || existing.dueDays || 0),
      billingDay: defaultRow.billingDay !== "" && defaultRow.billingDay != null ? defaultRow.billingDay : (existing.billingDay || ""),
      contractDueText: defaultRow.contractDueText || existing.contractDueText || "",
      preferredBillingTiming: defaultRow.preferredBillingTiming || existing.preferredBillingTiming || "",
      contactEmails: defaultRow.contactEmails || existing.contactEmails || "",
      contractStartDate: normalizeIsoDate(defaultRow.contractStartDate) || normalizeIsoDate(existing.contractStartDate) || getPartnerContractStartDate(defaultRow.partner, { ...state, ...{ pBilling: existingRows } }),
      goLiveDate: normalizeIsoDate(defaultRow.goLiveDate) || normalizeIsoDate(existing.goLiveDate) || getPartnerGoLiveDate(defaultRow.partner, { ...state, ...{ pBilling: existingRows } }),
      notYetLive: existing.notYetLive != null ? !!existing.notYetLive : !!defaultRow.notYetLive,
      integrationStatus: defaultRow.integrationStatus || existing.integrationStatus || "",
      lateFeePercentMonthly: Number(defaultRow.lateFeePercentMonthly || existing.lateFeePercentMonthly || 0),
      lateFeeStartDays: Number(defaultRow.lateFeeStartDays || existing.lateFeeStartDays || 0),
      serviceSuspensionDays: Number(defaultRow.serviceSuspensionDays || existing.serviceSuspensionDays || 0),
      lateFeeTerms: defaultRow.lateFeeTerms || existing.lateFeeTerms || "",
      note: defaultRow.note || existing.note || ""
    };
  });
  return [
    ...merged,
    ...Array.from(existingByPartner.values()).map((row) => buildDefaultPartnerBilling(row.partner, row))
  ];
}

function upsertPartnerBilling(partner, patch, { persist = true, log = true } = {}) {
  const existing = getPartnerBillingConfig(partner);
  const next = {
    ...buildDefaultPartnerBilling(partner, existing),
    ...patch
  };
  if (next.dueDays === "" || next.dueDays == null) next.dueDays = 0;
  if (next.billingDay === 0) next.billingDay = "";
  next.lateFeePercentMonthly = Number(next.lateFeePercentMonthly || 0);
  next.lateFeeStartDays = Number(next.lateFeeStartDays || 0);
  next.serviceSuspensionDays = Number(next.serviceSuspensionDays || 0);
  if (existing) {
    state.pBilling = state.pBilling.map((row) => (String(row.id) === String(existing.id) ? next : row));
  } else {
    state.pBilling = [...state.pBilling, next];
  }
  if (log) {
    const changedFields = Object.keys(patch || {}).filter((key) => formatLogValue(existing?.[key]) !== formatLogValue(next[key]));
    logWorkbookChange(
      existing ? "update_partner_billing" : "create_partner_billing",
      existing
        ? `Updated ${partner} billing fields: ${changedFields.join(", ") || "no field diff detected"}.`
        : `Created billing terms for ${partner}.`,
      { partner, section: "pBilling", fields: changedFields }
    );
  }
  if (persist) persistAndRender();
}

function getBillingDueDays(partner) {
  const config = getPartnerBillingConfig(partner);
  const explicit = Number(config?.dueDays || 0);
  if (explicit > 0) return explicit;
  return parseDueDaysFromPayBy(config?.payBy || "");
}

function getBillingDay(partner) {
  const config = getPartnerBillingConfig(partner);
  const day = Number(config?.billingDay || 0);
  if (!Number.isFinite(day) || day <= 0) return 0;
  return Math.min(Math.floor(day), 31);
}

function inferBillingDayFromTimingText(value) {
  const text = String(value || "").trim().toLowerCase();
  if (!text) return 0;
  if (text.includes("end of month")) return 31;
  if (text.includes("begining of month") || text.includes("beginning of month") || text.includes("start of month")) return 1;
  if (/first week|1st week/.test(text)) return 7;
  if (/second week|2nd week/.test(text)) return 14;
  if (/third week|3rd week/.test(text)) return 21;
  if (/fourth week|4th week/.test(text)) return 28;
  const dayMatch = text.match(/\b(\d{1,2})(?:st|nd|rd|th)\b/);
  if (dayMatch) return Math.min(Number(dayMatch[1]), 31);
  if (/\bfirst\b/.test(text)) return 1;
  if (/\bsecond\b/.test(text)) return 2;
  if (/\bthird\b/.test(text)) return 3;
  if (/\bfourth\b/.test(text)) return 4;
  return 0;
}

function getInferredBillingDay(partner) {
  const explicit = getBillingDay(partner);
  if (explicit) return explicit;
  const config = getPartnerBillingConfig(partner);
  const preferred = inferBillingDayFromTimingText(config?.preferredBillingTiming);
  if (preferred) return preferred;
  const note = inferBillingDayFromTimingText(config?.note);
  if (note) return note;
  return 1;
}

function normalizeInvoiceTrackingKind(kind) {
  return kind === "payable" ? "payable" : "receivable";
}

function getInvoiceTrackingRecord(partner, period, kind = "receivable") {
  const normalizedKind = normalizeInvoiceTrackingKind(kind);
  return (state.pInvoices || []).find((row) =>
    norm(row.partner) === norm(partner)
    && normalizeMonthKey(row.period) === normalizeMonthKey(period)
    && normalizeInvoiceTrackingKind(row.kind) === normalizedKind
  ) || null;
}

function upsertInvoiceTrackingRecord(partner, period, kind = "receivable", patch, { persist = true, log = true } = {}) {
  const periodKey = normalizeMonthKey(period);
  const normalizedKind = normalizeInvoiceTrackingKind(kind);
  const existing = getInvoiceTrackingRecord(partner, periodKey, normalizedKind);
  const next = {
    id: existing?.id || uid(),
    partner,
    period: periodKey,
    kind: normalizedKind,
    invoiceDate: existing?.invoiceDate || "",
    paid: !!existing?.paid,
    amountPaid: Number(existing?.amountPaid || 0),
    amountDueOverride: Number(existing?.amountDueOverride || 0),
    dueDateOverride: existing?.dueDateOverride || "",
    paidDate: existing?.paidDate || "",
    sourceSheet: existing?.sourceSheet || "",
    sourceStatus: existing?.sourceStatus || "",
    sourceFeeInfo: existing?.sourceFeeInfo || "",
    note: existing?.note || "",
    ...patch
  };
  next.amountPaid = roundCurrency(Number(next.amountPaid || 0));
  next.amountDueOverride = roundCurrency(Number(next.amountDueOverride || 0));
  if ((next.paid || next.amountPaid > 0) && !normalizeIsoDate(next.invoiceDate)) {
    next.invoiceDate = inferInvoiceTrackingInvoiceDate(partner, periodKey, normalizedKind);
  }
  if (existing) {
    state.pInvoices = state.pInvoices.map((row) => (String(row.id) === String(existing.id) ? next : row));
  } else {
    state.pInvoices = [...state.pInvoices, next];
  }
  if (log) {
    const changedFields = Object.keys(patch || {}).filter((key) => formatLogValue(existing?.[key]) !== formatLogValue(next[key]));
    logWorkbookChange(
      existing ? "update_invoice_tracking" : "create_invoice_tracking",
      existing
        ? `Updated ${partner} ${periodKey} ${normalizedKind} tracking fields: ${changedFields.join(", ") || "no field diff detected"}.`
        : `Created ${partner} ${periodKey} ${normalizedKind} tracking row.`,
      { partner, period: periodKey, kind: normalizedKind, section: "pInvoices", fields: changedFields }
    );
  }
  if (persist) persistAndRender();
}

function nextMonthKey(period) {
  const [year, month] = normalizeMonthKey(period).split("-").map(Number);
  if (!year || !month) return "";
  const date = new Date(year, month, 1);
  return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, "0")}`;
}

function previousMonthKey(period) {
  const [year, month] = normalizeMonthKey(period).split("-").map(Number);
  if (!year || !month) return "";
  const date = new Date(year, month - 2, 1);
  return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, "0")}`;
}

function getExpectedInvoiceSendDate(partner, period) {
  const billingDay = getInferredBillingDay(partner);
  if (!billingDay) return "";
  const sendMonth = nextMonthKey(period);
  const [year, month] = sendMonth.split("-").map(Number);
  if (!year || !month) return "";
  const day = Math.min(billingDay, daysInMonth(year, month));
  return `${year}-${String(month).padStart(2, "0")}-${String(day).padStart(2, "0")}`;
}

function inferInvoiceTrackingInvoiceDate(partner, period, kind = "receivable") {
  const normalizedPeriod = normalizeMonthKey(period);
  if (!normalizedPeriod) return "";
  const expectedSendDate = getExpectedInvoiceSendDate(partner, normalizedPeriod);
  if (expectedSendDate) return expectedSendDate;
  const config = getPartnerBillingConfig(partner);
  const frequency = String(config?.billingFreq || "").toLowerCase();
  if (frequency.includes("quarter")) {
    return `${nextMonthKey(normalizedPeriod)}-01`.replace("--", "-");
  }
  const nextPeriod = nextMonthKey(normalizedPeriod);
  if (!nextPeriod) return "";
  const [year, month] = nextPeriod.split("-").map(Number);
  if (!year || !month) return "";
  return `${year}-${String(month).padStart(2, "0")}-01`;
}

function backfillMissingInvoiceTrackingDates({ log = false } = {}) {
  const rows = Array.isArray(state.pInvoices) ? state.pInvoices : [];
  let changed = 0;
  const touchedPartners = new Set();
  state.pInvoices = rows.map((row) => {
    if (!row) return row;
    const hasPayment = !!row.paid || Number(row.amountPaid || 0) > 0;
    if (!hasPayment || normalizeIsoDate(row.invoiceDate)) return row;
    const inferredInvoiceDate = inferInvoiceTrackingInvoiceDate(row.partner, row.period, row.kind);
    if (!inferredInvoiceDate) return row;
    changed += 1;
    touchedPartners.add(row.partner);
    return {
      ...row,
      invoiceDate: inferredInvoiceDate,
    };
  });
  if (changed && log) {
    logWorkbookChange(
      "invoice_tracking_dates_backfill",
      `Backfilled invoice dates for ${changed} paid invoice row${changed === 1 ? "" : "s"} across ${touchedPartners.size} partner${touchedPartners.size === 1 ? "" : "s"}.`,
      { section: "pInvoices", rows: changed, partners: [...touchedPartners] }
    );
  }
  return changed;
}

function getInvoiceDueDate(partner, period, invoiceDate) {
  const dueDays = getBillingDueDays(partner);
  if (!invoiceDate || dueDays <= 0) return "";
  return addDaysToIsoDate(invoiceDate, dueDays);
}

function getAllInvoicePeriods() {
  const periods = new Set();
  ["ltxn", "lrev", "lva", "lrs", "lfxp"].forEach((key) => {
    (state[key] || []).forEach((row) => {
      const period = normalizeMonthKey(row.period);
      if (period) periods.add(period);
    });
  });
  (state.pInvoices || []).forEach((row) => {
    const period = normalizeMonthKey(row.period);
    if (period) periods.add(period);
  });
  if (!periods.size && LOOKER_IMPORT_PERIOD) periods.add(LOOKER_IMPORT_PERIOD);
  return [...periods].sort(comparePeriods);
}

function getImportedLookerPeriods() {
  const periods = new Set();
  ["ltxn", "lrev", "lva", "lrs", "lfxp"].forEach((key) => {
    (state[key] || []).forEach((row) => {
      const period = normalizeMonthKey(row.period);
      if (period) periods.add(period);
    });
  });
  return [...periods].sort(comparePeriods);
}

function orderLookerImportFiles(records) {
  const orderIndex = new Map(LOOKER_FILE_ORDER_OPTIONS.map((option, index) => [option.value, index]));
  return [...(records || [])].sort((a, b) => {
    const aOrder = orderIndex.get(a.fileType) ?? 999;
    const bOrder = orderIndex.get(b.fileType) ?? 999;
    if (aOrder !== bOrder) return aOrder - bOrder;
    return String(a.fileLabel || "").localeCompare(String(b.fileLabel || ""));
  });
}

function buildLookerAuditRecord(result, savedAt, { source = "manual" } = {}) {
  return {
    fileType: result.fileType,
    fileLabel: result.fileLabel || LOOKER_FILE_OPTION_MAP[result.fileType]?.label || result.fileType,
    period: result.period,
    savedAt,
    source,
    warnings: [...(result.warnings || [])],
    sectionCounts: { ...((result.stats || {}).sectionCounts || {}) },
    stats: { ...(result.stats || {}) },
    changeSummary: JSON.parse(JSON.stringify(result.changeSummary || {})),
    sourceMetadata: JSON.parse(JSON.stringify(result.sourceMetadata || {})),
    dataCoverage: JSON.parse(JSON.stringify(result.dataCoverage || {}))
  };
}

function getUploadChannel(record) {
  const explicitChannel = String(record?.sourceChannel || "").trim().toLowerCase();
  if (explicitChannel) return explicitChannel;
  const source = String(record?.source || "").trim().toLowerCase();
  const runId = String(record?.runId || "").trim().toLowerCase();
  if (source === "manual" || runId.startsWith("manual-")) return "manual";
  if (runId.startsWith("n8n-") || runId.startsWith("synth-workflow-")) return "workflow";
  if (!runId && source && source !== "manual") return "workflow";
  if (runId) return "ad_hoc";
  return "workflow";
}

function getUploadChannelLabel(channel) {
  return channel === "manual" ? "Manual Upload" : "n8n / Workflow Upload";
}

function getRunMissingUploadSources(run) {
  if (!run || getUploadChannel(run) !== "workflow") return [];
  const present = new Set((run.files || []).map((fileRecord) => String(fileRecord.fileType || "")));
  return WORKFLOW_LOOKER_FILE_OPTIONS.filter((option) => !present.has(option.value));
}

function normalizeLookerRunRecord(run) {
  if (!run || typeof run !== "object") return null;
  const files = orderLookerImportFiles(Array.isArray(run.files) ? run.files : []);
  const normalized = {
    ...run,
    files,
    sourceChannel: getUploadChannel(run)
  };
  if (!normalized.changeSummary || typeof normalized.changeSummary !== "object") {
    const changedPartners = new Set();
    const changedPeriods = new Set();
    let totalChangedGroups = 0;
    let changedFileCount = 0;
    files.forEach((fileRecord) => {
      const summary = fileRecord.changeSummary || {};
      totalChangedGroups += Number(summary.totalChangedGroups || 0);
      (summary.changedPartners || []).forEach((partner) => changedPartners.add(partner));
      (summary.changedPeriods || []).forEach((period) => changedPeriods.add(period));
      if (Number(summary.totalChangedGroups || 0) > 0) changedFileCount += 1;
    });
    normalized.changeSummary = {
      totalChangedGroups,
      changedPartners: [...changedPartners].sort(),
      changedPeriods: [...changedPeriods].sort(comparePeriods),
      partnerCount: changedPartners.size,
      periodCount: changedPeriods.size,
      changedFileCount
    };
  }
  return normalized;
}

function buildLatestRunsByChannel(runs) {
  const latestRunByChannel = { manual: null, workflow: null };
  (runs || []).forEach((run) => {
    const channel = getUploadChannel(run);
    if ((channel === "manual" || channel === "workflow") && !latestRunByChannel[channel]) latestRunByChannel[channel] = run;
  });
  return latestRunByChannel;
}

function normalizeLookerImportAudit(audit) {
  if (!audit || typeof audit !== "object") return null;
  const seedRuns = Array.isArray(audit.runs) ? audit.runs : [];
  const legacyLatestRun = audit.latestRun && typeof audit.latestRun === "object" ? [audit.latestRun] : [];
  const runs = [...seedRuns, ...legacyLatestRun]
    .map((run) => normalizeLookerRunRecord(run))
    .filter(Boolean)
    .reduce((acc, run) => {
      const runId = String(run.runId || "");
      if (runId && acc.some((entry) => String(entry.runId || "") === runId)) return acc;
      acc.push(run);
      return acc;
    }, [])
    .sort((a, b) => String(b.savedAt || "").localeCompare(String(a.savedAt || "")));
  const latestRun = runs[0] || null;
  const synthesizedRuns = [];
  const byFileTypeRecords = Object.values(audit.byFileType || {}).filter((record) => record && typeof record === "object");
  ["manual", "workflow"].forEach((channel) => {
    const channelFiles = byFileTypeRecords.filter((record) => getUploadChannel(record) === channel);
    if (!channelFiles.length) return;
    const latestFile = [...channelFiles].sort((a, b) => String(b.savedAt || "").localeCompare(String(a.savedAt || "")))[0];
    synthesizedRuns.push(normalizeLookerRunRecord({
      runId: `synth-${channel}-${latestFile.savedAt || "unknown"}`,
      period: latestFile.period || "",
      savedAt: latestFile.savedAt || "",
      source: channel === "manual" ? "manual" : latestFile.source || "server",
      files: channelFiles
    }));
  });
  const allRuns = [...runs];
  synthesizedRuns.forEach((run) => {
    if (!run) return;
    const channel = getUploadChannel(run);
    if (allRuns.some((entry) => getUploadChannel(entry) === channel)) return;
    allRuns.push(run);
  });
  allRuns.sort((a, b) => String(b.savedAt || "").localeCompare(String(a.savedAt || "")));
  return {
    ...audit,
    byFileType: { ...(audit.byFileType || {}) },
    runs: allRuns,
    latestRun: allRuns[0] || latestRun,
    latestRunByChannel: buildLatestRunsByChannel(allRuns)
  };
}

function updateLookerImportAudit(result, { savedAt = new Date().toISOString(), source = "manual", runId = `manual-${Date.now()}` } = {}) {
  const record = buildLookerAuditRecord(result, savedAt, { source });
  const existingAudit = normalizeLookerImportAudit(state.lookerImportAudit) || {};
  const byFileType = {
    ...(existingAudit.byFileType || {}),
    [record.fileType]: record
  };
  const priorRun = (existingAudit.runs || []).find((entry) => String(entry.runId || "") === runId) || { files: [] };
  const files = orderLookerImportFiles([
    ...(priorRun.files || []).filter((entry) => entry.fileType !== record.fileType),
    record
  ]);
  const nextRun = normalizeLookerRunRecord({
    ...(priorRun || {}),
    runId,
    period: record.period,
    savedAt,
    source,
    files
  });
  const runs = [nextRun, ...((existingAudit.runs || []).filter((entry) => String(entry.runId || "") !== runId))]
    .sort((a, b) => String(b.savedAt || "").localeCompare(String(a.savedAt || "")));
  state.lookerImportAudit = {
    byFileType,
    runs,
    latestRun: runs[0] || nextRun,
    latestRunByChannel: buildLatestRunsByChannel(runs)
  };
}

function buildFallbackLookerImportAudit() {
  const periods = getImportedLookerPeriods();
  const latestPeriod = periods.at(-1);
  if (!latestPeriod) return null;
  const sectionCounts = Object.fromEntries(
    ["ltxn", "lrev", "lva", "lrs", "lfxp"]
      .map((section) => [
        section,
        (state[section] || []).filter((row) => normalizeMonthKey(row.period) === latestPeriod).length
      ])
      .filter(([, count]) => count > 0)
  );
  if (!Object.keys(sectionCounts).length) return null;
  return {
    byFileType: Object.fromEntries(
      WORKFLOW_LOOKER_FILE_OPTIONS.map((option) => [option.value, {
        fileType: option.value,
        fileLabel: option.label,
        period: latestPeriod,
        savedAt: state.lastSavedAt || "",
        source: "shared-snapshot",
        warnings: [],
        sectionCounts: {},
        dataCoverage: {
          coverageMonth: latestPeriod,
          source: "billing_period",
          exact: false
        }
      }])
    ),
    runs: [{
      runId: "shared-snapshot-fallback",
      period: latestPeriod,
      savedAt: state.lastSavedAt || "",
      source: "shared-snapshot",
      sourceChannel: "workflow",
      files: [],
      fallbackSectionCounts: sectionCounts
    }],
    latestRun: {
      runId: "shared-snapshot-fallback",
      period: latestPeriod,
      savedAt: state.lastSavedAt || "",
      source: "shared-snapshot",
      sourceChannel: "workflow",
      files: [],
      fallbackSectionCounts: sectionCounts
    },
    latestRunByChannel: {
      manual: null,
      workflow: {
        runId: "shared-snapshot-fallback",
        period: latestPeriod,
        savedAt: state.lastSavedAt || "",
        source: "shared-snapshot",
        sourceChannel: "workflow",
        files: [],
        fallbackSectionCounts: sectionCounts
      }
    }
  };
}

function getResolvedLookerImportAudit() {
  if (state.lookerImportAudit && typeof state.lookerImportAudit === "object") return normalizeLookerImportAudit(state.lookerImportAudit);
  return normalizeLookerImportAudit(buildFallbackLookerImportAudit());
}

function getLookerImportConfirmation() {
  const audit = getResolvedLookerImportAudit();
  const latestRun = audit?.latestRun;
  if (!latestRun) return null;
  const changeSummary = latestRun.changeSummary && typeof latestRun.changeSummary === "object"
    ? latestRun.changeSummary
    : {
        totalChangedGroups: 0,
        partnerCount: 0,
        changedFileCount: 0
      };
  const sectionCounts = latestRun.files?.length
    ? latestRun.files.reduce((acc, fileRecord) => {
        Object.entries(fileRecord.sectionCounts || {}).forEach(([section, count]) => {
          acc[section] = (acc[section] || 0) + Number(count || 0);
        });
        return acc;
      }, {})
    : { ...(latestRun.fallbackSectionCounts || {}) };
  const filteredCounts = Object.fromEntries(Object.entries(sectionCounts).filter(([, count]) => Number(count) > 0));
  if (!Object.keys(filteredCounts).length && !Number(changeSummary.totalChangedGroups || 0)) return null;
  return {
    period: latestRun.period,
    sectionCounts: filteredCounts,
    savedAt: latestRun.savedAt || state.lastSavedAt || null,
    changeSummary,
    coverageText: summarizeLookerRunCoverage(latestRun.files || [], latestRun.period)
  };
}

function getLookerFileOptionsWithStatus() {
  const audit = getResolvedLookerImportAudit();
  const fallbackSavedAt = audit?.latestRun?.savedAt || state.lastSavedAt || "";
  return MANUAL_LOOKER_FILE_OPTIONS.map((option) => {
    const auditKeys = LOOKER_FILE_OPTION_AUDIT_MAP[option.value] || [option.value];
    const auditRecords = auditKeys
      .map((key) => audit?.byFileType?.[key])
      .filter(Boolean)
      .sort((a, b) => String(b.savedAt || "").localeCompare(String(a.savedAt || "")));
    const auditRecord = auditRecords[0] || null;
    const timestamp = auditRecord?.savedAt || fallbackSavedAt;
    const sourceLabel = auditRecord ? getUploadChannelLabel(getUploadChannel(auditRecord)) : "";
    const updatedLabel = timestamp ? `Updated ${formatDateTimeCompact(timestamp)}${sourceLabel ? ` via ${sourceLabel}` : ""}` : "Not imported yet";
    return {
      ...option,
      label: `${option.label} — ${updatedLabel}`
    };
  });
}

function renderUploadRunSummary(run, { emptyMessage = "No uploads recorded yet.", summaryTitle = "Latest upload overview" } = {}) {
  if (!run) return `<p class="empty-state">${html(emptyMessage)}</p>`;
  const overallSummary = summarizeLookerChangeSummary(run.changeSummary || {});
  const missingSources = getRunMissingUploadSources(run);
  const summaryText = run.files?.length
    ? run.files.map((fileRecord) => ({
        title: fileRecord.fileLabel,
        updated: fileRecord.savedAt,
        period: fileRecord.period,
        detail: summarizeLookerChangeSummary(fileRecord.changeSummary) || summarizeLookerSectionCounts(fileRecord.sectionCounts || {}) || "No rows changed for this billing month.",
        warnings: fileRecord.warnings || [],
        changeSummary: fileRecord.changeSummary || null,
        sourceMetadata: fileRecord.sourceMetadata || null,
        dataCoverage: fileRecord.dataCoverage || null,
        sourceChannel: getUploadChannel(fileRecord)
      }))
    : [{
        title: "Latest shared upload",
        updated: run.savedAt,
        period: run.period,
        detail: summarizeLookerSectionCounts(run.fallbackSectionCounts || {}) || "No section changes were detected.",
        warnings: [],
        changeSummary: run.changeSummary || null,
        sourceMetadata: null,
        dataCoverage: {
          coverageMonth: run.period,
          source: "billing_period",
          exact: false
        },
        sourceChannel: getUploadChannel(run)
      }];
  return `
    <div class="upload-summary-list">
      <div class="upload-summary-meta" style="margin-bottom:8px">${run.savedAt ? `Updated ${html(formatDateTime(run.savedAt))}` : "No saved time available"} · ${html(getUploadChannelLabel(getUploadChannel(run)))}</div>
      ${missingSources.length ? `<div class="summary-banner warning"><h4>Workflow run incomplete</h4><p>Saved ${html(String((run.files || []).length))} of ${html(String(WORKFLOW_LOOKER_FILE_OPTIONS.length))} configured upload sources. Missing: ${html(missingSources.map((option) => option.label).join(", "))}.</p></div>` : ""}
      ${overallSummary ? `<div class="summary-banner info"><h4>${html(summaryTitle)}</h4><p>${html(overallSummary)}</p></div>` : ""}
      ${summaryText.map((item) => `
        <div class="upload-summary-item">
          <div class="upload-summary-header">
            <strong>${html(item.title)}</strong>
            <span class="helper-pill">${html(item.period ? formatPeriodLabel(item.period) : "No period")}</span>
          </div>
          <div class="upload-summary-meta">${item.updated ? `Updated ${html(formatDateTime(item.updated))}` : "No saved time available"} · ${html(getUploadChannelLabel(item.sourceChannel || getUploadChannel(item)))}</div>
          ${describeLookerRecordCoverage(item, { compact: true }) ? `<div class="upload-summary-meta">${html(describeLookerRecordCoverage(item, { compact: true }))}</div>` : ""}
          ${item.sourceMetadata?.dashboardId || item.sourceMetadata?.reportName ? `<div class="upload-summary-meta">Source: ${[
            item.sourceMetadata?.reportName ? `Looker · ${item.sourceMetadata.reportName}` : "",
            item.sourceMetadata?.dashboardId ? `Dashboard ${item.sourceMetadata.dashboardId}` : "",
            item.sourceMetadata?.resolvedTileId ? `Tile ${item.sourceMetadata.resolvedTileId}` : "",
            item.sourceMetadata?.byteCount ? `${Number(item.sourceMetadata.byteCount).toLocaleString("en-US")} bytes` : ""
          ].filter(Boolean).join(" · ")}</div>` : ""}
          <div class="upload-summary-detail">${html(item.detail)}</div>
          ${renderLookerChangeSections(item.changeSummary)}
          ${item.warnings.length ? `<ul class="bulleted-list upload-summary-warnings">${item.warnings.map((warning) => `<li>${html(warning)}</li>`).join("")}</ul>` : ""}
        </div>
      `).join("")}
    </div>
  `;
}

function collectHistoricalImportGapRuns(audit) {
  return (audit?.runs || [])
    .map((run) => {
      const missingSources = getRunMissingUploadSources(run).map((option) => `Missing upload source: ${option.label}`);
      const warningItems = (run.files || []).flatMap((fileRecord) =>
        (fileRecord.warnings || []).map((warning) => `${fileRecord.fileLabel || fileRecord.fileType || "Upload source"}: ${warning}`)
      );
      const gapItems = [...missingSources, ...warningItems];
      if (!gapItems.length) return null;
      return {
        run,
        gapItems,
        missingSourceCount: missingSources.length,
        warningCount: warningItems.length
      };
    })
    .filter(Boolean);
}

function renderHistoricalImportGaps() {
  const audit = getResolvedLookerImportAudit();
  const gapRuns = collectHistoricalImportGapRuns(audit);
  if (!gapRuns.length) return `<p class="empty-state">No historical import gaps recorded yet.</p>`;
  return `
    <div class="upload-summary-list">
      ${gapRuns.map(({ run, gapItems, missingSourceCount, warningCount }) => `
        <div class="upload-summary-item">
          <div class="upload-summary-header">
            <strong>${html(run.savedAt ? formatDateTime(run.savedAt) : "Import date unavailable")}</strong>
            <span class="helper-pill">${html(run.period ? formatPeriodLabel(run.period) : "No period")}</span>
          </div>
          <div class="upload-summary-meta">${html(getUploadChannelLabel(getUploadChannel(run)))}</div>
          <div class="upload-summary-detail">${html([
            missingSourceCount ? `${missingSourceCount} missing source${missingSourceCount === 1 ? "" : "s"}` : "",
            warningCount ? `${warningCount} warning${warningCount === 1 ? "" : "s"}` : ""
          ].filter(Boolean).join(" · ") || "Import gaps recorded")}</div>
          <ul class="bulleted-list upload-summary-warnings">${gapItems.map((item) => `<li>${html(item)}</li>`).join("")}</ul>
        </div>
      `).join("")}
    </div>
  `;
}

function getMonthlyInvoiceDraft(partner, period) {
  const key = `${partner}|${period}`;
  if (!monthlyInvoiceCache.has(key)) {
    monthlyInvoiceCache.set(key, calculateLocalInvoiceForPeriod(partner, period));
  }
  return monthlyInvoiceCache.get(key);
}

function evaluateInvoiceTrackingStatus({ amountDue, amountPaid, invoiceDate, dueDate, expectedSendDate }) {
  const receivable = roundCurrency(amountDue);
  const paid = roundCurrency(amountPaid);
  const balance = roundCurrency(Math.max(receivable - paid, 0));
  const today = todayIsoDate();
  if (receivable <= 0) {
    return { kind: "credit", label: "Credit to Partner", tone: "credit" };
  }
  if (balance <= 0 && paid > 0) {
    return { kind: "paid", label: "Paid", tone: "success" };
  }
  if (dueDate && balance > 0) {
    const overdueDays = diffDays(dueDate, today);
    if (overdueDays > 0) {
      return {
        kind: "past_due",
        label: `Past Due by ${overdueDays} Day${overdueDays === 1 ? "" : "s"}`,
        tone: "danger"
      };
    }
  }
  if (paid > 0 && balance > 0) {
    return { kind: "partial", label: "Partially Paid", tone: "warning" };
  }
  if (dueDate && invoiceDate) {
    return { kind: "due", label: `Due on ${formatIsoDate(dueDate)}`, tone: "info" };
  }
  if (!invoiceDate && expectedSendDate) {
    return { kind: "send", label: `Send on ${formatIsoDate(expectedSendDate)}`, tone: "muted" };
  }
  if (invoiceDate) {
    return { kind: "open", label: "Due date unavailable", tone: "muted" };
  }
  return { kind: "unsent", label: "Invoice date not set", tone: "muted" };
}

function evaluatePayableTrackingStatus({ amountDue, amountPaid, invoiceDate }) {
  const payable = roundCurrency(amountDue);
  const paid = roundCurrency(amountPaid);
  const balance = roundCurrency(Math.max(payable - paid, 0));
  if (payable <= 0) return null;
  if (balance <= 0 && paid > 0) {
    return { kind: "paid", label: "Paid", tone: "success" };
  }
  if (paid > 0 && balance > 0) {
    return { kind: "partial", label: "Partially Paid", tone: "warning" };
  }
  if (invoiceDate) {
    return { kind: "open", label: "Veem Owes", tone: "credit" };
  }
  return { kind: "credit", label: "Credit to Partner", tone: "credit" };
}

function buildInvoiceTrackingEntry(partner, period, kind = "receivable") {
  const normalizedKind = normalizeInvoiceTrackingKind(kind);
  const invoice = getMonthlyInvoiceDraft(partner, period);
  const documents = buildInvoiceDocuments(invoice);
  const document = documents.find((doc) => doc.kind === normalizedKind) || null;
  const record = getInvoiceTrackingRecord(partner, period, normalizedKind);
  const amountDue = roundCurrency(Number(record?.amountDueOverride || document?.amountDue || 0));
  const amountPaid = roundCurrency(Number(record?.amountPaid || 0));
  const invoiceDate = normalizeIsoDate(record?.invoiceDate);
  const dueDate = normalizedKind === "receivable"
    ? (normalizeIsoDate(record?.dueDateOverride) || getInvoiceDueDate(partner, period, invoiceDate))
    : "";
  const expectedSendDate = getExpectedInvoiceSendDate(partner, period);
  const balance = roundCurrency(Math.max(amountDue - amountPaid, 0));
  return {
    partner,
    period,
    kind: normalizedKind,
    invoice,
    document,
    record,
    amountDue,
    amountPaid,
    balance,
    invoiceDate,
    dueDate,
    expectedSendDate,
    status: normalizedKind === "receivable"
      ? evaluateInvoiceTrackingStatus({ amountDue, amountPaid, invoiceDate, dueDate, expectedSendDate })
      : evaluatePayableTrackingStatus({ amountDue, amountPaid, invoiceDate })
  };
}

function getPartnerInvoiceTrackingEntries(partner) {
  return getAllInvoicePeriods()
    .flatMap((period) => ([
      buildInvoiceTrackingEntry(partner, period, "receivable"),
      buildInvoiceTrackingEntry(partner, period, "payable")
    ]))
    .filter((entry) => entry.amountDue > 0 || entry.record)
    .sort((a, b) => comparePeriods(b.period, a.period) || (a.kind === b.kind ? 0 : a.kind === "receivable" ? -1 : 1));
}

function summarizeInvoiceRangeStatus(partner, startPeriod, endPeriod) {
  const periods = enumeratePeriods(startPeriod, endPeriod);
  const receivableEntries = periods.map((period) => buildInvoiceTrackingEntry(partner, period, "receivable")).filter((entry) => entry.amountDue > 0);
  if (!receivableEntries.length) {
    const payableEntries = periods.map((period) => buildInvoiceTrackingEntry(partner, period, "payable")).filter((entry) => entry.amountDue > 0);
    if (!payableEntries.length) return null;
    const payableOutstanding = payableEntries.filter((entry) => entry.balance > 0);
    if (!payableOutstanding.length) {
      return { kind: "paid", label: "Paid", tone: "success" };
    }
    if (payableEntries.some((entry) => entry.amountPaid > 0 && entry.balance > 0)) {
      return { kind: "partial", label: "Partially Paid", tone: "warning" };
    }
    return { kind: "credit", label: "Credit to Partner", tone: "credit" };
  }
  const outstanding = receivableEntries.filter((entry) => entry.balance > 0);
  if (!outstanding.length) {
    return { kind: "paid", label: "Paid", tone: "success" };
  }
  const overdue = outstanding.filter((entry) => entry.status.kind === "past_due");
  if (overdue.length) {
    const maxDays = Math.max(...overdue.map((entry) => diffDays(entry.dueDate, todayIsoDate())));
    return { kind: "past_due", label: `Past Due by ${maxDays} Day${maxDays === 1 ? "" : "s"}`, tone: "danger" };
  }
  if (receivableEntries.some((entry) => entry.amountPaid > 0 && entry.balance > 0)) {
    return { kind: "partial", label: "Partially Paid", tone: "warning" };
  }
  const dueDates = outstanding.map((entry) => entry.dueDate).filter(Boolean).sort(comparePeriods);
  if (dueDates.length) {
    return { kind: "due", label: `Due on ${formatIsoDate(dueDates[0])}`, tone: "info" };
  }
  const sendDates = receivableEntries.map((entry) => entry.expectedSendDate).filter(Boolean).sort(comparePeriods);
  if (sendDates.length) {
    return { kind: "send", label: `Send on ${formatIsoDate(sendDates[0])}`, tone: "muted" };
  }
  return { kind: "open", label: "Open", tone: "info" };
}

function collectInvoiceNotifications() {
  const notifications = [];
  const today = todayIsoDate();
  const currentMonth = normalizeMonthKey(today);
  const invoiceMonth = previousMonthKey(currentMonth);

  getPartnerOptions({ includeArchived: false, includeArchivedTag: false }).forEach((option) => {
    const partner = option.value || option;
    if (invoiceMonth) {
      const entry = buildInvoiceTrackingEntry(partner, invoiceMonth, "receivable");
      if (entry.amountDue > 0 && !entry.invoiceDate && entry.expectedSendDate) {
        const daysUntilSend = diffDays(today, entry.expectedSendDate);
        if (daysUntilSend >= 0 && daysUntilSend <= 10) {
          notifications.push({
            id: `send-${partner}-${invoiceMonth}`,
            tone: daysUntilSend <= 5 ? "warning" : "info",
            tag: daysUntilSend <= 5 ? "5 Day Warning" : "10 Day Warning",
            title: `${partner} ${formatPeriodLabel(invoiceMonth)} invoice is coming up to send`,
            detail: `Expected send date ${formatIsoDate(entry.expectedSendDate)} · ${fmt(entry.amountDue)} open receivable`
          });
        }
      }
    }
  });

  (state.pInvoices || []).forEach((row) => {
    const partner = row.partner;
    if (!partner || isPartnerArchived(partner)) return;
    if (normalizeInvoiceTrackingKind(row.kind) !== "receivable") return;
    const period = normalizeMonthKey(row.period);
    const entry = buildInvoiceTrackingEntry(partner, period, "receivable");
    if (entry.balance <= 0 || entry.status.kind !== "past_due") return;
    notifications.push({
      id: `pastdue-${partner}-${period}`,
      tone: "danger",
      tag: "Past Due",
      title: `${partner} ${formatPeriodLabel(period)} is past due`,
      detail: `${entry.status.label} · ${fmt(entry.balance)} outstanding`
    });
  });

  return notifications.sort((a, b) => {
    const rank = { danger: 0, warning: 1, info: 2 };
    return (rank[a.tone] ?? 9) - (rank[b.tone] ?? 9) || a.title.localeCompare(b.title);
  });
}

function summarizePartnerOutstanding(partner) {
  const entries = getPartnerInvoiceTrackingEntries(partner);
  const today = todayIsoDate();
  let owesUs = 0;
  let weOwePartner = 0;
  let partialCount = 0;
  let maxPastDueDays = 0;
  let nearestSendWarning = null;
  let dueSoonDate = "";

  entries.forEach((entry) => {
    if (entry.kind === "receivable" && entry.amountDue > 0) {
      owesUs += entry.balance;
      if (entry.amountPaid > 0 && entry.balance > 0) partialCount += 1;
      if (entry.status.kind === "past_due" && entry.dueDate) {
        maxPastDueDays = Math.max(maxPastDueDays, diffDays(entry.dueDate, today));
      }
      if (!entry.invoiceDate && entry.expectedSendDate) {
        const daysUntilSend = diffDays(today, entry.expectedSendDate);
        if (daysUntilSend >= 0 && daysUntilSend <= 10 && (nearestSendWarning == null || daysUntilSend < nearestSendWarning)) {
          nearestSendWarning = daysUntilSend;
        }
      }
      if (entry.invoiceDate && entry.dueDate && entry.balance > 0) {
        const untilDue = diffDays(today, entry.dueDate);
        if (untilDue >= 0 && (!dueSoonDate || comparePeriods(entry.dueDate, dueSoonDate) < 0)) {
          dueSoonDate = entry.dueDate;
        }
      }
      return;
    }

    const partnerCredit = roundCurrency(entry.balance || 0);
    if (partnerCredit > 0) weOwePartner += partnerCredit;
  });

  owesUs = roundCurrency(owesUs);
  weOwePartner = roundCurrency(weOwePartner);
  const netPosition = roundCurrency(owesUs - weOwePartner);

  let status;
  if (maxPastDueDays > 0) {
    status = { tone: "danger", label: `Past Due by ${maxPastDueDays} Day${maxPastDueDays === 1 ? "" : "s"}` };
  } else if (partialCount > 0 && owesUs > 0) {
    status = { tone: "warning", label: "Partially Paid" };
  } else if (nearestSendWarning != null) {
    status = {
      tone: nearestSendWarning <= 5 ? "warning" : "info",
      label: nearestSendWarning <= 5 ? "5 Day Send Warning" : "10 Day Send Warning"
    };
  } else if (owesUs > 0 && dueSoonDate) {
    status = { tone: "info", label: `Due on ${formatIsoDate(dueSoonDate)}` };
  } else if (owesUs > 0) {
    status = { tone: "info", label: "Outstanding" };
  } else if (weOwePartner > 0) {
    status = { tone: "credit", label: "Veem Owes" };
  } else {
    status = { tone: "success", label: "No Open Balance" };
  }

  return {
    partner,
    owesUs,
    weOwePartner,
    netPosition,
    status,
    entries
  };
}

function renderInvoiceStatusTag(status) {
  if (!status) return "";
  return `<span class="invoice-status-pill is-${html(status.tone || "muted")}">${html(status.label)}</span>`;
}

function getPartnerOwesLabel(partner, { plural = false } = {}) {
  if (partner) return `${partner} Owes`;
  return plural ? "Partners Owe" : "Partner Owes";
}

function renderSettlementTag(summary) {
  const label = summary.netPosition > 0
    ? getPartnerOwesLabel(summary.partner)
    : summary.netPosition < 0
      ? "Veem Owes"
      : "Settled";
  const tone = summary.netPosition > 0 ? "info" : summary.netPosition < 0 ? "credit" : "success";
  return renderInvoiceStatusTag({ label, tone });
}

function renderSummaryAmountCell(amount) {
  if (amount > 0) return `<span class="summary-amount">${fmt(amount)}</span>`;
  return `<span class="summary-placeholder">—</span>`;
}

function getPartnerLifecycleStatus(partner, startPeriod = "", endPeriod = "") {
  if (!partner) return { label: "Active", tone: "active" };
  if (isPartnerArchived(partner)) return { label: "Archived", tone: "archived" };
  const rangeStart = startPeriod || LOOKER_IMPORT_PERIOD;
  const rangeEnd = endPeriod || rangeStart;
  const periods = enumeratePeriods(rangeStart, rangeEnd);
  const hasActivePeriod = !periods.length || periods.some((period) => isPartnerActiveForPeriod(state, partner, period));
  return hasActivePeriod
    ? { label: "Active", tone: "active" }
    : { label: "Closed", tone: "closed" };
}

function renderPartnerLifecycleBadge(status) {
  if (!status) return "";
  return `<span class="invoice-partner-status is-${html(status.tone || "active")}">${html(status.label)}</span>`;
}

function parseFixedFeePdfMemoEntry(line) {
  const match = String(line?.desc || "").match(/^(.*?)\s\(([\d,]+)x(\$[\d,]+\.\d{2})(?:\s+(imported))?\)$/);
  if (!match) return null;
  return {
    label: match[1].trim(),
    count: Number(match[2].replace(/,/g, "")),
    unitFee: match[3],
    imported: !!match[4],
    reason: line?.active === false && line?.inactiveReason ? line.inactiveReason : ""
  };
}

function parsePdfUnitCountFromDesc(line) {
  const desc = String(line?.desc || "").trim();
  const patterns = [
    /:\s*([\d,]+)\s+[A-Za-z/ ]+\s+x\s+(\$[\d,]+\.\d{2}(?:\/mo)?)/,
    /\(([\d,]+)\s*x\s*(\$[\d,]+\.\d{2}(?:\/mo)?)\s*(?:imported)?\)/,
    /\b([\d,]+)x(\$[\d,]+\.\d{2}(?:\/mo)?)/,
    /\bx\s+(\$[\d,]+\.\d{2}(?:\/mo)?)/
  ];
  for (const pattern of patterns) {
    const match = desc.match(pattern);
    if (!match) continue;
    if (match.length === 3) {
      return {
        count: Number(match[1].replace(/,/g, "")),
        unit: match[2]
      };
    }
    if (match.length === 2) {
      return {
        count: 1,
        unit: match[1]
      };
    }
  }
  return null;
}

function buildPdfGroupColumns(group, isReceivable) {
  const lines = group?.lines || [];
  const displayCharge = group.isInactive ? group.displayCharge : group.charge;
  const displayPay = group.isInactive ? group.displayPay : group.pay;
  const displayOffset = group.isInactive ? group.displayOffset : group.offset;
  const amountValue = Number(isReceivable
    ? (displayCharge || displayOffset || 0)
    : (displayPay || 0));

  const fixedFeeEntries = lines.map(parseFixedFeePdfMemoEntry);
  if (fixedFeeEntries.length && fixedFeeEntries.every(Boolean)) {
    const totalCount = fixedFeeEntries.reduce((sum, entry) => sum + Number(entry.count || 0), 0);
    const units = [...new Set(fixedFeeEntries.map((entry) => entry.unitFee).filter(Boolean))];
    return {
      product: group.label,
      perUnit: units.length === 1 ? units[0] : "Varies",
      count: totalCount > 0 ? totalCount.toLocaleString("en-US") : "—"
    };
  }

  const parsedEntries = lines.map(parsePdfUnitCountFromDesc).filter(Boolean);
  if (parsedEntries.length === lines.length && parsedEntries.length) {
    const totalCount = parsedEntries.reduce((sum, entry) => sum + Number(entry.count || 0), 0);
    const units = [...new Set(parsedEntries.map((entry) => entry.unit).filter(Boolean))];
    return {
      product: group.label,
      perUnit: units.length === 1 ? units[0] : "Varies",
      count: totalCount > 0 ? totalCount.toLocaleString("en-US") : "—"
    };
  }

  if (Number(group.activityTxnCount || 0) > 0) {
    return {
      product: group.label,
      perUnit: "—",
      count: Number(group.activityTxnCount).toLocaleString("en-US")
    };
  }

  if (Math.abs(amountValue) > 0) {
    return {
      product: group.label,
      perUnit: formatSignedInvoiceAmount(amountValue),
      count: "1"
    };
  }

  return {
    product: group.label,
    perUnit: "—",
    count: "—"
  };
}

function buildPdfGroupMemo(group) {
  const lines = group?.lines || [];
  if (!lines.length) return "&nbsp;";

  const fixedFeeEntries = lines.map(parseFixedFeePdfMemoEntry);
  if (fixedFeeEntries.every(Boolean)) {
    const aggregated = new Map();
    fixedFeeEntries.forEach((entry) => {
      const key = [entry.label, entry.unitFee, entry.imported ? "imported" : "", entry.reason].join("|");
      const existing = aggregated.get(key) || { ...entry, count: 0 };
      existing.count += entry.count;
      aggregated.set(key, existing);
    });
    return [...aggregated.values()].map((entry) => {
      const suffix = entry.imported ? " imported" : "";
      const reason = entry.reason ? ` [${entry.reason}]` : "";
      return html(`${entry.label} (${entry.count.toLocaleString("en-US")}x${entry.unitFee}${suffix})${reason}`);
    }).join("<br>");
  }

  const memoCounts = new Map();
  lines.forEach((line) => {
    const reason = line.active === false && line.inactiveReason ? ` [${line.inactiveReason}]` : "";
    const text = `${line.desc}${reason}`;
    memoCounts.set(text, (memoCounts.get(text) || 0) + 1);
  });
  return [...memoCounts.entries()].map(([text, count]) => html(count > 1 ? `${text} × ${count}` : text)).join("<br>");
}

function buildInvoicePdfDocument(doc) {
  const periodLabel = doc.periodLabel || formatPeriodRangeLabel(doc.periodStart || doc.period, doc.periodEnd || doc.period);
  const periodDateRange = doc.periodDateRange || formatPeriodDateRange(doc.periodStart || doc.period, doc.periodEnd || doc.period);
  const isReceivable = doc.kind !== "payable";
  const summaryLabel = isReceivable ? "Balance Due" : "Amount Due To Partner";
  const totalLabel = isReceivable ? "Total Due" : "Partner Payout Total";
  const adjustmentsLabel = isReceivable ? "Payments/Credits" : "Adjustments";
  const adjustmentsValue = isReceivable ? Number(doc.creditTotal || 0) : 0;
  const totalValue = Number(doc.amountDue || 0);
  const rows = (doc.groups || []).map((group) => {
    const displayCharge = group.isInactive ? group.displayCharge : group.charge;
    const displayPay = group.isInactive ? group.displayPay : group.pay;
    const displayOffset = group.isInactive ? group.displayOffset : group.offset;
    const columns = buildPdfGroupColumns(group, isReceivable);
    const amount = isReceivable
      ? displayCharge
        ? formatSignedInvoiceAmount(displayCharge)
        : displayOffset
          ? `(${fmt(displayOffset)})`
          : fmt(0)
      : displayPay
        ? formatSignedInvoiceAmount(displayPay)
        : fmt(0);
    return `
      <tr class="${group.isInactive ? "is-inactive" : ""}">
        <td>${html(columns.product)}</td>
        <td class="align-right">${html(columns.perUnit)}</td>
        <td class="align-right">${html(columns.count)}</td>
        <td class="align-right">${amount}</td>
      </tr>
    `;
  }).join("");
  const notesMarkup = doc.notes?.length
    ? `<ul class="pdf-notes-list">${doc.notes.map((note) => `<li>${html(note)}</li>`).join("")}</ul>`
    : `<div class="pdf-notes-empty"></div>`;

  return `<!DOCTYPE html>
  <html lang="en">
    <head>
      <meta charset="UTF-8">
      <title>${html(doc.partner)} ${html(doc.periodLabel || doc.period)} ${html(doc.title)}</title>
      <style>
        @page {
          size: letter;
          margin: 0.52in;
        }
        * {
          box-sizing: border-box;
        }
        body {
          margin: 0;
          color: #121212;
          background: #ffffff;
          font-family: "Avenir Next", "Helvetica Neue", Arial, sans-serif;
          -webkit-print-color-adjust: exact;
          print-color-adjust: exact;
        }
        .invoice-sheet {
          width: 100%;
          min-height: 10.4in;
          display: flex;
          flex-direction: column;
          gap: 18px;
        }
        .pdf-header {
          display: flex;
          justify-content: space-between;
          align-items: flex-start;
          gap: 28px;
        }
        .pdf-logo {
          font-size: 2rem;
          font-weight: 900;
          line-height: 1;
          letter-spacing: -0.04em;
          display: inline-flex;
          align-items: baseline;
          gap: 0;
        }
        .pdf-logo-v {
          color: #f28a1a;
        }
        .pdf-logo-rest {
          color: #1d73c9;
        }
        .pdf-company {
          margin-top: 34px;
          font-size: 0.9rem;
          font-weight: 600;
        }
        .pdf-blank-block {
          margin-top: 10px;
          border: 1.5px solid #d9d9d9;
          background: #ffffff;
        }
        .pdf-blank-block.small {
          width: 170px;
          height: 44px;
        }
        .pdf-blank-block.billto {
          width: 190px;
          height: 88px;
        }
        .pdf-blank-block.remit {
          width: 395px;
          height: 108px;
        }
        .pdf-label {
          font-size: 0.88rem;
          font-weight: 600;
          margin-bottom: 10px;
        }
        .pdf-right {
          min-width: 250px;
        }
        .pdf-title {
          text-align: right;
          font-size: 1.25rem;
          font-weight: 800;
          margin: 0 0 22px;
        }
        .pdf-meta-grid {
          display: grid;
          grid-template-columns: auto 1fr;
          gap: 11px 14px;
          align-items: center;
        }
        .pdf-meta-grid dt {
          font-size: 0.82rem;
          font-weight: 700;
          text-align: right;
          white-space: nowrap;
        }
        .pdf-meta-grid dd {
          margin: 0;
        }
        .pdf-inline-blank {
          height: 18px;
          border-bottom: 1.5px solid #1f1f1f;
          width: 100%;
        }
        .pdf-billto {
          margin-top: 18px;
        }
        .pdf-period-note {
          font-size: 0.76rem;
          color: #5d5d5d;
          margin-top: 10px;
          white-space: nowrap;
        }
        table {
          width: 100%;
          border-collapse: collapse;
        }
        .pdf-line-table {
          table-layout: fixed;
        }
        .pdf-line-table thead th {
          padding: 6px 8px;
          background: #e7e7e7;
          font-size: 0.78rem;
          font-weight: 800;
          text-align: center;
        }
        .pdf-line-table tbody td {
          padding: 4px 8px;
          border-bottom: 1px solid #ececec;
          font-size: 0.78rem;
          vertical-align: top;
          text-align: center;
        }
        .pdf-line-table tbody tr:last-child td {
          border-bottom: none;
        }
        .pdf-line-table tbody tr.is-inactive td {
          color: #9b938a;
          text-decoration: line-through;
          background: #f7f4ef;
        }
        .pdf-line-table .align-right {
          text-align: center;
        }
        .align-right {
          text-align: right;
          white-space: nowrap;
        }
        .pdf-bottom {
          margin-top: auto;
          display: flex;
          justify-content: space-between;
          align-items: flex-end;
          gap: 28px;
        }
        .pdf-notes {
          flex: 1;
        }
        .pdf-notes-title {
          margin-top: 16px;
          font-size: 0.82rem;
        }
        .pdf-notes-list {
          margin: 8px 0 0 16px;
          padding: 0;
          font-size: 0.78rem;
          line-height: 1.4;
        }
        .pdf-notes-empty {
          min-height: 18px;
        }
        .pdf-totals {
          width: 270px;
        }
        .pdf-totals-row {
          display: flex;
          justify-content: space-between;
          align-items: baseline;
          gap: 20px;
          font-size: 0.84rem;
          margin-bottom: 12px;
        }
        .pdf-totals-row strong {
          font-size: 0.98rem;
        }
        .pdf-total-row {
          display: flex;
          justify-content: space-between;
          align-items: baseline;
          gap: 20px;
          margin-top: 18px;
          padding-top: 12px;
          border-top: 2px solid #222222;
        }
        .pdf-total-row strong {
          font-size: 1.1rem;
        }
        .pdf-total-row .value {
          font-size: 1.12rem;
          font-weight: 800;
        }
      </style>
    </head>
    <body>
      <div class="invoice-sheet">
        <section class="pdf-header">
          <div>
            <div class="pdf-logo"><span class="pdf-logo-v">v</span><span class="pdf-logo-rest">eem</span></div>
            <div class="pdf-company">Veem Payments Inc</div>
            <div class="pdf-blank-block small"></div>
            <div class="pdf-billto">
              <div class="pdf-label">Bill To:</div>
              <div class="pdf-blank-block billto"></div>
            </div>
            <div class="pdf-period-note">${html(doc.partner)} · ${html(doc.title)} · ${html(periodLabel)} · ${html(periodDateRange)}</div>
          </div>
          <div class="pdf-right">
            <h1 class="pdf-title">Invoice</h1>
            <dl class="pdf-meta-grid">
              <dt>Invoice #</dt><dd><div class="pdf-inline-blank"></div></dd>
              <dt>Invoice Date</dt><dd><div class="pdf-inline-blank"></div></dd>
              <dt>Due Date</dt><dd><div class="pdf-inline-blank"></div></dd>
              <dt>Terms</dt><dd><div class="pdf-inline-blank"></div></dd>
              <dt>P.O. Number:</dt><dd><div class="pdf-inline-blank"></div></dd>
            </dl>
          </div>
        </section>

        <section>
          <table class="pdf-line-table">
            <thead>
              <tr>
                <th style="width: 40%">Product</th>
                <th style="width: 20%" class="align-right">Per Unit $</th>
                <th style="width: 20%" class="align-right">Count</th>
                <th style="width: 20%" class="align-right">Amount</th>
              </tr>
            </thead>
            <tbody>
              ${rows || `<tr><td colspan="4">No invoice lines were generated.</td></tr>`}
            </tbody>
          </table>
        </section>

        <section class="pdf-bottom">
          <div class="pdf-notes">
            <div class="pdf-blank-block remit"></div>
            <div class="pdf-notes-title">Notes</div>
            ${notesMarkup}
          </div>
          <div class="pdf-totals">
            <div class="pdf-totals-row"><span>${html(adjustmentsLabel)}</span><span>USD ${adjustmentsValue.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</span></div>
            <div class="pdf-totals-row"><strong>${html(summaryLabel)}</strong><span>USD ${totalValue.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</span></div>
            <div class="pdf-total-row"><strong>${html(totalLabel)}</strong><span class="value">USD ${totalValue.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</span></div>
          </div>
        </section>
      </div>
      <script>
        window.addEventListener("load", function () {
          window.setTimeout(function () {
            window.print();
          }, 180);
        });
        window.addEventListener("afterprint", function () {
          window.close();
        });
      <\/script>
    </body>
  </html>`;
}

function exportInvoicePdf(docKind = "") {
  if (!state.inv) return;
  const documents = buildInvoiceDocuments(state.inv);
  const targetDoc = docKind
    ? documents.find((doc) => doc.kind === docKind)
    : documents[0];
  if (!targetDoc) {
    showToast("No invoice available", "No document was generated for this side of the billing period.", "warning");
    return;
  }
  const popup = window.open("", "_blank", "width=1024,height=1320");
  if (!popup) {
    showToast("PDF export blocked", "Allow pop-ups for localhost to open the printable invoice.", "warning");
    return;
  }
  popup.document.open();
  popup.document.write(buildInvoicePdfDocument(targetDoc));
  popup.document.close();
}

function clearCurrentInvoiceSelection({ renderNow = true } = {}) {
  state.sp = "";
  state.inv = null;
  state.invoiceExplorer = null;
  state.checkerReport = null;
  state.checkerStatus = "idle";
  state.checkerError = "";
  state.invoiceArtifactStatus = "idle";
  state.invoiceArtifactError = "";
  state.invoiceArtifactRecord = null;
  state.privateInvoiceLinkStatus = "idle";
  state.privateInvoiceLinkError = "";
  state.privateInvoiceLinkResult = null;
  if (renderNow) render();
}

function restoreSnapshot(data) {
  const { snapshot } = migrateSnapshot(data);
  DATA_KEYS.forEach((key) => {
    if (snapshot[key] != null) state[key] = snapshot[key];
  });
  persistAndRender();
  logWorkbookChange("restore_snapshot", "Restored workbook data from a backup file.", { section: "workbook" });
}

function replacePartnerRows(currentRows, defaultRows, partners) {
  const partnerSet = new Set(partners);
  const safeCurrentRows = Array.isArray(currentRows) ? currentRows : [];
  return [
    ...safeCurrentRows.filter((row) => !row || !partnerSet.has(row.partner)),
    ...defaultRows.filter((row) => partnerSet.has(row.partner))
  ];
}

function migrateSnapshot(saved) {
  if (!saved || typeof saved !== "object") {
    return { snapshot: saved, changed: false };
  }
  const version = Number(saved._version || 0);

  const defaults = createInitialWorkbookData();
  const snapshot = { ...saved };

  if (version < 4) {
    snapshot.off = replacePartnerRows(saved.off, defaults.off, ["Stampli", "Skydo"]);
    snapshot.vol = replacePartnerRows(saved.vol, defaults.vol, ["Maplewave", "Skydo", "Stampli"]);
    snapshot.fxRates = replacePartnerRows(saved.fxRates, defaults.fxRates, ["Stampli"]);
    snapshot.rs = replacePartnerRows(saved.rs, defaults.rs, ["Stampli"]);
    snapshot.mins = replacePartnerRows(saved.mins, defaults.mins, ["Stampli"]);
    snapshot.plat = replacePartnerRows(saved.plat, defaults.plat, ["Skydo"]);
    snapshot.revf = replacePartnerRows(saved.revf, defaults.revf, ["Skydo"]);
  }

  if (version < 5) {
    snapshot.ltxn = defaults.ltxn;
    snapshot.lrev = defaults.lrev;
    snapshot.lva = defaults.lva;
    snapshot.lrs = defaults.lrs;
  }

  if (version < 6) {
    snapshot.lva = (Array.isArray(snapshot.lva) ? snapshot.lva : defaults.lva).map((row) => ({ closedAccounts: 0, ...row }));
  }

  if (version < 7) {
    snapshot.ltxn = defaults.ltxn;
    snapshot.lrev = defaults.lrev;
    snapshot.lva = defaults.lva;
    snapshot.lrs = defaults.lrs;
  }

  if (version < 8) {
    snapshot.ltxn = defaults.ltxn;
    snapshot.lrev = defaults.lrev;
    snapshot.lva = defaults.lva;
    snapshot.lrs = defaults.lrs;
  }

  if (version < 9) {
    snapshot.mins = replacePartnerRows(saved.mins, defaults.mins, ["Everflow", "Halorecruiting", "Q2"]);
  }

  if (version < 10) {
    snapshot.mins = replacePartnerRows(snapshot.mins ?? saved.mins, defaults.mins, ["Shepherd"]);
  }

  if (version < 11) {
    snapshot.rs = replacePartnerRows(snapshot.rs ?? saved.rs, defaults.rs, ["Shepherd"]);
    snapshot.mins = replacePartnerRows(snapshot.mins ?? saved.mins, defaults.mins, ["Shepherd"]);
  }

  if (version < 12) {
    snapshot.mins = replacePartnerRows(snapshot.mins ?? saved.mins, defaults.mins, ["Finastra", "Magaya", "Fulfil"]);
  }

  if (version < 13) {
    snapshot.lfxp = defaults.lfxp;
  }

  if (version < 14) {
    snapshot.lfxp = defaults.lfxp;
  }

  if (version < 15) {
    snapshot.lfxp = defaults.lfxp;
  }

  if (version < 16) {
    snapshot.ltxn = defaults.ltxn;
    snapshot.lrev = defaults.lrev;
    snapshot.lrs = defaults.lrs;
    snapshot.lfxp = defaults.lfxp;
  }

  if (version < 17) {
    snapshot.ltxn = defaults.ltxn;
    snapshot.lrev = defaults.lrev;
    snapshot.lrs = defaults.lrs;
    snapshot.lfxp = defaults.lfxp;
  }

  if (version < 18) {
    snapshot.workspaceMode = undefined;
    snapshot.workspaceLabel = undefined;
  }

  if (version < 20) {
    snapshot.pActive = Array.isArray(snapshot.pActive) ? snapshot.pActive : defaults.pActive;
  }

  if (version < 21) {
    snapshot.cap = (Array.isArray(snapshot.cap) ? snapshot.cap : defaults.cap).map((row) => ({ startDate: "", endDate: "", ...row }));
    snapshot.vaFees = (Array.isArray(snapshot.vaFees) ? snapshot.vaFees : defaults.vaFees).map((row) => ({ startDate: "", endDate: "", ...row }));
    snapshot.impl = (Array.isArray(snapshot.impl) ? snapshot.impl : defaults.impl).map((row) => ({ startDate: "", endDate: "", ...row }));
  }

  if (version < 22) {
    snapshot.pArchived = Array.isArray(snapshot.pArchived) ? snapshot.pArchived : defaults.pArchived;
  }

  if (version < 23) {
    snapshot.pBilling = Array.isArray(snapshot.pBilling) ? snapshot.pBilling : defaults.pBilling;
    snapshot.pInvoices = Array.isArray(snapshot.pInvoices) ? snapshot.pInvoices : defaults.pInvoices;
  }

  if (version < 24) {
    snapshot.ps = (Array.isArray(snapshot.ps) ? snapshot.ps : defaults.ps).filter((partner) => norm(partner) !== "tabapay");
    snapshot.off = (Array.isArray(snapshot.off) ? snapshot.off : defaults.off).filter((row) => norm(row.partner) !== "tabapay");
    snapshot.vol = (Array.isArray(snapshot.vol) ? snapshot.vol : defaults.vol).filter((row) => norm(row.partner) !== "tabapay");
    snapshot.pBilling = (Array.isArray(snapshot.pBilling) ? snapshot.pBilling : defaults.pBilling).filter((row) => norm(row.partner) !== "tabapay");
    snapshot.pInvoices = (Array.isArray(snapshot.pInvoices) ? snapshot.pInvoices : defaults.pInvoices).filter((row) => norm(row.partner) !== "tabapay");
    snapshot.pActive = (Array.isArray(snapshot.pActive) ? snapshot.pActive : defaults.pActive).filter((row) => norm(row.partner) !== "tabapay");
    snapshot.pArchived = (Array.isArray(snapshot.pArchived) ? snapshot.pArchived : defaults.pArchived).filter((partner) => norm(partner) !== "tabapay");
    const existingProviderRows = (Array.isArray(snapshot.pCosts) ? snapshot.pCosts : defaults.pCosts).filter((row) => norm(row.provider) !== "tabapay").map((row) => ({ startDate: "", endDate: "", feeType: "Per Item", note: "", ...row }));
    const tabaProviderRows = defaults.pCosts.filter((row) => norm(row.provider) === "tabapay");
    snapshot.pCosts = [...existingProviderRows, ...tabaProviderRows];
  }

  if (version < 25) {
    snapshot.pBilling = mergePartnerBillingDefaults(snapshot.pBilling, defaults.pBilling);
  }

  if (version < 27) {
    snapshot.pBilling = mergePartnerBillingDefaults(snapshot.pBilling, defaults.pBilling).map((row) => ({
      ...row,
      contractStartDate: normalizeIsoDate(row.contractStartDate) || getPartnerContractStartDate(row.partner, snapshot),
      goLiveDate: normalizeIsoDate(row.goLiveDate) || getPartnerGoLiveDate(row.partner, snapshot)
    }));
    snapshot.impl = (Array.isArray(snapshot.impl) ? snapshot.impl : defaults.impl).map((row) => ({
      ...row,
      startDate: row.feeType === "Implementation"
        ? (normalizeIsoDate(row.startDate) || getPartnerContractStartDate(row.partner, { ...snapshot, pBilling: snapshot.pBilling }) || normalizeIsoDate(row.goLiveDate))
        : (row.startDate || "")
    }));
  }

  if (version < 28) {
    snapshot.pBilling = mergePartnerBillingDefaults(snapshot.pBilling, defaults.pBilling).map((row) => ({
      ...row,
      notYetLive: !!row.notYetLive
    }));
  }

  if (version < 29) {
    snapshot.pBilling = mergePartnerBillingDefaults(snapshot.pBilling, defaults.pBilling).map((row) => ({
      ...row,
      notYetLive: !!row.notYetLive,
      integrationStatus: row.integrationStatus || ""
    }));
  }

  if (version < 30) {
    const hubspotPartners = new Set([
      "altpay",
      "athena",
      "blindpay",
      "capi",
      "cellpay",
      "clearshift",
      "graph finance",
      "lianlian",
      "maplewave",
      "nomad",
      "nsave",
      "nuvion",
      "remittanceshub",
      "repay",
      "skydo",
      "triplea",
      "yeepay"
    ]);
    const defaultsByPartner = new Map((defaults.pBilling || []).map((row) => [norm(row.partner), row]));
    snapshot.pBilling = mergePartnerBillingDefaults(snapshot.pBilling, defaults.pBilling).map((row) => {
      const partnerKey = norm(row.partner);
      if (!hubspotPartners.has(partnerKey)) return row;
      const defaultRow = defaultsByPartner.get(partnerKey);
      if (!defaultRow) return row;
      return {
        ...row,
        goLiveDate: normalizeIsoDate(defaultRow.goLiveDate) || "",
        notYetLive: !!defaultRow.notYetLive,
        integrationStatus: defaultRow.integrationStatus || ""
      };
    });
  }

  if (version < 31) {
    snapshot.pBilling = mergePartnerBillingDefaults(snapshot.pBilling, defaults.pBilling).map((row) => ({
      ...row,
      lateFeePercentMonthly: Number(row.lateFeePercentMonthly || 0),
      lateFeeStartDays: Number(row.lateFeeStartDays || 0),
      serviceSuspensionDays: Number(row.serviceSuspensionDays || 0),
      lateFeeTerms: row.lateFeeTerms || ""
    }));
  }

  if (version < 32) {
    snapshot.accessLogs = Array.isArray(snapshot.accessLogs) ? snapshot.accessLogs : [];
    snapshot.adminSettings = buildDefaultAdminSettings(snapshot.adminSettings || defaults.adminSettings);
  }

  if (version < 33) {
    snapshot.impl = (Array.isArray(snapshot.impl) ? snapshot.impl : defaults.impl).map((row) => normalizeImplementationRow(row));
  }

  if (version < 34) {
    const removedPartners = new Set(["nuvei", "paynearme", "highnote"]);
    const renamedPartners = new Map([
      ["LightNet", "Lightnet"],
      ["MultiGate", "Multigate"],
      ["YeePay", "Yeepay"]
    ]);
    const normalizePartnerName = (value) => renamedPartners.get(value) || value;
    const keepPartner = (value) => !removedPartners.has(norm(value));
    const rewritePartnerRows = (rows) => (Array.isArray(rows) ? rows : []).flatMap((row) => {
      if (typeof row === "string") {
        if (!keepPartner(row)) return [];
        return [normalizePartnerName(row)];
      }
      if (!row || typeof row !== "object") return [row];
      if ("partner" in row) {
        if (!keepPartner(row.partner)) return [];
        return [{ ...row, partner: normalizePartnerName(row.partner) }];
      }
      return [row];
    });
    snapshot.ps = rewritePartnerRows(snapshot.ps ?? defaults.ps);
    snapshot.pBilling = rewritePartnerRows(snapshot.pBilling ?? defaults.pBilling);
    snapshot.pArchived = rewritePartnerRows(snapshot.pArchived ?? defaults.pArchived);
    snapshot.pActive = rewritePartnerRows(snapshot.pActive ?? defaults.pActive);
    snapshot.pInvoices = rewritePartnerRows(snapshot.pInvoices ?? defaults.pInvoices);
    snapshot.off = rewritePartnerRows(snapshot.off ?? defaults.off);
    snapshot.vol = rewritePartnerRows(snapshot.vol ?? defaults.vol);
    snapshot.mins = rewritePartnerRows(snapshot.mins ?? defaults.mins);
    snapshot.plat = rewritePartnerRows(snapshot.plat ?? defaults.plat);
    snapshot.revf = rewritePartnerRows(snapshot.revf ?? defaults.revf);
    snapshot.impl = rewritePartnerRows(snapshot.impl ?? defaults.impl);
    snapshot.vaFees = rewritePartnerRows(snapshot.vaFees ?? defaults.vaFees);
    snapshot.cap = rewritePartnerRows(snapshot.cap ?? defaults.cap);
    snapshot.ltxn = rewritePartnerRows(snapshot.ltxn ?? defaults.ltxn);
    snapshot.lrev = rewritePartnerRows(snapshot.lrev ?? defaults.lrev);
    snapshot.lrs = rewritePartnerRows(snapshot.lrs ?? defaults.lrs);
    snapshot.lfxp = rewritePartnerRows(snapshot.lfxp ?? defaults.lfxp);
    snapshot.lva = rewritePartnerRows(snapshot.lva ?? defaults.lva);
  }

  if (version < 35) {
    // 2026-04-21: Dedicated Stampli USD Abroad / credit-complete Looker feeds were
    // confirmed incorrect. partner_offline_billing is authoritative for those
    // transactions. Purge any lingering rows so the calc can no longer be
    // contaminated by a previously ingested snapshot.
    snapshot.ltxn = Array.isArray(snapshot.ltxn)
      ? snapshot.ltxn.filter((row) => !(row && UNTRUSTED_DIRECT_INVOICE_SOURCES.has(row.directInvoiceSource)))
      : defaults.ltxn;
  }

  snapshot._version = STORAGE_VERSION;
  snapshot._saved = new Date().toISOString();
  return { snapshot, changed: version < STORAGE_VERSION };
}

function resetToDefaults() {
  if (saveTimer.id) clearTimeout(saveTimer.id);
  Object.assign(state, createInitialWorkbookData());
  state.sp = "";
  state.pv = "";
  state.inv = null;
  state.invoiceExplorer = null;
  state.cf = "";
  state.fxSearch = "";
  state.cText = "";
  state.cStatus = "idle";
  state.cError = "";
  state.cParsed = null;
  state.cName = "";
  state.cVerifyPartner = "";
  state.cFileName = "";
  state.cPendingFile = null;
  state.cExtractStatus = "idle";
  state.cDetectedIncremental = false;
  state.cImported = false;
  state.cImportBehavior = "override";
  state.cDiff = null;
  state.confirmDel = false;
  state.lastSaved = null;
  state.lastSavedAt = null;
  state.lookerImportAudit = null;
  state.perStart = LOOKER_IMPORT_PERIOD;
  state.perEnd = LOOKER_IMPORT_PERIOD;
  state.useDateRange = false;
  state.billingSummaryPartner = "";
  state.lookerImportType = MANUAL_LOOKER_FILE_OPTIONS[0].value;
  state.lookerImportPeriod = LOOKER_IMPORT_PERIOD;
  state.lookerImportText = "";
  state.lookerImportStatus = "idle";
  state.lookerImportError = "";
  state.lookerImportResult = null;
  state.invoiceArtifactStatus = "idle";
  state.invoiceArtifactError = "";
  state.invoiceArtifactRecord = null;
  state.privateInvoiceLinkStatus = "idle";
  state.privateInvoiceLinkError = "";
  state.privateInvoiceLinkResult = null;
  state.lookerImportFileName = "";
  state.lookerImportPendingFile = null;
  state.lookerImportContext = {};
  state.lookerImportedDetailRows = [];
  state.accessLogs = [];
  state.adminSettings = buildDefaultAdminSettings();
  localStorage.removeItem(STORAGE_KEY);
  state.workspaceMode = isSharedWorkbookEnabled() ? "shared" : "local";
  state.workspaceLabel = getWorkspaceLabel();
  if (!isSharedWorkbookEnabled()) {
    render();
    logWorkbookChange("reset_defaults", "Reset workbook data to defaults.", { section: "workbook" });
    return;
  }
  persistAndRender();
  logWorkbookChange("reset_defaults", "Reset workbook data to defaults.", { section: "workbook" });
}

function readBoundValue(key) {
  const el = root.querySelector(`[data-bind="${key}"]`);
  if (!el) return state[key];
  if (el.type === "checkbox") return !!el.checked;
  return el.value;
}

function setBoundValue(key, value) {
  state[key] = value;
}

function renderOptions(opts, selected) {
  return (opts || []).map((option) => {
    const value = typeof option === "string" ? option : option.value;
    const label = typeof option === "string" ? option : option.label;
    return `<option value="${html(value)}"${String(value) === String(selected ?? "") ? " selected" : ""}>${html(label)}</option>`;
  }).join("");
}

function renderDatalistOptions(opts) {
  return (opts || []).map((option) => {
    const value = typeof option === "string" ? option : option.value;
    const label = typeof option === "string" ? option : option.label;
    return `<option value="${html(value)}" label="${html(label)}"></option>`;
  }).join("");
}

function resolvePartnerName(value, opts = getPartnerOptions({ includeArchived: false })) {
  const raw = String(value || "").trim();
  if (!raw) return "";
  const match = (opts || []).find((option) => {
    const partnerValue = typeof option === "string" ? option : option.value;
    return norm(partnerValue) === norm(raw);
  });
  if (!match) return "";
  return typeof match === "string" ? match : match.value;
}

function contractPartnerLookupKey(value) {
  const ignoredTokens = new Set([
    "inc",
    "llc",
    "ltd",
    "limited",
    "corp",
    "corporation",
    "company",
    "co",
    "network",
    "networks",
    "payment",
    "payments",
    "pay",
    "group",
    "holdings",
    "holdco",
    "technology",
    "technologies",
    "solutions",
    "global",
    "international"
  ]);
  const tokens = String(value || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .split(/\s+/)
    .filter(Boolean)
    .filter((token) => !ignoredTokens.has(token));
  return tokens.join(" ");
}

function suggestVerifyPartnerName(value, opts = getPartnerOptions({ includeArchived: true, includeArchivedTag: false })) {
  const direct = resolvePartnerName(value, opts);
  if (direct) return direct;
  const targetKey = contractPartnerLookupKey(value);
  if (!targetKey) return "";
  const optionEntries = (opts || []).map((option) => {
    const partnerValue = typeof option === "string" ? option : option.value;
    return { value: partnerValue, key: contractPartnerLookupKey(partnerValue) };
  });
  const exact = optionEntries.find((entry) => entry.key === targetKey);
  if (exact) return exact.value;
  const partial = optionEntries.find((entry) => entry.key && (entry.key.includes(targetKey) || targetKey.includes(entry.key)));
  return partial ? partial.value : "";
}

function renderInfoTip(text) {
  if (!text) return "";
  return `
    <span class="info-tip" tabindex="0" aria-label="${html(text)}">
      <span class="info-tip-icon" aria-hidden="true">i</span>
      <span class="info-tip-bubble">${html(text)}</span>
    </span>
  `;
}

function renderLabelWithInfo(label, info, { className = "label" } = {}) {
  return `<span class="label-row"><span class="${className}">${html(label)}</span>${renderInfoTip(info)}</span>`;
}

function renderArchivedTag(partner) {
  return isPartnerArchived(partner) ? `<span class="archived-tag">ARCHIVED</span>` : "";
}

function positionInfoTip(tip) {
  if (!tip) return;
  tip.classList.remove("is-left", "is-right", "is-below");
  const bubble = tip.querySelector(".info-tip-bubble");
  if (!bubble) return;
  const viewportPadding = 16;
  const initialRect = bubble.getBoundingClientRect();
  if (initialRect.left < viewportPadding) {
    tip.classList.add("is-left");
  } else if (initialRect.right > window.innerWidth - viewportPadding) {
    tip.classList.add("is-right");
  }
  const adjustedRect = bubble.getBoundingClientRect();
  if (adjustedRect.top < viewportPadding) {
    tip.classList.add("is-below");
  }
}

function renderInputCell(section, row, col, readOnly) {
  const value = row[col.key];
  if (readOnly) {
    if (col.type === "bool") return value ? "Y" : "N";
    if (col.key === "partner") return `${html(value ?? "")} ${renderArchivedTag(value)}`;
    return html(value ?? "");
  }
  if (col.type === "bool") {
    return `<input class="table-input" type="checkbox" data-section="${section}" data-id="${row.id}" data-key="${col.key}" data-field-type="bool"${value ? " checked" : ""}>`;
  }
  if (col.type === "select") {
    const control = `<select class="table-select" data-section="${section}" data-id="${row.id}" data-key="${col.key}" data-field-type="select"><option value=""></option>${renderOptions(col.opts, value)}</select>`;
    if (col.key === "partner") {
      return `<div class="partner-cell">${control}${renderArchivedTag(value)}</div>`;
    }
    return control;
  }
  const inputType = col.type === "number" ? "number" : col.type === "month" ? "month" : "text";
  const fieldType = col.type === "number" ? "number" : col.type === "month" ? "month" : "text";
  return `<input class="table-input" type="${inputType}" value="${html(value ?? "")}" data-section="${section}" data-id="${row.id}" data-key="${col.key}" data-field-type="${fieldType}"${col.step ? ` step="${col.step}"` : ""}>`;
}

function arePageTableRowsExpanded(pageId = state.tab, defaultOpen = false) {
  return state.pageTableRowsExpanded?.[pageId] ?? defaultOpen;
}

function areTableRowsExpanded(rowsKey = "", pageId = state.tab, defaultOpen = false) {
  if (!rowsKey) return arePageTableRowsExpanded(pageId, defaultOpen);
  return state.tableRowsExpanded?.[rowsKey] ?? arePageTableRowsExpanded(pageId, defaultOpen);
}

function renderPageTableToggle(pageId = state.tab) {
  const expanded = arePageTableRowsExpanded(pageId);
  return `
    <div class="button-row page-table-toggle-row">
      <button class="button ghost small" data-action="toggle-page-table-rows" data-page="${pageId}">
        ${expanded ? "Minimize All Rows" : "Maximize All Rows"}
      </button>
    </div>
  `;
}

function getCurrentPageTableRowsKeys(pageId = state.tab) {
  const keys = [];
  if (pageId === "invoice") {
    keys.push("billing-summary");
    return keys;
  }
  if (pageId === "partner") {
    if (!state.pv) return keys;
    const partner = state.pv;
    [
      `partner-activity-${partner}`,
      `partner-billing-${partner}`,
      `partner-off-${partner}`,
      `partner-vol-${partner}`,
      `partner-rs-${partner}`,
      `partner-min-${partner}`,
      `partner-plat-${partner}`,
      `partner-revf-${partner}`,
      `partner-impl-${partner}`,
      `partner-cap-${partner}`
    ].forEach((key) => keys.push(key));
    if (state.surch.some((row) => row.partner === partner)) keys.push(`partner-surch-${partner}`);
    if (state.fxRates.some((row) => row.partner === partner)) keys.push(`partner-fx-${partner}`);
    if (state.vaFees.some((row) => row.partner === partner)) keys.push(`partner-va-${partner}`);
    return keys;
  }
  if (pageId === "looker") {
    ["looker-txns", "looker-rev", "looker-rs", "looker-va"].forEach((key) => keys.push(key));
    return keys;
  }
  if (pageId === "import") {
    ["preview-offline", "preview-volume", "preview-minimums"].forEach((key) => {
      if (state.cParsed) keys.push(key);
    });
    return keys;
  }
  return keys;
}

function getCurrentPageSectionStateKeys(pageId = state.tab) {
  const keys = [];
  if (pageId === "invoice") {
    keys.push(sectionKey("billing-summary"));
    if (state.inv?.notes?.length) {
      keys.push(sectionKey(`invoice-notes:${state.inv.partner}:${state.inv.period}`));
    }
    (state.inv?.groups || []).forEach((group) => {
      keys.push(invoiceGroupSectionKey(group.id));
      keys.push(sectionKey(`invoice-group-revenue-${group.id}`));
      keys.push(sectionKey(`invoice-group-volume-${group.id}`));
    });
    return keys;
  }
  if (pageId === "partner") {
    if (!state.pv) return keys;
    const partner = state.pv;
    [
      `partner-activity-${partner}`,
      `partner-billing-${partner}`,
      `partner-off-${partner}`,
      `partner-vol-${partner}`,
      `partner-rs-${partner}`,
      `partner-min-${partner}`,
      `partner-plat-${partner}`,
      `partner-revf-${partner}`,
      `partner-impl-${partner}`,
      `partner-cap-${partner}`
    ].forEach((key) => keys.push(sectionKey(key)));
    if (state.surch.some((row) => row.partner === partner)) keys.push(sectionKey(`partner-surch-${partner}`));
    if (state.fxRates.some((row) => row.partner === partner)) keys.push(sectionKey(`partner-fx-${partner}`));
    if (state.vaFees.some((row) => row.partner === partner)) keys.push(sectionKey(`partner-va-${partner}`));
    return keys;
  }
  if (pageId === "looker") {
    keys.push(sectionKey("looker-gaps"));
    keys.push(sectionKey("looker-last-upload"));
    ["looker-txns", "looker-rev", "looker-rs", "looker-va"].forEach((key) => keys.push(sectionKey(key)));
    return keys;
  }
  if (pageId === "import") {
    ["preview-offline", "preview-volume", "preview-minimums"].forEach((key) => {
      if (state.cParsed) keys.push(sectionKey(key));
    });
    return keys;
  }
  return keys;
}

function renderDataTable({ section, cols, rows, readOnly = false, filterFn = null, emptyLabel = "No rows", rowsKey = "", pageId = state.tab }) {
  const list = filterFn ? rows.filter(filterFn) : rows;
  const rowsExpanded = rowsKey ? areTableRowsExpanded(rowsKey, pageId) : true;
  const colSpan = cols.length + (readOnly ? 0 : 1);
  const bodyMarkup = list.length === 0
    ? `<tr><td colspan="${colSpan}" class="empty-state">${html(emptyLabel)}</td></tr>`
      : !rowsExpanded
      ? ""
      : list.map((row) => `
            <tr class="${row.partner && isPartnerArchived(row.partner) ? "is-archived" : ""}">
              ${cols.map((col) => `<td>${renderInputCell(section, row, col, readOnly)}</td>`).join("")}
              ${readOnly ? "" : `<td><button class="button ghost small" data-action="delete-row" data-section="${section}" data-id="${row.id}">Delete</button></td>`}
            </tr>
          `).join("");
  return `
    <div class="table-wrap">
      <table class="data-table">
        <thead>
          <tr>
            ${cols.map((col) => `<th style="min-width:${col.w || 80}px">${html(col.label)}</th>`).join("")}
            ${readOnly ? "" : "<th style=\"min-width:72px\">Actions</th>"}
          </tr>
        </thead>
        <tbody>
          ${bodyMarkup}
        </tbody>
      </table>
    </div>
    ${readOnly ? "" : `<div class="button-row" style="margin-top:12px"><button class="button secondary small" data-action="add-row" data-section="${section}">Add Row</button></div>`}
  `;
}

function sectionKey(key) {
  return `section:${key}`;
}

function isSectionOpen(key, defaultOpen = false) {
  return state.openSections[key] ?? defaultOpen;
}

function renderSection({ key, title, badge = "", content, note = "", defaultOpen = false, tableRowsKey = "", pageId = state.tab }) {
  const sectionStateKey = sectionKey(key);
  const useTableRowsToggle = !!tableRowsKey;
  const sectionExpanded = isSectionOpen(sectionStateKey, defaultOpen);
  const rowsExpanded = useTableRowsToggle ? areTableRowsExpanded(tableRowsKey, pageId, defaultOpen) : sectionExpanded;
  const open = useTableRowsToggle ? rowsExpanded : sectionExpanded;
  return `
    <div class="section-shell panel">
      <button
        class="section-toggle"
        data-action="${useTableRowsToggle ? "toggle-section-rows" : "toggle-section"}"
        ${useTableRowsToggle ? `data-rows-key="${tableRowsKey}" data-page="${pageId}"` : `data-key="${sectionStateKey}" data-default-open="${defaultOpen ? "true" : "false"}"`}
      >
        <span class="section-headline">
          <strong>${html(title)}</strong>
          ${badge ? `<span class="helper-pill">${html(String(badge))}</span>` : ""}
          ${renderInfoTip(note)}
        </span>
        <span class="toggle-indicator">${rowsExpanded ? "▾" : "▸"}</span>
      </button>
      ${open ? `<div class="section-body">${content}</div>` : ""}
    </div>
  `;
}

function categoryClass(category) {
  const map = {
    "Offline": "cat-offline",
    "Volume": "cat-volume",
    "Surcharge": "cat-surcharge",
    "FX": "cat-fx",
    "Revenue": "cat-revenue",
    "Rev Share": "cat-rev-share",
    "Reversal": "cat-reversal",
    "Platform": "cat-platform",
    "Minimum": "cat-minimum",
    "Impl Fee": "cat-impl-fee",
    "Impl Credit": "cat-impl-credit",
    "Virtual Acct": "cat-virtual-acct",
    "Account Setup": "cat-account-setup",
    "Settlement": "cat-settlement"
  };
  return map[category] || "cat-offline";
}

function formatCompareValue(value) {
  if (typeof value === "number") {
    if (value > 0 && value < 1) return fmtPct(value);
    return fmt(value);
  }
  return html(value ?? "");
}

function countSelectedContractImportChanges() {
  if (!state.cImportPlan) return 0;
  return state.cImportPlan.changes.filter((change) => state.cSelectedImportRows[change.id] !== false).length;
}

function renderContractChangeActionTag(action) {
  const styles = {
    add: "background:#dff4e7;color:#1c5d3c",
    replace: "background:#f7edc8;color:#7c5312",
    remove: "background:#f8d9d3;color:#a33b29"
  };
  const label = action === "replace" ? "Replace" : action === "remove" ? "Remove" : "Add";
  return `<span class="helper-pill" style="${styles[action] || styles.add}">${label}</span>`;
}

function renderContractImportPlan(plan) {
  if (!plan) return "";
  const selectedCount = countSelectedContractImportChanges();
  const description = plan.behavior === "append"
    ? "These are the new fee rows that would be added. Existing workbook rows will stay untouched, so verify can still show differences until you use Override existing rows."
    : "These are the workbook changes that would be applied. Replacements show the current workbook row beside the parsed contract row.";
  return `
    <div class="card">
      <div class="section-header compact">
        <div>
          <h3 class="section-title">Planned Workbook Changes</h3>
          <div class="section-note">${html(description)}</div>
        </div>
        <div class="button-row">
          <span class="helper-pill">${selectedCount} selected</span>
          <span class="helper-pill">${plan.counts.add || 0} add</span>
          ${plan.behavior === "override" ? `<span class="helper-pill">${plan.counts.replace || 0} replace</span><span class="helper-pill">${plan.counts.remove || 0} remove</span>` : ""}
          <button class="button ghost" data-action="select-all-contract-changes"${plan.changes.length ? "" : " disabled"}>Select All</button>
          <button class="button ghost" data-action="deselect-all-contract-changes"${plan.changes.length ? "" : " disabled"}>Deselect All</button>
        </div>
      </div>
      ${renderPreviewTable(plan.changes, ["Include", "Action", "Category", "Parsed Contract Row", "Workbook Row"], (change) => `
        <tr>
          <td style="width:80px"><input type="checkbox" data-action="toggle-contract-change" data-change-id="${html(change.id)}"${state.cSelectedImportRows[change.id] !== false ? " checked" : ""}></td>
          <td>${renderContractChangeActionTag(change.action)}</td>
          <td>${html(change.sectionLabel)}</td>
          <td>${change.newRow ? html(describeContractImportRow(change.section, change.newRow)) : "—"}</td>
          <td>${change.existingRow ? html(describeContractImportRow(change.section, change.existingRow)) : "—"}</td>
        </tr>
      `, plan.behavior === "append" ? "No new rows would be added." : "No workbook changes would be made.", "contract-import-plan", "import")}
    </div>
  `;
}

function activityRowKey(row) {
  return [
    row.period,
    row.partner,
    row.txnType,
    row.speedFlag,
    row.processingMethod,
    row.payerFunding,
    row.payeeFunding,
    row.payerCcy,
    row.payeeCcy,
    row.txnCount,
    row.totalVolume,
    row.customerRevenue,
    row.estRevenue,
    row.avgTxnSize,
    row.revenueBasis
  ].map((part) => String(part ?? "")).join("|");
}

function invoiceGroupSectionKey(groupId) {
  return sectionKey(`invoice-group:${groupId}`);
}

function invoiceGroupSubsectionKey(groupId, name) {
  return sectionKey(`invoice-group:${groupId}:${name}`);
}

function summarizeInvoiceGroup(group) {
  const parts = [];
  if (group.activityRows.length) {
    parts.push(`${group.activityRowCount} imported row${group.activityRowCount === 1 ? "" : "s"}`);
    if (group.activityTxnCount > 0) parts.push(`${group.activityTxnCount.toLocaleString("en-US")} txns`);
    if (group.activityVolume > 0) parts.push(`${fmt(group.activityVolume)} volume`);
  }
  if (group.lines.length > 1) {
    parts.push(`${group.lines.length} calc lines`);
  }
  if (!parts.length) parts.push(group.lines[0]?.desc || "");
  return parts.join(" · ");
}

function describeActivitySummaryRow(row) {
  const label = [row.txnType, row.speedFlag, row.processingMethod].filter(Boolean).join(" ");
  const funding = [row.payerFunding, row.payeeFunding].filter(Boolean).join(" → ");
  const meta = [];
  if (state.inv && (state.inv.periodStart || state.inv.periodEnd) && (state.inv.periodStart !== state.inv.periodEnd) && row.period) {
    meta.push(formatPeriodLabel(row.period));
  }
  if (row.txnCount) meta.push(`${Number(row.txnCount).toLocaleString("en-US")} txns`);
  if (row.totalVolume) meta.push(`${fmt(row.totalVolume)} volume`);
  if (funding) meta.push(funding);
  return [label, meta.join(" · ")].filter(Boolean).join(" — ");
}

function getInvoicePeriodActivityRows(partner, startPeriod, endPeriod) {
  const periods = new Set(enumeratePeriods(startPeriod, endPeriod));
  const activityMap = new Map();
  (state.ltxn || []).forEach((row) => {
    const period = normalizeMonthKey(row.period);
    if (norm(row.partner) !== norm(partner)) return;
    if (!periods.has(period)) return;
    if (!isPartnerActiveForPeriod(state, partner, period)) return;
    activityMap.set(activityRowKey(row), row);
  });
  return [...activityMap.values()].sort((a, b) =>
    comparePeriods(a.period, b.period)
    || String(a.txnType || "").localeCompare(String(b.txnType || ""))
    || String(a.speedFlag || "").localeCompare(String(b.speedFlag || ""))
    || String(a.processingMethod || "").localeCompare(String(b.processingMethod || ""))
  );
}

function summarizeInvoicePeriodActivityRows(rows) {
  const months = new Set(rows.map((row) => normalizeMonthKey(row.period)).filter(Boolean));
  const txnCount = rows.reduce((sum, row) => sum + Number(row.txnCount || 0), 0);
  const totalVolume = rows.reduce((sum, row) => sum + Number(row.totalVolume || 0), 0);
  const customerRevenue = rows.reduce((sum, row) => sum + Number(row.customerRevenue || 0), 0);
  const generatedRevenue = rows.reduce((sum, row) => sum + Number(row.generatedRevenueSupport || 0), 0);
  const estRevenue = rows.reduce((sum, row) => sum + Number(row.estRevenue || 0), 0);
  const parts = [
    `${months.size} month${months.size === 1 ? "" : "s"}`,
    `${rows.length} imported row${rows.length === 1 ? "" : "s"}`
  ];
  if (txnCount > 0) parts.push(`${txnCount.toLocaleString("en-US")} txns`);
  if (totalVolume > 0) parts.push(`${fmt(totalVolume)} volume`);
  if (generatedRevenue > 0) parts.push(`${fmt(generatedRevenue)} generated rev`);
  if (customerRevenue > 0) parts.push(`${fmt(customerRevenue)} imported rev`);
  if (estRevenue > 0) parts.push(`${fmt(estRevenue)} est rev`);
  return parts.join(" · ");
}

function renderInvoicePeriodActivityRows(rows) {
  const byPeriod = new Map();
  rows.forEach((row) => {
    const period = normalizeMonthKey(row.period) || "unknown";
    if (!byPeriod.has(period)) byPeriod.set(period, []);
    byPeriod.get(period).push(row);
  });
  return [...byPeriod.entries()].sort((a, b) => comparePeriods(a[0], b[0])).map(([period, periodRows]) => {
    const txnCount = periodRows.reduce((sum, row) => sum + Number(row.txnCount || 0), 0);
    const totalVolume = periodRows.reduce((sum, row) => sum + Number(row.totalVolume || 0), 0);
    const customerRevenue = periodRows.reduce((sum, row) => sum + Number(row.customerRevenue || 0), 0);
    const generatedRevenue = periodRows.reduce((sum, row) => sum + Number(row.generatedRevenueSupport || 0), 0);
    const estRevenue = periodRows.reduce((sum, row) => sum + Number(row.estRevenue || 0), 0);
    return `
      <div class="invoice-detail-subsection">
        <div class="invoice-detail-subbody">
          <div class="invoice-detail-meta">
            <span class="helper-pill"><strong>${html(formatPeriodLabel(period) || period)}</strong></span>
            ${txnCount ? `<span class="helper-pill">${txnCount.toLocaleString("en-US")} txns</span>` : ""}
            ${totalVolume ? `<span class="helper-pill">${fmt(totalVolume)} volume</span>` : ""}
            ${generatedRevenue ? `<span class="helper-pill">${fmt(generatedRevenue)} generated rev</span>` : ""}
            ${customerRevenue ? `<span class="helper-pill">${fmt(customerRevenue)} imported rev</span>` : ""}
            ${estRevenue ? `<span class="helper-pill">${fmt(estRevenue)} est rev</span>` : ""}
          </div>
          <div class="invoice-detail-list">
            ${periodRows.map((row) => `
              <div class="invoice-detail-item">
                <div>
                  <div>${html(describeActivitySummaryRow(row) || "Imported transaction summary")}</div>
                  <div class="invoice-detail-reason">
                    ${[
                      Number(row.generatedRevenueSupport || 0) ? `Generated revenue: ${fmt(row.generatedRevenueSupport)}` : "",
                      Number(row.customerRevenue || 0) ? `Imported revenue: ${fmt(row.customerRevenue)}` : "",
                      Number(row.estRevenue || 0) ? `Est revenue already charged: ${fmt(row.estRevenue)}` : "",
                      Number(row.avgTxnSize || 0) ? `Avg txn: ${fmt(row.avgTxnSize)}` : ""
                    ].filter(Boolean).join(" · ")}
                  </div>
                </div>
                <div class="mono">${Number(row.totalVolume || 0) ? fmt(row.totalVolume) : "—"}</div>
              </div>
            `).join("")}
          </div>
        </div>
      </div>
    `;
  }).join("");
}

function formatSignedInvoiceAmount(value) {
  const amount = Number(value || 0);
  if (amount < 0) return `(${fmt(Math.abs(amount))})`;
  return fmt(amount);
}

function formatInvoiceDetailAmount(line) {
  if (line.active === false) {
    const inactiveAmount = line.dir === "charge" || line.dir === "pay" ? formatSignedInvoiceAmount(line.amount) : `(${fmt(line.amount)})`;
    return `<span class="invoice-inactive-amount">${inactiveAmount}</span>`;
  }
  if (line.dir === "charge") return formatSignedInvoiceAmount(line.amount);
  if (line.dir === "pay") return formatSignedInvoiceAmount(line.amount);
  return `(${fmt(line.amount)})`;
}

function renderInvoiceDetailSubsection({ groupId, name, title, summary, content, defaultOpen = false }) {
  const key = invoiceGroupSubsectionKey(groupId, name);
  const open = isSectionOpen(key, defaultOpen);
  return `
    <div class="invoice-detail-subsection">
      <button class="invoice-detail-subtoggle" data-action="toggle-section" data-key="${key}" data-default-open="${defaultOpen ? "true" : "false"}">
        <span class="section-headline">
          <strong>${html(title)}</strong>
          ${summary ? `<span class="helper">${html(summary)}</span>` : ""}
        </span>
        <span class="toggle-indicator">${open ? "▾" : "▸"}</span>
      </button>
      ${open ? `<div class="invoice-detail-subbody">${content}</div>` : ""}
    </div>
  `;
}

function calculateActiveInvoiceTotals(lines) {
  return lines.reduce((totals, line) => {
    if (line.active === false) return totals;
    if (line.dir === "charge") totals.chg += Number(line.amount || 0);
    if (line.dir === "pay") totals.pay += Number(line.amount || 0);
    if (line.dir === "offset") {
      totals.offset += Number(line.amount || 0);
      totals.pay += Number(line.amount || 0);
    }
    return totals;
  }, { chg: 0, pay: 0, offset: 0 });
}

function groupInvoiceLines(lines) {
  const groups = [];
  const map = new Map();
  lines.forEach((line, index) => {
    const key = line.groupKey || `${line.cat}|${line.dir}|${line.desc}`;
    let group = map.get(key);
    if (!group) {
      group = {
        id: `invoice-group-${index}`,
        key,
        cat: line.cat,
        dir: line.dir,
        label: line.groupLabel || line.desc,
        lines: [],
        charge: 0,
        pay: 0,
        offset: 0,
        displayCharge: 0,
        displayPay: 0,
        displayOffset: 0,
        activityRows: [],
        activityRowCount: 0,
        activityTxnCount: 0,
        activityVolume: 0,
        isInactive: false,
        hasInactiveLines: false
      };
      map.set(key, group);
      groups.push(group);
    }
    group.lines.push(line);
    if (line.dir === "charge") {
      group.displayCharge += line.amount;
      if (line.active !== false) group.charge += line.amount;
    }
    if (line.dir === "pay") {
      group.displayPay += line.amount;
      if (line.active !== false) group.pay += line.amount;
    }
    if (line.dir === "offset") {
      group.displayOffset += line.amount;
      if (line.active !== false) group.offset += line.amount;
    }
  });

  groups.forEach((group) => {
    group.isInactive = group.lines.every((line) => line.active === false);
    group.hasInactiveLines = group.lines.some((line) => line.active === false);
    const activityMap = new Map();
    group.lines.forEach((line) => {
      (line.activityRows || []).forEach((row) => {
        activityMap.set(activityRowKey(row), row);
      });
    });
    group.activityRows = [...activityMap.values()];
    group.activityRowCount = group.activityRows.length;
    group.activityTxnCount = group.activityRows.reduce((sum, row) => sum + Number(row.txnCount || 0), 0);
    group.activityVolume = group.activityRows.reduce((sum, row) => sum + Number(row.totalVolume || 0), 0);
    group.summary = summarizeInvoiceGroup(group);
  });

  return groups;
}

function buildInvoiceDocument(inv, kind) {
  const hasReceivableAmount = (group) => [group.charge, group.offset, group.displayCharge, group.displayOffset].some((value) => Math.abs(Number(value || 0)) > 0.0001);
  const hasPayableAmount = (group) => [group.pay, group.displayPay].some((value) => Math.abs(Number(value || 0)) > 0.0001);
  const sourceLines = (inv.lines || []).filter((line) => {
    if (kind === "receivable") return line.dir === "charge" || line.dir === "offset";
    return line.dir === "pay";
  });
  const totals = calculateActiveInvoiceTotals(sourceLines);
  const amountDue = kind === "receivable"
    ? roundCurrency(Math.max(totals.chg - totals.offset, 0))
    : roundCurrency(Math.max(totals.pay, 0));
  const primaryGroups = (inv.groups || []).filter((group) => (kind === "receivable" ? hasReceivableAmount(group) : hasPayableAmount(group)));
  const fallbackGroups = groupInvoiceLines(sourceLines).filter((group) => (kind === "receivable" ? hasReceivableAmount(group) : hasPayableAmount(group)));
  const groups = primaryGroups.length ? primaryGroups : fallbackGroups;
  const hasVisibleGroups = groups.some((group) => (kind === "receivable" ? hasReceivableAmount(group) : hasPayableAmount(group)));
  if (!hasVisibleGroups && amountDue <= 0 && !sourceLines.length) return null;
  return {
    kind,
    key: `${inv.partner}:${inv.period}:${kind}`,
    partner: inv.partner,
    period: inv.period,
    periodStart: inv.periodStart,
    periodEnd: inv.periodEnd,
    periodLabel: inv.periodLabel,
    periodDateRange: inv.periodDateRange,
    title: kind === "receivable" ? "AR Invoice" : "AP Invoice",
    amountLabel: kind === "receivable" ? getPartnerOwesLabel(inv.partner) : "Veem Owes",
    exportLabel: kind === "receivable" ? "AR Invoice" : "AP Invoice",
    amountDue,
    chargeTotal: totals.chg,
    creditTotal: totals.offset,
    payTotal: totals.pay,
    lines: sourceLines,
    groups,
    notes: inv.notes || []
  };
}

function buildInvoiceDocuments(inv) {
  const docs = ["receivable", "payable"]
    .map((kind) => buildInvoiceDocument(inv, kind))
    .filter(Boolean);
  return docs.map((doc) => ({
    ...doc,
    buttonLabel: doc.kind === "receivable" ? "Download AR Invoice" : "Download AP Invoice"
  }));
}

function findInvoiceGroup(groupId) {
  if (!state.inv?.groups?.length || !groupId) return null;
  return state.inv.groups.find((group) => group.id === groupId) || null;
}

function detailManifestKey(partner, period) {
  return `${partner}|${period}`;
}

function buildFallbackDetailFilePath(partner, period) {
  const partnerSlug = slugifyFilenamePart(partner || "partner");
  const periodSlug = slugifyFilenamePart(period || "period");
  return `./looker-detail-files/${partnerSlug}-${periodSlug}-details.json`;
}

function buildFallbackDetailCsvPath(partner, period) {
  const partnerSlug = slugifyFilenamePart(partner || "partner");
  const periodSlug = slugifyFilenamePart(period || "period");
  return `./hosted-detail-files-v1/${partnerSlug}-${periodSlug}-details.csv`;
}

function looksLikeHtmlDocument(textValue, contentType = "") {
  const normalizedType = String(contentType || "").toLowerCase();
  if (normalizedType.includes("text/html")) return true;
  const trimmed = String(textValue || "").trimStart().toLowerCase();
  return trimmed.startsWith("<!doctype") || trimmed.startsWith("<html") || trimmed.startsWith("<body") || (trimmed.startsWith("<") && trimmed.includes("</html>"));
}

function parseDetailCsv(textValue) {
  const rows = [];
  let row = [];
  let cell = "";
  let inQuotes = false;
  const input = String(textValue || "").replace(/^\uFEFF/, "").replace(/\u0000/g, "");
  for (let i = 0; i < input.length; i += 1) {
    const char = input[i];
    const next = input[i + 1];
    if (inQuotes) {
      if (char === '"' && next === '"') {
        cell += '"';
        i += 1;
      } else if (char === '"') {
        inQuotes = false;
      } else {
        cell += char;
      }
      continue;
    }
    if (char === '"') {
      inQuotes = true;
    } else if (char === ",") {
      row.push(cell);
      cell = "";
    } else if (char === "\n") {
      row.push(cell);
      rows.push(row);
      row = [];
      cell = "";
    } else if (char === "\r") {
      if (next !== "\n") {
        row.push(cell);
        rows.push(row);
        row = [];
        cell = "";
      }
    } else {
      cell += char;
    }
  }
  row.push(cell);
  if (row.length > 1 || row[0] !== "") rows.push(row);
  if (!rows.length) return [];
  const headers = rows[0].map((value) => String(value || "").trim());
  return rows.slice(1).map((values) => {
    const output = {};
    headers.forEach((header, index) => {
      if (!header) return;
      output[header] = values[index] ?? "";
    });
    return output;
  }).filter((entry) => Object.values(entry).some((value) => String(value || "").trim()));
}

async function loadInvoiceDetailRows(partner, period) {
  const key = detailManifestKey(partner, period);
  let baseRows = [];
  if (detailFileCache.has(key)) {
    baseRows = detailFileCache.get(key);
  } else {
    const fileCandidates = Array.from(new Set([
      buildFallbackDetailCsvPath(partner, period),
      LOOKER_DETAIL_MANIFEST[key],
      buildFallbackDetailFilePath(partner, period),
    ].filter(Boolean)));
    for (const filePath of fileCandidates) {
      const response = await fetch(filePath);
      if (response.status === 403 || response.status === 404) {
        continue;
      }
      if (!response.ok) {
        throw new Error(`Could not load detail rows (${response.status})`);
      }
      const rawText = await response.text();
      if (looksLikeHtmlDocument(rawText, response.headers.get("content-type"))) {
        continue;
      }
      if (filePath.endsWith(".csv")) {
        baseRows = parseDetailCsv(rawText);
      } else {
        try {
          baseRows = JSON.parse(rawText);
        } catch (error) {
          throw new Error(`Could not parse detail rows for ${partner} ${period}.`);
        }
      }
      detailFileCache.set(key, baseRows);
      break;
    }
  }
  const overrideRows = (state.lookerImportedDetailRows || []).filter((row) => norm(row.partner) === norm(partner) && norm(row.period) === norm(period));
  if (!overrideRows.length) return baseRows;
  const overrideSources = new Set(overrideRows.map((row) => row.detailSource || row.detailCategory || "uploaded_looker_detail"));
  const preservedBase = baseRows.filter((row) => !(norm(row.partner) === norm(partner) && norm(row.period) === norm(period) && overrideSources.has(row.detailSource || row.detailCategory || "uploaded_looker_detail")));
  return [...preservedBase, ...overrideRows];
}

async function loadInvoiceDetailRowsForRange(partner, startPeriod, endPeriod) {
  const periods = enumeratePeriods(startPeriod, endPeriod);
  const batches = await Promise.all(periods.map((period) => loadInvoiceDetailRows(partner, period)));
  return batches.flat();
}

function fileToBase64(file) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onerror = () => reject(reader.error || new Error("Could not read the selected file."));
    reader.onload = () => {
      const bytes = new Uint8Array(reader.result);
      let binary = "";
      const chunkSize = 0x8000;
      for (let index = 0; index < bytes.length; index += chunkSize) {
        binary += String.fromCharCode(...bytes.subarray(index, index + chunkSize));
      }
      resolve(window.btoa(binary));
    };
    reader.readAsArrayBuffer(file);
  });
}

function clearContractImportContents() {
  state.cText = "";
  state.cStatus = "idle";
  state.cError = "";
  state.cParsed = null;
  state.cName = "";
  state.cVerifyPartner = "";
  state.cFileName = "";
  state.cPendingFile = null;
  state.cExtractStatus = "idle";
  state.cDetectedIncremental = false;
  state.cImported = false;
  state.cImportSummary = null;
  state.cImportBehavior = "override";
  state.cDiff = null;
  state.cImportPlan = null;
  state.cSelectedImportRows = {};
  const input = root.querySelector("#contract-file-upload");
  if (input) input.value = "";
  render();
}

function clearLookerImportContents() {
  state.lookerImportText = "";
  state.lookerImportError = "";
  state.lookerImportStatus = "idle";
  state.lookerImportResult = null;
  state.lookerImportFileName = "";
  state.lookerImportPendingFile = null;
  const input = root.querySelector("#looker-import-file");
  if (input) input.value = "";
  render();
}

function ensureRowIds(rows) {
  return (rows || []).map((row) => (row.id ? row : { id: uid(), ...row }));
}

function replaceRows(existingRows, incomingRows, shouldRemove) {
  const preserved = existingRows.filter((row) => !shouldRemove(row));
  return [...preserved, ...ensureRowIds(incomingRows)];
}

function revenueSourceKey(row) {
  return row?.revenueSource || "summary";
}

function applyLookerSectionUpdate(section, fileType, period, incomingRows) {
  const rows = stripUntrustedDirectInvoiceRows(incomingRows || []);
  if (section === "ltxn") {
    // Always purge any existing rows with untrusted direct-invoice markers for this
    // period, regardless of which fileType is being imported. Belt-and-suspenders:
    // partner_offline_billing is authoritative for Stampli USD Abroad.
    state.ltxn = state.ltxn.filter((row) => !(row.period === period && isUntrustedDirectInvoiceRow(row)));
    if (fileType === "partner_offline_billing") {
      state.ltxn = replaceRows(state.ltxn, rows, (row) => row.period === period && !row.revenueBasis && !row.directInvoiceSource);
      return;
    }
    if (fileType === "all_stampli_credit_complete") {
      // Dropped on 2026-04-21: dedicated Stampli credit-complete / USD Abroad feeds
      // were confirmed incorrect. partner_offline_billing is authoritative. Ignore
      // this fileType entirely so stale runs can't reintroduce bad rows.
      return;
    }
    if (fileType === "partner_rev_share_v2" || fileType === "partner_revenue_share") {
      state.ltxn = replaceRows(state.ltxn, rows, (row) => row.period === period && !!row.revenueBasis);
      return;
    }
    state.ltxn = replaceRows(state.ltxn, rows, (row) => row.period === period);
    return;
  }
  if (section === "lrev") {
    state.lrev = replaceRows(state.lrev, rows, (row) => row.period === period);
    return;
  }
  if (section === "lva") {
    const partners = new Set(rows.map((row) => row.partner));
    state.lva = replaceRows(state.lva, rows, (row) => row.period === period && (!partners.size || partners.has(row.partner)));
    return;
  }
  if (section === "lrs") {
    if (fileType === "partner_revenue_summary") {
      state.lrs = replaceRows(state.lrs, rows, (row) => row.period === period && revenueSourceKey(row) === "billing_summary");
      return;
    }
    const partnerSourcePairs = new Set(rows.map((row) => `${row.partner || ""}|${revenueSourceKey(row)}`));
    state.lrs = replaceRows(state.lrs, rows, (row) => row.period === period && (!partnerSourcePairs.size || partnerSourcePairs.has(`${row.partner || ""}|${revenueSourceKey(row)}`)));
    return;
  }
  if (section === "lfxp") {
    const partners = new Set(rows.map((row) => row.partner));
    state.lfxp = replaceRows(state.lfxp, rows, (row) => row.period === period && (!partners.size || partners.has(row.partner)));
  }
}

function mergeLookerImportContext(contextUpdate) {
  if (!contextUpdate || typeof contextUpdate !== "object") return;
  state.lookerImportContext = {
    ...state.lookerImportContext,
    ...contextUpdate
  };
}

function mergeLookerDetailOverrides(detailRows, period) {
  const rows = detailRows || [];
  const incomingSources = new Set(rows.map((row) => row.detailSource || row.detailCategory || "uploaded_looker_detail"));
  state.lookerImportedDetailRows = [
    ...(state.lookerImportedDetailRows || []).filter((row) => !(row.period === period && incomingSources.has(row.detailSource || row.detailCategory || "uploaded_looker_detail"))),
    ...rows
  ];
}

async function submitLookerImport() {
  if (state.lookerImportStatus === "parsing") return;
  if (!state.lookerImportText.trim() && !state.lookerImportPendingFile) {
    state.lookerImportStatus = "error";
    state.lookerImportError = "Paste tabular data or choose a CSV/XLSX file first.";
    render();
    return;
  }
  state.lookerImportStatus = "parsing";
  state.lookerImportError = "";
  state.lookerImportResult = null;
  render();
  try {
    if (!isLookerImportEnabled()) {
      throw new Error("Manual upload automation is not configured. Connect BILLING_APP_CONFIG.lookerImportWebhookUrl to enable in-browser imports.");
    }
    const payload = {
      fileType: state.lookerImportType,
      period: state.lookerImportPeriod,
      pastedText: state.lookerImportText.trim(),
      context: state.lookerImportContext
    };
    if (state.lookerImportPendingFile) {
      payload.fileName = state.lookerImportPendingFile.name;
      payload.fileBase64 = await fileToBase64(state.lookerImportPendingFile);
    }
    const result = await importLookerFileAndSave(payload);
    Object.entries(result.sections || {}).forEach(([section, rows]) => {
      applyLookerSectionUpdate(section, result.fileType, result.period, rows);
    });
    mergeLookerImportContext(result.contextUpdate);
    mergeLookerDetailOverrides(result.detailRows, result.period);
    updateLookerImportAudit(result, { savedAt: new Date().toISOString(), source: "manual" });
    state.lookerImportResult = result;
    state.lookerImportStatus = "success";
    state.lookerImportError = "";
    state.lookerImportFileName = state.lookerImportPendingFile?.name || state.lookerImportFileName;
    state.lookerImportPendingFile = null;
    state.lookerImportText = "";
    state.inv = null;
    persistAndRender();
    logWorkbookChange(
      "import_looker_data",
      `Imported ${result.fileLabel} for ${result.period}.`,
      { section: "looker_import", fileType: result.fileType, period: result.period }
    );
    showToast("Looker data updated", `Replaced ${result.period} rows using ${result.fileLabel}.`, "success");
  } catch (error) {
    state.lookerImportStatus = "error";
    state.lookerImportError = String(error.message || error);
    render();
  }
}

function matchesTxnSummary(detailRow, summaryRow) {
  return detailRow.detailCategory === "transaction"
    && norm(detailRow.partner) === norm(summaryRow.partner)
    && norm(detailRow.period) === norm(summaryRow.period)
    && norm(detailRow.txnType) === norm(summaryRow.txnType)
    && norm(detailRow.speedFlag) === norm(summaryRow.speedFlag)
    && norm(detailRow.processingMethod) === norm(summaryRow.processingMethod)
    && optionalMatch(summaryRow.payerFunding, detailRow.payerFunding)
    && optionalMatch(summaryRow.payeeFunding, detailRow.payeeFunding)
    && optionalMatch(summaryRow.payerCcy, detailRow.payerCcy)
    && optionalMatch(summaryRow.payeeCcy, detailRow.payeeCcy)
    && optionalMatch(summaryRow.payerCountry, detailRow.payerCountry)
    && optionalMatch(summaryRow.payeeCountry, detailRow.payeeCountry);
}

function matchesReversalSummary(detailRow, summaryRow) {
  return detailRow.detailCategory === "reversal"
    && norm(detailRow.partner) === norm(summaryRow.partner)
    && norm(detailRow.period) === norm(summaryRow.period)
    && optionalMatch(summaryRow.payerFunding, detailRow.payerFunding);
}

function buildInvoiceExportRows(detailRows, scope, group) {
  if (!state.inv) return [];
  const matchingDescriptions = scope === "all" ? "" : (group?.lines || []).map((line) => line.desc).join(" | ");
  return detailRows.map((row) => ({
    exportScope: scope === "all" ? "all_transactions_for_range" : "matching_transactions",
    invoicePartner: state.inv.partner,
    invoicePeriod: state.inv.period,
    invoicePeriodStart: state.inv.periodStart || state.inv.period,
    invoicePeriodEnd: state.inv.periodEnd || state.inv.period,
    invoicePeriodLabel: state.inv.periodLabel || state.inv.period,
    invoiceCategory: group?.cat || "",
    invoiceGroup: group?.label || "",
    invoiceDescriptions: matchingDescriptions,
    ...row
  }));
}

async function exportInvoiceTransactions(scope, groupId) {
  if (!state.inv) return;
  const group = findInvoiceGroup(groupId);
  const detailRows = (await loadInvoiceDetailRowsForRange(state.inv.partner, state.inv.periodStart || state.inv.period, state.inv.periodEnd || state.inv.period))
    .filter((row) => !row.period || isPartnerActiveForPeriod(state, state.inv.partner, row.period));
  if (!detailRows.length) {
    showToast("Nothing to export", "No payment-level detail file was available for that partner and date range.", "warning");
    return;
  }
  const filtered = scope === "all"
    ? detailRows
    : detailRows.filter((detailRow) => (group?.activityRows || []).some((summaryRow) => (
      group.cat === "Reversal"
        ? matchesReversalSummary(detailRow, summaryRow)
        : matchesTxnSummary(detailRow, summaryRow)
    )));
  const rows = buildInvoiceExportRows(filtered, scope, group);
  if (!rows.length) {
    showToast("Nothing to export", "No payment-level rows matched that invoice line.", "warning");
    return;
  }
  const scopeLabel = scope === "all" ? "all-period-transactions" : "matching-transactions";
  const groupLabel = scope === "all" ? "all" : slugifyFilenamePart(group?.label || "invoice-line");
  const filename = `${slugifyFilenamePart(state.inv.partner)}-${buildInvoicePeriodKey(state.inv.periodStart || state.inv.period, state.inv.periodEnd || state.inv.period)}-${groupLabel}-${scopeLabel}.csv`;
  downloadCsv(filename, rows);
  showToast("CSV exported", `${rows.length} row${rows.length === 1 ? "" : "s"} downloaded for ${scope === "all" ? "the full date range" : (group?.label || "that invoice line")}.`, "success");
}

function buildInvoiceArtifactTimestampKey(value = new Date().toISOString()) {
  return String(value || "")
    .replaceAll(":", "-")
    .replaceAll(".", "-");
}

function buildInvoiceArtifactBundleKey(inv, generatedAt) {
  const periodStart = inv?.periodStart || inv?.period || "";
  const periodEnd = inv?.periodEnd || inv?.period || periodStart;
  return [
    slugifyFilenamePart(inv?.partner || "partner"),
    buildInvoicePeriodKey(periodStart, periodEnd),
    buildInvoiceArtifactTimestampKey(generatedAt)
  ].filter(Boolean).join("-");
}

function getPrivateInvoiceLinkUrl(result) {
  return String(
    result?.privateUrl
    || result?.downloadUrl
    || result?.url
    || result?.link
    || ""
  ).trim();
}

async function buildInvoiceArtifactPayload(inv = state.inv, { trigger = "generate_invoice" } = {}) {
  if (!inv) {
    throw new Error("Calculate an invoice before creating delivery artifacts.");
  }
  const generatedAt = new Date().toISOString();
  const periodStart = inv.periodStart || inv.period;
  const periodEnd = inv.periodEnd || inv.period;
  const bundleKey = buildInvoiceArtifactBundleKey(inv, generatedAt);
  let detailRows = [];
  let detailWarning = "";
  try {
    detailRows = (await loadInvoiceDetailRowsForRange(inv.partner, periodStart, periodEnd))
      .filter((row) => !row.period || isPartnerActiveForPeriod(state, inv.partner, row.period));
  } catch (error) {
    console.error("Could not load invoice detail rows for artifact packaging", error);
    detailWarning = String(error?.message || error || "Could not load detail rows.");
  }
  const transactionRows = buildInvoiceExportRows(detailRows, "all", null);
  const documents = buildInvoiceDocuments(inv).map((doc) => ({
    kind: doc.kind,
    title: doc.title,
    fileName: `${bundleKey}-${doc.kind === "receivable" ? "ar-invoice" : "ap-invoice"}.html`,
    amountDue: doc.amountDue,
    data: doc,
    pdfHtml: buildInvoicePdfDocument(doc)
  }));
  const receivableDoc = documents.find((doc) => doc.kind === "receivable") || null;
  const payableDoc = documents.find((doc) => doc.kind === "payable") || null;
  return {
    mode: "invoice_artifact",
    trigger,
    generatedAt,
    bundleKey,
    workspace: {
      mode: state.workspaceMode || "local",
      label: state.workspaceLabel || getWorkspaceLabel()
    },
    actor: {
      email: state.currentUserEmail || "",
      role: state.currentUserRole || ""
    },
    partner: inv.partner,
    period: inv.period,
    periodStart,
    periodEnd,
    periodKey: buildInvoicePeriodKey(periodStart, periodEnd),
    periodLabel: inv.periodLabel || formatPeriodRangeLabel(periodStart, periodEnd),
    periodDateRange: inv.periodDateRange || formatPeriodDateRange(periodStart, periodEnd),
    summary: {
      arAmount: Number(receivableDoc?.amountDue || 0),
      apAmount: Number(payableDoc?.amountDue || 0),
      netAmount: Math.abs(Number(inv.net || 0)),
      netDirection: Number(inv.net || 0) >= 0 ? getPartnerOwesLabel(inv.partner) : "Veem Owes",
      transactionRowCount: transactionRows.length
    },
    invoice: inv,
    documents,
    transactions: {
      fileName: `${bundleKey}-transactions.csv`,
      rowCount: transactionRows.length,
      csvText: rowsToCsvText(transactionRows),
      rows: transactionRows
    },
    warnings: [
      ...(Array.isArray(inv.notes) ? inv.notes : []),
      ...(detailWarning ? [`Transaction detail export warning: ${detailWarning}`] : [])
    ]
  };
}

async function archiveInvoiceArtifactCopy(inv = state.inv, {
  trigger = "generate_invoice",
  showSuccessToast = false,
  showUnavailableToast = false,
  showErrorToast = true
} = {}) {
  const payload = await buildInvoiceArtifactPayload(inv, { trigger });
  if (!isInvoiceArtifactEnabled()) {
    state.invoiceArtifactStatus = "idle";
    state.invoiceArtifactError = "";
    state.invoiceArtifactRecord = null;
    if (showUnavailableToast) {
      showToast("Archive not configured", "Connect BILLING_APP_CONFIG.invoiceArtifactWriteBaseUrl to save a timestamped invoice copy on each run.", "warning");
    }
    render();
    return { payload, result: null };
  }

  state.invoiceArtifactStatus = "saving";
  state.invoiceArtifactError = "";
  render();
  try {
    const result = await saveInvoiceArtifact(payload);
    state.invoiceArtifactRecord = {
      trigger,
      artifactId: result?.artifactId || result?.id || "",
      savedAt: result?.savedAt || payload.generatedAt,
      generatedAt: payload.generatedAt,
      bundleKey: payload.bundleKey,
      fileCount: Number(result?.fileCount || payload.documents.length + 1),
      response: result || null
    };
    state.invoiceArtifactStatus = "success";
    state.invoiceArtifactError = "";
    if (showSuccessToast) {
      showToast("Invoice archived", "Saved a timestamped copy of the invoice package.", "success");
    }
    render();
    return { payload, result };
  } catch (error) {
    console.error("Could not save invoice artifact", error);
    state.invoiceArtifactStatus = "error";
    state.invoiceArtifactError = String(error?.message || error || "Unknown error");
    if (showErrorToast) {
      showToast("Invoice archive failed", state.invoiceArtifactError, "warning");
    }
    render();
    throw error;
  }
}

async function generateInvoicePrivateLinkForCurrentSelection() {
  if (!state.inv) {
    showToast("Invoice required", "Calculate the invoice before generating a private partner link.", "warning");
    return;
  }

  state.privateInvoiceLinkStatus = "creating";
  state.privateInvoiceLinkError = "";
  render();

  try {
    let archiveContext = null;
    try {
      archiveContext = await archiveInvoiceArtifactCopy(state.inv, {
        trigger: "generate_private_link",
        showSuccessToast: false,
        showUnavailableToast: false,
        showErrorToast: false
      });
    } catch (error) {
      console.error("Could not pre-save invoice artifact before private link generation", error);
    }

    if (!isPrivateInvoiceLinkEnabled()) {
      state.privateInvoiceLinkStatus = "error";
      state.privateInvoiceLinkError = "Connect BILLING_APP_CONFIG.privateInvoiceLinkSignerUrl to generate a private partner link.";
      showToast("Private link not configured", state.privateInvoiceLinkError, "warning");
      render();
      return;
    }

    const payload = {
      mode: "invoice_private_link",
      requestedAt: new Date().toISOString(),
      partner: state.inv.partner,
      period: state.inv.period,
      periodStart: state.inv.periodStart || state.inv.period,
      periodEnd: state.inv.periodEnd || state.inv.period,
      invoiceArtifact: archiveContext?.payload || await buildInvoiceArtifactPayload(state.inv, { trigger: "generate_private_link" }),
      archivedArtifact: archiveContext?.result || state.invoiceArtifactRecord || null
    };
    const result = await generatePrivateInvoiceLink(payload);
    const privateUrl = getPrivateInvoiceLinkUrl(result);
    if (!privateUrl) {
      throw new Error("The private-link service did not return a download URL.");
    }
    state.privateInvoiceLinkResult = {
      ...result,
      privateUrl,
      generatedAt: payload.requestedAt
    };
    state.privateInvoiceLinkStatus = "success";
    state.privateInvoiceLinkError = "";
    recordAccessActivity(
      "generate_private_invoice_link",
      `Generated a private invoice link for ${state.inv.partner} ${payload.periodStart === payload.periodEnd ? payload.periodStart : `${payload.periodStart} to ${payload.periodEnd}`}.`,
      {
        category: "activity",
        partner: state.inv.partner,
        periodStart: payload.periodStart,
        periodEnd: payload.periodEnd
      }
    );
    showToast("Private link ready", "Generated a private partner download link.", "success");
    render();
  } catch (error) {
    console.error("Could not generate private invoice link", error);
    state.privateInvoiceLinkStatus = "error";
    state.privateInvoiceLinkError = String(error?.message || error || "Unknown error");
    showToast("Private link failed", state.privateInvoiceLinkError, "error");
    render();
  }
}

function copyPrivateInvoiceLinkToClipboard() {
  const privateUrl = getPrivateInvoiceLinkUrl(state.privateInvoiceLinkResult);
  if (!privateUrl) {
    showToast("No link available", "Generate a private link before trying to copy it.", "warning");
    return;
  }
  navigator.clipboard.writeText(privateUrl)
    .then(() => {
      recordAccessActivity(
        "copy_private_invoice_link",
        `Copied the private invoice link for ${state.inv?.partner || "the selected partner"}.`,
        { category: "activity", partner: state.inv?.partner || "" }
      );
      showToast("Private link copied", "The partner download link is on your clipboard.", "success");
    })
    .catch((error) => {
      console.error("Could not copy private invoice link", error);
      showToast("Copy failed", "Could not copy the private link to your clipboard.", "warning");
    });
}

function parseStructuredContract(rawText) {
  const raw = String(rawText || "").trim();
  if (!raw) {
    throw new Error("Paste extracted JSON or a model response containing JSON first.");
  }
  const cleanText = raw.replace(/```json|```/g, "").trim();
  const start = cleanText.indexOf("{");
  const end = cleanText.lastIndexOf("}");
  if (start === -1 || end === -1) {
    throw new Error("No JSON detected. This browser build parses structured JSON only. Use Copy Extraction Prompt to extract pricing with an LLM, then paste the JSON back here.");
  }
  const candidate = cleanText.slice(start, end + 1).replace(/,\s*([}\]])/g, "$1");
  const parsed = JSON.parse(candidate);
  if (!parsed || typeof parsed !== "object") {
    throw new Error("Parsed data was empty.");
  }
  return parsed;
}

function detectPerTierMarginalPricing(rawText) {
  const text = String(rawText || "").toLowerCase().replace(/\s+/g, " ").trim();
  if (!text) return false;
  const hasPerTierBasis = text.includes("tiered pricing is applied on a per-tier basis");
  const hasIncrementalThreshold = text.includes("only the incremental volume above that threshold is priced at the applicable tier rate");
  const hasPriorVolumeLanguage = text.includes("all prior volume remains priced at the applicable lower-tier rates");
  return hasPerTierBasis && (hasIncrementalThreshold || hasPriorVolumeLanguage);
}

function expandCcy(group) {
  if (!group) return "";
  const upper = String(group).toUpperCase().trim();
  if (upper === "MAJORS") return MAJORS;
  if (upper === "MINORS") return MINORS;
  if (upper === "TERTIARY") return TERTIARY;
  return group;
}

async function parseContract() {
  state.cText = readBoundValue("cText");
  state.cDetectedIncremental = state.cDetectedIncremental || detectPerTierMarginalPricing(state.cText);
  state.cStatus = "parsing";
  state.cError = "";
  state.cParsed = null;
  state.cImported = false;
  state.cImportSummary = null;
  state.cDiff = null;
  state.cImportPlan = null;
  state.cSelectedImportRows = {};
  render();
  try {
    const result = parseStructuredContract(state.cText);
    state.cParsed = result;
    state.cName = result.partnerName && result.partnerName !== "Partner" ? result.partnerName : "";
    if (state.cMode === "verify") {
      state.cVerifyPartner = suggestVerifyPartnerName(state.cName);
    }
    refreshContractImportPlan();
    state.cStatus = "success";
    state.cError = "";
    render();
    return;
  } catch (structuredError) {
    try {
      const result = await parseContractText({
        fileName: state.cFileName || state.cPendingFile?.name || "",
        text: state.cText
      });
      state.cParsed = result;
      state.cName = result.partnerName && result.partnerName !== "Partner" ? result.partnerName : state.cName;
      if (state.cMode === "verify") {
        state.cVerifyPartner = suggestVerifyPartnerName(state.cName);
      }
      state.cDetectedIncremental = state.cDetectedIncremental || detectPerTierMarginalPricing(state.cText);
      refreshContractImportPlan();
      state.cStatus = "success";
      state.cError = "";
      render();
      if (result?.warnings?.length) {
        showToast("Contract parsed with notes", `${result.warnings.length} parser note${result.warnings.length === 1 ? "" : "s"} detected. Review before importing.`, "warning");
      } else {
        showToast("Contract parsed", "Pricing was extracted directly from the uploaded contract text.", "success");
      }
    } catch (error) {
      const fallbackMessage = String(structuredError?.message || structuredError || "");
      const parseMessage = String(error?.message || error || "");
      state.cStatus = "error";
      state.cError = parseMessage || fallbackMessage;
      render();
    }
  }
}

async function extractContractFile() {
  if (!state.cPendingFile) {
    showToast("No file selected", "Choose a contract PDF first.", "warning");
    return;
  }

  state.cExtractStatus = "parsing";
  state.cError = "";
  render();

  try {
    const isPlainTextFile = state.cPendingFile.type.startsWith("text/") || /\.txt$/i.test(state.cPendingFile.name || "");
    const payload = isPlainTextFile
      ? {
          text: await state.cPendingFile.text(),
          fileName: state.cPendingFile.name,
          charCount: 0
        }
      : await extractContractText({
          fileName: state.cPendingFile.name,
          fileBase64: await fileToBase64(state.cPendingFile)
        });
    state.cText = payload?.text || "";
    state.cFileName = payload?.fileName || state.cPendingFile.name;
    state.cPendingFile = null;
    state.cParsed = null;
    state.cDetectedIncremental = detectPerTierMarginalPricing(state.cText);
    state.cImported = false;
    state.cImportSummary = null;
    state.cDiff = null;
    state.cImportPlan = null;
    state.cSelectedImportRows = {};
    state.cVerifyPartner = "";
    state.cStatus = "idle";
    state.cExtractStatus = "success";
    state.cError = "";
    const input = root.querySelector("#contract-file-upload");
    if (input) input.value = "";
    render();
    if (!state.cText.trim()) {
      showToast("PDF loaded", "The PDF was uploaded, but no text was extracted. It may be scanned or image-only.", "warning");
    } else {
      showToast("Contract text loaded", `${state.cFileName} loaded with ${Number(payload?.charCount || state.cText.length).toLocaleString()} characters.`, "success");
    }
  } catch (error) {
    state.cExtractStatus = "error";
    state.cError = String(error.message || error);
    render();
  }
}

function copyExtractionPrompt() {
  const contractText = readBoundValue("cText");
  const payload = `${CONTRACT_PROMPT}\n\nExtract all pricing from this contract text:\n\n${contractText || "[Paste contract pricing here]"}`;
  navigator.clipboard.writeText(payload)
    .then(() => showToast("Prompt copied", "The extraction prompt and contract text are on your clipboard.", "success"))
    .catch(() => showToast("Clipboard blocked", "Copy failed in this browser. You can still select the text manually.", "error"));
}

function normalizeIsoDate(value) {
  const text = String(value || "").trim().slice(0, 10);
  return /^\d{4}-\d{2}-\d{2}$/.test(text) ? text : "";
}

function previousDay(dateText) {
  const iso = normalizeIsoDate(dateText);
  if (!iso) return "";
  const date = new Date(`${iso}T12:00:00`);
  date.setDate(date.getDate() - 1);
  return date.toISOString().slice(0, 10);
}

function rowSignature(row, fields) {
  return fields.map((field) => norm(row[field])).join("|");
}

function mergeContractRowsByEffectiveDate(existingRows, newRows, { effectiveDate, startKey = "startDate", endKey = "endDate", matches }) {
  if (!newRows.length) return existingRows;
  const cutoffDate = previousDay(effectiveDate);
  return [
    ...existingRows.flatMap((existingRow) => {
      if (!newRows.some((newRow) => matches(existingRow, newRow))) return [existingRow];
      const rowStart = normalizeIsoDate(existingRow[startKey]);
      const rowEnd = normalizeIsoDate(existingRow[endKey]);
      if (rowStart && rowStart === effectiveDate) return [];
      if (rowStart && comparePeriods(rowStart, effectiveDate) > 0) return [existingRow];
      if (rowEnd && comparePeriods(rowEnd, effectiveDate) < 0) return [existingRow];
      if (!cutoffDate) return [existingRow];
      return [{ ...existingRow, [endKey]: cutoffDate }];
    }),
    ...newRows
  ];
}

function dedupeRowsByPartnerSignature(rows, signatureFields, { startKey = "startDate" } = {}) {
  const groups = new Map();
  rows.forEach((row, index) => {
    const key = `${norm(row.partner)}|${rowSignature(row, signatureFields)}`;
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push({ row, index });
  });
  const compareIso = (left, right) => comparePeriods(normalizeIsoDate(left || ""), normalizeIsoDate(right || ""));
  return [...groups.values()]
    .map((items) => {
      const chosen = [...items].sort((a, b) => {
        const startDelta = compareIso(b.row[startKey], a.row[startKey]);
        if (startDelta) return startDelta;
        return b.index - a.index;
      })[0];
      return { ...chosen.row, _index: chosen.index };
    })
    .sort((a, b) => a._index - b._index)
    .map(({ _index, ...row }) => row);
}

function cleanupDuplicateContractRows() {
  state.off = dedupeRowsByPartnerSignature(state.off, ["txnType", "speedFlag", "payerFunding", "payeeFunding", "payerCcy", "payeeCcy", "payerCountry", "payeeCountry", "payerCountryGroup", "payeeCountryGroup", "processingMethod", "minAmt", "maxAmt", "fee"]);
  state.vol = dedupeRowsByPartnerSignature(state.vol, ["txnType", "speedFlag", "payerFunding", "payeeFunding", "payeeCardType", "ccyGroup", "minVol", "maxVol", "rate", "note"]);
  state.fxRates = dedupeRowsByPartnerSignature(state.fxRates, ["payerCorridor", "payerCcy", "payeeCorridor", "payeeCcy", "minTxnSize", "maxTxnSize", "minVol", "maxVol", "rate", "note"]);
  state.cap = dedupeRowsByPartnerSignature(state.cap, ["productType", "capType", "amount"]);
  state.mins = dedupeRowsByPartnerSignature(state.mins, ["minAmount", "minVol", "maxVol", "implFeeOffset"]);
  state.revf = dedupeRowsByPartnerSignature(state.revf, ["payerFunding", "feePerReversal"]);
  state.plat = dedupeRowsByPartnerSignature(state.plat, ["monthlyFee"]);
  state.impl = dedupeRowsByPartnerSignature(state.impl.map((row) => normalizeImplementationRow(row)), ["feeType", "feeAmount", "applyAgainstMin", "creditMode", "creditAmount", "creditWindowDays", "note"]);
  state.vaFees = dedupeRowsByPartnerSignature(state.vaFees, ["feeType", "minAccounts", "maxAccounts", "discount", "feePerAccount", "note"]);
  state.surch = dedupeRowsByPartnerSignature(state.surch, ["surchargeType", "rate", "minVol", "maxVol", "note"]);
}

function mergeContractRowsUnique(existingRows, newRows, { matches }) {
  if (!newRows.length) return existingRows;
  return [
    ...existingRows,
    ...newRows.filter((newRow) => !existingRows.some((existingRow) => matches(existingRow, newRow)))
  ];
}

function replaceContractRows(existingRows, newRows, { samePartner }) {
  return [
    ...existingRows.filter((row) => !samePartner(row)),
    ...newRows
  ];
}

function replacePartnerRowsForEffectiveDate(existingRows, newRows, { samePartner, effectiveDate, startKey = "startDate" }) {
  const targetDate = normalizeIsoDate(effectiveDate);
  if (!targetDate) return existingRows;
  return [
    ...existingRows.filter((row) => {
      if (!samePartner(row)) return true;
      const rowStart = normalizeIsoDate(row[startKey]);
      const rowEnd = normalizeIsoDate(row.endDate);
      if (rowStart === targetDate) return false;
      if (!rowEnd && (!rowStart || comparePeriods(rowStart, targetDate) >= 0)) return false;
      return true;
    }),
    ...newRows
  ];
}

function getComparablePartnerRows(rows, partner, signatureFields, effectiveDate, { startKey = "startDate", endKey = "endDate" } = {}) {
  const targetDate = normalizeIsoDate(effectiveDate);
  const filtered = (rows || []).filter((row) => {
    if (row.partner !== partner) return false;
    const rowStart = normalizeIsoDate(row[startKey]);
    const rowEnd = normalizeIsoDate(row[endKey]);
    if (targetDate) {
      if (rowStart && comparePeriods(rowStart, targetDate) > 0) return false;
      if (rowEnd && comparePeriods(rowEnd, targetDate) < 0) return false;
      return true;
    }
    return !rowEnd;
  });
  return dedupeRowsByPartnerSignature(filtered, signatureFields, { startKey });
}

function mergeImportedContractRows(existingRows, newRows, { samePartner, matches, behavior, effectiveDate, startKey = "startDate" }) {
  if (behavior === "append") {
    const withoutCurrentVersion = replacePartnerRowsForEffectiveDate(existingRows, [], { samePartner, effectiveDate, startKey });
    return mergeContractRowsUnique(withoutCurrentVersion, newRows, { matches });
  }
  return replaceContractRows(existingRows, newRows, { samePartner });
}

const CONTRACT_IMPORT_SECTION_CONFIGS = [
  {
    key: "off",
    label: "Offline",
    currentSignatureFields: ["txnType", "speedFlag", "payerFunding", "payeeFunding", "payerCcy", "payeeCcy", "payerCountry", "payeeCountry", "payerCountryGroup", "payeeCountryGroup", "processingMethod", "minAmt", "maxAmt", "fee"],
    identityFields: ["txnType", "speedFlag", "payerFunding", "payeeFunding", "payerCcy", "payeeCcy", "payerCountry", "payeeCountry", "payerCountryGroup", "payeeCountryGroup", "processingMethod", "minAmt", "maxAmt"],
    compareFields: ["txnType", "speedFlag", "payerFunding", "payeeFunding", "payerCcy", "payeeCcy", "payerCountry", "payeeCountry", "payerCountryGroup", "payeeCountryGroup", "processingMethod", "minAmt", "maxAmt", "fee", "note"]
  },
  {
    key: "vol",
    label: "Volume",
    currentSignatureFields: ["txnType", "speedFlag", "payerFunding", "payeeFunding", "payeeCardType", "ccyGroup", "minVol", "maxVol", "rate", "note"],
    identityFields: ["txnType", "speedFlag", "payerFunding", "payeeFunding", "payeeCardType", "ccyGroup", "minVol", "maxVol"],
    compareFields: ["txnType", "speedFlag", "payerFunding", "payeeFunding", "payeeCardType", "ccyGroup", "minVol", "maxVol", "rate", "note"]
  },
  {
    key: "fxRates",
    label: "FX",
    currentSignatureFields: ["payerCorridor", "payerCcy", "payeeCorridor", "payeeCcy", "minTxnSize", "maxTxnSize", "minVol", "maxVol", "rate", "note"],
    identityFields: ["payerCorridor", "payerCcy", "payeeCorridor", "payeeCcy", "minTxnSize", "maxTxnSize", "minVol", "maxVol"],
    compareFields: ["payerCorridor", "payerCcy", "payeeCorridor", "payeeCcy", "minTxnSize", "maxTxnSize", "minVol", "maxVol", "rate", "note"]
  },
  {
    key: "cap",
    label: "Fee Cap",
    currentSignatureFields: ["productType", "capType", "amount"],
    identityFields: ["productType", "capType"],
    compareFields: ["productType", "capType", "amount"]
  },
  {
    key: "mins",
    label: "Minimum",
    currentSignatureFields: ["minAmount", "minVol", "maxVol", "implFeeOffset"],
    identityFields: ["minVol", "maxVol", "implFeeOffset"],
    compareFields: ["minAmount", "minVol", "maxVol", "implFeeOffset"]
  },
  {
    key: "revf",
    label: "Reversal",
    currentSignatureFields: ["payerFunding", "feePerReversal"],
    identityFields: ["payerFunding"],
    compareFields: ["payerFunding", "feePerReversal"]
  },
  {
    key: "plat",
    label: "Platform",
    currentSignatureFields: ["monthlyFee"],
    identityFields: [],
    compareFields: ["monthlyFee"]
  },
  {
    key: "impl",
    label: "Implementation",
    currentSignatureFields: ["feeType", "feeAmount", "applyAgainstMin", "creditMode", "creditAmount", "creditWindowDays", "note"],
    identityFields: ["feeType", "note"],
    compareFields: ["feeType", "feeAmount", "applyAgainstMin", "creditMode", "creditAmount", "creditWindowDays", "note"]
  },
  {
    key: "vaFees",
    label: "Virtual Acct",
    currentSignatureFields: ["feeType", "minAccounts", "maxAccounts", "discount", "feePerAccount", "note"],
    identityFields: ["feeType", "minAccounts", "maxAccounts", "note"],
    compareFields: ["feeType", "minAccounts", "maxAccounts", "discount", "feePerAccount", "note"]
  },
  {
    key: "surch",
    label: "Surcharge",
    currentSignatureFields: ["surchargeType", "rate", "minVol", "maxVol", "note"],
    identityFields: ["surchargeType", "minVol", "maxVol", "note"],
    compareFields: ["surchargeType", "rate", "minVol", "maxVol", "note"]
  }
];

function getContractImportSectionConfig(sectionKey) {
  return CONTRACT_IMPORT_SECTION_CONFIGS.find((section) => section.key === sectionKey) || null;
}

function rowsEqualByFields(left, right, fields) {
  return (fields || []).every((field) => {
    const leftValue = left?.[field];
    const rightValue = right?.[field];
    if (typeof leftValue === "number" || typeof rightValue === "number") {
      return Math.abs(Number(leftValue || 0) - Number(rightValue || 0)) < 0.00001;
    }
    return norm(leftValue) === norm(rightValue);
  });
}

function buildContractImportSections(name, parsed, effectiveDate, existingBilling) {
  const sections = [];

  if (parsed.offlineRates?.length) {
    sections.push({
      key: "off",
      rows: parsed.offlineRates.map((row) => ({
        id: uid(),
        partner: name,
        txnType: row.txnType || "Domestic",
        speedFlag: row.speedFlag || "Standard",
        minAmt: row.minAmt || 0,
        maxAmt: row.maxAmt || 10000000,
        payerFunding: "",
        payeeFunding: "",
        fee: row.fee,
        payerCcy: row.payerCcy || "USD",
        payeeCcy: row.payeeCcy || "USD",
        payerCountry: row.payerCountry || "",
        payeeCountry: row.payeeCountry || "",
        payerCountryGroup: row.payerCountryGroup || "",
        payeeCountryGroup: row.payeeCountryGroup || "",
        processingMethod: row.processingMethod || "",
        note: row.note || "",
        startDate: effectiveDate,
        endDate: ""
      }))
    });
  }

  const contractVolumeRows = (parsed.volumeRates || []).filter((row) => row.txnType !== "FX" && !row.ccyGroup);
  if (contractVolumeRows.length) {
    sections.push({
      key: "vol",
      rows: contractVolumeRows.map((row) => ({
        id: uid(),
        partner: name,
        txnType: row.txnType || "",
        speedFlag: row.speedFlag || "",
        rate: row.rate,
        payerFunding: row.payerFunding || "",
        payeeFunding: row.payeeFunding || "",
        payeeCardType: row.payeeCardType || "",
        ccyGroup: expandCcy(row.ccyGroup),
        minVol: row.minVol || 0,
        maxVol: row.maxVol || 1e9,
        startDate: effectiveDate,
        endDate: "",
        note: row.note || ""
      }))
    });
  }

  const parsedFx = [
    ...(parsed.fxRates || []),
    ...(parsed.volumeRates || []).filter((row) => row.txnType === "FX" || row.ccyGroup).map((row) => {
      const group = row.ccyGroup || "";
      let payeeCorridor = "";
      let payeeCcy = "";
      if (group === "MAJORS" || group === "Major") payeeCorridor = "Major";
      else if (group === "MINORS" || group === "Minor") payeeCorridor = "Minor";
      else if (group === "TERTIARY" || group === "Tertiary") payeeCorridor = "Tertiary";
      else if (group) {
        payeeCcy = group;
        payeeCorridor = getCorridor(group);
      }
      return {
        payerCorridor: "",
        payerCcy: "",
        payeeCorridor,
        payeeCcy,
        minTxnSize: 0,
        maxTxnSize: 1e9,
        minVol: row.minVol || 0,
        maxVol: row.maxVol || 1e9,
        rate: row.rate,
        note: row.note || ""
      };
    })
  ];
  if (parsedFx.length) {
    sections.push({
      key: "fxRates",
      rows: parsedFx.map((row) => ({
        id: uid(),
        partner: name,
        payerCorridor: row.payerCorridor || "",
        payerCcy: row.payerCcy || "",
        payeeCorridor: row.payeeCorridor || "",
        payeeCcy: row.payeeCcy || "",
        minTxnSize: row.minTxnSize || 0,
        maxTxnSize: row.maxTxnSize || 1e9,
        minVol: row.minVol || 0,
        maxVol: row.maxVol || 1e9,
        rate: row.rate,
        startDate: effectiveDate,
        endDate: "",
        note: row.note || ""
      }))
    });
  }

  if (parsed.feeCaps?.length) {
    sections.push({
      key: "cap",
      rows: parsed.feeCaps.map((row) => ({
        id: uid(),
        partner: name,
        productType: row.productType || (row.speedFlag === "RTP" ? "RTP" : row.speedFlag === "FasterACH" ? "FasterACH" : "ACH"),
        capType: row.capType || "Max Fee",
        amount: row.capAmount || row.amount || 0,
        startDate: effectiveDate,
        endDate: ""
      }))
    });
  }

  if (parsed.minimums?.length) {
    sections.push({
      key: "mins",
      rows: parsed.minimums.map((row) => ({
        id: uid(),
        partner: name,
        startDate: effectiveDate,
        endDate: "",
        minAmount: row.minAmount,
        minVol: row.minVol || 0,
        maxVol: row.maxVol || 1e9,
        implFeeOffset: false
      }))
    });
  }

  if (parsed.reversalFees?.length) {
    sections.push({
      key: "revf",
      rows: parsed.reversalFees.map((row) => ({
        id: uid(),
        partner: name,
        payerFunding: row.payerFunding || "",
        feePerReversal: row.feePerReversal,
        startDate: effectiveDate,
        endDate: ""
      }))
    });
  }

  if (parsed.platformFees?.length) {
    const rows = parsed.platformFees.filter((row) => row.monthlyFee > 0).map((row) => ({
      id: uid(),
      partner: name,
      monthlyFee: row.monthlyFee,
      startDate: effectiveDate,
      endDate: ""
    }));
    if (rows.length) sections.push({ key: "plat", rows });
  }

  if (parsed.implFees?.length) {
    sections.push({
      key: "impl",
      rows: parsed.implFees.map((row) => ({
        id: uid(),
        partner: name,
        feeType: row.feeType || "Implementation",
        feeAmount: row.feeAmount,
        goLiveDate: existingBilling?.goLiveDate || "",
        startDate: effectiveDate,
        endDate: "",
        applyAgainstMin: false,
        creditMode: row.creditMode || "",
        creditAmount: Number(row.creditAmount || 0),
        creditWindowDays: Number(row.creditWindowDays || 0),
        note: row.note || ""
      }))
    });
  }

  if (parsed.virtualAccountFees?.length) {
    sections.push({
      key: "vaFees",
      rows: parsed.virtualAccountFees.map((row) => ({
        id: uid(),
        partner: name,
        feeType: row.feeType,
        minAccounts: row.minAccounts,
        maxAccounts: row.maxAccounts,
        discount: row.discount || 0,
        feePerAccount: row.feePerAccount,
        startDate: effectiveDate,
        endDate: "",
        note: row.note || ""
      }))
    });
  }

  if (parsed.surcharges?.length) {
    sections.push({
      key: "surch",
      rows: parsed.surcharges.map((row) => ({
        id: uid(),
        partner: name,
        surchargeType: row.surchargeType || "Same Currency",
        rate: row.rate,
        minVol: row.minVol || 0,
        maxVol: row.maxVol || 1e9,
        startDate: effectiveDate,
        endDate: "",
        note: row.note || ""
      }))
    });
  }

  return sections;
}

function describeContractImportRow(sectionKey, row) {
  if (!row) return "—";
  if (sectionKey === "off") return `${row.txnType} · ${row.speedFlag}${row.processingMethod ? ` · ${row.processingMethod}` : ""} · ${row.payerCcy}/${row.payeeCcy} · ${row.minAmt}-${row.maxAmt} · ${fmt(row.fee)}`;
  if (sectionKey === "vol") return `${row.txnType || "*"} · ${row.speedFlag || "*"} · ${fmtPct(row.rate)} · vol ${row.minVol}-${row.maxVol}${row.payeeCardType ? ` · ${row.payeeCardType}` : ""}`;
  if (sectionKey === "fxRates") return `FX ${row.payeeCcy || row.payeeCorridor || "Corridor"} · ${fmtPct(row.rate)} · vol ${row.minVol}-${row.maxVol}`;
  if (sectionKey === "cap") return `${row.productType} · ${row.capType} · ${fmt(row.amount)}`;
  if (sectionKey === "mins") return `Minimum ${fmt(row.minAmount)} · vol ${row.minVol}-${row.maxVol}`;
  if (sectionKey === "revf") return `${row.payerFunding || "All funding"} reversal · ${fmt(row.feePerReversal)}`;
  if (sectionKey === "plat") return `Monthly platform fee · ${fmt(row.monthlyFee)}`;
  if (sectionKey === "impl") return `${row.feeType} · ${fmt(row.feeAmount)}${row.note ? ` · ${row.note}` : ""}`;
  if (sectionKey === "vaFees") return `${row.feeType} · ${row.minAccounts}-${row.maxAccounts} · ${fmt(row.feePerAccount)}${row.note ? ` · ${row.note}` : ""}`;
  if (sectionKey === "surch") return `${row.surchargeType} · ${fmtPct(row.rate)} · vol ${row.minVol}-${row.maxVol}`;
  return JSON.stringify(row);
}

function findMatchingExistingImportRow(existingRows, incomingRow, config, usedIndexes) {
  if (!config.identityFields.length) {
    return existingRows.findIndex((row, index) => !usedIndexes.has(index));
  }
  return existingRows.findIndex((row, index) => !usedIndexes.has(index) && rowsEqualByFields(row, incomingRow, config.identityFields));
}

function buildContractImportPlan() {
  const parsed = state.cParsed;
  const partner = state.cMode === "verify" ? state.cVerifyPartner : state.cName;
  if (!parsed || !partner || !state.ps.includes(partner)) return null;

  const existingBilling = getPartnerBillingConfig(partner);
  const effectiveDate = parsed.effectiveDate || existingBilling?.contractStartDate || "";
  const importBehavior = state.cImportBehavior || "override";
  const sections = buildContractImportSections(partner, parsed, effectiveDate, existingBilling);
  const changes = [];

  sections.forEach((section) => {
    const config = getContractImportSectionConfig(section.key);
    if (!config || !section.rows.length) return;
    const comparableRows = getComparablePartnerRows(state[section.key], partner, config.currentSignatureFields, effectiveDate);
    const futureOpenRows = effectiveDate
      ? (state[section.key] || []).filter((row) => row.partner === partner && !normalizeIsoDate(row.endDate) && normalizeIsoDate(row.startDate) && comparePeriods(normalizeIsoDate(row.startDate), effectiveDate) > 0)
      : [];
    const existingRows = [
      ...comparableRows,
      ...futureOpenRows.filter((row) => !comparableRows.some((existing) => (existing.id && row.id && existing.id === row.id)))
    ];
    const usedIndexes = new Set();

    section.rows.forEach((row, index) => {
      const existingIndex = findMatchingExistingImportRow(existingRows, row, config, usedIndexes);
      const existingRow = existingIndex >= 0 ? existingRows[existingIndex] : null;
      const changeId = `${section.key}:${importBehavior}:${index}:${rowSignature(row, config.identityFields.length ? config.identityFields : config.compareFields)}`;

      if (importBehavior === "append") {
        if (!existingRow) {
          changes.push({
            id: changeId,
            section: section.key,
            sectionLabel: config.label,
            action: "add",
            newRow: row,
            existingRow: null
          });
        }
        return;
      }

      if (!existingRow) {
        changes.push({
          id: changeId,
          section: section.key,
          sectionLabel: config.label,
          action: "add",
          newRow: row,
          existingRow: null
        });
        return;
      }

      usedIndexes.add(existingIndex);
      if (!rowsEqualByFields(existingRow, row, config.compareFields)) {
        changes.push({
          id: changeId,
          section: section.key,
          sectionLabel: config.label,
          action: "replace",
          newRow: row,
          existingRow
        });
      }
    });

    if (importBehavior === "override") {
      existingRows.forEach((row, index) => {
        if (usedIndexes.has(index)) return;
        changes.push({
          id: `${section.key}:remove:${index}:${row.id || rowSignature(row, config.compareFields)}`,
          section: section.key,
          sectionLabel: config.label,
          action: "remove",
          newRow: null,
          existingRow: row
        });
      });
    }
  });

  const counts = changes.reduce((acc, change) => {
    acc[change.action] = (acc[change.action] || 0) + 1;
    return acc;
  }, { add: 0, replace: 0, remove: 0 });

  return {
    partner,
    behavior: importBehavior,
    effectiveDate,
    changes,
    counts
  };
}

function refreshContractImportPlan() {
  const plan = buildContractImportPlan();
  state.cImportPlan = plan;
  state.cSelectedImportRows = plan
    ? Object.fromEntries(plan.changes.map((change) => [change.id, true]))
    : {};
}

function getSelectedContractImportChanges() {
  const plan = state.cImportPlan || buildContractImportPlan();
  if (!plan) return [];
  return plan.changes.filter((change) => state.cSelectedImportRows[change.id] !== false);
}

function setAllContractImportRowsSelected(selected) {
  if (!state.cImportPlan) return;
  state.cSelectedImportRows = Object.fromEntries(state.cImportPlan.changes.map((change) => [change.id, !!selected]));
}

function applySelectedContractImportChanges(existingRows, selectedChanges, sectionKey) {
  if (!selectedChanges.length) return existingRows;
  const config = getContractImportSectionConfig(sectionKey);
  const removeReferences = selectedChanges
    .filter((change) => change.existingRow)
    .map((change) => change.existingRow);
  const remainingRows = existingRows.filter((row) => {
    return !removeReferences.some((existingRow) => {
      if (existingRow?.id && row?.id) return row.id === existingRow.id;
      return row.partner === existingRow.partner
        && normalizeIsoDate(row.startDate) === normalizeIsoDate(existingRow.startDate)
        && normalizeIsoDate(row.endDate) === normalizeIsoDate(existingRow.endDate)
        && rowsEqualByFields(row, existingRow, config?.compareFields || []);
    });
  });
  const addedRows = selectedChanges
    .filter((change) => change.action === "add" || change.action === "replace")
    .map((change) => ({ ...change.newRow, id: uid() }));
  return [...remainingRows, ...addedRows];
}

function importToWorkbook() {
  const targetName = state.cMode === "verify" ? state.cVerifyPartner : state.cName;
  if (!state.cParsed || !targetName) return;
  const parsed = state.cParsed;
  const name = targetName;
  const existingBilling = getPartnerBillingConfig(name);
  const effectiveDate = parsed.effectiveDate || existingBilling?.contractStartDate || "";
  const importBehavior = state.ps.includes(name) ? (state.cImportBehavior || "override") : "override";
  const shouldEnableIncremental = state.cDetectedIncremental || detectPerTierMarginalPricing(state.cText);
  const sections = buildContractImportSections(name, parsed, effectiveDate, existingBilling);
  const importPlan = state.cImportPlan && state.cImportPlan.partner === name && state.cImportPlan.behavior === importBehavior
    ? state.cImportPlan
    : buildContractImportPlan();
  const selectedChanges = state.ps.includes(name)
    ? getSelectedContractImportChanges()
    : sections.flatMap((section) => section.rows.map((row, index) => ({
      id: `${section.key}:new:${index}`,
      section: section.key,
      sectionLabel: getContractImportSectionConfig(section.key)?.label || section.key,
      action: "add",
      newRow: row,
      existingRow: null
    })));

  if (state.ps.includes(name) && !selectedChanges.length) {
    showToast("No rows selected", "Select at least one contract row to update before pushing to the workbook.", "warning");
    return;
  }

  if (!state.ps.includes(name)) state.ps = [...state.ps, name];
  if (shouldEnableIncremental) state.pConfig = { ...state.pConfig, [name]: true };
  upsertPartnerBilling(name, {
    billingFreq: existingBilling?.billingFreq || "Monthly",
    payBy: existingBilling?.payBy || "",
    dueDays: Number(existingBilling?.dueDays || 0),
    billingDay: existingBilling?.billingDay || "",
    contractStartDate: existingBilling?.contractStartDate || effectiveDate,
    goLiveDate: existingBilling?.goLiveDate || "",
    notYetLive: existingBilling?.notYetLive ?? false,
    integrationStatus: existingBilling?.integrationStatus || "",
    note: existingBilling?.note || ""
  }, { persist: false, log: false });
  if (parsed.billingTerms && (parsed.billingTerms.payBy || parsed.billingTerms.billingFreq)) {
    upsertPartnerBilling(name, {
      billingFreq: parsed.billingTerms.billingFreq || existingBilling?.billingFreq || "Monthly",
      payBy: parsed.billingTerms.payBy || existingBilling?.payBy || "",
      dueDays: parseDueDaysFromPayBy(parsed.billingTerms.payBy || existingBilling?.payBy || "") || Number(existingBilling?.dueDays || 0),
      billingDay: existingBilling?.billingDay || "",
      contractStartDate: existingBilling?.contractStartDate || effectiveDate,
      goLiveDate: existingBilling?.goLiveDate || "",
      notYetLive: existingBilling?.notYetLive ?? false,
      integrationStatus: existingBilling?.integrationStatus || "",
      note: existingBilling?.note || ""
    }, { persist: false, log: false });
  }

  if (state.ps.includes(name) && importPlan) {
    const selectedBySection = selectedChanges.reduce((acc, change) => {
      if (!acc[change.section]) acc[change.section] = [];
      acc[change.section].push(change);
      return acc;
    }, {});
    Object.entries(selectedBySection).forEach(([sectionKey, changes]) => {
      state[sectionKey] = applySelectedContractImportChanges(state[sectionKey], changes, sectionKey);
    });
  } else {
    sections.forEach((section) => {
      state[section.key] = [...state[section.key], ...section.rows];
    });
  }

  cleanupDuplicateContractRows();
  state.cImportSummary = selectedChanges.reduce((acc, change) => {
    acc[change.section] = (acc[change.section] || 0) + 1;
    return acc;
  }, {});
  state.cImportPlan = buildContractImportPlan();
  state.cSelectedImportRows = state.cImportPlan
    ? Object.fromEntries(state.cImportPlan.changes.map((change) => [change.id, true]))
    : {};
  state.cImported = true;
  state.cDiff = null;
  persistAndRender();
  logWorkbookChange(
    importBehavior === "append" ? "append_contract_rows" : "override_contract_rows",
    `${importBehavior === "append" ? "Added" : "Replaced"} ${selectedChanges.length} contract change${selectedChanges.length === 1 ? "" : "s"} for ${name}.`,
    { partner: name, section: "contract_import", behavior: importBehavior, changeCount: selectedChanges.length }
  );
  showToast(
    importBehavior === "append" ? "New contract fees added" : "Existing contract rows overridden",
    importBehavior === "append"
      ? `Applied ${selectedChanges.length} selected new fee row${selectedChanges.length === 1 ? "" : "s"} for ${name}. Existing workbook rows were preserved, so Verify may still show differences until you use Override existing rows.`
      : `Applied ${selectedChanges.length} selected contract change${selectedChanges.length === 1 ? "" : "s"} for ${name}.`,
    "success"
  );
}

function verifyContract() {
  if (!state.cParsed) return;
  const parsed = state.cParsed;
  const name = state.cMode === "verify" ? state.cVerifyPartner : state.cName;
  if (!name) return;
  refreshContractImportPlan();
  const comparisonEffectiveDate = parsed.effectiveDate || getPartnerContractStartDate(name) || "";
  const results = [];
  const match = (cat, label, contractVal, workbookVal, key) => {
    const isNum = typeof contractVal === "number" && typeof workbookVal === "number";
    const ok = isNum ? Math.abs(contractVal - workbookVal) < 0.00001 : String(contractVal || "") === String(workbookVal || "");
    results.push({ cat, label, contractVal, workbookVal, status: ok ? "match" : "mismatch", key });
  };
  const missing = (cat, label, detail) => results.push({ cat, label, contractVal: detail, workbookVal: "—", status: "missing", key: "" });
  const extra = (cat, label, detail) => results.push({ cat, label, contractVal: "—", workbookVal: detail, status: "extra", key: "" });

  const wbOff = getComparablePartnerRows(state.off, name, ["txnType", "speedFlag", "payerFunding", "payeeFunding", "payerCcy", "payeeCcy", "payerCountry", "payeeCountry", "payerCountryGroup", "payeeCountryGroup", "processingMethod", "minAmt", "maxAmt", "fee"], comparisonEffectiveDate);
  (parsed.offlineRates || []).forEach((contractRow) => {
    const desc = `${contractRow.txnType} ${contractRow.speedFlag} ${contractRow.processingMethod || ""} [${contractRow.minAmt}-${contractRow.maxAmt}]`.trim();
    const wb = wbOff.find((row) => row.txnType === contractRow.txnType && row.speedFlag === contractRow.speedFlag && (row.processingMethod || "") === (contractRow.processingMethod || "") && row.minAmt <= contractRow.minAmt && row.maxAmt >= contractRow.maxAmt);
    if (!wb) {
      missing("Offline", desc, `$${contractRow.fee}`);
      return;
    }
    match("Offline", `${desc} fee`, contractRow.fee, wb.fee, "fee");
  });
  wbOff.forEach((row) => {
    const desc = `${row.txnType} ${row.speedFlag} ${row.processingMethod || ""} [${row.minAmt}-${row.maxAmt}]`.trim();
    const contractRow = (parsed.offlineRates || []).find((item) => item.txnType === row.txnType && item.speedFlag === row.speedFlag && (item.processingMethod || "") === (row.processingMethod || "") && row.minAmt <= item.maxAmt && row.maxAmt >= item.minAmt);
    if (!contractRow) extra("Offline", desc, `$${row.fee}`);
  });

  const wbVol = getComparablePartnerRows(state.vol, name, ["txnType", "speedFlag", "payerFunding", "payeeFunding", "payeeCardType", "ccyGroup", "minVol", "maxVol", "rate", "note"], comparisonEffectiveDate);
  const contractVolumeRows = (parsed.volumeRates || []).filter((row) => row.txnType !== "FX" && !row.ccyGroup);
  contractVolumeRows.forEach((contractRow) => {
    const desc = `${contractRow.txnType || "*"} ${contractRow.speedFlag || "*"} ${contractRow.payerFunding ? "payer:" + contractRow.payerFunding : ""} ${contractRow.payeeCardType || ""} ${contractRow.ccyGroup || ""} [vol ${contractRow.minVol}-${contractRow.maxVol}]`.replace(/\s+/g, " ").trim();
    const wb = wbVol.find((row) => (!contractRow.txnType || row.txnType === contractRow.txnType) && (!contractRow.speedFlag || row.speedFlag === contractRow.speedFlag) && (!contractRow.payerFunding || row.payerFunding === contractRow.payerFunding) && (!contractRow.payeeFunding || row.payeeFunding === contractRow.payeeFunding) && (!contractRow.payeeCardType || row.payeeCardType === contractRow.payeeCardType) && Math.abs(row.rate - contractRow.rate) < 0.00001 && row.minVol <= contractRow.minVol + 1 && row.maxVol >= contractRow.maxVol - 1);
    if (!wb) {
      const wbClose = wbVol.find((row) => (!contractRow.txnType || row.txnType === contractRow.txnType) && (!contractRow.speedFlag || row.speedFlag === contractRow.speedFlag) && (!contractRow.payerFunding || row.payerFunding === contractRow.payerFunding) && (!contractRow.payeeCardType || row.payeeCardType === contractRow.payeeCardType));
      if (wbClose) match("Volume", `${desc} rate`, contractRow.rate, wbClose.rate, "rate");
      else missing("Volume", desc, `${(contractRow.rate * 100).toFixed(4)}%`);
      return;
    }
    match("Volume", `${desc} rate`, contractRow.rate, wb.rate, "rate");
  });

  const wbFx = getComparablePartnerRows(state.fxRates, name, ["payerCorridor", "payerCcy", "payeeCorridor", "payeeCcy", "minTxnSize", "maxTxnSize", "minVol", "maxVol", "rate", "note"], comparisonEffectiveDate);
  const parsedFx = [...(parsed.fxRates || []), ...(parsed.volumeRates || []).filter((row) => row.txnType === "FX" || row.ccyGroup)];
  parsedFx.forEach((contractRow) => {
    const group = contractRow.ccyGroup || contractRow.payeeCorridor || "";
    const ccy = contractRow.payeeCcy || "";
    const desc = `FX ${ccy || group} rate=${fmtPct(contractRow.rate)} [vol ${contractRow.minVol || 0}-${contractRow.maxVol || "∞"}]`;
    let corridor = "";
    if (group === "MAJORS" || group === "Major") corridor = "Major";
    else if (group === "MINORS" || group === "Minor") corridor = "Minor";
    else if (group === "TERTIARY" || group === "Tertiary") corridor = "Tertiary";
    else if (group && ALL_CCYS.includes(group)) corridor = getCorridor(group);
    const payeeCcy = ccy || (group && ALL_CCYS.includes(group) ? group : "");
    const wb = wbFx.find((row) => (payeeCcy ? row.payeeCcy === payeeCcy : (!row.payeeCcy && row.payeeCorridor === corridor)) && Math.abs(row.rate - contractRow.rate) < 0.00001);
    if (!wb) {
      missing("FX", desc, fmtPct(contractRow.rate));
      return;
    }
    match("FX", desc, contractRow.rate, wb.rate, "rate");
  });

  const wbCap = getComparablePartnerRows(state.cap, name, ["productType", "capType", "amount"], comparisonEffectiveDate);
  (parsed.feeCaps || []).forEach((contractRow) => {
    const productType = contractRow.productType || (contractRow.speedFlag === "RTP" ? "RTP" : contractRow.speedFlag === "FasterACH" ? "FasterACH" : "ACH");
    const capType = contractRow.capType || "Max Fee";
    const amount = contractRow.amount || contractRow.capAmount || 0;
    const wb = wbCap.find((row) => row.productType === productType && row.capType === capType);
    if (!wb) {
      missing("Fee Cap", `${productType} ${capType}`, fmt(amount));
      return;
    }
    match("Fee Cap", `${productType} ${capType}`, amount, wb.amount, "amount");
  });

  const wbMin = getComparablePartnerRows(state.mins, name, ["minAmount", "minVol", "maxVol", "implFeeOffset"], comparisonEffectiveDate);
  (parsed.minimums || []).forEach((contractRow) => {
    const wb = wbMin.find((row) => Math.abs(row.minAmount - contractRow.minAmount) < 1 && row.minVol <= (contractRow.minVol || 0) + 1 && row.maxVol >= (contractRow.maxVol || 1e9) - 1);
    if (!wb) {
      missing("Minimum", `$${contractRow.minAmount} [vol ${contractRow.minVol || 0}-${contractRow.maxVol || "∞"}]`, fmt(contractRow.minAmount));
      return;
    }
    match("Minimum", `Min ${fmt(contractRow.minAmount)}`, contractRow.minAmount, wb.minAmount, "minAmount");
  });
  wbMin.forEach((row) => {
    const contractRow = (parsed.minimums || []).find((item) => Math.abs(item.minAmount - row.minAmount) < 1);
    if (!contractRow) extra("Minimum", `$${row.minAmount} [vol ${row.minVol}-${row.maxVol}]`, fmt(row.minAmount));
  });

  const wbRev = getComparablePartnerRows(state.revf, name, ["payerFunding", "feePerReversal"], comparisonEffectiveDate);
  const usedReversalRows = new Set();
  (parsed.reversalFees || []).forEach((contractRow) => {
    const exactIndex = wbRev.findIndex((row, index) => !usedReversalRows.has(index) && (row.payerFunding || "") === (contractRow.payerFunding || "") && Math.abs(Number(row.feePerReversal || 0) - Number(contractRow.feePerReversal || 0)) < 0.00001);
    const fallbackIndex = wbRev.findIndex((row, index) => !usedReversalRows.has(index) && (row.payerFunding || "") === (contractRow.payerFunding || ""));
    const wbIndex = exactIndex >= 0 ? exactIndex : fallbackIndex;
    const wb = wbIndex >= 0 ? wbRev[wbIndex] : null;
    if (!wb) {
      missing("Reversal", `${contractRow.payerFunding || "All"} reversal`, fmt(contractRow.feePerReversal));
      return;
    }
    usedReversalRows.add(wbIndex);
    match("Reversal", `${contractRow.payerFunding || "All"} reversal fee`, contractRow.feePerReversal, wb.feePerReversal, "fee");
  });
  wbRev.forEach((row, index) => {
    if (usedReversalRows.has(index)) return;
    extra("Reversal", `${row.payerFunding || "All"} reversal`, fmt(row.feePerReversal));
  });

  const wbPlat = getComparablePartnerRows(state.plat, name, ["monthlyFee"], comparisonEffectiveDate);
  (parsed.platformFees || []).filter((row) => row.monthlyFee > 0).forEach((contractRow) => {
    const wb = wbPlat.find((row) => Math.abs(row.monthlyFee - contractRow.monthlyFee) < 1);
    if (!wb) {
      missing("Platform", "Monthly fee", fmt(contractRow.monthlyFee));
      return;
    }
    match("Platform", "Monthly fee", contractRow.monthlyFee, wb.monthlyFee, "monthlyFee");
  });

  const wbImpl = getComparablePartnerRows(state.impl.map((row) => normalizeImplementationRow(row)), name, ["feeType", "feeAmount", "applyAgainstMin", "creditMode", "creditAmount", "creditWindowDays", "note"], comparisonEffectiveDate);
  (parsed.implFees || []).forEach((contractRow) => {
    const wb = wbImpl.find((row) => row.feeType === contractRow.feeType && (row.note || "") === (contractRow.note || ""));
    if (!wb) {
      missing("Impl", contractRow.feeType, fmt(contractRow.feeAmount));
      return;
    }
    match("Impl", `${contractRow.feeType} amount`, contractRow.feeAmount, wb.feeAmount, "feeAmount");
    match("Impl", `${contractRow.feeType} credit target`, contractRow.creditMode || "", wb.creditMode || "", "creditMode");
    match("Impl", `${contractRow.feeType} credit amount`, Number(contractRow.creditAmount || 0), Number(wb.creditAmount || 0), "creditAmount");
    match("Impl", `${contractRow.feeType} launch window`, Number(contractRow.creditWindowDays || 0), Number(wb.creditWindowDays || 0), "creditWindowDays");
  });

  const wbVa = getComparablePartnerRows(state.vaFees, name, ["feeType", "minAccounts", "maxAccounts", "discount", "feePerAccount", "note"], comparisonEffectiveDate);
  (parsed.virtualAccountFees || []).forEach((contractRow) => {
    const wb = wbVa.find((row) => row.feeType === contractRow.feeType && row.minAccounts === contractRow.minAccounts);
    if (!wb) {
      missing("Virtual Acct", `${contractRow.feeType} [${contractRow.minAccounts}-${contractRow.maxAccounts}]`, fmt(contractRow.feePerAccount));
      return;
    }
    match("Virtual Acct", `${contractRow.feeType} [${contractRow.minAccounts}-${contractRow.maxAccounts}] fee`, contractRow.feePerAccount, wb.feePerAccount, "feePerAccount");
  });

  const wbSurch = getComparablePartnerRows(state.surch, name, ["surchargeType", "rate", "minVol", "maxVol", "note"], comparisonEffectiveDate);
  (parsed.surcharges || []).forEach((contractRow) => {
    const wb = wbSurch.find((row) => row.surchargeType === contractRow.surchargeType && Math.abs(row.rate - contractRow.rate) < 0.00001 && row.minVol <= (contractRow.minVol || 0) + 1 && row.maxVol >= (contractRow.maxVol || 1e9) - 1);
    if (!wb) {
      missing("Surcharge", `${contractRow.surchargeType} ${fmtPct(contractRow.rate)}`, fmtPct(contractRow.rate));
      return;
    }
    match("Surcharge", `${contractRow.surchargeType} rate`, contractRow.rate, wb.rate, "rate");
  });
  wbSurch.forEach((row) => {
    const contractRow = (parsed.surcharges || []).find((item) => item.surchargeType === row.surchargeType && Math.abs(item.rate - row.rate) < 0.00001);
    if (!contractRow) extra("Surcharge", `${row.surchargeType} ${fmtPct(row.rate)}`, fmtPct(row.rate));
  });

  state.cDiff = {
    partner: name,
    results,
    matches: results.filter((row) => row.status === "match").length,
    mismatches: results.filter((row) => row.status === "mismatch").length,
    missing: results.filter((row) => row.status === "missing").length,
    extra: results.filter((row) => row.status === "extra").length
  };
  if (!state.cImportPlan) refreshContractImportPlan();
  render();
}

function deletePartner(name) {
  state.ps = state.ps.filter((partner) => partner !== name);
  state.pArchived = state.pArchived.filter((partner) => norm(partner) !== norm(name));
  state.pActive = state.pActive.filter((row) => row.partner !== name);
  state.pBilling = state.pBilling.filter((row) => row.partner !== name);
  state.pInvoices = state.pInvoices.filter((row) => row.partner !== name);
  state.off = state.off.filter((row) => row.partner !== name);
  state.vol = state.vol.filter((row) => row.partner !== name);
  state.fxRates = state.fxRates.filter((row) => row.partner !== name);
  state.cap = state.cap.filter((row) => row.partner !== name);
  state.rs = state.rs.filter((row) => row.partner !== name);
  state.mins = state.mins.filter((row) => row.partner !== name);
  state.plat = state.plat.filter((row) => row.partner !== name);
  state.revf = state.revf.filter((row) => row.partner !== name);
  state.impl = state.impl.filter((row) => row.partner !== name);
  state.vaFees = state.vaFees.filter((row) => row.partner !== name);
  state.surch = state.surch.filter((row) => row.partner !== name);
  state.ltxn = state.ltxn.filter((row) => row.partner !== name);
  state.lrev = state.lrev.filter((row) => row.partner !== name);
  state.lva = state.lva.filter((row) => row.partner !== name);
  state.lrs = state.lrs.filter((row) => row.partner !== name);
  state.lfxp = state.lfxp.filter((row) => row.partner !== name);
  state.pConfig = Object.fromEntries(Object.entries(state.pConfig).filter(([key]) => key !== name));
  if (state.sp === name) state.sp = "";
  if (state.pv === name) state.pv = "";
  state.confirmDel = false;
  state.inv = null;
  state.invoiceExplorer = null;
  persistAndRender();
  logWorkbookChange("delete_partner", `Deleted partner ${name} and all related workbook rows.`, { partner: name, section: "partner" });
}

function archivePartner(name) {
  if (!name || isPartnerArchived(name)) return;
  state.pArchived = [...state.pArchived, name];
  if (state.sp === name) state.sp = "";
  if (state.inv?.partner === name) state.inv = null;
  state.confirmDel = false;
  state.invoiceExplorer = null;
  persistAndRender();
  logWorkbookChange("archive_partner", `Archived partner ${name}.`, { partner: name, section: "partner" });
}

function unarchivePartner(name) {
  if (!name) return;
  state.pArchived = state.pArchived.filter((partner) => norm(partner) !== norm(name));
  state.confirmDel = false;
  persistAndRender();
  logWorkbookChange("unarchive_partner", `Restored archived partner ${name}.`, { partner: name, section: "partner" });
}

function applyInvoiceResult(invoice) {
  if (!invoice) return;
  state.invoiceExplorer = null;
  state.invoiceArtifactStatus = "idle";
  state.invoiceArtifactError = "";
  state.invoiceArtifactRecord = null;
  state.privateInvoiceLinkStatus = "idle";
  state.privateInvoiceLinkError = "";
  state.privateInvoiceLinkResult = null;
  const preservedOpenSections = Object.fromEntries(Object.entries(state.openSections).filter(([key]) => !key.includes("invoice-group:")));
  const groups = (Array.isArray(invoice.groups) && invoice.groups.length ? invoice.groups : groupInvoiceLines(invoice.lines || [])).map((group, index) => ({
    id: group.id || `invoice-group-remote-${index}`,
    ...group
  }));
  groups.forEach((group) => {
    preservedOpenSections[invoiceGroupSectionKey(group.id)] = false;
  });
  preservedOpenSections[sectionKey(`invoice-notes:${invoice.partner}:${invoice.period}`)] = false;
  state.openSections = preservedOpenSections;
  const status = summarizeInvoiceRangeStatus(invoice.partner, invoice.periodStart || invoice.period, invoice.periodEnd || invoice.period);
  state.inv = {
    ...invoice,
    groups,
    status
  };
  render();
}

function calculateLocalInvoiceForPeriod(partner, period, options = {}) {
  const { skipImplementationCredits = false, suppressNotes = false } = options;
  const activePartner = partner;
  const activePeriod = period;
  if (!isPartnerActiveForPeriod(state, activePartner, activePeriod)) {
    return {
      partner: activePartner,
      period: activePeriod,
      periodStart: activePeriod,
      periodEnd: activePeriod,
      periodLabel: formatPeriodRangeLabel(activePeriod, activePeriod),
      periodDateRange: formatPeriodDateRange(activePeriod, activePeriod),
      lines: [],
      groups: [],
      notes: [`Partner marked inactive for ${formatPeriodLabel(activePeriod)}. Billing was skipped for this month.`],
      inactivePeriod: true,
      chg: 0,
      pay: 0,
      net: 0,
      dir: "Partner Owes Us"
    };
  }
  const lines = [];
  const notes = [];
  // Defensive filter: dedicated Stampli USD Abroad / credit-complete feeds were
  // confirmed wrong on 2026-04-21; offline_billing is authoritative. Ingest paths
  // and snapshot migrations also strip these, this is belt-and-suspenders.
  const txns = state.ltxn.filter((row) => row.partner === activePartner && row.period === activePeriod && !isUntrustedDirectInvoiceRow(row));
  const revs = state.lrev.filter((row) => row.partner === activePartner && row.period === activePeriod);
  const revShareSummaries = state.lrs.filter((row) => row.partner === activePartner && row.period === activePeriod);
  const fxPartnerPayoutRows = state.lfxp.filter((row) => row.partner === activePartner && row.period === activePeriod);
  const revShareRows = state.rs.filter((row) => row.partner === activePartner && inRange(activePeriod + "-15", row.startDate, row.endDate));
  const isIncremental = !!state.pConfig[activePartner];
  const periodVolume = txns.reduce((sum, row) => sum + row.totalVolume, 0);
  const recurringBillingActive = isRecurringBillingLiveForPeriod(activePartner, activePeriod);
    const summaryChargeRows = revShareSummaries.filter((row) => Number(row.revenueOwed || 0) > 0);
    const summaryPayRows = revShareSummaries.filter((row) => Number(row.partnerRevenueShare || 0) > 0);
    const authoritativePayoutSummary = !!summaryPayRows.length && !revShareRows.length && !fxPartnerPayoutRows.length;
  const authoritativeRecurringChargeSummary = summaryChargeRows.some((row) => String(row.revenueSource || "") === "billing_summary");
  const minimumRow = recurringBillingActive
    ? state.mins.find((row) => row.partner === activePartner && inRange(activePeriod + "-15", row.startDate, row.endDate) && periodVolume >= row.minVol && periodVolume <= row.maxVol)
    : null;
  const summaryMinimumAmount = revShareSummaries.reduce((max, row) => Math.max(max, Number(row.monthlyMinimumRevenue || 0)), 0);
  const effectiveMinimumAmount = minimumRow?.minAmount > 0 ? minimumRow.minAmount : summaryMinimumAmount;
  const fxMarkupActivityRows = txns.filter((row) => (row.txnType === "FX" || (row.payerCcy === "USD" && row.payeeCcy && row.payeeCcy !== "USD")) && row.processingMethod === "Wire");
  const preCollectedRevenueTotal = roundCurrency(txns.reduce((sum, row) => sum + Number(row.estRevenue || 0), 0));
  const appendLine = ({ activityRows = [], groupLabel = "", groupKey = "", active = true, minimumEligible = false, ...line }) => {
    const normalizedGroupLabel = groupLabel || line.desc;
    lines.push({
      id: uid(),
      ...line,
      active,
      minimumEligible,
      groupLabel: normalizedGroupLabel,
      groupKey: groupKey || `${line.cat}|${line.dir}|${normalizedGroupLabel}`,
      activityRows
    });
  };
  const applyPreCollectedRevenueOffsets = () => {
    if (!recurringBillingActive) return 0;
    if (authoritativeRecurringChargeSummary) return 0;
    const remainingByActivityKey = new Map(
      txns
        .filter((row) => Number(row.estRevenue || 0) > 0)
        .map((row) => [activityRowKey(row), roundCurrency(Number(row.estRevenue || 0))])
    );
    if (!remainingByActivityKey.size) return 0;
    let usedTotal = 0;
    lines.forEach((line) => {
      if (line.dir !== "charge" || line.minimumEligible !== true || line.active === false) return;
      const activityRows = line.activityRows || [];
      if (!activityRows.length) return;
      const available = roundCurrency(activityRows.reduce((sum, row) => sum + Number(remainingByActivityKey.get(activityRowKey(row)) || 0), 0));
      if (available <= 0) return;
      const amount = Number(line.amount || 0);
      if (!(amount > 0)) return;
      const credit = roundCurrency(Math.min(amount, available));
      if (!(credit > 0)) return;
      let remainingCredit = credit;
      activityRows.forEach((row) => {
        if (!(remainingCredit > 0)) return;
        const key = activityRowKey(row);
        const remaining = Number(remainingByActivityKey.get(key) || 0);
        if (!(remaining > 0)) return;
        const applied = roundCurrency(Math.min(remaining, remainingCredit));
        remainingByActivityKey.set(key, roundCurrency(remaining - applied));
        remainingCredit = roundCurrency(remainingCredit - applied);
      });
      usedTotal = roundCurrency(usedTotal + credit);
      if (credit >= amount - 0.01) {
        line.active = false;
        line.inactiveReason = `Already charged at transaction time via Est Revenue ${fmt(credit)}`;
      } else {
        line.amount = roundCurrency(amount - credit);
        line.desc = `${line.desc} (less ${fmt(credit)} already charged)`;
      }
    });
    return usedTotal;
  };
  const applyMonthlyMinimumRule = () => {
    if (!recurringBillingActive) return;
    if (!(effectiveMinimumAmount > 0)) return;
    if (authoritativeRecurringChargeSummary) return;
    const eligibleLines = lines.filter((line) => line.dir === "charge" && line.minimumEligible && line.active !== false);
    const invoicedGeneratedRevenue = eligibleLines.reduce((sum, line) => sum + Number(line.amount || 0), 0);
    const generatedRevenue = roundCurrency(invoicedGeneratedRevenue + preCollectedRevenueTotal);
    const minimumDesc = `Monthly minimum fee for period (${fmt(effectiveMinimumAmount)})`;
    if (generatedRevenue < effectiveMinimumAmount) {
      eligibleLines.forEach((line) => {
        line.active = false;
        line.inactiveReason = `Replaced by monthly minimum ${fmt(effectiveMinimumAmount)}`;
      });
      appendLine({
        cat: "Minimum",
        desc: preCollectedRevenueTotal > 0
          ? `${minimumDesc} replaces ${fmt(invoicedGeneratedRevenue)} invoiced revenue + ${fmt(preCollectedRevenueTotal)} pre-collected revenue`
          : `${minimumDesc} replaces ${fmt(generatedRevenue)} generated revenue`,
        amount: effectiveMinimumAmount,
        dir: "charge",
        groupLabel: "Monthly minimum",
        implementationCreditEligible: "monthly_minimum"
      });
    } else {
      appendLine({
        cat: "Minimum",
        desc: minimumDesc,
        amount: effectiveMinimumAmount,
        dir: "charge",
        groupLabel: "Monthly minimum",
        active: false
      });
      lines[lines.length - 1].inactiveReason = preCollectedRevenueTotal > 0
        ? `Not applicable because invoiced revenue ${fmt(invoicedGeneratedRevenue)} + pre-collected revenue ${fmt(preCollectedRevenueTotal)} exceeds minimum`
        : `Not applicable because generated revenue ${fmt(generatedRevenue)} exceeds minimum`;
    }
  };

  const getProductType = (txn, rate) => {
    if (rate && rate.ccyGroup === "GBP" && !rate.txnType) return "GBP 0.7%";
    if (rate && rate.speedFlag === "RTP") return "RTP";
    if (rate && rate.speedFlag === "FasterACH") return "FasterACH";
    if (txn.speedFlag === "RTP" || (rate && rate.speedFlag === "RTP")) return "RTP";
    if (txn.speedFlag === "FasterACH" || (rate && rate.speedFlag === "FasterACH")) return "FasterACH";
    if (txn.processingMethod === "Wire" || (rate?.txnType === "FX" && rate?.processingMethod === "Wire")) return "Wire";
    if (rate && rate.payerFunding === "Card" && rate.payeeCardType === "Credit" && rate.txnType === "FX") return "Card Credit FX";
    if (rate && rate.payerFunding === "Card" && rate.payeeCardType === "Credit") return "Card Credit Domestic";
    if (rate && rate.payerFunding === "Card" && rate.payeeCardType === "Debit" && rate.txnType === "FX") return "Card Debit FX";
    if (rate && rate.payerFunding === "Card" && rate.payeeCardType === "Debit") return "Card Debit Domestic";
    if (rate && rate.payeeFunding === "Card" && rate.payeeCardType === "Debit") return "Push-to-Debit";
    if (rate && rate.txnType === "FX") {
      const corridor = rate.ccyGroup === MAJORS ? "Major" : rate.ccyGroup === MINORS ? "Minor" : rate.ccyGroup === TERTIARY ? "Tertiary" : getCorridor(rate.ccyGroup || "");
      if (corridor === "Major") return "FX Majors";
      if (corridor === "Minor") return "FX Minors";
      if (corridor === "Tertiary") return "FX Tertiary";
      return "FX Majors";
    }
    return "ACH";
  };

  const applyFeeCaps = (partner, productType, feePerTxn, txnCount) => {
    const activeCaps = state.cap
      .filter((row) => row.partner === partner && row.productType === productType && inRange(activePeriod + "-15", row.startDate, row.endDate))
      .sort((a, b) => String(b.startDate || "").localeCompare(String(a.startDate || "")));
    const maxCap = activeCaps.find((row) => row.capType === "Max Fee");
    const minCap = activeCaps.find((row) => row.capType === "Min Fee");
    let adjFee = feePerTxn;
    let capNote = "";
    if (maxCap && adjFee > maxCap.amount) {
      adjFee = maxCap.amount;
      capNote = ` MAX@${fmt(maxCap.amount)}/txn`;
    }
    if (minCap && adjFee < minCap.amount) {
      adjFee = minCap.amount;
      capNote = ` MIN@${fmt(minCap.amount)}/txn`;
    }
    return { adjFee, total: adjFee * txnCount, capNote, capped: capNote !== "" };
  };

  const volumeGroupSignature = (row) => `${row.txnType}|${row.speedFlag}|${row.payerFunding}|${row.payeeFunding}|${row.payeeCardType}|${row.ccyGroup}`;
  const buildRateGroupLabel = (activityRows, rateRow) => {
    const noteLabel = String(rateRow?.note || "").trim();
    if (noteLabel) return noteLabel;
    const txnTypes = [...new Set((activityRows || []).map((row) => row.txnType).filter(Boolean))];
    const speedFlags = [...new Set((activityRows || []).map((row) => row.speedFlag).filter(Boolean))];
    if (txnTypes.length === 1) {
      return [txnTypes[0], speedFlags[0] || rateRow?.speedFlag || ""].filter(Boolean).join(" ");
    }
    return getProductType(activityRows[0] || {}, rateRow);
  };

  const summaryChargeCategory = (summary) => {
    const normalized = String(summary.summaryBillingType || summary.summaryLabel || "").toLowerCase();
    if (normalized.includes("subscription") || normalized.includes("platform")) return "Platform";
    if (normalized.includes("reversal")) return "Reversal";
    if (normalized.includes("volume")) return "Volume";
    if (normalized.includes("txn") || normalized.includes("count")) return "Txn Count";
    if (normalized.includes("minimum")) return "Minimum";
    return "Revenue";
  };

  const summaryLineLabel = (summary, fallback) => String(summary.summaryLabel || summary.summaryBillingType || fallback).trim() || fallback;
  const isSubscriptionSummary = (summary) => String(summary.summaryBillingType || summary.summaryLabel || "").toLowerCase().includes("subscription");
  const isSubscriptionComponentSummary = (summary) => {
    const normalized = String(summary.summaryBillingType || summary.summaryLabel || "").trim().toLowerCase().replace(/\s+/g, "_");
    return normalized === "txn_count" || normalized === "volume";
  };
  const configuredTransactionFeeRows = [
    ...state.off.filter((row) => row.partner === activePartner && inRange(activePeriod + "-15", row.startDate, row.endDate)),
    ...state.vol.filter((row) => row.partner === activePartner && inRange(activePeriod + "-15", row.startDate, row.endDate)),
    ...state.fxRates.filter((row) => row.partner === activePartner && inRange(activePeriod + "-15", row.startDate, row.endDate)),
    ...state.surch.filter((row) => row.partner === activePartner && inRange(activePeriod + "-15", row.startDate, row.endDate))
  ];
  const partnerReversalFees = state.revf.filter((row) => row.partner === activePartner && inRange(activePeriod + "-15", row.startDate, row.endDate));
  const partnerVaFees = state.vaFees.filter((row) => row.partner === activePartner && inRange(activePeriod + "-15", row.startDate, row.endDate));
  const accountSetupRows = state.impl.filter((row) => row.partner === activePartner && row.feeType === "Account Setup" && inRange(activePeriod + "-15", row.startDate || row.goLiveDate, row.endDate));
  const dailySettlementRows = state.impl.filter((row) => row.partner === activePartner && row.feeType === "Daily Settlement" && inRange(activePeriod + "-15", row.startDate || row.goLiveDate, row.endDate));
  const vaData = state.lva.find((row) => row.partner === activePartner && row.period === activePeriod);
  const subscriptionSummaryRows = summaryChargeRows.filter((summary) => isSubscriptionSummary(summary));
  const hasCombinedSubscriptionSummary = !!subscriptionSummaryRows.length && (
    subscriptionSummaryRows.some((summary) => String(summary.summaryComputation || "").includes("+"))
    || summaryChargeRows.some((summary) => isSubscriptionComponentSummary(summary))
  );
  let usedDefaultReversalFee = false;

  if (recurringBillingActive && !authoritativeRecurringChargeSummary) {
    txns.forEach((txn) => {
      const directInvoiceAmount = Number(txn.directInvoiceAmount || 0);
      if (directInvoiceAmount !== 0) {
        const directRate = txn.txnCount > 0 ? Math.abs(directInvoiceAmount) / txn.txnCount : Math.abs(Number(txn.directInvoiceRate || 0));
        const label = [txn.txnType, txn.speedFlag, txn.processingMethod || ""].filter(Boolean).join(" ");
        appendLine({
          cat: "Offline",
          desc: directInvoiceAmount < 0
            ? `${label} reversal adjustment (${txn.txnCount}x${fmt(directRate)} imported)`
            : `${label} (${txn.txnCount}x${fmt(directRate)} imported)`,
          amount: directInvoiceAmount,
          dir: "charge",
          groupLabel: label,
          minimumEligible: true,
          activityRows: [txn]
        });
        return;
      }
      state.off
        .filter((row) => row.partner === activePartner && txnMatchesPricingRow(row, txn) && txn.minAmt >= row.minAmt && txn.maxAmt <= row.maxAmt && inRange(activePeriod + "-15", row.startDate, row.endDate))
        .forEach((row) => {
          const amount = row.fee * txn.txnCount;
          const label = [txn.txnType, txn.speedFlag, txn.processingMethod || ""].filter(Boolean).join(" ");
          appendLine({
            cat: "Offline",
            desc: `${label} (${txn.txnCount}x${fmt(row.fee)})`,
            amount,
            dir: "charge",
            groupLabel: label,
            minimumEligible: true,
            activityRows: [txn]
          });
        });
    });

    const partnerVolumeRates = state.vol.filter((row) => row.partner === activePartner && inRange(activePeriod + "-15", row.startDate, row.endDate));
    if (partnerVolumeRates.length) {
      const volumeGroups = {};
      partnerVolumeRates.forEach((row) => {
      const key = volumeGroupSignature(row);
      if (!volumeGroups[key]) volumeGroups[key] = [];
      volumeGroups[key].push(row);
    });
    Object.values(volumeGroups).forEach((tiers) => {
      const sortedTiers = [...tiers].sort((a, b) => a.minVol - b.minVol);
      const baseRate = sortedTiers[0];
      const matchingTxns = txns.filter((txn) => txnMatchesPricingRow(baseRate, txn));
      if (!matchingTxns.length) return;
      const combinedVolume = matchingTxns.reduce((sum, txn) => sum + Number(txn.totalVolume || 0), 0);
      const combinedTxnCount = matchingTxns.reduce((sum, txn) => sum + Number(txn.txnCount || 0), 0);
      const label = buildRateGroupLabel(matchingTxns, baseRate);
      if (isIncremental && sortedTiers.length > 1) {
        let remaining = combinedVolume;
        let totalFee = 0;
        const parts = [];
        for (const tier of sortedTiers) {
          if (remaining <= 0) break;
          const bandSize = tier.maxVol - tier.minVol + 1;
          const volumeInBand = Math.min(remaining, bandSize);
          totalFee += tier.rate * volumeInBand;
          parts.push(`${fmtPct(tier.rate)}x${fmt(volumeInBand)}`);
          remaining -= volumeInBand;
        }
        if (totalFee > 0) {
          const productType = getProductType(matchingTxns[0], baseRate);
          const feePerTxn = combinedTxnCount > 0 ? totalFee / combinedTxnCount : 0;
          const adjusted = applyFeeCaps(activePartner, productType, feePerTxn, combinedTxnCount);
          const amount = adjusted.capped ? adjusted.total : totalFee;
          appendLine({
            cat: "Volume",
            desc: `${label} incremental [${parts.join(" + ")}]${adjusted.capNote}`,
            amount,
            dir: "charge",
            groupLabel: label,
            minimumEligible: true,
            activityRows: matchingTxns
          });
        }
      } else {
        const tier = sortedTiers.find((row) => combinedVolume >= row.minVol && combinedVolume <= row.maxVol);
        if (!tier) return;
        const productType = getProductType(matchingTxns[0], tier);
        const capNotes = new Set();
        const amount = matchingTxns.reduce((sum, txn) => {
          const rawAmount = tier.rate * Number(txn.totalVolume || 0);
          const feePerTxn = Number(txn.txnCount || 0) > 0 ? rawAmount / Number(txn.txnCount || 0) : 0;
          const adjusted = applyFeeCaps(activePartner, productType, feePerTxn, Number(txn.txnCount || 0));
          if (adjusted.capNote) capNotes.add(adjusted.capNote);
          return sum + (adjusted.capped ? adjusted.total : rawAmount);
        }, 0);
        const capNote = [...capNotes].join("");
        appendLine({
          cat: "Volume",
          desc: `${label} ${tier.note || ""} (${fmtPct(tier.rate)}x${fmt(combinedVolume)}${capNote})`.trim(),
          amount,
          dir: "charge",
          groupLabel: label,
          minimumEligible: true,
          activityRows: matchingTxns
        });
      }
    });
    }

    const partnerSurcharges = state.surch.filter((row) => row.partner === activePartner && inRange(activePeriod + "-15", row.startDate, row.endDate));
    if (partnerSurcharges.length) {
      const groups = {};
      partnerSurcharges.forEach((row) => {
      if (!groups[row.surchargeType]) groups[row.surchargeType] = [];
      groups[row.surchargeType].push(row);
    });
    Object.entries(groups).forEach(([type, tiers]) => {
      const sortedTiers = [...tiers].sort((a, b) => a.minVol - b.minVol);
      const matchingTxns = txns.filter((txn) => Number(txn.totalVolume || 0) > 0);
      if (!matchingTxns.length) return;
      const combinedVolume = matchingTxns.reduce((sum, txn) => sum + Number(txn.totalVolume || 0), 0);
      if (isIncremental && sortedTiers.length > 1) {
        let remaining = combinedVolume;
        let totalFee = 0;
        const parts = [];
        for (const tier of sortedTiers) {
          if (remaining <= 0) break;
          const bandSize = tier.maxVol - tier.minVol + 1;
          const volumeInBand = Math.min(remaining, bandSize);
          totalFee += tier.rate * volumeInBand;
          parts.push(`${fmtPct(tier.rate)}x${fmt(volumeInBand)}`);
          remaining -= volumeInBand;
        }
        if (totalFee > 0) {
          appendLine({
            cat: "Surcharge",
            desc: `${type} incremental [${parts.join(" + ")}]`,
            amount: totalFee,
            dir: "charge",
            groupLabel: type,
            minimumEligible: true,
            activityRows: matchingTxns
          });
        }
      } else {
        const tier = sortedTiers.find((row) => combinedVolume >= row.minVol && combinedVolume <= row.maxVol);
        if (!tier) return;
        appendLine({
          cat: "Surcharge",
          desc: `${tier.surchargeType} ${tier.note || ""} (${fmtPct(tier.rate)}x${fmt(combinedVolume)})`.trim(),
          amount: tier.rate * combinedVolume,
          dir: "charge",
          groupLabel: tier.surchargeType,
          minimumEligible: true,
          activityRows: matchingTxns
        });
      }
    });
    }

    if (authoritativePayoutSummary) {
      summaryPayRows.forEach((summary) => {
        if (String(summary.revenueSource || "") === "billing_summary") {
          const label = summaryLineLabel(summary, "Partner payout");
          appendLine({
            cat: "Rev Share",
            desc: summary.summaryComputation || label,
            amount: summary.partnerRevenueShare,
            dir: "pay",
            groupLabel: label
          });
        } else {
          appendLine({
            cat: "Rev Share",
            desc: `Partner rev-share payout from revenue report (net revenue ${fmt(summary.netRevenue)})`,
            amount: summary.partnerRevenueShare,
            dir: "pay",
            groupLabel: "Partner rev-share payout"
          });
        }
      });
    }
    summaryChargeRows.forEach((summary) => {
      if (hasCombinedSubscriptionSummary && isSubscriptionComponentSummary(summary)) return;
      if (String(summary.revenueSource || "") === "billing_summary") {
        const label = summaryLineLabel(summary, "Partner-generated revenue");
        const summaryCategory = summaryChargeCategory(summary);
        appendLine({
          cat: summaryCategory,
          desc: summary.summaryComputation || label,
          amount: summary.revenueOwed,
          dir: "charge",
          groupLabel: label,
          minimumEligible: true,
          implementationCreditEligible: summaryCategory === "Minimum"
            ? "monthly_minimum"
            : summaryCategory === "Platform"
              ? "monthly_subscription"
              : ""
        });
      } else {
        const minNote = summary.monthlyMinimumRevenue > 0 ? `, minimum ${fmt(summary.monthlyMinimumRevenue)}` : "";
        appendLine({
          cat: "Revenue",
          desc: `Partner-generated revenue from revenue report (${fmt(summary.revenueOwed)} owed${minNote})`,
          amount: summary.revenueOwed,
          dir: "charge",
          groupLabel: "Partner-generated revenue",
          minimumEligible: true
        });
      }
    });
    const generatedRevenueByActivity = {};
    lines.forEach((line) => {
      if (line.dir !== "charge" || line.active === false) return;
      if (!["Offline", "Volume", "FX", "Surcharge", "Revenue", "Txn Count"].includes(line.cat)) return;
      const activityRows = line.activityRows || [];
      const amount = Number(line.amount || 0);
      if (!amount || !activityRows.length) return;
      let weights = [];
      if (["Volume", "FX", "Surcharge"].includes(line.cat)) {
        weights = activityRows.map((row) => Number(row.totalVolume || 0));
      } else if (["Offline", "Txn Count"].includes(line.cat)) {
        weights = activityRows.map((row) => Number(row.txnCount || 0));
      }
      const totalWeight = weights.reduce((sum, value) => sum + (value > 0 ? value : 0), 0);
      activityRows.forEach((row, index) => {
        const key = activityRowKey(row);
        const allocated = totalWeight > 0 && Number(weights[index] || 0) > 0
          ? roundCurrency(amount * (weights[index] / totalWeight))
          : roundCurrency(amount / activityRows.length);
        generatedRevenueByActivity[key] = roundCurrency(Number(generatedRevenueByActivity[key] || 0) + allocated);
      });
    });
    txns.forEach((txn) => {
      txn.generatedRevenueSupport = roundCurrency(Number(generatedRevenueByActivity[activityRowKey(txn)] || 0));
    });
    if (!authoritativePayoutSummary && revShareRows.length) {
      const revShareLines = [];
      let revShareMatchCount = 0;
      let revShareRevenueCount = 0;
      revShareRows.forEach((share) => {
        txns.filter((txn) => revShareScopeMatches(share, txn)).forEach((txn) => {
          revShareMatchCount += 1;
          const { totalCost } = calculateRevShareTotalCost(state.pCosts, txn, activePeriod);
          const estRevenue = Number(txn.estRevenue || 0);
          const importedRevenue = Number(txn.customerRevenue || 0);
          const generatedRevenue = Number(generatedRevenueByActivity[activityRowKey(txn)] || 0);
          let sourceRevenue = 0;
          let revenueSourceLabel = "contract-generated revenue";
          if (estRevenue > 0) {
            sourceRevenue = estRevenue;
            revenueSourceLabel = "est revenue";
          } else if (importedRevenue > 0) {
            sourceRevenue = importedRevenue;
            revenueSourceLabel = "imported revenue";
          } else {
            sourceRevenue = generatedRevenue;
          }
          const revenueBase = Math.max(sourceRevenue - totalCost, 0);
          if (sourceRevenue > 0) revShareRevenueCount += 1;
          const payback = share.revSharePct * revenueBase;
          if (payback > 0) {
            const scope = [share.txnType || txn.txnType || "Payout", share.speedFlag || txn.speedFlag || ""].filter(Boolean).join(" ");
            const desc = `${scope}: ${fmtPct(share.revSharePct)}x(${fmt(sourceRevenue)} ${revenueSourceLabel}-${fmt(totalCost)} cost)`;
            revShareLines.push({ id: uid(), cat: "Rev Share", desc, amount: payback, dir: "pay", groupLabel: scope || "Partner rev-share payout", groupKey: `Rev Share|pay|${scope || "Partner rev-share payout"}`, activityRows: [txn] });
          }
        });
      });
      if (!revShareLines.length) {
        const scopes = [...new Set(revShareRows.map((share) => [share.txnType || "Payout", share.speedFlag || ""].filter(Boolean).join(" ")).filter(Boolean))];
        const scopeLabel = scopes.length ? scopes.join(", ") : "configured rev-share";
        if (!revShareMatchCount) {
          notes.push(`Revenue share is configured for ${scopeLabel}, but no matching payout transactions were imported for ${activePartner} in ${activePeriod}.`);
        } else if (!revShareRevenueCount) {
          notes.push(`Revenue share is configured for ${scopeLabel}, but neither Est Revenue, the imported revenue fields, nor the contract-derived transaction charges produced a revenue base for the partner payout.`);
        }
      }
      revShareLines.forEach((line) => lines.push(line));
    }

    if (fxPartnerPayoutRows.length) {
      fxPartnerPayoutRows.forEach((row) => {
      const shareActivitySummaryRow = {
        partner: activePartner,
        period: activePeriod,
        txnType: "FX",
        speedFlag: "Standard",
        processingMethod: "Wire",
        payerFunding: "Bank",
        payeeFunding: "Bank",
        payerCcy: "USD",
        payeeCcy: "",
        payerCountry: "",
        payeeCountry: "",
        txnCount: row.shareTxnCount || row.txnCount,
        totalVolume: row.shareTotalMidMarketUsd || row.shareTotalUsdDebited || row.totalMidMarketUsd,
      };
      const reversalActivitySummaryRow = {
        partner: activePartner,
        period: activePeriod,
        txnType: "FX Reversal",
        speedFlag: "Standard",
        processingMethod: "Wire",
        payerFunding: "Bank",
        payeeFunding: "Bank",
        payerCcy: "USD",
        payeeCcy: "",
        payerCountry: "",
        payeeCountry: "",
        txnCount: row.reversalTxnCount || 0,
        totalVolume: row.reversalTotalMidMarketUsd || row.reversalTotalUsdDebited || 0,
      };
      let shareAmount = Number(row.shareAmount) || 0;
      let reversalAmount = Number(row.reversalAmount) || 0;
      const netPayout = Number(row.partnerPayout) || 0;
      if (shareAmount <= 0 && netPayout > 0) shareAmount = netPayout;
      if (reversalAmount <= 0 && netPayout < 0) reversalAmount = Math.abs(netPayout);
      if (shareAmount > 0) {
        appendLine({
          cat: "Rev Share",
          desc: `FX partner markup payout (${row.shareTxnCount || row.txnCount} payout txns, markup ${fmt(shareAmount)}${reversalAmount > 0 && netPayout > 0 ? `, net after reversals ${fmt(netPayout)}` : ""})`,
          amount: shareAmount,
          dir: "pay",
          groupLabel: "FX partner markup payout",
          activityRows: [shareActivitySummaryRow]
        });
      }
      if (reversalAmount > 0) {
        appendLine({
          cat: "Rev Share",
          desc: `FX partner markup reversal adjustment (${row.reversalTxnCount || 0} reversal txns, reversed ${fmt(reversalAmount)}${shareAmount > 0 ? `, net balance ${fmt(netPayout)}` : ""})`,
          amount: -reversalAmount,
          dir: "pay",
          groupLabel: "FX partner markup reversal",
          activityRows: [reversalActivitySummaryRow]
        });
      }
      if (row.note) {
        notes.push(`Stampli FX payout: ${row.note}`);
      }
    });
    } else if (activePartner === "Stampli") {
      if (!fxMarkupActivityRows.length) {
        notes.push(`No Stampli FX transactions were imported for ${activePeriod}. The supplied data only contains Domestic and USD Abroad rows, so the FX partner-markup payout remains $0.00.`);
      } else {
        notes.push(`Stampli FX transactions were imported for ${activePeriod}, but no FX partner-markup payout summary was derived from the raw payment detail.`);
      }
    }

    const partnerFxRates = state.fxRates.filter((row) => row.partner === activePartner && inRange(activePeriod + "-15", row.startDate, row.endDate));
    if (!authoritativeRecurringChargeSummary && partnerFxRates.length) {
      txns.filter((txn) => txn.payerCcy !== txn.payeeCcy).forEach((txn) => {
      const avgSize = txn.avgTxnSize || (txn.txnCount > 0 ? txn.totalVolume / txn.txnCount : 0);
      const payeeCorridor = getCorridor(txn.payeeCcy);
      const payerCorridor = getCorridor(txn.payerCcy);
      const matches = partnerFxRates.filter((row) => {
        const payeeOk = row.payeeCcy ? row.payeeCcy === txn.payeeCcy : (!row.payeeCorridor || row.payeeCorridor === payeeCorridor);
        const payerOk = !row.payerCcy && !row.payerCorridor ? true : (row.payerCcy ? row.payerCcy === txn.payerCcy : row.payerCorridor === payerCorridor);
        const sizeOk = avgSize >= row.minTxnSize && avgSize <= row.maxTxnSize;
        return payeeOk && payerOk && sizeOk;
      });
      if (!matches.length) return;
      const specific = matches.filter((row) => row.payeeCcy === txn.payeeCcy);
      const pool = specific.length ? specific : matches;
      const tiers = [...pool].sort((a, b) => a.minVol - b.minVol);
      if (isIncremental && tiers.length > 1 && tiers.some((row) => row.minVol !== tiers[0].minVol)) {
        let remaining = txn.totalVolume;
        let totalFee = 0;
        const parts = [];
        for (const tier of tiers) {
          if (remaining <= 0) break;
          const bandSize = tier.maxVol - tier.minVol + 1;
          const volumeInBand = Math.min(remaining, bandSize);
          totalFee += tier.rate * volumeInBand;
          parts.push(`${fmtPct(tier.rate)}x${fmt(volumeInBand)}`);
          remaining -= volumeInBand;
        }
        if (totalFee > 0) {
          const label = `${txn.payerCcy}→${txn.payeeCcy}`;
          appendLine({
            cat: "FX",
            desc: `${label} incremental [${parts.join(" + ")}]`,
            amount: totalFee,
            dir: "charge",
            groupLabel: label,
            minimumEligible: true,
            activityRows: [txn]
          });
        }
      } else {
        const best = pool.find((row) => txn.totalVolume >= row.minVol && txn.totalVolume <= row.maxVol) || pool[0];
        if (best) {
          const amount = best.rate * txn.totalVolume;
          const label = `${txn.payerCcy}→${txn.payeeCcy}`;
          appendLine({
            cat: "FX",
            desc: `${label} @ ${(best.rate * 100).toFixed(4)}% (avg txn ${fmt(avgSize)}) x ${fmt(txn.totalVolume)}`,
            amount,
            dir: "charge",
            groupLabel: label,
            minimumEligible: true,
            activityRows: [txn]
          });
        }
      }
    });
    }

    if (!authoritativeRecurringChargeSummary) {
      revs.forEach((row) => {
        const match = partnerReversalFees.find((fee) => (!fee.payerFunding || fee.payerFunding === row.payerFunding));
        const hasConfiguredReversalFee = !!match && match.feePerReversal !== "" && match.feePerReversal != null && Number.isFinite(Number(match.feePerReversal));
        const feePerReversal = hasConfiguredReversalFee ? Number(match.feePerReversal) : DEFAULT_REVERSAL_FEE_PER_TXN;
        if (!hasConfiguredReversalFee) usedDefaultReversalFee = true;
        const amount = feePerReversal * row.reversalCount;
        appendLine({
          cat: "Reversal",
          desc: `${row.payerFunding || "All"} ${row.reversalCount}x${fmt(feePerReversal)}${hasConfiguredReversalFee ? "" : " default"}`,
          amount,
          dir: "charge",
          groupLabel: `${row.payerFunding || "All"} reversals`,
          minimumEligible: true,
          activityRows: [row]
        });
      });
    }

    const platformFee = state.plat.find((row) => row.partner === activePartner && inRange(activePeriod + "-15", row.startDate, row.endDate));
    if (platformFee) {
      if (!hasCombinedSubscriptionSummary) {
        appendLine({
          cat: "Platform",
          desc: "Monthly subscription",
          amount: platformFee.monthlyFee,
          dir: "charge",
          groupLabel: "Monthly platform fee",
          implementationCreditEligible: "monthly_subscription"
        });
      }
    }

    if (!authoritativeRecurringChargeSummary && vaData) {
      if (vaData.newAccountsOpened > 0) {
      const tier = partnerVaFees.filter((row) => row.feeType === "Account Opening").find((row) => vaData.newAccountsOpened >= row.minAccounts && vaData.newAccountsOpened <= row.maxAccounts);
      if (tier) {
        const amount = tier.feePerAccount * vaData.newAccountsOpened;
        appendLine({ cat: "Virtual Acct", desc: `Account Opening: ${vaData.newAccountsOpened} accts x ${fmt(tier.feePerAccount)}`, amount, dir: "charge", groupLabel: "Account Opening", minimumEligible: true });
      }
    }
      if (vaData.totalActiveAccounts > 0) {
        const tier = partnerVaFees.filter((row) => row.feeType === "Monthly Active").find((row) => vaData.totalActiveAccounts >= row.minAccounts && vaData.totalActiveAccounts <= row.maxAccounts);
        if (tier) {
          const amount = tier.feePerAccount * vaData.totalActiveAccounts;
          appendLine({ cat: "Virtual Acct", desc: `Monthly Active: ${vaData.totalActiveAccounts} accts x ${fmt(tier.feePerAccount)}/mo`, amount, dir: "charge", groupLabel: "Monthly Active", minimumEligible: true });
        }
      }
      if (vaData.dormantAccounts > 0) {
        const tier = partnerVaFees.filter((row) => row.feeType === "Dormancy").find((row) => vaData.dormantAccounts >= row.minAccounts && vaData.dormantAccounts <= row.maxAccounts);
        if (tier) {
          const amount = tier.feePerAccount * vaData.dormantAccounts;
          appendLine({ cat: "Virtual Acct", desc: `Dormancy: ${vaData.dormantAccounts} accts x ${fmt(tier.feePerAccount)}/mo`, amount, dir: "charge", groupLabel: "Dormancy", minimumEligible: true });
        }
      }
    if (vaData.closedAccounts > 0) {
      const tier = partnerVaFees.filter((row) => row.feeType === "Account Closing").find((row) => vaData.closedAccounts >= row.minAccounts && vaData.closedAccounts <= row.maxAccounts);
      if (tier) {
        const amount = tier.feePerAccount * vaData.closedAccounts;
        appendLine({ cat: "Virtual Acct", desc: `Account Closing: ${vaData.closedAccounts} accts x ${fmt(tier.feePerAccount)}`, amount, dir: "charge", groupLabel: "Account Closing", minimumEligible: true });
      }
    }
    const annualBusinessSetup = accountSetupRows.find((row) => norm(row.note).includes("year-end active") && norm(row.note).includes("per business"));
    const annualIndividualSetup = accountSetupRows.find((row) => norm(row.note).includes("year-end active") && norm(row.note).includes("per individual"));
    const standardSetupFee = accountSetupRows.find((row) => !norm(row.note).includes("year-end active"));
    if (isCalendarYearEndPeriod(activePeriod) && annualBusinessSetup && Number(vaData.totalBusinessAccounts || 0) > 0) {
      const amount = annualBusinessSetup.feeAmount * Number(vaData.totalBusinessAccounts || 0);
      appendLine({ cat: "Account Setup", desc: `Year-end business accounts: ${vaData.totalBusinessAccounts} x ${fmt(annualBusinessSetup.feeAmount)}`, amount, dir: "charge", groupLabel: "Year-end business account setup", minimumEligible: true });
    }
    if (isCalendarYearEndPeriod(activePeriod) && annualIndividualSetup && Number(vaData.totalIndividualAccounts || 0) > 0) {
      const amount = annualIndividualSetup.feeAmount * Number(vaData.totalIndividualAccounts || 0);
      appendLine({ cat: "Account Setup", desc: `Year-end individual accounts: ${vaData.totalIndividualAccounts} x ${fmt(annualIndividualSetup.feeAmount)}`, amount, dir: "charge", groupLabel: "Year-end individual account setup", minimumEligible: true });
    }
    if (vaData.newBusinessSetups > 0 && standardSetupFee) {
      const amount = standardSetupFee.feeAmount * vaData.newBusinessSetups;
      appendLine({ cat: "Account Setup", desc: `${vaData.newBusinessSetups} biz x ${fmt(standardSetupFee.feeAmount)}`, amount, dir: "charge", groupLabel: "Account Setup", minimumEligible: true });
    }
    if (vaData.settlementCount > 0) {
      const settlementFee = dailySettlementRows[0];
      if (settlementFee) {
        const amount = settlementFee.feeAmount * vaData.settlementCount;
        appendLine({ cat: "Settlement", desc: `${vaData.settlementCount} sweeps x ${fmt(settlementFee.feeAmount)}`, amount, dir: "charge", groupLabel: "Daily Settlement", minimumEligible: true });
      }
    }
    }
  }

  const summarizeImplementationCreditBase = () => lines.reduce((acc, line) => {
    if (line.dir !== "charge" || line.active === false) return acc;
    const mode = String(line.implementationCreditEligible || "");
    if (!mode) return acc;
    acc[mode] = roundCurrency((acc[mode] || 0) + Number(line.amount || 0));
    return acc;
  }, {});

  const applyImplementationCredits = () => {
    if (skipImplementationCredits) return;
    const implementationRows = state.impl.filter((row) => row.partner === activePartner && row.feeType === "Implementation");
    if (!implementationRows.length) return;
    const currentBaseByMode = summarizeImplementationCreditBase();
    implementationRows.forEach((row) => {
      const mode = normalizeImplementationCreditMode(row);
      const creditAmount = getImplementationCreditAmount(row);
      const startPeriod = getImplementationCreditStartPeriod(activePartner, row);
      if (!mode || !(creditAmount > 0) || !startPeriod || comparePeriods(activePeriod, startPeriod) < 0) return;
      const targetBase = Number(currentBaseByMode[mode] || 0);
      if (!(targetBase > 0)) return;
      const priorPeriods = enumeratePeriods(startPeriod, activePeriod).slice(0, -1);
      let previouslyApplied = 0;
      for (const priorPeriod of priorPeriods) {
        if (previouslyApplied >= creditAmount) break;
        const priorInvoice = calculateLocalInvoiceForPeriod(activePartner, priorPeriod, { skipImplementationCredits: true, suppressNotes: true });
        const priorBase = Number(priorInvoice.implementationCreditBaseByMode?.[mode] || 0);
        if (!(priorBase > 0)) continue;
        previouslyApplied = roundCurrency(previouslyApplied + Math.min(priorBase, creditAmount - previouslyApplied));
      }
      const remainingCredit = roundCurrency(creditAmount - previouslyApplied);
      if (!(remainingCredit > 0)) return;
      const appliedCredit = roundCurrency(Math.min(remainingCredit, targetBase));
      if (!(appliedCredit > 0)) return;
      appendLine({
        cat: "Impl Credit",
        desc: `Implementation fee credit vs ${getImplementationCreditLabel(mode)}`,
        amount: appliedCredit,
        dir: "offset",
        groupLabel: "Implementation credit"
      });
    });
  };

  const preCollectedRevenueUsed = applyPreCollectedRevenueOffsets();

  if (recurringBillingActive && !authoritativeRecurringChargeSummary) {
    if (configuredTransactionFeeRows.length && !txns.length) {
      notes.push("Transaction-priced fees are configured for this partner, but no transaction upload was imported for this period. Offline, volume, FX, and surcharge charges may be missing.");
    }
    if (partnerReversalFees.length && !revs.length) {
      notes.push("Reversal fees are configured for this partner, but no reversal upload was imported for this period. Reversal charges may be missing.");
    }
    if ((partnerVaFees.length || accountSetupRows.length || dailySettlementRows.length) && !vaData) {
      notes.push("Virtual-account, account-setup, or settlement fees are configured for this partner, but no account-usage upload was imported for this period. Those charges may be missing.");
    }
  }
  if (usedDefaultReversalFee) {
    notes.push(`Default reversal fee applied at ${fmt(DEFAULT_REVERSAL_FEE_PER_TXN)} per reversal where no partner-specific reversal fee was defined in the contract.`);
  }
  if (preCollectedRevenueUsed > 0 || preCollectedRevenueTotal > 0) {
    notes.push(`Pre-collected revenue from transaction-time charges: ${fmt(preCollectedRevenueTotal)}. ${fmt(preCollectedRevenueUsed)} was excluded from this invoice to avoid double charging and still counts toward monthly minimum calculations.`);
  }

  applyMonthlyMinimumRule();

  if (!recurringBillingActive) {
    if (isPartnerNotYetLive(activePartner)) {
      const goLiveDate = getPartnerGoLiveDate(activePartner);
      notes.push(goLiveDate
        ? `Partner is marked not yet live. Only implementation bills until go-live is confirmed. Target go-live date: ${formatIsoDate(goLiveDate)}.`
        : "Partner is marked not yet live. Only implementation bills during integration until a go-live date is set.");
    } else if (getPartnerGoLiveDate(activePartner)) {
      notes.push(`Recurring monthly billing begins at go-live date ${formatIsoDate(getPartnerGoLiveDate(activePartner))}.`);
    }
  }

  const implFee = state.impl.find((row) => row.partner === activePartner && row.feeType === "Implementation" && normalizeMonthKey(getImplementationBillingDate(activePartner, row)) === activePeriod);
  if (implFee) {
    appendLine({ cat: "Impl Fee", desc: "Implementation fee", amount: implFee.feeAmount, dir: "charge", groupLabel: "Implementation fee" });
  }

  applyImplementationCredits();

  const implementationCreditBaseByMode = summarizeImplementationCreditBase();

  const totals = calculateActiveInvoiceTotals(lines);
  const chg = totals.chg;
  const pay = totals.pay;

  const groups = groupInvoiceLines(lines);
  const net = chg - pay;
  return {
    partner: activePartner,
    period: activePeriod,
    periodStart: activePeriod,
    periodEnd: activePeriod,
    periodLabel: formatPeriodRangeLabel(activePeriod, activePeriod),
    periodDateRange: formatPeriodDateRange(activePeriod, activePeriod),
    lines,
    groups,
    notes: suppressNotes ? [] : notes,
    implementationCreditBaseByMode,
    chg,
    pay,
    net,
    dir: net >= 0 ? "Partner Owes Us" : "We Owe Partner"
  };
}

function combineInvoicesForRange(partner, startPeriod, endPeriod, monthlyInvoices) {
  const normalized = normalizePeriodRange(startPeriod, endPeriod);
  const lines = [];
  const notes = [];
  monthlyInvoices.forEach((invoice) => {
    const sourcePeriod = invoice.periodStart || invoice.period;
    (invoice.lines || []).forEach((line) => {
      lines.push({
        ...line,
        id: uid(),
        sourcePeriod,
        activityRows: (line.activityRows || []).map((row) => ({ ...row, period: row.period || sourcePeriod }))
      });
    });
    (invoice.notes || []).forEach((note) => {
      if (invoice.inactivePeriod && normalized.start !== normalized.end) return;
      if (normalized.start === normalized.end) notes.push(note);
      else notes.push(`${formatPeriodLabel(sourcePeriod)}: ${note}`);
    });
  });
  const groups = groupInvoiceLines(lines);
  const totals = calculateActiveInvoiceTotals(lines);
  const net = totals.chg - totals.pay;
  return {
    partner,
    period: buildInvoicePeriodKey(normalized.start, normalized.end),
    periodStart: normalized.start,
    periodEnd: normalized.end,
    periodLabel: formatPeriodRangeLabel(normalized.start, normalized.end),
    periodDateRange: formatPeriodDateRange(normalized.start, normalized.end),
    lines,
    groups,
    notes,
    chg: totals.chg,
    pay: totals.pay,
    net,
    dir: net >= 0 ? "Partner Owes Us" : "We Owe Partner"
  };
}

async function calculateInvoice() {
  state.sp = readBoundValue("sp");
  state.perStart = readBoundValue("perStart");
  state.perEnd = state.useDateRange ? readBoundValue("perEnd") : state.perStart;
  const { start, end } = normalizePeriodRange(state.perStart, state.perEnd);
  state.perStart = start;
  state.perEnd = end;
  const resolvedPartner = resolvePartnerName(state.sp);
  if (!resolvedPartner) {
    showToast("Partner required", "Choose a valid partner before calculating the invoice.", "warning");
    return;
  }
  state.sp = resolvedPartner;
  if (!state.perStart || !state.perEnd) {
    showToast("Date required", "Choose a valid invoice month or date range before calculating.", "warning");
    return;
  }

  try {
    if (isSharedWorkbookEnabled()) {
      try {
        await refreshSharedWorkspace({ showSuccessToast: false, showErrorToast: false, retries: 2, retryDelayMs: 250 });
      } catch (error) {
        console.error("Could not refresh shared workbook before invoice calculation", error);
      }
    }
    if (isRemoteInvoiceReadEnabled()) {
      try {
        const payload = await fetchSharedDraftInvoice(state.sp, state.perStart, state.perEnd);
        if (payload?.invoice) {
          applyInvoiceResult(payload.invoice);
          try {
            await archiveInvoiceArtifactCopy(payload.invoice, {
              trigger: "generate_invoice",
              showSuccessToast: false,
              showUnavailableToast: false,
              showErrorToast: true
            });
          } catch (archiveError) {
            console.error("Invoice generated but artifact archive failed", archiveError);
          }
          return;
        }
      } catch (error) {
        console.error("Could not load remote invoice", error);
        showToast("Remote invoice unavailable", "Using the local calculator for this draft.", "warning");
      }
    }

    state.invoiceExplorer = null;
    const periods = enumeratePeriods(state.perStart, state.perEnd);
    const monthlyInvoices = periods.map((period) => calculateLocalInvoiceForPeriod(state.sp, period));
    const combinedInvoice = combineInvoicesForRange(state.sp, state.perStart, state.perEnd, monthlyInvoices);
    applyInvoiceResult(combinedInvoice);
    try {
      await archiveInvoiceArtifactCopy(combinedInvoice, {
        trigger: "generate_invoice",
        showSuccessToast: false,
        showUnavailableToast: false,
        showErrorToast: true
      });
    } catch (archiveError) {
      console.error("Invoice generated but artifact archive failed", archiveError);
    }
  } catch (error) {
    console.error("Could not calculate invoice", error);
    showToast("Invoice calculation failed", String(error?.message || error || "Unknown error"), "error");
  }
}

function buildCheckerSelectionPayload() {
  state.sp = readBoundValue("sp");
  state.perStart = readBoundValue("perStart");
  state.perEnd = state.useDateRange ? readBoundValue("perEnd") : state.perStart;
  if (state.useDateRange && !state.perEnd) {
    return { error: "Choose an end month before running the checker on a date range." };
  }
  const { start, end } = normalizePeriodRange(state.perStart, state.perEnd);
  state.perStart = start;
  state.perEnd = end;
  const resolvedPartner = resolvePartnerName(state.sp);
  if (!resolvedPartner) {
    return { error: "Choose a valid partner before running the checker." };
  }
  state.sp = resolvedPartner;
  if (!state.perStart || !state.perEnd) {
    return { error: "Choose a valid invoice month or date range before running the checker." };
  }
  return {
    partner: state.sp,
    startPeriod: state.perStart,
    endPeriod: state.perEnd
  };
}

function renderCheckerDiffTable(run) {
  const diffs = Array.isArray(run?.diffs) ? run.diffs : [];
  return `
    <div class="table-wrap">
      <table class="data-table">
        <thead>
          <tr>
            <th>Bucket</th>
            <th>Maker</th>
            <th>Checker</th>
            <th>Delta</th>
          </tr>
        </thead>
        <tbody>
          ${!diffs.length
            ? `<tr><td colspan="4" class="empty-state">No bucket mismatches were found for this run.</td></tr>`
            : diffs.map((diff) => `
              <tr>
                <td>${html(diff.bucket || "")}</td>
                <td class="align-right mono">${fmt(diff.maker)}</td>
                <td class="align-right mono">${fmt(diff.checker)}</td>
                <td class="align-right mono">${fmt(diff.delta)}</td>
              </tr>
            `).join("")}
        </tbody>
      </table>
    </div>
  `;
}

function renderCheckerRunCards(report) {
  const runs = Array.isArray(report?.runs) ? report.runs : [];
  if (!runs.length) {
    return `<div class="summary-banner"><h4>No runs were returned</h4><p>Run the checker again to generate a fresh comparison.</p></div>`;
  }
  return runs.map((run) => {
    const passed = !!run.passed;
    const totalDeltas = run.totalDeltas || {};
    const sourceStats = run.sourceStats || {};
    return `
      <div class="summary-banner ${passed ? "success" : "danger"}" style="margin-top:14px">
        <h4>${html(run.partner || "Unknown partner")} · ${html(run.period || "Unknown period")} ${passed ? "passed" : "needs review"}</h4>
        <p>Maker net ${fmt(run?.maker?.net)} vs checker net ${fmt(run?.checker?.net)}. Total delta ${fmt(totalDeltas.net || 0)}.</p>
        <p>Source rows: ltxn ${Number(sourceStats.ltxn_rows || 0)}, lrev ${Number(sourceStats.lrev_rows || 0)}, lrs ${Number(sourceStats.lrs_rows || 0)}, lfxp ${Number(sourceStats.lfxp_rows || 0)}, lva ${Number(sourceStats.lva_rows || 0)}, impl ${Number(sourceStats.impl_rows || 0)}.</p>
      </div>
      ${passed ? "" : renderCheckerDiffTable(run)}
      ${Array.isArray(run.notes) && run.notes.length ? `
        <div class="invoice-note-card" style="margin-top:12px">
          <strong>Notes</strong>
          <ul class="bulleted-list" style="margin-top:8px">
            ${run.notes.map((note) => `<li>${html(note)}</li>`).join("")}
          </ul>
        </div>
      ` : ""}
    `;
  }).join("");
}

function renderBillingCheckerPanel() {
  const report = state.checkerReport;
  const checkerEnabled = isBillingCheckerEnabled();
  const selectionPartner = resolvePartnerName(state.sp);
  const selectionEnd = state.useDateRange ? state.perEnd : state.perStart;
  const selectionReady = checkerEnabled && !!selectionPartner && !!state.perStart && !!selectionEnd && state.checkerStatus !== "running";
  const selectionLabel = selectionPartner
    ? `${selectionPartner} · ${formatPeriodDateRange(state.perStart, selectionEnd)}`
    : "Choose a partner and month to run the checker.";
  const badge = report
    ? `${report.passedCount || 0} pass${report.passedCount === 1 ? "" : "es"} · ${report.failedCount || 0} fail${report.failedCount === 1 ? "" : "s"}`
    : "Not run yet";
  const headerNote = checkerEnabled
    ? "Independent pre-bill reconciliation against contract terms and transaction data."
    : "Checker webhook not configured yet for this hosted frontend.";
  const reportPartnerLabel = report?.partnerFilter ? report.partnerFilter : "All partners";
  const reportPeriodLabel = report?.periodFilter?.periods?.length
    ? report.periodFilter.periods.join(", ")
    : report?.periodFilter?.startPeriod && report?.periodFilter?.endPeriod
      ? formatPeriodDateRange(report.periodFilter.startPeriod, report.periodFilter.endPeriod)
      : "All periods";
  const content = `
    <div class="button-row" style="margin-bottom:14px">
      <button class="button ghost" data-action="clear-billing-checker"${report ? "" : " disabled"}>Clear Result</button>
      <span class="helper-pill">${html(selectionLabel)}</span>
      ${state.checkerStatus === "running" ? `<span class="helper-pill">Checking...</span>` : ""}
      ${state.checkerError ? `<span class="helper-pill" style="background:#f7edc8;color:#7c5312">${html(state.checkerError)}</span>` : ""}
    </div>
    ${report ? `
      <div class="grid-4">
        <div class="kpi-card"><strong>${report.runCount || 0}</strong><span>Runs</span></div>
        <div class="kpi-card"><strong>${report.passedCount || 0}</strong><span>Passed</span></div>
        <div class="kpi-card"><strong>${report.failedCount || 0}</strong><span>Failed</span></div>
        <div class="kpi-card"><strong>${report.generatedAt ? formatDateTime(report.generatedAt) : "—"}</strong><span>Last Run</span></div>
      </div>
      <div class="summary-banner ${report.failedCount ? "danger" : "success"}" style="margin-top:14px">
        <h4>${report.failedCount ? "Checker found exceptions" : "Checker passed"}</h4>
        <p>${report.failedCount ? "Review the bucket mismatches below before billing is approved." : "The maker and checker totals matched for this selection."}</p>
        <p>Report scope: ${html(reportPartnerLabel)} · ${html(reportPeriodLabel)}</p>
      </div>
      ${renderCheckerRunCards(report)}
    ` : `
      <div class="summary-banner">
        <h4>Run the checker before approving billing</h4>
        <p>${checkerEnabled
          ? "This compares the current app output to an independent recalculation from the same contract terms and imported transaction data."
          : "Connect BILLING_APP_CONFIG.checkerWebhookUrl to run the checker from this browser build."}</p>
      </div>
    `}
  `;
  return renderSection({
    key: "billing-checker",
    title: "Maker / Checker",
    badge,
    note: headerNote,
    content,
    defaultOpen: true
  });
}

async function runBillingChecker() {
  const payload = buildCheckerSelectionPayload();
  if (payload.error) {
    showToast("Checker input required", payload.error, "warning");
    return;
  }
  if (!isBillingCheckerEnabled()) {
    showToast("Checker not configured", "Connect BILLING_APP_CONFIG.checkerWebhookUrl to run the independent checker from this hosted build.", "warning");
    return;
  }
  state.checkerStatus = "running";
  state.checkerError = "";
  render();
  try {
    if (isSharedWorkbookEnabled()) {
      try {
        await refreshSharedWorkspace({ showSuccessToast: false, showErrorToast: false, retries: 2, retryDelayMs: 250 });
      } catch (error) {
        console.error("Could not refresh shared workbook before checker run", error);
      }
    }
    const report = await fetchBillingCheckerReport({
      partner: payload.partner,
      startPeriod: payload.startPeriod,
      endPeriod: payload.endPeriod,
      epsilon: 0.01
    });
    state.checkerReport = report;
    state.checkerStatus = report?.failedCount ? "warning" : "success";
    state.checkerError = "";
    recordAccessActivity(
      "run_billing_checker",
      `Ran the billing checker for ${payload.partner} ${payload.startPeriod === payload.endPeriod ? payload.startPeriod : `${payload.startPeriod} to ${payload.endPeriod}`}.`,
      {
        category: "activity",
        partner: payload.partner,
        periodStart: payload.startPeriod,
        periodEnd: payload.endPeriod,
        failedCount: Number(report?.failedCount || 0),
        passedCount: Number(report?.passedCount || 0)
      }
    );
    if (report?.failedCount) {
      showToast("Checker found exceptions", `${report.failedCount} run${report.failedCount === 1 ? "" : "s"} need review before billing is approved.`, "warning");
    } else {
      showToast("Checker passed", "Maker and checker totals matched for this selection.", "success");
    }
    render();
  } catch (error) {
    console.error("Could not run billing checker", error);
    state.checkerStatus = "error";
    state.checkerError = String(error?.message || error || "Unknown error");
    showToast("Checker failed", state.checkerError, "error");
    render();
  }
}

function renderInvoiceDeliveryPanel() {
  if (!state.inv) return "";
  const config = getSharedBackendConfig();
  const privateUrl = getPrivateInvoiceLinkUrl(state.privateInvoiceLinkResult);
  const tone = state.privateInvoiceLinkStatus === "error" || state.invoiceArtifactStatus === "error"
    ? "warning"
    : state.privateInvoiceLinkStatus === "success" || state.invoiceArtifactStatus === "success"
      ? "success"
      : "";
  const archiveConfigured = !!config.invoiceArtifactWriteBaseUrl;
  const linkConfigured = !!(config.privateInvoiceLinkSignerUrl || config.privateInvoiceLinkWriteBaseUrl);
  const archiveSummary = state.invoiceArtifactStatus === "saving"
    ? "Saving a timestamped invoice copy..."
    : state.invoiceArtifactStatus === "success" && state.invoiceArtifactRecord?.savedAt
      ? `Last archived ${formatDateTime(state.invoiceArtifactRecord.savedAt)}`
      : archiveConfigured
        ? "Timestamped invoice archiving is ready."
        : "Connect BILLING_APP_CONFIG.invoiceArtifactWriteBaseUrl to save a timestamped copy on each invoice run.";
  const linkSummary = state.privateInvoiceLinkStatus === "creating"
    ? "Generating a private partner download link..."
    : state.privateInvoiceLinkStatus === "success" && privateUrl
      ? "Private partner download link is ready."
      : linkConfigured
        ? "Private link generation is ready."
        : "Connect BILLING_APP_CONFIG.privateInvoiceLinkSignerUrl to generate a partner-safe private link.";
  return `
    <div class="summary-banner ${tone}" style="margin-top:14px">
      <h4>Partner Delivery Package</h4>
      <p>${html(archiveSummary)}</p>
      <p>${html(linkSummary)}</p>
      ${state.invoiceArtifactRecord?.artifactId ? `<p>Artifact ID: ${html(state.invoiceArtifactRecord.artifactId)}</p>` : ""}
      ${state.invoiceArtifactError ? `<p class="footer-note" style="color:#a33b29">${html(state.invoiceArtifactError)}</p>` : ""}
      ${state.privateInvoiceLinkError ? `<p class="footer-note" style="color:#a33b29">${html(state.privateInvoiceLinkError)}</p>` : ""}
      ${privateUrl ? `
        <div class="button-row" style="margin-top:12px">
          <a class="button secondary" href="${html(privateUrl)}" target="_blank" rel="noopener">Open Private Link</a>
          <button class="button ghost" data-action="copy-private-invoice-link">Copy Private Link</button>
        </div>
        <div class="footer-note" style="margin-top:10px;word-break:break-all">${html(privateUrl)}</div>
      ` : ""}
    </div>
  `;
}

function renderHeader() {
  const statusText = state.lastSaved
    ? `${state.workspaceMode === "shared" ? "Shared" : "Saved"} ${html(state.lastSaved)}`
    : state.workspaceMode === "shared"
      ? `Connected to ${html(state.workspaceLabel)}`
      : state.workspaceMode === "local-seeded"
      ? `Seeded from ${html(state.workspaceLabel)}`
      : state.workspaceMode === "local-fallback"
      ? `Using local fallback`
        : "Using local defaults";
  const sessionLabel = getSessionLabel();
  return `
    <header class="hero">
      <div class="hero-inner">
        <div>
          <h1>Partner Billing Workbook</h1>
          <p>${state.pCosts.length} provider fees · ${state.fxRates.length} FX rates · ${state.ps.length} partners</p>
        </div>
        <div class="header-actions">
          <span class="status-pill">${statusText}</span>
          ${sessionLabel ? `<span class="status-pill">${html(sessionLabel)}</span>` : ""}
          ${isAdminAuthenticated() ? `
            <button class="button secondary small" data-action="open-admin-portal">Admin Portal</button>
            ${isSharedWorkbookEnabled() ? `<button class="button ghost small" data-action="refresh-shared-workspace"${state.workspaceRefreshing ? " disabled" : ""}>${state.workspaceRefreshing ? "Refreshing..." : state.workspaceMode === "local-fallback" ? "Retry Shared Refresh" : "Refresh Shared Data"}</button>` : ""}
            <button class="button ghost small" data-action="export-backup">Export Backup</button>
            <button class="button ghost small" data-action="import-backup">Import Backup</button>
            <button class="button warning small" data-action="reset-defaults">Reset to Defaults</button>
          ` : `
            <button class="button secondary small" data-action="open-admin-login">Admin</button>
          `}
          ${hasAccessSession() ? `<button class="button ghost small" data-action="logout-session">Logout</button>` : ""}
          <input id="backup-file" type="file" accept="application/json" hidden>
        </div>
      </div>
    </header>
  `;
}

function renderPartnerStrip() {
  return `
    <div class="partner-strip">
      <div class="partner-strip-row">
        <div class="partner-chip-list">
          ${state.ps.map((partner) => `
            <button
              type="button"
              class="partner-chip${state.pv === partner ? " is-active" : ""}${isPartnerArchived(partner) ? " is-archived" : ""}"
              data-action="select-partner-chip"
              data-partner="${html(partner)}"
            >
              ${html(partner)}${renderArchivedTag(partner)}
            </button>
          `).join("")}
        </div>
        <div class="partner-add">
          <input class="input" style="min-width:220px" placeholder="Add a partner..." value="${html(state.np)}" data-bind="np" data-bind-live="true">
          <button class="button secondary small" data-action="add-partner">Add</button>
        </div>
      </div>
    </div>
  `;
}

function renderMainTabs() {
  return `<div class="main-tabs">${getVisibleMainTabs().map((tab) => `<button class="tab-button${state.tab === tab.id ? " is-active" : ""}" data-action="set-tab" data-tab="${tab.id}">${html(tab.label)}</button>`).join("")}</div>`;
}

function renderAccessGate({ overlay = false } = {}) {
  const wrapperClass = overlay ? "access-overlay" : "access-gate";
  const contentClass = overlay ? "access-panel access-panel-overlay" : "access-panel";
  const choiceView = state.authView === "choice";
  const adminView = state.authView === "admin";
  const guestView = state.authView === "guest";
  return `
    <div class="${wrapperClass}">
      <div class="${contentClass}">
        ${overlay ? `<button class="access-close" data-action="close-auth-overlay" aria-label="Close">×</button>` : ""}
        <div class="access-badge">${overlay ? "Admin access required" : "Welcome"}</div>
        <h1>${overlay ? "Admin Portal Access" : "Partner Billing Workbook"}</h1>
        <p>${overlay ? "Enter the admin username and password to unlock admin tools and protected tabs." : ""}</p>
        ${choiceView ? `
          <div class="access-choice-grid">
            <button class="button primary" data-action="open-auth-admin">Admin</button>
            <button class="button secondary" data-action="open-auth-guest">Guest</button>
          </div>
        ` : ""}
        ${adminView ? `
          <div class="stack">
            <label class="field">
              <span class="label">Username</span>
              <input class="input" value="${html(state.authUsername)}" data-bind="authUsername" data-bind-live="true" placeholder="VeemAdmin">
            </label>
            <label class="field">
              <span class="label">Password</span>
              <input class="input" type="password" value="${html(state.authPassword)}" data-bind="authPassword" data-bind-live="true" placeholder="Password">
            </label>
            <div class="button-row">
              <button class="button primary" data-action="submit-admin-login">Enter Admin Portal</button>
              <button class="button ghost" data-action="open-auth-choice">Back</button>
            </div>
          </div>
        ` : ""}
        ${guestView ? `
          <div class="stack">
            <label class="field">
              <span class="label">Guest Name</span>
              <input class="input" value="${html(state.guestNameDraft)}" data-bind="guestNameDraft" data-bind-live="true" placeholder="Your name">
            </label>
            <div class="button-row">
              <button class="button primary" data-action="submit-guest-login">Continue as Guest</button>
              <button class="button ghost" data-action="open-auth-choice">Back</button>
            </div>
          </div>
        ` : ""}
        ${state.authError ? `<div class="summary-banner warning"><h4>Access issue</h4><p>${html(state.authError)}</p></div>` : ""}
      </div>
    </div>
  `;
}

function renderBillingSummary() {
  const summaryPartnerOwesLabel = getPartnerOwesLabel(state.billingSummaryPartner);
  const totalPartnerOwesLabel = getPartnerOwesLabel(state.billingSummaryPartner, { plural: !state.billingSummaryPartner });
  const summaries = getPartnerOptions({ includeArchived: false, includeArchivedTag: false })
    .map((option) => summarizePartnerOutstanding(option.value || option))
    .filter((summary) => !state.billingSummaryPartner || norm(summary.partner) === norm(state.billingSummaryPartner))
    .filter((summary) => {
      if (state.billingSummaryPartner) return true;
      return !(summary.status.tone === "success" && summary.owesUs <= 0 && summary.weOwePartner <= 0);
    })
    .sort((a, b) => {
      const severity = { danger: 0, warning: 1, info: 2, credit: 3, muted: 4, success: 5 };
      return (severity[a.status.tone] ?? 9) - (severity[b.status.tone] ?? 9)
        || Math.abs(b.netPosition) - Math.abs(a.netPosition)
        || a.partner.localeCompare(b.partner);
    });
  const totals = summaries.reduce((acc, summary) => {
    acc.owesUs += summary.owesUs;
    acc.weOwePartner += summary.weOwePartner;
    return acc;
  }, { owesUs: 0, weOwePartner: 0 });
  const net = roundCurrency(totals.owesUs - totals.weOwePartner);
  const rowsExpanded = areTableRowsExpanded("billing-summary", "invoice");
  return renderSection({
    key: "billing-summary",
    tableRowsKey: "billing-summary",
    pageId: "invoice",
    title: "Billing Summary",
    badge: summaries.length || "",
    note: "Shows the current open balance by partner, including unpaid invoices, partner credits, send reminders, and past-due status. Fully settled partners are hidden unless you filter to one directly.",
    defaultOpen: false,
    content: `
      <div class="stack">
        <div class="field-grid">
          <label class="field" style="max-width:320px">
            ${renderLabelWithInfo("Partner Filter", "Filter the summary to one partner or leave it on All Partners to scan the whole billing book.")}
            <select class="select" data-bind="billingSummaryPartner">
              ${renderOptions([{ value: "", label: "All Partners" }, ...getPartnerOptions({ includeArchived: false, includeArchivedTag: false })], state.billingSummaryPartner)}
            </select>
          </label>
          <div class="kpi-card">
            <strong>${fmt(totals.owesUs)}</strong>
            <span>${html(totalPartnerOwesLabel)}</span>
          </div>
          <div class="kpi-card">
            <strong>${fmt(totals.weOwePartner)}</strong>
            <span>Veem Owes</span>
          </div>
          <div class="kpi-card ${net >= 0 ? "info" : "warning"}">
            <strong>${fmt(Math.abs(net))}</strong>
            <span>${net >= 0 ? "Net Settlement To Veem" : "Net Settlement To Partners"}</span>
          </div>
        </div>
        <div class="table-wrap">
          <table class="data-table billing-summary-table">
            <thead>
              <tr>
                <th>Partner</th>
                <th>Status</th>
                <th>${html(summaryPartnerOwesLabel)}</th>
                <th>Veem Owes</th>
                <th>Settlement</th>
              </tr>
            </thead>
            <tbody>
              ${!summaries.length ? `<tr><td colspan="5" class="empty-state">No matching partners in the summary filter.</td></tr>` : !rowsExpanded ? "" : summaries.map((summary) => `
                <tr>
                  <td>${html(summary.partner)}</td>
                  <td>${renderInvoiceStatusTag(summary.status)}</td>
                  <td class="mono align-right">${renderSummaryAmountCell(summary.owesUs)}</td>
                  <td class="mono align-right">${renderSummaryAmountCell(summary.weOwePartner)}</td>
                  <td>${renderSettlementTag(summary)}</td>
                </tr>
              `).join("")}
            </tbody>
          </table>
        </div>
      </div>
    `
  });
}

function renderPartnerBillingSection(partner) {
  const config = buildDefaultPartnerBilling(partner, getPartnerBillingConfig(partner));
  const dueDays = Number(config.dueDays || getBillingDueDays(partner) || 0);
  const entries = getPartnerInvoiceTrackingEntries(partner);
  const rowsExpanded = areTableRowsExpanded(`partner-billing-${partner}`, "partner");
  return `
    <div class="stack">
      <div class="field-grid billing-field-grid">
        <label class="field">
          ${renderLabelWithInfo("Billing Frequency", "Imported from the contract when available. This is stored per partner and can be edited manually.")}
          <select class="select" data-action="update-partner-billing" data-partner="${html(partner)}" data-key="billingFreq">
            ${renderOptions(["Monthly", "Quarterly", "Annual", "Custom"], config.billingFreq || "Monthly")}
          </select>
        </label>
        <label class="field">
          ${renderLabelWithInfo("Payment Terms", "Raw contract payment language used for due-date logic, for example Net 30 or Due in 7 days.")}
          <input class="input" type="text" value="${html(config.payBy || "")}" placeholder="Net 30 / Due in 7 days" data-action="update-partner-billing" data-partner="${html(partner)}" data-key="payBy">
        </label>
        <label class="field">
          ${renderLabelWithInfo("Due Days", "Parsed from the contract when possible. You can override it manually if the contract language is more specific than the parser could infer.")}
          <input class="input" type="number" min="0" step="1" value="${html(dueDays || "")}" placeholder="30" data-action="update-partner-billing" data-partner="${html(partner)}" data-key="dueDays">
        </label>
        <label class="field">
          ${renderLabelWithInfo("Monthly Billing Day", "Used for the 10-day and 5-day send reminders. The app assumes invoices for a month are expected on this day of the following month.")}
          <input class="input" type="number" min="1" max="31" step="1" value="${html(config.billingDay || "")}" placeholder="10" data-action="update-partner-billing" data-partner="${html(partner)}" data-key="billingDay">
        </label>
        <label class="field">
          ${renderLabelWithInfo("Contract Start Date", "Autofilled from the contract effective/sign date when available. One-time implementation fees bill against this date, not the go-live date.")}
          <input class="input" type="date" value="${html(config.contractStartDate || "")}" data-action="update-partner-billing" data-partner="${html(partner)}" data-key="contractStartDate">
        </label>
        <label class="field">
          ${renderLabelWithInfo("Go Live Date", "Recurring billing starts only once the partner is live. Before go-live, only implementation can bill. After go-live, processing fees, other fees, and monthly subscription fees bill monthly for the prior / preceding month.")}
          <div class="field-control-stack">
            <input class="input" type="date" value="${html(config.goLiveDate || "")}" data-action="update-partner-billing" data-partner="${html(partner)}" data-key="goLiveDate">
            <label class="inline-check is-subfield">
              <input type="checkbox" data-action="update-partner-billing" data-partner="${html(partner)}" data-key="notYetLive"${config.notYetLive ? " checked" : ""}>
              <span>Partner is not yet live</span>
            </label>
          </div>
        </label>
        <label class="field billing-span-2">
          ${renderLabelWithInfo("Integration Status", "Operational onboarding status from HubSpot. This will eventually sync in live, but can be edited here for now.")}
          <input class="input" type="text" value="${html(config.integrationStatus || "")}" placeholder="Integration Underway (Partners Onboarding)" data-action="update-partner-billing" data-partner="${html(partner)}" data-key="integrationStatus">
        </label>
        <label class="field billing-span-2">
          ${renderLabelWithInfo("Contract Due Timing", "Operational note from AP Summary / contract review showing how the agreement describes the due timing.")}
          <input class="input" type="text" value="${html(config.contractDueText || "")}" placeholder="7th of the following month" data-action="update-partner-billing" data-partner="${html(partner)}" data-key="contractDueText">
        </label>
        <label class="field billing-span-2">
          ${renderLabelWithInfo("Preferred Billing Timing", "Partner-specific preference for when invoices are usually sent or expected each cycle.")}
          <input class="input" type="text" value="${html(config.preferredBillingTiming || "")}" placeholder="1st week of following month" data-action="update-partner-billing" data-partner="${html(partner)}" data-key="preferredBillingTiming">
        </label>
        <label class="field">
          ${renderLabelWithInfo("Late Fee % / Month", "If the contract applies late fees, store the monthly percentage here, for example 1.5 for 1.5% monthly interest. Leave blank / 0 if late fees do not apply.")}
          <input class="input" type="number" min="0" step="0.01" value="${html(config.lateFeePercentMonthly || "")}" placeholder="1.5" data-action="update-partner-billing" data-partner="${html(partner)}" data-key="lateFeePercentMonthly">
        </label>
        <label class="field">
          ${renderLabelWithInfo("Late Fee Starts After Days", "How many days after the due date late fees begin. For example, if fees start after 30 days overdue, enter 30.")}
          <input class="input" type="number" min="0" step="1" value="${html(config.lateFeeStartDays || "")}" placeholder="30" data-action="update-partner-billing" data-partner="${html(partner)}" data-key="lateFeeStartDays">
        </label>
        <label class="field">
          ${renderLabelWithInfo("Suspend Services After Days", "Optional. If the contract allows service suspension after a certain number of overdue days, enter that threshold here for tracking and downstream operations.")}
          <input class="input" type="number" min="0" step="1" value="${html(config.serviceSuspensionDays || "")}" placeholder="30" data-action="update-partner-billing" data-partner="${html(partner)}" data-key="serviceSuspensionDays">
        </label>
        <label class="field billing-span-2">
          ${renderLabelWithInfo("Late Fee Terms", "Store the exact contract wording or summary for late fees / suspension here so it is preserved in billing records and available to downstream systems.")}
          <input class="input" type="text" value="${html(config.lateFeeTerms || "")}" placeholder="1.5% monthly interest begins after 30 days overdue" data-action="update-partner-billing" data-partner="${html(partner)}" data-key="lateFeeTerms">
        </label>
        <label class="field field-wide billing-span-4">
          ${renderLabelWithInfo("Partner Contact Emails", "Billing / AP contact emails from the AP Summary workbook. You can edit this list manually if the contacts change.")}
          <textarea class="textarea textarea-compact" data-action="update-partner-billing" data-partner="${html(partner)}" data-key="contactEmails" placeholder="billing@example.com, finance@example.com">${html(config.contactEmails || "")}</textarea>
        </label>
      </div>
      <div class="table-wrap">
        <table class="data-table invoice-tracker-table">
          <thead>
            <tr>
              <th>Invoice Month</th>
              <th>Period Total</th>
              <th>Invoice Date</th>
              <th>Due Date</th>
              <th>Status</th>
              <th>Paid?</th>
              <th>Amount Paid</th>
              <th>Balance</th>
            </tr>
          </thead>
          <tbody>
            ${!entries.length ? `<tr><td colspan="8" class="empty-state">No invoice periods available yet. Import activity first, then invoice tracking will appear here.</td></tr>` : !rowsExpanded ? "" : entries.map((entry) => {
              const trackDisabled = entry.amountDue <= 0;
              const kindLabel = entry.kind === "payable" ? "Veem Owes" : getPartnerOwesLabel(partner);
              return `
                <tr>
                  <td>
                    <div>${html(formatPeriodLabel(entry.period))}</div>
                    <div class="invoice-tracker-meta">${html(kindLabel)}</div>
                  </td>
                  <td>
                    ${fmt(entry.amountDue)}
                  </td>
                  <td>
                    <input
                      class="table-input"
                      type="date"
                      value="${html(entry.invoiceDate || "")}"
                      ${trackDisabled ? "disabled" : ""}
                      data-action="update-invoice-date"
                      data-partner="${html(partner)}"
                      data-period="${html(entry.period)}"
                      data-kind="${html(entry.kind)}"
                    >
                    ${entry.kind === "receivable" && !entry.invoiceDate && entry.expectedSendDate && entry.amountDue > 0 ? `<div class="invoice-tracker-meta">Expected ${html(formatIsoDate(entry.expectedSendDate))}</div>` : ""}
                  </td>
                  <td>${entry.dueDate ? html(formatIsoDate(entry.dueDate)) : "—"}</td>
                  <td>${renderInvoiceStatusTag(entry.status)}</td>
                  <td class="align-center">
                    <input
                      class="table-input"
                      type="checkbox"
                      ${entry.record?.paid ? "checked" : ""}
                      ${trackDisabled ? "disabled" : ""}
                      data-action="toggle-invoice-paid"
                      data-partner="${html(partner)}"
                      data-period="${html(entry.period)}"
                      data-kind="${html(entry.kind)}"
                      data-amount-due="${entry.amountDue}"
                    >
                  </td>
                  <td>
                    <input
                      class="table-input"
                      type="number"
                      min="0"
                      step="0.01"
                      value="${html(entry.record && entry.amountPaid > 0 ? entry.amountPaid : "")}"
                      placeholder="${entry.amountDue > 0 ? Number(entry.amountDue).toFixed(2) : ""}"
                      ${trackDisabled ? "disabled" : ""}
                      data-action="update-invoice-amount-paid"
                      data-partner="${html(partner)}"
                      data-period="${html(entry.period)}"
                      data-kind="${html(entry.kind)}"
                      data-amount-due="${entry.amountDue}"
                    >
                  </td>
                  <td>${fmt(entry.balance)}</td>
                </tr>
              `;
            }).join("")}
          </tbody>
        </table>
      </div>
    </div>
  `;
}

function renderInvoiceDocumentTable(doc) {
  const rowsExpanded = true;
  const periodActivityRows = getInvoicePeriodActivityRows(doc.partner, doc.periodStart || doc.period, doc.periodEnd || doc.period);
  return `
    <div class="panel invoice-document-panel">
      <div class="section-header compact invoice-document-header">
        <div>
          <h3 class="section-title">${html(doc.title)}</h3>
          <p class="invoice-document-note">${html(doc.amountLabel)} ${fmt(doc.amountDue)}</p>
        </div>
        <div class="invoice-document-actions">
          <span class="invoice-status-pill ${doc.kind === "receivable" ? "is-info" : "is-muted"}">${html(doc.amountLabel)} ${fmt(doc.amountDue)}</span>
          <button class="invoice-icon-button" data-action="export-invoice-pdf" data-doc-kind="${doc.kind}" title="${html(doc.buttonLabel)}" aria-label="${html(doc.buttonLabel)}">
            <span aria-hidden="true">${html(doc.buttonLabel)}</span>
          </button>
        </div>
      </div>
      <div class="table-wrap">
        <table class="data-table">
          <thead>
            <tr>
              <th>Category</th>
              <th>Description</th>
              <th>${doc.kind === "receivable" ? "We Charge" : "Reference"}</th>
              <th>${doc.kind === "receivable" ? "Credits" : "Amount"}</th>
            </tr>
          </thead>
          <tbody>
            ${!doc.groups.length ? `<tr><td colspan="4" class="empty-state">No lines were generated for this document.</td></tr>` : !rowsExpanded ? "" : doc.groups.map((group) => {
                const open = isSectionOpen(invoiceGroupSectionKey(group.id));
                const chargeMarkup = doc.kind === "receivable"
                  ? group.charge
                    ? formatSignedInvoiceAmount(group.charge)
                    : group.isInactive && group.displayCharge
                      ? `<span class="invoice-inactive-amount">${formatSignedInvoiceAmount(group.displayCharge)}</span>`
                      : ""
                  : "";
                const amountMarkup = doc.kind === "receivable"
                  ? group.offset
                    ? `(${fmt(group.offset)})`
                    : group.isInactive && group.displayOffset
                      ? `<span class="invoice-inactive-amount">(${fmt(group.displayOffset)})</span>`
                      : ""
                  : group.pay
                    ? formatSignedInvoiceAmount(group.pay)
                    : group.isInactive && group.displayPay
                      ? `<span class="invoice-inactive-amount">${formatSignedInvoiceAmount(group.displayPay)}</span>`
                      : "";
                return `
                  <tr class="invoice-group-row${group.isInactive ? " is-inactive" : ""}">
                    <td><span class="category-chip ${categoryClass(group.cat)}">${html(group.cat)}</span></td>
                    <td>
                      <button class="invoice-line-toggle" data-action="toggle-invoice-group" data-group-id="${group.id}">
                        <span class="toggle-indicator">${open ? "▾" : "▸"}</span>
                        <span>${html(group.label)}</span>
                      </button>
                      <div class="invoice-line-meta">${html(group.summary)}</div>
                    </td>
                    <td class="mono align-right">${chargeMarkup || (doc.kind === "payable" ? "—" : "")}</td>
                    <td class="mono align-right">${amountMarkup}</td>
                  </tr>
                  ${open ? `
                    <tr class="invoice-detail-row">
                      <td colspan="4">
                        <div class="invoice-detail-card">
                          <div class="invoice-detail-meta">
                            ${group.activityRowCount ? `<span class="helper-pill">${group.activityRowCount} imported row${group.activityRowCount === 1 ? "" : "s"}</span>` : ""}
                            ${group.activityTxnCount ? `<span class="helper-pill">${group.activityTxnCount.toLocaleString("en-US")} txns</span>` : ""}
                            ${group.activityVolume ? `<span class="helper-pill">${fmt(group.activityVolume)} volume</span>` : ""}
                            ${group.lines.length > 1 ? `<span class="helper-pill">${group.lines.length} calc lines</span>` : ""}
                          </div>
                          ${renderInvoiceDetailSubsection({
                            groupId: group.id,
                            name: "revenue",
                            title: doc.kind === "receivable" ? "Revenue" : "Payout",
                            summary: `${group.lines.length} line${group.lines.length === 1 ? "" : "s"} · ${doc.kind === "payable" ? formatSignedInvoiceAmount(group.pay || group.displayPay) : group.charge ? formatSignedInvoiceAmount(group.charge || group.displayCharge) : `(${fmt(group.offset || group.displayOffset)})`}`,
                            content: `
                              <div class="invoice-detail-list">
                                ${group.lines.map((line) => `
                                  <div class="invoice-detail-item${line.active === false ? " is-inactive" : ""}">
                                    <div>
                                      <div>${html(line.desc)}</div>
                                      ${state.inv.periodStart !== state.inv.periodEnd && line.sourcePeriod ? `<div class="invoice-detail-reason">${html(formatPeriodLabel(line.sourcePeriod))}</div>` : ""}
                                      ${line.active === false && line.inactiveReason ? `<div class="invoice-detail-reason">${html(line.inactiveReason)}</div>` : ""}
                                    </div>
                                    <div class="mono">${formatInvoiceDetailAmount(line)}</div>
                                  </div>
                                `).join("")}
                              </div>
                            `
                          })}
                          ${group.activityRows.length ? renderInvoiceDetailSubsection({
                            groupId: group.id,
                            name: "volume",
                            title: "Volume",
                            summary: `${group.activityRowCount} support row${group.activityRowCount === 1 ? "" : "s"} · ${fmt(group.activityVolume)}`,
                            content: `
                              <div class="invoice-detail-list">
                                ${group.activityRows.map((row) => `
                                  <div class="invoice-detail-item">
                                    <div>
                                      <div>${html(describeActivitySummaryRow(row))}</div>
                                      ${Number(row.generatedRevenueSupport || 0) ? `<div class="invoice-detail-reason">Generated revenue: ${fmt(row.generatedRevenueSupport)}</div>` : ""}
                                      ${Number(row.customerRevenue || 0) ? `<div class="invoice-detail-reason">Imported revenue: ${fmt(row.customerRevenue)}</div>` : ""}
                                      ${Number(row.estRevenue || 0) ? `<div class="invoice-detail-reason">Est revenue already charged: ${fmt(row.estRevenue)}</div>` : ""}
                                    </div>
                                    <div class="mono">${fmt(row.totalVolume)}</div>
                                  </div>
                                `).join("")}
                              </div>
                            `
                          }) : ""}
                          ${periodActivityRows.length ? renderInvoiceDetailSubsection({
                            groupId: group.id,
                            name: "all-period-activity",
                            title: "All Period Transaction Data",
                            summary: summarizeInvoicePeriodActivityRows(periodActivityRows),
                            content: renderInvoicePeriodActivityRows(periodActivityRows)
                          }) : ""}
                          <div class="button-row invoice-detail-actions">
                            ${group.activityRowCount ? `<button class="button secondary small" data-action="open-invoice-explorer" data-scope="matching" data-group-id="${group.id}">Export Matching Transactions CSV</button>` : ""}
                          </div>
                        </div>
                      </td>
                    </tr>
                  ` : ""}
                `;
              }).join("")}
          </tbody>
          <tfoot>
            ${doc.kind === "receivable"
              ? `<tr><td colspan="2">Subtotals</td><td class="align-right mono">${fmt(doc.chargeTotal)}</td><td class="align-right mono">${doc.creditTotal ? `(${fmt(doc.creditTotal)})` : fmt(0)}</td></tr>`
              : `<tr><td colspan="2">Subtotals</td><td class="align-right mono">—</td><td class="align-right mono">${fmt(doc.payTotal)}</td></tr>`}
            <tr><td colspan="2">${html(doc.kind === "receivable" ? "BALANCE DUE" : "AMOUNT DUE TO PARTNER")}</td><td colspan="2" class="align-right mono">${fmt(doc.amountDue)}</td></tr>
          </tfoot>
        </table>
      </div>
    </div>
  `;
}

function renderInvoiceTab() {
  const noteSectionKey = state.inv ? `invoice-notes:${state.inv.partner}:${state.inv.period}` : "invoice-notes";
  const summaryPanel = renderBillingSummary();
  const invoiceDocuments = state.inv ? buildInvoiceDocuments(state.inv) : [];
  const receivableDoc = invoiceDocuments.find((doc) => doc.kind === "receivable") || null;
  const payableDoc = invoiceDocuments.find((doc) => doc.kind === "payable") || null;
  const privateLinkReady = !!getPrivateInvoiceLinkUrl(state.privateInvoiceLinkResult);
  const netAmount = state.inv ? Math.abs(Number(state.inv.net || 0)) : 0;
  const netLabel = state.inv ? (Number(state.inv.net || 0) >= 0 ? getPartnerOwesLabel(state.inv.partner) : "Veem Owes") : "";
  const partnerLifecycle = state.inv ? getPartnerLifecycleStatus(state.inv.partner, state.inv.periodStart || state.inv.period, state.inv.periodEnd || state.inv.period) : null;
  const banner = state.inv ? `
    <div class="invoice-banner">
      <div class="invoice-banner-main">
        <div class="invoice-title-row">
          <h3>${html(state.inv.partner)}</h3>
          ${renderPartnerLifecycleBadge(partnerLifecycle)}
        </div>
        <p>Range: ${html(state.inv.periodDateRange || formatPeriodDateRange(state.inv.periodStart || state.inv.period, state.inv.periodEnd || state.inv.period))}</p>
      </div>
      <div class="invoice-banner-aside">
        <div class="invoice-meta-actions">
          ${receivableDoc ? `<button class="invoice-icon-button" data-action="export-invoice-pdf" data-doc-kind="receivable" title="Download AR invoice PDF" aria-label="Download AR invoice PDF">
            <span aria-hidden="true">Download AR Invoice</span>
          </button>` : ""}
          ${payableDoc ? `<button class="invoice-icon-button" data-action="export-invoice-pdf" data-doc-kind="payable" title="Download AP invoice PDF" aria-label="Download AP invoice PDF">
            <span aria-hidden="true">Download AP Invoice</span>
          </button>` : ""}
          <button class="invoice-icon-button" data-action="export-invoice-period-transactions" title="Export all transactions for this partner and date range" aria-label="Export all transactions for this partner and date range">
            <span aria-hidden="true">Download All Transactions</span>
          </button>
          <button class="invoice-icon-button" data-action="generate-private-invoice-link"${state.privateInvoiceLinkStatus === "creating" ? " disabled" : ""} title="Generate a private partner download link" aria-label="Generate a private partner download link">
            <span aria-hidden="true">${state.privateInvoiceLinkStatus === "creating" ? "Generating Private Link..." : "Generate Private Link"}</span>
          </button>
          ${privateLinkReady ? `<button class="invoice-icon-button" data-action="copy-private-invoice-link" title="Copy the private partner download link" aria-label="Copy the private partner download link">
            <span aria-hidden="true">Copy Private Link</span>
          </button>` : ""}
        </div>
        <div class="invoice-banner-badges">
          ${receivableDoc ? `<div class="amount-badge"><div>AR Invoice</div><div>${fmt(receivableDoc.amountDue)}</div></div>` : ""}
          ${payableDoc ? `<div class="amount-badge warning"><div>AP Invoice</div><div>${fmt(payableDoc.amountDue)}</div></div>` : ""}
          ${(receivableDoc && payableDoc) ? `<div class="amount-badge"><div>${html(netLabel)}</div><div>${fmt(netAmount)}</div></div>` : ""}
        </div>
      </div>
    </div>
  ` : "";
  const deliveryPanel = state.inv ? renderInvoiceDeliveryPanel() : "";
  const notesBlock = state.inv?.notes?.length ? renderSection({
    key: noteSectionKey,
    title: "Invoice Notes",
    badge: state.inv.notes.length,
    note: "These items need source data review before treating the invoice as final.",
    content: `
      <ul class="bulleted-list">
        ${state.inv.notes.map((note) => `<li>${html(note)}</li>`).join("")}
      </ul>
    `
  }) : "";
  const documentTables = state.inv
    ? (invoiceDocuments.length
      ? invoiceDocuments.map((doc) => renderInvoiceDocumentTable(doc)).join("")
      : `<div class="panel"><div class="empty-state">No invoice lines were generated for this selection.</div></div>`)
    : "";
  const invoiceSection = state.inv ? `${banner}${deliveryPanel}${notesBlock}${documentTables}` : "";
  const checkerPanel = renderBillingCheckerPanel();
  const checkerSelectionReady = isBillingCheckerEnabled() && !!resolvePartnerName(state.sp) && !!state.perStart && (!state.useDateRange || !!state.perEnd);
  return `
    <div class="stack">
      ${renderPageTableToggle("invoice")}
      <div class="card">
        <div class="section-header compact">
          <div>
            <h3 class="section-title">Generate Invoice ${renderInfoTip("Run a single-month or multi-month invoice range based on workbook config and imported activity.")}</h3>
          </div>
        </div>
        <div class="field-grid" style="margin-top:16px">
          <label class="field">
            <span class="label">Partner</span>
            <div class="search-field">
              <input class="input" type="text" list="invoice-partner-options" data-bind="sp" value="${html(state.sp)}" placeholder="Search partner..." autocomplete="off">
              <datalist id="invoice-partner-options">${renderDatalistOptions(getPartnerOptions({ includeArchived: false }))}</datalist>
            </div>
          </label>
          <label class="field">
            ${renderLabelWithInfo("Start Month", `Date coverage runs from ${formatPeriodBoundary(state.perStart, "start")} through ${formatPeriodBoundary(state.perEnd, "end")}.`)}
            <input class="input" type="month" data-bind="perStart" value="${html(state.perStart)}">
          </label>
          <div class="field">
            ${renderLabelWithInfo("Date Range", "Leave this off for normal one-month billing. Turn it on only when you want to extend the invoice across multiple months.")}
            <label class="inline-check">
              <input type="checkbox" data-action="toggle-date-range"${state.useDateRange ? " checked" : ""}>
              Enable date range
            </label>
          </div>
          <label class="field">
            ${renderLabelWithInfo("End Month", `Date coverage runs from ${formatPeriodBoundary(state.perStart, "start")} through ${formatPeriodBoundary(state.perEnd, "end")}.`)}
            <input class="input${state.useDateRange ? "" : " is-disabled-range"}" type="month" data-bind="perEnd" value="${html(state.perEnd)}"${state.useDateRange ? "" : " disabled"}>
          </label>
          <div class="field">
            <span class="label">Run</span>
            <div class="button-row">
              <button class="button primary" data-action="calculate-invoice">Calculate</button>
              <button class="button secondary" data-action="run-billing-checker"${checkerSelectionReady ? "" : " disabled"}>${state.checkerStatus === "running" ? "Checking..." : "Run Checker"}</button>
              <button class="button secondary" data-action="clear-invoice-selection">Clear</button>
            </div>
          </div>
        </div>
      </div>
      ${invoiceSection}
      ${checkerPanel}
      ${summaryPanel}
    </div>
  `;
}

function renderPartnerTab() {
  if (!state.pv) {
    return `
      <div class="stack">
        ${renderPageTableToggle("partner")}
        <div class="card">
          <div class="field-grid">
            <label class="field">
              ${renderLabelWithInfo("Partner", "Pick a partner to inspect and edit only their pricing rows.")}
              <select class="select" data-bind="pv">${renderOptions([{ value: "", label: "Select..." }, ...getPartnerOptions()], state.pv)}</select>
            </label>
          </div>
        </div>
      </div>
    `;
  }

  const partner = state.pv;
  const archived = isPartnerArchived(partner);
  const sections = [];
  sections.push(renderSection({
    key: `partner-activity-${partner}`,
    tableRowsKey: `partner-activity-${partner}`,
    pageId: "partner",
    title: "Partner Activity",
    badge: state.pActive.filter((row) => row.partner === partner).length,
    note: "Default is active when no row matches. If rows overlap, inactive wins.",
    content: renderDataTable({
      section: "pActive",
      cols: getTableConfigs().pActive,
      rows: state.pActive,
      rowsKey: `partner-activity-${partner}`,
      pageId: "partner",
      filterFn: (row) => row.partner === partner,
      emptyLabel: "No activity schedule rows. Partner is active by default."
    })
  }));
  sections.push(renderSection({
    key: `partner-billing-${partner}`,
    tableRowsKey: `partner-billing-${partner}`,
    pageId: "partner",
    title: "Billing & Invoice Tracking",
    badge: getPartnerInvoiceTrackingEntries(partner).length,
    note: "Contract payment terms feed due dates and invoice timing. Contact emails and late-fee settings are stored here for downstream systems and recordkeeping. Payment state is tracked manually per invoice month.",
    defaultOpen: false,
    content: renderPartnerBillingSection(partner)
  }));
  sections.push(renderSection({ key: `partner-off-${partner}`, tableRowsKey: `partner-off-${partner}`, pageId: "partner", title: "Offline Rates", badge: state.off.filter((row) => row.partner === partner).length, content: renderDataTable({ section: "off", cols: getTableConfigs().off, rows: state.off, rowsKey: `partner-off-${partner}`, pageId: "partner", filterFn: (row) => row.partner === partner }) }));
  sections.push(renderSection({ key: `partner-vol-${partner}`, tableRowsKey: `partner-vol-${partner}`, pageId: "partner", title: "Volume Rates", badge: state.vol.filter((row) => row.partner === partner).length, content: renderDataTable({ section: "vol", cols: getTableConfigs().vol, rows: state.vol, rowsKey: `partner-vol-${partner}`, pageId: "partner", filterFn: (row) => row.partner === partner }) }));
  if (state.surch.some((row) => row.partner === partner)) {
    sections.push(renderSection({ key: `partner-surch-${partner}`, tableRowsKey: `partner-surch-${partner}`, pageId: "partner", title: "Surcharges", badge: state.surch.filter((row) => row.partner === partner).length, content: renderDataTable({ section: "surch", cols: getTableConfigs().surch, rows: state.surch, rowsKey: `partner-surch-${partner}`, pageId: "partner", filterFn: (row) => row.partner === partner }) }));
  }
  sections.push(renderSection({ key: `partner-rs-${partner}`, tableRowsKey: `partner-rs-${partner}`, pageId: "partner", title: "Rev Share", badge: state.rs.filter((row) => row.partner === partner).length, content: renderDataTable({ section: "rs", cols: getTableConfigs().rs, rows: state.rs, rowsKey: `partner-rs-${partner}`, pageId: "partner", filterFn: (row) => row.partner === partner }) }));
  sections.push(renderSection({ key: `partner-min-${partner}`, tableRowsKey: `partner-min-${partner}`, pageId: "partner", title: "Minimums", badge: state.mins.filter((row) => row.partner === partner).length, content: renderDataTable({ section: "mins", cols: getTableConfigs().mins, rows: state.mins, rowsKey: `partner-min-${partner}`, pageId: "partner", filterFn: (row) => row.partner === partner }) }));
  if (state.fxRates.some((row) => row.partner === partner)) {
    sections.push(renderSection({ key: `partner-fx-${partner}`, tableRowsKey: `partner-fx-${partner}`, pageId: "partner", title: "FX Rates", badge: state.fxRates.filter((row) => row.partner === partner).length, content: renderDataTable({ section: "fxRates", cols: getTableConfigs().fxRates, rows: state.fxRates, rowsKey: `partner-fx-${partner}`, pageId: "partner", filterFn: (row) => row.partner === partner }) }));
  }
  sections.push(renderSection({ key: `partner-plat-${partner}`, tableRowsKey: `partner-plat-${partner}`, pageId: "partner", title: "Platform Fees", badge: state.plat.filter((row) => row.partner === partner).length, content: renderDataTable({ section: "plat", cols: getTableConfigs().plat, rows: state.plat, rowsKey: `partner-plat-${partner}`, pageId: "partner", filterFn: (row) => row.partner === partner }) }));
  sections.push(renderSection({ key: `partner-revf-${partner}`, tableRowsKey: `partner-revf-${partner}`, pageId: "partner", title: "Reversal Fees", badge: state.revf.filter((row) => row.partner === partner).length, content: renderDataTable({ section: "revf", cols: getTableConfigs().revf, rows: state.revf, rowsKey: `partner-revf-${partner}`, pageId: "partner", filterFn: (row) => row.partner === partner }) }));
  sections.push(renderSection({ key: `partner-impl-${partner}`, tableRowsKey: `partner-impl-${partner}`, pageId: "partner", title: "Implementation Fees", badge: state.impl.filter((row) => row.partner === partner).length, content: renderDataTable({ section: "impl", cols: getTableConfigs().impl, rows: state.impl, rowsKey: `partner-impl-${partner}`, pageId: "partner", filterFn: (row) => row.partner === partner }) }));
  sections.push(renderSection({ key: `partner-cap-${partner}`, tableRowsKey: `partner-cap-${partner}`, pageId: "partner", title: "Fee Caps", badge: state.cap.filter((row) => row.partner === partner).length, content: renderDataTable({ section: "cap", cols: getTableConfigs().cap, rows: state.cap, rowsKey: `partner-cap-${partner}`, pageId: "partner", filterFn: (row) => row.partner === partner }) }));
  if (state.vaFees.some((row) => row.partner === partner)) {
    sections.push(renderSection({ key: `partner-va-${partner}`, tableRowsKey: `partner-va-${partner}`, pageId: "partner", title: "Virtual Account Fees", badge: state.vaFees.filter((row) => row.partner === partner).length, content: renderDataTable({ section: "vaFees", cols: getTableConfigs().vaFees, rows: state.vaFees, rowsKey: `partner-va-${partner}`, pageId: "partner", filterFn: (row) => row.partner === partner }) }));
  }

  const deleteBox = state.confirmDel ? `
    <div class="destructive-box" style="margin-top:16px">
      <p><strong>Delete "${html(partner)}" from the workbook?</strong></p>
      <p class="footer-note">This will permanently delete all customer data, pricing rows, imported activity, and invoice support for this partner. Provider cost tables are not affected.</p>
      <p class="footer-note">You can archive the partner instead to remove them from invoice generation while keeping their historical setup visible everywhere else.</p>
      <div class="button-row" style="margin-top:14px">
        <button class="button danger" data-action="confirm-delete-partner">Delete Anyways</button>
        ${archived ? "" : `<button class="button secondary" data-action="archive-partner">Archive Partner</button>`}
        <button class="button ghost" data-action="cancel-delete-partner">Cancel</button>
      </div>
    </div>
  ` : "";

  return `
    <div class="stack">
      ${renderPageTableToggle("partner")}
      <div class="card">
        <div class="field-grid">
          <label class="field">
            ${renderLabelWithInfo("Partner", `Inspect and manage a single partner's configuration.${archived ? " This partner is archived and excluded from invoice generation." : ""}`)}
            <select class="select" data-bind="pv">${renderOptions([{ value: "", label: "Select..." }, ...getPartnerOptions()], partner)}</select>
          </label>
          <div class="field">
            ${renderLabelWithInfo("Pricing Mode", "Use only when the contract says volume above a threshold gets the new rate while lower-tier volume keeps the lower-tier rate.")}
            <label class="inline-check">
              <input type="checkbox" data-action="toggle-incremental"${state.pConfig[partner] ? " checked" : ""}>
              Per-tier marginal pricing
            </label>
            ${archived ? `<div class="archived-banner">${renderArchivedTag(partner)}${renderInfoTip("Archived partners remain visible in configuration tables but are hidden from invoice selection.")}</div>` : ""}
          </div>
          <div class="field">
            ${renderLabelWithInfo("Danger Zone", archived ? "Unarchive to make this partner billable again." : "Archive removes the partner from invoice generation without deleting history.")}
            <div class="button-row">
              <button class="button secondary" data-action="${archived ? "unarchive-partner" : "archive-partner"}">${archived ? "Unarchive Partner" : "Archive Partner"}</button>
              <button class="button danger" data-action="toggle-delete-partner">Delete Partner</button>
            </div>
          </div>
        </div>
        ${deleteBox}
      </div>
      ${sections.join("")}
    </div>
  `;
}

function renderRatesTab() {
  const tabs = `<div class="sub-tabs">${rateTabs.map((tab) => `<button class="tab-button${state.sub === tab.id ? " is-active" : ""}" data-action="set-subtab" data-sub="${tab.id}">${html(tab.label)}</button>`).join("")}</div>`;
  const configs = getTableConfigs();
  let content = "";
  if (state.sub === "offline") content = renderSection({ key: "rates-offline", title: "Offline · Fixed $ Per Txn", defaultOpen: true, content: renderDataTable({ section: "off", cols: configs.off, rows: state.off }) });
  if (state.sub === "volume") content = renderSection({ key: "rates-volume", title: "Volume · % of Volume", defaultOpen: true, content: renderDataTable({ section: "vol", cols: configs.vol, rows: state.vol }) });
  if (state.sub === "feecap") content = renderSection({ key: "rates-feecap", title: "Fee Caps", defaultOpen: true, content: renderDataTable({ section: "cap", cols: configs.cap, rows: state.cap }) });
  if (state.sub === "surcharge") content = renderSection({
    key: "rates-surcharge",
    title: "Surcharges",
    note: "Extra percentage fees stacked on top of offline and volume pricing.",
    defaultOpen: true,
    content: renderDataTable({ section: "surch", cols: configs.surch, rows: state.surch })
  });
  if (state.sub === "revshare") content = renderSection({ key: "rates-revshare", title: "Revenue Share", defaultOpen: true, content: renderDataTable({ section: "rs", cols: configs.rs, rows: state.rs }) });
  if (state.sub === "fx") {
    content = renderSection({
      key: "rates-fx",
      title: "FX Conversion Rates",
      note: "Matched by payee currency or corridor, txn size band, and monthly volume band.",
      defaultOpen: true,
      content: `
        <div class="card" style="margin-top:0">
          <div class="section-header">
            <label class="field" style="min-width:220px">
              <span class="label">Filter Partner</span>
              <select class="select" data-bind="fxSearch">${renderOptions([{ value: "", label: "All Partners" }, ...[...new Set(state.fxRates.map((row) => row.partner))].sort()], state.fxSearch)}</select>
            </label>
          </div>
          ${renderDataTable({ section: "fxRates", cols: configs.fxRates, rows: state.fxRates, filterFn: state.fxSearch ? (row) => row.partner === state.fxSearch : null })}
        </div>
      `
    });
  }
  if (state.sub === "minimum") content = renderSection({ key: "rates-minimum", title: "Monthly Minimum", defaultOpen: true, content: renderDataTable({ section: "mins", cols: configs.mins, rows: state.mins }) });
  if (state.sub === "platform") content = renderSection({ key: "rates-platform", title: "Platform Subscription", defaultOpen: true, content: renderDataTable({ section: "plat", cols: configs.plat, rows: state.plat }) });
  if (state.sub === "reversal") content = renderSection({ key: "rates-reversal", title: "Reversal Fees", defaultOpen: true, content: renderDataTable({ section: "revf", cols: configs.revf, rows: state.revf }) });
  if (state.sub === "impl") content = renderSection({ key: "rates-impl", title: "Implementation Fees", defaultOpen: true, content: renderDataTable({ section: "impl", cols: configs.impl, rows: state.impl }) });
  if (state.sub === "vacct") content = renderSection({ key: "rates-vacct", title: "Virtual Accounts", defaultOpen: true, content: renderDataTable({ section: "vaFees", cols: configs.vaFees, rows: state.vaFees }) });
  return `<div class="stack">${tabs}${content}</div>`;
}

function renderLookerTab() {
  const configs = getTableConfigs();
  const lookerImportEnabled = isLookerImportEnabled();
  const fileOptions = getLookerFileOptionsWithStatus();
  const audit = getResolvedLookerImportAudit();
  const latestWorkflowRun = audit?.latestRunByChannel?.workflow || null;
  const latestManualRun = audit?.latestRunByChannel?.manual || null;
  const sharedImportStatus = getLookerImportConfirmation();
  const sharedImportBanner = sharedImportStatus ? `
    <div class="summary-banner success">
      <h4>${html(formatPeriodLabel(sharedImportStatus.period))} data upload loaded</h4>
      <p>
        ${sharedImportStatus.savedAt ? `Last synced ${html(formatDateTime(sharedImportStatus.savedAt))}` : "Latest shared upload is loaded."}${sharedImportStatus.coverageText ? ` · ${html(sharedImportStatus.coverageText)}` : ""}
      </p>
      <div class="tag-list" style="margin-top:10px">
        ${sharedImportStatus.changeSummary?.totalChangedGroups
          ? [
              `<span class="helper-pill">${html(`${Number(sharedImportStatus.changeSummary.totalChangedGroups || 0).toLocaleString("en-US")} changed partner-period${Number(sharedImportStatus.changeSummary.totalChangedGroups || 0) === 1 ? "" : "s"}`)}</span>`,
              Number(sharedImportStatus.changeSummary.partnerCount || 0) ? `<span class="helper-pill">${html(`${Number(sharedImportStatus.changeSummary.partnerCount || 0).toLocaleString("en-US")} partner${Number(sharedImportStatus.changeSummary.partnerCount || 0) === 1 ? "" : "s"} touched`)}</span>` : "",
              Number(sharedImportStatus.changeSummary.changedFileCount || 0) ? `<span class="helper-pill">${html(`${Number(sharedImportStatus.changeSummary.changedFileCount || 0).toLocaleString("en-US")} file${Number(sharedImportStatus.changeSummary.changedFileCount || 0) === 1 ? "" : "s"} changed`)}</span>` : ""
            ].filter(Boolean).join("")
          : Object.entries(sharedImportStatus.sectionCounts).map(([section, count]) => `<span class="helper-pill">${html(describeLookerSectionCount(section, Number(count)))}</span>`).join("")}
      </div>
    </div>
  ` : "";
  const importSummary = state.lookerImportResult ? `
    <div class="summary-banner ${state.lookerImportResult.warnings?.length ? "warning" : "success"}">
      <h4>${html(state.lookerImportResult.fileLabel)} imported for ${html(state.lookerImportResult.period)}</h4>
      <p>
        ${(state.lookerImportResult.stats?.sectionCounts
          ? summarizeLookerSectionCounts(state.lookerImportResult.stats.sectionCounts)
          : "Parser finished.")}
      </p>
      ${describeLookerRecordCoverage(state.lookerImportResult) ? `<p>${html(describeLookerRecordCoverage(state.lookerImportResult))}</p>` : ""}
      ${state.lookerImportResult.warnings?.length ? `<ul class="bulleted-list" style="margin-top:10px">${state.lookerImportResult.warnings.map((warning) => `<li>${html(warning)}</li>`).join("")}</ul>` : ""}
    </div>
  ` : "";
  return `
    <div class="stack">
      ${renderPageTableToggle("looker")}
      ${sharedImportBanner}
      ${renderSection({
        key: "looker-workflow-upload",
        title: "Workflow Upload Summary",
        note: "Shows the latest n8n / workflow-driven data upload currently loaded in the workbook.",
        content: renderUploadRunSummary(latestWorkflowRun, {
          emptyMessage: "No workflow uploads have been recorded yet.",
          summaryTitle: "Latest workflow upload overview"
        }),
        defaultOpen: false
      })}
      ${renderSection({
        key: "looker-manual-upload",
        title: "Manual Upload Summary",
        note: "Shows the latest in-app manual upload currently saved in the workbook.",
        content: renderUploadRunSummary(latestManualRun, {
          emptyMessage: "No manual uploads have been recorded yet.",
          summaryTitle: "Latest manual upload overview"
        }),
        defaultOpen: false
      })}
      ${renderSection({
        key: "looker-last-upload",
        title: "Last Upload Details",
        note: "The tables below reflect the combined data currently loaded from both manual uploads and n8n / workflow uploads.",
        content: renderUploadRunSummary(audit?.latestRun, {
          emptyMessage: "No data uploads have been recorded yet.",
          summaryTitle: "Latest loaded upload overview"
        }),
        defaultOpen: false
      })}
      <div class="card">
        <div class="section-header">
          <div>
            <h3 class="section-title">Manual Data Upload ${renderInfoTip("Select the upload source type, choose the billing month, then paste tabular data or upload the CSV/XLSX. The selected month will be replaced for that source only to avoid duplicating history.")}</h3>
          </div>
          <span class="helper-pill">${html(state.lookerImportStatus === "idle" ? "Ready" : state.lookerImportStatus === "parsing" ? "Parsing..." : state.lookerImportStatus === "success" ? "Imported" : "Error")}</span>
        </div>
        ${lookerImportEnabled ? "" : `
          <div class="summary-banner warning" style="margin-top:16px">
            <h4>Manual upload webhook not configured</h4>
            <p>Set BILLING_APP_CONFIG.lookerImportWebhookUrl to let the hosted frontend hand manual uploads off to n8n.</p>
          </div>
        `}
        <div class="field-grid" style="margin-top:16px">
          <label class="field">
            <span class="label">Upload Source File Type</span>
            <select class="select" data-bind="lookerImportType">${renderOptions(fileOptions, state.lookerImportType)}</select>
          </label>
          <label class="field">
            <span class="label">Billing Period</span>
            <input class="input" type="month" data-bind="lookerImportPeriod" value="${html(state.lookerImportPeriod)}">
          </label>
          <div class="field">
            ${renderLabelWithInfo("File Upload", "Upload a CSV or XLSX export for the selected source and billing month.")}
            <div class="button-row">
              <button class="button secondary" data-action="choose-looker-file">Choose File</button>
              ${state.lookerImportFileName ? `<span class="helper-pill">${html(state.lookerImportFileName)}</span>` : ""}
            </div>
            <input id="looker-import-file" type="file" accept=".csv,.xlsx,.xls,.txt" hidden>
          </div>
        </div>
        <div class="field" style="margin-top:16px">
          <span class="label">Paste Data Instead</span>
          <textarea class="textarea" data-bind="lookerImportText" data-bind-live="true" placeholder="Paste the exported upload table directly here if you copied it from Sheets / CSV / Looker...">${html(state.lookerImportText)}</textarea>
        </div>
        <div class="button-row" style="margin-top:14px">
          <button class="button primary" data-action="run-looker-import"${state.lookerImportStatus === "parsing" || !lookerImportEnabled ? " disabled" : ""}>Import & Replace Month</button>
          <button class="button ghost" data-action="clear-looker-import">Clear Contents</button>
          ${state.lookerImportError ? `<span class="footer-note" style="color:#a33b29">${html(state.lookerImportError)}</span>` : ""}
        </div>
        ${importSummary}
      </div>
      ${renderSection({
        key: "looker-gaps",
        title: "Import Gaps",
        note: "Shows historical upload gaps and warnings by import date so older outstanding issues remain visible.",
        content: renderHistoricalImportGaps(),
        defaultOpen: false
      })}
      ${renderSection({ key: "looker-txns", tableRowsKey: "looker-txns", pageId: "looker", title: "Transaction Data", badge: state.ltxn.length, note: `Reflects the combined loaded transaction upload data. Avg Txn $ is used for FX tier lookup.`, content: renderDataTable({ section: "ltxn", cols: configs.ltxn, rows: state.ltxn, rowsKey: "looker-txns", pageId: "looker" }) })}
      ${renderSection({ key: "looker-rev", tableRowsKey: "looker-rev", pageId: "looker", title: "Reversal Data", badge: state.lrev.length, note: "Reflects the combined loaded reversal data from workflow and manual uploads.", content: renderDataTable({ section: "lrev", cols: configs.lrev, rows: state.lrev, rowsKey: "looker-rev", pageId: "looker" }) })}
      ${renderSection({ key: "looker-rs", tableRowsKey: "looker-rs", pageId: "looker", title: "Revenue Share Summary", badge: state.lrs.length, note: "Used as the source of truth for rev-share invoice payouts when present. Reflects both manual and workflow uploads.", content: renderDataTable({ section: "lrs", cols: configs.lrs, rows: state.lrs, rowsKey: "looker-rs", pageId: "looker" }) })}
      ${renderSection({ key: "looker-va", tableRowsKey: "looker-va", pageId: "looker", title: "Virtual Account / Setup / Settlement Data", badge: state.lva.length, note: "Tracks account openings, dormancy, account closings, business setup counts, and settlement sweeps across both upload paths.", content: renderDataTable({ section: "lva", cols: configs.lva, rows: state.lva, rowsKey: "looker-va", pageId: "looker" }) })}
    </div>
  `;
}

function renderCostsTab() {
  const configs = getTableConfigs();
  return `
    <div class="stack">
      ${renderSection({
        key: "costs-provider-fees",
        title: "Provider Transaction Fees",
        badge: state.pCosts.length,
        defaultOpen: true,
        note: "Provider cost rows remain visible here so rate changes are easy to review.",
        content: `
          <div class="card" style="margin-top: 0">
            <div class="section-header">
              <label class="field" style="min-width:220px">
                <span class="label">Provider Filter</span>
                <select class="select" data-bind="cf">${renderOptions([{ value: "", label: "All" }, ...getProviderOptions()], state.cf)}</select>
              </label>
            </div>
            ${renderDataTable({ section: "pCosts", cols: configs.pCosts, rows: state.pCosts, filterFn: state.cf ? (row) => row.provider === state.cf : null })}
          </div>
        `
      })}
    </div>
  `;
}

function renderPreviewTable(rows, headers, rowRenderer, emptyMessage, rowsKey = "", pageId = "import") {
  const rowsExpanded = areTableRowsExpanded(rowsKey, pageId);
  return `
    <div class="table-wrap">
      <table class="data-table">
        <thead><tr>${headers.map((header) => `<th>${html(header)}</th>`).join("")}</tr></thead>
        <tbody>${!rows.length ? `<tr><td colspan="${headers.length}" class="empty-state">${html(emptyMessage)}</td></tr>` : !rowsExpanded ? "" : rows.map(rowRenderer).join("")}</tbody>
      </table>
    </div>
  `;
}

function renderImportTab() {
  const contractParseEnabled = isContractParseEnabled();
  const contractExtractEnabled = isContractExtractEnabled();
  const parsed = state.cParsed;
  const counts = parsed ? [
    { label: "offline rates", count: (parsed.offlineRates || []).length },
    { label: "volume rates", count: (parsed.volumeRates || []).length },
    { label: "fee caps", count: (parsed.feeCaps || []).length },
    { label: "surcharges", count: (parsed.surcharges || []).length },
    { label: "minimums", count: (parsed.minimums || []).length },
    { label: "reversal fees", count: (parsed.reversalFees || []).length },
    { label: "impl fees", count: (parsed.implFees || []).length },
    { label: "VA fees", count: (parsed.virtualAccountFees || []).length },
    { label: "other fees", count: (parsed.otherFees || []).length },
    { label: "rev share tiers", count: (parsed.revShareTiers || []).length },
    { label: "rev share fees", count: (parsed.revShareFees || []).length }
  ] : [];
  const importedCounts = state.cImportSummary ? [
    { label: "offline rates", count: state.cImportSummary.off || 0 },
    { label: "volume rates", count: state.cImportSummary.vol || 0 },
    { label: "fee caps", count: state.cImportSummary.cap || 0 },
    { label: "surcharges", count: state.cImportSummary.surch || 0 },
    { label: "minimums", count: state.cImportSummary.mins || 0 },
    { label: "reversal fees", count: state.cImportSummary.revf || 0 },
    { label: "impl fees", count: state.cImportSummary.impl || 0 },
    { label: "VA fees", count: state.cImportSummary.vaFees || 0 },
    { label: "FX rates", count: state.cImportSummary.fxRates || 0 },
    { label: "platform fees", count: state.cImportSummary.plat || 0 }
  ] : [];
  const importTargetName = state.cMode === "verify" ? state.cVerifyPartner : state.cName;
  const importTargetExists = !!importTargetName && state.ps.includes(importTargetName);

  const diffSection = state.cDiff ? `
    <div class="stack">
      <div class="grid-4">
        <div class="kpi-card success"><strong>${state.cDiff.matches}</strong><span>Matches</span></div>
        <div class="kpi-card danger"><strong>${state.cDiff.mismatches}</strong><span>Mismatches</span></div>
        <div class="kpi-card warning"><strong>${state.cDiff.missing}</strong><span>In Contract, Not Workbook</span></div>
        <div class="kpi-card info"><strong>${state.cDiff.extra}</strong><span>In Workbook, Not Contract</span></div>
      </div>
      <div class="summary-banner ${state.cDiff.mismatches === 0 && state.cDiff.missing === 0 && state.cDiff.extra === 0 ? "success" : "warning"}">
        <h4>${state.cDiff.mismatches === 0 && state.cDiff.missing === 0 && state.cDiff.extra === 0 ? "All rates match the contract" : `Issues found for ${html(state.cDiff.partner)}`}</h4>
        <p>${state.cDiff.mismatches === 0 && state.cDiff.missing === 0 && state.cDiff.extra === 0 ? `${state.cDiff.matches} rates verified.` : `${state.cDiff.mismatches} value mismatches, ${state.cDiff.missing} missing rows, ${state.cDiff.extra} extra workbook rows.`}</p>
        ${state.cImportBehavior === "append" && (state.cDiff.mismatches > 0 || state.cDiff.missing > 0 || state.cDiff.extra > 0) ? `<p style="margin-top:8px">You are using <strong>Add only new fees found</strong>. That mode preserves unmatched workbook rows on purpose, so remaining differences are expected until you switch to <strong>Override existing rows</strong>.</p>` : ""}
      </div>
      <div class="card">
        <div class="section-header compact">
          <div><h3 class="section-title">Detailed Comparison</h3></div>
        </div>
        ${renderPreviewTable(state.cDiff.results, ["Status", "Category", "Rate / Fee", "Contract Value", "Workbook Value"], (row) => `
          <tr class="row-${row.status}">
            <td><span class="status-tag status-${row.status}">${html(row.status.toUpperCase())}</span></td>
            <td>${html(row.cat)}</td>
            <td>${html(row.label)}</td>
            <td class="mono">${formatCompareValue(row.contractVal)}</td>
            <td class="mono">${formatCompareValue(row.workbookVal)}</td>
          </tr>
        `, "No comparison rows available.")}
      </div>
    </div>
  ` : "";

  const importPlanSection = state.cImportPlan && importTargetExists
    ? renderContractImportPlan(state.cImportPlan)
    : (parsed && importTargetExists
      ? `<div class="summary-banner info"><h4>Review workbook changes before pushing</h4><p>Select the existing customer and import behavior to preview the exact rows that would be added, replaced, or removed.</p></div>`
      : "");

  const previewSection = parsed && !state.cDiff ? `
    <div class="stack">
      <div class="tag-list">${counts.map((item) => `<span class="helper-pill">${item.count} ${html(item.label)}</span>`).join("")}</div>
      ${(parsed.offlineRates || []).length ? renderSection({
        key: "preview-offline",
        tableRowsKey: "preview-offline",
        pageId: "import",
        title: "Offline Rates Preview",
        badge: (parsed.offlineRates || []).length,
        content: renderPreviewTable(parsed.offlineRates || [], ["Txn Type", "Speed", "Min", "Max", "Fee", "Ccy", "Method", "Note"], (row) => `
          <tr><td>${html(row.txnType)}</td><td>${html(row.speedFlag)}</td><td class="mono">${html(row.minAmt)}</td><td class="mono">${html(row.maxAmt)}</td><td class="mono">${fmt(row.fee)}</td><td>${html(`${row.payerCcy || ""}/${row.payeeCcy || ""}`)}</td><td>${html(row.processingMethod || "")}</td><td>${html(row.note || "")}</td></tr>
        `, "No offline rates detected.", "preview-offline", "import")
      }) : ""}
      ${(parsed.volumeRates || []).length ? renderSection({
        key: "preview-volume",
        tableRowsKey: "preview-volume",
        pageId: "import",
        title: "Volume Rates Preview",
        badge: (parsed.volumeRates || []).length,
        content: renderPreviewTable(parsed.volumeRates || [], ["Txn Type", "Speed", "Rate", "Payer", "Payee", "Card", "Ccy Group", "Min Vol", "Max Vol", "Note"], (row) => `
          <tr><td>${html(row.txnType || "")}</td><td>${html(row.speedFlag || "")}</td><td class="mono">${fmtPct(row.rate)}</td><td>${html(row.payerFunding || "")}</td><td>${html(row.payeeFunding || "")}</td><td>${html(row.payeeCardType || "")}</td><td>${html(row.ccyGroup || "")}</td><td class="mono">${html(row.minVol || 0)}</td><td class="mono">${html(row.maxVol || 0)}</td><td>${html(row.note || "")}</td></tr>
        `, "No volume rates detected.", "preview-volume", "import")
      }) : ""}
      ${(parsed.minimums || []).length ? renderSection({
        key: "preview-minimums",
        tableRowsKey: "preview-minimums",
        pageId: "import",
        title: "Minimums Preview",
        badge: (parsed.minimums || []).length,
        content: renderPreviewTable(parsed.minimums || [], ["Min Amount", "Vol Lower", "Vol Upper", "Note"], (row) => `
          <tr><td class="mono">${fmt(row.minAmount)}</td><td class="mono">${html(row.minVol || 0)}</td><td class="mono">${html(row.maxVol || 0)}</td><td>${html(row.note || "")}</td></tr>
        `, "No minimums detected.", "preview-minimums", "import")
      }) : ""}
      ${(parsed.feeCaps || []).length ? renderSection({
        key: "preview-feecaps",
        tableRowsKey: "preview-feecaps",
        pageId: "import",
        title: "Fee Caps Preview",
        badge: (parsed.feeCaps || []).length,
        content: renderPreviewTable(parsed.feeCaps || [], ["Product", "Cap Type", "Amount"], (row) => `
          <tr><td>${html(row.productType || "")}</td><td>${html(row.capType || "")}</td><td class="mono">${fmt(row.amount)}</td></tr>
        `, "No fee caps detected.", "preview-feecaps", "import")
      }) : ""}
      ${(parsed.reversalFees || []).length ? renderSection({
        key: "preview-reversals",
        tableRowsKey: "preview-reversals",
        pageId: "import",
        title: "Reversal Fees Preview",
        badge: (parsed.reversalFees || []).length,
        content: renderPreviewTable(parsed.reversalFees || [], ["Funding", "Fee", "Note"], (row) => `
          <tr><td>${html(row.payerFunding || "")}</td><td class="mono">${fmt(row.feePerReversal)}</td><td>${html(row.note || "")}</td></tr>
        `, "No reversal fees detected.", "preview-reversals", "import")
      }) : ""}
      ${(parsed.implFees || []).length ? renderSection({
        key: "preview-impl",
        tableRowsKey: "preview-impl",
        pageId: "import",
        title: "Implementation Fees Preview",
        badge: (parsed.implFees || []).length,
        content: renderPreviewTable(parsed.implFees || [], ["Fee Type", "Amount", "Credit To", "Credit $", "Launch ≤ Days", "Note"], (row) => `
          <tr><td>${html(row.feeType || "")}</td><td class="mono">${fmt(row.feeAmount)}</td><td>${html(row.creditMode || "")}</td><td class="mono">${Number(row.creditAmount || 0) ? fmt(row.creditAmount) : "—"}</td><td>${Number(row.creditWindowDays || 0) || "—"}</td><td>${html(row.note || "")}</td></tr>
        `, "No implementation fees detected.", "preview-impl", "import")
      }) : ""}
      ${(parsed.platformFees || []).length ? renderSection({
        key: "preview-platform",
        tableRowsKey: "preview-platform",
        pageId: "import",
        title: "Platform Fees Preview",
        badge: (parsed.platformFees || []).length,
        content: renderPreviewTable(parsed.platformFees || [], ["Monthly Fee", "Note"], (row) => `
          <tr><td class="mono">${fmt(row.monthlyFee)}</td><td>${html(row.note || "")}</td></tr>
        `, "No platform fees detected.", "preview-platform", "import")
      }) : ""}
      ${(parsed.virtualAccountFees || []).length ? renderSection({
        key: "preview-va",
        tableRowsKey: "preview-va",
        pageId: "import",
        title: "Virtual Account Fees Preview",
        badge: (parsed.virtualAccountFees || []).length,
        content: renderPreviewTable(parsed.virtualAccountFees || [], ["Fee Type", "Min Accts", "Max Accts", "Discount", "Fee / Acct", "Note"], (row) => `
          <tr><td>${html(row.feeType || "")}</td><td class="mono">${html(row.minAccounts || 0)}</td><td class="mono">${html(row.maxAccounts || 0)}</td><td class="mono">${fmtPct(row.discount || 0)}</td><td class="mono">${fmt(row.feePerAccount)}</td><td>${html(row.note || "")}</td></tr>
        `, "No virtual account fees detected.", "preview-va", "import")
      }) : ""}
      ${(parsed.surcharges || []).length ? renderSection({
        key: "preview-surcharges",
        tableRowsKey: "preview-surcharges",
        pageId: "import",
        title: "Surcharges Preview",
        badge: (parsed.surcharges || []).length,
        content: renderPreviewTable(parsed.surcharges || [], ["Type", "Rate", "Min Vol", "Max Vol", "Note"], (row) => `
          <tr><td>${html(row.surchargeType || "")}</td><td class="mono">${fmtPct(row.rate)}</td><td class="mono">${html(row.minVol || 0)}</td><td class="mono">${html(row.maxVol || 0)}</td><td>${html(row.note || "")}</td></tr>
        `, "No surcharges detected.", "preview-surcharges", "import")
      }) : ""}
      ${(parsed.otherFees || []).length ? renderSection({
        key: "preview-otherfees",
        tableRowsKey: "preview-otherfees",
        pageId: "import",
        title: "Other Fees Preview",
        badge: (parsed.otherFees || []).length,
        note: "Preview only. These tax-document and miscellaneous fee inputs are captured for review but do not have matching workbook import tables yet.",
        content: renderPreviewTable(parsed.otherFees || [], ["Fee Type", "Rate", "Amount", "Note"], (row) => `
          <tr><td>${html(row.feeType || "")}</td><td class="mono">${row.rate != null ? fmtPct(row.rate) : ""}</td><td class="mono">${row.amount != null ? fmt(row.amount) : ""}</td><td>${html(row.note || "")}</td></tr>
        `, "No other fees detected.", "preview-otherfees", "import")
      }) : ""}
      ${(parsed.revShareTiers || []).length ? renderSection({
        key: "preview-revsharetiers",
        tableRowsKey: "preview-revsharetiers",
        pageId: "import",
        title: "Rev Share Tiers Preview",
        badge: (parsed.revShareTiers || []).length,
        note: "Preview only. Tiered rev-share schedules are shown here for review but are not auto-imported into the workbook yet.",
        content: renderPreviewTable(parsed.revShareTiers || [], ["Min Vol", "Max Vol", "Rev Share", "Note"], (row) => `
          <tr><td class="mono">${html(row.minVol || 0)}</td><td class="mono">${html(row.maxVol || 0)}</td><td class="mono">${fmtPct(row.revSharePct)}</td><td>${html(row.note || "")}</td></tr>
        `, "No revenue-share tiers detected.", "preview-revsharetiers", "import")
      }) : ""}
      ${(parsed.revShareFees || []).length ? renderSection({
        key: "preview-revsharefees",
        tableRowsKey: "preview-revsharefees",
        pageId: "import",
        title: "Rev Share Fee Inputs Preview",
        badge: (parsed.revShareFees || []).length,
        note: "Preview only. These rev-share fee inputs are captured for review but do not have matching workbook import tables yet.",
        content: renderPreviewTable(parsed.revShareFees || [], ["Fee Type", "Rate", "Amount", "Note"], (row) => `
          <tr><td>${html(row.feeType || "")}</td><td class="mono">${row.rate != null ? fmtPct(row.rate) : ""}</td><td class="mono">${row.amount != null ? fmt(row.amount) : ""}</td><td>${html(row.note || "")}</td></tr>
        `, "No revenue-share fee inputs detected.", "preview-revsharefees", "import")
      }) : ""}
    </div>
  ` : "";

  return `
    <div class="stack">
      ${renderPageTableToggle("import")}
      <div class="mode-tabs">
        <button class="tab-button${state.cMode === "import" ? " is-active" : ""}" data-action="set-contract-mode" data-mode="import">Import New Partner</button>
        <button class="tab-button${state.cMode === "verify" ? " is-active" : ""}" data-action="set-contract-mode" data-mode="verify">Update Existing Customer</button>
      </div>
      <div class="card">
        <div class="section-header">
          <div>
            <h3 class="section-title">${state.cMode === "verify" ? "Update Existing Customer Against Workbook" : "Upload Contract PDF or Paste Contract Text / JSON"} ${renderInfoTip(state.cMode === "verify" ? "Upload the whole contract PDF or paste contract text / JSON, then select the existing workbook customer to compare and update against." : "Upload the whole contract PDF to extract text into the parser, or paste contract text or structured JSON directly if you already have it.")}</h3>
          </div>
          <div class="tag-list">
            <span class="helper-pill">${html(state.cStatus === "idle" ? "Ready" : state.cStatus === "parsing" ? "Parsing..." : state.cStatus === "success" ? "Parsed" : "Error")}</span>
            ${state.cExtractStatus !== "idle" ? `<span class="helper-pill">${html(state.cExtractStatus === "parsing" ? "Extracting PDF..." : state.cExtractStatus === "success" ? "PDF Loaded" : "PDF Error")}</span>` : ""}
          </div>
        </div>
        ${contractExtractEnabled && contractParseEnabled ? "" : `
          <div class="summary-banner warning" style="margin-bottom:16px">
            <h4>Contract automation is only partially configured</h4>
            <p>${html([
              !contractExtractEnabled ? "PDF extraction needs BILLING_APP_CONFIG.contractExtractWebhookUrl." : "",
              !contractParseEnabled ? "Raw contract text parsing needs BILLING_APP_CONFIG.contractParseWebhookUrl." : "",
              "Structured JSON paste still works in the browser."
            ].filter(Boolean).join(" "))}</p>
          </div>
        `}
        <div class="button-row" style="margin-bottom:14px">
          <button class="button secondary" data-action="choose-contract-file">Choose PDF</button>
          <button class="button primary" data-action="extract-contract-file"${state.cPendingFile && (contractExtractEnabled || (state.cPendingFile?.type || "").startsWith("text/") || /\.txt$/i.test(state.cPendingFile?.name || "")) ? "" : " disabled"}>${state.cExtractStatus === "parsing" ? "Loading PDF..." : "Load PDF Text"}</button>
          ${(state.cPendingFile || state.cFileName) ? `<span class="helper-pill">${html(state.cPendingFile?.name || state.cFileName)}</span>` : `${renderInfoTip("PDF or text file upload supported.")}`}
          ${state.cDetectedIncremental ? `<span class="helper-pill" style="background:#dff4e7;color:#1c5d3c">Per-tier marginal pricing detected</span>` : ""}
          <input id="contract-file-upload" type="file" accept=".pdf,.txt,text/plain,application/pdf" hidden>
        </div>
        <textarea class="textarea" data-bind="cText" data-bind-live="true" placeholder="Uploaded contract text, pasted contract text, or extracted pricing JSON will appear here...">${html(state.cText)}</textarea>
        <div class="button-row" style="margin-top:14px">
          <button class="button primary" data-action="parse-contract">Parse Contract</button>
          <button class="button secondary" data-action="copy-extraction-prompt">Copy Extraction Prompt</button>
          <button class="button ghost" data-action="clear-contract-import">Clear Contents</button>
          <span class="footer-note">${html(String((state.cText || "").length))} chars</span>
          ${state.cError ? `<span class="footer-note" style="color:#a33b29">${html(state.cError)}</span>` : ""}
        </div>
      </div>
      ${parsed ? `
        <div class="card">
          <div class="section-header">
            <div>
              <div class="section-title-row">
                <span class="label">${state.cMode === "verify" ? "Update Existing Customer" : "Partner Name"}</span>
                ${state.cMode === "verify"
                  ? `<select class="select" style="min-width:260px; max-width:360px" data-bind="cVerifyPartner">${renderOptions([{ value: "", label: "Select existing partner..." }, ...getPartnerOptions({ includeArchived: true, includeArchivedTag: false })], state.cVerifyPartner)}</select>`
                  : `<input class="input" style="min-width:260px; max-width:360px" value="${html(state.cName)}" data-bind="cName">`
                }
                ${state.cMode === "verify" && state.cVerifyPartner ? `<span class="helper-pill">Will compare against workbook</span>` : ""}
                ${state.cMode === "verify" && !state.cVerifyPartner ? `<span class="helper-pill" style="background:#f7edc8;color:#7c5312">Select the workbook customer to update</span>` : ""}
                ${state.cMode === "import" && state.cName && state.ps.includes(state.cName) ? `<span class="helper-pill" style="background:#f7edc8;color:#7c5312">Existing partner</span>` : ""}
                ${state.cMode === "import" && state.cName && !state.ps.includes(state.cName) ? `<span class="helper-pill" style="background:#dff4e7;color:#1c5d3c">New partner</span>` : ""}
                ${state.cMode === "import" && state.cDetectedIncremental ? `<span class="helper-pill" style="background:#dff4e7;color:#1c5d3c">Will auto-enable per-tier marginal pricing</span>` : ""}
              </div>
              ${importTargetExists ? `
                <div class="field" style="margin-top:12px; max-width:520px">
                  ${renderLabelWithInfo("Import Behavior", "Override existing rows replaces this partner's current contract-priced rows with the parsed contract and is the exact-sync option. Add only new fees found keeps existing rows and adds only fee lines that do not already exist, so Verify may still show differences afterward.")}
                  <select class="select" data-bind="cImportBehavior">
                    ${renderOptions([
                      { value: "override", label: "Override existing rows" },
                      { value: "append", label: "Add only new fees found" }
                    ], state.cImportBehavior)}
                  </select>
                </div>
              ` : ""}
              <div class="section-note">${state.cMode === "verify" ? `Parsed contract customer: ${html(state.cName || "n/a")} · ` : ""}Effective: ${html(parsed.effectiveDate || "n/a")} · ${html(parsed.billingTerms?.billingFreq || "n/a")} · ${html(parsed.billingTerms?.payBy || "n/a")}</div>
            </div>
            <div class="button-row">
              ${state.cMode === "verify"
                ? `
                  <button class="button primary" data-action="verify-contract"${!state.cVerifyPartner ? " disabled" : ""}>Verify Against Workbook</button>
                  <button class="button success" data-action="import-contract"${!state.cVerifyPartner || !countSelectedContractImportChanges() ? " disabled" : ""}>Update Workbook</button>
                `
                : state.cImported
                  ? `<span class="helper-pill" style="background:#dff4e7;color:#1c5d3c">Imported</span>`
                  : `<button class="button success" data-action="import-contract"${!state.cName ? " disabled" : ""}>Import to Workbook</button>`
              }
            </div>
          </div>
          ${(parsed.warnings || []).length ? `<div class="summary-banner warning"><h4>Parser Notes</h4><ul class="bulleted-list" style="margin-top:10px">${(parsed.warnings || []).map((warning) => `<li>${html(warning)}</li>`).join("")}</ul></div>` : ""}
          ${state.cImported ? `<div class="summary-banner success"><h4>Successfully imported ${html(importTargetName || state.cName)}</h4><p>${importedCounts.filter((item) => item.count > 0).map((item) => `${item.count} ${item.label}`).join(" · ") || "No workbook rows were imported."}</p></div>` : ""}
        </div>
      ` : ""}
      ${diffSection}
      ${importPlanSection}
      ${previewSection}
    </div>
  `;
}

function renderAdminTab() {
  const logs = Array.isArray(state.accessLogs) ? state.accessLogs : [];
  const configurableTabs = allMainTabs.filter((tab) => tab.id !== "admin");
  const adminView = state.adminView || "overview";
  const logFilter = state.adminLogFilter || "changes";
  const filteredLogs = logs.filter((row) => {
    if (logFilter === "all") return true;
    if (logFilter === "changes") return row.category === "change";
    if (logFilter === "guest") return row.actorRole === "guest";
    if (logFilter === "admin") return row.actorRole === "admin";
    return row.category === logFilter;
  });
  const summary = logs.reduce((acc, row) => {
    if (row.actorRole === "guest" && row.action === "guest_login") acc.guestLogins += 1;
    if (row.action === "admin_login") acc.adminLogins += 1;
    if (row.action === "admin_access_requested") acc.deniedAttempts += 1;
    if (row.action === "calculate_invoice") acc.invoiceRuns += 1;
    if (row.category === "change") acc.changeEvents += 1;
    return acc;
  }, { guestLogins: 0, adminLogins: 0, deniedAttempts: 0, invoiceRuns: 0, changeEvents: 0 });

  return `
    <div class="stack">
      <div class="sub-tabs">
        <button class="tab-button${adminView === "overview" ? " is-active" : ""}" data-action="set-admin-view" data-view="overview">Overview</button>
        <button class="tab-button${adminView === "logs" ? " is-active" : ""}" data-action="set-admin-view" data-view="logs">Logs</button>
      </div>
      <div class="grid-4">
        <div class="kpi-card"><strong>${logs.length}</strong><span>Tracked Activities</span></div>
        <div class="kpi-card"><strong>${summary.guestLogins}</strong><span>Guest Logins</span></div>
        <div class="kpi-card"><strong>${summary.deniedAttempts}</strong><span>Blocked Admin Attempts</span></div>
        <div class="kpi-card"><strong>${summary.changeEvents}</strong><span>Workbook Changes Logged</span></div>
      </div>
      ${adminView === "overview" ? `
        <div class="card">
          <div class="section-header compact">
            <div>
              <h3 class="section-title">Admin Access Rules</h3>
              <p class="section-note">Choose which pages guest users can access. Admin Portal always stays locked to admin users.</p>
            </div>
          </div>
          <div class="table-wrap">
            <table class="data-table">
              <thead>
                <tr>
                  <th>Page</th>
                  <th>Guest Access</th>
                  <th>Status</th>
                </tr>
              </thead>
              <tbody>
                ${configurableTabs.map((tab) => {
                  const allowed = getAdminSettings().guestAllowedTabs.includes(tab.id);
                  return `
                    <tr>
                      <td>${html(tab.label)}</td>
                      <td><input type="checkbox" data-action="update-guest-tab-access" data-tab="${tab.id}" ${allowed ? "checked" : ""}></td>
                      <td>${allowed ? `<span class="helper-pill">Guest can access</span>` : `<span class="helper-pill" style="background:#f7edc8;color:#7c5312">Admin only</span>`}</td>
                    </tr>
                  `;
                }).join("")}
              </tbody>
            </table>
          </div>
        </div>
        <div class="card">
          <div class="section-header compact">
            <div>
              <h3 class="section-title">Recent Activity Preview</h3>
              <p class="section-note">Latest login, invoice, and change events. Open the Logs page for the full audit trail.</p>
            </div>
          </div>
          <div class="table-wrap">
            <table class="data-table">
              <thead>
                <tr>
                  <th>When</th>
                  <th>User</th>
                  <th>Role</th>
                  <th>Category</th>
                  <th>Detail</th>
                </tr>
              </thead>
              <tbody>
                ${!logs.length ? `<tr><td colspan="5" class="empty-state">No access activity has been logged yet.</td></tr>` : logs.slice(0, 15).map((row) => `
                  <tr>
                    <td>${html(formatDateTime(row.timestamp))}</td>
                    <td>${html(row.actorName || "")}</td>
                    <td>${html((row.actorRole || "").toUpperCase())}</td>
                    <td>${html(row.category || "activity")}</td>
                    <td>${html(row.detail || "")}</td>
                  </tr>
                `).join("")}
              </tbody>
            </table>
          </div>
        </div>
      ` : `
        <div class="card">
          <div class="section-header compact">
            <div>
              <h3 class="section-title">Activity Logs</h3>
              <p class="section-note">Tracks every workbook change plus guest and admin activity. Each row records who performed the action and what changed.</p>
            </div>
          </div>
          <div class="admin-log-toolbar">
            <label class="field admin-log-filter-field">
              <span class="label">Log Filter</span>
              <select class="select" data-bind="adminLogFilter">
                ${renderOptions([
                  { value: "changes", label: "Workbook Changes" },
                  { value: "all", label: "All Activity" },
                  { value: "admin", label: "Admin Activity" },
                  { value: "guest", label: "Guest Activity" },
                  { value: "access", label: "Access Events" },
                  { value: "security", label: "Security Events" }
                ], logFilter)}
              </select>
            </label>
            <div class="helper-pill admin-log-pill">${filteredLogs.length} matching log row${filteredLogs.length === 1 ? "" : "s"}</div>
            <div class="helper-pill admin-log-pill">${summary.invoiceRuns} invoice run${summary.invoiceRuns === 1 ? "" : "s"} logged</div>
          </div>
          <div class="table-wrap">
            <table class="data-table">
              <thead>
                <tr>
                  <th>When</th>
                  <th>User</th>
                  <th>Role</th>
                  <th>Category</th>
                  <th>Action</th>
                  <th>Detail</th>
                  <th>Tab</th>
                </tr>
              </thead>
              <tbody>
                ${!filteredLogs.length ? `<tr><td colspan="7" class="empty-state">No log rows match this filter yet.</td></tr>` : filteredLogs.slice(0, 250).map((row) => `
                  <tr>
                    <td>${html(formatDateTime(row.timestamp))}</td>
                    <td>${html(row.actorName || "")}</td>
                    <td>${html((row.actorRole || "").toUpperCase())}</td>
                    <td>${html(row.category || "activity")}</td>
                    <td>${html(row.action || "")}</td>
                    <td>${html(row.detail || "")}</td>
                    <td>${html(row.tab || "")}</td>
                  </tr>
                `).join("")}
              </tbody>
            </table>
          </div>
        </div>
      `}
    </div>
  `;
}

function renderTabContent() {
  if (state.tab === "invoice") return renderInvoiceTab();
  if (state.tab === "partner") return renderPartnerTab();
  if (state.tab === "rates") return renderRatesTab();
  if (state.tab === "looker") return renderLookerTab();
  if (state.tab === "costs") return renderCostsTab();
  if (state.tab === "admin") return renderAdminTab();
  return renderImportTab();
}

function renderToast() {
  if (!state.toast) return "";
  return `<div class="toast ${html(state.toast.tone)}"><strong>${html(state.toast.title)}</strong><div>${html(state.toast.message)}</div></div>`;
}

function render() {
  state.adminSettings = buildDefaultAdminSettings(state.adminSettings);
  if (!allMainTabs.some((tab) => tab.id === state.tab)) {
    state.tab = "invoice";
  }
  if (hasAccessSession() && !isTabAccessible(state.tab)) {
    state.tab = "invoice";
  }
  if (!hasAccessSession()) {
    root.innerHTML = renderAccessGate({ overlay: false });
    return;
  }
  monthlyInvoiceCache.clear();
  root.innerHTML = `
    <div class="app-shell">
      ${renderHeader()}
      <main class="workspace">
        ${renderMainTabs()}
        ${state.tab === "partner" ? renderPartnerStrip() : ""}
        ${renderTabContent()}
      </main>
      ${renderToast()}
      ${state.authOverlayOpen ? renderAccessGate({ overlay: true }) : ""}
    </div>
  `;
}

function addRow(section) {
  const configs = getTableConfigs();
  const cols = configs[section];
  if (!cols) return;
  const row = { id: uid() };
  cols.forEach((col) => {
    row[col.key] = defaultValueForColumn(col);
  });
  if (section === "pActive") {
    row.partner = state.pv || row.partner;
    row.status = "Active";
  }
  state[section] = [...state[section], row];
  persistAndRender();
  logWorkbookChange("add_row", `Added ${describeSectionLabel(section)} row: ${describeSectionRow(section, row)}.`, { section, rowId: row.id, partner: row.partner || "" });
}

function updateRow(section, id, key, value) {
  const existing = (state[section] || []).find((row) => String(row.id) === String(id)) || null;
  state[section] = state[section].map((row) => (String(row.id) === String(id) ? { ...row, [key]: value } : row));
  persistAndRender();
  const next = (state[section] || []).find((row) => String(row.id) === String(id)) || existing;
  logWorkbookChange(
    "update_row",
    `Updated ${describeSectionLabel(section)} row ${describeSectionRow(section, next)}: ${key} ${formatLogValue(existing?.[key])} → ${formatLogValue(value)}.`,
    { section, rowId: id, partner: next?.partner || existing?.partner || "", field: key }
  );
}

function deleteRow(section, id) {
  const existing = (state[section] || []).find((row) => String(row.id) === String(id)) || null;
  state[section] = state[section].filter((row) => String(row.id) !== String(id));
  persistAndRender();
  logWorkbookChange("delete_row", `Deleted ${describeSectionLabel(section)} row ${describeSectionRow(section, existing || { id })}.`, { section, rowId: id, partner: existing?.partner || "" });
}

root.addEventListener("click", (event) => {
  const target = event.target.closest("[data-action]");
  if (!target) return;
  const action = target.dataset.action;

  if (action === "open-admin-login") {
    openAuthGate("admin", { overlay: hasAccessSession() });
    render();
    return;
  }
  if (action === "open-admin-portal") {
    if (!canAdminEdit()) {
      promptAdminAccess("Admin login is required to open the admin portal.");
      return;
    }
    state.tab = "admin";
    recordAccessActivity("open_admin_portal", "Opened the admin portal.", { category: "access" });
    render();
    return;
  }
  if (action === "logout-session") {
    logoutAccessSession();
    return;
  }
  if (action === "open-auth-admin") {
    state.authView = "admin";
    state.authError = "";
    render();
    return;
  }
  if (action === "open-auth-guest") {
    state.authView = "guest";
    state.authError = "";
    render();
    return;
  }
  if (action === "open-auth-choice") {
    state.authView = "choice";
    state.authError = "";
    render();
    return;
  }
  if (action === "submit-admin-login") {
    beginAdminSession();
    return;
  }
  if (action === "submit-guest-login") {
    beginGuestSession();
    return;
  }
  if (action === "close-auth-overlay") {
    state.authOverlayOpen = false;
    state.authError = "";
    state.authView = "choice";
    render();
    return;
  }
  if (action === "set-admin-view") {
    state.adminView = target.dataset.view === "logs" ? "logs" : "overview";
    render();
    return;
  }
  if (action === "update-guest-tab-access") {
    const tabId = target.dataset.tab || "";
    setGuestTabAccess(tabId, !getAdminSettings().guestAllowedTabs.includes(tabId));
    return;
  }

  if (action === "set-tab") {
    const tabId = target.dataset.tab || "";
    if (isAdminLockedTab(tabId)) {
      promptAdminAccess("That page is locked to admin users.");
      return;
    }
    state.tab = tabId;
    render();
    if (state.tab === "looker") {
      void maybeAutoRefreshSharedWorkspace({ force: true });
    }
    return;
  }
  if (ADMIN_ONLY_ACTIONS.has(action) && !canAdminEdit()) {
    promptAdminAccess("This action is locked to admin users.");
    return;
  }
  if (action === "select-partner-chip") {
    state.pv = target.dataset.partner || "";
    state.confirmDel = false;
    render();
    return;
  }
  if (action === "set-subtab") {
    state.sub = target.dataset.sub;
    render();
    return;
  }
  if (action === "set-contract-mode") {
    state.cMode = target.dataset.mode;
    state.cDiff = null;
    if (state.cMode === "verify") {
      state.cVerifyPartner = state.cVerifyPartner || suggestVerifyPartnerName(state.cName);
    }
    refreshContractImportPlan();
    render();
    return;
  }
  if (action === "select-all-contract-changes") {
    setAllContractImportRowsSelected(true);
    render();
    return;
  }
  if (action === "deselect-all-contract-changes") {
    setAllContractImportRowsSelected(false);
    render();
    return;
  }
  if (action === "toggle-section") {
    const key = target.dataset.key;
    const defaultOpen = target.dataset.defaultOpen !== "false";
    state.openSections[key] = !isSectionOpen(key, defaultOpen);
    render();
    return;
  }
  if (action === "toggle-section-rows") {
    const rowsKey = target.dataset.rowsKey || "";
    const pageId = target.dataset.page || state.tab;
    if (!rowsKey) return;
    state.tableRowsExpanded[rowsKey] = !areTableRowsExpanded(rowsKey, pageId);
    render();
    return;
  }
  if (action === "add-partner") {
    const name = String(readBoundValue("np") || "").trim();
    if (name && !state.ps.includes(name)) {
      state.ps = [...state.ps, name];
      state.np = "";
      persistAndRender();
      logWorkbookChange("add_partner", `Added partner ${name} to the workbook.`, { partner: name, section: "partner" });
    }
    return;
  }
  if (action === "reset-defaults") {
    if (window.confirm("Reset all workbook data to defaults? This removes your local changes.")) resetToDefaults();
    return;
  }
  if (action === "refresh-shared-workspace") {
    void refreshSharedWorkspace({ showSuccessToast: true, showErrorToast: true, retries: 5, retryDelayMs: 800 });
    return;
  }
  if (action === "calculate-invoice") {
    recordAccessActivity(
      "calculate_invoice",
      `Ran invoice for ${state.sp || "all partners"} ${state.useDateRange ? `from ${state.perStart || ""} to ${state.perEnd || ""}` : `for ${state.perStart || ""}`}.`,
      { category: "activity" }
    );
    void calculateInvoice().catch((error) => {
      console.error("Unhandled invoice calculation error", error);
      showToast("Invoice calculation failed", String(error?.message || error || "Unknown error"), "error");
    });
    return;
  }
  if (action === "run-billing-checker") {
    void runBillingChecker();
    return;
  }
  if (action === "clear-billing-checker") {
    state.checkerReport = null;
    state.checkerStatus = "idle";
    state.checkerError = "";
    render();
    return;
  }
  if (action === "clear-invoice-selection") {
    clearCurrentInvoiceSelection();
    return;
  }
  if (action === "toggle-date-range") {
    state.useDateRange = !state.useDateRange;
    if (!state.useDateRange) state.perEnd = state.perStart;
    render();
    return;
  }
  if (action === "toggle-page-table-rows") {
    const pageId = target.dataset.page || state.tab;
    const nextExpanded = !arePageTableRowsExpanded(pageId);
    state.pageTableRowsExpanded[pageId] = nextExpanded;
    getCurrentPageTableRowsKeys(pageId).forEach((key) => {
      state.tableRowsExpanded[key] = nextExpanded;
    });
    const sectionStateKeys = new Set([
      ...getCurrentPageSectionStateKeys(pageId),
      ...Array.from(root.querySelectorAll(".section-toggle[data-key]")).map((button) => button.dataset.key).filter(Boolean)
    ]);
    sectionStateKeys.forEach((key) => {
      state.openSections[key] = nextExpanded;
    });
    render();
    return;
  }
  if (action === "toggle-invoice-group") {
    const key = invoiceGroupSectionKey(target.dataset.groupId);
    state.openSections[key] = !isSectionOpen(key);
    render();
    return;
  }
  if (action === "open-invoice-explorer") {
    void exportInvoiceTransactions(target.dataset.scope === "matching" ? "matching" : "all", target.dataset.groupId || "").catch((error) => {
      showToast("Export failed", String(error.message || error), "error");
    });
    return;
  }
  if (action === "export-invoice-period-transactions") {
    recordAccessActivity("export_invoice_transactions", `Exported period transactions for ${state.sp || "all partners"}.`, { category: "activity" });
    void exportInvoiceTransactions("all", "").catch((error) => {
      showToast("Export failed", String(error.message || error), "error");
    });
    return;
  }
  if (action === "export-invoice-pdf") {
    recordAccessActivity("export_invoice_pdf", `Exported ${target.dataset.docKind || "invoice"} PDF for ${state.sp || "unknown partner"}.`, { category: "activity" });
    exportInvoicePdf(target.dataset.docKind || "");
    return;
  }
  if (action === "generate-private-invoice-link") {
    void generateInvoicePrivateLinkForCurrentSelection();
    return;
  }
  if (action === "copy-private-invoice-link") {
    copyPrivateInvoiceLinkToClipboard();
    return;
  }
  if (action === "toggle-delete-partner") {
    state.confirmDel = true;
    render();
    return;
  }
  if (action === "archive-partner") {
    if (state.pv) archivePartner(state.pv);
    return;
  }
  if (action === "unarchive-partner") {
    if (state.pv) unarchivePartner(state.pv);
    return;
  }
  if (action === "cancel-delete-partner") {
    state.confirmDel = false;
    render();
    return;
  }
  if (action === "confirm-delete-partner") {
    if (state.pv) deletePartner(state.pv);
    return;
  }
  if (action === "toggle-incremental") {
    if (state.pv) {
      const nextValue = !state.pConfig[state.pv];
      state.pConfig = { ...state.pConfig, [state.pv]: nextValue };
      persistAndRender();
      logWorkbookChange("toggle_incremental_pricing", `${state.pv} per-tier marginal pricing ${nextValue ? "enabled" : "disabled"}.`, { partner: state.pv, section: "partner" });
    }
    return;
  }
  if (action === "add-row") {
    addRow(target.dataset.section);
    return;
  }
  if (action === "choose-looker-file") {
    const input = root.querySelector("#looker-import-file");
    if (input) input.click();
    return;
  }
  if (action === "choose-contract-file") {
    const input = root.querySelector("#contract-file-upload");
    if (input) input.click();
    return;
  }
  if (action === "run-looker-import") {
    void submitLookerImport();
    return;
  }
  if (action === "extract-contract-file") {
    void extractContractFile();
    return;
  }
  if (action === "clear-looker-import") {
    clearLookerImportContents();
    return;
  }
  if (action === "clear-contract-import") {
    clearContractImportContents();
    return;
  }
  if (action === "delete-row") {
    deleteRow(target.dataset.section, target.dataset.id);
    return;
  }
  if (action === "parse-contract") {
    void parseContract();
    return;
  }
  if (action === "copy-extraction-prompt") {
    copyExtractionPrompt();
    return;
  }
  if (action === "import-contract") {
    importToWorkbook();
    return;
  }
  if (action === "verify-contract") {
    verifyContract();
    return;
  }
  if (action === "export-backup") {
    downloadJson(`billing-workbook-backup-${new Date().toISOString().slice(0, 10)}.json`, exportSnapshot());
    return;
  }
  if (action === "import-backup") {
    const input = root.querySelector("#backup-file");
    if (input) input.click();
    return;
  }
});

root.addEventListener("input", (event) => {
  const target = event.target;
  if (target.matches("[data-bind-live]")) {
    state[target.dataset.bind] = target.type === "checkbox" ? !!target.checked : target.value;
    if (target.dataset.bind === "cText") {
      state.cDetectedIncremental = detectPerTierMarginalPricing(target.value);
    }
  }
});

root.addEventListener("mouseover", (event) => {
  const tip = event.target.closest(".info-tip");
  if (!tip || !root.contains(tip)) return;
  positionInfoTip(tip);
});

root.addEventListener("focusin", (event) => {
  const tip = event.target.closest(".info-tip");
  if (!tip || !root.contains(tip)) return;
  positionInfoTip(tip);
});

document.addEventListener("visibilitychange", () => {
  if (document.visibilityState !== "visible") return;
  if (state.tab !== "looker") return;
  void maybeAutoRefreshSharedWorkspace();
});

window.addEventListener("focus", () => {
  if (state.tab !== "looker") return;
  void maybeAutoRefreshSharedWorkspace();
});

setInterval(() => {
  if (document.visibilityState !== "visible") return;
  if (state.tab !== "looker") return;
  void maybeAutoRefreshSharedWorkspace();
}, SHARED_WORKSPACE_AUTO_REFRESH_MS);

root.addEventListener("change", (event) => {
  const target = event.target;

  if (
    !canAdminEdit() &&
    (
      target.matches("#backup-file")
      || (
        !isTabAccessible(state.tab) && (
          target.matches("#looker-import-file")
          || target.matches("#contract-file-upload")
          || target.matches("[data-action='update-partner-billing']")
          || target.matches("[data-action='update-invoice-date']")
          || target.matches("[data-action='toggle-invoice-paid']")
          || target.matches("[data-action='update-invoice-amount-paid']")
          || target.matches("[data-action='toggle-contract-change']")
          || target.matches("[data-section][data-id][data-key]")
        )
      )
    )
  ) {
    promptAdminAccess("This section is locked to admin users.");
    render();
    return;
  }

  if (target.matches("#backup-file")) {
    const file = target.files?.[0];
    if (!file) return;
    const reader = new FileReader();
    reader.onload = () => {
      try {
        const parsed = JSON.parse(String(reader.result || "{}"));
        restoreSnapshot(parsed);
        showToast("Backup restored", "Workbook data was imported into local storage.", "success");
      } catch (error) {
        showToast("Import failed", "That file was not valid workbook JSON.", "error");
      } finally {
        target.value = "";
      }
    };
    reader.readAsText(file);
    return;
  }

  if (target.matches("#looker-import-file")) {
    const file = target.files?.[0];
    state.lookerImportPendingFile = file || null;
    state.lookerImportFileName = file?.name || "";
    state.lookerImportError = "";
    state.lookerImportResult = null;
    state.lookerImportStatus = "idle";
    render();
    return;
  }

  if (target.matches("#contract-file-upload")) {
    const file = target.files?.[0];
    state.cPendingFile = file || null;
    state.cFileName = file?.name || state.cFileName;
    state.cExtractStatus = "idle";
    state.cError = "";
    render();
    return;
  }

  if (target.matches("[data-action='update-partner-billing']")) {
    const partner = target.dataset.partner;
    const key = target.dataset.key;
    if (!partner || !key) return;
    const existing = getPartnerBillingConfig(partner);
    let value = target.type === "checkbox" ? !!target.checked : target.value;
    if (key === "dueDays") value = value === "" ? 0 : Math.max(0, Math.floor(Number(value)));
    if (key === "billingDay") value = value === "" ? "" : Math.min(31, Math.max(1, Math.floor(Number(value))));
    if (key === "lateFeePercentMonthly") value = value === "" ? 0 : Math.max(0, Number(value));
    if (key === "lateFeeStartDays" || key === "serviceSuspensionDays") value = value === "" ? 0 : Math.max(0, Math.floor(Number(value)));
    const patch = { [key]: value };
    if (key === "payBy" && !(Number(existing?.dueDays || 0) > 0)) {
      patch.dueDays = parseDueDaysFromPayBy(value);
    }
    upsertPartnerBilling(partner, patch);
    return;
  }

  if (target.matches("[data-action='update-invoice-date']")) {
    const partner = target.dataset.partner;
    const period = target.dataset.period;
    const kind = target.dataset.kind || "receivable";
    if (!partner || !period) return;
    upsertInvoiceTrackingRecord(partner, period, kind, { invoiceDate: normalizeIsoDate(target.value) || "" });
    return;
  }

  if (target.matches("[data-action='toggle-invoice-paid']")) {
    const partner = target.dataset.partner;
    const period = target.dataset.period;
    const kind = target.dataset.kind || "receivable";
    if (!partner || !period) return;
    const existing = getInvoiceTrackingRecord(partner, period, kind);
    const amountDue = roundCurrency(Number(target.dataset.amountDue || 0));
    const checked = !!target.checked;
    upsertInvoiceTrackingRecord(partner, period, kind, {
      paid: checked,
      amountPaid: checked ? amountDue : roundCurrency(Number(existing?.amountPaid || 0))
    });
    return;
  }

  if (target.matches("[data-action='update-invoice-amount-paid']")) {
    const partner = target.dataset.partner;
    const period = target.dataset.period;
    const kind = target.dataset.kind || "receivable";
    if (!partner || !period) return;
    const amountPaid = target.value === "" ? 0 : Math.max(0, Number(target.value));
    const amountDue = roundCurrency(Number(target.dataset.amountDue || 0));
    upsertInvoiceTrackingRecord(partner, period, kind, {
      paid: amountDue > 0 && amountPaid >= amountDue - 0.005,
      amountPaid: roundCurrency(amountPaid)
    });
    return;
  }

  if (target.matches("[data-action='toggle-contract-change']")) {
    const changeId = target.dataset.changeId || "";
    if (!changeId) return;
    state.cSelectedImportRows = {
      ...state.cSelectedImportRows,
      [changeId]: !!target.checked
    };
    render();
    return;
  }

  if (target.matches("[data-bind]")) {
    setBoundValue(target.dataset.bind, target.type === "checkbox" ? !!target.checked : target.value);
    if (target.dataset.bind === "pv") state.confirmDel = false;
    if (target.dataset.bind === "cName" || target.dataset.bind === "cVerifyPartner") {
      const selectedName = target.dataset.bind === "cVerifyPartner" ? state.cVerifyPartner : state.cName;
      if (selectedName && state.ps.includes(selectedName)) state.cImportBehavior = "override";
      state.cDiff = null;
      refreshContractImportPlan();
    }
    if (target.dataset.bind === "cImportBehavior") {
      state.cDiff = null;
      refreshContractImportPlan();
    }
    if (target.dataset.bind === "perStart" && !state.useDateRange) state.perEnd = target.value;
    if (target.dataset.bind === "perEnd" && !state.useDateRange) state.perEnd = state.perStart;
    if (target.dataset.bind === "lookerImportType" || target.dataset.bind === "lookerImportPeriod") {
      state.lookerImportError = "";
      state.lookerImportResult = null;
      state.lookerImportStatus = "idle";
    }
    if (target.dataset.bind === "sp") {
      if (!state.sp) {
        clearCurrentInvoiceSelection();
        return;
      }
      state.inv = null;
      state.invoiceExplorer = null;
      render();
      return;
    }
    render();
    return;
  }

  if (target.matches("[data-section][data-id][data-key]")) {
    const { section, id, key, fieldType } = target.dataset;
    let value;
    if (fieldType === "bool") value = !!target.checked;
    else if (fieldType === "number") value = target.value === "" ? 0 : Number(target.value);
    else value = target.value;
    updateRow(section, id, key, value);
  }
});

async function initApp() {
  await loadState();
  render();
}

window.addEventListener("error", (event) => {
  renderFatalAppError(event.error || event.message);
});

window.addEventListener("unhandledrejection", (event) => {
  renderFatalAppError(event.reason);
});

void initApp().catch((error) => {
  renderFatalAppError(error);
});

// Named exports for the test harness (tests/backtest.mjs, tests/batch-validate.mjs).
// app.js runs as `<script type="module">` in the browser, so these exports are
// inert there — the browser still evaluates the top-level code including
// initApp(). Node harnesses stub document/window before importing and drive
// the calc via calculateLocalInvoiceForPeriod.
export { state, calculateLocalInvoiceForPeriod };

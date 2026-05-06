const PARTNER_PATTERNS = [
  ["altpay", "Altpay"],
  ["athena", "Athena"],
  ["bhn", "BHN"],
  ["capi", "Capi"],
  ["cellpay", "Cellpay"],
  ["clearshift", "Clearshift"],
  ["everflow", "Everflow"],
  ["factura", "Factura"],
  ["finastra", "Finastra"],
  ["goldstack", "Goldstack"],
  ["fulfil", "Fulfil"],
  ["gme_remit", "GME_Remit"],
  ["gme remit", "GME_Remit"],
  ["gmeremit", "GME_Remit"],
  ["graph finance", "Graph Finance"],
  ["graph", "Graph Finance"],
  ["halorecruiting", "Halorecruiting"],
  ["jazz cash", "Jazz Cash"],
  ["jazz", "Jazz Cash"],
  ["lightnet", "Lightnet"],
  ["magaya", "Magaya"],
  ["m-daq global ltd", "M-DAQ"],
  ["m daq global ltd", "M-DAQ"],
  ["m-daq", "M-DAQ"],
  ["m daq", "M-DAQ"],
  ["multigate", "Multigate"],
  ["nibss", "NIBSS ( TurboTech)"],
  ["nium", "Nium"],
  ["ohent", "OhentPay"],
  ["oson", "Oson"],
  ["q2", "Q2"],
  ["repay", "Repay"],
  ["shepherd", "Shepherd"],
  ["stampli", "Stampli"],
  ["skydo", "Skydo"],
  ["blindpay", "Blindpay"],
  ["yeepay", "Yeepay"],
  ["nuvion", "Nuvion"],
  ["maplewave", "Maplewave"],
  ["triple a technologies", "TripleA"],
  ["triplea technologies", "TripleA"],
  ["triple a", "TripleA"],
  ["triple-a", "TripleA"],
  ["triplea", "TripleA"],
  ["nomadglobal", "Nomad"],
  ["nsave", "Nsave"],
  ["lianlian", "LianLian"],
  ["whish", "Whish"],
  ["remittances hub", "Remittanceshub"],
  ["remittanceshub", "Remittanceshub"],
  ["vg pay", "VG Pay"],
  ["vgpay", "VG Pay"],
  ["vigipay", "VG Pay"],
];

const PARTNER_ALIASES = Object.fromEntries(PARTNER_PATTERNS);
const STAMPLI_FX_PARTNER = "Stampli";
const STAMPLI_COMPANY_MARKUP_BPS = 0.004;
const STAMPLI_MARKUP_AMOUNT_TOLERANCE = 0.011;
const STAMPLI_MARKUP_RATE_TOLERANCE = 0.0001;
const FX_VARIABLE_SPREAD_TIERS = [
  [0, 1_000_000, 0.0015],
  [1_000_001, 5_000_000, 0.0012],
  [5_000_001, 10_000_000, 0.0010],
];
const FX_DEFAULT_PAYMENT_FEE = 9.0;
const FX_PAYMENT_FEE_LABEL = "SWIFT - SHA";
const FX_LOCAL_PAYMENT_FEES = {
  AUD: 2.0,
  CAD: 1.0,
  CZK: 2.0,
  DKK: 1.0,
  EUR: 1.0,
  GBP: 1.0,
  HKD: 2.0,
  HRK: 3.0,
  HUF: 2.0,
  IDR: 2.0,
  INR: 1.0,
  MYR: 1.0,
  NOK: 1.0,
  PHP: 3.0,
  PLN: 1.0,
  RON: 2.0,
  SEK: 1.0,
  SGD: 2.0,
  USD: 0.4,
};
const EEA_COUNTRY_CODES = new Set([
  "AT", "BE", "BG", "HR", "CY", "CZ", "DK", "EE", "FI", "FR", "DE", "GR", "HU", "IS", "IE", "IT",
  "LV", "LI", "LT", "LU", "MT", "NL", "NO", "PL", "PT", "RO", "SK", "SI", "ES", "SE",
]);
const US_COUNTRY_CODES = new Set(["US", "USA"]);
const UK_COUNTRY_CODES = new Set(["GB", "UK"]);
const CA_COUNTRY_CODES = new Set(["CA", "CAN"]);
const AU_COUNTRY_CODES = new Set(["AU", "AUS"]);
const SAME_CURRENCY_DOMESTIC_TXN_TYPES = {
  AUD: "AUD Domestic",
  CAD: "CAD Domestic",
  EUR: "EUR Domestic",
  GBP: "GBP Domestic",
};

const SECTION_CHANGE_LABELS = {
  ltxn: "Transactions",
  lrev: "Reversals",
  lva: "Virtual Accounts",
  lrs: "Revenue Share",
  lfxp: "Stampli FX Payout",
};

const SECTION_CHANGE_FIELDS = {
  ltxn: ["txnCount", "totalVolume", "customerRevenue", "estRevenue", "directInvoiceAmount"],
  lrev: ["txnCount", "totalVolume", "customerRevenue"],
  lva: ["totalActiveAccounts", "newAccountsOpened", "dormantAccounts", "newBusinessSetups", "settlementCount", "closedAccounts"],
  lrs: ["partnerRevenueShare", "revenueOwed", "monthlyMinimumRevenue", "netRevenue", "summaryLineAmount"],
  lfxp: ["txnCount", "shareAmount", "volume", "paymentUsdEquivalentAmount"],
};

const LVA_CONTEXT_FILE_TYPES = new Set([
  "partner_offline_billing",
  "all_registered_accounts",
  "all_registered_accounts_offline",
  "all_registered_accounts_rev_share",
  "vba_accounts",
  "vba_transactions_cc",
  "vba_transactions_citi",
  "vba_transactions",
]);

const STAMPLI_FX_CONTEXT_FILE_TYPES = new Set([
  "stampli_fx_revenue_share",
  "stampli_fx_revenue_reversal",
]);

const FULL_HISTORY_FILE_TYPES = new Set([
  "partner_offline_billing",
  "partner_offline_billing_reversals",
  "all_registered_accounts",
  "all_registered_accounts_offline",
  "all_registered_accounts_rev_share",
  "vba_accounts",
  "vba_transactions",
  "vba_transactions_cc",
  "vba_transactions_citi",
  "partner_rev_share_v2",
  "partner_revenue_share",
  "revenue_share_report",
  "partner_revenue_reversal",
  "rev_share_reversals",
  "partner_revenue_summary",
  "stampli_fx_revenue_share",
  "stampli_fx_revenue_reversal",
]);

function text(value) {
  if (value === null || value === undefined) return "";
  if (typeof value === "boolean") return value ? "TRUE" : "FALSE";
  if (typeof value === "number" && Number.isInteger(value)) return String(value);
  return String(value).replace(/\u0000/g, "").trim();
}

function money(value) {
  if (value === null || value === undefined || value === "") return 0;
  if (typeof value === "number") return value;
  const raw = text(value).replace(/\$/g, "").replace(/,/g, "");
  const parsed = Number(raw);
  return Number.isFinite(parsed) ? parsed : 0;
}

function boolish(value) {
  if (typeof value === "boolean") return value;
  return new Set(["true", "yes", "1", "rtp"]).has(text(value).toLowerCase());
}

function normalizeHeader(value) {
  return text(value).toLowerCase().replace(/[^a-z0-9]/g, "");
}

function slugify(value) {
  return text(value).toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "") || "export";
}

function normalizePartnerName(rawPartner) {
  const cleaned = text(rawPartner);
  if (!cleaned) return "";
  const lowered = cleaned.toLowerCase();
  if (PARTNER_ALIASES[lowered]) return PARTNER_ALIASES[lowered];
  for (const [prefix, partner] of PARTNER_PATTERNS) {
    if (lowered.startsWith(prefix)) return partner;
  }
  return cleaned;
}

function normalizeRevenuePartner(rawPartner) {
  const cleaned = text(rawPartner);
  if (!cleaned) return "";
  return normalizePartnerName(cleaned.split("|")[0].trim());
}

function monthKey(value) {
  if (value === null || value === undefined || value === "") return "";
  if (value instanceof Date && !Number.isNaN(value.valueOf())) {
    return value.toISOString().slice(0, 7);
  }
  const raw = text(value);
  if (!raw) return "";
  const direct = raw.match(/^(\d{4})-(\d{2})/);
  if (direct) return `${direct[1]}-${direct[2]}`;
  const parsed = parseDateish(raw);
  if (parsed) return parsed.toISOString().slice(0, 7);
  const short = raw.match(/^([A-Za-z]{3})-(\d{2})$/);
  if (short) {
    const lookup = {
      jan: "01", feb: "02", mar: "03", apr: "04", may: "05", jun: "06",
      jul: "07", aug: "08", sep: "09", oct: "10", nov: "11", dec: "12",
    };
    const mm = lookup[short[1].toLowerCase()];
    if (mm) return `20${short[2]}-${mm}`;
  }
  return raw.slice(0, 7);
}

function parseDateish(value) {
  if (value === null || value === undefined || value === "") return null;
  if (value instanceof Date && !Number.isNaN(value.valueOf())) return new Date(Date.UTC(value.getFullYear(), value.getMonth(), value.getDate()));
  const raw = text(value);
  if (!raw) return null;
  const iso = raw.match(/^(\d{4})-(\d{2})-(\d{2})/);
  if (iso) return new Date(`${iso[1]}-${iso[2]}-${iso[3]}T00:00:00Z`);
  const us = raw.match(/^(\d{1,2})\/(\d{1,2})\/(\d{2,4})/);
  if (us) {
    const year = us[3].length === 2 ? `20${us[3]}` : us[3];
    const month = String(us[1]).padStart(2, "0");
    const day = String(us[2]).padStart(2, "0");
    return new Date(`${year}-${month}-${day}T00:00:00Z`);
  }
  const parsed = new Date(raw);
  if (!Number.isNaN(parsed.valueOf())) {
    return new Date(Date.UTC(parsed.getUTCFullYear(), parsed.getUTCMonth(), parsed.getUTCDate()));
  }
  return null;
}

function isoValue(value) {
  const parsed = parseDateish(value);
  return parsed ? parsed.toISOString().slice(0, 10) : text(value);
}

function parseDateishFromRow(row, ...keys) {
  return parseDateish(rowValueFirst(row, ...keys));
}

function rowValueByPatterns(row, ...patterns) {
  const normalizedPatterns = patterns.filter(Boolean).map((pattern) => String(pattern).toLowerCase());
  let bestValue = null;
  let bestScore = -1;
  for (const [key, value] of Object.entries(row || {})) {
    const normalizedKey = normalizeHeader(key);
    for (const pattern of normalizedPatterns) {
      let score = -1;
      if (normalizedKey === pattern) score = 4;
      else if (normalizedKey.endsWith(pattern)) score = 3;
      else if (normalizedKey.includes(`${pattern}id`)) score = 2;
      else if (normalizedKey.includes(pattern)) score = 1;
      if (score > bestScore) {
        bestScore = score;
        bestValue = value;
      }
    }
  }
  return bestValue;
}

function rowValueFirst(row, ...keys) {
  let patterns = [];
  if (keys.length && typeof keys[keys.length - 1] === "object" && keys[keys.length - 1] && Array.isArray(keys[keys.length - 1].patterns)) {
    patterns = keys.pop().patterns;
  }
  for (const key of keys) {
    if (key && row?.[key] !== null && row?.[key] !== undefined && row?.[key] !== "") return row[key];
  }
  if (patterns.length) {
    const matched = rowValueByPatterns(row, ...patterns);
    if (matched !== null && matched !== undefined && matched !== "") return matched;
  }
  return null;
}

function matchesPeriod(month, period) {
  return !period || month === period;
}

function normalizeCountryCode(value) {
  const raw = text(value).toUpperCase();
  if (raw === "UNITED STATES" || raw === "US") return "US";
  if (new Set(["UNITED KINGDOM", "GREAT BRITAIN", "GB", "UK"]).has(raw)) return "GB";
  return raw;
}

function isEeaCountry(value) {
  return EEA_COUNTRY_CODES.has(normalizeCountryCode(value));
}

function titleCaseFunding(value) {
  const normalized = text(value).toLowerCase();
  const mapping = { bank: "Bank", wallet: "Wallet", card: "Card", credit: "Credit", debit: "Debit", cash: "Cash" };
  return mapping[normalized] || (normalized ? normalized[0].toUpperCase() + normalized.slice(1) : "");
}

function normalizeFixedTxnType(rawType) {
  const value = text(rawType);
  const normalized = value.toLowerCase().replace(/[_-]/g, " ").replace(/\s+/g, " ").trim();
  const mapping = {
    domestic: "Domestic",
    "usd abroad": "USD Abroad",
    fx: "FX",
    "cad domestic": "CAD Domestic",
    "gbp domestic": "GBP Domestic",
    "eur domestic": "EUR Domestic",
    "aud domestic": "AUD Domestic",
    "incoming us": "Payin",
    payin: "Payin",
    payout: "Payout",
  };
  return mapping[value] || mapping[normalized] || value;
}

function normalizeSpeed(isRtp, fasterAch) {
  if (isRtp) return "RTP";
  if (fasterAch) return "FasterACH";
  return "Standard";
}

function normalizeProcessingMethod(txnType, speedFlag, methods) {
  if (txnType === "FX" || txnType === "USD Abroad") return "Wire";
  if (txnType === "CAD Domestic" || methods.has("eft")) return "EFT";
  if (speedFlag === "RTP") return "RTP";
  return "ACH";
}

function deriveContractTxnType(rawTxnType, payerCcy = "", payeeCcy = "", payerCountry = "", payeeCountry = "", paymentType = "") {
  const normalizedRaw = normalizeFixedTxnType(rawTxnType);
  const normalizedPaymentType = normalizeFixedTxnType(paymentType);
  if (["Payin", "Payout"].includes(normalizedPaymentType)) return normalizedPaymentType;

  const payerCcyText = text(payerCcy).toUpperCase();
  const payeeCcyText = text(payeeCcy).toUpperCase();
  const payerCountryCode = normalizeCountryCode(payerCountry);
  const payeeCountryCode = normalizeCountryCode(payeeCountry);

  if (normalizedRaw === "FX" && payerCcyText && payeeCcyText && payerCcyText === payeeCcyText) {
    if (payeeCcyText === "USD" && payeeCountryCode && !US_COUNTRY_CODES.has(payeeCountryCode)) return "USD Abroad";
    if (SAME_CURRENCY_DOMESTIC_TXN_TYPES[payeeCcyText]) return SAME_CURRENCY_DOMESTIC_TXN_TYPES[payeeCcyText];
  }
  if (["Domestic", "USD Abroad", "FX", "CAD Domestic", "GBP Domestic", "EUR Domestic", "AUD Domestic", "Payin", "Payout"].includes(normalizedRaw)) {
    return normalizedRaw;
  }
  if (payerCcyText && payeeCcyText && payerCcyText !== payeeCcyText) return "FX";
  if (payeeCcyText === "USD" && payeeCountryCode && !US_COUNTRY_CODES.has(payeeCountryCode)) return "USD Abroad";
  if (payerCcyText === "CAD" && payeeCcyText === "CAD" && CA_COUNTRY_CODES.has(payerCountryCode) && CA_COUNTRY_CODES.has(payeeCountryCode)) return "CAD Domestic";
  if (payerCcyText === "GBP" && payeeCcyText === "GBP" && UK_COUNTRY_CODES.has(payerCountryCode) && UK_COUNTRY_CODES.has(payeeCountryCode)) return "GBP Domestic";
  if (payerCcyText === "EUR" && payeeCcyText === "EUR" && isEeaCountry(payerCountryCode) && isEeaCountry(payeeCountryCode)) return "EUR Domestic";
  if (payerCcyText === "AUD" && payeeCcyText === "AUD" && AU_COUNTRY_CODES.has(payerCountryCode) && AU_COUNTRY_CODES.has(payeeCountryCode)) return "AUD Domestic";
  return "Domestic";
}

function extractEstRevenue(row) {
  return money(row["Est Revenue"] ?? row["Estimated Revenue"] ?? rowValueByPatterns(row, "estrevenue", "estimatedrevenue", "estrev"));
}

function inferPartner(row) {
  for (const key of [
    "**  Initiator Customer Account ** Partner Group Source",
    "Partner Offline Billing PARTNER",
    "Partner Group With Bank",
    "PARTNER",
    "Partner",
    "partner",
  ]) {
    const partner = normalizePartnerName(row?.[key]);
    if (partner) return partner;
  }
  for (const pattern of ["partnergroupsource", "partnergroupwithbank", "partnerofflinebillingpartner"]) {
    const partner = normalizePartnerName(rowValueByPatterns(row, pattern));
    if (partner) return partner;
  }
  const haystack = [
    text(row?.["Payer Email"] ?? rowValueByPatterns(row, "payeremail", "payeraccountprimaryemail")),
    text(row?.["Payee Email"] ?? rowValueByPatterns(row, "payeeemail", "payeeaccountprimaryemail")),
    text(row?.["Payer Business Name"] ?? rowValueByPatterns(row, "payerbusinessname", "payeraccountname")),
    text(row?.["Payee Business Name"] ?? rowValueByPatterns(row, "payeebusinessname", "payeeaccountname")),
  ].join(" ").toLowerCase();
  for (const [needle, partner] of PARTNER_PATTERNS) {
    if (haystack.includes(needle)) return partner;
  }
  return "";
}

function buildOfflinePaymentSeed(row, { includeDetailRows = true } = {}) {
  const seed = {
    "Txn Type": rowValueFirst(row, "** Payment For Sales DV ** Txn Type (Dom/Fx/Abroad..", "Txn Type", "Payment Type", { patterns: ["txntype", "paymenttype"] }),
    "Payment Type": rowValueFirst(row, "** Payment For Sales DV ** Payment Type", "Payment Type", { patterns: ["paymenttype"] }),
    "** Payment For Sales DV ** Payer Funding Method Type": rowValueFirst(row, "** Payment For Sales DV ** Payer Funding Method Type", "Payer Funding Method", { patterns: ["payerfundingmethodtype", "payerfundingmethod"] }),
    "** Payment For Sales DV ** Payee Funding Method Type": rowValueFirst(row, "** Payment For Sales DV ** Payee Funding Method Type", "Payee Funding Method", { patterns: ["payeefundingmethodtype", "payeefundingmethod"] }),
    "** Payment For Sales DV ** Payer Amount Currency": rowValueFirst(row, "** Payment For Sales DV ** Payer Amount Currency", "Payer Amount Currency", { patterns: ["payeramountcurrency"] }),
    "** Payment For Sales DV ** Payee Amount Currency": rowValueFirst(row, "** Payment For Sales DV ** Payee Amount Currency", "Currency (Payee Amount Currency)", "Payee Amount Currency", { patterns: ["payeeamountcurrency"] }),
    "** Payment For Sales DV ** Payer Country": rowValueFirst(row, "** Payment For Sales DV ** Payer Country", "Payer Country", { patterns: ["payercountry"] }),
    "** Payment For Sales DV ** Payee Country": rowValueFirst(row, "** Payment For Sales DV ** Payee Country", "Payee Country", { patterns: ["payeecountry"] }),
    "** Payment For Sales DV ** Payer Account ID": rowValueFirst(row, "** Payment For Sales DV ** Payer Account ID", "Account ID", { patterns: ["payeraccountid", "accountid"] }),
    "Date of Payment Submission": rowValueFirst(row, "Date of Payment Submission", "** Payment For Sales DV ** Time Created Date", { patterns: ["dateofpaymentsubmission", "timecreateddate"] }),
    "Credit Complete Date": rowValueFirst(row, "Credit Complete Date", "Transaction Lookup Dates Credit Complete Timestamp Date", "Transaction Lookup Dates Credit Complete Timestamp Time", { patterns: ["creditcompletedate", "creditcompletetimestampdate", "creditcompletetimestamptime"] }),
    "** Payment For Sales DV ** Payee Amount Number": rowValueFirst(row, "** Payment For Sales DV ** Payee Amount Number", "Foreign Currency Amount (Payee Amount Number)", { patterns: ["payeeamountnumber"] }),
    "USD Amount Debited to the Customer": rowValueFirst(row, "USD Amount Debited to the Customer", "** Payment For Sales DV ** Total USD Amount Number", "** Payment For Sales DV ** USD Amount Number", { patterns: ["usdamountdebited", "totalusdamountnumber", "usdamountnumber"] }),
    "Payment USD Equivalent Amount": rowValueFirst(row, "Payment USD Equivalent Amount", "** Payment For Sales DV ** Total USD Amount Number", "** Payment For Sales DV ** USD Amount Number", { patterns: ["paymentusdequivalentamount", "totalusdamountnumber", "usdamountnumber"] }),
    __estRevenue: extractEstRevenue(row),
  };
  if (!includeDetailRows) return seed;
  return {
    ...seed,
    "Payer Email": text(row["Payer Email"] || rowValueByPatterns(row, "payeremail", "payeraccountprimaryemail")),
    "Payer Business Name": text(row["Payer Business Name"] || rowValueByPatterns(row, "payerbusinessname", "payeraccountname")),
    "Payee Email": text(row["Payee Email"] || rowValueByPatterns(row, "payeeemail", "payeeaccountprimaryemail")),
    "Payee Business Name": text(row["Payee Business Name"] || rowValueByPatterns(row, "payeebusinessname", "payeeaccountname")),
    "Credit Rail": text(row["Credit Rail"] || rowValueByPatterns(row, "creditrail")),
    "Transaction Processing Method": text(row["Transaction Processing Method"] || rowValueByPatterns(row, "transactionprocessingmethod")),
    "Funding Method Used": text(row["Funding Method Used"] || rowValueByPatterns(row, "fundingmethodused")),
    "**  Initiator Customer Account ** Partner Group Source": text(row["**  Initiator Customer Account ** Partner Group Source"] || rowValueByPatterns(row, "partnergroupsource")),
    "** Payment For Sales DV ** Initiator Status": text(row["** Payment For Sales DV ** Initiator Status"] || rowValueByPatterns(row, "initiatorstatus")),
    "**  Initiator Customer Account ** Type Defn": text(row["**  Initiator Customer Account ** Type Defn"] || rowValueByPatterns(row, "typedefn")),
  };
}

function parseCsv(textValue) {
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
  const headers = rows[0].map((value) => text(value));
  return rows.slice(1).map((values) => {
    const output = {};
    headers.forEach((header, index) => {
      if (!header) return;
      output[header] = values[index] ?? "";
    });
    return output;
  }).filter((entry) => Object.values(entry).some((value) => text(value)));
}

function csvRowToObject(headers, values) {
  const output = {};
  headers.forEach((header, index) => {
    if (!header) return;
    output[header] = values[index] ?? "";
  });
  return output;
}

function iterateCsvRows(textValue, onRow) {
  let row = [];
  let cell = "";
  let inQuotes = false;
  let headers = null;
  let sawFirstChar = false;

  const commitRow = () => {
    if (!headers) {
      headers = row.map((value) => text(value));
      return;
    }
    if (!row.length) return;
    const entry = csvRowToObject(headers, row);
    if (Object.values(entry).some((value) => text(value))) {
      onRow(entry);
    }
  };

  const processChunk = (chunkValue) => {
    const input = String(chunkValue || "");
    for (let i = 0; i < input.length; i += 1) {
      const char = input[i];
      const next = input[i + 1];
      if (!sawFirstChar) {
        sawFirstChar = true;
        if (char === "\uFEFF") continue;
      }
      if (char === "\u0000") continue;
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
        commitRow();
        row = [];
        cell = "";
      } else if (char === "\r") {
        if (next !== "\n") {
          row.push(cell);
          commitRow();
          row = [];
          cell = "";
        }
      } else {
        cell += char;
      }
    }
  };

  processChunk(textValue);
  row.push(cell);
  if (row.length > 1 || row[0] !== "") {
    commitRow();
  }
}

function iterateCsvBufferRows(bufferValue, onRow, { chunkSize = 65536 } = {}) {
  let row = [];
  let cell = "";
  let inQuotes = false;
  let headers = null;
  let sawFirstChar = false;
  const decoder = new TextDecoder("utf-8");
  const inputBuffer = Buffer.isBuffer(bufferValue) ? bufferValue : Buffer.from(bufferValue || "");

  const commitRow = () => {
    if (!headers) {
      headers = row.map((value) => text(value));
      return;
    }
    if (!row.length) return;
    const entry = csvRowToObject(headers, row);
    if (Object.values(entry).some((value) => text(value))) {
      onRow(entry);
    }
  };

  const processChunk = (chunkValue) => {
    const input = String(chunkValue || "");
    for (let i = 0; i < input.length; i += 1) {
      const char = input[i];
      const next = input[i + 1];
      if (!sawFirstChar) {
        sawFirstChar = true;
        if (char === "\uFEFF") continue;
      }
      if (char === "\u0000") continue;
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
        commitRow();
        row = [];
        cell = "";
      } else if (char === "\r") {
        if (next !== "\n") {
          row.push(cell);
          commitRow();
          row = [];
          cell = "";
        }
      } else {
        cell += char;
      }
    }
  };

  for (let offset = 0; offset < inputBuffer.length; offset += chunkSize) {
    const end = Math.min(offset + chunkSize, inputBuffer.length);
    processChunk(decoder.decode(inputBuffer.subarray(offset, end), { stream: end < inputBuffer.length }));
  }
  processChunk(decoder.decode());
  row.push(cell);
  if (row.length > 1 || row[0] !== "") {
    commitRow();
  }
}

function normalizeRegisteredAccountRows(rows, { preferTimeCreated = false } = {}) {
  const normalizedRows = [];
  for (const row of rows || []) {
    const partner = normalizePartnerName(
      row["Partner Name"] ||
      row["Partner Group Source"] ||
      row["Partner Group With Bank"] ||
      rowValueByPatterns(row, "partnername", "partnergroupsource", "partnergroupwithbank")
    );
    const accountId = text(
      row["Account Id"] ||
      row["Account ID"] ||
      row["ACCOUNT_ID"] ||
      row["customer_account.id"] ||
      rowValueByPatterns(row, "accountid", "customeraccountid")
    );
    const joinDate = preferTimeCreated
      ? parseDateishFromRow(row, "Time Created Date", "Time Created Time", "TIME_CREATED_date", { patterns: ["timecreateddate", "timecreatedtime"] })
      : parseDateishFromRow(row, "Join Date Time", { patterns: ["joindatetime"] });
    const typeDefn = text(row["Type Defn"] || rowValueByPatterns(row, "typedefn"));
    const status = text(row["Status"] || row["STATUS"] || rowValueByPatterns(row, "status"));
    if (!partner || !accountId || !joinDate) continue;
    normalizedRows.push({
      "Partner Name": partner,
      "Account Id": accountId,
      "Join Date Time": joinDate.toISOString().slice(0, 10),
      "Type Defn": typeDefn,
      "Status": status,
    });
  }
  return normalizedRows;
}

function mergeRegisteredAccountRows(...rowSets) {
  const merged = new Map();
  for (const rows of rowSets) {
    for (const row of rows || []) {
      const key = `${text(row["Partner Name"])}|${text(row["Account Id"])}`;
      if (key === "|") continue;
      const existing = merged.get(key);
      if (!existing) {
        merged.set(key, { ...row });
        continue;
      }
      const candidate = { ...existing };
      for (const field of ["Join Date Time", "Type Defn", "Status"]) {
        if (!text(candidate[field]) && text(row[field])) candidate[field] = row[field];
      }
      merged.set(key, candidate);
    }
  }
  return [...merged.values()];
}

function buildVbaTransactionActivity(rows) {
  const activity = new Map();
  for (const row of rows || []) {
    const accountId = text(row["Account Id"] || row["ACCOUNT_ID"] || rowValueByPatterns(row, "accountid"));
    if (!accountId) continue;
    const completedDate = parseDateish(
      rowValueFirst(
        row,
        "CC Completed Time",
        "Citi Trx Completed Time",
        "Customer Virtual Bank Account Payments CC COMPLETED AT Date",
        "Customer Virtual Bank Transaction Report Citi COMPLETED AT Date",
        { patterns: [
          "cccompletedtime",
          "cititrxcompletedtime",
          "customervirtualbankaccountpaymentscccompletedatdate",
          "customervirtualbanktransactionreportciticompletedatdate",
        ] }
      )
    );
    if (!completedDate) continue;
    const existing = activity.get(accountId) || [];
    existing.push(completedDate.toISOString().slice(0, 10));
    activity.set(accountId, existing);
  }
  return Object.fromEntries(
    [...activity.entries()].map(([accountId, days]) => [accountId, [...new Set(days)].sort()])
  );
}

function monthEnd(period) {
  const [year, month] = String(period).split("-").map(Number);
  const end = new Date(Date.UTC(year, month, 0));
  return end;
}

function buildVirtualAccountUsage(registerRows, accountActivity, settlementDays, periods) {
  const targetPartners = new Set();
  for (const row of registerRows || []) {
    const partner = normalizePartnerName(
      row["Partner Name"] ||
      row["Partner Group Source"] ||
      row["Partner Group With Bank"] ||
      rowValueByPatterns(row, "partnername", "partnergroupsource", "partnergroupwithbank")
    );
    if (partner) targetPartners.add(partner);
  }
  Object.keys(settlementDays || {}).forEach((key) => {
    const [partner] = String(key).split("|");
    if (partner) targetPartners.add(partner);
  });
  if (!targetPartners.size) return [];

  const perMonth = new Map();
  for (const partner of targetPartners) {
    for (const period of periods || []) {
      perMonth.set(`${partner}|${period}`, {
        period,
        partner,
        newAccountsOpened: 0,
        totalActiveAccounts: 0,
        totalBusinessAccounts: 0,
        totalIndividualAccounts: 0,
        dormantAccounts: 0,
        closedAccounts: 0,
        newBusinessSetups: 0,
        settlementCount: (settlementDays?.[`${partner}|${period}`] || []).length,
      });
    }
  }

  const accountsByPartner = new Map();
  for (const row of registerRows || []) {
    const partner = normalizePartnerName(
      row["Partner Name"] ||
      row["Partner Group Source"] ||
      row["Partner Group With Bank"] ||
      rowValueByPatterns(row, "partnername", "partnergroupsource", "partnergroupwithbank")
    );
    if (!targetPartners.has(partner)) continue;
    const joinDate = parseDateishFromRow(row, "Join Date Time", { patterns: ["joindatetime"] });
    if (!joinDate) continue;
    const accountId = text(row["Account Id"] || row["ACCOUNT_ID"] || rowValueByPatterns(row, "accountid", "customeraccountid"));
    const activities = [...new Set(accountActivity?.[accountId] || [])].sort();
    const entry = {
      accountId,
      joinDate: joinDate.toISOString().slice(0, 10),
      typeDefn: text(row["Type Defn"] || rowValueByPatterns(row, "typedefn")),
      status: text(row["Status"] || row["STATUS"] || rowValueByPatterns(row, "status")).toLowerCase(),
      activities,
    };
    const existing = accountsByPartner.get(partner) || [];
    existing.push(entry);
    accountsByPartner.set(partner, existing);
  }

  for (const [partner, accounts] of accountsByPartner.entries()) {
    for (const period of periods || []) {
      const entry = perMonth.get(`${partner}|${period}`);
      if (!entry) continue;
      const periodStart = new Date(`${period}-01T00:00:00Z`);
      const periodEnd = monthEnd(period);
      const dormantCutoff = new Date(periodEnd);
      dormantCutoff.setUTCDate(dormantCutoff.getUTCDate() - 90);
      for (const account of accounts) {
        const joinDate = new Date(`${account.joinDate}T00:00:00Z`);
        if (joinDate > periodEnd) continue;
        entry.totalActiveAccounts += 1;
        const typeDefn = account.typeDefn.toLowerCase();
        if (typeDefn === "business") entry.totalBusinessAccounts += 1;
        else if (typeDefn === "individual") entry.totalIndividualAccounts += 1;
        if (joinDate >= periodStart && joinDate <= periodEnd) {
          entry.newAccountsOpened += 1;
          if (typeDefn === "business") entry.newBusinessSetups += 1;
        }
        if (joinDate > dormantCutoff) continue;
        const activities = account.activities.map((day) => new Date(`${day}T00:00:00Z`)).filter((day) => day <= periodEnd).sort((a, b) => a - b);
        const lastActivity = activities.length ? activities[activities.length - 1] : null;
        if (!lastActivity || lastActivity < dormantCutoff) entry.dormantAccounts += 1;
        if (["closed", "inactive", "deactivated"].includes(account.status)) entry.closedAccounts += 1;
      }
    }
  }

  return [...perMonth.values()].sort((a, b) => `${a.partner}|${a.period}`.localeCompare(`${b.partner}|${b.period}`));
}

function revenuePaymentId(row) {
  return text(rowValueFirst(row, "Partner Revenue Share Fixed Payment ID", "Payment Payment ID", "Payment ID", { patterns: ["paymentid"] }));
}

function dedupeRevenueRecordList(rows) {
  const ordered = [];
  const seenPaymentIds = new Set();
  for (const row of rows || []) {
    const paymentId = revenuePaymentId(row);
    if (paymentId && seenPaymentIds.has(paymentId)) continue;
    if (paymentId) seenPaymentIds.add(paymentId);
    ordered.push(row);
  }
  return ordered;
}

function buildRevenueDetailTransactions(rows, period, { includeDetailRows = true } = {}) {
  const dedupedRows = dedupeRevenueRecordList(rows);
  const grouped = new Map();
  const detailRows = [];
  for (const row of dedupedRows) {
    const creditCompleteValue = rowValueFirst(
      row,
      "Credit Complete Date",
      "Transaction Lookup Dates Credit Complete Timestamp Time",
      "Transaction Lookup Dates Credit Complete Timestamp Date",
      "Credit Complete Timestamp Date",
      { patterns: ["creditcompletedate", "creditcompletetimestamptime", "creditcompletetimestampdate"] }
    );
    const month = monthKey(creditCompleteValue);
    if (!month || !matchesPeriod(month, period)) continue;
    const partner = normalizeRevenuePartner(
      row["Partner Group With Bank"] ||
      row["Partner Group Source"] ||
      rowValueByPatterns(row, "partnergroupwithbank", "partnergroupsource")
    );
    if (!partner) continue;

    const rawTxnType = rowValueFirst(row, "Payment Txn Type", "Txn Type", { patterns: ["paymenttxntype", "txntype"] });
    const isRtp = boolish(row["IsRTP"] || rowValueByPatterns(row, "isrtp"));
    const fasterAch = text(row["Is Faster Ach"] || rowValueByPatterns(row, "isfasterach")) === "FasterACH";
    const speedFlag = normalizeSpeed(isRtp, fasterAch);
    const paymentType = text(rowValueFirst(row, "Payment Payment Type", "Payment Type", { patterns: ["paymentpaymenttype", "paymenttype"] }));
    const payerCcy = text(rowValueFirst(row, "Payment Payer Amount Currency", "Payer Amount Currency", { patterns: ["payeramountcurrency"] })) || "USD";
    const payeeCcy = text(rowValueFirst(row, "Payment Payee Amount Currency", "Payee Amount Currency", { patterns: ["payeeamountcurrency"] })) || "USD";
    const payerCountry = normalizeCountryCode(rowValueFirst(row, "Payment Payer Country", "Payer Country", { patterns: ["payercountry"] }));
    const payeeCountry = normalizeCountryCode(rowValueFirst(row, "Payment Payee Country", "Payee Country", { patterns: ["payeecountry"] }));
    const txnType = deriveContractTxnType(rawTxnType, payerCcy, payeeCcy, payerCountry, payeeCountry, paymentType);
    const processingMethod = speedFlag === "RTP" ? "RTP" : (txnType === "FX" || txnType === "USD Abroad" ? "Wire" : text(rowValueFirst(row, "Payment Transaction Processing Method", "Transaction Processing Method", { patterns: ["transactionprocessingmethod", "processingmethod"] })) || "ACH");
    const walletFlag = paymentType.toLowerCase().includes("wallet");
    const customerRevenueValue = row["Net Revenue"] ?? row["Fixed Fee"] ?? rowValueByPatterns(row, "netrevenue", "fixedfee");
    const payeeAmount = money(rowValueFirst(row, "Payment Payee Amount Number", "Payee Amount Number", "Foreign Currency Amount (Payee Amount Number)", { patterns: ["payeeamountnumber", "foreigncurrencyamount"] }));
    const paymentUsdEquivalentAmount = money(rowValueFirst(row, "Payment Total USD Amount Number", "USD Amount Number", "Total USD Amount Number", { patterns: ["totalusdamountnumber", "usdamountnumber"] }));
    const creditUsdRate = payeeAmount > 0 && paymentUsdEquivalentAmount > 0
      ? Number((paymentUsdEquivalentAmount / payeeAmount).toFixed(8))
      : 0;
    const estRevenue = Number(extractEstRevenue(row).toFixed(2));
    const revenueBasis = row["Net Revenue"] !== null && row["Net Revenue"] !== undefined && row["Net Revenue"] !== "" ? "net" : "gross";
    const key = [
      partner, month, txnType, speedFlag,
      walletFlag ? "Wallet" : "", walletFlag ? "Wallet" : "",
      payerCcy, payeeCcy, payerCountry, payeeCountry, processingMethod, revenueBasis,
    ].join("|");
    const current = grouped.get(key) || { txnCount: 0, totalVolume: 0, customerRevenue: 0, estRevenue: 0 };
    current.txnCount += 1;
    current.totalVolume += paymentUsdEquivalentAmount;
    current.customerRevenue += money(customerRevenueValue);
    current.estRevenue += estRevenue;
    grouped.set(key, current);
    if (includeDetailRows) {
      detailRows.push({
        detailCategory: "transaction",
        detailSource: "revenue_share",
        partner,
        period: month,
        paymentId: text(rowValueFirst(row, "Partner Revenue Share Fixed Payment ID", "Payment Payment ID", "Payment ID", { patterns: ["paymentid"] })),
        txnType,
        speedFlag,
        processingMethod,
        payerFunding: walletFlag ? "Wallet" : "",
        payeeFunding: walletFlag ? "Wallet" : "",
        payerCcy,
        payeeCcy,
        payeeAmountCurrency: payeeCcy,
        payeeAmount: Number(payeeAmount.toFixed(2)),
        usdAmountDebited: Number(paymentUsdEquivalentAmount.toFixed(2)),
        paymentUsdEquivalentAmount: Number(paymentUsdEquivalentAmount.toFixed(2)),
        creditUsdRate,
        payerCountry,
        payeeCountry,
        accountId: text(row["Account Id"] || rowValueByPatterns(row, "accountid", "payeraccountid")),
        paymentType,
        submissionDate: isoValue(rowValueFirst(row, "Payment Time Created Date", "Time Created Date", { patterns: ["timecreateddate"] })),
        creditCompleteDate: isoValue(creditCompleteValue),
        txnTypeRaw: text(rawTxnType),
        payerName: text(row["Payer Name"] || rowValueByPatterns(row, "payername")),
        payeeName: text(row["Payee Name"] || rowValueByPatterns(row, "payeename")),
        payerEmail: text(row["Payer Email"] || rowValueByPatterns(row, "payercustomeraccountprimaryemail")),
        payeeEmail: text(row["Payee Email"] || rowValueByPatterns(row, "payeecustomeraccountprimaryemail")),
        usdAmount: Number(paymentUsdEquivalentAmount.toFixed(2)),
        customerRevenue: Number(money(customerRevenueValue).toFixed(2)),
        estRevenue,
        netRevenue: Number(money(row["Net Revenue"] || rowValueByPatterns(row, "netrevenue")).toFixed(2)),
        countPricing: Number(money(row["Count Pricing"] || rowValueByPatterns(row, "countpricing")).toFixed(2)),
        isRTP: isRtp,
        isFasterAch: fasterAch,
        initiatorStatus: text(row["Initiator Status"] || rowValueByPatterns(row, "initiatorstatus")),
        typeDefn: text(row["Type Defn"] || rowValueByPatterns(row, "typedefn")),
        revenueBasis,
      });
    }
  }

  const output = [];
  for (const [key, aggregate] of [...grouped.entries()].sort()) {
    const [partner, month, txnType, speedFlag, payerFunding, payeeFunding, payerCcy, payeeCcy, payerCountry, payeeCountry, processingMethod, revenueBasis] = key.split("|");
    const txnCount = aggregate.txnCount;
    const totalVolume = Number(aggregate.totalVolume.toFixed(2));
    const customerRevenue = Number(aggregate.customerRevenue.toFixed(2));
    const estRevenue = Number(aggregate.estRevenue.toFixed(2));
    output.push({
      period: month,
      partner,
      txnType,
      speedFlag,
      minAmt: txnCount,
      maxAmt: txnCount,
      payerFunding,
      payeeFunding,
      payerCcy,
      payeeCcy,
      payerCountry,
      payeeCountry,
      processingMethod,
      txnCount,
      totalVolume,
      customerRevenue,
      estRevenue,
      avgTxnSize: txnCount ? Number((totalVolume / txnCount).toFixed(2)) : 0,
      revenueBasis,
    });
  }
  return [output, detailRows];
}

function buildRevenueShareSummary(rows, period, { allowBillingMonthFallback = true } = {}) {
  const output = [];
  for (const row of rows || []) {
    const month = monthKey(rowValueFirst(
      row,
      "Credit Complete Date",
      "Transaction Lookup Dates Credit Complete Timestamp Time",
      "Transaction Lookup Dates Credit Complete Timestamp Date",
      "Credit Complete Timestamp Date",
      { patterns: ["creditcompletedate", "creditcompletetimestamptime", "creditcompletetimestampdate"] }
    ));
    if (month) {
      if (!matchesPeriod(month, period)) continue;
      const partner = normalizeRevenuePartner(row["Partner Group Source"] || row["Partner Group With Bank"]);
      if (!partner) continue;
      output.push({
        period: month,
        partner,
        netRevenue: money(row["Net Revenue"]),
        partnerRevenueShare: money(row["Partner Net Revenue Share"]),
        revenueOwed: money(row["Revenue Owed"]),
        monthlyMinimumRevenue: money(row["Monthly Minimum Revenue"]),
        revenueSource: "summary",
      });
      continue;
    }
    if (!allowBillingMonthFallback) continue;
    const summaryMonth = monthKey(rowValueByPatterns(row, "billingmonthmonth", "billingmonth", "billingmo"));
    if (!summaryMonth || !matchesPeriod(summaryMonth, period)) continue;
    const partner = normalizeRevenuePartner(rowValueByPatterns(row, "partnername", "partner"));
    if (!partner) continue;
    const totalAmountRaw = rowValueByPatterns(row, "totalamountfee", "totalamount", "totalamo");
    if (totalAmountRaw === null || totalAmountRaw === undefined || totalAmountRaw === "") continue;
    const totalAmount = Math.abs(money(totalAmountRaw));
    const billingType = text(rowValueByPatterns(row, "billingtype", "billingty")) || "Billing Summary";
    const computation = text(rowValueByPatterns(row, "computationmemo", "computation", "computati", "memo"));
    const normalizedContext = `${billingType} ${computation}`.toLowerCase();
    const direction = /revsharepayout|revshare payout|partner net revenue share|we pay|veem owes|payout/.test(normalizedContext) ? "pay" : "charge";
    let count = 0;
    let unitAmount = 0;
    const match = computation.replace(/%/g, "").match(/([-+]?[0-9][0-9,]*(?:\.[0-9]+)?)\s*\*\s*\$?([-+]?[0-9][0-9,]*(?:\.[0-9]+)?)/);
    if (match) {
      count = money(match[1]);
      unitAmount = money(match[2]);
    }
    const isMinimumRow = normalizedContext.includes("minimum");
    output.push({
      period: summaryMonth,
      partner,
      netRevenue: 0,
      partnerRevenueShare: direction === "pay" ? totalAmount : 0,
      revenueOwed: direction === "charge" ? totalAmount : 0,
      monthlyMinimumRevenue: direction === "charge" && isMinimumRow ? totalAmount : 0,
      revenueSource: "billing_summary",
      summaryDirection: direction,
      summaryBillingType: billingType,
      summaryLabel: billingType.trim() || "Billing Summary",
      summaryComputation: computation,
      summaryCount: Number(count.toFixed(2)),
      summaryUnitAmount: Number(unitAmount.toFixed(6)),
      summaryLineAmount: Number(totalAmount.toFixed(2)),
    });
  }
  return output.sort((a, b) => `${a.partner}|${a.period}|${a.summaryLabel || a.revenueSource || ""}|${a.summaryComputation || ""}`.localeCompare(`${b.partner}|${b.period}|${b.summaryLabel || b.revenueSource || ""}|${b.summaryComputation || ""}`));
}

function buildRevenueReversalSummary(rows, period) {
  const output = [];
  for (const row of rows || []) {
    const month = monthKey(rowValueFirst(
      row,
      "Refund Complete Date",
      "Refund Completed Date",
      "Transaction Lookup Dates Refund Complete Timestamp Time",
      "Transaction Lookup Dates Refund Complete Timestamp Date",
      { patterns: ["refundcompletedate", "refundcompleteddate", "refundcompletetimestamptime", "refundcompletetimestampdate"] }
    ));
    if (!month || !matchesPeriod(month, period)) continue;
    const partner = normalizeRevenuePartner(row["Partner Group Source"] || row["Partner Group With Bank"] || rowValueByPatterns(row, "partnergroupsource", "partnergroupwithbank"));
    if (!partner) continue;
    const netRevenue = Math.abs(money(row["Net Revenue"] || rowValueByPatterns(row, "netrevenue")));
    let partnerShare = Math.abs(money(row["Partner Net Revenue Share"] || rowValueByPatterns(row, "partnernetrevenueshare")));
    let revenueOwed = Math.abs(money(row["Revenue Owed"] || rowValueByPatterns(row, "revenueowed")));
    if (partnerShare <= 0 && revenueOwed <= 0) {
      const rate = money(row["Partner Revenue Share Rate"] || rowValueByPatterns(row, "inipartnerrevenuesharerate", "partnerrevenuesharerate"));
      if (rate > 0 && netRevenue > 0) partnerShare = Number((netRevenue * rate).toFixed(2));
    }
    output.push({
      period: month,
      partner,
      netRevenue: -netRevenue,
      partnerRevenueShare: -partnerShare,
      revenueOwed: -revenueOwed,
      monthlyMinimumRevenue: 0,
      revenueSource: "reversal",
    });
  }
  return output.sort((a, b) => `${a.partner}|${a.period}`.localeCompare(`${b.partner}|${b.period}`));
}

function chooseFxVariableSpreadRate(periodVolume) {
  for (const [minVolume, maxVolume, rate] of FX_VARIABLE_SPREAD_TIERS) {
    if (periodVolume >= minVolume && periodVolume <= maxVolume) return rate;
  }
  return FX_VARIABLE_SPREAD_TIERS[FX_VARIABLE_SPREAD_TIERS.length - 1][2];
}

function chooseFxPaymentFee(payeeCurrency) {
  const normalized = text(payeeCurrency).toUpperCase();
  if (Object.prototype.hasOwnProperty.call(FX_LOCAL_PAYMENT_FEES, normalized)) return [FX_LOCAL_PAYMENT_FEES[normalized], `Local payment fee (${normalized})`];
  return [FX_DEFAULT_PAYMENT_FEE, FX_PAYMENT_FEE_LABEL];
}

function chooseStampliMidMarketUsd(row, month, avgRatioByMonthCurrency) {
  const payeeCurrency = text(rowValueFirst(row, "** Payment For Sales DV ** Payee Amount Currency", "Currency (Payee Amount Currency)", "Payee Amount Currency", { patterns: ["payeeamountcurrency"] })).toUpperCase();
  const payeeAmount = money(rowValueFirst(row, "** Payment For Sales DV ** Payee Amount Number", "Foreign Currency Amount (Payee Amount Number)", { patterns: ["payeeamountnumber"] }));
  let midMarketUsd = Number(money(rowValueFirst(row, "Payment USD Equivalent Amount", "** Payment For Sales DV ** Total USD Amount Number", "** Payment For Sales DV ** USD Amount Number", { patterns: ["paymentusdequivalentamount", "totalusdamountnumber", "usdamountnumber"] })).toFixed(2));
  if (midMarketUsd > 0) return [midMarketUsd, false];
  const ratio = avgRatioByMonthCurrency[`${month}|${payeeCurrency}`] || 0;
  if (payeeAmount > 0 && ratio > 0) return [Number((payeeAmount * ratio).toFixed(2)), true];
  return [0, false];
}

function calculateStampliMarkupFromFeedRow(row, month, avgRatioByMonthCurrency) {
  let [midMarketUsd, usedPeriodAverage] = chooseStampliMidMarketUsd(row, month, avgRatioByMonthCurrency);
  const transactionVolumeUsd = Number(money(rowValueFirst(row, "** Payment For Sales DV ** Total USD Amount Number", "** Payment For Sales DV ** USD Amount Number", "Payment USD Equivalent Amount", { patterns: ["totalusdamountnumber", "usdamountnumber", "paymentusdequivalentamount"] })).toFixed(2));
  if (midMarketUsd <= 0) midMarketUsd = transactionVolumeUsd;
  const usdDebited = Number(money(rowValueFirst(row, "USD Amount Debited to the Customer", "** Payment For Sales DV ** Total USD Amount Number", "** Payment For Sales DV ** USD Amount Number", { patterns: ["usdamountdebited", "totalusdamountnumber", "usdamountnumber"] })).toFixed(2));
  const providedAmount = Number(money(row["Stampli Markup Amount"]).toFixed(2));
  const customerMarkupPct = Number(money(rowValueFirst(row, "Customer Markup (%)", "Partner Revenue Share Variable Payer Markup Rate", { patterns: ["customermarkup", "payermarkuprate"] })).toFixed(6));
  const stampliBuyRatePct = Number(money(rowValueFirst(row, "Stampli Buy Rate (%)", "Partner Revenue Share Variable Ini Partner Buy Rate", { patterns: ["stamplibuyrate", "inipartnerbuyrate"] })).toFixed(6));
  const providedMarkupPct = Number(money(rowValueFirst(row, "Stampli Markup (%)", "Partner Revenue Share Variable Ini Partner Revenue Share Rate", { patterns: ["stamplimarkup", "inipartnerrevenuesharerate"] })).toFixed(6));
  const derivedMarkupPct = customerMarkupPct || stampliBuyRatePct ? Number((customerMarkupPct - stampliBuyRatePct).toFixed(6)) : 0;
  const effectiveMarkupPct = providedMarkupPct || derivedMarkupPct;
  const markupBaseUsd = transactionVolumeUsd || midMarketUsd;
  const calculatedAmount = markupBaseUsd > 0 && effectiveMarkupPct ? Number((markupBaseUsd * effectiveMarkupPct).toFixed(2)) : 0;
  const grossMarkup = usdDebited > 0 && midMarketUsd > 0 ? Number((usdDebited - midMarketUsd).toFixed(2)) : 0;
  const amountMatchesSheet = calculatedAmount > 0 && providedAmount > 0 ? Math.abs(calculatedAmount - providedAmount) <= STAMPLI_MARKUP_AMOUNT_TOLERANCE : false;
  const pctMatchesComponents = providedMarkupPct && derivedMarkupPct ? Math.abs(providedMarkupPct - derivedMarkupPct) <= STAMPLI_MARKUP_RATE_TOLERANCE : false;
  let usedAmount;
  if (providedAmount > 0 && (calculatedAmount <= 0 || amountMatchesSheet)) usedAmount = providedAmount;
  else if (calculatedAmount > 0) usedAmount = calculatedAmount;
  else usedAmount = providedAmount;
  return {
    midMarketUsd,
    usedPeriodAverage,
    usdDebited,
    grossMarkup,
    customerMarkupPct,
    stampliBuyRatePct,
    providedMarkupPct,
    derivedMarkupPct,
    effectiveMarkupPct,
    providedAmount,
    calculatedAmount,
    usedAmount: Number(usedAmount.toFixed(2)),
    amountMatchesSheet,
    pctMatchesComponents,
  };
}

function buildStampliFxPartnerPayoutsFromFeed(shareRows, reversalRows, period, creditCompleteLookup = {}, { includeDetailRows = true } = {}) {
  const ratioTotals = {};
  const addRatioRow = (row, month) => {
    const payeeCurrency = text(rowValueFirst(row, "** Payment For Sales DV ** Payee Amount Currency", "Currency (Payee Amount Currency)", { patterns: ["payeeamountcurrency"] })).toUpperCase();
    const payeeAmount = money(rowValueFirst(row, "** Payment For Sales DV ** Payee Amount Number", "Foreign Currency Amount (Payee Amount Number)", { patterns: ["payeeamountnumber"] }));
    const midMarketUsd = money(rowValueFirst(row, "Payment USD Equivalent Amount", "** Payment For Sales DV ** Total USD Amount Number", "** Payment For Sales DV ** USD Amount Number", { patterns: ["paymentusdequivalentamount", "totalusdamountnumber", "usdamountnumber"] }));
    if (!payeeCurrency || payeeAmount <= 0 || midMarketUsd <= 0) return;
    const key = `${month}|${payeeCurrency}`;
    ratioTotals[key] ||= { midUsd: 0, payeeAmount: 0 };
    ratioTotals[key].midUsd += midMarketUsd;
    ratioTotals[key].payeeAmount += payeeAmount;
  };

  for (const row of shareRows || []) {
    const paymentId = text(rowValueFirst(row, "** Payment For Sales DV ** Payment Id", "Payment Payment ID", "Payment ID", { patterns: ["paymentid"] }));
    if (!paymentId) continue;
    const month = monthKey(rowValueFirst(row, "Credit Complete Date", "Transaction Lookup Dates Credit Complete Timestamp Date", "Transaction Lookup Dates Credit Complete Timestamp Time", { patterns: ["creditcompletedate", "creditcompletetimestampdate", "creditcompletetimestamptime"] }) || creditCompleteLookup[paymentId] || "");
    if (!month || !matchesPeriod(month, period)) continue;
    addRatioRow(row, month);
  }
  for (const row of reversalRows || []) {
    const paymentId = text(rowValueFirst(row, "** Payment For Sales DV ** Payment Id", "Payment Payment ID", "Payment ID", { patterns: ["paymentid"] }));
    if (!paymentId) continue;
    const month = monthKey(rowValueFirst(row, "Refund Complete Date", "Refund Completed Date", "Transaction Lookup Dates Refund Complete Timestamp Date", "Transaction Lookup Dates Refund Complete Timestamp Time", { patterns: ["refundcompletedate", "refundcompleteddate", "refundcompletetimestampdate", "refundcompletetimestamptime"] }));
    if (!month || !matchesPeriod(month, period)) continue;
    addRatioRow(row, month);
  }
  const avgRatioByMonthCurrency = Object.fromEntries(Object.entries(ratioTotals).filter(([, totals]) => totals.payeeAmount > 0).map(([key, totals]) => [key, totals.midUsd / totals.payeeAmount]));

  const grouped = {};
  const detailRows = [];
  const seenShareIds = new Set();
  const seenReversalIds = new Set();
  const getEntry = (month) => {
    grouped[month] ||= {
      partner: STAMPLI_FX_PARTNER,
      period: month,
      txnCount: 0,
      shareTxnCount: 0,
      reversalTxnCount: 0,
      partnerPayout: 0,
      shareAmount: 0,
      reversalAmount: 0,
      totalUsdDebited: 0,
      totalMidMarketUsd: 0,
      shareTotalUsdDebited: 0,
      shareTotalMidMarketUsd: 0,
      reversalTotalUsdDebited: 0,
      reversalTotalMidMarketUsd: 0,
      totalGrossMarkup: 0,
      totalVariableSpreadCost: 0,
      totalPerTxnFee: 0,
      totalCompanyMarkup: 0,
      variableSpreadRate: 0,
      usedPeriodAverageCount: 0,
      missingMidMarketCount: 0,
      missingCustomerChargeCount: 0,
      negativePayoutTxnCount: 0,
      validationCheckedCount: 0,
      validationAmountMismatchCount: 0,
      validationPctCheckedCount: 0,
      validationPctMismatchCount: 0,
      validationAmountDelta: 0,
      skippedBlankPaymentIdCount: 0,
      note: "Direct Stampli FX revenue-share feed",
    };
    return grouped[month];
  };

  for (const row of shareRows || []) {
    const paymentId = text(rowValueFirst(row, "** Payment For Sales DV ** Payment Id", "Payment Payment ID", "Payment ID", { patterns: ["paymentid"] }));
    const creditCompleteValue = rowValueFirst(row, "Credit Complete Date", "Transaction Lookup Dates Credit Complete Timestamp Date", "Transaction Lookup Dates Credit Complete Timestamp Time", { patterns: ["creditcompletedate", "creditcompletetimestampdate", "creditcompletetimestamptime"] });
    const month = monthKey(creditCompleteValue || creditCompleteLookup[paymentId] || "");
    if (!month || !matchesPeriod(month, period)) continue;
    const entry = getEntry(month);
    if (!paymentId) {
      entry.skippedBlankPaymentIdCount += 1;
      continue;
    }
    if (seenShareIds.has(paymentId)) continue;
    seenShareIds.add(paymentId);
    const calc = calculateStampliMarkupFromFeedRow(row, month, avgRatioByMonthCurrency);
    entry.shareTxnCount += 1;
    entry.txnCount += 1;
    entry.shareAmount += calc.usedAmount;
    entry.partnerPayout += calc.usedAmount;
    entry.totalUsdDebited += calc.usdDebited;
    entry.totalMidMarketUsd += calc.midMarketUsd;
    entry.shareTotalUsdDebited += calc.usdDebited;
    entry.shareTotalMidMarketUsd += calc.midMarketUsd;
    entry.totalGrossMarkup += calc.grossMarkup;
    if (calc.usedPeriodAverage) entry.usedPeriodAverageCount += 1;
    if (calc.midMarketUsd <= 0) entry.missingMidMarketCount += 1;
    if (calc.usdDebited <= 0) entry.missingCustomerChargeCount += 1;
    if (calc.providedAmount > 0 && calc.calculatedAmount > 0) {
      entry.validationCheckedCount += 1;
      entry.validationAmountDelta += Number((calc.calculatedAmount - calc.providedAmount).toFixed(2));
      if (!calc.amountMatchesSheet) entry.validationAmountMismatchCount += 1;
    }
    if (calc.providedMarkupPct && calc.derivedMarkupPct) {
      entry.validationPctCheckedCount += 1;
      if (!calc.pctMatchesComponents) entry.validationPctMismatchCount += 1;
    }
    if (includeDetailRows) {
      detailRows.push({
        detailCategory: "transaction",
        detailSource: "stampli_fx_revenue_share",
        stampliFxDirection: "share",
        partner: STAMPLI_FX_PARTNER,
        period: month,
        paymentId,
        txnType: "FX",
        speedFlag: "Standard",
        processingMethod: "Wire",
        payerFunding: "Bank",
        payeeFunding: "Bank",
        payerCcy: "USD",
        payeeCcy: text(rowValueFirst(row, "** Payment For Sales DV ** Payee Amount Currency", "Currency (Payee Amount Currency)", { patterns: ["payeeamountcurrency"] })).toUpperCase(),
        payerCountry: "",
        payeeCountry: normalizeCountryCode(rowValueFirst(row, "** Payment For Sales DV ** Payee Country", "Payee Country", { patterns: ["payeecountry"] })),
        accountId: text(rowValueFirst(row, "** Payment For Sales DV ** Payer Account ID", "Account ID", { patterns: ["payeraccountid", "accountid"] })),
        paymentType: "FX",
        submissionDate: isoValue(rowValueFirst(row, "Date of Payment Submission", "** Payment For Sales DV ** Time Created Date", { patterns: ["dateofpaymentsubmission", "timecreateddate"] })),
        creditCompleteDate: isoValue(creditCompleteValue),
        payerEmail: text(row["Payer Email"]),
        payerBusinessName: text(row["Payer Business Name"]),
        payeeEmail: text(row["Payee Email"]),
        payeeBusinessName: text(row["Payee Business Name"]),
        payeeAmountCurrency: text(rowValueFirst(row, "** Payment For Sales DV ** Payee Amount Currency", "Currency (Payee Amount Currency)", { patterns: ["payeeamountcurrency"] })).toUpperCase(),
        payeeAmount: Number(money(rowValueFirst(row, "** Payment For Sales DV ** Payee Amount Number", "Foreign Currency Amount (Payee Amount Number)", { patterns: ["payeeamountnumber"] })).toFixed(2)),
        usdAmountDebited: calc.usdDebited,
        paymentUsdEquivalentAmount: calc.midMarketUsd,
        customerMarkupPct: calc.customerMarkupPct,
        stampliBuyRatePct: calc.stampliBuyRatePct,
        stampliMarkupPct: calc.providedMarkupPct,
        stampliMarkupPctCalculated: calc.derivedMarkupPct,
        stampliMarkupPctUsed: calc.effectiveMarkupPct,
        stampliMarkupPctMatchesSheet: calc.pctMatchesComponents,
        stampliMarkupAmount: calc.usedAmount,
        stampliMarkupAmountProvided: calc.providedAmount,
        stampliMarkupAmountCalculated: calc.calculatedAmount,
        stampliMarkupAmountMatchesSheet: calc.amountMatchesSheet,
        midMarketFallbackUsed: calc.usedPeriodAverage,
      });
    }
  }

  for (const row of reversalRows || []) {
    const refundCompleteValue = rowValueFirst(row, "Refund Complete Date", "Refund Completed Date", "Transaction Lookup Dates Refund Complete Timestamp Date", "Transaction Lookup Dates Refund Complete Timestamp Time", { patterns: ["refundcompletedate", "refundcompleteddate", "refundcompletetimestampdate", "refundcompletetimestamptime"] });
    const month = monthKey(refundCompleteValue);
    if (!month || !matchesPeriod(month, period)) continue;
    const paymentId = text(rowValueFirst(row, "** Payment For Sales DV ** Payment Id", "Payment Payment ID", "Payment ID", { patterns: ["paymentid"] }));
    const entry = getEntry(month);
    if (!paymentId) {
      entry.skippedBlankPaymentIdCount += 1;
      continue;
    }
    if (seenReversalIds.has(paymentId)) continue;
    seenReversalIds.add(paymentId);
    const calc = calculateStampliMarkupFromFeedRow(row, month, avgRatioByMonthCurrency);
    entry.reversalTxnCount += 1;
    entry.txnCount += 1;
    entry.reversalAmount += calc.usedAmount;
    entry.partnerPayout -= calc.usedAmount;
    entry.totalUsdDebited += calc.usdDebited;
    entry.totalMidMarketUsd += calc.midMarketUsd;
    entry.reversalTotalUsdDebited += calc.usdDebited;
    entry.reversalTotalMidMarketUsd += calc.midMarketUsd;
    entry.totalGrossMarkup += calc.grossMarkup;
    if (calc.usedPeriodAverage) entry.usedPeriodAverageCount += 1;
    if (calc.midMarketUsd <= 0) entry.missingMidMarketCount += 1;
    if (calc.usdDebited <= 0) entry.missingCustomerChargeCount += 1;
    if (calc.providedAmount > 0 && calc.calculatedAmount > 0) {
      entry.validationCheckedCount += 1;
      entry.validationAmountDelta += Number((calc.calculatedAmount - calc.providedAmount).toFixed(2));
      if (!calc.amountMatchesSheet) entry.validationAmountMismatchCount += 1;
    }
    if (calc.providedMarkupPct && calc.derivedMarkupPct) {
      entry.validationPctCheckedCount += 1;
      if (!calc.pctMatchesComponents) entry.validationPctMismatchCount += 1;
    }
    if (includeDetailRows) {
      detailRows.push({
        detailCategory: "transaction",
        detailSource: "stampli_fx_reversal",
        stampliFxDirection: "reversal",
        partner: STAMPLI_FX_PARTNER,
        period: month,
        paymentId,
        txnType: "FX",
        speedFlag: "Standard",
        processingMethod: "Wire",
        payerFunding: "Bank",
        payeeFunding: "Bank",
        payerCcy: "USD",
        payeeCcy: text(rowValueFirst(row, "** Payment For Sales DV ** Payee Amount Currency", "Currency (Payee Amount Currency)", { patterns: ["payeeamountcurrency"] })).toUpperCase(),
        payerCountry: "",
        payeeCountry: normalizeCountryCode(rowValueFirst(row, "** Payment For Sales DV ** Payee Country", "Payee Country", { patterns: ["payeecountry"] })),
        accountId: text(rowValueFirst(row, "** Payment For Sales DV ** Payer Account ID", "Account ID", { patterns: ["payeraccountid", "accountid"] })),
        paymentType: "FX Reversal",
        submissionDate: isoValue(rowValueFirst(row, "Payment Submission Date", "** Payment For Sales DV ** Time Created Date", { patterns: ["paymentsubmissiondate", "timecreateddate"] })),
        creditCompleteDate: isoValue(rowValueFirst(row, "Credit Complete Date", "Transaction Lookup Dates Credit Complete Timestamp Date", "Transaction Lookup Dates Credit Complete Timestamp Time", { patterns: ["creditcompletedate", "creditcompletetimestampdate", "creditcompletetimestamptime"] })),
        reversalDate: isoValue(refundCompleteValue),
        payerEmail: text(row["Payer Email"]),
        payerBusinessName: text(row["Payer Business Name"]),
        payeeEmail: text(row["Payee Email"]),
        payeeBusinessName: text(row["Payee Business Name"]),
        payeeAmountCurrency: text(rowValueFirst(row, "** Payment For Sales DV ** Payee Amount Currency", "Currency (Payee Amount Currency)", { patterns: ["payeeamountcurrency"] })).toUpperCase(),
        payeeAmount: Number(money(rowValueFirst(row, "** Payment For Sales DV ** Payee Amount Number", "Foreign Currency Amount (Payee Amount Number)", { patterns: ["payeeamountnumber"] })).toFixed(2)),
        usdAmountDebited: calc.usdDebited,
        paymentUsdEquivalentAmount: calc.midMarketUsd,
        customerMarkupPct: calc.customerMarkupPct,
        stampliBuyRatePct: calc.stampliBuyRatePct,
        stampliMarkupPct: calc.providedMarkupPct,
        stampliMarkupPctCalculated: calc.derivedMarkupPct,
        stampliMarkupPctUsed: calc.effectiveMarkupPct,
        stampliMarkupPctMatchesSheet: calc.pctMatchesComponents,
        stampliMarkupAmount: -calc.usedAmount,
        stampliMarkupAmountProvided: -calc.providedAmount,
        stampliMarkupAmountCalculated: -calc.calculatedAmount,
        stampliMarkupAmountMatchesSheet: calc.amountMatchesSheet,
        midMarketFallbackUsed: calc.usedPeriodAverage,
      });
    }
  }

  const output = [];
  for (const month of Object.keys(grouped).sort()) {
    const entry = grouped[month];
    const noteParts = ["Direct Stampli FX revenue-share feed"];
    if (entry.shareTxnCount || entry.reversalTxnCount) noteParts.push(`${entry.shareTxnCount} payout txn(s), ${entry.reversalTxnCount} reversal txn(s)`);
    if (entry.validationCheckedCount) noteParts.push(`Calculated markup matched sheet on ${entry.validationCheckedCount - entry.validationAmountMismatchCount}/${entry.validationCheckedCount} amount checks`);
    if (entry.validationPctCheckedCount) noteParts.push(`Markup-rate formula matched on ${entry.validationPctCheckedCount - entry.validationPctMismatchCount}/${entry.validationPctCheckedCount} rows`);
    if (entry.validationAmountMismatchCount) noteParts.push(`Used calculated amount on ${entry.validationAmountMismatchCount} row(s) where sheet amount differed by ${entry.validationAmountDelta.toFixed(2)} total`);
    if (entry.usedPeriodAverageCount) noteParts.push(`Used period-average mid-market fallback for ${entry.usedPeriodAverageCount} txn(s)`);
    if (entry.missingMidMarketCount) noteParts.push(`${entry.missingMidMarketCount} txn(s) missing mid-market equivalent`);
    if (entry.skippedBlankPaymentIdCount) noteParts.push(`Skipped ${entry.skippedBlankPaymentIdCount} blank footer row(s)`);
    entry.partnerPayout = Number(entry.partnerPayout.toFixed(2));
    entry.shareAmount = Number(entry.shareAmount.toFixed(2));
    entry.reversalAmount = Number(entry.reversalAmount.toFixed(2));
    entry.totalUsdDebited = Number(entry.totalUsdDebited.toFixed(2));
    entry.totalMidMarketUsd = Number(entry.totalMidMarketUsd.toFixed(2));
    entry.shareTotalUsdDebited = Number(entry.shareTotalUsdDebited.toFixed(2));
    entry.shareTotalMidMarketUsd = Number(entry.shareTotalMidMarketUsd.toFixed(2));
    entry.reversalTotalUsdDebited = Number(entry.reversalTotalUsdDebited.toFixed(2));
    entry.reversalTotalMidMarketUsd = Number(entry.reversalTotalMidMarketUsd.toFixed(2));
    entry.totalGrossMarkup = Number(entry.totalGrossMarkup.toFixed(2));
    entry.validationAmountDelta = Number(entry.validationAmountDelta.toFixed(2));
    entry.note = noteParts.join("; ");
    output.push(entry);
  }
  return [output, detailRows];
}

function finalizeOfflineTransactions(paymentAggs, periodsSeen, { includeDetailRows = true } = {}) {
  const grouped = new Map();
  const accountActivity = {};
  const settlementDays = {};
  const detailRows = [];
  const unmatchedPaymentIds = new Set();
  const unmatchedExamples = new Map();

  for (const [paymentId, agg] of paymentAggs.entries()) {
    const first = agg.row;
    const partner = agg.partner;
    if (!partner) {
      unmatchedPaymentIds.add(paymentId);
      const example = [text(first["Payer Business Name"]), text(first["Payee Business Name"])].filter(Boolean).join(" / ") || "Unknown";
      unmatchedExamples.set(example, (unmatchedExamples.get(example) || 0) + 1);
      continue;
    }
    const month = agg.month;
    const rawTxnType = rowValueFirst(first, "** Payment For Sales DV ** Txn Type (Dom/Fx/Abroad..", "Txn Type", "Payment Type", { patterns: ["txntype", "paymenttype"] });
    const paymentType = text(rowValueFirst(first, "** Payment For Sales DV ** Payment Type", "Payment Type", { patterns: ["paymenttype"] }));
    const payerFunding = titleCaseFunding(rowValueFirst(first, "** Payment For Sales DV ** Payer Funding Method Type", "Payer Funding Method", { patterns: ["payerfundingmethodtype", "payerfundingmethod"] }));
    const payeeFunding = titleCaseFunding(rowValueFirst(first, "** Payment For Sales DV ** Payee Funding Method Type", "Payee Funding Method", { patterns: ["payeefundingmethodtype", "payeefundingmethod"] }));
    const payerCcy = text(rowValueFirst(first, "** Payment For Sales DV ** Payer Amount Currency", "Payer Amount Currency", { patterns: ["payeramountcurrency"] })) || "USD";
    const payeeCcy = text(rowValueFirst(first, "** Payment For Sales DV ** Payee Amount Currency", "Currency (Payee Amount Currency)", "Payee Amount Currency", { patterns: ["payeeamountcurrency"] })) || "USD";
    const payerCountry = normalizeCountryCode(rowValueFirst(first, "** Payment For Sales DV ** Payer Country", "Payer Country", { patterns: ["payercountry"] }));
    const payeeCountry = normalizeCountryCode(rowValueFirst(first, "** Payment For Sales DV ** Payee Country", "Payee Country", { patterns: ["payeecountry"] }));
    const txnType = deriveContractTxnType(rawTxnType, payerCcy, payeeCcy, payerCountry, payeeCountry, paymentType);
    const speedFlag = normalizeSpeed(agg.isRtp, agg.fasterAch);
    const processingMethod = normalizeProcessingMethod(txnType, speedFlag, agg.methods);
    const accountId = text(rowValueFirst(first, "** Payment For Sales DV ** Payer Account ID", "Account ID", { patterns: ["payeraccountid", "accountid"] }));
    const submissionValue = rowValueFirst(first, "Date of Payment Submission", "** Payment For Sales DV ** Time Created Date", { patterns: ["dateofpaymentsubmission", "timecreateddate"] });
    const creditComplete = rowValueFirst(first, "Credit Complete Date", "Transaction Lookup Dates Credit Complete Timestamp Date", "Transaction Lookup Dates Credit Complete Timestamp Time", { patterns: ["creditcompletedate", "creditcompletetimestampdate", "creditcompletetimestamptime"] });
    const payeeAmount = Number(money(rowValueFirst(first, "** Payment For Sales DV ** Payee Amount Number", "Foreign Currency Amount (Payee Amount Number)", { patterns: ["payeeamountnumber"] })).toFixed(2));
    const usdAmountDebited = Number(money(rowValueFirst(first, "USD Amount Debited to the Customer", "** Payment For Sales DV ** Total USD Amount Number", "** Payment For Sales DV ** USD Amount Number", { patterns: ["usdamountdebited", "totalusdamountnumber", "usdamountnumber"] })).toFixed(2));
    const paymentUsdEquivalentAmount = Number(money(rowValueFirst(first, "Payment USD Equivalent Amount", "** Payment For Sales DV ** Total USD Amount Number", "** Payment For Sales DV ** USD Amount Number", { patterns: ["paymentusdequivalentamount", "totalusdamountnumber", "usdamountnumber"] })).toFixed(2));
    const estRevenue = Number(money(first.__estRevenue).toFixed(2));
    const key = [partner, month, txnType, speedFlag, payerFunding, payeeFunding, payerCcy, payeeCcy, payerCountry, payeeCountry, processingMethod].join("|");
    const current = grouped.get(key) || { txnCount: 0, totalVolume: 0, estRevenue: 0 };
    current.txnCount += 1;
    current.totalVolume += paymentUsdEquivalentAmount;
    current.estRevenue += estRevenue;
    grouped.set(key, current);

    const txDate = parseDateish(creditComplete);
    if (accountId && txDate) {
      const isoDay = txDate.toISOString().slice(0, 10);
      accountActivity[accountId] ||= new Set();
      accountActivity[accountId].add(isoDay);
    }
    if (txDate) {
      const key2 = `${partner}|${month}`;
      settlementDays[key2] ||= new Set();
      settlementDays[key2].add(txDate.toISOString().slice(0, 10));
    }

    if (includeDetailRows) {
      detailRows.push({
        detailCategory: "transaction",
        detailSource: "offline_billing",
        partner,
        period: month,
        paymentId,
        txnType,
        speedFlag,
        processingMethod,
        payerFunding,
        payeeFunding,
        payerCcy,
        payeeCcy,
        payerCountry,
        payeeCountry,
        accountId,
        paymentType,
        submissionDate: isoValue(submissionValue),
        creditCompleteDate: isoValue(creditComplete),
        creditCompleteMonth: month,
        payerEmail: text(first["Payer Email"] || rowValueByPatterns(first, "payeremail", "payeraccountprimaryemail")),
        payerBusinessName: text(first["Payer Business Name"] || rowValueByPatterns(first, "payerbusinessname", "payeraccountname")),
        payeeEmail: text(first["Payee Email"] || rowValueByPatterns(first, "payeeemail", "payeeaccountprimaryemail")),
        payeeBusinessName: text(first["Payee Business Name"] || rowValueByPatterns(first, "payeebusinessname", "payeeaccountname")),
        creditRail: text(first["Credit Rail"] || rowValueByPatterns(first, "creditrail")),
        transactionProcessingMethodRaw: text(first["Transaction Processing Method"] || rowValueByPatterns(first, "transactionprocessingmethod")),
        fundingMethodUsed: text(first["Funding Method Used"] || rowValueByPatterns(first, "fundingmethodused")),
        payeeAmountCurrency: payeeCcy,
        payeeAmount,
        usdAmountDebited,
        paymentUsdEquivalentAmount,
        estRevenue,
        txnTypeRaw: text(rawTxnType),
        isRTP: agg.isRtp,
        isFasterAch: agg.fasterAch,
        partnerGroupSource: text(first["**  Initiator Customer Account ** Partner Group Source"] || rowValueByPatterns(first, "partnergroupsource")),
        initiatorStatus: text(first["** Payment For Sales DV ** Initiator Status"] || rowValueByPatterns(first, "initiatorstatus")),
        typeDefn: text(first["**  Initiator Customer Account ** Type Defn"] || rowValueByPatterns(first, "typedefn")),
      });
    }
  }

  const output = [];
  for (const [key, aggregate] of [...grouped.entries()].sort()) {
    const [partner, month, txnType, speedFlag, payerFunding, payeeFunding, payerCcy, payeeCcy, payerCountry, payeeCountry, processingMethod] = key.split("|");
    const txnCount = aggregate.txnCount;
    const totalVolume = Number(aggregate.totalVolume.toFixed(2));
    output.push({
      period: month,
      partner,
      txnType,
      speedFlag,
      minAmt: txnCount,
      maxAmt: txnCount,
      payerFunding,
      payeeFunding,
      payerCcy,
      payeeCcy,
      payerCountry,
      payeeCountry,
      processingMethod,
      txnCount,
      totalVolume,
      customerRevenue: 0,
      estRevenue: Number(aggregate.estRevenue.toFixed(2)),
      avgTxnSize: txnCount ? Number((totalVolume / txnCount).toFixed(2)) : 0,
    });
  }

  return [
    output,
    {
      paymentIdsProcessed: paymentAggs.size,
      paymentIdsImported: output.length,
      unmatchedPaymentIds: unmatchedPaymentIds.size,
      unmatchedExamples: [...unmatchedExamples.entries()].sort((a, b) => b[1] - a[1]).slice(0, 15),
    },
    Object.fromEntries(Object.entries(accountActivity).map(([accountId, days]) => [accountId, [...days].sort()])),
    Object.fromEntries(Object.entries(settlementDays).map(([key, days]) => [key, [...days].sort()])),
    [...periodsSeen].sort(),
    detailRows,
  ];
}

function buildOfflineTransactions(rows, period, { includeDetailRows = true } = {}) {
  const paymentAggs = new Map();
  const periodsSeen = new Set();

  for (const row of rows || []) {
    const creditCompleteValue = rowValueFirst(
      row,
      "Credit Complete Date",
      "Transaction Lookup Dates Credit Complete Timestamp Date",
      "Transaction Lookup Dates Credit Complete Timestamp Time",
      { patterns: ["creditcompletedate", "creditcompletetimestampdate", "creditcompletetimestampdate", "creditcompletetimestamptime"] }
    );
    const billingMonthValue = rowValueFirst(
      row,
      "Billing Month",
      "Billing Month Month",
      { patterns: ["billingmonthmonth", "billingmonth", "billingmo"] }
    );
    const month = monthKey(creditCompleteValue || billingMonthValue);
    if (!month || !matchesPeriod(month, period)) continue;
    const paymentId = text(rowValueFirst(row, "Payment Payment ID", "Payment ID", { patterns: ["paymentpaymentid", "paymentid"] }));
    if (!paymentId) continue;
    periodsSeen.add(month);
    const existing = paymentAggs.get(paymentId);
    const method = text(row["Transaction Processing Method"] || rowValueByPatterns(row, "transactionprocessingmethod")).toLowerCase();
    const isRtp = boolish(row["Extra Info IsRTP"] || rowValueByPatterns(row, "extrainfoisrtp", "isrtp"));
    const fasterAch = text(row["Extra Info Is Faster Ach"] || rowValueByPatterns(row, "extrainfoisfasterach", "isfasterach")) === "FasterACH";
    if (!existing) {
      paymentAggs.set(paymentId, {
        month,
        partner: inferPartner(row),
        row: buildOfflinePaymentSeed(row, { includeDetailRows }),
        isRtp,
        fasterAch,
        methods: method ? new Set([method]) : new Set(),
      });
    } else {
      if (!existing.partner) existing.partner = inferPartner(row);
      existing.isRtp = existing.isRtp || isRtp;
      existing.fasterAch = existing.fasterAch || fasterAch;
      if (method) existing.methods.add(method);
    }
  }

  return finalizeOfflineTransactions(paymentAggs, periodsSeen, { includeDetailRows });
}

function buildOfflineTransactionsFromCsv(csvSource, period, { includeDetailRows = true } = {}) {
  const paymentAggs = new Map();
  const periodsSeen = new Set();
  const iterateRows = Buffer.isBuffer(csvSource) ? iterateCsvBufferRows : iterateCsvRows;
  iterateRows(csvSource, (row) => {
    const creditCompleteValue = rowValueFirst(
      row,
      "Credit Complete Date",
      "Transaction Lookup Dates Credit Complete Timestamp Date",
      "Transaction Lookup Dates Credit Complete Timestamp Time",
      { patterns: ["creditcompletedate", "creditcompletetimestampdate", "creditcompletetimestampdate", "creditcompletetimestamptime"] }
    );
    const billingMonthValue = rowValueFirst(
      row,
      "Billing Month",
      "Billing Month Month",
      { patterns: ["billingmonthmonth", "billingmonth", "billingmo"] }
    );
    const month = monthKey(creditCompleteValue || billingMonthValue);
    if (!month || !matchesPeriod(month, period)) return;
    const paymentId = text(rowValueFirst(row, "Payment Payment ID", "Payment ID", { patterns: ["paymentpaymentid", "paymentid"] }));
    if (!paymentId) return;
    periodsSeen.add(month);
    const existing = paymentAggs.get(paymentId);
    const method = text(row["Transaction Processing Method"] || rowValueByPatterns(row, "transactionprocessingmethod")).toLowerCase();
    const isRtp = boolish(row["Extra Info IsRTP"] || rowValueByPatterns(row, "extrainfoisrtp", "isrtp"));
    const fasterAch = text(row["Extra Info Is Faster Ach"] || rowValueByPatterns(row, "extrainfoisfasterach", "isfasterach")) === "FasterACH";
    if (!existing) {
      paymentAggs.set(paymentId, {
        month,
        partner: inferPartner(row),
        row: buildOfflinePaymentSeed(row, { includeDetailRows }),
        isRtp,
        fasterAch,
        methods: method ? new Set([method]) : new Set(),
      });
      return;
    }
    if (!existing.partner) existing.partner = inferPartner(row);
    existing.isRtp = existing.isRtp || isRtp;
    existing.fasterAch = existing.fasterAch || fasterAch;
    if (method) existing.methods.add(method);
  });
  return finalizeOfflineTransactions(paymentAggs, periodsSeen, { includeDetailRows });
}

function buildOfflineReversals(rows, period, accountPartnerLookup = {}, { includeDetailRows = true } = {}) {
  const grouped = new Map();
  const unmatchedExamples = new Map();
  const periodsSeen = new Set();
  const seenPaymentIds = new Set();
  const detailRows = [];
  for (const row of rows || []) {
    const reversalValue = rowValueFirst(
      row,
      "Refund Complete Date",
      "Refund Completed Date",
      "Transaction Lookup Dates Refund Complete Timestamp Date",
      "Transaction Lookup Dates Refund Complete Timestamp Time",
      { patterns: ["refundcompletedate", "refundcompleteddate", "refundcompletetimestampdate", "refundcompletetimestamptime"] }
    );
    const billingMonthValue = rowValueFirst(
      row,
      "Partner Offline Billing Billing Month Month",
      "Billing Month Month",
      "Billing Month",
      { patterns: ["partnerofflinebillingbillingmonthmonth", "billingmonthmonth", "billingmonth", "billingmo"] }
    );
    const month = monthKey(billingMonthValue || reversalValue);
    if (!month || !matchesPeriod(month, period)) continue;
    periodsSeen.add(month);
    const paymentId = text(row["Payment ID"] || rowValueByPatterns(row, "paymentid"));
    if (!paymentId || seenPaymentIds.has(paymentId)) continue;
    seenPaymentIds.add(paymentId);
    let partner = inferPartner(row);
    if (!partner) {
      const accountId = text(rowValueFirst(row, "** Payment For Sales DV ** Payer Account ID", "Account ID", { patterns: ["payeraccountid", "accountid"] }));
      partner = normalizePartnerName(accountPartnerLookup[accountId]);
    }
    if (!partner) {
      const example = [text(row["Payer Business Name"]), text(row["Payee Business Name"])].filter(Boolean).join(" / ") || "Unknown";
      unmatchedExamples.set(example, (unmatchedExamples.get(example) || 0) + 1);
      continue;
    }
    const priority = text(row["Payment Priority"] || rowValueByPatterns(row, "paymentpriority", "payerfundingmethodtype", "payerfundingmethod", "fundingmethodused")).toLowerCase();
    const payerFunding = titleCaseFunding(priority.includes(" - ") ? priority.split(" - ")[0] : priority);
    const key = `${month}|${partner}|${payerFunding}`;
    grouped.set(key, (grouped.get(key) || 0) + 1);
    if (includeDetailRows) {
      detailRows.push({
        detailCategory: "reversal",
        detailSource: "offline_reversal",
        partner,
        period: month,
        paymentId,
        txnType: "",
        speedFlag: "",
        processingMethod: "",
        payerFunding,
        payeeFunding: "",
        payerCcy: text(rowValueFirst(row, "** Payment For Sales DV ** Payer Amount Currency", "Payer Amount Currency", { patterns: ["payeramountcurrency"] })) || "USD",
        payeeCcy: text(rowValueFirst(row, "** Payment For Sales DV ** Payee Amount Currency", "Currency (Payee Amount Currency)", "Payee Amount Currency", { patterns: ["payeeamountcurrency"] })) || "USD",
        payerCountry: normalizeCountryCode(rowValueFirst(row, "** Payment For Sales DV ** Payer Country", "Payer Country", { patterns: ["payercountry"] })),
        payeeCountry: normalizeCountryCode(rowValueFirst(row, "** Payment For Sales DV ** Payee Country", "Payee Country", { patterns: ["payeecountry"] })),
        accountId: text(rowValueFirst(row, "** Payment For Sales DV ** Payer Account ID", "Account ID", { patterns: ["payeraccountid", "accountid"] })),
        paymentType: text(row["Payment Type"] || rowValueByPatterns(row, "paymenttype", "txntype")),
        submissionDate: isoValue(rowValueFirst(row, "Date of Payment Submission", "** Payment For Sales DV ** Time Created Date", { patterns: ["dateofpaymentsubmission", "timecreateddate"] })),
        billingMonth: month,
        reversalDate: isoValue(reversalValue),
        payerEmail: text(row["Payer Email"] || rowValueByPatterns(row, "payeremail", "payeraccountprimaryemail")),
        payerBusinessName: text(row["Payer Business Name"] || rowValueByPatterns(row, "payerbusinessname", "payeraccountname")),
        payeeEmail: text(row["Payee Email"] || rowValueByPatterns(row, "payeeemail", "payeeaccountprimaryemail")),
        payeeBusinessName: text(row["Payee Business Name"] || rowValueByPatterns(row, "payeebusinessname", "payeeaccountname")),
        paymentPriority: text(row["Payment Priority"] || rowValueByPatterns(row, "paymentpriority")),
        payeeAmountCurrency: text(rowValueFirst(row, "Foreign Currency Amount (Payee Amount Number)", { patterns: ["payeeamountnumber"] })),
        payeeAmount: Number(money(row["Foreign Currency Amount (Payee Amount Number)"] || rowValueByPatterns(row, "payeeamountnumber")).toFixed(2)),
        usdAmountDebited: Number(money(row["USD Amount Debited to the Customer"] || rowValueByPatterns(row, "usdamountdebited", "payeramountnumber")).toFixed(2)),
        paymentUsdEquivalentAmount: Number(money(row["Payment USD Equivalent Amount"] || rowValueByPatterns(row, "paymentusdequivalentamount", "usdamountnumber")).toFixed(2)),
      });
    }
  }
  const output = [...grouped.entries()].sort().map(([key, reversalCount]) => {
    const [month, partner, payerFunding] = key.split("|");
    return { period: month, partner, payerFunding, reversalCount };
  });
  return [output, { unmatchedExamples: [...unmatchedExamples.entries()].sort((a, b) => b[1] - a[1]).slice(0, 15) }, [...periodsSeen].sort(), detailRows];
}

function mergeAccountActivity(existing, incoming) {
  const merged = {};
  for (const [accountId, days] of Object.entries(existing || {})) merged[accountId] = [...new Set(days || [])].sort();
  for (const [accountId, days] of Object.entries(incoming || {})) merged[accountId] = [...new Set([...(merged[accountId] || []), ...(days || [])])].sort();
  return merged;
}

function contextRegisteredAccountRows(context) {
  const rows = context?.registeredAccountRows;
  return Array.isArray(rows) ? rows.filter((row) => row && typeof row === "object").map((row) => ({ ...row })) : [];
}

function enumeratePeriodRange(startPeriod, endPeriod) {
  if (!startPeriod || !endPeriod) return [];
  const periods = [];
  let [year, month] = startPeriod.split("-").map(Number);
  const [endYear, endMonth] = endPeriod.split("-").map(Number);
  while (year < endYear || (year === endYear && month <= endMonth)) {
    periods.push(`${year.toString().padStart(4, "0")}-${month.toString().padStart(2, "0")}`);
    if (month === 12) { year += 1; month = 1; } else month += 1;
  }
  return periods;
}

function deriveVirtualAccountPeriods(registerRows, accountActivity, settlementDays, fallbackPeriod) {
  const explicitPeriod = text(fallbackPeriod);
  if (explicitPeriod) return [explicitPeriod];
  const dates = [];
  for (const row of registerRows || []) {
    const joinDate = parseDateishFromRow(row, "Join Date Time", { patterns: ["joindatetime"] });
    if (joinDate) dates.push(joinDate);
  }
  for (const dayValues of Object.values(accountActivity || {})) {
    for (const day of dayValues || []) {
      const parsed = parseDateish(day);
      if (parsed) dates.push(parsed);
    }
  }
  for (const dayValues of Object.values(settlementDays || {})) {
    for (const day of dayValues || []) {
      const parsed = parseDateish(day);
      if (parsed) dates.push(parsed);
    }
  }
  if (!dates.length) return [];
  const sorted = dates.sort((a, b) => a - b);
  const startPeriod = monthKey(sorted[0]);
  const endPeriod = monthKey(sorted[sorted.length - 1]);
  return enumeratePeriodRange(startPeriod, endPeriod);
}

function filterRowsForSummary(rows) {
  if (!rows?.length) return false;
  const keys = new Set(Object.keys(rows[0] || {}));
  return keys.has("Partner Net Revenue Share") || keys.has("Revenue Owed") || keys.has("Monthly Minimum Revenue");
}

function normalizeNumber(value) {
  if (value === null || value === undefined || value === "" || value === false) return null;
  const number = Number(value);
  if (!Number.isFinite(number)) return null;
  if (Math.abs(number - Math.round(number)) < 1e-9) return Math.round(number);
  return Number(number.toFixed(2));
}

function summarizeSectionGroups(rows, section) {
  const fields = SECTION_CHANGE_FIELDS[section] || [];
  const grouped = {};
  for (const row of rows || []) {
    const partner = text(row.partnerGroup || row.partner);
    const period = text(row.period || row.refundPeriod || row.creditCompleteMonth);
    if (!partner && !period) continue;
    const key = `${partner}|${period}`;
    grouped[key] ||= { partner, period, rows: 0 };
    grouped[key].rows += 1;
    for (const field of fields) {
      const number = normalizeNumber(row[field]);
      if (number === null) continue;
      grouped[key][field] = normalizeNumber((grouped[key][field] || 0) + number);
    }
  }
  return grouped;
}

function compactMetricMap(values) {
  const compacted = {};
  for (const [key, value] of Object.entries(values || {})) {
    if (key === "partner" || key === "period") continue;
    if (key === "rows") {
      compacted[key] = Number(value || 0);
      continue;
    }
    if (value !== null && value !== undefined && value !== 0 && value !== 0.0 && value !== "") compacted[key] = value;
  }
  if (!("rows" in compacted)) compacted.rows = Number(values?.rows || 0);
  return compacted;
}

function revenueSourceKey(row) {
  return text(row.revenueSource || "summary");
}

function buildSectionReplacePredicate(section, fileType, period, incomingRows) {
  const rows = incomingRows || [];
  const targetPeriods = new Set(rows.map((row) => text(row.period || row.refundPeriod || row.creditCompleteMonth)).filter(Boolean));
  if (period) targetPeriods.add(period);
  const periodMatches = (row) => targetPeriods.size && targetPeriods.has(text(row.period || row.refundPeriod || row.creditCompleteMonth));
  if (section === "ltxn") {
    if (fileType === "partner_offline_billing") return (row) => periodMatches(row) && !row.revenueBasis && !row.directInvoiceSource;
    if (["partner_rev_share_v2", "partner_revenue_share", "revenue_share_report"].includes(fileType)) return (row) => periodMatches(row) && Boolean(row.revenueBasis);
    return (row) => periodMatches(row);
  }
  if (section === "lrev") return (row) => periodMatches(row);
  if (section === "lva") {
    const partners = new Set(rows.map((row) => row.partner).filter(Boolean));
    return (row) => periodMatches(row) && (!partners.size || partners.has(row.partner));
  }
  if (section === "lrs") {
    const partnerSourcePairs = new Set(rows.filter((row) => row.partner).map((row) => `${row.partner}|${revenueSourceKey(row)}`));
    return (row) => periodMatches(row) && (!partnerSourcePairs.size || partnerSourcePairs.has(`${row.partner}|${revenueSourceKey(row)}`));
  }
  if (section === "lfxp") {
    const partners = new Set(rows.map((row) => row.partner).filter(Boolean));
    return (row) => periodMatches(row) && (!partners.size || partners.has(row.partner));
  }
  return () => false;
}

function ensureRowIds(rows) {
  return (rows || []).map((row) => (row?.id ? { ...row } : { id: Math.random().toString(36).slice(2, 8), ...row }));
}

function replaceRows(existingRows, incomingRows, shouldRemove) {
  const preserved = (existingRows || []).filter((row) => !shouldRemove(row));
  return [...preserved, ...ensureRowIds(incomingRows)];
}

function serializeOfflineContext(accountActivity, settlementDays) {
  return {
    accountActivity: Object.fromEntries(Object.entries(accountActivity || {}).map(([accountId, days]) => [accountId, [...new Set(days || [])].sort()])),
    settlementDays: Object.fromEntries(Object.entries(settlementDays || {}).map(([key, days]) => [key, [...new Set(days || [])].sort()])),
  };
}

function deserializeOfflineContext(context) {
  const normalized = context || {};
  return [normalized.accountActivity || {}, normalized.settlementDays || {}];
}

function mergeLookerImportContext(snapshot, contextUpdate) {
  if (!contextUpdate || typeof contextUpdate !== "object" || !Object.keys(contextUpdate).length) return;
  snapshot.lookerImportContext = { ...(snapshot.lookerImportContext || {}), ...contextUpdate };
}

function mergeLookerDetailOverrides(snapshot, detailRows, period) {
  const rows = detailRows || [];
  if (!rows.length) return;
  const incomingSources = new Set(rows.map((row) => row.detailSource || row.detailCategory || "uploaded_looker_detail"));
  const existingRows = snapshot.lookerImportedDetailRows || [];
  const targetPeriods = new Set(rows.map((row) => text(row.period)).filter(Boolean));
  if (period) targetPeriods.add(period);
  snapshot.lookerImportedDetailRows = [
    ...existingRows.filter((row) => !(targetPeriods.has(text(row.period)) && incomingSources.has(row.detailSource || row.detailCategory || "uploaded_looker_detail"))),
    ...rows,
  ];
}

function applyLookerSectionUpdate(snapshot, section, fileType, period, incomingRows) {
  const predicate = buildSectionReplacePredicate(section, fileType, period, incomingRows || []);
  if (["ltxn", "lrev", "lva", "lrs", "lfxp"].includes(section)) {
    snapshot[section] = replaceRows(snapshot[section] || [], incomingRows || [], predicate);
  }
}

function buildSectionChangeSummary(beforeSnapshot, afterSnapshot, section, fileType, period, incomingRows) {
  const predicate = buildSectionReplacePredicate(section, fileType, period, incomingRows || []);
  const beforeRows = (beforeSnapshot?.[section] || []).filter((row) => predicate(row));
  const afterRows = (afterSnapshot?.[section] || []).filter((row) => predicate(row));
  const beforeGroups = summarizeSectionGroups(beforeRows, section);
  const afterGroups = summarizeSectionGroups(afterRows, section);
  const changedGroups = [];
  for (const key of [...new Set([...Object.keys(beforeGroups), ...Object.keys(afterGroups)])].sort()) {
    const beforeValues = beforeGroups[key] || { partner: key.split("|")[0], period: key.split("|")[1], rows: 0 };
    const afterValues = afterGroups[key] || { partner: key.split("|")[0], period: key.split("|")[1], rows: 0 };
    if (JSON.stringify(compactMetricMap(beforeValues)) === JSON.stringify(compactMetricMap(afterValues))) continue;
    const delta = {};
    for (const field of ["rows", ...(SECTION_CHANGE_FIELDS[section] || [])]) {
      const beforeNumber = normalizeNumber(beforeValues[field] || 0) || 0;
      const afterNumber = normalizeNumber(afterValues[field] || 0) || 0;
      if (beforeNumber === afterNumber) continue;
      delta[field] = normalizeNumber(afterNumber - beforeNumber);
    }
    changedGroups.push({
      partner: key.split("|")[0],
      period: key.split("|")[1],
      before: compactMetricMap(beforeValues),
      after: compactMetricMap(afterValues),
      delta,
    });
  }
  return { section, label: SECTION_CHANGE_LABELS[section] || section, changedGroupCount: changedGroups.length, changedGroups };
}

function buildLookerImportChangeSummary(beforeSnapshot, afterSnapshot, result) {
  const sectionSummaries = Object.entries(result.sections || {}).map(([section, rows]) => buildSectionChangeSummary(beforeSnapshot, afterSnapshot, section, result.fileType || "", result.period || "", rows || [])).filter((summary) => summary.changedGroupCount);
  const changedPartnerPeriods = new Set();
  for (const section of sectionSummaries) for (const group of section.changedGroups || []) changedPartnerPeriods.add(`${group.partner}|${group.period}`);
  const changedPartners = [...new Set([...changedPartnerPeriods].map((value) => value.split("|")[0]).filter(Boolean))].sort();
  const changedPeriods = [...new Set([...changedPartnerPeriods].map((value) => value.split("|")[1]).filter(Boolean))].sort();
  return {
    totalChangedGroups: changedPartnerPeriods.size,
    partnerCount: changedPartners.length,
    periodCount: changedPeriods.length,
    changedPartners,
    changedPeriods,
    sections: sectionSummaries,
  };
}

function updateLookerImportAudit(snapshot, result, runId, savedAt, source = "n8n-cloud") {
  const audit = snapshot.lookerImportAudit && typeof snapshot.lookerImportAudit === "object" ? { ...snapshot.lookerImportAudit } : {};
  const byFileType = { ...(audit.byFileType || {}) };
  const record = {
    fileType: String(result.fileType || ""),
    fileLabel: String(result.fileLabel || ""),
    period: String(result.period || ""),
    savedAt,
    source,
    warnings: [...(result.warnings || [])],
    sectionCounts: { ...((result.stats || {}).sectionCounts || {}) },
    stats: { ...(result.stats || {}) },
    changeSummary: { ...(result.changeSummary || {}) },
    sourceMetadata: { ...(result.sourceMetadata || {}) },
  };
  byFileType[record.fileType] = record;
  snapshot.lookerImportAudit = { ...audit, byFileType, latestRun: { runId, period: record.period, savedAt, source } };
}

function filterRowsForPeriod(rows, period) {
  return (rows || []).filter((row) => text(row.period) === text(period));
}

export function parseLookerCsvImport({ fileType, period = "", csvText = "", csvBuffer = null, context = {}, includeDetailRows = true }) {
  const sections = {};
  let detailRows = [];
  const warnings = [];
  const contextUpdate = {};
  const stats = { fileType, period };

  if (fileType === "partner_offline_billing") {
    const offlineCsvSource = Buffer.isBuffer(csvBuffer) ? csvBuffer : csvText;
    const [ltxn, meta, accountActivity, settlementDays, periodsSeen, parsedDetailRows] = buildOfflineTransactionsFromCsv(offlineCsvSource, null, { includeDetailRows });
    sections.ltxn = ltxn;
    detailRows = parsedDetailRows;
    contextUpdate.offlineContext = serializeOfflineContext(accountActivity, settlementDays);
    stats.periodsSeen = periodsSeen;
    stats.paymentIdsProcessed = meta.paymentIdsProcessed || 0;
    stats.paymentIdsImported = meta.paymentIdsImported || 0;
    if (meta.unmatchedPaymentIds) warnings.push(`${meta.unmatchedPaymentIds} payment IDs could not be matched to a partner.`);
  } else if (fileType === "partner_offline_billing_reversals") {
    const rows = parseCsv(csvText);
    const registeredRows = contextRegisteredAccountRows(context);
    const accountPartnerLookup = Object.fromEntries(registeredRows.map((row) => [text(row["Account Id"] || row["ACCOUNT_ID"]), normalizePartnerName(row["Partner Name"] || row["Partner Group Source"] || row["Partner Group With Bank"])]).filter(([accountId, partner]) => accountId && partner));
    const [lrev, meta, periodsSeen, parsedDetailRows] = buildOfflineReversals(rows, null, accountPartnerLookup, { includeDetailRows });
    sections.lrev = lrev;
    detailRows = parsedDetailRows;
    stats.periodsSeen = periodsSeen;
    stats.reversalRows = lrev.length;
    if ((meta.unmatchedExamples || []).length) warnings.push("Some reversal rows could not be matched to a partner.");
  } else if (["all_registered_accounts", "all_registered_accounts_offline", "all_registered_accounts_rev_share", "vba_accounts"].includes(fileType)) {
    const rows = parseCsv(csvText);
    const [accountActivity, settlementDays] = deserializeOfflineContext(context.offlineContext || {});
    const existingRegisteredRows = contextRegisteredAccountRows(context);
    const normalizedRows = normalizeRegisteredAccountRows(rows, { preferTimeCreated: fileType === "vba_accounts" });
    const mergedRegisteredRows = mergeRegisteredAccountRows(existingRegisteredRows, normalizedRows);
    contextUpdate.registeredAccountRows = mergedRegisteredRows;
    if (!Object.keys(accountActivity).length && !Object.keys(settlementDays).length) warnings.push("Partner Offline Billing context was not supplied, so dormant accounts and settlement counts may be incomplete.");
    const targetPeriods = deriveVirtualAccountPeriods(mergedRegisteredRows, accountActivity, settlementDays, "");
    const lva = buildVirtualAccountUsage(mergedRegisteredRows, accountActivity, settlementDays, targetPeriods);
    sections.lva = lva;
    stats.registeredRows = rows.length;
    stats.registeredAccountsLoaded = mergedRegisteredRows.length;
  } else if (["vba_transactions", "vba_transactions_cc", "vba_transactions_citi"].includes(fileType)) {
    const rows = parseCsv(csvText);
    const [accountActivity, settlementDays] = deserializeOfflineContext(context.offlineContext || {});
    const vbaActivity = buildVbaTransactionActivity(rows);
    const mergedActivity = mergeAccountActivity(accountActivity, vbaActivity);
    contextUpdate.offlineContext = serializeOfflineContext(mergedActivity, settlementDays);
    const registeredRows = contextRegisteredAccountRows(context);
    if (!registeredRows.length) {
      warnings.push("Registered account looks have not been imported yet, so VBA dormancy could not be recalculated.");
    } else {
      const targetPeriods = deriveVirtualAccountPeriods(registeredRows, mergedActivity, settlementDays, "");
      sections.lva = buildVirtualAccountUsage(registeredRows, mergedActivity, settlementDays, targetPeriods);
    }
    stats.vbaTransactionRows = rows.length;
  } else if (["partner_rev_share_v2", "partner_revenue_share", "revenue_share_report"].includes(fileType)) {
    const rows = parseCsv(csvText);
    const [ltxn, parsedDetailRows] = buildRevenueDetailTransactions(rows, null, { includeDetailRows });
    sections.ltxn = ltxn;
    detailRows = parsedDetailRows;
    if (filterRowsForSummary(rows)) {
      sections.lrs = buildRevenueShareSummary(rows, null, { allowBillingMonthFallback: fileType !== "revenue_share_report" });
    }
    stats.revenueRows = rows.length;
  } else if (["partner_revenue_reversal", "rev_share_reversals"].includes(fileType)) {
    const rows = parseCsv(csvText);
    sections.lrs = buildRevenueReversalSummary(rows, null);
    stats.reversalRows = rows.length;
  } else if (["stampli_fx_revenue_share", "stampli_fx_revenue_reversal"].includes(fileType)) {
    const rows = parseCsv(csvText);
    const shareRows = fileType === "stampli_fx_revenue_share" ? rows : (context.stampliFxShareRows || []);
    const reversalRows = fileType === "stampli_fx_revenue_reversal" ? rows : (context.stampliFxReversalRows || []);
    if (fileType === "stampli_fx_revenue_share") contextUpdate.stampliFxShareRows = rows;
    if (fileType === "stampli_fx_revenue_reversal") contextUpdate.stampliFxReversalRows = rows;
    const [lfxp, parsedDetailRows] = buildStampliFxPartnerPayoutsFromFeed(shareRows, reversalRows, null, context.stampliCreditCompleteLookup || {}, { includeDetailRows });
    sections.lfxp = lfxp;
    detailRows = parsedDetailRows;
    stats.shareRowsLoaded = shareRows.length;
    stats.reversalRowsLoaded = reversalRows.length;
  } else {
    throw new Error(`Unsupported Looker file type for JS runtime: ${fileType}`);
  }

  stats.sectionCounts = Object.fromEntries(Object.entries(sections).map(([section, sectionRows]) => [section, (sectionRows || []).length]));
  return {
    fileType,
    fileLabel: fileType,
    period,
    sections,
    detailRows,
    contextUpdate,
    warnings,
    stats,
  };
}

export function applyLookerImportResult(snapshot, result, { savedAt = new Date().toISOString(), runId = `n8n-${Date.now()}`, source = "n8n-cloud", includeDetailRows = true } = {}) {
  const updatedSnapshot = { ...(snapshot || {}) };
  const warnings = [...(result.warnings || [])];
  for (const [section, rows] of Object.entries(result.sections || {})) {
    const predicate = buildSectionReplacePredicate(section, String(result.fileType || ""), String(result.period || ""), rows || []);
    const existingRows = (updatedSnapshot[section] || []).filter((row) => predicate(row));
    if ((!rows || !rows.length) && existingRows.length) {
      warnings.push(`${SECTION_CHANGE_LABELS[section] || section} import returned 0 rows, so ${existingRows.length} existing stored row(s) were preserved.`);
      continue;
    }
    applyLookerSectionUpdate(updatedSnapshot, section, String(result.fileType || ""), String(result.period || ""), rows || []);
  }
  mergeLookerImportContext(updatedSnapshot, result.contextUpdate || {});
  mergeLookerDetailOverrides(updatedSnapshot, result.detailRows || [], String(result.period || ""));
  const context = updatedSnapshot.lookerImportContext || {};
  const [accountActivity, settlementDays] = deserializeOfflineContext(context.offlineContext || {});
  if (LVA_CONTEXT_FILE_TYPES.has(String(result.fileType || "")) && context.registeredAccountRows) {
    const periods = deriveVirtualAccountPeriods(context.registeredAccountRows, accountActivity, settlementDays, "");
    updatedSnapshot.lva = buildVirtualAccountUsage(context.registeredAccountRows, accountActivity, settlementDays, periods);
  }
  if (STAMPLI_FX_CONTEXT_FILE_TYPES.has(String(result.fileType || ""))) {
    const [lfxp, stampliDetailRows] = buildStampliFxPartnerPayoutsFromFeed(context.stampliFxShareRows || [], context.stampliFxReversalRows || [], null, context.stampliCreditCompleteLookup || {}, { includeDetailRows });
    updatedSnapshot.lfxp = lfxp;
    if (includeDetailRows) mergeLookerDetailOverrides(updatedSnapshot, stampliDetailRows || [], "");
  }
  result.warnings = warnings;
  result.changeSummary = buildLookerImportChangeSummary(snapshot || {}, updatedSnapshot, result);
  updatedSnapshot._saved = savedAt;
  updateLookerImportAudit(updatedSnapshot, result, runId, savedAt, source);
  return updatedSnapshot;
}

export function applyLookerCsvToWorkbook(workbookPayload, importPayload, options = {}) {
  const snapshot = workbookPayload?.snapshot && typeof workbookPayload.snapshot === "object" ? workbookPayload.snapshot : (workbookPayload || {});
  const context = snapshot.lookerImportContext || {};
  const includeDetailRows = options.includeDetailRows !== false;
  const result = parseLookerCsvImport({ ...importPayload, context, includeDetailRows });
  const nextSnapshot = applyLookerImportResult(snapshot, result, options);
  const nextWorkbook = workbookPayload?.snapshot && typeof workbookPayload.snapshot === "object"
    ? { ...workbookPayload, snapshot: nextSnapshot, savedAt: options.savedAt || new Date().toISOString() }
    : nextSnapshot;
  return { workbook: nextWorkbook, result };
}

export function fileTypeUsesFullHistory(fileType) {
  return FULL_HISTORY_FILE_TYPES.has(fileType);
}

export function normalizeCloudSyncSummary(results) {
  const reports = results || [];
  return {
    configuredCount: reports.length,
    importedCount: reports.filter((item) => item.status === "imported").length,
    errorCount: reports.filter((item) => item.status === "error").length,
    warningCount: reports.reduce((sum, item) => sum + (item.warnings || []).length, 0),
    hasWarnings: reports.some((item) => (item.warnings || []).length),
    hasErrors: reports.some((item) => item.status === "error"),
    reports,
  };
}

export function buildS3WorkbookKeys(prefix = "") {
  const normalized = text(prefix).replace(/^\/+|\/+$/g, "");
  if (!normalized) {
    return {
      workbookKey: "data/current-workbook.json",
      summaryKey: "data/looker-sync/latest-summary.json",
    };
  }
  return {
    workbookKey: `${normalized}/current-workbook.json`,
    summaryKey: `${normalized}/looker-sync/latest-summary.json`,
  };
}

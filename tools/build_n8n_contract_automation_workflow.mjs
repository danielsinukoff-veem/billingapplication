import fs from "node:fs";
import path from "node:path";

const OUTPUT_PATH = path.resolve("docs/n8n-contract-automation.workflow.json");

function slugify(value) {
  return String(value || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 64) || "node";
}

function codeNode(name, position, jsCode) {
  return {
    parameters: {
      mode: "runOnceForAllItems",
      language: "javaScript",
      jsCode,
    },
    id: slugify(name),
    name,
    type: "n8n-nodes-base.code",
    typeVersion: 2,
    position,
  };
}

function webhookNode(name, pathValue, position) {
  return {
    parameters: {
      httpMethod: "POST",
      path: pathValue,
      responseMode: "lastNode",
      options: {
        allowedOrigins: "https://billing.qa-us-west-2.veem.com",
      },
    },
    id: slugify(name),
    name,
    type: "n8n-nodes-base.webhook",
    typeVersion: 2.1,
    position,
    webhookId: pathValue,
  };
}

const sampleExtractRequestCode = String.raw`
function pdfEscape(value) {
  return String(value || "").replace(/\\/g, "\\\\").replace(/\(/g, "\\(").replace(/\)/g, "\\)");
}

function makeSimplePdfBase64(text) {
  const content = "BT /F1 12 Tf 72 720 Td (" + pdfEscape(text).slice(0, 180) + ") Tj ET\n";
  const objects = [
    "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n",
    "2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n",
    "3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >>\nendobj\n",
    "4 0 obj\n<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>\nendobj\n",
    "5 0 obj\n<< /Length " + Buffer.byteLength(content, "utf8") + " >>\nstream\n" + content + "endstream\nendobj\n",
  ];
  let pdf = "%PDF-1.4\n";
  const offsets = [0];
  for (const object of objects) {
    offsets.push(Buffer.byteLength(pdf, "utf8"));
    pdf += object;
  }
  const xrefOffset = Buffer.byteLength(pdf, "utf8");
  pdf += "xref\n0 6\n0000000000 65535 f \n";
  for (const offset of offsets.slice(1)) {
    pdf += String(offset).padStart(10, "0") + " 00000 n \n";
  }
  pdf += "trailer\n<< /Root 1 0 R /Size 6 >>\nstartxref\n" + xrefOffset + "\n%%EOF\n";
  return Buffer.from(pdf, "utf8").toString("base64");
}

return [{
  json: {
    fileName: "sample-contract.pdf",
    fileBase64: makeSimplePdfBase64("Partner: Sample Partner. Effective Date: March 1, 2026. Monthly minimum fee $1,000. Reversal fee $2.50 per reversal. FX markup 20 bps."),
    contentType: "application/pdf",
  },
}];
`;

const normalizeExtractRequestCode = String.raw`
const first = $input.first();
const raw = first?.json || {};
const body = raw.body && typeof raw.body === "object" ? raw.body : raw;

function clean(value) {
  return String(value || "").trim();
}

const fileName = clean(body.fileName || body.name || "contract.txt");
const text = clean(body.text);
const fileBase64 = clean(body.fileBase64 || body.data || body.base64);
const contentType = clean(body.contentType || body.mimeType);

if (!text && !fileBase64) {
  throw new Error("Contract extraction request needs either text or fileBase64.");
}

return [{
  json: {
    fileName,
    text,
    fileBase64,
    contentType,
    receivedAt: new Date().toISOString(),
  },
}];
`;

const buildPdfBinaryCode = String.raw`
const item = $input.first()?.json || {};

function clean(value) {
  return String(value || "").trim();
}

function decodeBase64(value) {
  try {
    return Buffer.from(String(value || ""), "base64");
  } catch {
    return null;
  }
}

if (clean(item.text)) {
  throw new Error("This endpoint is for PDF bytes. Text files are handled directly by the app before this webhook is called.");
}

const buffer = decodeBase64(item.fileBase64);
if (!buffer) throw new Error("fileBase64 could not be decoded.");
const header = buffer.subarray(0, 8).toString("utf8");
if (!header.startsWith("%PDF")) {
  throw new Error("Uploaded file is not a PDF. Send a PDF base64 payload or use text upload in the app.");
}

const fileName = clean(item.fileName) || "contract.pdf";
const mimeType = clean(item.contentType) || "application/pdf";
const binaryData = await this.helpers.prepareBinaryData(buffer, fileName, mimeType);
return [{
  json: {
    fileName,
    sourceContentType: mimeType,
  },
  binary: {
    data: binaryData,
  },
}];
`;

const normalizePdfTextResponseCode = String.raw`
const extracted = $input.first()?.json || {};
let source = {};
try {
  source = $("Build PDF Binary").first()?.json || {};
} catch {
  source = {};
}

function clean(value) {
  return String(value || "").trim();
}

function collectText(value) {
  if (!value) return "";
  if (typeof value === "string") return value;
  if (Array.isArray(value)) return value.map(collectText).filter(Boolean).join("\n");
  if (typeof value === "object") {
    return [
      value.text,
      value.data,
      value.content,
      value.pageContent,
      value.markdown,
      value.pages,
    ].map(collectText).filter(Boolean).join("\n");
  }
  return "";
}

const text = clean(collectText(extracted));
const warnings = [];
if (!text) {
  throw new Error("PDF text extraction returned 0 characters. This PDF is likely scanned/image-only or protected, so n8n Extract From File cannot read it. Use an OCR/document parser node for this file, or paste the contract text manually.");
}

return [{
  json: {
    fileName: clean(source.fileName) || clean(extracted.fileName) || "contract.pdf",
    text,
    charCount: text.length,
    warnings,
  },
}];
`;

function extractFromPdfNode(name, position) {
  return {
    parameters: {
      operation: "pdf",
      binaryPropertyName: "data",
      options: {
        joinPages: true,
        keepSource: "json",
      },
    },
    id: slugify(name),
    name,
    type: "n8n-nodes-base.extractFromFile",
    typeVersion: 1,
    position,
  };
}

const sampleParseRequestCode = String.raw`
return [{
  json: {
    fileName: "sample-contract.txt",
    text: [
      "Partner: Sample Partner",
      "Effective Date: March 1, 2026",
      "Customer shall pay a monthly minimum fee of $1,000.",
      "Company shall charge $2.50 per reversal.",
      "FX markup is 20 bps for major corridors and 35 bps for minor corridors.",
      "Settlement report is provided on the 7th Business Day of each month.",
    ].join("\n"),
  },
}];
`;

const normalizeParseRequestCode = String.raw`
const first = $input.first();
const raw = first?.json || {};
const body = raw.body && typeof raw.body === "object" ? raw.body : raw;

function clean(value) {
  return String(value || "").trim();
}

const fileName = clean(body.fileName || body.name || "contract.txt");
const text = clean(body.text || body.contractText || body.rawText);

if (!text) {
  throw new Error("Contract parse request did not include contract text.");
}

return [{
  json: {
    fileName,
    text,
    receivedAt: new Date().toISOString(),
  },
}];
`;

const deterministicParseCode = String.raw`
const input = $input.first()?.json || {};
const text = String(input.text || "");
const normalized = text.replace(/\r/g, "\n");
const compact = normalized.replace(/\s+/g, " ").trim();
const lower = compact.toLowerCase();
const warnings = [];

function clean(value) {
  return String(value || "").replace(/\s+/g, " ").trim();
}

function titleCase(value) {
  return clean(value).replace(/\b([a-z])/g, (match) => match.toUpperCase());
}

function parseDate(value) {
  const textValue = clean(value);
  if (!textValue) return "";
  const iso = textValue.match(/\b(20\d{2})[-/](\d{1,2})[-/](\d{1,2})\b/);
  if (iso) {
    return iso[1] + "-" + String(iso[2]).padStart(2, "0") + "-" + String(iso[3]).padStart(2, "0");
  }
  const date = new Date(textValue);
  if (Number.isNaN(date.getTime())) return "";
  return date.toISOString().slice(0, 10);
}

function moneyToNumber(value) {
  if (value == null) return 0;
  const raw = String(value).replace(/[,$]/g, "");
  const match = raw.match(/-?\d+(?:\.\d+)?/);
  return match ? Number(match[0]) : 0;
}

function percentToDecimal(value) {
  if (value == null) return 0;
  const raw = String(value).toLowerCase();
  const num = moneyToNumber(raw);
  if (!Number.isFinite(num)) return 0;
  if (raw.includes("bps") || raw.includes("basis point")) return num / 10000;
  return num / 100;
}

function pushUnique(array, row, keys) {
  const exists = array.some((existing) => keys.every((key) => clean(existing[key]).toLowerCase() === clean(row[key]).toLowerCase()));
  if (!exists) array.push(row);
}

function sectionAround(keyword, radius = 240) {
  const index = lower.indexOf(keyword);
  if (index < 0) return "";
  return compact.slice(Math.max(0, index - radius), Math.min(compact.length, index + radius));
}

function findMoneyNear(keywords) {
  for (const keyword of keywords) {
    const section = sectionAround(keyword, 260);
    const match = section.match(/\$\s*[\d,]+(?:\.\d{1,2})?/);
    if (match) return moneyToNumber(match[0]);
  }
  return 0;
}

function findPartnerName() {
  function normalizeCandidate(value, { allowAlias = false } = {}) {
    let candidate = clean(value)
      .replace(/[“”]/g, "\"")
      .replace(/[‘’]/g, "'")
      .replace(/\s*,?\s*(?:a|an)\s+(?:Delaware|California|Canadian|US|U\.S\.|United States|licensed|foreign)\b.*$/i, "")
      .replace(/\s+(?:and\s+its\s+affiliates|together\s+with|collectively).*$/i, "")
      .replace(/\s*\([^)]*$/i, "")
      .replace(/^[,:"'() ]+|[,:"'() .]+$/g, "");
    candidate = candidate.replace(/\b(?:Inc|Inc\.|LLC|Ltd|Ltd\.|Limited|Corp|Corp\.|Corporation|Co|Co\.)$/i, "").trim();
    if (/^trans[-\s]?fi$/i.test(candidate)) candidate = "TransFi";
    if (!candidate) return "";
    if (!allowAlias && !/^[A-Z0-9]/.test(candidate)) return "";
    const lowerCandidate = candidate.toLowerCase();
    if (/\b(agreement|services|effective|requirements|dictate|customer|company|partner|parties|background|whereas|section|exhibit|schedule|subsidiaries)\b/i.test(candidate)) return "";
    if (candidate.length > 80 || candidate.split(/\s+/).length > 8) return "";
    if (!/[a-z]/i.test(candidate)) return "";
    if (lowerCandidate === "veem") return "";
    return candidate;
  }

  const quoteNormalized = compact
    .replace(/[“”]/g, "\"")
    .replace(/[‘’]/g, "'")
    .replace(/[–—]/g, "-");

  const partnerParentheticals = quoteNormalized.match(/\([^)]*\bPartner\b[^)]*\)/gi) || [];
  for (const parenthetical of partnerParentheticals) {
    const quotedValues = [...parenthetical.matchAll(/["']([^"']{2,80})["']/g)]
      .map((match) => normalizeCandidate(match[1], { allowAlias: true }))
      .filter(Boolean)
      .filter((candidate) => !/\b(partner|company|parties)\b/i.test(candidate));
    if (quotedValues.length) return quotedValues[quotedValues.length - 1];
  }

  const partyPatterns = [
    /\b["']?Company["']?\)?\s+(?:and|&)\s+([A-Z][A-Za-z0-9&.,'() \-]{1,140}?)(?=,?\s+(?:a|an)\s+|\s+\([^)]*\bPartner\b|\s+and\s+its\s+affiliates|\s+together\s+with|[,)]|$)/i,
    /\bbetween\b[\s\S]{0,500}?\b(?:Veem|Company)\b[\s\S]{0,500}?\b(?:and|&)\s+([A-Z][A-Za-z0-9&.,'() \-]{1,140}?)(?=,?\s+(?:a|an)\s+|\s+\([^)]*\bPartner\b|\s+and\s+its\s+affiliates|\s+together\s+with|[,)]|$)/i,
    /\b(?:and|&)\s+([A-Z][A-Za-z0-9&.,'() \-]{1,140}?)(?=,?\s+(?:a|an)\s+[^.]{0,180}\([^)]*\bPartner\b)/i,
  ];
  for (const pattern of partyPatterns) {
    const match = quoteNormalized.match(pattern);
    const candidate = normalizeCandidate(match?.[1] || "");
    if (candidate) return candidate;
  }

  const explicitPatterns = [
    /\bpartner\s*(?:name)?\s*[:\-]\s*([A-Z][A-Za-z0-9&.,'() \-]{1,80})(?=$|[\n.;])/i,
    /\bcustomer\s*(?:name)?\s*[:\-]\s*([A-Z][A-Za-z0-9&.,'() \-]{1,80})(?=$|[\n.;])/i,
    /\bbetween\s+([A-Z][A-Za-z0-9&.,'() \-]{1,80})\s+(?:and|&)\s+(?:veem|company)\b/i,
  ];
  for (const pattern of explicitPatterns) {
    const match = normalized.match(pattern);
    const candidate = normalizeCandidate(match?.[1] || "");
    if (candidate) return candidate;
  }

  const fromFile = normalizeCandidate(String(input.fileName || "")
    .replace(/\.[a-z0-9]+$/i, "")
    .replace(/complete|docusign|veem|agreement|contract|pricing|invoice/gi, " ")
    .replace(/[_-]+/g, " "));
  return fromFile ? titleCase(fromFile) : "Partner";
}

function findEffectiveDate() {
  const patterns = [
    /effective\s+date\s*[:\-]?\s*([A-Za-z]+\s+\d{1,2},?\s+20\d{2}|20\d{2}[-/]\d{1,2}[-/]\d{1,2})/i,
    /effective\s+(?:as\s+of|on)\s+([A-Za-z]+\s+\d{1,2},?\s+20\d{2}|20\d{2}[-/]\d{1,2}[-/]\d{1,2})/i,
    /commencement\s+date\s*[:\-]?\s*([A-Za-z]+\s+\d{1,2},?\s+20\d{2}|20\d{2}[-/]\d{1,2}[-/]\d{1,2})/i,
  ];
  for (const pattern of patterns) {
    const match = compact.match(pattern);
    const date = parseDate(match?.[1]);
    if (date) return date;
  }
  return "";
}

function findBillingTerms() {
  const payByMatch = compact.match(/\b(?:net\s*\d+|within\s+\d+\s+days?|7th\s+business\s+day|seventh\s+business\s+day|monthly\s+in\s+arrears)\b/i);
  let billingFreq = "";
  if (/\bmonthly\b/i.test(compact)) billingFreq = "Monthly";
  else if (/\bquarterly\b/i.test(compact)) billingFreq = "Quarterly";
  else if (/\bannually|annual\b/i.test(compact)) billingFreq = "Annual";
  return {
    billingFreq,
    payBy: payByMatch ? clean(payByMatch[0]) : "",
  };
}

function amountToNumber(value) {
  const raw = clean(value).replace(/[,$]/g, "").replace(/\s+/g, "").toUpperCase();
  const match = raw.match(/-?\d+(?:\.\d+)?/);
  if (!match) return 0;
  const number = Number(match[0]);
  if (!Number.isFinite(number)) return 0;
  if (raw.includes("MM") || raw.includes("M")) return number * 1000000;
  return number;
}

function segmentBetween(startPattern, endPatterns = []) {
  const startMatch = compact.match(startPattern);
  if (!startMatch || startMatch.index == null) return "";
  const start = startMatch.index;
  let end = compact.length;
  for (const pattern of endPatterns) {
    const rest = compact.slice(start + startMatch[0].length);
    const match = rest.match(pattern);
    if (match && match.index != null) {
      end = Math.min(end, start + startMatch[0].length + match.index);
    }
  }
  return compact.slice(start, end);
}

function numberRangeFromTokens(minToken, maxToken, plusToken = "") {
  const min = amountToNumber(minToken);
  const max = plusToken || !maxToken ? 1000000000 : amountToNumber(maxToken);
  return { min, max: max || 1000000000 };
}

function parseMoneyTierRows(section, valueCount = 1) {
  const rows = [];
  const re = /\b(?:Tier\s*)?(\d+)\s+(\$?\d[\d,.]*(?:MM?|M)?)(?:\s*[–-]\s*(\$?\d[\d,.]*(?:MM?|M)?))?(\+)?\s+\$\s*(\d+(?:\.\d+)?)(?:\s+\$\s*(\d+(?:\.\d+)?))?/gi;
  let match;
  while ((match = re.exec(section))) {
    const { min, max } = numberRangeFromTokens(match[2], match[3], match[4]);
    const values = [match[5], match[6]].filter(Boolean).map((value) => Number(value));
    if (values.length >= valueCount) rows.push({ tier: Number(match[1]), min, max, values });
  }
  return rows;
}

function parsePercentTierRows(section) {
  const rows = [];
  const re = /\b(?:Tier\s*)?(\d+)\s+(\$?\d[\d,.]*(?:MM?|M)?)(?:\s*[–-]\s*(\$?\d[\d,.]*(?:MM?|M)?))?(\+)?\s+(\d+(?:\.\d+)?)\s*%/gi;
  let match;
  while ((match = re.exec(section))) {
    const { min, max } = numberRangeFromTokens(match[2], match[3], match[4]);
    rows.push({ tier: Number(match[1]), min, max, rate: Number(match[5]) / 100 });
  }
  return rows;
}

function parseBpsTierRows(section) {
  const rows = [];
  const re = /(\$?\d[\d,.]*(?:MM?|M)?)(?:\s*[–-]\s*(\$?\d[\d,.]*(?:MM?|M)?))?(\+)?\s+(\d+(?:\.\d+)?)\s*bps(?:\s*\((\d+(?:\.\d+)?)\s*%\))?/gi;
  let match;
  while ((match = re.exec(section))) {
    const { min, max } = numberRangeFromTokens(match[1], match[2], match[3]);
    const rate = match[5] ? Number(match[5]) / 100 : Number(match[4]) / 10000;
    rows.push({ min, max, rate });
  }
  return rows;
}

function extractOfflineRates() {
  const rows = [];
  const ach = segmentBetween(/ACH\s*\(Pay-in\s+or\s+Payout\)/i, [/Instant\s+US\s+Bank\s+Transfer/i]);
  for (const row of parseMoneyTierRows(ach, 2)) {
    rows.push({
      txnType: "Domestic",
      speedFlag: "Standard",
      minAmt: row.min,
      maxAmt: row.max,
      payerCcy: "USD",
      payeeCcy: "USD",
      processingMethod: "ACH",
      fee: row.values[0],
      note: "ACH next-day monthly transaction count tier " + row.tier + ".",
    });
    rows.push({
      txnType: "Domestic",
      speedFlag: "FasterACH",
      minAmt: row.min,
      maxAmt: row.max,
      payerCcy: "USD",
      payeeCcy: "USD",
      processingMethod: "ACH",
      fee: row.values[1],
      note: "ACH same-day monthly transaction count tier " + row.tier + ".",
    });
  }

  const swift = segmentBetween(/SWIFT\s+Wire\s+Transfers/i, [/Foreign\s+Exchange/i]);
  for (const row of parseMoneyTierRows(swift, 1)) {
    rows.push({
      txnType: "USD Abroad",
      speedFlag: "Standard",
      minAmt: row.min,
      maxAmt: row.max,
      payerCcy: "USD",
      payeeCcy: "USD",
      processingMethod: "SWIFT",
      fee: row.values[0],
      note: "SWIFT wire monthly transaction count tier " + row.tier + ".",
    });
  }

  if (!rows.length) {
    const candidates = compact.match(/(?:offline|domestic|international|same[- ]currency|bank transfer|ach|wire|swift)[^.]{0,160}\$\s*[\d,]+(?:\.\d{1,2})?/gi) || [];
    for (const candidate of candidates) {
      if (/minimum|platform|implementation|setup|reversal|return|volume|tpv/i.test(candidate)) continue;
      const feeMatch = candidate.match(/\$\s*[\d,]+(?:\.\d{1,2})?/);
      if (!feeMatch) continue;
      const fee = moneyToNumber(feeMatch[0]);
      if (fee <= 0 || fee > 1000) continue;
      pushUnique(rows, {
        txnType: /international|abroad|cross[- ]border|swift|wire/i.test(candidate) ? "USD Abroad" : "Domestic",
        speedFlag: /same[- ]day/i.test(candidate) ? "FasterACH" : "Standard",
        minAmt: 0,
        maxAmt: 1000000000,
        payerCcy: "USD",
        payeeCcy: "USD",
        processingMethod: /swift|wire/i.test(candidate) ? "SWIFT" : "",
        fee,
        note: "Detected flat transaction fee from contract text.",
      }, ["txnType", "speedFlag", "processingMethod", "fee"]);
    }
  }
  return rows;
}

function extractVolumeRates() {
  const rows = [];
  const rtp = segmentBetween(/Instant\s+US\s+Bank\s+Transfer/i, [/Instant\s+Deposit\s+to\s+Card/i]);
  for (const row of parsePercentTierRows(rtp)) {
    rows.push({
      txnType: "Domestic",
      speedFlag: "RTP",
      payerFunding: "",
      payeeFunding: "Bank",
      payeeCardType: "",
      ccyGroup: "",
      minVol: row.min,
      maxVol: row.max,
      rate: row.rate,
      note: "RTP monthly TPV tier " + row.tier + ".",
    });
  }

  const card = segmentBetween(/Instant\s+Deposit\s+to\s+Card/i, [/Other\s+Fees|Cross\s+Border|SWIFT\s+Wire/i]);
  for (const row of parsePercentTierRows(card)) {
    rows.push({
      txnType: "Payout",
      speedFlag: "Expedited",
      payerFunding: "",
      payeeFunding: "Debit",
      payeeCardType: "Debit",
      ccyGroup: "",
      minVol: row.min,
      maxVol: row.max,
      rate: row.rate,
      note: "Push-to-debit monthly TPV tier " + row.tier + ".",
    });
  }

  const stablecoin = segmentBetween(/Stablecoin\s+Ramp\s+Services/i, [/Plaid\s+Bank\s+Account|Specialized\s+Module|6\.\s+Monthly\s+Minimum/i]);
  for (const row of parsePercentTierRows(stablecoin)) {
    rows.push({
      txnType: "Transfer",
      speedFlag: "",
      payerFunding: "",
      payeeFunding: "Wallet",
      payeeCardType: "",
      ccyGroup: "Stablecoin",
      minVol: row.min,
      maxVol: row.max,
      rate: row.rate,
      note: "Stablecoin ramp monthly volume tier " + row.tier + ".",
    });
  }

  return rows;
}

function extractFxRates() {
  const rows = [];
  const fx = segmentBetween(/Foreign\s+Exchange/i, [/3\.\s+Named\s+Virtual\s+Accounts|Named\s+Virtual\s+Accounts/i]);
  const corridorStarts = [
    { name: "Major", start: /Majors\b/i, end: [/Minors\b/i] },
    { name: "Minor", start: /Minors\b/i, end: [/Tertiary\b/i] },
    { name: "Tertiary", start: /Tertiary\b/i, end: [] },
  ];
  for (const corridor of corridorStarts) {
    const section = fx ? (() => {
      const match = fx.match(corridor.start);
      if (!match || match.index == null) return "";
      const start = match.index;
      let end = fx.length;
      for (const pattern of corridor.end) {
        const rest = fx.slice(start + match[0].length);
        const endMatch = rest.match(pattern);
        if (endMatch && endMatch.index != null) end = Math.min(end, start + match[0].length + endMatch.index);
      }
      return fx.slice(start, end);
    })() : "";
    for (const row of parsePercentTierRows(section)) {
      rows.push({
        payerCorridor: "",
        payerCcy: "",
        payeeCorridor: corridor.name,
        payeeCcy: "",
        minTxnSize: 0,
        maxTxnSize: 1000000000,
        minVol: row.min,
        maxVol: row.max,
        rate: row.rate,
        note: "FX " + corridor.name.toLowerCase() + " corridor monthly volume tier " + row.tier + ".",
      });
    }
  }
  return rows;
}

function extractFeeCaps() {
  const rows = [];
  const rtp = segmentBetween(/Instant\s+US\s+Bank\s+Transfer/i, [/Instant\s+Deposit\s+to\s+Card/i]);
  const cap = rtp.match(/\bCap\s*:\s*\$\s*([\d,]+(?:\.\d{1,2})?)/i);
  if (cap) rows.push({ productType: "RTP", capType: "Max Fee", amount: moneyToNumber(cap[1]), note: "RTP per-transaction fee cap." });
  return rows;
}

function extractMinimums() {
  const rows = [];
  const minimums = segmentBetween(/Monthly\s+Minimum\s+Fees/i, [/Billing\s*&\s*Payment\s+Timing|7\.\s+Billing/i]);
  const specs = [
    { profile: "Enterprise", minVol: 50000000, maxVol: 1000000000, pattern: /Enterprise\s+\$?\s*50\s*MM\+\s+\$\s*([\d,]+)/i },
    { profile: "Growth", minVol: 5000000, maxVol: 50000000, pattern: /Growth\s+\$?\s*5\s*(?:M|MM)?\s*[–-]\s*\$?\s*50\s*(?:M|MM)?\s+\$\s*([\d,]+)/i },
    { profile: "Starter", minVol: 0, maxVol: 5000000, pattern: /Starter\s+\$?\s*0\s*[–-]\s*\$?\s*5\s*(?:M|MM)?\s+\$\s*([\d,]+)/i },
  ];
  for (const spec of specs) {
    const match = minimums.match(spec.pattern);
    if (!match) continue;
    rows.push({
      minAmount: moneyToNumber(match[1]),
      minVol: spec.minVol,
      maxVol: spec.maxVol,
      note: spec.profile + " monthly minimum. First month waived if contract states integration/testing waiver.",
    });
  }
  if (!rows.length) {
    const amount = findMoneyNear(["minimum monthly", "monthly minimum", "minimum revenue", "minimum fee", "monthly minimum fee"]);
    if (amount > 0 && amount < 1000000) {
      rows.push({ minAmount: amount, minVol: 0, maxVol: 1000000000, note: "Detected monthly minimum." });
    }
  }
  return rows;
}

function extractReversalFees() {
  const rows = [];
  const otherFees = segmentBetween(/Other\s+Fees/i, [/Cross\s+Border|SWIFT\s+Wire|Foreign\s+Exchange/i]);
  const explicit = otherFees.match(/(?:NSFs?\s*\/\s*)?Reversals?\s*:\s*\$\s*([\d,]+(?:\.\d{1,2})?)/i);
  if (explicit) {
    rows.push({ payerFunding: "", feePerReversal: moneyToNumber(explicit[1]), note: "NSF / reversal fee." });
    return rows;
  }
  const candidates = [
    ...(compact.match(/(?:reversal|reversed|return|nsf)[^.]{0,180}\$\s*[\d,]+(?:\.\d{1,2})?/gi) || []),
    ...(compact.match(/\$\s*[\d,]+(?:\.\d{1,2})?[^.]{0,180}(?:per\s+)?(?:reversal|reversed|return|nsf)/gi) || []),
  ];
  for (const candidate of candidates) {
    const match = candidate.match(/\$\s*[\d,]+(?:\.\d{1,2})?/);
    if (!match) continue;
    const fee = moneyToNumber(match[0]);
    if (fee <= 0 || fee > 1000) continue;
    rows.push({ payerFunding: "", feePerReversal: fee, note: "Detected explicit reversal fee." });
    break;
  }
  return rows;
}

function extractPlatformFees() {
  const rows = [];
  const modules = segmentBetween(/Specialized\s+Module\s+Fees/i, [/6\.\s+Monthly\s+Minimum|Monthly\s+Minimum\s+Fees/i]);
  for (const match of modules.matchAll(/([A-Za-z][A-Za-z0-9 /&-]+?Module)\s*:\s*\$\s*([\d,]+(?:\.\d{1,2})?)\s*per\s*month/gi)) {
    rows.push({ monthlyFee: moneyToNumber(match[2]), note: clean(match[1]) });
  }
  const amount = findMoneyNear(["platform fee", "monthly platform", "subscription fee"]);
  if (!rows.length && amount > 0 && amount < 1000000) rows.push({ monthlyFee: amount, note: "Detected monthly platform/subscription fee." });
  return rows;
}

function extractImplementationFees() {
  const rows = [];
  const amount = findMoneyNear(["implementation fee", "setup fee", "onboarding fee"]);
  if (amount > 0 && amount < 1000000) {
    rows.push({
      feeType: "Implementation",
      feeAmount: amount,
      creditMode: "",
      creditAmount: 0,
      creditWindowDays: 0,
      note: "Detected one-time implementation/setup fee.",
    });
  }
  return rows;
}

function extractVirtualAccountFees() {
  const rows = [];
  const va = segmentBetween(/Named\s+Virtual\s+Accounts/i, [/4\.\s+Surcharge\s+for\s+Non-FX\s+Flows|Surcharge\s+for\s+Non-FX\s+Flows/i]);
  const opening = va.match(/Account\s+Opening\s+Fee\s+\$\s*([\d,]+(?:\.\d{1,2})?)/i);
  if (opening) rows.push({ feeType: "Account Opening", minAccounts: 0, maxAccounts: 1000000000, discount: 0, feePerAccount: moneyToNumber(opening[1]), note: "One-time charge per named virtual account issued." });
  const monthly = va.match(/Monthly\s+Active\s+Account\s+Fee\s+\$\s*([\d,]+(?:\.\d{1,2})?)/i);
  if (monthly) rows.push({ feeType: "Monthly Active", minAccounts: 0, maxAccounts: 1000000000, discount: 0, feePerAccount: moneyToNumber(monthly[1]), note: "Monthly active named virtual account fee." });
  return rows;
}

function extractSurcharges() {
  const rows = [];
  const surcharges = segmentBetween(/Surcharge\s+for\s+Non-FX\s+Flows/i, [/5\.\s+Additional\s+Service\s+Modules|Additional\s+Service\s+Modules/i]);
  for (const row of parseBpsTierRows(surcharges)) {
    rows.push({
      surchargeType: "Same Currency",
      rate: row.rate,
      minVol: row.min,
      maxVol: row.max,
      note: "Non-FX ACH/SWIFT revenue recovery surcharge monthly TPV tier.",
    });
  }
  return rows;
}

function extractOtherFees() {
  const rows = [];
  const schedule = segmentBetween(/Schedule\s+A\s+Pricing\s+Schedule/i, [/Schedule\s+B/i]);
  const add = (row) => pushUnique(rows, row, ["feeType", "amount", "rate", "note"]);
  const compliance = schedule.match(/Annual\s+Compliance\s+Fee\s+\$\s*([\d,]+(?:\.\d{1,2})?)/i);
  if (compliance) add({ feeType: "Annual Compliance Fee", amount: moneyToNumber(compliance[1]), note: "Annual fee per customer account." });
  const chargeback = schedule.match(/Chargeback\s+Fee\s*:\s*\$\s*([\d,]+(?:\.\d{1,2})?)/i);
  if (chargeback) add({ feeType: "Chargeback Fee", amount: moneyToNumber(chargeback[1]), note: "Other Fees section." });
  const collection = schedule.match(/Collection\s+services\s*:\s*\$\s*([\d,]+(?:\.\d{1,2})?)/i);
  if (collection) add({ feeType: "Collection Services", amount: moneyToNumber(collection[1]), note: "Per payer." });
  const plaidAuth = schedule.match(/Auth\s*\(Account\s+Verification\)\s+One-time\s+per\s+connection\s+\$\s*([\d,]+(?:\.\d{1,2})?)/i);
  if (plaidAuth) add({ feeType: "Plaid Auth Account Verification", amount: moneyToNumber(plaidAuth[1]), note: "One-time per bank account connection." });
  for (const fee of extractPlatformFees()) {
    if (fee.note) add({ feeType: fee.note, amount: fee.monthlyFee, note: "Monthly specialized module fee." });
  }
  return rows;
}

function extractRevShareFees() {
  const rows = [];
  const candidates = compact.match(/(?:rtp|revenue split|revenue share|rev share)[^.]{0,220}\d+(?:\.\d+)?\s*%/gi) || [];
  for (const candidate of candidates) {
    const rateMatch = candidate.match(/\d+(?:\.\d+)?\s*%/);
    if (!rateMatch) continue;
    rows.push({
      feeType: /rtp/i.test(candidate) ? "RTP Revenue Split" : "Revenue Share",
      rate: percentToDecimal(rateMatch[0]),
      note: clean(candidate),
    });
  }
  return rows;
}

const parsed = {
  partnerName: findPartnerName(),
  effectiveDate: findEffectiveDate(),
  billingTerms: findBillingTerms(),
  offlineRates: extractOfflineRates(),
  volumeRates: extractVolumeRates(),
  fxRates: extractFxRates(),
  feeCaps: extractFeeCaps(),
  minimums: extractMinimums(),
  reversalFees: extractReversalFees(),
  platformFees: extractPlatformFees(),
  implFees: extractImplementationFees(),
  virtualAccountFees: extractVirtualAccountFees(),
  surcharges: extractSurcharges(),
  otherFees: extractOtherFees(),
  revShareTiers: [],
  revShareFees: extractRevShareFees(),
  warnings,
};

if (!parsed.effectiveDate) parsed.warnings.push("No effective date was detected.");
if (!parsed.billingTerms.payBy) parsed.warnings.push("No payment timing/pay-by term was detected.");
if (!parsed.offlineRates.length && !parsed.volumeRates.length && !parsed.fxRates.length && !parsed.feeCaps.length && !parsed.minimums.length && !parsed.reversalFees.length && !parsed.platformFees.length && !parsed.implFees.length && !parsed.virtualAccountFees.length && !parsed.surcharges.length && !parsed.otherFees.length && !parsed.revShareFees.length) {
  parsed.warnings.push("No billing fee rows were detected. Use an LLM parser node for this contract or paste structured JSON.");
}
if (parsed.revShareFees.length) {
  parsed.warnings.push("Revenue-share terms were detected for review. The current workbook import preview does not automatically import revenue-share fee rows.");
}

return [{ json: parsed }];
`;

function buildWorkflow() {
  const nodes = [];
  const connections = {};
  const addNode = (node) => nodes.push(node);
  const connect = (fromName, toName) => {
    connections[fromName] ||= { main: [[]] };
    connections[fromName].main[0].push({ node: toName, type: "main", index: 0 });
  };

  addNode(webhookNode("Contract Extract Webhook", "billing-contract-extract", [180, 220]));
  addNode({ parameters: {}, id: slugify("Manual Extract Test"), name: "Manual Extract Test", type: "n8n-nodes-base.manualTrigger", typeVersion: 1, position: [180, 380] });
  addNode(codeNode("Sample Extract Request", [440, 380], sampleExtractRequestCode));
  addNode(codeNode("Normalize Extract Request", [440, 220], normalizeExtractRequestCode));
  addNode(codeNode("Build PDF Binary", [700, 220], buildPdfBinaryCode));
  addNode(extractFromPdfNode("Extract PDF Text", [960, 220]));
  addNode(codeNode("Normalize PDF Text Response", [1220, 220], normalizePdfTextResponseCode));

  addNode(webhookNode("Contract Parse Webhook", "billing-contract-parse", [180, 700]));
  addNode({ parameters: {}, id: slugify("Manual Parse Test"), name: "Manual Parse Test", type: "n8n-nodes-base.manualTrigger", typeVersion: 1, position: [180, 860] });
  addNode(codeNode("Sample Parse Request", [440, 860], sampleParseRequestCode));
  addNode(codeNode("Normalize Parse Request", [440, 700], normalizeParseRequestCode));
  addNode(codeNode("Deterministic Contract Parser", [700, 700], deterministicParseCode));

  connect("Contract Extract Webhook", "Normalize Extract Request");
  connect("Manual Extract Test", "Sample Extract Request");
  connect("Sample Extract Request", "Normalize Extract Request");
  connect("Normalize Extract Request", "Build PDF Binary");
  connect("Build PDF Binary", "Extract PDF Text");
  connect("Extract PDF Text", "Normalize PDF Text Response");

  connect("Contract Parse Webhook", "Normalize Parse Request");
  connect("Manual Parse Test", "Sample Parse Request");
  connect("Sample Parse Request", "Normalize Parse Request");
  connect("Normalize Parse Request", "Deterministic Contract Parser");

  return {
    name: "Billing Workbook Contract Automation - Updated 2026-04-30",
    active: false,
    isArchived: false,
    nodes,
    connections,
    pinData: {},
    settings: {
      executionOrder: "v1",
      saveDataSuccessExecution: "none",
      saveExecutionProgress: false,
      saveManualExecutions: true,
    },
    tags: [],
  };
}

fs.mkdirSync(path.dirname(OUTPUT_PATH), { recursive: true });
fs.writeFileSync(OUTPUT_PATH, JSON.stringify(buildWorkflow(), null, 2) + "\n");
console.log("Wrote " + OUTPUT_PATH);

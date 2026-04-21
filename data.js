import { importedLookerData } from "./looker-import.js?v=20260316e";

const uid = () => Math.random().toString(36).slice(2, 8);

export const CONTRACT_PROMPT = `You are a contract pricing extraction engine. Given a partner services agreement with Veem Inc., extract ALL pricing from the Schedule A / Pricing Schedule into structured JSON.
IMPORTANT: Extract every single fee, rate, and tier. Do not summarize or skip any pricing detail.
Return ONLY valid JSON (no markdown, no backticks, no explanation) in this exact structure:
{"partnerName":"string - the ACTUAL company name (NOT 'Partner'). Find in the header: 'between Veem Inc... and [NAME]'. Use the short brand name.","effectiveDate":"YYYY-MM-DD","implementationFee":10000,"offlineRates":[{"txnType":"Domestic|USD Abroad|FX|CAD Domestic|GBP Domestic|EUR Domestic|AUD Domestic","speedFlag":"Standard|FasterACH|RTP","minAmt":0,"maxAmt":999,"fee":1.00,"payerCcy":"USD","payeeCcy":"USD","processingMethod":"ACH|Wire|RTP|Card|","note":""}],"volumeRates":[{"txnType":"Domestic|FX|","speedFlag":"Standard|FasterACH|RTP|","rate":0.009,"payerFunding":"Card|Bank|","payeeFunding":"Card|Bank|","payeeCardType":"Credit|Debit|","ccyGroup":"MAJORS|MINORS|TERTIARY|","minVol":0,"maxVol":999999,"note":"tier description"}],"feeCaps":[{"productType":"ACH|FasterACH|RTP|Wire|FX Majors|FX Minors|FX Tertiary|Card Credit Domestic|Card Debit Domestic|Push-to-Debit","capType":"Min Fee|Max Fee","amount":20}],"minimums":[{"minAmount":5000,"minVol":0,"maxVol":1990000,"note":""}],"reversalFees":[{"payerFunding":"Bank|Wallet|Card|","feePerReversal":5.00,"note":""}],"platformFees":[{"monthlyFee":0,"note":""}],"implFees":[{"feeType":"Implementation|Account Setup|Daily Settlement","feeAmount":10000,"note":""}],"virtualAccountFees":[{"feeType":"Account Opening|Monthly Active|Dormancy|Account Closing","minAccounts":1,"maxAccounts":99,"discount":0,"feePerAccount":5.00,"note":""}],"surcharges":[{"surchargeType":"Same Currency|Platform|Card Surcharge|Cross-Border","rate":0.0004,"minVol":0,"maxVol":5000000,"note":"tier description"}],"billingTerms":{"payBy":"Due in 7 days|Before 15th (Net 30)","billingFreq":"Monthly|Quarterly"}}
RULES:
- "Next Day ACH" -> speedFlag:"Standard", txnType:"Domestic"
- "Same Day ACH" -> speedFlag:"FasterACH", txnType:"Domestic"
- "Instant/RTP" -> speedFlag:"RTP"
- Volume-tiered txn fees: each tier = SEPARATE offlineRates entry with minAmt/maxAmt as txn count range
- Percentage fees (RTP, FX, Card): volumeRates with rate as decimal (0.9%=0.009)
- Volume tiers by TPV: use minVol/maxVol in dollars (0, 999999, 1000000, 4990000, etc.)
- FX rates: ccyGroup = "MAJORS" or "MINORS" or "TERTIARY" (shorthand labels, not full lists)
- Wire: Domestic wire -> txnType:"Domestic", processingMethod:"Wire". International -> txnType:"FX", processingMethod:"Wire"
- Credit Card (Payer Funding): payerFunding:"Card", payeeCardType:"Credit"
- Debit Card (Payer Funding): payerFunding:"Card", payeeCardType:"Debit"
- Push-to-Debit (Payout): payeeFunding:"Card", payeeCardType:"Debit"
- Cap fees (e.g. "Cap: $20/txn"): create a feeCap with productType matching the product (RTP, FasterACH, etc), capType="Max Fee", amount=cap value. For min fees (e.g. "$50 min"), use capType="Min Fee"
- "Instant Bank Transfer" and "RTP" are the SAME product - always use productType:"RTP" for both
- Surcharges: If the contract has a separate "transaction surcharge" or "same currency surcharge" that stacks ON TOP of base fees (not replacing them), extract as surcharges with rate as decimal and volume tiers. These are additional % fees applied to all txns within a volume band.
- Virtual Account fees have 4 types: "Account Opening" = one-time per new account, "Monthly Active" = recurring monthly fee per open/active account, "Dormancy" = monthly fee per inactive account (90+ days no inbound), "Account Closing" = one-time fee when closing an inactive account. If contract says "closing fee" or "account closing fee" for inactive accounts, use feeType:"Account Closing".
- Rates as decimals: 0.50%=0.005, 3.50%=0.035
- Include ALL tiers as separate entries
CRITICAL: Output COMPACT/MINIFIED JSON. No indentation. Start with { end with }. No trailing commas. Double quotes only.`;

const initPartners = ["Stampli", "Shepherd", "Everflow", "Q2", "Finastra", "Halorecruiting", "Magaya", "Fulfil", "Nomad", "Skydo", "TripleA", "Capi", "Nsave", "Cellpay", "Nuvion", "Yeepay", "Clearshift", "Graph Finance", "Remittanceshub", "Altpay", "Repay", "LianLian", "Blindpay", "Whish", "Athena", "Maplewave", "GME_Remit", "Oson", "BHN", "Factura", "Goldstack", "Jazz Cash", "Lightnet", "Multigate", "NIBSS ( TurboTech)", "Nium", "OhentPay", "VG Pay"];
const initPartnerConfig = { "VG Pay": true };
const initArchivedPartners = [];
const initPartnerActivity = [];
const pb = (
  id,
  partner,
  billingFreq,
  payBy,
  dueDays,
  billingDay,
  note,
  contractDueText = "",
  preferredBillingTiming = "",
  contactEmails = "",
  contractStartDate = "",
  goLiveDate = "",
  notYetLive = false,
  integrationStatus = "",
  lateFeePercentMonthly = 0,
  lateFeeStartDays = 0,
  serviceSuspensionDays = 0,
  lateFeeTerms = ""
) => ({
  id,
  partner,
  billingFreq,
  payBy,
  dueDays,
  billingDay,
  note,
  contractDueText,
  preferredBillingTiming,
  contactEmails,
  contractStartDate,
  goLiveDate,
  notYetLive,
  integrationStatus,
  lateFeePercentMonthly,
  lateFeeStartDays,
  serviceSuspensionDays,
  lateFeeTerms
});

const DEFAULT_CONTRACT_START_DATES = {
  Stampli: "2024-06-19",
  Shepherd: "2024-07-01",
  Everflow: "2024-01-01",
  Q2: "2024-01-01",
  Halorecruiting: "2023-09-19",
  Nomad: "2025-01-15",
  Skydo: "2025-03-01",
  TripleA: "2025-03-01",
  Capi: "2025-04-01",
  Nsave: "2025-04-01",
  Cellpay: "2025-05-22",
  Nuvion: "2025-10-21",
  Yeepay: "2025-08-06",
  Clearshift: "2026-01-01",
  "Graph Finance": "2026-01-01",
  Remittanceshub: "2025-12-01",
  Altpay: "2026-01-31",
  Repay: "2026-01-29",
  LianLian: "2025-04-01",
  Blindpay: "2025-10-21",
  Whish: "2024-07-04",
  Athena: "2025-02-27",
  Maplewave: "2026-01-31",
  Oson: "2024-07-05"
};

const DEFAULT_GO_LIVE_DATES = {
  Athena: "",
  TripleA: "",
  Capi: "",
  Nsave: "2025-05-15",
  Cellpay: "",
  Nuvion: "2025-12-19",
  Yeepay: "2025-12-01",
  Clearshift: "",
  "Graph Finance": "",
  Remittanceshub: "2025-11-24",
  Altpay: "",
  Repay: "",
  Maplewave: "2026-01-15",
  Blindpay: "2026-02-18",
  LianLian: "2025-12-01",
  Skydo: "2025-05-20",
  Nomad: "2025-01-15"
};

const DEFAULT_INTEGRATION_STATUS = {
  Altpay: "Integration Underway (Partners Onboarding)",
  Athena: "",
  Blindpay: "Live (Partners Onboarding)",
  Capi: "On Hold (Partners Onboarding)",
  Cellpay: "Sales Complete (Partners Onboarding)",
  Clearshift: "Integration Underway (Partners Onboarding)",
  "Graph Finance": "Integration Underway (Partners Onboarding)",
  LianLian: "Live (Partners Onboarding)",
  Maplewave: "Live (Partners Onboarding)",
  Nomad: "Live (Partners Onboarding)",
  Nsave: "Live (Partners Onboarding)",
  Nuvion: "Live (Partners Onboarding)",
  Remittanceshub: "Live (Partners Onboarding)",
  Repay: "Integration Underway (Partners Onboarding)",
  Skydo: "Live (Partners Onboarding)",
  TripleA: "Live (Partners Onboarding)",
  Yeepay: "Live (Partners Onboarding)"
};

// HubSpot lifecycle is informational only. Billing suppression should come from the
// manual "Partner is not yet live" checkbox on the partner page, not from an
// automatically imported integration status.
const DEFAULT_NOT_YET_LIVE = {};

function enrichPartnerBillingRows(rows) {
  return rows.map((row) => ({
    contractStartDate: DEFAULT_CONTRACT_START_DATES[row.partner] || "",
    goLiveDate: DEFAULT_GO_LIVE_DATES[row.partner] || "",
    notYetLive: DEFAULT_NOT_YET_LIVE[row.partner] ?? false,
    integrationStatus: DEFAULT_INTEGRATION_STATUS[row.partner] || "",
    ...row
  }));
}

const initPartnerBilling = [
  pb("pb_stampli", "Stampli", "Monthly", "Monthly settlement / setoff", 0, 2, "Source: Stampli.txt Warnings: Detected an FX markup or settlement formula that cannot be converted into fixed rate rows automatically.", "7th of the following month", "2nd of following month", "accounting@stampli.com"),
  pb("pb_shepherd", "Shepherd", "Quarterly", "Quarterly within 45 days from last day of quarter", 45, 7, "Source: Shepherd_Somewhere.txt Warnings: No pricing rows were confidently extracted. Review the contract text and complete any missing details manually.", "Not specified", "1st week of following month", "sheila@veem.com, ryan@somewhere.com, venice@somewhere.com, niel@somewhere.com, jerimiah@somewhere.com, nprada@somewhere.com, accountspayable@veem.com"),
  pb("pb_everflow", "Everflow", "Quarterly", "Quarterly within 45 days from last day of quarter", 45, 2, "Source: Everflow.txt Warnings: No pricing rows were confidently extracted. Review the contract text and complete any missing details manually.", "Not specified", "2nd of following month", "ed@everflow.io, arshi@everflow.io, peter@everflow.io, suzy@everflow.io, olivier@everflow.io, patrick@everflow.io, accountspayable@veem.com, sheila@veem.com, natalie@everflow.io"),
  pb("pb_q2", "Q2", "Monthly", "Monthly within 45 days from last day of month", 45, "", "Source: Q2.txt Warnings: No pricing rows were confidently extracted. Review the contract text and complete any missing details manually."),
  pb("pb_finastra", "Finastra", "Monthly", "", 0, "", "No mapped contract file"),
  pb("pb_halorecruiting", "Halorecruiting", "Quarterly", "Quarterly within 45 days from last day of quarter", 45, "", "Source: Halo_Recruiting.txt Warnings: No pricing rows were confidently extracted. Review the contract text and complete any missing details manually."),
  pb("pb_magaya", "Magaya", "Quarterly", "Due in 30 days", 30, "", "Operational source: Partner Payouts sheet"),
  pb("pb_fulfil", "Fulfil", "Monthly", "", 0, "", "No mapped contract file"),
  pb("pb_nomad", "Nomad", "Monthly", "Due in 30 days", 30, 1, "Source: Nomad.txt", "1st of following month", "1st of following month", "mathias.fischer@nomadglobal.com, financeiro@nomadglobal.com, pedro.barreiro@nomadglobal.com, alessandra.ferreira@nomadglobal.com, finops@nomadglobal.com, geovanne.pereira@nomadglobal.com"),
  pb("pb_skydo", "Skydo", "Monthly", "Due in 30 days", 30, 1, "Source: Skydo.txt Warnings: No pricing rows were confidently extracted. Review the contract text and complete any missing details manually.", "1st of following month", "1st of following month", "payables@skydo.com"),
  pb("pb_triplea", "TripleA", "Monthly", "", 0, "", "Source: Triple_A.txt No explicit due term extracted.", "Monthly", "", "finance@triple-a.io"),
  pb("pb_capi", "Capi", "Monthly", "Before 15th(Due Net 30)", 30, 1, "Source: Capi.txt No explicit due term extracted.", "Monthly", "1st of begining of month", "josh@capimoney.com"),
  pb("pb_nsave", "Nsave", "Monthly", "Before 15th(Due Net 30)", 30, 1, "Source: NSave_Addendum.txt Warnings: No pricing rows were confidently extracted. Review the contract text and complete any missing details manually. No explicit due term extracted.", "Monthly", "1st of begining of month", "amer@nsave.com, finance@nsave.com"),
  pb("pb_cellpay", "Cellpay", "Monthly", "Due in 7 days", 7, 1, "Source: Cellpay.txt", "Monthly", "1st of begining of month", "garene@cellpay.com"),
  pb("pb_nuvion", "Nuvion", "Monthly", "Due in 7 days", 7, 31, "Source: Nuvion.txt Warnings: Detected IC++ / pass-through card pricing. Review card pricing manually after import.", "Monthly", "End of Month", "oluwadara@nuvion.co"),
  pb("pb_yeepay", "Yeepay", "Monthly", "Due in 7 days", 7, 31, "Source: Yeepay.txt", "Monthly", "End of Month", "overseasops@yeepay.com, yuezhang.liu@yeepay.com, qiuyu.cui@yeepay.com"),
  pb("pb_clearshift", "Clearshift", "Monthly", "Due in 7 days", 7, "", "Source: Clearshift.txt"),
  pb("pb_graph_finance", "Graph Finance", "Monthly", "Due in 7 days", 7, "", "Source: Graph.txt"),
  pb("pb_remittanceshub", "Remittanceshub", "Monthly", "Due in 14 days", 14, "", "Operational source: Billed and Collected sheet"),
  pb("pb_altpay", "Altpay", "Monthly", "Due in 7 days", 7, "", "Source: Altpay.txt"),
  pb("pb_repay", "Repay", "Monthly", "Due in 30 days", 30, "", "Source: RePay.txt"),
  pb("pb_lianlian", "LianLian", "Monthly", "Due in 7 days", 7, 31, "Source: Lian_Lian.txt Warnings: No pricing rows were confidently extracted. Review the contract text and complete any missing details manually. No explicit due term extracted.", "Monthly", "End of Month"),
  pb("pb_blindpay", "Blindpay", "Monthly", "Due in 7 days", 7, 31, "Source: Blindpay.txt", "Monthly", "End of Month", "bernardo@blindpay.com"),
  pb("pb_whish", "Whish", "Monthly", "Before 15th(Due Net 30)", 30, 7, "Source: Whish.txt Warnings: No pricing rows were confidently extracted. Review the contract text and complete any missing details manually. No explicit due term extracted.", "Not specified", "1st week of following month         Make sure to include bank wire details on Invoice Notes section", "veem@whish.money, a.hanna@whish.money, sheila@veem.com"),
  pb("pb_athena", "Athena", "Monthly", "Before 15th(Due Net 30)", 30, 1, "Source: Athena.txt Warnings: No pricing rows were confidently extracted. Review the contract text and complete any missing details manually. No explicit due term extracted.", "Monthly", "1st of begining of month", "sam.nazzaro@athenabitcoin.com"),
  pb("pb_maplewave", "Maplewave", "Monthly", "Issued monthly in arrears", 0, "", "Source: Maplewave.txt Warnings: No pricing rows were confidently extracted. Review the contract text and complete any missing details manually."),
  pb("pb_gme_remit", "GME_Remit", "Monthly", "Due in 30 days", 30, "", "Operational source: Billed and Collected sheet"),
  pb("pb_oson", "Oson", "Monthly", "net 30", 30, 10, "Source: Oson.txt Warnings: No pricing rows were confidently extracted. Review the contract text and complete any missing details manually. No explicit due term extracted.", "Not specified", "1st week of following month         Make sure to include bank wire details on Invoice Notes section", "info@oson.kz, a.almambetov@oson.com"),
  pb("pb_bhn", "BHN", "Monthly", "Due in 30 days", 30, "", "Source: BHN.txt Warnings: Pricing extraction is incomplete and should be reviewed manually.", "", "", "", "2025-08-04", "", true, "Integration Underway (Partners Onboarding)"),
  pb("pb_factura", "Factura", "Quarterly", "Quarterly within 15 days from last day of quarter", 15, "", "Source: Factura.txt Main contract pricing is a $20 monthly subscription fee per Veem account plus referral/rebate terms. The app still needs a referred-business account count source to calculate this automatically.", "", "", "", "2024-02-14"),
  pb("pb_goldstack", "Goldstack", "Monthly", "Due in 7 days", 7, "", "Source: Goldstack.txt", "", "", "", "2026-01-12", "", true, "Integration Underway (Partners Onboarding)"),
  pb("pb_jazz_cash", "Jazz Cash", "Monthly", "", 0, "", "Source: Jazz_Cash.txt Warnings: Pricing requires manual review for complete fee mapping.", "", "", "", "2025-12-30", "", true, "Integration Underway (Partners Onboarding)"),
  pb("pb_lightnet", "Lightnet", "Monthly", "Due in 7 days", 7, "", "Source: Lightnet.txt", "", "", "", "2025-04-22", "", true, "On Hold (Partners Onboarding)"),
  pb("pb_multigate", "Multigate", "Monthly", "Due in 7 days", 7, "", "Source: Multigate.txt Warnings: Detected a period-based or ramping monthly minimum schedule. Review minimum rows manually after import.", "", "", "", "2025-10-22", "", true, "Integration Underway (Partners Onboarding)"),
  pb("pb_nibss", "NIBSS ( TurboTech)", "Monthly", "", 0, "", "Source: NIBSS.txt Warnings: Detected a period-based or ramping monthly minimum schedule. Review minimum rows manually after import.", "", "", "", "2025-03-01", "", true, "Closed Lost (Partners Onboarding)"),
  pb("pb_nium", "Nium", "Monthly", "Due in 7 days", 7, "", "Source: Nium.txt", "", "", "", "2025-10-23", "", true, "Integration Underway (Partners Onboarding)"),
  pb("pb_ohentpay", "OhentPay", "Monthly", "Due in 7 days", 7, "", "Source: OhentPay.txt", "", "", "", "2026-02-27", "", true, "Integration Underway (Partners Onboarding)"),
  pb("pb_vg_pay", "VG Pay", "Monthly", "Due in 7 days", 7, "", "Source: VG_Pay.txt", "", "", "", "2026-02-01", "", true, "Integration Underway (Partners Onboarding)")
];
const initInvoiceTracking = [];
const initAccessLogs = [];
const initAdminSettings = {
  guestAllowedTabs: ["invoice", "partner", "rates", "looker", "costs", "import"],
  guestAccessCustomized: false
};
const STAMPLI_EFFECTIVE_DATE = "2024-06-19";
const SKYDO_CARD_EFFECTIVE_DATE = "2025-08-01";
const TABAPAY_ORIGINAL_EFFECTIVE_DATE = "2021-03-18";
const TABAPAY_AMENDMENT_EFFECTIVE_DATE = "2026-01-06";
const TABAPAY_ORIGINAL_END_DATE = "2026-01-05";

const fxBands = [
  { min: 0, max: 199.99, label: "$0-200" },
  { min: 200, max: 499.99, label: "$200-500" },
  { min: 500, max: 999.99, label: "$500-1K" },
  { min: 1000, max: 1999.99, label: "$1K-2K" },
  { min: 2000, max: 4999.99, label: "$2K-5K" },
  { min: 5000, max: 9999.99, label: "$5K-10K" },
  { min: 10000, max: 24999.99, label: "$10K-25K" },
  { min: 25000, max: 49999.99, label: "$25K-50K" },
  { min: 50000, max: 99999.99, label: "$50K-100K" },
  { min: 100000, max: 249999.99, label: "$100K-250K" },
  { min: 250000, max: 499999.99, label: "$250K-500K" },
  { min: 500000, max: 999999.99, label: "$500K-1M" },
  { min: 1000000, max: 1000000000, label: "$1M+" }
];

const sfxRaw = [
["AED",.046,.0174,.0113,.0087,.0071,.0065,.0062,.0061,.0061,.006,.0057,.0054,.005],
["ALL",.044,.0226,.018,.016,.0149,.0144,.0142,.0141,.014,.014,.0128,.0125,.0121],
["AMD",.044,.0226,.018,.016,.0149,.0144,.0142,.0141,.014,.014,.0128,.0125,.0121],
["ARS",.054,.0326,.028,.026,.0249,.0244,.0242,.0241,.014,.014,.0128,.0125,.0121],
["AUD",.022,.0095,.0068,.0057,.005,.0047,.0046,.0045,.0045,.0045,.0033,.0031,.0029],
["BBD",.044,.0226,.018,.016,.0149,.0144,.0142,.0141,.014,.014,.0128,.0125,.0121],
["BDT",.0799,.0326,.02,.0145,.0114,.0101,.0095,.0092,.0091,.009,.0116,.0103,.0099],
["BGN",.0499,.0226,.0137,.0099,.0077,.0068,.0063,.0062,.0061,.006,.0057,.0054,.005],
["BHD",.0799,.0499,.0383,.0237,.0153,.0119,.0103,.0096,.0093,.0091,.0117,.0103,.0099],
["BMD",.036,.0146,.01,.008,.0069,.0064,.0062,.0061,.006,.006,.0057,.0054,.005],
["BND",.0799,.0497,.028,.0185,.0131,.0109,.0098,.0094,.0092,.0091,.0116,.0103,.0099],
["BRL",.008,.008,.008,.008,.008,.008,.008,.008,.008,.008,.0067,.0064,.006],
["BSD",.039,.0176,.013,.011,.0099,.0094,.0092,.0091,.009,.009,.0116,.0103,.0099],
["CAD",.022,.0095,.0068,.0057,.005,.0047,.0046,.0045,.0045,.0045,.0033,.0031,.0029],
["CHF",.0345,.0131,.0085,.0065,.0054,.0049,.0047,.0046,.0045,.0045,.0033,.0031,.0029],
["CLP",.049,.0276,.023,.021,.0199,.0194,.0192,.0191,.014,.014,.0128,.0125,.0121],
["CNY",.0469,.0219,.0126,.0086,.0062,.0053,.0048,.0047,.0046,.0045,.0033,.0031,.0029],
["COP",.0515,.0301,.0255,.0235,.0224,.0219,.0217,.0216,.0215,.0215,.0164,.0136,.0128],
["CRC",.0295,.0127,.0091,.0076,.0067,.0063,.0061,.0061,.006,.006,.0088,.0085,.0081],
["CZK",.0485,.0178,.0112,.0084,.0067,.0061,.0057,.0056,.0056,.0055,.0045,.0042,.004],
["DKK",.022,.0095,.0068,.0057,.005,.0047,.0046,.0045,.0045,.0045,.0033,.0031,.0029],
["DOP",.036,.0146,.01,.008,.0069,.0064,.0062,.0061,.006,.006,.006,.006,.006],
["DZD",.0799,.0516,.0289,.0189,.0133,.011,.0099,.0094,.0092,.0091,.0116,.0103,.0099],
["EGP",.056,.0203,.0127,.0093,.0074,.0067,.0063,.0061,.0061,.006,.0057,.0054,.005],
["EUR",.0195,.0088,.0065,.0055,.0049,.0047,.0046,.0045,.0045,.0045,.0033,.0031,.0029],
["FJD",.0699,.0335,.0189,.0124,.0088,.0073,.0066,.0063,.0061,.0061,.0088,.0085,.0081],
["GBP",.022,.0095,.0068,.0057,.005,.0047,.0046,.0045,.0045,.0045,.0033,.0031,.0029],
["GTQ",.041,.016,.0107,.0083,.007,.0065,.0062,.0061,.006,.006,.0057,.0054,.005],
["HKD",.0299,.0159,.0098,.0072,.0056,.005,.0047,.0046,.0046,.0045,.0033,.0031,.0029],
["HNL",.045,.0236,.019,.017,.0159,.0154,.0152,.0151,.015,.015,.0133,.013,.0126],
["HUF",.0399,.0229,.0263,.0159,.01,.0076,.0064,.0059,.0057,.0056,.0045,.0042,.004],
["IDR",.0325,.0136,.0095,.0078,.0068,.0064,.0062,.0061,.006,.006,.0057,.0054,.005],
["ILS",.0399,.0187,.0117,.0086,.0068,.0061,.0058,.0056,.0056,.0055,.0045,.0042,.004],
["INR",.041,.016,.0107,.0083,.007,.0065,.0062,.0061,.006,.006,.0042,.0042,.0042],
["ISK",.0799,.0499,.0309,.028,.0171,.0128,.0106,.0098,.0094,.0092,.0117,.0103,.0099],
["JMD",.046,.0174,.0113,.0087,.0071,.0065,.0062,.0061,.0061,.006,.0088,.0085,.0081],
["JOD",.0699,.0403,.022,.014,.0094,.0076,.0067,.0063,.0062,.0061,.0088,.0085,.0081],
["JPY",.182,.0559,.029,.0173,.0105,.0079,.0065,.006,.0057,.0056,.0045,.0042,.004],
["KES",.0676,.0236,.0142,.0101,.0078,.0068,.0064,.0062,.0061,.006,.0088,.0085,.0081],
["KRW",.041,.0196,.015,.013,.0119,.0114,.0112,.0111,.011,.011,.0113,.011,.0106],
["KWD",.0522,.0192,.0122,.0091,.0073,.0066,.0063,.0061,.0061,.006,.0088,.0085,.0081],
["KYD",.044,.0226,.018,.016,.0149,.0144,.0142,.0141,.014,.014,.0141,.0128,.0124],
["KZT",.047,.0177,.0115,.0087,.0072,.0065,.0062,.0061,.0061,.006,.0088,.0085,.0081],
["LBP",.044,.0226,.018,.016,.0149,.0144,.0142,.0141,.014,.014,.0141,.0128,.0124],
["LKR",.0799,.0386,.0255,.0197,.0165,.0151,.0145,.0142,.0141,.014,.0141,.0128,.0124],
["MAD",.0295,.0127,.0091,.0076,.0067,.0063,.0061,.0061,.006,.006,.0057,.0054,.005],
["MOP",.0799,.0533,.0297,.0193,.0134,.0111,.0099,.0094,.0092,.0091,.0116,.0103,.0099],
["MUR",.071,.0246,.0147,.0103,.0079,.0069,.0064,.0062,.0061,.006,.0088,.0085,.0081],
["MWK",.039,.0176,.013,.011,.0099,.0094,.0092,.0091,.009,.009,.0116,.0103,.0099],
["MXN",.023,.0105,.0078,.0067,.006,.0057,.0056,.0055,.0055,.0055,.0045,.0042,.004],
["MYR",.042,.0206,.016,.014,.0129,.0124,.0122,.0121,.012,.012,.0087,.0084,.008],
["MZN",.044,.0226,.018,.016,.0149,.0144,.0142,.0141,.014,.014,.0128,.0125,.0121],
["NOK",.0299,.0185,.011,.0078,.0059,.0052,.0048,.0046,.0046,.0045,.0033,.0031,.0029],
["NPR",.044,.0226,.018,.016,.0149,.0144,.0142,.0141,.014,.014,.0141,.0128,.0124],
["NZD",.022,.0095,.0068,.0057,.005,.0047,.0046,.0045,.0045,.0045,.0033,.0031,.0029],
["OMR",.0799,.0597,.0353,.0247,.0186,.0161,.0149,.0144,.0142,.0141,.0141,.0128,.0124],
["PEN",.056,.0203,.0127,.0093,.0074,.0067,.0063,.0061,.0061,.006,.0088,.0085,.0081],
["PHP",.009,.0061,.0055,.0053,.0051,.0051,.005,.005,.005,.005,.0036,.0034,.0032],
["PKR",.05,.0243,.0188,.0164,.015,.0145,.0142,.0141,.014,.014,.0141,.0128,.0124],
["PLN",.1215,.0386,.021,.0132,.0088,.007,.0062,.0058,.0057,.0056,.0045,.0042,.004],
["QAR",.0799,.0461,.029,.0215,.0172,.0155,.0146,.0143,.0142,.0141,.0141,.0128,.0124],
["RON",.0499,.0231,.014,.01,.0077,.0068,.0063,.0062,.0061,.006,.0057,.0054,.005],
["RSD",.041,.0196,.015,.013,.0119,.0114,.0112,.0111,.011,.011,.0113,.011,.0106],
["RUB",.0395,.0156,.0105,.0082,.007,.0064,.0062,.0061,.006,.006,.0057,.0054,.005],
["SAR",.0499,.0237,.0143,.0101,.0078,.0068,.0064,.0062,.0061,.006,.0057,.0054,.005],
["SEK",.0299,.0159,.0098,.0072,.0056,.005,.0047,.0046,.0046,.0045,.0033,.0031,.0029],
["SGD",.022,.0095,.0068,.0057,.005,.0047,.0046,.0045,.0045,.0045,.0033,.0031,.0029],
["SZL",.0425,.0211,.0165,.0145,.0134,.0129,.0127,.0126,.0125,.0125,.0121,.0117,.0113],
["THB",.0399,.0236,.0139,.0097,.0073,.0063,.0059,.0057,.0056,.0055,.0045,.0042,.004],
["TND",.0275,.0121,.0089,.0074,.0066,.0063,.0061,.0061,.006,.006,.0088,.0085,.0081],
["TRY",.042,.0159,.0104,.0079,.0065,.006,.0057,.0056,.0055,.0055,.0045,.0042,.004],
["TTD",.0235,.011,.0083,.0072,.0065,.0062,.0061,.006,.006,.006,.0088,.0085,.0081],
["TWD",.042,.0206,.016,.014,.0129,.0124,.0122,.0121,.012,.012,.0118,.0115,.0111],
["UGX",.048,.018,.0116,.0088,.0072,.0066,.0062,.0061,.0061,.006,.0088,.0085,.0081],
["UYU",.036,.0146,.01,.008,.0069,.0064,.0062,.0061,.006,.006,.0057,.0054,.005],
["VND",.046,.0174,.0113,.0087,.0071,.0065,.0062,.0061,.0061,.006,.0057,.0054,.005],
["ZAR",.0399,.0287,.0163,.0109,.0078,.0066,.006,.0057,.0056,.0055,.0045,.0042,.004],
["ZMW",.0475,.0179,.0115,.0088,.0072,.0066,.0062,.0061,.0061,.006,.0088,.0085,.0081]
];

const offRaw = [
["Nomad","Domestic","FasterACH",0,30000,0.60,"USD","USD","","2025-01-01",""],
["Nomad","Domestic","FasterACH",30001,60000,0.50,"USD","USD","","2025-02-01",""],
["Nomad","Domestic","FasterACH",60001,90000,0.40,"USD","USD","","2025-02-01",""],
["Nomad","Domestic","FasterACH",90000,10000000,0.30,"USD","USD","","2025-02-01",""],
["Nomad","USD Abroad","Standard",0,10000000,29.00,"USD","USD","Wire","2025-01-01",""],
["Skydo","Domestic","Standard",0,10000000,0.50,"USD","USD","","2025-03-01",""],
["Stampli","Domestic","FasterACH",0,10000000,0.30,"USD","USD","",STAMPLI_EFFECTIVE_DATE,""],
["Stampli","Domestic","Standard",0,10000000,0.05,"USD","USD","",STAMPLI_EFFECTIVE_DATE,""],
["Stampli","USD Abroad","Standard",0,10000000,15.00,"USD","USD","Wire",STAMPLI_EFFECTIVE_DATE,""],
["TripleA","Domestic","Standard",0,10000000,0.80,"USD","USD","","2024-04-01",""],
["TripleA","USD Abroad","Standard",0,10000000,24.00,"USD","USD","Wire","2024-04-01",""],
["Capi","Domestic","Standard",0,10000000,1.00,"USD","USD","","2024-04-28",""],
["Capi","CAD Domestic","Standard",0,10000000,1.00,"CAD","CAD","","2024-04-28",""],
["Capi","USD Abroad","Standard",0,10000000,20.00,"USD","USD","Wire","2024-04-28",""],
["Nsave","FX","Standard",0,10000000,15.00,"USD","","Wire","2025-05-01",""],
["Nsave","USD Abroad","Standard",0,10000000,15.00,"USD","USD","Wire","2025-05-01",""],
["Cellpay","Domestic","Standard",0,10000000,0.70,"USD","USD","","2025-06-01",""],
["Nuvion","Domestic","Standard",0,10000000,0.50,"USD","USD","","2025-10-01",""],
["Nuvion","CAD Domestic","Standard",0,10000000,0.50,"CAD","CAD","","2025-10-01",""],
["Nuvion","Domestic","FasterACH",0,10000000,0.80,"USD","USD","","2025-10-01",""],
["Nuvion","USD Abroad","Standard",0,10000000,19.00,"USD","USD","Wire","2025-10-01",""],
["Yeepay","Domestic","Standard",0,10000000,0.25,"USD","USD","","2025-11-01",""],
["Yeepay","Domestic","FasterACH",0,10000000,1.00,"USD","USD","","2025-11-01",""],
["Yeepay","Domestic","RTP",0,10000000,1.50,"USD","USD","","2025-11-01",""],
["Yeepay","CAD Domestic","Standard",0,10000000,0.40,"CAD","CAD","EFT","2025-11-01",""],
["Yeepay","USD Abroad","Standard",0,10000000,6.50,"USD","USD","","2025-11-01",""],
["Yeepay","Domestic","Standard",0,10000000,8.00,"USD","USD","Wire","2025-11-01",""],
["Yeepay","FX","Standard",0,10000000,20.00,"USD","","Wire","2025-11-01",""],
["Clearshift","Domestic","Standard",0,999,1.00,"USD","USD","","2026-01-01",""],
["Clearshift","Domestic","Standard",1000,4999,0.75,"USD","USD","","2026-01-01",""],
["Clearshift","Domestic","Standard",5000,19999,0.50,"USD","USD","","2026-01-01",""],
["Clearshift","Domestic","Standard",20000,10000000,0.25,"USD","USD","","2026-01-01",""],
["Clearshift","CAD Domestic","Standard",0,999,1.49,"CAD","CAD","","2026-01-01",""],
["Clearshift","CAD Domestic","Standard",1000,4999,1.24,"CAD","CAD","","2026-01-01",""],
["Clearshift","CAD Domestic","Standard",5000,19999,0.99,"CAD","CAD","","2026-01-01",""],
["Clearshift","CAD Domestic","Standard",20000,10000000,0.74,"CAD","CAD","","2026-01-01",""],
["Clearshift","GBP Domestic","Standard",0,999,0.75,"GBP","GBP","","2026-01-01",""],
["Clearshift","GBP Domestic","Standard",1000,4999,0.55,"GBP","GBP","","2026-01-01",""],
["Clearshift","GBP Domestic","Standard",5000,19999,0.40,"GBP","GBP","","2026-01-01",""],
["Clearshift","GBP Domestic","Standard",20000,10000000,0.20,"GBP","GBP","","2026-01-01",""],
["Clearshift","EUR Domestic","Standard",0,999,0.85,"EUR","EUR","","2026-01-01",""],
["Clearshift","EUR Domestic","Standard",1000,4999,0.65,"EUR","EUR","","2026-01-01",""],
["Clearshift","EUR Domestic","Standard",5000,19999,0.45,"EUR","EUR","","2026-01-01",""],
["Clearshift","EUR Domestic","Standard",20000,10000000,0.22,"EUR","EUR","","2026-01-01",""],
["Clearshift","Domestic","FasterACH",0,999,1.50,"USD","USD","","2026-01-01",""],
["Clearshift","Domestic","FasterACH",1000,4999,1.15,"USD","USD","","2026-01-01",""],
["Clearshift","Domestic","FasterACH",5000,19999,0.80,"USD","USD","","2026-01-01",""],
["Clearshift","Domestic","FasterACH",20000,10000000,0.50,"USD","USD","","2026-01-01",""],
["Clearshift","CAD Domestic","FasterACH",0,999,1.99,"CAD","CAD","","2026-01-01",""],
["Clearshift","CAD Domestic","FasterACH",1000,4999,1.64,"CAD","CAD","","2026-01-01",""],
["Clearshift","CAD Domestic","FasterACH",5000,19999,1.29,"CAD","CAD","","2026-01-01",""],
["Clearshift","CAD Domestic","FasterACH",20000,10000000,0.99,"CAD","CAD","","2026-01-01",""],
["Clearshift","GBP Domestic","FasterACH",0,999,1.15,"GBP","GBP","","2026-01-01",""],
["Clearshift","GBP Domestic","FasterACH",1000,4999,0.86,"GBP","GBP","","2026-01-01",""],
["Clearshift","GBP Domestic","FasterACH",5000,19999,0.60,"GBP","GBP","","2026-01-01",""],
["Clearshift","GBP Domestic","FasterACH",20000,10000000,0.40,"GBP","GBP","","2026-01-01",""],
["Clearshift","EUR Domestic","FasterACH",0,999,1.25,"EUR","EUR","","2026-01-01",""],
["Clearshift","EUR Domestic","FasterACH",1000,4999,1.00,"EUR","EUR","","2026-01-01",""],
["Clearshift","EUR Domestic","FasterACH",5000,19999,0.70,"EUR","EUR","","2026-01-01",""],
["Clearshift","EUR Domestic","FasterACH",20000,10000000,0.45,"EUR","EUR","","2026-01-01",""],
["Clearshift","Domestic","Standard",0,10000000,10.00,"USD","USD","Wire","2026-01-01",""],
["Clearshift","FX","Standard",0,10000000,15.00,"USD","","Wire","2026-01-01",""],
["Graph Finance","Domestic","Standard",0,999,1.00,"USD","USD","","2026-01-01",""],
["Graph Finance","Domestic","Standard",1000,4999,0.75,"USD","USD","","2026-01-01",""],
["Graph Finance","Domestic","Standard",5000,19999,0.50,"USD","USD","","2026-01-01",""],
["Graph Finance","Domestic","Standard",20000,10000000,0.25,"USD","USD","","2026-01-01",""],
["Graph Finance","Domestic","FasterACH",0,999,1.50,"USD","USD","","2026-01-01",""],
["Graph Finance","Domestic","FasterACH",1000,4999,1.15,"USD","USD","","2026-01-01",""],
["Graph Finance","Domestic","FasterACH",5000,19999,0.80,"USD","USD","","2026-01-01",""],
["Graph Finance","Domestic","FasterACH",20000,10000000,0.50,"USD","USD","","2026-01-01",""],
["Graph Finance","Domestic","Standard",0,10000000,10.00,"USD","USD","Wire","2026-01-01",""],
["Graph Finance","FX","Standard",0,10000000,15.00,"USD","","Wire","2026-01-01",""],
["Remittanceshub","Domestic","Standard",0,999,1.00,"USD","USD","","2025-11-24",""],
["Remittanceshub","Domestic","Standard",1000,4999,0.47,"USD","USD","","2025-11-24",""],
["Remittanceshub","Domestic","Standard",5000,19999,0.27,"USD","USD","","2025-11-24",""],
["Remittanceshub","Domestic","Standard",20000,10000000,0.10,"USD","USD","","2025-11-24",""],
["Remittanceshub","Domestic","FasterACH",0,999,1.50,"USD","USD","","2025-11-24",""],
["Remittanceshub","Domestic","FasterACH",1000,4999,0.87,"USD","USD","","2025-11-24",""],
["Remittanceshub","Domestic","FasterACH",5000,19999,0.60,"USD","USD","","2025-11-24",""],
["Remittanceshub","Domestic","FasterACH",20000,10000000,0.35,"USD","USD","","2025-11-24",""],
["Remittanceshub","USD Abroad","Standard",0,10000000,10.00,"USD","USD","Wire","2025-11-24",""],
["Remittanceshub","CAD Domestic","Standard",0,10000000,1.00,"CAD","CAD","","2025-11-24",""],
["Remittanceshub","GBP Domestic","Standard",0,10000000,1.50,"GBP","GBP","","2025-11-24",""],
["Remittanceshub","EUR Domestic","Standard",0,10000000,1.50,"EUR","EUR","","2025-11-24",""],
["Altpay","Domestic","Standard",0,999,1.00,"USD","USD","","2026-01-01",""],
["Altpay","Domestic","Standard",1000,4999,0.75,"USD","USD","","2026-01-01",""],
["Altpay","Domestic","Standard",5000,19999,0.50,"USD","USD","","2026-01-01",""],
["Altpay","Domestic","Standard",20000,10000000,0.25,"USD","USD","","2026-01-01",""],
["Altpay","CAD Domestic","Standard",0,999,1.49,"CAD","CAD","","2026-01-01",""],
["Altpay","CAD Domestic","Standard",1000,4999,1.24,"CAD","CAD","","2026-01-01",""],
["Altpay","CAD Domestic","Standard",5000,19999,0.99,"CAD","CAD","","2026-01-01",""],
["Altpay","CAD Domestic","Standard",20000,10000000,0.74,"CAD","CAD","","2026-01-01",""],
["Altpay","GBP Domestic","Standard",0,999,0.75,"GBP","GBP","","2026-01-01",""],
["Altpay","GBP Domestic","Standard",1000,4999,0.55,"GBP","GBP","","2026-01-01",""],
["Altpay","GBP Domestic","Standard",5000,19999,0.40,"GBP","GBP","","2026-01-01",""],
["Altpay","GBP Domestic","Standard",20000,10000000,0.20,"GBP","GBP","","2026-01-01",""],
["Altpay","AUD Domestic","Standard",0,999,1.49,"AUD","AUD","","2026-01-01",""],
["Altpay","AUD Domestic","Standard",1000,4999,1.24,"AUD","AUD","","2026-01-01",""],
["Altpay","AUD Domestic","Standard",5000,19999,0.99,"AUD","AUD","","2026-01-01",""],
["Altpay","AUD Domestic","Standard",20000,10000000,0.74,"AUD","AUD","","2026-01-01",""],
["Altpay","Domestic","FasterACH",0,999,1.50,"USD","USD","","2026-01-01",""],
["Altpay","Domestic","FasterACH",1000,4999,1.15,"USD","USD","","2026-01-01",""],
["Altpay","Domestic","FasterACH",5000,19999,0.80,"USD","USD","","2026-01-01",""],
["Altpay","Domestic","FasterACH",20000,10000000,0.50,"USD","USD","","2026-01-01",""],
["Altpay","CAD Domestic","FasterACH",0,999,1.99,"CAD","CAD","","2026-01-01",""],
["Altpay","CAD Domestic","FasterACH",1000,4999,1.64,"CAD","CAD","","2026-01-01",""],
["Altpay","CAD Domestic","FasterACH",5000,19999,1.29,"CAD","CAD","","2026-01-01",""],
["Altpay","CAD Domestic","FasterACH",20000,10000000,0.99,"CAD","CAD","","2026-01-01",""],
["Altpay","AUD Domestic","FasterACH",0,999,1.99,"AUD","AUD","","2026-01-01",""],
["Altpay","AUD Domestic","FasterACH",1000,4999,1.64,"AUD","AUD","","2026-01-01",""],
["Altpay","AUD Domestic","FasterACH",5000,19999,1.29,"AUD","AUD","","2026-01-01",""],
["Altpay","AUD Domestic","FasterACH",20000,10000000,0.99,"AUD","AUD","","2026-01-01",""],
["Altpay","GBP Domestic","FasterACH",0,999,1.15,"GBP","GBP","","2026-01-01",""],
["Altpay","GBP Domestic","FasterACH",1000,4999,0.85,"GBP","GBP","","2026-01-01",""],
["Altpay","GBP Domestic","FasterACH",5000,19999,0.60,"GBP","GBP","","2026-01-01",""],
["Altpay","GBP Domestic","FasterACH",20000,10000000,0.40,"GBP","GBP","","2026-01-01",""],
["Altpay","EUR Domestic","FasterACH",0,999,1.25,"EUR","EUR","","2026-01-01",""],
["Altpay","EUR Domestic","FasterACH",1000,4999,1.00,"EUR","EUR","","2026-01-01",""],
["Altpay","EUR Domestic","FasterACH",5000,19999,0.70,"EUR","EUR","","2026-01-01",""],
["Altpay","EUR Domestic","FasterACH",20000,10000000,0.45,"EUR","EUR","","2026-01-01",""],
["Altpay","Domestic","Standard",0,10000000,10.00,"USD","USD","Wire","2026-01-01",""],
["Altpay","FX","Standard",0,10000000,15.00,"USD","","Wire","2026-01-01",""],
["Repay","Domestic","Standard",0,10000000,10.00,"USD","USD","Wire","2026-01-01",""],
["Repay","FX","Standard",0,10000000,15.00,"USD","","Wire","2026-01-01",""],
["Repay","Domestic","Standard",0,10000000,0.25,"USD","USD","","2026-01-01",""],
["LianLian","Domestic","Standard",0,10000000,1.00,"USD","USD","","2025-04-01",""],
["LianLian","CAD Domestic","Standard",0,10000000,1.49,"CAD","CAD","","2025-04-01",""],
["LianLian","GBP Domestic","Standard",0,10000000,1.00,"GBP","GBP","","2025-04-01",""],
["LianLian","EUR Domestic","Standard",0,10000000,1.00,"EUR","EUR","","2025-04-01",""],
["Blindpay","Domestic","Standard",0,999,1.00,"USD","USD","","2026-01-01",""],
["Blindpay","Domestic","Standard",1000,4999,0.75,"USD","USD","","2026-01-01",""],
["Blindpay","Domestic","Standard",5000,19999,0.50,"USD","USD","","2026-01-01",""],
["Blindpay","Domestic","Standard",20000,10000000,0.25,"USD","USD","","2026-01-01",""],
["Blindpay","Domestic","FasterACH",0,999,1.50,"USD","USD","","2026-01-01",""],
["Blindpay","Domestic","FasterACH",1000,4999,1.15,"USD","USD","","2026-01-01",""],
["Blindpay","Domestic","FasterACH",5000,19999,0.80,"USD","USD","","2026-01-01",""],
["Blindpay","Domestic","FasterACH",20000,10000000,0.50,"USD","USD","","2026-01-01",""],
["Blindpay","Domestic","Standard",0,10000000,10.00,"USD","USD","Wire","2026-01-01",""],
["Blindpay","FX","Standard",0,10000000,15.00,"USD","","Wire","2026-01-01",""]
];

export const MAJORS = "AUD,CAD,CHF,CNY,DKK,EUR,GBP,HKD,JPY,NOK,NZD,PHP,SEK,SGD,USD";
export const MINORS = "AED,BBD,BDT,BGN,BHD,BMD,BND,BRL,BSD,BWP,BZD,CRC,CZK,DOP,DZD,EGP,ETB,FJD,GHS,GTQ,GYD,HTG,HUF,IDR,ILS,INR,ISK,JMD,JOD,KES,KWD,KYD,KZT,LBP,LKR,MAD,MOP,MUR,MWK,MXN,MZN,NGN,OMR,PEN,PGK,PKR,PLN,QAR,RON,RUB,RWF,SAR,SBD,THB,TND,TOP,TRY,TTD,TZS,UGX,UYU,VND,VUV,WST,XAF,XCD,XOF,ZAR,ZMW";
export const TERTIARY = "ALL,AMD,ANG,AOA,ARS,AWG,AZN,BAM,BIF,BOB,BTN,BYN,CDF,CLP,COP,CVE,DJF,ERN,FKP,GEL,GIP,GMD,GNF,HNL,KGS,KHR,KMF,KRW,LAK,LRD,LSL,LYD,MDL,MGA,MKD,MMK,MNT,MRU,MVR,MYR,NAD,NIO,NPR,PAB,PYG,RSD,SCR,SHP,SLE,SRD,SSP,STN,SVC,SZL,TJS,TMT,TWD,UAH,UZS,VES,XPF,YER,ZWD";
const MAJOR_CCYS = MAJORS.split(",");
const MINOR_CCYS = MINORS.split(",");
const TERTIARY_CCYS = TERTIARY.split(",");
export const ALL_CCYS = [...MAJOR_CCYS, ...MINOR_CCYS, ...TERTIARY_CCYS];
export const CORRIDORS = ["Major", "Minor", "Tertiary"];

export function getCorridor(ccy) {
  if (MAJOR_CCYS.includes(ccy)) return "Major";
  if (MINOR_CCYS.includes(ccy)) return "Minor";
  if (TERTIARY_CCYS.includes(ccy)) return "Tertiary";
  return "";
}

const volRaw = [
["Whish","","",0.004,"","","","",0,1e9,"2024-10-01","",""],
["Athena","","",0.002,"","","","",0,1e9,"2025-04-01","",""],
["TripleA","","",0.002,"","","","",0,1e9,"2025-05-01","","Platform Fee"],
["TripleA","Domestic","FasterACH",0.004,"bank","bank","","USD",0,1e9,"2025-05-01","",""],
["TripleA","CA Domestic","FasterACH",0.004,"bank","bank","","CAD",0,1e9,"2025-05-01","",""],
["TripleA","","RTP",0.008,"bank","","","",0,1e9,"2025-05-01","",""],
["TripleA","FX","",0.005,"","","",MAJORS,0,1e9,"2025-05-01","","GDV based"],
["TripleA","","Expedited",0.008,"","card","Debit","",0,1e9,"2025-05-01","",""],
["TripleA","","",0.004,"","virtualcard","","",0,1e9,"2025-05-01","",""],
["Capi","","",0,"","","","",0,1e9,"2024-04-28","",""],
["Nsave","FX","",0.015,"","","","PKR",0,1e9,"2025-05-01","",""],
["Nsave","FX","",0.015,"","","","BDT",0,1e9,"2025-05-01","",""],
["Nsave","FX","",0.015,"","","","DZD",0,1e9,"2025-05-01","",""],
["Nsave","FX","",0.009,"","","","EGP",0,1e9,"2025-05-01","",""],
["Nsave","FX","",0.0065,"","","","TRY",0,1e9,"2025-05-01","",""],
["Nsave","","",0.01,"","Card","Debit","",0,1e9,"2025-05-01","",""],
["Cellpay","Domestic","FasterACH",0.004,"","","","",0,1e9,"2025-06-01","",""],
["Cellpay","","RTP",0.008,"","","","",0,1e9,"2025-06-01","",""],
["Cellpay","","",0.0325,"Card","","","",0,1e9,"2025-06-01","","Card 3.25%"],
["Cellpay","","",0.008,"","","","",0,1e9,"2025-06-01","",""],
["Cellpay","FX","",0.005,"","","","CAD",0,1e9,"2025-06-01","",""],
["Nuvion","","RTP",0.006,"","","","",0,1e9,"2025-10-01","",""],
["Nuvion","FX","",0.003,"","","",MAJORS,0,1e9,"2025-10-01","","Majors"],
["Nuvion","FX","",0.0075,"","","",MINORS,0,1e9,"2025-10-01","","Minors"],
["Nuvion","FX","",0.015,"","","",TERTIARY,0,1e9,"2025-10-01","","Tertiary"],
["Yeepay","Domestic","FasterACH",0.001,"","","","",0,1e9,"2025-11-01","",""],
["Yeepay","Domestic","RTP",0.001,"","","","",0,1e9,"2025-11-01","",""],
["Yeepay","","",0.007,"","","","GBP",0,1e9,"2025-11-01","",""],
["Yeepay","FX","",0.002,"","","",MAJORS,0,49,"2025-11-01","","0-49 txns"],
["Yeepay","FX","",0.001,"","","",MAJORS,50,100,"2025-11-01","","50-100"],
["Yeepay","FX","",0.0008,"","","",MAJORS,101,1e9,"2025-11-01","","101+"],
["Yeepay","Domestic","",0.0125,"Card","","Debit","",0,1e9,"2025-11-01","",""],
["Yeepay","Domestic","",0.03,"Card","","Credit","",0,1e9,"2025-11-01","",""],
["Clearshift","Domestic","RTP",0.009,"","","","",0,999999,"2026-01-01","",""],
["Clearshift","Domestic","RTP",0.0075,"","","","",1e6,4990000,"2026-01-01","",""],
["Clearshift","Domestic","RTP",0.006,"","","","",5e6,49990000,"2026-01-01","",""],
["Clearshift","Domestic","RTP",0.005,"","","","",5e7,1e9,"2026-01-01","",""],
["Clearshift","Domestic","",0.035,"Card","","Credit","",0,999999,"2026-01-01","",""],
["Clearshift","Domestic","",0.03,"Card","","Credit","",1e6,4990000,"2026-01-01","",""],
["Clearshift","Domestic","",0.0275,"Card","","Credit","",5e6,49990000,"2026-01-01","",""],
["Clearshift","Domestic","",0.025,"Card","","Credit","",5e7,1e9,"2026-01-01","",""],
["Clearshift","FX","",0.045,"Card","","Credit","",0,999999,"2026-01-01","",""],
["Clearshift","FX","",0.04,"Card","","Credit","",1e6,4990000,"2026-01-01","",""],
["Clearshift","FX","",0.0375,"Card","","Credit","",5e6,49990000,"2026-01-01","",""],
["Clearshift","FX","",0.035,"Card","","Credit","",5e7,1e9,"2026-01-01","",""],
["Clearshift","Domestic","",0.02,"Card","","Debit","",0,999999,"2026-01-01","",""],
["Clearshift","Domestic","",0.0175,"Card","","Debit","",1e6,4990000,"2026-01-01","",""],
["Clearshift","Domestic","",0.015,"Card","","Debit","",5e6,49990000,"2026-01-01","",""],
["Clearshift","Domestic","",0.0125,"Card","","Debit","",5e7,1e9,"2026-01-01","",""],
["Clearshift","FX","",0.03,"Card","","Debit","",0,999999,"2026-01-01","",""],
["Clearshift","FX","",0.0275,"Card","","Debit","",1e6,4990000,"2026-01-01","",""],
["Clearshift","FX","",0.025,"Card","","Debit","",5e6,49990000,"2026-01-01","",""],
["Clearshift","FX","",0.0225,"Card","","Debit","",5e7,1e9,"2026-01-01","",""],
["Clearshift","FX","",0.005,"","","",MAJORS,0,999999,"2026-01-01","",""],
["Clearshift","FX","",0.004,"","","",MAJORS,1e6,4990000,"2026-01-01","",""],
["Clearshift","FX","",0.003,"","","",MAJORS,5e6,49990000,"2026-01-01","",""],
["Clearshift","FX","",0.0025,"","","",MAJORS,5e7,1e9,"2026-01-01","",""],
["Clearshift","FX","",0.01,"","","",MINORS,0,999999,"2026-01-01","",""],
["Clearshift","FX","",0.0075,"","","",MINORS,1e6,4990000,"2026-01-01","",""],
["Clearshift","FX","",0.005,"","","",MINORS,5e6,49990000,"2026-01-01","",""],
["Clearshift","FX","",0.0025,"","","",MINORS,5e7,1e9,"2026-01-01","",""],
["Clearshift","FX","",0.02,"","","",TERTIARY,0,999999,"2026-01-01","",""],
["Clearshift","FX","",0.0175,"","","",TERTIARY,1e6,4990000,"2026-01-01","",""],
["Clearshift","FX","",0.015,"","","",TERTIARY,5e6,49990000,"2026-01-01","",""],
["Clearshift","FX","",0.0125,"","","",TERTIARY,5e7,1e9,"2026-01-01","",""],
["Graph Finance","Domestic","RTP",0.009,"","","","",0,999999,"2026-01-01","",""],
["Graph Finance","Domestic","RTP",0.0075,"","","","",1e6,4990000,"2026-01-01","",""],
["Graph Finance","Domestic","RTP",0.006,"","","","",5e6,49990000,"2026-01-01","",""],
["Graph Finance","Domestic","RTP",0.005,"","","","",5e7,1e9,"2026-01-01","",""],
["Graph Finance","FX","",0.005,"","","",MAJORS,0,999999,"2026-01-01","",""],
["Graph Finance","FX","",0.004,"","","",MAJORS,1e6,4990000,"2026-01-01","",""],
["Graph Finance","FX","",0.003,"","","",MAJORS,5e6,49990000,"2026-01-01","",""],
["Graph Finance","FX","",0.0025,"","","",MAJORS,5e7,1e9,"2026-01-01","",""],
["Graph Finance","FX","",0.01,"","","",MINORS,0,999999,"2026-01-01","",""],
["Graph Finance","FX","",0.0075,"","","",MINORS,1e6,4990000,"2026-01-01","",""],
["Graph Finance","FX","",0.005,"","","",MINORS,5e6,49990000,"2026-01-01","",""],
["Graph Finance","FX","",0.0025,"","","",MINORS,5e7,1e9,"2026-01-01","",""],
["Graph Finance","FX","",0.02,"","","",TERTIARY,0,999999,"2026-01-01","",""],
["Graph Finance","FX","",0.0175,"","","",TERTIARY,1e6,4990000,"2026-01-01","",""],
["Graph Finance","FX","",0.015,"","","",TERTIARY,5e6,49990000,"2026-01-01","",""],
["Graph Finance","FX","",0.0125,"","","",TERTIARY,5e7,1e9,"2026-01-01","",""],
["Remittanceshub","FX","",0.001,"","","",MAJORS,0,1e10,"2025-11-24","",""],
["Remittanceshub","FX","",0.002,"","","",MINORS,0,1e10,"2025-11-24","",""],
["Altpay","Domestic","",0.02,"Card","","Debit","",0,999999,"2026-01-01","",""],
["Altpay","Domestic","",0.0175,"Card","","Debit","",1e6,4990000,"2026-01-01","",""],
["Altpay","Domestic","",0.015,"Card","","Debit","",5e6,49990000,"2026-01-01","",""],
["Altpay","Domestic","",0.0125,"Card","","Debit","",5e7,1e9,"2026-01-01","",""],
["Altpay","FX","",0.03,"Card","","Debit","",0,999999,"2026-01-01","",""],
["Altpay","FX","",0.0275,"Card","","Debit","",1e6,4990000,"2026-01-01","",""],
["Altpay","FX","",0.025,"Card","","Debit","",5e6,49990000,"2026-01-01","",""],
["Altpay","FX","",0.0225,"Card","","Debit","",5e7,1e9,"2026-01-01","",""],
["Altpay","FX","",0.005,"","","",MAJORS,0,999999,"2026-01-01","",""],
["Altpay","FX","",0.004,"","","",MAJORS,1e6,4990000,"2026-01-01","",""],
["Altpay","FX","",0.003,"","","",MAJORS,5e6,49990000,"2026-01-01","",""],
["Altpay","FX","",0.0025,"","","",MAJORS,5e7,1e9,"2026-01-01","",""],
["Altpay","FX","",0.01,"","","",MINORS,0,999999,"2026-01-01","",""],
["Altpay","FX","",0.0075,"","","",MINORS,1e6,4990000,"2026-01-01","",""],
["Altpay","FX","",0.005,"","","",MINORS,5e6,49990000,"2026-01-01","",""],
["Altpay","FX","",0.0025,"","","",MINORS,5e7,1e9,"2026-01-01","",""],
["Altpay","FX","",0.02,"","","",TERTIARY,0,999999,"2026-01-01","",""],
["Altpay","FX","",0.0175,"","","",TERTIARY,1e6,4990000,"2026-01-01","",""],
["Altpay","FX","",0.015,"","","",TERTIARY,5e6,49990000,"2026-01-01","",""],
["Altpay","FX","",0.0125,"","","",TERTIARY,5e7,1e9,"2026-01-01","",""],
["Repay","","RTP",0.009,"","","","",0,999999,"2026-01-01","",""],
["Repay","","RTP",0.0075,"","","","",1e6,4990000,"2026-01-01","",""],
["Repay","","RTP",0.006,"","","","",5e6,49990000,"2026-01-01","",""],
["Repay","","RTP",0.005,"","","","",5e7,1e9,"2026-01-01","",""],
["Repay","","",0.009,"","Card","Debit","",0,999999,"2026-01-01","",""],
["Repay","","",0.0075,"","Card","Debit","",1e6,4990000,"2026-01-01","",""],
["Repay","","",0.006,"","Card","Debit","",5e6,49990000,"2026-01-01","",""],
["Repay","","",0.005,"","Card","Debit","",5e7,1e9,"2026-01-01","",""],
["Repay","FX","",0.005,"","","",MAJORS,0,999999,"2026-01-01","",""],
["Repay","FX","",0.004,"","","",MAJORS,1e6,4990000,"2026-01-01","",""],
["Repay","FX","",0.003,"","","",MAJORS,5e6,49990000,"2026-01-01","",""],
["Repay","FX","",0.0025,"","","",MAJORS,5e7,1e9,"2026-01-01","",""],
["Repay","FX","",0.01,"","","",MINORS,0,999999,"2026-01-01","",""],
["Repay","FX","",0.0075,"","","",MINORS,1e6,4990000,"2026-01-01","",""],
["Repay","FX","",0.005,"","","",MINORS,5e6,49990000,"2026-01-01","",""],
["Repay","FX","",0.0025,"","","",MINORS,5e7,1e9,"2026-01-01","",""],
["Repay","FX","",0.02,"","","",TERTIARY,0,999999,"2026-01-01","",""],
["Repay","FX","",0.0175,"","","",TERTIARY,1e6,4990000,"2026-01-01","",""],
["Repay","FX","",0.015,"","","",TERTIARY,5e6,49990000,"2026-01-01","",""],
["Repay","FX","",0.0125,"","","",TERTIARY,5e7,1e9,"2026-01-01","",""],
["Maplewave","","",0.002,"","","","",0,1e9,"2026-01-01","2026-01-31","Jan only"],
["Maplewave","","",0.0025,"","","","",0,5e7,"2026-02-01","","0-50M"],
["Maplewave","","",0.002,"","","","",50000001,1e9,"2026-02-01","","50M+"],
["LianLian","","FasterACH",0.005,"","","","",0,1e9,"2025-04-01","",""],
["LianLian","","RTP",0.01,"","","","",0,1e9,"2025-04-01","",""],
["LianLian","","",0.035,"","","Debit","",0,1e9,"2025-04-01","","Card Debit"],
["LianLian","","",0.035,"","","Credit","",0,1e9,"2025-04-01","","Card Credit"],
["LianLian","FX","",0.005,"","","","USD",0,1e9,"2025-04-01","",""],
["Blindpay","","RTP",0.009,"","","","",0,999999,"2026-01-01","",""],
["Blindpay","","RTP",0.0075,"","","","",1e6,4990000,"2026-01-01","",""],
["Blindpay","","RTP",0.006,"","","","",5e6,49990000,"2026-01-01","",""],
["Blindpay","","RTP",0.005,"","","","",5e7,1e9,"2026-01-01","",""],
["Blindpay","Domestic","",0.02,"Card","","Debit","",0,999999,"2026-01-01","",""],
["Blindpay","Domestic","",0.0175,"Card","","Debit","",1e6,4990000,"2026-01-01","",""],
["Blindpay","Domestic","",0.015,"Card","","Debit","",5e6,49990000,"2026-01-01","",""],
["Blindpay","Domestic","",0.0125,"Card","","Debit","",5e7,1e9,"2026-01-01","",""],
["Blindpay","FX","",0.03,"Card","","Debit","",0,999999,"2026-01-01","",""],
["Blindpay","FX","",0.0275,"Card","","Debit","",1e6,4990000,"2026-01-01","",""],
["Blindpay","FX","",0.025,"Card","","Debit","",5e6,49990000,"2026-01-01","",""],
["Blindpay","FX","",0.0225,"Card","","Debit","",5e7,1e9,"2026-01-01","",""],
["Blindpay","Domestic","",0.035,"Card","","Credit","",0,999999,"2026-01-01","",""],
["Blindpay","Domestic","",0.03,"Card","","Credit","",1e6,4990000,"2026-01-01","",""],
["Blindpay","Domestic","",0.0275,"Card","","Credit","",5e6,49990000,"2026-01-01","",""],
["Blindpay","Domestic","",0.025,"Card","","Credit","",5e7,1e9,"2026-01-01","",""],
["Blindpay","FX","",0.045,"Card","","Credit","",0,999999,"2026-01-01","",""],
["Blindpay","FX","",0.04,"Card","","Credit","",1e6,4990000,"2026-01-01","",""],
["Blindpay","FX","",0.0375,"Card","","Credit","",5e6,49990000,"2026-01-01","",""],
["Blindpay","FX","",0.035,"Card","","Credit","",5e7,1e9,"2026-01-01","",""],
["Blindpay","","",0.009,"","Card","Debit","",0,999999,"2026-01-01","",""],
["Blindpay","","",0.0075,"","Card","Debit","",1e6,4990000,"2026-01-01","",""],
["Blindpay","","",0.006,"","Card","Debit","",5e6,49990000,"2026-01-01","",""],
["Blindpay","","",0.005,"","Card","Debit","",5e7,1e9,"2026-01-01","",""],
["Blindpay","FX","",0.005,"","","",MAJORS,0,999999,"2026-01-01","",""],
["Blindpay","FX","",0.004,"","","",MAJORS,1e6,4990000,"2026-01-01","",""],
["Blindpay","FX","",0.003,"","","",MAJORS,5e6,49990000,"2026-01-01","",""],
["Blindpay","FX","",0.0025,"","","",MAJORS,5e7,1e9,"2026-01-01","",""],
["Blindpay","FX","",0.01,"","","",MINORS,0,999999,"2026-01-01","",""],
["Blindpay","FX","",0.0075,"","","",MINORS,1e6,4990000,"2026-01-01","",""],
["Blindpay","FX","",0.005,"","","",MINORS,5e6,49990000,"2026-01-01","",""],
["Blindpay","FX","",0.0025,"","","",MINORS,5e7,1e9,"2026-01-01","",""],
["Blindpay","FX","",0.02,"","","",TERTIARY,0,999999,"2026-01-01","",""],
["Blindpay","FX","",0.0175,"","","",TERTIARY,1e6,4990000,"2026-01-01","",""],
["Blindpay","FX","",0.015,"","","",TERTIARY,5e6,49990000,"2026-01-01","",""],
["Blindpay","FX","",0.0125,"","","",TERTIARY,5e7,1e9,"2026-01-01","",""]
];

const isVolFx = (r) => (r[1] === "FX" && r[4] !== "Card" && r[5] !== "Card" && !r[6]) || (r[1] === "" && !r[2] && r[7] && r[7] !== MAJORS && r[7] !== MINORS && r[7] !== TERTIARY && ALL_CCYS.includes(r[7]) && r[4] !== "Card");

export const PRODUCT_TYPES = ["ACH", "FasterACH", "RTP", "Wire", "FX Majors", "FX Minors", "FX Tertiary", "Card Credit Domestic", "Card Credit FX", "Card Debit Domestic", "Card Debit FX", "Push-to-Debit", "GBP 0.7%"];
export const LOOKER_IMPORT_PERIOD = importedLookerData.period || "2025-02";
export const LOOKER_IMPORT_GAPS = importedLookerData.gaps || [];
export const LOOKER_DETAIL_MANIFEST = importedLookerData.detailManifest || {};

const minRaw = [
["Stampli",STAMPLI_EFFECTIVE_DATE,"2024-12-31",0,0,1e9],
["Stampli","2025-01-01","2025-06-30",4000,0,1e9],
["Stampli","2025-07-01","",8000,0,1e9],
["GME_Remit","2024-08-01","2025-07-31",2500,0,1e9],
["Oson","2024-08-01","2024-08-31",0,0,1e9],
["Oson","2024-09-01","2024-11-30",5000,0,1e9],
["Oson","2024-12-01","",7500,0,1e9],
["Whish","2024-08-01","2024-08-31",0,0,1e9],
["Whish","2024-09-01","2025-02-28",7500,0,1e9],
["Whish","2025-03-01","",10000,0,1e9],
["Everflow","2024-01-01","",0,0,1e9],
["Nomad","2025-01-01","",10000,0,1e9],
["Q2","2024-01-01","",0,0,1e9],
["Finastra","2024-01-01","",0,0,1e9],
["Magaya","2024-01-01","",0,0,1e9],
["Fulfil","2024-01-01","",0,0,1e9],
["Shepherd","2024-07-01","",0,0,1e9],
["TripleA","2025-05-01","",2500,0,1e9],
["Cellpay","2025-08-01","",5000,0,1e9],
["Halorecruiting","2024-01-01","",0,0,1e9],
["Clearshift","2026-02-01","",5000,0,999999],
["Clearshift","2026-02-01","",7500,1e6,4990000],
["Clearshift","2026-02-01","",10000,5e6,49990000],
["Clearshift","2026-02-01","",12500,5e7,1e9],
["Graph Finance","2026-02-01","",5000,0,999999],
["Graph Finance","2026-02-01","",7500,1e6,4990000],
["Graph Finance","2026-02-01","",10000,5e6,49990000],
["Graph Finance","2026-02-01","",12500,5e7,1e9],
["Remittanceshub","2026-06-01","",4000,0,1e9],
["Altpay","2026-02-01","2026-04-30",2500,0,1990000],
["Altpay","2026-05-01","2026-07-31",4000,0,1990000],
["Altpay","2026-08-01","",5000,0,1990000],
["Altpay","2026-02-01","",7500,2e6,9990000],
["Altpay","2026-02-01","",10000,1e7,49990000],
["Altpay","2026-02-01","",12500,5e7,1e9],
["Repay","2026-02-01","",5000,0,1990000],
["Repay","2026-02-01","",7500,2e6,9990000],
["Repay","2026-02-01","",10000,1e7,49990000],
["Repay","2026-02-01","",12500,5e7,1e9],
["Maplewave","2026-02-01","2026-02-28",5000,0,1e9],
["Maplewave","2026-03-01","2026-03-31",10000,0,1e9],
["Maplewave","2026-04-01","",15000,0,1e9]
];

const platformFeesRaw = [
  { partner: "Skydo", monthlyFee: 1000, startDate: "2025-03-01", endDate: "2025-06-30" },
  { partner: "Skydo", monthlyFee: 3000, startDate: "2025-07-01", endDate: "" },
  { partner: "Capi", monthlyFee: 10000, startDate: "2025-09-01", endDate: "" },
  { partner: "Nsave", monthlyFee: 4000, startDate: "2025-05-01", endDate: "" },
  { partner: "Yeepay", monthlyFee: 5000, startDate: "2025-12-01", endDate: "" }
];

const revFeeRaw = [
["Whish","",2.25,"2025-01-01",""],
["Nomad","",2.25,"2025-01-01",""],
["TripleA","",5.00,"2025-01-01",""],
["Cellpay","",5.00,"2025-06-01",""],
["Nuvion","Bank",5.00,"2025-10-01",""],
["Nuvion","Wallet",5.00,"2025-10-01",""],
["Nuvion","Card",30.00,"2025-10-01",""],
["Yeepay","Bank",5.00,"2025-11-01",""],
["Yeepay","Wallet",5.00,"2025-11-01",""],
["Yeepay","Card",25.00,"2025-11-01",""],
["Remittanceshub","Bank",5.00,"2025-11-24",""],
["Remittanceshub","Wallet",5.00,"2025-11-24",""],
["Remittanceshub","Card",25.00,"2025-11-24",""],
["Altpay","Bank",5.00,"2026-01-01",""],
["Altpay","Wallet",5.00,"2026-01-01",""],
["Altpay","Card",25.00,"2026-01-01",""],
["Repay","Bank",5.00,"2026-01-01",""],
["Repay","Wallet",5.00,"2026-01-01",""],
["Repay","Card",25.00,"2026-01-01",""],
["Blindpay","Bank",5.00,"2026-01-01",""],
["Blindpay","Wallet",5.00,"2026-01-01",""],
["Blindpay","Card",25.00,"2026-01-01",""]
];

const implFeesRaw = [
  { partner: "Nuvion", feeType: "Account Setup", feeAmount: 50, goLiveDate: "", applyAgainstMin: false, note: "One-time per business - KYC/AML/liveness" },
  { partner: "Nuvion", feeType: "Daily Settlement", feeAmount: 10, goLiveDate: "", applyAgainstMin: false, note: "Per settlement sweep from wallet to bank" },
  { partner: "Athena", feeType: "Implementation", feeAmount: 25000, goLiveDate: "2025-03-01", applyAgainstMin: false, note: "One-time, ends 2025-03-07" },
  { partner: "TripleA", feeType: "Implementation", feeAmount: 5000, goLiveDate: "2025-05-01", applyAgainstMin: false, note: "" },
  { partner: "Capi", feeType: "Implementation", feeAmount: 10000, goLiveDate: "2025-05-01", applyAgainstMin: false, creditMode: "Monthly Subscription", creditAmount: 10000, creditWindowDays: 90, note: "Refunded as an offset against future monthly subscription fees if launch occurs within 90 days of effective date" },
  { partner: "Nsave", feeType: "Implementation", feeAmount: 10000, goLiveDate: "2025-05-01", applyAgainstMin: false, creditMode: "Monthly Subscription", creditAmount: 10000, creditWindowDays: 90, note: "Refunded as an offset against future monthly subscription fees if launch occurs within 90 days of effective date" },
  { partner: "Cellpay", feeType: "Implementation", feeAmount: 10000, goLiveDate: "2025-06-01", applyAgainstMin: false, note: "" },
  { partner: "Blindpay", feeType: "Implementation", feeAmount: 10000, goLiveDate: "2026-02-18", applyAgainstMin: false, note: "Due upon execution of the contract" },
  { partner: "Nuvion", feeType: "Implementation", feeAmount: 10000, goLiveDate: "2025-11-01", applyAgainstMin: false, note: "" },
  { partner: "Yeepay", feeType: "Implementation", feeAmount: 10000, goLiveDate: "2025-12-01", applyAgainstMin: false, note: "Waived if launch within 3mo of effective date" },
  { partner: "Clearshift", feeType: "Implementation", feeAmount: 10000, goLiveDate: "2026-01-01", billingDate: "2026-01-01", applyAgainstMin: true, creditMode: "Monthly Minimum", creditAmount: 10000, creditWindowDays: 90, note: "Credited against future monthly minimum fees if launch occurs within 90 days of effective date" },
  { partner: "Graph Finance", feeType: "Implementation", feeAmount: 10000, goLiveDate: "2026-01-01", applyAgainstMin: true, creditMode: "Monthly Minimum", creditAmount: 10000, creditWindowDays: 90, note: "Credited against future monthly minimum fees if launch occurs within 90 days of effective date" },
  { partner: "Remittanceshub", feeType: "Implementation", feeAmount: 4000, goLiveDate: "2026-01-01", applyAgainstMin: false, note: "" },
  { partner: "Altpay", feeType: "Implementation", feeAmount: 10000, goLiveDate: "2026-02-01", applyAgainstMin: true, creditMode: "Monthly Minimum", creditAmount: 10000, creditWindowDays: 90, note: "Credited against future monthly minimum fees if launch occurs within 90 days of effective date" },
  { partner: "Repay", feeType: "Implementation", feeAmount: 10000, goLiveDate: "2026-02-01", applyAgainstMin: false, note: "" },
  { partner: "Maplewave", feeType: "Implementation", feeAmount: 10000, goLiveDate: "2026-01-01", billingDate: "2026-01-31", applyAgainstMin: false, note: "" },
  { partner: "Goldstack", feeType: "Implementation", feeAmount: 10000, goLiveDate: "", applyAgainstMin: true, creditMode: "Monthly Minimum", creditAmount: 10000, creditWindowDays: 90, note: "Credited against future monthly minimum fees if launch occurs within 90 days of effective date" },
  { partner: "OhentPay", feeType: "Implementation", feeAmount: 10000, goLiveDate: "", applyAgainstMin: true, creditMode: "Monthly Minimum", creditAmount: 10000, creditWindowDays: 90, note: "Credited against future monthly minimum fees if launch occurs within 90 days of effective date" }
];

const virtualAccountFeesRaw = [
  { partner: "Blindpay", feeType: "Account Opening", minAccounts: 1, maxAccounts: 99, discount: 0, feePerAccount: 5.0, note: "Tier 1 - 0% discount" },
  { partner: "Blindpay", feeType: "Account Opening", minAccounts: 100, maxAccounts: 999, discount: 0.5, feePerAccount: 2.5, note: "Tier 2 - 50% discount" },
  { partner: "Blindpay", feeType: "Account Opening", minAccounts: 1000, maxAccounts: 4999, discount: 0.8, feePerAccount: 1.0, note: "Tier 3 - 80% discount" },
  { partner: "Blindpay", feeType: "Account Opening", minAccounts: 5000, maxAccounts: 10000000, discount: 0.95, feePerAccount: 0.25, note: "Tier 4 - 95% discount" },
  { partner: "Blindpay", feeType: "Dormancy", minAccounts: 1, maxAccounts: 99, discount: 0, feePerAccount: 2.0, note: "Tier 1 - monthly per dormant acct" },
  { partner: "Blindpay", feeType: "Dormancy", minAccounts: 100, maxAccounts: 999, discount: 0, feePerAccount: 1.25, note: "Tier 2" },
  { partner: "Blindpay", feeType: "Dormancy", minAccounts: 1000, maxAccounts: 4999, discount: 0, feePerAccount: 0.75, note: "Tier 3" },
  { partner: "Blindpay", feeType: "Dormancy", minAccounts: 5000, maxAccounts: 10000000, discount: 0, feePerAccount: 0.25, note: "Tier 4" },
  { partner: "Yeepay", feeType: "Account Closing", minAccounts: 1, maxAccounts: 10000000, discount: 0, feePerAccount: 5.0, note: "$5 closing fee if inactive 6mo" },
  { partner: "Nuvion", feeType: "Account Opening", minAccounts: 1, maxAccounts: 10000000, discount: 0, feePerAccount: 2.0, note: "Per account" },
  { partner: "Nuvion", feeType: "Dormancy", minAccounts: 1, maxAccounts: 10000000, discount: 0, feePerAccount: 0.75, note: "Monthly per dormant acct (90 day rule)" }
];

const surchRaw = [];

const providerCostRaw = [];
const mkC = (provider, direction, txnName, corridorType, worldlink, minAmt, maxAmt, varFixed, fee, feeType, paymentOrChargeback) => providerCostRaw.push({ provider, direction, txnName, corridorType, worldlink, minAmt, maxAmt, varFixed, fee, feeType, paymentOrChargeback });
mkC("Citi","Out","Wire","Cross Border",true,0,10000000,"Fixed",4.00,"Per Item","Payment");
mkC("Citi","Out","ACH & SEPA","Cross Border",true,0,10000000,"Fixed",1.50,"Per Item","Payment");
mkC("Citi","Out","Instant Payments","Cross Border",true,0,10000000,"Fixed",3.00,"Per Item","Payment");
mkC("Citi","Out","ACH Return Fee","Cross Border",true,0,10000000,"Fixed",20.00,"Per Item","Chargeback");
mkC("Citi","Out","Wire Return Fee","Cross Border",true,0,10000000,"Fixed",15.00,"Per Item","Chargeback");
mkC("Citi","Out","Domestic/Cross-Border Wire","Domestic/Cross Border",false,0,10000000,"Fixed",4.00,"Per Item","Payment");
mkC("Citi","Out","Wire Return Fee","Domestic/Cross Border",false,0,10000000,"Fixed",8.50,"Per Item","Chargeback");
mkC("Citi","Out","ACH","Domestic",false,0,10000000,"Fixed",0.04,"Per Item","Payment");
mkC("Citi","Out","ACH - Same Day","Domestic",false,0,10000000,"Fixed",0.25,"Per Item","Payment");
mkC("Citi","Out","ACH Return Item","Domestic",false,0,10000000,"Fixed",2.25,"Per Item","Payment");
mkC("Citi","Out","ACH Return Fee","Domestic",false,0,10000000,"Fixed",65.00,"Per Item","Chargeback");
mkC("Citi","Out","Instant Payments","Domestic",false,0,3000,"Fixed",0.70,"Per Item","Payment");
mkC("Citi","Out","Instant Payments","Domestic",false,3001,10000,"Fixed",0.55,"Per Item","Payment");
mkC("Citi","Out","Instant Payments","Domestic",false,10001,10000000,"Fixed",0.40,"Per Item","Payment");
mkC("Citi","Out","Book Transfer","Domestic",false,0,10000000,"Fixed",4.00,"Per Item","Payment");
mkC("Citi","Out","Check Payment","Domestic",false,0,10000000,"Fixed",0.15,"Per Item","Payment");
mkC("Citi","Out","Check Return Fee","Domestic",false,0,10000000,"Fixed",50.00,"Per Item","Chargeback");
mkC("Citi","Out","Check Image - Capture","Domestic",false,0,10000000,"Fixed",0.04,"Per Item","Other");
mkC("Citi","In","Domestic Wire","Domestic",false,0,10000000,"Fixed",4.00,"Per Item","Payment");
mkC("Citi","In","Cross Border Wire","Cross Border",false,0,10000000,"Fixed",4.00,"Per Item","Payment");
mkC("Citi","In","ACH","Domestic",false,0,10000000,"Fixed",0.30,"Per Item","Payment");
mkC("Citi","In","Book Transfer","Domestic",false,0,10000000,"Fixed",4.00,"Per Item","Payment");
mkC("SVB","Out","ACH","Domestic",false,0,10000000,"Fixed",0.15,"Per Item","Payment");
mkC("SVB","Out","ACH - Same Day","Domestic",false,0,10000000,"Fixed",0.35,"Per Item","Payment");
mkC("SVB","Out","ACH Return Fee","Domestic",false,0,10000000,"Fixed",2.00,"Per Item","Chargeback");
mkC("SVB","Out","Wire Transfer - USD","Cross Border",false,0,10000000,"Fixed",8.00,"Per Item","Payment");
mkC("SVB","Out","Wire Transfer - Fx","Cross Border",false,0,10000000,"Fixed",4.00,"Per Item","Payment");
mkC("SVB","Out","Wire Transfer","Domestic",false,0,10000000,"Fixed",9.00,"Per Item","Payment");
mkC("SVB","In","Wire Transfer","Domestic",false,0,10000000,"Fixed",10.00,"Per Item","Payment");
mkC("SVB","Out","Wire Return Fee","Domestic/Cross Border",false,0,10000000,"Fixed",15.00,"Per Item","Chargeback");
mkC("NAB","Out","Domestic Payment - All","Domestic",false,0,10000000,"Fixed",0.23,"Per Item","Payment");
mkC("NAB","Out","International Payment - All","Cross Border",false,0,10000000,"Fixed",20.00,"Per Item","Payment");
mkC("Finexio","Out","Check","Domestic/Cross Border",false,0,10000000,"Fixed",13.34,"Per Item","Payment");
mkC("CurrencyCloud","Out","Conversion Volume Fee","Cross Border",false,0,1000000,"Variable",0.0015,"Per Item","Payment");
mkC("CurrencyCloud","Out","Conversion Volume Fee","Cross Border",false,1000001,5000000,"Variable",0.0012,"Per Item","Payment");
mkC("CurrencyCloud","Out","Conversion Volume Fee","Cross Border",false,5000001,10000000,"Variable",0.0010,"Per Item","Payment");
mkC("CurrencyCloud","Out","SWIFT - ACO flat rate","Cross Border",false,0,1000000,"Fixed",20.00,"Per Item","Payment");
mkC("CurrencyCloud","Out","SWIFT - SHA","Cross Border",false,0,1000000,"Fixed",9.00,"Per Item","Payment");
mkC("CurrencyCloud","Out","Local payment fee (EUR)","Cross Border",false,0,1000000,"Fixed",1.00,"Per Item","Payment");
mkC("CurrencyCloud","Out","Local payment fee (GBP)","Cross Border",false,0,1000000,"Fixed",1.00,"Per Item","Payment");
mkC("CurrencyCloud","Out","Local payment fee (USD)","Cross Border",false,0,1000000,"Fixed",0.40,"Per Item","Payment");
mkC("CurrencyCloud","Out","Local payment fee (AUD)","Cross Border",false,0,1000000,"Fixed",2.00,"Per Item","Payment");
mkC("CurrencyCloud","Out","Local payment fee (CAD)","Cross Border",false,0,1000000,"Fixed",1.00,"Per Item","Payment");
mkC("CurrencyCloud","Out","Local payment fee (CZK)","Cross Border",false,0,1000000,"Fixed",2.00,"Per Item","Payment");
mkC("CurrencyCloud","Out","Local payment fee (DKK)","Cross Border",false,0,1000000,"Fixed",1.00,"Per Item","Payment");
mkC("CurrencyCloud","Out","Local payment fee (HKD)","Cross Border",false,0,1000000,"Fixed",2.00,"Per Item","Payment");
mkC("CurrencyCloud","Out","Local payment fee (HRK)","Cross Border",false,0,1000000,"Fixed",3.00,"Per Item","Payment");
mkC("CurrencyCloud","Out","Local payment fee (HUF)","Cross Border",false,0,1000000,"Fixed",2.00,"Per Item","Payment");
mkC("CurrencyCloud","Out","Local payment fee (RON)","Cross Border",false,0,1000000,"Fixed",2.00,"Per Item","Payment");
mkC("CurrencyCloud","Out","Local payment fee (IDR)","Cross Border",false,0,1000000,"Fixed",2.00,"Per Item","Payment");
mkC("CurrencyCloud","Out","Local payment fee (INR)","Cross Border",false,0,1000000,"Fixed",1.00,"Per Item","Payment");
mkC("CurrencyCloud","Out","Local payment fee (MYR)","Cross Border",false,0,1000000,"Fixed",1.00,"Per Item","Payment");
mkC("CurrencyCloud","Out","Local payment fee (NOK)","Cross Border",false,0,1000000,"Fixed",1.00,"Per Item","Payment");
mkC("CurrencyCloud","Out","Local payment fee (PHP)","Cross Border",false,0,1000000,"Fixed",3.00,"Per Item","Payment");
mkC("CurrencyCloud","Out","Local payment fee (PLN)","Cross Border",false,0,1000000,"Fixed",1.00,"Per Item","Payment");
mkC("CurrencyCloud","Out","Local payment fee (SEK)","Cross Border",false,0,1000000,"Fixed",1.00,"Per Item","Payment");
mkC("CurrencyCloud","Out","Local payment fee (SGD)","Cross Border",false,0,1000000,"Fixed",2.00,"Per Item","Payment");
mkC("CurrencyCloud","In","SWIFT receipt fee","Cross Border",false,0,1000000,"Fixed",6.50,"Per Item","Payment");
mkC("CurrencyCloud","In","Local receipt fee (EUR)","Cross Border",false,0,1000000,"Fixed",0.50,"Per Item","Payment");
mkC("CurrencyCloud","In","Local receipt fee (GBP)","Cross Border",false,0,1000000,"Fixed",0.50,"Per Item","Payment");
mkC("CurrencyCloud","In","Local receipt fee (CAD)","Cross Border",false,0,1000000,"Fixed",0.50,"Per Item","Payment");
mkC("CurrencyCloud","In","Local receipt fee (USD - ACH)","Cross Border",false,0,1000000,"Fixed",0.50,"Per Item","Payment");
mkC("CurrencyCloud","In","Local receipt fee (USD - FedWire)","Cross Border",false,0,1000000,"Fixed",6.50,"Per Item","Payment");
mkC("TabaPay","In","Card Processing Fee","Domestic/Cross Border",false,0,1000000000,"Fixed",0.30,"Per Item","Payment");
mkC("TabaPay","In","Card Processing Fee","Domestic/Cross Border",false,0,1000000000,"Variable",0.0006,"Per Item","Payment");
mkC("TabaPay","Out","International Disbursement Markup","Cross Border",false,0,1000000000,"Variable",0.0040,"Per Item","Payment");
mkC("TabaPay","Out","OFAC Screening","Cross Border",false,0,1000000000,"Fixed",0.03,"Per Item","Other");
mkC("TabaPay","In","3DSecure","Domestic/Cross Border",false,0,1000000000,"Fixed",0.07,"Per Item","Other");
mkC("TabaPay","In","Tokenization","Domestic/Cross Border",false,0,1000000000,"Fixed",0.04,"Per Item","Other");
mkC("TabaPay","In","AVS Fee","Domestic/Cross Border",false,0,1000000000,"Fixed",0.05,"Per Item","Other");
mkC("TabaPay","In","Payment Authorization (No Funds Movement)","Domestic/Cross Border",false,0,1000000000,"Fixed",0.05,"Per Item","Other");
mkC("TabaPay","In","Chargeback Fee","Domestic/Cross Border",false,0,1000000000,"Fixed",15.00,"Per Item","Chargeback");
mkC("TabaPay","In","ACH Reject Fee","Domestic/Cross Border",false,0,1000000000,"Fixed",25.00,"Per Item","Other");
mkC("TabaPay","Out","International P2C Setup Fee","Cross Border",false,0,1000000000,"Fixed",2500.00,"One-Time","Other");
mkC("TabaPay","In","Stale Account Fee","Domestic/Cross Border",false,0,1000000000,"Fixed",0.01,"Monthly","Other");
mkC("TabaPay","In","Card Processing Fee","Domestic/Cross Border",false,0,5000000,"Fixed",0.05,"Per Item","Payment");
mkC("TabaPay","In","Card Processing Fee","Domestic/Cross Border",false,0,5000000,"Variable",0.0006,"Per Item","Payment");
mkC("TabaPay","In","Card Processing Fee","Domestic/Cross Border",false,5000001,20000000,"Fixed",0.045,"Per Item","Payment");
mkC("TabaPay","In","Card Processing Fee","Domestic/Cross Border",false,5000001,20000000,"Variable",0.0005,"Per Item","Payment");
mkC("TabaPay","In","Card Processing Fee","Domestic/Cross Border",false,20000001,1000000000,"Fixed",0.03,"Per Item","Payment");
mkC("TabaPay","In","Card Processing Fee","Domestic/Cross Border",false,20000001,1000000000,"Variable",0.0005,"Per Item","Payment");
mkC("TabaPay","In","Monthly Processing Minimum","Domestic/Cross Border",false,0,1000000000,"Fixed",10000.00,"Monthly Minimum","Other");
mkC("TabaPay","In","Chargeback Fee (Network Fees + $15)","Domestic/Cross Border",false,0,1000000000,"Fixed",15.00,"Per Item","Chargeback");
mkC("TabaPay","In","Card Network / Bank Partner Remediation - Research Fee","Domestic/Cross Border",false,0,1000000000,"Fixed",250.00,"Hourly","Other");
mkC("TabaPay","In","Authorization (No Funds Movement)","Domestic/Cross Border",false,0,1000000000,"Fixed",0.05,"Per Item","Other");
mkC("TabaPay","In","ACH Reject Fee","Domestic/Cross Border",false,0,1000000000,"Fixed",25.00,"Per Item","Other");
mkC("TabaPay","Out","OFAC Screening","Cross Border",false,0,1000000000,"Fixed",0.04,"Per Item","Other");
mkC("TabaPay","Out","FX Fee","Cross Border",false,0,1000000000,"Variable",0.00075,"Per Item","Payment");
mkC("TabaPay","Out","International Disbursement Markup","Cross Border",false,0,1000000000,"Variable",0.0040,"Per Item","Payment");
mkC("TabaPay","In","Real Time Monitoring","Domestic/Cross Border",false,0,1000000000,"Fixed",0.03,"Per Item","Other");
mkC("TabaPay","In","AVS / ANI","Domestic/Cross Border",false,0,1000000000,"Fixed",0.05,"Per Item","Other");
mkC("TabaPay","In","3-D Secure","Domestic/Cross Border",false,0,1000000000,"Fixed",0.07,"Per Item","Other");
mkC("TabaPay","In","Account Updater","Domestic/Cross Border",false,0,1000000000,"Fixed",0.05,"Per Item","Other");

const lookerTxnsRaw = importedLookerData.ltxn || [];

const reversalDataRaw = importedLookerData.lrev || [];

const virtualAccountUsageRaw = importedLookerData.lva || [];

const revShareSummaryRaw = importedLookerData.lrs || [];
const fxPartnerPayoutRaw = importedLookerData.lfxp || [];

const feeCapsRaw = [
  { partner: "Nuvion", productType: "RTP", capType: "Max Fee", amount: 20 },
  { partner: "Yeepay", productType: "GBP 0.7%", capType: "Max Fee", amount: 15 },
  { partner: "Yeepay", productType: "FasterACH", capType: "Max Fee", amount: 20 },
  { partner: "Yeepay", productType: "RTP", capType: "Max Fee", amount: 20 },
  { partner: "Repay", productType: "RTP", capType: "Max Fee", amount: 20 },
  { partner: "Blindpay", productType: "RTP", capType: "Max Fee", amount: 20 }
];

const revShareRaw = [
  { partner: "Stampli", txnType: "", speedFlag: "RTP", revSharePct: 0.2, startDate: STAMPLI_EFFECTIVE_DATE, endDate: "" },
  { partner: "Shepherd", txnType: "Payout", revSharePct: 0.30, startDate: "2024-07-01", endDate: "" },
  { partner: "Shepherd", txnType: "Payin", revSharePct: 0.30, startDate: "2024-07-01", endDate: "" },
  { partner: "Everflow", txnType: "Payout", revSharePct: 0.40, startDate: "2024-01-01", endDate: "" },
  { partner: "Everflow", txnType: "Payin", revSharePct: 0.40, startDate: "2024-01-01", endDate: "" },
  { partner: "Q2", txnType: "Payout", revSharePct: 0.40, startDate: "2024-01-01", endDate: "" },
  { partner: "Q2", txnType: "Payin", revSharePct: 0.40, startDate: "2024-01-01", endDate: "" },
  { partner: "Finastra", txnType: "Payout", revSharePct: 0.50, startDate: "2024-01-01", endDate: "" },
  { partner: "Finastra", txnType: "Payin", revSharePct: 0.50, startDate: "2024-01-01", endDate: "" },
  { partner: "Halorecruiting", txnType: "Payout", revSharePct: 0.20, startDate: "2024-01-01", endDate: "" },
  { partner: "Halorecruiting", txnType: "Payin", revSharePct: 0.20, startDate: "2024-01-01", endDate: "" },
  { partner: "Magaya", txnType: "Payout", revSharePct: 0.40, startDate: "2024-01-01", endDate: "" },
  { partner: "Magaya", txnType: "Payin", revSharePct: 0.40, startDate: "2024-01-01", endDate: "" },
  { partner: "Fulfil", txnType: "Payout", revSharePct: 0.20, startDate: "2024-01-01", endDate: "" },
  { partner: "Fulfil", txnType: "Payin", revSharePct: 0.20, startDate: "2024-01-01", endDate: "" }
];

const initOfflineRates = offRaw.map((r) => ({ id: uid(), partner: r[0], txnType: r[1], speedFlag: r[2], minAmt: r[3], maxAmt: r[4], payerFunding: "", payeeFunding: "", fee: r[5], payerCcy: r[6], payeeCcy: r[7], payerCountry: "", payeeCountry: "", processingMethod: r[8], startDate: r[9], endDate: r[10] }));
const initVolumeRates = volRaw.filter((r) => !isVolFx(r)).map((r) => ({ id: uid(), partner: r[0], txnType: r[1], speedFlag: r[2], rate: r[3], payerFunding: r[4], payeeFunding: r[5], payeeCardType: r[6], ccyGroup: r[7], minVol: r[8], maxVol: r[9], startDate: r[10], endDate: r[11], note: r[12] }));

const volFxToFxRate = (r) => {
  const g = r[7];
  let payeeCorridor = "";
  let payeeCcy = "";
  if (g === MAJORS) payeeCorridor = "Major";
  else if (g === MINORS) payeeCorridor = "Minor";
  else if (g === TERTIARY) payeeCorridor = "Tertiary";
  else if (g) {
    payeeCcy = g;
    payeeCorridor = getCorridor(g);
  }
  return { id: uid(), partner: r[0], payerCorridor: "", payerCcy: "", payeeCorridor, payeeCcy, minTxnSize: 0, maxTxnSize: 1e9, minVol: r[8], maxVol: r[9], rate: r[3], startDate: r[10], endDate: r[11], note: r[12] };
};
const initFxRates = volRaw.filter(isVolFx).map(volFxToFxRate);

const initFeeCaps = feeCapsRaw.map((row) => ({ id: uid(), startDate: "", endDate: "", ...row }));
const initRevShare = revShareRaw.map((row) => ({ id: uid(), ...row }));
const initMinimums = minRaw.map((r) => ({ id: uid(), partner: r[0], startDate: r[1], endDate: r[2], minAmount: r[3], minVol: r[4], maxVol: r[5], implFeeOffset: false }));
const initPlatformFees = platformFeesRaw.map((row) => ({ id: uid(), ...row }));
const initReversalFees = revFeeRaw.map((r) => ({ id: uid(), partner: r[0], payerFunding: r[1], feePerReversal: r[2], startDate: r[3], endDate: r[4] }));
const initImplFees = implFeesRaw.map((row) => ({
  id: uid(),
  startDate: DEFAULT_CONTRACT_START_DATES[row.partner] || "",
  endDate: "",
  creditMode: "",
  creditAmount: 0,
  creditWindowDays: 0,
  ...row
}));
const initVirtualAcctFees = virtualAccountFeesRaw.map((row) => ({ id: uid(), startDate: "", endDate: "", ...row }));
const initSurcharges = surchRaw.map((r) => ({ id: uid(), partner: r[0], surchargeType: r[1], rate: r[2], minVol: r[3], maxVol: r[4], startDate: r[5], endDate: r[6], note: r[7] }));
const initProviderCosts = providerCostRaw.map((row) => {
  const provider = row.provider;
  const txnName = row.txnName;
  let startDate = "";
  let endDate = "";
  let note = "";
  if (provider === "TabaPay") {
    const amendedCardProcessing = txnName === "Card Processing Fee" && (Number(row.minAmt) !== 0 || Number(row.maxAmt) !== 1000000000);
    const amendedRowNames = new Set(["Monthly Processing Minimum", "Chargeback Fee (Network Fees + $15)", "Card Network / Bank Partner Remediation - Research Fee", "Authorization (No Funds Movement)", "Real Time Monitoring", "AVS / ANI", "3-D Secure", "Account Updater", "FX Fee"]);
    const isAmended = amendedCardProcessing || amendedRowNames.has(txnName);
    startDate = isAmended ? TABAPAY_AMENDMENT_EFFECTIVE_DATE : TABAPAY_ORIGINAL_EFFECTIVE_DATE;
    endDate = isAmended ? "" : TABAPAY_ORIGINAL_END_DATE;
    if (txnName === "International Disbursement Markup") note = "Cost extra; min $1 / max $4 per transaction.";
    else if (txnName === "Chargeback Fee" || txnName === "Chargeback Fee (Network Fees + $15)") note = isAmended ? "Network fees extra." : "Contract fee excludes network costs.";
    else if (txnName === "Monthly Processing Minimum") note = "Processing-fee minimum excluding network / interchange cost.";
    else if (txnName === "Card Processing Fee") note = isAmended ? "Cost extra; inclusive of sponsor bank fees." : "Cost extra; original merchant agreement schedule.";
    else if (txnName === "AVS Fee" || txnName === "AVS / ANI") note = "Network cost extra.";
    else if (txnName === "Payment Authorization (No Funds Movement)" || txnName === "Authorization (No Funds Movement)") note = "Network cost extra.";
    else if (txnName === "Real Time Monitoring") note = "Optional; applies to all transactions if opted in.";
  }
  return { id: uid(), startDate, endDate, note, ...row };
});
const initLookerTxns = lookerTxnsRaw.map((row) => ({ id: uid(), ...row }));
const initReversalData = reversalDataRaw.map((row) => ({ id: uid(), ...row }));
const initVirtualAccountUsage = virtualAccountUsageRaw.map((row) => ({
  id: uid(),
  closedAccounts: Number(row.closedAccounts || 0),
  ...row
}));
const initRevShareSummary = revShareSummaryRaw.map((row) => ({ id: uid(), ...row }));
const initFxPartnerPayouts = fxPartnerPayoutRaw.map((row) => ({ id: uid(), ...row }));

const baseData = {
  ps: initPartners,
  pConfig: initPartnerConfig,
  pArchived: initArchivedPartners,
  pActive: initPartnerActivity,
  pBilling: enrichPartnerBillingRows(initPartnerBilling),
  pInvoices: initInvoiceTracking,
  off: initOfflineRates,
  vol: initVolumeRates,
  fxRates: initFxRates,
  cap: initFeeCaps,
  rs: initRevShare,
  mins: initMinimums,
  plat: initPlatformFees,
  revf: initReversalFees,
  impl: initImplFees,
  vaFees: initVirtualAcctFees,
  surch: initSurcharges,
  pCosts: initProviderCosts,
  ltxn: initLookerTxns,
  lrev: initReversalData,
  lva: initVirtualAccountUsage,
  lrs: initRevShareSummary,
  lfxp: initFxPartnerPayouts,
  accessLogs: initAccessLogs,
  adminSettings: initAdminSettings
};

function deepClone(value) {
  if (typeof structuredClone === "function") {
    return structuredClone(value);
  }
  return JSON.parse(JSON.stringify(value));
}

export function createInitialWorkbookData() {
  return deepClone(baseData);
}

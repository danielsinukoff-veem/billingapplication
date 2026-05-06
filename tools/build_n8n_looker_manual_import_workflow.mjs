import fs from "node:fs";
import path from "node:path";

const ROOT_DIR = "/Users/danielsinukoff/Documents/billing-workbook";
const RUNTIME_PATH = path.join(ROOT_DIR, "tools", "looker_sync_runtime.mjs");
const OUTPUT_PATH = path.join(ROOT_DIR, "docs", "n8n-looker-manual-import.workflow.json");

const DEFAULT_BUCKET = "veem-qa-billing-data";
const DEFAULT_WORKBOOK_KEY = "data/current-workbook.json";
const DEFAULT_WORKBOOK_HISTORY_PREFIX = "history/workbook";
const DEFAULT_SUMMARY_KEY = "data/looker-manual-import/latest-summary.json";

function stripExports(source) {
  return source.replace(/^export\s+/gm, "");
}

function slugify(value) {
  return String(value || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 64) || "node";
}

function makeNodeId(name) {
  return slugify(name);
}

function codeNode(name, position, jsCode) {
  return {
    parameters: {
      mode: "runOnceForAllItems",
      language: "javaScript",
      jsCode,
    },
    id: makeNodeId(name),
    name,
    type: "n8n-nodes-base.code",
    typeVersion: 2,
    position,
  };
}

function ifNode(name, position, leftValue) {
  return {
    parameters: {
      conditions: {
        options: {
          caseSensitive: true,
          leftValue: "",
          typeValidation: "strict",
          version: 2,
        },
        conditions: [
          {
            id: `${makeNodeId(name)}-condition`,
            leftValue,
            rightValue: true,
            operator: {
              type: "boolean",
              operation: "true",
              singleValue: true,
            },
          },
        ],
        combinator: "and",
      },
      options: {},
    },
    id: makeNodeId(name),
    name,
    type: "n8n-nodes-base.if",
    typeVersion: 2.2,
    position,
  };
}

function extractFromFileNode(name, position, operation) {
  return {
    parameters: {
      operation,
      binaryPropertyName: "uploadedSpreadsheet",
      options: {},
    },
    id: makeNodeId(name),
    name,
    type: "n8n-nodes-base.extractFromFile",
    typeVersion: 1,
    position,
  };
}

function s3UploadNode(name, position, binaryPropertyName, fileNameExpression, parentFolderExpression) {
  return {
    parameters: {
      resource: "file",
      operation: "upload",
      bucketName: "={{ $json.bucketName }}",
      fileName: fileNameExpression,
      binaryData: true,
      binaryPropertyName,
      additionalFields: {
        parentFolderKey: parentFolderExpression,
      },
    },
    id: makeNodeId(name),
    name,
    type: "n8n-nodes-base.awsS3",
    typeVersion: 1,
    position,
  };
}

function buildSampleRequestCode() {
  return [
    "return [{",
    "  json: {",
    "    fileType: \"revenue_share_report\",",
    "    period: \"2026-03\",",
    "    pastedText: \"Partner Name,Period,Revenue Share\\nSample Partner,2026-03,0\",",
    "    fileName: \"manual-test.csv\",",
    "  },",
    "}];",
  ].join("\n");
}

function buildContextCode() {
  return [
    "const input = $input.first()?.json || {};",
    "const body = input.body && typeof input.body === \"object\" ? input.body : input;",
    "function clean(value) { return String(value ?? \"\").trim(); }",
    "const now = new Date();",
    "const timestamp = now.toISOString().replace(/[-:.TZ]/g, \"\").slice(0, 14);",
    "const runToken = Math.random().toString(16).slice(2, 8);",
    "const fileType = clean(body.fileType);",
    "const period = clean(body.period);",
    "const pastedText = clean(body.pastedText || body.csvText || body.text);",
    "const fileBase64 = clean(body.fileBase64 || body.file || body.data);",
    "const fileName = clean(body.fileName) || \"manual-upload.csv\";",
    "const contentType = clean(body.contentType);",
    "function fileLooksLikeExcel(name, type) {",
    "  const normalizedName = clean(name).toLowerCase();",
    "  const normalizedType = clean(type).toLowerCase();",
    "  return /\\.(xlsx|xls|xlsm)$/.test(normalizedName) || normalizedType.includes(\"spreadsheet\") || normalizedType.includes(\"excel\");",
    "}",
    "function fileLooksLikeXls(name, type) {",
    "  const normalizedName = clean(name).toLowerCase();",
    "  const normalizedType = clean(type).toLowerCase();",
    "  return /\\.xls$/.test(normalizedName) || normalizedType.includes(\"ms-excel\");",
    "}",
    "if (!fileType) throw new Error(\"Manual Looker import requires fileType.\");",
    "if (!period) throw new Error(\"Manual Looker import requires period in YYYY-MM format.\");",
    "if (!/^\\d{4}-\\d{2}$/.test(period)) throw new Error(`Manual Looker import period must be YYYY-MM, got ${period}.`);",
    "if (!pastedText && !fileBase64) throw new Error(\"Manual Looker import requires pastedText or fileBase64.\");",
    "return [{ json: {",
    "  fileType,",
    "  period,",
    "  pastedText,",
    "  fileBase64,",
    "  fileName,",
    "  contentType,",
    "  isExcelUpload: Boolean(fileBase64 && fileLooksLikeExcel(fileName, contentType)),",
    "  isXlsUpload: Boolean(fileBase64 && fileLooksLikeXls(fileName, contentType)),",
    "  context: body.context && typeof body.context === \"object\" ? body.context : {},",
    "  includeDetailRows: body.includeDetailRows === true || clean(body.includeDetailRows).toLowerCase() === \"true\",",
    "  requestedBy: clean(body.requestedBy || body.user || body.source) || \"billing-workbook-ui\",",
    "  runId: clean(body.runId) || `manual-looker-${timestamp}-${runToken}-${period.replace(\"-\", \"\")}`,",
    `  bucketName: clean(body.bucketName) || ${JSON.stringify(DEFAULT_BUCKET)},`,
    `  workbookKey: clean(body.workbookKey) || ${JSON.stringify(DEFAULT_WORKBOOK_KEY)},`,
    `  workbookHistoryPrefix: clean(body.workbookHistoryPrefix) || ${JSON.stringify(DEFAULT_WORKBOOK_HISTORY_PREFIX)},`,
    `  summaryKey: clean(body.summaryKey) || ${JSON.stringify(DEFAULT_SUMMARY_KEY)},`,
    "} }];",
  ].join("\n");
}

function buildExcelBinaryCode() {
  return [
    "const ctx = $input.first()?.json || {};",
    "function clean(value) { return String(value ?? \"\").trim(); }",
    "function stripDataUrl(value) { return clean(value).replace(/^data:[^,]+,/, \"\"); }",
    "function mimeFromFile(name, contentType) {",
    "  const type = clean(contentType);",
    "  if (type) return type;",
    "  const lower = clean(name).toLowerCase();",
    "  if (lower.endsWith(\".xls\")) return \"application/vnd.ms-excel\";",
    "  return \"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet\";",
    "}",
    "if (!ctx.fileBase64) throw new Error(\"Excel manual import requires fileBase64.\");",
    "const buffer = Buffer.from(stripDataUrl(ctx.fileBase64), \"base64\");",
    "if (!buffer.length) throw new Error(\"Excel manual import file decoded to 0 bytes.\");",
    "const binary = await this.helpers.prepareBinaryData(buffer, ctx.fileName || \"manual-upload.xlsx\", mimeFromFile(ctx.fileName, ctx.contentType));",
    "return [{",
    "  json: {",
    "    ...ctx,",
    "    originalUploadByteCount: buffer.length,",
    "  },",
    "  binary: { uploadedSpreadsheet: binary },",
    "}];",
  ].join("\n");
}

function buildNormalizeExcelRowsCode() {
  return [
    "const source = $(\"Build Excel Binary\").all()[0]?.json || {};",
    "const rows = $input.all().map((item) => {",
    "  const json = item.json || {};",
    "  if (json.row && typeof json.row === \"object\" && !Array.isArray(json.row)) return json.row;",
    "  return json;",
    "}).filter((row) => row && Object.keys(row).length);",
    "function clean(value) { return String(value ?? \"\").trim(); }",
    "function cellText(value) {",
    "  if (value === null || value === undefined) return \"\";",
    "  if (value instanceof Date && !Number.isNaN(value.valueOf())) return value.toISOString();",
    "  if (typeof value === \"object\") return JSON.stringify(value);",
    "  return String(value);",
    "}",
    "function escapeCsv(value) {",
    "  const text = cellText(value);",
    "  if (/[\",\\n\\r]/.test(text)) return `\"${text.replace(/\"/g, '\"\"')}\"`;",
    "  return text;",
    "}",
    "const headers = [];",
    "for (const row of rows) {",
    "  for (const key of Object.keys(row)) {",
    "    const header = clean(key);",
    "    if (header && !headers.includes(header)) headers.push(header);",
    "  }",
    "}",
    "if (!rows.length || !headers.length) throw new Error(\"Excel manual import produced no table rows. Check that the first worksheet contains the Looker export table.\");",
    "const csvLines = [headers.map(escapeCsv).join(\",\")];",
    "for (const row of rows) csvLines.push(headers.map((header) => escapeCsv(row[header])).join(\",\"));",
    "return [{ json: {",
    "  ...source,",
    "  pastedText: csvLines.join(\"\\n\"),",
    "  csvText: csvLines.join(\"\\n\"),",
    "  fileBase64: \"\",",
    "  sourceFormat: source.isXlsUpload ? \"xls\" : \"xlsx\",",
    "  extractedRowCount: rows.length,",
    "} }];",
  ].join("\n");
}

function buildApplyImportCode(runtimeSource) {
  const runtimeLiteral = JSON.stringify(runtimeSource);
  return [
    "const input = $input.first();",
    "const ctx = input.json || {};",
    "if (!input.binary?.currentWorkbook) throw new Error(\"Download Current Workbook did not provide currentWorkbook binary data.\");",
    "const workbookBuffer = await this.helpers.getBinaryDataBuffer(0, \"currentWorkbook\");",
    "let workbookPayload;",
    "try {",
    "  workbookPayload = JSON.parse(workbookBuffer.toString(\"utf8\"));",
    "} catch (error) {",
    "  throw new Error(`Current workbook JSON could not be parsed: ${error.message}`);",
    "}",
    `const runtimeSource = ${runtimeLiteral};`,
    "const { applyLookerCsvToWorkbook } = new Function(`${runtimeSource}\\nreturn { applyLookerCsvToWorkbook };`)();",
    "function clean(value) { return String(value ?? \"\").trim(); }",
    "function stripDataUrl(value) { return clean(value).replace(/^data:[^,]+,/, \"\"); }",
    "function splitS3Key(key) {",
    "  const cleaned = String(key || \"\").replace(/^\\/+|\\/+$/g, \"\");",
    "  const parts = cleaned.split(\"/\");",
    "  const fileName = parts.pop() || \"artifact.json\";",
    "  return { parentFolderKey: parts.join(\"/\"), fileName };",
    "}",
    "function buildJsonBinary(fileName, jsonText) {",
    "  return { data: Buffer.from(jsonText, \"utf8\").toString(\"base64\"), fileName, mimeType: \"application/json\" };",
    "}",
    "function fileLooksLikeExcel(fileName, contentType) {",
    "  const name = clean(fileName).toLowerCase();",
    "  const type = clean(contentType).toLowerCase();",
    "  return /\\.(xlsx|xls|xlsm)$/.test(name) || type.includes(\"spreadsheet\") || type.includes(\"excel\");",
    "}",
    "let csvBuffer = null;",
    "let csvText = clean(ctx.csvText || ctx.pastedText);",
    "if (ctx.fileBase64) {",
    "  if (fileLooksLikeExcel(ctx.fileName, ctx.contentType)) {",
    "    throw new Error(\"Excel upload reached the importer before spreadsheet extraction. Re-import the latest workflow JSON so Excel files route through Extract From File first.\");",
    "  }",
    "  csvBuffer = Buffer.from(stripDataUrl(ctx.fileBase64), \"base64\");",
    "  csvText = csvBuffer.toString(\"utf8\");",
    "}",
    "if (!csvBuffer) csvBuffer = Buffer.from(csvText, \"utf8\");",
    "if (!csvText.trim()) throw new Error(\"Manual import file decoded to empty text.\");",
    "const savedAt = new Date().toISOString();",
    "const { workbook, result } = applyLookerCsvToWorkbook(workbookPayload, {",
    "  fileType: ctx.fileType,",
    "  period: ctx.period,",
    "  csvText,",
    "  csvBuffer,",
    "  context: ctx.context || {},",
    "}, {",
    "  includeDetailRows: ctx.includeDetailRows === true,",
    "  savedAt,",
    "  runId: ctx.runId,",
    "  source: \"n8n-manual-upload\",",
    "});",
    "result.sourceMetadata = {",
    "  source: \"manual-upload-webhook\",",
    "  fileName: ctx.fileName,",
    "  byteCount: Number(ctx.originalUploadByteCount || 0) || csvBuffer.length,",
    "  normalizedByteCount: csvBuffer.length,",
    "  sourceFormat: ctx.sourceFormat || \"csv-text\",",
    "  extractedRowCount: Number(ctx.extractedRowCount || 0) || undefined,",
    "  requestedBy: ctx.requestedBy,",
    "  savedAt,",
    "  runId: ctx.runId,",
    "};",
    "const auditSnapshot = workbook?.snapshot && typeof workbook.snapshot === \"object\" ? workbook.snapshot : workbook;",
    "const auditRecord = auditSnapshot?.lookerImportAudit?.byFileType?.[String(ctx.fileType || \"\")];",
    "if (auditRecord) auditRecord.sourceMetadata = { ...(auditRecord.sourceMetadata || {}), ...result.sourceMetadata };",
    "const workbookJson = JSON.stringify(workbook, null, 2);",
    "const responsePayload = { ...result, savedAt, runId: ctx.runId, source: \"n8n-manual-upload\" };",
    "const summaryPayload = {",
    "  savedAt,",
    "  runId: ctx.runId,",
    "  period: ctx.period,",
    "  fileType: ctx.fileType,",
    "  fileName: ctx.fileName,",
    "  warnings: result.warnings || [],",
    "  stats: result.stats || {},",
    "  changeSummary: result.changeSummary || {},",
    "  sourceMetadata: result.sourceMetadata || {},",
    "};",
    "const workbookKey = splitS3Key(ctx.workbookKey);",
    "const summaryKey = splitS3Key(ctx.summaryKey);",
    "const historyTimestamp = savedAt.replace(/[-:.TZ]/g, \"\").slice(0, 14);",
    "const historyParentFolderKey = String(ctx.workbookHistoryPrefix || \"\").replace(/^\\/+|\\/+$/g, \"\");",
    "const historyFileName = `current-workbook-${historyTimestamp}.json`;",
    "return [{",
    "  json: {",
    "    bucketName: ctx.bucketName,",
    "    workbookParentFolderKey: workbookKey.parentFolderKey,",
    "    workbookFileName: workbookKey.fileName,",
    "    workbookHistoryParentFolderKey: historyParentFolderKey,",
    "    workbookHistoryFileName: historyFileName,",
    "    summaryParentFolderKey: summaryKey.parentFolderKey,",
    "    summaryFileName: summaryKey.fileName,",
    "    savedAt,",
    "    runId: ctx.runId,",
    "    responsePayload,",
    "  },",
    "  binary: {",
    "    workbookFile: buildJsonBinary(workbookKey.fileName, workbookJson),",
    "    workbookHistoryFile: buildJsonBinary(historyFileName, workbookJson),",
    "    summaryFile: buildJsonBinary(summaryKey.fileName, JSON.stringify(summaryPayload, null, 2)),",
    "  },",
    "}];",
  ].join("\n");
}

function buildResponseCode() {
  return [
    "const result = $(\"Apply Manual Looker Import\").all()[0]?.json?.responsePayload;",
    "if (!result) throw new Error(\"Manual import response payload was not available after S3 uploads.\");",
    "return [{ json: result }];",
  ].join("\n");
}

function buildRestoreImportPayloadCode() {
  return [
    "const source = $(\"Apply Manual Looker Import\").all()[0];",
    "if (!source) throw new Error(\"Apply Manual Looker Import output was not available for the next upload.\");",
    "return [{ json: source.json || {}, binary: source.binary || {} }];",
  ].join("\n");
}

function buildWorkflow(runtimeSource) {
  const nodes = [];
  const connections = {};
  const addNode = (node) => nodes.push(node);
  const connect = (fromName, toName, outputIndex = 0) => {
    connections[fromName] ||= { main: [] };
    connections[fromName].main[outputIndex] ||= [];
    connections[fromName].main[outputIndex].push({ node: toName, type: "main", index: 0 });
  };

  addNode({
    parameters: {
      httpMethod: "POST",
      path: "billing-looker-manual-import",
      responseMode: "lastNode",
      options: {
        allowedOrigins: "https://billing.qa-us-west-2.veem.com",
      },
    },
    id: makeNodeId("Manual Looker Import Webhook"),
    name: "Manual Looker Import Webhook",
    type: "n8n-nodes-base.webhook",
    typeVersion: 2.1,
    position: [180, 260],
    webhookId: "billing-looker-manual-import",
  });

  addNode({
    parameters: {},
    id: makeNodeId("Manual Test Trigger"),
    name: "Manual Test Trigger",
    type: "n8n-nodes-base.manualTrigger",
    typeVersion: 1,
    position: [180, 440],
  });

  addNode(codeNode("Sample Manual Upload Request", [500, 440], buildSampleRequestCode()));
  addNode(codeNode("Build Manual Import Context", [500, 260], buildContextCode()));
  addNode(ifNode("Is Excel Upload", [820, 260], "={{ $json.isExcelUpload }}"));
  addNode(codeNode("Build Excel Binary", [1140, 120], buildExcelBinaryCode()));
  addNode(ifNode("Is XLS Upload", [1460, 120], "={{ $json.isXlsUpload }}"));
  addNode(extractFromFileNode("Extract XLS Rows", [1780, 0], "xls"));
  addNode(extractFromFileNode("Extract XLSX Rows", [1780, 220], "xlsx"));
  addNode(codeNode("Normalize Excel Rows", [2100, 120], buildNormalizeExcelRowsCode()));

  addNode({
    parameters: {
      resource: "file",
      operation: "download",
      bucketName: "={{ $json.bucketName }}",
      fileKey: "={{ $json.workbookKey }}",
      binaryPropertyName: "currentWorkbook",
    },
    id: makeNodeId("Download Current Workbook"),
    name: "Download Current Workbook",
    type: "n8n-nodes-base.awsS3",
    typeVersion: 1,
    position: [2440, 260],
  });

  addNode(codeNode("Apply Manual Looker Import", [2780, 260], buildApplyImportCode(runtimeSource)));
  addNode(s3UploadNode("Upload Current Workbook", [3120, 120], "workbookFile", "={{ $json.workbookFileName }}", "={{ $json.workbookParentFolderKey }}"));
  addNode(codeNode("Restore Import Payload For History", [3440, 120], buildRestoreImportPayloadCode()));
  addNode(s3UploadNode("Upload Workbook History", [3760, 120], "workbookHistoryFile", "={{ $json.workbookHistoryFileName }}", "={{ $json.workbookHistoryParentFolderKey }}"));
  addNode(codeNode("Restore Import Payload For Summary", [4080, 120], buildRestoreImportPayloadCode()));
  addNode(s3UploadNode("Upload Manual Import Summary", [4400, 120], "summaryFile", "={{ $json.summaryFileName }}", "={{ $json.summaryParentFolderKey }}"));
  addNode(codeNode("Build Webhook Response", [4720, 120], buildResponseCode()));

  addNode({
    parameters: {
      content: "Manual Looker uploads from the hosted billing app land here. The workflow accepts pasted text, CSV, XLS, and XLSX uploads. Excel files are normalized through Extract From File, then the same JS runtime as the scheduled Looker cloud sync applies the import, writes the updated workbook plus timestamped history to S3, and returns only the import result to the browser.",
      height: 260,
      width: 520,
      color: 5,
    },
    id: makeNodeId("Setup Note"),
    name: "Setup Note",
    type: "n8n-nodes-base.stickyNote",
    typeVersion: 1,
    position: [500, -40],
  });

  connect("Manual Looker Import Webhook", "Build Manual Import Context");
  connect("Manual Test Trigger", "Sample Manual Upload Request");
  connect("Sample Manual Upload Request", "Build Manual Import Context");
  connect("Build Manual Import Context", "Is Excel Upload");
  connect("Is Excel Upload", "Build Excel Binary", 0);
  connect("Is Excel Upload", "Download Current Workbook", 1);
  connect("Build Excel Binary", "Is XLS Upload");
  connect("Is XLS Upload", "Extract XLS Rows", 0);
  connect("Is XLS Upload", "Extract XLSX Rows", 1);
  connect("Extract XLS Rows", "Normalize Excel Rows");
  connect("Extract XLSX Rows", "Normalize Excel Rows");
  connect("Normalize Excel Rows", "Download Current Workbook");
  connect("Download Current Workbook", "Apply Manual Looker Import");
  connect("Apply Manual Looker Import", "Upload Current Workbook");
  connect("Upload Current Workbook", "Restore Import Payload For History");
  connect("Restore Import Payload For History", "Upload Workbook History");
  connect("Upload Workbook History", "Restore Import Payload For Summary");
  connect("Restore Import Payload For Summary", "Upload Manual Import Summary");
  connect("Upload Manual Import Summary", "Build Webhook Response");

  return {
    name: "Billing Workbook Looker Manual Import - Updated 2026-04-30",
    active: false,
    isArchived: false,
    nodes,
    connections,
    settings: {
      executionOrder: "v1",
      timezone: "America/Toronto",
      callerPolicy: "workflowsFromSameOwner",
      availableInMCP: false,
      saveExecutionProgress: false,
      saveDataSuccessExecution: "none",
      saveDataErrorExecution: "all",
    },
    pinData: {},
    tags: [],
  };
}

const runtimeSource = stripExports(fs.readFileSync(RUNTIME_PATH, "utf8"));
const workflow = buildWorkflow(runtimeSource);
fs.mkdirSync(path.dirname(OUTPUT_PATH), { recursive: true });
fs.writeFileSync(OUTPUT_PATH, JSON.stringify(workflow, null, 2) + "\n");
console.log(`wrote ${OUTPUT_PATH}`);

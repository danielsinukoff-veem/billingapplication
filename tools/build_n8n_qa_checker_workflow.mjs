import fs from "node:fs";
import path from "node:path";

const ROOT_DIR = "/Users/danielsinukoff/Documents/billing-workbook";
const RUNTIME_PATH = path.join(ROOT_DIR, "tools", "qa_checker_runtime.mjs");
const OUTPUT_PATH = path.join(ROOT_DIR, "docs", "n8n-qa-checker.workflow.json");

const DEFAULT_BUCKET = "veem-qa-billing-data";
const DEFAULT_WORKBOOK_KEY = "data/current-workbook.json";
const DEFAULT_QA_SUMMARY_KEY = "data/qa/latest-checker-summary.json";
const DEFAULT_QA_EXCEPTIONS_KEY = "data/qa/latest-checker-exceptions.csv";

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

function splitS3Key(key) {
  const normalized = String(key || "").replace(/^\/+|\/+$/g, "");
  const parts = normalized.split("/");
  const fileName = parts.pop() || "artifact.json";
  return {
    parentFolderKey: parts.join("/"),
    fileName,
  };
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

function buildRunContextCode() {
  return [
    "const input = $input.first()?.json || {};",
    "const body = input.body && typeof input.body === \"object\" ? input.body : input;",
    "const now = new Date();",
    "const previous = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth() - 1, 1));",
    "const defaultPeriod = `${previous.getUTCFullYear()}-${String(previous.getUTCMonth() + 1).padStart(2, \"0\")}`;",
    "const timestamp = now.toISOString().replace(/[-:.TZ]/g, \"\").slice(0, 14);",
    "const runToken = Math.random().toString(16).slice(2, 8);",
    "return [{ json: {",
    "  period: String(body.period || defaultPeriod),",
    "  runId: String(body.runId || `qa-${timestamp}-${runToken}`),",
    `  bucketName: String(body.bucketName || ${JSON.stringify(DEFAULT_BUCKET)}),`,
    `  workbookKey: String(body.workbookKey || ${JSON.stringify(DEFAULT_WORKBOOK_KEY)}),`,
    `  qaSummaryKey: String(body.qaSummaryKey || ${JSON.stringify(DEFAULT_QA_SUMMARY_KEY)}),`,
    `  qaExceptionsKey: String(body.qaExceptionsKey || ${JSON.stringify(DEFAULT_QA_EXCEPTIONS_KEY)}),`,
    "} }];",
  ].join("\n");
}

function buildRunCheckerCode(runtimeSource) {
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
    "const { runWorkbookQaCheck, qaReportToCsv, applyWorkbookQaFixes } = new Function(`${runtimeSource}\\nreturn { runWorkbookQaCheck, qaReportToCsv, applyWorkbookQaFixes };`)();",
    "const generatedAt = new Date().toISOString();",
    "const maintainedWorkbookPayload = applyWorkbookQaFixes(workbookPayload, { generatedAt });",
    "const report = runWorkbookQaCheck(maintainedWorkbookPayload, {",
    "  period: ctx.period,",
    "  generatedAt,",
    "  source: \"n8n-qa-checker\",",
    "});",
    "report.runId = ctx.runId;",
    "const snapshotForHistory = maintainedWorkbookPayload?.snapshot && typeof maintainedWorkbookPayload.snapshot === \"object\"",
    "  ? maintainedWorkbookPayload.snapshot",
    "  : maintainedWorkbookPayload && typeof maintainedWorkbookPayload === \"object\"",
    "    ? maintainedWorkbookPayload",
    "    : {};",
    "const priorHistory = Array.isArray(snapshotForHistory.qaCheckerHistory) ? snapshotForHistory.qaCheckerHistory : [];",
    "const historyEntry = {",
    "  generatedAt,",
    "  period: report.period,",
    "  runId: report.runId,",
    "  status: report.status,",
    "  issueCount: report.summary.issueCount,",
    "  criticalCount: report.summary.criticalCount,",
    "  warningCount: report.summary.warningCount,",
    "  infoCount: report.summary.infoCount,",
    "  partnersChecked: report.summary.partnersChecked,",
    "  partnersWithIssues: report.summary.partnersWithIssues,",
    "  source: report.source,",
    "  workbookSavedAt: report.workbookSavedAt,",
    "};",
    "const qaCheckerHistory = [",
    "  historyEntry,",
    "  ...priorHistory.filter((entry) => entry && entry.runId !== historyEntry.runId),",
    "].slice(0, 25);",
    "report.history = qaCheckerHistory;",
    "const csv = qaReportToCsv(report);",
    "function splitS3Key(key) {",
    "  const cleaned = String(key || \"\").replace(/^\\/+|\\/+$/g, \"\");",
    "  const parts = cleaned.split(\"/\");",
    "  const fileName = parts.pop() || \"artifact.json\";",
    "  return { parentFolderKey: parts.join(\"/\"), fileName };",
    "}",
    "function buildJsonBinary(fileName, jsonText) {",
    "  return { data: Buffer.from(jsonText, \"utf8\").toString(\"base64\"), fileName, mimeType: \"application/json\" };",
    "}",
    "function buildCsvBinary(fileName, csvText) {",
    "  return { data: Buffer.from(csvText, \"utf8\").toString(\"base64\"), fileName, mimeType: \"text/csv\" };",
    "}",
    "const summaryKey = splitS3Key(ctx.qaSummaryKey);",
    "const exceptionsKey = splitS3Key(ctx.qaExceptionsKey);",
    "const workbookKey = splitS3Key(ctx.workbookKey);",
    "const updatedSnapshot = { ...snapshotForHistory, _saved: generatedAt, qaCheckerLatest: report, qaCheckerHistory };",
    "const updatedWorkbookPayload = maintainedWorkbookPayload && typeof maintainedWorkbookPayload === \"object\" && maintainedWorkbookPayload.snapshot && typeof maintainedWorkbookPayload.snapshot === \"object\"",
    "  ? { ...maintainedWorkbookPayload, savedAt: generatedAt, snapshot: updatedSnapshot }",
    "  : { workspace: { label: \"Veem Billing Workspace\" }, user: {}, snapshot: updatedSnapshot, savedAt: generatedAt };",
    "return [{",
    "  json: {",
    "    bucketName: ctx.bucketName,",
    "    runId: ctx.runId,",
    "    period: report.period,",
    "    generatedAt,",
    "    status: report.status,",
    "    criticalCount: report.summary.criticalCount,",
    "    warningCount: report.summary.warningCount,",
    "    infoCount: report.summary.infoCount,",
    "    issueCount: report.summary.issueCount,",
    "    qaSummaryParentFolderKey: summaryKey.parentFolderKey,",
    "    qaSummaryFileName: summaryKey.fileName,",
    "    qaExceptionsParentFolderKey: exceptionsKey.parentFolderKey,",
    "    qaExceptionsFileName: exceptionsKey.fileName,",
    "    workbookParentFolderKey: workbookKey.parentFolderKey,",
    "    workbookFileName: workbookKey.fileName,",
    "    workbookKey: ctx.workbookKey,",
    "  },",
    "  binary: {",
    "    workbookWithQaFile: buildJsonBinary(workbookKey.fileName, JSON.stringify(updatedWorkbookPayload, null, 2)),",
    "    qaSummaryFile: buildJsonBinary(summaryKey.fileName, JSON.stringify(report, null, 2)),",
    "    qaExceptionsFile: buildCsvBinary(exceptionsKey.fileName, csv),",
    "  },",
    "}];",
  ].join("\n");
}

function buildRestoreCheckerPayloadCode(label = "next QA upload") {
  return [
    "const source = $(\"Run Deterministic QA Checker\").all()[0];",
    `if (!source) throw new Error(${JSON.stringify(`Run Deterministic QA Checker output was not available for the ${label}.`)});`,
    "return [{ json: source.json || {}, binary: source.binary || {} }];",
  ].join("\n");
}

function buildVerifyWorkbookQaStatusCode(label = "workbook") {
  return [
    "const expected = $(\"Run Deterministic QA Checker\").all()[0];",
    `if (!expected) throw new Error(${JSON.stringify(`Run Deterministic QA Checker output was not available for ${label} QA verification.`)});`,
    "if (!$input.first()?.binary?.verifiedWorkbook) throw new Error(\"Downloaded workbook verification file was missing.\");",
    "const buffer = await this.helpers.getBinaryDataBuffer(0, \"verifiedWorkbook\");",
    "let payload;",
    "try {",
    "  payload = JSON.parse(buffer.toString(\"utf8\"));",
    "} catch (error) {",
    "  throw new Error(`Uploaded workbook could not be parsed during QA verification: ${error.message}`);",
    "}",
    "const report = payload?.snapshot?.qaCheckerLatest || payload?.qaCheckerLatest;",
    "if (!report || typeof report !== \"object\") {",
    `  throw new Error(${JSON.stringify(`QA verification failed: ${label} does not contain snapshot.qaCheckerLatest after upload.`)});`,
    "}",
    "if (String(report.runId || \"\") !== String(expected.json?.runId || \"\")) {",
    "  throw new Error(`QA verification failed: workbook has runId ${report.runId || \"missing\"}, expected ${expected.json?.runId || \"missing\"}.`);",
    "}",
    "return [{ json: expected.json || {}, binary: expected.binary || {} }];",
  ].join("\n");
}

function s3UploadNode(name, position, binaryPropertyName, fileNameExpression, parentFolderExpression, bucketExpression = "={{ $json.bucketName }}") {
  return {
    parameters: {
      resource: "file",
      operation: "upload",
      bucketName: bucketExpression,
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

function s3DownloadNode(name, position, fileKeyExpression, binaryPropertyName, bucketExpression = "={{ $json.bucketName }}") {
  return {
    parameters: {
      resource: "file",
      operation: "download",
      bucketName: bucketExpression,
      fileKey: fileKeyExpression,
      binaryPropertyName,
    },
    id: makeNodeId(name),
    name,
    type: "n8n-nodes-base.awsS3",
    typeVersion: 1,
    position,
  };
}

function buildWorkflow(runtimeSource) {
  const nodes = [];
  const connections = {};
  const addNode = (node) => nodes.push(node);
  const connect = (fromName, toName) => {
    connections[fromName] ||= { main: [[]] };
    connections[fromName].main[0].push({ node: toName, type: "main", index: 0 });
  };

  addNode({
    parameters: {
      httpMethod: "POST",
      path: "billing-qa-checker",
      responseMode: "onReceived",
      options: {
        allowedOrigins: "https://billing.qa-us-west-2.veem.com",
        responseCode: 202,
      },
    },
    id: makeNodeId("QA Checker Webhook"),
    name: "QA Checker Webhook",
    type: "n8n-nodes-base.webhook",
    typeVersion: 2.1,
    position: [220, 220],
    webhookId: "billing-qa-checker",
  });

  addNode({
    parameters: {},
    id: makeNodeId("Manual Trigger"),
    name: "Manual Trigger",
    type: "n8n-nodes-base.manualTrigger",
    typeVersion: 1,
    position: [220, 380],
  });

  addNode({
    parameters: {},
    id: makeNodeId("When Called By Another Workflow"),
    name: "When Called By Another Workflow",
    type: "n8n-nodes-base.executeWorkflowTrigger",
    typeVersion: 1,
    position: [220, 540],
  });

  addNode({
    parameters: {
      rule: {
        interval: [
          {
            field: "cronExpression",
            expression: "=30 7 * * *",
          },
        ],
      },
    },
    id: makeNodeId("Daily Schedule"),
    name: "Daily Schedule",
    type: "n8n-nodes-base.scheduleTrigger",
    typeVersion: 1.2,
    position: [220, 700],
  });

  addNode(codeNode("Build QA Run Context", [560, 420], buildRunContextCode()));

  addNode(s3DownloadNode("Download Current Workbook", [900, 420], "={{ $json.workbookKey }}", "currentWorkbook"));

  addNode(codeNode("Run Deterministic QA Checker", [1240, 420], buildRunCheckerCode(runtimeSource)));

  addNode(s3UploadNode(
    "Upload Workbook QA Status",
    [1580, 100],
    "workbookWithQaFile",
    "={{ $json.workbookFileName }}",
    "={{ $json.workbookParentFolderKey }}"
  ));
  addNode(codeNode("Restore QA Payload For Verification", [1880, 100], buildRestoreCheckerPayloadCode("workbook QA verification")));
  addNode(s3DownloadNode("Download Verified Workbook QA Status", [2180, 100], "={{ $json.workbookKey }}", "verifiedWorkbook"));
  addNode(codeNode("Verify Workbook QA Status", [2480, 100], buildVerifyWorkbookQaStatusCode("data/current-workbook.json")));
  addNode(codeNode("Restore QA Payload For Summary", [2780, 100], buildRestoreCheckerPayloadCode()));
  addNode(s3UploadNode(
    "Upload Latest QA Summary",
    [3080, 100],
    "qaSummaryFile",
    "={{ $json.qaSummaryFileName }}",
    "={{ $json.qaSummaryParentFolderKey }}"
  ));
  addNode(codeNode("Restore QA Payload For Exceptions", [3380, 100], buildRestoreCheckerPayloadCode()));
  addNode(s3UploadNode(
    "Upload Latest QA Exceptions CSV",
    [3680, 100],
    "qaExceptionsFile",
    "={{ $json.qaExceptionsFileName }}",
    "={{ $json.qaExceptionsParentFolderKey }}"
  ));

  addNode({
    parameters: {
      content: "First checker layer: deterministic QA only. It reads veem-qa-billing-data/data/current-workbook.json, flags source-data and missing-fee exceptions, writes qaCheckerLatest back to the data-bucket object, and writes the latest summary/CSV under data/qa. The hosted UI reads the latest summary through the separate Billing QA Status API workflow so this checker does not need site-bucket permissions.",
      height: 260,
      width: 520,
      color: 5,
    },
    id: makeNodeId("Setup Note"),
    name: "Setup Note",
    type: "n8n-nodes-base.stickyNote",
    typeVersion: 1,
    position: [540, 40],
  });

  connect("QA Checker Webhook", "Build QA Run Context");
  connect("Manual Trigger", "Build QA Run Context");
  connect("When Called By Another Workflow", "Build QA Run Context");
  connect("Daily Schedule", "Build QA Run Context");
  connect("Build QA Run Context", "Download Current Workbook");
  connect("Download Current Workbook", "Run Deterministic QA Checker");
  connect("Run Deterministic QA Checker", "Upload Workbook QA Status");
  connect("Upload Workbook QA Status", "Restore QA Payload For Verification");
  connect("Restore QA Payload For Verification", "Download Verified Workbook QA Status");
  connect("Download Verified Workbook QA Status", "Verify Workbook QA Status");
  connect("Verify Workbook QA Status", "Restore QA Payload For Summary");
  connect("Restore QA Payload For Summary", "Upload Latest QA Summary");
  connect("Upload Latest QA Summary", "Restore QA Payload For Exceptions");
  connect("Restore QA Payload For Exceptions", "Upload Latest QA Exceptions CSV");

  return {
    name: "Billing Workbook QA Checker - Updated 2026-04-30",
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

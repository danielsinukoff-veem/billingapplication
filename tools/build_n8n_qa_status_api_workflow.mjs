import fs from "node:fs";
import path from "node:path";

const ROOT_DIR = "/Users/danielsinukoff/Documents/billing-workbook";
const OUTPUT_PATH = path.join(ROOT_DIR, "docs", "n8n-qa-status-api.workflow.json");

const DEFAULT_BUCKET = "veem-qa-billing-data";
const DEFAULT_QA_SUMMARY_KEY = "data/qa/latest-checker-summary.json";
const FRONTEND_ORIGIN = "https://billing.qa-us-west-2.veem.com";

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

function s3DownloadNode(name, position, fileKeyExpression, binaryPropertyName, bucketExpression = "={{ $json.bucketName }}") {
  return {
    parameters: {
      resource: "file",
      operation: "download",
      bucketName: bucketExpression,
      fileKey: fileKeyExpression,
      binaryPropertyName,
    },
    id: slugify(name),
    name,
    type: "n8n-nodes-base.awsS3",
    typeVersion: 1,
    position,
  };
}

const buildContextCode = [
  "const input = $input.first()?.json || {};",
  "const query = input.query && typeof input.query === \"object\" ? input.query : {};",
  "const body = input.body && typeof input.body === \"object\" ? input.body : {};",
  "const source = { ...query, ...body };",
  "return [{ json: {",
  `  bucketName: String(source.bucketName || ${JSON.stringify(DEFAULT_BUCKET)}),`,
  `  qaSummaryKey: String(source.qaSummaryKey || source.summaryKey || ${JSON.stringify(DEFAULT_QA_SUMMARY_KEY)}),`,
  "  requestedAt: new Date().toISOString(),",
  "} }];",
].join("\n");

const buildResponseCode = [
  "const input = $input.first();",
  "if (!input?.binary?.qaSummaryFile) {",
  "  throw new Error(\"Latest QA summary file was not downloaded. Run the QA checker first.\");",
  "}",
  "const buffer = await this.helpers.getBinaryDataBuffer(0, \"qaSummaryFile\");",
  "let report;",
  "try {",
  "  report = JSON.parse(buffer.toString(\"utf8\"));",
  "} catch (error) {",
  "  throw new Error(`Latest QA summary JSON could not be parsed: ${error.message}`);",
  "}",
  "return [{",
  "  json: {",
  "    ok: true,",
  "    source: \"s3:data/qa/latest-checker-summary.json\",",
  "    fetchedAt: new Date().toISOString(),",
  "    qaCheckerLatest: report,",
  "    report,",
  "  }",
  "}];",
].join("\n");

function buildWorkflow() {
  const nodes = [];
  const connections = {};
  const addNode = (node) => nodes.push(node);
  const connect = (fromName, toName) => {
    connections[fromName] ||= { main: [[]] };
    connections[fromName].main[0].push({ node: toName, type: "main", index: 0 });
  };

  addNode({
    parameters: {
      httpMethod: "GET",
      path: "billing-qa-status",
      responseMode: "lastNode",
      options: {
        allowedOrigins: FRONTEND_ORIGIN,
      },
    },
    id: slugify("QA Status Webhook"),
    name: "QA Status Webhook",
    type: "n8n-nodes-base.webhook",
    typeVersion: 2.1,
    position: [220, 240],
    webhookId: "billing-qa-status",
  });

  addNode({
    parameters: {},
    id: slugify("Manual Test Trigger"),
    name: "Manual Test Trigger",
    type: "n8n-nodes-base.manualTrigger",
    typeVersion: 1,
    position: [220, 420],
  });

  addNode(codeNode("Build QA Status Context", [560, 320], buildContextCode));
  addNode(s3DownloadNode("Download Latest QA Summary", [900, 320], "={{ $json.qaSummaryKey }}", "qaSummaryFile"));
  addNode(codeNode("Return QA Status Response", [1240, 320], buildResponseCode));

  addNode({
    parameters: {
      content: "Read-only status API for the hosted app. It downloads veem-qa-billing-data/data/qa/latest-checker-summary.json using n8n's existing AWS credential and returns it as JSON. This avoids needing site-bucket write access while Engineering is unavailable.",
      height: 200,
      width: 500,
      color: 5,
    },
    id: slugify("Setup Note"),
    name: "Setup Note",
    type: "n8n-nodes-base.stickyNote",
    typeVersion: 1,
    position: [520, 40],
  });

  connect("QA Status Webhook", "Build QA Status Context");
  connect("Manual Test Trigger", "Build QA Status Context");
  connect("Build QA Status Context", "Download Latest QA Summary");
  connect("Download Latest QA Summary", "Return QA Status Response");

  return {
    name: "Billing QA Status API - Updated 2026-04-30",
    active: false,
    isArchived: false,
    nodes,
    connections,
    settings: {
      executionOrder: "v1",
      timezone: "America/Toronto",
      availableInMCP: false,
      saveExecutionProgress: false,
      saveDataSuccessExecution: "none",
      saveDataErrorExecution: "all",
    },
    pinData: {},
    tags: [],
  };
}

const workflow = buildWorkflow();
fs.mkdirSync(path.dirname(OUTPUT_PATH), { recursive: true });
fs.writeFileSync(OUTPUT_PATH, JSON.stringify(workflow, null, 2) + "\n");
console.log(`wrote ${OUTPUT_PATH}`);

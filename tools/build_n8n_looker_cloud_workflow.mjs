import fs from "node:fs";
import path from "node:path";

const ROOT_DIR = "/Users/danielsinukoff/Documents/billing-workbook";
const RUNTIME_PATH = path.join(ROOT_DIR, "tools", "looker_sync_runtime.mjs");
const CONFIG_PATH = path.join(ROOT_DIR, "docs", "looker-direct-reports.json");
const OUTPUT_PATH = path.join(ROOT_DIR, "docs", "n8n-looker-cloud.workflow.json");

const DEFAULT_BUCKET = "veem-prod-virginia-poc-billing-fe-delete-me";
const DEFAULT_WORKBOOK_KEY = "partner-billing-form/data/current-workbook.json";
const DEFAULT_SUMMARY_KEY = "partner-billing-form/data/looker-sync/latest-summary.json";
const DEFAULT_WORKBOOK_HISTORY_PREFIX = "partner-billing-form/data/history/workbook";

function stripExports(source) {
  return source.replace(/^export\s+/gm, "");
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

function slugify(value) {
  return String(value || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 48) || "node";
}

function makeNodeId(name) {
  return slugify(name);
}

function buildContextCode(config, runtimeSource) {
  const configuredReportCount = Array.isArray(config.reports) ? config.reports.length : 0;
  return [
    "const now = new Date();",
    "const period = `${now.getUTCFullYear()}-${String(now.getUTCMonth() + 1).padStart(2, \"0\")}`;",
    "const timestamp = now.toISOString().replace(/[-:.TZ]/g, \"\").slice(0, 14);",
    "const runToken = Math.random().toString(16).slice(2, 8);",
    "return [{ json: {",
    "  period,",
    "  runId: `n8n-${timestamp}-${runToken}-${period.replace(\"-\", \"\")}`,",
    `  bucketName: ${JSON.stringify(DEFAULT_BUCKET)},`,
    `  workbookKey: ${JSON.stringify(DEFAULT_WORKBOOK_KEY)},`,
    `  summaryKey: ${JSON.stringify(DEFAULT_SUMMARY_KEY)},`,
    `  workbookHistoryPrefix: ${JSON.stringify(DEFAULT_WORKBOOK_HISTORY_PREFIX)},`,
    `  configuredReportCount: ${JSON.stringify(configuredReportCount)},`,
    `  lookerBaseUrl: ${JSON.stringify(config.baseUrl || "")},`,
    `  lookerApiVersion: ${JSON.stringify(config.apiVersion || "4.0")},`,
    `  forceProduction: ${JSON.stringify(Boolean(config.forceProduction))},`,
    "  lookerClientId: \"SET_LOOKER_CLIENT_ID\",",
    "  lookerClientSecret: \"SET_LOOKER_CLIENT_SECRET\",",
    "} }];",
  ].join("\n");
}

function buildInitializeWorkbookCode() {
  return [
    "const input = $input.first();",
    "const ctx = input.json || {};",
    "let workbookPayload = { workspace: { label: \"Veem Billing Workspace\" }, user: {}, snapshot: {} };",
    "if (input.binary?.currentWorkbook) {",
    "  try {",
    "    const workbookBuffer = await this.helpers.getBinaryDataBuffer(0, \"currentWorkbook\");",
    "    workbookPayload = JSON.parse(workbookBuffer.toString(\"utf8\"));",
    "  } catch (error) {",
    "    throw new Error(`Current workbook JSON could not be parsed: ${error.message}`);",
    "  }",
    "}",
    "return [{ json: { ...ctx, workbookPayload, reportResults: [] } }];",
  ].join("\n");
}

function buildExportQueryCode(spec) {
  const specJson = JSON.stringify({
    fileType: spec.fileType,
    fileName: spec.fileName,
    lookId: spec.lookId,
    reportName: spec.reportName,
    reportTimeout: spec.reportTimeout || 600,
    historyWindowDays: spec.historyWindowDays || 0,
    historyFilterKey: spec.historyFilterKey || "",
    periodFilterKey: spec.periodFilterKey || "",
    periodFilterMode: spec.periodFilterMode || "",
  }, null, 2);

  return [
    "const ctx = $(\"Build Run Context\").all()[0].json || {};",
    "const baseQuery = $input.first().json || {};",
    `const spec = ${specJson};`,
    "const sourceMetadata = {",
    "  lookId: String(spec.lookId),",
    "  reportName: spec.reportName,",
    "  fileName: spec.fileName,",
    "  resultFormat: \"csv\",",
    "  baseQueryId: String(baseQuery.id || \"\"),",
    "  baseQueryLimit: baseQuery.limit ?? null,",
    "  baseQueryColumnLimit: baseQuery.column_limit ?? null,",
    "  exportQueryLimit: \"-1\",",
    "};",
    "const mergedFilters = { ...(baseQuery.filters || {}) };",
    "function periodMonthValue(period) {",
    "  return String(period || \"\").replace(\"-\", \"/\");",
    "}",
    "function periodDateRangeValue(period) {",
    "  const year = Number(String(period || \"\").slice(0, 4));",
    "  const month = Number(String(period || \"\").slice(5, 7));",
    "  const lastDay = new Date(Date.UTC(year, month, 0)).getUTCDate();",
    "  return `${period}-01 to ${period}-${String(lastDay).padStart(2, \"0\")}`;",
    "}",
    "function buildPeriodFilterValue(period, mode) {",
    "  const normalized = String(mode || \"month\").trim().toLowerCase();",
    "  if ([\"month\", \"month_value\"].includes(normalized)) return periodMonthValue(period);",
    "  if ([\"date_range\", \"month_range\"].includes(normalized)) return periodDateRangeValue(period);",
    "  if (normalized === \"raw\") return String(period || \"\");",
    "  throw new Error(`Unsupported Looker period filter mode: ${mode}`);",
    "}",
    "function buildHistoryFilterValue(historyWindowDays) {",
    "  return `${Math.max(1, Number(historyWindowDays || 0))} day`;",
    "}",
    "if (spec.periodFilterKey) {",
    "  const filterValue = buildPeriodFilterValue(ctx.period, spec.periodFilterMode);",
    "  mergedFilters[spec.periodFilterKey] = filterValue;",
    "  sourceMetadata.periodFilterKey = spec.periodFilterKey;",
    "  sourceMetadata.periodFilterMode = spec.periodFilterMode || \"month\";",
    "  sourceMetadata.periodFilterValue = filterValue;",
    "} else if (spec.historyFilterKey && Number(spec.historyWindowDays || 0) > 0) {",
    "  const filterValue = buildHistoryFilterValue(spec.historyWindowDays);",
    "  mergedFilters[spec.historyFilterKey] = filterValue;",
    "  sourceMetadata.historyFilterKey = spec.historyFilterKey;",
    "  sourceMetadata.historyFilterValue = filterValue;",
    "}",
    "const payload = {",
    "  model: baseQuery.model,",
    "  view: baseQuery.view,",
    "  fields: baseQuery.fields,",
    "  filters: mergedFilters,",
    "  filter_expression: baseQuery.filter_expression,",
    "  sorts: baseQuery.sorts,",
    "  limit: \"-1\",",
    "  column_limit: baseQuery.column_limit,",
    "  total: baseQuery.total,",
    "  row_total: baseQuery.row_total,",
    "  subtotals: baseQuery.subtotals,",
    "  vis_config: baseQuery.vis_config,",
    "  filter_config: null,",
    "  query_timezone: baseQuery.query_timezone,",
    "  dynamic_fields: baseQuery.dynamic_fields,",
    "};",
    "return [{ json: { payload, sourceMetadata } }];",
  ].join("\n");
}

function buildApplyReportCode(spec, previousStateNodeName, buildExportNodeName, createExportNodeName, runtimeSource) {
  const specJson = JSON.stringify({
    fileType: spec.fileType,
    fileName: spec.fileName,
    lookId: spec.lookId,
    reportName: spec.reportName,
  }, null, 2);

  return [
    `const state = $(\"${previousStateNodeName}\").all()[0].json || {};`,
    "const ctx = $(\"Build Run Context\").all()[0].json || {};",
    `const runtimeSource = ${JSON.stringify(runtimeSource)};`,
    "const { applyLookerCsvToWorkbook } = new Function(`${runtimeSource}; return { applyLookerCsvToWorkbook };`)();",
    "const input = $input.first();",
    "if (!input.binary?.reportCsv) throw new Error(\"Looker CSV response binary was not present on the current item.\");",
    "const csvBuffer = await this.helpers.getBinaryDataBuffer(0, \"reportCsv\");",
    "const csvText = csvBuffer.toString(\"utf8\");",
    `const spec = ${specJson};`,
    "const workbookPayload = state.workbookPayload || { workspace: { label: \"Veem Billing Workspace\" }, user: {}, snapshot: {} };",
    "const reportResults = Array.isArray(state.reportResults) ? [...state.reportResults] : [];",
    `const sourceMetadata = { ...($(\"${buildExportNodeName}\").all()[0].json.sourceMetadata || {}), queryId: String($(\"${createExportNodeName}\").all()[0].json.id || \"\"), fetchedAt: new Date().toISOString(), byteCount: csvBuffer.length };`,
    "const { workbook, result } = applyLookerCsvToWorkbook(workbookPayload, {",
    "  fileType: spec.fileType,",
    "  period: ctx.period,",
    "  csvText,",
    "}, {",
    "  includeDetailRows: false,",
    "  savedAt: new Date().toISOString(),",
    "  runId: ctx.runId,",
    "  source: \"n8n-cloud\",",
    "});",
    "result.sourceMetadata = sourceMetadata;",
    "const entry = {",
    "  fileType: spec.fileType,",
    "  fileName: spec.fileName,",
    "  lookId: spec.lookId,",
    "  reportName: spec.reportName,",
    "  status: \"imported\",",
    "  savedAt: workbook.savedAt || new Date().toISOString(),",
    "  warnings: result.warnings || [],",
    "  stats: result.stats || {},",
    "  changeSummary: result.changeSummary || {},",
    "  sourceMetadata,",
    "  sectionKeys: Object.keys(result.sections || {}),",
    "  byteCount: sourceMetadata.byteCount || 0,",
    "};",
    "reportResults.push(entry);",
    "return [{ json: { ...state, workbookPayload: workbook, reportResults, lastReportEntry: entry } }];",
  ].join("\n");
}

function buildFinalizeCode(lastStateNodeName, runtimeSource) {
  return [
    `const state = $(\"${lastStateNodeName}\").all()[0].json || {};`,
    "const ctx = $(\"Build Run Context\").all()[0].json || {};",
    `const runtimeSource = ${JSON.stringify(runtimeSource)};`,
    "const { normalizeCloudSyncSummary } = new Function(`${runtimeSource}; return { normalizeCloudSyncSummary };`)();",
    "let workbookPayload = state.workbookPayload || { workspace: { label: \"Veem Billing Workspace\" }, user: {}, snapshot: {} };",
    "const reportResults = Array.isArray(state.reportResults) ? state.reportResults : [];",
    "const savedAt = new Date().toISOString();",
    "if (!workbookPayload.workspace || typeof workbookPayload.workspace !== \"object\") workbookPayload.workspace = {};",
    "if (!workbookPayload.workspace.label) workbookPayload.workspace.label = \"Veem Billing Workspace\";",
    "workbookPayload.savedAt = savedAt;",
    "function buildJsonBinary(fileName, jsonText) {",
    "  return {",
    "    data: Buffer.from(jsonText, \"utf8\").toString(\"base64\"),",
    "    fileName,",
    "    mimeType: \"application/json\",",
    "  };",
    "}",
    "function buildKeyParts(objectKey) {",
    "  const cleaned = String(objectKey || \"\").replace(/^\\/+|\\/+$/g, \"\");",
    "  const parts = cleaned.split(\"/\");",
    "  const fileName = parts.pop() || \"artifact.json\";",
    "  return { parentFolderKey: parts.join(\"/\"), fileName };",
    "}",
    "const summary = {",
    "  ...normalizeCloudSyncSummary(reportResults),",
    "  configSource: \"embedded-cloud-workflow\",",
    "  period: ctx.period,",
    "  runId: ctx.runId,",
    "  baseUrl: ctx.lookerBaseUrl,",
    "  apiVersion: ctx.lookerApiVersion,",
    "  forceProduction: ctx.forceProduction,",
    "  configuredReportCount: ctx.configuredReportCount || reportResults.length,",
    "  source: \"n8n-cloud\",",
    "  savedAt,",
    "};",
    "const workbookJson = JSON.stringify(workbookPayload, null, 2);",
    "const summaryJson = JSON.stringify(summary, null, 2);",
    "const workbookKeyParts = buildKeyParts(ctx.workbookKey);",
    "const summaryKeyParts = buildKeyParts(ctx.summaryKey);",
    "const historyTimestamp = savedAt.replace(/[-:.TZ]/g, \"\").slice(0, 14);",
    "const historyParentFolderKey = String(ctx.workbookHistoryPrefix || \"\").replace(/^\\/+|\\/+$/g, \"\");",
    "const historyFileName = `current-workbook-${historyTimestamp}.json`;",
    "return [{",
    "  json: {",
    "    bucketName: ctx.bucketName,",
    "    workbookParentFolderKey: workbookKeyParts.parentFolderKey,",
    "    workbookFileName: workbookKeyParts.fileName,",
    "    summaryParentFolderKey: summaryKeyParts.parentFolderKey,",
    "    summaryFileName: summaryKeyParts.fileName,",
    "    workbookHistoryParentFolderKey: historyParentFolderKey,",
    "    workbookHistoryFileName: historyFileName,",
    "    savedAt,",
    "    runId: ctx.runId,",
    "    period: ctx.period,",
    "    hasErrors: summary.hasErrors,",
    "    errorCount: summary.errorCount,",
    "    importedCount: summary.importedCount,",
    "  },",
    "  binary: {",
    "    workbookFile: buildJsonBinary(workbookKeyParts.fileName, workbookJson),",
    "    workbookHistoryFile: buildJsonBinary(historyFileName, workbookJson),",
    "    summaryFile: buildJsonBinary(summaryKeyParts.fileName, summaryJson),",
    "  },",
    "}];",
  ].join("\n");
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

function httpRequestNode(name, position, parameters) {
  return {
    parameters,
    id: makeNodeId(name),
    name,
    type: "n8n-nodes-base.httpRequest",
    typeVersion: 4.2,
    position,
  };
}

function buildWorkflow(config, runtimeSource) {
  const workbookKeyParts = splitS3Key(DEFAULT_WORKBOOK_KEY);
  const summaryKeyParts = splitS3Key(DEFAULT_SUMMARY_KEY);
  const nodes = [];
  const connections = {};

  const addNode = (node) => {
    nodes.push(node);
    return node;
  };

  const connect = (fromName, toName) => {
    connections[fromName] ||= { main: [[]] };
    connections[fromName].main[0].push({ node: toName, type: "main", index: 0 });
  };

  addNode({
    parameters: {},
    id: makeNodeId("Manual Trigger"),
    name: "Manual Trigger",
    type: "n8n-nodes-base.manualTrigger",
    typeVersion: 1,
    position: [220, 220],
  });

  addNode({
    parameters: {
      rule: {
        interval: [
          {
            field: "cronExpression",
            expression: "=0 7 * * *",
          },
        ],
      },
    },
    id: makeNodeId("Daily Schedule"),
    name: "Daily Schedule",
    type: "n8n-nodes-base.scheduleTrigger",
    typeVersion: 1.2,
    position: [220, 420],
  });

  addNode(codeNode("Build Run Context", [520, 320], buildContextCode(config)));

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
    position: [860, 320],
  });

  addNode(codeNode("Initialize Workbook State", [1180, 320], buildInitializeWorkbookCode()));

  addNode(httpRequestNode("Looker Login", [1480, 320], {
    method: "POST",
    url: "={{ $(\"Build Run Context\").all()[0].json.lookerBaseUrl.replace(/\\/+$/, '') + '/api/' + $(\"Build Run Context\").all()[0].json.lookerApiVersion + '/login' }}",
    sendBody: true,
    contentType: "form-urlencoded",
    specifyBody: "keypair",
    bodyParameters: {
      parameters: [
        {
          name: "client_id",
          value: "={{ $(\"Build Run Context\").all()[0].json.lookerClientId }}",
        },
        {
          name: "client_secret",
          value: "={{ $(\"Build Run Context\").all()[0].json.lookerClientSecret }}",
        },
      ],
    },
    options: {
      response: {
        response: {
          responseFormat: "json",
        },
      },
    },
  }));

  connect("Manual Trigger", "Build Run Context");
  connect("Daily Schedule", "Build Run Context");
  connect("Build Run Context", "Download Current Workbook");
  connect("Download Current Workbook", "Initialize Workbook State");
  connect("Initialize Workbook State", "Looker Login");

  let previousStateNodeName = "Initialize Workbook State";
  let previousTriggerNodeName = "Looker Login";
  const reportSpacing = 360;
  const reportStartX = 1800;

  for (const [index, report] of (config.reports || []).entries()) {
    const groupX = reportStartX + (index * reportSpacing);
    const safeLabel = report.reportName;
    const getLookName = `Get Look: ${safeLabel}`;
    const getQueryName = `Get Query: ${safeLabel}`;
    const buildExportName = `Build Export Query: ${safeLabel}`;
    const createExportName = `Create Export Query: ${safeLabel}`;
    const runCsvName = `Run CSV: ${safeLabel}`;
    const applyName = `Apply ${safeLabel}`;

    addNode(httpRequestNode(getLookName, [groupX, 60], {
      method: "GET",
      url: `={{ $(\"Build Run Context\").all()[0].json.lookerBaseUrl.replace(/\\/+$/, '') + '/api/' + $(\"Build Run Context\").all()[0].json.lookerApiVersion + '/looks/${report.lookId}' }}`,
      sendHeaders: true,
      specifyHeaders: "keypair",
      headerParameters: {
        parameters: [
          {
            name: "Authorization",
            value: "={{ 'token ' + $(\"Looker Login\").all()[0].json.access_token }}",
          },
          {
            name: "Accept",
            value: "application/json",
          },
        ],
      },
      options: {
        response: {
          response: {
            responseFormat: "json",
          },
        },
      },
    }));

    addNode(httpRequestNode(getQueryName, [groupX, 200], {
      method: "GET",
      url: "={{ $(\"Build Run Context\").all()[0].json.lookerBaseUrl.replace(/\\/+$/, '') + '/api/' + $(\"Build Run Context\").all()[0].json.lookerApiVersion + '/queries/' + $json.query_id }}",
      sendHeaders: true,
      specifyHeaders: "keypair",
      headerParameters: {
        parameters: [
          {
            name: "Authorization",
            value: "={{ 'token ' + $(\"Looker Login\").all()[0].json.access_token }}",
          },
          {
            name: "Accept",
            value: "application/json",
          },
        ],
      },
      options: {
        response: {
          response: {
            responseFormat: "json",
          },
        },
      },
    }));

    addNode(codeNode(buildExportName, [groupX, 340], buildExportQueryCode(report)));

    addNode(httpRequestNode(createExportName, [groupX, 500], {
      method: "POST",
      url: "={{ $(\"Build Run Context\").all()[0].json.lookerBaseUrl.replace(/\\/+$/, '') + '/api/' + $(\"Build Run Context\").all()[0].json.lookerApiVersion + '/queries' }}",
      sendHeaders: true,
      specifyHeaders: "keypair",
      headerParameters: {
        parameters: [
          {
            name: "Authorization",
            value: "={{ 'token ' + $(\"Looker Login\").all()[0].json.access_token }}",
          },
          {
            name: "Accept",
            value: "application/json",
          },
          {
            name: "Content-Type",
            value: "application/json",
          },
        ],
      },
      sendBody: true,
      contentType: "json",
      specifyBody: "json",
      jsonBody: "={{ $json.payload }}",
      options: {
        response: {
          response: {
            responseFormat: "json",
          },
        },
      },
    }));

    addNode(httpRequestNode(runCsvName, [groupX, 660], {
      method: "GET",
      url: "={{ $(\"Build Run Context\").all()[0].json.lookerBaseUrl.replace(/\\/+$/, '') + '/api/' + $(\"Build Run Context\").all()[0].json.lookerApiVersion + '/queries/' + $json.id + '/run/csv' }}",
      sendQuery: true,
      specifyQuery: "keypair",
      queryParameters: {
        parameters: [
          {
            name: "limit",
            value: "-1",
          },
          {
            name: "cache",
            value: "false",
          },
          {
            name: "force_production",
            value: "={{ $(\"Build Run Context\").all()[0].json.forceProduction ? 'true' : 'false' }}",
          },
        ],
      },
      sendHeaders: true,
      specifyHeaders: "keypair",
      headerParameters: {
        parameters: [
          {
            name: "Authorization",
            value: "={{ 'token ' + $(\"Looker Login\").all()[0].json.access_token }}",
          },
          {
            name: "Accept",
            value: "text/csv",
          },
        ],
      },
      options: {
        timeout: report.reportTimeout ? report.reportTimeout * 1000 : 600000,
        response: {
          response: {
            responseFormat: "file",
            outputPropertyName: "reportCsv",
          },
        },
      },
    }));

    addNode(codeNode(applyName, [groupX, 820], buildApplyReportCode(report, previousStateNodeName, buildExportName, createExportName, runtimeSource)));

    connect(previousTriggerNodeName, getLookName);
    connect(getLookName, getQueryName);
    connect(getQueryName, buildExportName);
    connect(buildExportName, createExportName);
    connect(createExportName, runCsvName);
    connect(runCsvName, applyName);

    previousStateNodeName = applyName;
    previousTriggerNodeName = applyName;
  }

  const finalX = reportStartX + ((config.reports || []).length * reportSpacing) + 240;
  addNode(codeNode("Finalize Cloud Sync", [finalX, 820], buildFinalizeCode(previousStateNodeName, runtimeSource)));

  addNode({
    parameters: {
      resource: "file",
      operation: "upload",
      bucketName: "={{ $json.bucketName }}",
      fileName: "={{ $json.workbookFileName }}",
      binaryData: true,
      binaryPropertyName: "workbookFile",
      additionalFields: {
        parentFolderKey: "={{ $json.workbookParentFolderKey }}",
      },
    },
    id: makeNodeId("Upload Current Workbook"),
    name: "Upload Current Workbook",
    type: "n8n-nodes-base.awsS3",
    typeVersion: 1,
    position: [finalX + 320, 680],
  });

  addNode({
    parameters: {
      resource: "file",
      operation: "upload",
      bucketName: "={{ $json.bucketName }}",
      fileName: "={{ $json.workbookHistoryFileName }}",
      binaryData: true,
      binaryPropertyName: "workbookHistoryFile",
      additionalFields: {
        parentFolderKey: "={{ $json.workbookHistoryParentFolderKey }}",
      },
    },
    id: makeNodeId("Upload Workbook History"),
    name: "Upload Workbook History",
    type: "n8n-nodes-base.awsS3",
    typeVersion: 1,
    position: [finalX + 320, 820],
  });

  addNode({
    parameters: {
      resource: "file",
      operation: "upload",
      bucketName: "={{ $json.bucketName }}",
      fileName: "={{ $json.summaryFileName }}",
      binaryData: true,
      binaryPropertyName: "summaryFile",
      additionalFields: {
        parentFolderKey: "={{ $json.summaryParentFolderKey }}",
      },
    },
    id: makeNodeId("Upload Sync Summary"),
    name: "Upload Sync Summary",
    type: "n8n-nodes-base.awsS3",
    typeVersion: 1,
    position: [finalX + 320, 960],
  });

  connect(previousTriggerNodeName, "Finalize Cloud Sync");
  connect("Finalize Cloud Sync", "Upload Current Workbook");
  connect("Finalize Cloud Sync", "Upload Workbook History");
  connect("Finalize Cloud Sync", "Upload Sync Summary");

  return {
    name: "Billing Workbook Looker Cloud Sync",
    active: false,
    isArchived: false,
    nodes,
    connections,
    settings: {
      executionOrder: "v1",
      timezone: "America/Toronto",
      callerPolicy: "workflowsFromSameOwner",
      availableInMCP: false,
    },
    pinData: {},
    tags: [],
  };
}

const runtimeSource = stripExports(fs.readFileSync(RUNTIME_PATH, "utf8"));
const config = JSON.parse(fs.readFileSync(CONFIG_PATH, "utf8"));
const workflow = buildWorkflow(config, runtimeSource);
const workbookKeyParts = splitS3Key(DEFAULT_WORKBOOK_KEY);
const summaryKeyParts = splitS3Key(DEFAULT_SUMMARY_KEY);
fs.writeFileSync(OUTPUT_PATH, JSON.stringify(workflow, null, 2) + "\n");

console.log(`Wrote ${OUTPUT_PATH}`);
console.log(`Reports: ${(config.reports || []).length}`);
console.log(`Workbook seed: s3://${DEFAULT_BUCKET}/${workbookKeyParts.parentFolderKey}/${workbookKeyParts.fileName}`);
console.log(`Summary key: s3://${DEFAULT_BUCKET}/${summaryKeyParts.parentFolderKey}/${summaryKeyParts.fileName}`);

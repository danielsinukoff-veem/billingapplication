#!/usr/bin/env node
import fs from "node:fs/promises";
import path from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";

const SCRIPT_DIR = path.dirname(fileURLToPath(import.meta.url));
const ROOT_DIR = path.resolve(SCRIPT_DIR, "..");
const ENV_PATH = path.join(ROOT_DIR, ".env.n8n");
const SPLIT_DIR = path.join(ROOT_DIR, "docs", "n8n-looker-cloud-split");
const LOOKER_NAME_RE = /^Billing Workbook Looker (0[1-9]|1[0-4]) /;
const QA_CHECKER_NAME = "Billing Workbook QA Checker - Updated 2026-04-30";

function slug(input) {
  return String(input)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 80);
}

async function loadLocalEnv() {
  let raw = "";
  try {
    raw = await fs.readFile(ENV_PATH, "utf8");
  } catch (error) {
    if (error.code === "ENOENT") return;
    throw error;
  }

  for (const line of raw.split(/\r?\n/)) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) continue;
    const match = trimmed.match(/^([A-Za-z_][A-Za-z0-9_]*)=(.*)$/);
    if (!match) continue;
    const [, key, rawValue] = match;
    if (!process.env[key]) process.env[key] = rawValue.trim().replace(/^['"]|['"]$/g, "");
  }
}

async function n8nRequest(method, route, body) {
  const apiKey = process.env.N8N_API_KEY;
  if (!apiKey) throw new Error("N8N_API_KEY is required to resolve live workflow IDs.");
  const baseUrl = (process.env.N8N_BASE_URL || "https://veem.app.n8n.cloud").replace(/\/+$/, "");

  const response = await fetch(new URL(route, baseUrl), {
    method,
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
      "X-N8N-API-KEY": apiKey
    },
    body: body ? JSON.stringify(body) : undefined
  });

  const text = await response.text();
  let json = null;
  if (text) {
    try {
      json = JSON.parse(text);
    } catch {
      json = { raw: text };
    }
  }
  if (!response.ok) {
    throw new Error(`${method} ${route} failed with HTTP ${response.status}: ${JSON.stringify(json).slice(0, 800)}`);
  }
  return json || {};
}

async function listAllWorkflows() {
  const workflows = [];
  const projectPart = process.env.N8N_PROJECT_ID
    ? `&projectId=${encodeURIComponent(process.env.N8N_PROJECT_ID)}`
    : "";
  let route = `/api/v1/workflows?limit=250${projectPart}`;

  while (route) {
    const page = await n8nRequest("GET", route);
    const items = Array.isArray(page) ? page : Array.isArray(page.data) ? page.data : [];
    workflows.push(...items);
    const nextCursor = page.nextCursor || page.nextPageCursor;
    route = nextCursor
      ? `/api/v1/workflows?limit=250${projectPart}&cursor=${encodeURIComponent(nextCursor)}`
      : "";
  }
  return workflows;
}

function makeDailySchedule() {
  return {
    parameters: {
      rule: {
        interval: [
          {
            field: "cronExpression",
            expression: "=0 0 * * *"
          }
        ]
      }
    },
    id: "daily-schedule",
    name: "Daily Schedule",
    type: "n8n-nodes-base.scheduleTrigger",
    typeVersion: 1.2,
    position: [220, 380]
  };
}

function makeRunChainWebhook() {
  return {
    parameters: {
      httpMethod: "POST",
      path: "billing-looker-cloud-chain",
      responseMode: "onReceived",
      options: {
        allowedOrigins: "https://billing.qa-us-west-2.veem.com",
        responseCode: 202
      }
    },
    id: "run-chain-webhook",
    name: "Run Chain Webhook",
    type: "n8n-nodes-base.webhook",
    typeVersion: 2.1,
    position: [220, 700],
    webhookId: "billing-looker-cloud-chain"
  };
}

function makeStepWebhook(index) {
  const step = String(index + 1).padStart(2, "0");
  return {
    parameters: {
      httpMethod: "POST",
      path: `billing-looker-cloud-step-${step}`,
      responseMode: "onReceived",
      options: {
        allowedOrigins: "https://billing.qa-us-west-2.veem.com",
        responseCode: 202
      }
    },
    id: `run-step-${step}-webhook`,
    name: `Run Step ${step} Webhook`,
    type: "n8n-nodes-base.webhook",
    typeVersion: 2.1,
    position: [220, 700],
    webhookId: `billing-looker-cloud-step-${step}`
  };
}

function makeTriggerNextNode(name, targetUrl, workflowName, position) {
  const isQaCheckerTrigger = name === "Trigger QA Checker";
  const jsonBody = isQaCheckerTrigger
    ? `={{ (() => { const now = new Date(); const previous = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth() - 1, 1)); const period = previous.getUTCFullYear() + "-" + String(previous.getUTCMonth() + 1).padStart(2, "0"); return { source: "looker-cloud-chain", previousWorkflow: ${JSON.stringify(workflowName)}, period, runId: $("Build Run Context").all()[0].json.runId, triggeredAt: new Date().toISOString() }; })() }}`
    : `={{ ({ source: "looker-cloud-chain", previousWorkflow: ${JSON.stringify(workflowName)}, period: $("Build Run Context").all()[0].json.period, runId: $("Build Run Context").all()[0].json.runId, triggeredAt: new Date().toISOString() }) }}`;
  return {
    parameters: {
      method: "POST",
      url: targetUrl,
      sendBody: true,
      contentType: "json",
      specifyBody: "json",
      jsonBody,
      options: {
        response: {
          response: {
            responseFormat: "json"
          }
        }
      },
      infoMessage: "Starts the next small workflow only after this workflow has uploaded its outputs."
    },
    id: slug(name),
    name,
    type: "n8n-nodes-base.httpRequest",
    typeVersion: 4.2,
    position
  };
}

function removeNodeAndReferences(workflow, predicate) {
  const removedNames = new Set(workflow.nodes.filter(predicate).map((node) => node.name));
  if (!removedNames.size) return removedNames;

  workflow.nodes = workflow.nodes.filter((node) => !removedNames.has(node.name));
  for (const name of removedNames) delete workflow.connections[name];

  for (const [sourceName, sourceConnections] of Object.entries(workflow.connections)) {
    for (const [connectionType, branches] of Object.entries(sourceConnections)) {
      sourceConnections[connectionType] = branches
        .map((branch) => branch.filter((connection) => !removedNames.has(connection.node)))
        .filter((branch) => branch.length);
    }
    if (Object.values(sourceConnections).every((branches) => !branches.length)) {
      delete workflow.connections[sourceName];
    }
  }

  return removedNames;
}

function connect(workflow, from, to) {
  workflow.connections[from] = workflow.connections[from] || {};
  workflow.connections[from].main = workflow.connections[from].main || [];
  workflow.connections[from].main[0] = workflow.connections[from].main[0] || [];
  const exists = workflow.connections[from].main[0].some((connection) => connection.node === to);
  if (!exists) workflow.connections[from].main[0].push({ node: to, type: "main", index: 0 });
}

function ensureTriggerConnection(workflow, triggerName) {
  const buildNode = workflow.nodes.find((node) => node.name === "Build Run Context");
  if (!buildNode) throw new Error(`${workflow.name}: missing Build Run Context`);
  connect(workflow, triggerName, buildNode.name);
}

function updateWorkflow(workflow, index, orderedNames) {
  removeNodeAndReferences(workflow, (node) =>
    node.name === "Run Chain Webhook" ||
    /^Run Step \d{2} Webhook$/.test(node.name) ||
    node.name.startsWith("Run Next: ") ||
    node.name === "Run QA Checker" ||
    node.name.startsWith("Trigger Next: ") ||
    node.name === "Trigger QA Checker"
  );

  if (index === 0) {
    removeNodeAndReferences(workflow, (node) => node.name === "Daily Schedule");
    workflow.nodes.push(makeDailySchedule());
    ensureTriggerConnection(workflow, "Daily Schedule");

    if (!workflow.nodes.some((node) => node.name === "Run Chain Webhook")) {
      workflow.nodes.push(makeRunChainWebhook());
    }
    ensureTriggerConnection(workflow, "Run Chain Webhook");
  } else {
    removeNodeAndReferences(workflow, (node) => node.name === "Daily Schedule");
    const stepWebhook = makeStepWebhook(index);
    workflow.nodes.push(stepWebhook);
    ensureTriggerConnection(workflow, stepWebhook.name);
  }

  const uploadSummary = workflow.nodes.find((node) => node.name === "Upload Sync Summary");
  if (!uploadSummary) throw new Error(`${workflow.name}: missing Upload Sync Summary`);

  const nextStep = index + 2;
  const isFinal = index === orderedNames.length - 1;
  const nodeName = isFinal ? "Trigger QA Checker" : `Trigger Next: Step ${String(nextStep).padStart(2, "0")}`;
  const targetUrl = isFinal
    ? "https://veem.app.n8n.cloud/webhook/billing-qa-checker"
    : `https://veem.app.n8n.cloud/webhook/billing-looker-cloud-step-${String(nextStep).padStart(2, "0")}`;
  const triggerNode = makeTriggerNextNode(
    nodeName,
    targetUrl,
    workflow.name,
    [uploadSummary.position[0] + 360, uploadSummary.position[1]]
  );
  workflow.nodes.push(triggerNode);
  connect(workflow, uploadSummary.name, triggerNode.name);
}

async function main() {
  await loadLocalEnv();

  const files = (await fs.readdir(SPLIT_DIR))
    .filter((file) => file.endsWith(".workflow.json"))
    .sort();
  const workflows = await Promise.all(files.map(async (file) => {
    const absolutePath = path.join(SPLIT_DIR, file);
    return { file, absolutePath, workflow: JSON.parse(await fs.readFile(absolutePath, "utf8")) };
  }));
  const orderedNames = workflows.map(({ workflow }) => workflow.name);

  const liveWorkflows = await listAllWorkflows();
  const workflowIdsByName = new Map(liveWorkflows.map((workflow) => [workflow.name, workflow.id]));
  for (const name of orderedNames) {
    if (!LOOKER_NAME_RE.test(name)) throw new Error(`Unexpected split workflow name: ${name}`);
    if (!workflowIdsByName.has(name)) throw new Error(`Workflow not found in n8n: ${name}`);
  }
  if (!workflowIdsByName.has(QA_CHECKER_NAME)) throw new Error(`Workflow not found in n8n: ${QA_CHECKER_NAME}`);

  for (let index = 0; index < workflows.length; index += 1) {
    updateWorkflow(workflows[index].workflow, index, orderedNames);
    await fs.writeFile(
      workflows[index].absolutePath,
      `${JSON.stringify(workflows[index].workflow, null, 2)}\n`
    );
  }

  console.log(`Updated ${workflows.length} Looker split workflows.`);
  console.log("Workflow 01 now runs daily at 00:00 and exposes POST /billing-looker-cloud-chain.");
  console.log("Workflows 02-14 expose per-step webhooks and are started by the previous workflow after upload.");
  console.log("Workflow 14 starts the QA checker after its upload finishes.");
}

main().catch((error) => {
  console.error(error.stack || error.message);
  process.exit(1);
});

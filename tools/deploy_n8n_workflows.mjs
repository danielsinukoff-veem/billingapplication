#!/usr/bin/env node
import fs from "node:fs/promises";
import path from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";

const SCRIPT_DIR = path.dirname(fileURLToPath(import.meta.url));
const ROOT_DIR = path.resolve(SCRIPT_DIR, "..");
const LOCAL_ENV_PATH = path.join(ROOT_DIR, ".env.n8n");

const SUPPORT_WORKFLOW_FILES = [
  "docs/n8n-qa-checker.workflow.json",
  "docs/n8n-qa-status-api.workflow.json",
  "docs/n8n-looker-manual-import.workflow.json",
  "docs/n8n-contract-automation.workflow.json",
  "docs/n8n-hubspot-partner-sync.workflow.json"
];

const RETIRED_WORKFLOW_NAMES = [
  "Billing Workbook Looker Cloud Sync",
  "Billing Workbook Looker Cloud Sync - Updated 2026-04-30",
  "ARCHIVED - Billing Workbook Looker Cloud Split Guide - Updated 2026-04-30"
];

function parseArgs(argv) {
  const args = {
    apply: false,
    activate: false,
    deleteOld: false,
    only: null,
    exclude: null,
    baseUrl: process.env.N8N_BASE_URL || "https://veem.app.n8n.cloud"
  };

  for (const arg of argv) {
    if (arg === "--apply") args.apply = true;
    else if (arg === "--dry-run") args.apply = false;
    else if (arg === "--activate") args.activate = true;
    else if (arg === "--delete-old") args.deleteOld = true;
    else if (arg.startsWith("--only=")) args.only = new RegExp(arg.slice("--only=".length));
    else if (arg.startsWith("--exclude=")) args.exclude = new RegExp(arg.slice("--exclude=".length));
    else if (arg.startsWith("--base-url=")) args.baseUrl = arg.slice("--base-url=".length);
    else if (arg === "--help" || arg === "-h") {
      printHelp();
      process.exit(0);
    } else {
      throw new Error(`Unknown argument: ${arg}`);
    }
  }

  args.baseUrl = args.baseUrl.replace(/\/+$/, "");
  return args;
}

async function loadLocalEnv() {
  let raw = "";
  try {
    raw = await fs.readFile(LOCAL_ENV_PATH, "utf8");
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
    if (process.env[key]) continue;
    process.env[key] = rawValue.trim().replace(/^['"]|['"]$/g, "");
  }
}

function printHelp() {
  console.log(`Deploy Partner Billing n8n workflows from repo JSON exports.

Usage:
  node tools/deploy_n8n_workflows.mjs --dry-run
  N8N_API_KEY=... node tools/deploy_n8n_workflows.mjs --apply

Options:
  --dry-run       Print what would be created/updated/deleted. Default.
  --apply         Create/update workflows in n8n through the public API.
  --activate      Activate workflows after create/update. Use only after credentials are assigned.
  --delete-old    Delete older duplicate/retired workflows. Never runs unless --apply is also set.
  --only=REGEX    Only deploy workflows whose names match this JavaScript regex.
  --exclude=REGEX Skip workflows whose names match this JavaScript regex.
  --base-url=URL  n8n base URL. Defaults to N8N_BASE_URL or https://veem.app.n8n.cloud.

Environment:
  N8N_API_KEY          Required for --apply or live duplicate discovery.
  N8N_BASE_URL         Optional n8n base URL.
  N8N_CREDENTIAL_MAP   Optional JSON map keyed by node type to inject credentials.

Example N8N_CREDENTIAL_MAP:
  {
    "n8n-nodes-base.hubspot": {
      "hubspotOAuth2Api": { "id": "abc", "name": "HubSpot OAuth2 API" }
    },
    "n8n-nodes-base.awsS3": {
      "aws": { "id": "def", "name": "AWS (IAM) account" }
    }
  }
`);
}

async function activeWorkflowFiles() {
  const splitDir = path.join(ROOT_DIR, "docs", "n8n-looker-cloud-split");
  const splitFiles = (await fs.readdir(splitDir))
    .filter((file) => file.endsWith(".workflow.json"))
    .sort()
    .map((file) => path.join("docs", "n8n-looker-cloud-split", file));

  return [...splitFiles, ...SUPPORT_WORKFLOW_FILES];
}

async function loadJson(relativePath) {
  const absolutePath = path.join(ROOT_DIR, relativePath);
  return JSON.parse(await fs.readFile(absolutePath, "utf8"));
}

function parseCredentialMap() {
  const raw = process.env.N8N_CREDENTIAL_MAP;
  if (!raw) return {};
  try {
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      throw new Error("value must be an object");
    }
    return parsed;
  } catch (error) {
    throw new Error(`N8N_CREDENTIAL_MAP is not valid JSON: ${error.message}`);
  }
}

function stripPlaceholderCredentials(node) {
  if (!node.credentials || typeof node.credentials !== "object") return;

  const cleaned = {};
  for (const [credentialType, credential] of Object.entries(node.credentials)) {
    const id = String(credential?.id || "");
    if (!id || /^REPLACE_WITH_/i.test(id)) continue;
    cleaned[credentialType] = credential;
  }

  if (Object.keys(cleaned).length) node.credentials = cleaned;
  else delete node.credentials;
}

function injectCredentials(node, credentialMap) {
  const mapped = credentialMap[node.type];
  if (!mapped) return;
  node.credentials = {
    ...(node.credentials || {}),
    ...mapped
  };
}

function injectRuntimeSecrets(node) {
  if (node.type !== "n8n-nodes-base.code") return;
  const jsCode = node.parameters?.jsCode;
  if (typeof jsCode !== "string") return;

  let patched = jsCode;
  if (process.env.LOOKER_CLIENT_ID) {
    patched = patched.replaceAll("SET_LOOKER_CLIENT_ID", process.env.LOOKER_CLIENT_ID);
  }
  if (process.env.LOOKER_CLIENT_SECRET) {
    patched = patched.replaceAll("SET_LOOKER_CLIENT_SECRET", process.env.LOOKER_CLIENT_SECRET);
  }

  if (patched !== jsCode) {
    node.parameters = {
      ...node.parameters,
      jsCode: patched
    };
  }
}

function workflowBaseName(name) {
  return String(name || "").replace(/\s+-\s+Updated\s+\d{4}-\d{2}-\d{2}$/i, "").trim();
}

function apiPayloadForWorkflow(workflow, credentialMap) {
  const nodes = JSON.parse(JSON.stringify(workflow.nodes || []));
  for (const node of nodes) {
    stripPlaceholderCredentials(node);
    injectCredentials(node, credentialMap);
    injectRuntimeSecrets(node);
  }

  const payload = {
    name: workflow.name,
    nodes,
    connections: workflow.connections || {},
    settings: workflow.settings || {},
    pinData: workflow.pinData || {}
  };

  if (process.env.N8N_PROJECT_ID) {
    payload.projectId = process.env.N8N_PROJECT_ID;
  }

  return payload;
}

async function loadDesiredWorkflows() {
  const files = await activeWorkflowFiles();
  const credentialMap = parseCredentialMap();
  const desired = [];

  for (const relativePath of files) {
    const workflow = await loadJson(relativePath);
    if (!workflow.name || !Array.isArray(workflow.nodes)) {
      throw new Error(`Invalid n8n workflow export: ${relativePath}`);
    }
    desired.push({
      file: relativePath,
      name: workflow.name,
      baseName: workflowBaseName(workflow.name),
      nodeCount: workflow.nodes.length,
      payload: apiPayloadForWorkflow(workflow, credentialMap)
    });
  }

  const duplicateNames = new Set();
  const seen = new Set();
  for (const workflow of desired) {
    if (seen.has(workflow.name)) duplicateNames.add(workflow.name);
    seen.add(workflow.name);
  }
  if (duplicateNames.size) {
    throw new Error(`Duplicate desired workflow names: ${[...duplicateNames].join(", ")}`);
  }

  return desired;
}

async function n8nRequest(args, method, route, body) {
  const apiKey = process.env.N8N_API_KEY;
  if (!apiKey) throw new Error("N8N_API_KEY is required for n8n API calls.");

  const response = await fetch(new URL(route, args.baseUrl), {
    method,
    headers: {
      "Accept": "application/json",
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
    const detail = json ? JSON.stringify(json).slice(0, 1000) : response.statusText;
    throw new Error(`${method} ${route} failed with HTTP ${response.status}: ${detail}`);
  }

  return json || {};
}

async function listAllWorkflows(args) {
  const workflows = [];
  const projectPart = process.env.N8N_PROJECT_ID
    ? `&projectId=${encodeURIComponent(process.env.N8N_PROJECT_ID)}`
    : "";
  let route = `/api/v1/workflows?limit=250${projectPart}`;

  while (route) {
    const page = await n8nRequest(args, "GET", route);
    const items = Array.isArray(page) ? page : Array.isArray(page.data) ? page.data : [];
    workflows.push(...items);

    const nextCursor = page.nextCursor || page.nextPageCursor;
    route = nextCursor
      ? `/api/v1/workflows?limit=250${projectPart}&cursor=${encodeURIComponent(nextCursor)}`
      : "";
  }

  return workflows;
}

function findOldWorkflowCandidates(existing, desired) {
  const desiredNames = new Set(desired.map((workflow) => workflow.name));
  const desiredBaseNames = new Set(desired.map((workflow) => workflow.baseName));
  const retiredNames = new Set(RETIRED_WORKFLOW_NAMES);

  return existing.filter((workflow) => {
    const name = String(workflow.name || "");
    if (desiredNames.has(name)) return false;
    if (retiredNames.has(name)) return true;
    if (!desiredBaseNames.has(workflowBaseName(name))) return false;
    return /Updated\s+\d{4}-\d{2}-\d{2}$/i.test(name) || desiredBaseNames.has(name);
  });
}

async function applyWorkflows(args, desired, existing) {
  const existingByName = new Map();
  for (const workflow of existing) {
    if (!existingByName.has(workflow.name)) existingByName.set(workflow.name, []);
    existingByName.get(workflow.name).push(workflow);
  }

  for (const workflow of desired) {
    const matches = existingByName.get(workflow.name) || [];
    if (matches.length) {
      const target = matches[0];
      console.log(`update ${workflow.name} (${workflow.nodeCount} nodes)`);
      const updatePayload = { ...workflow.payload };
      delete updatePayload.projectId;
      await n8nRequest(args, "PUT", `/api/v1/workflows/${encodeURIComponent(target.id)}`, updatePayload);
      if (args.activate) {
        await n8nRequest(args, "POST", `/api/v1/workflows/${encodeURIComponent(target.id)}/activate`);
      }
      if (matches.length > 1) {
        console.log(`  warning: ${matches.length - 1} duplicate exact-name workflow(s) still exist`);
      }
    } else {
      console.log(`create ${workflow.name} (${workflow.nodeCount} nodes)`);
      const created = await n8nRequest(args, "POST", "/api/v1/workflows", workflow.payload);
      if (args.activate && created.id) {
        await n8nRequest(args, "POST", `/api/v1/workflows/${encodeURIComponent(created.id)}/activate`);
      }
    }
  }
}

async function deleteOldWorkflows(args, oldWorkflows) {
  if (!oldWorkflows.length) return;
  if (!args.deleteOld) {
    console.log("\nolder/retired workflows found but not deleted:");
    for (const workflow of oldWorkflows) console.log(`  ${workflow.name} (${workflow.id})`);
    console.log("rerun with --apply --delete-old to delete these after verifying the new workflows exist.");
    return;
  }

  for (const workflow of oldWorkflows) {
    console.log(`delete old ${workflow.name} (${workflow.id})`);
    await n8nRequest(args, "DELETE", `/api/v1/workflows/${encodeURIComponent(workflow.id)}`);
  }
}

function printDryRun(desired) {
  console.log("dry run only. no n8n changes will be made.\n");
  console.log("workflow JSONs that would be created/updated:");
  for (const workflow of desired) {
    console.log(`  ${workflow.name} (${workflow.nodeCount} nodes) <- ${workflow.file}`);
  }
}

async function main() {
  await loadLocalEnv();
  const args = parseArgs(process.argv.slice(2));
  let desired = await loadDesiredWorkflows();
  if (args.only) desired = desired.filter((workflow) => args.only.test(workflow.name));
  if (args.exclude) desired = desired.filter((workflow) => !args.exclude.test(workflow.name));
  if (!desired.length) {
    throw new Error("No desired workflows matched the provided --only/--exclude filters.");
  }

  printDryRun(desired);

  if (!process.env.N8N_API_KEY) {
    console.log("\nN8N_API_KEY is not set, so live duplicate discovery and deploy were skipped.");
    console.log("Create/copy an n8n API key once, then rerun with N8N_API_KEY=... and --apply.");
    return;
  }

  const existing = await listAllWorkflows(args);
  const oldWorkflows = findOldWorkflowCandidates(existing, desired);

  if (!args.apply) {
    if (oldWorkflows.length) {
      console.log("\nolder/retired workflows currently in n8n:");
      for (const workflow of oldWorkflows) console.log(`  ${workflow.name} (${workflow.id})`);
    } else {
      console.log("\nno older/retired matching workflows found in n8n.");
    }
    return;
  }

  console.log("\napplying changes to n8n...");
  await applyWorkflows(args, desired, existing);
  await deleteOldWorkflows(args, oldWorkflows);

  console.log("\nfinished. Review credentials in n8n before activating if --activate was not used.");
}

main().catch((error) => {
  console.error(error.stack || error.message);
  process.exit(1);
});

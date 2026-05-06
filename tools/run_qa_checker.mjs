#!/usr/bin/env node
import fs from "node:fs";
import path from "node:path";
import { qaReportToCsv, runWorkbookQaCheck } from "./qa_checker_runtime.mjs";

function parseArgs(argv) {
  const args = {
    workbook: "data/current-workbook.json",
    period: "",
    out: "",
    csv: "",
    source: "local-cli",
  };
  for (let index = 2; index < argv.length; index += 1) {
    const token = argv[index];
    if (!token.startsWith("--")) continue;
    const key = token.slice(2);
    const value = argv[index + 1] && !argv[index + 1].startsWith("--") ? argv[++index] : "true";
    args[key] = value;
  }
  return args;
}

function ensureParent(filePath) {
  const parent = path.dirname(path.resolve(filePath));
  fs.mkdirSync(parent, { recursive: true });
}

const args = parseArgs(process.argv);
const workbookPath = path.resolve(args.workbook);
const workbookPayload = JSON.parse(fs.readFileSync(workbookPath, "utf8"));
const report = runWorkbookQaCheck(workbookPayload, {
  period: args.period,
  source: args.source,
});

if (args.out) {
  ensureParent(args.out);
  fs.writeFileSync(path.resolve(args.out), JSON.stringify(report, null, 2) + "\n");
}

if (args.csv) {
  ensureParent(args.csv);
  fs.writeFileSync(path.resolve(args.csv), qaReportToCsv(report));
}

console.log(JSON.stringify({
  status: report.status,
  period: report.period,
  issueCount: report.summary.issueCount,
  criticalCount: report.summary.criticalCount,
  warningCount: report.summary.warningCount,
  infoCount: report.summary.infoCount,
  partnersWithIssues: report.summary.partnersWithIssues,
}, null, 2));

if (report.summary.criticalCount > 0 && args["fail-on-critical"] !== "false") {
  process.exitCode = 2;
}

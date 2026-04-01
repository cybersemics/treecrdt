/**
 * Compare two benchmark summary.json files and print the same table as CI
 * (.github/workflows/benchmarks.yml "Prepare comment body" step).
 *
 * Usage: node scripts/bench-report.mjs <base-summary.json> <head-summary.json> [base-label] [head-label]
 * Example: node scripts/bench-report.mjs bench-base/benchmarks/summary.json bench-head/benchmarks/summary.json "Run 1" "Run 2"
 */

import fs from "node:fs";
import path from "node:path";

const baseFile = process.argv[2];
const headFile = process.argv[3];
const baseLabel = process.argv[4] ?? "Base";
const headLabel = process.argv[5] ?? "Head";

if (!baseFile || !headFile) {
  console.error("Usage: node scripts/bench-report.mjs <base-summary.json> <head-summary.json> [base-label] [head-label]");
  process.exit(1);
}

function readSummary(file) {
  const p = path.resolve(process.cwd(), file);
  if (!fs.existsSync(p)) return [];
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

const base = readSummary(baseFile);
const head = readSummary(headFile);

const keyFor = (row) =>
  `${row.implementation ?? "-"}|${row.storage ?? "-"}|${row.workload ?? row.name ?? "-"}`;

const mapBase = new Map(base.map((r) => [keyFor(r), r]));
const mapHead = new Map(head.map((r) => [keyFor(r), r]));
const keys = Array.from(new Set([...mapBase.keys(), ...mapHead.keys()])).sort();

const format = (value, digits = 2) =>
  value === null || value === undefined || Number.isNaN(Number(value))
    ? "-"
    : Number(value).toFixed(digits);
const formatPct = (value) =>
  value === null || value === undefined || Number.isNaN(Number(value))
    ? "-"
    : `${value >= 0 ? "+" : ""}${value.toFixed(2)}%`;

const severityLevel = (pct) => {
  if (pct === null || pct === undefined || Number.isNaN(pct)) return 0;
  const abs = Math.abs(pct);
  if (abs >= 30) return 3;
  if (abs >= 10) return 2;
  if (abs >= 5) return 1;
  return 0;
};
const statusEmoji = (deltaOps, deltaMs) => {
  const level = Math.max(severityLevel(deltaOps), severityLevel(deltaMs));
  return level === 3 ? "🔴" : level === 2 ? "🟠" : level === 1 ? "🟡" : "✅";
};

const lines = [];
lines.push(
  `| Impl | Storage | Workload | ${baseLabel} ops/s | ${headLabel} ops/s | Δ ops/s | ${baseLabel} p50 ms | ${headLabel} p50 ms | Δ p50 | Status |`
);
lines.push("|---|---|---|---:|---:|---:|---:|---:|---:|---:|");

let improved = 0;
let regressed = 0;

for (const key of keys) {
  const baseRow = mapBase.get(key);
  const headRow = mapHead.get(key);

  const baseOps = baseRow?.opsPerSec ?? null;
  const headOps = headRow?.opsPerSec ?? null;
  const deltaOps =
    baseOps !== null && headOps !== null ? ((headOps - baseOps) / baseOps) * 100 : null;

  if (deltaOps !== null) {
    if (deltaOps > 0) improved += 1;
    else if (deltaOps < 0) regressed += 1;
  }

  const baseMs = baseRow?.durationMs ?? null;
  const headMs = headRow?.durationMs ?? null;
  const deltaMs =
    baseMs !== null && headMs !== null ? ((headMs - baseMs) / baseMs) * 100 : null;

  const [impl, storage, workload] = key.split("|").map((s) => s.trim());
  const status = statusEmoji(deltaOps, deltaMs);
  lines.push(
    `| ${impl} | ${storage} | ${workload} | ${format(baseOps)} | ${format(headOps)} | ${formatPct(deltaOps)} | ${format(baseMs)} | ${format(headMs)} | ${formatPct(deltaMs)} | ${status} |`
  );
}

console.log("## Benchmarks (local compare)");
console.log("");
console.log(`${baseLabel} vs ${headLabel}`);
console.log(`Compared entries: ${keys.length} - Improved: ${improved} - Regressed: ${regressed}`);
console.log("");
if (keys.length === 0) {
  console.log("_No benchmark results were found._");
} else {
  console.log(lines.join("\n"));
  console.log("");
  console.log("**Status:** ✅ <5% change | 🟡 5–10% | 🟠 10–30% | 🔴 >30%");
}

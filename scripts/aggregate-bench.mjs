import fs from "node:fs/promises";
import path from "node:path";
import { repoRootFromImportMeta } from "./repo-root.mjs";

const repoRoot = repoRootFromImportMeta(import.meta.url, 1);
const benchRoot = path.join(repoRoot, "benchmarks");

async function pathExists(p) {
  try {
    await fs.access(p);
    return true;
  } catch {
    return false;
  }
}

async function walk(dir) {
  const entries = [];
  const items = await fs.readdir(dir, { withFileTypes: true });
  for (const item of items) {
    const full = path.join(dir, item.name);
    if (item.isDirectory()) {
      entries.push(...(await walk(full)));
    } else if (item.isFile() && item.name.endsWith(".json") && item.name !== "summary.json") {
      entries.push(full);
    }
  }
  return entries;
}

function toMarkdown(rows) {
  const header = ["Implementation", "Storage", "Workload", "Mode", "Iterations", "TotalOps", "Median (ms)", "P95 (ms)", "Ops/s", "File"];
  const lines = [header.join(" | "), header.map(() => "---").join(" | ")];
  for (const row of rows) {
    const mode = row.extra?.mode ?? "-";
    const iterations = row.extra?.iterations ?? 1;
    const p95Ms = row.extra?.p95Ms;
    lines.push(
      [
        row.implementation ?? "-",
        row.storage ?? "-",
        row.workload ?? row.name ?? "-",
        mode,
        iterations,
        row.totalOps ?? "-",
        row.durationMs?.toFixed?.(2) ?? "-",
        typeof p95Ms === "number" ? p95Ms.toFixed(2) : "-",
        row.opsPerSec?.toFixed?.(2) ?? "-",
        row.relativePath ?? "-",
      ].join(" | ")
    );
  }
  return lines.join("\n");
}

async function main() {
  if (!(await pathExists(benchRoot))) {
    console.warn(`No benchmarks directory found at ${benchRoot}`);
    return;
  }
  const files = await walk(benchRoot);
  const rows = [];
  for (const file of files) {
    try {
      const data = JSON.parse(await fs.readFile(file, "utf-8"));
      rows.push({
        ...data,
        relativePath: path.relative(repoRoot, file),
      });
    } catch (err) {
      console.warn(`Skipping ${file}: ${err}`);
    }
  }
  rows.sort((a, b) => {
    const byImplementation = String(a.implementation ?? "").localeCompare(String(b.implementation ?? ""));
    if (byImplementation) return byImplementation;
    const byStorage = String(a.storage ?? "").localeCompare(String(b.storage ?? ""));
    if (byStorage) return byStorage;
    const byWorkload = String(a.workload ?? a.name ?? "").localeCompare(String(b.workload ?? b.name ?? ""));
    if (byWorkload) return byWorkload;
    const byMode = String(a.extra?.mode ?? "").localeCompare(String(b.extra?.mode ?? ""));
    if (byMode) return byMode;
    return String(a.relativePath ?? "").localeCompare(String(b.relativePath ?? ""));
  });
  await fs.mkdir(benchRoot, { recursive: true });
  const summaryJson = path.join(benchRoot, "summary.json");
  const summaryMd = path.join(benchRoot, "summary.md");
  await fs.writeFile(summaryJson, JSON.stringify(rows, null, 2), "utf-8");
  await fs.writeFile(summaryMd, toMarkdown(rows), "utf-8");
  console.log(`Wrote ${rows.length} entries to ${path.relative(repoRoot, summaryJson)} and summary.md`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});

#!/usr/bin/env node
/**
 * Simulate CI benchmark comparison locally: run benchmarks twice (no code change),
 * then compare the two runs and print the same table as in .github/workflows/benchmarks.yml.
 *
 * Usage: node scripts/bench-compare-local.mjs
 *        (run from repo root, or script will chdir to repo root)
 *
 * Requires: pnpm, Node 20.x, and the same env as CI for full parity (Rust, wasm targets,
 * Emscripten, Playwright chromium for wa-sqlite e2e). Without them some benchmark steps may fail.
 */

import { execSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(__dirname, "..");

const BENCH_BASE = path.join(repoRoot, "bench-base", "benchmarks");
const BENCH_HEAD = path.join(repoRoot, "bench-head", "benchmarks");
const SUMMARY_JSON = "summary.json";

function rmrf(dir) {
  if (fs.existsSync(dir)) {
    fs.rmSync(dir, { recursive: true });
  }
}

function copyDir(src, dest) {
  fs.mkdirSync(path.dirname(dest), { recursive: true });
  if (fs.existsSync(dest)) fs.rmSync(dest, { recursive: true });
  fs.cpSync(src, dest, { recursive: true });
}

function runBenchmark() {
  execSync("pnpm run benchmark", {
    cwd: repoRoot,
    stdio: "inherit",
    env: { ...process.env, CI: "true" },
  });
}

function main() {
  process.chdir(repoRoot);

  console.log("=== Run 1 (base) ===\n");
  runBenchmark();
  const benchmarksDir = path.join(repoRoot, "benchmarks");
  if (!fs.existsSync(path.join(benchmarksDir, SUMMARY_JSON))) {
    console.error("Run 1: benchmarks/summary.json not found. Check benchmark output above.");
    process.exit(1);
  }
  copyDir(benchmarksDir, BENCH_BASE);
  console.log("\nSaved Run 1 to bench-base/benchmarks/\n");

  console.log("=== Run 2 (head) ===\n");
  runBenchmark();
  if (!fs.existsSync(path.join(benchmarksDir, SUMMARY_JSON))) {
    console.error("Run 2: benchmarks/summary.json not found. Check benchmark output above.");
    process.exit(1);
  }
  copyDir(benchmarksDir, BENCH_HEAD);
  console.log("\nSaved Run 2 to bench-head/benchmarks/\n");

  console.log("=== Comparison (same table as CI) ===\n");
  const reportScript = path.join(__dirname, "bench-report.mjs");
  execSync(
    `node "${reportScript}" "${path.join(BENCH_BASE, SUMMARY_JSON)}" "${path.join(BENCH_HEAD, SUMMARY_JSON)}" "Run 1" "Run 2"`,
    { cwd: repoRoot, stdio: "inherit" }
  );
}

main();

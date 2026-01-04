import { test, expect } from "@playwright/test";
import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { envIntList, writeResult } from "@treecrdt/benchmark";
import type { SyncBenchResult } from "../src/sync.js";

test("wa-sqlite sync OPFS benchmarks", async ({ page }) => {
  test.setTimeout(600_000);
  page.on("console", (msg) => console.log(`[page][${msg.type()}] ${msg.text()}`));
  await page.goto("/");
  await page.waitForFunction(() => typeof window.runTreecrdtSyncBench === "function");

  const rootChildrenSizes = envIntList("SYNC_BENCH_ROOT_CHILDREN_SIZES") ?? [1110];
  const sizes = envIntList("SYNC_BENCH_SIZES") ?? [100, 1000, 10_000];
  const results = await page.evaluate(async ({ rootChildrenSizes, sizes }) => {
    const runner = window.runTreecrdtSyncBench;
    if (!runner) throw new Error("runTreecrdtSyncBench not available");
    const base = await runner("browser-opfs-coop-sync", sizes, ["sync-all", "sync-children", "sync-one-missing"]);
    const rootChildren = await runner("browser-opfs-coop-sync", rootChildrenSizes, ["sync-root-children-fanout10"]);
    return [...base, ...rootChildren];
  }, { rootChildrenSizes, sizes });

  expect(Array.isArray(results)).toBeTruthy();
  const __dirname = path.dirname(fileURLToPath(import.meta.url));
  const repoRoot = path.resolve(__dirname, "../../../..");
  const outDir = path.join(repoRoot, "benchmarks", "wa-sqlite-sync-opfs");
  await fs.mkdir(outDir, { recursive: true });

  for (const result of results as SyncBenchResult[]) {
    const workloadName = result.workload ?? result.name;
    const outFile = path.join(outDir, `${workloadName}.json`);
    const payload = await writeResult(result, {
      implementation: result.implementation,
      storage: result.storage,
      workload: workloadName,
      outFile,
      extra: { count: result.totalOps },
    });
    console.log(JSON.stringify(payload));
  }
});

test("wa-sqlite sync memory (browser) benchmarks", async ({ page }) => {
  test.setTimeout(600_000);
  page.on("console", (msg) => console.log(`[page][${msg.type()}] ${msg.text()}`));
  await page.goto("/");
  await page.waitForFunction(() => typeof window.runTreecrdtSyncBench === "function");

  const rootChildrenSizes = envIntList("SYNC_BENCH_ROOT_CHILDREN_SIZES") ?? [1110];
  const sizes = envIntList("SYNC_BENCH_SIZES") ?? [100, 1000, 10_000];
  const results = await page.evaluate(async ({ rootChildrenSizes, sizes }) => {
    const runner = window.runTreecrdtSyncBench;
    if (!runner) throw new Error("runTreecrdtSyncBench not available");
    const base = await runner("browser-memory", sizes, ["sync-all", "sync-children", "sync-one-missing"]);
    const rootChildren = await runner("browser-memory", rootChildrenSizes, ["sync-root-children-fanout10"]);
    return [...base, ...rootChildren];
  }, { rootChildrenSizes, sizes });

  expect(Array.isArray(results)).toBeTruthy();
  const __dirname = path.dirname(fileURLToPath(import.meta.url));
  const repoRoot = path.resolve(__dirname, "../../../..");
  const outDir = path.join(repoRoot, "benchmarks", "wa-sqlite-sync-browser-memory");
  await fs.mkdir(outDir, { recursive: true });

  for (const result of results as SyncBenchResult[]) {
    const workloadName = result.workload ?? result.name;
    const outFile = path.join(outDir, `${workloadName}.json`);
    const payload = await writeResult(result, {
      implementation: result.implementation,
      storage: result.storage,
      workload: workloadName,
      outFile,
      extra: { count: result.totalOps },
    });
    console.log(JSON.stringify(payload));
  }
});

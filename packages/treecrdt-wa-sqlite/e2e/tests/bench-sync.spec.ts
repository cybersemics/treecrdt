import { test, expect } from "@playwright/test";
import path from "node:path";
import {
  DEFAULT_SYNC_BENCH_ROOT_CHILDREN_WORKLOADS,
  DEFAULT_SYNC_BENCH_WORKLOADS,
  syncBenchRootChildrenSizesFromEnv,
  syncBenchSizesFromEnv,
} from "@treecrdt/benchmark";
import { repoRootFromImportMeta, writeResult } from "@treecrdt/benchmark/node";
import type { SyncBenchResult } from "../src/sync.js";

test("wa-sqlite sync OPFS benchmarks", async ({ page }) => {
  test.setTimeout(600_000);
  page.on("console", (msg) => console.log(`[page][${msg.type()}] ${msg.text()}`));
  await page.goto("/");
  await page.waitForFunction(() => typeof window.runTreecrdtSyncBench === "function");

  const rootChildrenSizes = syncBenchRootChildrenSizesFromEnv();
  const sizes = syncBenchSizesFromEnv();
  const baseWorkloads = Array.from(DEFAULT_SYNC_BENCH_WORKLOADS);
  const rootChildrenWorkloads = Array.from(DEFAULT_SYNC_BENCH_ROOT_CHILDREN_WORKLOADS);

  const results = await page.evaluate(async ({ rootChildrenSizes, sizes, baseWorkloads, rootChildrenWorkloads }) => {
    const runner = window.runTreecrdtSyncBench;
    if (!runner) throw new Error("runTreecrdtSyncBench not available");
    const base = await runner("browser-opfs-coop-sync", sizes, baseWorkloads);
    const rootChildren = await runner("browser-opfs-coop-sync", rootChildrenSizes, rootChildrenWorkloads);
    return [...base, ...rootChildren];
  }, { rootChildrenSizes, sizes, baseWorkloads, rootChildrenWorkloads });

  expect(Array.isArray(results)).toBeTruthy();
  const repoRoot = repoRootFromImportMeta(import.meta.url, 4);
  const outDir = path.join(repoRoot, "benchmarks", "wa-sqlite-sync-opfs");

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

  const rootChildrenSizes = syncBenchRootChildrenSizesFromEnv();
  const sizes = syncBenchSizesFromEnv();
  const baseWorkloads = Array.from(DEFAULT_SYNC_BENCH_WORKLOADS);
  const rootChildrenWorkloads = Array.from(DEFAULT_SYNC_BENCH_ROOT_CHILDREN_WORKLOADS);

  const results = await page.evaluate(async ({ rootChildrenSizes, sizes, baseWorkloads, rootChildrenWorkloads }) => {
    const runner = window.runTreecrdtSyncBench;
    if (!runner) throw new Error("runTreecrdtSyncBench not available");
    const base = await runner("browser-memory", sizes, baseWorkloads);
    const rootChildren = await runner("browser-memory", rootChildrenSizes, rootChildrenWorkloads);
    return [...base, ...rootChildren];
  }, { rootChildrenSizes, sizes, baseWorkloads, rootChildrenWorkloads });

  expect(Array.isArray(results)).toBeTruthy();
  const repoRoot = repoRootFromImportMeta(import.meta.url, 4);
  const outDir = path.join(repoRoot, "benchmarks", "wa-sqlite-sync-browser-memory");

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

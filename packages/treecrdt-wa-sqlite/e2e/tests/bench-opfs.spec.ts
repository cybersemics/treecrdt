import { test, expect } from '@playwright/test';
import path from 'node:path';
import { repoRootFromImportMeta, writeResult } from '@treecrdt/benchmark/node';
import type { BenchResult } from '../src/bench.js';

test('wa-sqlite OPFS benchmarks', async ({ page }) => {
  test.setTimeout(180_000);
  page.on('console', (msg) => console.log(`[page][${msg.type()}] ${msg.text()}`));
  await page.goto('/');
  await page.waitForFunction(() => typeof window.runWaSqliteBench === 'function');

  const results = await page.evaluate(async () => {
    const runner = window.runWaSqliteBench;
    if (!runner) throw new Error('runWaSqliteOpfsBench not available');
    return await runner('browser-opfs-coop-sync');
  });

  expect(Array.isArray(results)).toBeTruthy();
  const repoRoot = repoRootFromImportMeta(import.meta.url, 4);
  const outDir = path.join(repoRoot, 'benchmarks', 'wa-sqlite-opfs');

  for (const result of results as BenchResult[]) {
    const workloadName = result.workload ?? result.name;
    const outFile = path.join(outDir, `${workloadName}.json`);
    const payload = await writeResult(result, {
      implementation: result.implementation,
      storage: result.storage,
      workload: workloadName,
      outFile,
      extra: { count: result.extra?.count ?? result.totalOps },
    });
    console.log(JSON.stringify(payload));
  }
});

test('wa-sqlite memory (browser) benchmarks', async ({ page }) => {
  test.setTimeout(180_000);
  page.on('console', (msg) => console.log(`[page][${msg.type()}] ${msg.text()}`));
  await page.goto('/');
  await page.waitForFunction(() => typeof window.runWaSqliteBench === 'function');

  const results = await page.evaluate(async () => {
    const runner = window.runWaSqliteBench;
    if (!runner) throw new Error('runWaSqliteBench not available');
    return await runner('browser-memory');
  });

  expect(Array.isArray(results)).toBeTruthy();
  const repoRoot = repoRootFromImportMeta(import.meta.url, 4);
  const outDir = path.join(repoRoot, 'benchmarks', 'wa-sqlite-browser-memory');

  for (const result of results as BenchResult[]) {
    const workloadName = result.workload ?? result.name;
    const outFile = path.join(outDir, `${workloadName}.json`);
    const payload = await writeResult(result, {
      implementation: result.implementation,
      storage: result.storage,
      workload: workloadName,
      outFile,
      extra: { count: result.extra?.count ?? result.totalOps },
    });
    console.log(JSON.stringify(payload));
  }
});

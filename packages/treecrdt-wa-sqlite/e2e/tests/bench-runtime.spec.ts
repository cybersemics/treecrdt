import { test, expect } from '@playwright/test';
import path from 'node:path';
import { repoRootFromImportMeta, writeResult } from '@treecrdt/benchmark/node';
import type { BenchmarkResult } from '@treecrdt/benchmark';

type RuntimeChoice = 'direct' | 'dedicated-worker' | 'shared-worker';
type StorageChoice = 'memory' | 'opfs';
type RemoteIngestChoice = 'append-many' | 'sync-peer';
type RuntimeScenario = {
  id: string;
  runtime: RuntimeChoice;
  storage: StorageChoice;
};

const defaultScenarios: RuntimeScenario[] = [
  { id: 'memory-direct', runtime: 'direct', storage: 'memory' },
  { id: 'memory-worker', runtime: 'dedicated-worker', storage: 'memory' },
  { id: 'opfs-worker', runtime: 'dedicated-worker', storage: 'opfs' },
  { id: 'opfs-shared', runtime: 'shared-worker', storage: 'opfs' },
];

const remoteOps = Number(process.env.TREECRDT_RUNTIME_BENCH_REMOTE_OPS ?? 2_000);
const remoteBatchSize = Number(process.env.TREECRDT_RUNTIME_BENCH_REMOTE_BATCH_SIZE ?? 500);
const remoteIngest = envRemoteIngest('TREECRDT_RUNTIME_BENCH_REMOTE_INGEST', 'sync-peer');
const localWrites = Number(process.env.TREECRDT_RUNTIME_BENCH_LOCAL_WRITES ?? 20);
const readSamples = Number(process.env.TREECRDT_RUNTIME_BENCH_READ_SAMPLES ?? 20);
const readIntervalMs = Number(process.env.TREECRDT_RUNTIME_BENCH_READ_INTERVAL_MS ?? 0);
const localWriteIntervalMs = Number(
  process.env.TREECRDT_RUNTIME_BENCH_LOCAL_WRITE_INTERVAL_MS ?? 5,
);
const yieldBetweenRemoteBatchesMs = Number(
  process.env.TREECRDT_RUNTIME_BENCH_REMOTE_BATCH_YIELD_MS ?? 1,
);

function envNumberList(name: string, fallback: number[]): number[] {
  const raw = process.env[name];
  if (!raw) return fallback;
  const values = raw
    .split(',')
    .map((part) => Number(part.trim()))
    .filter((value) => Number.isFinite(value));
  return values.length > 0 ? values : fallback;
}

function envRemoteIngest(name: string, fallback: RemoteIngestChoice): RemoteIngestChoice {
  const raw = process.env[name];
  if (raw === 'append-many' || raw === 'sync-peer') return raw;
  return fallback;
}

function envScenarioList(name: string, fallback: RuntimeScenario[]): RuntimeScenario[] {
  const raw = process.env[name];
  if (!raw) return fallback;
  const ids = raw
    .split(',')
    .map((part) => part.trim())
    .filter(Boolean);
  const selected = ids
    .map((id) => fallback.find((scenario) => scenario.id === id))
    .filter((scenario): scenario is RuntimeScenario => !!scenario);
  return selected.length > 0 ? selected : fallback;
}

const prefillSizes = envNumberList('TREECRDT_RUNTIME_BENCH_PREFILL_OPS', [0, 5_000]);
const scenarios = envScenarioList('TREECRDT_RUNTIME_BENCH_SCENARIOS', defaultScenarios);

test.skip(!!process.env.CI, 'Raw browser benchmarks run in the benchmark workflow.');

test('wa-sqlite runtime/storage mixed sync-ingest/local-write benchmarks', async ({ page }) => {
  test.setTimeout(600_000);
  page.on('console', (msg) => console.log(`[page][${msg.type()}] ${msg.text()}`));
  await page.goto('/');
  await page.waitForFunction(
    () => typeof window.__runTreecrdtRuntimeMixedWriteBench === 'function',
  );

  const repoRoot = repoRootFromImportMeta(import.meta.url, 4);
  const outDir = path.join(repoRoot, 'benchmarks', 'wa-sqlite-runtime');

  for (const scenario of scenarios) {
    for (const prefillOps of prefillSizes) {
      const suffix = `${Date.now().toString(36)}-${Math.random().toString(16).slice(2, 8)}`;
      const result = await page.evaluate(
        async (opts) => {
          const runner = window.__runTreecrdtRuntimeMixedWriteBench;
          if (!runner) throw new Error('__runTreecrdtRuntimeMixedWriteBench not available');
          return await runner(opts);
        },
        {
          runtime: scenario.runtime,
          storage: scenario.storage,
          docId: `runtime-mixed-${scenario.id}-${prefillOps}-${suffix}`,
          filename: `/runtime-mixed-${scenario.id}-${prefillOps}-${suffix}.db`,
          remoteIngest,
          prefillOps,
          remoteOps,
          remoteBatchSize,
          localWrites,
          readSamples,
          readIntervalMs,
          localWriteIntervalMs,
          yieldBetweenRemoteBatchesMs,
        },
      );

      expect(result.totalOps).toBe(remoteOps + localWrites);
      expect(result.extra.runtime).toBe(scenario.runtime);
      expect(result.extra.storage).toBe(scenario.storage);
      expect(result.extra.remoteIngest).toBe(remoteIngest);
      expect(result.extra.finalChildCount).toBe(prefillOps + remoteOps + localWrites);
      expect(result.extra.readKind).toBe('childrenPage(root, first 50)');
      expect(result.extra.readPageLimit).toBe(50);
      expect(result.extra.remoteBatchDurationsMs.length).toBe(
        Math.ceil(remoteOps / remoteBatchSize),
      );
      expect(result.extra.localWriteDurationsMs.length).toBe(localWrites);
      expect(result.extra.readDurationsMs.length).toBe(readSamples);

      const outFile = path.join(outDir, `${scenario.id}-prefill-${prefillOps}.json`);
      const payload = await writeResult(result as BenchmarkResult, {
        implementation: 'wa-sqlite',
        storage: `browser-${scenario.storage}-${scenario.runtime}`,
        workload: result.name,
        outFile,
      });
      console.log(JSON.stringify(payload));
    }
  }
});

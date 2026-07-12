import { test, expect } from '@playwright/test';
import path from 'node:path';
import { repoRootFromImportMeta, writeResult } from '@treecrdt/benchmark/node';
import { envInt, envIntList } from '@treecrdt/benchmark';

type RuntimeChoice = 'direct' | 'dedicated-worker' | 'shared-worker';
type StorageChoice = 'memory' | 'opfs';
type RemoteIngestPriority = 'normal' | 'background';
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

const remoteOps = envInt('TREECRDT_RUNTIME_BENCH_REMOTE_OPS') ?? 2_000;
const remoteBatchSize = envInt('TREECRDT_RUNTIME_BENCH_REMOTE_BATCH_SIZE') ?? 500;
const localWrites = envInt('TREECRDT_RUNTIME_BENCH_LOCAL_WRITES') ?? 20;
const readSamples = envInt('TREECRDT_RUNTIME_BENCH_READ_SAMPLES') ?? 20;
const readIntervalMs = envInt('TREECRDT_RUNTIME_BENCH_READ_INTERVAL_MS') ?? 0;
const localWriteIntervalMs = envInt('TREECRDT_RUNTIME_BENCH_LOCAL_WRITE_INTERVAL_MS') ?? 5;
const yieldBetweenRemoteBatchesMs = envInt('TREECRDT_RUNTIME_BENCH_REMOTE_BATCH_YIELD_MS') ?? 1;
const defaultRemoteIngestPriorities: RemoteIngestPriority[] = ['normal', 'background'];

function envChoiceList<T>(name: string, fallback: readonly T[], id: (choice: T) => string): T[] {
  const raw = process.env[name];
  if (!raw) return [...fallback];
  const requested = raw
    .split(',')
    .map((part) => part.trim())
    .filter(Boolean);
  if (requested.length === 0) return [...fallback];
  const choices = new Map(fallback.map((choice) => [id(choice), choice]));
  return requested.map((value) => {
    const choice = choices.get(value);
    if (!choice) throw new Error(`Invalid ${name} value: ${value}`);
    return choice;
  });
}

const prefillSizes = envIntList('TREECRDT_RUNTIME_BENCH_PREFILL_OPS') ?? [0, 5_000];
const trials = envInt('TREECRDT_RUNTIME_BENCH_TRIALS') ?? 4;
const scenarios = envChoiceList(
  'TREECRDT_RUNTIME_BENCH_SCENARIOS',
  defaultScenarios,
  (scenario) => scenario.id,
);
const remoteIngestPriorities = envChoiceList(
  'TREECRDT_RUNTIME_BENCH_PRIORITIES',
  defaultRemoteIngestPriorities,
  (priority) => priority,
);

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

  for (const [scenarioIndex, scenario] of scenarios.entries()) {
    for (const prefillOps of prefillSizes) {
      for (let trial = 1; trial <= trials; trial += 1) {
        const suffix = `${Date.now().toString(36)}-${Math.random().toString(16).slice(2, 8)}`;
        const trialPriorities =
          trial % 2 === 1 ? remoteIngestPriorities : [...remoteIngestPriorities].reverse();
        for (const remoteIngestPriority of trialPriorities) {
          const result = await page.evaluate(
            async (opts) => {
              const runner = window.__runTreecrdtRuntimeMixedWriteBench;
              if (!runner) throw new Error('__runTreecrdtRuntimeMixedWriteBench not available');
              return await runner(opts);
            },
            {
              runtime: scenario.runtime,
              storage: scenario.storage,
              docId: `runtime-mixed-${scenario.id}-${remoteIngestPriority}-${prefillOps}-${suffix}`,
              filename: `/rt-${scenarioIndex}-${prefillOps}-${trial}-${remoteIngestPriority[0]}-${suffix}.db`,
              prefillOps,
              remoteOps,
              remoteBatchSize,
              remoteIngestPriority,
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
          expect(result.extra.remoteIngestPriority).toBe(remoteIngestPriority);
          expect(result.extra.finalChildCount).toBe(prefillOps + remoteOps + localWrites);
          expect(result.extra.remoteBatchDurationsMs.length).toBe(
            Math.ceil(remoteOps / remoteBatchSize),
          );
          expect(result.extra.localWriteDurationsMs.length).toBe(localWrites);
          expect(result.extra.readDurationsMs.length).toBe(readSamples);

          const outFile = path.join(
            outDir,
            `${scenario.id}-${remoteIngestPriority}-prefill-${prefillOps}-trial-${trial}.json`,
          );
          const payload = await writeResult(result, {
            implementation: 'wa-sqlite',
            storage: `browser-${scenario.storage}-${scenario.runtime}`,
            workload: `${result.name}-trial-${trial}`,
            outFile,
            extra: { trial },
          });
          console.log(JSON.stringify(payload));
        }
      }
    }
  }
});

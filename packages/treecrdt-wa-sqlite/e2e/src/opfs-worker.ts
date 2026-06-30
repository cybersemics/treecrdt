/// <reference lib="webworker" />

import {
  buildWorkloads,
  runWorkloads,
  type BenchmarkResult,
  type WorkloadName,
} from '@treecrdt/benchmark';
import { createWaSqliteBenchAdapter, type StorageKind } from './bench-adapter';

type WorkerRequest = {
  type: 'run';
  storage?: StorageKind;
  sizes?: number[];
  workloads?: WorkloadName[];
  baseUrl?: string;
};

type WorkerResponse = { ok: true; results: BenchPayload[] } | { ok: false; error: string };

type BenchPayload = BenchmarkResult & {
  implementation: string;
  storage: StorageKind;
  workload: string;
  extra?: Record<string, unknown>;
};

const defaultSizes = [100, 1_000];
const defaultWorkloads: WorkloadName[] = ['insert-move', 'insert-chain', 'replay-log'];

async function runWaSqliteBenchInWorker(
  storage: StorageKind,
  baseUrl: string | undefined,
  sizes: number[] = defaultSizes,
  workloads: WorkloadName[] = defaultWorkloads,
): Promise<BenchPayload[]> {
  const workloadDefs = buildWorkloads(workloads, sizes);
  const results: BenchPayload[] = [];

  for (const workload of workloadDefs) {
    console.info(`[opfs-worker] workload ${workload.name} start`);
    // Factory must return a NEW adapter each time: runBenchmark calls it per iteration and closes after each.
    const adapterFactory = () => createWaSqliteBenchAdapter(storage, baseUrl);
    const res = await runWorkloads(adapterFactory, [workload]);
    const [result] = res;
    const mergedExtra =
      result.extra && workload.totalOps
        ? { ...result.extra, count: workload.totalOps }
        : (result.extra ?? (workload.totalOps ? { count: workload.totalOps } : undefined));
    results.push({
      ...result,
      implementation: 'wa-sqlite',
      storage,
      workload: workload.name,
      extra: mergedExtra,
    });
    console.info(`[opfs-worker] workload ${workload.name} done`);
  }

  return results;
}

self.onmessage = async (ev: MessageEvent<WorkerRequest>) => {
  if (ev.data?.type !== 'run') return;
  const storage: StorageKind = ev.data.storage ?? 'browser-opfs-coop-sync';
  try {
    const results = await runWaSqliteBenchInWorker(
      storage,
      ev.data.baseUrl,
      ev.data.sizes,
      ev.data.workloads,
    );
    const response: WorkerResponse = { ok: true, results };
    self.postMessage(response);
  } catch (err) {
    console.error('[opfs-worker] bench failed', err);
    const response: WorkerResponse = {
      ok: false,
      error: err instanceof Error ? err.message : String(err),
    };
    self.postMessage(response);
  }
};

import type { WorkloadName } from '@treecrdt/benchmark';

export type BenchResult = {
  implementation: string;
  storage: string;
  workload: string;
  name: string;
  totalOps: number;
  durationMs: number;
  opsPerSec: number;
  extra?: Record<string, unknown>;
};

type StorageKind = 'browser-opfs-coop-sync' | 'browser-memory';

type WorkerRequest = {
  type: 'run';
  storage: StorageKind;
  sizes?: number[];
  workloads?: WorkloadName[];
  baseUrl?: string;
};

type WorkerResponse = { ok: true; results: BenchResult[] } | { ok: false; error: string };

const DEFAULT_STORAGE: StorageKind = 'browser-opfs-coop-sync';

export async function runWaSqliteBench(
  storage: StorageKind = DEFAULT_STORAGE,
  sizes?: number[],
  workloads?: WorkloadName[],
): Promise<BenchResult[]> {
  // Use absolute base URL so workers resolve wa-sqlite assets correctly.
  const baseUrl =
    typeof window !== 'undefined'
      ? new URL('/', window.location.href).href
      : typeof import.meta !== 'undefined' && (import.meta as any).env?.BASE_URL
        ? (import.meta as any).env.BASE_URL
        : '/';
  console.info(`[bench] starting run storage=${storage} baseUrl=${baseUrl}`);
  return new Promise((resolve, reject) => {
    const worker = new Worker(new URL('./opfs-worker.ts', import.meta.url), { type: 'module' });
    // Tiny workloads now run 5+ iterations (new adapter per iteration); OPFS create is slow, so allow more time.
    const timeout = setTimeout(() => {
      worker.terminate();
      reject(new Error('wa-sqlite bench worker timed out'));
    }, 300_000);

    worker.onmessage = (ev: MessageEvent<WorkerResponse>) => {
      clearTimeout(timeout);
      worker.terminate();
      if (ev.data?.ok && Array.isArray(ev.data.results)) {
        resolve(ev.data.results);
      } else {
        reject(new Error(ev.data?.error ?? 'Unknown worker error'));
      }
    };
    worker.onerror = (err) => {
      clearTimeout(timeout);
      worker.terminate();
      reject(err);
    };

    const message: WorkerRequest = { type: 'run', storage, sizes, workloads, baseUrl };
    worker.postMessage(message);
  });
}

declare global {
  interface Window {
    runWaSqliteBench?: typeof runWaSqliteBench;
  }
}

if (typeof window !== 'undefined') {
  window.runWaSqliteBench = runWaSqliteBench;
}

import { createTreecrdtClient, type TreecrdtClient } from '@treecrdt/wa-sqlite/client';
import { makeOp, nodeIdFromInt } from '@treecrdt/benchmark';
import { replicaFromLabel } from './op-helpers.js';

type RuntimeChoice = 'auto' | 'dedicated-worker' | 'shared-worker';

type OpenResponsivenessClientOptions = {
  docId: string;
  filename: string;
  runtime?: RuntimeChoice;
};

type WriteBatchOptions = {
  batchCount: number;
  batchSize: number;
  startNodeInt?: number;
  replicaLabel?: string;
  yieldBetweenBatchesMs?: number;
};

type WriteBatchResult = {
  ok: true;
  totalOps: number;
  batchCount: number;
  batchSize: number;
  durationMs: number;
  batchDurationsMs: number[];
};

type WriteBatchStatus =
  | { state: 'idle' }
  | {
      state: 'running';
      totalOps: number;
      batchesStarted: number;
      batchesCompleted: number;
      durationMs: number;
    }
  | ({ state: 'done' } & WriteBatchResult)
  | { state: 'error'; message: string };

type ReadSampleOptions = {
  parent?: string;
  samples: number;
  intervalMs?: number;
};

type ReadSampleResult = {
  ok: true;
  samples: number;
  durationsMs: number[];
  minMs: number;
  p50Ms: number;
  p95Ms: number;
  maxMs: number;
  finalChildCount: number;
};

let responsivenessClient: TreecrdtClient | null = null;
let writeStatus: WriteBatchStatus = { state: 'idle' };
let writePromise: Promise<WriteBatchResult> | null = null;

function rootNode(): string {
  return '0'.repeat(32);
}

function orderKeyFromOffset(offset: number): Uint8Array {
  const bytes = new Uint8Array(4);
  new DataView(bytes.buffer).setUint32(0, offset + 1, false);
  return bytes;
}

async function sleep(ms: number): Promise<void> {
  await new Promise<void>((resolve) => setTimeout(resolve, ms));
}

function quantile(values: number[], q: number): number {
  if (values.length === 0) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  const index = Math.min(sorted.length - 1, Math.max(0, Math.ceil(q * sorted.length) - 1));
  return sorted[index]!;
}

function summarizeDurations(durationsMs: number[]) {
  return {
    minMs: Math.min(...durationsMs),
    p50Ms: quantile(durationsMs, 0.5),
    p95Ms: quantile(durationsMs, 0.95),
    maxMs: Math.max(...durationsMs),
  };
}

function ensureClient(): TreecrdtClient {
  if (!responsivenessClient) throw new Error('responsiveness client is not open');
  return responsivenessClient;
}

export async function openResponsivenessClient(opts: OpenResponsivenessClientOptions): Promise<{
  mode: TreecrdtClient['mode'];
  runtime: TreecrdtClient['runtime'];
  storage: TreecrdtClient['storage'];
}> {
  await closeResponsivenessClient();
  responsivenessClient = await createTreecrdtClient({
    docId: opts.docId,
    storage: { type: 'opfs', filename: opts.filename, fallback: 'throw' },
    runtime: { type: opts.runtime ?? 'shared-worker' },
  });
  return {
    mode: responsivenessClient.mode,
    runtime: responsivenessClient.runtime,
    storage: responsivenessClient.storage,
  };
}

export async function startResponsivenessWriteBatches(
  opts: WriteBatchOptions,
): Promise<WriteBatchStatus> {
  const client = ensureClient();
  if (writeStatus.state === 'running') throw new Error('write pressure is already running');

  const batchCount = opts.batchCount;
  const batchSize = opts.batchSize;
  const totalOps = batchCount * batchSize;
  const startNodeInt = opts.startNodeInt ?? 1;
  const replica = replicaFromLabel(opts.replicaLabel ?? 'responsiveness-writer');
  const parent = rootNode();

  const run = async (): Promise<WriteBatchResult> => {
    const start = performance.now();
    const batchDurationsMs: number[] = [];
    writeStatus = {
      state: 'running',
      totalOps,
      batchesStarted: 0,
      batchesCompleted: 0,
      durationMs: 0,
    };

    try {
      for (let batchIndex = 0; batchIndex < batchCount; batchIndex += 1) {
        const batchStartCounter = batchIndex * batchSize + 1;
        const batch = Array.from({ length: batchSize }, (_, offset) => {
          const counter = batchStartCounter + offset;
          const nodeOffset = counter - 1;
          return makeOp(replica, counter, counter, {
            type: 'insert',
            parent,
            node: nodeIdFromInt(startNodeInt + nodeOffset),
            orderKey: orderKeyFromOffset(nodeOffset),
          });
        });

        const running = writeStatus.state === 'running' ? writeStatus : null;
        if (running) {
          writeStatus = {
            ...running,
            batchesStarted: batchIndex + 1,
            durationMs: performance.now() - start,
          };
        }

        const batchStart = performance.now();
        await client.ops.appendMany(batch);
        batchDurationsMs.push(performance.now() - batchStart);

        const runningAfterBatch = writeStatus.state === 'running' ? writeStatus : null;
        if (runningAfterBatch) {
          writeStatus = {
            ...runningAfterBatch,
            batchesCompleted: batchIndex + 1,
            durationMs: performance.now() - start,
          };
        }

        if (opts.yieldBetweenBatchesMs && opts.yieldBetweenBatchesMs > 0) {
          await sleep(opts.yieldBetweenBatchesMs);
        }
      }

      const result: WriteBatchResult = {
        ok: true,
        totalOps,
        batchCount,
        batchSize,
        durationMs: performance.now() - start,
        batchDurationsMs,
      };
      writeStatus = { state: 'done', ...result };
      return result;
    } catch (err) {
      writeStatus = { state: 'error', message: err instanceof Error ? err.message : String(err) };
      throw err;
    }
  };

  writePromise = run();
  await sleep(0);
  return writeStatus;
}

export function responsivenessWriteStatus(): WriteBatchStatus {
  return writeStatus;
}

export async function waitResponsivenessWrites(): Promise<WriteBatchResult> {
  if (!writePromise) throw new Error('write pressure was not started');
  return await writePromise;
}

export async function sampleResponsivenessReads(
  opts: ReadSampleOptions,
): Promise<ReadSampleResult> {
  const client = ensureClient();
  const parent = opts.parent ?? rootNode();
  const intervalMs = opts.intervalMs ?? 0;
  const durationsMs: number[] = [];
  let finalChildCount = 0;

  for (let i = 0; i < opts.samples; i += 1) {
    const start = performance.now();
    finalChildCount = (await client.tree.children(parent)).length;
    durationsMs.push(performance.now() - start);
    if (intervalMs > 0) await sleep(intervalMs);
  }

  return {
    ok: true,
    samples: opts.samples,
    durationsMs,
    ...summarizeDurations(durationsMs),
    finalChildCount,
  };
}

export async function closeResponsivenessClient(): Promise<void> {
  const client = responsivenessClient;
  responsivenessClient = null;
  writePromise = null;
  writeStatus = { state: 'idle' };
  if (client) await client.close();
}

declare global {
  interface Window {
    __openTreecrdtResponsivenessClient?: typeof openResponsivenessClient;
    __startTreecrdtResponsivenessWriteBatches?: typeof startResponsivenessWriteBatches;
    __treecrdtResponsivenessWriteStatus?: typeof responsivenessWriteStatus;
    __waitTreecrdtResponsivenessWrites?: typeof waitResponsivenessWrites;
    __sampleTreecrdtResponsivenessReads?: typeof sampleResponsivenessReads;
    __closeTreecrdtResponsivenessClient?: typeof closeResponsivenessClient;
  }
}

if (typeof window !== 'undefined') {
  window.__openTreecrdtResponsivenessClient = openResponsivenessClient;
  window.__startTreecrdtResponsivenessWriteBatches = startResponsivenessWriteBatches;
  window.__treecrdtResponsivenessWriteStatus = responsivenessWriteStatus;
  window.__waitTreecrdtResponsivenessWrites = waitResponsivenessWrites;
  window.__sampleTreecrdtResponsivenessReads = sampleResponsivenessReads;
  window.__closeTreecrdtResponsivenessClient = closeResponsivenessClient;
}

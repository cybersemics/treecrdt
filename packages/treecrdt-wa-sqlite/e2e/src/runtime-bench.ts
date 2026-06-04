import { createTreecrdtClient, type TreecrdtClient } from '@treecrdt/wa-sqlite';
import { makeOp, nodeIdFromInt, type BenchmarkResult } from '@treecrdt/benchmark';
import type { Operation } from '@treecrdt/interface';
import { replicaFromLabel } from './op-helpers.js';

type RuntimeChoice = 'direct' | 'dedicated-worker' | 'shared-worker';
type StorageChoice = 'memory' | 'opfs';

type RuntimeMixedWriteBenchOptions = {
  runtime: RuntimeChoice;
  storage?: StorageChoice;
  docId?: string;
  filename?: string;
  prefillOps?: number;
  remoteOps?: number;
  remoteBatchSize?: number;
  localWrites?: number;
  readSamples?: number;
  readIntervalMs?: number;
  localWriteIntervalMs?: number;
  yieldBetweenRemoteBatchesMs?: number;
};

type RuntimeMixedWriteBenchResult = BenchmarkResult & {
  extra: {
    runtime: RuntimeChoice;
    storage: StorageChoice;
    prefillOps: number;
    remoteOps: number;
    remoteBatchSize: number;
    remoteBatchCount: number;
    remoteBatchDurationsMs: number[];
    remoteBatchMinMs: number;
    remoteBatchP50Ms: number;
    remoteBatchP95Ms: number;
    remoteBatchMaxMs: number;
    localWrites: number;
    localWriteDurationsMs: number[];
    localWriteMinMs: number;
    localWriteP50Ms: number;
    localWriteP95Ms: number;
    localWriteMaxMs: number;
    readSamples: number;
    readDurationsMs: number[];
    readMinMs: number;
    readP50Ms: number;
    readP95Ms: number;
    readMaxMs: number;
    localWriteIntervalMs: number;
    readIntervalMs: number;
    yieldBetweenRemoteBatchesMs: number;
    expectedChildCount: number;
    finalChildCount: number;
  };
};

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

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function quantile(values: number[], q: number): number {
  if (values.length === 0) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  const index = Math.min(sorted.length - 1, Math.max(0, Math.ceil(q * sorted.length) - 1));
  return sorted[index]!;
}

function summarizeDurations(durationsMs: number[]) {
  if (durationsMs.length === 0) {
    return {
      minMs: 0,
      p50Ms: 0,
      p95Ms: 0,
      maxMs: 0,
    };
  }
  return {
    minMs: Math.min(...durationsMs),
    p50Ms: quantile(durationsMs, 0.5),
    p95Ms: quantile(durationsMs, 0.95),
    maxMs: Math.max(...durationsMs),
  };
}

function makeInsertOps(opts: {
  replica: Uint8Array;
  count: number;
  startCounter: number;
  startLamport?: number;
  startNodeInt: number;
  startOrderOffset?: number;
}): Operation[] {
  return Array.from({ length: opts.count }, (_, offset) => {
    const counter = opts.startCounter + offset;
    const lamport = (opts.startLamport ?? opts.startCounter) + offset;
    const orderOffset = (opts.startOrderOffset ?? opts.startNodeInt) + offset;
    return makeOp(opts.replica, counter, lamport, {
      type: 'insert',
      parent: rootNode(),
      node: nodeIdFromInt(opts.startNodeInt + offset),
      orderKey: orderKeyFromOffset(orderOffset),
    });
  });
}

export async function runRuntimeMixedWriteBench(
  opts: RuntimeMixedWriteBenchOptions,
): Promise<RuntimeMixedWriteBenchResult> {
  const runtime = opts.runtime;
  const storage = opts.storage ?? 'opfs';
  const prefillOps = opts.prefillOps ?? 0;
  const remoteOps = opts.remoteOps ?? 2_000;
  const remoteBatchSize = opts.remoteBatchSize ?? 500;
  const localWrites = opts.localWrites ?? 20;
  const readSamples = opts.readSamples ?? 20;
  const readIntervalMs = opts.readIntervalMs ?? 0;
  const localWriteIntervalMs = opts.localWriteIntervalMs ?? 5;
  const yieldBetweenRemoteBatchesMs = opts.yieldBetweenRemoteBatchesMs ?? 1;
  const remoteBatchCount = Math.ceil(remoteOps / remoteBatchSize);
  const docId = opts.docId ?? `runtime-mixed-${storage}-${runtime}-${crypto.randomUUID()}`;
  const filename =
    opts.filename ?? `/runtime-mixed-${storage}-${runtime}-${crypto.randomUUID()}.db`;
  const remoteReplica = replicaFromLabel(`runtime-remote-${storage}-${runtime}`);
  const localReplica = replicaFromLabel(`runtime-local-${storage}-${runtime}`);
  const expectedChildCount = prefillOps + remoteOps + localWrites;
  const client = await createTreecrdtClient({
    docId,
    storage:
      storage === 'opfs' ? { type: 'opfs', filename, fallback: 'throw' } : { type: 'memory' },
    runtime: { type: runtime },
  });

  try {
    if (prefillOps > 0) {
      try {
        await client.ops.appendMany(
          makeInsertOps({
            replica: replicaFromLabel(`runtime-prefill-${storage}-${runtime}`),
            count: prefillOps,
            startCounter: 1,
            startNodeInt: 1,
            startOrderOffset: 1,
          }),
        );
      } catch (error) {
        throw new Error(`runtime mixed benchmark prefill failed: ${errorMessage(error)}`);
      }
    }

    const remoteBatchDurationsMs: number[] = [];
    const localWriteDurationsMs: number[] = [];
    const readDurationsMs: number[] = [];
    let finalChildCount = 0;
    const start = performance.now();

    const runRemoteIngest = async () => {
      for (let batchIndex = 0; batchIndex < remoteBatchCount; batchIndex += 1) {
        const batchStartCounter = batchIndex * remoteBatchSize + 1;
        const remaining = remoteOps - batchIndex * remoteBatchSize;
        const batchOps = Math.min(remoteBatchSize, remaining);
        const batch = makeInsertOps({
          replica: remoteReplica,
          count: batchOps,
          startCounter: batchStartCounter,
          startLamport: prefillOps + batchStartCounter,
          startNodeInt: 100_000 + batchIndex * remoteBatchSize,
          startOrderOffset: 100_000 + batchIndex * remoteBatchSize,
        });

        const batchStart = performance.now();
        try {
          await client.ops.appendMany(batch);
        } catch (error) {
          throw new Error(`runtime mixed benchmark remote ingest failed: ${errorMessage(error)}`);
        }
        remoteBatchDurationsMs.push(performance.now() - batchStart);

        if (batchIndex < remoteBatchCount - 1 && yieldBetweenRemoteBatchesMs >= 0) {
          await sleep(yieldBetweenRemoteBatchesMs);
        }
      }
    };

    const runLocalWrites = async () => {
      for (let i = 0; i < localWrites; i += 1) {
        if (i > 0 && localWriteIntervalMs > 0) await sleep(localWriteIntervalMs);
        const writeStart = performance.now();
        try {
          await client.local.insert(
            localReplica,
            rootNode(),
            nodeIdFromInt(200_000 + i),
            { type: 'last' },
            null,
          );
        } catch (error) {
          throw new Error(`runtime mixed benchmark local write failed: ${errorMessage(error)}`);
        }
        localWriteDurationsMs.push(performance.now() - writeStart);
      }
    };

    const runReads = async () => {
      for (let i = 0; i < readSamples; i += 1) {
        if (i > 0 && readIntervalMs > 0) await sleep(readIntervalMs);
        const readStart = performance.now();
        try {
          finalChildCount = (await client.tree.children(rootNode())).length;
        } catch (error) {
          throw new Error(`runtime mixed benchmark read sample failed: ${errorMessage(error)}`);
        }
        readDurationsMs.push(performance.now() - readStart);
      }
    };

    await Promise.all([runRemoteIngest(), runLocalWrites(), runReads()]);
    finalChildCount = (await client.tree.children(rootNode())).length;
    if (finalChildCount !== expectedChildCount) {
      throw new Error(
        `runtime mixed benchmark child count mismatch: expected ${expectedChildCount}, got ${finalChildCount}`,
      );
    }

    const durationMs = performance.now() - start;
    const totalOps = remoteOps + localWrites;
    const remoteBatchSummary = summarizeDurations(remoteBatchDurationsMs);
    const localWriteSummary = summarizeDurations(localWriteDurationsMs);
    const readSummary = summarizeDurations(readDurationsMs);

    return {
      name: `runtime-mixed-sync-ingest-local-writes-${storage}-${runtime}-prefill-${prefillOps}`,
      totalOps,
      durationMs,
      opsPerSec: durationMs > 0 ? (totalOps / durationMs) * 1000 : Infinity,
      extra: {
        runtime,
        storage,
        prefillOps,
        remoteOps,
        remoteBatchSize,
        remoteBatchCount,
        remoteBatchDurationsMs,
        remoteBatchMinMs: remoteBatchSummary.minMs,
        remoteBatchP50Ms: remoteBatchSummary.p50Ms,
        remoteBatchP95Ms: remoteBatchSummary.p95Ms,
        remoteBatchMaxMs: remoteBatchSummary.maxMs,
        localWrites,
        localWriteDurationsMs,
        localWriteMinMs: localWriteSummary.minMs,
        localWriteP50Ms: localWriteSummary.p50Ms,
        localWriteP95Ms: localWriteSummary.p95Ms,
        localWriteMaxMs: localWriteSummary.maxMs,
        readSamples,
        readDurationsMs,
        readMinMs: readSummary.minMs,
        readP50Ms: readSummary.p50Ms,
        readP95Ms: readSummary.p95Ms,
        readMaxMs: readSummary.maxMs,
        localWriteIntervalMs,
        readIntervalMs,
        yieldBetweenRemoteBatchesMs,
        expectedChildCount,
        finalChildCount,
      },
    };
  } finally {
    await client.close();
  }
}

declare global {
  interface Window {
    __runTreecrdtRuntimeMixedWriteBench?: typeof runRuntimeMixedWriteBench;
  }
}

if (typeof window !== 'undefined') {
  window.__runTreecrdtRuntimeMixedWriteBench = runRuntimeMixedWriteBench;
}

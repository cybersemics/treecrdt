import { createTreecrdtClient, type TreecrdtClient } from '@treecrdt/wa-sqlite';
import { makeOp, nodeIdFromInt, type BenchmarkResult } from '@treecrdt/benchmark';
import type { Operation } from '@treecrdt/interface';
import { bytesToHex } from '@treecrdt/interface/ids';
import { SyncPeer, type SyncBackend } from '@treecrdt/sync-protocol';
import { treecrdtSyncV0ProtobufCodec } from '@treecrdt/sync-protocol/protobuf';
import {
  createInMemoryDuplex,
  wrapDuplexTransportWithCodec,
} from '@treecrdt/sync-protocol/transport';
import { replicaFromLabel } from './op-helpers.js';

type RuntimeChoice = 'direct' | 'dedicated-worker' | 'shared-worker';
type StorageChoice = 'memory' | 'opfs';
type RemoteIngestChoice = 'append-many' | 'sync-peer';
const READ_PAGE_LIMIT = 50;
const READ_KIND = 'childrenPage(root, first 50)';

type RuntimeMixedWriteBenchOptions = {
  runtime: RuntimeChoice;
  storage?: StorageChoice;
  remoteIngest?: RemoteIngestChoice;
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
    remoteIngest: RemoteIngestChoice;
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
    readKind: typeof READ_KIND;
    readPageLimit: number;
    readDurationsMs: number[];
    readMinMs: number;
    readP50Ms: number;
    readP95Ms: number;
    readMaxMs: number;
    interBatchReadMs: number | null;
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

function deferredPromise<T = void>() {
  let resolve!: (value: T | PromiseLike<T>) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
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
  const remoteIngest = opts.remoteIngest ?? 'sync-peer';
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
    let interBatchReadMs: number | null = null;
    let interBatchReadDone: Promise<void> | null = null;
    let finalChildCount = 0;
    const start = performance.now();
    const runMeasuredRead = async () => {
      await client.tree.childrenPage(rootNode(), null, READ_PAGE_LIMIT);
    };

    const runAppendManyRemoteIngest = async () => {
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
          await client.ops.appendMany(batch, { priority: 'background' });
        } catch (error) {
          throw new Error(`runtime mixed benchmark remote ingest failed: ${errorMessage(error)}`);
        }
        remoteBatchDurationsMs.push(performance.now() - batchStart);

        if (batchIndex < remoteBatchCount - 1 && yieldBetweenRemoteBatchesMs >= 0) {
          await sleep(yieldBetweenRemoteBatchesMs);
        }
      }
    };

    const runSyncPeerRemoteIngest = async () => {
      if (remoteOps === 0) return;

      const remoteApplied = deferredPromise();
      let appliedRemoteOps = 0;
      const receiverBackend: SyncBackend<Operation> = {
        docId,
        maxLamport: async () => BigInt(await client.meta.headLamport()),
        listOpRefs: async (filter) => {
          if ('all' in filter) return client.opRefs.all();
          return client.opRefs.children(bytesToHex(filter.children.parent));
        },
        getOpsByOpRefs: async (opRefs) => client.ops.get(opRefs),
        applyOps: async (ops) => {
          if (ops.length === 0) return;

          const batchStart = performance.now();
          try {
            await client.ops.appendMany(ops, { priority: 'background' });
          } catch (error) {
            remoteApplied.reject(error);
            throw error;
          }

          remoteBatchDurationsMs.push(performance.now() - batchStart);
          const nextAppliedRemoteOps = appliedRemoteOps + ops.length;
          if (appliedRemoteOps === 0 && nextAppliedRemoteOps < remoteOps) {
            interBatchReadDone = new Promise<void>((resolve, reject) => {
              setTimeout(() => {
                const readStart = performance.now();
                runMeasuredRead().then(() => {
                  interBatchReadMs = performance.now() - readStart;
                  resolve();
                }, reject);
              }, 0);
            });
          }
          appliedRemoteOps = nextAppliedRemoteOps;
          if (appliedRemoteOps >= remoteOps) remoteApplied.resolve();
        },
      };
      const senderBackend: SyncBackend<Operation> = {
        docId,
        maxLamport: async () => 0n,
        listOpRefs: async () => [],
        getOpsByOpRefs: async () => [],
        applyOps: async () => {},
      };
      const [wireA, wireB] = createInMemoryDuplex<Uint8Array>();
      const transportA = wrapDuplexTransportWithCodec(wireA, treecrdtSyncV0ProtobufCodec);
      const transportB = wrapDuplexTransportWithCodec(wireB, treecrdtSyncV0ProtobufCodec);
      const senderPeer = new SyncPeer(senderBackend, { maxOpsPerBatch: remoteBatchSize });
      const receiverPeer = new SyncPeer(receiverBackend, { maxOpsPerBatch: remoteBatchSize });
      const detachSender = senderPeer.attach(transportA);
      const detachReceiver = receiverPeer.attach(transportB);

      try {
        const ops = makeInsertOps({
          replica: remoteReplica,
          count: remoteOps,
          startCounter: 1,
          startLamport: prefillOps + 1,
          startNodeInt: 100_000,
          startOrderOffset: 100_000,
        });
        await senderPeer.pushOps(transportA, ops, {
          filterId: `runtime-sync-peer-${crypto.randomUUID()}`,
          maxOpsPerBatch: remoteBatchSize,
        });
        await remoteApplied.promise;
        if (interBatchReadDone) await interBatchReadDone;
      } catch (error) {
        throw new Error(
          `runtime mixed benchmark remote sync ingest failed: ${errorMessage(error)}`,
        );
      } finally {
        detachSender();
        detachReceiver();
      }
    };

    const runRemoteIngest =
      remoteIngest === 'sync-peer' ? runSyncPeerRemoteIngest : runAppendManyRemoteIngest;

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
          await runMeasuredRead();
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
      name: `runtime-mixed-${remoteIngest}-ingest-local-writes-${storage}-${runtime}-prefill-${prefillOps}`,
      totalOps,
      durationMs,
      opsPerSec: durationMs > 0 ? (totalOps / durationMs) * 1000 : Infinity,
      extra: {
        runtime,
        storage,
        remoteIngest,
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
        readKind: READ_KIND,
        readPageLimit: READ_PAGE_LIMIT,
        readDurationsMs,
        readMinMs: readSummary.minMs,
        readP50Ms: readSummary.p50Ms,
        readP95Ms: readSummary.p95Ms,
        readMaxMs: readSummary.maxMs,
        interBatchReadMs,
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

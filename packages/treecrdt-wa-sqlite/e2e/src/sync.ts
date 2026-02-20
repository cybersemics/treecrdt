import { createTreecrdtClient, type TreecrdtClient } from '@treecrdt/wa-sqlite/client';
import {
  buildFanoutInsertTreeOps,
  buildSyncBenchCase,
  makeOp,
  maxLamport,
  nodeIdFromInt,
  quantile,
  SYNC_BENCH_DEFAULT_CODEWORDS_PER_MESSAGE,
  SYNC_BENCH_DEFAULT_MAX_CODEWORDS,
  SYNC_BENCH_DEFAULT_SUBSCRIBE_CODEWORDS_PER_MESSAGE,
  syncBenchTiming,
  type SyncBenchWorkload,
} from '@treecrdt/benchmark';
import type { Operation } from '@treecrdt/interface';
import { bytesToHex, nodeIdToBytes16 } from '@treecrdt/interface/ids';
import {
  createInMemoryConnectedPeers,
  makeQueuedSyncBackend,
  type FlushableSyncBackend,
} from '@treecrdt/sync/in-memory';
import { treecrdtSyncV0ProtobufCodec } from '@treecrdt/sync/protobuf';
import type { Filter } from '@treecrdt/sync';

export type SyncBenchResult = {
  implementation: string;
  storage: string;
  workload: string;
  name: string;
  totalOps: number;
  durationMs: number;
  opsPerSec: number;
  extra?: Record<string, unknown>;
};

type StorageKind = 'browser-memory' | 'browser-opfs-coop-sync';

function hexToBytes(hex: string): Uint8Array {
  return nodeIdToBytes16(hex);
}

function replicaFromLabel(label: string): Uint8Array {
  const encoded = new TextEncoder().encode(label);
  if (encoded.length === 0) throw new Error('replica label must not be empty');
  const out = new Uint8Array(32);
  for (let i = 0; i < out.length; i += 1) out[i] = encoded[i % encoded.length]!;
  return out;
}

const replicas = {
  a: replicaFromLabel('a'),
  b: replicaFromLabel('b'),
  m: replicaFromLabel('m'),
};

function hasOp(ops: Operation[], replica: Uint8Array, counter: number): boolean {
  const targetHex = bytesToHex(replica);
  return ops.some(
    (op) => bytesToHex(op.meta.id.replica) === targetHex && op.meta.id.counter === counter,
  );
}

function orderKeyFromPosition(position: number): Uint8Array {
  if (!Number.isInteger(position) || position < 0) throw new Error(`invalid position: ${position}`);
  const n = position + 1;
  if (n > 0xffff) throw new Error(`position too large for u16 order key: ${position}`);
  const bytes = new Uint8Array(2);
  new DataView(bytes.buffer).setUint16(0, n, false);
  return bytes;
}

function makeBackend(
  client: TreecrdtClient,
  docId: string,
  initialMaxLamport: number,
): FlushableSyncBackend<Operation> {
  return makeQueuedSyncBackend<Operation>({
    docId,
    initialMaxLamport,
    maxLamportFromOps: maxLamport,
    listOpRefs: async (filter) => {
      if ('all' in filter) return client.opRefs.all();
      return client.opRefs.children(bytesToHex(filter.children.parent));
    },
    getOpsByOpRefs: async (opRefs) => client.ops.get(opRefs),
    applyOps: async (ops) => client.ops.appendMany(ops),
  });
}

async function runAllE2e(): Promise<void> {
  const docId = `e2e-sync-all-${crypto.randomUUID()}`;
  const a = await createTreecrdtClient({ storage: 'memory', docId });
  const b = await createTreecrdtClient({ storage: 'memory', docId });
  try {
    const root = '0'.repeat(32);
    const aOps = [
      makeOp(replicas.a, 1, 1, {
        type: 'insert',
        parent: root,
        node: nodeIdFromInt(1),
        orderKey: orderKeyFromPosition(0),
      }),
    ];
    const bOps = [
      makeOp(replicas.b, 1, 2, {
        type: 'insert',
        parent: root,
        node: nodeIdFromInt(2),
        orderKey: orderKeyFromPosition(0),
      }),
    ];
    await a.ops.appendMany(aOps);
    await b.ops.appendMany(bOps);

    const backendA = makeBackend(a, docId, maxLamport(aOps));
    const backendB = makeBackend(b, docId, maxLamport(bOps));
    const {
      peerA: pa,
      transportA: ta,
      detach,
    } = createInMemoryConnectedPeers({
      backendA,
      backendB,
      codec: treecrdtSyncV0ProtobufCodec,
      peerOptions: { maxCodewords: SYNC_BENCH_DEFAULT_MAX_CODEWORDS },
    });
    try {
      await pa.syncOnce(
        ta,
        { all: {} },
        {
          maxCodewords: SYNC_BENCH_DEFAULT_MAX_CODEWORDS,
          codewordsPerMessage: SYNC_BENCH_DEFAULT_CODEWORDS_PER_MESSAGE,
        },
      );
      await Promise.all([backendA.flush(), backendB.flush()]);
    } finally {
      detach();
    }

    const finalA = await a.ops.all();
    const finalB = await b.ops.all();
    if (finalA.length !== 2 || finalB.length !== 2) {
      throw new Error(
        `sync-all: expected both sides to have 2 ops, got a=${finalA.length} b=${finalB.length}`,
      );
    }
    if (!hasOp(finalA, replicas.a, 1) || !hasOp(finalA, replicas.b, 1))
      throw new Error('sync-all: A missing ops');
    if (!hasOp(finalB, replicas.a, 1) || !hasOp(finalB, replicas.b, 1))
      throw new Error('sync-all: B missing ops');
  } finally {
    await Promise.allSettled([a.close(), b.close()]);
  }
}

async function runChildrenE2e(): Promise<void> {
  const docId = `e2e-sync-children-${crypto.randomUUID()}`;
  const a = await createTreecrdtClient({ storage: 'memory', docId });
  const b = await createTreecrdtClient({ storage: 'memory', docId });
  try {
    const parentAHex = 'a0'.repeat(16);
    const parentBHex = 'b0'.repeat(16);
    const aOps = [
      makeOp(replicas.a, 1, 1, {
        type: 'insert',
        parent: parentAHex,
        node: nodeIdFromInt(1),
        orderKey: orderKeyFromPosition(0),
      }),
      makeOp(replicas.a, 2, 2, {
        type: 'insert',
        parent: parentBHex,
        node: nodeIdFromInt(2),
        orderKey: orderKeyFromPosition(0),
      }),
    ];
    const bOps = [
      makeOp(replicas.b, 1, 3, {
        type: 'insert',
        parent: parentAHex,
        node: nodeIdFromInt(3),
        orderKey: orderKeyFromPosition(0),
      }),
      makeOp(replicas.b, 2, 4, {
        type: 'insert',
        parent: parentBHex,
        node: nodeIdFromInt(4),
        orderKey: orderKeyFromPosition(0),
      }),
    ];
    await a.ops.appendMany(aOps);
    await b.ops.appendMany(bOps);

    const backendA = makeBackend(a, docId, maxLamport(aOps));
    const backendB = makeBackend(b, docId, maxLamport(bOps));

    const filter: Filter = { children: { parent: hexToBytes(parentAHex) } };
    const {
      peerA: pa,
      transportA: ta,
      detach,
    } = createInMemoryConnectedPeers({
      backendA,
      backendB,
      codec: treecrdtSyncV0ProtobufCodec,
      peerOptions: { maxCodewords: SYNC_BENCH_DEFAULT_MAX_CODEWORDS },
    });
    try {
      await pa.syncOnce(ta, filter, {
        maxCodewords: SYNC_BENCH_DEFAULT_MAX_CODEWORDS,
        codewordsPerMessage: SYNC_BENCH_DEFAULT_CODEWORDS_PER_MESSAGE,
      });
      await Promise.all([backendA.flush(), backendB.flush()]);
    } finally {
      detach();
    }

    const finalA = await a.ops.all();
    const finalB = await b.ops.all();
    if (!hasOp(finalA, replicas.b, 1)) throw new Error('sync-children: expected A to receive b:1');
    if (hasOp(finalA, replicas.b, 2)) throw new Error('sync-children: A should not receive b:2');
    if (!hasOp(finalB, replicas.a, 1)) throw new Error('sync-children: expected B to receive a:1');
    if (hasOp(finalB, replicas.a, 2)) throw new Error('sync-children: B should not receive a:2');
  } finally {
    await Promise.allSettled([a.close(), b.close()]);
  }
}

async function runLargeFanoutAllE2e(): Promise<void> {
  const size = 100_000;
  const fanout = 10;
  const maxCodewords = 2_000_000;
  const codewordsPerMessage = 4096;

  const docId = `e2e-sync-large-fanout${fanout}-${crypto.randomUUID()}`;
  const a = await createTreecrdtClient({ storage: 'memory', docId });
  const b = await createTreecrdtClient({ storage: 'memory', docId });

  try {
    const root = '0'.repeat(32);
    const opsA = buildFanoutInsertTreeOps({ replica: replicas.a, size, fanout, root });
    await a.ops.appendMany(opsA);

    const backendA = makeBackend(a, docId, maxLamport(opsA));
    const backendB = makeBackend(b, docId, 0);
    const {
      peerB: pb,
      transportB: tb,
      detach,
    } = createInMemoryConnectedPeers({
      backendA,
      backendB,
      codec: treecrdtSyncV0ProtobufCodec,
      peerOptions: { maxCodewords },
    });
    try {
      await pb.syncOnce(tb, { all: {} }, { maxCodewords, codewordsPerMessage });
      await Promise.all([backendA.flush(), backendB.flush()]);
    } finally {
      detach();
    }

    const [countA, countB] = await Promise.all([a.tree.nodeCount(), b.tree.nodeCount()]);
    if (countA !== size || countB !== size) {
      throw new Error(
        `sync-large-fanout${fanout}: expected nodeCount a=b=${size}, got a=${countA} b=${countB}`,
      );
    }
  } finally {
    await Promise.allSettled([a.close(), b.close()]);
  }
}

export async function runTreecrdtSyncE2E(): Promise<{ ok: true }> {
  await runAllE2e();
  await runChildrenE2e();
  return { ok: true };
}

export async function runTreecrdtSyncLargeFanoutE2E(): Promise<{ ok: true }> {
  await runLargeFanoutAllE2e();
  return { ok: true };
}

async function sleep(ms: number): Promise<void> {
  await new Promise<void>((resolve) => setTimeout(resolve, ms));
}

async function waitUntil(
  predicate: () => Promise<boolean> | boolean,
  opts: { timeoutMs?: number; intervalMs?: number; message?: string } = {},
): Promise<void> {
  const timeoutMs = opts.timeoutMs ?? 5_000;
  const intervalMs = opts.intervalMs ?? 25;
  const start = performance.now();
  while (performance.now() - start < timeoutMs) {
    const ok = await predicate();
    if (ok) return;
    await sleep(intervalMs);
  }
  throw new Error(opts.message ?? `waitUntil timeout after ${timeoutMs}ms`);
}

export async function runTreecrdtSyncSubscribeE2E(): Promise<{ ok: true }> {
  const docId = `e2e-sync-subscribe-${crypto.randomUUID()}`;
  const a = await createTreecrdtClient({ storage: 'memory', docId });
  const b = await createTreecrdtClient({ storage: 'memory', docId });

  try {
    const root = '0'.repeat(32);

    const backendA = makeBackend(a, docId, 0);
    const backendB = makeBackend(b, docId, 0);
    const {
      peerA: pa,
      peerB: pb,
      transportA: ta,
      detach,
    } = createInMemoryConnectedPeers({
      backendA,
      backendB,
      codec: treecrdtSyncV0ProtobufCodec,
      peerOptions: { maxCodewords: SYNC_BENCH_DEFAULT_MAX_CODEWORDS },
    });

    // Subscribe to "all" and verify that new ops added on B show up on A without manual sync.
    try {
      const subAll = pa.subscribe(
        ta,
        { all: {} },
        {
          maxCodewords: SYNC_BENCH_DEFAULT_MAX_CODEWORDS,
          codewordsPerMessage: SYNC_BENCH_DEFAULT_SUBSCRIBE_CODEWORDS_PER_MESSAGE,
        },
      );
      try {
        const op1 = makeOp(replicas.b, 1, 1, {
          type: 'insert',
          parent: root,
          node: nodeIdFromInt(1),
          orderKey: orderKeyFromPosition(0),
        });
        await b.ops.append(op1);
        await pb.notifyLocalUpdate();
        await waitUntil(
          async () => {
            const opsA = await a.ops.all();
            return hasOp(opsA, replicas.b, 1);
          },
          { message: 'expected subscription(all) to deliver b:1 to A' },
        );
      } finally {
        subAll.stop();
        await subAll.done;
      }

      // Subscribe to "children(ROOT)" and verify that irrelevant ops do not leak.
      const subChildren = pa.subscribe(
        ta,
        { children: { parent: hexToBytes(root) } },
        {
          maxCodewords: SYNC_BENCH_DEFAULT_MAX_CODEWORDS,
          codewordsPerMessage: SYNC_BENCH_DEFAULT_SUBSCRIBE_CODEWORDS_PER_MESSAGE,
        },
      );
      try {
        const otherParent = 'a0'.repeat(16);
        const outside = makeOp(replicas.b, 2, 2, {
          type: 'insert',
          parent: otherParent,
          node: nodeIdFromInt(2),
          orderKey: orderKeyFromPosition(0),
        });
        await b.ops.append(outside);
        await pb.notifyLocalUpdate();

        // Give the subscription loop time to run at least once; we should not see the op.
        await sleep(250);
        const opsAfterOutside = await a.ops.all();
        if (hasOp(opsAfterOutside, replicas.b, 2))
          throw new Error('subscription(children) should not deliver ops outside filter');

        const inside = makeOp(replicas.b, 3, 3, {
          type: 'insert',
          parent: root,
          node: nodeIdFromInt(3),
          orderKey: orderKeyFromPosition(0),
        });
        await b.ops.append(inside);
        await pb.notifyLocalUpdate();
        await waitUntil(
          async () => {
            const opsA = await a.ops.all();
            return hasOp(opsA, replicas.b, 3);
          },
          { message: 'expected subscription(children) to deliver root child insert to A' },
        );
      } finally {
        subChildren.stop();
        await subChildren.done;
      }

      // Subscribe to "children(non-root)" and verify that we can pull grandchildren on demand.
      const parent = nodeIdFromInt(10);
      const parentInsert = makeOp(replicas.b, 4, 4, {
        type: 'insert',
        parent: root,
        node: parent,
        orderKey: orderKeyFromPosition(0),
      });
      await b.ops.append(parentInsert);
      await pa.syncOnce(
        ta,
        { children: { parent: hexToBytes(root) } },
        {
          maxCodewords: SYNC_BENCH_DEFAULT_MAX_CODEWORDS,
          codewordsPerMessage: SYNC_BENCH_DEFAULT_CODEWORDS_PER_MESSAGE,
        },
      );
      await waitUntil(async () => hasOp(await a.ops.all(), replicas.b, 4), {
        message:
          'expected children(ROOT) to deliver parent insert before subscribing to children(parent)',
      });

      const subGrandChildren = pa.subscribe(
        ta,
        { children: { parent: hexToBytes(parent) } },
        {
          maxCodewords: SYNC_BENCH_DEFAULT_MAX_CODEWORDS,
          codewordsPerMessage: SYNC_BENCH_DEFAULT_SUBSCRIBE_CODEWORDS_PER_MESSAGE,
        },
      );
      try {
        const outside = makeOp(replicas.b, 5, 5, {
          type: 'insert',
          parent: nodeIdFromInt(11),
          node: nodeIdFromInt(12),
          orderKey: orderKeyFromPosition(0),
        });
        await b.ops.append(outside);
        await pb.notifyLocalUpdate();
        await sleep(250);
        if (hasOp(await a.ops.all(), replicas.b, 5)) {
          throw new Error('subscription(children(non-root)) should not deliver ops outside filter');
        }

        const inside = makeOp(replicas.b, 6, 6, {
          type: 'insert',
          parent,
          node: nodeIdFromInt(13),
          orderKey: orderKeyFromPosition(0),
        });
        await b.ops.append(inside);
        await pb.notifyLocalUpdate();
        await waitUntil(async () => hasOp(await a.ops.all(), replicas.b, 6), {
          message:
            'expected subscription(children(non-root)) to deliver child insert under parent to A',
        });
      } finally {
        subGrandChildren.stop();
        await subGrandChildren.done;
      }

      return { ok: true };
    } finally {
      detach();
    }
  } finally {
    await Promise.allSettled([a.close(), b.close()]);
  }
}

async function runBenchOnce(
  storage: StorageKind,
  workload: SyncBenchWorkload,
  size: number,
  bench: ReturnType<typeof buildSyncBenchCase>,
): Promise<number> {
  const docId = `bench-sync-${workload}-${size}-${crypto.randomUUID()}`;
  const mode = storage === 'browser-opfs-coop-sync' ? 'opfs' : 'memory';
  const preferWorker = mode === 'opfs';
  const filenameA = mode === 'opfs' ? `/bench-sync-a-${crypto.randomUUID()}.db` : undefined;
  const filenameB = mode === 'opfs' ? `/bench-sync-b-${crypto.randomUUID()}.db` : undefined;
  const a = await createTreecrdtClient({ storage: mode, preferWorker, filename: filenameA, docId });
  const b = await createTreecrdtClient({ storage: mode, preferWorker, filename: filenameB, docId });

  try {
    await Promise.all([a.ops.appendMany(bench.opsA), b.ops.appendMany(bench.opsB)]);

    const backendA = makeBackend(a, docId, maxLamport(bench.opsA));
    const backendB = makeBackend(b, docId, maxLamport(bench.opsB));
    const {
      peerA: pa,
      transportA: ta,
      detach,
    } = createInMemoryConnectedPeers({
      backendA,
      backendB,
      codec: treecrdtSyncV0ProtobufCodec,
      peerOptions: { maxCodewords: SYNC_BENCH_DEFAULT_MAX_CODEWORDS },
    });
    try {
      const start = performance.now();
      await pa.syncOnce(ta, bench.filter as Filter, {
        maxCodewords: SYNC_BENCH_DEFAULT_MAX_CODEWORDS,
        codewordsPerMessage: SYNC_BENCH_DEFAULT_CODEWORDS_PER_MESSAGE,
      });
      await Promise.all([backendA.flush(), backendB.flush()]);
      const end = performance.now();

      if (workload === 'sync-root-children-fanout10') {
        const finalB = await b.ops.all();
        if (!hasOp(finalB, replicas.m, 1) || !hasOp(finalB, replicas.m, 2)) {
          throw new Error(
            'sync-root-children-fanout10: expected B to receive boundary-crossing moves',
          );
        }
      }
      if (workload === 'sync-one-missing') {
        const [refsA, refsB] = await Promise.all([a.opRefs.all(), b.opRefs.all()]);
        if (refsA.length !== bench.expectedFinalOpsA || refsB.length !== bench.expectedFinalOpsB) {
          throw new Error(
            `sync-one-missing: expected opRefs a=${bench.expectedFinalOpsA} b=${bench.expectedFinalOpsB}, got a=${refsA.length} b=${refsB.length}`,
          );
        }
      }

      return end - start;
    } finally {
      detach();
    }
  } finally {
    await Promise.allSettled([a.close(), b.close()]);
  }
}

async function runBenchCase(
  storage: StorageKind,
  workload: SyncBenchWorkload,
  size: number,
): Promise<SyncBenchResult> {
  const bench = buildSyncBenchCase({ workload, size });
  const { iterations, warmupIterations } = syncBenchTiming();

  const samplesMs: number[] = [];
  for (let i = 0; i < warmupIterations + iterations; i += 1) {
    const ms = await runBenchOnce(storage, workload, size, bench);
    if (i >= warmupIterations) samplesMs.push(ms);
  }

  const durationMs = quantile(samplesMs, 0.5);
  const opsPerSec = durationMs > 0 ? (bench.totalOps / durationMs) * 1000 : Infinity;
  return {
    implementation: 'wa-sqlite',
    storage,
    workload: bench.name,
    name: bench.name,
    totalOps: bench.totalOps,
    durationMs,
    opsPerSec,
    extra: {
      ...bench.extra,
      codewordsPerMessage: SYNC_BENCH_DEFAULT_CODEWORDS_PER_MESSAGE,
      maxCodewords: SYNC_BENCH_DEFAULT_MAX_CODEWORDS,
      iterations,
      warmupIterations,
      samplesMs,
      p95Ms: quantile(samplesMs, 0.95),
      minMs: Math.min(...samplesMs),
      maxMs: Math.max(...samplesMs),
    },
  };
}

export async function runTreecrdtSyncBench(
  storage: StorageKind = 'browser-memory',
  sizes: number[] = [100, 1000],
  workloads: SyncBenchWorkload[] = ['sync-all', 'sync-children'],
): Promise<SyncBenchResult[]> {
  const results: SyncBenchResult[] = [];
  for (const workload of workloads) {
    for (const size of sizes) {
      results.push(await runBenchCase(storage, workload, size));
    }
  }
  return results;
}

declare global {
  interface Window {
    runTreecrdtSyncE2E?: typeof runTreecrdtSyncE2E;
    runTreecrdtSyncLargeFanoutE2E?: typeof runTreecrdtSyncLargeFanoutE2E;
    runTreecrdtSyncSubscribeE2E?: typeof runTreecrdtSyncSubscribeE2E;
    runTreecrdtSyncBench?: typeof runTreecrdtSyncBench;
  }
}

if (typeof window !== 'undefined') {
  window.runTreecrdtSyncE2E = runTreecrdtSyncE2E;
  window.runTreecrdtSyncLargeFanoutE2E = runTreecrdtSyncLargeFanoutE2E;
  window.runTreecrdtSyncSubscribeE2E = runTreecrdtSyncSubscribeE2E;
  window.runTreecrdtSyncBench = runTreecrdtSyncBench;
}

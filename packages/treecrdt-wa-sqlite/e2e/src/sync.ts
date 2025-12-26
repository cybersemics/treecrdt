import { createTreecrdtClient, type TreecrdtClient } from "@treecrdt/wa-sqlite/client";
import { buildSyncBenchCase, envInt, makeOp, maxLamport, nodeIdFromInt, quantile, type SyncBenchWorkload } from "@treecrdt/benchmark";
import type { Operation } from "@treecrdt/interface";
import { bytesToHex, nodeIdToBytes16 } from "@treecrdt/interface/ids";
import { SyncPeer, treecrdtSyncV0ProtobufCodec } from "@treecrdt/sync";
import { createInMemoryDuplex, wrapDuplexTransportWithCodec } from "@treecrdt/sync/transport";
import type { Filter, OpRef, SyncBackend } from "@treecrdt/sync";

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

type StorageKind = "browser-memory" | "browser-opfs-coop-sync";

function hexToBytes(hex: string): Uint8Array {
  return nodeIdToBytes16(hex);
}

function hasOp(ops: Operation[], replica: string, counter: number): boolean {
  return ops.some((op) => op.meta.id.replica === replica && op.meta.id.counter === counter);
}


function makeBackend(
  client: TreecrdtClient,
  docId: string,
  initialMaxLamport: number
): SyncBackend<Operation> & { flush: () => Promise<void> } {
  let maxLamportValue = initialMaxLamport;
  let lastApply = Promise.resolve();

  return {
    docId,
    maxLamport: async () => BigInt(maxLamportValue),
    listOpRefs: async (filter: Filter) => {
      if ("all" in filter) return client.opRefs.all();
      return client.opRefs.children(bytesToHex(filter.children.parent));
    },
    getOpsByOpRefs: async (opRefs: OpRef[]) => client.ops.get(opRefs),
    applyOps: async (ops: Operation[]) => {
      if (ops.length === 0) return;
      const nextMax = maxLamport(ops);
      if (nextMax > maxLamportValue) maxLamportValue = nextMax;
      lastApply = lastApply.then(() => client.ops.appendMany(ops));
      await lastApply;
    },
    flush: async () => lastApply,
  };
}

async function runAllE2e(): Promise<void> {
  const docId = `e2e-sync-all-${crypto.randomUUID()}`;
  const a = await createTreecrdtClient({ storage: "memory", docId });
  const b = await createTreecrdtClient({ storage: "memory", docId });
  try {
    const root = "0".repeat(32);
    const aOps = [makeOp("a", 1, 1, { type: "insert", parent: root, node: nodeIdFromInt(1), position: 0 })];
    const bOps = [makeOp("b", 1, 2, { type: "insert", parent: root, node: nodeIdFromInt(2), position: 0 })];
    await a.ops.appendMany(aOps);
    await b.ops.appendMany(bOps);

    const backendA = makeBackend(a, docId, maxLamport(aOps));
    const backendB = makeBackend(b, docId, maxLamport(bOps));

    const [wa, wb] = createInMemoryDuplex<Uint8Array>();
    const ta = wrapDuplexTransportWithCodec(wa, treecrdtSyncV0ProtobufCodec);
    const tb = wrapDuplexTransportWithCodec(wb, treecrdtSyncV0ProtobufCodec);
    const pa = new SyncPeer(backendA, { maxCodewords: 200_000 });
    const pb = new SyncPeer(backendB, { maxCodewords: 200_000 });
    pa.attach(ta);
    pb.attach(tb);

    await pa.syncOnce(ta, { all: {} }, { maxCodewords: 200_000, codewordsPerMessage: 2048 });
    await Promise.all([backendA.flush(), backendB.flush()]);

    const finalA = await a.ops.all();
    const finalB = await b.ops.all();
    if (finalA.length !== 2 || finalB.length !== 2) {
      throw new Error(`sync-all: expected both sides to have 2 ops, got a=${finalA.length} b=${finalB.length}`);
    }
    if (!hasOp(finalA, "a", 1) || !hasOp(finalA, "b", 1)) throw new Error("sync-all: A missing ops");
    if (!hasOp(finalB, "a", 1) || !hasOp(finalB, "b", 1)) throw new Error("sync-all: B missing ops");
  } finally {
    await Promise.allSettled([a.close(), b.close()]);
  }
}

async function runChildrenE2e(): Promise<void> {
  const docId = `e2e-sync-children-${crypto.randomUUID()}`;
  const a = await createTreecrdtClient({ storage: "memory", docId });
  const b = await createTreecrdtClient({ storage: "memory", docId });
  try {
    const parentAHex = "a0".repeat(16);
    const parentBHex = "b0".repeat(16);
    const aOps = [
      makeOp("a", 1, 1, { type: "insert", parent: parentAHex, node: nodeIdFromInt(1), position: 0 }),
      makeOp("a", 2, 2, { type: "insert", parent: parentBHex, node: nodeIdFromInt(2), position: 0 }),
    ];
    const bOps = [
      makeOp("b", 1, 3, { type: "insert", parent: parentAHex, node: nodeIdFromInt(3), position: 0 }),
      makeOp("b", 2, 4, { type: "insert", parent: parentBHex, node: nodeIdFromInt(4), position: 0 }),
    ];
    await a.ops.appendMany(aOps);
    await b.ops.appendMany(bOps);

    const backendA = makeBackend(a, docId, maxLamport(aOps));
    const backendB = makeBackend(b, docId, maxLamport(bOps));

    const filter: Filter = { children: { parent: hexToBytes(parentAHex) } };
    const [wa, wb] = createInMemoryDuplex<Uint8Array>();
    const ta = wrapDuplexTransportWithCodec(wa, treecrdtSyncV0ProtobufCodec);
    const tb = wrapDuplexTransportWithCodec(wb, treecrdtSyncV0ProtobufCodec);
    const pa = new SyncPeer(backendA, { maxCodewords: 200_000 });
    const pb = new SyncPeer(backendB, { maxCodewords: 200_000 });
    pa.attach(ta);
    pb.attach(tb);

    await pa.syncOnce(ta, filter, { maxCodewords: 200_000, codewordsPerMessage: 2048 });
    await Promise.all([backendA.flush(), backendB.flush()]);

    const finalA = await a.ops.all();
    const finalB = await b.ops.all();
    if (!hasOp(finalA, "b", 1)) throw new Error("sync-children: expected A to receive b:1");
    if (hasOp(finalA, "b", 2)) throw new Error("sync-children: A should not receive b:2");
    if (!hasOp(finalB, "a", 1)) throw new Error("sync-children: expected B to receive a:1");
    if (hasOp(finalB, "a", 2)) throw new Error("sync-children: B should not receive a:2");
  } finally {
    await Promise.allSettled([a.close(), b.close()]);
  }
}

export async function runTreecrdtSyncE2E(): Promise<{ ok: true }> {
  await runAllE2e();
  await runChildrenE2e();
  return { ok: true };
}

async function sleep(ms: number): Promise<void> {
  await new Promise<void>((resolve) => setTimeout(resolve, ms));
}

async function waitUntil(
  predicate: () => Promise<boolean> | boolean,
  opts: { timeoutMs?: number; intervalMs?: number; message?: string } = {}
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
  const a = await createTreecrdtClient({ storage: "memory", docId });
  const b = await createTreecrdtClient({ storage: "memory", docId });

  try {
    const root = "0".repeat(32);

    const backendA = makeBackend(a, docId, 0);
    const backendB = makeBackend(b, docId, 0);
    const [wa, wb] = createInMemoryDuplex<Uint8Array>();
    const ta = wrapDuplexTransportWithCodec(wa, treecrdtSyncV0ProtobufCodec);
    const tb = wrapDuplexTransportWithCodec(wb, treecrdtSyncV0ProtobufCodec);
    const pa = new SyncPeer(backendA, { maxCodewords: 200_000 });
    const pb = new SyncPeer(backendB, { maxCodewords: 200_000 });
    pa.attach(ta);
    pb.attach(tb);

    // Subscribe to "all" and verify that new ops added on B show up on A without manual sync.
    const subAll = pa.subscribe(ta, { all: {} }, { intervalMs: 50, maxCodewords: 200_000, codewordsPerMessage: 1024 });
    try {
      const op1 = makeOp("b", 1, 1, { type: "insert", parent: root, node: nodeIdFromInt(1), position: 0 });
      await b.ops.append(op1);
      await waitUntil(async () => {
        const opsA = await a.ops.all();
        return hasOp(opsA, "b", 1);
      }, { message: "expected subscription(all) to deliver b:1 to A" });
    } finally {
      subAll.stop();
      await subAll.done;
    }

    // Subscribe to "children(ROOT)" and verify that irrelevant ops do not leak.
    const subChildren = pa.subscribe(
      ta,
      { children: { parent: hexToBytes(root) } },
      { intervalMs: 50, maxCodewords: 200_000, codewordsPerMessage: 1024 }
    );
    try {
      const otherParent = "a0".repeat(16);
      const outside = makeOp("b", 2, 2, { type: "insert", parent: otherParent, node: nodeIdFromInt(2), position: 0 });
      await b.ops.append(outside);

      // Give the subscription loop time to run at least once; we should not see the op.
      await sleep(250);
      const opsAfterOutside = await a.ops.all();
      if (hasOp(opsAfterOutside, "b", 2)) throw new Error("subscription(children) should not deliver ops outside filter");

      const inside = makeOp("b", 3, 3, { type: "insert", parent: root, node: nodeIdFromInt(3), position: 0 });
      await b.ops.append(inside);
      await waitUntil(async () => {
        const opsA = await a.ops.all();
        return hasOp(opsA, "b", 3);
      }, { message: "expected subscription(children) to deliver root child insert to A" });
    } finally {
      subChildren.stop();
      await subChildren.done;
    }

    return { ok: true };
  } finally {
    await Promise.allSettled([a.close(), b.close()]);
  }
}

async function runBenchOnce(
  storage: StorageKind,
  workload: SyncBenchWorkload,
  size: number,
  bench: ReturnType<typeof buildSyncBenchCase>
): Promise<number> {
  const docId = `bench-sync-${workload}-${size}-${crypto.randomUUID()}`;
  const mode = storage === "browser-opfs-coop-sync" ? "opfs" : "memory";
  const preferWorker = mode === "opfs";
  const filenameA = mode === "opfs" ? `/bench-sync-a-${crypto.randomUUID()}.db` : undefined;
  const filenameB = mode === "opfs" ? `/bench-sync-b-${crypto.randomUUID()}.db` : undefined;
  const a = await createTreecrdtClient({ storage: mode, preferWorker, filename: filenameA, docId });
  const b = await createTreecrdtClient({ storage: mode, preferWorker, filename: filenameB, docId });

  try {
    await Promise.all([a.ops.appendMany(bench.opsA), b.ops.appendMany(bench.opsB)]);

    const backendA = makeBackend(a, docId, maxLamport(bench.opsA));
    const backendB = makeBackend(b, docId, maxLamport(bench.opsB));

    const [wa, wb] = createInMemoryDuplex<Uint8Array>();
    const ta = wrapDuplexTransportWithCodec(wa, treecrdtSyncV0ProtobufCodec);
    const tb = wrapDuplexTransportWithCodec(wb, treecrdtSyncV0ProtobufCodec);
    const pa = new SyncPeer(backendA, { maxCodewords: 200_000 });
    const pb = new SyncPeer(backendB, { maxCodewords: 200_000 });
    pa.attach(ta);
    pb.attach(tb);

    const start = performance.now();
    await pa.syncOnce(ta, bench.filter as Filter, { maxCodewords: 200_000, codewordsPerMessage: 2048 });
    await Promise.all([backendA.flush(), backendB.flush()]);
    const end = performance.now();

    if (workload === "sync-root-children-fanout10") {
      const finalB = await b.ops.all();
      if (!hasOp(finalB, "m", 1) || !hasOp(finalB, "m", 2)) {
        throw new Error("sync-root-children-fanout10: expected B to receive boundary-crossing moves");
      }
    }
    if (workload === "sync-one-missing") {
      const [refsA, refsB] = await Promise.all([a.opRefs.all(), b.opRefs.all()]);
      if (refsA.length !== bench.expectedFinalOpsA || refsB.length !== bench.expectedFinalOpsB) {
        throw new Error(`sync-one-missing: expected opRefs a=${bench.expectedFinalOpsA} b=${bench.expectedFinalOpsB}, got a=${refsA.length} b=${refsB.length}`);
      }
    }

    return end - start;
  } finally {
    await Promise.allSettled([a.close(), b.close()]);
  }
}

function syncBenchTiming() {
  const iterations = Math.max(1, envInt("SYNC_BENCH_ITERATIONS") ?? envInt("BENCH_ITERATIONS") ?? 3);
  const warmupIterations = Math.max(
    0,
    envInt("SYNC_BENCH_WARMUP") ?? envInt("BENCH_WARMUP") ?? (iterations > 1 ? 1 : 0)
  );
  return { iterations, warmupIterations };
}

async function runBenchCase(
  storage: StorageKind,
  workload: SyncBenchWorkload,
  size: number
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
    implementation: "wa-sqlite",
    storage,
    workload: bench.name,
    name: bench.name,
    totalOps: bench.totalOps,
    durationMs,
    opsPerSec,
    extra: {
      ...bench.extra,
      codewordsPerMessage: 2048,
      maxCodewords: 200_000,
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
  storage: StorageKind = "browser-memory",
  sizes: number[] = [100, 1000],
  workloads: SyncBenchWorkload[] = ["sync-all", "sync-children"]
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
    runTreecrdtSyncSubscribeE2E?: typeof runTreecrdtSyncSubscribeE2E;
    runTreecrdtSyncBench?: typeof runTreecrdtSyncBench;
  }
}

if (typeof window !== "undefined") {
  window.runTreecrdtSyncE2E = runTreecrdtSyncE2E;
  window.runTreecrdtSyncSubscribeE2E = runTreecrdtSyncSubscribeE2E;
  window.runTreecrdtSyncBench = runTreecrdtSyncBench;
}

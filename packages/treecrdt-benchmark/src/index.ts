import type { TreecrdtAdapter, SerializeNodeId, SerializeReplica, Operation } from "@treecrdt/interface";
import { nodeIdToBytes16, replicaIdToBytes } from "@treecrdt/interface/ids";
import { envInt, quantile } from "./stats.js";
import type { WorkloadName } from "./workloads.js";

export type BenchmarkResult = {
  name: string;
  totalOps: number;
  durationMs: number;
  opsPerSec: number;
  extra?: Record<string, unknown>;
};

export type BenchmarkWorkload = {
  name: string;
  totalOps?: number;
  iterations?: number;
  warmupIterations?: number;
  prepare?: () => Promise<void> | void;
  run: (adapter: TreecrdtAdapter) => Promise<void | { extra?: Record<string, unknown> }>;
  cleanup?: () => Promise<void> | void;
};

const defaultSerializeNodeId: SerializeNodeId = nodeIdToBytes16;
const defaultSerializeReplica: SerializeReplica = replicaIdToBytes;

function orderKeyFromPosition(position: number): Uint8Array {
  if (!Number.isInteger(position) || position < 0) throw new Error(`invalid position: ${position}`);
  const n = position + 1;
  if (n > 0xffff) throw new Error(`position too large for u16 order key: ${position}`);
  const bytes = new Uint8Array(2);
  new DataView(bytes.buffer).setUint16(0, n, false);
  return bytes;
}

export async function runBenchmark(
  adapterFactory: () => Promise<TreecrdtAdapter> | TreecrdtAdapter,
  workload: BenchmarkWorkload
): Promise<BenchmarkResult> {
  const totalOps = workload.totalOps ?? -1;

  const envIterations = envInt("BENCH_ITERATIONS");
  const envWarmup = envInt("BENCH_WARMUP");
  const rawIterations = Math.max(1, workload.iterations ?? envIterations ?? 1);
  const minIterationsForTiny =
    totalOps >= 1 && totalOps <= 100 ? 10 : totalOps <= 1000 ? 7 : 1;
  const iterations = Math.max(minIterationsForTiny, rawIterations);
  const warmupIterations = Math.max(0, workload.warmupIterations ?? envWarmup ?? (iterations > 1 ? 1 : 0));

  const samplesMs: number[] = [];
  let lastExtra: Record<string, unknown> | undefined;

  for (let i = 0; i < warmupIterations + iterations; i += 1) {
    const adapter = await adapterFactory();
    try {
      if (workload.prepare) await workload.prepare();
      const start = performance.now();
      const runResult = await workload.run(adapter);
      const end = performance.now();
      if (runResult && typeof runResult === "object" && runResult.extra) lastExtra = runResult.extra;
      if (workload.cleanup) await workload.cleanup();
      if (i >= warmupIterations) samplesMs.push(end - start);
    } finally {
      if (adapter.close) await adapter.close();
    }
  }

  const durationMs = quantile(samplesMs, 0.5);
  const opsPerSec =
    totalOps > 0 && durationMs > 0
      ? (totalOps / durationMs) * 1000
      : durationMs > 0
        ? 1000 / durationMs
        : Infinity;
  return {
    name: workload.name,
    totalOps,
    durationMs,
    opsPerSec,
    extra:
      lastExtra || samplesMs.length > 1
        ? {
            ...(lastExtra ?? {}),
            iterations,
            warmupIterations,
            samplesMs,
            p95Ms: quantile(samplesMs, 0.95),
            minMs: Math.min(...samplesMs),
            maxMs: Math.max(...samplesMs),
          }
        : undefined,
  };
}

// Convenience workload builder: do a series of inserts followed by moves.
export function makeInsertMoveWorkload(opts: {
  count: number;
  serializeNodeId?: SerializeNodeId;
  serializeReplica?: SerializeReplica;
  replica?: Uint8Array;
}): BenchmarkWorkload {
  const serializeNodeId = opts.serializeNodeId ?? defaultSerializeNodeId;
  const serializeReplica = opts.serializeReplica ?? defaultSerializeReplica;
  const replica = opts.replica ?? defaultSerializeReplica("bench");

  const mkOp = (kind: Operation["kind"], counter: number, lamport: number): Operation => ({
    meta: { id: { replica, counter }, lamport },
    kind,
  });

  return {
    name: `insert-move-${opts.count}`,
    totalOps: opts.count * 2,
    run: async (adapter) => {
      // Pre-build ops so we can batch when supported.
      const ops: Operation[] = [];
      for (let i = 0; i < opts.count; i++) {
        const nodeHex = (i + 1).toString(16).padStart(32, "0");
        const parentHex = "0".padStart(32, "0");
        const insert = mkOp(
          { type: "insert", parent: parentHex, node: nodeHex, orderKey: orderKeyFromPosition(i) },
          i + 1,
          i + 1
        );
        ops.push(insert);
      }
      for (let i = 0; i < opts.count; i++) {
        const nodeHex = (i + 1).toString(16).padStart(32, "0");
        const parentHex = "0".padStart(32, "0");
        const mv = mkOp(
          { type: "move", node: nodeHex, newParent: parentHex, orderKey: orderKeyFromPosition(0) },
          opts.count + i + 1,
          opts.count + i + 1
        );
        ops.push(mv);
      }
      const usedBatch = !!adapter.appendOps;
      if (usedBatch && adapter.appendOps) {
        await adapter.appendOps(ops, serializeNodeId, serializeReplica);
      } else {
        for (const op of ops) {
          await adapter.appendOp(op, serializeNodeId, serializeReplica);
        }
      }
      await adapter.opsSince(0);
      return { extra: { mode: usedBatch ? "batch" : "sequential" } };
    },
  };
}

// Insert N nodes as a chain under ROOT (no moves), measuring pure insert cost.
export function makeInsertChainWorkload(opts: {
  count: number;
  serializeNodeId?: SerializeNodeId;
  serializeReplica?: SerializeReplica;
  replica?: Uint8Array;
}): BenchmarkWorkload {
  const serializeNodeId = opts.serializeNodeId ?? defaultSerializeNodeId;
  const serializeReplica = opts.serializeReplica ?? defaultSerializeReplica;
  const replica = opts.replica ?? defaultSerializeReplica("bench");

  const mkOp = (kind: Operation["kind"], counter: number, lamport: number): Operation => ({
    meta: { id: { replica, counter }, lamport },
    kind,
  });

  return {
    name: `insert-chain-${opts.count}`,
    totalOps: opts.count,
    run: async (adapter) => {
      let parentHex = "0".padStart(32, "0");
      const ops: Operation[] = [];
      for (let i = 0; i < opts.count; i++) {
        const nodeHex = (i + 1).toString(16).padStart(32, "0");
        const insert = mkOp(
          { type: "insert", parent: parentHex, node: nodeHex, orderKey: orderKeyFromPosition(0) },
          i + 1,
          i + 1
        );
        ops.push(insert);
        parentHex = nodeHex;
      }
      const usedBatch = !!adapter.appendOps;
      if (usedBatch && adapter.appendOps) {
        await adapter.appendOps(ops, serializeNodeId, serializeReplica);
      } else {
        for (const op of ops) {
          await adapter.appendOp(op, serializeNodeId, serializeReplica);
        }
      }
      await adapter.opsSince(0);
      return { extra: { mode: usedBatch ? "batch" : "sequential" } };
    },
  };
}

// Simulate initial sync: apply a pre-built log of inserts onto an empty adapter.
export function makeReplayLogWorkload(opts: {
  count: number;
  serializeNodeId?: SerializeNodeId;
  serializeReplica?: SerializeReplica;
  replica?: Uint8Array;
}): BenchmarkWorkload {
  const serializeNodeId = opts.serializeNodeId ?? defaultSerializeNodeId;
  const serializeReplica = opts.serializeReplica ?? defaultSerializeReplica;
  const replica = opts.replica ?? defaultSerializeReplica("bench");

  const mkOp = (kind: Operation["kind"], counter: number, lamport: number): Operation => ({
    meta: { id: { replica, counter }, lamport },
    kind,
  });

  const ops: Operation[] = [];
  let parentHex = "0".padStart(32, "0");
  for (let i = 0; i < opts.count; i++) {
    const nodeHex = (i + 1).toString(16).padStart(32, "0");
    const insert = mkOp(
      { type: "insert", parent: parentHex, node: nodeHex, orderKey: orderKeyFromPosition(0) },
      i + 1,
      i + 1
    );
    ops.push(insert);
    parentHex = nodeHex;
  }

  return {
    name: `replay-log-${opts.count}`,
    totalOps: ops.length,
    run: async (adapter) => {
      const usedBatch = !!adapter.appendOps;
      if (usedBatch && adapter.appendOps) {
        await adapter.appendOps(ops, serializeNodeId, serializeReplica);
      } else {
        for (const op of ops) {
          await adapter.appendOp(op, serializeNodeId, serializeReplica);
        }
      }
      await adapter.opsSince(0);
      return { extra: { mode: usedBatch ? "batch" : "sequential" } };
    },
  };
}

export function makeWorkload(name: WorkloadName, count: number): BenchmarkWorkload {
  if (name === "insert-chain") return makeInsertChainWorkload({ count });
  if (name === "replay-log") return makeReplayLogWorkload({ count });
  return makeInsertMoveWorkload({ count });
}

export function buildWorkloads(names: WorkloadName[], sizes: number[]): BenchmarkWorkload[] {
  const result: BenchmarkWorkload[] = [];
  for (const name of names) {
    for (const size of sizes) {
      result.push(makeWorkload(name, size));
    }
  }
  return result;
}

export async function runWorkloads(
  adapterFactory: () => Promise<TreecrdtAdapter> | TreecrdtAdapter,
  workloads: BenchmarkWorkload[]
): Promise<BenchmarkResult[]> {
  const results: BenchmarkResult[] = [];
  for (const workload of workloads) {
    const res = await runBenchmark(adapterFactory, workload);
    results.push(res);
  }
  return results;
}

export { DEFAULT_BENCH_SIZES, WORKLOAD_NAMES, type WorkloadName } from "./workloads.js";
export { benchTiming } from "./timing.js";
export * from "./sync.js";
export * from "./stats.js";

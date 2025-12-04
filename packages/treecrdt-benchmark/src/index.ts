import fs from "node:fs/promises";
import path from "node:path";
import type { TreecrdtAdapter, SerializeNodeId, SerializeReplica, Operation } from "@treecrdt/interface";

export type BenchmarkResult = {
  name: string;
  totalOps: number;
  durationMs: number;
  opsPerSec: number;
};

export type BenchmarkWorkload = {
  name: string;
  totalOps?: number;
  prepare?: () => Promise<void> | void;
  run: (adapter: TreecrdtAdapter) => Promise<void>;
  cleanup?: () => Promise<void> | void;
};

export type WorkloadName = "insert-move" | "insert-chain" | "replay-log";

const defaultSerializeNodeId: SerializeNodeId = (id) => {
  const clean = id.startsWith("0x") ? id.slice(2) : id;
  if (clean.length % 2 === 0) {
    const bytes = new Uint8Array(clean.length / 2);
    for (let i = 0; i < clean.length; i += 2) {
      bytes[i / 2] = parseInt(clean.slice(i, i + 2), 16);
    }
    return bytes;
  }
  const encoder = new TextEncoder();
  return encoder.encode(id);
};
const defaultSerializeReplica: SerializeReplica = (replica) =>
  typeof replica === "string" ? defaultSerializeNodeId(replica) : replica;

export async function runBenchmark(
  adapterFactory: () => Promise<TreecrdtAdapter> | TreecrdtAdapter,
  workload: BenchmarkWorkload
): Promise<BenchmarkResult> {
  const adapter = await adapterFactory();
  const totalOps = workload.totalOps ?? -1;
  if (workload.prepare) {
    await workload.prepare();
  }
  const start = performance.now();
  await workload.run(adapter);
  const end = performance.now();
  if (workload.cleanup) {
    await workload.cleanup();
  }
  if (adapter.close) {
    await adapter.close();
  }

  const durationMs = end - start;
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
      for (let i = 0; i < opts.count; i++) {
        const nodeHex = (i + 1).toString(16).padStart(32, "0");
        const parentHex = "0".padStart(32, "0");
        const insert = mkOp(
          { type: "insert", parent: parentHex, node: nodeHex, position: i },
          i + 1,
          i + 1
        );
        await adapter.appendOp(insert, serializeNodeId, serializeReplica);
      }
      for (let i = 0; i < opts.count; i++) {
        const nodeHex = (i + 1).toString(16).padStart(32, "0");
        const parentHex = "0".padStart(32, "0");
        const mv = mkOp(
          { type: "move", node: nodeHex, newParent: parentHex, position: 0 },
          opts.count + i + 1,
          opts.count + i + 1
        );
        await adapter.appendOp(mv, serializeNodeId, serializeReplica);
      }
      await adapter.opsSince(0);
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
      for (let i = 0; i < opts.count; i++) {
        const nodeHex = (i + 1).toString(16).padStart(32, "0");
        const insert = mkOp(
          { type: "insert", parent: parentHex, node: nodeHex, position: 0 },
          i + 1,
          i + 1
        );
        await adapter.appendOp(insert, serializeNodeId, serializeReplica);
        parentHex = nodeHex;
      }
      await adapter.opsSince(0);
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
      { type: "insert", parent: parentHex, node: nodeHex, position: 0 },
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
      for (const op of ops) {
        await adapter.appendOp(op, serializeNodeId, serializeReplica);
      }
      await adapter.opsSince(0);
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

export type BenchmarkOutput = BenchmarkResult & {
  implementation: string;
  storage: string;
  workload: string;
  timestamp: string;
  extra?: Record<string, unknown>;
  sourceFile?: string;
};

export async function writeResult(
  result: BenchmarkResult,
  opts: {
    implementation: string;
    storage: string;
    workload?: string;
    outFile: string;
    extra?: Record<string, unknown>;
  }
): Promise<BenchmarkOutput> {
  const payload: BenchmarkOutput = {
    implementation: opts.implementation,
    storage: opts.storage,
    workload: opts.workload ?? result.name,
    timestamp: new Date().toISOString(),
    ...result,
    extra: opts.extra,
    sourceFile: path.resolve(opts.outFile),
  };
  await fs.mkdir(path.dirname(opts.outFile), { recursive: true });
  await fs.writeFile(opts.outFile, JSON.stringify(payload, null, 2), "utf-8");
  return payload;
}

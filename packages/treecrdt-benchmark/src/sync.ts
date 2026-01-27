import type { Operation, OperationKind, ReplicaId } from "@treecrdt/interface";
import { nodeIdToBytes16 } from "@treecrdt/interface/ids";
import { envIntList } from "./stats.js";
import { benchTiming } from "./timing.js";

export type SyncBenchWorkload =
  | "sync-all"
  | "sync-children"
  | "sync-root-children-fanout10"
  | "sync-one-missing";

export const DEFAULT_SYNC_BENCH_SIZES = [100, 1000, 10_000] as const;
export const DEFAULT_SYNC_BENCH_ROOT_CHILDREN_SIZES = [1110] as const;

export const DEFAULT_SYNC_BENCH_WORKLOADS = ["sync-all", "sync-children", "sync-one-missing"] as const satisfies readonly SyncBenchWorkload[];
export const DEFAULT_SYNC_BENCH_ROOT_CHILDREN_WORKLOADS = ["sync-root-children-fanout10"] as const satisfies readonly SyncBenchWorkload[];

export const SYNC_BENCH_DEFAULT_MAX_CODEWORDS = 200_000;
export const SYNC_BENCH_DEFAULT_CODEWORDS_PER_MESSAGE = 2048;
export const SYNC_BENCH_DEFAULT_SUBSCRIBE_CODEWORDS_PER_MESSAGE = 1024;

export function syncBenchSizesFromEnv(): number[] {
  return envIntList("SYNC_BENCH_SIZES") ?? Array.from(DEFAULT_SYNC_BENCH_SIZES);
}

export function syncBenchRootChildrenSizesFromEnv(): number[] {
  return envIntList("SYNC_BENCH_ROOT_CHILDREN_SIZES") ?? Array.from(DEFAULT_SYNC_BENCH_ROOT_CHILDREN_SIZES);
}

export function syncBenchTiming(opts: { defaultIterations?: number } = {}): { iterations: number; warmupIterations: number } {
  return benchTiming({
    iterationsEnv: ["SYNC_BENCH_ITERATIONS", "BENCH_ITERATIONS"],
    warmupEnv: ["SYNC_BENCH_WARMUP", "BENCH_WARMUP"],
    defaultIterations: opts.defaultIterations ?? 3,
  });
}

export type SyncFilter =
  | { all: Record<string, never> }
  | { children: { parent: Uint8Array } };

export type SyncBenchCase = {
  name: string;
  opsA: Operation[];
  opsB: Operation[];
  filter: SyncFilter;
  totalOps: number;
  extra: Record<string, unknown>;
  expectedFinalOpsA: number;
  expectedFinalOpsB: number;
};

export function nodeIdFromInt(i: number): string {
  if (!Number.isInteger(i) || i < 0) throw new Error(`invalid node id: ${i}`);
  return i.toString(16).padStart(32, "0");
}

function orderKeyFromPosition(position: number): Uint8Array {
  if (!Number.isInteger(position) || position < 0) throw new Error(`invalid position: ${position}`);
  const n = position + 1;
  if (n > 0xffff) throw new Error(`position too large for u16 order key: ${position}`);
  const bytes = new Uint8Array(2);
  new DataView(bytes.buffer).setUint16(0, n, false);
  return bytes;
}

export function makeOp(
  replica: ReplicaId,
  counter: number,
  lamport: number,
  kind: OperationKind
): Operation {
  return { meta: { id: { replica, counter }, lamport }, kind };
}

export function maxLamport(ops: Operation[]): number {
  return ops.reduce((acc, op) => Math.max(acc, op.meta.lamport), 0);
}

type ParentCursor = { parent: string; nextChildPosition: number };

export function buildFanoutInsertTreeOps(opts: {
  replica: ReplicaId;
  size: number;
  fanout: number;
  root: string;
}): Operation[] {
  if (!Number.isInteger(opts.size) || opts.size <= 0) throw new Error(`invalid size: ${opts.size}`);
  if (!Number.isInteger(opts.fanout) || opts.fanout <= 0) throw new Error(`invalid fanout: ${opts.fanout}`);

  const ops: Operation[] = [];
  const queue: ParentCursor[] = [{ parent: opts.root, nextChildPosition: 0 }];

  for (let i = 1; i <= opts.size; i += 1) {
    const cursor = queue[0];
    if (!cursor) throw new Error("fanout tree queue empty");

    const parent = cursor.parent;
    const position = cursor.nextChildPosition;
    cursor.nextChildPosition += 1;
    if (cursor.nextChildPosition >= opts.fanout) queue.shift();

    const node = nodeIdFromInt(i);
    ops.push(
      makeOp(opts.replica, i, i, { type: "insert", parent, node, orderKey: orderKeyFromPosition(position) })
    );
    queue.push({ parent: node, nextChildPosition: 0 });
  }

  return ops;
}

export function buildSyncBenchCase(opts: {
  workload: SyncBenchWorkload;
  size: number;
  fanout?: number;
}): SyncBenchCase {
  const { workload } = opts;
  const size = opts.size;
  const root = "0".repeat(32);

  if (workload === "sync-one-missing") {
    const treeSize = size;
    if (!Number.isInteger(treeSize) || treeSize <= 0) throw new Error(`sync-one-missing requires size > 0`);

    const missingCounter = Math.min(treeSize, Math.max(1, Math.ceil(treeSize / 2)));
    const missingNode = nodeIdFromInt(missingCounter);

    const opsB: Operation[] = [];
    const opsA: Operation[] = [];
    for (let counter = 1; counter <= treeSize; counter += 1) {
      const op = makeOp("s", counter, counter, {
        type: "insert",
        parent: root,
        node: nodeIdFromInt(counter),
        orderKey: orderKeyFromPosition(counter - 1),
      });
      opsB.push(op);
      if (counter !== missingCounter) opsA.push(op);
    }

    return {
      name: `sync-one-missing-${treeSize}`,
      opsA,
      opsB,
      filter: { all: {} },
      totalOps: 1,
      extra: { opsPerPeer: treeSize, missingCounter, missingNode },
      expectedFinalOpsA: treeSize,
      expectedFinalOpsB: treeSize,
    };
  }

  if (workload === "sync-root-children-fanout10") {
    const fanout = opts.fanout ?? 10;
    const treeSize = size;
    if (treeSize < fanout) throw new Error(`sync-root-children-fanout10 requires size >= ${fanout}`);

    const sharedOps = buildFanoutInsertTreeOps({ replica: "s", size: treeSize, fanout, root });
    const movedNode = nodeIdFromInt(fanout);
    const trash = "f".repeat(32);
    const moveOut = makeOp("m", 1, treeSize + 1, {
      type: "move",
      node: movedNode,
      newParent: trash,
      orderKey: orderKeyFromPosition(0),
    });
    const moveBack = makeOp("m", 2, treeSize + 2, {
      type: "move",
      node: movedNode,
      newParent: root,
      orderKey: orderKeyFromPosition(fanout - 1),
    });

    const opsA = [...sharedOps, moveOut, moveBack];
    const opsB = sharedOps;
    const totalOps = 2;
    return {
      name: `sync-root-children-fanout10-${treeSize}`,
      opsA,
      opsB,
      filter: { children: { parent: nodeIdToBytes16(root) } },
      totalOps,
      extra: { treeSize, fanout, rootChildren: fanout, movedNode },
      expectedFinalOpsA: opsA.length,
      expectedFinalOpsB: treeSize + 2,
    };
  }

  if (workload === "sync-all") {
    const shared = Math.floor(size / 2);
    const unique = size - shared;
    const sharedOps: Operation[] = [];
    const aOps: Operation[] = [];
    const bOps: Operation[] = [];

    for (let i = 0; i < shared; i++) {
      const counter = i + 1;
      sharedOps.push(
        makeOp("s", counter, counter, {
          type: "insert",
          parent: root,
          node: nodeIdFromInt(counter),
          orderKey: orderKeyFromPosition(i),
        })
      );
    }
    for (let i = 0; i < unique; i++) {
      const counter = i + 1;
      const lamport = shared + counter;
      aOps.push(
        makeOp("a", counter, lamport, {
          type: "insert",
          parent: root,
          node: nodeIdFromInt(shared + counter),
          orderKey: orderKeyFromPosition(shared + i),
        })
      );
      bOps.push(
        makeOp("b", counter, lamport, {
          type: "insert",
          parent: root,
          node: nodeIdFromInt(shared + unique + counter),
          orderKey: orderKeyFromPosition(shared + i),
        })
      );
    }

    const opsA = [...sharedOps, ...aOps];
    const opsB = [...sharedOps, ...bOps];
    const totalOps = 2 * unique;
    const expectedFinal = size + unique;
    return {
      name: `sync-all-${size}`,
      opsA,
      opsB,
      filter: { all: {} },
      totalOps,
      extra: { opsPerPeer: size, shared, unique },
      expectedFinalOpsA: expectedFinal,
      expectedFinalOpsB: expectedFinal,
    };
  }

  // sync-children
  const targetParentHex = "a0".repeat(16);
  const otherParentHex = "b0".repeat(16);
  const targetCount = Math.floor(size / 2);
  const otherCount = size - targetCount;
  const sharedTarget = Math.floor(targetCount / 2);
  const uniqueTarget = targetCount - sharedTarget;

  const sharedOps: Operation[] = [];
  const aTarget: Operation[] = [];
  const bTarget: Operation[] = [];
  const aOther: Operation[] = [];
  const bOther: Operation[] = [];

  let lamport = 0;
  for (let i = 0; i < sharedTarget; i++) {
    lamport += 1;
    const counter = i + 1;
    sharedOps.push(
      makeOp("s", counter, lamport, {
        type: "insert",
        parent: targetParentHex,
        node: nodeIdFromInt(counter),
        orderKey: orderKeyFromPosition(i),
      })
    );
  }

  for (let i = 0; i < uniqueTarget; i++) {
    lamport += 1;
    const counter = i + 1;
    aTarget.push(
      makeOp("a", counter, lamport, {
        type: "insert",
        parent: targetParentHex,
        node: nodeIdFromInt(sharedTarget + counter),
        orderKey: orderKeyFromPosition(sharedTarget + i),
      })
    );
    bTarget.push(
      makeOp("b", counter, lamport, {
        type: "insert",
        parent: targetParentHex,
        node: nodeIdFromInt(sharedTarget + uniqueTarget + counter),
        orderKey: orderKeyFromPosition(sharedTarget + i),
      })
    );
  }

  for (let i = 0; i < otherCount; i++) {
    lamport += 1;
    aOther.push(
      makeOp("x", i + 1, lamport, {
        type: "insert",
        parent: otherParentHex,
        node: nodeIdFromInt(sharedTarget + 2 * uniqueTarget + i + 1),
        orderKey: orderKeyFromPosition(i),
      })
    );
    bOther.push(
      makeOp("y", i + 1, lamport, {
        type: "insert",
        parent: otherParentHex,
        node: nodeIdFromInt(sharedTarget + 2 * uniqueTarget + otherCount + i + 1),
        orderKey: orderKeyFromPosition(i),
      })
    );
  }

  const opsA = [...sharedOps, ...aTarget, ...aOther];
  const opsB = [...sharedOps, ...bTarget, ...bOther];
  const totalOps = 2 * uniqueTarget;
  const expectedFinal = size + uniqueTarget;
  return {
    name: `sync-children-${size}`,
    opsA,
    opsB,
    filter: { children: { parent: nodeIdToBytes16(targetParentHex) } },
    totalOps,
    extra: { opsPerPeer: size, targetCount, otherCount, sharedTarget, uniqueTarget },
    expectedFinalOpsA: expectedFinal,
    expectedFinalOpsB: expectedFinal,
  };
}

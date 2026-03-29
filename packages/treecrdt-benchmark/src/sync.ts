import type { Operation, OperationKind, ReplicaId } from '@treecrdt/interface';
import { nodeIdToBytes16 } from '@treecrdt/interface/ids';
import { envIntList } from './stats.js';
import { benchTiming } from './timing.js';

export type SyncBenchWorkload =
  | 'sync-all'
  | 'sync-balanced-children-cold-start'
  | 'sync-balanced-children-payloads-cold-start'
  | 'sync-children'
  | 'sync-children-cold-start'
  | 'sync-children-payloads'
  | 'sync-children-payloads-cold-start'
  | 'sync-root-children-fanout10'
  | 'sync-one-missing';

export const DEFAULT_SYNC_BENCH_SIZES = [100, 1000, 10_000] as const;
export const DEFAULT_SYNC_BENCH_ROOT_CHILDREN_SIZES = [1110] as const;
export const DEFAULT_SYNC_BENCH_FANOUT = 10;
export const DEFAULT_SYNC_BENCH_PAGE_SIZE = 10;
export const DEFAULT_SYNC_BENCH_PAYLOAD_BYTES = 512;

export const DEFAULT_SYNC_BENCH_WORKLOADS = [
  'sync-one-missing',
  'sync-balanced-children-cold-start',
  'sync-balanced-children-payloads-cold-start',
] as const satisfies readonly SyncBenchWorkload[];
export const SYNTHETIC_SYNC_BENCH_WORKLOADS = [
  'sync-all',
  'sync-children',
  'sync-children-cold-start',
  'sync-children-payloads',
  'sync-children-payloads-cold-start',
] as const satisfies readonly SyncBenchWorkload[];
export const ALL_SYNC_BENCH_WORKLOADS = [
  ...DEFAULT_SYNC_BENCH_WORKLOADS,
  ...SYNTHETIC_SYNC_BENCH_WORKLOADS,
] as const satisfies readonly SyncBenchWorkload[];
export const DEFAULT_SYNC_BENCH_ROOT_CHILDREN_WORKLOADS = [
  'sync-root-children-fanout10',
] as const satisfies readonly SyncBenchWorkload[];

export const SYNC_BENCH_DEFAULT_MAX_CODEWORDS = 200_000;
export const SYNC_BENCH_DEFAULT_CODEWORDS_PER_MESSAGE = 2048;
export const SYNC_BENCH_DEFAULT_SUBSCRIBE_CODEWORDS_PER_MESSAGE = 1024;

export function syncBenchSizesFromEnv(): number[] {
  return envIntList('SYNC_BENCH_SIZES') ?? Array.from(DEFAULT_SYNC_BENCH_SIZES);
}

export function syncBenchRootChildrenSizesFromEnv(): number[] {
  return (
    envIntList('SYNC_BENCH_ROOT_CHILDREN_SIZES') ??
    Array.from(DEFAULT_SYNC_BENCH_ROOT_CHILDREN_SIZES)
  );
}

export function syncBenchTiming(opts: { defaultIterations?: number } = {}): {
  iterations: number;
  warmupIterations: number;
} {
  return benchTiming({
    iterationsEnv: ['SYNC_BENCH_ITERATIONS', 'BENCH_ITERATIONS'],
    warmupEnv: ['SYNC_BENCH_WARMUP', 'BENCH_WARMUP'],
    defaultIterations: opts.defaultIterations ?? 10,
  });
}

export type SyncFilter = { all: Record<string, never> } | { children: { parent: Uint8Array } };

export type SyncBenchCase = {
  name: string;
  opsA: Operation[];
  opsB: Operation[];
  filter: SyncFilter;
  totalOps: number;
  extra: Record<string, unknown>;
  expectedFinalOpsA: number;
  expectedFinalOpsB: number;
  firstView?: {
    parent: string;
    pageSize: number;
    expectedChildren: number;
    includePayloads: boolean;
    payloadBytes?: number;
  };
};

export function nodeIdFromInt(i: number): string {
  if (!Number.isInteger(i) || i < 0) throw new Error(`invalid node id: ${i}`);
  return i.toString(16).padStart(32, '0');
}

function orderKeyFromPosition(position: number): Uint8Array {
  if (!Number.isInteger(position) || position < 0) throw new Error(`invalid position: ${position}`);
  const n = position + 1;
  if (n > 0xffff) throw new Error(`position too large for u16 order key: ${position}`);
  const bytes = new Uint8Array(2);
  new DataView(bytes.buffer).setUint16(0, n, false);
  return bytes;
}

function replicaFromLabel(label: string): Uint8Array {
  const encoded = new TextEncoder().encode(label);
  if (encoded.length === 0) throw new Error('label must not be empty');
  const out = new Uint8Array(32);
  for (let i = 0; i < out.length; i += 1) out[i] = encoded[i % encoded.length]!;
  return out;
}

export function makeOp(
  replica: ReplicaId,
  counter: number,
  lamport: number,
  kind: OperationKind,
): Operation {
  return { meta: { id: { replica, counter }, lamport }, kind };
}

export function maxLamport(ops: Operation[]): number {
  return ops.reduce((acc, op) => Math.max(acc, op.meta.lamport), 0);
}

type ParentCursor = { parent: string; nextChildPosition: number };

function payloadBytesFromSeed(seed: number, size = 512): Uint8Array {
  if (!Number.isInteger(seed) || seed < 0) throw new Error(`invalid payload seed: ${seed}`);
  if (!Number.isInteger(size) || size <= 0) throw new Error(`invalid payload size: ${size}`);
  const out = new Uint8Array(size);
  for (let i = 0; i < out.length; i += 1) {
    out[i] = (seed + i * 31) % 251;
  }
  return out;
}

export function buildFanoutInsertTreeOps(opts: {
  replica: ReplicaId;
  size: number;
  fanout: number;
  root: string;
}): Operation[] {
  if (!Number.isInteger(opts.size) || opts.size <= 0) throw new Error(`invalid size: ${opts.size}`);
  if (!Number.isInteger(opts.fanout) || opts.fanout <= 0)
    throw new Error(`invalid fanout: ${opts.fanout}`);

  const ops: Operation[] = [];
  const queue: ParentCursor[] = [{ parent: opts.root, nextChildPosition: 0 }];

  for (let i = 1; i <= opts.size; i += 1) {
    const cursor = queue[0];
    if (!cursor) throw new Error('fanout tree queue empty');

    const parent = cursor.parent;
    const position = cursor.nextChildPosition;
    cursor.nextChildPosition += 1;
    if (cursor.nextChildPosition >= opts.fanout) queue.shift();

    const node = nodeIdFromInt(i);
    ops.push(
      makeOp(opts.replica, i, i, {
        type: 'insert',
        parent,
        node,
        orderKey: orderKeyFromPosition(position),
      }),
    );
    queue.push({ parent: node, nextChildPosition: 0 });
  }

  return ops;
}

function targetChildrenForFirstChild(treeSize: number, fanout: number): string[] {
  const childCount = Math.min(fanout, Math.max(0, treeSize - fanout));
  return Array.from({ length: childCount }, (_, i) => nodeIdFromInt(fanout + i + 1));
}

function buildBalancedChildrenColdStartCase(opts: {
  size: number;
  fanout: number;
  replicas: { s: ReplicaId; p: ReplicaId };
  root: string;
  payloadBytes: number;
  withPayloads: boolean;
}): SyncBenchCase {
  const treeSize = opts.size;
  if (!Number.isInteger(treeSize) || treeSize <= opts.fanout) {
    throw new Error(`balanced children cold-start requires size > fanout (${opts.fanout})`);
  }

  const sharedOps = buildFanoutInsertTreeOps({
    replica: opts.replicas.s,
    size: treeSize,
    fanout: opts.fanout,
    root: opts.root,
  });
  const scopeRootInsert = sharedOps[0];
  if (!scopeRootInsert || scopeRootInsert.kind.type !== 'insert') {
    throw new Error('expected balanced tree seed to start with scope root insert');
  }

  const targetParent = scopeRootInsert.kind.node;
  const targetChildren = targetChildrenForFirstChild(treeSize, opts.fanout);
  const opsA: Operation[] = [scopeRootInsert];
  const opsB: Operation[] = [...sharedOps];

  if (opts.withPayloads) {
    let counter = 0;
    let lamport = maxLamport(sharedOps);
    for (let i = 0; i < sharedOps.length; i += 1) {
      const op = sharedOps[i];
      if (op?.kind.type !== 'insert') continue;
      opsB.push(
        makeOp(opts.replicas.p, ++counter, ++lamport, {
          type: 'payload',
          node: op.kind.node,
          payload: payloadBytesFromSeed(i + 1, opts.payloadBytes),
        }),
      );
    }
  }

  const transferredOps = opts.withPayloads ? 1 + targetChildren.length * 2 : targetChildren.length;
  return {
    name: `sync-balanced-children${opts.withPayloads ? '-payloads' : ''}-cold-start-fanout${opts.fanout}-${treeSize}`,
    opsA,
    opsB,
    filter: { children: { parent: nodeIdToBytes16(targetParent) } },
    totalOps: transferredOps,
    extra: {
      treeSize,
      fanout: opts.fanout,
      targetParent,
      targetDepth: 1,
      targetChildren: targetChildren.length,
      coldStart: true,
      balancedTree: true,
      knownScopeRoot: true,
      payloadBytes: opts.withPayloads ? opts.payloadBytes : 0,
      payloadsEverywhere: opts.withPayloads,
      pageSize: Math.min(DEFAULT_SYNC_BENCH_PAGE_SIZE, targetChildren.length),
    },
    expectedFinalOpsA: opsA.length + transferredOps,
    expectedFinalOpsB: opsB.length,
    firstView: {
      parent: targetParent,
      pageSize: Math.min(DEFAULT_SYNC_BENCH_PAGE_SIZE, targetChildren.length),
      expectedChildren: targetChildren.length,
      includePayloads: opts.withPayloads,
      payloadBytes: opts.withPayloads ? opts.payloadBytes : undefined,
    },
  };
}

export function buildSyncBenchCase(opts: {
  workload: SyncBenchWorkload;
  size: number;
  fanout?: number;
  payloadBytes?: number;
}): SyncBenchCase {
  const { workload } = opts;
  const size = opts.size;
  const root = '0'.repeat(32);
  const replicas = {
    a: replicaFromLabel('a'),
    b: replicaFromLabel('b'),
    m: replicaFromLabel('m'),
    p: replicaFromLabel('p'),
    s: replicaFromLabel('s'),
    x: replicaFromLabel('x'),
    y: replicaFromLabel('y'),
  };
  const fanout = opts.fanout ?? DEFAULT_SYNC_BENCH_FANOUT;
  const payloadBytes = opts.payloadBytes ?? DEFAULT_SYNC_BENCH_PAYLOAD_BYTES;

  if (workload === 'sync-one-missing') {
    const treeSize = size;
    if (!Number.isInteger(treeSize) || treeSize <= 0)
      throw new Error(`sync-one-missing requires size > 0`);

    const missingCounter = Math.min(treeSize, Math.max(1, Math.ceil(treeSize / 2)));
    const missingNode = nodeIdFromInt(missingCounter);

    const opsB: Operation[] = [];
    const opsA: Operation[] = [];
    for (let counter = 1; counter <= treeSize; counter += 1) {
      const op = makeOp(replicas.s, counter, counter, {
        type: 'insert',
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

  if (workload === 'sync-balanced-children-cold-start') {
    return buildBalancedChildrenColdStartCase({
      size,
      fanout,
      replicas: { s: replicas.s, p: replicas.p },
      root,
      payloadBytes,
      withPayloads: false,
    });
  }

  if (workload === 'sync-balanced-children-payloads-cold-start') {
    return buildBalancedChildrenColdStartCase({
      size,
      fanout,
      replicas: { s: replicas.s, p: replicas.p },
      root,
      payloadBytes,
      withPayloads: true,
    });
  }

  if (workload === 'sync-root-children-fanout10') {
    const treeSize = size;
    if (treeSize < fanout)
      throw new Error(`sync-root-children-fanout10 requires size >= ${fanout}`);

    const sharedOps = buildFanoutInsertTreeOps({
      replica: replicas.s,
      size: treeSize,
      fanout,
      root,
    });
    const movedNode = nodeIdFromInt(fanout);
    const trash = 'f'.repeat(32);
    const moveOut = makeOp(replicas.m, 1, treeSize + 1, {
      type: 'move',
      node: movedNode,
      newParent: trash,
      orderKey: orderKeyFromPosition(0),
    });
    const moveBack = makeOp(replicas.m, 2, treeSize + 2, {
      type: 'move',
      node: movedNode,
      newParent: root,
      orderKey: orderKeyFromPosition(fanout - 1),
    });

    const opsA = [...sharedOps, moveOut, moveBack];
    const opsB = sharedOps;
    const totalOps = 2;
    return {
      name: `sync-root-children-fanout${fanout}-${treeSize}`,
      opsA,
      opsB,
      filter: { children: { parent: nodeIdToBytes16(root) } },
      totalOps,
      extra: { treeSize, fanout, rootChildren: fanout, movedNode },
      expectedFinalOpsA: opsA.length,
      expectedFinalOpsB: treeSize + 2,
    };
  }

  if (workload === 'sync-all') {
    const shared = Math.floor(size / 2);
    const unique = size - shared;
    const sharedOps: Operation[] = [];
    const aOps: Operation[] = [];
    const bOps: Operation[] = [];

    for (let i = 0; i < shared; i++) {
      const counter = i + 1;
      sharedOps.push(
        makeOp(replicas.s, counter, counter, {
          type: 'insert',
          parent: root,
          node: nodeIdFromInt(counter),
          orderKey: orderKeyFromPosition(i),
        }),
      );
    }
    for (let i = 0; i < unique; i++) {
      const counter = i + 1;
      const lamport = shared + counter;
      aOps.push(
        makeOp(replicas.a, counter, lamport, {
          type: 'insert',
          parent: root,
          node: nodeIdFromInt(shared + counter),
          orderKey: orderKeyFromPosition(shared + i),
        }),
      );
      bOps.push(
        makeOp(replicas.b, counter, lamport, {
          type: 'insert',
          parent: root,
          node: nodeIdFromInt(shared + unique + counter),
          orderKey: orderKeyFromPosition(shared + i),
        }),
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

  if (workload === 'sync-children-payloads') {
    const targetParentHex = 'a0'.repeat(16);
    const otherParentHex = 'b0'.repeat(16);
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
    let counterS = 0;
    let counterA = 0;
    let counterB = 0;
    let counterX = 0;
    let counterY = 0;

    sharedOps.push(
      makeOp(replicas.s, ++counterS, ++lamport, {
        type: 'insert',
        parent: root,
        node: targetParentHex,
        orderKey: orderKeyFromPosition(0),
      }),
    );
    sharedOps.push(
      makeOp(replicas.s, ++counterS, ++lamport, {
        type: 'insert',
        parent: root,
        node: otherParentHex,
        orderKey: orderKeyFromPosition(1),
      }),
    );
    sharedOps.push(
      makeOp(replicas.s, ++counterS, ++lamport, {
        type: 'payload',
        node: targetParentHex,
        payload: payloadBytesFromSeed(10_000, payloadBytes),
      }),
    );

    bTarget.push(
      makeOp(replicas.b, ++counterB, ++lamport, {
        type: 'payload',
        node: targetParentHex,
        payload: payloadBytesFromSeed(20_000, payloadBytes),
      }),
    );

    for (let i = 0; i < sharedTarget; i += 1) {
      const child = nodeIdFromInt(i + 1);
      sharedOps.push(
        makeOp(replicas.s, ++counterS, ++lamport, {
          type: 'insert',
          parent: targetParentHex,
          node: child,
          orderKey: orderKeyFromPosition(i),
        }),
      );
      sharedOps.push(
        makeOp(replicas.s, ++counterS, ++lamport, {
          type: 'payload',
          node: child,
          payload: payloadBytesFromSeed(i + 1, payloadBytes),
        }),
      );
    }

    for (let i = 0; i < uniqueTarget; i += 1) {
      const position = sharedTarget + i;
      const childA = nodeIdFromInt(sharedTarget + i + 1);
      const childB = nodeIdFromInt(sharedTarget + uniqueTarget + i + 1);

      aTarget.push(
        makeOp(replicas.a, ++counterA, ++lamport, {
          type: 'insert',
          parent: targetParentHex,
          node: childA,
          orderKey: orderKeyFromPosition(position),
        }),
      );
      aTarget.push(
        makeOp(replicas.a, ++counterA, ++lamport, {
          type: 'payload',
          node: childA,
          payload: payloadBytesFromSeed(30_000 + i, payloadBytes),
        }),
      );

      bTarget.push(
        makeOp(replicas.b, ++counterB, ++lamport, {
          type: 'insert',
          parent: targetParentHex,
          node: childB,
          orderKey: orderKeyFromPosition(position),
        }),
      );
      bTarget.push(
        makeOp(replicas.b, ++counterB, ++lamport, {
          type: 'payload',
          node: childB,
          payload: payloadBytesFromSeed(40_000 + i, payloadBytes),
        }),
      );
    }

    for (let i = 0; i < otherCount; i += 1) {
      const childA = nodeIdFromInt(sharedTarget + 2 * uniqueTarget + i + 1);
      const childB = nodeIdFromInt(sharedTarget + 2 * uniqueTarget + otherCount + i + 1);

      aOther.push(
        makeOp(replicas.x, ++counterX, ++lamport, {
          type: 'insert',
          parent: otherParentHex,
          node: childA,
          orderKey: orderKeyFromPosition(i),
        }),
      );
      aOther.push(
        makeOp(replicas.x, ++counterX, ++lamport, {
          type: 'payload',
          node: childA,
          payload: payloadBytesFromSeed(50_000 + i, payloadBytes),
        }),
      );

      bOther.push(
        makeOp(replicas.y, ++counterY, ++lamport, {
          type: 'insert',
          parent: otherParentHex,
          node: childB,
          orderKey: orderKeyFromPosition(i),
        }),
      );
      bOther.push(
        makeOp(replicas.y, ++counterY, ++lamport, {
          type: 'payload',
          node: childB,
          payload: payloadBytesFromSeed(60_000 + i, payloadBytes),
        }),
      );
    }

    const opsA = [...sharedOps, ...aTarget, ...aOther];
    const opsB = [...sharedOps, ...bTarget, ...bOther];
    return {
      name: `sync-children-payloads-${size}`,
      opsA,
      opsB,
      filter: { children: { parent: nodeIdToBytes16(targetParentHex) } },
      totalOps: aTarget.length + bTarget.length,
      extra: {
        nodesPerPeer: size,
        payloadBytes,
        targetCount,
        otherCount,
        sharedTarget,
        uniqueTarget,
        parentPayloadRefresh: true,
      },
      expectedFinalOpsA: opsA.length + bTarget.length,
      expectedFinalOpsB: opsB.length + aTarget.length,
    };
  }

  if (workload === 'sync-children-payloads-cold-start') {
    const targetParentHex = 'a0'.repeat(16);
    const otherParentHex = 'b0'.repeat(16);
    const targetCount = Math.floor(size / 2);
    const otherCount = size - targetCount;
    let lamport = 0;
    let counterS = 0;
    let counterB = 0;
    let counterY = 0;

    const scopeRootInsert = makeOp(replicas.s, ++counterS, ++lamport, {
      type: 'insert',
      parent: root,
      node: targetParentHex,
      orderKey: orderKeyFromPosition(0),
    });
    const otherParentInsert = makeOp(replicas.s, ++counterS, ++lamport, {
      type: 'insert',
      parent: root,
      node: otherParentHex,
      orderKey: orderKeyFromPosition(1),
    });
    const scopeRootPayload = makeOp(replicas.b, ++counterB, ++lamport, {
      type: 'payload',
      node: targetParentHex,
      payload: payloadBytesFromSeed(20_000, payloadBytes),
    });

    const opsA: Operation[] = [scopeRootInsert];
    const opsB: Operation[] = [scopeRootInsert, otherParentInsert, scopeRootPayload];

    for (let i = 0; i < targetCount; i += 1) {
      const child = nodeIdFromInt(i + 1);
      opsB.push(
        makeOp(replicas.b, ++counterB, ++lamport, {
          type: 'insert',
          parent: targetParentHex,
          node: child,
          orderKey: orderKeyFromPosition(i),
        }),
      );
      opsB.push(
        makeOp(replicas.b, ++counterB, ++lamport, {
          type: 'payload',
          node: child,
          payload: payloadBytesFromSeed(30_000 + i, payloadBytes),
        }),
      );
    }

    for (let i = 0; i < otherCount; i += 1) {
      const child = nodeIdFromInt(targetCount + i + 1);
      opsB.push(
        makeOp(replicas.y, ++counterY, ++lamport, {
          type: 'insert',
          parent: otherParentHex,
          node: child,
          orderKey: orderKeyFromPosition(i),
        }),
      );
      opsB.push(
        makeOp(replicas.y, ++counterY, ++lamport, {
          type: 'payload',
          node: child,
          payload: payloadBytesFromSeed(40_000 + i, payloadBytes),
        }),
      );
    }

    const transferredOps = 1 + targetCount * 2;
    return {
      name: `sync-children-payloads-cold-start-${size}`,
      opsA,
      opsB,
      filter: { children: { parent: nodeIdToBytes16(targetParentHex) } },
      totalOps: transferredOps,
      extra: {
        nodesPerPeer: size,
        payloadBytes,
        targetCount,
        otherCount,
        coldStart: true,
        knownScopeRoot: true,
        parentPayloadRefresh: true,
      },
      expectedFinalOpsA: opsA.length + transferredOps,
      expectedFinalOpsB: opsB.length,
    };
  }

  if (workload === 'sync-children-cold-start') {
    const targetParentHex = 'a0'.repeat(16);
    const otherParentHex = 'b0'.repeat(16);
    const targetCount = Math.floor(size / 2);
    const otherCount = size - targetCount;

    let lamport = 0;
    let counterS = 0;
    let counterB = 0;
    let counterY = 0;

    const scopeRootInsert = makeOp(replicas.s, ++counterS, ++lamport, {
      type: 'insert',
      parent: root,
      node: targetParentHex,
      orderKey: orderKeyFromPosition(0),
    });
    const otherParentInsert = makeOp(replicas.s, ++counterS, ++lamport, {
      type: 'insert',
      parent: root,
      node: otherParentHex,
      orderKey: orderKeyFromPosition(1),
    });

    const opsA: Operation[] = [scopeRootInsert];
    const opsB: Operation[] = [scopeRootInsert, otherParentInsert];

    for (let i = 0; i < targetCount; i += 1) {
      opsB.push(
        makeOp(replicas.b, ++counterB, ++lamport, {
          type: 'insert',
          parent: targetParentHex,
          node: nodeIdFromInt(i + 1),
          orderKey: orderKeyFromPosition(i),
        }),
      );
    }

    for (let i = 0; i < otherCount; i += 1) {
      opsB.push(
        makeOp(replicas.y, ++counterY, ++lamport, {
          type: 'insert',
          parent: otherParentHex,
          node: nodeIdFromInt(targetCount + i + 1),
          orderKey: orderKeyFromPosition(i),
        }),
      );
    }

    return {
      name: `sync-children-cold-start-${size}`,
      opsA,
      opsB,
      filter: { children: { parent: nodeIdToBytes16(targetParentHex) } },
      totalOps: targetCount,
      extra: {
        nodesPerPeer: size,
        targetCount,
        otherCount,
        coldStart: true,
        knownScopeRoot: true,
      },
      expectedFinalOpsA: opsA.length + targetCount,
      expectedFinalOpsB: opsB.length,
    };
  }

  // sync-children
  const targetParentHex = 'a0'.repeat(16);
  const otherParentHex = 'b0'.repeat(16);
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
      makeOp(replicas.s, counter, lamport, {
        type: 'insert',
        parent: targetParentHex,
        node: nodeIdFromInt(counter),
        orderKey: orderKeyFromPosition(i),
      }),
    );
  }

  for (let i = 0; i < uniqueTarget; i++) {
    lamport += 1;
    const counter = i + 1;
    aTarget.push(
      makeOp(replicas.a, counter, lamport, {
        type: 'insert',
        parent: targetParentHex,
        node: nodeIdFromInt(sharedTarget + counter),
        orderKey: orderKeyFromPosition(sharedTarget + i),
      }),
    );
    bTarget.push(
      makeOp(replicas.b, counter, lamport, {
        type: 'insert',
        parent: targetParentHex,
        node: nodeIdFromInt(sharedTarget + uniqueTarget + counter),
        orderKey: orderKeyFromPosition(sharedTarget + i),
      }),
    );
  }

  for (let i = 0; i < otherCount; i++) {
    lamport += 1;
    aOther.push(
      makeOp(replicas.x, i + 1, lamport, {
        type: 'insert',
        parent: otherParentHex,
        node: nodeIdFromInt(sharedTarget + 2 * uniqueTarget + i + 1),
        orderKey: orderKeyFromPosition(i),
      }),
    );
    bOther.push(
      makeOp(replicas.y, i + 1, lamport, {
        type: 'insert',
        parent: otherParentHex,
        node: nodeIdFromInt(sharedTarget + 2 * uniqueTarget + otherCount + i + 1),
        orderKey: orderKeyFromPosition(i),
      }),
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

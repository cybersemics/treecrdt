import path from 'node:path';

import type { Operation, ReplicaId } from '@treecrdt/interface';
import type { TreecrdtEngine } from '@treecrdt/interface/engine';

import type { BenchmarkResult } from './index.js';
import {
  parseFlagValue,
  parseNonNegativeIntFlag,
  parsePositiveIntFlag,
  payloadBytesFromSeed,
  replicaFromLabel,
} from './helpers.js';
import { buildFanoutInsertTreeOps, nodeIdFromInt } from './sync.js';
import { writeResult, type BenchmarkOutput } from './node.js';
import { quantile, summarizeSamples } from './stats.js';

export {
  parseFlagValue,
  parseNonNegativeIntFlag,
  parsePositiveIntFlag,
  payloadBytesFromSeed,
  replicaFromLabel,
} from './helpers.js';

export type HotWriteBenchKind = 'payload-edit' | 'insert-sibling' | 'move-leaf' | 'move-subtree';
export type HotWriteConfigEntry = [count: number, iterations: number];

export const ALL_HOT_WRITE_BENCHES = [
  'payload-edit',
  'insert-sibling',
  'move-leaf',
  'move-subtree',
] as const satisfies readonly HotWriteBenchKind[];

export const DEFAULT_HOT_WRITE_CONFIG: ReadonlyArray<HotWriteConfigEntry> = [
  [10_000, 3],
  [100_000, 1],
] as const;

export const DEFAULT_HOT_WRITE_FANOUT = 10;
export const DEFAULT_HOT_WRITE_PAYLOAD_BYTES = 512;
export const HOT_WRITE_ROOT = '0'.repeat(32);

export type HotWriteSeed = {
  ops: Operation[];
} & HotWriteSeedTargets;

export type HotWriteSeedTargets = {
  targetParent: string;
  payloadNode: string;
  moveLeafNode: string;
  moveLeafOriginalParent: string;
  moveNode: string;
  moveNodeOriginalParent: string;
};

export type HotWriteWorkload = {
  name: string;
  totalOps: number;
  run: (
    engine: TreecrdtEngine,
    ctx: { writeIndex: number; totalWrites: number },
  ) => Promise<{ extra?: Record<string, unknown> } | void>;
};

export function parseHotWriteConfigFromArgv(argv: string[]): Array<HotWriteConfigEntry> | null {
  let customConfig: Array<HotWriteConfigEntry> | null = null;
  const defaultIterations = Math.max(1, Number(process.env.BENCH_ITERATIONS ?? '1') || 1);
  for (const arg of argv) {
    if (arg.startsWith('--count=')) {
      const val = arg.slice('--count='.length).trim();
      const count = val ? Number(val) : 10_000;
      customConfig = [[Number.isFinite(count) && count > 0 ? count : 10_000, defaultIterations]];
      break;
    }
    if (arg.startsWith('--counts=')) {
      const vals = arg
        .slice('--counts='.length)
        .split(',')
        .map((s) => s.trim())
        .filter((s) => s.length > 0);
      const parsed = vals
        .map((s) => {
          const n = Number(s);
          return Number.isFinite(n) && n > 0 ? n : null;
        })
        .filter((n): n is number => n != null)
        .map((count) => [count, defaultIterations] as HotWriteConfigEntry);
      if (parsed.length > 0) customConfig = parsed;
      break;
    }
  }
  return customConfig;
}

export function parseHotWriteKinds(argv: string[]): HotWriteBenchKind[] {
  const raw =
    parseFlagValue(argv, '--benches') ??
    parseFlagValue(argv, '--bench') ??
    process.env.HOT_WRITE_BENCHES ??
    process.env.HOT_WRITE_BENCH;
  if (!raw) return Array.from(ALL_HOT_WRITE_BENCHES);

  const seen = new Set<HotWriteBenchKind>();
  for (const value of raw
    .split(',')
    .map((part) => part.trim())
    .filter((part) => part.length > 0)) {
    if (!(ALL_HOT_WRITE_BENCHES as readonly string[]).includes(value)) {
      throw new Error(
        `invalid hot-write bench "${value}", expected one of: ${ALL_HOT_WRITE_BENCHES.join(', ')}`,
      );
    }
    seen.add(value as HotWriteBenchKind);
  }
  return seen.size > 0 ? Array.from(seen) : Array.from(ALL_HOT_WRITE_BENCHES);
}

export function buildHotWriteSeedTargets(opts: {
  size: number;
  fanout: number;
}): HotWriteSeedTargets {
  if (!Number.isInteger(opts.size) || opts.size <= opts.fanout) {
    throw new Error(`hot-write seed requires size > fanout (${opts.fanout})`);
  }
  const targetParent = nodeIdFromInt(1);
  const payloadNode = nodeIdFromInt(opts.fanout + 1);
  const moveLeafNode = nodeIdFromInt(opts.size);
  const moveLeafOriginalParent = nodeIdFromInt(Math.floor((opts.size - 2) / opts.fanout) + 1);
  const moveNode = nodeIdFromInt(opts.fanout);
  const moveNodeOriginalParent = HOT_WRITE_ROOT;

  return {
    targetParent,
    payloadNode,
    moveLeafNode,
    moveLeafOriginalParent,
    moveNode,
    moveNodeOriginalParent,
  };
}

export function buildHotWriteSeed(opts: {
  size: number;
  fanout: number;
  payloadBytes: number;
  seed?: HotWriteSeedTargets;
}): HotWriteSeed {
  const seed = opts.seed ?? buildHotWriteSeedTargets({ size: opts.size, fanout: opts.fanout });
  const replica = replicaFromLabel('bench');
  const insertOps = buildFanoutInsertTreeOps({
    replica,
    size: opts.size,
    fanout: opts.fanout,
    root: HOT_WRITE_ROOT,
  });

  const payloadOp: Operation = {
    meta: { id: { replica, counter: insertOps.length + 1 }, lamport: insertOps.length + 1 },
    kind: {
      type: 'payload',
      node: seed.payloadNode,
      payload: payloadBytesFromSeed(10_000, opts.payloadBytes),
    },
  };

  return {
    ...seed,
    ops: [...insertOps, payloadOp],
  };
}

export function createHotWriteWorkload(opts: {
  bench: HotWriteBenchKind;
  size: number;
  fanout: number;
  payloadBytes: number;
  seed: HotWriteSeedTargets;
  writesPerSample?: number;
  warmupWrites?: number;
}): HotWriteWorkload {
  const targetParent = opts.seed.targetParent;
  const totalWrites = (opts.writesPerSample ?? 1) + (opts.warmupWrites ?? 0);
  const nameSuffix =
    totalWrites > 1 ? `-warm${opts.warmupWrites ?? 0}-repeat${opts.writesPerSample ?? 1}` : '';
  if (opts.bench === 'payload-edit') {
    const replica = replicaFromLabel('payload-writer');
    const payloadNode = opts.seed.payloadNode;
    return {
      name: `hot-write-payload-edit-fanout${opts.fanout}-${opts.size}${nameSuffix}`,
      totalOps: 1,
      run: async (engine, ctx) => {
        const expectedPayload = payloadBytesFromSeed(90_000 + ctx.writeIndex, opts.payloadBytes);
        const mutationStart = performance.now();
        const op = await engine.local.payload(replica, payloadNode, expectedPayload);
        const mutationMs = performance.now() - mutationStart;
        if (op.kind.type !== 'payload' || op.kind.node !== payloadNode) {
          throw new Error('payload edit did not return the target node');
        }
        const verifyStart = performance.now();
        const stored = await engine.tree.getPayload(payloadNode);
        const verifyMs = performance.now() - verifyStart;
        if (!stored || !equalBytes(stored, expectedPayload)) {
          throw new Error('payload edit did not persist the expected bytes');
        }
        return {
          extra: {
            payloadNode,
            payloadBytes: expectedPayload.length,
            mutationMs,
            verifyMs,
          },
        };
      },
    };
  }

  if (opts.bench === 'insert-sibling') {
    const replica = replicaFromLabel('insert-writer');
    return {
      name: `hot-write-insert-sibling-fanout${opts.fanout}-${opts.size}${nameSuffix}`,
      totalOps: 1,
      run: async (engine, ctx) => {
        const newNode = nodeIdFromInt(opts.size + 10_000 + ctx.writeIndex + 1);
        const payload = payloadBytesFromSeed(91_000 + ctx.writeIndex, opts.payloadBytes);
        const mutationStart = performance.now();
        const op = await engine.local.insert(
          replica,
          targetParent,
          newNode,
          { type: 'last' },
          payload,
        );
        const mutationMs = performance.now() - mutationStart;
        if (op.kind.type !== 'insert' || op.kind.node !== newNode) {
          throw new Error('insert did not return the new node');
        }
        const verifyStart = performance.now();
        const parent = await engine.tree.parent(newNode);
        if (parent !== targetParent) {
          throw new Error(
            `inserted node parent mismatch: expected ${targetParent}, got ${String(parent)}`,
          );
        }
        const stored = await engine.tree.getPayload(newNode);
        const verifyMs = performance.now() - verifyStart;
        if (!stored || !equalBytes(stored, payload)) {
          throw new Error('inserted node payload missing');
        }
        return {
          extra: {
            targetParent,
            insertedNode: newNode,
            payloadBytes: payload.length,
            mutationMs,
            verifyMs,
          },
        };
      },
    };
  }

  const replica = replicaFromLabel(
    opts.bench === 'move-leaf' ? 'move-leaf-writer' : 'move-subtree-writer',
  );
  const moveTargets =
    totalWrites === 1
      ? [
          {
            node: opts.bench === 'move-leaf' ? opts.seed.moveLeafNode : opts.seed.moveNode,
            originalParent:
              opts.bench === 'move-leaf'
                ? opts.seed.moveLeafOriginalParent
                : opts.seed.moveNodeOriginalParent,
          },
        ]
      : collectMoveTargets({
          bench: opts.bench,
          size: opts.size,
          fanout: opts.fanout,
          totalWrites,
        });
  return {
    name: `hot-write-${opts.bench}-fanout${opts.fanout}-${opts.size}${nameSuffix}`,
    totalOps: 1,
    run: async (engine, ctx) => {
      const target = moveTargets[ctx.writeIndex];
      if (!target) throw new Error(`missing ${opts.bench} move target for write ${ctx.writeIndex}`);
      const moveNode = target.node;
      const mutationStart = performance.now();
      const op = await engine.local.move(replica, moveNode, targetParent, { type: 'last' });
      const mutationMs = performance.now() - mutationStart;
      if (op.kind.type !== 'move' || op.kind.node !== moveNode) {
        throw new Error('move did not return the moved node');
      }
      const verifyStart = performance.now();
      const parent = await engine.tree.parent(moveNode);
      if (parent !== targetParent) {
        throw new Error(
          `moved node parent mismatch: expected ${targetParent}, got ${String(parent)}`,
        );
      }
      if (!(await engine.tree.exists(moveNode))) {
        throw new Error('moved node disappeared after subtree move');
      }
      const verifyMs = performance.now() - verifyStart;
      return {
        extra: {
          movedNode: moveNode,
          movedFromParent: target.originalParent,
          movedToParent: targetParent,
          mutationMs,
          verifyMs,
        },
      };
    },
  };
}

export async function runHotWriteBenchmarks(opts: {
  repoRoot: string;
  implementation: string;
  storage: string;
  config: ReadonlyArray<HotWriteConfigEntry>;
  benches: readonly HotWriteBenchKind[];
  fanout: number;
  payloadBytes: number;
  writesPerSample?: number;
  warmupWrites?: number;
  openSeededEngine: (args: {
    bench: HotWriteBenchKind;
    size: number;
    seed: HotWriteSeedTargets;
    getSeed: () => HotWriteSeed;
    sampleIndex: number;
  }) => Promise<TreecrdtEngine>;
  outDirName?: string;
}): Promise<BenchmarkOutput[]> {
  const outputs: BenchmarkOutput[] = [];

  for (const [size, iterations] of opts.config) {
    const seed = buildHotWriteSeedTargets({ size, fanout: opts.fanout });
    let fullSeed: HotWriteSeed | null = null;
    const getSeed = () => {
      if (fullSeed == null) {
        fullSeed = buildHotWriteSeed({
          size,
          fanout: opts.fanout,
          payloadBytes: opts.payloadBytes,
          seed,
        });
      }
      return fullSeed;
    };
    for (const bench of opts.benches) {
      const workload = createHotWriteWorkload({
        bench,
        size,
        fanout: opts.fanout,
        payloadBytes: opts.payloadBytes,
        seed,
        writesPerSample: opts.writesPerSample,
        warmupWrites: opts.warmupWrites,
      });
      const result = await runHotWriteBenchmark({
        sampleDocs: iterations,
        writesPerSample: opts.writesPerSample ?? 1,
        warmupWrites: opts.warmupWrites ?? 0,
        openSeededEngine: (sampleIndex) =>
          opts.openSeededEngine({ bench, size, seed, getSeed, sampleIndex }),
        workload,
      });
      const outFile = path.join(
        opts.repoRoot,
        'benchmarks',
        opts.outDirName ?? 'hot-write',
        `${opts.implementation}-${opts.storage}-${result.name}.json`,
      );
      outputs.push(
        await writeResult(result, {
          implementation: opts.implementation,
          storage: opts.storage,
          workload: result.name,
          outFile,
          extra: {
            count: size,
            bench,
            fanout: opts.fanout,
            payloadBytes: opts.payloadBytes,
            writesPerSample: opts.writesPerSample ?? 1,
            warmupWrites: opts.warmupWrites ?? 0,
            ...result.extra,
          },
        }),
      );
    }
  }

  return outputs;
}

async function runHotWriteBenchmark(opts: {
  sampleDocs: number;
  writesPerSample: number;
  warmupWrites: number;
  openSeededEngine: (sampleIndex: number) => Promise<TreecrdtEngine>;
  workload: HotWriteWorkload;
}): Promise<BenchmarkResult> {
  const durations: number[] = [];
  let lastExtra: Record<string, unknown> | undefined;
  const numericExtraSamples = new Map<string, number[]>();

  for (let sampleIndex = 0; sampleIndex < opts.sampleDocs; sampleIndex += 1) {
    const engine = await opts.openSeededEngine(sampleIndex);
    try {
      const totalWrites = opts.warmupWrites + opts.writesPerSample;
      for (let writeIndex = 0; writeIndex < totalWrites; writeIndex += 1) {
        const start = performance.now();
        const runResult = await opts.workload.run(engine, { writeIndex, totalWrites });
        const end = performance.now();
        if (runResult?.extra) lastExtra = runResult.extra;
        if (writeIndex >= opts.warmupWrites) {
          durations.push(end - start);
          if (runResult?.extra) {
            for (const [key, value] of Object.entries(runResult.extra)) {
              if (typeof value !== 'number' || !Number.isFinite(value)) continue;
              const values = numericExtraSamples.get(key) ?? [];
              values.push(value);
              numericExtraSamples.set(key, values);
            }
          }
        }
      }
    } finally {
      await engine.close();
    }
  }

  const durationMs = quantile(durations, 0.5);
  const durationSummary = summarizeSamples(durations);
  const summarizedNumericExtras = Object.fromEntries(
    [...numericExtraSamples.entries()].map(([key, values]) => [
      `${key}Summary`,
      summarizeSamples(values),
    ]),
  );
  const totalOps = opts.workload.totalOps;
  return {
    name: opts.workload.name,
    totalOps,
    durationMs,
    opsPerSec: durationMs > 0 ? (totalOps / durationMs) * 1000 : Infinity,
    extra: {
      ...(lastExtra ?? {}),
      iterations: durations.length,
      sampleDocs: opts.sampleDocs,
      writesPerSample: opts.writesPerSample,
      warmupWrites: opts.warmupWrites,
      warmupIterations: opts.warmupWrites,
      samplesMs: durations,
      minMs: durationSummary.min,
      meanMs: durationSummary.mean,
      p95Ms: durationSummary.p95,
      p99Ms: durationSummary.p99,
      maxMs: durationSummary.max,
      ...summarizedNumericExtras,
    },
  };
}

function equalBytes(a: Uint8Array, b: Uint8Array): boolean {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i += 1) {
    if (a[i] !== b[i]) return false;
  }
  return true;
}

function collectMoveTargets(opts: {
  bench: 'move-leaf' | 'move-subtree';
  size: number;
  fanout: number;
  totalWrites: number;
}): Array<{ node: string; originalParent: string }> {
  const targets: Array<{ node: string; originalParent: string }> = [];
  for (
    let nodeIndex = opts.size;
    nodeIndex >= 1 && targets.length < opts.totalWrites;
    nodeIndex -= 1
  ) {
    if (opts.bench === 'move-leaf' && hasBalancedChildren(nodeIndex, opts.size, opts.fanout)) {
      continue;
    }
    if (opts.bench === 'move-subtree') {
      if (!hasBalancedChildren(nodeIndex, opts.size, opts.fanout)) continue;
      if (nodeIndex <= opts.fanout) continue;
    }
    if (rootChildIndex(nodeIndex, opts.fanout) === 1) continue;
    const parentIndex = balancedTreeParentIndex(nodeIndex, opts.fanout);
    if (parentIndex == null) continue;
    targets.push({
      node: nodeIdFromInt(nodeIndex),
      originalParent: nodeIdFromInt(parentIndex),
    });
  }
  if (targets.length < opts.totalWrites) {
    throw new Error(
      `not enough ${opts.bench} targets for ${opts.totalWrites} writes in size ${opts.size}`,
    );
  }
  return targets;
}

function hasBalancedChildren(nodeIndex: number, size: number, fanout: number): boolean {
  return nodeIndex * fanout + 1 <= size;
}

function balancedTreeParentIndex(nodeIndex: number, fanout: number): number | null {
  if (nodeIndex <= 0) throw new Error(`invalid balanced-tree node index: ${nodeIndex}`);
  if (nodeIndex <= fanout) return null;
  return Math.floor((nodeIndex - (fanout + 1)) / fanout) + 1;
}

function rootChildIndex(nodeIndex: number, fanout: number): number {
  let cursor = nodeIndex;
  while (cursor > fanout) {
    const parent = balancedTreeParentIndex(cursor, fanout);
    if (parent == null) break;
    cursor = parent;
  }
  return cursor;
}

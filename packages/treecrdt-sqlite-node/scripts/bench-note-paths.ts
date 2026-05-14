import fs from 'node:fs/promises';
import path from 'node:path';

import Database from 'better-sqlite3';

import {
  buildFanoutInsertTreeOps,
  nodeIdFromInt,
  quantile,
  runBenchmark,
  type BenchmarkWorkload,
} from '@justtemporary/benchmark';
import { repoRootFromImportMeta, writeResult } from '@justtemporary/benchmark/node';
import type { Operation, ReplicaId } from '@justtemporary/interface';
import { nodeIdToBytes16 } from '@justtemporary/interface/ids';

import { createTreecrdtClient, createSqliteNodeApi, loadTreecrdtExtension } from '../dist/index.js';

type StorageKind = 'memory' | 'file';
type NotePathBenchKind = 'read-children-payloads' | 'insert-into-large-tree';
type ConfigEntry = [number, number];

const ALL_NOTE_PATH_BENCHES = [
  'read-children-payloads',
  'insert-into-large-tree',
] as const satisfies readonly NotePathBenchKind[];
const NOTE_PATH_BENCH_CONFIG: ReadonlyArray<ConfigEntry> = [
  [10_000, 3],
  [50_000, 2],
];

const STORAGES: readonly StorageKind[] = ['memory', 'file'];
const DEFAULT_FANOUT = 10;
const DEFAULT_PAGE_SIZE = 10;
const DEFAULT_PAYLOAD_BYTES = 512;
const ROOT = '0'.repeat(32);

function envInt(name: string): number | undefined {
  const raw = process.env[name];
  if (raw == null || raw === '') return undefined;
  const n = Number(raw);
  return Number.isFinite(n) ? n : undefined;
}

function parseConfigFromArgv(argv: string[]): Array<ConfigEntry> | null {
  let customConfig: Array<ConfigEntry> | null = null;
  const defaultIterations = Math.max(1, envInt('BENCH_ITERATIONS') ?? 1);
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
        .map((count) => [count, defaultIterations] as ConfigEntry);
      if (parsed.length > 0) customConfig = parsed;
      break;
    }
  }
  return customConfig;
}

function parseFlagValue(argv: string[], flag: string): string | undefined {
  const prefix = `${flag}=`;
  const raw = argv.find((arg) => arg.startsWith(prefix));
  return raw ? raw.slice(prefix.length).trim() : undefined;
}

function parsePositiveIntFlag(
  argv: string[],
  flag: string,
  envName: string,
  fallback: number,
): number {
  const raw = parseFlagValue(argv, flag) ?? process.env[envName];
  if (!raw) return fallback;
  const value = Number(raw);
  if (!Number.isInteger(value) || value <= 0) {
    throw new Error(`invalid ${flag} value "${raw}", expected a positive integer`);
  }
  return value;
}

function parseKinds(argv: string[]): NotePathBenchKind[] {
  const raw =
    parseFlagValue(argv, '--benches') ??
    parseFlagValue(argv, '--bench') ??
    process.env.NOTE_PATH_BENCHES ??
    process.env.NOTE_PATH_BENCH;
  if (!raw) return Array.from(ALL_NOTE_PATH_BENCHES);

  const seen = new Set<NotePathBenchKind>();
  for (const value of raw
    .split(',')
    .map((part) => part.trim())
    .filter((part) => part.length > 0)) {
    if (!(ALL_NOTE_PATH_BENCHES as readonly string[]).includes(value)) {
      throw new Error(
        `invalid note-path bench "${value}", expected one of: ${ALL_NOTE_PATH_BENCHES.join(', ')}`,
      );
    }
    seen.add(value as NotePathBenchKind);
  }
  return seen.size > 0 ? Array.from(seen) : Array.from(ALL_NOTE_PATH_BENCHES);
}

function payloadBytesFromSeed(seed: number, size = DEFAULT_PAYLOAD_BYTES): Uint8Array {
  if (!Number.isInteger(seed) || seed < 0) throw new Error(`invalid payload seed: ${seed}`);
  const out = new Uint8Array(size);
  for (let i = 0; i < out.length; i += 1) {
    out[i] = (seed + i * 31) % 251;
  }
  return out;
}

function replicaFromLabel(label: string): Uint8Array {
  const encoded = new TextEncoder().encode(label);
  if (encoded.length === 0) throw new Error('label must not be empty');
  const out = new Uint8Array(32);
  for (let i = 0; i < out.length; i += 1) out[i] = encoded[i % encoded.length]!;
  return out;
}

function makePayloadOp(
  replica: ReplicaId,
  counter: number,
  lamport: number,
  node: string,
  payload: Uint8Array,
): Operation {
  return {
    meta: { id: { replica, counter }, lamport },
    kind: { type: 'payload', node, payload },
  };
}

function buildNotePathSeedOps(opts: { size: number; fanout: number; payloadBytes: number }) {
  const replica = replicaFromLabel('bench');
  const insertOps = buildFanoutInsertTreeOps({
    replica,
    size: opts.size,
    fanout: opts.fanout,
    root: ROOT,
  });

  const targetParent = nodeIdFromInt(1);
  const targetChildrenCount = Math.min(opts.fanout, Math.max(0, opts.size - opts.fanout));
  const targetChildren = Array.from({ length: targetChildrenCount }, (_, i) =>
    nodeIdFromInt(opts.fanout + i + 1),
  );

  const payloadOps: Operation[] = [];
  let counter = insertOps.length;
  let lamport = insertOps.length;

  payloadOps.push(
    makePayloadOp(
      replica,
      ++counter,
      ++lamport,
      targetParent,
      payloadBytesFromSeed(10_000, opts.payloadBytes),
    ),
  );
  for (let i = 0; i < targetChildren.length; i += 1) {
    payloadOps.push(
      makePayloadOp(
        replica,
        ++counter,
        ++lamport,
        targetChildren[i]!,
        payloadBytesFromSeed(20_000 + i, opts.payloadBytes),
      ),
    );
  }

  return {
    ops: [...insertOps, ...payloadOps],
    targetParent,
    targetChildren,
  };
}

async function openSeededClient(opts: {
  repoRoot: string;
  storage: StorageKind;
  bench: NotePathBenchKind;
  size: number;
  seedOps: Operation[];
}): Promise<Awaited<ReturnType<typeof createTreecrdtClient>>> {
  const dbPath =
    opts.storage === 'memory'
      ? ':memory:'
      : path.join(
          opts.repoRoot,
          'tmp',
          'sqlite-node-note-paths',
          `${opts.bench}-${opts.size}-${crypto.randomUUID()}.db`,
        );

  if (opts.storage === 'file') {
    await fs.mkdir(path.dirname(dbPath), { recursive: true });
  }

  const db = new Database(dbPath);
  loadTreecrdtExtension(db);
  const api = createSqliteNodeApi(db);
  await api.setDocId('treecrdt-note-paths-bench');
  await api.appendOps!(opts.seedOps, nodeIdToBytes16, (replica) => replica);

  const client = await createTreecrdtClient(db, { docId: 'treecrdt-note-paths-bench' });
  return {
    ...client,
    close: async () => {
      await client.close();
      if (opts.storage === 'file') {
        await fs.rm(dbPath).catch(() => {});
      }
    },
  };
}

function readChildrenPayloadsWorkload(opts: {
  size: number;
  targetParent: string;
  expectedChildren: number;
  pageSize: number;
  payloadBytes: number;
  fanout: number;
}): BenchmarkWorkload {
  const visibleChildren = Math.min(opts.expectedChildren, opts.pageSize);
  return {
    name: `read-children-payloads-fanout${opts.fanout}-${opts.size}`,
    totalOps: visibleChildren + 1,
    iterations: 1,
    warmupIterations: 0,
    run: async (adapter: any) => {
      const rows = await adapter.tree.childrenPage(opts.targetParent, null, opts.pageSize);
      if (!Array.isArray(rows)) throw new Error('childrenPage did not return rows');
      if (rows.length !== visibleChildren) {
        throw new Error(`expected ${visibleChildren} child rows, got ${rows.length}`);
      }

      const parentPayload = await adapter.tree.getPayload(opts.targetParent);
      if (!(parentPayload instanceof Uint8Array) || parentPayload.length !== opts.payloadBytes) {
        throw new Error('target parent payload missing');
      }

      const payloads = await Promise.all(
        rows.map((row: { node: string }) => adapter.tree.getPayload(row.node)),
      );
      if (
        payloads.some(
          (payload) => !(payload instanceof Uint8Array) || payload.length !== opts.payloadBytes,
        )
      ) {
        throw new Error('one or more child payloads missing');
      }

      return {
        extra: {
          returnedChildren: rows.length,
          targetChildren: opts.expectedChildren,
          payloadBytes: opts.payloadBytes,
          pageSize: opts.pageSize,
          targetParent: opts.targetParent,
        },
      };
    },
  };
}

function insertIntoLargeTreeWorkload(opts: {
  size: number;
  targetParent: string;
  payloadBytes: number;
  fanout: number;
}): BenchmarkWorkload {
  const replica = replicaFromLabel('writer');
  const newNode = nodeIdFromInt(opts.size + 10_000);
  const payload = payloadBytesFromSeed(90_000, opts.payloadBytes);

  return {
    name: `insert-into-balanced-tree-fanout${opts.fanout}-${opts.size}`,
    totalOps: 1,
    iterations: 1,
    warmupIterations: 0,
    run: async (adapter: any) => {
      const op = await adapter.local.insert(
        replica,
        opts.targetParent,
        newNode,
        { type: 'last' },
        payload,
      );
      if (op.kind.type !== 'insert' || op.kind.node !== newNode) {
        throw new Error('insert did not return the new node');
      }

      const parent = await adapter.tree.parent(newNode);
      if (parent !== opts.targetParent) {
        throw new Error(
          `inserted node parent mismatch: expected ${opts.targetParent}, got ${String(parent)}`,
        );
      }

      const storedPayload = await adapter.tree.getPayload(newNode);
      if (!(storedPayload instanceof Uint8Array) || storedPayload.length !== opts.payloadBytes) {
        throw new Error('inserted node payload missing');
      }

      return {
        extra: {
          payloadBytes: opts.payloadBytes,
          targetParent: opts.targetParent,
          insertedNode: newNode,
        },
      };
    },
  };
}

async function runWorkload(opts: {
  repoRoot: string;
  storage: StorageKind;
  bench: NotePathBenchKind;
  size: number;
  iterations: number;
  fanout: number;
  payloadBytes: number;
  seedOps: Operation[];
  workload: BenchmarkWorkload;
}) {
  let result: Awaited<ReturnType<typeof runBenchmark>>;
  const adapterFactory = async () =>
    (await openSeededClient({
      repoRoot: opts.repoRoot,
      storage: opts.storage,
      bench: opts.bench,
      size: opts.size,
      seedOps: opts.seedOps,
    })) as any;

  if (opts.iterations > 1) {
    const durations: number[] = [];
    let lastExtra: Record<string, unknown> | undefined;
    for (let i = 0; i < opts.iterations; i += 1) {
      const next = await runBenchmark(adapterFactory as any, opts.workload);
      durations.push(next.durationMs);
      lastExtra = next.extra;
    }
    const durationMs = quantile(durations, 0.5);
    const totalOps = opts.workload.totalOps ?? -1;
    result = {
      name: opts.workload.name,
      totalOps,
      durationMs,
      opsPerSec:
        totalOps > 0 && durationMs > 0
          ? (totalOps / durationMs) * 1000
          : durationMs > 0
            ? 1000 / durationMs
            : Infinity,
      extra: {
        ...(lastExtra ?? {}),
        iterations: opts.iterations,
        samplesMs: durations,
        p95Ms: quantile(durations, 0.95),
        minMs: Math.min(...durations),
        maxMs: Math.max(...durations),
      },
    };
  } else {
    result = await runBenchmark(adapterFactory as any, opts.workload);
  }

  const outFile = path.join(
    opts.repoRoot,
    'benchmarks',
    'sqlite-node-note-paths',
    `${opts.storage}-${result.name}.json`,
  );
  const payload = await writeResult(result, {
    implementation: 'sqlite-node',
    storage: opts.storage,
    workload: result.name,
    outFile,
    extra: {
      count: opts.size,
      bench: opts.bench,
      fanout: opts.fanout,
      payloadBytes: opts.payloadBytes,
      ...result.extra,
    },
  });
  console.log(JSON.stringify(payload, null, 2));
}

async function main() {
  const repoRoot = repoRootFromImportMeta(import.meta.url, 3);
  const argv = process.argv.slice(2);
  const config = parseConfigFromArgv(argv) ?? [...NOTE_PATH_BENCH_CONFIG];
  const benches = parseKinds(argv);
  const fanout = parsePositiveIntFlag(argv, '--fanout', 'NOTE_PATH_BENCH_FANOUT', DEFAULT_FANOUT);
  const pageSize = parsePositiveIntFlag(
    argv,
    '--page-size',
    'NOTE_PATH_BENCH_PAGE_SIZE',
    DEFAULT_PAGE_SIZE,
  );
  const payloadBytes = parsePositiveIntFlag(
    argv,
    '--payload-bytes',
    'NOTE_PATH_BENCH_PAYLOAD_BYTES',
    DEFAULT_PAYLOAD_BYTES,
  );

  for (const [size, iterations] of config) {
    const seed = buildNotePathSeedOps({ size, fanout, payloadBytes });
    const readWorkload = readChildrenPayloadsWorkload({
      size,
      targetParent: seed.targetParent,
      expectedChildren: seed.targetChildren.length,
      pageSize,
      payloadBytes,
      fanout,
    });
    const insertWorkload = insertIntoLargeTreeWorkload({
      size,
      targetParent: seed.targetParent,
      payloadBytes,
      fanout,
    });

    for (const storage of STORAGES) {
      if (benches.includes('read-children-payloads')) {
        await runWorkload({
          repoRoot,
          storage,
          bench: 'read-children-payloads',
          size,
          iterations,
          fanout,
          payloadBytes,
          seedOps: seed.ops,
          workload: readWorkload,
        });
      }
      if (benches.includes('insert-into-large-tree')) {
        await runWorkload({
          repoRoot,
          storage,
          bench: 'insert-into-large-tree',
          size,
          iterations,
          fanout,
          payloadBytes,
          seedOps: seed.ops,
          workload: insertWorkload,
        });
      }
    }
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});

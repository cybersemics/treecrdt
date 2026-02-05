import fs from "node:fs/promises";
import path from "node:path";
import Database from "better-sqlite3";
import {
  buildSyncBenchCase,
  DEFAULT_SYNC_BENCH_ROOT_CHILDREN_WORKLOADS,
  DEFAULT_SYNC_BENCH_WORKLOADS,
  SYNC_BENCH_DEFAULT_CODEWORDS_PER_MESSAGE,
  SYNC_BENCH_DEFAULT_MAX_CODEWORDS,
  maxLamport,
  quantile,
  type SyncBenchWorkload,
} from "@treecrdt/benchmark";
import { repoRootFromImportMeta, writeResult } from "@treecrdt/benchmark/node";
import type { Operation } from "@treecrdt/interface";
import { decodeSqliteOpRefs, decodeSqliteOps } from "@treecrdt/interface/sqlite";
import { nodeIdToBytes16 } from "@treecrdt/interface/ids";
import {
  createInMemoryConnectedPeers,
  makeQueuedSyncBackend,
  type FlushableSyncBackend,
} from "@treecrdt/sync/in-memory";
import { treecrdtSyncV0ProtobufCodec } from "@treecrdt/sync/protobuf";
import type { Filter } from "@treecrdt/sync";
import {
  createSqliteNodeApi,
  loadTreecrdtExtension,
} from "../dist/index.js";

type StorageKind = "memory" | "file";
type ConfigEntry = [number, number];

const SYNC_BENCH_CONFIG: ReadonlyArray<ConfigEntry> = [
  [100, 10],
  [1_000, 5],
  [10_000, 10],
];

const SYNC_BENCH_ROOT_CONFIG: ReadonlyArray<ConfigEntry> = [[1110, 10]];

function envInt(name: string): number | undefined {
  const raw = process.env[name];
  if (raw == null || raw === "") return undefined;
  const n = Number(raw);
  return Number.isFinite(n) ? n : undefined;
}

function parseConfigFromArgv(argv: string[]): Array<ConfigEntry> | null {
  let customConfig: Array<ConfigEntry> | null = null;
  const defaultIterations = Math.max(1, envInt("BENCH_ITERATIONS") ?? 1);
  for (const arg of argv) {
    if (arg.startsWith("--count=")) {
      const val = arg.slice("--count=".length).trim();
      const count = val ? Number(val) : 500;
      customConfig = [[Number.isFinite(count) && count > 0 ? count : 500, defaultIterations]];
      break;
    }
    if (arg.startsWith("--counts=")) {
      const vals = arg
        .slice("--counts=".length)
        .split(",")
        .map((s) => s.trim())
        .filter((s) => s.length > 0);
      const parsed = vals
        .map((s) => {
          const n = Number(s);
          return Number.isFinite(n) && n > 0 ? n : null;
        })
        .filter((n): n is number => n != null)
        .map((c) => [c, defaultIterations] as ConfigEntry);
      if (parsed.length > 0) customConfig = parsed;
      break;
    }
  }
  return customConfig;
}

type BenchCase = {
  storage: StorageKind;
  workload: SyncBenchWorkload;
  size: number;
  iterations: number;
};

type SyncBenchResult = {
  name: string;
  totalOps: number;
  durationMs: number;
  opsPerSec: number;
  extra?: Record<string, unknown>;
};

function hexToBytes(hex: string): Uint8Array {
  return nodeIdToBytes16(hex);
}

function parseOpRefs(raw: any): Uint8Array[] {
  return decodeSqliteOpRefs(raw);
}

function parseOps(raw: any): Operation[] {
  return decodeSqliteOps(raw);
}

function makeBackend(opts: {
  db: Database.Database;
  docId: string;
  initialMaxLamport: number;
}): FlushableSyncBackend<Operation> {
  const api = createSqliteNodeApi(opts.db);

  return makeQueuedSyncBackend<Operation>({
    docId: opts.docId,
    initialMaxLamport: opts.initialMaxLamport,
    maxLamportFromOps: maxLamport,
    listOpRefs: async (filter) => {
      if ("all" in filter) {
        return parseOpRefs(await api.opRefsAll());
      }
      const parent = Buffer.from(filter.children.parent);
      return parseOpRefs(await api.opRefsChildren(parent));
    },
    getOpsByOpRefs: async (opRefs) => {
      if (opRefs.length === 0) return [];
      return parseOps(await api.opsByOpRefs(opRefs));
    },
    applyOps: async (ops) => api.appendOps!(ops, hexToBytes, (r) => (typeof r === "string" ? Buffer.from(r) : r)),
  });
}

async function openDb(opts: { storage: StorageKind; dbPath?: string; docId: string }): Promise<Database.Database> {
  const db = new Database(opts.storage === "memory" ? ":memory:" : opts.dbPath ?? ":memory:");
  loadTreecrdtExtension(db);
  await createSqliteNodeApi(db).setDocId(opts.docId);
  return db;
}

async function runBenchOnce(
  repoRoot: string,
  { storage, workload, size }: BenchCase,
  bench: ReturnType<typeof buildSyncBenchCase>
): Promise<number> {
  const runId = crypto.randomUUID();
  const outDir = path.join(repoRoot, "tmp", "sqlite-node-sync-bench");
  const dbPathA =
    storage === "file" ? path.join(outDir, `${runId}-${workload}-${size}-a.db`) : undefined;
  const dbPathB =
    storage === "file" ? path.join(outDir, `${runId}-${workload}-${size}-b.db`) : undefined;
  if (storage === "file") {
    await fs.mkdir(outDir, { recursive: true });
  }

  const docId = `sqlite-node-sync-bench-${runId}`;
  const a = await openDb({ storage, dbPath: dbPathA, docId });
  const b = await openDb({ storage, dbPath: dbPathB, docId });

  try {
    const opsA = bench.opsA;
    const opsB = bench.opsB;
    const filter = bench.filter as Filter;

    const apiA = createSqliteNodeApi(a);
    const apiB = createSqliteNodeApi(b);
    await Promise.all([
      apiA.appendOps!(opsA, hexToBytes, (r) => (typeof r === "string" ? Buffer.from(r) : r)),
      apiB.appendOps!(opsB, hexToBytes, (r) => (typeof r === "string" ? Buffer.from(r) : r)),
    ]);

    const backendA = makeBackend({ db: a, docId, initialMaxLamport: maxLamport(opsA) });
    const backendB = makeBackend({ db: b, docId, initialMaxLamport: maxLamport(opsB) });

    const { peerA: pa, transportA: ta, detach } = createInMemoryConnectedPeers({
      backendA,
      backendB,
      codec: treecrdtSyncV0ProtobufCodec,
      peerOptions: { maxCodewords: SYNC_BENCH_DEFAULT_MAX_CODEWORDS },
    });
    try {
      const start = performance.now();
      await pa.syncOnce(ta, filter, {
        maxCodewords: SYNC_BENCH_DEFAULT_MAX_CODEWORDS,
        codewordsPerMessage: SYNC_BENCH_DEFAULT_CODEWORDS_PER_MESSAGE,
      });
      await Promise.all([backendA.flush(), backendB.flush()]);
      const end = performance.now();

      const countA = (a.prepare("SELECT COUNT(*) AS cnt FROM ops").get() as any).cnt as number;
      const countB = (b.prepare("SELECT COUNT(*) AS cnt FROM ops").get() as any).cnt as number;
      if (countA !== bench.expectedFinalOpsA || countB !== bench.expectedFinalOpsB) {
        throw new Error(
          `sync bench mismatch: expected a=${bench.expectedFinalOpsA} b=${bench.expectedFinalOpsB}, got a=${countA} b=${countB}`
        );
      }

      const durationMs = end - start;
      return durationMs;
    } finally {
      detach();
    }
  } finally {
    a.close();
    b.close();
    if (storage === "file") {
      await Promise.allSettled([dbPathA ? fs.rm(dbPathA) : Promise.resolve(), dbPathB ? fs.rm(dbPathB) : Promise.resolve()]);
    }
  }
}

async function runBenchCase(
  repoRoot: string,
  benchCase: BenchCase
): Promise<SyncBenchResult> {
  const bench = buildSyncBenchCase({ workload: benchCase.workload, size: benchCase.size });
  const { size, iterations } = benchCase;

  const samplesMs: number[] = [];
  for (let i = 0; i < iterations; i += 1) {
    samplesMs.push(await runBenchOnce(repoRoot, benchCase, bench));
  }

  const durationMs =
    iterations > 1 ? quantile(samplesMs, 0.5) : samplesMs[0] ?? 0;
  const opsPerSec = durationMs > 0 ? (bench.totalOps / durationMs) * 1000 : Infinity;

  return {
    name: bench.name,
    totalOps: bench.totalOps,
    durationMs,
    opsPerSec,
    extra: {
      ...bench.extra,
      count: size,
      codewordsPerMessage: SYNC_BENCH_DEFAULT_CODEWORDS_PER_MESSAGE,
      maxCodewords: SYNC_BENCH_DEFAULT_MAX_CODEWORDS,
      iterations: iterations > 1 ? iterations : undefined,
      avgDurationMs: iterations > 1 ? durationMs : undefined,
      samplesMs,
      p95Ms: quantile(samplesMs, 0.95),
      minMs: Math.min(...samplesMs),
      maxMs: Math.max(...samplesMs),
    },
  };
}

async function main() {
  const argv = process.argv.slice(2);
  const repoRoot = repoRootFromImportMeta(import.meta.url, 3);

  const config = parseConfigFromArgv(argv) ?? [...SYNC_BENCH_CONFIG];
  const rootConfig = [...SYNC_BENCH_ROOT_CONFIG];

  const cases: BenchCase[] = [];
  for (const storage of ["memory", "file"] as const) {
    for (const workload of DEFAULT_SYNC_BENCH_WORKLOADS) {
      for (const [size, iterations] of config) {
        cases.push({ storage, workload, size, iterations });
      }
    }
    for (const workload of DEFAULT_SYNC_BENCH_ROOT_CHILDREN_WORKLOADS) {
      for (const [size, iterations] of rootConfig) {
        cases.push({ storage, workload, size, iterations });
      }
    }
  }

  for (const benchCase of cases) {
    const result = await runBenchCase(repoRoot, benchCase);
    const outFile = path.join(
      repoRoot,
      "benchmarks",
      "sqlite-node-sync",
      `${benchCase.storage}-${result.name}.json`
    );
    const payload = await writeResult(result, {
      implementation: "sqlite-node",
      storage: benchCase.storage,
      workload: result.name,
      outFile,
      extra: { count: benchCase.size, ...result.extra },
    });
    console.log(JSON.stringify(payload));
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});

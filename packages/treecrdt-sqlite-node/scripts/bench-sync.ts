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
  syncBenchRootChildrenSizesFromEnv,
  syncBenchSizesFromEnv,
  syncBenchTiming,
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
} from "@treecrdt/sync/bench";
import { treecrdtSyncV0ProtobufCodec } from "@treecrdt/sync/protobuf";
import type { Filter } from "@treecrdt/sync";
import { createSqliteNodeAdapter, loadTreecrdtExtension } from "../dist/index.js";

type StorageKind = "memory" | "file";
type BenchCase = { storage: StorageKind; workload: SyncBenchWorkload; size: number };

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
  const adapter = createSqliteNodeAdapter(opts.db);

  const fetchJson = (sql: string, param?: unknown): any => {
    const stmt = opts.db.prepare(sql);
    const row = (param === undefined ? stmt.get() : stmt.get(param)) as any;
    const json = row?.json ?? Object.values(row ?? {})[0];
    return json ? JSON.parse(String(json)) : [];
  };

  return makeQueuedSyncBackend<Operation>({
    docId: opts.docId,
    initialMaxLamport: opts.initialMaxLamport,
    maxLamportFromOps: maxLamport,
    listOpRefs: async (filter) => {
      if ("all" in filter) {
        return parseOpRefs(fetchJson("SELECT treecrdt_oprefs_all() AS json"));
      }
      const parent = Buffer.from(filter.children.parent);
      return parseOpRefs(fetchJson("SELECT treecrdt_oprefs_children(?) AS json", parent));
    },
    getOpsByOpRefs: async (opRefs) => {
      if (opRefs.length === 0) return [];
      const payload = opRefs.map((r) => Array.from(r));
      return parseOps(fetchJson("SELECT treecrdt_ops_by_oprefs(?) AS json", JSON.stringify(payload)));
    },
    applyOps: async (ops) => adapter.appendOps!(ops, hexToBytes, (r) => (typeof r === "string" ? Buffer.from(r) : r)),
  });
}

async function openDb(opts: { storage: StorageKind; dbPath?: string; docId: string }): Promise<Database.Database> {
  const db = new Database(opts.storage === "memory" ? ":memory:" : opts.dbPath ?? ":memory:");
  loadTreecrdtExtension(db);
  db.prepare("SELECT treecrdt_set_doc_id(?)").get(opts.docId);
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

    // Seed each peer.
    const adapterA = createSqliteNodeAdapter(a);
    const adapterB = createSqliteNodeAdapter(b);
    await Promise.all([
      adapterA.appendOps!(opsA, hexToBytes, (r) => (typeof r === "string" ? Buffer.from(r) : r)),
      adapterB.appendOps!(opsB, hexToBytes, (r) => (typeof r === "string" ? Buffer.from(r) : r)),
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

      // Sanity check (outside timed region).
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
  const { iterations, warmupIterations } = syncBenchTiming();

  const samplesMs: number[] = [];
  for (let i = 0; i < warmupIterations + iterations; i += 1) {
    const ms = await runBenchOnce(repoRoot, benchCase, bench);
    if (i >= warmupIterations) samplesMs.push(ms);
  }

  const durationMs = quantile(samplesMs, 0.5);
  const opsPerSec = durationMs > 0 ? (bench.totalOps / durationMs) * 1000 : Infinity;

  return {
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

async function main() {
  const repoRoot = repoRootFromImportMeta(import.meta.url, 3);

  const cases: BenchCase[] = [];
  const sizes = syncBenchSizesFromEnv();
  const rootChildrenSizes = syncBenchRootChildrenSizesFromEnv();
  for (const storage of ["memory", "file"] as const) {
    for (const workload of DEFAULT_SYNC_BENCH_WORKLOADS) {
      for (const size of sizes) {
        cases.push({ storage, workload, size });
      }
    }
    for (const workload of DEFAULT_SYNC_BENCH_ROOT_CHILDREN_WORKLOADS) {
      for (const size of rootChildrenSizes) {
        cases.push({ storage, workload, size });
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
      extra: { count: result.totalOps },
    });
    console.log(JSON.stringify(payload));
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});

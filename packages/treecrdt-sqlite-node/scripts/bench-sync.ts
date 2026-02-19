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

const SYNC_BENCH_COUNTS: readonly number[] = [100, 1_000, 10_000];
const SYNC_BENCH_ROOT_COUNTS: readonly number[] = [1110];

function parseCountsFromArgv(argv: string[]): number[] | null {
  for (const arg of argv) {
    if (arg.startsWith("--count=")) {
      const val = arg.slice("--count=".length).trim();
      const count = val ? Number(val) : 500;
      return [Number.isFinite(count) && count > 0 ? count : 500];
    }
    if (arg.startsWith("--counts=")) {
      const nums = arg
        .slice("--counts=".length)
        .split(",")
        .map((s) => Number(s.trim()))
        .filter((n) => Number.isFinite(n) && n > 0);
      if (nums.length > 0) return nums;
    }
  }
  return null;
}

type BenchCase = {
  storage: StorageKind;
  workload: SyncBenchWorkload;
  size: number;
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
  const durationMs = await runBenchOnce(repoRoot, benchCase, bench);
  const opsPerSec = durationMs > 0 ? (bench.totalOps / durationMs) * 1000 : Infinity;

  return {
    name: bench.name,
    totalOps: bench.totalOps,
    durationMs,
    opsPerSec,
    extra: {
      ...bench.extra,
      count: benchCase.size,
      codewordsPerMessage: SYNC_BENCH_DEFAULT_CODEWORDS_PER_MESSAGE,
      maxCodewords: SYNC_BENCH_DEFAULT_MAX_CODEWORDS,
    },
  };
}

async function main() {
  const argv = process.argv.slice(2);
  const repoRoot = repoRootFromImportMeta(import.meta.url, 3);

  const counts = parseCountsFromArgv(argv) ?? [...SYNC_BENCH_COUNTS];
  const rootCounts = [...SYNC_BENCH_ROOT_COUNTS];

  const cases: BenchCase[] = [];
  for (const storage of ["memory", "file"] as const) {
    for (const workload of DEFAULT_SYNC_BENCH_WORKLOADS) {
      for (const size of counts) {
        cases.push({ storage, workload, size });
      }
    }
    for (const workload of DEFAULT_SYNC_BENCH_ROOT_CHILDREN_WORKLOADS) {
      for (const size of rootCounts) {
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
      extra: { count: benchCase.size, ...result.extra },
    });
    console.log(JSON.stringify(payload));
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});

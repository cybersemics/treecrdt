import fs from "node:fs/promises";
import path from "node:path";
import Database from "better-sqlite3";
import { fileURLToPath } from "node:url";
import {
  buildSyncBenchCase,
  envInt,
  envIntList,
  maxLamport,
  quantile,
  type SyncBenchWorkload,
  writeResult,
} from "@treecrdt/benchmark";
import type { Operation } from "@treecrdt/interface";
import { decodeNodeId, decodeReplicaId, nodeIdToBytes16 } from "@treecrdt/interface/ids";
import { SyncPeer, treecrdtSyncV0ProtobufCodec } from "@treecrdt/sync";
import { createInMemoryDuplex, wrapDuplexTransportWithCodec } from "@treecrdt/sync/transport";
import type { Filter, OpRef, SyncBackend } from "@treecrdt/sync";
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
  if (!Array.isArray(raw)) return [];
  return raw.map((val) => (val instanceof Uint8Array ? val : Uint8Array.from(val)));
}

function parseOps(raw: any): Operation[] {
  if (!Array.isArray(raw)) return [];
  return raw.map((row) => {
    const replica = decodeReplicaId(row.replica);
    const base = { meta: { id: { replica, counter: Number(row.counter) }, lamport: Number(row.lamport) } } as Operation;
    if (row.kind === "insert") {
      return {
        ...base,
        kind: { type: "insert", parent: decodeNodeId(row.parent), node: decodeNodeId(row.node), position: row.position ?? 0 },
      } as Operation;
    }
    if (row.kind === "move") {
      return {
        ...base,
        kind: { type: "move", node: decodeNodeId(row.node), newParent: decodeNodeId(row.new_parent), position: row.position ?? 0 },
      } as Operation;
    }
    if (row.kind === "delete") {
      return { ...base, kind: { type: "delete", node: decodeNodeId(row.node) } } as Operation;
    }
    return { ...base, kind: { type: "tombstone", node: decodeNodeId(row.node) } } as Operation;
  });
}

function makeBackend(opts: {
  db: Database.Database;
  docId: string;
  initialMaxLamport: number;
}): SyncBackend<Operation> & { flush: () => Promise<void> } {
  const adapter = createSqliteNodeAdapter(opts.db);
  let maxLamportValue = opts.initialMaxLamport;
  let lastApply = Promise.resolve();

  const fetchJson = (sql: string, param?: unknown): any => {
    const stmt = opts.db.prepare(sql);
    const row = param === undefined ? stmt.get() : stmt.get(param);
    const json = row?.json ?? Object.values(row ?? {})[0];
    return json ? JSON.parse(String(json)) : [];
  };

  return {
    docId: opts.docId,
    maxLamport: async () => BigInt(maxLamportValue),
    listOpRefs: async (filter: Filter) => {
      if ("all" in filter) {
        return parseOpRefs(fetchJson("SELECT treecrdt_oprefs_all() AS json"));
      }
      const parent = Buffer.from(filter.children.parent);
      return parseOpRefs(fetchJson("SELECT treecrdt_oprefs_children(?) AS json", parent));
    },
    getOpsByOpRefs: async (opRefs: OpRef[]) => {
      if (opRefs.length === 0) return [];
      const payload = opRefs.map((r) => Array.from(r));
      return parseOps(fetchJson("SELECT treecrdt_ops_by_oprefs(?) AS json", JSON.stringify(payload)));
    },
    applyOps: async (ops: Operation[]) => {
      if (ops.length === 0) return;
      const nextMax = maxLamport(ops);
      if (nextMax > maxLamportValue) maxLamportValue = nextMax;
      lastApply = lastApply.then(() => adapter.appendOps!(ops, hexToBytes, (r) => (typeof r === "string" ? Buffer.from(r) : r)));
      await lastApply;
    },
    flush: async () => lastApply,
  };
}

async function openDb(opts: { storage: StorageKind; dbPath?: string; docId: string }): Promise<Database.Database> {
  const db = new Database(opts.storage === "memory" ? ":memory:" : opts.dbPath ?? ":memory:");
  loadTreecrdtExtension(db);
  db.prepare("SELECT treecrdt_set_doc_id(?)").get(opts.docId);
  return db;
}

function syncBenchTiming() {
  const iterations = Math.max(1, envInt("SYNC_BENCH_ITERATIONS") ?? envInt("BENCH_ITERATIONS") ?? 3);
  const warmupIterations = Math.max(0, envInt("SYNC_BENCH_WARMUP") ?? envInt("BENCH_WARMUP") ?? (iterations > 1 ? 1 : 0));
  return { iterations, warmupIterations };
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

    const [wa, wb] = createInMemoryDuplex<Uint8Array>();
    const ta = wrapDuplexTransportWithCodec(wa, treecrdtSyncV0ProtobufCodec);
    const tb = wrapDuplexTransportWithCodec(wb, treecrdtSyncV0ProtobufCodec);
    const pa = new SyncPeer(backendA, { maxCodewords: 200_000 });
    const pb = new SyncPeer(backendB, { maxCodewords: 200_000 });
    pa.attach(ta);
    pb.attach(tb);

    const start = performance.now();
    await pa.syncOnce(ta, filter, { maxCodewords: 200_000, codewordsPerMessage: 2048 });
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

async function main() {
  const __dirname = path.dirname(fileURLToPath(import.meta.url));
  const repoRoot = path.resolve(__dirname, "../..", "..");

  const cases: BenchCase[] = [];
  const sizes = envIntList("SYNC_BENCH_SIZES") ?? [100, 1000, 10_000];
  const rootChildrenSizes = envIntList("SYNC_BENCH_ROOT_CHILDREN_SIZES") ?? [1110];
  for (const storage of ["memory", "file"] as const) {
    for (const workload of ["sync-all", "sync-children", "sync-one-missing"] as const) {
      for (const size of sizes) {
        cases.push({ storage, workload, size });
      }
    }
    for (const size of rootChildrenSizes) {
      cases.push({ storage, workload: "sync-root-children-fanout10", size });
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

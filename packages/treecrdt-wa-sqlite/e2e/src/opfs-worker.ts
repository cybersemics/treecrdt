/// <reference lib="webworker" />

import SQLiteESMFactory from "wa-sqlite";
import * as SQLite from "wa-sqlite/sqlite-api";
import { OPFSCoopSyncVFS } from "../../../../vendor/wa-sqlite/src/examples/OPFSCoopSyncVFS.js";
import sqliteWasmUrl from "/wa-sqlite/wa-sqlite-async.wasm?url";
import { buildWorkloads, runWorkloads, type BenchmarkResult, type WorkloadName } from "@treecrdt/benchmark";
import { createWaSqliteAdapter, loadTreecrdtExtension } from "@treecrdt/wa-sqlite";
import type { Database } from "wa-sqlite";
import type { TreecrdtAdapter } from "@treecrdt/interface";

type StorageKind = "browser-opfs-coop-sync" | "browser-memory";

type WorkerRequest = {
  type: "run";
  storage?: StorageKind;
  sizes?: number[];
  workloads?: WorkloadName[];
};

type WorkerResponse =
  | { ok: true; results: BenchPayload[] }
  | { ok: false; error: string };

type BenchPayload = BenchmarkResult & {
  implementation: string;
  storage: StorageKind;
  workload: string;
  extra?: Record<string, unknown>;
};

const defaultSizes = [1, 10, 100, 1_000];
const defaultWorkloads: WorkloadName[] = ["insert-move", "insert-chain", "replay-log"];

async function createAdapter(storage: StorageKind): Promise<TreecrdtAdapter & { close: () => Promise<void> }> {
  const module = await SQLiteESMFactory({
    locateFile: (file: string) => (file.endsWith(".wasm") ? sqliteWasmUrl : file),
  });

  const sqlite3 = SQLite.Factory(module);
  if (storage === "browser-opfs-coop-sync") {
    const vfs = await OPFSCoopSyncVFS.create("opfs", module, {});
    sqlite3.vfs_register(vfs, true);
  }

  const filename = storage === "browser-opfs-coop-sync" ? "/treecrdt-opfs-bench.db" : ":memory:";
  const handle = await sqlite3.open_v2(filename);
  const db = makeDbAdapter(sqlite3, handle);
  await loadTreecrdtExtension({ db });

  const adapter = createWaSqliteAdapter(db);
  return Object.assign(adapter, {
    close: async () => {
      try {
        await sqlite3.close(handle);
      } catch {
        /* ignore */
      }
    },
  });
}

function makeDbAdapter(sqlite3: ReturnType<typeof SQLite.Factory>, handle: number): Database {
  const prepare: Database["prepare"] = async (sql) => {
    const iter = sqlite3.statements(handle, sql, { unscoped: true });
    const { value } = await iter.next();
    if (iter.return) {
      await iter.return();
    }
    if (!value) {
      throw new Error(`Failed to prepare statement: ${sql}`);
    }
    return value;
  };

  const db: Database = {
    prepare,
    bind: async (stmt, index, value) => sqlite3.bind(stmt, index, value),
    step: async (stmt) => sqlite3.step(stmt),
    column_text: async (stmt, index) => sqlite3.column_text(stmt, index),
    finalize: async (stmt) => sqlite3.finalize(stmt),
    exec: async (sql) => sqlite3.exec(handle, sql),
  };
  return db;
}

async function runWaSqliteBenchInWorker(
  storage: StorageKind,
  sizes: number[] = defaultSizes,
  workloads: WorkloadName[] = defaultWorkloads
): Promise<BenchPayload[]> {
  const workloadDefs = buildWorkloads(workloads, sizes);
  const results = await runWorkloads(
    async () => createAdapter(storage),
    workloadDefs
  );

  return results.map((result, idx) => {
    const workload = workloadDefs[idx];
    return {
      ...result,
      implementation: "wa-sqlite",
      storage,
      workload: workload.name,
      extra: { count: workload.totalOps ?? result.totalOps },
    };
  });
}

self.onmessage = async (ev: MessageEvent<WorkerRequest>) => {
  if (ev.data?.type !== "run") return;
  const storage: StorageKind = ev.data.storage ?? "browser-opfs-coop-sync";
  try {
    const results = await runWaSqliteBenchInWorker(storage, ev.data.sizes, ev.data.workloads);
    const response: WorkerResponse = { ok: true, results };
    self.postMessage(response);
  } catch (err) {
    const response: WorkerResponse = { ok: false, error: err instanceof Error ? err.message : String(err) };
    self.postMessage(response);
  }
};

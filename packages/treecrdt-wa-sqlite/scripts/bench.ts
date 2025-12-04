import fs from "node:fs";
import fsPromises from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { runBenchmark, makeInsertMoveWorkload, makeInsertChainWorkload, writeResult } from "@treecrdt/benchmark";
import { createWaSqliteAdapter } from "../dist/index.js";

type StorageKind = "memory" | "file";

type CliOptions = {
  count: number;
  storage: StorageKind;
  outFile?: string;
  workload: "insert-move" | "insert-chain";
  sizes?: number[];
};

function parseArgs(): CliOptions {
  const opts: CliOptions = { count: 500, storage: "memory", workload: "insert-move" };
  for (const arg of process.argv.slice(2)) {
    if (arg.startsWith("--count=")) {
      opts.count = Number(arg.slice("--count=".length)) || opts.count;
    } else if (arg.startsWith("--sizes=")) {
      opts.sizes = arg
        .slice("--sizes=".length)
        .split(",")
        .map((s) => Number(s.trim()))
        .filter((n) => Number.isFinite(n) && n > 0);
    } else if (arg.startsWith("--storage=")) {
      const val = arg.slice("--storage=".length);
      if (val === "file" || val === "memory") {
        opts.storage = val;
      }
    } else if (arg.startsWith("--out=")) {
      opts.outFile = arg.slice("--out=".length);
    } else if (arg.startsWith("--workload=")) {
      const val = arg.slice("--workload=".length);
      if (val === "insert-move" || val === "insert-chain") {
        opts.workload = val;
      }
    }
  }
  return opts;
}

function makeWorkload(name: "insert-move" | "insert-chain", count: number) {
  if (name === "insert-chain") return makeInsertChainWorkload({ count });
  return makeInsertMoveWorkload({ count });
}

type AdapterBundle = {
  adapter: ReturnType<typeof createWaSqliteAdapter> & { close?: () => Promise<void> };
  sqlite3: any;
  handle: number;
};

async function createAdapter(filename: string): Promise<AdapterBundle> {
  const __dirname = path.dirname(fileURLToPath(import.meta.url));
  const repoRoot = path.resolve(__dirname, "../../..");
  const wasmPath = path.join(repoRoot, "vendor/wa-sqlite/dist/wa-sqlite-async.wasm");
  const wasmBinary = fs.readFileSync(wasmPath);
  const mod = await import(path.join(repoRoot, "vendor/wa-sqlite/dist/wa-sqlite-async.mjs"));
  const SQLite = await import(path.join(repoRoot, "vendor/wa-sqlite/src/sqlite-api.js"));
  const module = await mod.default({
    wasmBinary,
    locateFile: (f: string) => (f.endsWith(".wasm") ? wasmPath : f),
  });
  const sqlite3 = SQLite.Factory(module);
  const handle = await sqlite3.open_v2(filename);

  // Probe the extension registration to fail fast with a clearer message.
  try {
    await sqlite3.exec(handle, "SELECT treecrdt_ops_since(0)");
  } catch (err) {
    const msg = sqlite3.errmsg ? sqlite3.errmsg(handle) : String(err);
    throw new Error(`treecrdt extension not registered: ${msg}`);
  }

  // Minimal adapter that matches wa-sqlite Database shape used in our helper.
  const db = {
    prepare: async (sql: string) => {
      const iter = sqlite3.statements(handle, sql, { unscoped: true });
      const { value } = await iter.next();
      if (iter.return) await iter.return();
      if (!value) throw new Error(`Failed to prepare: ${sql}`);
      return value;
    },
    bind: async (stmt: unknown, idx: number, val: unknown) => sqlite3.bind(stmt, idx, val),
    step: async (stmt: unknown) => sqlite3.step(stmt),
    column_text: async (stmt: unknown, idx: number) => sqlite3.column_text(stmt, idx),
    finalize: async (stmt: unknown) => sqlite3.finalize(stmt),
    exec: async (sql: string) => sqlite3.exec(handle, sql),
  };

  const adapter = createWaSqliteAdapter(db);
  const adapterWithClose = { ...adapter };

  return { adapter: adapterWithClose, sqlite3, handle };
}

async function main() {
  const opts = parseArgs();
  const __dirname = path.dirname(fileURLToPath(import.meta.url));
  const repoRoot = path.resolve(__dirname, "../../..");

  const filename =
    opts.storage === "memory"
      ? ":memory:"
      : path.join(repoRoot, "tmp", "wa-sqlite-bench", "bench.db");
  if (opts.storage === "file") {
    await fsPromises.mkdir(path.dirname(filename), { recursive: true });
    if (fs.existsSync(filename)) {
      await fsPromises.rm(filename);
    }
  }

  const sizes = opts.sizes && opts.sizes.length > 0 ? opts.sizes : [opts.count];
  for (const size of sizes) {
    const { adapter, sqlite3, handle } = await createAdapter(filename);
    const workload = makeWorkload(opts.workload, size);
    let result;
    try {
      result = await runBenchmark(() => adapter, workload);
    } catch (err) {
      const msg = sqlite3.errmsg ? sqlite3.errmsg(handle) : String(err);
      console.error(`Benchmark failed: ${msg}`);
      throw err;
    }

    const outFile =
      opts.outFile ??
      path.join(
        repoRoot,
        "benchmarks",
        "wa-sqlite",
        `${opts.storage}-${workload.name}.json`
      );
    const payload = await writeResult(result, {
      implementation: "wa-sqlite",
      storage: opts.storage,
      workload: workload.name,
      outFile,
      extra: { count: size },
    });
    console.log(JSON.stringify(payload, null, 2));

    if (adapter.close) {
      await adapter.close();
    }
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});

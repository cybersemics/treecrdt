import fs from "node:fs";
import fsPromises from "node:fs/promises";
import { createRequire } from "node:module";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  buildWorkloads,
  runWorkloads,
  writeResult,
  type WorkloadName,
} from "@treecrdt/benchmark";
import { createWaSqliteAdapter } from "../dist/index.js";

type CliOptions = {
  count: number;
  outFile?: string;
  workload: WorkloadName;
  workloads?: WorkloadName[];
  sizes?: number[];
};

function parseArgs(): CliOptions {
  const opts: CliOptions = { count: 500, workload: "insert-move" };
  for (const arg of process.argv.slice(2)) {
    if (arg.startsWith("--count=")) {
      opts.count = Number(arg.slice("--count=".length)) || opts.count;
    } else if (arg.startsWith("--sizes=")) {
      opts.sizes = arg
        .slice("--sizes=".length)
        .split(",")
        .map((s) => Number(s.trim()))
        .filter((n) => Number.isFinite(n) && n > 0);
    } else if (arg.startsWith("--out=")) {
      opts.outFile = arg.slice("--out=".length);
    } else if (arg.startsWith("--workload=")) {
      const val = arg.slice("--workload=".length);
      if (val === "insert-move" || val === "insert-chain" || val === "replay-log") {
        opts.workload = val as WorkloadName;
      }
    } else if (arg.startsWith("--workloads=")) {
      const vals = arg
        .slice("--workloads=".length)
        .split(",")
        .map((s) => s.trim())
        .filter((s) => s.length > 0);
      opts.workloads = vals.filter((v): v is WorkloadName =>
        v === "insert-move" || v === "insert-chain" || v === "replay-log"
      );
    }
  }
  return opts;
}

type AdapterBundle = {
  adapter: ReturnType<typeof createWaSqliteAdapter> & { close?: () => Promise<void> };
  sqlite3: any;
  handle: number;
};

async function createAdapter(filename: string): Promise<AdapterBundle> {
  const __dirname = path.dirname(fileURLToPath(import.meta.url));
  const repoRoot = path.resolve(__dirname, "../../..");
  const vendorPkgRoot = (() => {
    try {
      const require = createRequire(import.meta.url);
      const pkgJson = require.resolve("@treecrdt/wa-sqlite-vendor/package.json");
      return path.dirname(pkgJson);
    } catch {
      return path.join(repoRoot, "packages/treecrdt-wa-sqlite-vendor");
    }
  })();
  const vendorWaSqliteRoot = path.join(vendorPkgRoot, "wa-sqlite");
  const vendorDistRoot = path.join(vendorPkgRoot, "dist");

  const wasmPath = path.join(vendorDistRoot, "wa-sqlite-async.wasm");
  const wasmBinary = fs.readFileSync(wasmPath);
  const mod = await import(path.join(vendorDistRoot, "wa-sqlite-async.mjs"));
  const SQLite = await import(path.join(vendorWaSqliteRoot, "src/sqlite-api.js"));
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

  const sizes = opts.sizes && opts.sizes.length > 0 ? opts.sizes : [1, 10, 100, 1000, 10000];
  const workloads =
    opts.workloads && opts.workloads.length > 0
      ? opts.workloads
      : (["insert-move", "insert-chain", "replay-log"] as WorkloadName[]);
  const workloadDefs = buildWorkloads(workloads, sizes);

  // wa-sqlite is browser-first; in Node we only exercise the in-memory runtime.
  const filename = ":memory:";
  const { adapter, sqlite3, handle } = await createAdapter(filename);
  let results;
  try {
    results = await runWorkloads(() => adapter, workloadDefs);
  } catch (err) {
    const msg = sqlite3.errmsg ? sqlite3.errmsg(handle) : String(err);
    console.error(`Benchmark failed: ${msg}`);
    throw err;
  }

  for (const result of results) {
    const outFile =
      opts.outFile ??
      path.join(
        repoRoot,
        "benchmarks",
        "wa-sqlite",
        `memory-${result.name}.json`
      );
    const payload = await writeResult(result, {
      implementation: "wa-sqlite",
      storage: "memory",
      workload: result.name,
      outFile,
      extra: { count: result.totalOps },
    });
    console.log(JSON.stringify(payload, null, 2));
  }

  if (adapter.close) {
    await adapter.close();
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});

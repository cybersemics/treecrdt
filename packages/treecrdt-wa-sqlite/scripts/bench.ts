import fs from "node:fs";
import { createRequire } from "node:module";
import path from "node:path";
import {
  buildWorkloads,
  runWorkloads,
} from "@treecrdt/benchmark";
import { parseBenchCliArgs, repoRootFromImportMeta, writeResult } from "@treecrdt/benchmark/node";
import { createWaSqliteApi } from "../dist/index.js";
import { makeDbAdapter } from "../dist/db.js";

async function loadSqlite3(repoRoot: string): Promise<any> {
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
  return SQLite.Factory(module);
}

async function main() {
  const argv = process.argv.slice(2);
  const opts = parseBenchCliArgs({ argv });
  const repoRoot = repoRootFromImportMeta(import.meta.url, 3);

  const workloadDefs = buildWorkloads(opts.workloads, opts.sizes);

  // wa-sqlite is browser-first; in Node we only exercise the in-memory runtime.
  const sqlite3 = await loadSqlite3(repoRoot);
  const docId = "treecrdt-wa-sqlite-bench";

  // Probe extension registration once so benchmark timing isn't dominated by setup errors.
  const probeHandle = await sqlite3.open_v2(":memory:");
  try {
    await sqlite3.exec(probeHandle, "SELECT treecrdt_ops_since(0)");
  } catch (err) {
    const msg = sqlite3.errmsg ? sqlite3.errmsg(probeHandle) : String(err);
    throw new Error(`treecrdt extension not registered: ${msg}`);
  } finally {
    await sqlite3.close(probeHandle);
  }

  const adapterFactory = async () => {
    const handle = await sqlite3.open_v2(":memory:");
    const db = makeDbAdapter(sqlite3, handle);
    const api = createWaSqliteApi(db);
    await api.setDocId(docId);
    return { ...api, close: () => db.close?.() };
  };

  const results = await runWorkloads(adapterFactory, workloadDefs);

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
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});

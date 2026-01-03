import fs from "node:fs/promises";
import path from "node:path";
import Database from "better-sqlite3";
import {
  benchTiming,
  buildWorkloads,
  runBenchmark,
} from "@treecrdt/benchmark";
import { parseBenchCliArgs, repoRootFromImportMeta, writeResult } from "@treecrdt/benchmark/node";
import { createSqliteNodeAdapter, loadTreecrdtExtension } from "../dist/index.js";

type StorageKind = "memory" | "file";

function parseStorages(argv: string[]): StorageKind[] {
  let storage: StorageKind = "memory";
  let storages: StorageKind[] | undefined;
  for (const arg of argv) {
    if (arg.startsWith("--storage=")) {
      const val = arg.slice("--storage=".length);
      if (val === "memory" || val === "file") storage = val;
    } else if (arg.startsWith("--storages=")) {
      const vals = arg
        .slice("--storages=".length)
        .split(",")
        .map((s) => s.trim())
        .filter((s) => s.length > 0);
      const parsed = vals.filter((v): v is StorageKind => v === "memory" || v === "file");
      if (parsed.length > 0) storages = parsed;
    }
  }
  if (storages && storages.length > 0) return storages;
  return Array.from(new Set<StorageKind>([storage, "file"]));
}

async function main() {
  const argv = process.argv.slice(2);
  const opts = parseBenchCliArgs({ argv });
  const storages = parseStorages(argv);
  const repoRoot = repoRootFromImportMeta(import.meta.url, 3);

  const workloadDefs = buildWorkloads(opts.workloads, opts.sizes);
  const timing = benchTiming({ defaultIterations: 3 });
  for (const w of workloadDefs) {
    w.iterations = timing.iterations;
    w.warmupIterations = timing.warmupIterations;
  }

  for (const workload of workloadDefs) {
    for (const storage of storages) {
      const adapterFactory = async () => {
        const dbPath =
          storage === "memory"
            ? ":memory:"
            : path.join(repoRoot, "tmp", "sqlite-node-bench", `${workload.name}-${crypto.randomUUID()}.db`);
        if (storage === "file") {
          await fs.mkdir(path.dirname(dbPath), { recursive: true });
        }

        const db = new Database(dbPath);
        loadTreecrdtExtension(db);
        db.prepare("SELECT treecrdt_set_doc_id(?)").get("treecrdt-sqlite-node-bench");
        return {
          ...createSqliteNodeAdapter(db),
          close: async () => {
            db.close();
            if (storage === "file") {
              await fs.rm(dbPath).catch(() => {});
            }
          },
        };
      };

      const result = await runBenchmark(adapterFactory, workload);

      const outFile =
        opts.outFile ??
        path.join(repoRoot, "benchmarks", "sqlite-node", `${storage}-${workload.name}.json`);
      const payload = await writeResult(result, {
        implementation: "sqlite-node",
        storage,
        workload: workload.name,
        outFile,
        extra: { count: result.totalOps },
      });
      console.log(JSON.stringify(payload, null, 2));
    }
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});

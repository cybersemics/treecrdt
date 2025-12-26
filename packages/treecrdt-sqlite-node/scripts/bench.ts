import fs from "node:fs/promises";
import path from "node:path";
import Database from "better-sqlite3";
import { fileURLToPath } from "node:url";
import {
  buildWorkloads,
  envInt,
  runBenchmark,
  writeResult,
  type WorkloadName,
} from "@treecrdt/benchmark";
import { createSqliteNodeAdapter, loadTreecrdtExtension } from "../dist/index.js";

type StorageKind = "memory" | "file";

type CliOptions = {
  count: number;
  storage: StorageKind;
  storages?: StorageKind[];
  outFile?: string;
  workload: WorkloadName;
  workloads?: WorkloadName[];
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
      if (val === "memory" || val === "file") {
        opts.storage = val;
      }
    } else if (arg.startsWith("--storages=")) {
      const vals = arg
        .slice("--storages=".length)
        .split(",")
        .map((s) => s.trim())
        .filter((s) => s.length > 0);
      opts.storages = vals.filter((v): v is StorageKind => v === "memory" || v === "file");
    } else if (arg.startsWith("--out=")) {
      opts.outFile = arg.slice("--out=".length);
    } else if (arg.startsWith("--workload=")) {
      const val = arg.slice("--workload=".length);
      if (val === "insert-move" || val === "insert-chain" || val === "replay-log") {
        opts.workload = val;
      }
    } else if (arg.startsWith("--workloads=")) {
      const vals = arg
        .slice("--workloads=".length)
        .split(",")
        .map((s) => s.trim())
        .filter((s) => s.length > 0);
      opts.workloads = vals.filter((v): v is "insert-move" | "insert-chain" | "replay-log" =>
        v === "insert-move" || v === "insert-chain" || v === "replay-log"
      );
    }
  }
  return opts;
}

async function main() {
  const opts = parseArgs();
  const __dirname = path.dirname(fileURLToPath(import.meta.url));
  const repoRoot = path.resolve(__dirname, "../..", "..");

  const sizes = opts.sizes && opts.sizes.length > 0 ? opts.sizes : [1, 10, 100, 1000, 10000];
  const workloads =
    opts.workloads && opts.workloads.length > 0
      ? opts.workloads
      : (["insert-move", "insert-chain", "replay-log"] as WorkloadName[]);
  const workloadDefs = buildWorkloads(workloads, sizes);
  const iterations = Math.max(1, envInt("BENCH_ITERATIONS") ?? 3);
  const warmupIterations = Math.max(0, envInt("BENCH_WARMUP") ?? (iterations > 1 ? 1 : 0));
  for (const w of workloadDefs) {
    w.iterations = iterations;
    w.warmupIterations = warmupIterations;
  }
  const storages =
    opts.storages && opts.storages.length > 0 ? opts.storages : (opts.storage ? [opts.storage, "file"] : ["memory", "file"]);

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

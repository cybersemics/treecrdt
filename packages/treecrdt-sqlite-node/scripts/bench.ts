import fs from "node:fs/promises";
import path from "node:path";
import Database from "better-sqlite3";
import { fileURLToPath } from "node:url";
import { runBenchmark, makeInsertMoveWorkload, makeInsertChainWorkload, writeResult } from "@treecrdt/benchmark";
import { createSqliteNodeAdapter, loadTreecrdtExtension } from "../dist/index.js";

type StorageKind = "memory" | "file";

type CliOptions = {
  count: number;
  storage: StorageKind;
  outFile?: string;
  workload: "insert-move" | "insert-chain";
  workloads?: ("insert-move" | "insert-chain")[];
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
    } else if (arg.startsWith("--out=")) {
      opts.outFile = arg.slice("--out=".length);
    } else if (arg.startsWith("--workload=")) {
      const val = arg.slice("--workload=".length);
      if (val === "insert-move" || val === "insert-chain") {
        opts.workload = val;
      }
    } else if (arg.startsWith("--workloads=")) {
      const vals = arg
        .slice("--workloads=".length)
        .split(",")
        .map((s) => s.trim())
        .filter((s) => s.length > 0);
      opts.workloads = vals.filter((v): v is "insert-move" | "insert-chain" =>
        v === "insert-move" || v === "insert-chain"
      );
    }
  }
  return opts;
}

function makeWorkload(name: "insert-move" | "insert-chain", count: number) {
  if (name === "insert-chain") return makeInsertChainWorkload({ count });
  return makeInsertMoveWorkload({ count });
}

async function main() {
  const opts = parseArgs();
  const __dirname = path.dirname(fileURLToPath(import.meta.url));
  const repoRoot = path.resolve(__dirname, "../..", "..");

  const dbPath =
    opts.storage === "memory"
      ? ":memory:"
      : path.join(repoRoot, "tmp", "sqlite-node-bench", "bench.db");
  if (opts.storage === "file") {
    await fs.mkdir(path.dirname(dbPath), { recursive: true });
    // remove stale db to avoid reusing data
    try {
      await fs.rm(dbPath);
    } catch {
      // ignore
    }
  }

  const sizes = opts.sizes && opts.sizes.length > 0 ? opts.sizes : [1, 10, 100, 1000, 10000];
  const workloads = opts.workloads && opts.workloads.length > 0 ? opts.workloads : ["insert-move", "insert-chain"];

  for (const workloadName of workloads) {
    for (const size of sizes) {
      const db = new Database(dbPath);
      loadTreecrdtExtension(db);
      const adapter = {
        ...createSqliteNodeAdapter(db),
        close: async () => {
          db.close();
        },
      };

      const workload = makeWorkload(workloadName, size);
      const result = await runBenchmark(() => adapter, workload);

      const outFile =
        opts.outFile ??
        path.join(repoRoot, "benchmarks", "sqlite-node", `${opts.storage}-${workload.name}.json`);
      const payload = await writeResult(result, {
        implementation: "sqlite-node",
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
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});

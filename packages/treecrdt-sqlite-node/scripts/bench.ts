import fs from "node:fs/promises";
import path from "node:path";
import Database from "better-sqlite3";
import { makeWorkload, runBenchmark } from "@treecrdt/benchmark";
import { repoRootFromImportMeta, writeResult } from "@treecrdt/benchmark/node";
import { createSqliteNodeApi, loadTreecrdtExtension } from "../dist/index.js";

type StorageKind = "memory" | "file";

const INSERT_MOVE_BENCH_COUNTS: readonly number[] = [100, 1_000, 10_000];

const WORKLOAD: "insert-move" = "insert-move";
const STORAGES: ReadonlyArray<StorageKind> = ["memory", "file"];

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

async function main() {
  const argv = process.argv.slice(2);
  const repoRoot = repoRootFromImportMeta(import.meta.url, 3);

  const counts = parseCountsFromArgv(argv) ?? [...INSERT_MOVE_BENCH_COUNTS];

  for (const size of counts) {
    const workload = makeWorkload(WORKLOAD, size);

    for (const storage of STORAGES) {
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
        const api = createSqliteNodeApi(db);
        await api.setDocId("treecrdt-sqlite-node-bench");
        return {
          ...api,
          close: async () => {
            db.close();
            if (storage === "file") {
              await fs.rm(dbPath).catch(() => {});
            }
          },
        };
      };

      const result = await runBenchmark(adapterFactory, workload);

      const outFile = path.join(repoRoot, "benchmarks", "sqlite-node", `${storage}-${result.name}.json`);
      const payload = await writeResult(result, {
        implementation: "sqlite-node",
        storage,
        workload: result.name,
        outFile,
        extra: { count: size, ...result.extra },
      });
      console.log(JSON.stringify(payload, null, 2));
    }
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});

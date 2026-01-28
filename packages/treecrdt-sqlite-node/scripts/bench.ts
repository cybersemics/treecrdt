import fs from "node:fs/promises";
import path from "node:path";
import Database from "better-sqlite3";
import { makeWorkload, runBenchmark } from "@treecrdt/benchmark";
import { repoRootFromImportMeta, writeResult } from "@treecrdt/benchmark/node";
import { createSqliteNodeApi, loadTreecrdtExtension } from "../dist/index.js";

type StorageKind = "memory" | "file";

const CI_CONFIG: ReadonlyArray<[number, number]> = [
  [100, 5],
  [1_000, 5],
  [10_000, 1],
];

const LOCAL_CONFIG: ReadonlyArray<[number, number]> = [
  [1, 1],
  [10, 1],
  [100, 1],
  [1_000, 1],
  [10_000, 1],
];

const WORKLOAD: "insert-move" = "insert-move";
const STORAGES: ReadonlyArray<StorageKind> = ["memory", "file"];

function isCi(): boolean {
  return process.env.CI === "true";
}

function envInt(name: string): number | undefined {
  const raw = process.env[name];
  if (raw == null || raw === "") return undefined;
  const n = Number(raw);
  return Number.isFinite(n) ? n : undefined;
}

function parseConfigFromArgv(argv: string[]): Array<[number, number]> | null {
  let customConfig: Array<[number, number]> | null = null;
  const defaultIterations = Math.max(1, envInt("BENCH_ITERATIONS") ?? 1);
  for (const arg of argv) {
    if (arg.startsWith("--count=")) {
      const val = arg.slice("--count=".length).trim();
      const count = val ? Number(val) : 500;
      customConfig = [[Number.isFinite(count) && count > 0 ? count : 500, defaultIterations]];
      break;
    }
    if (arg.startsWith("--counts=")) {
      const vals = arg
        .slice("--counts=".length)
        .split(",")
        .map((s) => s.trim())
        .filter((s) => s.length > 0);
      const parsed = vals
        .map((s) => {
          const n = Number(s);
          return Number.isFinite(n) && n > 0 ? n : null;
        })
        .filter((n): n is number => n != null)
        .map((c) => [c, defaultIterations] as [number, number]);
      if (parsed.length > 0) customConfig = parsed;
      break;
    }
  }
  return customConfig;
}

async function main() {
  const argv = process.argv.slice(2);
  const repoRoot = repoRootFromImportMeta(import.meta.url, 3);

  const baseConfig = isCi() ? CI_CONFIG : LOCAL_CONFIG;
  const config: Array<[number, number]> = parseConfigFromArgv(argv) ?? [...baseConfig];

  for (const [size, iterations] of config) {
    const workload = makeWorkload(WORKLOAD, size);
    workload.iterations = 1;
    workload.warmupIterations = 0;

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

      let result: Awaited<ReturnType<typeof runBenchmark>>;
      if (iterations > 1) {
        const durations: number[] = [];
        for (let i = 0; i < iterations; i += 1) {
          const r = await runBenchmark(adapterFactory, workload);
          durations.push(r.durationMs);
        }
        const avgDurationMs = durations.reduce((a, b) => a + b, 0) / durations.length;
        const totalOps = workload.totalOps ?? -1;
        result = {
          name: workload.name,
          totalOps,
          durationMs: avgDurationMs,
          opsPerSec:
            totalOps > 0 && avgDurationMs > 0
              ? (totalOps / avgDurationMs) * 1000
              : avgDurationMs > 0
                ? 1000 / avgDurationMs
                : Infinity,
          extra: {
            count: totalOps > 0 ? totalOps : undefined,
            iterations,
            avgDurationMs,
            samplesMs: durations,
          },
        };
      } else {
        result = await runBenchmark(adapterFactory, workload);
      }

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

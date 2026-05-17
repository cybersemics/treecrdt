import fs from 'node:fs/promises';
import path from 'node:path';
import Database from 'better-sqlite3';
import { makeWorkload, quantile, runBenchmark } from '@treecrdt/benchmark';
import { repoRootFromImportMeta, writeResult } from '@treecrdt/benchmark/node';
import { createSqliteNodeApi, loadTreecrdtExtension } from '../dist/index.js';

type StorageKind = 'memory' | 'file';

const INSERT_MOVE_BENCH_CONFIG: ReadonlyArray<[number, number]> = [
  [100, 10],
  [1_000, 10],
  [10_000, 10],
];

const WORKLOAD: 'insert-move' = 'insert-move';
const STORAGES: ReadonlyArray<StorageKind> = ['memory', 'file'];

function envInt(name: string): number | undefined {
  const raw = process.env[name];
  if (raw == null || raw === '') return undefined;
  const n = Number(raw);
  return Number.isFinite(n) ? n : undefined;
}

function parseConfigFromArgv(argv: string[]): Array<[number, number]> | null {
  let customConfig: Array<[number, number]> | null = null;
  const defaultIterations = Math.max(1, envInt('BENCH_ITERATIONS') ?? 1);
  for (const arg of argv) {
    if (arg.startsWith('--count=')) {
      const val = arg.slice('--count='.length).trim();
      const count = val ? Number(val) : 500;
      customConfig = [[Number.isFinite(count) && count > 0 ? count : 500, defaultIterations]];
      break;
    }
    if (arg.startsWith('--counts=')) {
      const vals = arg
        .slice('--counts='.length)
        .split(',')
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

  const config: Array<[number, number]> = parseConfigFromArgv(argv) ?? [
    ...INSERT_MOVE_BENCH_CONFIG,
  ];

  for (const [size, iterations] of config) {
    const workload = makeWorkload(WORKLOAD, size);
    workload.iterations = 1;
    workload.warmupIterations = 0;

    for (const storage of STORAGES) {
      const adapterFactory = async () => {
        const dbPath =
          storage === 'memory'
            ? ':memory:'
            : path.join(
                repoRoot,
                'tmp',
                'sqlite-node-bench',
                `${workload.name}-${crypto.randomUUID()}.db`,
              );
        if (storage === 'file') {
          await fs.mkdir(path.dirname(dbPath), { recursive: true });
        }

        const db = new Database(dbPath);
        loadTreecrdtExtension(db);
        const api = createSqliteNodeApi(db);
        await api.setDocId('treecrdt-sqlite-node-bench');
        return {
          ...api,
          close: async () => {
            db.close();
            if (storage === 'file') {
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
        const medianDurationMs = quantile(durations, 0.5);
        const totalOps = workload.totalOps ?? -1;
        result = {
          name: workload.name,
          totalOps,
          durationMs: medianDurationMs,
          opsPerSec:
            totalOps > 0 && medianDurationMs > 0
              ? (totalOps / medianDurationMs) * 1000
              : medianDurationMs > 0
                ? 1000 / medianDurationMs
                : Infinity,
          extra: {
            count: totalOps > 0 ? totalOps : undefined,
            iterations,
            avgDurationMs: medianDurationMs,
            samplesMs: durations,
          },
        };
      } else {
        result = await runBenchmark(adapterFactory, workload);
      }

      const outFile = path.join(
        repoRoot,
        'benchmarks',
        'sqlite-node',
        `${storage}-${result.name}.json`,
      );
      const payload = await writeResult(result, {
        implementation: 'sqlite-node',
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

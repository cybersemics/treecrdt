import path from "node:path";
import { randomUUID } from "node:crypto";

import { benchTiming, buildWorkloads, runWorkloads } from "@treecrdt/benchmark";
import { parseBenchCliArgs, repoRootFromImportMeta, writeResult } from "@treecrdt/benchmark/node";

import { createPostgresNapiAdapterFactory } from "../src/index.js";

const POSTGRES_URL = process.env.TREECRDT_POSTGRES_URL;

async function main() {
  if (!POSTGRES_URL) {
    console.warn("Skipping postgres-napi benchmark because TREECRDT_POSTGRES_URL is not set");
    return;
  }

  const opts = parseBenchCliArgs({
    defaultWorkloads: ["insert-move", "insert-chain"] as const,
  });
  const repoRoot = repoRootFromImportMeta(import.meta.url, 3);

  const timing = benchTiming({ defaultIterations: 7 });
  const workloadDefs = buildWorkloads(opts.workloads, opts.sizes);
  for (const w of workloadDefs) {
    const totalOps = w.totalOps ?? 0;
    w.iterations = totalOps >= 10_000 ? 10 : timing.iterations;
    w.warmupIterations = timing.warmupIterations;
  }

  const factory = createPostgresNapiAdapterFactory(POSTGRES_URL);
  await factory.ensureSchema();
  await factory.resetForTests();
  const benchDocId = `bench-${randomUUID()}`;

  const results = await runWorkloads(async () => {
    await factory.resetDocForTests(benchDocId);
    return factory.open(benchDocId);
  }, workloadDefs);

  for (const result of results) {
    const outFile = opts.outFile ?? path.join(repoRoot, "benchmarks", "postgres-napi", `${result.name}.json`);
    const payload = await writeResult(result, {
      implementation: "postgres-napi",
      storage: "postgres",
      workload: result.name,
      outFile,
      extra: { count: result.totalOps, ...result.extra },
    });
    console.log(JSON.stringify(payload, null, 2));
  }
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});

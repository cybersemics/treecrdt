import path from "node:path";
import {
  benchTiming,
  buildWorkloads,
  runWorkloads,
} from "@treecrdt/benchmark";
import { parseBenchCliArgs, repoRootFromImportMeta, writeResult } from "@treecrdt/benchmark/node";
import { createWasmAdapter } from "../dist/index.js";

async function main() {
  const opts = parseBenchCliArgs({
    defaultWorkloads: ["insert-move", "insert-chain"] as const,
  });
  const repoRoot = repoRootFromImportMeta(import.meta.url, 3);

  const timing = benchTiming({ defaultIterations: 3 });
  const workloadDefs = buildWorkloads(opts.workloads, opts.sizes);
  for (const w of workloadDefs) {
    w.iterations = timing.iterations;
    w.warmupIterations = timing.warmupIterations;
  }

  const results = await runWorkloads(() => createWasmAdapter(), workloadDefs);
  for (const result of results) {
    const outFile = opts.outFile ?? path.join(repoRoot, "benchmarks", "wasm", `${result.name}.json`);
    const payload = await writeResult(result, {
      implementation: "wasm",
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

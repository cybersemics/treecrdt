import path from "node:path";
import { fileURLToPath } from "node:url";
import { runBenchmark, makeInsertMoveWorkload, makeInsertChainWorkload, writeResult } from "@treecrdt/benchmark";
import { createWasmAdapter } from "../dist/index.js";

type CliOptions = {
  count: number;
  outFile?: string;
  workload: "insert-move" | "insert-chain";
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
      if (val === "insert-move" || val === "insert-chain") {
        opts.workload = val;
      }
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
  const repoRoot = path.resolve(__dirname, "../../..");

  const sizes = opts.sizes && opts.sizes.length > 0 ? opts.sizes : [opts.count];
  for (const size of sizes) {
    const workload = makeWorkload(opts.workload, size);
    const adapterFactory = () => createWasmAdapter();
    const result = await runBenchmark(adapterFactory, workload);

    const outFile =
      opts.outFile ?? path.join(repoRoot, "benchmarks", "wasm", `${workload.name}.json`);

    const payload = await writeResult(result, {
      implementation: "wasm",
      storage: "memory",
      workload: workload.name,
      outFile,
      extra: { count: size },
    });
    console.log(JSON.stringify(payload, null, 2));
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});

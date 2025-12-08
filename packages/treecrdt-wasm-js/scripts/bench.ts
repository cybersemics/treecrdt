import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  runBenchmark,
  makeInsertMoveWorkload,
  makeInsertChainWorkload,
  makeReplayLogWorkload,
  writeResult,
} from "@treecrdt/benchmark";
import { createWasmAdapter } from "../dist/index.js";

type CliOptions = {
  count: number;
  outFile?: string;
  workload: "insert-move" | "insert-chain" | "replay-log";
  workloads?: ("insert-move" | "insert-chain" | "replay-log")[];
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

function makeWorkload(name: "insert-move" | "insert-chain" | "replay-log", count: number) {
  if (name === "insert-chain") return makeInsertChainWorkload({ count });
  if (name === "replay-log") return makeReplayLogWorkload({ count });
  return makeInsertMoveWorkload({ count });
}

async function main() {
  const opts = parseArgs();
  const __dirname = path.dirname(fileURLToPath(import.meta.url));
  const repoRoot = path.resolve(__dirname, "../../..");

  const sizes = opts.sizes && opts.sizes.length > 0 ? opts.sizes : [1, 10, 100, 1000, 10000];
  const workloads = opts.workloads && opts.workloads.length > 0 ? opts.workloads : ["insert-move", "insert-chain"];

  for (const workloadName of workloads) {
    for (const size of sizes) {
      const workload = makeWorkload(workloadName, size);
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
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});

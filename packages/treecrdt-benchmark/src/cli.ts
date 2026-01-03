import { DEFAULT_BENCH_SIZES, WORKLOAD_NAMES, type WorkloadName } from "./workloads.js";

export type BenchCliArgs = {
  sizes: number[];
  workloads: WorkloadName[];
  outFile?: string;
};

function parseNumberList(raw: string): number[] {
  return raw
    .split(",")
    .map((s) => Number(s.trim()))
    .filter((n) => Number.isFinite(n) && n > 0);
}

function parseWorkloadList(raw: string, allowed: Set<WorkloadName>): WorkloadName[] {
  const vals = raw
    .split(",")
    .map((s) => s.trim())
    .filter((s) => s.length > 0);
  return vals.filter((v): v is WorkloadName => allowed.has(v as WorkloadName));
}

export function parseBenchCliArgs(opts: {
  argv?: string[];
  defaultSizes?: readonly number[];
  defaultWorkloads?: readonly WorkloadName[];
} = {}): BenchCliArgs {
  const argv = opts.argv ?? process.argv.slice(2);
  const allowed = new Set<WorkloadName>(WORKLOAD_NAMES);

  let outFile: string | undefined;
  let count: number | undefined;
  let sizes: number[] | undefined;
  let workload: WorkloadName | undefined;
  let workloads: WorkloadName[] | undefined;

  for (const arg of argv) {
    if (arg.startsWith("--out=")) {
      outFile = arg.slice("--out=".length);
      continue;
    }
    if (arg.startsWith("--count=")) {
      const n = Number(arg.slice("--count=".length));
      if (Number.isFinite(n) && n > 0) count = n;
      continue;
    }
    if (arg.startsWith("--sizes=")) {
      const parsed = parseNumberList(arg.slice("--sizes=".length));
      if (parsed.length > 0) sizes = parsed;
      continue;
    }
    if (arg.startsWith("--workload=")) {
      const val = arg.slice("--workload=".length) as WorkloadName;
      if (allowed.has(val)) workload = val;
      continue;
    }
    if (arg.startsWith("--workloads=")) {
      const parsed = parseWorkloadList(arg.slice("--workloads=".length), allowed);
      if (parsed.length > 0) workloads = parsed;
      continue;
    }
  }

  const finalSizes =
    sizes && sizes.length > 0
      ? sizes
      : count && count > 0
        ? [count]
        : Array.from(opts.defaultSizes ?? DEFAULT_BENCH_SIZES);

  const finalWorkloads =
    workloads && workloads.length > 0
      ? workloads
      : workload
        ? [workload]
        : Array.from(opts.defaultWorkloads ?? WORKLOAD_NAMES);

  return { sizes: finalSizes, workloads: finalWorkloads, outFile };
}

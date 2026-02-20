import { DEFAULT_BENCH_SIZES, WORKLOAD_NAMES, type WorkloadName } from './workloads.js';
import { Command, InvalidArgumentError } from 'commander';

export type BenchCliArgs = {
  sizes: number[];
  workloads: WorkloadName[];
  outFile?: string;
};

function parseNumberList(raw: string): number[] {
  const parts = raw
    .split(',')
    .map((s) => s.trim())
    .filter((s) => s.length > 0);
  if (parts.length === 0) {
    throw new InvalidArgumentError('expected a comma-separated list of positive numbers');
  }

  const nums = parts.map((p) => Number(p));
  const invalid = parts.filter((p, idx) => !Number.isFinite(nums[idx]) || nums[idx] <= 0);
  if (invalid.length > 0) {
    throw new InvalidArgumentError(`invalid number(s): ${invalid.join(', ')}`);
  }
  return nums;
}

function parseWorkloadList(raw: string, allowed: Set<WorkloadName>): WorkloadName[] {
  const vals = raw
    .split(',')
    .map((s) => s.trim())
    .filter((s) => s.length > 0);
  if (vals.length === 0) {
    throw new InvalidArgumentError('expected a comma-separated list of workloads');
  }

  const invalid = vals.filter((v) => !allowed.has(v as WorkloadName));
  if (invalid.length > 0) {
    throw new InvalidArgumentError(
      `invalid workload(s): ${invalid.join(', ')} (allowed: ${WORKLOAD_NAMES.join(', ')})`,
    );
  }
  return vals as WorkloadName[];
}

export function parseBenchCliArgs(
  opts: {
    argv?: string[];
    defaultSizes?: readonly number[];
    defaultWorkloads?: readonly WorkloadName[];
  } = {},
): BenchCliArgs {
  const argv = opts.argv ?? process.argv.slice(2);
  const allowed = new Set<WorkloadName>(WORKLOAD_NAMES);
  const defaultSizes = Array.from(opts.defaultSizes ?? DEFAULT_BENCH_SIZES);
  const defaultWorkloads = Array.from(opts.defaultWorkloads ?? WORKLOAD_NAMES);

  const program = new Command()
    .name('treecrdt-bench')
    .description('TreeCRDT benchmark runner options (used by package bench scripts).')
    // Bench scripts may add their own flags (e.g. --storage); ignore unknown flags here.
    .allowUnknownOption(true)
    .allowExcessArguments(true)
    .option('--out <file>', 'write output JSON to file')
    .option('--count <n>', 'shorthand for --sizes <n>', (val) => {
      const n = Number(val);
      if (!Number.isFinite(n) || n <= 0)
        throw new InvalidArgumentError(`invalid --count value: ${val}`);
      return n;
    })
    .option(
      '--sizes <n1,n2,...>',
      `comma-separated benchmark sizes (default: ${defaultSizes.join(',')})`,
      (val) => {
        return parseNumberList(val);
      },
    )
    .option('--workload <name>', `single workload (${WORKLOAD_NAMES.join(', ')})`, (val) => {
      if (!allowed.has(val as WorkloadName)) {
        throw new InvalidArgumentError(
          `invalid --workload value: ${val} (allowed: ${WORKLOAD_NAMES.join(', ')})`,
        );
      }
      return val as WorkloadName;
    })
    .option(
      '--workloads <w1,w2,...>',
      `comma-separated workloads (allowed: ${WORKLOAD_NAMES.join(', ')})`,
      (val) => {
        const parsed = parseWorkloadList(val, allowed);
        return Array.from(new Set(parsed));
      },
    );

  program.parse(argv, { from: 'user' });

  const parsed = program.opts<{
    out?: string;
    count?: number;
    sizes?: number[];
    workload?: WorkloadName;
    workloads?: WorkloadName[];
  }>();

  const outFile = parsed.out && parsed.out.length > 0 ? parsed.out : undefined;
  const count = parsed.count;
  const sizes = parsed.sizes;
  const workload = parsed.workload;
  const workloads = parsed.workloads;

  const finalSizes =
    sizes && sizes.length > 0 ? sizes : count && count > 0 ? [count] : defaultSizes;

  const finalWorkloads =
    workloads && workloads.length > 0 ? workloads : workload ? [workload] : defaultWorkloads;

  return { sizes: finalSizes, workloads: finalWorkloads, outFile };
}

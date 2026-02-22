export const WORKLOAD_NAMES = ['insert-move', 'insert-chain', 'replay-log'] as const;
export type WorkloadName = (typeof WORKLOAD_NAMES)[number];

export const DEFAULT_BENCH_SIZES = [100, 1000, 10000] as const;

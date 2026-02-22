import { envInt } from './stats.js';

type EnvKey = string | readonly string[];

function firstEnvInt(keys: EnvKey): number | undefined {
  if (typeof keys === 'string') return envInt(keys);
  for (const key of keys) {
    const val = envInt(key);
    if (val !== undefined) return val;
  }
  return undefined;
}

export function benchTiming(
  opts: {
    iterationsEnv?: EnvKey;
    warmupEnv?: EnvKey;
    defaultIterations?: number;
  } = {},
): { iterations: number; warmupIterations: number } {
  const iterationsEnv = opts.iterationsEnv ?? 'BENCH_ITERATIONS';
  const warmupEnv = opts.warmupEnv ?? 'BENCH_WARMUP';
  const iterations = Math.max(1, firstEnvInt(iterationsEnv) ?? opts.defaultIterations ?? 1);
  const warmupIterations = Math.max(0, firstEnvInt(warmupEnv) ?? (iterations > 1 ? 1 : 0));
  return { iterations, warmupIterations };
}

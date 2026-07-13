import assert from 'node:assert/strict';
import test from 'node:test';

import { runBenchmark } from '../dist/index.js';

async function countRuns(workload, envIterations) {
  const previous = process.env.BENCH_ITERATIONS;
  if (envIterations === undefined) delete process.env.BENCH_ITERATIONS;
  else process.env.BENCH_ITERATIONS = envIterations;

  let runs = 0;
  try {
    await runBenchmark(() => ({}), {
      name: 'tiny',
      totalOps: 1,
      warmupIterations: 0,
      run: async () => {
        runs += 1;
      },
      ...workload,
    });
    return runs;
  } finally {
    if (previous === undefined) delete process.env.BENCH_ITERATIONS;
    else process.env.BENCH_ITERATIONS = previous;
  }
}

test('an explicit workload iteration count overrides the tiny-workload heuristic', async () => {
  assert.equal(await countRuns({ iterations: 1 }, '4'), 1);
});

test('an environment iteration count overrides the tiny-workload heuristic', async () => {
  assert.equal(await countRuns({}, '3'), 3);
});

test('tiny workloads still receive extra samples when no iteration count is requested', async () => {
  assert.equal(await countRuns({}, undefined), 10);
});

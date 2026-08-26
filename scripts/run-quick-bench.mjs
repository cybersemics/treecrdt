import { spawnSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';

import { repoRootFromImportMeta } from './repo-root.mjs';

const repoRoot = process.env.TREECRDT_BENCH_ROOT
  ? path.resolve(process.env.TREECRDT_BENCH_ROOT)
  : repoRootFromImportMeta(import.meta.url, 1);
const quickEnv = {
  BENCH_ITERATIONS: '3',
  BENCH_WARMUP: '1',
};

const commands = [
  ['pnpm', ['run', 'benchmark:web', '--grep-invert=sync']],
  ['pnpm', ['-C', 'packages/treecrdt-sqlite-node', 'run', 'benchmark:ops', '--', '--count=1000']],
  [
    'pnpm',
    ['-C', 'packages/treecrdt-sqlite-node', 'run', 'benchmark:note-paths', '--', '--count=10000'],
  ],
  [process.execPath, ['scripts/run-sync-bench.mjs', 'direct', '--count=1000']],
  [
    'pnpm',
    [
      '-C',
      'packages/treecrdt-wasm-js',
      'run',
      'benchmark',
      '--',
      '--sizes=1000',
      '--workloads=insert-move,insert-chain',
    ],
  ],
  [
    'cargo',
    [
      'bench',
      '-p',
      'treecrdt-core',
      '--bench',
      'core',
      '--features',
      'bench',
      '--',
      '--count=1000',
    ],
  ],
  ['pnpm', ['run', 'benchmark:aggregate']],
];

fs.rmSync(path.join(repoRoot, 'benchmarks'), { recursive: true, force: true });
fs.mkdirSync(path.join(repoRoot, 'benchmarks'), { recursive: true });

for (const [command, args] of commands) {
  console.log(`\n[benchmark:quick] ${command} ${args.join(' ')}`);
  const result = spawnSync(command, args, {
    cwd: repoRoot,
    env: { ...process.env, ...quickEnv },
    stdio: 'inherit',
  });
  if (result.error) throw result.error;
  if (result.status !== 0) process.exit(result.status ?? 1);
}

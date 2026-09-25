import { execFileSync } from 'node:child_process';
import { unlinkSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

const root = fileURLToPath(new URL('../', import.meta.url));
execFileSync(
  'wasm-pack',
  [
    'build',
    '../treecrdt-wasm',
    '--target',
    'web',
    '--out-dir',
    '../treecrdt-wasm-js/pkg-web',
    '--release',
  ],
  {
    cwd: root,
    stdio: 'inherit',
  },
);
// wasm-pack's generated ignore file otherwise excludes the binary from package-manager tarballs.
try {
  unlinkSync(new URL('../pkg-web/.gitignore', import.meta.url));
} catch (error) {
  if (error.code !== 'ENOENT') throw error;
}

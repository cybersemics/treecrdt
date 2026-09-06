import { unlinkSync } from 'node:fs';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const packageDirectory = fileURLToPath(new URL('../', import.meta.url));
for (const [target, directory] of [
  ['nodejs', 'pkg'],
  ['web', 'pkg-web'],
]) {
  const outputDirectory = fileURLToPath(new URL(`../${directory}`, import.meta.url));
  const result = spawnSync(
    'wasm-pack',
    ['build', '../treecrdt-wasm', '--target', target, '--out-dir', outputDirectory, '--release'],
    { cwd: packageDirectory, stdio: 'inherit' },
  );
  if (result.error) throw result.error;
  if (result.status !== 0) process.exit(result.status ?? 1);
  // wasm-pack ignores generated artifacts by default, which also excludes them from npm packs.
  unlinkSync(new URL(`../${directory}/.gitignore`, import.meta.url));
}

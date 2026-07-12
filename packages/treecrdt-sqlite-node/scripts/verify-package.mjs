import fs from 'node:fs';
import path from 'node:path';
import { dirnameFromImportMeta } from '../../../scripts/repo-root.mjs';

const scriptDir = dirnameFromImportMeta(import.meta.url);
const packageRoot = path.resolve(scriptDir, '..');
const nativeDir = path.join(packageRoot, 'native');
const supportedArtifacts = new Map([
  ['x86_64-unknown-linux-gnu', 'libtreecrdt_sqlite_ext-linux-x64.so'],
  ['aarch64-apple-darwin', 'libtreecrdt_sqlite_ext-darwin-arm64.dylib'],
  ['x86_64-apple-darwin', 'libtreecrdt_sqlite_ext-darwin-x64.dylib'],
  ['x86_64-pc-windows-msvc', 'treecrdt_sqlite_ext-win32-x64.dll'],
]);

function currentNativeFile() {
  const ext =
    process.platform === 'darwin' ? '.dylib' : process.platform === 'win32' ? '.dll' : '.so';
  const base = ext === '.dll' ? 'treecrdt_sqlite_ext' : 'libtreecrdt_sqlite_ext';
  return `${base}-${process.platform}-${process.arch}${ext}`;
}

function requireFile(file, message) {
  if (!fs.existsSync(file)) {
    throw new Error(`${message}: ${path.relative(packageRoot, file)}`);
  }

  const stat = fs.lstatSync(file);
  if (!stat.isFile()) {
    throw new Error(`Expected a regular file: ${path.relative(packageRoot, file)}`);
  }

  if (stat.size === 0) {
    throw new Error(`Expected a non-empty file: ${path.relative(packageRoot, file)}`);
  }
}

requireFile(path.join(packageRoot, 'dist', 'index.js'), 'Missing built JavaScript');
requireFile(path.join(packageRoot, 'dist', 'index.d.ts'), 'Missing built TypeScript declarations');

const currentOnly = process.argv.includes('--current');
const expectedArtifacts = currentOnly
  ? [...supportedArtifacts].filter(([, file]) => file === currentNativeFile())
  : supportedArtifacts;

if (expectedArtifacts.length === 0) {
  throw new Error(`Unsupported native target: ${process.platform}/${process.arch}`);
}

for (const [triple, file] of expectedArtifacts) {
  requireFile(path.join(nativeDir, file), `Missing TreeCRDT SQLite artifact for ${triple}`);
}

if (!currentOnly) {
  const supportedFiles = new Set(supportedArtifacts.values());
  const unexpectedFiles = fs
    .readdirSync(nativeDir, { withFileTypes: true })
    .filter((entry) => !supportedFiles.has(entry.name))
    .map((entry) => entry.name)
    .sort();
  if (unexpectedFiles.length > 0) {
    throw new Error(`Unexpected native package artifacts: ${unexpectedFiles.join(', ')}`);
  }
}

console.log(
  `Verified @treecrdt/sqlite-node ${currentOnly ? 'current-platform' : 'release'} artifacts`,
);

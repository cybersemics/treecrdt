import fs from 'node:fs';
import path from 'node:path';
import { dirnameFromImportMeta } from '../../../scripts/repo-root.mjs';

const scriptDir = dirnameFromImportMeta(import.meta.url);
const packageRoot = path.resolve(scriptDir, '..');

function platformExtension() {
  switch (process.platform) {
    case 'darwin':
      return '.dylib';
    case 'win32':
      return '.dll';
    default:
      return '.so';
  }
}

function expectedNativePath() {
  const ext = platformExtension();
  const base = ext === '.dll' ? 'treecrdt_sqlite_ext' : 'libtreecrdt_sqlite_ext';
  return path.join(packageRoot, 'native', `${base}-${process.platform}-${process.arch}${ext}`);
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
requireFile(expectedNativePath(), 'Missing built TreeCRDT SQLite native extension');

console.log('Verified @treecrdt/sqlite-node package artifacts');

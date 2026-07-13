import path from 'node:path';
import { createRequire } from 'node:module';
import { dirnameFromImportMeta } from '../../../scripts/repo-root.mjs';

const scriptDir = dirnameFromImportMeta(import.meta.url);
const packageRoot = path.resolve(scriptDir, '..');
const extensionArg = process.argv[2];
const moduleRoot = path.resolve(process.argv[3] ?? packageRoot);

if (!extensionArg) {
  throw new Error('Usage: node smoke-native.mjs <extension-path> [better-sqlite3-module-root]');
}

const extensionPath = path.resolve(extensionArg);
const require = createRequire(path.join(moduleRoot, 'package.json'));
const Database = require('better-sqlite3');
const db = new Database(':memory:');

try {
  db.loadExtension(extensionPath, 'sqlite3_treecrdt_init');
  const row = db.prepare('SELECT treecrdt_version() AS version').get();
  if (typeof row?.version !== 'string' || row.version.length === 0) {
    throw new Error('treecrdt_version() did not return a version string');
  }
  console.log(`Loaded ${path.basename(extensionPath)} (TreeCRDT ${row.version})`);
} finally {
  db.close();
}

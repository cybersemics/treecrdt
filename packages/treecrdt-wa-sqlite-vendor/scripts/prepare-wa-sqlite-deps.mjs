import { execFileSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';

const packageRoot = process.cwd();
const makefilePath = path.join(packageRoot, 'wa-sqlite', 'Makefile');
const makefile = fs.readFileSync(makefilePath, 'utf8');
const version = /^SQLITE_VERSION\s*=\s*(\S+)/m.exec(makefile)?.[1];

if (!version) {
  throw new Error(`SQLITE_VERSION not found in ${makefilePath}`);
}

execFileSync(
  'make',
  ['-C', 'wa-sqlite', `deps/${version}/sqlite3.c`, 'deps/extension-functions.c'],
  {
    cwd: packageRoot,
    stdio: 'inherit',
  },
);

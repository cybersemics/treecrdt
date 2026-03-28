import { describe } from 'vitest';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { defineSyncBackendContract } from '../../sync/protocol/tests/helpers/sync-backend-contract.ts';
import { createTreecrdtSyncBackendFromClient } from '../../sync/material/sqlite/dist/backend.js';

import {
  createTreecrdtClient,
  defaultExtensionPath,
  loadTreecrdtExtension,
} from '../dist/index.js';

async function loadDatabaseCtor() {
  return (
    await import('better-sqlite3').catch((err) => {
      throw new Error(
        `better-sqlite3 native binding not available; ensure it is installed/built before running native tests: ${err}`,
      );
    })
  ).default;
}

describe('sqlite-node sync backend contract', () => {
  defineSyncBackendContract('sqlite-node sync backend', async () => {
    const Database = await loadDatabaseCtor();
    const dir = mkdtempSync(join(tmpdir(), 'treecrdt-node-sync-backend-'));
    const dbPath = join(dir, 'backend.sqlite');
    const dbs: Array<{ close: () => void }> = [];

    return {
      supportsDocIsolationAcrossOpen: false,
      openBackend: async (docId) => {
        const db = new Database(dbPath);
        dbs.push(db);
        loadTreecrdtExtension(db, { extensionPath: defaultExtensionPath() });
        return createTreecrdtSyncBackendFromClient(
          await createTreecrdtClient(db, { docId }),
          docId,
        );
      },
      close: async () => {
        for (const db of dbs) db.close();
        rmSync(dir, { recursive: true, force: true });
      },
    };
  });
});

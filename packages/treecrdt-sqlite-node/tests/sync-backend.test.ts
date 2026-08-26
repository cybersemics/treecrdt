import { describe } from 'vitest';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { defineSyncBackendContract } from '../../sync-protocol/protocol/tests/helpers/sync-backend-contract.ts';
import { createTreecrdtSyncBackendFromClient } from '../../sync-protocol/material/sqlite/dist/backend.js';

import { createTreecrdtClient, type SqliteNodeClient } from '../dist/index.js';

describe('sqlite-node sync backend contract', () => {
  defineSyncBackendContract('sqlite-node sync backend', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'treecrdt-node-sync-backend-'));
    const dbPath = join(dir, 'backend.sqlite');
    const clients: SqliteNodeClient[] = [];

    return {
      supportsDocIsolationAcrossOpen: false,
      openBackend: async (docId) => {
        const client = await createTreecrdtClient({
          docId,
          storage: { type: 'file', filename: dbPath },
        });
        clients.push(client);
        return createTreecrdtSyncBackendFromClient(client, docId);
      },
      close: async () => {
        for (const client of clients) await client.close();
        rmSync(dir, { recursive: true, force: true });
      },
    };
  });
});

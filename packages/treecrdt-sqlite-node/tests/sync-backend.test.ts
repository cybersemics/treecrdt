import { describe, expect, test } from 'vitest';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { makeOp, nodeIdFromInt } from '@treecrdt/benchmark';
import { bytesToHex, nodeIdToBytes16 } from '@treecrdt/interface/ids';
import { deriveOpRefV0 } from '@treecrdt/sync-protocol';

import { defineSyncBackendContract } from '../../sync-protocol/protocol/tests/helpers/sync-backend-contract.ts';
import { createTreecrdtSyncBackendFromClient } from '../../sync-protocol/material/sqlite/dist/backend.js';

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

describe('sqlite-node sync backend payload fallback', () => {
  test('lists the create-only insert that wrote a scope root payload when its row is gone', async () => {
    const Database = await loadDatabaseCtor();
    const db = new Database(':memory:');
    try {
      loadTreecrdtExtension(db, { extensionPath: defaultExtensionPath() });
      const docId = 'doc-payload-fallback';
      const backend = createTreecrdtSyncBackendFromClient(
        await createTreecrdtClient(db, { docId }),
        docId,
      );
      const replica = new Uint8Array(32).fill(7);
      const root = '0'.repeat(32);
      const [placed, created] = [nodeIdFromInt(120), nodeIdFromInt(121)];
      const key = (digit: number) => Uint8Array.of(0, digit);
      await backend.applyOps([
        makeOp(replica, 1, 1, { type: 'insert', parent: root, node: placed, orderKey: key(1) }),
        // The node is in the tree, so this create changes nothing, its payload included.
        makeOp(replica, 2, 2, {
          type: 'insert',
          parent: root,
          node: placed,
          orderKey: key(2),
          payload: Uint8Array.of(1),
          ifAbsent: true,
        }),
        makeOp(replica, 3, 3, {
          type: 'insert',
          parent: root,
          node: created,
          orderKey: key(3),
          payload: Uint8Array.of(2),
          ifAbsent: true,
        }),
      ]);
      // Stale derived state, as after a failed materialization: the backend reads the op log.
      db.exec('DELETE FROM tree_payload');

      const refsOf = async (parent: string) =>
        (await backend.listOpRefs({ children: { parent: nodeIdToBytes16(parent) } })).map(
          bytesToHex,
        );
      const refOf = (counter: bigint) => bytesToHex(deriveOpRefV0(docId, { replica, counter }));
      expect(await refsOf(created)).toContain(refOf(3n));
      expect(await refsOf(placed)).not.toContain(refOf(2n));
    } finally {
      db.close();
    }
  });
});

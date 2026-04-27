import Database from 'better-sqlite3';
import { defineProofMaterialStoreContract } from '../../protocol/tests/helpers/proof-material-contract.ts';
import { definePendingProofMaterialStoreContract } from '../../protocol/tests/helpers/pending-proof-material-contract.ts';
import { defineReplayOnlyAuthStoreContract } from '../../protocol/tests/helpers/replay-only-auth-contract.ts';
import type { SqliteRunner } from '@treecrdt/interface/sqlite';

import {
  createCapabilityMaterialStore,
  createOpAuthStore,
  createPendingOpsStore,
} from '../dist/index.js';

function createRunner(db: Database.Database): SqliteRunner {
  const toBindings = (params: unknown[]) =>
    params.reduce<Record<number, unknown>>((acc, value, index) => {
      acc[index + 1] = value instanceof Uint8Array ? Buffer.from(value) : value;
      return acc;
    }, {});

  return {
    exec: async (sql) => {
      db.exec(sql);
    },
    getText: async (sql, params = []) => {
      const row = db.prepare(sql).get(toBindings(params)) as Record<string, unknown> | undefined;
      if (!row) return null;
      const value = Object.values(row)[0];
      if (value === undefined || value === null) return null;
      if (Buffer.isBuffer(value)) return value.toString('utf8');
      if (value instanceof Uint8Array) return Buffer.from(value).toString('utf8');
      return typeof value === 'string' ? value : String(value);
    },
  };
}

defineProofMaterialStoreContract('sqlite proof material stores', async () => {
  const db = new Database(':memory:');
  const runner = createRunner(db);

  return {
    createDocStores: async (docId) => {
      const opAuth = createOpAuthStore({ runner, docId });
      const capabilities = createCapabilityMaterialStore({ runner, docId });
      await opAuth.init();
      await capabilities.init();
      return { opAuth, capabilities };
    },
    close: async () => {
      db.close();
    },
  };
});

definePendingProofMaterialStoreContract('sqlite pending proof material stores', async () => {
  const db = new Database(':memory:');
  const runner = createRunner(db);

  return {
    createPendingStore: async (docId) => {
      const pending = createPendingOpsStore({ runner, docId });
      return pending;
    },
    close: async () => {
      db.close();
    },
  };
});

defineReplayOnlyAuthStoreContract('sqlite replay-only auth material stores', async () => {
  const db = new Database(':memory:');
  const runner = createRunner(db);

  return {
    createDocStores: async (docId) => {
      const opAuth = createOpAuthStore({ runner, docId });
      const capabilities = createCapabilityMaterialStore({ runner, docId });
      await opAuth.init();
      await capabilities.init();
      return { opAuth, capabilities };
    },
    close: async () => {
      db.close();
    },
  };
});

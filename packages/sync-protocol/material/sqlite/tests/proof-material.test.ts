import Database from 'better-sqlite3';
import { expect, test } from 'vitest';
import { defineProofMaterialStoreContract } from '../../protocol/tests/helpers/proof-material-contract.ts';
import { definePendingProofMaterialStoreContract } from '../../protocol/tests/helpers/pending-proof-material-contract.ts';
import { defineReplayOnlyAuthStoreContract } from '../../protocol/tests/helpers/replay-only-auth-contract.ts';
import type { SqliteRunner } from '@justthrowaway/interface/sqlite';

import {
  createCapabilityMaterialStore,
  createOpAuthStore,
  createPendingOpsStore,
  createTreecrdtSqliteAuthApi,
  createTreecrdtSqliteAuthBackend,
  createTreecrdtSqliteAuthSession,
  createTreecrdtSqliteSyncDiagnostics,
} from '../dist/index.js';

function testKey(byte: number): Uint8Array {
  return new Uint8Array(32).fill(byte);
}

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

test('sqlite auth backend helper bundles scope evaluator and auth stores', async () => {
  const db = new Database(':memory:');
  const runner = createRunner(db);

  try {
    const backend = createTreecrdtSqliteAuthBackend({ runner, docId: 'doc-auth-helper' });
    expect(typeof backend.scopeEvaluator).toBe('function');
    expect('pendingOpsStore' in backend).toBe(false);

    await backend.capabilityStore.init();
    await backend.opAuthStore.init();

    await backend.capabilityStore.storeCapabilities([
      { name: 'auth.capability', value: 'token-a' },
    ]);
    await expect(backend.capabilityStore.listCapabilities()).resolves.toEqual([
      { name: 'auth.capability', value: 'token-a' },
    ]);
  } finally {
    db.close();
  }
});

test('sqlite auth session helper wires the backend automatically', async () => {
  const db = new Database(':memory:');
  const runner = createRunner(db);

  try {
    const session = createTreecrdtSqliteAuthSession({
      runner,
      docId: 'doc-auth-session-helper',
      trust: { issuerPublicKeys: [] },
      local: {
        privateKey: testKey(4),
        publicKey: testKey(5),
      },
      allowUnsigned: true,
    });
    await session.ready;
    expect(session.getState().status).toBe('ready');
  } finally {
    db.close();
  }
});

test('sqlite client auth api hides runner wiring', async () => {
  const db = new Database(':memory:');
  const runner = createRunner(db);

  try {
    const auth = createTreecrdtSqliteAuthApi({
      runner,
      docId: 'doc-client-auth-api',
    });
    const session = auth.createSession({
      trust: { issuerPublicKeys: [] },
      local: {
        privateKey: testKey(4),
        publicKey: testKey(5),
      },
      allowUnsigned: true,
    });

    await session.ready;
    expect(session.getState().status).toBe('ready');
  } finally {
    db.close();
  }
});

test('sqlite sync diagnostics lists pending ops without exposing the raw store', async () => {
  const db = new Database(':memory:');
  const runner = createRunner(db);

  try {
    const diagnostics = createTreecrdtSqliteSyncDiagnostics({
      runner,
      docId: 'doc-diagnostics',
    });
    await expect(diagnostics.listPendingOps()).resolves.toEqual([]);
  } finally {
    db.close();
  }
});

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

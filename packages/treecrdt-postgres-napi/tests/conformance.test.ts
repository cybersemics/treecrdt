import { randomUUID } from 'node:crypto';

import { describe, expect, test, vi } from 'vitest';

import type { Operation } from '@treecrdt/interface';
import type { TreecrdtEngine } from '@treecrdt/interface/engine';
import { bytesToHex } from '@treecrdt/interface/ids';
import { deriveOpRefV0 } from '@treecrdt/sync-protocol';
import {
  conformanceSlugify,
  runTreecrdtEngineConformanceScenario,
  treecrdtEngineConformanceScenarios,
} from '@treecrdt/engine-conformance';

import { createTreecrdtPostgresClient } from '../dist/index.js';

const POSTGRES_URL = process.env.TREECRDT_POSTGRES_URL;
const maybeDescribe = POSTGRES_URL ? describe : describe.skip;
const root = '0'.repeat(32);
const replica = Uint8Array.from({ length: 32 }, (_, i) => (i === 31 ? 1 : 0));

function nodeIdFromInt(n: number): string {
  return n.toString(16).padStart(32, '0');
}

function publicOpRef(docId: string, op: Operation): Uint8Array {
  return deriveOpRefV0(docId, {
    replica: op.meta.id.replica,
    counter: op.meta.id.counter,
  });
}

function wrapDocId(inner: TreecrdtEngine, publicDocId: string): TreecrdtEngine {
  const opRefsAll = async (): Promise<Uint8Array[]> => {
    const all = await inner.ops.all();
    return all.map((op) => publicOpRef(publicDocId, op));
  };

  const opRefsChildren = async (parent: string): Promise<Uint8Array[]> => {
    const ops = await inner.ops.children(parent);
    return ops.map((op) => publicOpRef(publicDocId, op));
  };

  const getByPublicOpRefs = async (opRefs: Uint8Array[]): Promise<Operation[]> => {
    if (opRefs.length === 0) return [];
    const all = await inner.ops.all();
    const byOpRef = new Map<string, Operation>();
    for (const op of all) byOpRef.set(bytesToHex(publicOpRef(publicDocId, op)), op);
    return opRefs.map((opRef) => {
      const op = byOpRef.get(bytesToHex(opRef));
      if (!op) throw new Error('opRef missing locally');
      return op;
    });
  };

  return {
    ...inner,
    docId: publicDocId,
    ops: {
      ...inner.ops,
      get: getByPublicOpRefs,
    },
    opRefs: {
      all: opRefsAll,
      children: opRefsChildren,
    },
  };
}

function internalDocId(publicDocId: string, key: string): string {
  return `${publicDocId}::${key}::${randomUUID()}`;
}

test('conformance registry includes materialization-event scenarios', () => {
  const names = treecrdtEngineConformanceScenarios().map((s) => s.name);
  expect(names).toContain('materialization events: structural batch');
  expect(names).toContain('materialization events: payload coalescing');
  expect(names).toContain('materialization events: defensive restore');
  expect(names).toContain('local ops: materialization events include writeId');
});

maybeDescribe('engine conformance scenarios (postgres-napi engine)', () => {
  test('postgres auth-aware local write rolls back on auth failure', async () => {
    const client = await createTreecrdtPostgresClient(POSTGRES_URL!, {
      docId: internalDocId('postgres-auth-local-rollback', 'auth-failure'),
    });
    const events: unknown[] = [];
    const unsubscribe = client.onMaterialized((event) => events.push(event));
    const authSession = {
      authorizeLocalOps: vi.fn(async () => {
        throw new Error('local auth denied');
      }),
    };
    const node = nodeIdFromInt(10);

    try {
      await expect(
        client.local.insert(replica, root, node, { type: 'last' }, null, { authSession }),
      ).rejects.toThrow('local auth denied');

      expect(authSession.authorizeLocalOps).toHaveBeenCalledTimes(1);
      expect(events).toHaveLength(0);
      expect(await client.tree.exists(node)).toBe(false);
      expect(await client.ops.all()).toHaveLength(0);
    } finally {
      unsubscribe();
      await client.close();
    }
  });

  test('postgres auth-aware local write emits materialization after auth succeeds', async () => {
    const client = await createTreecrdtPostgresClient(POSTGRES_URL!, {
      docId: internalDocId('postgres-auth-local-success', 'auth-success'),
    });
    const events: unknown[] = [];
    const unsubscribe = client.onMaterialized((event) => events.push(event));
    const authSession = {
      authorizeLocalOps: vi.fn(async () => {
        expect(events).toHaveLength(0);
      }),
    };
    const node = nodeIdFromInt(11);

    try {
      const op = await client.local.insert(replica, root, node, { type: 'last' }, null, {
        authSession,
      });

      expect(op.kind.type).toBe('insert');
      expect(authSession.authorizeLocalOps).toHaveBeenCalledTimes(1);
      expect(events).toHaveLength(1);
      expect(await client.tree.exists(node)).toBe(true);
      expect(await client.ops.all()).toHaveLength(1);
    } finally {
      unsubscribe();
      await client.close();
    }
  });

  for (const scenario of treecrdtEngineConformanceScenarios()) {
    test(`postgres engine conformance: ${scenario.name}`, async () => {
      const persistentInternal = new Map<string, string>();
      let ephemeralIndex = 0;

      const openWrapped = async (opts: {
        docId: string;
        persistentName?: string;
      }): Promise<TreecrdtEngine> => {
        const persistentKey =
          opts.persistentName == null
            ? null
            : `${opts.docId}:${conformanceSlugify(opts.persistentName) || 'db'}`;
        const existingPersistentDoc = persistentKey
          ? persistentInternal.get(persistentKey)
          : undefined;
        const actualDocId =
          existingPersistentDoc ??
          internalDocId(
            opts.docId,
            persistentKey
              ? `persistent-${conformanceSlugify(opts.persistentName || 'db') || 'db'}`
              : `peer-${ephemeralIndex++}`,
          );
        if (persistentKey && !existingPersistentDoc)
          persistentInternal.set(persistentKey, actualDocId);

        const raw = await createTreecrdtPostgresClient(POSTGRES_URL!, { docId: actualDocId });
        return wrapDocId(raw, opts.docId);
      };

      await runTreecrdtEngineConformanceScenario(scenario, {
        docIdPrefix: 'treecrdt-postgres-conformance',
        openEngine: ({ docId }) => openWrapped({ docId }),
        openPersistentEngine: ({ docId, name }) => openWrapped({ docId, persistentName: name }),
      });
    }, 90_000);
  }
});

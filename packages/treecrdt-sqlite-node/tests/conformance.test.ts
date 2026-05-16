import { expect, test, vi } from 'vitest';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  conformanceSlugify,
  runTreecrdtEngineConformanceScenario,
  treecrdtEngineConformanceScenarios,
} from '@treecrdt/engine-conformance';
import {
  createTreecrdtClient,
  defaultExtensionPath,
  loadTreecrdtExtension,
} from '../dist/index.js';

const root = '0'.repeat(32);
const replica = Uint8Array.from({ length: 32 }, (_, i) => (i === 31 ? 1 : 0));

function nodeIdFromInt(n: number): string {
  return n.toString(16).padStart(32, '0');
}

async function createNodeEngine(opts: { docId: string; path?: string }) {
  const { default: Database } = await import('better-sqlite3').catch((err) => {
    throw new Error(
      `better-sqlite3 native binding not available; ensure it is installed/built before running native tests: ${err}`,
    );
  });

  const db = new Database(opts.path ?? ':memory:');
  loadTreecrdtExtension(db, { extensionPath: defaultExtensionPath() });
  return await createTreecrdtClient(db, { docId: opts.docId });
}

test('conformance registry includes materialization-event scenarios', () => {
  const names = treecrdtEngineConformanceScenarios().map((s) => s.name);
  expect(names).toContain('materialization events: structural batch');
  expect(names).toContain('materialization events: payload coalescing');
  expect(names).toContain('materialization events: defensive restore');
  expect(names).toContain('local ops: materialization events include writeId');
});

test('sqlite auth-aware local write rolls back on auth failure', async () => {
  const client = await createNodeEngine({ docId: 'sqlite-auth-local-rollback' });
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

test('sqlite auth-aware local write emits materialization after auth succeeds', async () => {
  const client = await createNodeEngine({ docId: 'sqlite-auth-local-success' });
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
  test(`sqlite engine conformance (node): ${scenario.name}`, async () => {
    let persistentDir: string | null = null;
    const persistentPaths = new Map<string, string>();
    const ensurePersistentDir = () => {
      if (persistentDir) return persistentDir;
      persistentDir = mkdtempSync(join(tmpdir(), 'treecrdt-node-conformance-'));
      return persistentDir;
    };

    await runTreecrdtEngineConformanceScenario(scenario, {
      docIdPrefix: 'treecrdt-node-conformance',
      openEngine: ({ docId }) => createNodeEngine({ docId }),
      openPersistentEngine: ({ docId, name }) => {
        const dir = ensurePersistentDir();
        const key = conformanceSlugify(name || 'db');
        const existing = persistentPaths.get(key);
        const path = existing ?? join(dir, `${key}.sqlite`);
        persistentPaths.set(key, path);
        return createNodeEngine({ docId, path });
      },
      cleanup: () => {
        if (persistentDir) rmSync(persistentDir, { recursive: true, force: true });
      },
    });
  });
}

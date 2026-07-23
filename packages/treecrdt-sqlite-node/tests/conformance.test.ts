import { expect, test, vi } from 'vitest';
import { existsSync, mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, relative, resolve } from 'node:path';
import {
  conformanceSlugify,
  runTreecrdtEngineConformanceScenario,
  treecrdtEngineConformanceScenarios,
} from '@treecrdt/engine-conformance';
import { createTreecrdtClient, type SqliteNodeClient } from '../dist/index.js';

const root = '0'.repeat(32);
const replica = Uint8Array.from({ length: 32 }, (_, i) => (i === 31 ? 1 : 0));

function nodeIdFromInt(n: number): string {
  return n.toString(16).padStart(32, '0');
}

async function createNodeEngine(opts: { docId: string; path?: string }) {
  return await createTreecrdtClient({
    docId: opts.docId,
    storage: opts.path ? { type: 'file', filename: opts.path } : { type: 'memory' },
  });
}

test('conformance registry includes materialization-event scenarios', () => {
  const names = treecrdtEngineConformanceScenarios().map((s) => s.name);
  expect(names).toContain('materialization events: structural batch');
  expect(names).toContain('materialization events: payload coalescing');
  expect(names).toContain('materialization events: defensive restore');
  expect(names).toContain('local ops: materialization changes include writeId');
});

test('sqlite-node managed client defaults to direct in-memory storage', async () => {
  const client = await createTreecrdtClient({ docId: 'sqlite-node-managed-memory' });
  const node = nodeIdFromInt(1);

  try {
    expect(client.mode).toBe('node');
    expect(client.runtime).toBe('direct');
    expect(client.storage).toBe('memory');
    expect(client.filename).toBe(':memory:');

    await client.local.insert(replica, root, node, { type: 'last' }, null);
    expect(await client.tree.exists(node)).toBe(true);

    await client.close();
    await client.close();
  } finally {
    await client.drop();
  }
});

test('sqlite-node managed client reopens and drops file storage', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'treecrdt-node-managed-file-'));
  const filename = join(dir, 'treecrdt.sqlite');
  const relativeFilename = relative(process.cwd(), filename);
  const node = nodeIdFromInt(2);
  let reopened: SqliteNodeClient | null = null;

  try {
    const client = await createTreecrdtClient({
      docId: 'sqlite-node-managed-file',
      storage: { type: 'file', filename: relativeFilename },
    });

    try {
      expect(client.runtime).toBe('direct');
      expect(client.storage).toBe('file');
      expect(client.filename).toBe(resolve(relativeFilename));
      await client.local.insert(replica, root, node, { type: 'last' }, null);
    } finally {
      await client.close();
    }

    reopened = await createTreecrdtClient({
      docId: 'sqlite-node-managed-file',
      storage: { type: 'file', filename },
    });
    expect(await reopened.tree.exists(node)).toBe(true);

    await reopened.drop();
    reopened = null;
    expect(existsSync(filename)).toBe(false);
  } finally {
    await reopened?.drop();
    rmSync(dir, { recursive: true, force: true });
  }
});

test('sqlite-node managed client closes the database when initialization fails', async () => {
  const { default: Database } = await import('better-sqlite3');
  const close = vi.spyOn(Database.prototype, 'close');

  try {
    await expect(
      createTreecrdtClient({
        extension: { extensionPath: '/missing/treecrdt-extension' },
      }),
    ).rejects.toThrow();
    expect(close).toHaveBeenCalledOnce();
  } finally {
    close.mockRestore();
  }
});

test('sqlite-node managed client rejects unsupported runtime options', async () => {
  await expect(
    createTreecrdtClient({ runtime: { type: 'dedicated-worker' } as any }),
  ).rejects.toThrow('@treecrdt/sqlite-node only supports runtime');
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

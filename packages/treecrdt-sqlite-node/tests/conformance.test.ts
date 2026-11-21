import { expect, test, vi } from 'vitest';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  getEd25519PublicKey,
  issueTreecrdtCapabilityTokenV1,
  randomEd25519SecretKey,
} from '@treecrdt/auth';
import {
  conformanceSlugify,
  runTreecrdtEngineConformanceScenario,
  treecrdtEngineConformanceScenarios,
} from '@treecrdt/engine-conformance';
import {
  createDurableMaterialization,
  createMaterializationDispatcher,
  type MaterializationCursor,
  type MaterializationEvent,
} from '@treecrdt/interface/engine';
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
  expect(names).toContain('local ops: materialization changes include writeId');
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
    expect(await client.tree.get(node)).toBeUndefined();
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
    expect(await client.tree.get(node)).toBeDefined();
    expect(await client.ops.all()).toHaveLength(1);
  } finally {
    unsubscribe();
    await client.close();
  }
});

test('sqlite auth-aware local writes are authorized against the tree before the write', async () => {
  const docId = 'sqlite-auth-local-scope';
  const client = await createNodeEngine({ docId });
  const issuerSk = randomEd25519SecretKey();
  const localSk = randomEd25519SecretKey();
  const [issuerPk, localPk] = await Promise.all([
    getEd25519PublicKey(issuerSk),
    getEd25519PublicKey(localSk),
  ]);
  const scope = nodeIdFromInt(20);
  const outside = nodeIdFromInt(21);
  const node = nodeIdFromInt(22);
  const child = nodeIdFromInt(23);

  try {
    await client.local.insert(localPk, root, scope, { type: 'last' }, null);
    await client.local.insert(localPk, root, outside, { type: 'last' }, null);
    await client.local.insert(localPk, outside, node, { type: 'last' }, null);
    const session = client.auth.createSession({
      trust: { issuerPublicKeys: [issuerPk] },
      local: {
        privateKey: localSk,
        publicKey: localPk,
        capabilityTokens: [
          issueTreecrdtCapabilityTokenV1({
            issuerPrivateKey: issuerSk,
            subjectPublicKey: localPk,
            docId,
            actions: ['write_structure', 'write_payload', 'delete', 'tombstone'],
            rootNodeId: scope,
          }),
        ],
      },
    });
    await session.ready;
    const authorizeLocalOps = vi.spyOn(session, 'authorizeLocalOps');

    // The source of a move is checked where the node is before the move, as peers check it.
    await expect(
      client.local.move(localPk, node, scope, { type: 'last' }, { authSession: session }),
    ).rejects.toThrow('capability does not allow op');
    expect((await (await client.tree.get(node))?.parent())?.id).toBe(outside);

    // Op auth the session stores through this connection does not invalidate the minted op.
    await client.local.insert(localPk, scope, child, { type: 'last' }, null, {
      authSession: session,
    });
    expect(authorizeLocalOps).toHaveBeenCalledTimes(2);
    expect((await (await client.tree.get(child))?.parent())?.id).toBe(scope);
  } finally {
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

/** `onMaterializedDurable` over `client` with positions kept in memory, for the test to inspect. */
function durableOver(
  client: Awaited<ReturnType<typeof createNodeEngine>>,
  onMaterialized = client.onMaterialized,
) {
  const positions = new Map<string, MaterializationCursor>();
  const { onMaterializedDurable } = createDurableMaterialization({
    changedSince: client.tree.changedSince,
    onMaterialized,
    cursors: {
      load: async (name) => positions.get(name) ?? null,
      save: async (name, cursor) => void positions.set(name, cursor),
    },
  });
  return { onMaterializedDurable, positions };
}

test('a durable subscription reads again when an event arrives after a later one', async () => {
  const client = await createNodeEngine({ docId: 'durable-out-of-order' });
  await client.local.insert(replica, root, nodeIdFromInt(1), { type: 'last' }, null);
  // The subscription listens to a dispatcher the test feeds, so the test chooses the order.
  const dispatcher = createMaterializationDispatcher();
  const delivered: string[][] = [];
  const subscription = await durableOver(client, dispatcher.onMaterialized).onMaterializedDurable(
    'index',
    (event) => void delivered.push(event.changes.map((change) => `${change.kind}:${change.node}`)),
  );
  delivered.length = 0;
  const emitted: MaterializationEvent[] = [];
  client.onMaterialized((event) => emitted.push(event));
  const a = nodeIdFromInt(2);
  const b = nodeIdFromInt(3);
  await client.local.insert(replica, root, a, { type: 'last' }, null);
  await client.local.insert(replica, root, b, { type: 'last' }, null);

  // The second insert's event overtakes the first, then arrives once more.
  dispatcher.emitEvent(emitted[1]!);
  dispatcher.emitEvent(emitted[0]!);
  dispatcher.emitEvent(emitted[1]!);
  await subscription.settled();

  expect(delivered).toEqual([[`insert:${b}`], [`insert:${a}`, `insert:${b}`]]);
  await client.close();
});

test('a durable subscription starts over when its position is ahead of the database', async () => {
  const client = await createNodeEngine({ docId: 'durable-position-ahead' });
  const node = nodeIdFromInt(1);
  await client.local.insert(replica, root, node, { type: 'last' }, null);
  const { onMaterializedDurable, positions } = durableOver(client);
  positions.set('index', { version: 0, headSeq: 999 });
  const reset = vi.fn();
  const delivered: string[] = [];

  await onMaterializedDurable(
    'index',
    (event) => void delivered.push(...event.changes.map((change) => change.node)),
    { reset },
  );

  expect(reset).toHaveBeenCalledTimes(1);
  expect(delivered).toContain(node);
  await client.close();
});

test('a durable subscription saves its position no more often than saveIntervalMs', async () => {
  const client = await createNodeEngine({ docId: 'durable-save-interval' });
  const { onMaterializedDurable, positions } = durableOver(client);
  const eager = await onMaterializedDurable('eager', () => undefined, { saveIntervalMs: 0 });
  const lazy = await onMaterializedDurable('lazy', () => undefined);
  const opened = positions.get('lazy')!.headSeq;

  await client.local.insert(replica, root, nodeIdFromInt(1), { type: 'last' }, null);
  await Promise.all([eager.settled(), lazy.settled()]);

  expect(positions.get('eager')!.headSeq).toBeGreaterThan(opened);
  expect(positions.get('lazy')!.headSeq).toBe(opened);
  await client.close();
});

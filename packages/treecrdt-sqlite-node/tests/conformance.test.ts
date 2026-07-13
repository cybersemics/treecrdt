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
  getEd25519PublicKey,
  issueTreecrdtCapabilityTokenV1,
  randomEd25519SecretKey,
} from '@treecrdt/auth';
import type { MaterializationEvent } from '@treecrdt/interface/engine';
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

function validAuthProof() {
  return { sig: new Uint8Array(64), proofRef: new Uint8Array(16) };
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
    expect(await client.tree.exists(node)).toBe(false);
    expect(await client.ops.all()).toHaveLength(0);
  } finally {
    unsubscribe();
    await client.close();
  }
});

test('sqlite materialization exposes the committed signer only for authenticated writes', async () => {
  const docId = 'sqlite-auth-local-signer';
  const client = await createNodeEngine({ docId });
  const events: MaterializationEvent[] = [];
  const unsubscribe = client.onMaterialized((event) => events.push(event));
  const issuerPrivateKey = randomEd25519SecretKey();
  const issuerPublicKey = await getEd25519PublicKey(issuerPrivateKey);
  const signerPrivateKey = randomEd25519SecretKey();
  const signerPublicKey = await getEd25519PublicKey(signerPrivateKey);
  const authSession = client.auth.createSession({
    trust: { issuerPublicKeys: [issuerPublicKey] },
    local: {
      privateKey: signerPrivateKey,
      publicKey: signerPublicKey,
      capabilityTokens: [
        issueTreecrdtCapabilityTokenV1({
          issuerPrivateKey,
          subjectPublicKey: signerPublicKey,
          docId,
          actions: ['write_structure'],
        }),
      ],
    },
  });
  await authSession.ready;
  const unauthenticatedNode = nodeIdFromInt(11);
  const authenticatedNode = nodeIdFromInt(12);

  try {
    await client.local.insert(signerPublicKey, root, unauthenticatedNode, { type: 'last' }, null);
    expect(events).toHaveLength(1);
    expect(events[0]!.changes[0]!.source?.signer).toBeUndefined();
    events.length = 0;

    const op = await client.local.insert(
      signerPublicKey,
      root,
      authenticatedNode,
      { type: 'last' },
      null,
      {
        authSession,
      },
    );

    expect(op.kind.type).toBe('insert');
    expect(events).toHaveLength(1);
    expect(events[0]!.changes[0]!.source?.operation?.id).toEqual(op.meta.id);
    expect(events[0]!.changes[0]!.source?.signer?.publicKey).toEqual(op.meta.id.replica);
    expect(await client.tree.exists(authenticatedNode)).toBe(true);
    expect(await client.ops.all()).toHaveLength(2);
  } finally {
    unsubscribe();
    await client.close();
  }
});

test('sqlite auth wait cannot roll back an unrelated write on the same connection', async () => {
  const client = await createNodeEngine({ docId: 'sqlite-auth-local-isolation' });
  let authStarted!: () => void;
  const started = new Promise<void>((resolve) => {
    authStarted = resolve;
  });
  let rejectAuth!: () => void;
  const release = new Promise<void>((resolve) => {
    rejectAuth = resolve;
  });
  const authSession = {
    authorizeLocalOps: vi.fn(async () => {
      authStarted();
      await release;
      throw new Error('local auth denied');
    }),
  };
  const deniedNode = nodeIdFromInt(20);
  const unrelatedNode = nodeIdFromInt(21);

  try {
    const denied = client.local.insert(replica, root, deniedNode, { type: 'last' }, null, {
      authSession,
    });
    await started;

    const unrelated = await client.local.insert(
      replica,
      root,
      unrelatedNode,
      { type: 'last' },
      null,
    );
    rejectAuth();

    await expect(denied).rejects.toThrow('local auth denied');
    expect(unrelated.meta.id.counter).toBe(1);
    expect(await client.tree.exists(deniedNode)).toBe(false);
    expect(await client.tree.exists(unrelatedNode)).toBe(true);
    expect(await client.ops.all()).toHaveLength(1);
  } finally {
    await client.close();
  }
});

test('sqlite rejects authenticated prepare and commit inside caller transactions', async () => {
  const client = await createNodeEngine({ docId: 'sqlite-auth-outer-transaction' });
  const nodeAtPrepare = nodeIdFromInt(22);
  const nodeAtCommit = nodeIdFromInt(23);
  const prepareAuth = {
    authorizeLocalOps: vi.fn(async () => [validAuthProof()]),
  };

  try {
    await client.runner.exec('BEGIN');
    await expect(
      client.local.insert(replica, root, nodeAtPrepare, { type: 'last' }, null, {
        authSession: prepareAuth,
      }),
    ).rejects.toThrow(/require autocommit/i);
    expect(prepareAuth.authorizeLocalOps).not.toHaveBeenCalled();
    await client.runner.exec('ROLLBACK');

    const commitAuth = {
      authorizeLocalOps: vi.fn(async () => {
        await client.runner.exec('BEGIN');
        return [validAuthProof()];
      }),
    };
    await expect(
      client.local.insert(replica, root, nodeAtCommit, { type: 'last' }, null, {
        authSession: commitAuth,
      }),
    ).rejects.toThrow(/require autocommit/i);
    expect(commitAuth.authorizeLocalOps).toHaveBeenCalledTimes(1);
    await client.runner.exec('ROLLBACK');

    expect(await client.ops.all()).toHaveLength(0);
    expect(await client.tree.exists(nodeAtPrepare)).toBe(false);
    expect(await client.tree.exists(nodeAtCommit)).toBe(false);
  } finally {
    try {
      await client.runner.exec('ROLLBACK');
    } catch {
      // no transaction remained open
    }
    await client.close();
  }
});

test('sqlite reauthorizes against fresh tree state after an optimistic conflict', async () => {
  const client = await createNodeEngine({ docId: 'sqlite-auth-local-stale-tree' });
  const events: MaterializationEvent[] = [];
  const unsubscribe = client.onMaterialized((event) => events.push(event));
  const otherReplica = Uint8Array.from(replica, (value, index) =>
    index === replica.length - 1 ? value + 1 : value,
  );
  const sourceParent = nodeIdFromInt(30);
  const concurrentParent = nodeIdFromInt(31);
  const destination = nodeIdFromInt(32);
  const node = nodeIdFromInt(33);

  try {
    for (const parent of [sourceParent, concurrentParent, destination]) {
      await client.local.insert(replica, root, parent, { type: 'last' }, null);
    }
    await client.local.insert(replica, sourceParent, node, { type: 'last' }, null);

    let firstAuthStarted!: () => void;
    const firstStarted = new Promise<void>((resolve) => {
      firstAuthStarted = resolve;
    });
    let releaseFirstAuth!: () => void;
    const release = new Promise<void>((resolve) => {
      releaseFirstAuth = resolve;
    });
    const proposals: Array<{ op: any; parent: string | null }> = [];
    const authSession = {
      authorizeLocalOps: vi.fn(async (ops: readonly any[]) => {
        proposals.push({ op: ops[0], parent: await client.tree.parent(node) });
        if (proposals.length === 1) {
          firstAuthStarted();
          await release;
        }
        return [validAuthProof()];
      }),
    };

    const authorizedMove = client.local.move(
      replica,
      node,
      destination,
      { type: 'last' },
      { authSession },
    );
    await firstStarted;
    await client.local.move(otherReplica, node, concurrentParent, { type: 'last' });
    releaseFirstAuth();
    const committed = await authorizedMove;

    expect(authSession.authorizeLocalOps).toHaveBeenCalledTimes(2);
    expect(proposals.map((proposal) => proposal.parent)).toEqual([sourceParent, concurrentParent]);
    // The conflicting remote-replica write does not consume this replica's counter, so the retry
    // deliberately reuses the id only after authorizing its new full body.
    expect(proposals[0].op.meta.id.counter).toBe(proposals[1].op.meta.id.counter);
    expect(proposals[0].op.meta.lamport).not.toBe(proposals[1].op.meta.lamport);
    expect(committed).toEqual(proposals[1].op);
    expect(await client.tree.parent(node)).toBe(destination);
    expect(await client.ops.all()).toHaveLength(6);
    const signedChanges = events
      .flatMap((event) => event.changes)
      .filter((change) => change.source?.signer);
    expect(signedChanges).toHaveLength(1);
    expect(signedChanges[0]!.source?.operation?.id).toEqual(committed.meta.id);
    expect(signedChanges[0]!.source?.signer?.publicKey).toEqual(committed.meta.id.replica);
  } finally {
    unsubscribe();
    await client.close();
  }
});

test('sqlite stores local proof material atomically with the operation', async () => {
  const client = await createNodeEngine({ docId: 'sqlite-auth-atomic-proof' });
  const events: unknown[] = [];
  const unsubscribe = client.onMaterialized((event) => events.push(event));
  const sig = new Uint8Array(64).fill(7);
  const proofRef = new Uint8Array(16).fill(8);
  const authSession = {
    authorizeLocalOps: vi.fn(async () => [{ sig, proofRef }]),
  };
  const node = nodeIdFromInt(40);

  try {
    await client.local.insert(replica, root, node, { type: 'last' }, null, { authSession });

    expect(await client.tree.exists(node)).toBe(true);
    expect(await client.ops.all()).toHaveLength(1);
    expect(events).toHaveLength(1);
    expect(
      await client.runner.getText('SELECT hex(sig) FROM treecrdt_sync_op_auth WHERE doc_id = ?1', [
        'sqlite-auth-atomic-proof',
      ]),
    ).toBe(Buffer.from(sig).toString('hex').toUpperCase());
    expect(authSession.authorizeLocalOps).toHaveBeenCalledTimes(1);
  } finally {
    unsubscribe();
    await client.close();
  }
});

test('sqlite rejects malformed local proof material before committing', async () => {
  const client = await createNodeEngine({ docId: 'sqlite-auth-invalid-proof' });
  const invalidProofs: any[] = [
    undefined,
    null,
    { sig: new Uint8Array(63) },
    { sig: new Uint8Array(64) },
    { sig: new Uint8Array(64), proofRef: new Uint8Array(15) },
  ];

  try {
    for (let index = 0; index < invalidProofs.length; index += 1) {
      const authSession = {
        authorizeLocalOps: vi.fn(async () => [invalidProofs[index]!]),
      };
      await expect(
        client.local.insert(replica, root, nodeIdFromInt(41 + index), { type: 'last' }, null, {
          authSession,
        }),
      ).rejects.toThrow(/invalid proof/i);
    }
    expect(await client.ops.all()).toHaveLength(0);
  } finally {
    await client.close();
  }
});

test('sqlite rejects authorization operation mutation before commit', async () => {
  const docId = 'sqlite-auth-operation-mutation';
  const client = await createNodeEngine({ docId });
  const node = nodeIdFromInt(50);
  const authSession = {
    authorizeLocalOps: vi.fn(async (ops: readonly any[]) => {
      ops[0].meta.id.counter += 1;
      return [validAuthProof()];
    }),
  };

  try {
    await expect(
      client.local.insert(replica, root, node, { type: 'last' }, null, { authSession }),
    ).rejects.toThrow(/mutated the proposed operation/);

    expect(await client.tree.exists(node)).toBe(false);
    expect(await client.ops.all()).toHaveLength(0);
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

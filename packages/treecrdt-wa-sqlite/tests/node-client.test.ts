import { expect, test, vi } from 'vitest';
import vm from 'node:vm';
import type { Operation } from '@treecrdt/interface';
import {
  runTreecrdtEngineConformanceScenario,
  treecrdtEngineConformanceScenarios,
} from '@treecrdt/engine-conformance';
import { createTreecrdtClient } from '../dist/index.node.js';

const root = '0'.repeat(32);
const replica = Uint8Array.from({ length: 32 }, (_, i) => (i === 31 ? 1 : 0));

function nodeIdFromInt(n: number): string {
  return n.toString(16).padStart(32, '0');
}

async function createWaEngine(opts: { docId: string }) {
  return await createTreecrdtClient({ docId: opts.docId });
}

type Deferred<T> = {
  promise: Promise<T>;
  resolve: (value: T) => void;
  reject: (err: unknown) => void;
};

function deferred<T>(): Deferred<T> {
  let resolve!: (value: T) => void;
  let reject!: (err: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

/** Local auth session whose authorizations wait until the test allows or denies them. */
function heldAuthSession() {
  const started = deferred<void>();
  const decision = deferred<void>();
  const authorized: Operation[] = [];
  return {
    authorized,
    started: started.promise,
    allow: () => decision.resolve(),
    deny: (err: Error) => decision.reject(err),
    authSession: {
      authorizeLocalOps: async (ops: readonly Operation[]) => {
        authorized.push(...ops);
        started.resolve();
        await decision.promise;
      },
    },
  };
}

test('createTreecrdtClient smoke: insert and read in Node', async () => {
  const client = await createWaEngine({ docId: 'wa-sqlite-node-smoke' });
  const node = nodeIdFromInt(1);

  try {
    await client.local.insert(replica, root, node, { type: 'last' }, null);
    expect(await client.tree.get(node)).toBeDefined();
    expect(await client.ops.all()).toHaveLength(1);
  } finally {
    await client.close();
  }
});

test('createTreecrdtClient accepts cross-realm typed array payloads in Node', async () => {
  const client = await createWaEngine({ docId: 'wa-sqlite-node-cross-realm-payload' });
  const payload = vm.runInNewContext('new Uint8Array([1, 2, 3])') as Uint8Array;

  try {
    await client.local.payload(replica, root, payload);
    expect(await client.tree.root.payload()).toEqual(Uint8Array.from([1, 2, 3]));
  } finally {
    await client.close();
  }
});

test('createTreecrdtClient rejects persistent storage on Node', async () => {
  await expect(
    createTreecrdtClient({ docId: 'wa-sqlite-node-persistent', persistent: true }),
  ).rejects.toThrow(/persistent storage is not supported on Node/);
});

test('createTreecrdtClient rejects cross-tab mode on Node', async () => {
  await expect(
    createTreecrdtClient({ docId: 'wa-sqlite-node-cross-tab', crossTab: true }),
  ).rejects.toThrow(/requires SharedWorker support/);
});

test('createTreecrdtClient rejects an empty docId', async () => {
  await expect(createTreecrdtClient({ docId: '' })).rejects.toThrow(/non-empty docId/);
});

test('auth-checked local write stays unwritten while authorization is pending', async () => {
  const client = await createWaEngine({ docId: 'wa-sqlite-node-auth-pending' });
  const auth = heldAuthSession();
  const node = nodeIdFromInt(1);

  try {
    const write = client.local.insert(replica, root, node, { type: 'last' }, null, {
      authSession: auth.authSession,
    });
    await Promise.race([auth.started, write]);

    expect(await client.tree.get(node)).toBeUndefined();
    expect(await client.ops.all()).toHaveLength(0);

    auth.allow();
    expect(await write).toEqual(auth.authorized[0]);
    expect(await client.tree.get(node)).toBeDefined();
  } finally {
    await client.close();
  }
});

test('denied auth-checked local write keeps writes made during authorization', async () => {
  const client = await createWaEngine({ docId: 'wa-sqlite-node-auth-denied' });
  const auth = heldAuthSession();
  const deniedNode = nodeIdFromInt(1);
  const concurrentNode = nodeIdFromInt(2);

  try {
    const denied = client.local.insert(replica, root, deniedNode, { type: 'last' }, null, {
      authSession: auth.authSession,
    });
    await Promise.race([auth.started, denied]);
    await client.local.insert(replica, root, concurrentNode, { type: 'last' }, null);
    auth.deny(new Error('local auth denied'));

    await expect(denied).rejects.toThrow('local auth denied');
    expect(await client.tree.get(concurrentNode)).toBeDefined();
    expect(await client.tree.get(deniedNode)).toBeUndefined();
    expect(await client.ops.all()).toHaveLength(1);
  } finally {
    await client.close();
  }
});

test('concurrent auth-checked local writes take turns and both commit', async () => {
  const client = await createWaEngine({ docId: 'wa-sqlite-node-auth-concurrent' });
  const first = heldAuthSession();
  const second = heldAuthSession();
  const firstNode = nodeIdFromInt(1);
  const secondNode = nodeIdFromInt(2);

  try {
    const firstWrite = client.local.insert(replica, root, firstNode, { type: 'last' }, null, {
      authSession: first.authSession,
    });
    await Promise.race([first.started, firstWrite]);
    const secondWrite = client.local.insert(replica, root, secondNode, { type: 'last' }, null, {
      authSession: second.authSession,
    });
    // Given a turn of the event loop, the second write still waits for the first.
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(second.authorized).toHaveLength(0);

    first.allow();
    expect(await firstWrite).toEqual(first.authorized[0]);
    await Promise.race([second.started, secondWrite]);
    second.allow();
    expect(await secondWrite).toEqual(second.authorized[0]);
    expect(second.authorized).toHaveLength(1);
    expect((await client.ops.all()).map((op) => op.meta.id.counter).sort()).toEqual([1, 2]);
  } finally {
    await client.close();
  }
});

test('auth-checked local write gives up when every authorization is overtaken', async () => {
  const client = await createWaEngine({ docId: 'wa-sqlite-node-auth-overtaken' });
  const node = nodeIdFromInt(1);
  let concurrentNode = 100;
  const authorizeLocalOps = vi.fn(async () => {
    await client.local.insert(
      replica,
      root,
      nodeIdFromInt(++concurrentNode),
      { type: 'last' },
      null,
    );
  });

  try {
    await expect(
      client.local.insert(replica, root, node, { type: 'last' }, null, {
        authSession: { authorizeLocalOps },
      }),
    ).rejects.toThrow('not written after 4 authorizations');
    expect(authorizeLocalOps).toHaveBeenCalledTimes(4);
    expect(await client.tree.get(node)).toBeUndefined();
    expect(await client.ops.all()).toHaveLength(4);
  } finally {
    await client.close();
  }
});

test('auth-checked local write keeps the payload it was called with', async () => {
  const client = await createWaEngine({ docId: 'wa-sqlite-node-auth-payload' });
  const auth = heldAuthSession();
  const node = nodeIdFromInt(1);
  const payload = Uint8Array.from([1, 2, 3]);

  try {
    const write = client.local.insert(replica, root, node, { type: 'last' }, payload, {
      authSession: auth.authSession,
    });
    await Promise.race([auth.started, write]);
    payload.fill(9);
    auth.allow();
    await write;

    expect(auth.authorized).toHaveLength(1);
    expect(await (await client.tree.get(node))?.payload()).toEqual(Uint8Array.from([1, 2, 3]));
  } finally {
    await client.close();
  }
});

for (const scenario of treecrdtEngineConformanceScenarios()) {
  test(`wa-sqlite engine conformance (node, memory): ${scenario.name}`, async () => {
    await runTreecrdtEngineConformanceScenario(scenario, {
      docIdPrefix: 'treecrdt-wa-node-conformance',
      openEngine: ({ docId }) => createWaEngine({ docId }),
    });
  });
}

import { afterEach, expect, test, vi } from 'vitest';
import { createMemoryClient, type MemoryClient } from '../dist/index.node.js';
import { createWasmAdapter } from '../dist/adapter.js';
import { decodeSqliteOps } from '@treecrdt/interface/sqlite';
import { WasmTree } from '../pkg-web/treecrdt_wasm.js';
import type { Operation, OperationId } from '@treecrdt/interface';

const root = '0'.repeat(32);
const a = '1'.repeat(32);
const b = '2'.repeat(32);
const c = '3'.repeat(32);
const d = '4'.repeat(32);
const payload = new Uint8Array([0, 127, 255]);
const clients: MemoryClient[] = [];
async function open() {
  const client = await createMemoryClient();
  clients.push(client);
  return client;
}
afterEach(() => {
  for (const client of clients.splice(0)) client.close();
});

const operationIds = (operations: readonly Operation[]): OperationId[] =>
  operations.map((operation) => operation.meta.id);

test('initializes explicitly and returns synchronous typed local operations and reads', async () => {
  const client = await open();
  const operation = client.local.insert(root, a, null, payload);
  expect(operation).not.toBeInstanceOf(Promise);
  expect(operation.kind).toMatchObject({ type: 'insert', parent: root, node: a, payload });
  expect(operation.meta.id.replica).toBeInstanceOf(Uint8Array);
  expect(client.tree.children(root)).toEqual([a]);
  expect(client.tree.get(a)?.payload).toEqual(payload);
  expect(client.operationsFrom(0)).toEqual([operation]);
  expect(client.operationsAt([0, 0])).toEqual([operation, operation]);
  expect(() => client.operationsAt([1])).toThrow();
});

test('composes read-your-writes commands and publishes one completed snapshot', async () => {
  const client = await open();
  const initial = client.getSnapshot();
  const listener = vi.fn(() => expect(client.tree.children(a)).toEqual([b]));
  client.subscribe(listener);
  const result = client.transact((document) => {
    document.local.insert(root, a, null, payload);
    expect(document.getSnapshot().get(a)?.payload).toEqual(payload);
    expect(listener).not.toHaveBeenCalled();
    document.local.insert(a, b, null, payload);
    expect(document.getSnapshot().get(a)?.children).toEqual([b]);
    expect(listener).not.toHaveBeenCalled();
    return 42;
  });
  expect(result.value).toBe(42);
  expect(result.operations).toHaveLength(2);
  expect(listener).toHaveBeenCalledTimes(1);
  expect(initial.has(a)).toBe(false);
  expect(client.getSnapshot()).toBe(client.getSnapshot());
});

test('restores snapshot identity, native log and clock after a transaction throws following intermediate reads', async () => {
  const client = await open();
  client.local.insert(root, a, null, payload);
  const initial = client.getSnapshot();
  const count = client.operationCount();
  const lamport = client.maxLamport();
  const listener = vi.fn();
  client.subscribe(listener);
  const failure = new Error('rejected');
  expect(() =>
    client.transact((document) => {
      document.local.payload(a, new Uint8Array([3]));
      expect(document.getSnapshot()).not.toBe(initial);
      document.local.insert(a, b, null, payload);
      expect(document.getSnapshot().has(b)).toBe(true);
      throw failure;
    }),
  ).toThrow(failure);
  expect(client.getSnapshot()).toBe(initial);
  expect(client.operationCount()).toBe(count);
  expect(client.maxLamport()).toBe(lamport);
  expect(client.tree.exists(b)).toBe(false);
  expect(listener).not.toHaveBeenCalled();
  expect(client.local.payload(a, new Uint8Array([4])).meta.id.counter).toBe(count + 1);
});

test('preserves map and unchanged row identity and keeps net no-op transactions quiet', async () => {
  const client = await open();
  client.transact((document) => {
    document.local.insert(root, a, null, payload);
    document.local.insert(root, b, a, payload);
  });
  const initial = client.getSnapshot();
  const listener = vi.fn();
  client.subscribe(listener);
  const invisible = client.transact((document) => document.local.payload(a, payload));
  expect(invisible.operations).toEqual([invisible.value]);
  expect(client.getSnapshot()).toBe(initial);
  client.transact((document) => {
    document.local.payload(a, new Uint8Array([3]));
    expect(document.getSnapshot()).not.toBe(initial);
    document.local.payload(a, payload);
  });
  expect(client.getSnapshot()).toBe(initial);
  expect(listener).not.toHaveBeenCalled();
  client.local.payload(a, new Uint8Array([4]));
  expect(client.getSnapshot()).not.toBe(initial);
  expect(client.getSnapshot().get(b)).toBe(initial.get(b));
  expect(listener).toHaveBeenCalledTimes(1);
});

test('does not expose mutable snapshot maps, rows, children or payload storage', async () => {
  const client = await open();
  client.local.insert(root, a, null, payload);
  const snapshot = client.getSnapshot();
  expect('set' in snapshot).toBe(false);
  expect('delete' in snapshot).toBe(false);
  expect(Object.isFrozen(snapshot.get(a))).toBe(true);
  expect(Object.isFrozen(snapshot.get(a)!.children)).toBe(true);
  snapshot.get(a)!.payload![0] = 9;
  expect(snapshot.get(a)!.payload).toEqual(payload);
  snapshot.forEach((_row, _id, view) => expect(view).toBe(snapshot));
});

test('publishes payload-less nodes and clearing payloads as visible snapshot changes', async () => {
  const client = await open();
  expect(client.getSnapshot().get(root)?.payload).toBeNull();
  const listener = vi.fn();
  client.subscribe(listener);
  client.local.insert(root, a);
  const inserted = client.getSnapshot();
  expect(inserted.get(a)).toMatchObject({ id: a, parentId: root, payload: null });
  expect(inserted.get(root)?.children).toEqual([a]);
  expect(listener).toHaveBeenCalledTimes(1);
  client.local.payload(a, payload);
  expect(client.getSnapshot().get(a)?.payload).toEqual(payload);
  client.local.payload(a, null);
  expect(client.getSnapshot().get(a)?.payload).toBeNull();
  expect(listener).toHaveBeenCalledTimes(3);
});

test('cannot commit earlier writes when a caught mutator error poisons the native transaction', async () => {
  const client = await open();
  const initial = client.getSnapshot();
  const listener = vi.fn();
  client.subscribe(listener);
  expect(() =>
    client.transact((document) => {
      document.local.insert(root, a, null, payload);
      expect(() => document.local.move(a, 'not-a-node')).toThrow();
    }),
  ).toThrow();
  expect(client.getSnapshot()).toBe(initial);
  expect(client.operationCount()).toBe(0);
  expect(listener).not.toHaveBeenCalled();
});

test('rejects nested and asynchronous transactions without retaining earlier writes', async () => {
  const client = await open();
  const initial = client.getSnapshot();
  expect(() =>
    client.transact((document) => {
      document.local.insert(root, a, null, payload);
      document.transact(() => {});
    }),
  ).toThrow(/Nested/);
  expect(() =>
    client.transact((document) => {
      document.local.insert(root, a, null, payload);
      return Promise.resolve();
    }),
  ).toThrow(/synchronous/);
  expect(client.getSnapshot()).toBe(initial);
  expect(client.operationCount()).toBe(0);
});

test('isolates subscriber errors and supports unsubscribing and idempotent close', async () => {
  const client = await open();
  const error = new Error('subscriber failed');
  const report = vi.spyOn(console, 'error').mockImplementation(() => {});
  const listener = vi.fn();
  client.subscribe(() => {
    throw error;
  });
  const unsubscribe = client.subscribe(listener);
  expect(() => client.local.insert(root, a, null, payload)).not.toThrow();
  expect(report).toHaveBeenCalledWith('TreeCRDT subscriber failed', error);
  expect(listener).toHaveBeenCalledTimes(1);
  unsubscribe();
  client.local.payload(a, new Uint8Array([2]));
  expect(listener).toHaveBeenCalledTimes(1);
  client.close();
  client.close();
  expect(() => client.getSnapshot()).toThrow(/closed/);
});

test('keeps the legacy benchmark adapter backed by typed operations including insert payloads', async () => {
  const source = await open();
  const operation = source.local.insert(root, a, null, payload);
  const adapter = await createWasmAdapter();
  try {
    await adapter.appendOps!(
      [operation],
      () => new Uint8Array(),
      (replica) => replica,
    );
    expect(decodeSqliteOps(await adapter.opsSince(0))).toEqual([operation]);
    expect(await adapter.treePayload(new Uint8Array(16).fill(0x11))).toEqual(payload);
  } finally {
    await adapter.close?.();
  }
});

test('rolls back after native ingestion mutated state and a JS boundary throws', async () => {
  const source = await open();
  const client = await open();
  client.local.insert(root, a, null, payload);
  const incoming = source.local.insert(root, b, null, payload);
  const snapshot = client.getSnapshot();
  const count = client.operationCount();
  const append = WasmTree.prototype.appendOperations;
  const failure = new Error('failure after ingestion');
  const listener = vi.fn();
  client.subscribe(listener);
  vi.spyOn(WasmTree.prototype, 'appendOperations').mockImplementationOnce(function (
    this: WasmTree,
    operations,
  ) {
    append.call(this, operations);
    throw failure;
  });
  expect(() => client.appendOperations([incoming])).toThrow(failure);
  expect(client.getSnapshot()).toBe(snapshot);
  expect(client.operationCount()).toBe(count);
  expect(client.tree.exists(b)).toBe(false);
  expect(listener).not.toHaveBeenCalled();
  expect(client.local.payload(a, new Uint8Array([4])).meta.id.counter).toBe(count + 1);
});

test('preserves received history and the next local identity when a later placement is invalid', async () => {
  const source = await open();
  const client = await open();
  const original = source.local.insert(root, a, null, payload);
  client.appendOperations([original]);
  const snapshot = client.getSnapshot();
  expect(() =>
    client.transact((document) => {
      document.local.payload(a, new Uint8Array([9]));
      document.local.insert(root, b, '3'.repeat(32), payload);
    }),
  ).toThrow(/after node/);
  expect(client.getSnapshot()).toBe(snapshot);
  expect(client.operationsFrom(0)).toEqual([original]);
  const next = client.local.payload(a, new Uint8Array([4]));
  expect(next.meta.id.counter).toBe(1);
  expect(next.meta.lamport).toBe(original.meta.lamport + 1);
});

test('reverts mixed edits and their receipts using only fresh operations, retaining the full log', async () => {
  const client = await open();
  client.transact((document) => {
    document.local.insert(root, a, null, payload);
    document.local.insert(root, b, a, payload);
    document.local.insert(root, d, b, payload);
  });
  const before = [...client.getSnapshot()];
  const edit = client.transact((document) => {
    document.local.insert(b, c, null, payload);
    document.local.move(a, b, c);
    document.local.payload(b, new Uint8Array([9]));
    document.local.delete(d);
  }).operations;
  const edited = [...client.getSnapshot()];
  const retained = client.operationsFrom(0);
  const listener = vi.fn();
  client.subscribe(listener);

  const undo = client.revert(operationIds(edit));
  expect(undo).not.toBeInstanceOf(Promise);
  expect(undo.length).toBeGreaterThan(0);
  expect([...client.getSnapshot()]).toEqual(before);
  expect(listener).toHaveBeenCalledTimes(1);
  expect(client.operationsFrom(0)).toEqual([...retained, ...undo]);

  const redo = client.revert(operationIds(undo));
  expect(redo.length).toBeGreaterThan(0);
  expect([...client.getSnapshot()]).toEqual(edited);
  expect(listener).toHaveBeenCalledTimes(2);
  const history = client.operationsFrom(0);
  expect(history).toEqual([...retained, ...undo, ...redo]);
  expect(history.map((operation) => operation.meta.id.counter)).toEqual(
    history.map((_, index) => index + 1),
  );
  expect(history.map((operation) => operation.meta.lamport)).toEqual(
    history.map((_, index) => index + 1),
  );
  for (const operation of [...undo, ...redo]) {
    expect(operation.meta.id.replica).toBeInstanceOf(Uint8Array);
  }

  const snapshot = client.getSnapshot();
  expect(client.revert([])).toEqual([]);
  expect(client.getSnapshot()).toBe(snapshot);
  expect(client.operationsFrom(0)).toEqual(history);
  expect(listener).toHaveBeenCalledTimes(2);
});

test('composes revert with normal writes in one synchronous transaction and publication', async () => {
  const client = await open();
  client.local.insert(root, a, null, payload);
  const edited = client.local.payload(a, new Uint8Array([9]));
  const listener = vi.fn();
  client.subscribe(listener);
  const result = client.transact((document) => {
    const reverted = document.revert([edited.meta.id]);
    expect(document.getSnapshot().get(a)?.payload).toEqual(payload);
    expect(listener).not.toHaveBeenCalled();
    const inserted = document.local.insert(a, b, null, payload);
    expect(document.getSnapshot().get(a)?.children).toEqual([b]);
    expect(listener).not.toHaveBeenCalled();
    return [...reverted, inserted];
  });
  expect(result.operations).toEqual(result.value);
  expect(client.tree.payload(a)).toEqual(payload);
  expect(client.tree.children(a)).toEqual([b]);
  expect(listener).toHaveBeenCalledTimes(1);
});

test('rolls back a revert and intermediate reads when its transaction later throws', async () => {
  const client = await open();
  client.local.insert(root, a, null, payload);
  const edited = client.local.payload(a, new Uint8Array([9]));
  const snapshot = client.getSnapshot();
  const history = client.operationsFrom(0);
  const lamport = client.maxLamport();
  const listener = vi.fn();
  client.subscribe(listener);
  const failure = new Error('failure after revert');
  expect(() =>
    client.transact((document) => {
      document.revert([edited.meta.id]);
      expect(document.getSnapshot().get(a)?.payload).toEqual(payload);
      document.local.insert(a, b, null, payload);
      expect(document.getSnapshot().has(b)).toBe(true);
      throw failure;
    }),
  ).toThrow(failure);
  expect(client.getSnapshot()).toBe(snapshot);
  expect(client.operationsFrom(0)).toEqual(history);
  expect(client.maxLamport()).toBe(lamport);
  expect(listener).not.toHaveBeenCalled();
  const next = client.local.payload(a, new Uint8Array([4]));
  expect(next.meta.id.counter).toBe(edited.meta.id.counter + 1);
  expect(next.meta.lamport).toBe(lamport + 1);
});

test('invalid revert IDs are atomic and poison a containing transaction even when caught', async () => {
  const client = await open();
  client.local.insert(root, a, null, payload);
  const edited = client.local.payload(a, new Uint8Array([9]));
  const snapshot = client.getSnapshot();
  const history = client.operationsFrom(0);
  const lamport = client.maxLamport();
  const listener = vi.fn();
  client.subscribe(listener);
  const unknown = { ...edited.meta.id, counter: 1000 };
  for (const invalid of [unknown, { replica: new Uint8Array(31), counter: 1 }]) {
    expect(() => client.revert([edited.meta.id, invalid])).toThrow();
    expect(client.getSnapshot()).toBe(snapshot);
    expect(client.operationsFrom(0)).toEqual(history);
    expect(client.maxLamport()).toBe(lamport);
  }
  expect(() =>
    client.transact((document) => {
      document.local.insert(root, b, a, payload);
      expect(() => document.revert([edited.meta.id, unknown])).toThrow();
    }),
  ).toThrow();
  expect(client.getSnapshot()).toBe(snapshot);
  expect(client.operationsFrom(0)).toEqual(history);
  expect(client.maxLamport()).toBe(lamport);
  expect(listener).not.toHaveBeenCalled();
  expect(client.local.payload(a, payload).meta.id.counter).toBe(edited.meta.id.counter + 1);
});

test.each(['deleted', 'moved'] as const)(
  'rejects a %s undo anchor without publishing or appending part of the revert',
  async (anchorState) => {
    const client = await open();
    client.transact((document) => {
      document.local.insert(root, a, null, payload);
      document.local.insert(root, b, a, payload);
      document.local.insert(root, c, b, payload);
    });
    const edit = client.transact((document) => {
      document.local.move(b, c);
      document.local.payload(c, new Uint8Array([9]));
    }).operations;
    if (anchorState === 'deleted') client.local.delete(a);
    else client.local.move(a, c);
    const snapshot = client.getSnapshot();
    const history = client.operationsFrom(0);
    const listener = vi.fn();
    client.subscribe(listener);
    expect(() => client.revert(operationIds(edit))).toThrow(/anchor/);
    expect(client.getSnapshot()).toBe(snapshot);
    expect(client.operationsFrom(0)).toEqual(history);
    expect(client.maxLamport()).toBe(history.at(-1)!.meta.lamport);
    expect(listener).not.toHaveBeenCalled();
    expect(client.local.payload(c, payload).meta.id.counter).toBe(history.length + 1);
  },
);

test('replicates forward-only undo and redo, explicitly overwriting intervening remote writes', async () => {
  const left = await open();
  const right = await open();
  const initial = left.transact((document) => {
    document.local.insert(root, a, null, payload);
    document.local.insert(root, b, a, payload);
    document.local.insert(root, c, b, payload);
  }).operations;
  right.appendOperations(initial);
  const edit = left.transact((document) => {
    document.local.move(a, b);
    document.local.payload(a, new Uint8Array([1]));
  }).operations;
  right.appendOperations(edit);
  const remote = right.transact((document) => {
    document.local.move(a, c);
    document.local.payload(a, new Uint8Array([2]));
  }).operations;
  left.appendOperations(remote);
  expect([...left.getSnapshot()]).toEqual([...right.getSnapshot()]);

  const undo = left.revert(operationIds(edit));
  expect(left.tree.parent(a)).toBe(root);
  expect(left.tree.payload(a)).toEqual(payload);
  expect(undo.every((operation) => operation.meta.lamport > remote.at(-1)!.meta.lamport)).toBe(
    true,
  );
  right.appendOperations([...undo].reverse());
  expect([...right.getSnapshot()]).toEqual([...left.getSnapshot()]);

  const redo = left.revert(operationIds(undo));
  // Reverting a compensation restores the state it replaced, including those remote writes.
  expect(left.tree.parent(a)).toBe(c);
  expect(left.tree.payload(a)).toEqual(new Uint8Array([2]));
  right.appendOperations([...redo].reverse());
  expect([...right.getSnapshot()]).toEqual([...left.getSnapshot()]);
  const count = right.operationCount();
  right.appendOperations([...initial, ...edit, ...remote, ...undo, ...redo]);
  expect(right.operationCount()).toBe(count);
  expect(left.operationCount()).toBe(count);
  expect(right.operationsFrom(0)).toEqual(expect.arrayContaining(left.operationsFrom(0)));
});

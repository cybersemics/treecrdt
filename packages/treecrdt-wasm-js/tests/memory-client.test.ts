import { afterEach, expect, test, vi } from 'vitest';
import { WasmTree } from '../pkg/treecrdt_wasm.js';
import { createInitializedMemoryClient, type MemoryClient } from '../src/memory-client.js';

const root = '0'.repeat(32);
const a = '1'.padStart(32, '0');
const b = '2'.padStart(32, '0');
const clients: MemoryClient[] = [];
function open() {
  const native = new WasmTree('01'.repeat(32));
  const client = createInitializedMemoryClient(native, 'document');
  clients.push(client);
  return { client, native };
}
afterEach(() => {
  for (const client of clients.splice(0)) client.close();
  vi.restoreAllMocks();
});

test('reads owned current rows after each synchronous command', () => {
  const { client, native } = open();
  const enumeration = vi.spyOn(native, 'nodeIds');
  client.local.insert('0', '1', { type: 'last' }, Uint8Array.of(1));
  client.local.insert(root, b, { type: 'after', after: '1' }, null);
  const retainedRow = client.get(a)!;
  const retainedContent = client.getContent(a)!;
  const retainedChildren = client.getChildren(root);
  expect(retainedContent).not.toHaveProperty('children');
  expect(retainedChildren).toEqual([a, b]);
  retainedContent.payload!.fill(98);
  retainedRow.payload!.fill(99);
  expect(client.getContent(a)?.payload).toEqual(Uint8Array.of(1));

  client.local.move('1', '2', { type: 'last' });
  client.local.payload(a, Uint8Array.of(2));
  expect(client.get(a)).toMatchObject({ parentId: b, payload: Uint8Array.of(2) });
  expect(client.getContent(a)).toMatchObject({ parentId: b, payload: Uint8Array.of(2) });
  expect(client.getChildren(root)).toEqual([b]);
  expect(client.getChildren('2')).toEqual([a]);
  expect(enumeration).not.toHaveBeenCalled();
  expect(retainedRow.parentId).toBe(root);
  expect(retainedContent.parentId).toBe(root);
  expect(retainedChildren).toEqual([a, b]);
  client.local.delete(a);
  expect(client.get(a)).toBeUndefined();
  expect(client.getContent(a)).toBeUndefined();
  expect(client.getChildren(b)).toEqual([]);
});

test.each(['missing sibling', 'invalid placement'])(
  '%s failure leaves the next local operation unchanged',
  (failure) => {
    const { client } = open();
    const { client: control } = open();
    const placement =
      failure === 'missing sibling'
        ? { type: 'after' as const, after: b }
        : ({ type: 'invalid' } as never);
    expect(() => client.local.insert(root, a, placement, null)).toThrow();
    expect(client.get(a)).toBeUndefined();
    expect(client.operationsFrom(0)).toEqual([]);
    expect(client.local.insert(root, b, { type: 'last' }, null)).toEqual(
      control.local.insert(root, b, { type: 'last' }, null),
    );
  },
);

test('operation exchange preserves arrival order, duplicate suppression and owned bytes', () => {
  const { client: source } = open();
  const { client: receiver } = open();
  const insert = source.local.insert(root, a, { type: 'last' }, Uint8Array.of(1));
  const payload = source.local.payload(a, Uint8Array.of(42));
  expect(() =>
    receiver.ops.appendMany([
      insert,
      { ...payload, meta: { ...payload.meta, id: { replica: Uint8Array.of(1), counter: 2 } } },
    ]),
  ).toThrow();
  expect(receiver.operationCount()).toBe(0);
  receiver.ops.appendMany([payload, insert, payload]);
  expect(receiver.get(a)?.payload).toEqual(Uint8Array.of(42));
  expect(receiver.getChildren(root)).toEqual([a]);
  expect(receiver.operationCount()).toBe(2);
  expect(receiver.operationsFrom(0)).toEqual([payload, insert]);
  expect(receiver.meta.headLamport()).toBe(payload.meta.lamport);
  insert.meta.id.replica.fill(99);
  expect(receiver.operationsFrom(1)[0]?.meta.id.replica).toEqual(new Uint8Array(32).fill(1));
  for (const cursor of [NaN, 0.9, 2 ** 32]) {
    expect(() => receiver.operationsFrom(cursor)).toThrow();
  }
  receiver.close();
  expect(() => receiver.get(a)).toThrow('closed');
  expect(() => receiver.getContent(a)).toThrow('closed');
  expect(() => receiver.getChildren(root)).toThrow('closed');
  expect(() => receiver.operationsFrom(0)).toThrow('closed');
  expect(() => receiver.local.payload(a, null)).toThrow('closed');
});

test.each(['write', 'close', 'log'])(
  'rejects reentrant %s from an input getter and remains usable',
  (action) => {
    const { client } = open();
    const placement = {
      get type(): 'last' {
        if (action === 'write') client.local.payload(a, null);
        else if (action === 'close') client.close();
        else client.operationCount();
        return 'last';
      },
    };
    expect(() => client.local.insert(root, a, placement, null)).toThrow();
    expect(client.local.insert(root, b, { type: 'last' }, null).meta.id.counter).toBe(1);
    expect(client.getChildren(root)).toEqual([b]);
  },
);

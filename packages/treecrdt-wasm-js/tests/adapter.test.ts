import { runInNewContext } from 'node:vm';
import { afterEach, beforeEach, expect, onTestFinished, test, vi } from 'vitest';
import type { Operation, TreecrdtAdapter } from '@treecrdt/interface';
import { bytesToHex, nodeIdToBytes16, replicaIdToBytes } from '@treecrdt/interface/ids';
import { createWasmAdapter } from '@treecrdt/wasm';
import { createMemoryClient } from '@treecrdt/wasm/memory';
import { encodeVersionVector, WasmTree } from '../pkg/treecrdt_wasm.js';

const replica = new Uint8Array(32).fill(1);
const node = '00000000000000000000000000000001';
const insert = {
  meta: { id: { replica, counter: 1 }, lamport: 1 },
  kind: { type: 'insert', parent: '0'.repeat(32), node, orderKey: Uint8Array.of(128) },
} satisfies Operation;
let adapter: TreecrdtAdapter;

beforeEach(async () => {
  adapter = await createWasmAdapter();
});

afterEach(async () => {
  await adapter.close?.();
  vi.restoreAllMocks();
});

test.each([
  ['local', Uint8Array],
  ['foreign', runInNewContext('Uint8Array') as Uint8ArrayConstructor],
])(
  'passes a %s-realm byte batch into WASM and preserves payload and delete data',
  async (_, Bytes) => {
    const memory = await createMemoryClient();
    onTestFinished(() => memory.close());
    const appendOps = vi.spyOn(WasmTree.prototype, 'appendOps');
    const insertWithBytes = {
      ...insert,
      kind: { ...insert.kind, orderKey: Bytes.of(99, 128, 99).subarray(1, 2) },
    };
    const payload: Operation = {
      meta: { id: { replica, counter: 2 }, lamport: 2 },
      kind: { type: 'payload', node, payload: Bytes.of(99, 42, 99).subarray(1, 2) },
    };
    const operations = [payload, insertWithBytes, insertWithBytes];
    await adapter.appendOps!(operations, nodeIdToBytes16, replicaIdToBytes);
    expect(appendOps).toHaveBeenCalledTimes(1);
    expect(await adapter.treePayload(nodeIdToBytes16(node))).toEqual(Uint8Array.of(42));
    expect(await adapter.opsSince(0)).toMatchObject([
      { kind: 'payload', payload: '2a' },
      { kind: 'insert', order_key: '80' },
    ]);
    memory.appendOperations(operations);
    expect(memory.get(node)?.payload).toEqual(Uint8Array.of(42));
    expect(memory.operationsFrom(0)[1]?.kind).toMatchObject({ orderKey: Uint8Array.of(128) });

    const knownState = Bytes.from([
      99,
      ...encodeVersionVector({ entries: [{ replica, frontier: 2n, ranges: [[4n, 4n]] }] }),
      99,
    ]).subarray(1, -1);
    const deletion: Operation = {
      meta: { id: { replica: Bytes.from(replica), counter: 3 }, lamport: 3, knownState },
      kind: { type: 'delete', node },
    };
    await adapter.appendOps!([deletion], nodeIdToBytes16, replicaIdToBytes);
    expect(await adapter.treeExists(nodeIdToBytes16(node))).toBe(false);
    expect(await adapter.opsSince(2)).toMatchObject([
      { kind: 'delete', known_state: Array.from(knownState) },
    ]);
    memory.appendOperations([deletion]);
    expect(memory.get(node)).toBeUndefined();
    expect(memory.operationsFrom(2)[0]?.meta.knownState).toEqual(Uint8Array.from(knownState));
  },
);

test.each([undefined, new Uint8Array(), Uint8Array.of(255)])(
  'rejects invalid delete knownState %s before applying the batch',
  async (knownState) => {
    const invalid: Operation = {
      meta: { id: { replica, counter: 2 }, lamport: 2, knownState },
      kind: { type: 'delete', node },
    };
    await expect(
      adapter.appendOps!([insert, invalid], nodeIdToBytes16, replicaIdToBytes),
    ).rejects.toThrow(
      knownState?.length ? 'truncated header' : 'delete operations require meta.knownState',
    );
    expect(await adapter.treeExists(nodeIdToBytes16(node))).toBe(false);
    expect(await adapter.opsSince(0)).toEqual([]);
  },
);

test.each([undefined, new Uint8Array(), Uint8Array.of(99, 0, 255, 99).subarray(1, 3)])(
  'preserves insert payload %s and copies byte views before returning',
  async (payload) => {
    const serializedReplica = new Uint8Array(34).fill(2).subarray(1, 33);
    const expectedPayload = payload?.slice() ?? null;
    await adapter.appendOp(
      { ...insert, kind: { ...insert.kind, payload } },
      nodeIdToBytes16,
      () => serializedReplica,
    );
    payload?.fill(99);
    serializedReplica.fill(99);
    expect(await adapter.treePayload(nodeIdToBytes16(node))).toEqual(expectedPayload);
    expect(await adapter.opsSince(0)).toMatchObject([
      {
        kind: 'insert',
        replica: '02'.repeat(32),
        payload: expectedPayload === null ? undefined : bytesToHex(expectedPayload),
      },
    ]);
  },
);

test('round-trips typed move and tombstone operations through existing read rows', async () => {
  const newParent = 'f'.repeat(32);
  await adapter.appendOps!(
    [
      insert,
      {
        meta: { id: { replica, counter: 2 }, lamport: 2 },
        kind: { type: 'move', node, newParent, orderKey: Uint8Array.of(0, 255) },
      },
      {
        meta: { id: { replica, counter: 3 }, lamport: 3 },
        kind: { type: 'tombstone', node },
      },
    ],
    nodeIdToBytes16,
    replicaIdToBytes,
  );
  expect(await adapter.opsSince(1)).toMatchObject([
    { kind: 'move', new_parent: newParent, order_key: '00ff' },
    { kind: 'tombstone' },
  ]);
});

test('copies each replica when the serializer reuses a buffer', async () => {
  const buffer = new Uint8Array(32);
  const second = {
    ...insert,
    meta: { ...insert.meta, id: { replica: new Uint8Array(32), counter: 1 } },
  };
  await adapter.appendOps!([insert, second], nodeIdToBytes16, (replica) => {
    buffer.set(replica);
    return buffer;
  });
  expect(await adapter.opsSince(0)).toHaveLength(2);
});

test('rejects a missing payload but accepts an explicit clear', async () => {
  const memory = await createMemoryClient();
  onTestFinished(() => memory.close());
  const invalid = { ...insert, kind: { type: 'payload', node } } as Operation;
  await expect(
    adapter.appendOps!([insert, invalid], nodeIdToBytes16, replicaIdToBytes),
  ).rejects.toThrow('payload operations require payload or null');
  expect(await adapter.opsSince(0)).toEqual([]);
  expect(() => memory.appendOperations([insert, invalid])).toThrow(
    'payload operations require payload or null',
  );
  expect(memory.operationCount()).toBe(0);
  const withPayload = { ...insert, kind: { ...insert.kind, payload: Uint8Array.of(42) } };
  const cleared: Operation = {
    meta: { id: { replica, counter: 2 }, lamport: 2 },
    kind: { type: 'payload', node, payload: null },
  };
  await adapter.appendOp(withPayload, nodeIdToBytes16, replicaIdToBytes);
  await adapter.appendOp(cleared, nodeIdToBytes16, replicaIdToBytes);
  expect(await adapter.treePayload(nodeIdToBytes16(node))).toBeNull();
  memory.appendOperations([withPayload, cleared]);
  expect(memory.get(node)?.payload).toBeNull();
});

test('accepts a typed operation when returning a materialization delta', () => {
  const tree = new WasmTree('7761736d');
  try {
    expect(tree.appendOpWithDelta(insert)).toEqual([insert.kind.parent, node]);
  } finally {
    tree.free();
  }
});

test.each([
  { meta: { ...insert.meta, id: { replica, counter: -1 } } },
  { meta: { ...insert.meta, id: { replica, counter: Number.MAX_SAFE_INTEGER + 1 } } },
  { meta: { ...insert.meta, lamport: 1.5 } },
  { meta: { ...insert.meta, lamport: 9_007_199_254_740_992n } },
  { meta: { ...insert.meta, id: { replica: [256], counter: 2 } } },
  { meta: { ...insert.meta, knownState: Uint8Array.of(255) } },
  { kind: { ...insert.kind, orderKey: '80' } },
  { kind: { ...insert.kind, orderKey: undefined } },
  { kind: { ...insert.kind, parent: Uint8Array.of(48, 48) } },
  { kind: { ...insert.kind, payload: [-1] } },
  { kind: { ...insert.kind, node: '€a' } },
])('rejects malformed typed input %# before ingesting the batch', (invalid) => {
  const tree = new WasmTree('7761736d');
  try {
    expect(() =>
      tree.appendOps([insert, { ...insert, ...invalid } as unknown as Operation]),
    ).toThrow();
    expect(tree.opsSince(0n)).toEqual([]);
  } finally {
    tree.free();
  }
});

import { afterEach, beforeEach, expect, test, vi } from 'vitest';
import type { Operation, TreecrdtAdapter } from '@treecrdt/interface';
import { nodeIdToBytes16, replicaIdToBytes } from '@treecrdt/interface/ids';
import { createWasmAdapter } from '@treecrdt/wasm';
import { encodeVersionVector, WasmTree } from '../pkg/treecrdt_wasm.js';

const replica = new Uint8Array(32).fill(1);
const node = '00000000000000000000000000000001';
const insert: Operation = {
  meta: { id: { replica, counter: 1 }, lamport: 1 },
  kind: { type: 'insert', parent: '0'.repeat(32), node, orderKey: Uint8Array.of(128) },
};
let adapter: TreecrdtAdapter;

beforeEach(async () => {
  adapter = await createWasmAdapter();
});

afterEach(async () => {
  await adapter.close?.();
  vi.restoreAllMocks();
});

test('passes the whole batch into WASM and preserves payload and delete data', async () => {
  const appendOps = vi.spyOn(WasmTree.prototype, 'appendOps');
  const payload: Operation = {
    meta: { id: { replica, counter: 2 }, lamport: 2 },
    kind: { type: 'payload', node, payload: Uint8Array.of(42) },
  };
  await adapter.appendOps!([payload, insert, insert], nodeIdToBytes16, replicaIdToBytes);
  expect(appendOps).toHaveBeenCalledTimes(1);
  expect(await adapter.treePayload(nodeIdToBytes16(node))).toEqual(Uint8Array.of(42));
  expect(await adapter.opsSince(0)).toHaveLength(2);

  await adapter.appendOps!(
    [
      {
        meta: {
          id: { replica, counter: 3 },
          lamport: 3,
          knownState: encodeVersionVector({ entries: [{ replica, frontier: 2n, ranges: [] }] }),
        },
        kind: { type: 'delete', node },
      },
    ],
    nodeIdToBytes16,
    replicaIdToBytes,
  );
  expect(await adapter.treeExists(nodeIdToBytes16(node))).toBe(false);
});

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

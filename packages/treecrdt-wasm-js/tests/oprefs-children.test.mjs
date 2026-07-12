import assert from 'node:assert/strict';
import test from 'node:test';

import { createWasmAdapter } from '../dist/index.js';

const root = '0'.repeat(32);
const replica = Uint8Array.from({ length: 32 }, (_, index) => index + 1);
const textEncoder = new TextEncoder();

function node(value) {
  return value.toString(16).padStart(32, '0');
}

function hexToBytes(hex) {
  return Uint8Array.from(Buffer.from(hex, 'hex'));
}

function meta(counter, knownState) {
  return {
    id: { replica, counter },
    lamport: counter,
    ...(knownState ? { knownState } : {}),
  };
}

function orderKey(position) {
  return Uint8Array.of(0, position + 1);
}

function insert(counter, parent, child) {
  return {
    meta: meta(counter),
    kind: { type: 'insert', parent, node: child, orderKey: orderKey(counter) },
  };
}

function move(counter, child, newParent) {
  return {
    meta: meta(counter),
    kind: { type: 'move', node: child, newParent, orderKey: orderKey(counter) },
  };
}

function payload(counter, child, value) {
  return {
    meta: meta(counter),
    kind: {
      type: 'payload',
      node: child,
      payload: value === null ? null : textEncoder.encode(value),
    },
  };
}

function knownState(frontier) {
  return textEncoder.encode(
    JSON.stringify({
      entries: [{ replica: Array.from(replica), frontier, ranges: [] }],
    }),
  );
}

function deleteNode(counter, child, frontier) {
  return {
    meta: meta(counter, knownState(frontier)),
    kind: { type: 'delete', node: child },
  };
}

function tombstone(counter, child) {
  return {
    meta: meta(counter),
    kind: { type: 'tombstone', node: child },
  };
}

async function append(adapter, ops) {
  await adapter.appendOps(ops, hexToBytes, (value) => value);
}

async function selectedOps(adapter, parent) {
  const rawRefs = await adapter.opRefsChildren(hexToBytes(parent));
  const refs = rawRefs.map((ref) => Uint8Array.from(ref));
  return await adapter.opsByOpRefs(refs);
}

test('children filter includes move-away, payload visibility, and delete operations', async () => {
  const adapter = await createWasmAdapter();
  const source = node(1);
  const destination = node(2);
  const child = node(3);
  const ops = [
    insert(1, root, source),
    insert(2, root, destination),
    insert(3, source, child),
    payload(4, child, 'before move'),
    move(5, child, destination),
    payload(6, child, null),
    deleteNode(7, child, 6),
  ];

  try {
    // Exercise the adapter's out-of-order append path as well as canonical filter ordering.
    await append(adapter, [ops[0], ops[1], ops[2], ops[4], ops[3], ops[5], ops[6]]);

    assert.deepEqual(
      (await selectedOps(adapter, source)).map((op) => op.counter),
      [3, 4, 5],
    );
    assert.deepEqual(
      (await selectedOps(adapter, destination)).map((op) => [op.counter, op.kind]),
      [
        [4, 'payload'],
        [5, 'move'],
        [6, 'payload'],
        [7, 'delete'],
      ],
    );
  } finally {
    await adapter.close();
  }
});

test('children filter includes descendant operations that restore a direct child', async () => {
  const adapter = await createWasmAdapter();
  const parent = node(11);
  const descendant = node(12);

  try {
    await append(adapter, [
      insert(1, root, parent),
      deleteNode(2, parent, 1),
      insert(3, parent, descendant),
    ]);

    assert.deepEqual(
      (await selectedOps(adapter, root)).map((op) => op.counter),
      [1, 2, 3],
    );
  } finally {
    await adapter.close();
  }
});

test('children filter includes tombstones and omits rejected cycles', async () => {
  const adapter = await createWasmAdapter();
  const parent = node(21);
  const child = node(22);

  try {
    await append(adapter, [
      insert(1, root, parent),
      insert(2, parent, child),
      move(3, parent, child),
      tombstone(4, child),
    ]);

    assert.deepEqual(
      (await selectedOps(adapter, parent)).map((op) => op.counter),
      [2, 4],
    );
    assert.deepEqual(await selectedOps(adapter, child), []);
  } finally {
    await adapter.close();
  }
});

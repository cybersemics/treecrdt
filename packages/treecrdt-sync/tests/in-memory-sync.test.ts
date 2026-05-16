import { expect, test } from 'vitest';
import { bytesToHex, nodeIdToBytes16 } from '@treecrdt/interface/ids';
import { makeOp, maxLamport, nodeIdFromInt } from '@treecrdt/benchmark';
import { createTreecrdtSyncBackendFromClient } from '@treecrdt/sync-sqlite/backend';
import { treecrdtSyncV0ProtobufCodec } from '@treecrdt/sync-protocol/protobuf';
import { SyncPeer, deriveOpRefV0 } from '@treecrdt/sync-protocol';
import {
  createInMemoryDuplex,
  wrapDuplexTransportWithCodec,
} from '@treecrdt/sync-protocol/transport';
import type { Operation } from '@treecrdt/interface';

import { createTreecrdtWebSocketSyncFromTransport } from '../src/create-sync-from-transport.js';
import { DEFAULT_MAX_OPS_PER_BATCH } from '../src/constants.js';
import type { TreecrdtWebSocketSyncClient } from '../src/types.js';
import { ROOT, createInMemoryTestClient, orderKeyFromPosition } from './test-helpers.js';

function replicaFromLabel(label: string): Uint8Array {
  const encoded = new TextEncoder().encode(label);
  if (encoded.length === 0) throw new Error('replica label must not be empty');
  const out = new Uint8Array(32);
  for (let i = 0; i < out.length; i += 1) out[i] = encoded[i % encoded.length]!;
  return out;
}

const replicas = { a: replicaFromLabel('a'), b: replicaFromLabel('b') };

async function headAsBigint(client: TreecrdtWebSocketSyncClient): Promise<bigint> {
  return BigInt(await client.meta.headLamport());
}

async function runSyncOnceInMemory(
  aClient: TreecrdtWebSocketSyncClient,
  bClient: TreecrdtWebSocketSyncClient,
  docId: string,
): Promise<void> {
  const [wireA, wireB] = createInMemoryDuplex<Uint8Array>();
  const transportA = wrapDuplexTransportWithCodec(wireA, treecrdtSyncV0ProtobufCodec);
  const transportB = wrapDuplexTransportWithCodec(wireB, treecrdtSyncV0ProtobufCodec);

  const backendB = createTreecrdtSyncBackendFromClient(bClient, docId, {
    maxLamport: () => headAsBigint(bClient),
  });
  const peerB = new SyncPeer(backendB, {
    maxCodewords: 100_000,
    maxOpsPerBatch: 2_000,
    deriveOpRef: (op, ctx) =>
      deriveOpRefV0(ctx.docId, { replica: op.meta.id.replica, counter: op.meta.id.counter }),
  });
  const detachB = peerB.attach(transportB);

  const onCloseB = () => {
    try {
      detachB();
    } catch {
      // ignore
    }
  };

  const sync = createTreecrdtWebSocketSyncFromTransport(aClient, transportA, onCloseB, {
    syncPeerOptions: { maxCodewords: 100_000, maxOpsPerBatch: 2_000 },
  });
  try {
    await sync.syncOnce({ all: {} });
  } finally {
    await sync.close();
  }
}

test('syncOnce pulls an insert from a remote peer (in-memory transport)', async () => {
  const docId = `sync-socket-mem-${Math.random().toString(16).slice(2)}`;
  const orderKey = new Uint8Array(2);
  const nodeId = nodeIdFromInt(1);
  const opB = makeOp(replicas.b, 1, 1, {
    type: 'insert',
    parent: ROOT,
    node: nodeId,
    orderKey,
  });
  const { client: aClient, getOps: getAllA } = createInMemoryTestClient(docId, []);
  const { client: bClient } = createInMemoryTestClient(docId, [opB]);
  const allA = await getAllA();
  expect(allA.length).toBe(0);

  await runSyncOnceInMemory(aClient, bClient, docId);
  const after = await getAllA();
  expect(after.length).toBe(1);
  const [first] = after;
  expect(first!.meta.lamport).toBe(1);
  expect(bytesToHex(first!.meta.id.replica)).toBe(bytesToHex(replicas.b));
  expect('kind' in first! && first!.kind.type === 'insert' ? first!.kind.node : '').toBe(nodeId);
  expect('kind' in first! && first!.kind.type === 'insert' ? first!.kind.parent : '').toBe(ROOT);
  expect(
    (() => {
      if (!('kind' in first!) || first!.kind.type !== 'insert') return new Uint8Array();
      return nodeIdToBytes16(first!.kind.node);
    })(),
  ).toEqual(nodeIdToBytes16(nodeId));
});

test('syncOnce defaults split inbound applies into modest batches', async () => {
  const docId = `sync-default-batches-${Math.random().toString(16).slice(2)}`;
  const totalOps = DEFAULT_MAX_OPS_PER_BATCH * 2 + 1;
  const remoteOps = Array.from({ length: totalOps }, (_, index) =>
    makeOp(replicas.b, index + 1, index + 1, {
      type: 'insert',
      parent: ROOT,
      node: nodeIdFromInt(index + 1),
      orderKey: orderKeyFromPosition(index),
    }),
  );
  const { client: aClient, getOps: getAllA } = createInMemoryTestClient(docId, []);
  const { client: bClient } = createInMemoryTestClient(docId, remoteOps);
  const appendBatchSizes: number[] = [];
  const appendMany = aClient.ops.appendMany.bind(aClient.ops);
  aClient.ops.appendMany = async (ops, writeOpts) => {
    appendBatchSizes.push(ops.length);
    return appendMany(ops, writeOpts);
  };

  const [wireA, wireB] = createInMemoryDuplex<Uint8Array>();
  const transportA = wrapDuplexTransportWithCodec(wireA, treecrdtSyncV0ProtobufCodec);
  const transportB = wrapDuplexTransportWithCodec(wireB, treecrdtSyncV0ProtobufCodec);
  const backendB = createTreecrdtSyncBackendFromClient(bClient, docId, {
    maxLamport: () => headAsBigint(bClient),
  });
  const peerB = new SyncPeer(backendB, {
    maxCodewords: 100_000,
    deriveOpRef: (op, ctx) =>
      deriveOpRefV0(ctx.docId, { replica: op.meta.id.replica, counter: op.meta.id.counter }),
  });
  const detachB = peerB.attach(transportB);
  const sync = createTreecrdtWebSocketSyncFromTransport(aClient, transportA, detachB);

  try {
    await sync.syncOnce({ all: {} }, { maxCodewords: 100_000 });
  } finally {
    await sync.close();
  }

  expect((await getAllA()).length).toBe(totalOps);
  expect(appendBatchSizes.length).toBeGreaterThan(1);
  expect(Math.max(...appendBatchSizes)).toBeLessThanOrEqual(DEFAULT_MAX_OPS_PER_BATCH);
  expect(appendBatchSizes.reduce((sum, size) => sum + size, 0)).toBe(totalOps);
});

test('syncOnce pulls insert, move, payload, and delete operations', async () => {
  const docId = `sync-socket-mem-mix-${Math.random().toString(16).slice(2)}`;
  const n1 = nodeIdFromInt(1);
  const n2 = nodeIdFromInt(2);
  const payloadBytes = new TextEncoder().encode('hello-payload');
  const ops: Operation[] = [
    makeOp(replicas.b, 1, 1, {
      type: 'insert',
      parent: ROOT,
      node: n1,
      orderKey: orderKeyFromPosition(0),
    }),
    makeOp(replicas.b, 2, 2, {
      type: 'insert',
      parent: ROOT,
      node: n2,
      orderKey: orderKeyFromPosition(1),
    }),
    makeOp(replicas.b, 3, 3, {
      type: 'move',
      node: n2,
      newParent: n1,
      orderKey: orderKeyFromPosition(0),
    }),
    makeOp(replicas.b, 4, 4, {
      type: 'payload',
      node: n1,
      payload: payloadBytes,
    }),
    {
      meta: {
        id: { replica: replicas.b, counter: 5 },
        lamport: 5,
        knownState: new Uint8Array([0x5b, 0x5d]), // minimal non-empty (sync requires this for delete)
      },
      kind: { type: 'delete' as const, node: n2 },
    },
  ];
  const { client: aClient, getOps: getAllA } = createInMemoryTestClient(docId, []);
  const { client: bClient } = createInMemoryTestClient(docId, ops);
  expect((await getAllA()).length).toBe(0);
  expect(maxLamport(await bClient.ops.all())).toBe(5);

  await runSyncOnceInMemory(aClient, bClient, docId);

  const after = await getAllA();
  expect(after.length).toBe(5);
  const byKind = after.reduce<Record<string, number>>((acc, o) => {
    acc[o.kind.type] = (acc[o.kind.type] ?? 0) + 1;
    return acc;
  }, {});
  expect(byKind.insert).toBe(2);
  expect(byKind.move).toBe(1);
  expect(byKind.payload).toBe(1);
  expect(byKind.delete).toBe(1);

  const del = after.find((o) => o.kind.type === 'delete')!;
  expect(del?.kind.type).toBe('delete');
  if (del?.kind.type === 'delete') expect(del.kind.node).toBe(n2);

  const payloadOp = after.find((o) => o.kind.type === 'payload')!;
  expect(payloadOp?.kind.type).toBe('payload');
  if (payloadOp?.kind.type === 'payload') {
    expect(new Uint8Array(payloadOp.kind.payload!)).toEqual(payloadBytes);
  }

  const moveOp = after.find((o) => o.kind.type === 'move')!;
  expect(moveOp?.kind.type).toBe('move');
  if (moveOp?.kind.type === 'move') {
    expect(moveOp.kind.node).toBe(n2);
    expect(moveOp.kind.newParent).toBe(n1);
  }

  const inserts = after.filter((o) => o.kind.type === 'insert');
  expect(inserts).toHaveLength(2);
  const nodes = new Set(inserts.map((o) => (o.kind.type === 'insert' ? o.kind.node : '')));
  expect(nodes).toEqual(new Set([n1, n2]));
});

test('pushLocalOps uploads an insert to the remote peer (in-memory transport)', async () => {
  const docId = `sync-push-local-${Math.random().toString(16).slice(2)}`;
  const nodeId = nodeIdFromInt(1);
  const opA = makeOp(replicas.a, 1, 1, {
    type: 'insert',
    parent: ROOT,
    node: nodeId,
    orderKey: orderKeyFromPosition(0),
  });

  const { client: aClient } = createInMemoryTestClient(docId, []);
  const { client: bClient, getOps: getAllB } = createInMemoryTestClient(docId, []);
  await aClient.ops.append(opA);
  expect((await getAllB()).length).toBe(0);

  const [wireA, wireB] = createInMemoryDuplex<Uint8Array>();
  const transportA = wrapDuplexTransportWithCodec(wireA, treecrdtSyncV0ProtobufCodec);
  const transportB = wrapDuplexTransportWithCodec(wireB, treecrdtSyncV0ProtobufCodec);

  const backendB = createTreecrdtSyncBackendFromClient(bClient, docId, {
    maxLamport: () => headAsBigint(bClient),
  });
  const peerB = new SyncPeer(backendB, {
    maxCodewords: 100_000,
    maxOpsPerBatch: 2_000,
    deriveOpRef: (op, ctx) =>
      deriveOpRefV0(ctx.docId, { replica: op.meta.id.replica, counter: op.meta.id.counter }),
  });
  const detachB = peerB.attach(transportB);
  const onCloseB = () => {
    try {
      detachB();
    } catch {
      // ignore
    }
  };

  const sync = createTreecrdtWebSocketSyncFromTransport(aClient, transportA, onCloseB, {
    syncPeerOptions: { maxCodewords: 100_000, maxOpsPerBatch: 2_000 },
  });
  try {
    await sync.pushLocalOps([opA]);
  } finally {
    await sync.close();
  }

  const afterB = await getAllB();
  expect(afterB.length).toBe(1);
  const [first] = afterB;
  expect(bytesToHex(first!.meta.id.replica)).toBe(bytesToHex(replicas.a));
  expect(first!.meta.lamport).toBe(1);
  expect('kind' in first! && first!.kind.type === 'insert' ? first!.kind.node : '').toBe(nodeId);
});

test('pushLocalOps with no ops is a no-op (no syncOnce)', async () => {
  const docId = `sync-pushnoop-${Math.random().toString(16).slice(2)}`;

  const { client: aClient } = createInMemoryTestClient(docId, []);
  const { client: bClient, getOps: getAllB } = createInMemoryTestClient(docId, []);
  const opB = makeOp(replicas.b, 1, 1, {
    type: 'insert',
    parent: ROOT,
    node: nodeIdFromInt(42),
    orderKey: orderKeyFromPosition(0),
  });
  await bClient.ops.append(opB);
  expect((await getAllB()).length).toBe(1);

  const [wireA, wireB] = createInMemoryDuplex<Uint8Array>();
  const transportA = wrapDuplexTransportWithCodec(wireA, treecrdtSyncV0ProtobufCodec);
  const transportB = wrapDuplexTransportWithCodec(wireB, treecrdtSyncV0ProtobufCodec);

  const backendB = createTreecrdtSyncBackendFromClient(bClient, docId, {
    maxLamport: () => headAsBigint(bClient),
  });
  const peerB = new SyncPeer(backendB, {
    maxCodewords: 100_000,
    maxOpsPerBatch: 2_000,
    deriveOpRef: (op, ctx) =>
      deriveOpRefV0(ctx.docId, { replica: op.meta.id.replica, counter: op.meta.id.counter }),
  });
  const detachB = peerB.attach(transportB);
  const onCloseB = () => {
    try {
      detachB();
    } catch {
      // ignore
    }
  };

  const sync = createTreecrdtWebSocketSyncFromTransport(aClient, transportA, onCloseB, {
    syncPeerOptions: { maxCodewords: 100_000, maxOpsPerBatch: 2_000 },
  });
  try {
    await sync.pushLocalOps();
    await sync.pushLocalOps([]);
    const onA = await aClient.ops.all();
    expect(onA.length).toBe(0);
  } finally {
    await sync.close();
  }

  expect((await getAllB()).length).toBe(1);
});

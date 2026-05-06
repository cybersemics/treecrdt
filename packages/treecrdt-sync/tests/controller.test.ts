import { expect, test, vi } from 'vitest';
import { bytesToHex } from '@treecrdt/interface/ids';
import { makeOp, nodeIdFromInt } from '@treecrdt/benchmark';
import { createTreecrdtSyncBackendFromClient } from '@treecrdt/sync-sqlite/backend';
import { treecrdtSyncV0ProtobufCodec } from '@treecrdt/sync-protocol/protobuf';
import { SyncPeer, deriveOpRefV0 } from '@treecrdt/sync-protocol';
import {
  createInMemoryDuplex,
  wrapDuplexTransportWithCodec,
} from '@treecrdt/sync-protocol/transport';
import type { Operation } from '@treecrdt/interface';
import type { TreecrdtWebSocketSync } from '../src/types.js';

import { createOutboundSync, createSyncController } from '../src/controller.js';
import { createTreecrdtWebSocketSyncFromTransport } from '../src/create-sync-from-transport.js';
import type { TreecrdtWebSocketSyncClient } from '../src/types.js';
import { ROOT, createInMemoryTestClient, orderKeyFromPosition } from './test-helpers.js';

function replicaFromLabel(label: string): Uint8Array {
  const encoded = new TextEncoder().encode(label);
  if (encoded.length === 0) throw new Error('replica label must not be empty');
  const out = new Uint8Array(32);
  for (let i = 0; i < out.length; i += 1) out[i] = encoded[i % encoded.length]!;
  return out;
}

const replicas = { a: replicaFromLabel('a') };

async function headAsBigint(client: TreecrdtWebSocketSyncClient): Promise<bigint> {
  return BigInt(await client.meta.headLamport());
}

function makeInsertOp(counter = 1): Operation {
  return makeOp(replicas.a, counter, counter, {
    type: 'insert',
    parent: ROOT,
    node: nodeIdFromInt(counter),
    orderKey: orderKeyFromPosition(counter - 1),
  });
}

function createConnectedInMemorySync(
  aClient: TreecrdtWebSocketSyncClient,
  bClient: TreecrdtWebSocketSyncClient,
  docId: string,
): TreecrdtWebSocketSync {
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

  return createTreecrdtWebSocketSyncFromTransport(
    aClient,
    transportA,
    () => {
      try {
        detachB();
      } catch {
        // ignore
      }
    },
    { syncPeerOptions: { maxCodewords: 100_000, maxOpsPerBatch: 2_000 } },
  );
}

function createFakeSync(opts: { failPushes?: number; failStarts?: number } = {}) {
  let failPushes = opts.failPushes ?? 0;
  let failStarts = opts.failStarts ?? 0;
  const pushed: Operation[][] = [];
  const sync: TreecrdtWebSocketSync = {
    onChange: () => () => {},
    syncOnce: vi.fn(async () => {
      if (failStarts > 0) {
        failStarts -= 1;
        throw new Error('startup failed');
      }
    }),
    startLive: vi.fn(async () => {}),
    stopLive: vi.fn(() => {}),
    pushLocalOps: vi.fn(async (ops = []) => {
      if (failPushes > 0) {
        failPushes -= 1;
        throw new Error('push failed');
      }
      pushed.push([...ops]);
    }),
    close: vi.fn(async () => {}),
  };
  return { pushed, sync };
}

function opUploadKey(op: Operation): string {
  return `${bytesToHex(op.meta.id.replica)}:${op.meta.id.counter.toString()}`;
}

function createFakePeer(opts: { failPushes?: number; failSyncs?: number } = {}) {
  let failPushes = opts.failPushes ?? 0;
  let failSyncs = opts.failSyncs ?? 0;
  const pushed: Operation[][] = [];
  const synced: unknown[] = [];
  const peer = {
    pushOps: vi.fn(async (_transport: unknown, ops: readonly Operation[]) => {
      if (failPushes > 0) {
        failPushes -= 1;
        throw new Error('direct push failed');
      }
      pushed.push([...ops]);
    }),
    syncOnce: vi.fn(async (_transport: unknown, filter: unknown) => {
      if (failSyncs > 0) {
        failSyncs -= 1;
        throw new Error('fallback sync failed');
      }
      synced.push(filter);
    }),
  } as unknown as SyncPeer<Operation>;
  return { peer, pushed, synced };
}

test('controller queues local ops before start and flushes after startup', async () => {
  const docId = `sync-controller-queue-${Math.random().toString(16).slice(2)}`;
  const op = makeInsertOp();
  const { client: aClient } = createInMemoryTestClient(docId, []);
  const { client: bClient, getOps: getAllB } = createInMemoryTestClient(docId, []);
  await aClient.ops.append(op);

  const sync = createConnectedInMemorySync(aClient, bClient, docId);
  const statuses: string[] = [];
  const controller = createSyncController(sync, {
    live: false,
    onStatus: (status) => statuses.push(`${status.state}:${status.pendingOps}`),
  });

  try {
    await controller.pushLocalOps([op]);
    expect(controller.pendingOpCount).toBe(1);
    expect(await getAllB()).toHaveLength(0);

    await controller.start();

    expect(controller.pendingOpCount).toBe(0);
    const afterB = await getAllB();
    expect(afterB).toHaveLength(1);
    expect(bytesToHex(afterB[0]!.meta.id.replica)).toBe(bytesToHex(replicas.a));
    expect(statuses).toContain('live:0');
  } finally {
    await controller.close();
  }
});

test('controller keeps failed pushes queued for retry', async () => {
  const op = makeInsertOp();
  const { pushed, sync } = createFakeSync({ failPushes: 1 });
  const errors: unknown[] = [];
  const controller = createSyncController(sync, {
    live: false,
    onError: (err) => errors.push(err),
  });

  await controller.start();
  await expect(controller.pushLocalOps([op])).rejects.toThrow('push failed');
  expect(controller.pendingOpCount).toBe(1);
  expect(controller.status.state).toBe('error');
  expect(errors).toHaveLength(1);

  await controller.flushPendingOps();

  expect(controller.pendingOpCount).toBe(0);
  expect(controller.status.state).toBe('live');
  expect(pushed.flat()).toEqual([op]);
});

test('controller retries startup and flushes ops queued before failed start', async () => {
  const op = makeInsertOp();
  const { pushed, sync } = createFakeSync({ failPushes: 1 });
  const controller = createSyncController(sync, { live: false });

  await controller.pushLocalOps([op]);
  await expect(controller.start()).rejects.toThrow('push failed');
  expect(controller.pendingOpCount).toBe(1);
  expect(pushed).toHaveLength(0);

  await controller.start();

  expect(controller.pendingOpCount).toBe(0);
  expect(pushed.flat()).toEqual([op]);
});

test('controller close stops future flushes and rejects new work', async () => {
  const op = makeInsertOp();
  const { pushed, sync } = createFakeSync();
  const controller = createSyncController(sync, { live: false });

  await controller.pushLocalOps([op]);
  await controller.close();

  expect(controller.status.state).toBe('closed');
  expect(pushed).toHaveLength(0);
  expect(sync.close).toHaveBeenCalledTimes(1);
  await expect(controller.start()).rejects.toThrow('closed');
  await expect(controller.pushLocalOps([makeInsertOp(2)])).rejects.toThrow('closed');
});

test('outbound sync queues local ops until a selected peer is available', async () => {
  const op = makeInsertOp();
  const { peer, pushed } = createFakePeer();
  const controller = createOutboundSync({
    localPeer: peer,
    opKey: opUploadKey,
    shouldSyncPeer: (peerId) => peerId.startsWith('remote:'),
  });

  controller.queue([op, op]);
  await controller.flush();

  expect(controller.pendingOpCount).toBe(1);
  expect(pushed).toHaveLength(0);

  controller.addPeer('local:tab', {} as any);
  await controller.flush();

  expect(controller.pendingOpCount).toBe(1);
  expect(pushed).toHaveLength(0);

  controller.addPeer('remote:server', {} as any);
  await controller.flush();

  expect(controller.pendingOpCount).toBe(0);
  expect(pushed).toEqual([[op]]);
});

test('outbound sync keeps failed direct pushes queued', async () => {
  const op = makeInsertOp();
  const { peer, pushed } = createFakePeer({ failPushes: 1 });
  const errors: unknown[] = [];
  const controller = createOutboundSync({
    localPeer: peer,
    opKey: opUploadKey,
    onError: ({ error }) => errors.push(error),
  });
  controller.addPeer('remote:server', {} as any);

  controller.queue([op]);
  await controller.flush();

  expect(controller.pendingOpCount).toBe(1);
  expect(errors).toHaveLength(1);
  expect(pushed).toHaveLength(0);

  await controller.flush();

  expect(controller.pendingOpCount).toBe(0);
  expect(pushed).toEqual([[op]]);
});

test('outbound sync runs fallback sync when no exact ops are available', async () => {
  const filter = { children: { parent: nodeIdFromInt(42) } };
  const { peer, synced } = createFakePeer();
  const controller = createOutboundSync({
    localPeer: peer,
    getFallbackFilters: () => [filter],
  });
  controller.addPeer('remote:server', {} as any);

  controller.queue();
  await controller.flush();

  expect(controller.pendingOpCount).toBe(0);
  expect(synced).toEqual([filter]);
});

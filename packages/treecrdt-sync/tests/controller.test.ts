import { expect, test, vi } from 'vitest';
import type { Operation } from '@treecrdt/interface';
import { makeOp, nodeIdFromInt } from '@treecrdt/benchmark';
import type { SyncPeer } from '@treecrdt/sync-protocol';

import { createOutboundSync } from '../src/controller.js';
import { ROOT, orderKeyFromPosition } from './test-helpers.js';

function replicaFromLabel(label: string): Uint8Array {
  const encoded = new TextEncoder().encode(label);
  if (encoded.length === 0) throw new Error('replica label must not be empty');
  const out = new Uint8Array(32);
  for (let i = 0; i < out.length; i += 1) out[i] = encoded[i % encoded.length]!;
  return out;
}

const replicas = { a: replicaFromLabel('a') };

function makeInsertOp(counter = 1): Operation {
  return makeOp(replicas.a, counter, counter, {
    type: 'insert',
    parent: ROOT,
    node: nodeIdFromInt(counter),
    orderKey: orderKeyFromPosition(counter - 1),
  });
}

function createFakePeer<Op = Operation>(opts: { failPushes?: number } = {}) {
  let failPushes = opts.failPushes ?? 0;
  const pushed: Op[][] = [];
  const peer = {
    notifyLocalUpdate: vi.fn(async () => {}),
    pushOps: vi.fn(async (_transport: unknown, ops: readonly Op[]) => {
      if (failPushes > 0) {
        failPushes -= 1;
        throw new Error('direct push failed');
      }
      pushed.push([...ops]);
    }),
  } as unknown as SyncPeer<Op>;
  return { peer, pushed };
}

test('outbound sync queues local ops until an outbound peer is available', async () => {
  const op = makeInsertOp();
  const { peer, pushed } = createFakePeer();
  const controller = createOutboundSync({
    localPeer: peer,
  });

  controller.queueOps([op, op]);
  await controller.flush();

  expect(peer.notifyLocalUpdate).toHaveBeenCalledWith([op, op]);
  expect(controller.pendingOpCount).toBe(1);
  expect(pushed).toHaveLength(0);

  controller.addPeer('remote:server', {} as any);
  await controller.flush();

  expect(controller.pendingOpCount).toBe(0);
  expect(pushed).toEqual([[op]]);
});

test('outbound sync keeps queued ops while offline', async () => {
  const op = makeInsertOp();
  let online = false;
  const { peer, pushed } = createFakePeer();
  const controller = createOutboundSync({
    localPeer: peer,
    isOnline: () => online,
  });
  controller.addPeer('remote:server', {} as any);

  controller.queueOps([op]);
  await controller.flush();

  expect(controller.pendingOpCount).toBe(1);
  expect(pushed).toEqual([]);

  online = true;
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
    onError: ({ error }) => errors.push(error),
  });
  controller.addPeer('remote:server', {} as any);

  controller.queueOps([op]);
  await controller.flush();

  expect(controller.pendingOpCount).toBe(1);
  expect(errors).toHaveLength(1);
  expect(pushed).toHaveLength(0);

  await controller.flush();

  expect(controller.pendingOpCount).toBe(0);
  expect(pushed).toEqual([[op]]);
});

test('outbound sync accepts custom op keys for non-TreeCRDT op shapes', async () => {
  type CustomOp = { id: string };
  const op: CustomOp = { id: 'local-write-1' };
  const { peer, pushed } = createFakePeer<CustomOp>();
  const controller = createOutboundSync<CustomOp>({
    localPeer: peer,
    opKey: (next) => next.id,
  });

  controller.queueOps([op, { id: op.id }]);
  await controller.flush();

  expect(peer.notifyLocalUpdate).toHaveBeenCalledWith([op, { id: op.id }]);
  expect(controller.pendingOpCount).toBe(1);
  expect(pushed).toHaveLength(0);

  controller.addPeer('remote:server', {} as any);
  await controller.flush();

  expect(controller.pendingOpCount).toBe(0);
  expect(pushed).toEqual([[op]]);
});

test('outbound sync ignores empty op batches', async () => {
  const { peer, pushed } = createFakePeer();
  const statuses: number[] = [];
  const controller = createOutboundSync({
    localPeer: peer,
    onStatus: (status) => statuses.push(status.pendingOps),
  });
  controller.addPeer('remote:server', {} as any);

  controller.queueOps([]);
  await controller.flush();

  expect(peer.notifyLocalUpdate).not.toHaveBeenCalled();
  expect(controller.pendingOpCount).toBe(0);
  expect(pushed).toEqual([]);
  expect(statuses.at(-1)).toBe(0);
});

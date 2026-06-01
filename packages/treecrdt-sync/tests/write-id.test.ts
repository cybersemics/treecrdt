import { expect, test } from 'vitest';
import { makeOp, nodeIdFromInt } from '@treecrdt/benchmark';
import { treecrdtSyncV0ProtobufCodec } from '@treecrdt/sync-protocol/protobuf';
import { wrapDuplexTransportWithCodec } from '@treecrdt/sync-protocol/transport';
import { createInMemoryDuplex } from '@treecrdt/sync-protocol/transport';
import type { Operation } from '@treecrdt/interface';
import type { MaterializationEvent } from '@treecrdt/interface/engine';

import { createTreecrdtWebSocketSyncFromTransport } from '../src/create-sync-from-transport.js';
import { ROOT, createInMemoryTestClientWithWriteId, orderKeyFromPosition } from './test-helpers.js';

function replicaFromLabel(label: string): Uint8Array {
  const encoded = new TextEncoder().encode(label);
  if (encoded.length === 0) throw new Error('replica label must not be empty');
  const out = new Uint8Array(32);
  for (let i = 0; i < out.length; i += 1) out[i] = encoded[i % encoded.length]!;
  return out;
}

const r = replicaFromLabel('w');

/**
 * A minimal duplex so {@link createTreecrdtWebSocketSyncFromTransport} can attach; no second peer.
 */
function noopTransport() {
  const [wireA, _wireB] = createInMemoryDuplex<Uint8Array>();
  return wrapDuplexTransportWithCodec(wireA, treecrdtSyncV0ProtobufCodec);
}

test('onMaterialized receives per-change writeIds from appendMany(…, { writeId })', async () => {
  const docId = `write-id-${Math.random().toString(16).slice(2)}`;
  const { client } = createInMemoryTestClientWithWriteId(docId, []);
  const transport = noopTransport();
  const sync = createTreecrdtWebSocketSyncFromTransport(client, transport, undefined);

  const events: MaterializationEvent[] = [];
  const u = client.onMaterialized((e) => {
    events.push(e);
  });

  const n1 = nodeIdFromInt(1);
  const op: Operation = makeOp(r, 1, 1, {
    type: 'insert',
    parent: ROOT,
    node: n1,
    orderKey: orderKeyFromPosition(0),
  });

  try {
    await client.ops.appendMany([op], { writeId: 'my-batch-42' });
  } finally {
    u();
    await sync.close();
  }

  const withWrite = events.find((e) =>
    e.changes.some(
      (c) => c.kind === 'insert' && c.node === n1 && c.source?.writeIds?.includes('my-batch-42'),
    ),
  );
  expect(withWrite).toBeDefined();
  expect('writeIds' in withWrite!).toBe(false);
  expect(withWrite!.changes.every((change) => change.source?.writeIds?.[0] === 'my-batch-42')).toBe(
    true,
  );
});

test('onMaterialized receives per-change writeIds from append(…, { writeId })', async () => {
  const docId = `write-id-s-${Math.random().toString(16).slice(2)}`;
  const { client } = createInMemoryTestClientWithWriteId(docId, []);
  const transport = noopTransport();
  const sync = createTreecrdtWebSocketSyncFromTransport(client, transport, undefined);
  const seen: string[] = [];
  let sawRootWriteIds = false;
  const u = client.onMaterialized((e) => {
    sawRootWriteIds ||= 'writeIds' in e;
    for (const change of e.changes) {
      if (change.source?.writeIds?.[0]) seen.push(change.source.writeIds[0]!);
    }
  });
  const n1 = nodeIdFromInt(1);
  const op: Operation = makeOp(r, 1, 1, {
    type: 'insert',
    parent: ROOT,
    node: n1,
    orderKey: orderKeyFromPosition(0),
  });
  try {
    await client.ops.append(op, { writeId: 'single-append' });
  } finally {
    u();
    await sync.close();
  }
  expect(sawRootWriteIds).toBe(false);
  expect(seen).toContain('single-append');
});

import { randomUUID } from 'node:crypto';

import { expect, test } from 'vitest';

import type { Operation, ReplicaId } from '@justthrowaway/interface';
import { bytesToHex, nodeIdToBytes16 } from '@justthrowaway/interface/ids';
import { makeOp, nodeIdFromInt } from '@justthrowaway/benchmark';
import { deriveOpRefV0, type SyncBackend } from '@justthrowaway/sync-protocol';

type SyncBackendHarness = {
  openBackend: (docId: string) => Promise<SyncBackend<Operation>> | SyncBackend<Operation>;
  supportsDocIsolationAcrossOpen?: boolean;
  close?: () => Promise<void> | void;
};

function orderKeyFromPosition(position: number): Uint8Array {
  if (!Number.isInteger(position) || position < 0) throw new Error(`invalid position: ${position}`);
  const n = position + 1;
  if (n > 0xffff) throw new Error(`position too large for u16 order key: ${position}`);
  const bytes = new Uint8Array(2);
  new DataView(bytes.buffer).setUint16(0, n, false);
  return bytes;
}

function replicaFromLabel(label: string): ReplicaId {
  const encoded = new TextEncoder().encode(label);
  if (encoded.length === 0) throw new Error('label must not be empty');
  const out = new Uint8Array(32);
  for (let i = 0; i < out.length; i += 1) out[i] = encoded[i % encoded.length]!;
  return out;
}

const root = '0'.repeat(32);

export function defineSyncBackendContract(
  label: string,
  createHarness: () => Promise<SyncBackendHarness> | SyncBackendHarness,
): void {
  test(`${label}: getOpsByOpRefs preserves request order and errors on missing`, async () => {
    const harness = await createHarness();
    try {
      const docId = `doc-order-${randomUUID()}`;
      const backend = await harness.openBackend(docId);
      const replica = replicaFromLabel('a');

      await backend.applyOps([
        makeOp(replica, 1, 1, {
          type: 'insert',
          parent: root,
          node: nodeIdFromInt(1),
          orderKey: orderKeyFromPosition(0),
        }),
        makeOp(replica, 2, 2, {
          type: 'insert',
          parent: root,
          node: nodeIdFromInt(2),
          orderKey: orderKeyFromPosition(1),
        }),
      ]);

      const refs = await backend.listOpRefs({ all: {} });
      expect(refs).toHaveLength(2);
      expect(await backend.maxLamport()).toBe(2n);

      const ops = await backend.getOpsByOpRefs([refs[1]!, refs[0]!]);
      expect(ops.map((op) => op.meta.id.counter)).toEqual([2, 1]);

      await expect(backend.getOpsByOpRefs([new Uint8Array(16)])).rejects.toThrow(
        /missing|not found/i,
      );
    } finally {
      await harness.close?.();
    }
  });

  test(`${label}: doc isolation is preserved through openBackend when supported`, async () => {
    const harness = await createHarness();
    try {
      if (harness.supportsDocIsolationAcrossOpen === false) return;
      const replicaA = replicaFromLabel('a');
      const replicaB = replicaFromLabel('b');
      const a = await harness.openBackend(`doc-a-${randomUUID()}`);
      const b = await harness.openBackend(`doc-b-${randomUUID()}`);

      await a.applyOps([
        makeOp(replicaA, 1, 1, {
          type: 'insert',
          parent: root,
          node: nodeIdFromInt(11),
          orderKey: orderKeyFromPosition(0),
        }),
      ]);
      await b.applyOps([
        makeOp(replicaB, 1, 1, {
          type: 'insert',
          parent: root,
          node: nodeIdFromInt(22),
          orderKey: orderKeyFromPosition(0),
        }),
      ]);

      expect(await a.listOpRefs({ all: {} })).toHaveLength(1);
      expect(await b.listOpRefs({ all: {} })).toHaveLength(1);
    } finally {
      await harness.close?.();
    }
  });

  test(`${label}: children(parent) includes boundary-crossing moves and moved payloads`, async () => {
    const harness = await createHarness();
    try {
      const docId = `doc-children-move-${randomUUID()}`;
      const backend = await harness.openBackend(docId);
      const replica = replicaFromLabel('m');
      const parentA = nodeIdFromInt(101);
      const parentB = nodeIdFromInt(102);
      const node = nodeIdFromInt(103);

      await backend.applyOps([
        makeOp(replica, 1, 1, {
          type: 'insert',
          parent: root,
          node: parentA,
          orderKey: orderKeyFromPosition(0),
        }),
        makeOp(replica, 2, 2, {
          type: 'insert',
          parent: root,
          node: parentB,
          orderKey: orderKeyFromPosition(1),
        }),
        makeOp(replica, 3, 3, {
          type: 'insert',
          parent: parentA,
          node,
          orderKey: orderKeyFromPosition(0),
        }),
        makeOp(replica, 4, 4, {
          type: 'payload',
          node,
          payload: new Uint8Array([7]),
        }),
        makeOp(replica, 5, 5, {
          type: 'move',
          node,
          newParent: parentB,
          orderKey: orderKeyFromPosition(0),
        }),
        makeOp(replica, 6, 6, {
          type: 'payload',
          node,
          payload: new Uint8Array([8]),
        }),
      ]);

      const refsParentB = await backend.listOpRefs({
        children: { parent: nodeIdToBytes16(parentB) },
      });
      const opsParentB = await backend.getOpsByOpRefs(refsParentB);
      const kindsParentB = new Set(opsParentB.map((op) => op.kind.type));
      expect(kindsParentB.has('move')).toBe(true);
      expect(kindsParentB.has('payload')).toBe(true);

      const refsParentA = await backend.listOpRefs({
        children: { parent: nodeIdToBytes16(parentA) },
      });
      const opsParentA = await backend.getOpsByOpRefs(refsParentA);
      expect(opsParentA.some((op) => op.kind.type === 'move')).toBe(true);
    } finally {
      await harness.close?.();
    }
  });

  test(`${label}: children(parent) includes the scope root payload opRef exactly once`, async () => {
    const harness = await createHarness();
    try {
      const docId = `doc-scope-root-${randomUUID()}`;
      const backend = await harness.openBackend(docId);
      const replica = replicaFromLabel('p');
      const parent = nodeIdFromInt(100);
      const child = nodeIdFromInt(101);

      await backend.applyOps([
        makeOp(replica, 1, 1, {
          type: 'insert',
          parent: root,
          node: parent,
          orderKey: orderKeyFromPosition(0),
        }),
        makeOp(replica, 2, 2, {
          type: 'payload',
          node: parent,
          payload: new Uint8Array([9]),
        }),
        makeOp(replica, 3, 3, {
          type: 'insert',
          parent,
          node: child,
          orderKey: orderKeyFromPosition(0),
        }),
      ]);

      const refs = await backend.listOpRefs({
        children: { parent: nodeIdToBytes16(parent) },
      });
      const payloadWriterHex = bytesToHex(deriveOpRefV0(docId, { replica, counter: 2n }));

      expect(refs.map(bytesToHex).filter((hex) => hex === payloadWriterHex)).toHaveLength(1);
    } finally {
      await harness.close?.();
    }
  });
}

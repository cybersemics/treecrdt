import { randomUUID } from 'node:crypto';

import { expect, test } from 'vitest';

import type { Operation } from '@treecrdt/interface';
import { bytesToHex } from '@treecrdt/interface/ids';
import { deriveOpRefV0 } from '@treecrdt/sync-protocol';
import type { PendingOp, SyncPendingOpsStore } from '@treecrdt/sync-protocol';

type PendingProofMaterialHarness = {
  createPendingStore: (
    docId: string,
  ) => Promise<SyncPendingOpsStore<Operation>> | SyncPendingOpsStore<Operation>;
  close?: () => Promise<void> | void;
};

function makeReplica(fill: number): Uint8Array {
  return new Uint8Array(32).fill(fill);
}

function makeNodeHex(counter: number): string {
  return counter.toString(16).padStart(32, '0').slice(-32);
}

function makeInsertOp(replicaFill: number, counter: number): Operation {
  return {
    meta: {
      id: { replica: makeReplica(replicaFill), counter },
      lamport: counter,
    },
    kind: {
      type: 'insert',
      parent: '0'.repeat(32),
      node: makeNodeHex(counter),
      orderKey: new Uint8Array([counter & 0xff]),
    },
  };
}

function makePending(
  replicaFill: number,
  counter: number,
  sigFill: number,
  proofFill?: number,
  message?: string,
): PendingOp<Operation> {
  return {
    op: makeInsertOp(replicaFill, counter),
    auth: {
      sig: new Uint8Array(64).fill(sigFill),
      ...(proofFill === undefined ? {} : { proofRef: new Uint8Array(16).fill(proofFill) }),
    },
    reason: 'missing_context',
    ...(message ? { message } : {}),
  };
}

function normalizePending(value: PendingOp<Operation>) {
  const kind =
    value.op.kind.type === 'insert'
      ? {
          ...value.op.kind,
          orderKey: Array.from(value.op.kind.orderKey),
        }
      : value.op.kind;

  return {
    replicaHex: bytesToHex(value.op.meta.id.replica),
    counter: value.op.meta.id.counter,
    lamport: value.op.meta.lamport,
    kind,
    sigHex: bytesToHex(value.auth.sig),
    proofRefHex: value.auth.proofRef ? bytesToHex(value.auth.proofRef) : null,
    reason: value.reason,
    message: value.message ?? null,
  };
}

function sortPending(values: PendingOp<Operation>[]): ReturnType<typeof normalizePending>[] {
  return values
    .map(normalizePending)
    .sort((a, b) => a.counter - b.counter || a.replicaHex.localeCompare(b.replicaHex));
}

function sortHex(values: Uint8Array[]): string[] {
  return values.map((value) => bytesToHex(value)).sort();
}

export function definePendingProofMaterialStoreContract(
  label: string,
  createHarness: () => Promise<PendingProofMaterialHarness> | PendingProofMaterialHarness,
): void {
  test(`${label}: pending ops round-trip with auth sidecar`, async () => {
    const harness = await createHarness();
    try {
      const docId = `doc-pending-${randomUUID()}`;
      const pending = await harness.createPendingStore(docId);
      const first = makePending(7, 1, 9, 4, 'need ancestry');
      const second = makePending(7, 2, 8);

      await pending.init();
      await pending.storePendingOps([first, second]);

      expect(sortPending(await pending.listPendingOps())).toEqual(sortPending([first, second]));
      expect(sortHex(await pending.listPendingOpRefs())).toEqual(
        sortHex([
          deriveOpRefV0(docId, {
            replica: first.op.meta.id.replica,
            counter: BigInt(first.op.meta.id.counter),
          }),
          deriveOpRefV0(docId, {
            replica: second.op.meta.id.replica,
            counter: BigInt(second.op.meta.id.counter),
          }),
        ]),
      );
    } finally {
      await harness.close?.();
    }
  });

  test(`${label}: pending ops updates and deletes stay scoped to one doc`, async () => {
    const harness = await createHarness();
    try {
      const docA = `doc-pending-a-${randomUUID()}`;
      const docB = `doc-pending-b-${randomUUID()}`;
      const pendingA = await harness.createPendingStore(docA);
      const pendingB = await harness.createPendingStore(docB);
      const original = makePending(5, 1, 1, 2, 'old');
      const updated = makePending(5, 1, 3, 4, 'new');
      const otherDoc = makePending(5, 1, 7, 8, 'other-doc');

      await pendingA.init();
      await pendingB.init();
      await pendingA.storePendingOps([original]);
      await pendingA.storePendingOps([updated]);
      await pendingB.storePendingOps([otherDoc]);

      expect(sortPending(await pendingA.listPendingOps())).toEqual(sortPending([updated]));
      expect(sortPending(await pendingB.listPendingOps())).toEqual(sortPending([otherDoc]));

      await pendingA.deletePendingOps([updated.op, makeInsertOp(9, 99)]);

      expect(await pendingA.listPendingOps()).toEqual([]);
      expect(sortPending(await pendingB.listPendingOps())).toEqual(sortPending([otherDoc]));
    } finally {
      await harness.close?.();
    }
  });
}

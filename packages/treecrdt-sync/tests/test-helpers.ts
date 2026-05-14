import { createMaterializationDispatcher } from '@justthrowaway/interface/engine';
import type { Change } from '@justthrowaway/interface/engine';
import { bytesToHex } from '@justthrowaway/interface/ids';
import type { Operation, WriteOptions } from '@justthrowaway/interface';
import { deriveOpRefV0, type OpRef } from '@justthrowaway/sync-protocol';

import type { TreecrdtWebSocketSyncClient } from '../src/types.js';

export const ROOT = '0'.repeat(32);

export function orderKeyFromPosition(position: number): Uint8Array {
  if (!Number.isInteger(position) || position < 0) throw new Error(`invalid position: ${position}`);
  const n = position + 1;
  if (n > 0xffff) throw new Error(`position too large for u16 order key: ${position}`);
  const bytes = new Uint8Array(2);
  new DataView(bytes.buffer).setUint16(0, n, false);
  return bytes;
}

function maxLamportInMemory(ops: Operation[]): number {
  if (ops.length === 0) return 0;
  return ops.reduce((m, o) => Math.max(m, o.meta.lamport), 0);
}

function opToChanges(op: Operation): Change[] {
  const k = op.kind;
  if (k.type === 'insert') {
    return [
      {
        kind: 'insert',
        node: k.node,
        parentAfter: k.parent,
        payload: k.payload ?? null,
      },
    ];
  }
  if (k.type === 'move') {
    return [{ kind: 'move', node: k.node, parentBefore: null, parentAfter: k.newParent }];
  }
  if (k.type === 'payload') {
    return [{ kind: 'payload', node: k.node, payload: k.payload }];
  }
  if (k.type === 'delete') {
    return [{ kind: 'delete', node: k.node, parentBefore: null }];
  }
  if (k.type === 'tombstone') {
    return [{ kind: 'delete', node: k.node, parentBefore: null }];
  }
  return [];
}

export function createInMemoryTestClient(
  docId: string,
  initial: Operation[] = [],
): { client: TreecrdtWebSocketSyncClient; getOps: () => Promise<Operation[]> } {
  const store: { ops: Operation[] } = { ops: [...initial] };
  const refToOp = (ref: OpRef) => {
    const want = bytesToHex(ref);
    return store.ops.find(
      (o) =>
        want ===
        bytesToHex(
          deriveOpRefV0(docId, { replica: o.meta.id.replica, counter: o.meta.id.counter }),
        ),
    );
  };
  const client: TreecrdtWebSocketSyncClient = {
    docId,
    mode: 'memory',
    storage: 'memory',
    onMaterialized: () => () => {},
    meta: { headLamport: async () => maxLamportInMemory(store.ops) },
    runner: undefined,
    opRefs: {
      all: async () =>
        store.ops.map((o) =>
          deriveOpRefV0(docId, { replica: o.meta.id.replica, counter: o.meta.id.counter }),
        ),
      children: async (parent) =>
        store.ops
          .filter((o) => o.kind.type === 'insert' && o.kind.parent === parent)
          .map((o) =>
            deriveOpRefV0(docId, { replica: o.meta.id.replica, counter: o.meta.id.counter }),
          ),
    },
    ops: {
      get: async (refs) =>
        refs.map((r) => refToOp(r)).filter((o): o is Operation => o !== undefined),
      append: async (op) => {
        store.ops.push(op);
      },
      appendMany: async (appended) => {
        store.ops.push(...appended);
      },
      all: async () => store.ops,
      since: async () => [],
      children: async () => [],
    },
    tree: {
      children: async () => [],
      dump: async () => [],
      nodeCount: async () => 0,
      parent: async () => null,
      exists: async () => false,
      getPayload: async () => null,
    },
    local: {
      insert: async () => {
        throw new Error('not used');
      },
      move: async () => {
        throw new Error('not used');
      },
      delete: async () => {
        throw new Error('not used');
      },
      payload: async () => {
        throw new Error('not used');
      },
    },
    close: async () => {},
  } as unknown as TreecrdtWebSocketSyncClient;
  return { client, getOps: () => client.ops.all() };
}

/**
 * Mock engine: `append` / `appendMany` honor `WriteOptions.writeId` and emit
 * `MaterializationEvent` (including `writeIds`) the same way as the dispatcher used in wa-sqlite.
 */
export function createInMemoryTestClientWithWriteId(
  docId: string,
  initial: Operation[] = [],
): { client: TreecrdtWebSocketSyncClient; getOps: () => Promise<Operation[]> } {
  const store: { ops: Operation[] } = { ops: [...initial] };
  const { emitOutcome, onMaterialized } = createMaterializationDispatcher();
  let headSeq = 0;

  const refToOp = (ref: OpRef) => {
    const want = bytesToHex(ref);
    return store.ops.find(
      (o) =>
        want ===
        bytesToHex(
          deriveOpRefV0(docId, { replica: o.meta.id.replica, counter: o.meta.id.counter }),
        ),
    );
  };

  const appendAndEmit = (appended: Operation[], writeOpts?: WriteOptions) => {
    if (appended.length === 0) return;
    const changes = appended.flatMap((op) => opToChanges(op));
    if (changes.length === 0) return;
    headSeq += 1;
    emitOutcome({ headSeq, changes }, writeOpts?.writeId);
  };

  const client: TreecrdtWebSocketSyncClient = {
    docId,
    mode: 'memory',
    storage: 'memory',
    onMaterialized,
    meta: { headLamport: async () => maxLamportInMemory(store.ops) },
    runner: undefined,
    opRefs: {
      all: async () =>
        store.ops.map((o) =>
          deriveOpRefV0(docId, { replica: o.meta.id.replica, counter: o.meta.id.counter }),
        ),
      children: async (parent) =>
        store.ops
          .filter((o) => o.kind.type === 'insert' && o.kind.parent === parent)
          .map((o) =>
            deriveOpRefV0(docId, { replica: o.meta.id.replica, counter: o.meta.id.counter }),
          ),
    },
    ops: {
      get: async (refs) =>
        refs.map((r) => refToOp(r)).filter((o): o is Operation => o !== undefined),
      append: async (op, writeOpts) => {
        store.ops.push(op);
        appendAndEmit([op], writeOpts);
      },
      appendMany: async (appended, writeOpts) => {
        store.ops.push(...appended);
        appendAndEmit(appended, writeOpts);
      },
      all: async () => store.ops,
      since: async () => [],
      children: async () => [],
    },
    tree: {
      children: async () => [],
      dump: async () => [],
      nodeCount: async () => 0,
      parent: async () => null,
      exists: async () => false,
      getPayload: async () => null,
    },
    local: {
      insert: async () => {
        throw new Error('not used');
      },
      move: async () => {
        throw new Error('not used');
      },
      delete: async () => {
        throw new Error('not used');
      },
      payload: async () => {
        throw new Error('not used');
      },
    },
    close: async () => {},
  } as unknown as TreecrdtWebSocketSyncClient;
  return { client, getOps: () => client.ops.all() };
}

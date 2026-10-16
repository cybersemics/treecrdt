import type { Operation } from '@treecrdt/interface';
import { bytesToHex } from '@treecrdt/interface/ids';
import type { OpRef, SyncBackend } from './types.js';

/** Operation access shared by synchronous and asynchronous TreeCRDT clients. */
export type TreecrdtSyncBackendClient = {
  docId?: string;
  meta?: {
    headLamport?: () => number | Promise<number>;
  };
  opRefs: {
    all: () => OpRef[] | Promise<OpRef[]>;
    children?: (parentHex: string) => OpRef[] | Promise<OpRef[]>;
  };
  ops: {
    all?: () => Operation[] | Promise<Operation[]>;
    get: (opRefs: OpRef[]) => Operation[] | Promise<Operation[]>;
    appendMany: (ops: Operation[]) => void | Promise<unknown>;
  };
};

export function createTreecrdtSyncBackendFromClient(
  client: TreecrdtSyncBackendClient,
  docId: string,
  opts: { maxLamport?: () => bigint | Promise<bigint> } = {},
): SyncBackend<Operation> {
  if (client.docId !== undefined && client.docId !== docId) {
    throw new Error('Sync document id does not match client.docId');
  }

  return {
    docId,
    maxLamport: async () => {
      if (opts.maxLamport) return opts.maxLamport();
      if (client.meta?.headLamport) return BigInt(await client.meta.headLamport());
      if (client.ops.all) {
        const ops = await client.ops.all();
        return BigInt(ops.reduce((max, op) => Math.max(max, op.meta.lamport), 0));
      }
      throw new Error('maxLamport: missing client.meta.headLamport and client.ops.all');
    },
    listOpRefs: async (filter) => {
      if ('all' in filter) return client.opRefs.all();
      if (!client.opRefs.children) {
        throw new Error('This client supports only full-document filters');
      }
      return client.opRefs.children(bytesToHex(filter.children.parent));
    },
    getOpsByOpRefs: async (refs) => client.ops.get(refs),
    applyOps: async (ops) => {
      if (ops.length > 0) await client.ops.appendMany(ops);
    },
  };
}

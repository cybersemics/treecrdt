import type { Operation } from '@treecrdt/interface';
import { bytesToHex, hexToBytes } from '@treecrdt/interface/ids';
import { deriveOpRefV0, type SyncBackend } from '@treecrdt/sync-protocol';
import type { MemoryClient } from './memory-client.js';

/** Adapts a memory client to full-document synchronization without retaining a second operation log. */
export function createMemorySyncBackend(
  client: MemoryClient,
  { docId }: { docId: string },
): SyncBackend<Operation> {
  const indices = new Map<string, number>();
  let indexed = 0;
  const refresh = () => {
    const operations = client.operationsFrom(indexed);
    for (const operation of operations) {
      indices.set(bytesToHex(deriveOpRefV0(docId, operation.meta.id)), indexed++);
    }
  };
  return {
    docId,
    maxLamport: async () => BigInt(client.maxLamport()),
    listOpRefs: async (filter) => {
      if (!('all' in filter)) throw new Error('Memory sync supports only full-document filters');
      refresh();
      return [...indices.keys()].map(hexToBytes);
    },
    getOpsByOpRefs: async (refs) => {
      refresh();
      return client.operationsAt(
        refs.map((ref) => {
          const index = indices.get(bytesToHex(ref));
          if (index === undefined) throw new Error(`Missing memory operation ${bytesToHex(ref)}`);
          return index;
        }),
      );
    },
    applyOps: async (operations) => {
      client.appendOperations(operations);
    },
  };
}

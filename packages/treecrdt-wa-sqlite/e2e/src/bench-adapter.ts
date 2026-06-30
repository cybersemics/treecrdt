import { createTreecrdtClient, type TreecrdtClient } from '@treecrdt/wa-sqlite';
import type { TreecrdtAdapter } from '@treecrdt/interface';
import { emptyMaterializationOutcome } from '@treecrdt/interface/engine';
import { bytesToHex } from '@treecrdt/interface/ids';

export type StorageKind =
  | 'browser-opfs-coop-sync'
  | 'browser-opfs-shared-worker'
  | 'browser-opfs-single-owner-wal'
  | 'browser-opfs-write-ahead'
  | 'browser-memory';

export async function createWaSqliteBenchAdapter(
  storage: StorageKind,
  baseUrl?: string,
): Promise<TreecrdtAdapter & { close: () => Promise<void> }> {
  const isOpfs =
    storage === 'browser-opfs-coop-sync' ||
    storage === 'browser-opfs-shared-worker' ||
    storage === 'browser-opfs-single-owner-wal' ||
    storage === 'browser-opfs-write-ahead';
  const isSharedWorker = storage === 'browser-opfs-shared-worker';
  const clientStorage = isOpfs ? 'opfs' : 'memory';
  let client: TreecrdtClient | null = null;
  const effectiveBase =
    baseUrl ?? (typeof location !== 'undefined' ? new URL('/', location.href).href : '/');
  const filename = clientStorage === 'opfs' ? `/bench-${crypto.randomUUID()}.db` : undefined;
  const docId = `bench-${crypto.randomUUID()}`;
  const runtime = isSharedWorker ? 'shared-worker' : isOpfs ? 'dedicated-worker' : 'direct';

  try {
    console.info(
      `[wa-sqlite-bench] creating client storage=${clientStorage} runtime=${runtime} base=${effectiveBase}`,
    );
    client = await createTreecrdtClient({
      storage: isOpfs
        ? {
            type: 'opfs',
            filename,
            fallback: 'throw',
            writeMode:
              storage === 'browser-opfs-single-owner-wal'
                ? 'single-owner-wal'
                : storage === 'browser-opfs-write-ahead'
                  ? 'opfs-write-ahead'
                  : 'default',
          }
        : { type: 'memory' },
      runtime: { type: runtime },
      assets: { baseUrl: effectiveBase },
      docId,
    });
    // sanity check to ensure DB is valid
    await client.ops.all();
  } catch (err) {
    if (client?.close) {
      await client.close();
    }
    const reason = err instanceof Error ? err.message : String(err);
    console.error(
      `createWaSqliteBenchAdapter failed (${clientStorage}/${runtime}) base=${effectiveBase}:`,
      err,
    );
    throw new Error(
      JSON.stringify({
        where: 'createWaSqliteBenchAdapter',
        storage: clientStorage,
        runtime,
        base: effectiveBase,
        message: reason,
      }),
    );
  }

  return {
    setDocId: async (nextDocId) => {
      if (nextDocId !== client.docId) {
        throw new Error(
          `docId is fixed at client creation (expected ${client.docId}, got ${nextDocId})`,
        );
      }
    },
    docId: async () => client.docId,
    opRefsAll: async () => client.opRefs.all(),
    opRefsChildren: async (parent) => client.opRefs.children(bytesToHex(parent)),
    opsByOpRefs: async (opRefs) => client.ops.get(opRefs),
    treeChildren: async (parent) => client.tree.children(bytesToHex(parent)),
    treeDump: async () => client.tree.dump(),
    treeNodeCount: async () => client.tree.nodeCount(),
    headLamport: async () => client.meta.headLamport(),
    replicaMaxCounter: async (replica) => client.meta.replicaMaxCounter(replica),
    appendOp: async (op, serializeNodeId, serializeReplica) => {
      await client.ops.append({
        ...op,
        meta: {
          ...op.meta,
          id: {
            replica: serializeReplica(op.meta.id.replica),
            counter: op.meta.id.counter,
          },
        },
      });
      return emptyMaterializationOutcome();
    },
    appendOps: async (ops, serializeNodeId, serializeReplica) => {
      await client.ops.appendMany(
        ops.map((op) => ({
          ...op,
          meta: {
            ...op.meta,
            id: { replica: serializeReplica(op.meta.id.replica), counter: op.meta.id.counter },
          },
        })),
      );
      return emptyMaterializationOutcome();
    },
    opsSince: async (lamport, root) => client.ops.since(lamport, root),
    close: async () => client.close(),
  };
}

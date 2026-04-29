import type { Operation, ReplicaId } from '@treecrdt/interface';
import { nodeIdFromBytes16, nodeIdToBytes16, replicaIdToBytes } from '@treecrdt/interface/ids';
import { createMaterializationDispatcher } from '@treecrdt/interface/engine';
import type { LocalWriteOptions, TreecrdtEngine, WriteOptions } from '@treecrdt/interface/engine';
import type { TreecrdtSqlitePlacement } from '@treecrdt/interface/sqlite';

import {
  nativeToMaterializationOutcome,
  nativeToOperation,
  operationToNativeWithSerializers,
} from './codec.js';
import {
  loadNative,
  type NativeLocalOpResult,
  type NativeMaterializationOutcome,
  type NativePreparedLocalOpTx,
} from './native.js';

function ensureNonEmptyString(name: string, value: string): void {
  if (typeof value !== 'string' || value.length === 0) {
    throw new Error(`${name} must be a non-empty string`);
  }
}

function bigintToSafeNumber(name: string, value: bigint): number {
  if (value < 0n || value > BigInt(Number.MAX_SAFE_INTEGER)) {
    throw new Error(`${name} out of JS safe range: ${String(value)}`);
  }
  return Number(value);
}

function placementToArgs(placement: TreecrdtSqlitePlacement): {
  type: string;
  after: Uint8Array | null;
} {
  if (placement.type === 'first') return { type: 'first', after: null };
  if (placement.type === 'last') return { type: 'last', after: null };
  return { type: 'after', after: nodeIdToBytes16(placement.after) };
}

/**
 * Postgres-backed TreeCRDT client (Node-only), implemented via the Rust N-API adapter.
 *
 * This mirrors the high-level `createTreecrdtClient` shape from `@treecrdt/sqlite-node` so apps
 * can swap storage backends with minimal changes.
 */
export async function createTreecrdtPostgresClient(
  url: string,
  opts: { docId?: string } = {},
): Promise<TreecrdtEngine> {
  ensureNonEmptyString('url', url);
  const native = loadNative();
  const factory = new native.PgFactory(url);
  await factory.ensureSchema();

  const docId = opts.docId ?? 'treecrdt';
  ensureNonEmptyString('docId', docId);

  const backend = factory.open(docId);
  const materialized = createMaterializationDispatcher();
  const emitNativeOutcome = (nativeOutcome: NativeMaterializationOutcome) => {
    materialized.emitOutcome(nativeToMaterializationOutcome(nativeOutcome));
  };
  const finishLocalOp = (result: NativeLocalOpResult): Operation => {
    emitNativeOutcome(result.outcome);
    return nativeToOperation(result.op);
  };
  const finishPreparedLocalOp = async (
    tx: NativePreparedLocalOpTx,
    writeOpts?: LocalWriteOptions,
  ): Promise<Operation> => {
    if (writeOpts?.authSession) {
      const op = nativeToOperation(tx.op());
      try {
        await writeOpts.authSession.authorizeLocalOps([op]);
      } catch (err) {
        try {
          tx.rollback();
        } catch {
          // Preserve the auth failure. The native transaction also rolls back on drop.
        }
        throw err;
      }
    }
    return finishLocalOp(tx.commit());
  };
  const ensureMaterializedImpl = () => {
    emitNativeOutcome(backend.ensureMaterialized());
  };

  const encodeReplica = (replica: Operation['meta']['id']['replica']): Uint8Array =>
    replicaIdToBytes(replica);

  const opsSinceImpl = async (lamport: number, root?: string): Promise<Operation[]> => {
    const rootBytes = root === undefined ? null : nodeIdToBytes16(root);
    return backend.opsSince(BigInt(lamport), rootBytes).map(nativeToOperation);
  };

  const opRefsAllImpl = async () => backend.listOpRefsAll();
  const opRefsChildrenImpl = async (parent: string) => {
    ensureMaterializedImpl();
    return backend.listOpRefsChildren(nodeIdToBytes16(parent));
  };

  const opsByOpRefsImpl = async (opRefs: Uint8Array[]) => {
    if (opRefs.length === 0) return [];
    return backend.getOpsByOpRefs(opRefs).map(nativeToOperation);
  };

  const treeChildrenImpl = async (parent: string) => {
    ensureMaterializedImpl();
    return backend.treeChildren(nodeIdToBytes16(parent)).map((b) => nodeIdFromBytes16(b));
  };

  const treeChildrenPageImpl = async (
    parent: string,
    cursor: { orderKey: Uint8Array; node: Uint8Array } | null,
    limit: number,
  ) => {
    ensureMaterializedImpl();
    const rows = backend.treeChildrenPage(
      nodeIdToBytes16(parent),
      cursor?.orderKey ?? null,
      cursor?.node ?? null,
      limit,
    );
    return rows.map((r) => ({ node: nodeIdFromBytes16(r.node), orderKey: r.orderKey ?? null }));
  };

  const treeDumpImpl = async () => {
    ensureMaterializedImpl();
    const rows = backend.treeDump();
    return rows.map((r) => ({
      node: nodeIdFromBytes16(r.node),
      parent: r.parent ? nodeIdFromBytes16(r.parent) : null,
      orderKey: r.orderKey ?? null,
      tombstone: Boolean(r.tombstone),
    }));
  };

  const treeNodeCountImpl = async () => {
    ensureMaterializedImpl();
    return bigintToSafeNumber('treeNodeCount', backend.treeNodeCount());
  };
  const treeParentImpl = async (node: string) => {
    ensureMaterializedImpl();
    const result = backend.treeParent(nodeIdToBytes16(node));
    return result === null || result === undefined ? null : nodeIdFromBytes16(result);
  };
  const treeExistsImpl = async (node: string) => {
    ensureMaterializedImpl();
    return backend.treeExists(nodeIdToBytes16(node));
  };
  const treeGetPayloadImpl = async (node: string) => {
    ensureMaterializedImpl();
    const result = backend.treePayload(nodeIdToBytes16(node));
    return result === null || result === undefined ? null : result;
  };
  const headLamportImpl = async () => bigintToSafeNumber('headLamport', backend.maxLamport());
  const replicaMaxCounterImpl = async (replica: ReplicaId) =>
    bigintToSafeNumber('replicaMaxCounter', backend.replicaMaxCounter(encodeReplica(replica)));

  const localInsertImpl = async (
    replica: ReplicaId,
    parent: string,
    node: string,
    placement: TreecrdtSqlitePlacement,
    payload: Uint8Array | null,
    writeOpts?: LocalWriteOptions,
  ) => {
    const { type, after } = placementToArgs(placement);
    const tx = backend.prepareLocalInsert(
      encodeReplica(replica),
      nodeIdToBytes16(parent),
      nodeIdToBytes16(node),
      type,
      after,
      payload,
    );
    return finishPreparedLocalOp(tx, writeOpts);
  };

  const localMoveImpl = async (
    replica: ReplicaId,
    node: string,
    newParent: string,
    placement: TreecrdtSqlitePlacement,
    writeOpts?: LocalWriteOptions,
  ) => {
    const { type, after } = placementToArgs(placement);
    const tx = backend.prepareLocalMove(
      encodeReplica(replica),
      nodeIdToBytes16(node),
      nodeIdToBytes16(newParent),
      type,
      after,
    );
    return finishPreparedLocalOp(tx, writeOpts);
  };

  const localDeleteImpl = async (
    replica: ReplicaId,
    node: string,
    writeOpts?: LocalWriteOptions,
  ) => {
    const tx = backend.prepareLocalDelete(encodeReplica(replica), nodeIdToBytes16(node));
    return finishPreparedLocalOp(tx, writeOpts);
  };

  const localPayloadImpl = async (
    replica: ReplicaId,
    node: string,
    payload: Uint8Array | null,
    writeOpts?: LocalWriteOptions,
  ) => {
    const tx = backend.prepareLocalPayload(encodeReplica(replica), nodeIdToBytes16(node), payload);
    return finishPreparedLocalOp(tx, writeOpts);
  };

  return {
    mode: 'node',
    storage: 'postgres',
    docId,
    ops: {
      append: async (op, writeOpts?: WriteOptions) => {
        const outcome = nativeToMaterializationOutcome(
          backend.applyOps([operationToNativeWithSerializers(op, nodeIdToBytes16, encodeReplica)]),
        );
        materialized.emitOutcome(outcome, writeOpts?.writeId);
      },
      appendMany: async (ops, writeOpts?: WriteOptions) => {
        if (ops.length === 0) return;
        const outcome = nativeToMaterializationOutcome(
          backend.applyOps(
            ops.map((op) => operationToNativeWithSerializers(op, nodeIdToBytes16, encodeReplica)),
          ),
        );
        materialized.emitOutcome(outcome, writeOpts?.writeId);
      },
      all: () => opsSinceImpl(0),
      since: opsSinceImpl,
      children: async (parent) => opsByOpRefsImpl(await opRefsChildrenImpl(parent)),
      get: opsByOpRefsImpl,
    },
    opRefs: {
      all: opRefsAllImpl,
      children: opRefsChildrenImpl,
    },
    tree: {
      children: treeChildrenImpl,
      childrenPage: treeChildrenPageImpl,
      dump: treeDumpImpl,
      nodeCount: treeNodeCountImpl,
      parent: treeParentImpl,
      exists: treeExistsImpl,
      getPayload: treeGetPayloadImpl,
    },
    meta: {
      headLamport: headLamportImpl,
      replicaMaxCounter: replicaMaxCounterImpl,
    },
    local: {
      insert: localInsertImpl,
      move: localMoveImpl,
      delete: localDeleteImpl,
      payload: localPayloadImpl,
    },
    onMaterialized: materialized.onMaterialized,
    close: async () => {
      // no-op: native layer opens per-call connections
    },
  };
}

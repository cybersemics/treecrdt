import type { Operation, ReplicaId } from '@treecrdt/interface';
import { nodeIdFromBytes16, nodeIdToBytes16, replicaIdToBytes } from '@treecrdt/interface/ids';
import {
  createAuthorizedLocalWriter,
  createMaterializationDispatcher,
  createTreecrdtEngineLocal,
  createTreecrdtTreeNodes,
} from '@treecrdt/interface/engine';
import type { LocalWriteOptions, TreecrdtEngine, WriteOptions } from '@treecrdt/interface/engine';
import type { TreecrdtSqlitePlacement } from '@treecrdt/interface/sqlite';

import {
  nativeToMaterializationOutcome,
  nativeToOperation,
  operationToNativeWithSerializers,
} from './codec.js';
import {
  loadNative,
  type NativeLocalOpCommit,
  type NativeLocalOpResult,
  type NativeMaterializationOutcome,
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
  const emitNativeOutcome = (nativeOutcome: NativeMaterializationOutcome, writeId?: string) => {
    materialized.emitOutcome(nativeToMaterializationOutcome(nativeOutcome), writeId);
  };
  const finishLocalOp = (result: NativeLocalOpResult, writeId?: string): Operation => {
    emitNativeOutcome(result.outcome, writeId);
    return nativeToOperation(result.op);
  };
  // Backend calls are synchronous: with a transaction held across `authorizeLocalOps`, any other
  // write to the doc in this process would block the event loop, so the authorization could never
  // finish. Auth-checked writes therefore use `local*IfUnchanged`, which end their transaction
  // before returning.
  const writeAuthorized = createAuthorizedLocalWriter();
  const writeLocalOp = async <Args extends unknown[]>(
    args: [...Args],
    write: (...args: Args) => NativeLocalOpResult,
    writeIfUnchanged: (token: string | null, ...args: Args) => NativeLocalOpCommit,
    writeOpts?: LocalWriteOptions,
  ): Promise<Operation> => {
    const authSession = writeOpts?.authSession;
    if (!authSession) return finishLocalOp(write(...args), writeOpts?.writeId);

    // Copy caller-owned bytes so that mutating them before the write lands cannot change the op.
    const copied = args.map((arg) => (arg instanceof Uint8Array ? Uint8Array.from(arg) : arg));
    return writeAuthorized((token) => {
      const result = writeIfUnchanged(token, ...(copied as Args));
      if (result.outcome) emitNativeOutcome(result.outcome, writeOpts?.writeId);
      return { op: nativeToOperation(result.op), token: result.token ?? undefined };
    }, authSession);
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

  const treeChildrenImpl = async (parent: string, offset: number | null, limit: number | null) => {
    ensureMaterializedImpl();
    return backend
      .treeChildren(nodeIdToBytes16(parent), offset, limit)
      .map((b) => nodeIdFromBytes16(b));
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
    return writeLocalOp(
      [
        encodeReplica(replica),
        nodeIdToBytes16(parent),
        nodeIdToBytes16(node),
        type,
        after,
        payload,
      ],
      (...args) => backend.localInsert(...args),
      (token, ...args) => backend.localInsertIfUnchanged(...args, token),
      writeOpts,
    );
  };

  const localMoveImpl = async (
    replica: ReplicaId,
    node: string,
    newParent: string,
    placement: TreecrdtSqlitePlacement,
    writeOpts?: LocalWriteOptions,
  ) => {
    const { type, after } = placementToArgs(placement);
    return writeLocalOp(
      [encodeReplica(replica), nodeIdToBytes16(node), nodeIdToBytes16(newParent), type, after],
      (...args) => backend.localMove(...args),
      (token, ...args) => backend.localMoveIfUnchanged(...args, token),
      writeOpts,
    );
  };

  const localDeleteImpl = async (
    replica: ReplicaId,
    node: string,
    writeOpts?: LocalWriteOptions,
  ) => {
    return writeLocalOp(
      [encodeReplica(replica), nodeIdToBytes16(node)],
      (...args) => backend.localDelete(...args),
      (token, ...args) => backend.localDeleteIfUnchanged(...args, token),
      writeOpts,
    );
  };

  const localPayloadImpl = async (
    replica: ReplicaId,
    node: string,
    payload: Uint8Array | null,
    writeOpts?: LocalWriteOptions,
  ) => {
    return writeLocalOp(
      [encodeReplica(replica), nodeIdToBytes16(node), payload],
      (...args) => backend.localPayload(...args),
      (token, ...args) => backend.localPayloadIfUnchanged(...args, token),
      writeOpts,
    );
  };

  const local = createTreecrdtEngineLocal({
    insert: localInsertImpl,
    move: localMoveImpl,
    delete: localDeleteImpl,
    payload: localPayloadImpl,
  });

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
      ...createTreecrdtTreeNodes({
        exists: treeExistsImpl,
        parent: treeParentImpl,
        payload: treeGetPayloadImpl,
        children: treeChildrenImpl,
      }),
      dump: treeDumpImpl,
      nodeCount: treeNodeCountImpl,
    },
    meta: {
      headLamport: headLamportImpl,
      replicaMaxCounter: replicaMaxCounterImpl,
    },
    local,
    onMaterialized: materialized.onMaterialized,
    close: async () => {
      // no-op: native layer opens per-call connections
    },
  };
}

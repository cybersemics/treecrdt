import type { Operation, ReplicaId } from "@treecrdt/interface";
import { nodeIdFromBytes16, nodeIdToBytes16, replicaIdToBytes } from "@treecrdt/interface/ids";
import type { TreecrdtEngine } from "@treecrdt/interface/engine";
import type { TreecrdtSqlitePlacement } from "@treecrdt/interface/sqlite";

import { nativeToOperation, operationToNativeWithSerializers } from "./codec.js";
import { loadNative } from "./native.js";

function ensureNonEmptyString(name: string, value: string): void {
  if (typeof value !== "string" || value.length === 0) {
    throw new Error(`${name} must be a non-empty string`);
  }
}

function bigintToSafeNumber(name: string, value: bigint): number {
  if (value < 0n || value > BigInt(Number.MAX_SAFE_INTEGER)) {
    throw new Error(`${name} out of JS safe range: ${String(value)}`);
  }
  return Number(value);
}

function placementToArgs(placement: TreecrdtSqlitePlacement): { type: string; after: Uint8Array | null } {
  if (placement.type === "first") return { type: "first", after: null };
  if (placement.type === "last") return { type: "last", after: null };
  return { type: "after", after: nodeIdToBytes16(placement.after) };
}

/**
 * Postgres-backed TreeCRDT client (Node-only), implemented via the Rust N-API adapter.
 *
 * This mirrors the high-level `createTreecrdtClient` shape from `@treecrdt/sqlite-node` so apps
 * can swap storage backends with minimal changes.
 */
export async function createTreecrdtPostgresClient(
  url: string,
  opts: { docId?: string } = {}
): Promise<TreecrdtEngine> {
  ensureNonEmptyString("url", url);
  const native = loadNative();
  const factory = new native.PgFactory(url);
  await factory.ensureSchema();

  const docId = opts.docId ?? "treecrdt";
  ensureNonEmptyString("docId", docId);

  const backend = factory.open(docId);

  const encodeReplica = (replica: Operation["meta"]["id"]["replica"]): Uint8Array => replicaIdToBytes(replica);

  const opsSinceImpl = async (lamport: number, root?: string): Promise<Operation[]> => {
    const rootBytes = root === undefined ? null : nodeIdToBytes16(root);
    return backend.opsSince(BigInt(lamport), rootBytes).map(nativeToOperation);
  };

  const opRefsAllImpl = async () => backend.listOpRefsAll();
  const opRefsChildrenImpl = async (parent: string) => backend.listOpRefsChildren(nodeIdToBytes16(parent));

  const opsByOpRefsImpl = async (opRefs: Uint8Array[]) => {
    if (opRefs.length === 0) return [];
    return backend.getOpsByOpRefs(opRefs).map(nativeToOperation);
  };

  const treeChildrenImpl = async (parent: string) =>
    backend.treeChildren(nodeIdToBytes16(parent)).map((b) => nodeIdFromBytes16(b));

  const treeChildrenPageImpl = async (
    parent: string,
    cursor: { orderKey: Uint8Array; node: Uint8Array } | null,
    limit: number
  ) => {
    const rows = backend.treeChildrenPage(
      nodeIdToBytes16(parent),
      cursor?.orderKey ?? null,
      cursor?.node ?? null,
      limit
    );
    return rows.map((r) => ({ node: nodeIdFromBytes16(r.node), orderKey: r.orderKey ?? null }));
  };

  const treeDumpImpl = async () => {
    const rows = backend.treeDump();
    return rows.map((r) => ({
      node: nodeIdFromBytes16(r.node),
      parent: r.parent ? nodeIdFromBytes16(r.parent) : null,
      orderKey: r.orderKey ?? null,
      tombstone: Boolean(r.tombstone),
    }));
  };

  const treeNodeCountImpl = async () => bigintToSafeNumber("treeNodeCount", backend.treeNodeCount());
  const headLamportImpl = async () => bigintToSafeNumber("headLamport", backend.maxLamport());
  const replicaMaxCounterImpl = async (replica: ReplicaId) =>
    bigintToSafeNumber("replicaMaxCounter", backend.replicaMaxCounter(encodeReplica(replica)));

  const localInsertImpl = async (
    replica: ReplicaId,
    parent: string,
    node: string,
    placement: TreecrdtSqlitePlacement,
    payload: Uint8Array | null
  ) => {
    const { type, after } = placementToArgs(placement);
    const op = backend.localInsert(
      encodeReplica(replica),
      nodeIdToBytes16(parent),
      nodeIdToBytes16(node),
      type,
      after,
      payload
    );
    return nativeToOperation(op);
  };

  const localMoveImpl = async (
    replica: ReplicaId,
    node: string,
    newParent: string,
    placement: TreecrdtSqlitePlacement
  ) => {
    const { type, after } = placementToArgs(placement);
    const op = backend.localMove(
      encodeReplica(replica),
      nodeIdToBytes16(node),
      nodeIdToBytes16(newParent),
      type,
      after
    );
    return nativeToOperation(op);
  };

  const localDeleteImpl = async (replica: ReplicaId, node: string) => {
    const op = backend.localDelete(encodeReplica(replica), nodeIdToBytes16(node));
    return nativeToOperation(op);
  };

  const localPayloadImpl = async (replica: ReplicaId, node: string, payload: Uint8Array | null) => {
    const op = backend.localPayload(encodeReplica(replica), nodeIdToBytes16(node), payload);
    return nativeToOperation(op);
  };

  return {
    mode: "node",
    storage: "postgres",
    docId,
    ops: {
      append: async (op) => {
        backend.applyOps([operationToNativeWithSerializers(op, nodeIdToBytes16, encodeReplica)]);
      },
      appendMany: async (ops) => {
        if (ops.length === 0) return;
        backend.applyOps(ops.map((op) => operationToNativeWithSerializers(op, nodeIdToBytes16, encodeReplica)));
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
    close: async () => {
      // no-op: native layer opens per-call connections
    },
  };
}


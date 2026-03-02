import type { Operation, SerializeNodeId, SerializeReplica, TreecrdtAdapter } from "@treecrdt/interface";
import { nodeIdToBytes16 } from "@treecrdt/interface/ids";

import { nativeOpToSqliteRow, operationToNativeWithSerializers } from "./codec.js";
import { loadNative } from "./native.js";

export type PostgresNapiAdapterFactory = {
  ensureSchema: () => Promise<void>;
  resetForTests: () => Promise<void>;
  resetDocForTests: (docId: string) => Promise<void>;
  open: (docId: string) => Promise<TreecrdtAdapter>;
};

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

function opToNative(op: Operation, serializeNodeId: SerializeNodeId, serializeReplica: SerializeReplica) {
  return operationToNativeWithSerializers(op, serializeNodeId, serializeReplica);
}

export function createPostgresNapiAdapterFactory(url: string): PostgresNapiAdapterFactory {
  ensureNonEmptyString("url", url);
  const native = loadNative();
  const factory = new native.PgFactory(url);

  return {
    ensureSchema: async () => factory.ensureSchema(),
    resetForTests: async () => factory.resetForTests(),
    resetDocForTests: async (docId: string) => {
      ensureNonEmptyString("docId", docId);
      factory.resetDocForTests(docId);
    },
    open: async (initialDocId: string) => {
      ensureNonEmptyString("docId", initialDocId);
      let docId = initialDocId;
      let backend = factory.open(docId);

      const adapter: TreecrdtAdapter = {
        setDocId: async (next) => {
          ensureNonEmptyString("docId", next);
          docId = next;
          backend = factory.open(docId);
        },
        docId: async () => docId,
        opRefsAll: async () => backend.listOpRefsAll(),
        opRefsChildren: async (parent) => backend.listOpRefsChildren(parent),
        opsByOpRefs: async (opRefs) => backend.getOpsByOpRefs(opRefs).map(nativeOpToSqliteRow),
        treeChildren: async (parent) => backend.treeChildren(parent),
        treeChildrenPage: async (parent, cursor, limit) => {
          const rows = backend.treeChildrenPage(parent, cursor?.orderKey ?? null, cursor?.node ?? null, limit);
          return rows.map((r) => ({ node: r.node, order_key: r.orderKey ?? null }));
        },
        treeDump: async () => {
          const rows = backend.treeDump();
          return rows.map((r) => ({
            node: r.node,
            parent: r.parent ?? null,
            order_key: r.orderKey ?? null,
            tombstone: r.tombstone,
          }));
        },
        treeNodeCount: async () => bigintToSafeNumber("treeNodeCount", backend.treeNodeCount()),
        headLamport: async () => bigintToSafeNumber("headLamport", backend.maxLamport()),
        replicaMaxCounter: async (replica) =>
          bigintToSafeNumber("replicaMaxCounter", backend.replicaMaxCounter(replica)),
        appendOp: async (op, serializeNodeId, serializeReplica) => {
          backend.applyOps([opToNative(op, serializeNodeId, serializeReplica)]);
        },
        appendOps: async (ops, serializeNodeId, serializeReplica) => {
          if (ops.length === 0) return;
          backend.applyOps(ops.map((op) => opToNative(op, serializeNodeId, serializeReplica)));
        },
        opsSince: async (lamport, root) => {
          const rootBytes = root === undefined ? null : nodeIdToBytes16(root);
          const rows = backend.opsSince(BigInt(lamport), rootBytes);
          return rows.map(nativeOpToSqliteRow);
        },
        close: async () => {
          // no-op: native layer opens per-call connections
        },
      };

      return adapter;
    },
  };
}

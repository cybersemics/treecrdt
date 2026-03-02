import type { Operation } from "@treecrdt/interface";
import type { Filter, OpRef, SyncBackend } from "@treecrdt/sync";

import { nativeToOperation, operationToNative } from "./codec.js";
import { loadNative } from "./native.js";

export { createPostgresNapiAdapterFactory, type PostgresNapiAdapterFactory } from "./adapter.js";
export { createTreecrdtPostgresClient } from "./client.js";

export type PostgresNapiSyncBackendFactory = {
  ensureSchema: () => Promise<void>;
  resetForTests: () => Promise<void>;
  resetDocForTests: (docId: string) => Promise<void>;
  open: (docId: string) => Promise<SyncBackend<Operation>>;
};

function ensureNonEmptyString(name: string, value: string): void {
  if (typeof value !== "string" || value.length === 0) {
    throw new Error(`${name} must be a non-empty string`);
  }
}

export function createPostgresNapiSyncBackendFactory(url: string): PostgresNapiSyncBackendFactory {
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
    open: async (docId: string) => {
      ensureNonEmptyString("docId", docId);
      const nativeBackend = factory.open(docId);

      const backend: SyncBackend<Operation> = {
        docId,
        maxLamport: async () => nativeBackend.maxLamport(),
        listOpRefs: async (filter: Filter) => {
          if ("all" in filter) return nativeBackend.listOpRefsAll();
          const parent = Buffer.from(filter.children.parent);
          return nativeBackend.listOpRefsChildren(parent);
        },
        getOpsByOpRefs: async (opRefs: OpRef[]) => {
          if (opRefs.length === 0) return [];
          const rows = nativeBackend.getOpsByOpRefs(opRefs.map((r) => Buffer.from(r)));
          return rows.map(nativeToOperation);
        },
        applyOps: async (ops: Operation[]) => {
          if (ops.length === 0) return;
          nativeBackend.applyOps(ops.map(operationToNative));
        },
      };

      return backend;
    },
  };
}

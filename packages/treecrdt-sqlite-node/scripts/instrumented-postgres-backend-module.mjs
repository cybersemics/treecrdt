import { createPostgresNapiSyncBackendFactory as createBaseFactory } from "@treecrdt/postgres-napi";

import { wrapBackendWithProfiler } from "./backend-profiler.mjs";

export function createPostgresNapiSyncBackendFactory(url) {
  const baseFactory = createBaseFactory(url);

  return {
    ...baseFactory,
    open: async (docId) => {
      const backend = await baseFactory.open(docId);
      return wrapBackendWithProfiler(backend, { docId, label: "server-postgres" });
    },
  };
}

import type { Operation } from '@justthrowaway/interface';
import type { SqliteRunner } from '@justthrowaway/interface/sqlite';
import type { PendingOp } from '@justthrowaway/sync-protocol';

import { createPendingOpsStore } from './proof-material/index.js';

export type TreecrdtSqliteSyncDiagnostics = {
  /**
   * Lists ops that sync has persisted for retry after missing auth/subtree context is resolved.
   *
   * This is diagnostics/sync state, not part of the auth session inputs.
   */
  listPendingOps: () => Promise<Array<PendingOp<Operation>>>;
};

export function createTreecrdtSqliteSyncDiagnostics(opts: {
  runner: SqliteRunner;
  docId: string;
  nowMs?: () => number;
}): TreecrdtSqliteSyncDiagnostics {
  const pendingOpsStore = createPendingOpsStore(opts);
  let ready = false;

  const ensureReady = async () => {
    if (ready) return;
    await pendingOpsStore.init();
    ready = true;
  };

  return {
    listPendingOps: async () => {
      await ensureReady();
      return pendingOpsStore.listPendingOps();
    },
  };
}

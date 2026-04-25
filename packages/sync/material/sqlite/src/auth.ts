import type { SqliteRunner } from '@treecrdt/interface/sqlite';
import {
  createTreecrdtSqliteSubtreeScopeEvaluator,
  type TreecrdtScopeEvaluator,
} from '@treecrdt/auth';

import {
  createCapabilityMaterialStore,
  createOpAuthStore,
  createPendingOpsStore,
  type SqliteCapabilityMaterialStore,
  type SqliteOpAuthStore,
  type SqlitePendingOpsStore,
} from './proof-material/index.js';

export type TreecrdtSqliteAuthBackend = {
  /**
   * SQLite-backed subtree evaluator for capability scopes.
   *
   * Auth itself stays backend-agnostic; the storage adapter owns how scope checks read
   * the local materialized tree.
   */
  scopeEvaluator: TreecrdtScopeEvaluator;
  capabilityStore: SqliteCapabilityMaterialStore;
  opAuthStore: SqliteOpAuthStore;
  pendingOpsStore: SqlitePendingOpsStore;
};

export function createTreecrdtSqliteAuthBackend(opts: {
  runner: SqliteRunner;
  docId: string;
  nowMs?: () => number;
}): TreecrdtSqliteAuthBackend {
  const storeOpts = { runner: opts.runner, docId: opts.docId, nowMs: opts.nowMs };
  return {
    scopeEvaluator: createTreecrdtSqliteSubtreeScopeEvaluator(opts.runner),
    capabilityStore: createCapabilityMaterialStore(storeOpts),
    opAuthStore: createOpAuthStore(storeOpts),
    pendingOpsStore: createPendingOpsStore(storeOpts),
  };
}

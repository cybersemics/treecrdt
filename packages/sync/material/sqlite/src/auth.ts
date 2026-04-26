import type { SqliteRunner } from '@treecrdt/interface/sqlite';
import {
  createTreecrdtAuthSession,
  createTreecrdtSqliteSubtreeScopeEvaluator,
  type TreecrdtAuthSession,
  type TreecrdtAuthSessionOptions,
  type TreecrdtScopeEvaluator,
} from '@treecrdt/auth';

import {
  createCapabilityMaterialStore,
  createOpAuthStore,
  type SqliteCapabilityMaterialStore,
  type SqliteOpAuthStore,
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
  };
}

export type TreecrdtSqliteAuthSessionOptions = Omit<
  TreecrdtAuthSessionOptions,
  'backend' | 'scopeEvaluator' | 'capabilityStore' | 'opAuthStore'
> & {
  runner: SqliteRunner;
  nowMs?: () => number;
};

export function createTreecrdtSqliteAuthSession(
  opts: TreecrdtSqliteAuthSessionOptions,
): TreecrdtAuthSession {
  const { runner, nowMs, ...sessionOpts } = opts;
  return createTreecrdtAuthSession({
    ...sessionOpts,
    backend: createTreecrdtSqliteAuthBackend({
      runner,
      docId: sessionOpts.docId,
      nowMs,
    }),
  });
}

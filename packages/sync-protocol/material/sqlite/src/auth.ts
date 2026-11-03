import type { Kysely, TreecrdtDb } from '@treecrdt/interface/sqlite';
import {
  createTreecrdtAuthSession,
  describeTreecrdtCapabilityTokenV1,
  createTreecrdtSqliteSubtreeScopeEvaluator,
  type TreecrdtAuthSessionTrust,
  type TreecrdtAuthSession,
  type TreecrdtAuthSessionOptions,
  type TreecrdtCapabilityRevocationOptions,
  type TreecrdtCapabilityTokenV1,
  type TreecrdtScopeEvaluator,
} from '@treecrdt/auth';

import {
  createCapabilityMaterialStore,
  createOpAuthStore,
  type SqliteCapabilityMaterialStore,
  type SqliteOpAuthStore,
} from './proof-material/index.js';
import type { TreecrdtSyncSqliteDb } from './sync-schema.js';

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
  db: Kysely<TreecrdtDb>;
  docId: string;
  nowMs?: () => number;
}): TreecrdtSqliteAuthBackend {
  const storeDb = opts.db as unknown as Kysely<TreecrdtSyncSqliteDb>;
  const storeOpts = { db: storeDb, docId: opts.docId, nowMs: opts.nowMs };
  return {
    scopeEvaluator: createTreecrdtSqliteSubtreeScopeEvaluator(opts.db),
    capabilityStore: createCapabilityMaterialStore(storeOpts),
    opAuthStore: createOpAuthStore(storeOpts),
  };
}

export type TreecrdtSqliteAuthSessionOptions = Omit<
  TreecrdtAuthSessionOptions,
  'backend' | 'scopeEvaluator' | 'capabilityStore' | 'opAuthStore'
> & {
  db: Kysely<TreecrdtDb>;
  nowMs?: () => number;
};

export type TreecrdtSqliteClientAuthSessionOptions = Omit<
  TreecrdtSqliteAuthSessionOptions,
  'db' | 'docId'
> & {
  docId?: string;
};

export function createTreecrdtSqliteAuthSession(
  opts: TreecrdtSqliteAuthSessionOptions,
): TreecrdtAuthSession {
  const { db, nowMs, ...sessionOpts } = opts;
  return createTreecrdtAuthSession({
    ...sessionOpts,
    backend: createTreecrdtSqliteAuthBackend({
      db,
      docId: sessionOpts.docId,
      nowMs,
    }),
  });
}

export type TreecrdtSqliteDescribeCapabilityTokenOptions = {
  tokenBytes: Uint8Array;
  issuerPublicKeys?: readonly Uint8Array[];
  trust?: TreecrdtAuthSessionTrust;
  docId?: string;
  nowSec?: number;
} & TreecrdtCapabilityRevocationOptions;

export type TreecrdtSqliteEvaluateScopeOptions = Omit<
  Parameters<TreecrdtScopeEvaluator>[0],
  'docId'
> & {
  docId?: string;
};

export type TreecrdtSqliteAuthApi = {
  createSession: (opts: TreecrdtSqliteClientAuthSessionOptions) => TreecrdtAuthSession;
  describeCapabilityToken: (
    opts: TreecrdtSqliteDescribeCapabilityTokenOptions,
  ) => Promise<TreecrdtCapabilityTokenV1>;
  evaluateScope: (opts: TreecrdtSqliteEvaluateScopeOptions) => ReturnType<TreecrdtScopeEvaluator>;
};

export function createTreecrdtSqliteAuthApi(opts: {
  db: Kysely<TreecrdtDb>;
  docId: string;
  nowMs?: () => number;
}): TreecrdtSqliteAuthApi {
  const scopeEvaluator = createTreecrdtSqliteSubtreeScopeEvaluator(opts.db);
  return {
    createSession: (sessionOpts) =>
      createTreecrdtSqliteAuthSession({
        ...sessionOpts,
        db: opts.db,
        docId: sessionOpts.docId ?? opts.docId,
        nowMs: sessionOpts.nowMs ?? opts.nowMs,
      }),
    describeCapabilityToken: (describeOpts) =>
      describeTreecrdtCapabilityTokenV1({
        ...describeOpts,
        issuerPublicKeys:
          describeOpts.trust?.issuerPublicKeys ?? describeOpts.issuerPublicKeys ?? [],
        docId: describeOpts.docId ?? opts.docId,
        scopeEvaluator,
      }),
    evaluateScope: (scopeOpts) =>
      scopeEvaluator({
        ...scopeOpts,
        docId: scopeOpts.docId ?? opts.docId,
      }),
  };
}

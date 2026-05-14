import type { SqliteRunner } from '@justtemporary/interface/sqlite';
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
} from '@justtemporary/auth';

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

export type TreecrdtSqliteClientAuthSessionOptions = Omit<
  TreecrdtSqliteAuthSessionOptions,
  'runner' | 'docId'
> & {
  docId?: string;
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
  runner: SqliteRunner;
  docId: string;
  nowMs?: () => number;
}): TreecrdtSqliteAuthApi {
  const scopeEvaluator = createTreecrdtSqliteSubtreeScopeEvaluator(opts.runner);
  return {
    createSession: (sessionOpts) =>
      createTreecrdtSqliteAuthSession({
        ...sessionOpts,
        runner: opts.runner,
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

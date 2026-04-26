import type { Operation } from '@treecrdt/interface';
import type { Capability, SyncAuth } from '@treecrdt/sync';

import {
  createTreecrdtIdentityChainCapabilityV1,
  type TreecrdtIdentityChainV1,
} from './identity.js';
import {
  createTreecrdtCoseCwtAuth,
  type TreecrdtCoseCwtAuthOptions,
} from './internal/cose-cwt-auth.js';

type MaybePromise<T> = T | Promise<T>;

export type TreecrdtAuthSessionState =
  | { status: 'loading' }
  | { status: 'ready' }
  | { status: 'error'; error: unknown };

export type TreecrdtAuthSessionBackend = Pick<
  TreecrdtCoseCwtAuthOptions,
  'scopeEvaluator' | 'capabilityStore' | 'opAuthStore'
>;

export type TreecrdtAuthSessionIdentity = {
  /**
   * Optional local identity chain, or provider for apps that create it lazily.
   *
   * Provider failures are intentionally best-effort: identity disclosure should not prevent
   * signed capability auth from becoming ready.
   */
  local?:
    | TreecrdtIdentityChainV1
    | (() => MaybePromise<TreecrdtIdentityChainV1 | null | undefined>);
  onLocalError?: (error: unknown) => void;
  onPeer?: TreecrdtCoseCwtAuthOptions['onPeerIdentityChain'];
};

type LegacyTreecrdtAuthSessionOptions = Pick<
  TreecrdtCoseCwtAuthOptions,
  'scopeEvaluator' | 'capabilityStore' | 'opAuthStore' | 'onPeerIdentityChain'
> & {
  /** Prefer `identity.local`; kept so existing callers do not need to migrate immediately. */
  localIdentityChain?:
    | TreecrdtIdentityChainV1
    | (() => MaybePromise<TreecrdtIdentityChainV1 | null | undefined>);
  /** Prefer `identity.onLocalError`; kept so existing callers do not need to migrate immediately. */
  onIdentityChainError?: (error: unknown) => void;
};

export type TreecrdtAuthSessionOptions = Omit<
  TreecrdtCoseCwtAuthOptions,
  | 'localIdentityChain'
  | 'scopeEvaluator'
  | 'capabilityStore'
  | 'opAuthStore'
  | 'onPeerIdentityChain'
> &
  LegacyTreecrdtAuthSessionOptions & {
    /** Doc used to warm local auth material before the session is handed to sync. */
    docId: string;
    /** Backend-owned auth dependencies, e.g. subtree scope checks and proof-material stores. */
    backend?: TreecrdtAuthSessionBackend;
    identity?: TreecrdtAuthSessionIdentity;
  };

export type TreecrdtAuthSession = {
  syncAuth: SyncAuth<Operation>;
  readonly ready: Promise<SyncAuth<Operation>>;
  getState: () => TreecrdtAuthSessionState;
  refresh: () => Promise<SyncAuth<Operation>>;
};

/**
 * Creates one long-lived sync auth object and warms its local material before exposing readiness.
 *
 * This is intentionally framework-agnostic. Apps can keep the returned `syncAuth` stable while
 * awaiting `ready` before passing it into sync startup, avoiding per-app warmup wrappers.
 */
export function createTreecrdtAuthSession(opts: TreecrdtAuthSessionOptions): TreecrdtAuthSession {
  const {
    docId,
    backend,
    identity,
    localIdentityChain,
    onIdentityChainError,
    scopeEvaluator,
    capabilityStore,
    opAuthStore,
    onPeerIdentityChain,
    ...authOptsBase
  } = opts;
  const authOpts: TreecrdtCoseCwtAuthOptions = {
    ...authOptsBase,
    scopeEvaluator: backend?.scopeEvaluator ?? scopeEvaluator,
    capabilityStore: backend?.capabilityStore ?? capabilityStore,
    opAuthStore: backend?.opAuthStore ?? opAuthStore,
    onPeerIdentityChain: identity?.onPeer ?? onPeerIdentityChain,
  };
  const resolvedLocalIdentityChain = identity?.local ?? localIdentityChain;
  const resolvedOnIdentityChainError = identity?.onLocalError ?? onIdentityChainError;
  const baseAuth = createTreecrdtCoseCwtAuth(authOpts);
  let state: TreecrdtAuthSessionState = { status: 'loading' };

  const localIdentityCapability = async () => {
    if (!resolvedLocalIdentityChain) return null;
    try {
      const chain =
        typeof resolvedLocalIdentityChain === 'function'
          ? await resolvedLocalIdentityChain()
          : resolvedLocalIdentityChain;
      return chain ? createTreecrdtIdentityChainCapabilityV1(chain) : null;
    } catch (err) {
      resolvedOnIdentityChainError?.(err);
      return null;
    }
  };

  const addLocalIdentity = async (baseCaps: MaybePromise<Capability[] | undefined>) => {
    const caps = [...((await baseCaps) ?? [])];
    const identityCap = await localIdentityCapability();
    if (identityCap) caps.push(identityCap);
    return caps;
  };

  const syncAuth: SyncAuth<Operation> = {
    ...baseAuth,
    helloCapabilities: (ctx) => addLocalIdentity(baseAuth.helloCapabilities?.(ctx)),
    onHello: (hello, ctx) => addLocalIdentity(baseAuth.onHello?.(hello, ctx)),
  };

  const warm = async () => {
    state = { status: 'loading' };
    try {
      await syncAuth.helloCapabilities?.({ docId });
      state = { status: 'ready' };
      return syncAuth;
    } catch (error) {
      state = { status: 'error', error };
      throw error;
    }
  };

  let ready = warm();

  return {
    syncAuth,
    get ready() {
      return ready;
    },
    getState: () => state,
    refresh: () => {
      ready = warm();
      return ready;
    },
  };
}

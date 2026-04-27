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

export type TreecrdtAuthSessionOptions = Omit<TreecrdtCoseCwtAuthOptions, 'localIdentityChain'> & {
  /** Doc used to warm local auth material before the session is handed to sync. */
  docId: string;
  /**
   * Optional local identity chain, or provider for apps that create it lazily.
   *
   * Provider failures are intentionally best-effort: identity disclosure should not prevent
   * signed capability auth from becoming ready.
   */
  localIdentityChain?:
    | TreecrdtIdentityChainV1
    | (() => MaybePromise<TreecrdtIdentityChainV1 | null | undefined>);
  onIdentityChainError?: (error: unknown) => void;
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
  const { docId, localIdentityChain, onIdentityChainError, ...authOpts } = opts;
  const baseAuth = createTreecrdtCoseCwtAuth(authOpts);
  let state: TreecrdtAuthSessionState = { status: 'loading' };

  const localIdentityCapability = async () => {
    if (!localIdentityChain) return null;
    try {
      const chain =
        typeof localIdentityChain === 'function' ? await localIdentityChain() : localIdentityChain;
      return chain ? createTreecrdtIdentityChainCapabilityV1(chain) : null;
    } catch (err) {
      onIdentityChainError?.(err);
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

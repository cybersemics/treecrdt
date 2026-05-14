import type { Operation } from '@justtemporary/interface';
import type {
  Capability,
  Hello,
  OpAuth,
  SyncAuth,
  SyncAuthHelloContext,
  SyncAuthOpsContext,
} from '@justtemporary/sync-protocol';

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

export type TreecrdtAuthSessionTrust = {
  issuerPublicKeys: Uint8Array[];
};

export type TreecrdtAuthSessionLocal = {
  privateKey: Uint8Array;
  publicKey: Uint8Array;
  capabilityTokens?: Uint8Array[];
  revocationRecords?: Uint8Array[];
};

export type TreecrdtAuthSessionLocalAuthorizeOptions = Partial<SyncAuthOpsContext>;

type LegacyTreecrdtAuthSessionOptions = Pick<
  TreecrdtCoseCwtAuthOptions,
  'scopeEvaluator' | 'capabilityStore' | 'opAuthStore' | 'onPeerIdentityChain'
> &
  Partial<
    Pick<
      TreecrdtCoseCwtAuthOptions,
      | 'issuerPublicKeys'
      | 'localPrivateKey'
      | 'localPublicKey'
      | 'localCapabilityTokens'
      | 'localRevocationRecords'
    >
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
  | 'issuerPublicKeys'
  | 'localPrivateKey'
  | 'localPublicKey'
  | 'localCapabilityTokens'
  | 'localRevocationRecords'
> &
  LegacyTreecrdtAuthSessionOptions & {
    /** Doc used to warm local auth material before the session is handed to sync. */
    docId: string;
    /** Backend-owned auth dependencies, e.g. subtree scope checks and proof-material stores. */
    backend?: TreecrdtAuthSessionBackend;
    identity?: TreecrdtAuthSessionIdentity;
    trust?: TreecrdtAuthSessionTrust;
    local?: TreecrdtAuthSessionLocal;
  };

export type TreecrdtAuthSession = {
  syncAuth: SyncAuth<Operation>;
  readonly ready: Promise<SyncAuth<Operation>>;
  getState: () => TreecrdtAuthSessionState;
  refresh: () => Promise<SyncAuth<Operation>>;
  authorizeLocalOps: (
    ops: readonly Operation[],
    opts?: TreecrdtAuthSessionLocalAuthorizeOptions,
  ) => Promise<OpAuth[]>;
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
    trust,
    local,
    localIdentityChain,
    onIdentityChainError,
    scopeEvaluator,
    capabilityStore,
    opAuthStore,
    onPeerIdentityChain,
    issuerPublicKeys,
    localPrivateKey,
    localPublicKey,
    localCapabilityTokens,
    localRevocationRecords,
    ...authOptsBase
  } = opts;
  const resolvedIssuerPublicKeys = trust?.issuerPublicKeys ?? issuerPublicKeys;
  const resolvedLocalPrivateKey = local?.privateKey ?? localPrivateKey;
  const resolvedLocalPublicKey = local?.publicKey ?? localPublicKey;
  if (!resolvedIssuerPublicKeys) throw new Error('auth session requires trust.issuerPublicKeys');
  if (!resolvedLocalPrivateKey) throw new Error('auth session requires local.privateKey');
  if (!resolvedLocalPublicKey) throw new Error('auth session requires local.publicKey');

  const authOpts: TreecrdtCoseCwtAuthOptions = {
    ...authOptsBase,
    issuerPublicKeys: resolvedIssuerPublicKeys,
    localPrivateKey: resolvedLocalPrivateKey,
    localPublicKey: resolvedLocalPublicKey,
    localCapabilityTokens: local?.capabilityTokens ?? localCapabilityTokens,
    localRevocationRecords: local?.revocationRecords ?? localRevocationRecords,
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
    helloCapabilities: (ctx: SyncAuthHelloContext) =>
      addLocalIdentity(baseAuth.helloCapabilities?.(ctx)),
    onHello: (hello: Hello, ctx: SyncAuthHelloContext) =>
      addLocalIdentity(baseAuth.onHello?.(hello, ctx)),
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

  const authorizeLocalOps: TreecrdtAuthSession['authorizeLocalOps'] = async (
    ops,
    ctxOverrides = {},
  ) => {
    if (ops.length === 0) return [];
    if (!syncAuth.signOps || !syncAuth.verifyOps) {
      throw new Error('auth session is missing local op signing/verification hooks');
    }
    const ctx: SyncAuthOpsContext = {
      docId,
      purpose: 'reconcile',
      filterId: '__local__',
      ...ctxOverrides,
    };
    const auth = await syncAuth.signOps(ops, ctx);
    if (auth.length !== ops.length) {
      throw new Error(`signOps returned ${auth.length} entries for ${ops.length} ops`);
    }
    const res = await syncAuth.verifyOps(ops, auth, ctx);
    const dispositions = res?.dispositions;
    if (dispositions && dispositions.length !== ops.length) {
      throw new Error(
        `verifyOps returned ${dispositions.length} dispositions for ${ops.length} ops`,
      );
    }
    const rejected = dispositions?.find((d) => d.status !== 'allow');
    if (rejected?.status === 'pending_context') {
      throw new Error(rejected.message ?? 'missing subtree context to authorize op');
    }
    return auth;
  };

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
    authorizeLocalOps,
  };
}

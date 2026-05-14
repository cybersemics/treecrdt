import type { Operation } from '@justthrowaway/interface';
import type {
  MaterializationEvent,
  MaterializationListener,
  TreecrdtEngine,
} from '@justthrowaway/interface/engine';
import type { TreecrdtSyncBackendClient } from '@justthrowaway/sync-sqlite/backend';
import type {
  Filter,
  SyncAuth,
  SyncPeerOptions,
  SyncOnceOptions,
  SyncSubscribeOptions,
} from '@justthrowaway/sync-protocol';
import type { DiscoveryRouteCache } from '@justthrowaway/discovery';

/**
 * {@link TreecrdtEngine} (e.g. wa-sqlite) plus the materialization API used to proxy `onChange`.
 */
export type TreecrdtWebSocketSyncClient = TreecrdtEngine & TreecrdtSyncBackendClient;

export type ConnectTreecrdtWebSocketSyncOptions = {
  /**
   * `WebSocket` **constructor** when the runtime has no global (e.g. Node before a global client
   * is standard), or a non-default spec client. In Node, use e.g. `import { WebSocket } from
   * "undici"`. A spec `EventTarget` implementation is required (the same applies to the built-in
   * wire via `addEventListener` for `message`).
   */
  webSocketCtor?: typeof WebSocket;
  /**
   * WebSocket URL, or `http(s)://` discovery bootstrap URL.
   */
  baseUrl: string;
  /**
   * Required for HTTPS discovery; optional for `ws://` / `wss://` direct URLs.
   */
  fetch?: typeof fetch;
  discoveryCache?: DiscoveryRouteCache;
  resolveDocPath?: string;
  enablePendingSidecar?: boolean;
  auth?: SyncAuth<Operation>;
  syncPeerOptions?: Partial<SyncPeerOptions<Operation>>;
  /**
   * If the live `subscribe` session ends with an error after `startLive` has settled (e.g. wire
   * drop), that rejection is passed here. Defaults to `console.error` so behavior matches older
   * releases; pass a no-op or your own logger to avoid logging in production.
   */
  onLiveError?: (error: unknown) => void;
  webSocketBinaryType?: BinaryType;
};

export type TreecrdtWebSocketSync = {
  /**
   * Materialization subscription; forwards to `client.onMaterialized` with the same
   * `MaterializationEvent` shape. Do not register the same logic on the client as well, or
   * listeners will run twice.
   */
  onChange: (listener: MaterializationListener) => () => void;
  syncOnce: (filter?: Filter, opts?: SyncOnceOptions) => Promise<void>;
  startLive: (opts?: SyncSubscribeOptions) => Promise<void>;
  stopLive: () => void;
  /**
   /**
    * Upload local ops to the peer. For local→remote only; pass ops from your edit API (not from onChange).
    * No-ops if empty. For full sync, use syncOnce instead.
    */
  pushLocalOps: (ops?: readonly Operation[]) => Promise<void>;
  close: () => Promise<void>;
};

export type CreateTreecrdtWebSocketSyncFromTransportOptions = {
  enablePendingSidecar?: boolean;
  auth?: SyncAuth<Operation>;
  syncPeerOptions?: Partial<SyncPeerOptions<Operation>>;
  /**
   * @see {@link ConnectTreecrdtWebSocketSyncOptions.onLiveError}
   */
  onLiveError?: (error: unknown) => void;
};

export type { MaterializationEvent, MaterializationListener };

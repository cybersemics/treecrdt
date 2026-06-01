import type { Operation } from '@treecrdt/interface';
import type {
  MaterializationEvent,
  MaterializationListener,
  TreecrdtEngine,
} from '@treecrdt/interface/engine';
import type { TreecrdtSyncBackendClient } from '@treecrdt/sync-sqlite/backend';
import type {
  Filter,
  SyncAuth,
  SyncMessage,
  SyncPeerOptions,
  SyncOnceOptions,
  SyncPeer,
  SyncPushOptions,
  SyncSubscribeOptions,
} from '@treecrdt/sync-protocol';
import type { DuplexTransport } from '@treecrdt/sync-protocol/transport';
import type { DiscoveryRouteCache } from '@treecrdt/discovery';

/**
 * {@link TreecrdtEngine} (e.g. wa-sqlite) plus the SQLite sync backend surface from
 * `@treecrdt/sync-sqlite`.
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
  syncOnce: (filter?: Filter, opts?: SyncOnceOptions) => Promise<void>;
  startLive: (opts?: SyncSubscribeOptions) => Promise<void>;
  stopLive: () => void;
  /**
   * Upload local ops to the peer. For local-to-remote only; pass ops from your edit API, not from
   * `onChange`. No-ops if empty. For full reconciliation, use `syncOnce`.
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

export type OutboundSyncStatus = {
  peerCount: number;
  pendingOps: number;
  running: boolean;
  scheduled: boolean;
};

export type OutboundSyncOptions<Op = Operation> = {
  localPeer: SyncPeer<Op>;
  /**
   * Stable key used to coalesce repeated local write hints before upload.
   */
  opKey?: (op: Op) => string;
  /**
   * Allows apps to keep queued work while offline instead of turning transient offline state into
   * sync errors.
   */
  isOnline?: () => boolean;
  pushOptions?: (peerId: string) => SyncPushOptions | undefined;
  pushTimeoutMs?: number | ((peerId: string) => number | undefined);
  onError?: (ctx: { peerId: string; error: unknown }) => void;
  onStatus?: (status: OutboundSyncStatus) => void;
};

export type OutboundSync<Op = Operation> = {
  readonly status: OutboundSyncStatus;
  readonly pendingOpCount: number;
  readonly peerCount: number;
  addPeer: (peerId: string, transport: DuplexTransport<SyncMessage<Op>>) => void;
  removePeer: (peerId: string) => void;
  clearPeers: () => void;
  queueOps: (ops: readonly Op[]) => void;
  flush: () => Promise<void>;
  close: () => void;
};

export type { MaterializationEvent, MaterializationListener };

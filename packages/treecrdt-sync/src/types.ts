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

export type SyncControllerState = 'idle' | 'starting' | 'live' | 'stopped' | 'error' | 'closed';

export type SyncControllerStatus = {
  state: SyncControllerState;
  pendingOps: number;
  error?: unknown;
};

export type SyncControllerOptions = {
  /**
   * Initial reconciliation to run before the controller is considered live-ready.
   * Pass `false` to skip initial reconciliation.
   */
  initialSync?: false | { filter?: Filter; opts?: SyncOnceOptions };
  /**
   * Live subscription options. Pass `false` for explicit push/reconcile only.
   */
  live?: false | SyncSubscribeOptions;
  /**
   * Optional safety-net reconciliation while the controller is running.
   */
  reconcileIntervalMs?: number;
  onStatus?: (status: SyncControllerStatus) => void;
  onError?: (error: unknown) => void;
};

export type ConnectSyncControllerOptions = ConnectTreecrdtWebSocketSyncOptions & {
  controller?: SyncControllerOptions;
};

export type OutboundSyncStatus = {
  peerCount: number;
  pendingOps: number;
  needsFullSync: boolean;
  running: boolean;
  scheduled: boolean;
};

export type OutboundSyncRunPushContext<Op = Operation> = {
  localPeer: SyncPeer<Op>;
  peerId: string;
  transport: DuplexTransport<SyncMessage<Op>>;
  ops: readonly Op[];
};

export type OutboundSyncRunSyncContext<Op = Operation> = {
  localPeer: SyncPeer<Op>;
  peerId: string;
  transport: DuplexTransport<SyncMessage<Op>>;
  filter: Filter;
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
  /**
   * Select which attached transports should receive queued local writes. Useful when one SyncPeer
   * owns both local-tab mesh transports and a remote websocket transport.
   */
  shouldSyncPeer?: (peerId: string) => boolean;
  /**
   * Filters to reconcile when callers request a fallback sync without exact local ops.
   */
  getFallbackFilters?: () => readonly Filter[];
  /**
   * Override low-level push execution for app-specific timeouts, batching, or logging.
   */
  runPush?: (ctx: OutboundSyncRunPushContext<Op>) => Promise<void>;
  /**
   * Override fallback reconciliation for app-specific timeouts or syncOnce options.
   */
  runSync?: (ctx: OutboundSyncRunSyncContext<Op>) => Promise<void>;
  pushOptions?: (peerId: string) => SyncPushOptions | undefined;
  syncOptions?: (peerId: string, filter: Filter) => SyncOnceOptions | undefined;
  onWorkStart?: () => void;
  onWorkEnd?: () => void;
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
  queue: (ops?: readonly Op[]) => void;
  flush: () => Promise<void>;
  close: () => void;
};

export type SyncController = {
  readonly status: SyncControllerStatus;
  readonly pendingOpCount: number;
  start: () => Promise<void>;
  stopLive: () => void;
  pushLocalOps: (ops?: readonly Operation[]) => Promise<void>;
  flushPendingOps: () => Promise<void>;
  syncOnce: (filter?: Filter, opts?: SyncOnceOptions) => Promise<void>;
  onChange: TreecrdtWebSocketSync['onChange'];
  close: () => Promise<void>;
};

export type { MaterializationEvent, MaterializationListener };

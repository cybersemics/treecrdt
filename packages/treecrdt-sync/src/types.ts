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
  SyncPeerOptions,
  SyncOnceOptions,
  SyncPushOptions,
  SyncSubscribeOptions,
} from '@treecrdt/sync-protocol';
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
   * Upload local ops to the peer. For local→remote only; pass ops from your edit API.
   * No-ops if empty. For full sync, use syncOnce instead.
   */
  pushLocalOps: (ops?: readonly Operation[], opts?: SyncPushOptions) => Promise<void>;
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
  hasTarget: boolean;
  pendingOps: number;
  flushing: boolean;
  closed: boolean;
};

export type OutboundSyncFlushResult =
  | { status: 'drained' }
  | { status: 'deferred'; reason: 'no-target' | 'offline'; pendingOps: number }
  | { status: 'failed'; error: unknown; pendingOps: number }
  | { status: 'closed' };

/**
 * One replaceable destination for exact committed local operations.
 *
 * This deliberately does not expose a `SyncPeer` or transport. A high-level websocket handle can
 * be used with `(ops, opts) => sync.pushLocalOps(ops, opts)`, while low-level callers can wrap
 * `peer.pushOps(transport, ops, opts)`. Targets must honor `opts.signal`; replacement, timeout,
 * and close wait for the aborted attempt before moving on.
 */
export type OutboundSyncPushTarget<Op = Operation> = (
  ops: readonly Op[],
  opts: SyncPushOptions,
) => Promise<void>;

export type OutboundSyncOptions<Op = Operation> = {
  /**
   * Stable key used to coalesce repeated local write hints before upload.
   *
   * Defaults to TreeCRDT `Operation.meta.id` when the queued op has the standard operation shape.
   * Provide this only for custom op shapes or custom coalescing behavior.
   */
  opKey?: (op: Op) => string;
  /**
   * Allows apps to keep queued work while offline instead of turning transient offline state into
   * sync errors.
   */
  isOnline?: () => boolean;
  /** Optional low-level peer notification used to wake local live subscriptions. */
  notifyLocalUpdate?: (ops: readonly Op[]) => void | Promise<void>;
  pushOptions?: SyncPushOptions;
  /** Abort a stalled push after this duration while keeping its ops queued for retry. */
  pushTimeoutMs?: number;
  onError?: (error: unknown) => void;
  onStatus?: (status: OutboundSyncStatus) => void;
};

export type OutboundSync<Op = Operation> = {
  readonly status: OutboundSyncStatus;
  /**
   * Replace the remote upload target. The returned cleanup removes only this registration, so a
   * stale socket cleanup cannot remove a newer target.
   */
  setTarget: (target: OutboundSyncPushTarget<Op>) => () => void;
  /**
   * Queue exact committed local ops. This also invokes `notifyLocalUpdate`, when supplied, to wake
   * low-level live subscriptions.
   */
  queueOps: (ops: readonly Op[]) => void;
  /**
   * Attempt to flush queued work. Offline and missing-target states are deferred, while push
   * errors are reported as failed; all three keep the ops queued for an explicit retry.
   */
  flush: () => Promise<OutboundSyncFlushResult>;
  /**
   * Abort and await active work, then discard queued ops. Repeated calls share one barrier.
   * Afterwards, queue/target updates are ignored and `flush()` reports `closed`.
   */
  close: () => Promise<void>;
};

export type { MaterializationEvent, MaterializationListener };

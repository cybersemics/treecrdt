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

/**
 * One replaceable destination for exact committed local operations.
 *
 * This deliberately does not expose a `SyncPeer` or transport. A high-level websocket handle can
 * be used with `(ops, opts) => sync.pushLocalOps(ops, opts)`, while low-level callers can wrap
 * `peer.pushOps(transport, ops, opts)`. Resolution acknowledges application of all supplied ops.
 * Forward `opts.signal` where supported. Local storage targets must await their actual write, not
 * race an uninterruptible write against cancellation. Network cancellation only stops waiting;
 * an already-started send or remote application may still finish after the target promise rejects.
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
   * Prevent attempts while offline. Waiting calls reject, but operations remain queued for retry.
   */
  isOnline?: () => boolean;
  /** Optional low-level peer notification used to wake local live subscriptions. */
  notifyLocalUpdate?: (ops: readonly Op[]) => void | Promise<void>;
  pushOptions?: SyncPushOptions;
  /** Reject waiting calls and request cancellation after this duration; retain ops for retry. */
  pushTimeoutMs?: number;
  onError?: (error: unknown) => void;
  onStatus?: (status: OutboundSyncStatus) => void;
};

export type OutboundSync<Op = Operation> = {
  readonly status: OutboundSyncStatus;
  /**
   * Replace the push target. The returned cleanup removes only this registration, so a
   * stale socket cleanup cannot remove a newer target.
   */
  setTarget: (target: OutboundSyncPushTarget<Op>) => () => void;
  /**
   * Synchronously enqueue exact committed local ops and acknowledge this batch plus earlier work.
   * Later writes do not delay or fail this promise. A failed attempt rejects dependent promises,
   * retaining operations for a new push, flush, or target registration to retry. Rejected promises
   * never revive. Also invokes the best-effort `notifyLocalUpdate`, when supplied.
   */
  push: (ops: readonly Op[]) => Promise<void>;
  /**
   * Attempt and acknowledge the accepted prefix at invocation, without waiting for later writes.
   * Missing-target, offline, and failed attempts reject while retaining operations for retry.
   */
  flush: () => Promise<void>;
  /**
   * Wait until execution stops, including work enqueued by synchronous completion callbacks.
   * Does not retry retained operations or acknowledge success; resolves even after a failed attempt.
   */
  whenIdle: () => Promise<void>;
  /**
   * Abort active work, discard queued ops, and await the active target promise. Repeated calls share
   * one barrier.
   * Reject unsettled calls immediately. Afterwards push/flush reject and target updates are ignored.
   */
  close: () => Promise<void>;
};

export type { MaterializationEvent, MaterializationListener };

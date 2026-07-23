import type { RibltDecoder16 } from '@treecrdt/riblt-wasm';

import {
  AUTH_CAPABILITY_NAME,
  isAnyAuthCapability,
  isAuthCapability,
} from './auth-capabilities.js';
import type { SyncAuth, SyncAuthVerifyOpsResult, SyncOpPurpose } from './auth.js';
import {
  buildInitiatorHelloCapabilities,
  capabilitySetFingerprint,
  DIRECT_SEND_EMPTY_RECEIVER_FILTER_CAPABILITY,
  DIRECT_SEND_EMPTY_RECEIVER_MAX_OPS_PER_BATCH,
  DIRECT_SEND_SMALL_SCOPE_FILTER_CAPABILITY,
  peerRequestedDirectSendFilter,
  peerSelectedDirectSendEmptyReceiverFilter,
  peerSelectedDirectSendFilter,
  peerSupportsDirectSendEmptyReceiver,
  peerSupportsDirectSendSmallScope,
} from './capabilities.js';
import { ErrorCode, RibltFailureReason } from './types.js';
import type {
  Capability,
  Filter,
  Hello,
  HelloAck,
  OpRef,
  OpsBatch,
  PendingOp,
  RibltCodewords,
  RibltStatus,
  Subscribe,
  SubscribeAck,
  SyncBackend,
  SyncMessage,
  Unsubscribe,
} from './types.js';
import { traceHello } from './traces.js';
import type { DuplexTransport } from './transport/index.js';

function randomId(prefix: string): string {
  const uuid =
    typeof globalThis.crypto?.randomUUID === 'function'
      ? globalThis.crypto.randomUUID()
      : `${Math.random().toString(16).slice(2)}_${Date.now().toString(16)}`;
  return `${prefix}_${uuid}`;
}

type Pending<T> = {
  promise: Promise<T>;
  resolve: (value: T) => void;
  reject: (err: unknown) => void;
};

function deferred<T>(): Pending<T> {
  let resolve!: (value: T) => void;
  let reject!: (err: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  // Avoid "unhandled rejection" warnings when a promise is rejected before the
  // awaiting code reaches it (we still propagate failures via awaits/races).
  void promise.catch(() => {});
  return { promise, resolve, reject };
}

function ignoreErrors(action?: () => void): void {
  try {
    action?.();
  } catch {
    // best-effort cleanup or error reporting
  }
}

type ResponderSession = {
  filter: Filter;
  round: number;
  decoder: RibltDecoder16;
  expectedIndex: bigint;
  awaitingIncomingDone: boolean;
};

type InitiatorSession<Op> = {
  transport: DuplexTransport<SyncMessage<Op>>;
  filter: Filter;
  filterId: string;
  round: number;
  ack: Pending<HelloAck>;
  terminalStatus: Pending<RibltStatus>;
  codewordCredits: number;
  codewordCreditSignal: Pending<void>;
  receivedOps: Pending<void>;
  awaitingUploadAck: boolean;
  done: boolean;
};

function rejectInitiatorSession<Op>(session: InitiatorSession<Op>, error: Error): void {
  session.done = true;
  session.ack.reject(error);
  session.terminalStatus.reject(error);
  session.codewordCreditSignal.reject(error);
  session.receivedOps.reject(error);
}

export type SyncPeerOptions<Op = unknown> = {
  /** Upper bound on RIBLT codewords exchanged while reconciling one filter. */
  maxCodewords?: number;
  /** Split uploaded or replayed ops into smaller wire batches to avoid giant frames. */
  maxOpsPerBatch?: number;
  /** Reject Hello messages that try to attach too many filters at once. */
  maxHelloFilters?: number;
  /** Direct-send tiny scopes instead of running a full RIBLT round when possible. */
  directSendThreshold?: number;
  /** Require an auth capability before accepting scoped Hello or Subscribe filters. */
  requireAuthForFilters?: boolean;
  /** Auth policy hooks for outgoing filters, incoming ops, and post-verify side effects. */
  auth?: SyncAuth<Op>;
  /** Derive stable op refs from ops so relay fast-forwarding can track delivered entries. */
  deriveOpRef?: (op: Op, ctx: { docId: string }) => OpRef;
};

export type SyncPeerAttachOptions<Op = unknown> = {
  onError?: (ctx: { error: unknown; transport: DuplexTransport<SyncMessage<Op>> }) => void;
};

export type SyncOnceOptions = {
  immediate?: boolean;
  codewordsPerMessage?: number;
  maxCodewords?: number;
  maxOpsPerBatch?: number;
};

export type SyncPushOptions = {
  /** Split a direct push into smaller wire batches to avoid giant frames. */
  maxOpsPerBatch?: number;
  /**
   * Reuse an existing opsBatch stream id for a direct push.
   *
   * This is not a filter definition; it only groups related push chunks and
   * the final done marker on the receiver.
   */
  filterId?: string;
};

export type SyncSubscribeOptions = SyncOnceOptions & {
  intervalMs?: number;
  signal?: AbortSignal;
};

export type SyncSubscription = {
  stop: () => void;
  ready: Promise<void>;
  done: Promise<void>;
};

function sleepUntil(ms: number, signal?: AbortSignal): Promise<boolean> {
  if (!Number.isFinite(ms) || ms < 0) throw new Error(`invalid intervalMs: ${ms}`);
  if (ms === 0) return Promise.resolve(true);

  return new Promise((resolve) => {
    let done = false;
    const timer = setTimeout(() => {
      if (done) return;
      done = true;
      signal?.removeEventListener('abort', onAbort);
      resolve(true);
    }, ms);

    const onAbort = () => {
      if (done) return;
      done = true;
      clearTimeout(timer);
      signal?.removeEventListener('abort', onAbort);
      resolve(false);
    };

    if (signal) {
      if (signal.aborted) {
        onAbort();
        return;
      }
      signal.addEventListener('abort', onAbort);
    }
  });
}

const yieldToMacrotask: () => Promise<void> = (() => {
  const setImmediateImpl = (globalThis as any).setImmediate as
    | undefined
    | ((cb: () => void) => void);
  if (typeof setImmediateImpl === 'function') {
    return async () => new Promise<void>((resolve) => setImmediateImpl(resolve));
  }

  if (typeof MessageChannel !== 'undefined') {
    const queue: Array<() => void> = [];
    const channel = new MessageChannel();
    channel.port1.onmessage = () => {
      queue.shift()?.();
    };
    return async () =>
      new Promise<void>((resolve) => {
        queue.push(resolve);
        channel.port2.postMessage(null);
      });
  }

  return async () => new Promise<void>((resolve) => setTimeout(resolve, 0));
})();

function bytesToHex(bytes: Uint8Array): string {
  let out = '';
  for (const b of bytes) out += b.toString(16).padStart(2, '0');
  return out;
}

function waitForAbort(signal: AbortSignal): Promise<void> {
  if (signal.aborted) return Promise.resolve();
  return new Promise<void>((resolve) =>
    signal.addEventListener('abort', () => resolve(), { once: true }),
  );
}

type ResponderSubscription<Op> = {
  subscriptionId: string;
  filter: Filter;
  sentOpRefs: Set<string>;
  transport: DuplexTransport<SyncMessage<Op>>;
};

type InitiatorSubscription<Op> = {
  transport: DuplexTransport<SyncMessage<Op>>;
  ack: Pending<SubscribeAck>;
  failed: Pending<never>;
};

type PendingPushOp<Op> = {
  opRef: OpRef;
  opRefHex: string;
  op: Op;
};

type SyncTransport<Op> = DuplexTransport<SyncMessage<Op>>;
// Responder ids are peer-chosen and only unique within one transport session.
type TransportOwnedMap<Op, Value> = Map<string, Map<SyncTransport<Op>, Value>>;

function getTransportOwned<Op, Value>(
  state: TransportOwnedMap<Op, Value>,
  transport: SyncTransport<Op>,
  id: string,
): Value | undefined {
  return state.get(id)?.get(transport);
}

function setTransportOwned<Op, Value>(
  state: TransportOwnedMap<Op, Value>,
  transport: SyncTransport<Op>,
  id: string,
  value: Value,
): void {
  const byTransport = state.get(id) ?? new Map<SyncTransport<Op>, Value>();
  byTransport.set(transport, value);
  state.set(id, byTransport);
}

function deleteTransportOwned<Op, Value>(
  state: TransportOwnedMap<Op, Value>,
  transport: SyncTransport<Op>,
  id: string,
): boolean {
  const byTransport = state.get(id);
  if (!byTransport) return false;
  const deleted = byTransport.delete(transport);
  if (byTransport.size === 0) state.delete(id);
  return deleted;
}

function dropTransportOwned<Op, Value>(
  state: TransportOwnedMap<Op, Value>,
  transport: SyncTransport<Op>,
): void {
  for (const id of state.keys()) deleteTransportOwned(state, transport, id);
}

function peerAdvertisedOpAuth(capabilities: readonly Capability[]): boolean {
  return capabilities.some(isAnyAuthCapability);
}

export class SyncPeer<Op> {
  private readonly maxCodewords: number;
  private readonly maxOpsPerBatch: number;
  private readonly maxHelloFilters: number;
  private readonly directSendThreshold: number;
  private readonly requireAuthForFilters: boolean;
  private readonly auth?: SyncAuth<Op>;
  private readonly deriveOpRef?: (op: Op, ctx: { docId: string }) => OpRef;
  private readonly transportHasAuth = new WeakMap<DuplexTransport<SyncMessage<Op>>, boolean>();
  private readonly transportPeerCapabilities = new WeakMap<
    DuplexTransport<SyncMessage<Op>>,
    Hello['capabilities']
  >();
  private readonly transportLastSentHelloCaps = new WeakMap<
    DuplexTransport<SyncMessage<Op>>,
    string
  >();
  private readonly transportHelloAckWaiters = new WeakMap<
    DuplexTransport<SyncMessage<Op>>,
    Set<Pending<void>>
  >();
  private readonly transportDirectPushStreamIds = new WeakMap<
    DuplexTransport<SyncMessage<Op>>,
    string
  >();
  private readonly responderSessions: TransportOwnedMap<Op, ResponderSession> = new Map();
  private readonly initiatorSessions = new Map<string, InitiatorSession<Op>>();
  private readonly responderSubscriptions: TransportOwnedMap<Op, ResponderSubscription<Op>> =
    new Map();
  private readonly initiatorSubscriptions = new Map<string, InitiatorSubscription<Op>>();
  private readonly responderAwaitingUploadAcks: TransportOwnedMap<Op, true> = new Map();
  private readonly opsBatchQueues: TransportOwnedMap<Op, Promise<void>> = new Map();
  private readonly pendingPushOpsByRefHex = new Map<string, PendingPushOp<Op>>();
  private pushNeedsFullScan = false;
  private pushScheduled = false;
  private pushRunning = false;
  private pushInFlight: Promise<void> = Promise.resolve();
  private reprocessPendingRunning = false;
  private reprocessPendingInFlight: Promise<void> = Promise.resolve();

  constructor(
    private readonly backend: SyncBackend<Op>,
    opts: SyncPeerOptions<Op> = {},
  ) {
    this.maxCodewords = opts.maxCodewords ?? 50_000;
    // Keep wire batches modest by default; large 5k-op frames were a real
    // source of remote ingest instability on production-like sync servers.
    this.maxOpsPerBatch = opts.maxOpsPerBatch ?? 500;
    this.auth = opts.auth;
    this.maxHelloFilters = opts.maxHelloFilters ?? 8;
    this.directSendThreshold = opts.directSendThreshold ?? 0;
    if (!Number.isInteger(this.directSendThreshold) || this.directSendThreshold < 0) {
      throw new Error(`invalid directSendThreshold: ${opts.directSendThreshold}`);
    }
    this.requireAuthForFilters = opts.requireAuthForFilters ?? Boolean(opts.auth);
    this.deriveOpRef = opts.deriveOpRef;
  }

  attach(
    transport: DuplexTransport<SyncMessage<Op>>,
    attachOpts: SyncPeerAttachOptions<Op> = {},
  ): () => void {
    let failed = false;
    let unsubscribeMessage: (() => void) | undefined;
    let unsubscribeTerminal: (() => void) | undefined;
    const stopListening = () => {
      ignoreErrors(unsubscribeMessage);
      unsubscribeMessage = undefined;
      ignoreErrors(unsubscribeTerminal);
      unsubscribeTerminal = undefined;
    };
    const fail = (error: unknown, report: boolean, close: boolean) => {
      if (failed) return;
      failed = true;
      stopListening();
      const normalized =
        error instanceof Error ? error : new Error(String(error ?? 'sync transport closed'));
      this.failPendingSessionsForTransport(transport, normalized);
      this.dropResponderStateForTransport(transport);
      if (close) ignoreErrors(() => transport.close?.(normalized));
      if (report) ignoreErrors(() => attachOpts.onError?.({ error: normalized, transport }));
    };

    unsubscribeMessage = transport.onMessage((msg) => {
      if (failed) return;
      void this.handleMessage(transport, msg)
        .catch((error) => {
          fail(error, true, true);
        })
        .finally(() => {
          if (failed) this.dropResponderStateForTransport(transport);
        });
    });
    if (failed) stopListening();

    if (!failed && transport.onTerminal) {
      const unsubscribe = transport.onTerminal((error) => fail(error, true, false));
      if (failed) ignoreErrors(unsubscribe);
      else unsubscribeTerminal = unsubscribe;
    }

    return () => fail(new Error('sync transport detached'), false, false);
  }

  // Live subscriptions are responder-owned: once a peer subscribes, local writes
  // feed this push loop until the subscription is removed or the transport fails.
  notifyLocalUpdate(ops?: readonly Op[]): Promise<void> {
    if (this.responderSubscriptions.size === 0) return Promise.resolve();
    if (ops && ops.length > 0 && this.deriveOpRef) {
      for (const op of ops) {
        const opRef = this.deriveOpRef(op, { docId: this.backend.docId });
        const opRefHex = bytesToHex(opRef);
        this.pendingPushOpsByRefHex.set(opRefHex, { opRef, opRefHex, op });
      }
    } else {
      this.pushNeedsFullScan = true;
    }
    this.pushScheduled = true;
    if (!this.pushRunning) {
      this.pushRunning = true;
      this.pushInFlight = this.pushInFlight
        .then(() => this.runPushLoop())
        .catch(() => {
          // best-effort: push failures should not permanently stall future pushes
        });
    }
    return this.pushInFlight;
  }

  private async runPushLoop(): Promise<void> {
    try {
      while (this.pushScheduled) {
        this.pushScheduled = false;
        // A full scan means "some subscription-relevant state changed, but we
        // do not have an exact delta set to push from". Delta pushes are only
        // safe when notifyLocalUpdate supplied concrete ops and deriveOpRef is available.
        const deltaOps =
          this.pushNeedsFullScan || this.pendingPushOpsByRefHex.size === 0
            ? []
            : Array.from(this.pendingPushOpsByRefHex.values());
        const forceFullScan = this.pushNeedsFullScan;
        this.pushNeedsFullScan = false;
        this.pendingPushOpsByRefHex.clear();
        for (const subscriptions of this.responderSubscriptions.values()) {
          for (const sub of subscriptions.values()) {
            try {
              await this.pushSubscription(sub, { deltaOps, forceFullScan });
            } catch {
              deleteTransportOwned(this.responderSubscriptions, sub.transport, sub.subscriptionId);
            }
            await yieldToMacrotask();
          }
        }
      }
    } finally {
      this.pushRunning = false;
    }
  }

  private async refreshHelloCapabilities(
    transport: DuplexTransport<SyncMessage<Op>>,
    opts: { force?: boolean; waitForAck?: boolean } = {},
  ): Promise<void> {
    if (!this.auth?.helloCapabilities) return;

    const [maxLamport, capabilities] = await Promise.all([
      this.backend.maxLamport(),
      this.auth.helloCapabilities({ docId: this.backend.docId }),
    ]);
    const fingerprint = capabilitySetFingerprint(capabilities);
    if (!opts.force && this.transportLastSentHelloCaps.get(transport) === fingerprint) return;

    let waiter: Pending<void> | undefined;
    if (opts.waitForAck) {
      waiter = deferred<void>();
      const waiters = this.transportHelloAckWaiters.get(transport) ?? new Set<Pending<void>>();
      waiters.add(waiter);
      this.transportHelloAckWaiters.set(transport, waiters);
    }

    try {
      await transport.send({
        v: 0,
        docId: this.backend.docId,
        payload: { case: 'hello', value: { capabilities, filters: [], maxLamport } },
      });
      this.transportLastSentHelloCaps.set(transport, fingerprint);
      await waiter?.promise;
    } catch (err) {
      if (waiter) {
        const waiters = this.transportHelloAckWaiters.get(transport);
        waiters?.delete(waiter);
        if (waiters && waiters.size === 0) this.transportHelloAckWaiters.delete(transport);
        waiter.reject(err);
      }
      throw err;
    }
  }

  private resolveDirectPushStreamId(
    transport: DuplexTransport<SyncMessage<Op>>,
    requestedStreamId?: string,
  ): string {
    if (requestedStreamId) return requestedStreamId;

    let streamId = this.transportDirectPushStreamIds.get(transport);
    if (!streamId) {
      streamId = randomId('push');
      this.transportDirectPushStreamIds.set(transport, streamId);
    }
    return streamId;
  }

  private resolveMaxOpsPerBatch(requestedBatchSize?: number): number {
    const batchSize = requestedBatchSize ?? this.maxOpsPerBatch;
    if (!Number.isFinite(batchSize) || batchSize <= 0) {
      throw new Error(`invalid maxOpsPerBatch: ${batchSize}`);
    }
    return batchSize;
  }

  private async sendDoneOpsBatch(
    transport: DuplexTransport<SyncMessage<Op>>,
    filterId: string,
  ): Promise<void> {
    await transport.send({
      v: 0,
      docId: this.backend.docId,
      payload: { case: 'opsBatch', value: { filterId, ops: [], done: true } },
    });
  }

  private async pushSubscription(
    sub: ResponderSubscription<Op>,
    opts: { deltaOps?: readonly PendingPushOp<Op>[]; forceFullScan?: boolean } = {},
  ): Promise<void> {
    // Only the unscoped "all" filter can use the exact delta list directly.
    // Scoped filters still need a backend rescan to decide which newly written
    // ops belong to the subscription.
    if (!opts.forceFullScan && 'all' in sub.filter && opts.deltaOps && opts.deltaOps.length > 0) {
      await this.pushSubscriptionDeltaAll(sub, opts.deltaOps);
      return;
    }

    let opRefs: OpRef[];
    try {
      opRefs = await this.backend.listOpRefs(sub.filter);
    } catch (err) {
      deleteTransportOwned(this.responderSubscriptions, sub.transport, sub.subscriptionId);
      return;
    }

    const newOpRefs: OpRef[] = [];
    // Even in the rescan path we stay incremental: sentOpRefs tracks what this
    // subscriber has already seen, so a "full scan" here means rediscovering
    // matching refs, not replaying the entire filter result.
    for (const r of opRefs) {
      const hex = bytesToHex(r);
      if (sub.sentOpRefs.has(hex)) continue;
      newOpRefs.push(r);
    }
    if (newOpRefs.length === 0) return;

    // Live subscriptions can outlive the capability snapshot from the initial handshake.
    // Refresh it before the push so proof_ref verification can succeed on newly seen authors.
    await this.refreshHelloCapabilities(sub.transport);

    for (let start = 0; start < newOpRefs.length; start += this.maxOpsPerBatch) {
      const chunk = newOpRefs.slice(start, start + this.maxOpsPerBatch);
      let ops = await this.backend.getOpsByOpRefs(chunk);
      const peerCaps = this.transportPeerCapabilities.get(sub.transport) ?? [];

      // Apply peer-scoped visibility restrictions (best-effort).
      if (this.auth?.filterOutgoingOps && ops.length > 0) {
        const allowed = await this.auth.filterOutgoingOps(ops, {
          docId: this.backend.docId,
          purpose: 'subscribe',
          filter: sub.filter,
          capabilities: peerCaps,
        });
        if (allowed.length !== ops.length) {
          throw new Error(
            `filterOutgoingOps returned ${allowed.length} flags for ${ops.length} ops`,
          );
        }

        const allowedRefs: OpRef[] = [];
        const allowedOps: Op[] = [];
        for (let i = 0; i < ops.length; i += 1) {
          if (allowed[i] === true) {
            allowedRefs.push(chunk[i]!);
            allowedOps.push(ops[i]!);
          }
        }

        // Record everything as sent so we don't repeatedly attempt to send filtered ops.
        for (const r of chunk) sub.sentOpRefs.add(bytesToHex(r));

        if (allowedOps.length === 0) {
          await yieldToMacrotask();
          continue;
        }

        ops = allowedOps;
        chunk.length = 0;
        chunk.push(...allowedRefs);
      }

      const shouldAttachAuth = peerAdvertisedOpAuth(peerCaps);
      const auth =
        shouldAttachAuth && this.auth?.signOps
          ? await this.auth.signOps(ops, {
              docId: this.backend.docId,
              purpose: 'subscribe',
              filterId: sub.subscriptionId,
            })
          : undefined;
      if (auth && auth.length !== ops.length)
        throw new Error(`signOps returned ${auth.length} entries for ${ops.length} ops`);
      const done = start + this.maxOpsPerBatch >= newOpRefs.length;
      await sub.transport.send({
        v: 0,
        docId: this.backend.docId,
        payload: {
          case: 'opsBatch',
          value: { filterId: sub.subscriptionId, ops, ...(auth ? { auth } : {}), done },
        },
      });

      for (const r of chunk) sub.sentOpRefs.add(bytesToHex(r));
      await yieldToMacrotask();
    }
  }

  private async pushSubscriptionDeltaAll(
    sub: ResponderSubscription<Op>,
    deltaOps: readonly PendingPushOp<Op>[],
  ): Promise<void> {
    // The exact-delta fast path is only safe for { all: true } subscriptions:
    // every new local op belongs to the filter, so we can skip listOpRefs().
    const unsent = deltaOps.filter((entry) => !sub.sentOpRefs.has(entry.opRefHex));
    if (unsent.length === 0) return;

    // Live subscriptions can outlive the capability snapshot from the initial handshake.
    // Refresh it before the push so proof_ref verification can succeed on newly seen authors.
    await this.refreshHelloCapabilities(sub.transport);

    const peerCaps = this.transportPeerCapabilities.get(sub.transport) ?? [];
    const filter = sub.filter;
    const maxOpsPerBatch = this.maxOpsPerBatch;

    for (let start = 0; start < unsent.length; start += maxOpsPerBatch) {
      const chunk = unsent.slice(start, start + maxOpsPerBatch);
      let refs = chunk.map((entry) => entry.opRef);
      let ops = chunk.map((entry) => entry.op);

      if (this.auth?.filterOutgoingOps && ops.length > 0) {
        const allowed = await this.auth.filterOutgoingOps(ops, {
          docId: this.backend.docId,
          purpose: 'subscribe',
          filter,
          capabilities: peerCaps,
        });
        if (allowed.length !== ops.length) {
          throw new Error(
            `filterOutgoingOps returned ${allowed.length} flags for ${ops.length} ops`,
          );
        }

        const allowedRefs: OpRef[] = [];
        const allowedOps: Op[] = [];
        for (let i = 0; i < ops.length; i += 1) {
          if (allowed[i] === true) {
            allowedRefs.push(refs[i]!);
            allowedOps.push(ops[i]!);
          }
        }

        for (const ref of refs) sub.sentOpRefs.add(bytesToHex(ref));

        if (allowedOps.length === 0) {
          await yieldToMacrotask();
          continue;
        }

        refs = allowedRefs;
        ops = allowedOps;
      }

      const shouldAttachAuth = peerAdvertisedOpAuth(peerCaps);
      const auth =
        shouldAttachAuth && this.auth?.signOps
          ? await this.auth.signOps(ops, {
              docId: this.backend.docId,
              purpose: 'subscribe',
              filterId: sub.subscriptionId,
            })
          : undefined;
      if (auth && auth.length !== ops.length) {
        throw new Error(`signOps returned ${auth.length} entries for ${ops.length} ops`);
      }

      const done = start + maxOpsPerBatch >= unsent.length;
      await sub.transport.send({
        v: 0,
        docId: this.backend.docId,
        payload: {
          case: 'opsBatch',
          value: { filterId: sub.subscriptionId, ops, ...(auth ? { auth } : {}), done },
        },
      });

      for (const ref of refs) sub.sentOpRefs.add(bytesToHex(ref));
      await yieldToMacrotask();
    }
  }

  async syncOnce(
    transport: DuplexTransport<SyncMessage<Op>>,
    filter: Filter,
    opts: SyncOnceOptions = {},
  ): Promise<void> {
    // syncOnce negotiates one of three wire modes for this filter:
    // 1. the normal RIBLT reconcile path,
    // 2. direct-send for small scoped reads, or
    // 3. direct-send upload when the initiator is an empty receiver.
    // The capability exchange below advertises support and lets the peer pick the cheaper mode.
    const filterId = randomId('f');
    const round = 0;
    const maxLamport = await this.backend.maxLamport();
    const localOpRefsBeforeHello = await this.backend.listOpRefs(filter);
    const capabilities = buildInitiatorHelloCapabilities(
      (await this.auth?.helloCapabilities?.({ docId: this.backend.docId })) ?? [],
      { filterId, localHasOps: localOpRefsBeforeHello.length > 0 },
    );
    const hello: Hello = { capabilities, filters: [{ id: filterId, filter }], maxLamport };

    const session: InitiatorSession<Op> = {
      transport,
      filter,
      filterId,
      round,
      ack: deferred<HelloAck>(),
      terminalStatus: deferred<RibltStatus>(),
      codewordCredits: 1,
      codewordCreditSignal: deferred<void>(),
      receivedOps: deferred<void>(),
      awaitingUploadAck: false,
      done: false,
    };
    this.initiatorSessions.set(filterId, session);

    try {
      await transport.send({
        v: 0,
        docId: this.backend.docId,
        payload: { case: 'hello', value: hello },
      });
      const ack = await session.ack.promise;

      // For tiny scoped reads the responder can skip RIBLT entirely and send
      // the result as direct ops once Hello/HelloAck agrees on that shortcut.
      if (
        localOpRefsBeforeHello.length === 0 &&
        peerSelectedDirectSendFilter(ack.capabilities, filterId)
      ) {
        await session.receivedOps.promise;
        return;
      }

      let opRefs = await this.backend.listOpRefs(filter);

      // If we have peer capabilities (from HelloAck) and an auth layer that can scope outgoing ops,
      // filter the local set to avoid advertising/sending ops the peer cannot receive.
      if (this.auth?.filterOutgoingOps && opRefs.length > 0) {
        const peerCaps = this.transportPeerCapabilities.get(transport) ?? [];
        const ops = await this.backend.getOpsByOpRefs(opRefs);
        const allowed = await this.auth.filterOutgoingOps(ops, {
          docId: this.backend.docId,
          purpose: 'reconcile',
          filter,
          capabilities: peerCaps,
        });
        if (allowed.length !== ops.length) {
          throw new Error(
            `filterOutgoingOps returned ${allowed.length} flags for ${ops.length} ops`,
          );
        }
        opRefs = opRefs.filter((_r, idx) => allowed[idx] === true);
      }

      if (peerSelectedDirectSendEmptyReceiverFilter(ack.capabilities, filterId)) {
        session.awaitingUploadAck = true;
        const uploadMaxOpsPerBatch =
          opts.maxOpsPerBatch ?? DIRECT_SEND_EMPTY_RECEIVER_MAX_OPS_PER_BATCH;
        if (opRefs.length > 0) {
          await this.sendOpsBatches(transport, filterId, opRefs, {
            maxOpsPerBatch: uploadMaxOpsPerBatch,
            filter,
          });
        } else {
          await this.sendDoneOpsBatch(transport, filterId);
        }
        await session.receivedOps.promise;
        return;
      }

      const { RibltEncoder16 } = await import('@treecrdt/riblt-wasm');
      const enc = new RibltEncoder16();
      for (const r of opRefs) enc.addSymbol(r);

      const codewordsPerMessage = opts.codewordsPerMessage ?? 512;
      const maxCodewords = BigInt(opts.maxCodewords ?? 50_000);

      let nextIndex = 0n;
      while (!session.done && nextIndex < maxCodewords) {
        if (session.codewordCredits <= 0) {
          const wakeForCredits = session.codewordCreditSignal.promise;
          await Promise.race([
            session.terminalStatus.promise.then(
              () => undefined,
              () => undefined,
            ),
            wakeForCredits,
          ]);
          continue;
        }

        session.codewordCredits -= 1;
        const startIndex = nextIndex;
        const codewords: RibltCodewords['codewords'] = [];
        for (let i = 0; i < codewordsPerMessage && nextIndex < maxCodewords; i += 1) {
          codewords.push(enc.nextCodeword() as any);
          nextIndex += 1n;
        }

        await transport.send({
          v: 0,
          docId: this.backend.docId,
          payload: {
            case: 'ribltCodewords',
            value: { filterId, round, startIndex, codewords },
          },
        });
        await yieldToMacrotask();
      }

      if (!session.done) throw new Error('riblt: max codewords exceeded');

      const status = await session.terminalStatus.promise;
      if (status.payload.case === 'failed') {
        const { reason, message } = status.payload.value;
        const name = RibltFailureReason[reason] ?? String(reason);
        const detail = message ? `: ${message}` : '';
        throw new Error(`riblt: ${name}${detail}`);
      }

      const receiverMissing =
        status.payload.case === 'decoded' ? status.payload.value.receiverMissing : [];
      if (receiverMissing.length > 0) {
        await this.sendOpsBatches(transport, filterId, receiverMissing, {
          maxOpsPerBatch: opts.maxOpsPerBatch,
        });
      } else {
        await this.sendDoneOpsBatch(transport, filterId);
      }

      await session.receivedOps.promise;
    } finally {
      this.initiatorSessions.delete(filterId);
    }
  }

  /**
   * Send a known set of ops directly without first reconciling state via
   * `syncOnce()`.
   *
   * The `opsBatch` wire format still carries a `filterId`; for direct pushes
   * that field acts only as a stable stream id so the receiver can order chunks
   * and interpret the final `done` marker correctly.
   */
  async pushOps(
    transport: DuplexTransport<SyncMessage<Op>>,
    ops: readonly Op[],
    opts: SyncPushOptions = {},
  ): Promise<void> {
    if (ops.length === 0) return;

    await this.refreshHelloCapabilities(transport, { waitForAck: true });

    const streamId = this.resolveDirectPushStreamId(transport, opts.filterId);
    const batchSize = this.resolveMaxOpsPerBatch(opts.maxOpsPerBatch);

    const peerCapabilities = this.transportPeerCapabilities.get(transport) ?? [];
    const shouldAttachAuth = peerAdvertisedOpAuth(peerCapabilities);

    for (let start = 0; start < ops.length; start += batchSize) {
      const chunk = ops.slice(start, start + batchSize);
      const auth =
        shouldAttachAuth && this.auth?.signOps
          ? await this.auth.signOps(chunk, {
              docId: this.backend.docId,
              purpose: 'reconcile',
              filterId: streamId,
            })
          : undefined;
      if (auth && auth.length !== chunk.length) {
        throw new Error(`signOps returned ${auth.length} entries for ${chunk.length} ops`);
      }

      await transport.send({
        v: 0,
        docId: this.backend.docId,
        payload: {
          case: 'opsBatch',
          value: {
            filterId: streamId,
            ops: [...chunk],
            ...(auth ? { auth } : {}),
            done: start + batchSize >= ops.length,
          },
        },
      });
      await yieldToMacrotask();
    }
  }

  private async sendOpsBatches(
    transport: DuplexTransport<SyncMessage<Op>>,
    filterId: string,
    opRefs: OpRef[],
    opts: { maxOpsPerBatch?: number; filter?: Filter } = {},
  ): Promise<void> {
    const maxOpsPerBatch = this.resolveMaxOpsPerBatch(opts.maxOpsPerBatch);

    const initiatorSession = this.initiatorSessions.get(filterId);
    const filter =
      opts.filter ??
      getTransportOwned(this.responderSessions, transport, filterId)?.filter ??
      (initiatorSession?.transport === transport ? initiatorSession.filter : undefined);
    const peerCaps = this.transportPeerCapabilities.get(transport) ?? [];

    if (opRefs.length === 0) {
      await this.sendDoneOpsBatch(transport, filterId);
      return;
    }

    for (let start = 0; start < opRefs.length; start += maxOpsPerBatch) {
      const chunk = opRefs.slice(start, start + maxOpsPerBatch);
      let ops = await this.backend.getOpsByOpRefs(chunk);

      if (filter && this.auth?.filterOutgoingOps && ops.length > 0) {
        const allowed = await this.auth.filterOutgoingOps(ops, {
          docId: this.backend.docId,
          purpose: 'reconcile',
          filter,
          capabilities: peerCaps,
        });
        if (allowed.length !== ops.length) {
          throw new Error(
            `filterOutgoingOps returned ${allowed.length} flags for ${ops.length} ops`,
          );
        }

        const nextOps: Op[] = [];
        for (let i = 0; i < ops.length; i += 1) {
          if (allowed[i] === true) nextOps.push(ops[i]!);
        }
        ops = nextOps;
      }

      const done = start + maxOpsPerBatch >= opRefs.length;

      if (ops.length === 0) {
        if (done) {
          await this.sendDoneOpsBatch(transport, filterId);
        }
        continue;
      }

      const shouldAttachAuth = peerAdvertisedOpAuth(peerCaps);
      const auth =
        shouldAttachAuth && this.auth?.signOps
          ? await this.auth.signOps(ops, {
              docId: this.backend.docId,
              purpose: 'reconcile',
              filterId,
            })
          : undefined;
      if (auth && auth.length !== ops.length) {
        throw new Error(`signOps returned ${auth.length} entries for ${ops.length} ops`);
      }

      await transport.send({
        v: 0,
        docId: this.backend.docId,
        payload: { case: 'opsBatch', value: { filterId, ops, ...(auth ? { auth } : {}), done } },
      });
      await yieldToMacrotask();
    }
  }

  subscribe(
    transport: DuplexTransport<SyncMessage<Op>>,
    filter: Filter,
    opts: SyncSubscribeOptions = {},
  ): SyncSubscription {
    const controller = new AbortController();
    if (opts.signal) {
      if (opts.signal.aborted) {
        controller.abort();
      } else {
        opts.signal.addEventListener('abort', () => controller.abort(), { once: true });
      }
    }

    const signal = controller.signal;
    const intervalMs = opts.intervalMs ?? 0;
    const immediate = opts.immediate ?? true;
    const codewordsPerMessage = opts.codewordsPerMessage;
    const maxCodewords = opts.maxCodewords;
    const maxOpsPerBatch = opts.maxOpsPerBatch;

    const subscriptionId = randomId('sub');
    const session: InitiatorSubscription<Op> = {
      transport,
      ack: deferred<SubscribeAck>(),
      failed: deferred<never>(),
    };
    const failed = session.failed.promise;
    this.initiatorSubscriptions.set(subscriptionId, session);
    const ready = deferred<void>();
    let readySettled = false;
    const resolveReady = () => {
      if (readySettled) return;
      readySettled = true;
      ready.resolve();
    };
    const rejectReady = (err: unknown) => {
      if (readySettled) return;
      readySettled = true;
      ready.reject(err);
    };

    const done = (async () => {
      let sentSubscribe = false;
      try {
        if (signal.aborted) {
          resolveReady();
          return;
        }

        // If the responder requires capability-gated filters/subscriptions, send an initial
        // Hello (no filters) so it can record our capabilities before Subscribe arrives.
        if (this.auth?.helloCapabilities) {
          await this.refreshHelloCapabilities(transport, { force: true });
        }

        await transport.send({
          v: 0,
          docId: this.backend.docId,
          payload: { case: 'subscribe', value: { subscriptionId, filter } },
        });
        sentSubscribe = true;

        const ackOrAbort = await Promise.race([
          session.ack.promise.then(() => ({ case: 'ack' as const })),
          failed,
          waitForAbort(signal).then(() => ({ case: 'aborted' as const })),
        ]);
        if (ackOrAbort.case === 'aborted') {
          resolveReady();
          return;
        }
        if (signal.aborted) {
          resolveReady();
          return;
        }
        if (immediate) {
          await this.syncOnce(transport, filter, {
            codewordsPerMessage,
            maxCodewords,
            maxOpsPerBatch,
          });
        }
        resolveReady();

        if (intervalMs > 0) {
          while (!signal.aborted) {
            const slept = await Promise.race([sleepUntil(intervalMs, signal), failed]);
            if (!slept) break;
            if (signal.aborted) break;
            await this.syncOnce(transport, filter, {
              codewordsPerMessage,
              maxCodewords,
              maxOpsPerBatch,
            });
          }
        } else {
          await Promise.race([waitForAbort(signal), failed]);
        }
      } catch (err) {
        rejectReady(err);
        throw err;
      } finally {
        controller.abort();
        resolveReady();
        this.initiatorSubscriptions.delete(subscriptionId);
        if (sentSubscribe) {
          try {
            await transport.send({
              v: 0,
              docId: this.backend.docId,
              payload: { case: 'unsubscribe', value: { subscriptionId } },
            });
          } catch {
            // ignore transport failures during teardown
          }
        }
      }
    })();

    return { stop: () => controller.abort(), ready: ready.promise, done };
  }

  private async handleMessage(
    transport: DuplexTransport<SyncMessage<Op>>,
    msg: SyncMessage<Op>,
  ): Promise<void> {
    if (msg.docId !== this.backend.docId) return;

    if (msg.payload.case === 'error') {
      try {
        await this.onError(transport, msg.payload.value);
      } catch {
        // ignore error while handling error
      }
      return;
    }

    try {
      switch (msg.payload.case) {
        case 'hello':
          await this.onHello(transport, msg.payload.value);
          return;
        case 'helloAck':
          await this.onHelloAck(transport, msg.payload.value);
          return;
        case 'ribltCodewords':
          await this.onRibltCodewords(transport, msg.payload.value);
          return;
        case 'ribltStatus':
          await this.onRibltStatus(transport, msg.payload.value);
          return;
        case 'opsBatch':
          await this.enqueueOpsBatch(transport, msg.payload.value);
          return;
        case 'subscribe':
          await this.onSubscribe(transport, msg.payload.value);
          return;
        case 'subscribeAck':
          await this.onSubscribeAck(transport, msg.payload.value);
          return;
        case 'unsubscribe':
          await this.onUnsubscribe(transport, msg.payload.value);
          return;
        default: {
          const _exhaustive: never = msg.payload;
          return _exhaustive;
        }
      }
    } catch (err: any) {
      let filterId: string | undefined;
      let subscriptionId: string | undefined;
      switch (msg.payload.case) {
        case 'ribltCodewords':
        case 'ribltStatus':
        case 'opsBatch':
          filterId = msg.payload.value.filterId;
          if (
            msg.payload.case === 'opsBatch' &&
            this.initiatorSubscriptions.get(filterId)?.transport === transport
          ) {
            subscriptionId = filterId;
          }
          break;
        case 'subscribe':
        case 'subscribeAck':
        case 'unsubscribe':
          subscriptionId = msg.payload.value.subscriptionId;
          break;
      }

      try {
        await this.onError(transport, {
          code: ErrorCode.ERROR_CODE_UNSPECIFIED,
          message: String(err?.message ?? err ?? 'error'),
          ...(filterId ? { filterId } : {}),
          ...(subscriptionId ? { subscriptionId } : {}),
        });

        await transport.send({
          v: 0,
          docId: this.backend.docId,
          payload: {
            case: 'error',
            value: {
              code: ErrorCode.ERROR_CODE_UNSPECIFIED,
              message: String(err?.message ?? err ?? 'error'),
              ...(filterId ? { filterId } : {}),
              ...(subscriptionId ? { subscriptionId } : {}),
            },
          },
        });
      } catch {
        // ignore transport failures while reporting errors
      }
    }
  }

  private async onHello(transport: DuplexTransport<SyncMessage<Op>>, hello: Hello): Promise<void> {
    const traceStartedAt = performance.now();
    traceHello(this.backend.docId, traceStartedAt, 'start', {
      filters: hello.filters.length,
      capabilities: hello.capabilities.length,
    });
    const hasAuthCapability = hello.capabilities.some(isAuthCapability);
    const supportsDirectSendSmallScope = peerSupportsDirectSendSmallScope(hello.capabilities);

    // Record the presence of auth capabilities immediately so concurrent messages (e.g. Subscribe)
    // can't race and get rejected before `onHello` completes.
    if (hasAuthCapability) this.transportHasAuth.set(transport, true);
    if (peerAdvertisedOpAuth(hello.capabilities))
      this.transportPeerCapabilities.set(transport, hello.capabilities);

    let ackCapabilities: HelloAck['capabilities'] = [];
    try {
      ackCapabilities = (await this.auth?.onHello?.(hello, { docId: this.backend.docId })) ?? [];
      traceHello(this.backend.docId, traceStartedAt, 'after-auth-onHello', {
        ackCapabilities: ackCapabilities.length,
      });
    } catch (err: any) {
      await transport.send({
        v: 0,
        docId: this.backend.docId,
        payload: {
          case: 'error',
          value: {
            code: ErrorCode.ERROR_CODE_UNSPECIFIED,
            message: String(err?.message ?? err ?? 'auth error'),
          },
        },
      });
      return;
    }

    const maxLamport = await this.backend.maxLamport();
    traceHello(this.backend.docId, traceStartedAt, 'after-maxLamport', {
      maxLamport: Number(maxLamport),
    });
    const acceptedFilters: string[] = [];
    const rejectedFilters: HelloAck['rejectedFilters'] = [];
    const directSendFilters: Array<{
      id: string;
      filter: Filter;
      opRefs: OpRef[];
    }> = [];

    for (let i = 0; i < hello.filters.length; i += 1) {
      const spec = hello.filters[i]!;
      const id = spec.id;
      const filter = spec.filter;
      if (!id || !filter) continue;

      if (i >= this.maxHelloFilters) {
        rejectedFilters.push({
          id,
          reason: ErrorCode.TOO_MANY_FILTERS,
          message: `max filters per Hello exceeded (${this.maxHelloFilters})`,
        });
        continue;
      }

      if (this.requireAuthForFilters && !hasAuthCapability) {
        rejectedFilters.push({
          id,
          reason: ErrorCode.UNAUTHORIZED,
          message: `missing "${AUTH_CAPABILITY_NAME}" token; send a valid capability token in Hello.capabilities`,
        });
        continue;
      }

      try {
        await this.auth?.authorizeFilter?.(filter, {
          docId: this.backend.docId,
          purpose: 'hello',
          capabilities: hello.capabilities,
        });
      } catch (err: any) {
        rejectedFilters.push({
          id,
          reason: ErrorCode.UNAUTHORIZED,
          message: String(err?.message ?? err ?? 'unauthorized filter'),
        });
        continue;
      }

      let localOpRefs: OpRef[];
      try {
        localOpRefs = await this.backend.listOpRefs(filter);
        traceHello(this.backend.docId, traceStartedAt, 'after-listOpRefs', {
          filterId: id,
          opRefs: localOpRefs.length,
        });
      } catch (err: any) {
        rejectedFilters.push({
          id,
          reason: ErrorCode.FILTER_NOT_SUPPORTED,
          message: String(err?.message ?? err ?? 'filter not supported'),
        });
        continue;
      }

      if (!('all' in filter) && this.auth?.filterOutgoingOps && localOpRefs.length > 0) {
        try {
          const ops = await this.backend.getOpsByOpRefs(localOpRefs);
          const allowed = await this.auth.filterOutgoingOps(ops, {
            docId: this.backend.docId,
            purpose: 'hello',
            filter,
            capabilities: hello.capabilities,
          });
          if (allowed.length !== ops.length) {
            throw new Error(
              `filterOutgoingOps returned ${allowed.length} flags for ${ops.length} ops`,
            );
          }
          localOpRefs = localOpRefs.filter((_r, idx) => allowed[idx] === true);
          traceHello(this.backend.docId, traceStartedAt, 'after-filterOutgoingOps', {
            filterId: id,
            fetchedOps: ops.length,
            allowedOpRefs: localOpRefs.length,
          });
        } catch (err: any) {
          rejectedFilters.push({
            id,
            reason: ErrorCode.UNAUTHORIZED,
            message: String(err?.message ?? err ?? 'failed to filter ops'),
          });
          continue;
        }
      }

      acceptedFilters.push(id);

      if (
        supportsDirectSendSmallScope &&
        this.directSendThreshold > 0 &&
        peerRequestedDirectSendFilter(hello.capabilities, id) &&
        localOpRefs.length <= this.directSendThreshold
      ) {
        ackCapabilities.push({
          name: DIRECT_SEND_SMALL_SCOPE_FILTER_CAPABILITY,
          value: id,
        });
        directSendFilters.push({
          id,
          filter,
          opRefs: localOpRefs,
        });
        traceHello(this.backend.docId, traceStartedAt, 'after-direct-send-selection', {
          filterId: id,
          opRefs: localOpRefs.length,
        });
        continue;
      }

      if (peerSupportsDirectSendEmptyReceiver(hello.capabilities) && localOpRefs.length === 0) {
        ackCapabilities.push({
          name: DIRECT_SEND_EMPTY_RECEIVER_FILTER_CAPABILITY,
          value: id,
        });
        setTransportOwned(this.responderAwaitingUploadAcks, transport, id, true);
        continue;
      }

      const { RibltDecoder16 } = await import('@treecrdt/riblt-wasm');
      const decoder = new RibltDecoder16();
      for (const r of localOpRefs) decoder.addLocalSymbol(r);
      traceHello(this.backend.docId, traceStartedAt, 'after-decoder-setup', {
        filterId: id,
        opRefs: localOpRefs.length,
      });
      setTransportOwned(this.responderSessions, transport, id, {
        filter,
        round: 0,
        decoder,
        expectedIndex: 0n,
        awaitingIncomingDone: false,
      });
    }

    await transport.send({
      v: 0,
      docId: this.backend.docId,
      payload: {
        case: 'helloAck',
        value: {
          capabilities: ackCapabilities,
          acceptedFilters,
          rejectedFilters,
          maxLamport,
        },
      },
    });
    traceHello(this.backend.docId, traceStartedAt, 'after-helloAck-send', {
      acceptedFilters: acceptedFilters.length,
      rejectedFilters: rejectedFilters.length,
    });

    for (const directSend of directSendFilters) {
      await this.sendOpsBatches(transport, directSend.id, directSend.opRefs, {
        filter: directSend.filter,
      });
    }
  }

  private async onHelloAck(
    transport: DuplexTransport<SyncMessage<Op>>,
    ack: HelloAck,
  ): Promise<void> {
    await this.auth?.onHelloAck?.(ack, { docId: this.backend.docId });

    const hasAuthCapability = ack.capabilities.some(isAuthCapability);
    if (hasAuthCapability) this.transportHasAuth.set(transport, true);
    if (peerAdvertisedOpAuth(ack.capabilities))
      this.transportPeerCapabilities.set(transport, ack.capabilities);
    const waiters = this.transportHelloAckWaiters.get(transport);
    if (waiters && waiters.size > 0) {
      this.transportHelloAckWaiters.delete(transport);
      for (const waiter of waiters) waiter.resolve();
    }

    for (const id of ack.acceptedFilters) {
      const session = this.initiatorSessions.get(id);
      if (session?.transport === transport) session.ack.resolve(ack);
    }
    for (const rej of ack.rejectedFilters) {
      const session = this.initiatorSessions.get(rej.id);
      if (session?.transport === transport) {
        const reason = ErrorCode[rej.reason] ?? String(rej.reason);
        const detail = rej.message ? `: ${rej.message}` : '';
        session.ack.reject(new Error(`${reason}${detail}`));
      }
    }
  }

  private async onRibltCodewords(
    transport: DuplexTransport<SyncMessage<Op>>,
    msg: RibltCodewords,
  ): Promise<void> {
    const session = getTransportOwned(this.responderSessions, transport, msg.filterId);
    if (!session) return;

    if (msg.round !== session.round) return;
    if (msg.startIndex !== session.expectedIndex) {
      await transport.send({
        v: 0,
        docId: this.backend.docId,
        payload: {
          case: 'ribltStatus',
          value: {
            filterId: msg.filterId,
            round: msg.round,
            payload: { case: 'failed', value: { reason: RibltFailureReason.OUT_OF_ORDER } },
          },
        },
      });
      deleteTransportOwned(this.responderSessions, transport, msg.filterId);
      return;
    }

    try {
      for (const cw of msg.codewords) {
        if (session.expectedIndex >= BigInt(this.maxCodewords)) break;
        session.decoder.addCodeword(cw as any);
        session.expectedIndex += 1n;
      }
      session.decoder.tryDecode();
    } catch (err: any) {
      await transport.send({
        v: 0,
        docId: this.backend.docId,
        payload: {
          case: 'ribltStatus',
          value: {
            filterId: msg.filterId,
            round: msg.round,
            payload: {
              case: 'failed',
              value: {
                reason: RibltFailureReason.DECODE_FAILED,
                message: String(err?.message ?? err ?? ''),
              },
            },
          },
        },
      });
      deleteTransportOwned(this.responderSessions, transport, msg.filterId);
      return;
    }

    if (!session.decoder.decoded()) {
      if (session.expectedIndex >= BigInt(this.maxCodewords)) {
        await transport.send({
          v: 0,
          docId: this.backend.docId,
          payload: {
            case: 'ribltStatus',
            value: {
              filterId: msg.filterId,
              round: msg.round,
              payload: {
                case: 'failed',
                value: { reason: RibltFailureReason.MAX_CODEWORDS_EXCEEDED },
              },
            },
          },
        });
        deleteTransportOwned(this.responderSessions, transport, msg.filterId);
      } else {
        await transport.send({
          v: 0,
          docId: this.backend.docId,
          payload: {
            case: 'ribltStatus',
            value: {
              filterId: msg.filterId,
              round: msg.round,
              payload: {
                case: 'more',
                value: { codewordsReceived: session.decoder.codewordsReceived(), credits: 1 },
              },
            },
          },
        });
      }
      return;
    }

    const receiverMissing = session.decoder.remoteMissing() as unknown as OpRef[];
    const senderMissing = session.decoder.localMissing() as unknown as OpRef[];
    const codewordsReceived = session.decoder.codewordsReceived();
    session.awaitingIncomingDone = receiverMissing.length > 0;

    await transport.send({
      v: 0,
      docId: this.backend.docId,
      payload: {
        case: 'ribltStatus',
        value: {
          filterId: msg.filterId,
          round: msg.round,
          payload: {
            case: 'decoded',
            value: { senderMissing, receiverMissing, codewordsReceived },
          },
        },
      },
    });

    if (senderMissing.length > 0) {
      await this.sendOpsBatches(transport, msg.filterId, senderMissing);
      if (!session.awaitingIncomingDone) {
        deleteTransportOwned(this.responderSessions, transport, msg.filterId);
      }
    } else if (!session.awaitingIncomingDone) {
      await this.sendDoneOpsBatch(transport, msg.filterId);
      deleteTransportOwned(this.responderSessions, transport, msg.filterId);
    }
  }

  private async onSubscribe(
    transport: DuplexTransport<SyncMessage<Op>>,
    msg: Subscribe,
  ): Promise<void> {
    if (!msg.subscriptionId) return;
    if (!msg.filter) return;

    if (this.requireAuthForFilters && !this.transportHasAuth.get(transport)) {
      await transport.send({
        v: 0,
        docId: this.backend.docId,
        payload: {
          case: 'error',
          value: {
            code: ErrorCode.UNAUTHORIZED,
            message: `missing "${AUTH_CAPABILITY_NAME}" token; send Hello before Subscribe`,
            subscriptionId: msg.subscriptionId,
          },
        },
      });
      return;
    }

    try {
      const peerCaps = this.transportPeerCapabilities.get(transport) ?? [];
      await this.auth?.authorizeFilter?.(msg.filter, {
        docId: this.backend.docId,
        purpose: 'subscribe',
        capabilities: peerCaps,
      });
    } catch (err: any) {
      await transport.send({
        v: 0,
        docId: this.backend.docId,
        payload: {
          case: 'error',
          value: {
            code: ErrorCode.UNAUTHORIZED,
            message: String(err?.message ?? err ?? 'unauthorized filter'),
            subscriptionId: msg.subscriptionId,
          },
        },
      });
      return;
    }

    try {
      const [opRefs, maxLamport] = await Promise.all([
        this.backend.listOpRefs(msg.filter),
        this.backend.maxLamport(),
      ]);

      const sentOpRefs = new Set<string>();
      for (const r of opRefs) sentOpRefs.add(bytesToHex(r));

      setTransportOwned(this.responderSubscriptions, transport, msg.subscriptionId, {
        subscriptionId: msg.subscriptionId,
        filter: msg.filter,
        sentOpRefs,
        transport,
      });

      await transport.send({
        v: 0,
        docId: this.backend.docId,
        payload: {
          case: 'subscribeAck',
          value: { subscriptionId: msg.subscriptionId, currentLamport: maxLamport },
        },
      });

      // Close a race where local updates happen between `listOpRefs` and the caller
      // registering its own update hooks; this makes subscriptions robust even
      // without explicit `notifyLocalUpdate()` calls for every writer.
      void this.notifyLocalUpdate();
    } catch (err: any) {
      await transport.send({
        v: 0,
        docId: this.backend.docId,
        payload: {
          case: 'error',
          value: {
            code: ErrorCode.FILTER_NOT_SUPPORTED,
            message: String(err?.message ?? err ?? 'subscribe failed'),
            subscriptionId: msg.subscriptionId,
          },
        },
      });
    }
  }

  private async onSubscribeAck(
    transport: DuplexTransport<SyncMessage<Op>>,
    ack: SubscribeAck,
  ): Promise<void> {
    const sub = this.initiatorSubscriptions.get(ack.subscriptionId);
    if (!sub || sub.transport !== transport) return;
    sub.ack.resolve(ack);
  }

  private async onUnsubscribe(
    transport: DuplexTransport<SyncMessage<Op>>,
    msg: Unsubscribe,
  ): Promise<void> {
    deleteTransportOwned(this.responderSubscriptions, transport, msg.subscriptionId);
  }

  private async onError(
    transport: DuplexTransport<SyncMessage<Op>>,
    err: {
      code: ErrorCode;
      message: string;
      filterId?: string;
      subscriptionId?: string;
    },
  ): Promise<void> {
    const code = ErrorCode[err.code] ?? String(err.code);
    const e = new Error(`${code}: ${err.message}`);

    if (err.subscriptionId) {
      const sub = this.initiatorSubscriptions.get(err.subscriptionId);
      if (sub?.transport === transport) {
        sub.ack.reject(e);
        sub.failed.reject(e);
        this.initiatorSubscriptions.delete(err.subscriptionId);
      }
    }

    if (err.filterId) {
      const session = this.initiatorSessions.get(err.filterId);
      if (session?.transport === transport) {
        rejectInitiatorSession(session, e);
        this.initiatorSessions.delete(err.filterId);
      }
      return;
    }

    if (err.subscriptionId) return;

    for (const [filterId, session] of this.initiatorSessions) {
      if (session.transport !== transport) continue;
      rejectInitiatorSession(session, e);
      this.initiatorSessions.delete(filterId);
    }
  }

  private failPendingSessionsForTransport(
    transport: DuplexTransport<SyncMessage<Op>>,
    error: unknown,
  ): void {
    const e = error instanceof Error ? error : new Error(String(error));

    const helloWaiters = this.transportHelloAckWaiters.get(transport);
    if (helloWaiters) {
      this.transportHelloAckWaiters.delete(transport);
      for (const waiter of helloWaiters) waiter.reject(e);
    }

    for (const [subscriptionId, sub] of this.initiatorSubscriptions) {
      if (sub.transport !== transport) continue;
      sub.ack.reject(e);
      sub.failed.reject(e);
      this.initiatorSubscriptions.delete(subscriptionId);
    }

    for (const [filterId, session] of this.initiatorSessions) {
      if (session.transport !== transport) continue;
      rejectInitiatorSession(session, e);
      this.initiatorSessions.delete(filterId);
    }
  }

  private dropResponderStateForTransport(transport: DuplexTransport<SyncMessage<Op>>): void {
    dropTransportOwned(this.responderSessions, transport);
    dropTransportOwned(this.responderSubscriptions, transport);
    dropTransportOwned(this.responderAwaitingUploadAcks, transport);
    dropTransportOwned(this.opsBatchQueues, transport);
  }

  private async onRibltStatus(
    transport: DuplexTransport<SyncMessage<Op>>,
    status: RibltStatus,
  ): Promise<void> {
    const session = this.initiatorSessions.get(status.filterId);
    if (!session || session.transport !== transport) return;
    if (status.round !== session.round) return;
    if (session.done) return;
    if (status.payload.case === 'more') {
      const credits = Math.max(1, Math.trunc(status.payload.value.credits));
      session.codewordCredits += credits;
      const signal = session.codewordCreditSignal;
      session.codewordCreditSignal = deferred<void>();
      signal.resolve();
      return;
    }
    session.done = true;
    if (status.payload.case === 'decoded') {
      session.awaitingUploadAck = status.payload.value.receiverMissing.length > 0;
    }
    session.terminalStatus.resolve(status);
    const signal = session.codewordCreditSignal;
    session.codewordCreditSignal = deferred<void>();
    signal.resolve();
  }

  private async enqueueOpsBatch(
    transport: DuplexTransport<SyncMessage<Op>>,
    batch: OpsBatch<Op>,
  ): Promise<void> {
    // Apply batches sequentially per transport/filter so a later done marker cannot overtake
    // earlier ops without making an unrelated transport wait on the same peer-chosen id.
    const previous =
      getTransportOwned(this.opsBatchQueues, transport, batch.filterId) ?? Promise.resolve();
    const current = previous
      .catch(() => {
        // A prior batch failure should not permanently poison the queue.
      })
      .then(() => this.onOpsBatch(transport, batch));
    setTransportOwned(this.opsBatchQueues, transport, batch.filterId, current);
    try {
      await current;
    } finally {
      if (getTransportOwned(this.opsBatchQueues, transport, batch.filterId) === current) {
        deleteTransportOwned(this.opsBatchQueues, transport, batch.filterId);
      }
    }
  }

  private async onOpsBatch(
    transport: DuplexTransport<SyncMessage<Op>>,
    batch: OpsBatch<Op>,
  ): Promise<void> {
    // opsBatch is shared by both flows:
    // - reconcile / direct-send during syncOnce
    // - incremental pushes for live subscriptions
    const purpose: SyncOpPurpose =
      this.initiatorSubscriptions.get(batch.filterId)?.transport === transport
        ? 'subscribe'
        : 'reconcile';
    const auth = batch.auth;
    if (auth && auth.length !== batch.ops.length) {
      throw new Error(
        `OpsBatch.auth length ${auth.length} does not match ops length ${batch.ops.length}`,
      );
    }

    const verifyRes = await this.auth?.verifyOps?.(batch.ops, auth, {
      docId: this.backend.docId,
      purpose,
      filterId: batch.filterId,
    });
    const dispositions =
      verifyRes === undefined
        ? undefined
        : ((verifyRes as SyncAuthVerifyOpsResult)?.dispositions ??
          (() => {
            throw new Error('verifyOps must return void or { dispositions: [...] }');
          })());
    if (dispositions && dispositions.length !== batch.ops.length) {
      throw new Error(
        `verifyOps returned ${dispositions.length} dispositions for ${batch.ops.length} ops`,
      );
    }
    if (auth && auth.length > 0) {
      await this.auth?.onVerifiedOps?.(batch.ops, auth, {
        docId: this.backend.docId,
        purpose,
        filterId: batch.filterId,
      });
    }

    const pending: PendingOp<Op>[] = [];
    const allowedOps: Op[] = [];

    for (let i = 0; i < batch.ops.length; i += 1) {
      const op = batch.ops[i]!;
      const d = dispositions?.[i];
      if (!d || d.status === 'allow') {
        allowedOps.push(op);
        continue;
      }
      if (d.status !== 'pending_context') {
        throw new Error(`unknown disposition: ${(d as any)?.status ?? String(d)}`);
      }
      if (!auth) {
        throw new Error('verifyOps returned pending_context but OpsBatch.auth is missing');
      }
      pending.push({
        op,
        auth: auth[i]!,
        reason: 'missing_context',
        ...(d.message ? { message: d.message } : {}),
      });
    }

    if (pending.length > 0) {
      if (!this.backend.storePendingOps) {
        throw new Error(
          'received ops requiring pending-context handling, but backend.storePendingOps is not implemented',
        );
      }
      await this.backend.storePendingOps(pending);
    }

    await this.backend.applyOps(allowedOps);
    if (allowedOps.length > 0) void this.notifyLocalUpdate(allowedOps);
    await this.reprocessPendingOps();

    const responderSession = getTransportOwned(this.responderSessions, transport, batch.filterId);
    // The empty done ack is only a completion signal. Send it after applyOps/reprocessPending
    // finishes so "done" means every prior batch for this filter has been durably handled.
    if (
      !responderSession &&
      getTransportOwned(this.responderAwaitingUploadAcks, transport, batch.filterId) &&
      batch.done
    ) {
      deleteTransportOwned(this.responderAwaitingUploadAcks, transport, batch.filterId);
      await this.sendDoneOpsBatch(transport, batch.filterId);
    }
    if (responderSession && batch.done) {
      if (responderSession.awaitingIncomingDone) {
        responderSession.awaitingIncomingDone = false;
        await this.sendDoneOpsBatch(transport, batch.filterId);
      }
      deleteTransportOwned(this.responderSessions, transport, batch.filterId);
    }

    const session = this.initiatorSessions.get(batch.filterId);
    if (session?.transport === transport && batch.done) {
      if (!session.awaitingUploadAck || batch.ops.length === 0) {
        session.receivedOps.resolve();
      }
    }
  }

  private async reprocessPendingOps(): Promise<void> {
    if (this.reprocessPendingRunning) {
      await this.reprocessPendingInFlight;
      return;
    }
    if (!this.backend.listPendingOps || !this.backend.deletePendingOps) return;
    if (!this.auth?.verifyOps) return;

    this.reprocessPendingRunning = true;
    this.reprocessPendingInFlight = (async () => {
      // Pending ops are retried after every successful applyOps pass so auth
      // schemes that need causal context can unlock buffered ops incrementally.
      const maxRounds = 100;
      for (let round = 0; round < maxRounds; round += 1) {
        const pending = await this.backend.listPendingOps!();
        if (pending.length === 0) return;

        let progress = false;
        const appliedOps: Op[] = [];

        for (const p of pending) {
          const ctx = {
            docId: this.backend.docId,
            purpose: 'reprocess_pending' as const,
            filterId: '__pending__',
          };
          let res: void | SyncAuthVerifyOpsResult;
          try {
            res = await this.auth!.verifyOps!([p.op], [p.auth], ctx);
          } catch {
            // Context is now sufficient to prove this op is invalid/unauthorized.
            // Drop it from pending so it doesn't block future progress.
            await this.backend.deletePendingOps!([p.op]);
            progress = true;
            continue;
          }

          const dispositions =
            res === undefined
              ? undefined
              : ((res as SyncAuthVerifyOpsResult)?.dispositions ??
                (() => {
                  throw new Error('verifyOps must return void or { dispositions: [...] }');
                })());
          const d = dispositions?.[0];
          if (d && d.status === 'pending_context') continue;

          await this.backend.applyOps([p.op]);
          await this.backend.deletePendingOps!([p.op]);
          progress = true;
          appliedOps.push(p.op);
        }

        if (appliedOps.length > 0) void this.notifyLocalUpdate(appliedOps);
        if (!progress) return;
      }
      throw new Error('pending-op reprocessing exceeded max rounds');
    })().finally(() => {
      this.reprocessPendingRunning = false;
    });

    await this.reprocessPendingInFlight;
  }
}

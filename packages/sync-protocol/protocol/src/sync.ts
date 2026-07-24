import { RibltDecoder16, RibltEncoder16 } from '@treecrdt/riblt-wasm';

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
  DIRECT_SEND_EMPTY_RECEIVER_SUPPORT_CAPABILITY,
  DIRECT_SEND_SMALL_SCOPE_FILTER_CAPABILITY,
  DIRECT_SEND_SMALL_SCOPE_REQUEST_CAPABILITY,
  DIRECT_SEND_SMALL_SCOPE_SUPPORT_CAPABILITY,
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
  SyncError,
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

type PeerCapabilitySnapshot = {
  fingerprint: string;
  capabilities: Hello['capabilities'];
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
  peerSnapshot: PeerCapabilitySnapshot;
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

type PendingHelloExchange = {
  ack: Pending<HelloAck>;
  capabilityGeneration: number;
  filterId?: string;
  processing?: boolean;
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
  /** Abort an in-flight reconciliation and release its local session state. */
  signal?: AbortSignal;
};

export type SyncPushOptions = {
  /** Split a direct push into smaller wire batches to avoid giant frames. */
  maxOpsPerBatch?: number;
  /** Abort an in-flight direct push before it sends any later chunks. */
  signal?: AbortSignal;
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

function syncAbortReason(signal: AbortSignal): Error {
  if (signal.reason instanceof Error) return signal.reason;
  const error = new Error(signal.reason === undefined ? 'sync aborted' : String(signal.reason));
  error.name = 'AbortError';
  return error;
}

function throwIfSyncAborted(signal?: AbortSignal): void {
  if (signal?.aborted) throw syncAbortReason(signal);
}

function awaitSyncStep<T>(run: () => T | PromiseLike<T>, signal?: AbortSignal): Promise<T> {
  if (signal?.aborted) return Promise.reject(syncAbortReason(signal));

  return new Promise<T>((resolve, reject) => {
    let settled = false;
    const onAbort = () => {
      if (settled) return;
      settled = true;
      signal?.removeEventListener('abort', onAbort);
      if (signal) reject(syncAbortReason(signal));
    };
    signal?.addEventListener('abort', onAbort, { once: true });

    let promise: Promise<T>;
    try {
      promise = Promise.resolve(run());
    } catch (error) {
      signal?.removeEventListener('abort', onAbort);
      reject(error);
      return;
    }

    promise.then(
      (value) => {
        if (settled) return;
        settled = true;
        signal?.removeEventListener('abort', onAbort);
        resolve(value);
      },
      (error) => {
        if (settled) return;
        settled = true;
        signal?.removeEventListener('abort', onAbort);
        reject(error);
      },
    );
  });
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

function assertOutgoingFilterLength(allowed: readonly boolean[], expected: number): void {
  if (allowed.length !== expected) {
    throw new Error(`filterOutgoingOps returned ${allowed.length} flags for ${expected} ops`);
  }
}

function copyCapabilities(capabilities: readonly Capability[]): Capability[] {
  return capabilities.map(({ name, value }) => ({ name, value }));
}

function authorizationCapabilities(capabilities: readonly Capability[]): Capability[] {
  return copyCapabilities(capabilities).filter(
    (capability) =>
      capability.name !== DIRECT_SEND_SMALL_SCOPE_SUPPORT_CAPABILITY &&
      capability.name !== DIRECT_SEND_SMALL_SCOPE_REQUEST_CAPABILITY &&
      capability.name !== DIRECT_SEND_SMALL_SCOPE_FILTER_CAPABILITY &&
      capability.name !== DIRECT_SEND_EMPTY_RECEIVER_SUPPORT_CAPABILITY &&
      capability.name !== DIRECT_SEND_EMPTY_RECEIVER_FILTER_CAPABILITY,
  );
}

const EMPTY_CAPABILITY_FINGERPRINT = capabilitySetFingerprint([]);

function assertCurrentCapabilityLease(current: boolean): void {
  if (!current) throw new Error('peer capabilities changed during outbound operation selection');
}

export class SyncPeer<Op> {
  private readonly maxCodewords: number;
  private readonly maxOpsPerBatch: number;
  private readonly maxHelloFilters: number;
  private readonly directSendThreshold: number;
  private readonly requireAuthForFilters: boolean;
  private readonly auth?: SyncAuth<Op>;
  private readonly deriveOpRef?: (op: Op, ctx: { docId: string }) => OpRef;
  private readonly transportPeerCapabilities = new WeakMap<
    DuplexTransport<SyncMessage<Op>>,
    Hello['capabilities']
  >();
  private readonly transportHelloExchangeSequences = new WeakMap<
    DuplexTransport<SyncMessage<Op>>,
    bigint
  >();
  private readonly transportPeerCapabilityGeneration = new WeakMap<
    DuplexTransport<SyncMessage<Op>>,
    number
  >();
  private readonly transportPeerCapabilityFingerprints = new WeakMap<
    DuplexTransport<SyncMessage<Op>>,
    string
  >();
  private readonly peerCapabilityRecoveryRequired = new WeakSet<DuplexTransport<SyncMessage<Op>>>();
  private readonly peerCapabilityValidationPending = new WeakSet<
    DuplexTransport<SyncMessage<Op>>
  >();
  private readonly transportDirectPushStreamIds = new WeakMap<
    DuplexTransport<SyncMessage<Op>>,
    string
  >();
  private readonly transportTerminalErrors = new WeakMap<DuplexTransport<SyncMessage<Op>>, Error>();
  private readonly responderSessions: TransportOwnedMap<Op, ResponderSession> = new Map();
  private readonly initiatorSessions = new Map<string, InitiatorSession<Op>>();
  private readonly pendingHelloExchanges: TransportOwnedMap<Op, PendingHelloExchange> = new Map();
  private readonly responderSubscriptions: TransportOwnedMap<Op, ResponderSubscription<Op>> =
    new Map();
  private readonly initiatorSubscriptions = new Map<string, InitiatorSubscription<Op>>();
  private readonly responderAwaitingUploadAcks: TransportOwnedMap<Op, true> = new Map();
  private readonly opsBatchQueues: TransportOwnedMap<Op, Promise<void>> = new Map();
  private readonly responderHelloAbortControllers = new WeakMap<
    DuplexTransport<SyncMessage<Op>>,
    Map<string, AbortController>
  >();
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

  private async sendProtocolError(
    transport: DuplexTransport<SyncMessage<Op>>,
    error: SyncError,
  ): Promise<void> {
    this.assertTransportActive(transport);
    await transport.send({
      v: 0,
      docId: this.backend.docId,
      payload: { case: 'error', value: error },
    });
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
      this.transportTerminalErrors.set(transport, normalized);
      this.failPendingSessionsForTransport(transport, normalized);
      this.dropResponderStateForTransport(transport);
      if (close) ignoreErrors(() => transport.close?.(normalized));
      if (report) ignoreErrors(() => attachOpts.onError?.({ error: normalized, transport }));
    };

    unsubscribeMessage = transport.onMessage((msg) => {
      if (failed) return;
      void this.dispatchMessage(transport, msg)
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
            } catch (err) {
              deleteTransportOwned(this.responderSubscriptions, sub.transport, sub.subscriptionId);
              try {
                await this.sendProtocolError(sub.transport, {
                  code: ErrorCode.ERROR_CODE_UNSPECIFIED,
                  message: err instanceof Error ? err.message : String(err),
                  subscriptionId: sub.subscriptionId,
                });
              } catch {
                // The subscription is already terminal locally; ignore transport failure.
              }
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
    signal?: AbortSignal,
  ): Promise<void> {
    this.assertTransportActive(transport);
    if (!this.auth?.helloCapabilities) return;

    const [maxLamport, advertisedCapabilities] = await awaitSyncStep(
      () =>
        Promise.all([
          this.backend.maxLamport(),
          this.auth!.helloCapabilities!({ docId: this.backend.docId }),
        ]),
      signal,
    );
    const capabilities = copyCapabilities(advertisedCapabilities);
    this.assertTransportActive(transport);
    const { exchangeId, ack } = this.beginHelloExchange(transport);
    try {
      await awaitSyncStep(
        () =>
          transport.send({
            v: 0,
            docId: this.backend.docId,
            payload: {
              case: 'hello',
              value: { exchangeId, capabilities, filters: [], maxLamport },
            },
          }),
        signal,
      );
      await awaitSyncStep(() => ack.promise, signal);
    } finally {
      deleteTransportOwned(this.pendingHelloExchanges, transport, exchangeId);
    }
  }

  private async dispatchMessage(
    transport: DuplexTransport<SyncMessage<Op>>,
    msg: SyncMessage<Op>,
  ): Promise<void> {
    const isInboundHello = msg.docId === this.backend.docId && msg.payload.case === 'hello';
    const capabilityGeneration = isInboundHello
      ? this.beginPeerCapabilitySnapshot(transport)
      : undefined;
    this.assertTransportActive(transport);
    await this.handleMessage(transport, msg, capabilityGeneration);
  }

  private rejectPendingHelloExchanges(
    transport: DuplexTransport<SyncMessage<Op>>,
    error: unknown,
  ): void {
    for (const [exchangeId, byTransport] of this.pendingHelloExchanges) {
      const exchange = byTransport.get(transport);
      if (!exchange) continue;
      exchange.ack.reject(error);
      deleteTransportOwned(this.pendingHelloExchanges, transport, exchangeId);
    }
  }

  private assertTransportActive(transport: DuplexTransport<SyncMessage<Op>>): void {
    const terminalError = this.transportTerminalErrors.get(transport);
    if (terminalError) throw terminalError;
  }

  private beginPeerCapabilitySnapshot(transport: DuplexTransport<SyncMessage<Op>>): number {
    const generation = (this.transportPeerCapabilityGeneration.get(transport) ?? 0) + 1;
    this.transportPeerCapabilityGeneration.set(transport, generation);
    // Clear old authority immediately; publish the replacement only after its
    // auth hook succeeds and only if no newer snapshot has arrived.
    this.transportPeerCapabilities.set(transport, []);
    this.peerCapabilityValidationPending.add(transport);
    return generation;
  }

  private beginHelloExchange(
    transport: DuplexTransport<SyncMessage<Op>>,
    filterId?: string,
  ): { exchangeId: string; ack: Pending<HelloAck> } {
    this.assertTransportActive(transport);
    const sequence = (this.transportHelloExchangeSequences.get(transport) ?? 0n) + 1n;
    this.transportHelloExchangeSequences.set(transport, sequence);
    const exchangeId = `h_${sequence.toString(36)}`;
    const ack = deferred<HelloAck>();
    setTransportOwned(this.pendingHelloExchanges, transport, exchangeId, {
      ack,
      capabilityGeneration: this.beginPeerCapabilitySnapshot(transport),
      ...(filterId ? { filterId } : {}),
    });
    return { exchangeId, ack };
  }

  private publishPeerCapabilitySnapshot(
    transport: DuplexTransport<SyncMessage<Op>>,
    generation: number,
    capabilities: Hello['capabilities'],
  ): boolean | undefined {
    this.assertTransportActive(transport);
    if (!this.isCurrentPeerCapabilityGeneration(transport, generation)) return undefined;
    const acceptedCapabilities = authorizationCapabilities(capabilities);
    const fingerprint = capabilitySetFingerprint(acceptedCapabilities);
    const changed =
      this.peerCapabilityRecoveryRequired.has(transport) ||
      this.transportPeerCapabilityFingerprints.get(transport) !== fingerprint;
    this.transportPeerCapabilities.set(transport, acceptedCapabilities);
    this.transportPeerCapabilityFingerprints.set(transport, fingerprint);
    this.peerCapabilityRecoveryRequired.delete(transport);
    this.peerCapabilityValidationPending.delete(transport);
    return changed;
  }

  private peerCapabilitySnapshot(
    transport: DuplexTransport<SyncMessage<Op>>,
  ): PeerCapabilitySnapshot {
    const capabilities = this.transportPeerCapabilities.get(transport) ?? [];
    return {
      fingerprint: capabilitySetFingerprint(capabilities),
      capabilities,
    };
  }

  private isCurrentPeerCapabilityGeneration(
    transport: DuplexTransport<SyncMessage<Op>>,
    generation: number,
  ): boolean {
    return (
      !this.transportTerminalErrors.has(transport) &&
      (this.transportPeerCapabilityGeneration.get(transport) ?? 0) === generation
    );
  }

  private isCurrentPeerCapabilitySnapshot(
    transport: DuplexTransport<SyncMessage<Op>>,
    snapshot: Pick<PeerCapabilitySnapshot, 'fingerprint'>,
  ): boolean {
    if (this.transportTerminalErrors.has(transport)) return false;
    if (this.peerCapabilityValidationPending.has(transport)) return false;
    const acceptedFingerprint = this.transportPeerCapabilityFingerprints.get(transport);
    if (acceptedFingerprint !== undefined) return acceptedFingerprint === snapshot.fingerprint;
    return (
      (this.transportPeerCapabilityGeneration.get(transport) ?? 0) === 0 &&
      snapshot.fingerprint === EMPTY_CAPABILITY_FINGERPRINT
    );
  }

  private hasCurrentSubscriptionCapabilitySnapshot(
    transport: DuplexTransport<SyncMessage<Op>>,
    snapshot: PeerCapabilitySnapshot,
  ): boolean {
    if (this.peerCapabilityValidationPending.has(transport)) {
      this.peerCapabilityRecoveryRequired.add(transport);
      return false;
    }
    if (this.isCurrentPeerCapabilitySnapshot(transport, snapshot)) return true;
    this.rescanSubscriptionsAfterCapabilityChange(transport, true);
    return false;
  }

  private rescanSubscriptionsAfterCapabilityChange(
    transport: DuplexTransport<SyncMessage<Op>>,
    capabilitiesChanged: boolean,
  ): void {
    if (!capabilitiesChanged) return;
    for (const subscriptions of this.responderSubscriptions.values()) {
      if (!subscriptions.has(transport)) continue;
      void this.notifyLocalUpdate();
      return;
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
    signal?: AbortSignal,
  ): Promise<void> {
    this.assertTransportActive(transport);
    throwIfSyncAborted(signal);
    await awaitSyncStep(
      () =>
        transport.send({
          v: 0,
          docId: this.backend.docId,
          payload: { case: 'opsBatch', value: { filterId, ops: [], done: true } },
        }),
      signal,
    );
  }

  private async rejectResponderCapabilityLease(
    transport: DuplexTransport<SyncMessage<Op>>,
    filterId: string,
  ): Promise<void> {
    deleteTransportOwned(this.responderSessions, transport, filterId);
    await this.sendProtocolError(transport, {
      code: ErrorCode.UNAUTHORIZED,
      message: 'peer capabilities changed during reconciliation; retry',
      filterId,
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
    const peerSnapshot = this.peerCapabilitySnapshot(sub.transport);

    for (let start = 0; start < newOpRefs.length; start += this.maxOpsPerBatch) {
      const chunk = newOpRefs.slice(start, start + this.maxOpsPerBatch);
      let ops = await this.backend.getOpsByOpRefs(chunk);
      const peerCaps = peerSnapshot.capabilities;

      // Apply peer-scoped visibility restrictions (best-effort).
      if (this.auth?.filterOutgoingOps && ops.length > 0) {
        const allowed = await this.auth.filterOutgoingOps(ops, {
          docId: this.backend.docId,
          purpose: 'subscribe',
          filter: sub.filter,
          capabilities: peerCaps,
        });
        assertOutgoingFilterLength(allowed, ops.length);
        if (!this.hasCurrentSubscriptionCapabilitySnapshot(sub.transport, peerSnapshot)) return;

        const allowedRefs: OpRef[] = [];
        const allowedOps: Op[] = [];
        for (let i = 0; i < ops.length; i += 1) {
          if (allowed[i] === true) {
            allowedRefs.push(chunk[i]!);
            allowedOps.push(ops[i]!);
          }
        }

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
      if (!this.hasCurrentSubscriptionCapabilitySnapshot(sub.transport, peerSnapshot)) return;
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

    const peerSnapshot = this.peerCapabilitySnapshot(sub.transport);
    const peerCaps = peerSnapshot.capabilities;
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
        assertOutgoingFilterLength(allowed, ops.length);
        if (!this.hasCurrentSubscriptionCapabilitySnapshot(sub.transport, peerSnapshot)) return;

        const allowedRefs: OpRef[] = [];
        const allowedOps: Op[] = [];
        for (let i = 0; i < ops.length; i += 1) {
          if (allowed[i] === true) {
            allowedRefs.push(refs[i]!);
            allowedOps.push(ops[i]!);
          }
        }

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
      if (!this.hasCurrentSubscriptionCapabilitySnapshot(sub.transport, peerSnapshot)) return;

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
    this.assertTransportActive(transport);
    const signal = opts.signal;
    const step = <T>(run: () => T | PromiseLike<T>) => awaitSyncStep(run, signal);
    throwIfSyncAborted(signal);
    // syncOnce negotiates one of three wire modes for this filter:
    // 1. the normal RIBLT reconcile path,
    // 2. direct-send for small scoped reads, or
    // 3. direct-send upload when the initiator is an empty receiver.
    // The capability exchange below advertises support and lets the peer pick the cheaper mode.
    const filterId = randomId('f');
    const round = 0;
    const maxLamport = await step(() => this.backend.maxLamport());
    const localOpRefsBeforeHello = await step(() => this.backend.listOpRefs(filter));
    const localCapabilities = this.auth?.helloCapabilities
      ? await step(() => this.auth!.helloCapabilities!({ docId: this.backend.docId }))
      : [];
    throwIfSyncAborted(signal);
    const capabilities = buildInitiatorHelloCapabilities(localCapabilities, {
      filterId,
      localHasOps: localOpRefsBeforeHello.length > 0,
    });
    this.assertTransportActive(transport);
    const { exchangeId, ack: pendingAck } = this.beginHelloExchange(transport, filterId);
    const hello: Hello = {
      exchangeId,
      capabilities,
      filters: [{ id: filterId, filter }],
      maxLamport,
    };

    const session: InitiatorSession<Op> = {
      transport,
      filter,
      filterId,
      round,
      ack: pendingAck,
      terminalStatus: deferred<RibltStatus>(),
      codewordCredits: 1,
      codewordCreditSignal: deferred<void>(),
      receivedOps: deferred<void>(),
      awaitingUploadAck: false,
      done: false,
    };
    this.initiatorSessions.set(filterId, session);

    let helloSend: Promise<void> | undefined;
    let cancelled = false;
    let remoteCancellationScheduled = false;
    const scheduleRemoteCancellation = () => {
      if (!cancelled || !helloSend || !signal || remoteCancellationScheduled) return;
      remoteCancellationScheduled = true;
      const message = `sync cancelled: ${syncAbortReason(signal).message}`;
      void helloSend
        .then(() =>
          transport.send({
            v: 0,
            docId: this.backend.docId,
            payload: {
              case: 'error',
              value: { code: ErrorCode.ERROR_CODE_UNSPECIFIED, message, filterId },
            },
          }),
        )
        .catch(() => {
          // Best-effort remote cleanup; the local session is already cancelled.
        });
    };
    const cancelSession = () => {
      cancelled = true;
      session.done = true;
      this.initiatorSessions.delete(filterId);
      deleteTransportOwned(this.pendingHelloExchanges, transport, exchangeId);
      scheduleRemoteCancellation();
    };
    if (signal?.aborted) cancelSession();
    else signal?.addEventListener('abort', cancelSession, { once: true });

    try {
      this.assertTransportActive(transport);
      await step(() => {
        helloSend = transport.send({
          v: 0,
          docId: this.backend.docId,
          payload: { case: 'hello', value: hello },
        });
        // `transport.send` may synchronously trigger cancellation before its promise is assigned.
        scheduleRemoteCancellation();
        return helloSend;
      });
      const helloAck = await step(() => session.ack.promise);

      // For tiny scoped reads the responder can skip RIBLT entirely and send
      // the result as direct ops once Hello/HelloAck agrees on that shortcut.
      if (
        localOpRefsBeforeHello.length === 0 &&
        peerSelectedDirectSendFilter(helloAck.capabilities, filterId)
      ) {
        await step(() => session.receivedOps.promise);
        return;
      }

      const peerSnapshot = this.peerCapabilitySnapshot(transport);
      let opRefs = await step(() => this.backend.listOpRefs(filter));

      // If we have peer capabilities (from HelloAck) and an auth layer that can scope outgoing ops,
      // filter the local set to avoid advertising/sending ops the peer cannot receive.
      if (this.auth?.filterOutgoingOps && opRefs.length > 0) {
        const ops = await step(() => this.backend.getOpsByOpRefs(opRefs));
        const allowed = await step(() =>
          this.auth!.filterOutgoingOps!(ops, {
            docId: this.backend.docId,
            purpose: 'reconcile',
            filter,
            capabilities: peerSnapshot.capabilities,
          }),
        );
        assertOutgoingFilterLength(allowed, ops.length);
        assertCurrentCapabilityLease(this.isCurrentPeerCapabilitySnapshot(transport, peerSnapshot));
        opRefs = opRefs.filter((_r, idx) => allowed[idx] === true);
      }

      if (peerSelectedDirectSendEmptyReceiverFilter(helloAck.capabilities, filterId)) {
        session.awaitingUploadAck = true;
        const uploadMaxOpsPerBatch =
          opts.maxOpsPerBatch ?? DIRECT_SEND_EMPTY_RECEIVER_MAX_OPS_PER_BATCH;
        if (opRefs.length > 0) {
          await this.sendOpsBatches(transport, filterId, opRefs, {
            maxOpsPerBatch: uploadMaxOpsPerBatch,
            filter,
            signal,
          });
        } else {
          await this.sendDoneOpsBatch(transport, filterId, signal);
        }
        await step(() => session.receivedOps.promise);
        return;
      }

      const enc = new RibltEncoder16();
      for (const r of opRefs) enc.addSymbol(r);

      const codewordsPerMessage = opts.codewordsPerMessage ?? 512;
      const maxCodewords = BigInt(opts.maxCodewords ?? 50_000);

      let nextIndex = 0n;
      while (!session.done && nextIndex < maxCodewords) {
        throwIfSyncAborted(signal);
        if (session.codewordCredits <= 0) {
          const wakeForCredits = session.codewordCreditSignal.promise;
          await step(() =>
            Promise.race([
              session.terminalStatus.promise.then(
                () => undefined,
                () => undefined,
              ),
              wakeForCredits,
            ]),
          );
          continue;
        }

        session.codewordCredits -= 1;
        const startIndex = nextIndex;
        const codewords: RibltCodewords['codewords'] = [];
        for (let i = 0; i < codewordsPerMessage && nextIndex < maxCodewords; i += 1) {
          codewords.push(enc.nextCodeword() as any);
          nextIndex += 1n;
        }

        assertCurrentCapabilityLease(this.isCurrentPeerCapabilitySnapshot(transport, peerSnapshot));
        await step(() =>
          transport.send({
            v: 0,
            docId: this.backend.docId,
            payload: {
              case: 'ribltCodewords',
              value: { filterId, round, startIndex, codewords },
            },
          }),
        );
        await step(yieldToMacrotask);
      }

      throwIfSyncAborted(signal);
      if (!session.done) throw new Error('riblt: max codewords exceeded');

      const status = await step(() => session.terminalStatus.promise);
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
          signal,
        });
      } else {
        await this.sendDoneOpsBatch(transport, filterId, signal);
      }

      await step(() => session.receivedOps.promise);
    } finally {
      deleteTransportOwned(this.pendingHelloExchanges, transport, exchangeId);
      signal?.removeEventListener('abort', cancelSession);
      this.initiatorSessions.delete(filterId);
    }
  }

  /**
   * Send a known set of ops directly without first reconciling state via
   * `syncOnce()`.
   *
   * The `opsBatch` wire format still carries a `filterId`; for direct pushes
   * that field acts only as a stable stream id so the receiver can order chunks
   * and interpret the final `done` marker correctly. When an outgoing auth
   * filter is configured, direct pushes use an unscoped `all` filter and only
   * send the operations that filter allows for the peer's current capabilities.
   */
  async pushOps(
    transport: DuplexTransport<SyncMessage<Op>>,
    ops: readonly Op[],
    opts: SyncPushOptions = {},
  ): Promise<void> {
    if (ops.length === 0) return;

    const signal = opts.signal;
    const step = <T>(run: () => T | PromiseLike<T>) => awaitSyncStep(run, signal);
    throwIfSyncAborted(signal);
    await this.refreshHelloCapabilities(transport, signal);

    const peerSnapshot = this.peerCapabilitySnapshot(transport);
    const peerCapabilities = peerSnapshot.capabilities;
    let outgoingOps: readonly Op[] = ops;
    if (this.auth?.filterOutgoingOps) {
      const allowed = await step(() =>
        this.auth!.filterOutgoingOps!(outgoingOps, {
          docId: this.backend.docId,
          purpose: 'reconcile',
          filter: { all: {} },
          capabilities: peerCapabilities,
        }),
      );
      assertOutgoingFilterLength(allowed, outgoingOps.length);
      assertCurrentCapabilityLease(this.isCurrentPeerCapabilitySnapshot(transport, peerSnapshot));
      outgoingOps = outgoingOps.filter((_op, index) => allowed[index] === true);
      if (outgoingOps.length === 0) return;
    }

    const streamId = this.resolveDirectPushStreamId(transport, opts.filterId);
    const batchSize = this.resolveMaxOpsPerBatch(opts.maxOpsPerBatch);
    const shouldAttachAuth = peerAdvertisedOpAuth(peerCapabilities);

    for (let start = 0; start < outgoingOps.length; start += batchSize) {
      throwIfSyncAborted(signal);
      const chunk = outgoingOps.slice(start, start + batchSize);
      const auth =
        shouldAttachAuth && this.auth?.signOps
          ? await step(() =>
              this.auth!.signOps!(chunk, {
                docId: this.backend.docId,
                purpose: 'reconcile',
                filterId: streamId,
              }),
            )
          : undefined;
      if (auth && auth.length !== chunk.length) {
        throw new Error(`signOps returned ${auth.length} entries for ${chunk.length} ops`);
      }
      assertCurrentCapabilityLease(this.isCurrentPeerCapabilitySnapshot(transport, peerSnapshot));

      await step(() =>
        transport.send({
          v: 0,
          docId: this.backend.docId,
          payload: {
            case: 'opsBatch',
            value: {
              filterId: streamId,
              ops: [...chunk],
              ...(auth ? { auth } : {}),
              done: start + batchSize >= outgoingOps.length,
            },
          },
        }),
      );
      await step(yieldToMacrotask);
    }
  }

  private async sendOpsBatches(
    transport: DuplexTransport<SyncMessage<Op>>,
    filterId: string,
    opRefs: OpRef[],
    opts: {
      maxOpsPerBatch?: number;
      filter?: Filter;
      peerSnapshot?: PeerCapabilitySnapshot;
      signal?: AbortSignal;
    } = {},
  ): Promise<void> {
    const step = <T>(run: () => T | PromiseLike<T>) => awaitSyncStep(run, opts.signal);
    throwIfSyncAborted(opts.signal);
    const maxOpsPerBatch = this.resolveMaxOpsPerBatch(opts.maxOpsPerBatch);

    const initiatorSession = this.initiatorSessions.get(filterId);
    const filter =
      opts.filter ??
      getTransportOwned(this.responderSessions, transport, filterId)?.filter ??
      (initiatorSession?.transport === transport ? initiatorSession.filter : undefined);
    const peerSnapshot = opts.peerSnapshot ?? this.peerCapabilitySnapshot(transport);
    const peerCaps = peerSnapshot.capabilities;
    const hasCurrentCapabilities = () =>
      this.isCurrentPeerCapabilitySnapshot(transport, peerSnapshot);

    if (opRefs.length === 0) {
      assertCurrentCapabilityLease(hasCurrentCapabilities());
      await this.sendDoneOpsBatch(transport, filterId, opts.signal);
      return;
    }

    for (let start = 0; start < opRefs.length; start += maxOpsPerBatch) {
      throwIfSyncAborted(opts.signal);
      const chunk = opRefs.slice(start, start + maxOpsPerBatch);
      let ops = await step(() => this.backend.getOpsByOpRefs(chunk));

      if (filter && this.auth?.filterOutgoingOps && ops.length > 0) {
        const allowed = await step(() =>
          this.auth!.filterOutgoingOps!(ops, {
            docId: this.backend.docId,
            purpose: 'reconcile',
            filter,
            capabilities: peerCaps,
          }),
        );
        assertOutgoingFilterLength(allowed, ops.length);
        assertCurrentCapabilityLease(hasCurrentCapabilities());

        const nextOps: Op[] = [];
        for (let i = 0; i < ops.length; i += 1) {
          if (allowed[i] === true) nextOps.push(ops[i]!);
        }
        ops = nextOps;
      }

      const done = start + maxOpsPerBatch >= opRefs.length;

      if (ops.length === 0) {
        if (done) {
          assertCurrentCapabilityLease(hasCurrentCapabilities());
          await this.sendDoneOpsBatch(transport, filterId, opts.signal);
        }
        continue;
      }

      const shouldAttachAuth = peerAdvertisedOpAuth(peerCaps);
      const auth =
        shouldAttachAuth && this.auth?.signOps
          ? await step(() =>
              this.auth!.signOps!(ops, {
                docId: this.backend.docId,
                purpose: 'reconcile',
                filterId,
              }),
            )
          : undefined;
      if (auth && auth.length !== ops.length) {
        throw new Error(`signOps returned ${auth.length} entries for ${ops.length} ops`);
      }
      assertCurrentCapabilityLease(hasCurrentCapabilities());

      throwIfSyncAborted(opts.signal);
      await step(() =>
        transport.send({
          v: 0,
          docId: this.backend.docId,
          payload: {
            case: 'opsBatch',
            value: { filterId, ops, ...(auth ? { auth } : {}), done },
          },
        }),
      );
      await step(yieldToMacrotask);
    }
  }

  subscribe(
    transport: DuplexTransport<SyncMessage<Op>>,
    filter: Filter,
    opts: SyncSubscribeOptions = {},
  ): SyncSubscription {
    this.assertTransportActive(transport);
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
          await this.refreshHelloCapabilities(transport);
        }

        this.assertTransportActive(transport);
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
            signal,
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
              signal,
            });
          }
        } else {
          await Promise.race([waitForAbort(signal), failed]);
        }
      } catch (err) {
        // Stopping a subscription is normal completion, including while its immediate or periodic
        // reconciliation is awaiting an abortable backend/transport step.
        if (signal.aborted) {
          resolveReady();
          return;
        }
        rejectReady(err);
        throw err;
      } finally {
        controller.abort();
        resolveReady();
        this.initiatorSubscriptions.delete(subscriptionId);
        if (sentSubscribe) {
          try {
            this.assertTransportActive(transport);
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
    capabilityGeneration?: number,
  ): Promise<void> {
    if (msg.docId !== this.backend.docId) return;

    if (msg.payload.case === 'error') {
      try {
        if (
          !msg.payload.value.filterId &&
          !msg.payload.value.subscriptionId &&
          !msg.payload.value.exchangeId
        ) {
          const code = ErrorCode[msg.payload.value.code] ?? String(msg.payload.value.code);
          this.rejectPendingHelloExchanges(
            transport,
            new Error(`${code}: ${msg.payload.value.message}`),
          );
        }
        await this.onError(transport, msg.payload.value);
      } catch {
        // ignore error while handling error
      }
      return;
    }

    try {
      switch (msg.payload.case) {
        case 'hello':
          await this.onHello(
            transport,
            msg.payload.value,
            capabilityGeneration ?? this.beginPeerCapabilitySnapshot(transport),
          );
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
      if (this.transportTerminalErrors.has(transport)) return;
      if (
        msg.payload.case === 'hello' &&
        capabilityGeneration !== undefined &&
        this.isCurrentPeerCapabilityGeneration(transport, capabilityGeneration)
      ) {
        this.peerCapabilityRecoveryRequired.add(transport);
      }

      let filterId: string | undefined;
      let subscriptionId: string | undefined;
      let exchangeId: string | undefined;
      switch (msg.payload.case) {
        case 'hello':
          exchangeId = msg.payload.value.exchangeId;
          break;
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
        // A Hello exchange id is chosen by the remote peer. Do not feed it into
        // local initiator cleanup: both directions have independent id spaces.
        if (!exchangeId) {
          await this.onError(transport, {
            code: ErrorCode.ERROR_CODE_UNSPECIFIED,
            message: String(err?.message ?? err ?? 'error'),
            ...(filterId ? { filterId } : {}),
            ...(subscriptionId ? { subscriptionId } : {}),
          });
        }

        await this.sendProtocolError(transport, {
          code: ErrorCode.ERROR_CODE_UNSPECIFIED,
          message: String(err?.message ?? err ?? 'error'),
          ...(filterId ? { filterId } : {}),
          ...(subscriptionId ? { subscriptionId } : {}),
          ...(exchangeId ? { exchangeId } : {}),
        });
      } catch {
        // ignore transport failures while reporting errors
      }
    }
  }

  private async onHello(
    transport: DuplexTransport<SyncMessage<Op>>,
    hello: Hello,
    capabilityGeneration: number,
  ): Promise<void> {
    if (!hello.exchangeId) throw new Error('Hello.exchangeId missing');
    const transportControllers =
      this.responderHelloAbortControllers.get(transport) ?? new Map<string, AbortController>();
    const helloControllers = new Map<string, AbortController>();

    for (let i = 0; i < hello.filters.length && i < this.maxHelloFilters; i += 1) {
      const id = hello.filters[i]?.id;
      if (!id || helloControllers.has(id)) continue;
      const controller = new AbortController();
      transportControllers.get(id)?.abort(new Error('responder sync superseded'));
      transportControllers.set(id, controller);
      helloControllers.set(id, controller);
    }
    if (transportControllers.size > 0) {
      this.responderHelloAbortControllers.set(transport, transportControllers);
    }

    try {
      await this.onActiveHello(transport, hello, capabilityGeneration, helloControllers);
    } finally {
      for (const [id, controller] of helloControllers) {
        if (transportControllers.get(id) === controller) transportControllers.delete(id);
      }
      if (transportControllers.size === 0) {
        this.responderHelloAbortControllers.delete(transport);
      }
    }
  }

  private async onActiveHello(
    transport: DuplexTransport<SyncMessage<Op>>,
    hello: Hello,
    capabilityGeneration: number,
    helloControllers: ReadonlyMap<string, AbortController>,
  ): Promise<void> {
    const traceStartedAt = performance.now();
    const receivedCapabilities = copyCapabilities(hello.capabilities);
    traceHello(this.backend.docId, traceStartedAt, 'start', {
      filters: hello.filters.length,
      capabilities: receivedCapabilities.length,
    });
    const supportsDirectSendSmallScope = peerSupportsDirectSendSmallScope(receivedCapabilities);

    const ackCapabilities = copyCapabilities(
      (await this.auth?.onHello?.(
        { ...hello, capabilities: copyCapabilities(receivedCapabilities) },
        { docId: this.backend.docId },
      )) ?? [],
    );
    this.assertTransportActive(transport);
    traceHello(this.backend.docId, traceStartedAt, 'after-auth-onHello', {
      ackCapabilities: ackCapabilities.length,
    });
    const peerCapabilities = authorizationCapabilities(receivedCapabilities);
    const hasAuthCapability = peerCapabilities.some(isAuthCapability);
    const peerSnapshot: PeerCapabilitySnapshot = {
      fingerprint: capabilitySetFingerprint(peerCapabilities),
      capabilities: peerCapabilities,
    };
    const maxLamport = await this.backend.maxLamport();
    this.assertTransportActive(transport);
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
    const awaitingUploadFilterIds: string[] = [];
    const responderSessions: Array<[string, ResponderSession]> = [];
    const rejectDirectSendFilters = async (): Promise<void> => {
      for (const directSend of directSendFilters) {
        await this.sendProtocolError(transport, {
          code: ErrorCode.UNAUTHORIZED,
          message: 'peer capabilities changed during direct send; retry',
          filterId: directSend.id,
        });
      }
    };

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

      const signal = helloControllers.get(id)?.signal;
      if (!signal || signal.aborted) continue;

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
          capabilities: peerCapabilities,
        });
        this.assertTransportActive(transport);
      } catch (err: any) {
        if (signal.aborted) continue;
        rejectedFilters.push({
          id,
          reason: ErrorCode.UNAUTHORIZED,
          message: String(err?.message ?? err ?? 'unauthorized filter'),
        });
        continue;
      }
      if (signal.aborted) continue;

      let localOpRefs: OpRef[];
      try {
        localOpRefs = await this.backend.listOpRefs(filter);
        this.assertTransportActive(transport);
        traceHello(this.backend.docId, traceStartedAt, 'after-listOpRefs', {
          filterId: id,
          opRefs: localOpRefs.length,
        });
      } catch (err: any) {
        if (signal.aborted) continue;
        rejectedFilters.push({
          id,
          reason: ErrorCode.FILTER_NOT_SUPPORTED,
          message: String(err?.message ?? err ?? 'filter not supported'),
        });
        continue;
      }
      if (signal.aborted) continue;

      if (this.auth?.filterOutgoingOps && localOpRefs.length > 0) {
        try {
          const ops = await this.backend.getOpsByOpRefs(localOpRefs);
          if (signal.aborted) continue;
          const allowed = await this.auth.filterOutgoingOps(ops, {
            docId: this.backend.docId,
            purpose: 'hello',
            filter,
            capabilities: peerCapabilities,
          });
          if (signal.aborted) continue;
          this.assertTransportActive(transport);
          assertOutgoingFilterLength(allowed, ops.length);
          localOpRefs = localOpRefs.filter((_r, idx) => allowed[idx] === true);
          traceHello(this.backend.docId, traceStartedAt, 'after-filterOutgoingOps', {
            filterId: id,
            fetchedOps: ops.length,
            allowedOpRefs: localOpRefs.length,
          });
        } catch (err: any) {
          if (signal.aborted) continue;
          rejectedFilters.push({
            id,
            reason: ErrorCode.UNAUTHORIZED,
            message: String(err?.message ?? err ?? 'failed to filter ops'),
          });
          continue;
        }
      }

      if (signal.aborted) continue;

      acceptedFilters.push(id);

      if (
        supportsDirectSendSmallScope &&
        this.directSendThreshold > 0 &&
        peerRequestedDirectSendFilter(receivedCapabilities, id) &&
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

      if (peerSupportsDirectSendEmptyReceiver(receivedCapabilities) && localOpRefs.length === 0) {
        ackCapabilities.push({
          name: DIRECT_SEND_EMPTY_RECEIVER_FILTER_CAPABILITY,
          value: id,
        });
        awaitingUploadFilterIds.push(id);
        continue;
      }

      const decoder = new RibltDecoder16();
      for (const r of localOpRefs) decoder.addLocalSymbol(r);
      traceHello(this.backend.docId, traceStartedAt, 'after-decoder-setup', {
        filterId: id,
        opRefs: localOpRefs.length,
      });
      responderSessions.push([
        id,
        {
          peerSnapshot,
          filter,
          round: 0,
          decoder,
          expectedIndex: 0n,
          awaitingIncomingDone: false,
        },
      ]);
    }

    const capabilitiesChanged = this.publishPeerCapabilitySnapshot(
      transport,
      capabilityGeneration,
      receivedCapabilities,
    );
    if (
      capabilitiesChanged === undefined &&
      !this.isCurrentPeerCapabilitySnapshot(transport, peerSnapshot)
    ) {
      await this.sendProtocolError(transport, {
        code: ErrorCode.UNAUTHORIZED,
        message: 'Hello was superseded by a newer capability snapshot; retry',
        exchangeId: hello.exchangeId,
      });
      return;
    }

    this.assertTransportActive(transport);
    for (const id of awaitingUploadFilterIds) {
      if (helloControllers.get(id)?.signal.aborted) continue;
      setTransportOwned(this.responderAwaitingUploadAcks, transport, id, true);
    }
    for (const [id, session] of responderSessions) {
      if (helloControllers.get(id)?.signal.aborted) continue;
      setTransportOwned(this.responderSessions, transport, id, session);
    }
    const cleanup = () => {
      for (const id of awaitingUploadFilterIds) {
        deleteTransportOwned(this.responderAwaitingUploadAcks, transport, id);
      }
      for (const [id, session] of responderSessions) {
        if (getTransportOwned(this.responderSessions, transport, id) === session) {
          deleteTransportOwned(this.responderSessions, transport, id);
        }
      }
    };
    let keepSessions = false;
    try {
      this.assertTransportActive(transport);
      await transport.send({
        v: 0,
        docId: this.backend.docId,
        payload: {
          case: 'helloAck',
          value: {
            exchangeId: hello.exchangeId,
            capabilities: ackCapabilities,
            acceptedFilters: acceptedFilters.filter(
              (id) => !helloControllers.get(id)?.signal.aborted,
            ),
            rejectedFilters,
            maxLamport,
          },
        },
      });
      this.assertTransportActive(transport);
      // The peer may start its addressed RIBLT/upload session as soon as the Ack is sent.
      // Preserve those sessions across an overlapping capability exchange; their own
      // authorization lease is rechecked before any response data is sent.
      keepSessions = true;
      traceHello(this.backend.docId, traceStartedAt, 'after-helloAck-send', {
        acceptedFilters: acceptedFilters.length,
        rejectedFilters: rejectedFilters.length,
      });

      try {
        for (const directSend of directSendFilters) {
          const signal = helloControllers.get(directSend.id)?.signal;
          if (!signal || signal.aborted) continue;
          await this.sendOpsBatches(transport, directSend.id, directSend.opRefs, {
            filter: directSend.filter,
            peerSnapshot,
            signal,
          });
        }
      } catch {
        await rejectDirectSendFilters();
        return;
      }
      if (capabilitiesChanged !== undefined) {
        this.rescanSubscriptionsAfterCapabilityChange(transport, capabilitiesChanged);
      }
    } finally {
      if (!keepSessions) cleanup();
    }
  }

  private async onHelloAck(
    transport: DuplexTransport<SyncMessage<Op>>,
    ack: HelloAck,
  ): Promise<void> {
    if (!ack.exchangeId) throw new Error('HelloAck.exchangeId missing');
    const exchange = getTransportOwned(this.pendingHelloExchanges, transport, ack.exchangeId);
    if (!exchange || exchange.processing) return;
    exchange.processing = true;

    try {
      const receivedCapabilities = copyCapabilities(ack.capabilities);
      try {
        await this.auth?.onHelloAck?.(
          {
            ...ack,
            capabilities: copyCapabilities(receivedCapabilities),
            acceptedFilters: [...ack.acceptedFilters],
            rejectedFilters: ack.rejectedFilters.map((rejected) => ({ ...rejected })),
          },
          { docId: this.backend.docId },
        );
        this.assertTransportActive(transport);
      } catch (err) {
        if (this.isCurrentPeerCapabilityGeneration(transport, exchange.capabilityGeneration)) {
          this.peerCapabilityRecoveryRequired.add(transport);
        }
        exchange.ack.reject(err);
        return;
      }
      const capabilitiesChanged = this.publishPeerCapabilitySnapshot(
        transport,
        exchange.capabilityGeneration,
        receivedCapabilities,
      );
      this.assertTransportActive(transport);
      const acceptedCapabilities = authorizationCapabilities(receivedCapabilities);
      const receivedSnapshot: PeerCapabilitySnapshot = {
        fingerprint: capabilitySetFingerprint(acceptedCapabilities),
        capabilities: acceptedCapabilities,
      };
      if (
        capabilitiesChanged === undefined &&
        !this.isCurrentPeerCapabilitySnapshot(transport, receivedSnapshot)
      ) {
        exchange.ack.reject(new Error('Hello exchange was superseded by newer peer capabilities'));
        return;
      }

      if (!exchange.filterId) {
        exchange.ack.resolve(ack);
      } else {
        const rejection = ack.rejectedFilters.find(({ id }) => id === exchange.filterId);
        if (rejection) {
          const reason = ErrorCode[rejection.reason] ?? String(rejection.reason);
          const detail = rejection.message ? `: ${rejection.message}` : '';
          exchange.ack.reject(new Error(`${reason}${detail}`));
        } else if (ack.acceptedFilters.includes(exchange.filterId)) {
          exchange.ack.resolve(ack);
        } else {
          exchange.ack.reject(new Error(`HelloAck omitted filter result for ${exchange.filterId}`));
        }
      }
      if (capabilitiesChanged !== undefined) {
        this.rescanSubscriptionsAfterCapabilityChange(transport, capabilitiesChanged);
      }
    } finally {
      deleteTransportOwned(this.pendingHelloExchanges, transport, ack.exchangeId);
    }
  }

  private async onRibltCodewords(
    transport: DuplexTransport<SyncMessage<Op>>,
    msg: RibltCodewords,
  ): Promise<void> {
    const session = getTransportOwned(this.responderSessions, transport, msg.filterId);
    if (!session) return;
    if (!this.isCurrentPeerCapabilitySnapshot(transport, session.peerSnapshot)) {
      await this.rejectResponderCapabilityLease(transport, msg.filterId);
      return;
    }

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

    if (!this.isCurrentPeerCapabilitySnapshot(transport, session.peerSnapshot)) {
      await this.rejectResponderCapabilityLease(transport, msg.filterId);
      return;
    }

    if (senderMissing.length > 0) {
      await this.sendOpsBatches(transport, msg.filterId, senderMissing, {
        peerSnapshot: session.peerSnapshot,
      });
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
    this.assertTransportActive(transport);
    if (!msg.subscriptionId) return;
    if (!msg.filter) return;

    const peerSnapshot = this.peerCapabilitySnapshot(transport);
    if (this.requireAuthForFilters && !peerSnapshot.capabilities.some(isAuthCapability)) {
      await this.sendProtocolError(transport, {
        code: ErrorCode.UNAUTHORIZED,
        message: `missing "${AUTH_CAPABILITY_NAME}" token; send Hello before Subscribe`,
        subscriptionId: msg.subscriptionId,
      });
      return;
    }

    try {
      await this.auth?.authorizeFilter?.(msg.filter, {
        docId: this.backend.docId,
        purpose: 'subscribe',
        capabilities: peerSnapshot.capabilities,
      });
      this.assertTransportActive(transport);
    } catch (err: any) {
      await this.sendProtocolError(transport, {
        code: ErrorCode.UNAUTHORIZED,
        message: String(err?.message ?? err ?? 'unauthorized filter'),
        subscriptionId: msg.subscriptionId,
      });
      return;
    }

    try {
      const [opRefs, maxLamport] = await Promise.all([
        this.backend.listOpRefs(msg.filter),
        this.backend.maxLamport(),
      ]);
      this.assertTransportActive(transport);

      const sentOpRefs = new Set<string>();
      if (this.auth?.filterOutgoingOps && opRefs.length > 0) {
        const ops = await this.backend.getOpsByOpRefs(opRefs);
        const allowed = await this.auth.filterOutgoingOps(ops, {
          docId: this.backend.docId,
          purpose: 'subscribe',
          filter: msg.filter,
          capabilities: peerSnapshot.capabilities,
        });
        this.assertTransportActive(transport);
        assertOutgoingFilterLength(allowed, opRefs.length);
        for (let i = 0; i < opRefs.length; i += 1) {
          if (allowed[i] === true) sentOpRefs.add(bytesToHex(opRefs[i]!));
        }
      } else {
        for (const ref of opRefs) sentOpRefs.add(bytesToHex(ref));
      }
      if (!this.isCurrentPeerCapabilitySnapshot(transport, peerSnapshot)) {
        await this.sendProtocolError(transport, {
          code: ErrorCode.UNAUTHORIZED,
          message: 'peer capabilities changed while authorizing subscription; retry',
          subscriptionId: msg.subscriptionId,
        });
        return;
      }

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
      this.assertTransportActive(transport);

      // Close a race where local updates happen between `listOpRefs` and the caller
      // registering its own update hooks; this makes subscriptions robust even
      // without explicit `notifyLocalUpdate()` calls for every writer.
      void this.notifyLocalUpdate();
    } catch (err: any) {
      deleteTransportOwned(this.responderSubscriptions, transport, msg.subscriptionId);
      await this.sendProtocolError(transport, {
        code: ErrorCode.FILTER_NOT_SUPPORTED,
        message: String(err?.message ?? err ?? 'subscribe failed'),
        subscriptionId: msg.subscriptionId,
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
      exchangeId?: string;
    },
  ): Promise<void> {
    const code = ErrorCode[err.code] ?? String(err.code);
    const e = new Error(`${code}: ${err.message}`);

    if (err.exchangeId) {
      const exchange = getTransportOwned(this.pendingHelloExchanges, transport, err.exchangeId);
      if (exchange) {
        exchange.ack.reject(e);
        deleteTransportOwned(this.pendingHelloExchanges, transport, err.exchangeId);
      }
      return;
    }

    if (err.filterId) {
      this.responderHelloAbortControllers
        .get(transport)
        ?.get(err.filterId)
        ?.abort(new Error(err.message));
      deleteTransportOwned(this.responderSessions, transport, err.filterId);
      deleteTransportOwned(this.responderAwaitingUploadAcks, transport, err.filterId);
    }

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

    this.rejectPendingHelloExchanges(transport, e);

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

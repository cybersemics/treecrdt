import { RibltDecoder16, RibltEncoder16 } from "@treecrdt/riblt-wasm";

import type { SyncAuth, SyncAuthVerifyOpsResult, SyncOpPurpose } from "./auth.js";
import { ErrorCode, RibltFailureReason } from "./types.js";
import type {
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
} from "./types.js";
import type { DuplexTransport } from "./transport.js";

function randomId(prefix: string): string {
  const uuid =
    typeof globalThis.crypto?.randomUUID === "function"
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

type ResponderSession<Op> = {
  filter: Filter;
  round: number;
  decoder: RibltDecoder16;
  expectedIndex: bigint;
};

type InitiatorSession<Op> = {
  filter: Filter;
  filterId: string;
  round: number;
  ack: Pending<HelloAck>;
  status: Pending<RibltStatus>;
  receivedOps: Pending<void>;
  done: boolean;
};

export type SyncPeerOptions<Op = unknown> = {
  maxCodewords?: number;
  maxOpsPerBatch?: number;
  maxHelloFilters?: number;
  requireAuthForFilters?: boolean;
  auth?: SyncAuth<Op>;
};

export type SyncSubscribeOptions = {
  intervalMs?: number;
  immediate?: boolean;
  codewordsPerMessage?: number;
  maxCodewords?: number;
  maxOpsPerBatch?: number;
  signal?: AbortSignal;
};

export type SyncSubscription = {
  stop: () => void;
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
      signal?.removeEventListener("abort", onAbort);
      resolve(true);
    }, ms);

    const onAbort = () => {
      if (done) return;
      done = true;
      clearTimeout(timer);
      signal?.removeEventListener("abort", onAbort);
      resolve(false);
    };

    if (signal) {
      if (signal.aborted) {
        onAbort();
        return;
      }
      signal.addEventListener("abort", onAbort);
    }
  });
}

const yieldToMacrotask: () => Promise<void> = (() => {
  const setImmediateImpl = (globalThis as any).setImmediate as undefined | ((cb: () => void) => void);
  if (typeof setImmediateImpl === "function") {
    return async () => new Promise<void>((resolve) => setImmediateImpl(resolve));
  }

  if (typeof MessageChannel !== "undefined") {
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
  let out = "";
  for (const b of bytes) out += b.toString(16).padStart(2, "0");
  return out;
}

function waitForAbort(signal: AbortSignal): Promise<void> {
  if (signal.aborted) return Promise.resolve();
  return new Promise<void>((resolve) => signal.addEventListener("abort", () => resolve(), { once: true }));
}

type ResponderSubscription<Op> = {
  subscriptionId: string;
  filter: Filter;
  sentOpRefs: Set<string>;
  transport: DuplexTransport<SyncMessage<Op>>;
};

type InitiatorSubscription = {
  ack: Pending<SubscribeAck>;
  failed: Pending<unknown>;
};

export type SyncPeerAttachErrorContext<Op> = {
  transport: DuplexTransport<SyncMessage<Op>>;
  message: SyncMessage<Op>;
};

export type SyncPeerAttachOptions<Op> = {
  /**
   * Called when processing an incoming message throws.
   * Errors are otherwise best-effort swallowed to avoid unhandled promise rejections.
   */
  onError?: (err: unknown, ctx: SyncPeerAttachErrorContext<Op>) => void;
};

export class SyncPeer<Op> {
  private readonly maxCodewords: number;
  private readonly maxOpsPerBatch: number;
  private readonly maxHelloFilters: number;
  private readonly requireAuthForFilters: boolean;
  private readonly auth?: SyncAuth<Op>;
  private readonly transportHasAuth = new WeakMap<DuplexTransport<SyncMessage<Op>>, boolean>();
  private readonly transportPeerCapabilities = new WeakMap<DuplexTransport<SyncMessage<Op>>, Hello["capabilities"]>();
  private readonly responderSessions = new Map<string, ResponderSession<Op>>();
  private readonly initiatorSessions = new Map<string, InitiatorSession<Op>>();
  private readonly responderSubscriptions = new Map<string, ResponderSubscription<Op>>();
  private readonly initiatorSubscriptions = new Map<string, InitiatorSubscription>();
  private pushScheduled = false;
  private pushRunning = false;
  private pushInFlight: Promise<void> = Promise.resolve();
  private reprocessPendingRunning = false;
  private reprocessPendingInFlight: Promise<void> = Promise.resolve();

  constructor(
    private readonly backend: SyncBackend<Op>,
    opts: SyncPeerOptions<Op> = {}
  ) {
    this.maxCodewords = opts.maxCodewords ?? 50_000;
    this.maxOpsPerBatch = opts.maxOpsPerBatch ?? 5_000;
    this.auth = opts.auth;
    this.maxHelloFilters = opts.maxHelloFilters ?? 8;
    this.requireAuthForFilters = opts.requireAuthForFilters ?? Boolean(opts.auth);
  }

  attach(transport: DuplexTransport<SyncMessage<Op>>, opts: SyncPeerAttachOptions<Op> = {}): () => void {
    return transport.onMessage((msg) => {
      void this.handleMessage(transport, msg).catch((err) => {
        try {
          opts.onError?.(err, { transport, message: msg });
        } catch {
          // ignore user callback failures
        }
      });
    });
  }

  notifyLocalUpdate(): Promise<void> {
    if (this.responderSubscriptions.size === 0) return Promise.resolve();
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
        for (const sub of this.responderSubscriptions.values()) {
          try {
            await this.pushSubscription(sub);
          } catch {
            this.responderSubscriptions.delete(sub.subscriptionId);
          }
          await yieldToMacrotask();
        }
      }
    } finally {
      this.pushRunning = false;
    }
  }

  private async pushSubscription(sub: ResponderSubscription<Op>): Promise<void> {
    let opRefs: OpRef[];
    try {
      opRefs = await this.backend.listOpRefs(sub.filter);
    } catch (err) {
      this.responderSubscriptions.delete(sub.subscriptionId);
      return;
    }

    const newOpRefs: OpRef[] = [];
    for (const r of opRefs) {
      const hex = bytesToHex(r);
      if (sub.sentOpRefs.has(hex)) continue;
      newOpRefs.push(r);
    }
    if (newOpRefs.length === 0) return;

    for (let start = 0; start < newOpRefs.length; start += this.maxOpsPerBatch) {
      const chunk = newOpRefs.slice(start, start + this.maxOpsPerBatch);
      let ops = await this.backend.getOpsByOpRefs(chunk);

      // Apply peer-scoped visibility restrictions (best-effort).
      if (this.auth?.filterOutgoingOps && ops.length > 0) {
        const peerCaps = this.transportPeerCapabilities.get(sub.transport) ?? [];
        const allowed = await this.auth.filterOutgoingOps(ops, {
          docId: this.backend.docId,
          purpose: "subscribe",
          filter: sub.filter,
          capabilities: peerCaps,
        });
        if (allowed.length !== ops.length) {
          throw new Error(`filterOutgoingOps returned ${allowed.length} flags for ${ops.length} ops`);
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

      const auth = this.auth?.signOps
        ? await this.auth.signOps(ops, { docId: this.backend.docId, purpose: "subscribe", filterId: sub.subscriptionId })
        : undefined;
      if (auth && auth.length !== ops.length) throw new Error(`signOps returned ${auth.length} entries for ${ops.length} ops`);
      const done = start + this.maxOpsPerBatch >= newOpRefs.length;
      await sub.transport.send({
        v: 0,
        docId: this.backend.docId,
        payload: { case: "opsBatch", value: { filterId: sub.subscriptionId, ops, ...(auth ? { auth } : {}), done } },
      });

      for (const r of chunk) sub.sentOpRefs.add(bytesToHex(r));
      await yieldToMacrotask();
    }
  }

  async syncOnce(
    transport: DuplexTransport<SyncMessage<Op>>,
    filter: Filter,
    opts: { codewordsPerMessage?: number; maxCodewords?: number; maxOpsPerBatch?: number } = {}
  ): Promise<void> {
    const filterId = randomId("f");
    const round = 0;
    const maxLamport = await this.backend.maxLamport();
    const capabilities = (await this.auth?.helloCapabilities?.({ docId: this.backend.docId })) ?? [];
    const hello: Hello = { capabilities, filters: [{ id: filterId, filter }], maxLamport };

    const session: InitiatorSession<Op> = {
      filter,
      filterId,
      round,
      ack: deferred<HelloAck>(),
      status: deferred<RibltStatus>(),
      receivedOps: deferred<void>(),
      done: false,
    };
    this.initiatorSessions.set(filterId, session);

    try {
      await transport.send({
        v: 0,
        docId: this.backend.docId,
        payload: { case: "hello", value: hello },
      });
      await session.ack.promise;

      let opRefs = await this.backend.listOpRefs(filter);

      // If we have peer capabilities (from HelloAck) and an auth layer that can scope outgoing ops,
      // filter the local set to avoid advertising/sending ops the peer cannot receive.
      if (this.auth?.filterOutgoingOps && opRefs.length > 0) {
        const peerCaps = this.transportPeerCapabilities.get(transport) ?? [];
        const ops = await this.backend.getOpsByOpRefs(opRefs);
        const allowed = await this.auth.filterOutgoingOps(ops, {
          docId: this.backend.docId,
          purpose: "reconcile",
          filter,
          capabilities: peerCaps,
        });
        if (allowed.length !== ops.length) {
          throw new Error(`filterOutgoingOps returned ${allowed.length} flags for ${ops.length} ops`);
        }
        opRefs = opRefs.filter((_r, idx) => allowed[idx] === true);
      }

      const enc = new RibltEncoder16();
      for (const r of opRefs) enc.addSymbol(r);

      const codewordsPerMessage = opts.codewordsPerMessage ?? 512;
      const maxCodewords = BigInt(opts.maxCodewords ?? 50_000);

      let nextIndex = 0n;
      while (!session.done && nextIndex < maxCodewords) {
        const startIndex = nextIndex;
        const codewords: RibltCodewords["codewords"] = [];
        for (let i = 0; i < codewordsPerMessage && nextIndex < maxCodewords; i += 1) {
          codewords.push(enc.nextCodeword() as any);
          nextIndex += 1n;
        }

        await transport.send({
          v: 0,
          docId: this.backend.docId,
          payload: {
            case: "ribltCodewords",
            value: { filterId, round, startIndex, codewords },
          },
        });
        await yieldToMacrotask();
      }

      if (!session.done) {
        // The responder may still be processing the last streamed codewords; give it a brief grace
        // period to deliver its final ribltStatus before declaring timeout.
        await Promise.race([session.status.promise.catch(() => undefined), sleepUntil(1_000)]);
      }
      if (!session.done) throw new Error("riblt: max codewords exceeded");

      const status = await session.status.promise;
      if (status.payload.case === "failed") {
        const { reason, message } = status.payload.value;
        const name = RibltFailureReason[reason] ?? String(reason);
        const detail = message ? `: ${message}` : "";
        throw new Error(`riblt: ${name}${detail}`);
      }

      const receiverMissing =
        status.payload.case === "decoded" ? status.payload.value.receiverMissing : [];
      if (receiverMissing.length > 0) {
        await this.sendOpsBatches(transport, filterId, receiverMissing, {
          maxOpsPerBatch: opts.maxOpsPerBatch,
        });
      } else {
        await transport.send({
          v: 0,
          docId: this.backend.docId,
          payload: { case: "opsBatch", value: { filterId, ops: [], done: true } },
        });
      }

      await session.receivedOps.promise;
    } finally {
      this.initiatorSessions.delete(filterId);
    }
  }

  private async sendOpsBatches(
    transport: DuplexTransport<SyncMessage<Op>>,
    filterId: string,
    opRefs: OpRef[],
    opts: { maxOpsPerBatch?: number } = {}
  ): Promise<void> {
    const maxOpsPerBatch = opts.maxOpsPerBatch ?? this.maxOpsPerBatch;
    if (!Number.isFinite(maxOpsPerBatch) || maxOpsPerBatch <= 0) {
      throw new Error(`invalid maxOpsPerBatch: ${maxOpsPerBatch}`);
    }

    const filter =
      this.responderSessions.get(filterId)?.filter ??
      this.initiatorSessions.get(filterId)?.filter ??
      undefined;
    const peerCaps = this.transportPeerCapabilities.get(transport) ?? [];

    if (opRefs.length === 0) {
      await transport.send({
        v: 0,
        docId: this.backend.docId,
        payload: { case: "opsBatch", value: { filterId, ops: [], done: true } },
      });
      return;
    }

    for (let start = 0; start < opRefs.length; start += maxOpsPerBatch) {
      const chunk = opRefs.slice(start, start + maxOpsPerBatch);
      let ops = await this.backend.getOpsByOpRefs(chunk);

      if (filter && this.auth?.filterOutgoingOps && ops.length > 0) {
        const allowed = await this.auth.filterOutgoingOps(ops, {
          docId: this.backend.docId,
          purpose: "reconcile",
          filter,
          capabilities: peerCaps,
        });
        if (allowed.length !== ops.length) {
          throw new Error(`filterOutgoingOps returned ${allowed.length} flags for ${ops.length} ops`);
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
          await transport.send({
            v: 0,
            docId: this.backend.docId,
            payload: { case: "opsBatch", value: { filterId, ops: [], done: true } },
          });
        }
        continue;
      }

      const auth = this.auth?.signOps
        ? await this.auth.signOps(ops, { docId: this.backend.docId, purpose: "reconcile", filterId })
        : undefined;
      if (auth && auth.length !== ops.length) {
        throw new Error(`signOps returned ${auth.length} entries for ${ops.length} ops`);
      }

      await transport.send({
        v: 0,
        docId: this.backend.docId,
        payload: { case: "opsBatch", value: { filterId, ops, ...(auth ? { auth } : {}), done } },
      });
      await yieldToMacrotask();
    }
  }

  subscribe(
    transport: DuplexTransport<SyncMessage<Op>>,
    filter: Filter,
    opts: SyncSubscribeOptions = {}
  ): SyncSubscription {
    const controller = new AbortController();
    if (opts.signal) {
      if (opts.signal.aborted) {
        controller.abort();
      } else {
        opts.signal.addEventListener("abort", () => controller.abort(), { once: true });
      }
    }

    const signal = controller.signal;
    const intervalMs = opts.intervalMs ?? 0;
    const immediate = opts.immediate ?? true;
    const codewordsPerMessage = opts.codewordsPerMessage;
    const maxCodewords = opts.maxCodewords;
    const maxOpsPerBatch = opts.maxOpsPerBatch;

    const subscriptionId = randomId("sub");
    const session: InitiatorSubscription = { ack: deferred<SubscribeAck>(), failed: deferred<unknown>() };
    this.initiatorSubscriptions.set(subscriptionId, session);

    const done = (async () => {
      let sentSubscribe = false;
      try {
        if (signal.aborted) return;

        // If the responder requires capability-gated filters/subscriptions, send an initial
        // Hello (no filters) so it can record our capabilities before Subscribe arrives.
        if (this.auth?.helloCapabilities) {
          const [maxLamport, capabilities] = await Promise.all([
            this.backend.maxLamport(),
            this.auth.helloCapabilities({ docId: this.backend.docId }),
          ]);
          await transport.send({
            v: 0,
            docId: this.backend.docId,
            payload: { case: "hello", value: { capabilities, filters: [], maxLamport } },
          });
        }

        await transport.send({
          v: 0,
          docId: this.backend.docId,
          payload: { case: "subscribe", value: { subscriptionId, filter } },
        });
        sentSubscribe = true;

        const ackOrAbort = await Promise.race([
          session.ack.promise.then(() => ({ case: "ack" as const })),
          session.failed.promise.then((err) => ({ case: "failed" as const, err })),
          waitForAbort(signal).then(() => ({ case: "aborted" as const })),
        ]);
        if (ackOrAbort.case === "aborted") return;
        if (ackOrAbort.case === "failed") throw ackOrAbort.err;

        if (signal.aborted) return;
        if (immediate) {
          await this.syncOnce(transport, filter, { codewordsPerMessage, maxCodewords, maxOpsPerBatch });
        }

        if (intervalMs > 0) {
          while (!signal.aborted) {
            const slept = await sleepUntil(intervalMs, signal);
            if (!slept) break;
            if (signal.aborted) break;
            await this.syncOnce(transport, filter, { codewordsPerMessage, maxCodewords, maxOpsPerBatch });
          }
        } else {
          await Promise.race([
            waitForAbort(signal),
            session.failed.promise.then((err) => {
              throw err;
            }),
          ]);
        }
      } finally {
        this.initiatorSubscriptions.delete(subscriptionId);
        if (sentSubscribe) {
          try {
            await transport.send({
              v: 0,
              docId: this.backend.docId,
              payload: { case: "unsubscribe", value: { subscriptionId } },
            });
          } catch {
            // ignore transport failures during teardown
          }
        }
      }
    })();

    return { stop: () => controller.abort(), done };
  }

  private async handleMessage(
    transport: DuplexTransport<SyncMessage<Op>>,
    msg: SyncMessage<Op>
  ): Promise<void> {
    if (msg.docId !== this.backend.docId) return;

    if (msg.payload.case === "error") {
      try {
        await this.onError(msg.payload.value);
      } catch {
        // ignore error while handling error
      }
      return;
    }

    try {
      switch (msg.payload.case) {
        case "hello":
          await this.onHello(transport, msg.payload.value);
          return;
        case "helloAck":
          await this.onHelloAck(transport, msg.payload.value);
          return;
        case "ribltCodewords":
          await this.onRibltCodewords(transport, msg.payload.value);
          return;
        case "ribltStatus":
          await this.onRibltStatus(msg.payload.value);
          return;
        case "opsBatch":
          await this.onOpsBatch(msg.payload.value);
          return;
        case "subscribe":
          await this.onSubscribe(transport, msg.payload.value);
          return;
        case "subscribeAck":
          await this.onSubscribeAck(msg.payload.value);
          return;
        case "unsubscribe":
          await this.onUnsubscribe(msg.payload.value);
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
        case "ribltCodewords":
        case "ribltStatus":
        case "opsBatch":
          filterId = msg.payload.value.filterId;
          if (msg.payload.case === "opsBatch" && this.initiatorSubscriptions.has(filterId)) {
            subscriptionId = filterId;
          }
          break;
        case "subscribe":
        case "subscribeAck":
        case "unsubscribe":
          subscriptionId = msg.payload.value.subscriptionId;
          break;
      }

      try {
        await this.onError({
          code: ErrorCode.ERROR_CODE_UNSPECIFIED,
          message: String(err?.message ?? err ?? "error"),
          ...(filterId ? { filterId } : {}),
          ...(subscriptionId ? { subscriptionId } : {}),
        });

        await transport.send({
          v: 0,
          docId: this.backend.docId,
          payload: {
            case: "error",
            value: {
              code: ErrorCode.ERROR_CODE_UNSPECIFIED,
              message: String(err?.message ?? err ?? "error"),
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

  private async onHello(
    transport: DuplexTransport<SyncMessage<Op>>,
    hello: Hello
  ): Promise<void> {
    const hasAuthCapability = hello.capabilities.some((c) => c.name === "auth.capability");

    // Record the presence of auth capabilities immediately so concurrent messages (e.g. Subscribe)
    // can't race and get rejected before `onHello` completes.
    if (hasAuthCapability) this.transportHasAuth.set(transport, true);
    if (hasAuthCapability) this.transportPeerCapabilities.set(transport, hello.capabilities);

    let ackCapabilities: HelloAck["capabilities"] = [];
    try {
      ackCapabilities = (await this.auth?.onHello?.(hello, { docId: this.backend.docId })) ?? [];
    } catch (err: any) {
      await transport.send({
        v: 0,
        docId: this.backend.docId,
        payload: {
          case: "error",
          value: { code: ErrorCode.ERROR_CODE_UNSPECIFIED, message: String(err?.message ?? err ?? "auth error") },
        },
      });
      return;
    }

    const maxLamport = await this.backend.maxLamport();
    const acceptedFilters: string[] = [];
    const rejectedFilters: HelloAck["rejectedFilters"] = [];

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
          message: 'missing "auth.capability" token; send a valid capability token in Hello.capabilities',
        });
        continue;
      }

      try {
        await this.auth?.authorizeFilter?.(filter, { docId: this.backend.docId, purpose: "hello", capabilities: hello.capabilities });
      } catch (err: any) {
        rejectedFilters.push({
          id,
          reason: ErrorCode.UNAUTHORIZED,
          message: String(err?.message ?? err ?? "unauthorized filter"),
        });
        continue;
      }

      let localOpRefs: OpRef[];
      try {
        localOpRefs = await this.backend.listOpRefs(filter);
      } catch (err: any) {
        rejectedFilters.push({
          id,
          reason: ErrorCode.FILTER_NOT_SUPPORTED,
          message: String(err?.message ?? err ?? "filter not supported"),
        });
        continue;
      }

      if (!("all" in filter) && this.auth?.filterOutgoingOps && localOpRefs.length > 0) {
        try {
          const ops = await this.backend.getOpsByOpRefs(localOpRefs);
          const allowed = await this.auth.filterOutgoingOps(ops, {
            docId: this.backend.docId,
            purpose: "hello",
            filter,
            capabilities: hello.capabilities,
          });
          if (allowed.length !== ops.length) {
            throw new Error(`filterOutgoingOps returned ${allowed.length} flags for ${ops.length} ops`);
          }
          localOpRefs = localOpRefs.filter((_r, idx) => allowed[idx] === true);
        } catch (err: any) {
          rejectedFilters.push({
            id,
            reason: ErrorCode.UNAUTHORIZED,
            message: String(err?.message ?? err ?? "failed to filter ops"),
          });
          continue;
        }
      }

      acceptedFilters.push(id);
      const decoder = new RibltDecoder16();
      for (const r of localOpRefs) decoder.addLocalSymbol(r);
      this.responderSessions.set(id, {
        filter,
        round: 0,
        decoder,
        expectedIndex: 0n,
      });
    }

    await transport.send({
      v: 0,
      docId: this.backend.docId,
      payload: {
        case: "helloAck",
        value: {
          capabilities: ackCapabilities,
          acceptedFilters,
          rejectedFilters,
          maxLamport,
        },
      },
    });
  }

  private async onHelloAck(transport: DuplexTransport<SyncMessage<Op>>, ack: HelloAck): Promise<void> {
    await this.auth?.onHelloAck?.(ack, { docId: this.backend.docId });

    const hasAuthCapability = ack.capabilities.some((c) => c.name === "auth.capability");
    if (hasAuthCapability) this.transportHasAuth.set(transport, true);
    if (hasAuthCapability) this.transportPeerCapabilities.set(transport, ack.capabilities);

    for (const id of ack.acceptedFilters) {
      const session = this.initiatorSessions.get(id);
      if (session) session.ack.resolve(ack);
    }
    for (const rej of ack.rejectedFilters) {
      const session = this.initiatorSessions.get(rej.id);
      if (session) {
        const reason = ErrorCode[rej.reason] ?? String(rej.reason);
        const detail = rej.message ? `: ${rej.message}` : "";
        session.ack.reject(new Error(`${reason}${detail}`));
      }
    }
  }

  private async onRibltCodewords(
    transport: DuplexTransport<SyncMessage<Op>>,
    msg: RibltCodewords
  ): Promise<void> {
    const session = this.responderSessions.get(msg.filterId);
    if (!session) return;

    if (msg.round !== session.round) return;
    if (msg.startIndex !== session.expectedIndex) {
      await transport.send({
        v: 0,
        docId: this.backend.docId,
        payload: {
          case: "ribltStatus",
          value: {
            filterId: msg.filterId,
            round: msg.round,
            payload: { case: "failed", value: { reason: RibltFailureReason.OUT_OF_ORDER } },
          },
        },
      });
      this.responderSessions.delete(msg.filterId);
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
          case: "ribltStatus",
          value: {
            filterId: msg.filterId,
            round: msg.round,
            payload: {
              case: "failed",
              value: {
                reason: RibltFailureReason.DECODE_FAILED,
                message: String(err?.message ?? err ?? ""),
              },
            },
          },
        },
      });
      this.responderSessions.delete(msg.filterId);
      return;
    }

    if (!session.decoder.decoded()) {
      if (session.expectedIndex >= BigInt(this.maxCodewords)) {
        await transport.send({
          v: 0,
          docId: this.backend.docId,
          payload: {
            case: "ribltStatus",
            value: {
              filterId: msg.filterId,
              round: msg.round,
              payload: { case: "failed", value: { reason: RibltFailureReason.MAX_CODEWORDS_EXCEEDED } },
            },
          },
        });
        this.responderSessions.delete(msg.filterId);
      }
      return;
    }

    const receiverMissing = session.decoder.remoteMissing() as unknown as OpRef[];
    const senderMissing = session.decoder.localMissing() as unknown as OpRef[];
    const codewordsReceived = session.decoder.codewordsReceived();

    await transport.send({
      v: 0,
      docId: this.backend.docId,
      payload: {
        case: "ribltStatus",
        value: {
          filterId: msg.filterId,
          round: msg.round,
          payload: {
            case: "decoded",
            value: { senderMissing, receiverMissing, codewordsReceived },
          },
        },
      },
    });

    if (senderMissing.length > 0) {
      await this.sendOpsBatches(transport, msg.filterId, senderMissing);
    } else {
      await transport.send({
        v: 0,
        docId: this.backend.docId,
        payload: { case: "opsBatch", value: { filterId: msg.filterId, ops: [], done: true } },
      });
    }

    this.responderSessions.delete(msg.filterId);
  }

  private async onSubscribe(
    transport: DuplexTransport<SyncMessage<Op>>,
    msg: Subscribe
  ): Promise<void> {
    if (!msg.subscriptionId) return;
    if (!msg.filter) return;

    if (this.requireAuthForFilters && !this.transportHasAuth.get(transport)) {
      await transport.send({
        v: 0,
        docId: this.backend.docId,
        payload: {
          case: "error",
          value: {
            code: ErrorCode.UNAUTHORIZED,
            message: 'missing "auth.capability" token; send Hello before Subscribe',
            subscriptionId: msg.subscriptionId,
          },
        },
      });
      return;
    }

    try {
      const peerCaps = this.transportPeerCapabilities.get(transport) ?? [];
      await this.auth?.authorizeFilter?.(msg.filter, { docId: this.backend.docId, purpose: "subscribe", capabilities: peerCaps });
    } catch (err: any) {
      await transport.send({
        v: 0,
        docId: this.backend.docId,
        payload: {
          case: "error",
          value: {
            code: ErrorCode.UNAUTHORIZED,
            message: String(err?.message ?? err ?? "unauthorized filter"),
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

      this.responderSubscriptions.set(msg.subscriptionId, {
        subscriptionId: msg.subscriptionId,
        filter: msg.filter,
        sentOpRefs,
        transport,
      });

      await transport.send({
        v: 0,
        docId: this.backend.docId,
        payload: {
          case: "subscribeAck",
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
          case: "error",
          value: {
            code: ErrorCode.FILTER_NOT_SUPPORTED,
            message: String(err?.message ?? err ?? "subscribe failed"),
            subscriptionId: msg.subscriptionId,
          },
        },
      });
    }
  }

  private async onSubscribeAck(ack: SubscribeAck): Promise<void> {
    const sub = this.initiatorSubscriptions.get(ack.subscriptionId);
    if (!sub) return;
    sub.ack.resolve(ack);
  }

  private async onUnsubscribe(msg: Unsubscribe): Promise<void> {
    this.responderSubscriptions.delete(msg.subscriptionId);
  }

  private async onError(err: { code: ErrorCode; message: string; filterId?: string; subscriptionId?: string }): Promise<void> {
    if (err.subscriptionId) {
      const sub = this.initiatorSubscriptions.get(err.subscriptionId);
      if (sub) {
        const code = ErrorCode[err.code] ?? String(err.code);
        const e = new Error(`${code}: ${err.message}`);
        sub.ack.reject(e);
        sub.failed.reject(e);
        this.initiatorSubscriptions.delete(err.subscriptionId);
      }
    }

    if (!err.filterId) {
      if (this.initiatorSessions.size === 0) return;
      const code = ErrorCode[err.code] ?? String(err.code);
      const e = new Error(`${code}: ${err.message}`);
      for (const session of this.initiatorSessions.values()) {
        session.done = true;
        session.ack.reject(e);
        session.status.reject(e);
        session.receivedOps.reject(e);
      }
      this.initiatorSessions.clear();
      return;
    }
    const session = this.initiatorSessions.get(err.filterId);
    if (!session) return;

    const code = ErrorCode[err.code] ?? String(err.code);
    const e = new Error(`${code}: ${err.message}`);
    session.done = true;
    session.ack.reject(e);
    session.status.reject(e);
    session.receivedOps.reject(e);
    this.initiatorSessions.delete(err.filterId);
  }

  private async onRibltStatus(status: RibltStatus): Promise<void> {
    const session = this.initiatorSessions.get(status.filterId);
    if (!session) return;
    session.done = true;
    session.status.resolve(status);
  }

  private async onOpsBatch(batch: OpsBatch<Op>): Promise<void> {
    const purpose: SyncOpPurpose = this.initiatorSubscriptions.has(batch.filterId) ? "subscribe" : "reconcile";
    const auth = batch.auth;
    if (auth && auth.length !== batch.ops.length) {
      throw new Error(`OpsBatch.auth length ${auth.length} does not match ops length ${batch.ops.length}`);
    }

    const verifyRes = await this.auth?.verifyOps?.(batch.ops, auth, {
      docId: this.backend.docId,
      purpose,
      filterId: batch.filterId,
    });
    const dispositions =
      verifyRes === undefined
        ? undefined
        : (verifyRes as SyncAuthVerifyOpsResult)?.dispositions ??
          (() => {
            throw new Error("verifyOps must return void or { dispositions: [...] }");
          })();
    if (dispositions && dispositions.length !== batch.ops.length) {
      throw new Error(`verifyOps returned ${dispositions.length} dispositions for ${batch.ops.length} ops`);
    }
    if (auth && auth.length > 0) {
      await this.auth?.onVerifiedOps?.(batch.ops, auth, { docId: this.backend.docId, purpose, filterId: batch.filterId });
    }

    const pending: PendingOp<Op>[] = [];
    const allowedOps: Op[] = [];

    for (let i = 0; i < batch.ops.length; i += 1) {
      const op = batch.ops[i]!;
      const d = dispositions?.[i];
      if (!d || d.status === "allow") {
        allowedOps.push(op);
        continue;
      }
      if (d.status !== "pending_context") {
        throw new Error(`unknown disposition: ${(d as any)?.status ?? String(d)}`);
      }
      if (!auth) {
        throw new Error("verifyOps returned pending_context but OpsBatch.auth is missing");
      }
      pending.push({ op, auth: auth[i]!, reason: "missing_context", ...(d.message ? { message: d.message } : {}) });
    }

    if (pending.length > 0) {
      if (!this.backend.storePendingOps) {
        throw new Error("received ops requiring pending-context handling, but backend.storePendingOps is not implemented");
      }
      await this.backend.storePendingOps(pending);
    }

    await this.backend.applyOps(allowedOps);
    if (allowedOps.length > 0) void this.notifyLocalUpdate();
    await this.reprocessPendingOps();

    const session = this.initiatorSessions.get(batch.filterId);
    if (session && batch.done) session.receivedOps.resolve();

    const responderSession = this.responderSessions.get(batch.filterId);
    if (responderSession && batch.done) this.responderSessions.delete(batch.filterId);
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
      const maxRounds = 100;
      for (let round = 0; round < maxRounds; round += 1) {
        const pending = await this.backend.listPendingOps!();
        if (pending.length === 0) return;

        let progress = false;
        let appliedAny = false;

        for (const p of pending) {
          const ctx = { docId: this.backend.docId, purpose: "reprocess_pending" as const, filterId: "__pending__" };
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
              : (res as SyncAuthVerifyOpsResult)?.dispositions ??
                (() => {
                  throw new Error("verifyOps must return void or { dispositions: [...] }");
                })();
          const d = dispositions?.[0];
          if (d && d.status === "pending_context") continue;

          await this.backend.applyOps([p.op]);
          await this.backend.deletePendingOps!([p.op]);
          progress = true;
          appliedAny = true;
        }

        if (appliedAny) void this.notifyLocalUpdate();
        if (!progress) return;
      }
      throw new Error("pending-op reprocessing exceeded max rounds");
    })().finally(() => {
      this.reprocessPendingRunning = false;
    });

    await this.reprocessPendingInFlight;
  }
}

import { RibltDecoder16, RibltEncoder16 } from "@treecrdt/riblt-wasm";

import { ErrorCode, RibltFailureReason } from "./types.js";
import type {
  Filter,
  Hello,
  HelloAck,
  OpRef,
  OpsBatch,
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

export type SyncPeerOptions = {
  maxCodewords?: number;
  maxOpsPerBatch?: number;
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

export class SyncPeer<Op> {
  private readonly maxCodewords: number;
  private readonly maxOpsPerBatch: number;
  private readonly responderSessions = new Map<string, ResponderSession<Op>>();
  private readonly initiatorSessions = new Map<string, InitiatorSession<Op>>();
  private readonly responderSubscriptions = new Map<string, ResponderSubscription<Op>>();
  private readonly initiatorSubscriptions = new Map<string, InitiatorSubscription>();
  private pushScheduled = false;
  private pushRunning = false;
  private pushInFlight: Promise<void> = Promise.resolve();

  constructor(
    private readonly backend: SyncBackend<Op>,
    opts: SyncPeerOptions = {}
  ) {
    this.maxCodewords = opts.maxCodewords ?? 50_000;
    this.maxOpsPerBatch = opts.maxOpsPerBatch ?? 5_000;
  }

  attach(transport: DuplexTransport<SyncMessage<Op>>): () => void {
    return transport.onMessage((msg) => void this.handleMessage(transport, msg));
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
      const ops = await this.backend.getOpsByOpRefs(chunk);
      const done = start + this.maxOpsPerBatch >= newOpRefs.length;
      await sub.transport.send({
        v: 0,
        docId: this.backend.docId,
        payload: { case: "opsBatch", value: { filterId: sub.subscriptionId, ops, done } },
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
    const hello: Hello = { capabilities: [], filters: [{ id: filterId, filter }], maxLamport };

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

      const opRefs = await this.backend.listOpRefs(filter);
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
      const ops = await this.backend.getOpsByOpRefs(chunk);
      const done = start + maxOpsPerBatch >= opRefs.length;
      await transport.send({
        v: 0,
        docId: this.backend.docId,
        payload: { case: "opsBatch", value: { filterId, ops, done } },
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

  /**
   * Legacy polling subscription (periodic `syncOnce`), kept as a fallback.
   */
  subscribePolling(
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
    const intervalMs = opts.intervalMs ?? 1_000;
    const immediate = opts.immediate ?? true;
    const codewordsPerMessage = opts.codewordsPerMessage;
    const maxCodewords = opts.maxCodewords;
    const maxOpsPerBatch = opts.maxOpsPerBatch;

    const done = (async () => {
      let first = true;
      while (!signal.aborted) {
        if (first && !immediate) {
          first = false;
        } else {
          first = false;
          await this.syncOnce(transport, filter, { codewordsPerMessage, maxCodewords, maxOpsPerBatch });
        }
        if (signal.aborted) break;
        const slept = await sleepUntil(intervalMs, signal);
        if (!slept) break;
      }
    })();

    return {
      stop: () => controller.abort(),
      done,
    };
  }

  private async handleMessage(
    transport: DuplexTransport<SyncMessage<Op>>,
    msg: SyncMessage<Op>
  ): Promise<void> {
    if (msg.docId !== this.backend.docId) return;

    switch (msg.payload.case) {
      case "hello":
        return this.onHello(transport, msg.payload.value);
      case "helloAck":
        return this.onHelloAck(msg.payload.value);
      case "ribltCodewords":
        return this.onRibltCodewords(transport, msg.payload.value);
      case "ribltStatus":
        return this.onRibltStatus(msg.payload.value);
      case "opsBatch":
        return this.onOpsBatch(msg.payload.value);
      case "subscribe":
        return this.onSubscribe(transport, msg.payload.value);
      case "subscribeAck":
        return this.onSubscribeAck(msg.payload.value);
      case "unsubscribe":
        return this.onUnsubscribe(msg.payload.value);
      case "error":
        return this.onError(msg.payload.value);
      default: {
        const _exhaustive: never = msg.payload;
        return _exhaustive;
      }
    }
  }

  private async onHello(
    transport: DuplexTransport<SyncMessage<Op>>,
    hello: Hello
  ): Promise<void> {
    const maxLamport = await this.backend.maxLamport();
    const acceptedFilters: string[] = [];

    for (const spec of hello.filters) {
      acceptedFilters.push(spec.id);
      const localOpRefs = await this.backend.listOpRefs(spec.filter);
      const decoder = new RibltDecoder16();
      for (const r of localOpRefs) decoder.addLocalSymbol(r);
      this.responderSessions.set(spec.id, {
        filter: spec.filter,
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
          capabilities: [],
          acceptedFilters,
          rejectedFilters: [],
          maxLamport,
        },
      },
    });
  }

  private async onHelloAck(ack: HelloAck): Promise<void> {
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

    if (!err.filterId) return;
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
    await this.backend.applyOps(batch.ops);
    if (batch.ops.length > 0) void this.notifyLocalUpdate();

    const session = this.initiatorSessions.get(batch.filterId);
    if (session && batch.done) session.receivedOps.resolve();

    const responderSession = this.responderSessions.get(batch.filterId);
    if (responderSession && batch.done) this.responderSessions.delete(batch.filterId);
  }
}

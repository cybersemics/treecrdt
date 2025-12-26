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
  SyncBackend,
  SyncMessage,
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
};

export type SyncSubscribeOptions = {
  intervalMs?: number;
  immediate?: boolean;
  codewordsPerMessage?: number;
  maxCodewords?: number;
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

export class SyncPeer<Op> {
  private readonly maxCodewords: number;
  private readonly responderSessions = new Map<string, ResponderSession<Op>>();
  private readonly initiatorSessions = new Map<string, InitiatorSession<Op>>();

  constructor(
    private readonly backend: SyncBackend<Op>,
    opts: SyncPeerOptions = {}
  ) {
    this.maxCodewords = opts.maxCodewords ?? 50_000;
  }

  attach(transport: DuplexTransport<SyncMessage<Op>>): () => void {
    return transport.onMessage((msg) => void this.handleMessage(transport, msg));
  }

  async syncOnce(
    transport: DuplexTransport<SyncMessage<Op>>,
    filter: Filter,
    opts: { codewordsPerMessage?: number; maxCodewords?: number } = {}
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
        const ops = await this.backend.getOpsByOpRefs(receiverMissing);
        const batch: OpsBatch<Op> = { filterId, ops, done: true };
        await transport.send({
          v: 0,
          docId: this.backend.docId,
          payload: { case: "opsBatch", value: batch },
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
    const intervalMs = opts.intervalMs ?? 1_000;
    const immediate = opts.immediate ?? true;
    const codewordsPerMessage = opts.codewordsPerMessage;
    const maxCodewords = opts.maxCodewords;

    const done = (async () => {
      let first = true;
      while (!signal.aborted) {
        if (first && !immediate) {
          first = false;
        } else {
          first = false;
          await this.syncOnce(transport, filter, { codewordsPerMessage, maxCodewords });
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
        session.decoder.addCodeword(cw as any);
        session.expectedIndex += 1n;
        if (session.expectedIndex > BigInt(this.maxCodewords)) break;
      }
      if (session.expectedIndex > BigInt(this.maxCodewords)) {
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
        return;
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

    if (!session.decoder.decoded()) return;

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
      const ops = await this.backend.getOpsByOpRefs(senderMissing);
      await transport.send({
        v: 0,
        docId: this.backend.docId,
        payload: { case: "opsBatch", value: { filterId: msg.filterId, ops, done: true } },
      });
    } else {
      await transport.send({
        v: 0,
        docId: this.backend.docId,
        payload: { case: "opsBatch", value: { filterId: msg.filterId, ops: [], done: true } },
      });
    }

    this.responderSessions.delete(msg.filterId);
  }

  private async onError(err: { code: ErrorCode; message: string; filterId?: string }): Promise<void> {
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

    const session = this.initiatorSessions.get(batch.filterId);
    if (session && batch.done) session.receivedOps.resolve();

    const responderSession = this.responderSessions.get(batch.filterId);
    if (responderSession && batch.done) this.responderSessions.delete(batch.filterId);
  }
}

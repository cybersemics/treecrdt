export type Unsubscribe = () => void;

export interface DuplexTransport<M> {
  send(msg: M): Promise<void>;
  onMessage(handler: (msg: M) => void): Unsubscribe;
}

export type WireCodec<Message, Wire> = {
  encode(message: Message): Wire;
  decode(wire: Wire): Message;
};

export type BroadcastChannelLike = {
  readonly name: string;
  postMessage(message: unknown): void;
  addEventListener(type: "message", listener: (event: { data: unknown }) => void): void;
  removeEventListener(type: "message", listener: (event: { data: unknown }) => void): void;
  close(): void;
};

function coerceMessageDataToBytes(data: unknown): Uint8Array | null {
  if (data instanceof Uint8Array) return data;
  if (data instanceof ArrayBuffer) return new Uint8Array(data);
  if (ArrayBuffer.isView(data) && data.buffer instanceof ArrayBuffer) {
    return new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
  }
  return null;
}

export function createBroadcastDuplex<Message>(
  channel: BroadcastChannelLike,
  selfId: string,
  peerId: string,
  codec: WireCodec<Message, Uint8Array>,
  opts: {
    debug?: boolean;
    log?: (line: string) => void;
    createChannel?: (name: string) => BroadcastChannelLike;
  } = {}
): DuplexTransport<Message> & { close: () => void } {
  const createChannel =
    opts.createChannel ??
    ((name) => {
      const Ctor = (globalThis as any).BroadcastChannel as undefined | (new (name: string) => BroadcastChannelLike);
      if (!Ctor) throw new Error("BroadcastChannel is not available in this environment");
      return new Ctor(name);
    });

  const incoming = createChannel(`${channel.name}:sync:${peerId}->${selfId}`);
  const outgoing = createChannel(`${channel.name}:sync:${selfId}->${peerId}`);
  const handlers = new Set<(msg: Message) => void>();
  let listening = false;
  const debug = Boolean(opts.debug);
  const log = opts.log ?? ((line) => console.debug(line));

  const onMessage = (ev: { data: unknown }) => {
    const bytes = coerceMessageDataToBytes(ev.data);
    if (!bytes) return;

    let msg: Message;
    try {
      msg = codec.decode(bytes);
    } catch (err) {
      if (debug) log(`[sync:${selfId}] decode error from ${peerId}: ${String(err)}`);
      return;
    }
    for (const h of handlers) h(msg);
  };

  return {
    async send(msg) {
      outgoing.postMessage(codec.encode(msg));
    },
    onMessage(handler) {
      handlers.add(handler);
      if (!listening) {
        incoming.addEventListener("message", onMessage);
        listening = true;
      }

      return () => {
        handlers.delete(handler);
        if (handlers.size === 0 && listening) {
          incoming.removeEventListener("message", onMessage);
          listening = false;
        }
      };
    },
    close() {
      incoming.close();
      outgoing.close();
    },
  };
}

export function wrapDuplexTransportWithCodec<Wire, Message>(
  transport: DuplexTransport<Wire>,
  codec: WireCodec<Message, Wire>
): DuplexTransport<Message> {
  return {
    send: async (msg) => transport.send(codec.encode(msg)),
    onMessage: (handler) => transport.onMessage((wire) => handler(codec.decode(wire))),
  };
}

export function createInMemoryDuplex<M>(): [DuplexTransport<M>, DuplexTransport<M>] {
  const aHandlers = new Set<(msg: M) => void>();
  const bHandlers = new Set<(msg: M) => void>();

  const a: DuplexTransport<M> = {
    async send(msg) {
      queueMicrotask(() => {
        for (const h of bHandlers) h(msg);
      });
    },
    onMessage(handler) {
      aHandlers.add(handler);
      return () => aHandlers.delete(handler);
    },
  };

  const b: DuplexTransport<M> = {
    async send(msg) {
      queueMicrotask(() => {
        for (const h of aHandlers) h(msg);
      });
    },
    onMessage(handler) {
      bHandlers.add(handler);
      return () => bHandlers.delete(handler);
    },
  };

  return [a, b];
}

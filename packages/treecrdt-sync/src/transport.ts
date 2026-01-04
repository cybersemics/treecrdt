export type Unsubscribe = () => void;

export interface DuplexTransport<M> {
  send(msg: M): Promise<void>;
  onMessage(handler: (msg: M) => void): Unsubscribe;
}

export type WireCodec<Message, Wire> = {
  encode(message: Message): Wire;
  decode(wire: Wire): Message;
};

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

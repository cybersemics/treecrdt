import { createTransportCloseController } from './close-signal.js';
import type { TransportCloseSignal } from './close-signal.js';

export { createTransportCloseController } from './close-signal.js';
export type {
  TransportCloseController,
  TransportCloseHandler,
  TransportCloseSignal,
} from './close-signal.js';

export type Unsubscribe = () => void;

export interface DuplexTransport<M> {
  send(msg: M): Promise<void>;
  onMessage(handler: (msg: M) => void): Unsubscribe;
  /**
   * Permanently close the transport.
   *
   * Implementations must be idempotent, synchronously close {@link closeSignal},
   * reject future sends, and stop future message delivery.
   */
  close(reason?: unknown): void;
  /**
   * A required, read-only signal for permanent transport closure. It must close
   * after either a local {@link close} call or an observable underlying/remote closure.
   */
  readonly closeSignal: TransportCloseSignal;
}

export type WireCodec<Message, Wire> = {
  encode(message: Message): Wire;
  decode(wire: Wire): Message;
};

export type BroadcastChannelLike = {
  readonly name: string;
  postMessage(message: unknown): void;
  addEventListener(type: 'message', listener: (event: { data: unknown }) => void): void;
  removeEventListener(type: 'message', listener: (event: { data: unknown }) => void): void;
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
  } = {},
): DuplexTransport<Message> {
  const createChannel =
    opts.createChannel ??
    ((name) => {
      const Ctor = (globalThis as any).BroadcastChannel as
        | undefined
        | (new (name: string) => BroadcastChannelLike);
      if (!Ctor) throw new Error('BroadcastChannel is not available in this environment');
      return new Ctor(name);
    });

  const incoming = createChannel(`${channel.name}:sync:${peerId}->${selfId}`);
  const outgoing = createChannel(`${channel.name}:sync:${selfId}->${peerId}`);
  const handlers = new Set<(msg: Message) => void>();
  const closeController = createTransportCloseController();
  let listening = false;
  const debug = Boolean(opts.debug);
  const log = opts.log ?? ((line) => console.debug(line));

  const onMessage = (ev: { data: unknown }) => {
    if (closeController.signal.closed) return;
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
      if (closeController.signal.closed) {
        throw closeController.signal.reason instanceof Error
          ? closeController.signal.reason
          : new Error('broadcast transport is closed');
      }
      outgoing.postMessage(codec.encode(msg));
    },
    onMessage(handler) {
      if (closeController.signal.closed) return () => {};
      handlers.add(handler);
      if (!listening) {
        incoming.addEventListener('message', onMessage);
        listening = true;
      }

      return () => {
        handlers.delete(handler);
        if (handlers.size === 0 && listening) {
          incoming.removeEventListener('message', onMessage);
          listening = false;
        }
      };
    },
    close(reason) {
      if (closeController.signal.closed) return;
      closeController.close(reason);
      handlers.clear();
      if (listening) {
        incoming.removeEventListener('message', onMessage);
        listening = false;
      }
      try {
        incoming.close();
      } catch {
        // Keep closing the remaining channel.
      }
      try {
        outgoing.close();
      } catch {
        // The close signal is already settled.
      }
    },
    closeSignal: closeController.signal,
  };
}

export function wrapDuplexTransportWithCodec<Wire, Message>(
  transport: DuplexTransport<Wire>,
  codec: WireCodec<Message, Wire>,
): DuplexTransport<Message> {
  const closeController = createTransportCloseController();

  const closeForProtocolError = (error: unknown) => {
    // Settle before close() so a synchronous close callback or reentrant
    // message cannot replace the decode failure or deliver another message.
    closeController.close(error);
    try {
      transport.close(error);
    } catch {
      // The decode failure remains the primary close reason.
    }
  };

  let unsubscribeUpstreamClose = () => {};
  if (transport.closeSignal.closed) {
    closeController.close(transport.closeSignal.reason);
  } else {
    unsubscribeUpstreamClose = transport.closeSignal.subscribe((reason) => {
      closeController.close(reason);
    });
  }
  closeController.signal.subscribe(() => {
    unsubscribeUpstreamClose();
  });

  return {
    send: async (msg) => {
      if (closeController.signal.closed) {
        throw closeController.signal.reason instanceof Error
          ? closeController.signal.reason
          : new Error('transport is closed');
      }
      await transport.send(codec.encode(msg));
    },
    onMessage: (handler) => {
      if (closeController.signal.closed) return () => {};
      return transport.onMessage((wire) => {
        if (closeController.signal.closed) return;
        let message: Message;
        try {
          message = codec.decode(wire);
        } catch (error) {
          closeForProtocolError(error);
          return;
        }
        handler(message);
      });
    },
    close: (reason) => {
      if (closeController.signal.closed) return;
      closeController.close(reason);
      transport.close(reason);
    },
    closeSignal: closeController.signal,
  };
}

export function createInMemoryDuplex<M>(): [DuplexTransport<M>, DuplexTransport<M>] {
  const aHandlers = new Set<(msg: M) => void>();
  const bHandlers = new Set<(msg: M) => void>();
  const aCloseController = createTransportCloseController();
  const bCloseController = createTransportCloseController();

  const closePair = (reason?: unknown) => {
    aCloseController.close(reason);
    bCloseController.close(reason);
    aHandlers.clear();
    bHandlers.clear();
  };

  const a: DuplexTransport<M> = {
    async send(msg) {
      if (aCloseController.signal.closed) {
        throw aCloseController.signal.reason instanceof Error
          ? aCloseController.signal.reason
          : new Error('in-memory transport is closed');
      }
      queueMicrotask(() => {
        if (aCloseController.signal.closed || bCloseController.signal.closed) return;
        for (const h of bHandlers) h(msg);
      });
    },
    onMessage(handler) {
      if (aCloseController.signal.closed) return () => {};
      aHandlers.add(handler);
      return () => aHandlers.delete(handler);
    },
    close: closePair,
    closeSignal: aCloseController.signal,
  };

  const b: DuplexTransport<M> = {
    async send(msg) {
      if (bCloseController.signal.closed) {
        throw bCloseController.signal.reason instanceof Error
          ? bCloseController.signal.reason
          : new Error('in-memory transport is closed');
      }
      queueMicrotask(() => {
        if (aCloseController.signal.closed || bCloseController.signal.closed) return;
        for (const h of aHandlers) h(msg);
      });
    },
    onMessage(handler) {
      if (bCloseController.signal.closed) return () => {};
      bHandlers.add(handler);
      return () => bHandlers.delete(handler);
    },
    close: closePair,
    closeSignal: bCloseController.signal,
  };

  return [a, b];
}

export type TransportCloseHandler = (reason?: unknown) => void;

/** Read-only notification that a transport has permanently closed. */
export interface TransportCloseSignal {
  readonly closed: boolean;
  /**
   * Best-effort diagnostic reason. Consumers must treat every closed signal as
   * permanent and must not infer successful protocol completion from this value.
   */
  readonly reason: unknown;
  /**
   * Subscribe to closure. Subscribers added after closure are notified in a
   * microtask with the original reason.
   */
  subscribe(handler: TransportCloseHandler): () => void;
}

/** Adapter-owned writer for a {@link TransportCloseSignal}. */
export interface TransportCloseController {
  readonly signal: TransportCloseSignal;
  /** Close the signal once. The first reason wins. */
  close(reason?: unknown): void;
}

function callHandler(handler: TransportCloseHandler, reason?: unknown): void {
  try {
    handler(reason);
  } catch {
    // Close observers must not escape into an event loop or EventEmitter.
  }
}

/** Create a one-shot close controller for a custom transport adapter. */
export function createTransportCloseController(): TransportCloseController {
  const handlers = new Set<TransportCloseHandler>();
  let closed = false;
  let closeReason: unknown;

  const close = (reason?: unknown) => {
    if (closed) return;
    closed = true;
    closeReason = reason;
    for (const handler of handlers) callHandler(handler, reason);
    handlers.clear();
  };

  const subscribe = (handler: TransportCloseHandler) => {
    if (!closed) {
      handlers.add(handler);
      return () => handlers.delete(handler);
    }

    let active = true;
    queueMicrotask(() => {
      if (active) callHandler(handler, closeReason);
    });
    return () => {
      active = false;
    };
  };

  return {
    signal: {
      get closed() {
        return closed;
      },
      get reason() {
        return closeReason;
      },
      subscribe,
    },
    close,
  };
}

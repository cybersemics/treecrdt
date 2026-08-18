export type TerminalHandler = (error?: unknown) => void;

/**
 * A one-shot terminal notification for closeable transports.
 *
 * The first call to {@link notify} wins. Current subscribers are called
 * synchronously, while subscribers added after settlement are called in a
 * microtask. Observer errors are isolated from the transport event loop.
 */
export type TerminalSignal = {
  readonly settled: boolean;
  readonly error: unknown;
  notify(error?: unknown): void;
  subscribe(handler: TerminalHandler): () => void;
};

function callHandler(handler: TerminalHandler, error?: unknown): void {
  try {
    handler(error);
  } catch {
    // Terminal observers must not escape into an event loop or EventEmitter.
  }
}

/** Create a one-shot terminal signal for a custom transport adapter. */
export function createTerminalSignal(): TerminalSignal {
  const handlers = new Set<TerminalHandler>();
  let settled = false;
  let terminalError: unknown;

  const notify = (error?: unknown) => {
    if (settled) return;
    settled = true;
    terminalError = error;
    for (const handler of handlers) callHandler(handler, error);
    handlers.clear();
  };

  const subscribe = (handler: TerminalHandler) => {
    if (!settled) {
      handlers.add(handler);
      return () => handlers.delete(handler);
    }

    let active = true;
    queueMicrotask(() => {
      if (active) callHandler(handler, terminalError);
    });
    return () => {
      active = false;
    };
  };

  return {
    get settled() {
      return settled;
    },
    get error() {
      return terminalError;
    },
    notify,
    subscribe,
  };
}

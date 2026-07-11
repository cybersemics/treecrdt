type TerminalHandler = (error?: unknown) => void;

function callHandler(handler: TerminalHandler, error?: unknown): void {
  try {
    handler(error);
  } catch {
    // Terminal observers must not escape into an event loop or EventEmitter.
  }
}

/** @internal Small one-shot signal shared by built-in transports. */
export function createTerminalSignal() {
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

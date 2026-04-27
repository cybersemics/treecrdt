import type { DuplexTransport } from './index.js';

export type BrowserWebSocketMessageListener = (event: { data: unknown }) => void;

export type BrowserWebSocketLike = {
  readonly readyState: number;
  readonly bufferedAmount: number;
  send(data: Uint8Array): void;
  close(): void;
  addEventListener(type: 'message', listener: BrowserWebSocketMessageListener): void;
  removeEventListener(type: 'message', listener: BrowserWebSocketMessageListener): void;
};

export type BrowserWebSocketTransportOptions = {
  highWaterMark?: number;
  lowWaterMark?: number;
  pollMs?: number;
  drainTimeoutMs?: number;
};

const DEFAULT_HIGH_WATER_MARK = 256 * 1024;
const DEFAULT_LOW_WATER_MARK = 64 * 1024;
const DEFAULT_POLL_MS = 8;
const DEFAULT_DRAIN_TIMEOUT_MS = 5_000;

function browserWebSocketOpenState(ws: BrowserWebSocketLike): number {
  const ctor = (ws as { constructor?: { OPEN?: unknown } }).constructor;
  if (typeof ctor?.OPEN === 'number') return ctor.OPEN;
  return 1;
}

function coerceWebSocketDataToBytes(data: unknown): Promise<Uint8Array | null> | Uint8Array | null {
  if (data instanceof Uint8Array) return data;
  if (data instanceof ArrayBuffer) return new Uint8Array(data);
  if (ArrayBuffer.isView(data) && data.buffer instanceof ArrayBuffer) {
    return new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
  }
  if (typeof Blob !== 'undefined' && data instanceof Blob) {
    return data.arrayBuffer().then((buffer) => new Uint8Array(buffer));
  }
  return null;
}

function waitForWebSocketDrain(
  ws: BrowserWebSocketLike,
  opts: Required<BrowserWebSocketTransportOptions>,
  maxBufferedAmount: number,
): Promise<void> {
  const openState = browserWebSocketOpenState(ws);
  if (ws.readyState !== openState) {
    return Promise.reject(new Error('websocket is not open'));
  }
  if (ws.bufferedAmount <= maxBufferedAmount) {
    return Promise.resolve();
  }

  return new Promise<void>((resolve, reject) => {
    const deadline = Date.now() + opts.drainTimeoutMs;

    const poll = () => {
      if (ws.readyState !== openState) {
        reject(new Error('websocket closed while waiting for buffered data to drain'));
        return;
      }
      if (ws.bufferedAmount <= maxBufferedAmount || Date.now() >= deadline) {
        resolve();
        return;
      }
      setTimeout(poll, opts.pollMs);
    };

    setTimeout(poll, opts.pollMs);
  });
}

export function createBrowserWebSocketTransport(
  ws: BrowserWebSocketLike,
  opts: BrowserWebSocketTransportOptions = {},
): DuplexTransport<Uint8Array> & { close: () => void } {
  const resolvedOpts: Required<BrowserWebSocketTransportOptions> = {
    highWaterMark: opts.highWaterMark ?? DEFAULT_HIGH_WATER_MARK,
    lowWaterMark: opts.lowWaterMark ?? DEFAULT_LOW_WATER_MARK,
    pollMs: opts.pollMs ?? DEFAULT_POLL_MS,
    drainTimeoutMs: opts.drainTimeoutMs ?? DEFAULT_DRAIN_TIMEOUT_MS,
  };

  let sendQueue: Promise<void> = Promise.resolve();

  return {
    send: (bytes) => {
      const run = async () => {
        const openState = browserWebSocketOpenState(ws);
        if (ws.readyState !== openState) {
          throw new Error('websocket is not open');
        }
        if (ws.bufferedAmount > resolvedOpts.highWaterMark) {
          await waitForWebSocketDrain(ws, resolvedOpts, resolvedOpts.lowWaterMark);
        }
        try {
          ws.send(bytes);
        } catch (err) {
          throw err instanceof Error ? err : new Error(String(err));
        }
        await waitForWebSocketDrain(ws, resolvedOpts, resolvedOpts.lowWaterMark);
      };

      const next = sendQueue.then(run, run);
      sendQueue = next.catch(() => {});
      return next;
    },
    onMessage: (handler) => {
      let active = true;
      const onMessage = (event: { data: unknown }) => {
        void Promise.resolve(coerceWebSocketDataToBytes(event.data)).then((bytes) => {
          if (!active || !bytes) return;
          handler(bytes);
        });
      };
      ws.addEventListener('message', onMessage);
      return () => {
        active = false;
        ws.removeEventListener('message', onMessage);
      };
    },
    close: () => {
      try {
        ws.close();
      } catch {
        // ignore
      }
    },
  };
}

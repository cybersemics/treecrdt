import { afterEach, expect, test, vi } from 'vitest';

import { createBrowserWebSocketTransport } from '../dist/browser.js';
import { wrapDuplexTransportWithCodec } from '../dist/transport/index.js';
import type { DuplexTransport } from '../dist/transport/index.js';

type Listener = (event: any) => void;

class MockBrowserWebSocket {
  static readonly OPEN = 1;
  static readonly CLOSED = 3;

  readyState = MockBrowserWebSocket.OPEN;
  bufferedAmount = 0;
  readonly sent: Uint8Array[] = [];
  private readonly listeners = new Map<string, Set<Listener>>();

  send(data: Uint8Array): void {
    this.sent.push(data);
    this.bufferedAmount += data.byteLength;
  }

  close(): void {
    this.readyState = MockBrowserWebSocket.CLOSED;
    this.emit('close', { code: 1000 });
  }

  addEventListener(type: string, listener: Listener): void {
    const listeners = this.listeners.get(type) ?? new Set<Listener>();
    listeners.add(listener);
    this.listeners.set(type, listeners);
  }

  removeEventListener(type: string, listener: Listener): void {
    this.listeners.get(type)?.delete(listener);
  }

  emitMessage(data: unknown): void {
    this.emit('message', { data });
  }

  emitClose(code = 1006, reason = ''): void {
    this.readyState = MockBrowserWebSocket.CLOSED;
    this.emit('close', { code, reason });
  }

  emitError(): void {
    this.emit('error', {});
  }

  private emit(type: string, event: unknown): void {
    for (const listener of this.listeners.get(type) ?? []) listener(event);
  }
}

afterEach(() => {
  vi.useRealTimers();
});

test('browser websocket transport coerces inbound message payloads to bytes', async () => {
  const ws = new MockBrowserWebSocket();
  const transport = createBrowserWebSocketTransport(ws);
  const received: number[][] = [];

  const detach = transport.onMessage((msg) => {
    received.push(Array.from(msg));
  });

  ws.emitMessage(new Uint8Array([1, 2, 3]).buffer);
  await Promise.resolve();

  detach();

  expect(received).toEqual([[1, 2, 3]]);
});

test('browser websocket transport serializes sends until buffered data drains', async () => {
  vi.useFakeTimers();

  const ws = new MockBrowserWebSocket();
  const transport = createBrowserWebSocketTransport(ws, {
    highWaterMark: 0,
    lowWaterMark: 0,
    pollMs: 5,
    drainTimeoutMs: 100,
  });

  const first = transport.send(new Uint8Array([1]));
  await Promise.resolve();
  expect(ws.sent.map((bytes) => Array.from(bytes))).toEqual([[1]]);

  const second = transport.send(new Uint8Array([2]));
  await Promise.resolve();
  expect(ws.sent.map((bytes) => Array.from(bytes))).toEqual([[1]]);

  await vi.advanceTimersByTimeAsync(20);
  expect(ws.sent.map((bytes) => Array.from(bytes))).toEqual([[1]]);

  ws.bufferedAmount = 0;
  await vi.advanceTimersByTimeAsync(5);
  await first;
  await Promise.resolve();

  expect(ws.sent.map((bytes) => Array.from(bytes))).toEqual([[1], [2]]);

  ws.bufferedAmount = 0;
  await vi.advanceTimersByTimeAsync(5);
  await second;
});

test('browser websocket transport reports close and error as terminal', async () => {
  const closedSocket = new MockBrowserWebSocket();
  const closedTransport = createBrowserWebSocketTransport(closedSocket);
  const closeError = new Promise<unknown>((resolve) =>
    closedTransport.onTerminal?.((error) => resolve(error)),
  );

  closedSocket.emitClose(1006, 'connection lost');
  await expect(closeError).resolves.toEqual(
    expect.objectContaining({ message: 'websocket closed (1006): connection lost' }),
  );

  const erroredSocket = new MockBrowserWebSocket();
  const erroredTransport = createBrowserWebSocketTransport(erroredSocket);
  const socketError = new Promise<unknown>((resolve) =>
    erroredTransport.onTerminal?.((error) => resolve(error)),
  );

  erroredSocket.emitError();
  await expect(socketError).resolves.toEqual(
    expect.objectContaining({ message: 'websocket error' }),
  );
});

test('a malformed codec frame closes the browser websocket without escaping', async () => {
  const ws = new MockBrowserWebSocket();
  const wire = createBrowserWebSocketTransport(ws);
  const transport = wrapDuplexTransportWithCodec(wire, {
    encode: (value: string) => new TextEncoder().encode(value),
    decode: () => {
      throw new Error('invalid protobuf frame');
    },
  });
  const failure = new Promise<unknown>((resolve) =>
    transport.onTerminal?.((error) => resolve(error)),
  );
  transport.onMessage(() => {
    throw new Error('malformed frame must not be delivered');
  });

  ws.emitMessage(new Uint8Array([0xff]).buffer);

  await expect(failure).resolves.toEqual(
    expect.objectContaining({ message: 'invalid protobuf frame' }),
  );
  expect(ws.readyState).toBe(MockBrowserWebSocket.CLOSED);
});

test('codec wrapper removes its upstream terminal listener when detached', () => {
  const terminalHandlers = new Set<(error?: unknown) => void>();
  const wire: DuplexTransport<Uint8Array> = {
    send: async () => {},
    onMessage: () => () => {},
    onTerminal: (handler) => {
      terminalHandlers.add(handler);
      return () => terminalHandlers.delete(handler);
    },
  };
  const transport = wrapDuplexTransportWithCodec(wire, {
    encode: (value: string) => new TextEncoder().encode(value),
    decode: (bytes) => new TextDecoder().decode(bytes),
  });

  const unsubscribe = transport.onTerminal?.(() => {});
  expect(terminalHandlers.size).toBe(1);
  unsubscribe?.();
  expect(terminalHandlers.size).toBe(0);
});

import { afterEach, expect, test, vi } from 'vitest';

import { createBrowserWebSocketTransport } from '../dist/browser.js';

type MessageListener = (event: { data: unknown }) => void;

class MockBrowserWebSocket {
  static readonly OPEN = 1;
  static readonly CLOSED = 3;

  readyState = MockBrowserWebSocket.OPEN;
  bufferedAmount = 0;
  readonly sent: Uint8Array[] = [];
  private readonly listeners = new Set<MessageListener>();

  send(data: Uint8Array): void {
    this.sent.push(data);
    this.bufferedAmount += data.byteLength;
  }

  close(): void {
    this.readyState = MockBrowserWebSocket.CLOSED;
  }

  addEventListener(type: 'message', listener: MessageListener): void {
    if (type !== 'message') return;
    this.listeners.add(listener);
  }

  removeEventListener(type: 'message', listener: MessageListener): void {
    if (type !== 'message') return;
    this.listeners.delete(listener);
  }

  emitMessage(data: unknown): void {
    for (const listener of this.listeners) listener({ data });
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

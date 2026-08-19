import { expect, test } from 'vitest';

import {
  createInMemoryDuplex,
  createTransportCloseController,
  wrapDuplexTransportWithCodec,
} from '../dist/transport/index.js';
import type { DuplexTransport } from '../dist/transport/index.js';

test('close controller delivers the first reason once and isolates observers', async () => {
  const controller = createTransportCloseController();
  const firstReason = new Error('first close reason');
  const secondReason = new Error('second close reason');
  const current: unknown[] = [];
  let throwingObserverCalls = 0;

  expect('close' in controller.signal).toBe(false);

  controller.signal.subscribe(() => {
    throwingObserverCalls += 1;
    throw new Error('observer failure');
  });
  controller.signal.subscribe((reason) => current.push(reason));

  expect(() => controller.close(firstReason)).not.toThrow();
  controller.close(secondReason);

  expect(controller.signal.closed).toBe(true);
  expect(controller.signal.reason).toBe(firstReason);
  expect(throwingObserverCalls).toBe(1);
  expect(current).toEqual([firstReason]);

  const late: unknown[] = [];
  controller.signal.subscribe((reason) => late.push(reason));
  const unsubscribeCanceled = controller.signal.subscribe(() => late.push('canceled'));
  unsubscribeCanceled();

  expect(late).toEqual([]);
  await Promise.resolve();
  expect(late).toEqual([firstReason]);
});

test('codec wrapper preserves a decode error across synchronous upstream close', () => {
  const decodeError = new Error('invalid frame');
  const upstreamError = new Error('socket closed');
  const upstreamClose = createTransportCloseController();
  let messageHandler: ((wire: Uint8Array) => void) | undefined;
  let closedWith: unknown;
  let decodeCalls = 0;
  const delivered: string[] = [];

  const wire: DuplexTransport<Uint8Array> = {
    send: async () => {},
    onMessage: (handler) => {
      messageHandler = handler;
      return () => {
        if (messageHandler === handler) messageHandler = undefined;
      };
    },
    close: (reason) => {
      closedWith = reason;
      upstreamClose.close(upstreamError);
      messageHandler?.(new Uint8Array([0x01]));
    },
    closeSignal: upstreamClose.signal,
  };
  const transport = wrapDuplexTransportWithCodec(wire, {
    encode: (value: string) => new TextEncoder().encode(value),
    decode: () => {
      decodeCalls += 1;
      if (decodeCalls === 1) throw decodeError;
      return 'reentrant message';
    },
  });
  const closeReasons: unknown[] = [];

  transport.closeSignal.subscribe((reason) => closeReasons.push(reason));
  transport.onMessage((message) => delivered.push(message));
  messageHandler?.(new Uint8Array([0xff]));

  expect(closedWith).toBe(decodeError);
  expect(transport.closeSignal.reason).toBe(decodeError);
  expect(closeReasons).toEqual([decodeError]);
  expect(decodeCalls).toBe(1);
  expect(delivered).toEqual([]);
});

test('codec wrapper synchronously mirrors an already closed upstream transport', async () => {
  const upstreamClose = createTransportCloseController();
  const reason = new Error('connection already closed');
  upstreamClose.close(reason);
  const wire: DuplexTransport<Uint8Array> = {
    send: async () => {},
    onMessage: () => () => {},
    close: (closeReason) => upstreamClose.close(closeReason),
    closeSignal: upstreamClose.signal,
  };

  const transport = wrapDuplexTransportWithCodec(wire, {
    encode: (value: string) => new TextEncoder().encode(value),
    decode: (bytes) => new TextDecoder().decode(bytes),
  });

  expect(transport.closeSignal.closed).toBe(true);
  expect(transport.closeSignal.reason).toBe(reason);
  await expect(transport.send('ignored')).rejects.toBe(reason);
});

test('closing either in-memory endpoint closes the pair and drops queued delivery', async () => {
  const [a, b] = createInMemoryDuplex<string>();
  const reason = new Error('connection closed');
  const received: string[] = [];
  b.onMessage((message) => received.push(message));

  const queuedSend = a.send('queued');
  b.close(reason);
  await queuedSend;
  await Promise.resolve();

  expect(received).toEqual([]);
  expect(a.closeSignal.closed).toBe(true);
  expect(b.closeSignal.closed).toBe(true);
  expect(a.closeSignal.reason).toBe(reason);
  expect(b.closeSignal.reason).toBe(reason);
  await expect(a.send('after close')).rejects.toBe(reason);
  await expect(b.send('after close')).rejects.toBe(reason);

  b.close(new Error('later reason'));
  expect(a.closeSignal.reason).toBe(reason);
});

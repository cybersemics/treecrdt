import { expect, test } from 'vitest';

import { createTerminalSignal, wrapDuplexTransportWithCodec } from '../dist/transport/index.js';
import type { DuplexTransport } from '../dist/transport/index.js';

test('terminal signal delivers the first cause once and isolates observers', async () => {
  const signal = createTerminalSignal();
  const firstCause = new Error('first terminal cause');
  const secondCause = new Error('second terminal cause');
  const current: unknown[] = [];
  let throwingObserverCalls = 0;

  signal.subscribe(() => {
    throwingObserverCalls += 1;
    throw new Error('observer failure');
  });
  signal.subscribe((error) => current.push(error));

  expect(() => signal.notify(firstCause)).not.toThrow();
  signal.notify(secondCause);

  expect(signal.settled).toBe(true);
  expect(signal.error).toBe(firstCause);
  expect(throwingObserverCalls).toBe(1);
  expect(current).toEqual([firstCause]);

  const late: unknown[] = [];
  signal.subscribe((error) => late.push(error));
  const unsubscribeCanceled = signal.subscribe(() => late.push('canceled'));
  unsubscribeCanceled();

  expect(late).toEqual([]);
  await Promise.resolve();
  expect(late).toEqual([firstCause]);
});

test('codec wrapper preserves a decode error across synchronous upstream close', () => {
  const decodeError = new Error('invalid frame');
  const upstreamError = new Error('socket closed');
  let messageHandler: ((wire: Uint8Array) => void) | undefined;
  let terminalHandler: ((error?: unknown) => void) | undefined;
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
    close: (error) => {
      closedWith = error;
      terminalHandler?.(upstreamError);
      messageHandler?.(new Uint8Array([0x01]));
    },
    onTerminal: (handler) => {
      terminalHandler = handler;
      return () => {
        if (terminalHandler === handler) terminalHandler = undefined;
      };
    },
  };
  const transport = wrapDuplexTransportWithCodec(wire, {
    encode: (value: string) => new TextEncoder().encode(value),
    decode: () => {
      decodeCalls += 1;
      if (decodeCalls === 1) throw decodeError;
      return 'reentrant message';
    },
  });
  const terminalCauses: unknown[] = [];

  transport.onTerminal?.((error) => terminalCauses.push(error));
  transport.onMessage((message) => delivered.push(message));
  messageHandler?.(new Uint8Array([0xff]));

  expect(closedWith).toBe(decodeError);
  expect(terminalCauses).toEqual([decodeError]);
  expect(decodeCalls).toBe(1);
  expect(delivered).toEqual([]);
});

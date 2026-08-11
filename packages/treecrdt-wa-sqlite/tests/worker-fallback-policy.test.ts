import { afterEach, beforeEach, expect, test, vi } from 'vitest';

import type { RpcRequest, RpcStorageFallback } from '../src/rpc.js';

const openTreecrdtDb = vi.hoisted(() => vi.fn());

vi.mock('../src/open.js', () => ({ openTreecrdtDb }));

function initRequest(fallback: RpcStorageFallback): RpcRequest<'init'> {
  return {
    id: 1,
    method: 'init',
    params: ['/', '/policy.db', 'opfs', 'fallback-policy', fallback],
  };
}

beforeEach(() => {
  vi.resetModules();
  openTreecrdtDb.mockReset().mockRejectedValue(new Error('stop after inspecting options'));
});

afterEach(() => {
  vi.unstubAllGlobals();
});

test.each([
  ['throw', true],
  ['memory', false],
] as const)(
  'dedicated worker maps %s fallback to requireOpfs=%s',
  async (fallback, requireOpfs) => {
    const scope = {
      onmessage: null as ((event: MessageEvent<RpcRequest>) => Promise<void>) | null,
      postMessage: vi.fn(),
    };
    vi.stubGlobal('self', scope);
    await import('../src/worker.js');

    await scope.onmessage?.({ data: initRequest(fallback) } as MessageEvent<RpcRequest>);

    expect(openTreecrdtDb).toHaveBeenCalledWith(expect.objectContaining({ requireOpfs }));
  },
);

test.each([
  ['throw', true],
  ['memory', false],
] as const)('shared worker maps %s fallback to requireOpfs=%s', async (fallback, requireOpfs) => {
  const port = {
    onmessage: null as ((event: MessageEvent<RpcRequest>) => void) | null,
    postMessage: vi.fn(),
    start: vi.fn(),
  };
  const scope = {
    onconnect: null as ((event: MessageEvent) => void) | null,
  };
  vi.stubGlobal('self', scope);
  await import('../src/shared-worker.js');

  scope.onconnect?.({ ports: [port] } as unknown as MessageEvent);
  port.onmessage?.({ data: initRequest(fallback) } as MessageEvent<RpcRequest>);

  await vi.waitFor(() => expect(openTreecrdtDb).toHaveBeenCalledOnce());
  expect(openTreecrdtDb).toHaveBeenCalledWith(expect.objectContaining({ requireOpfs }));
});

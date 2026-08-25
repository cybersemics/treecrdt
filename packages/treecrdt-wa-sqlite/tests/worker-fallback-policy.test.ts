import * as Comlink from 'comlink';
import { afterEach, beforeEach, expect, test, vi } from 'vitest';

import { TreecrdtBackend } from '../src/backend.js';

const openTreecrdtDb = vi.hoisted(() => vi.fn());

vi.mock('../src/open.js', () => ({ openTreecrdtDb }));

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
] as const)('backend maps %s fallback to requireOpfs=%s', async (fallback, requireOpfs) => {
  const backend = new TreecrdtBackend(openTreecrdtDb);

  await expect(
    backend.init({
      baseUrl: '/',
      filename: '/policy.db',
      storage: 'opfs',
      docId: 'fallback-policy',
      fallback,
    }),
  ).rejects.toThrow('stop after inspecting options');

  expect(openTreecrdtDb).toHaveBeenCalledWith(expect.objectContaining({ requireOpfs }));
});

test.each([
  ['throw', true],
  ['memory', false],
] as const)(
  'shared worker maps %s fallback to requireOpfs=%s with any-context VFS',
  async (fallback, requireOpfs) => {
    const channel = new MessageChannel();
    const scope = {
      onconnect: null as ((event: MessageEvent) => void) | null,
    };
    vi.stubGlobal('self', scope);
    await import('../src/shared-worker.js');

    scope.onconnect?.({ ports: [channel.port1] } as unknown as MessageEvent);
    channel.port2.start();
    const remote = Comlink.wrap<{
      init(config: {
        baseUrl: string;
        filename: string;
        storage: 'opfs';
        docId: string;
        fallback: 'memory' | 'throw';
      }): Promise<unknown>;
    }>(channel.port2);

    await expect(
      remote.init({
        baseUrl: '/',
        filename: '/policy.db',
        storage: 'opfs',
        docId: 'fallback-policy',
        fallback,
      }),
    ).rejects.toThrow('stop after inspecting options');

    expect(openTreecrdtDb).toHaveBeenCalledWith(
      expect.objectContaining({ requireOpfs, opfsVfs: 'any-context' }),
    );

    remote[Comlink.releaseProxy]();
    channel.port1.close();
    channel.port2.close();
  },
);

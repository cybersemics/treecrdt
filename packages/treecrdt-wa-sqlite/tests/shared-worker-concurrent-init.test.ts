import * as Comlink from 'comlink';
import { afterEach, expect, test, vi } from 'vitest';

import type { TreecrdtConnection } from '../src/connection.js';
import type { BackendInitConfig, BackendInitResult } from '../src/session.js';
import type { Database } from '../src/types.js';

const openTreecrdtDb = vi.hoisted(() => vi.fn());

vi.mock('../src/open.js', () => ({ openTreecrdtDb }));

type Deferred<T> = {
  promise: Promise<T>;
  resolve: (value: T) => void;
};

function deferred<T>(): Deferred<T> {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((next) => {
    resolve = next;
  });
  return { promise, resolve };
}

function opened(marker: string): BackendInitResult & { db: Database } {
  return {
    db: {
      exec: vi.fn(async () => undefined),
      getText: vi.fn(async () => marker),
      close: vi.fn(async () => undefined),
    } as unknown as Database,
    storage: 'memory',
    filename: ':memory:',
  };
}

function attach(scope: { onconnect: ((event: MessageEvent) => void) | null }) {
  const { port1, port2 } = new MessageChannel();
  scope.onconnect?.({ ports: [port1] } as unknown as MessageEvent);
  port2.start();
  return {
    port1,
    port2,
    remote: Comlink.wrap<TreecrdtConnection>(port2),
  };
}

const config = (docId: string): BackendInitConfig => ({
  baseUrl: '/',
  storage: 'memory',
  docId,
  fallback: 'memory',
});

afterEach(() => {
  vi.unstubAllGlobals();
  vi.resetModules();
  openTreecrdtDb.mockReset();
});

test('concurrent shared-worker init rejects a different database config', async () => {
  const firstOpenStarted = deferred<void>();
  const releaseFirstOpen = deferred<void>();
  openTreecrdtDb
    .mockImplementationOnce(async () => {
      firstOpenStarted.resolve();
      await releaseFirstOpen.promise;
      return opened('A');
    })
    .mockImplementationOnce(async () => opened('B'));

  const scope = { onconnect: null as ((event: MessageEvent) => void) | null };
  vi.stubGlobal('self', scope);
  await import('../src/shared-worker.js');

  const a = attach(scope);
  const b = attach(scope);
  try {
    const initA = a.remote.init(config('doc-a'));
    await firstOpenStarted.promise;

    const initBOutcome = b.remote.init(config('doc-b')).then(
      () => 'resolved',
      (error) => (error instanceof Error ? error.message : String(error)),
    );
    // Messages on one port are ordered. A completed call after init means initB
    // has reached its first await and already queued owner.open(configB).
    await b.remote.notifyMaterialized({ headSeq: 0, changes: [] });

    releaseFirstOpen.resolve();
    await initA;

    expect(await initBOutcome).toMatch(/different TreeCRDT database/);
    expect(openTreecrdtDb).toHaveBeenCalledTimes(1);
  } finally {
    releaseFirstOpen.resolve();
    a.remote[Comlink.releaseProxy]();
    b.remote[Comlink.releaseProxy]();
    a.port1.close();
    a.port2.close();
    b.port1.close();
    b.port2.close();
  }
});

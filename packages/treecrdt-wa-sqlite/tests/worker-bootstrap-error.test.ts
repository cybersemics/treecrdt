import { afterEach, expect, test, vi } from 'vitest';

import { createTreecrdtClient } from '../src/client.browser.js';
import type { BackendInitResult } from '../src/session.js';
import { createMockConnection, installDedicatedWorker } from './mock-worker.js';

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((next) => {
    resolve = next;
  });
  return { promise, resolve };
}

async function settlesWithin<T>(promise: Promise<T>, timeoutMs: number): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<never>((_resolve, reject) => {
    timer = setTimeout(
      () => reject(new Error(`timed out waiting ${timeoutMs}ms for promise to settle`)),
      timeoutMs,
    );
  });
  try {
    return await Promise.race([promise, timeout]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}

afterEach(() => {
  vi.unstubAllGlobals();
});

test('dedicated-worker error during init rejects client creation and terminates the endpoint', async () => {
  const initStarted = deferred<void>();
  const connection = createMockConnection();
  connection.init = async (): Promise<BackendInitResult> => {
    initStarted.resolve();
    return await new Promise<BackendInitResult>(() => {});
  };
  const endpoint = installDedicatedWorker(connection);

  const opening = createTreecrdtClient({
    storage: { type: 'memory' },
    runtime: { type: 'dedicated-worker' },
    docId: 'worker-bootstrap-error',
  });

  try {
    await initStarted.promise;
    endpoint.emitError('worker bootstrap failed');

    await expect(settlesWithin(opening, 100)).rejects.toThrow(/worker bootstrap failed/);
    expect(endpoint.terminated).toBe(true);
  } finally {
    endpoint.closeEndpoint();
  }
});

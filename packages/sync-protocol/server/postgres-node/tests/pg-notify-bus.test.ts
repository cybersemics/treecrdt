import { beforeEach, expect, test, vi } from 'vitest';
import type { WebSocketSyncServerOptions } from '@treecrdt/sync-server-core';
import { silentSyncServerLogger } from '@treecrdt/sync-server-core';

import { PostgresDocUpdateBus, startSyncServer } from '../src/server.ts';

vi.mock('@treecrdt/sync-server-core', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@treecrdt/sync-server-core')>();
  return {
    ...actual,
    startWebSocketSyncServer: <Op>(opts: WebSocketSyncServerOptions<Op>) =>
      actual.startWebSocketSyncServer({ ...opts, port: 0 }),
  };
});

vi.mock('@treecrdt/sync-postgres', () => {
  const createStore = () => ({ init: async () => {}, close: async () => {} });
  return { createOpAuthStore: createStore, createCapabilityMaterialStore: createStore };
});

const pgMockState = {
  clientActiveQueries: 0,
  clientQueries: [] as string[],
  poolQueries: [] as string[],
  clientListeners: new Map<string, (error?: unknown) => void>(),
};

const serverOptions = {
  host: '127.0.0.1',
  postgresUrl: 'postgres://example.invalid/treecrdt',
  backendModule: new URL('./fixtures/backend.mjs', import.meta.url).href,
};

vi.mock('pg', () => {
  class MockPool {
    async query(sql: string): Promise<{ rowCount: number }> {
      pgMockState.poolQueries.push(sql);
      await new Promise((resolve) => setTimeout(resolve, 5));
      return { rowCount: sql.includes('treecrdt_meta') ? 1 : 0 };
    }

    async end(): Promise<void> {}
  }

  class MockClient {
    on(event: string, handler: (error?: unknown) => void) {
      pgMockState.clientListeners.set(event, handler);
      return this;
    }

    async connect(): Promise<void> {}

    async query(sql: string): Promise<{ rowCount: number }> {
      pgMockState.clientQueries.push(sql);
      pgMockState.clientActiveQueries += 1;
      if (pgMockState.clientActiveQueries > 1) {
        pgMockState.clientActiveQueries -= 1;
        throw new Error('single pg client query overlap');
      }
      await new Promise((resolve) => setTimeout(resolve, 5));
      pgMockState.clientActiveQueries -= 1;
      return { rowCount: 0 };
    }

    async end(): Promise<void> {}
  }

  return { Pool: MockPool, Client: MockClient };
});

beforeEach(() => {
  pgMockState.clientActiveQueries = 0;
  pgMockState.clientQueries.length = 0;
  pgMockState.poolQueries.length = 0;
  pgMockState.clientListeners.clear();
});

test('doc update bus does not overlap application queries on a single pg client', async () => {
  const bus = await PostgresDocUpdateBus.create({
    postgresUrl: 'postgres://example.invalid/treecrdt',
    channel: 'treecrdt_sync_doc_updates',
    onDocUpdate: () => {},
  });

  await expect(
    Promise.all([bus.hasDoc('doc-a'), bus.ping(), bus.publishDocUpdate('doc-a')]),
  ).resolves.toEqual([true, undefined, undefined]);

  expect(pgMockState.poolQueries).toHaveLength(3);
  expect(
    pgMockState.clientQueries.filter(
      (sql) => !sql.startsWith('LISTEN ') && !sql.startsWith('UNLISTEN '),
    ),
  ).toEqual([]);

  await bus.close();
});

test('listen client error calls the fatal hook once and ignores errors after close', async () => {
  const fatal = vi.fn();
  const bus = await PostgresDocUpdateBus.create({
    postgresUrl: 'postgres://example.invalid/treecrdt',
    channel: 'treecrdt_sync_doc_updates',
    onDocUpdate: () => {},
    onFatal: fatal,
  });

  const onError = pgMockState.clientListeners.get('error');
  expect(onError).toBeTypeOf('function');
  onError?.(new Error('connection terminated'));
  onError?.(new Error('connection terminated'));
  expect(fatal).toHaveBeenCalledTimes(1);

  await bus.close();
  onError?.(new Error('after close'));
  expect(fatal).toHaveBeenCalledTimes(1);
});

test('startSyncServer closes the server by default when the LISTEN connection fails', async () => {
  const info = vi.fn();
  const server = await startSyncServer({
    ...serverOptions,
    log: { ...silentSyncServerLogger, info },
  });
  const healthUrl = `http://${server.host}:${server.port}/health`;

  try {
    expect((await fetch(healthUrl)).status).toBe(200);
    pgMockState.clientListeners.get('error')!(new Error('connection terminated'));

    expect(info).toHaveBeenCalledWith('closing', { host: server.host, port: server.port });
    await server.close();
    await expect(fetch(healthUrl)).rejects.toThrow();
    expect(info.mock.calls.filter(([message]) => message === 'closing')).toHaveLength(1);
  } finally {
    await server.close();
  }
});

test('readiness fails when a custom LISTEN failure callback takes over shutdown', async () => {
  const onPgNotifyFatal = vi.fn();
  const server = await startSyncServer({ ...serverOptions, onPgNotifyFatal });
  const baseUrl = `http://${server.host}:${server.port}`;

  try {
    const status = await fetch(`${baseUrl}/status`);
    expect(await status.json()).toMatchObject({ ready: true });

    const error = new Error('connection terminated');
    pgMockState.clientListeners.get('error')!(error);

    expect(onPgNotifyFatal.mock.calls).toEqual([[error]]);
    expect((await fetch(`${baseUrl}/health`)).status).toBe(503);
    const failedStatus = await fetch(`${baseUrl}/status`);
    expect(await failedStatus.json()).toMatchObject({
      ok: false,
      ready: false,
      readyDetail: 'postgres notification listener unavailable',
    });
  } finally {
    await server.close();
  }
});

test('a LISTEN failure during startup rejects instead of returning a server handle', async () => {
  const error = new Error('connection terminated');
  const info = vi.fn((message: string) => {
    if (message === 'listening') pgMockState.clientListeners.get('error')!(error);
  });
  const startup = startSyncServer({ ...serverOptions, log: { ...silentSyncServerLogger, info } });
  await expect(startup.then((server) => server.close())).rejects.toBe(error);
  expect(info).toHaveBeenCalledWith('closing', expect.anything());
});

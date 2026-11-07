import { beforeEach, expect, test, vi } from 'vitest';

const pgMockState = {
  clientActiveQueries: 0,
  clientQueries: [] as string[],
  poolQueries: [] as string[],
  clientListeners: new Map<string, (error?: unknown) => void>(),
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
  const { PostgresDocUpdateBus } = await import('../src/server.ts');

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
  const { PostgresDocUpdateBus } = await import('../src/server.ts');
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

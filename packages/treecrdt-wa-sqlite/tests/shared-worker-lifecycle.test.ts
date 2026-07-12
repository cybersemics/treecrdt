import { afterEach, beforeEach, expect, test, vi } from 'vitest';

import { SHARED_WORKER_DROPPED_ERROR, type RpcRequest } from '../src/rpc.js';

const mocks = vi.hoisted(() => ({
  clearOpfsStorage: vi.fn(),
  openTreecrdtDb: vi.fn(),
}));

vi.mock('../src/opfs.js', () => ({ clearOpfsStorage: mocks.clearOpfsStorage }));
vi.mock('../src/open.js', () => ({ openTreecrdtDb: mocks.openTreecrdtDb }));

class FakePort {
  onmessage: ((event: MessageEvent) => void) | null = null;
  onmessageerror: ((event: MessageEvent) => void) | null = null;
  readonly outbound: any[] = [];
  closeCalls = 0;
  failPosts = false;

  postMessage(message: unknown): void {
    if (this.failPosts) throw new Error('stale port');
    this.outbound.push(message);
  }

  start(): void {}

  close(): void {
    this.closeCalls += 1;
  }

  send(request: RpcRequest): void {
    this.onmessage?.({ data: request } as MessageEvent);
  }

  messageError(): void {
    this.onmessageerror?.({} as MessageEvent);
  }
}

type FakeScope = {
  onconnect: ((event: MessageEvent) => void) | null;
};

let scope: FakeScope;

function opened(close = vi.fn(async () => undefined)) {
  const api = {
    headLamport: vi.fn(async () => 7),
  };
  return {
    api,
    close,
    result: { db: { close }, api, storage: 'memory' as const, filename: ':memory:' },
  };
}

function connect(): FakePort {
  const port = new FakePort();
  scope.onconnect?.({ ports: [port] } as unknown as MessageEvent);
  return port;
}

function request(port: FakePort, id: number, method: RpcRequest['method']): void {
  const params =
    method === 'init' ? (['/', undefined, 'memory', 'lifecycle-test'] as const) : ([] as const);
  port.send({ id, method, params } as RpcRequest);
}

async function response(port: FakePort, id: number): Promise<any> {
  await vi.waitFor(() => {
    expect(port.outbound.some((message) => message.id === id)).toBe(true);
  });
  return port.outbound.find((message) => message.id === id);
}

beforeEach(async () => {
  vi.resetModules();
  mocks.clearOpfsStorage.mockReset();
  mocks.openTreecrdtDb.mockReset();
  scope = { onconnect: null };
  vi.stubGlobal('self', scope);
  await import('../src/shared-worker.js');
});

afterEach(() => {
  vi.unstubAllGlobals();
});

test('drop terminates every peer and permits a fresh shared session', async () => {
  const first = opened();
  const second = opened();
  mocks.openTreecrdtDb.mockResolvedValueOnce(first.result).mockResolvedValueOnce(second.result);
  const source = connect();
  const peer = connect();

  request(source, 1, 'init');
  request(peer, 2, 'init');
  expect((await response(source, 1)).ok).toBe(true);
  expect((await response(peer, 2)).ok).toBe(true);

  request(source, 3, 'drop');
  expect(await response(source, 3)).toMatchObject({ ok: true });
  await vi.waitFor(() => {
    expect(peer.outbound).toContainEqual({
      type: 'terminal',
      error: SHARED_WORKER_DROPPED_ERROR,
    });
  });
  expect(first.close).toHaveBeenCalledOnce();
  expect(peer.onmessage).toBeNull();

  const replacement = connect();
  request(replacement, 4, 'init');
  expect((await response(replacement, 4)).ok).toBe(true);
  expect(mocks.openTreecrdtDb).toHaveBeenCalledTimes(2);
});

test('a failed drop rejects its source but still terminally invalidates peers', async () => {
  const closeError = new Error('close failed');
  const first = opened(
    vi.fn(async () => {
      throw closeError;
    }),
  );
  const second = opened();
  mocks.openTreecrdtDb.mockResolvedValueOnce(first.result).mockResolvedValueOnce(second.result);
  const source = connect();
  const peer = connect();
  request(source, 1, 'init');
  request(peer, 2, 'init');
  await response(source, 1);
  await response(peer, 2);

  request(source, 3, 'drop');
  expect(await response(source, 3)).toMatchObject({ ok: false, error: 'close failed' });
  expect(peer.outbound).toContainEqual({
    type: 'terminal',
    error: SHARED_WORKER_DROPPED_ERROR,
  });

  const replacement = connect();
  request(replacement, 4, 'init');
  expect((await response(replacement, 4)).ok).toBe(true);
});

test('message errors and failed broadcasts prune stale ports before final close', async () => {
  const first = opened();
  let emitMaterialized: ((event: any) => void) | undefined;
  mocks.openTreecrdtDb.mockImplementationOnce(async (options) => {
    emitMaterialized = options.onMaterialized;
    return first.result;
  });
  const live = connect();
  const messageErrorPort = connect();
  const failedPostPort = connect();
  request(live, 1, 'init');
  request(messageErrorPort, 2, 'init');
  request(failedPostPort, 3, 'init');
  await response(live, 1);
  await response(messageErrorPort, 2);
  await response(failedPostPort, 3);

  messageErrorPort.messageError();
  failedPostPort.failPosts = true;
  emitMaterialized?.({ headSeq: 1, changes: [{ node: 'n' }] });
  expect(messageErrorPort.closeCalls).toBe(1);
  expect(failedPostPort.closeCalls).toBe(1);
  expect(failedPostPort.onmessage).toBeNull();

  request(live, 4, 'close');
  expect((await response(live, 4)).ok).toBe(true);
  expect(first.close).toHaveBeenCalledOnce();
});

test('closing one client preserves the database until the final client closes', async () => {
  const first = opened();
  mocks.openTreecrdtDb.mockResolvedValue(first.result);
  const firstPort = connect();
  const finalPort = connect();
  request(firstPort, 1, 'init');
  request(finalPort, 2, 'init');
  await response(firstPort, 1);
  await response(finalPort, 2);

  request(firstPort, 3, 'close');
  expect((await response(firstPort, 3)).ok).toBe(true);
  expect(first.close).not.toHaveBeenCalled();
  request(finalPort, 4, 'headLamport');
  expect(await response(finalPort, 4)).toMatchObject({ ok: true, result: 7 });

  request(finalPort, 5, 'close');
  expect((await response(finalPort, 5)).ok).toBe(true);
  expect(first.close).toHaveBeenCalledOnce();
});

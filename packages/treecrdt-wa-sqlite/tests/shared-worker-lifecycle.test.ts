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

async function initialize(port: FakePort, id: number): Promise<void> {
  request(port, id, 'init');
  expect(await response(port, id)).toMatchObject({ ok: true });
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

  await initialize(source, 1);
  await initialize(peer, 2);

  request(source, 3, 'drop');
  expect(await response(source, 3)).toMatchObject({ ok: true });
  expect(peer.outbound).toContainEqual({
    type: 'terminal',
    error: SHARED_WORKER_DROPPED_ERROR,
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
  await initialize(source, 1);
  await initialize(peer, 2);

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
  await initialize(live, 1);
  await initialize(messageErrorPort, 2);
  await initialize(failedPostPort, 3);

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
  await initialize(firstPort, 1);
  await initialize(finalPort, 2);

  request(firstPort, 3, 'close');
  expect((await response(firstPort, 3)).ok).toBe(true);
  expect(first.close).not.toHaveBeenCalled();
  request(finalPort, 4, 'headLamport');
  expect(await response(finalPort, 4)).toMatchObject({ ok: true, result: 7 });

  request(finalPort, 5, 'close');
  expect((await response(finalPort, 5)).ok).toBe(true);
  expect(first.close).toHaveBeenCalledOnce();
});

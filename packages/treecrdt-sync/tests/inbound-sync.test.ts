import { expect, test, vi } from 'vitest';
import type { Operation } from '@treecrdt/interface';
import type { SyncPeer, SyncSubscription } from '@treecrdt/sync-protocol';

import { createInboundSync } from '../src/inbound-sync.js';

function deferred<T = void>() {
  let resolve!: (value: T | PromiseLike<T>) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

function createFakeSubscription(): SyncSubscription & {
  resolveReady: () => void;
  rejectReady: (error: unknown) => void;
  rejectDone: (error: unknown) => void;
  stopped: ReturnType<typeof vi.fn>;
} {
  const ready = deferred<void>();
  const done = deferred<void>();
  const stopped = vi.fn();
  return {
    ready: ready.promise,
    done: done.promise,
    stop: stopped,
    resolveReady: () => ready.resolve(),
    rejectReady: (error) => ready.reject(error),
    rejectDone: (error) => done.reject(error),
    stopped,
  };
}

function createFakePeer() {
  const subscriptions: ReturnType<typeof createFakeSubscription>[] = [];
  const peer = {
    subscribe: vi.fn(() => {
      const sub = createFakeSubscription();
      subscriptions.push(sub);
      return sub;
    }),
    syncOnce: vi.fn(async () => {}),
  } as unknown as SyncPeer<Operation>;
  return { peer, subscriptions };
}

test('inbound sync starts live scopes for existing and future peers', () => {
  const { peer } = createFakePeer();
  const inbound = createInboundSync({ localPeer: peer });
  inbound.addPeer('peer-a', {} as any);

  const scope = inbound.scope({ all: {} });
  scope.startLive();

  expect(peer.subscribe).toHaveBeenCalledTimes(1);
  expect(scope.livePeerCount).toBe(1);

  inbound.addPeer('peer-b', {} as any);

  expect(peer.subscribe).toHaveBeenCalledTimes(2);
  expect(scope.livePeerCount).toBe(2);
});

test('inbound sync keeps live scopes active when peers are cleared', () => {
  const { peer, subscriptions } = createFakePeer();
  const inbound = createInboundSync({ localPeer: peer });
  const scope = inbound.scope({ all: {} });
  scope.startLive();
  inbound.addPeer('peer-a', {} as any);

  inbound.clearPeers();

  expect(subscriptions[0]!.stopped).toHaveBeenCalledTimes(1);
  expect(scope.live).toBe(true);
  expect(scope.livePeerCount).toBe(0);

  inbound.addPeer('peer-b', {} as any);

  expect(peer.subscribe).toHaveBeenCalledTimes(2);
  expect(scope.livePeerCount).toBe(1);
});

test('inbound sync stops subscriptions when peers are removed', () => {
  const { peer, subscriptions } = createFakePeer();
  const inbound = createInboundSync({ localPeer: peer });
  const scope = inbound.scope({ all: {} });
  scope.startLive();
  inbound.addPeer('peer-a', {} as any);

  expect(subscriptions).toHaveLength(1);

  inbound.removePeer('peer-a');

  expect(subscriptions[0]!.stopped).toHaveBeenCalledTimes(1);
  expect(scope.livePeerCount).toBe(0);
});

test('inbound sync ignores peers rejected by shouldSyncPeer', async () => {
  const { peer } = createFakePeer();
  const inbound = createInboundSync({
    localPeer: peer,
    shouldSyncPeer: (peerId) => peerId === 'peer-a',
  });
  const scope = inbound.scope({ all: {} });
  scope.startLive();

  inbound.addPeer('peer-a', {} as any);
  inbound.addPeer('peer-b', {} as any);

  expect(peer.subscribe).toHaveBeenCalledTimes(1);

  await scope.syncOnce();

  expect(peer.syncOnce).toHaveBeenCalledTimes(1);
});

test('inbound sync reports ready and live subscription failures', async () => {
  const { peer, subscriptions } = createFakePeer();
  const errors: string[] = [];
  const inbound = createInboundSync({
    localPeer: peer,
    onError: ({ phase, error }) =>
      errors.push(`${phase}:${error instanceof Error ? error.message : String(error)}`),
  });
  const scope = inbound.scope({ all: {} });
  scope.startLive();
  inbound.addPeer('peer-a', {} as any);
  subscriptions[0]!.rejectReady(new Error('initial catch-up failed'));
  await subscriptions[0]!.ready.catch(() => undefined);

  expect(errors).toEqual(['ready:initial catch-up failed']);
  expect(scope.livePeerCount).toBe(0);

  inbound.addPeer('peer-b', {} as any);
  subscriptions[1]!.resolveReady();
  await subscriptions[1]!.ready;
  subscriptions[1]!.rejectDone(new Error('live failed'));
  await subscriptions[1]!.done.catch(() => undefined);

  expect(errors).toEqual(['ready:initial catch-up failed', 'live:live failed']);
  expect(scope.livePeerCount).toBe(0);
});

test('inbound sync syncOnce reconciles all available peers', async () => {
  const { peer } = createFakePeer();
  const inbound = createInboundSync({ localPeer: peer });
  const filter = { all: {} };
  inbound.addPeer('peer-a', {} as any);
  inbound.addPeer('peer-b', {} as any);

  await inbound.scope(filter).syncOnce();

  expect(peer.syncOnce).toHaveBeenCalledTimes(2);
  expect(peer.syncOnce).toHaveBeenNthCalledWith(1, expect.anything(), filter, undefined);
  expect(peer.syncOnce).toHaveBeenNthCalledWith(2, expect.anything(), filter, undefined);
});

test('inbound sync syncOnce supports custom peer ordering and sync execution', async () => {
  const { peer } = createFakePeer();
  const calls: string[] = [];
  const inbound = createInboundSync({
    localPeer: peer,
    selectPeers: (peerIds) => [...peerIds].reverse().slice(0, 1),
    runSync: async ({ peerId }) => {
      calls.push(peerId);
    },
  });
  const filter = { all: {} };
  inbound.addPeer('peer-a', {} as any);
  inbound.addPeer('peer-b', {} as any);

  await inbound.scope(filter).syncOnce();

  expect(calls).toEqual(['peer-b']);
  expect(peer.syncOnce).not.toHaveBeenCalled();
});

import { expect, test, vi } from 'vitest';
import type { Operation } from '@treecrdt/interface';
import type { Filter, SyncPeer, SyncSubscription } from '@treecrdt/sync-protocol';

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

const allFilter: Filter = { all: {} };
const childFilter: Filter = { children: { parent: new Uint8Array(16) } };

test('inbound sync starts live scopes for existing and future peers', () => {
  const { peer } = createFakePeer();
  const inbound = createInboundSync({ localPeer: peer });
  inbound.addPeer('peer-a', {} as any);

  inbound.setLiveScopes([allFilter]);

  expect(peer.subscribe).toHaveBeenCalledTimes(1);
  expect(inbound.status.livePeerCount).toBe(1);

  inbound.addPeer('peer-b', {} as any);

  expect(peer.subscribe).toHaveBeenCalledTimes(2);
  expect(inbound.status.livePeerCount).toBe(2);
});

test('inbound sync declaratively diffs live scopes', () => {
  const { peer, subscriptions } = createFakePeer();
  const inbound = createInboundSync({ localPeer: peer });
  inbound.addPeer('peer-a', {} as any);

  inbound.setLiveScopes([allFilter]);
  inbound.setLiveScopes([childFilter]);

  expect(subscriptions[0]!.stopped).toHaveBeenCalledTimes(1);
  expect(peer.subscribe).toHaveBeenCalledTimes(2);
  expect(inbound.status.liveScopeCount).toBe(1);
  expect(inbound.status.livePeerCount).toBe(1);

  inbound.setLiveScopes([]);

  expect(subscriptions[1]!.stopped).toHaveBeenCalledTimes(1);
  expect(inbound.status.liveScopeCount).toBe(0);
  expect(inbound.status.livePeerCount).toBe(0);
});

test('inbound sync stops subscriptions when peers are removed', () => {
  const { peer, subscriptions } = createFakePeer();
  const inbound = createInboundSync({ localPeer: peer });
  inbound.setLiveScopes([allFilter]);
  inbound.addPeer('peer-a', {} as any);

  expect(subscriptions).toHaveLength(1);

  inbound.removePeer('peer-a');

  expect(subscriptions[0]!.stopped).toHaveBeenCalledTimes(1);
  expect(inbound.status.livePeerCount).toBe(0);
});

test('inbound sync suppresses stale subscription failures after peer removal', async () => {
  const { peer, subscriptions } = createFakePeer();
  const errors: unknown[] = [];
  const inbound = createInboundSync({
    localPeer: peer,
    onError: ({ error }) => errors.push(error),
  });
  inbound.setLiveScopes([allFilter]);
  inbound.addPeer('peer-a', {} as any);

  inbound.removePeer('peer-a');
  subscriptions[0]!.rejectReady(new Error('stale ready failure'));
  await subscriptions[0]!.ready.catch(() => undefined);

  expect(errors).toEqual([]);
});

test('inbound sync reports ready and live subscription failures', async () => {
  const { peer, subscriptions } = createFakePeer();
  const errors: string[] = [];
  const inbound = createInboundSync({
    localPeer: peer,
    onError: ({ phase, error }) =>
      errors.push(`${phase}:${error instanceof Error ? error.message : String(error)}`),
  });
  inbound.setLiveScopes([allFilter]);
  inbound.addPeer('peer-a', {} as any);
  subscriptions[0]!.rejectReady(new Error('initial catch-up failed'));
  await subscriptions[0]!.ready.catch(() => undefined);

  expect(errors).toEqual(['ready:initial catch-up failed']);
  expect(inbound.status.livePeerCount).toBe(0);

  inbound.addPeer('peer-b', {} as any);
  subscriptions[1]!.resolveReady();
  await subscriptions[1]!.ready;
  subscriptions[1]!.rejectDone(new Error('live failed'));
  await subscriptions[1]!.done.catch(() => undefined);

  expect(errors).toEqual(['ready:initial catch-up failed', 'live:live failed']);
  expect(inbound.status.livePeerCount).toBe(0);
});

test('inbound sync syncOnce reconciles all available peers', async () => {
  const { peer } = createFakePeer();
  const inbound = createInboundSync({ localPeer: peer });
  inbound.addPeer('peer-a', {} as any);
  inbound.addPeer('peer-b', {} as any);

  await inbound.syncOnce(allFilter);

  expect(peer.syncOnce).toHaveBeenCalledTimes(2);
  expect(peer.syncOnce).toHaveBeenNthCalledWith(1, expect.anything(), allFilter, undefined);
  expect(peer.syncOnce).toHaveBeenNthCalledWith(2, expect.anything(), allFilter, undefined);
});

test('inbound sync syncOnce supports selected peer ids', async () => {
  const { peer } = createFakePeer();
  const inbound = createInboundSync({ localPeer: peer });
  inbound.addPeer('peer-a', {} as any);
  inbound.addPeer('peer-b', {} as any);

  await inbound.syncOnce([allFilter, childFilter], { peerIds: ['peer-b'] });

  expect(peer.syncOnce).toHaveBeenCalledTimes(2);
  expect(peer.syncOnce).toHaveBeenNthCalledWith(1, expect.anything(), allFilter, undefined);
  expect(peer.syncOnce).toHaveBeenNthCalledWith(2, expect.anything(), childFilter, undefined);
});

import { expect, test, vi } from 'vitest';
import type { Operation } from '@treecrdt/interface';
import type {
  Filter,
  SyncMessage,
  SyncOnceOptions,
  SyncPeer,
  SyncSubscription,
} from '@treecrdt/sync-protocol';
import type { DuplexTransport } from '@treecrdt/sync-protocol/transport';

import {
  createInboundSync,
  InboundSyncAggregateError,
  type InboundSync,
} from '../src/inbound-sync.js';

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
  resolveDone: () => void;
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
    resolveDone: () => done.resolve(),
    rejectDone: (error) => done.reject(error),
    stopped,
  };
}

function createFakePeer() {
  const subscriptions: ReturnType<typeof createFakeSubscription>[] = [];
  const syncOnce = vi.fn(
    async (
      _transport: DuplexTransport<SyncMessage<Operation>>,
      _filter: Filter,
      _opts?: SyncOnceOptions,
    ) => {},
  );
  const peer = {
    subscribe: vi.fn(() => {
      const sub = createFakeSubscription();
      subscriptions.push(sub);
      return sub;
    }),
    syncOnce,
  } as unknown as SyncPeer<Operation>;
  return { peer, subscriptions, syncOnce };
}

const allFilter: Filter = { all: {} };
const childFilter: Filter = { children: { parent: new Uint8Array(16) } };

test('inbound sync subscribes existing and future peers', () => {
  const { peer } = createFakePeer();
  const inbound = createInboundSync({ localPeer: peer });
  inbound.addAttachedPeer('peer-a', {} as any);

  inbound.subscribe(allFilter);

  expect(peer.subscribe).toHaveBeenCalledTimes(1);
  expect(inbound.status.activeSubscriptionCount).toBe(1);

  inbound.addAttachedPeer('peer-b', {} as any);

  expect(peer.subscribe).toHaveBeenCalledTimes(2);
  expect(inbound.status.activeSubscriptionCount).toBe(2);
});

test('inbound sync declaratively diffs subscription filters', () => {
  const { peer, subscriptions } = createFakePeer();
  const inbound = createInboundSync({ localPeer: peer });
  inbound.addAttachedPeer('peer-a', {} as any);

  inbound.subscribe(allFilter);
  inbound.subscribe(childFilter);

  expect(subscriptions[0]!.stopped).toHaveBeenCalledTimes(1);
  expect(peer.subscribe).toHaveBeenCalledTimes(2);
  expect(inbound.status.subscriptionFilterCount).toBe(1);
  expect(inbound.status.activeSubscriptionCount).toBe(1);

  inbound.subscribe([]);

  expect(subscriptions[1]!.stopped).toHaveBeenCalledTimes(1);
  expect(inbound.status.subscriptionFilterCount).toBe(0);
  expect(inbound.status.activeSubscriptionCount).toBe(0);
});

test('inbound sync stops subscriptions when peers are removed', () => {
  const { peer, subscriptions } = createFakePeer();
  const inbound = createInboundSync({ localPeer: peer });
  inbound.subscribe(allFilter);
  inbound.addAttachedPeer('peer-a', {} as any);

  expect(subscriptions).toHaveLength(1);

  inbound.removePeer('peer-a');

  expect(subscriptions[0]!.stopped).toHaveBeenCalledTimes(1);
  expect(inbound.status.activeSubscriptionCount).toBe(0);
});

test('inbound sync suppresses stale subscription failures after peer removal', async () => {
  const { peer, subscriptions } = createFakePeer();
  const errors: unknown[] = [];
  const inbound = createInboundSync({
    localPeer: peer,
    onError: ({ error }) => errors.push(error),
  });
  inbound.subscribe(allFilter);
  inbound.addAttachedPeer('peer-a', {} as any);

  inbound.removePeer('peer-a');
  subscriptions[0]!.rejectReady(new Error('stale ready failure'));
  await subscriptions[0]!.ready.catch(() => undefined);

  expect(errors).toEqual([]);
});

test('inbound sync replacement cannot restore or report the stale subscription', async () => {
  const { peer, subscriptions } = createFakePeer();
  const errors: unknown[] = [];
  const inbound = createInboundSync({
    localPeer: peer,
    onError: ({ error }) => errors.push(error),
  });
  inbound.subscribe(allFilter);
  inbound.addAttachedPeer('peer-a', {} as any);
  inbound.addAttachedPeer('peer-a', {} as any);

  subscriptions[0]!.rejectReady(new Error('stale ready failure'));
  await subscriptions[0]!.ready.catch(() => undefined);

  expect(subscriptions[0]!.stopped).toHaveBeenCalledTimes(1);
  expect(subscriptions).toHaveLength(2);
  expect(inbound.status.activeSubscriptionCount).toBe(1);
  expect(errors).toEqual([]);

  subscriptions[0]!.resolveDone();
  subscriptions[1]!.resolveReady();
  await subscriptions[1]!.ready;
  const closing = inbound.close();
  subscriptions[1]!.resolveDone();
  await closing;
});

test('inbound sync registration cleanup cannot remove its replacement', async () => {
  const { peer, subscriptions, syncOnce } = createFakePeer();
  const inbound = createInboundSync({ localPeer: peer });
  const transportA = {} as DuplexTransport<SyncMessage<Operation>>;
  const transportB = {} as DuplexTransport<SyncMessage<Operation>>;
  inbound.subscribe(allFilter);
  const unregisterA = inbound.addAttachedPeer('peer-a', transportA);
  const unregisterB = inbound.addAttachedPeer('peer-a', transportB);

  unregisterA();

  expect(inbound.peerCount).toBe(1);
  expect(subscriptions[0]!.stopped).toHaveBeenCalledTimes(1);
  expect(subscriptions[1]!.stopped).not.toHaveBeenCalled();
  expect(inbound.status.activeSubscriptionCount).toBe(1);

  await inbound.syncOnce(allFilter);
  expect(syncOnce).toHaveBeenCalledWith(
    transportB,
    allFilter,
    expect.objectContaining({ signal: expect.any(AbortSignal) }),
  );

  unregisterB();
  unregisterB();
  expect(inbound.peerCount).toBe(0);
  expect(subscriptions[1]!.stopped).toHaveBeenCalledTimes(1);
});

test('inbound sync keeps a duplicate registration until its last lease is released', () => {
  const { peer, subscriptions } = createFakePeer();
  const inbound = createInboundSync({ localPeer: peer });
  const transport = {} as DuplexTransport<SyncMessage<Operation>>;
  inbound.subscribe(allFilter);
  const unregisterFirst = inbound.addAttachedPeer('peer-a', transport);
  const unregisterSecond = inbound.addAttachedPeer('peer-a', transport);

  expect(peer.subscribe).toHaveBeenCalledTimes(1);

  unregisterFirst();
  unregisterFirst();
  expect(inbound.peerCount).toBe(1);
  expect(subscriptions[0]!.stopped).not.toHaveBeenCalled();

  unregisterSecond();
  expect(inbound.peerCount).toBe(0);
  expect(subscriptions[0]!.stopped).toHaveBeenCalledTimes(1);
});

test('inbound sync clear updates every filter before notifying observers', () => {
  const { peer, subscriptions } = createFakePeer();
  let inbound!: InboundSync<Operation>;
  let reentered = false;
  let armed = false;
  inbound = createInboundSync({
    localPeer: peer,
    onStatus: (status) => {
      if (!armed || reentered || status.peerCount !== 0) return;
      reentered = true;
      inbound.addAttachedPeer('peer-b', {} as any);
    },
  });
  inbound.subscribe([allFilter, childFilter]);
  inbound.addAttachedPeer('peer-a', {} as any);
  armed = true;

  inbound.clearPeers();

  expect(reentered).toBe(true);
  expect(inbound.peerCount).toBe(1);
  expect(inbound.status.activeSubscriptionCount).toBe(2);
  expect(subscriptions).toHaveLength(4);
  expect(subscriptions.slice(0, 2).every(({ stopped }) => stopped.mock.calls.length === 1)).toBe(
    true,
  );
  expect(subscriptions.slice(2).every(({ stopped }) => stopped.mock.calls.length === 0)).toBe(true);
});

test('inbound sync clear cannot erase a peer registered by a teardown callback', () => {
  const { peer, subscriptions } = createFakePeer();
  const inbound = createInboundSync({ localPeer: peer });
  inbound.subscribe([allFilter, childFilter]);
  inbound.addAttachedPeer('peer-a', {} as any);
  subscriptions[0]!.stopped.mockImplementationOnce(() => {
    inbound.addAttachedPeer('peer-b', {} as any);
  });

  inbound.clearPeers();

  expect(inbound.peerCount).toBe(1);
  expect(inbound.status.activeSubscriptionCount).toBe(2);
  expect(subscriptions).toHaveLength(4);
  expect(subscriptions.slice(2).every(({ stopped }) => stopped.mock.calls.length === 0)).toBe(true);
});

test('inbound sync nested subscribe declaration is latest-wins', () => {
  const subscriptions: ReturnType<typeof createFakeSubscription>[] = [];
  let inbound!: InboundSync<Operation>;
  let nested = false;
  const peer = {
    subscribe: vi.fn(() => {
      const subscription = createFakeSubscription();
      subscriptions.push(subscription);
      if (!nested) {
        nested = true;
        inbound.subscribe(childFilter);
      }
      return subscription;
    }),
  } as unknown as SyncPeer<Operation>;
  inbound = createInboundSync({ localPeer: peer });
  inbound.addAttachedPeer('peer-a', {} as any);

  inbound.subscribe([allFilter, childFilter]);

  expect(peer.subscribe).toHaveBeenCalledTimes(2);
  expect(inbound.status.subscriptionFilterCount).toBe(1);
  expect(inbound.status.activeSubscriptionCount).toBe(1);
  expect(subscriptions[0]!.stopped).toHaveBeenCalledTimes(1);
  expect(subscriptions[1]!.stopped).not.toHaveBeenCalled();
});

test('inbound sync reports ready and live subscription failures', async () => {
  const { peer, subscriptions } = createFakePeer();
  const errors: string[] = [];
  const inbound = createInboundSync({
    localPeer: peer,
    onError: ({ phase, error }) =>
      errors.push(`${phase}:${error instanceof Error ? error.message : String(error)}`),
  });
  inbound.subscribe(allFilter);
  inbound.addAttachedPeer('peer-a', {} as any);
  subscriptions[0]!.rejectReady(new Error('initial catch-up failed'));
  await subscriptions[0]!.ready.catch(() => undefined);

  expect(errors).toEqual(['ready:initial catch-up failed']);
  expect(inbound.status.activeSubscriptionCount).toBe(0);

  inbound.addAttachedPeer('peer-b', {} as any);
  subscriptions[1]!.resolveReady();
  await subscriptions[1]!.ready;
  subscriptions[1]!.rejectDone(new Error('live failed'));
  await subscriptions[1]!.done.catch(() => undefined);

  expect(errors).toEqual(['ready:initial catch-up failed', 'live:live failed']);
  expect(inbound.status.activeSubscriptionCount).toBe(0);
});

test('inbound sync retries a failed subscription when the same filters are declared again', async () => {
  const { peer, subscriptions } = createFakePeer();
  const inbound = createInboundSync({ localPeer: peer });
  inbound.addAttachedPeer('peer-a', {} as any);
  inbound.subscribe(allFilter);

  subscriptions[0]!.rejectReady(new Error('transient ready failure'));
  await subscriptions[0]!.ready.catch(() => undefined);
  expect(inbound.status.activeSubscriptionCount).toBe(0);

  inbound.subscribe(allFilter);

  expect(peer.subscribe).toHaveBeenCalledTimes(2);
  expect(inbound.status.activeSubscriptionCount).toBe(1);
});

test('inbound sync syncOnce resolves after every available peer succeeds', async () => {
  const { peer, syncOnce } = createFakePeer();
  const inbound = createInboundSync({ localPeer: peer });
  inbound.addAttachedPeer('peer-a', {} as any);
  inbound.addAttachedPeer('peer-b', {} as any);

  await inbound.syncOnce([allFilter, childFilter]);

  expect(syncOnce).toHaveBeenCalledTimes(4);
});

test('inbound sync syncOnce deduplicates equivalent filters', async () => {
  const { peer, syncOnce } = createFakePeer();
  const inbound = createInboundSync({ localPeer: peer });
  inbound.addAttachedPeer('peer-a', {} as any);

  await inbound.syncOnce([
    allFilter,
    { all: {} },
    childFilter,
    { children: { parent: childFilter.children.parent.slice() } },
  ]);

  expect(syncOnce).toHaveBeenCalledTimes(2);
  expect(syncOnce.mock.calls.map(([, filter]) => filter)).toEqual([allFilter, childFilter]);
});

test('inbound sync syncOnce supports selected peer ids', async () => {
  const { peer } = createFakePeer();
  const inbound = createInboundSync({ localPeer: peer });
  inbound.addAttachedPeer('peer-a', {} as any);
  inbound.addAttachedPeer('peer-b', {} as any);

  await inbound.syncOnce([allFilter, childFilter], { peerIds: ['peer-b'] });

  expect(peer.syncOnce).toHaveBeenCalledTimes(2);
  expect(peer.syncOnce).toHaveBeenNthCalledWith(1, expect.anything(), allFilter, {
    signal: expect.any(AbortSignal),
  });
  expect(peer.syncOnce).toHaveBeenNthCalledWith(2, expect.anything(), childFilter, {
    signal: expect.any(AbortSignal),
  });
});

test('inbound sync syncOnce rejects when only some targets fail', async () => {
  const { peer, syncOnce } = createFakePeer();
  const failedTransport = {} as any;
  const healthyTransport = {} as any;
  syncOnce.mockImplementation(async (transport) => {
    if (transport === failedTransport) throw new Error('peer-a failed');
  });

  const inbound = createInboundSync({ localPeer: peer });
  inbound.addAttachedPeer('peer-a', failedTransport);
  inbound.addAttachedPeer('peer-b', healthyTransport);

  const error = await inbound.syncOnce(allFilter).catch((cause) => cause);

  expect(error).toBeInstanceOf(InboundSyncAggregateError);
  expect(error.failures).toEqual([
    {
      peerId: 'peer-a',
      filter: allFilter,
      error: expect.objectContaining({ message: 'peer-a failed' }),
    },
  ]);
  expect(syncOnce).toHaveBeenCalledTimes(2);
});

test('inbound sync syncOnce reports every target when all fail', async () => {
  const { peer, syncOnce } = createFakePeer();
  syncOnce.mockRejectedValue(new Error('offline'));

  const inbound = createInboundSync({ localPeer: peer });
  inbound.addAttachedPeer('peer-a', {} as any);
  inbound.addAttachedPeer('peer-b', {} as any);

  const error = await inbound.syncOnce(allFilter).catch((cause) => cause);

  expect(error).toBeInstanceOf(InboundSyncAggregateError);
  expect(error.failures.map(({ peerId }: { peerId: string }) => peerId)).toEqual([
    'peer-a',
    'peer-b',
  ]);
});

test('inbound sync observer failures do not stop remaining targets or replace the aggregate', async () => {
  const { peer, syncOnce } = createFakePeer();
  syncOnce.mockRejectedValue(new Error('offline'));

  const inbound = createInboundSync({
    localPeer: peer,
    onError: () => {
      throw new Error('error observer failed');
    },
    onStatus: () => {
      throw new Error('status observer failed');
    },
  });
  inbound.addAttachedPeer('peer-a', {} as any);
  inbound.addAttachedPeer('peer-b', {} as any);

  const error = await inbound.syncOnce(allFilter).catch((cause) => cause);

  expect(error).toBeInstanceOf(InboundSyncAggregateError);
  expect(error.failures).toHaveLength(2);
  expect(syncOnce).toHaveBeenCalledTimes(2);
  expect(inbound.status.busy).toBe(false);
});

test('inbound sync is registered before an onStatus callback can close it', async () => {
  const { peer, syncOnce } = createFakePeer();
  let inbound!: InboundSync<Operation>;
  let closing: Promise<void> | undefined;
  inbound = createInboundSync({
    localPeer: peer,
    onStatus: (status) => {
      if (status.busy && !closing) closing = inbound.close();
    },
  });
  inbound.addAttachedPeer('peer-a', {} as any);

  const syncResult = inbound.syncOnce(allFilter).catch((error) => error);
  await Promise.resolve();
  expect(closing).toBeDefined();
  await closing;

  expect(await syncResult).toBeInstanceOf(InboundSyncAggregateError);
  expect(syncOnce).not.toHaveBeenCalled();
  expect(inbound.status.busy).toBe(false);
});

test('inbound sync close from SyncPeer.syncOnce still drains that reconciliation', async () => {
  const releaseSync = deferred<void>();
  let inbound!: InboundSync<Operation>;
  let closing: Promise<void> | undefined;
  const syncOnce = vi.fn(
    (
      _transport: DuplexTransport<SyncMessage<Operation>>,
      _filter: Filter,
      opts?: SyncOnceOptions,
    ) => {
      closing = inbound.close();
      return releaseSync.promise.then(() => {
        if (opts?.signal?.aborted) throw opts.signal.reason;
      });
    },
  );
  const peer = { syncOnce } as unknown as SyncPeer<Operation>;
  inbound = createInboundSync({ localPeer: peer });
  inbound.addAttachedPeer('peer-a', {} as any);

  const syncResult = inbound.syncOnce(allFilter).catch((error) => error);
  await Promise.resolve();

  expect(closing).toBeDefined();
  let closeSettled = false;
  void closing!.then(() => {
    closeSettled = true;
  });
  await Promise.resolve();
  expect(closeSettled).toBe(false);

  releaseSync.resolve();
  await closing;
  expect(await syncResult).toBeInstanceOf(InboundSyncAggregateError);
});

test('inbound sync onStatus close stops and drains an atomically started subscription', async () => {
  const ready = deferred<void>();
  const done = deferred<void>();
  const stop = vi.fn(() => {
    ready.resolve();
    done.resolve();
  });
  const peer = {
    subscribe: vi.fn(
      () =>
        ({
          ready: ready.promise,
          done: done.promise,
          stop,
        }) satisfies SyncSubscription,
    ),
  } as unknown as SyncPeer<Operation>;
  let inbound!: InboundSync<Operation>;
  let closing: Promise<void> | undefined;
  inbound = createInboundSync({
    localPeer: peer,
    onStatus: (status) => {
      if (status.activeSubscriptionCount === 1 && !closing) closing = inbound.close();
    },
  });
  inbound.addAttachedPeer('peer-a', {} as any);

  inbound.subscribe(allFilter);
  await closing;

  expect(peer.subscribe).toHaveBeenCalledTimes(1);
  expect(stop).toHaveBeenCalledTimes(1);
  expect(inbound.status).toEqual({
    peerCount: 0,
    subscriptionFilterCount: 0,
    activeSubscriptionCount: 0,
    busy: false,
  });
});

test('inbound sync close from SyncPeer.subscribe stops and drains the returned subscription', async () => {
  const ready = deferred<void>();
  const done = deferred<void>();
  const stop = vi.fn();
  let inbound!: InboundSync<Operation>;
  let closing: Promise<void> | undefined;
  const peer = {
    subscribe: vi.fn(() => {
      closing = inbound.close();
      return {
        ready: ready.promise,
        done: done.promise,
        stop,
      } satisfies SyncSubscription;
    }),
  } as unknown as SyncPeer<Operation>;
  inbound = createInboundSync({ localPeer: peer });
  inbound.addAttachedPeer('peer-a', {} as any);

  inbound.subscribe([allFilter, childFilter]);

  expect(closing).toBeDefined();
  expect(stop).toHaveBeenCalledTimes(1);
  let closeSettled = false;
  void closing!.then(() => {
    closeSettled = true;
  });
  await Promise.resolve();
  expect(closeSettled).toBe(false);

  ready.resolve();
  done.resolve();
  await closing;
  expect(peer.subscribe).toHaveBeenCalledTimes(1);
  expect(inbound.status).toEqual({
    peerCount: 0,
    subscriptionFilterCount: 0,
    activeSubscriptionCount: 0,
    busy: false,
  });
});

test.each(['replace', 'remove', 'clear'] as const)(
  'inbound sync aborts stale reconciliation when peers %s',
  async (action) => {
    const calls: { signal: AbortSignal }[] = [];
    const errors: unknown[] = [];
    const syncOnce = vi.fn(
      (
        _transport: DuplexTransport<SyncMessage<Operation>>,
        _filter: Filter,
        opts?: SyncOnceOptions,
      ) =>
        new Promise<void>((_resolve, reject) => {
          const signal = opts?.signal;
          if (!signal) throw new Error('expected a cancellation signal');
          calls.push({ signal });
          const rejectAbort = () => reject(signal.reason);
          if (signal.aborted) rejectAbort();
          else signal.addEventListener('abort', rejectAbort, { once: true });
        }),
    );
    const peer = { syncOnce } as unknown as SyncPeer<Operation>;
    const inbound = createInboundSync({
      localPeer: peer,
      onError: ({ error }) => errors.push(error),
    });
    inbound.addAttachedPeer('peer-a', {} as any);

    const result = inbound.syncOnce([allFilter, childFilter]).catch((error) => error);
    await Promise.resolve();

    if (action === 'replace') inbound.addAttachedPeer('peer-a', {} as any);
    else if (action === 'remove') inbound.removePeer('peer-a');
    else inbound.clearPeers();

    const error = await result;
    expect(error).toBeInstanceOf(InboundSyncAggregateError);
    expect(calls).toHaveLength(1);
    expect(calls[0]!.signal.aborted).toBe(true);
    expect(syncOnce).toHaveBeenCalledTimes(1);
    expect(errors).toEqual([]);

    await inbound.close();
  },
);

test('inbound sync cancels every concurrent reconciliation for a removed peer', async () => {
  const signals: AbortSignal[] = [];
  const syncOnce = vi.fn(
    (
      _transport: DuplexTransport<SyncMessage<Operation>>,
      _filter: Filter,
      opts?: SyncOnceOptions,
    ) =>
      new Promise<void>((_resolve, reject) => {
        const signal = opts?.signal;
        if (!signal) throw new Error('expected a cancellation signal');
        signals.push(signal);
        signal.addEventListener('abort', () => reject(signal.reason), { once: true });
      }),
  );
  const peer = { syncOnce } as unknown as SyncPeer<Operation>;
  const inbound = createInboundSync({ localPeer: peer });
  inbound.addAttachedPeer('peer-a', {} as any);

  const first = inbound.syncOnce(allFilter).catch((error) => error);
  const second = inbound.syncOnce(allFilter).catch((error) => error);
  await Promise.resolve();
  inbound.removePeer('peer-a');

  expect(await first).toBeInstanceOf(InboundSyncAggregateError);
  expect(await second).toBeInstanceOf(InboundSyncAggregateError);
  expect(signals).toHaveLength(2);
  expect(signals.every((signal) => signal.aborted)).toBe(true);

  await inbound.close();
});

test('inbound sync close is an idempotent abort-and-drain barrier', async () => {
  const abortObserved = deferred<void>();
  const releaseAbort = deferred<void>();
  const syncOnce = vi.fn(
    (
      _transport: DuplexTransport<SyncMessage<Operation>>,
      _filter: Filter,
      opts?: SyncOnceOptions,
    ) =>
      new Promise<void>((_resolve, reject) => {
        const signal = opts?.signal;
        if (!signal) throw new Error('expected a cancellation signal');
        signal.addEventListener(
          'abort',
          () => {
            abortObserved.resolve();
            void releaseAbort.promise.then(() => reject(signal.reason));
          },
          { once: true },
        );
      }),
  );
  const peer = { syncOnce } as unknown as SyncPeer<Operation>;
  const errors: unknown[] = [];
  const inbound = createInboundSync({
    localPeer: peer,
    onError: ({ error }) => errors.push(error),
  });
  inbound.addAttachedPeer('peer-a', {} as any);
  const syncResult = inbound.syncOnce(allFilter).catch((error) => error);
  await Promise.resolve();

  const closing = inbound.close();
  expect(inbound.close()).toBe(closing);
  await abortObserved.promise;

  let closeSettled = false;
  void closing.then(() => {
    closeSettled = true;
  });
  await Promise.resolve();
  expect(closeSettled).toBe(false);

  releaseAbort.resolve();
  await closing;

  expect(await syncResult).toBeInstanceOf(InboundSyncAggregateError);
  expect(errors).toEqual([]);
  expect(inbound.status).toEqual({
    peerCount: 0,
    subscriptionFilterCount: 0,
    activeSubscriptionCount: 0,
    busy: false,
  });
  expect(() => inbound.addAttachedPeer('peer-b', {} as any)).toThrow(/closed/i);
  expect(() => inbound.removePeer('peer-a')).toThrow(/closed/i);
  expect(() => inbound.clearPeers()).toThrow(/closed/i);
  expect(() => inbound.subscribe(allFilter)).toThrow(/closed/i);
  await expect(inbound.syncOnce(allFilter)).rejects.toThrow(/closed/i);
});

test('inbound sync close waits for subscription readiness and completion', async () => {
  const ready = deferred<void>();
  const done = deferred<void>();
  const stop = vi.fn();
  const peer = {
    subscribe: vi.fn(
      () =>
        ({
          ready: ready.promise,
          done: done.promise,
          stop,
        }) satisfies SyncSubscription,
    ),
  } as unknown as SyncPeer<Operation>;
  const inbound = createInboundSync({ localPeer: peer });
  inbound.addAttachedPeer('peer-a', {} as any);
  inbound.subscribe(allFilter);

  const closing = inbound.close();
  expect(stop).toHaveBeenCalledTimes(1);

  let closeSettled = false;
  void closing.then(() => {
    closeSettled = true;
  });
  ready.resolve();
  await Promise.resolve();
  expect(closeSettled).toBe(false);

  done.resolve();
  await closing;
  expect(closeSettled).toBe(true);
  expect(inbound.status.busy).toBe(false);
});

test('inbound sync timeout aborts the underlying sync and stops further work', async () => {
  vi.useFakeTimers();
  try {
    let signal: AbortSignal | undefined;
    let sent = 0;
    const syncOnce = vi.fn(
      async (_transport: unknown, _filter: Filter, opts?: { signal?: AbortSignal }) =>
        new Promise<void>((_resolve, reject) => {
          signal = opts?.signal;
          const timer = setInterval(() => {
            sent += 1;
          }, 5);
          signal?.addEventListener(
            'abort',
            () => {
              clearInterval(timer);
              reject(signal?.reason);
            },
            { once: true },
          );
        }),
    );
    const peer = { syncOnce } as unknown as SyncPeer<Operation>;
    const inbound = createInboundSync({ localPeer: peer });
    inbound.addAttachedPeer('peer-a', {} as any);

    const result = inbound.syncOnce(allFilter, { syncTimeoutMs: 20 }).catch((error) => error);
    await vi.advanceTimersByTimeAsync(20);
    const error = await result;

    expect(error).toBeInstanceOf(InboundSyncAggregateError);
    expect(error.failures[0].error).toEqual(
      expect.objectContaining({ message: 'sync with peer-a timed out' }),
    );
    expect(signal?.aborted).toBe(true);

    const sentAtTimeout = sent;
    await vi.advanceTimersByTimeAsync(50);
    expect(sent).toBe(sentAtTimeout);
  } finally {
    vi.useRealTimers();
  }
});

import { expect, test, vi } from 'vitest';
import type { Operation } from '@treecrdt/interface';
import { makeOp, nodeIdFromInt } from '@treecrdt/benchmark';

import { createOutboundSync } from '../src/controller.js';
import type { OutboundSync, OutboundSyncPushTarget } from '../src/types.js';
import { ROOT, orderKeyFromPosition } from './test-helpers.js';

function replicaFromLabel(label: string): Uint8Array {
  const encoded = new TextEncoder().encode(label);
  if (encoded.length === 0) throw new Error('replica label must not be empty');
  const out = new Uint8Array(32);
  for (let i = 0; i < out.length; i += 1) out[i] = encoded[i % encoded.length]!;
  return out;
}

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((res) => {
    resolve = res;
  });
  return { promise, resolve };
}

async function tick(): Promise<void> {
  await new Promise<void>((resolve) => setTimeout(resolve, 0));
}

const replicas = { a: replicaFromLabel('a') };

function makeInsertOp(counter = 1): Operation {
  return makeOp(replicas.a, counter, counter, {
    type: 'insert',
    parent: ROOT,
    node: nodeIdFromInt(counter),
    orderKey: orderKeyFromPosition(counter - 1),
  });
}

function createRecordingTarget<Op = Operation>(opts: { failPushes?: number } = {}) {
  let failPushes = opts.failPushes ?? 0;
  const pushed: Op[][] = [];
  const target: OutboundSyncPushTarget<Op> = vi.fn(async (ops) => {
    if (failPushes > 0) {
      failPushes -= 1;
      throw new Error('direct push failed');
    }
    pushed.push([...ops]);
  });
  return { pushed, target };
}

test('outbound sync defers without a target, dedupes, and drains after one is installed', async () => {
  const op = makeInsertOp();
  const { pushed, target } = createRecordingTarget();
  const controller = createOutboundSync();

  controller.queueOps([op, op]);
  await expect(controller.flush()).resolves.toEqual({
    status: 'deferred',
    reason: 'no-target',
    pendingOps: 1,
  });
  expect(controller.status).toMatchObject({ hasTarget: false, pendingOps: 1 });

  controller.setTarget(target);
  await expect(controller.flush()).resolves.toEqual({ status: 'drained' });

  expect(controller.status).toMatchObject({ hasTarget: true, pendingOps: 0 });
  expect(pushed).toEqual([[op]]);
});

test('target cleanup is identity-safe when a connection is replaced', async () => {
  const op = makeInsertOp();
  const oldTarget = vi.fn(async () => {});
  const newTarget = vi.fn(async () => {});
  const controller = createOutboundSync();

  const releaseOld = controller.setTarget(oldTarget);
  const releaseNew = controller.setTarget(newTarget);
  releaseOld();

  expect(controller.status.hasTarget).toBe(true);
  controller.queueOps([op]);
  await controller.flush();

  expect(oldTarget).not.toHaveBeenCalled();
  expect(newTarget).toHaveBeenCalledWith(
    [op],
    expect.objectContaining({ signal: expect.anything() }),
  );

  releaseNew();
  releaseNew();
  expect(controller.status.hasTarget).toBe(false);
});

test('a target installed while a no-target flush settles triggers a retry', async () => {
  const op = makeInsertOp();
  const target = vi.fn(async () => {});
  let controller!: OutboundSync<Operation>;
  let sawActiveFlush = false;
  let installed = false;
  controller = createOutboundSync({
    onStatus: (status) => {
      if (status.flushing) sawActiveFlush = true;
      if (
        sawActiveFlush &&
        !installed &&
        !status.flushing &&
        !status.hasTarget &&
        status.pendingOps === 1
      ) {
        installed = true;
        controller.setTarget(target);
      }
    },
  });
  controller.queueOps([op]);

  await expect(controller.flush()).resolves.toEqual({
    status: 'deferred',
    reason: 'no-target',
    pendingOps: 1,
  });
  for (let i = 0; i < 10 && target.mock.calls.length === 0; i += 1) await tick();

  expect(target).toHaveBeenCalledWith([op], expect.objectContaining({ signal: expect.anything() }));
  expect(controller.status.pendingOps).toBe(0);
});

test('offline work is deferred until the caller explicitly retries', async () => {
  const op = makeInsertOp();
  let online = false;
  const { pushed, target } = createRecordingTarget();
  const controller = createOutboundSync({ isOnline: () => online });
  controller.setTarget(target);
  controller.queueOps([op]);

  await expect(controller.flush()).resolves.toEqual({
    status: 'deferred',
    reason: 'offline',
    pendingOps: 1,
  });

  online = true;
  await tick();
  expect(pushed).toEqual([]);

  await expect(controller.flush()).resolves.toEqual({ status: 'drained' });
  expect(pushed).toEqual([[op]]);
});

test('a failed push returns its error and keeps the batch queued for retry', async () => {
  const op = makeInsertOp();
  const { pushed, target } = createRecordingTarget({ failPushes: 1 });
  const errors: unknown[] = [];
  const controller = createOutboundSync({ onError: (error) => errors.push(error) });
  controller.setTarget(target);
  controller.queueOps([op]);

  const first = await controller.flush();
  expect(first).toMatchObject({ status: 'failed', pendingOps: 1 });
  expect(first.status === 'failed' ? first.error : undefined).toEqual(
    new Error('direct push failed'),
  );
  expect(errors).toHaveLength(1);
  expect(controller.status.pendingOps).toBe(1);

  await expect(controller.flush()).resolves.toEqual({ status: 'drained' });
  expect(pushed).toEqual([[op]]);
});

test('ops queued during a failed push wait for an explicit retry', async () => {
  const firstOp = makeInsertOp(1);
  const secondOp = makeInsertOp(2);
  const firstStarted = deferred();
  const failFirst = deferred();
  const deliveries: Operation[][] = [];
  let attempts = 0;
  const target: OutboundSyncPushTarget<Operation> = vi.fn(async (ops) => {
    attempts += 1;
    if (attempts === 1) {
      firstStarted.resolve();
      await failFirst.promise;
      throw new Error('first push failed');
    }
    deliveries.push([...ops]);
  });
  const controller = createOutboundSync();
  controller.setTarget(target);
  controller.queueOps([firstOp]);

  const flushing = controller.flush();
  await firstStarted.promise;
  controller.queueOps([secondOp]);
  failFirst.resolve();

  await expect(flushing).resolves.toMatchObject({ status: 'failed', pendingOps: 2 });
  await tick();
  expect(target).toHaveBeenCalledTimes(1);
  expect(controller.status.pendingOps).toBe(2);

  await expect(controller.flush()).resolves.toEqual({ status: 'drained' });
  expect(deliveries).toEqual([[firstOp, secondOp]]);
});

test('concurrent flush calls share the drain including work queued during the push', async () => {
  const firstOp = makeInsertOp(1);
  const secondOp = makeInsertOp(2);
  const deliveries: Operation[][] = [];
  const firstGate = deferred();
  const secondGate = deferred();
  const firstStarted = deferred();
  const secondStarted = deferred();
  const target: OutboundSyncPushTarget<Operation> = vi.fn(async (ops) => {
    deliveries.push([...ops]);
    if (deliveries.length === 1) {
      firstStarted.resolve();
      await firstGate.promise;
    } else {
      secondStarted.resolve();
      await secondGate.promise;
    }
  });
  const controller = createOutboundSync();
  controller.setTarget(target);

  controller.queueOps([firstOp]);
  const activeFlush = controller.flush();
  await firstStarted.promise;
  controller.queueOps([secondOp]);
  const concurrentFlush = controller.flush();
  let concurrentResolved = false;
  void concurrentFlush.then(() => {
    concurrentResolved = true;
  });

  expect(concurrentFlush).toBe(activeFlush);
  firstGate.resolve();
  await secondStarted.promise;
  expect(concurrentResolved).toBe(false);

  secondGate.resolve();
  await expect(activeFlush).resolves.toEqual({ status: 'drained' });
  expect(deliveries).toEqual([[firstOp], [secondOp]]);
});

test('a timed-out push is aborted before the queued batch is retried', async () => {
  const op = makeInsertOp();
  const pushed: Operation[][] = [];
  let attempts = 0;
  let activePushes = 0;
  let maxActivePushes = 0;
  let timedOutSignal: AbortSignal | undefined;
  const target: OutboundSyncPushTarget<Operation> = vi.fn((ops, opts) => {
    attempts += 1;
    activePushes += 1;
    maxActivePushes = Math.max(maxActivePushes, activePushes);

    if (attempts === 1) {
      timedOutSignal = opts.signal;
      return new Promise<void>((_resolve, reject) => {
        const onAbort = () => {
          activePushes -= 1;
          reject(timedOutSignal?.reason);
        };
        if (timedOutSignal?.aborted) onAbort();
        else timedOutSignal?.addEventListener('abort', onAbort, { once: true });
      });
    }

    activePushes -= 1;
    pushed.push([...ops]);
    return Promise.resolve();
  });
  const controller = createOutboundSync({ pushTimeoutMs: 5 });
  controller.setTarget(target);
  controller.queueOps([op]);

  await expect(controller.flush()).resolves.toMatchObject({ status: 'failed', pendingOps: 1 });
  expect(timedOutSignal?.aborted).toBe(true);

  await expect(controller.flush()).resolves.toEqual({ status: 'drained' });
  expect(maxActivePushes).toBe(1);
  expect(pushed).toEqual([[op]]);
});

test('an in-flight batch is replayed to a replacement target', async () => {
  const op = makeInsertOp();
  let oldSignal: AbortSignal | undefined;
  const oldStarted = deferred();
  const oldTarget: OutboundSyncPushTarget<Operation> = vi.fn((_ops, opts) => {
    oldSignal = opts.signal;
    oldStarted.resolve();
    return new Promise<void>((_resolve, reject) => {
      const onAbort = () => reject(oldSignal?.reason);
      if (oldSignal?.aborted) onAbort();
      else oldSignal?.addEventListener('abort', onAbort, { once: true });
    });
  });
  const newTarget = vi.fn(async () => {});
  const controller = createOutboundSync();
  controller.setTarget(oldTarget);

  controller.queueOps([op]);
  const flushing = controller.flush();
  await oldStarted.promise;
  controller.setTarget(newTarget);

  await expect(flushing).resolves.toEqual({ status: 'drained' });
  expect(oldSignal?.aborted).toBe(true);
  expect(newTarget).toHaveBeenCalledWith(
    [op],
    expect.objectContaining({ signal: expect.anything() }),
  );
  expect(controller.status.pendingOps).toBe(0);
});

test('a replacement that was already tried is not retried implicitly after failure', async () => {
  const firstOp = makeInsertOp(1);
  const secondOp = makeInsertOp(2);
  const oldStarted = deferred();
  let oldSignal: AbortSignal | undefined;
  const oldTarget: OutboundSyncPushTarget<Operation> = vi.fn((_ops, opts) => {
    oldSignal = opts.signal;
    oldStarted.resolve();
    return new Promise<void>((_resolve, reject) => {
      const onAbort = () => reject(oldSignal?.reason);
      if (oldSignal?.aborted) onAbort();
      else oldSignal?.addEventListener('abort', onAbort, { once: true });
    });
  });
  const replacementBatches: Operation[][] = [];
  const newTarget: OutboundSyncPushTarget<Operation> = vi.fn(async (ops) => {
    replacementBatches.push([...ops]);
    if (replacementBatches.length === 1) throw new Error('replacement push failed');
  });
  const controller = createOutboundSync();
  controller.setTarget(oldTarget);
  controller.queueOps([firstOp]);

  const flushing = controller.flush();
  await oldStarted.promise;
  controller.queueOps([secondOp]);
  controller.setTarget(newTarget);

  await expect(flushing).resolves.toMatchObject({ status: 'failed', pendingOps: 2 });
  await tick();
  expect(replacementBatches).toEqual([[firstOp, secondOp]]);
  expect(controller.status.pendingOps).toBe(2);

  await expect(controller.flush()).resolves.toEqual({ status: 'drained' });
  expect(replacementBatches).toEqual([
    [firstOp, secondOp],
    [firstOp, secondOp],
  ]);
});

test('an already-aborted parent signal does not invoke the target', async () => {
  const parent = new AbortController();
  parent.abort(new Error('already cancelled'));
  const target = vi.fn(async () => {});
  const controller = createOutboundSync({ pushOptions: { signal: parent.signal } });
  controller.setTarget(target);
  controller.queueOps([makeInsertOp()]);

  await expect(controller.flush()).resolves.toMatchObject({ status: 'failed', pendingOps: 1 });
  expect(target).not.toHaveBeenCalled();
});

test('notifyLocalUpdate and observers are optional and isolated from queue delivery', async () => {
  const op = makeInsertOp();
  const target = vi.fn(async () => {});
  const controller = createOutboundSync({
    notifyLocalUpdate: async () => {
      throw new Error('local notification failed');
    },
    onStatus: () => {
      throw new Error('status observer failed');
    },
    onError: () => {
      throw new Error('error observer failed');
    },
  });
  controller.setTarget(target);
  controller.queueOps([op]);

  await expect(controller.flush()).resolves.toEqual({ status: 'drained' });
  expect(target).toHaveBeenCalledWith([op], expect.objectContaining({ signal: expect.anything() }));
});

test('a synchronous local notification cannot enqueue work after closing the controller', async () => {
  let controller!: OutboundSync<Operation>;
  controller = createOutboundSync({
    notifyLocalUpdate: () => controller.close(),
  });

  controller.queueOps([makeInsertOp()]);
  await controller.close();

  expect(controller.status).toEqual({
    hasTarget: false,
    pendingOps: 0,
    flushing: false,
    closed: true,
  });
});

test('close installs its shared barrier before notifying reentrant observers', async () => {
  let controller!: OutboundSync<Operation>;
  let reentrantClose: Promise<void> | undefined;
  controller = createOutboundSync({
    onStatus: (status) => {
      if (status.closed) reentrantClose = controller.close();
    },
  });

  const firstClose = controller.close();

  expect(reentrantClose).toBe(firstClose);
  await firstClose;
});

test('work queued by the final status notification is not stranded behind the settling flush', async () => {
  const firstOp = makeInsertOp(1);
  const secondOp = makeInsertOp(2);
  const target = vi.fn(async () => {});
  let controller!: OutboundSync<Operation>;
  let queuedFromFinalStatus = false;
  controller = createOutboundSync({
    onStatus: (status) => {
      if (
        !queuedFromFinalStatus &&
        !status.flushing &&
        status.pendingOps === 0 &&
        target.mock.calls.length === 1
      ) {
        queuedFromFinalStatus = true;
        controller.queueOps([secondOp]);
      }
    },
  });
  controller.setTarget(target);

  controller.queueOps([firstOp]);
  await controller.flush();
  for (let i = 0; i < 10 && target.mock.calls.length < 2; i += 1) await tick();

  expect(target.mock.calls.map(([ops]) => ops)).toEqual([[firstOp], [secondOp]]);
  expect(controller.status.pendingOps).toBe(0);
});

test('custom op keys dedupe non-TreeCRDT shapes', async () => {
  type CustomOp = { id: string };
  const op: CustomOp = { id: 'local-write-1' };
  const { pushed, target } = createRecordingTarget<CustomOp>();
  const controller = createOutboundSync<CustomOp>({ opKey: (next) => next.id });
  controller.setTarget(target);

  controller.queueOps([op, { id: op.id }]);
  await controller.flush();

  expect(pushed).toEqual([[op]]);
});

test('a reentrant close from opKey cannot enqueue or notify after teardown', async () => {
  type CustomOp = { id: string };
  const notifyLocalUpdate = vi.fn();
  let controller!: OutboundSync<CustomOp>;
  controller = createOutboundSync<CustomOp>({
    opKey: (op) => {
      void controller.close();
      return op.id;
    },
    notifyLocalUpdate,
  });

  controller.queueOps([{ id: 'close-during-key' }]);
  await controller.close();

  expect(notifyLocalUpdate).not.toHaveBeenCalled();
  expect(controller.status).toEqual({
    hasTarget: false,
    pendingOps: 0,
    flushing: false,
    closed: true,
  });
});

test('close aborts and awaits an active flush, clears state, and is idempotent', async () => {
  const started = deferred();
  const abortObserved = deferred();
  const abortCleanup = deferred();
  let pushSignal: AbortSignal | undefined;
  let reentrantCloseFromAbort: Promise<void> | undefined;
  let controller!: OutboundSync<Operation>;
  const target: OutboundSyncPushTarget<Operation> = vi.fn((_ops, opts) => {
    pushSignal = opts.signal;
    started.resolve();
    return new Promise<void>((_resolve, reject) => {
      const onAbort = async () => {
        reentrantCloseFromAbort = controller.close();
        abortObserved.resolve();
        await abortCleanup.promise;
        reject(pushSignal?.reason);
      };
      if (pushSignal?.aborted) onAbort();
      else pushSignal?.addEventListener('abort', onAbort, { once: true });
    });
  });
  controller = createOutboundSync();
  controller.setTarget(target);
  controller.queueOps([makeInsertOp()]);

  const flushing = controller.flush();
  await started.promise;
  const firstClose = controller.close();
  const secondClose = controller.close();

  expect(secondClose).toBe(firstClose);
  await abortObserved.promise;
  expect(reentrantCloseFromAbort).toBe(firstClose);
  let closeResolved = false;
  void firstClose.then(() => {
    closeResolved = true;
  });
  await tick();
  expect(closeResolved).toBe(false);

  abortCleanup.resolve();
  await firstClose;
  await expect(flushing).resolves.toEqual({ status: 'closed' });
  expect(pushSignal?.aborted).toBe(true);
  expect(controller.status).toEqual({
    hasTarget: false,
    pendingOps: 0,
    flushing: false,
    closed: true,
  });

  controller.queueOps([makeInsertOp(2)]);
  controller.setTarget(vi.fn(async () => {}));
  await expect(controller.flush()).resolves.toEqual({ status: 'closed' });
  expect(controller.status.pendingOps).toBe(0);
});

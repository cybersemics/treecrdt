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

test('outbound sync rejects without a target, dedupes, and drains after one is installed', async () => {
  const op = makeInsertOp();
  const { pushed, target } = createRecordingTarget();
  const controller = createOutboundSync();

  const first = controller.push([op, op]);
  await Promise.all([
    expect(first).rejects.toThrow('no target'),
    expect(controller.flush()).rejects.toThrow('no target'),
  ]);
  expect(controller.status).toMatchObject({ hasTarget: false, pendingOps: 1 });

  controller.setTarget(target);
  await expect(controller.flush()).resolves.toBeUndefined();

  expect(controller.status).toMatchObject({ hasTarget: true, pendingOps: 0 });
  expect(pushed).toEqual([[op]]);
  await expect(first).rejects.toThrow('no target');
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
  await controller.push([op]);

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
  void controller.push([op]).catch(() => {});

  await expect(controller.flush()).rejects.toThrow('no target');
  for (let i = 0; i < 10 && controller.status.pendingOps > 0; i += 1) await tick();

  expect(target).toHaveBeenCalledWith([op], expect.objectContaining({ signal: expect.anything() }));
  expect(controller.status.pendingOps).toBe(0);
});

test('offline work is retained until the caller explicitly retries', async () => {
  const op = makeInsertOp();
  let online = false;
  const { pushed, target } = createRecordingTarget();
  const controller = createOutboundSync({ isOnline: () => online });
  controller.setTarget(target);
  await expect(controller.push([op])).rejects.toThrow('offline');

  online = true;
  await tick();
  expect(pushed).toEqual([]);

  await expect(controller.flush()).resolves.toBeUndefined();
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
  void controller.push([firstOp]).catch(() => {});

  const flushing = controller.flush();
  await firstStarted.promise;
  void controller.push([secondOp]).catch(() => {});
  failFirst.resolve();

  await expect(flushing).rejects.toThrow('first push failed');
  await tick();
  expect(target).toHaveBeenCalledTimes(1);
  expect(controller.status.pendingOps).toBe(2);

  await expect(controller.flush()).resolves.toBeUndefined();
  expect(deliveries).toEqual([[firstOp], [secondOp]]);
});

test('push and flush acknowledge their accepted prefix without waiting for later work', async () => {
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

  const firstPush = controller.push([firstOp]);
  const activeFlush = controller.flush();
  await firstStarted.promise;
  const secondPush = controller.push([secondOp]);
  const concurrentFlush = controller.flush();
  let concurrentResolved = false;
  void concurrentFlush.then(() => {
    concurrentResolved = true;
  });

  expect(concurrentFlush).not.toBe(activeFlush);
  firstGate.resolve();
  await secondStarted.promise;
  await expect(firstPush).resolves.toBeUndefined();
  await expect(activeFlush).resolves.toBeUndefined();
  expect(concurrentResolved).toBe(false);

  secondGate.resolve();
  await expect(secondPush).resolves.toBeUndefined();
  await expect(concurrentFlush).resolves.toBeUndefined();
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
  void controller.push([op]).catch(() => {});

  await expect(controller.flush()).rejects.toThrow('timed out');
  expect(timedOutSignal?.aborted).toBe(true);

  await expect(controller.flush()).resolves.toBeUndefined();
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

  void controller.push([op]).catch(() => {});
  const flushing = controller.flush();
  await oldStarted.promise;
  controller.setTarget(newTarget);

  await expect(flushing).resolves.toBeUndefined();
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
  void controller.push([firstOp]).catch(() => {});

  const flushing = controller.flush();
  await oldStarted.promise;
  void controller.push([secondOp]).catch(() => {});
  controller.setTarget(newTarget);

  await expect(flushing).rejects.toThrow('replacement push failed');
  await tick();
  expect(replacementBatches).toEqual([[firstOp]]);
  expect(controller.status.pendingOps).toBe(2);

  await expect(controller.flush()).resolves.toBeUndefined();
  expect(replacementBatches).toEqual([[firstOp], [firstOp], [secondOp]]);
});

test('an already-aborted parent signal does not invoke the target', async () => {
  const parent = new AbortController();
  parent.abort(new Error('already cancelled'));
  const target = vi.fn(async () => {});
  const controller = createOutboundSync({ pushOptions: { signal: parent.signal } });
  controller.setTarget(target);
  await expect(controller.push([makeInsertOp()])).rejects.toThrow('already cancelled');
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
  await controller.push([op]);
  expect(target).toHaveBeenCalledWith([op], expect.objectContaining({ signal: expect.anything() }));
});

test('a synchronous local notification cannot enqueue work after closing the controller', async () => {
  let controller!: OutboundSync<Operation>;
  controller = createOutboundSync({
    notifyLocalUpdate: () => controller.close(),
  });

  await expect(controller.push([makeInsertOp()])).rejects.toThrow('closed');
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
        void controller.push([secondOp]).catch(() => {});
      }
    },
  });
  controller.setTarget(target);

  await controller.push([firstOp]);
  await controller.whenIdle();

  expect(target.mock.calls.map(([ops]) => ops)).toEqual([[firstOp], [secondOp]]);
  expect(controller.status.pendingOps).toBe(0);
});

test('custom op keys dedupe non-TreeCRDT shapes', async () => {
  type CustomOp = { id: string };
  const op: CustomOp = { id: 'local-write-1' };
  const { pushed, target } = createRecordingTarget<CustomOp>();
  const controller = createOutboundSync<CustomOp>({ opKey: (next) => next.id });
  controller.setTarget(target);

  await controller.push([op, { id: op.id }]);

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

  await expect(controller.push([{ id: 'close-during-key' }])).rejects.toThrow('closed');
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
  void controller.push([makeInsertOp()]).catch(() => {});

  const flushing = controller.flush();
  const rejectedFlush = expect(flushing).rejects.toThrow('closed');
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
  await rejectedFlush;
  expect(pushSignal?.aborted).toBe(true);
  expect(controller.status).toEqual({
    hasTarget: false,
    pendingOps: 0,
    flushing: false,
    closed: true,
  });

  void controller.push([makeInsertOp(2)]).catch(() => {});
  controller.setTarget(vi.fn(async () => {}));
  await expect(controller.flush()).rejects.toThrow('closed');
  expect(controller.status.pendingOps).toBe(0);
});

test('a later failure cannot reject an earlier acknowledged command or flush', async () => {
  const firstOp = makeInsertOp(1);
  const secondOp = makeInsertOp(2);
  const failure = new Error('second failed');
  const target = vi.fn(async (ops: readonly Operation[]) => {
    if (ops[0] === secondOp) throw failure;
  });
  const controller = createOutboundSync();
  controller.setTarget(target);
  const first = controller.push([firstOp]);
  const prefix = controller.flush();
  const second = controller.push([secondOp]);
  const secondRejected = expect(second).rejects.toBe(failure);
  const allRejected = expect(controller.flush()).rejects.toBe(failure);

  await Promise.all([first, prefix, secondRejected, allRejected]);
  expect(target.mock.calls.map(([ops]) => ops)).toEqual([[firstOp], [secondOp]]);
  expect(controller.status.pendingOps).toBe(1);
  await expect(first).resolves.toBeUndefined();
});

test.each(['flush', 'target', 'push'] as const)(
  '%s retries retained work without reviving rejected promises',
  async (retry) => {
    const op = makeInsertOp();
    const { target, pushed } = createRecordingTarget({ failPushes: 1 });
    const errors: unknown[] = [];
    const controller = createOutboundSync({ onError: (error) => errors.push(error) });
    controller.setTarget(target);
    const first = controller.push([op]);
    await expect(first).rejects.toThrow('direct push failed');
    await controller.whenIdle();
    expect(target).toHaveBeenCalledTimes(1);
    expect(errors).toHaveLength(1);
    expect(controller.status.pendingOps).toBe(1);
    if (retry === 'target') {
      controller.setTarget(target);
      for (let i = 0; i < 10 && controller.status.pendingOps > 0; i += 1) await tick();
    } else if (retry === 'push') await controller.push([makeInsertOp(2)]);
    else await controller.flush();
    expect(controller.status.pendingOps).toBe(0);
    expect(pushed[0]).toEqual([op]);
    await expect(first).rejects.toThrow('direct push failed');
  },
);

test('pending and in-flight duplicates share delivery, but acknowledged keys are not kept forever', async () => {
  const gate = deferred();
  const started = deferred();
  const op = makeInsertOp();
  const target = vi.fn(async () => {
    started.resolve();
    await gate.promise;
  });
  const controller = createOutboundSync();
  controller.setTarget(target);
  const first = controller.push([op, op]);
  const pendingDuplicate = controller.push([op]);
  await started.promise;
  const activeDuplicate = controller.push([op]);
  let acknowledged = false;
  void activeDuplicate.then(() => {
    acknowledged = true;
  });
  await tick();
  expect(acknowledged).toBe(false);
  expect(controller.status.pendingOps).toBe(1);
  gate.resolve();
  await Promise.all([first, pendingDuplicate, activeDuplicate]);
  expect(target).toHaveBeenCalledTimes(1);
  await controller.push([op]);
  expect(target).toHaveBeenCalledTimes(2);
});

test('duplicate-only and empty pushes reject with their earlier failed prefix', async () => {
  const { target } = createRecordingTarget({ failPushes: 1 });
  const controller = createOutboundSync();
  controller.setTarget(target);
  const op = makeInsertOp();
  await Promise.all([
    expect(controller.push([op])).rejects.toThrow('direct push failed'),
    expect(controller.push([op])).rejects.toThrow('direct push failed'),
    expect(controller.push([])).rejects.toThrow('direct push failed'),
  ]);
  await controller.flush();
  expect(target).toHaveBeenCalledTimes(2);
});

test('acknowledgment continuations can close before a later command starts', async () => {
  const controller = createOutboundSync();
  const target = vi.fn(async () => {});
  controller.setTarget(target);
  const first = controller.push([makeInsertOp(1)]);
  const closing = first.then(() => controller.close());
  const rejected = expect(controller.push([makeInsertOp(2)])).rejects.toThrow('closed');
  await Promise.all([closing, rejected]);
  expect(target).toHaveBeenCalledTimes(1);
});

test('a replaced unabortable target cannot acknowledge the replacement or overlap it', async () => {
  const oldGate = deferred();
  const newGate = deferred();
  const oldStarted = deferred();
  const newStarted = deferred();
  const oldTarget = vi.fn(async () => {
    oldStarted.resolve();
    await oldGate.promise;
  });
  const newTarget = vi.fn(async () => {
    newStarted.resolve();
    await newGate.promise;
  });
  const controller = createOutboundSync();
  controller.setTarget(oldTarget);
  const push = controller.push([makeInsertOp()]);
  let acknowledged = false;
  void push.then(() => {
    acknowledged = true;
  });
  await oldStarted.promise;
  controller.setTarget(newTarget);
  await tick();
  expect(newTarget).not.toHaveBeenCalled();
  oldGate.resolve();
  await newStarted.promise;
  expect(acknowledged).toBe(false);
  newGate.resolve();
  await push;
});

test.each(['retry', 'close', 'idle'] as const)(
  'timeout rejects promptly but %s still awaits unabortable owned work',
  async (next) => {
    const gate = deferred();
    const started = deferred();
    const target = vi.fn(async () => {
      started.resolve();
      await gate.promise;
    });
    const controller = createOutboundSync({ pushTimeoutMs: 5 });
    controller.setTarget(target);
    const rejected = expect(controller.push([makeInsertOp()])).rejects.toThrow('timed out');
    await started.promise;
    await rejected;
    expect(controller.status.pendingOps).toBe(1);
    const barrier =
      next === 'retry'
        ? controller.flush()
        : next === 'close'
          ? controller.close()
          : controller.whenIdle();
    let settled = false;
    void barrier.then(() => {
      settled = true;
    });
    await tick();
    expect(settled).toBe(false);
    expect(target).toHaveBeenCalledTimes(1);
    gate.resolve();
    await barrier;
    expect(target).toHaveBeenCalledTimes(next === 'retry' ? 2 : 1);
    expect(controller.status.pendingOps).toBe(next === 'idle' ? 1 : 0);
  },
);

test('whenIdle includes work enqueued synchronously by an acknowledgment callback', async () => {
  const firstOp = makeInsertOp(1);
  const secondOp = makeInsertOp(2);
  const started = deferred();
  const gate = deferred();
  const controller = createOutboundSync();
  controller.setTarget(async (ops) => {
    if (ops[0] === secondOp) {
      started.resolve();
      await gate.promise;
    }
  });
  const second = controller.push([firstOp]).then(() => controller.push([secondOp]));
  let idle = false;
  const barrier = controller.whenIdle().then(() => {
    idle = true;
  });
  await started.promise;
  expect(idle).toBe(false);
  gate.resolve();
  await Promise.all([barrier, second]);
  expect(controller.status.pendingOps).toBe(0);
});

test('removing a target rejects promptly without releasing its unabortable work', async () => {
  const gate = deferred();
  const started = deferred();
  const controller = createOutboundSync();
  const remove = controller.setTarget(async () => {
    started.resolve();
    await gate.promise;
  });
  const push = controller.push([makeInsertOp()]);
  const rejected = expect(push).rejects.toThrow('no target');
  await started.promise;
  remove();
  await rejected;
  let closed = false;
  const closing = controller.close().then(() => {
    closed = true;
  });
  await tick();
  expect(closed).toBe(false);
  gate.resolve();
  await closing;
});

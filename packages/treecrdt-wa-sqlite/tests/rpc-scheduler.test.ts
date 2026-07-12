import { createMaterializationDispatcher } from '@treecrdt/interface/engine';
import { expect, test } from 'vitest';
import { makeTreecrdtClientFromCall } from '../src/client.js';
import type { RpcMethod } from '../src/rpc.js';
import {
  createPrioritizedRpcCall,
  createRpcScheduler,
  type RpcSchedulePriority,
} from '../src/rpc-scheduler.js';
import type { ClientMaterializationDispatcher, RpcCall } from '../src/types.js';

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

async function queuedOrder(
  jobs: Array<[RpcSchedulePriority, string]>,
  initialPriority: RpcSchedulePriority = 'background',
): Promise<string[]> {
  const schedule = createRpcScheduler();
  const gate = deferred();
  const order: string[] = [];
  const running = schedule(initialPriority, async () => {
    order.push('initial');
    await gate.promise;
  });
  await Promise.resolve();
  const queued = jobs.map(([priority, label]) =>
    schedule(priority, async () => {
      order.push(label);
    }),
  );
  gate.resolve();
  await Promise.all([running, ...queued]);
  return order;
}

test('foreground work can run between background jobs', async () => {
  const order = await queuedOrder([
    ['background', 'background'],
    ['foreground', 'foreground'],
  ]);
  expect(order).toEqual(['initial', 'foreground', 'background']);
});

test('normal work is an ordering barrier for later foreground work', async () => {
  const order = await queuedOrder([
    ['normal', 'normal'],
    ['background', 'background'],
    ['foreground', 'foreground'],
  ]);
  expect(order).toEqual(['initial', 'normal', 'foreground', 'background']);
});

test('foreground bursts do not starve queued background work', async () => {
  const order = await queuedOrder(
    [
      ['background', 'background'],
      ...Array.from(
        { length: 9 },
        (_, index) => ['foreground', `foreground-${index}`] as [RpcSchedulePriority, string],
      ),
    ],
    'normal',
  );
  expect(order.indexOf('background')).toBe(9);
});

test('engine read classification composes with background scheduling', async () => {
  const gate = deferred();
  const order: RpcMethod[] = [];
  let appendCalls = 0;
  const outcome = { headSeq: 0, changes: [] };
  const runRaw = (async (method: RpcMethod) => {
    order.push(method);
    if (method === 'appendMany') {
      appendCalls += 1;
      if (appendCalls === 1) await gate.promise;
      return outcome;
    }
    if (method === 'treeNodeCount') return 0;
    throw new Error(`unexpected test method: ${method}`);
  }) as RpcCall;
  const dispatcher = createMaterializationDispatcher();
  const materialized = {
    ...dispatcher,
    enableCrossTab: () => undefined,
    emitIncomingEvent: dispatcher.emitEvent,
    close: () => undefined,
  } satisfies ClientMaterializationDispatcher;
  const client = makeTreecrdtClientFromCall({
    mode: 'worker',
    runtime: 'dedicated-worker',
    storage: 'memory',
    docId: 'rpc-priority-test',
    call: createPrioritizedRpcCall(runRaw),
    materialized,
    close: async () => undefined,
    drop: async () => undefined,
  });

  const firstBackground = client.ops.appendMany([], { priority: 'background' });
  await Promise.resolve();
  const secondBackground = client.ops.appendMany([], { priority: 'background' });
  const foregroundRead = client.tree.nodeCount();
  gate.resolve();

  await Promise.all([firstBackground, secondBackground, foregroundRead]);
  expect(order).toEqual(['appendMany', 'treeNodeCount', 'appendMany']);
});

test.each(['foreground', 'normal', 'background'] satisfies RpcSchedulePriority[])(
  'a rejected %s job does not stall the scheduler',
  async (priority) => {
    const schedule = createRpcScheduler();
    const rejected = schedule(priority, async () => {
      throw new Error('expected failure');
    });
    const next = schedule('normal', async () => 'completed');

    await expect(rejected).rejects.toThrow('expected failure');
    await expect(next).resolves.toBe('completed');
  },
);

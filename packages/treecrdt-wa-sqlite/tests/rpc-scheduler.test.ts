import { expect, test } from 'vitest';
import { createRpcScheduler, type RpcSchedulePriority } from '../src/rpc-scheduler.js';

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

test('foreground work can run between background jobs', async () => {
  const schedule = createRpcScheduler();
  const first = deferred();
  const order: string[] = [];

  const running = schedule('background', async () => {
    order.push('background-1');
    await first.promise;
  });
  await Promise.resolve();
  const background = schedule('background', async () => {
    order.push('background-2');
  });
  const foreground = schedule('foreground', async () => {
    order.push('foreground');
  });

  first.resolve();
  await Promise.all([running, background, foreground]);
  expect(order).toEqual(['background-1', 'foreground', 'background-2']);
});

test('normal work is an ordering barrier for later foreground work', async () => {
  const schedule = createRpcScheduler();
  const first = deferred();
  const order: string[] = [];

  const running = schedule('background', async () => {
    order.push('background-1');
    await first.promise;
  });
  await Promise.resolve();
  const normal = schedule('normal', async () => {
    order.push('normal');
  });
  const background = schedule('background', async () => {
    order.push('background-2');
  });
  const foreground = schedule('foreground', async () => {
    order.push('foreground');
  });

  first.resolve();
  await Promise.all([running, normal, background, foreground]);
  expect(order).toEqual(['background-1', 'normal', 'foreground', 'background-2']);
});

test('foreground bursts do not starve queued background work', async () => {
  const schedule = createRpcScheduler();
  const first = deferred();
  const order: string[] = [];

  const running = schedule('normal', () => first.promise);
  await Promise.resolve();
  const jobs: Promise<void>[] = [
    schedule('background', async () => {
      order.push('background');
    }),
  ];
  for (let index = 0; index < 9; index += 1) {
    jobs.push(
      schedule('foreground', async () => {
        order.push(`foreground-${index}`);
      }),
    );
  }

  first.resolve();
  await Promise.all([running, ...jobs]);
  expect(order.indexOf('background')).toBe(8);
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

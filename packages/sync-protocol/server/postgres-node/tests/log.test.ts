import { Writable } from 'node:stream';

import { expect, test } from 'vitest';

import { createCliSyncServerLogger } from '../src/log.ts';

test('cli logger drops lines below the configured level', async () => {
  const lines: Array<Record<string, unknown>> = [];
  const destination = new Writable({
    write(chunk, _encoding, callback) {
      for (const line of chunk.toString().split('\n')) {
        if (line.trim().length > 0) lines.push(JSON.parse(line) as Record<string, unknown>);
      }
      callback();
    },
  });
  const log = await createCliSyncServerLogger({
    level: 'warn',
    pretty: false,
    destination,
  });

  log.debug('doc opened', { docId: 'abc' });
  log.info('peer connected', { docId: 'abc' });
  log.warn('health check failed', { body: 'postgres unavailable' });
  log.error('failed to open doc', { error: 'boom' });

  expect(lines.map((entry) => entry.msg)).toEqual(['health check failed', 'failed to open doc']);
  expect(lines[0]).toMatchObject({
    level: 40,
    component: 'sync-server',
    body: 'postgres unavailable',
  });
  expect(lines[1]).toMatchObject({ level: 50, error: 'boom' });
});

test('asynchronous destination errors do not escape', async () => {
  const destination = new Writable({
    write(_chunk, _encoding, callback) {
      callback();
    },
  });
  const log = await createCliSyncServerLogger({
    level: 'info',
    pretty: false,
    destination,
  });

  await new Promise<void>((resolve, reject) => {
    const onUncaught = (error: Error) => {
      clearTimeout(timer);
      reject(error);
    };
    const timer = setTimeout(() => {
      process.off('uncaughtException', onUncaught);
      resolve();
    }, 50);
    process.once('uncaughtException', onUncaught);
    destination.emit('error', new Error('stream failed'));
    log.info('still logging');
  });
});

test('synchronous destination errors do not escape', async () => {
  const destination = new Writable({
    write() {
      throw new Error('sink down');
    },
  });
  const log = await createCliSyncServerLogger({
    level: 'debug',
    pretty: false,
    destination,
  });
  expect(() => log.error('status check failed')).not.toThrow();
});

import { Writable } from 'node:stream';

import { expect, test } from 'vitest';

import {
  createSyncServerLogger,
  resolveSyncServerLogLevel,
  resolveSyncServerLogPretty,
} from '../dist/index.js';

function captureLogger(level: 'debug' | 'info' | 'warn' | 'error' | 'silent') {
  const lines: Array<Record<string, unknown>> = [];
  const destination = new Writable({
    write(chunk, _encoding, callback) {
      for (const line of chunk.toString().split('\n')) {
        if (line.trim().length > 0) lines.push(JSON.parse(line) as Record<string, unknown>);
      }
      callback();
    },
  });
  const log = createSyncServerLogger({
    level,
    bindings: { component: 'sync-server' },
    destination,
  });
  return { lines, log };
}

test('resolveSyncServerLogLevel uses debug outside production', () => {
  expect(resolveSyncServerLogLevel({})).toBe('debug');
  expect(resolveSyncServerLogLevel({ NODE_ENV: 'development' })).toBe('debug');
  expect(resolveSyncServerLogLevel({ NODE_ENV: 'dev' })).toBe('debug');
  expect(resolveSyncServerLogLevel({ NODE_ENV: 'test' })).toBe('debug');
});

test('resolveSyncServerLogLevel uses info in production', () => {
  expect(resolveSyncServerLogLevel({ NODE_ENV: 'production' })).toBe('info');
  expect(resolveSyncServerLogLevel({ NODE_ENV: 'prod' })).toBe('info');
  expect(resolveSyncServerLogLevel({ NODE_ENV: ' Production ' })).toBe('info');
});

test('TREECRDT_LOG_LEVEL overrides NODE_ENV', () => {
  expect(resolveSyncServerLogLevel({ NODE_ENV: 'production', TREECRDT_LOG_LEVEL: 'debug' })).toBe(
    'debug',
  );
  expect(resolveSyncServerLogLevel({ NODE_ENV: 'development', TREECRDT_LOG_LEVEL: 'error' })).toBe(
    'error',
  );
  expect(resolveSyncServerLogLevel({ TREECRDT_LOG_LEVEL: ' silent ' })).toBe('silent');
});

test('resolveSyncServerLogLevel rejects an unknown level', () => {
  expect(() => resolveSyncServerLogLevel({ TREECRDT_LOG_LEVEL: 'verbose' })).toThrow(
    /TREECRDT_LOG_LEVEL/,
  );
});

test('resolveSyncServerLogPretty follows production unless overridden', () => {
  expect(resolveSyncServerLogPretty({})).toBe(true);
  expect(resolveSyncServerLogPretty({ NODE_ENV: 'development' })).toBe(true);
  expect(resolveSyncServerLogPretty({ NODE_ENV: 'production' })).toBe(false);
  expect(resolveSyncServerLogPretty({ NODE_ENV: 'prod' })).toBe(false);
  expect(resolveSyncServerLogPretty({ NODE_ENV: 'production', TREECRDT_LOG_PRETTY: 'true' })).toBe(
    true,
  );
  expect(resolveSyncServerLogPretty({ NODE_ENV: 'development', TREECRDT_LOG_PRETTY: 'json' })).toBe(
    false,
  );
  expect(() => resolveSyncServerLogPretty({ TREECRDT_LOG_PRETTY: 'maybe' })).toThrow(
    /TREECRDT_LOG_PRETTY/,
  );
});

test('createSyncServerLogger drops lines below the configured level', () => {
  const { lines, log } = captureLogger('warn');

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

test('logger sink failures do not escape', () => {
  const destination = new Writable({
    write() {
      throw new Error('sink down');
    },
  });
  const log = createSyncServerLogger({ level: 'debug', destination });
  expect(() => log.error('status check failed')).not.toThrow();
});

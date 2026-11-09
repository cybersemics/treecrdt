import { expect, test } from 'vitest';

import { resolveSyncServerLogLevel, resolveSyncServerLogPretty } from '../dist/index.js';

const levelCases: Array<[Record<string, string | undefined>, string]> = [
  [{}, 'info'],
  [{ NODE_ENV: 'production' }, 'info'],
  [{ NODE_ENV: 'development' }, 'info'],
  [{ NODE_ENV: 'dev' }, 'info'],
  [{ TREECRDT_LOG_LEVEL: 'debug' }, 'debug'],
  [{ NODE_ENV: 'production', TREECRDT_LOG_LEVEL: 'error' }, 'error'],
  [{ TREECRDT_LOG_LEVEL: ' silent ' }, 'silent'],
];

test.each(levelCases)('resolveSyncServerLogLevel(%j) is %s', (env, level) => {
  expect(resolveSyncServerLogLevel(env)).toBe(level);
});

test('resolveSyncServerLogLevel rejects an unknown level', () => {
  expect(() => resolveSyncServerLogLevel({ TREECRDT_LOG_LEVEL: 'verbose' })).toThrow(
    /TREECRDT_LOG_LEVEL/,
  );
});

const prettyCases: Array<[Record<string, string | undefined>, boolean]> = [
  [{}, false],
  [{ NODE_ENV: 'development' }, false],
  [{ NODE_ENV: 'production' }, false],
  [{ TREECRDT_LOG_PRETTY: 'true' }, true],
  [{ NODE_ENV: 'production', TREECRDT_LOG_PRETTY: '1' }, true],
  [{ TREECRDT_LOG_PRETTY: 'json' }, false],
  [{ TREECRDT_LOG_PRETTY: 'false' }, false],
];

test.each(prettyCases)('resolveSyncServerLogPretty(%j) is %s', (env, pretty) => {
  expect(resolveSyncServerLogPretty(env)).toBe(pretty);
});

test('resolveSyncServerLogPretty rejects an unknown value', () => {
  expect(() => resolveSyncServerLogPretty({ TREECRDT_LOG_PRETTY: 'maybe' })).toThrow(
    /TREECRDT_LOG_PRETTY/,
  );
});

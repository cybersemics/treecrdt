import { expect, test } from '@playwright/test';

import {
  areCurrentConnections,
  deleteCurrentConnection,
  isCurrentConnection,
  isCurrentSyncGeneration,
  runConnectionCleanup,
} from '../src/playground/syncHelpers.js';

test('connection decisions require the exact current object', () => {
  const stale = {};
  const replacement = {};
  const connections = new Map([['peer-a', replacement]]);

  expect(isCurrentConnection(connections, 'peer-a', stale)).toBe(false);
  expect(deleteCurrentConnection(connections, 'peer-a', stale)).toBe(false);
  expect(connections.get('peer-a')).toBe(replacement);
  expect(areCurrentConnections(connections, new Map([['peer-a', stale]]))).toBe(false);

  expect(isCurrentConnection(connections, 'peer-a', replacement)).toBe(true);
  expect(deleteCurrentConnection(connections, 'peer-a', replacement)).toBe(true);
  expect(connections.has('peer-a')).toBe(false);
});

test('generation decisions reject obsolete work', () => {
  expect(isCurrentSyncGeneration(7, 7)).toBe(true);
  expect(isCurrentSyncGeneration(8, 7)).toBe(false);
  expect(isCurrentSyncGeneration(null, 7)).toBe(false);
});

test('connection cleanup preserves layer order and runs every layer after an error', () => {
  const calls: string[] = [];

  expect(() =>
    runConnectionCleanup({
      deleteCurrent: () => {
        calls.push('delete');
        throw new Error('delete failed');
      },
      unregisterInbound: () => calls.push('inbound'),
      unsetOutbound: () => calls.push('outbound'),
      detachPeer: () => calls.push('peer'),
    }),
  ).toThrow('delete failed');
  expect(calls).toEqual(['delete', 'inbound', 'outbound', 'peer']);
});

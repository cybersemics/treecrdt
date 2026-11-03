import { expect, test } from 'vitest';
import createTreecrdtSession from '../src/session.js';
import { createTreecrdtDb } from '@treecrdt/interface/sqlite';
import type { Database } from '../src/types.js';

function createFakeDatabase(): Database {
  const meta = new Map<string, string>();

  return {
    db: createTreecrdtDb({
      query: async (sql, params = []) => {
        const normalized = sql.replace(/\s+/g, ' ').toLowerCase();
        if (normalized.startsWith('insert into "meta"') || normalized.startsWith('insert into meta')) {
          meta.set(String(params[0]), String(params[1]));
          return [];
        }
        if (normalized.includes('from "meta"') || normalized.includes('from meta')) {
          const key = params[0] !== undefined ? String(params[0]) : undefined;
          if (key !== undefined) {
            const value = meta.get(key);
            return value === undefined ? [] : [{ key, value }];
          }
          return [...meta.entries()].map(([k, value]) => ({ key: k, value }));
        }
        return [];
      },
    }),
    execBatch: async () => undefined,
    close: async () => undefined,
  };
}

test('createTreecrdtDb over session runs typed queries through sqlQuery', async () => {
  const owner = createTreecrdtSession(async () => ({
    db: createFakeDatabase(),
    filename: 'mem',
    storage: 'memory',
  }));
  await owner.open({ storage: 'memory', docId: 'kysely-session' });

  const db = createTreecrdtDb({
    query: (sql, params) => owner.session.sqlQuery(sql, params as any),
  });

  await db.insertInto('meta').values({ key: 'a', value: '1' }).execute();
  const row = await db
    .selectFrom('meta')
    .select('value')
    .where('key', '=', 'a')
    .executeTakeFirst();
  expect(row?.value).toBe('1');
});

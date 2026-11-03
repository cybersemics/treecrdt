import { expect, test } from 'vitest';
import {
  createTreecrdtClient,
  defaultExtensionPath,
  loadTreecrdtExtension,
} from '../dist/index.js';

async function openClient(docId: string) {
  const { default: Database } = await import('better-sqlite3');
  const raw = new Database(':memory:');
  loadTreecrdtExtension(raw, { extensionPath: defaultExtensionPath() });
  return await createTreecrdtClient(raw, { docId });
}

test('client.db supports typed meta CRUD', async () => {
  const client = await openClient('kysely-meta-crud');
  try {
    await client.db.insertInto('meta').values({ key: 'k', value: 'v1' }).execute();
    await expect(
      client.db.selectFrom('meta').selectAll().where('key', '=', 'k').executeTakeFirst(),
    ).resolves.toEqual({ key: 'k', value: 'v1' });

    await client.db.updateTable('meta').set({ value: 'v2' }).where('key', '=', 'k').execute();
    await expect(
      client.db.selectFrom('meta').select('value').where('key', '=', 'k').executeTakeFirst(),
    ).resolves.toEqual({ value: 'v2' });

    await client.db.deleteFrom('meta').where('key', '=', 'k').execute();
    await expect(
      client.db.selectFrom('meta').selectAll().where('key', '=', 'k').executeTakeFirst(),
    ).resolves.toBeUndefined();
  } finally {
    await client.close();
  }
});

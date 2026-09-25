import { expect, test } from '@playwright/test';

test('loads the published browser entry and WASM asset from a non-root base URL', async ({
  page,
}) => {
  const errors: string[] = [];
  page.on('pageerror', (error) => errors.push(error.message));
  await page.goto('./');
  await page.waitForFunction(() => typeof window.verifyMemoryClient === 'function');
  expect(await page.evaluate(() => window.verifyMemoryClient())).toEqual({
    value: 0,
    synchronous: true,
    notifications: 1,
    payload: [1, 2, 3],
    children: ['1'.repeat(32)],
    opCount: 1,
    syncRefSize: 16,
    syncedKind: 'insert',
  });
  expect(errors).toEqual([]);
});

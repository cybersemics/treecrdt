import { test, expect } from '@playwright/test';

/**
 * Closed client e2e: verifies that calls on a closed/dropped client reject with
 * "TreecrdtClient was closed" instead of hanging, and that close/drop are idempotent.
 *
 * Run locally:
 *   cd packages/treecrdt-wa-sqlite/e2e
 *   pnpm run test:e2e -- tests/closed-client.spec.ts
 */
test('closed client rejects further calls and close/drop are idempotent', async ({
  page,
}, testInfo) => {
  if (testInfo.project.name !== 'chromium-dev') test.skip();
  test.setTimeout(180_000);
  page.on('console', (msg) => console.log(`[page][${msg.type()}] ${msg.text()}`));

  await page.goto('/');
  await page.waitForFunction(() => typeof (window as any).__runClosedClientE2E === 'function');
  const result = await page.evaluate(async () => {
    const runner = (window as any).__runClosedClientE2E;
    if (!runner) throw new Error('__runClosedClientE2E not available');
    return await runner();
  });

  expect(result).toEqual({ ok: true });
});

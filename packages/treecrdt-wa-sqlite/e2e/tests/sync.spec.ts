import { test, expect } from '@playwright/test';

test('sync v0 e2e', async ({ page }) => {
  test.setTimeout(90_000);
  page.on('console', (msg) => console.log(`[page][${msg.type()}] ${msg.text()}`));

  await page.goto('/');
  await page.waitForFunction(() => typeof window.runTreecrdtSyncE2E === 'function');
  const result = await page.evaluate(async () => {
    const runner = window.runTreecrdtSyncE2E;
    if (!runner) throw new Error('runTreecrdtSyncE2E not available');
    return await runner();
  });

  expect(result).toEqual({ ok: true });
});

test('appendMany emits materialization event e2e', async ({ page }) => {
  test.setTimeout(90_000);
  await page.goto('/');
  await page.waitForFunction(
    () => typeof window.runTreecrdtMaterializationEventE2E === 'function',
  );

  const result = await page.evaluate(async () => {
    const runner = window.runTreecrdtMaterializationEventE2E;
    if (!runner) throw new Error('runTreecrdtMaterializationEventE2E not available');
    return await runner();
  });

  expect(result.ok).toBe(true);
  expect(result.eventIds.length).toBeGreaterThanOrEqual(3);
  expect(new Set(result.eventIds).size).toBe(result.eventIds.length);
  expect(result.eventIds).toEqual([...result.eventIds].sort());
  expect(result.children.length).toBe(1);
});

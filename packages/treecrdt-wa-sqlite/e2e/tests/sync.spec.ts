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
  await page.waitForFunction(() => typeof window.runTreecrdtMaterializationEventE2E === 'function');

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

test('auth-aware local writes roll back and defer materialization events e2e', async ({ page }) => {
  test.setTimeout(90_000);
  await page.goto('/');
  await page.waitForFunction(() => typeof window.runTreecrdtAuthLocalWriteE2E === 'function');

  const result = await page.evaluate(async () => {
    const runner = window.runTreecrdtAuthLocalWriteE2E;
    if (!runner) throw new Error('runTreecrdtAuthLocalWriteE2E not available');
    return await runner();
  });

  expect(result.ok).toBe(true);
  for (const mode of [result.direct, result.worker]) {
    expect(mode.rollback.exists).toBe(false);
    expect(mode.rollback.eventCount).toBe(0);
    expect(mode.rollback.opCount).toBe(0);
    expect(mode.success.exists).toBe(true);
    expect(mode.success.eventCount).toBe(1);
    expect(mode.success.opCount).toBe(1);
    expect(mode.success.authorizedBeforeEvent).toBe(true);
  }
});

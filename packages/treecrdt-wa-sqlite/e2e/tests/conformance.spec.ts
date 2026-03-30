import { test, expect } from '@playwright/test';

const cases = [
  { storage: 'memory' as const, timeoutMs: 120_000 },
  { storage: 'opfs' as const, timeoutMs: 180_000 },
] as const;

for (const c of cases) {
  test(`engine conformance (shared suite): ${c.storage}`, async ({ page }) => {
    test.setTimeout(c.timeoutMs);

    await page.goto('/');
    await page.waitForFunction(() => typeof window.runTreecrdtEngineConformanceE2E === 'function');
    const result = await page.evaluate(async (storage) => {
      const runner = window.runTreecrdtEngineConformanceE2E;
      if (!runner) throw new Error('runTreecrdtEngineConformanceE2E not available');
      return await runner(storage);
    }, c.storage);

    expect(result).toEqual({ ok: true });
  });
}

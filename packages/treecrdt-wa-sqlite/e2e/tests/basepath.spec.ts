import { test, expect } from '@playwright/test';

test.describe.serial('non-root base path', () => {
  test.setTimeout(180_000);

  test('memory client loads from base path (dev)', async ({ page }, testInfo) => {
    if (testInfo.project.name !== 'chromium-dev') test.skip();
    await page.goto('/base-path/');
    const summary = await page.evaluate(async () => {
      const fn = (window as any).__createTreecrdtClient;
      if (!fn) return null;
      const base = new URL('/base-path/', window.location.href).href;
      return await fn('memory', base);
    });
    expect(summary).not.toBeNull();
    expect(summary.mode).toBe('direct');
    expect(summary.storage).toBe('memory');
  });

  test('opfs client uses worker mode in preview build with base path', async ({
    page,
  }, testInfo) => {
    if (testInfo.project.name !== 'chromium-basepath-preview') test.skip();
    await page.goto('/base-path/');
    await page.waitForSelector('[data-testid="run-demo"]', { timeout: 30_000 });
    const summary = await page.evaluate(async () => {
      const fn = (window as any).__createTreecrdtClient;
      if (!fn) return null;
      return await fn('opfs');
    });
    expect(summary).not.toBeNull();
    expect(summary.mode).toBe('worker');
    expect(summary.storage).toBe('opfs');
  });

  test("opfs init fails when OPFS VFS chunk can't load (and throws)", async ({
    page,
  }, testInfo) => {
    if (testInfo.project.name !== 'chromium-basepath-preview') test.skip();
    await page.route('**/OPFSCoopSyncVFS*.js', (route) => route.abort());
    await page.goto('/base-path/');
    await page.waitForSelector('[data-testid="run-demo"]', { timeout: 30_000 });
    const result = await page.evaluate(async () => {
      const fn = (window as any).__createTreecrdtClient;
      if (!fn) return { ok: false, message: '__createTreecrdtClient missing' };
      try {
        await fn('opfs');
        return { ok: true, message: '' };
      } catch (err) {
        return { ok: false, message: err instanceof Error ? err.message : String(err) };
      }
    });
    expect(result.ok).toBe(false);
    expect(result.message).toContain('OPFS requested but could not be initialized');
  });
});

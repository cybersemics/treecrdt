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

  test('opfs client uses the synchronous build in a dedicated worker with a base path', async ({
    page,
  }, testInfo) => {
    if (testInfo.project.name !== 'chromium-basepath-preview') test.skip();
    const wasmRequests: string[] = [];
    const moduleRequests: string[] = [];
    page.on('request', (request) => {
      const url = new URL(request.url());
      if (url.pathname.endsWith('.wasm')) wasmRequests.push(url.pathname);
      if (url.pathname.endsWith('.mjs')) moduleRequests.push(url.pathname);
    });
    await page.goto('/base-path/');
    await page.waitForSelector('[data-testid="run-demo"]', { timeout: 30_000 });
    const summary = await page.evaluate(async () => {
      const fn = (window as any).__createTreecrdtClient;
      if (!fn) return null;
      return await fn('opfs', undefined, 'dedicated-worker');
    });
    expect(summary).not.toBeNull();
    expect(summary.mode).toBe('worker');
    expect(summary.runtime).toBe('dedicated-worker');
    expect(summary.storage).toBe('opfs');
    expect(moduleRequests).toContain('/base-path/wa-sqlite/wa-sqlite.mjs');
    expect(wasmRequests).toContainEqual(
      expect.stringMatching(/^\/base-path\/assets\/wa-sqlite-[A-Za-z0-9_-]+\.wasm$/),
    );
    expect(wasmRequests.some((path) => path.includes('/wa-sqlite-async-'))).toBe(false);
  });

  test('direct opfs uses the Asyncify build in preview with base path', async ({
    page,
  }, testInfo) => {
    if (testInfo.project.name !== 'chromium-basepath-preview') test.skip();
    const wasmRequests: string[] = [];
    const moduleRequests: string[] = [];
    page.on('request', (request) => {
      const url = new URL(request.url());
      if (url.pathname.endsWith('.wasm')) wasmRequests.push(url.pathname);
      if (url.pathname.endsWith('.mjs')) moduleRequests.push(url.pathname);
    });
    await page.goto('/base-path/');
    await page.waitForSelector('[data-testid="run-demo"]', { timeout: 30_000 });
    const summary = await page.evaluate(async () => {
      const fn = (window as any).__createTreecrdtClient;
      if (!fn) return null;
      return await fn('opfs', undefined, 'direct');
    });
    expect(summary).not.toBeNull();
    expect(summary.mode).toBe('direct');
    expect(summary.runtime).toBe('direct');
    expect(summary.storage).toBe('opfs');
    expect(moduleRequests).toContain('/base-path/wa-sqlite/wa-sqlite-async.mjs');
    expect(wasmRequests).toContainEqual(
      expect.stringMatching(/^\/base-path\/assets\/wa-sqlite-async-[A-Za-z0-9_-]+\.wasm$/),
    );
  });

  test("opfs init fails when OPFS VFS chunk can't load (and throws)", async ({
    context,
    page,
  }, testInfo) => {
    if (testInfo.project.name !== 'chromium-basepath-preview') test.skip();
    await context.route('**/OPFS*VFS*.js', (route) => route.abort());
    await page.goto('/base-path/');
    await page.waitForSelector('[data-testid="run-demo"]', { timeout: 30_000 });
    const result = await page.evaluate(async () => {
      const fn = (window as any).__createTreecrdtClient;
      if (!fn) return { ok: false, message: '__createTreecrdtClient missing' };
      try {
        await fn('opfs', undefined, 'dedicated-worker');
        return { ok: true, message: '' };
      } catch (err) {
        return { ok: false, message: err instanceof Error ? err.message : String(err) };
      }
    });
    expect(result.ok).toBe(false);
    expect(result.message).toContain('OPFS requested but could not be initialized');
  });
});

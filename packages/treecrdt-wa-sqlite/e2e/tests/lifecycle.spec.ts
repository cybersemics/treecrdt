import { test, expect, type BrowserContext, type Page } from '@playwright/test';

type LifecycleHarness = NonNullable<Window['__treecrdtLifecycle']>;
type LifecycleOptions = Parameters<LifecycleHarness['drop']>[0];

const reloadCases: Array<{
  name: string;
  closeBeforeReload: boolean;
  crossOriginIsolated: boolean;
}> = [
  { name: 'after explicit close', closeBeforeReload: true, crossOriginIsolated: true },
  { name: 'without explicit close', closeBeforeReload: false, crossOriginIsolated: true },
  { name: 'without cross-origin isolation', closeBeforeReload: false, crossOriginIsolated: false },
];

/** The dev server sends COOP/COEP; strip them to load the app without cross-origin isolation. */
async function serveWithoutCrossOriginIsolation(context: BrowserContext) {
  await context.route('**/*', async (route) => {
    const response = await route.fetch();
    const headers = response.headers();
    delete headers['cross-origin-opener-policy'];
    delete headers['cross-origin-embedder-policy'];
    await route.fulfill({ response, headers });
  });
}

async function waitForHarness(page: Page) {
  await page.goto('/');
  await page.waitForFunction(
    () =>
      typeof window.__treecrdtLifecycle?.support === 'function' &&
      typeof window.__treecrdtLifecycle?.drop === 'function' &&
      typeof window.__treecrdtLifecycle?.write === 'function' &&
      typeof window.__treecrdtLifecycle?.read === 'function',
  );
}

async function support(page: Page): Promise<ReturnType<LifecycleHarness['support']>> {
  return page.evaluate(() => {
    const harness = window.__treecrdtLifecycle;
    if (!harness) throw new Error('__treecrdtLifecycle not available');
    return harness.support();
  });
}

async function drop(page: Page, opts: LifecycleOptions) {
  await page.evaluate(async (dropOpts) => {
    const harness = window.__treecrdtLifecycle;
    if (!harness) throw new Error('__treecrdtLifecycle not available');
    await harness.drop(dropOpts);
  }, opts);
}

async function write(page: Page, opts: LifecycleOptions & { closeBeforeReload?: boolean }) {
  return page.evaluate(async (writeOpts) => {
    const harness = window.__treecrdtLifecycle;
    if (!harness) throw new Error('__treecrdtLifecycle not available');
    return await harness.write(writeOpts);
  }, opts);
}

async function read(page: Page, opts: LifecycleOptions) {
  return page.evaluate(async (readOpts) => {
    const harness = window.__treecrdtLifecycle;
    if (!harness) throw new Error('__treecrdtLifecycle not available');
    return await harness.read(readOpts);
  }, opts);
}

/** This harness opens persistent clients without cross-tab mode. */
function expectLifecycleTree(state: Awaited<ReturnType<typeof read>>) {
  expect(state).toMatchObject({
    mode: 'worker',
    runtime: 'dedicated-worker',
    storage: 'opfs',
    headLamport: 2,
    parentExists: true,
    childExists: true,
    childParent: expect.any(String),
    parentPayload: 'browser lifecycle parent',
    childPayload: 'browser lifecycle child',
  });
  expect(state.rootChildren).toEqual([state.parentId]);
  expect(state.parentChildren).toEqual([state.childId]);
  expect(state.childParent).toBe(state.parentId);
}

test.describe('browser OPFS lifecycle', () => {
  for (const reloadCase of reloadCases) {
    test(`reopens the persistent store after browser reload ${reloadCase.name}`, async ({
      context,
      page,
    }, testInfo) => {
      if (testInfo.project.name !== 'chromium-dev') test.skip();
      test.setTimeout(120_000);
      page.on('console', (msg) => console.log(`[page][${msg.type()}] ${msg.text()}`));
      if (!reloadCase.crossOriginIsolated) await serveWithoutCrossOriginIsolation(context);

      const suffix = `${Date.now().toString(36)}-${Math.random().toString(16).slice(2, 8)}`;
      const opts = { docId: `lifecycle-${suffix}` };

      await waitForHarness(page);
      expect(await page.evaluate(() => window.crossOriginIsolated)).toBe(
        reloadCase.crossOriginIsolated,
      );
      expect(await support(page)).toEqual({ available: true });

      try {
        expectLifecycleTree(
          await write(page, { ...opts, closeBeforeReload: reloadCase.closeBeforeReload }),
        );

        await page.reload({ waitUntil: 'load' });
        await waitForHarness(page);

        expectLifecycleTree(await read(page, opts));
      } finally {
        await drop(page, opts).catch(() => {});
      }
    });
  }
});

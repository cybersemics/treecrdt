import { test, expect, type Page } from '@playwright/test';

type LifecycleHarness = NonNullable<Window['__treecrdtLifecycle']>;
type LifecycleOptions = Parameters<LifecycleHarness['drop']>[0];

const reloadCases: Array<{
  name: string;
  closeBeforeReload: boolean;
}> = [
  { name: 'after explicit close', closeBeforeReload: true },
  { name: 'without explicit close', closeBeforeReload: false },
];

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
      page,
    }, testInfo) => {
      if (testInfo.project.name !== 'chromium-dev') test.skip();
      test.setTimeout(120_000);
      page.on('console', (msg) => console.log(`[page][${msg.type()}] ${msg.text()}`));

      const suffix = `${Date.now().toString(36)}-${Math.random().toString(16).slice(2, 8)}`;
      const opts = { docId: `lifecycle-${suffix}` };

      await waitForHarness(page);
      const opfsSupport = await support(page);
      if (!opfsSupport.available) test.skip(true, `OPFS unavailable: ${opfsSupport.reason}`);

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

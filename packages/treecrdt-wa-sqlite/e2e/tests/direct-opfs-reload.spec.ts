import { test, expect, type Page } from '@playwright/test';

type DirectOpfsHarness = NonNullable<Window['__treecrdtDirectOpfsReload']>;

async function waitForHarness(page: Page) {
  await page.goto('/');
  await page.waitForFunction(
    () =>
      typeof window.__treecrdtDirectOpfsReload?.support === 'function' &&
      typeof window.__treecrdtDirectOpfsReload?.drop === 'function' &&
      typeof window.__treecrdtDirectOpfsReload?.write === 'function' &&
      typeof window.__treecrdtDirectOpfsReload?.read === 'function',
  );
}

async function support(page: Page): Promise<ReturnType<DirectOpfsHarness['support']>> {
  return page.evaluate(() => {
    const harness = window.__treecrdtDirectOpfsReload;
    if (!harness) throw new Error('__treecrdtDirectOpfsReload not available');
    return harness.support();
  });
}

async function drop(page: Page, opts: { docId: string; filename: string }) {
  await page.evaluate(async (dropOpts) => {
    const harness = window.__treecrdtDirectOpfsReload;
    if (!harness) throw new Error('__treecrdtDirectOpfsReload not available');
    await harness.drop(dropOpts);
  }, opts);
}

async function write(
  page: Page,
  opts: { docId: string; filename: string; closeBeforeReload?: boolean },
) {
  return page.evaluate(async (writeOpts) => {
    const harness = window.__treecrdtDirectOpfsReload;
    if (!harness) throw new Error('__treecrdtDirectOpfsReload not available');
    return await harness.write(writeOpts);
  }, opts);
}

async function read(page: Page, opts: { docId: string; filename: string }) {
  return page.evaluate(async (readOpts) => {
    const harness = window.__treecrdtDirectOpfsReload;
    if (!harness) throw new Error('__treecrdtDirectOpfsReload not available');
    return await harness.read(readOpts);
  }, opts);
}

function expectReloadedTree(state: Awaited<ReturnType<typeof read>>) {
  expect(state).toMatchObject({
    mode: 'direct',
    runtime: 'direct',
    storage: 'opfs',
    headLamport: 2,
    parentExists: true,
    childExists: true,
    childParent: expect.any(String),
    parentPayload: 'direct opfs parent',
    childPayload: 'direct opfs child',
  });
  expect(state.rootChildren).toEqual([state.parentId]);
  expect(state.parentChildren).toEqual([state.childId]);
  expect(state.childParent).toBe(state.parentId);
}

test.describe('direct OPFS reload lifecycle', () => {
  test('reopens a direct OPFS store after explicit close and browser reload', async ({
    page,
  }, testInfo) => {
    if (testInfo.project.name !== 'chromium-dev') test.skip();
    test.setTimeout(120_000);
    page.on('console', (msg) => console.log(`[page][${msg.type()}] ${msg.text()}`));

    const suffix = `${Date.now().toString(36)}-${Math.random().toString(16).slice(2, 8)}`;
    const opts = {
      docId: `direct-opfs-reload-close-${suffix}`,
      filename: `/direct-opfs-reload-close-${suffix}.db`,
    };

    await waitForHarness(page);
    const opfsSupport = await support(page);
    if (!opfsSupport.available) test.skip(true, `OPFS unavailable: ${opfsSupport.reason}`);

    try {
      await drop(page, opts);
      const initialState = await write(page, { ...opts, closeBeforeReload: true });
      expectReloadedTree(initialState);

      await page.reload({ waitUntil: 'load' });
      await waitForHarness(page);

      expectReloadedTree(await read(page, opts));
    } finally {
      await drop(page, opts).catch(() => {});
    }
  });

  test('reopens a direct OPFS store after browser reload without explicit close', async ({
    page,
  }, testInfo) => {
    if (testInfo.project.name !== 'chromium-dev') test.skip();
    test.setTimeout(120_000);
    page.on('console', (msg) => console.log(`[page][${msg.type()}] ${msg.text()}`));

    const suffix = `${Date.now().toString(36)}-${Math.random().toString(16).slice(2, 8)}`;
    const opts = {
      docId: `direct-opfs-reload-implicit-${suffix}`,
      filename: `/direct-opfs-reload-implicit-${suffix}.db`,
    };

    await waitForHarness(page);
    const opfsSupport = await support(page);
    if (!opfsSupport.available) test.skip(true, `OPFS unavailable: ${opfsSupport.reason}`);

    try {
      await drop(page, opts);
      const initialState = await write(page, { ...opts, closeBeforeReload: false });
      expectReloadedTree(initialState);

      await page.reload({ waitUntil: 'load' });
      await waitForHarness(page);

      expectReloadedTree(await read(page, opts));
    } finally {
      await drop(page, opts).catch(() => {});
    }
  });
});

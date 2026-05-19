import { test, expect, type Page } from '@playwright/test';

type LifecycleHarness = NonNullable<Window['__treecrdtLifecycle']>;
type LifecycleRuntime = 'direct' | 'dedicated-worker';

const scenarios: Array<{
  runtime: LifecycleRuntime;
  expectedMode: 'direct' | 'worker';
}> = [
  { runtime: 'direct', expectedMode: 'direct' },
  { runtime: 'dedicated-worker', expectedMode: 'worker' },
];

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

async function drop(
  page: Page,
  opts: { docId: string; filename: string; runtime: LifecycleRuntime },
) {
  await page.evaluate(async (dropOpts) => {
    const harness = window.__treecrdtLifecycle;
    if (!harness) throw new Error('__treecrdtLifecycle not available');
    await harness.drop(dropOpts);
  }, opts);
}

async function write(
  page: Page,
  opts: {
    docId: string;
    filename: string;
    runtime: LifecycleRuntime;
    closeBeforeReload?: boolean;
  },
) {
  return page.evaluate(async (writeOpts) => {
    const harness = window.__treecrdtLifecycle;
    if (!harness) throw new Error('__treecrdtLifecycle not available');
    return await harness.write(writeOpts);
  }, opts);
}

async function read(
  page: Page,
  opts: { docId: string; filename: string; runtime: LifecycleRuntime },
) {
  return page.evaluate(async (readOpts) => {
    const harness = window.__treecrdtLifecycle;
    if (!harness) throw new Error('__treecrdtLifecycle not available');
    return await harness.read(readOpts);
  }, opts);
}

function expectReloadedTree(
  state: Awaited<ReturnType<typeof read>>,
  expected: { mode: 'direct' | 'worker'; runtime: LifecycleRuntime },
) {
  expect(state).toMatchObject({
    mode: expected.mode,
    runtime: expected.runtime,
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
  for (const scenario of scenarios) {
    for (const reloadCase of reloadCases) {
      test(`reopens ${scenario.runtime} OPFS store after browser reload ${reloadCase.name}`, async ({
        page,
      }, testInfo) => {
        if (testInfo.project.name !== 'chromium-dev') test.skip();
        test.setTimeout(120_000);
        page.on('console', (msg) => console.log(`[page][${msg.type()}] ${msg.text()}`));

        const suffix = `${Date.now().toString(36)}-${Math.random().toString(16).slice(2, 8)}`;
        const opts = {
          docId: `lifecycle-${scenario.runtime}-${suffix}`,
          filename: `/lifecycle-${scenario.runtime}-${suffix}.db`,
          runtime: scenario.runtime,
        };

        await waitForHarness(page);
        const opfsSupport = await support(page);
        if (!opfsSupport.available) test.skip(true, `OPFS unavailable: ${opfsSupport.reason}`);

        try {
          await drop(page, opts);
          const initialState = await write(page, {
            ...opts,
            closeBeforeReload: reloadCase.closeBeforeReload,
          });
          expectReloadedTree(initialState, {
            mode: scenario.expectedMode,
            runtime: scenario.runtime,
          });

          await page.reload({ waitUntil: 'load' });
          await waitForHarness(page);

          expectReloadedTree(await read(page, opts), {
            mode: scenario.expectedMode,
            runtime: scenario.runtime,
          });
        } finally {
          await drop(page, opts).catch(() => {});
        }
      });
    }
  }
});

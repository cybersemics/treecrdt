import { expect, test, type Page } from '@playwright/test';

type RuntimeHarness = NonNullable<Window['__treecrdtRuntimeConformance']>;
type RunOptions = Parameters<RuntimeHarness['run']>[0];
type Runtime = RunOptions['runtime'];

const runtimes: Runtime[] = ['direct', 'dedicated-worker', 'shared-worker'];

async function waitForHarness(page: Page) {
  await page.goto('/');
  await page.waitForFunction(
    () =>
      typeof window.__treecrdtRuntimeConformance?.support === 'function' &&
      typeof window.__treecrdtRuntimeConformance?.run === 'function',
  );
}

async function support(page: Page): Promise<ReturnType<RuntimeHarness['support']>> {
  return page.evaluate(() => {
    const harness = window.__treecrdtRuntimeConformance;
    if (!harness) throw new Error('__treecrdtRuntimeConformance not available');
    return harness.support();
  });
}

async function run(page: Page, opts: RunOptions) {
  return page.evaluate(async (runOptions) => {
    const harness = window.__treecrdtRuntimeConformance;
    if (!harness) throw new Error('__treecrdtRuntimeConformance not available');
    return harness.run(runOptions);
  }, opts);
}

test.describe('wa-sqlite runtime lifecycle conformance', () => {
  for (const runtime of runtimes) {
    test(`${runtime}: teardown is terminal and the runtime is reusable`, async ({
      page,
    }, testInfo) => {
      if (testInfo.project.name !== 'chromium-dev') test.skip();
      test.setTimeout(120_000);
      page.on('console', (message) => console.log(`[page][${message.type()}] ${message.text()}`));

      await waitForHarness(page);
      await expect(run(page, { runtime, scenario: 'terminal-teardown' })).resolves.toEqual({
        ok: true,
      });
    });

    test(`${runtime}: recovers from failed OPFS initialization`, async ({ page }, testInfo) => {
      if (testInfo.project.name !== 'chromium-dev') test.skip();
      test.setTimeout(120_000);
      page.on('console', (message) => console.log(`[page][${message.type()}] ${message.text()}`));

      await waitForHarness(page);
      const opfsSupport = await support(page);
      if (!opfsSupport.available) test.skip(true, `OPFS unavailable: ${opfsSupport.reason}`);

      await expect(run(page, { runtime, scenario: 'opfs-recovery' })).resolves.toEqual({
        ok: true,
      });
    });
  }
});

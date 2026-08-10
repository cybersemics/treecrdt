import { expect, test } from '@playwright/test';

test('dedicated worker opens a usable memory client after OPFS open fails', async ({
  page,
}, testInfo) => {
  if (testInfo.project.name !== 'chromium-dev') test.skip();
  test.setTimeout(120_000);

  await page.goto('/');
  await page.waitForFunction(() => typeof window.__treecrdtFallback?.run === 'function');

  const support = await page.evaluate(() => window.__treecrdtFallback!.support());
  if (!support.available) test.skip(true, `OPFS unavailable: ${support.reason}`);

  const result = await page.evaluate(() => window.__treecrdtFallback!.run());

  expect(result).toEqual({
    mode: 'worker',
    runtime: 'dedicated-worker',
    storage: 'memory',
    children: ['1'.repeat(32)],
    payload: 'memory fallback works',
  });
});

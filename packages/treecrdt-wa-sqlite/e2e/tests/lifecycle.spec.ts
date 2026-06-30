import { test, expect, type Page } from '@playwright/test';

type LifecycleHarness = NonNullable<Window['__treecrdtLifecycle']>;
type LifecycleRuntime = 'direct' | 'dedicated-worker';
type LifecycleWriteMode = 'default' | 'single-owner-wal' | 'opfs-write-ahead';

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
  opts: {
    docId: string;
    filename: string;
    runtime: LifecycleRuntime;
    writeMode?: LifecycleWriteMode;
  },
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
    writeMode?: LifecycleWriteMode;
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
  opts: {
    docId: string;
    filename: string;
    runtime: LifecycleRuntime;
    writeMode?: LifecycleWriteMode;
  },
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

function isWriteAheadUnsupportedError(err: unknown): boolean {
  const message = err instanceof Error ? err.message : String(err);
  return message.toLowerCase().includes('readwrite-unsafe');
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

  test('opens dedicated-worker OPFS store in single-owner WAL mode', async ({ page }, testInfo) => {
    if (testInfo.project.name !== 'chromium-dev') test.skip();
    test.setTimeout(120_000);
    page.on('console', (msg) => console.log(`[page][${msg.type()}] ${msg.text()}`));

    const suffix = `${Date.now().toString(36)}-${Math.random().toString(16).slice(2, 8)}`;
    const opts = {
      docId: `lifecycle-wal-${suffix}`,
      filename: `/lifecycle-wal-${suffix}.db`,
      runtime: 'dedicated-worker' as const,
      writeMode: 'single-owner-wal' as const,
    };

    await waitForHarness(page);
    const opfsSupport = await support(page);
    if (!opfsSupport.available) test.skip(true, `OPFS unavailable: ${opfsSupport.reason}`);

    try {
      await drop(page, opts);
      const written = await write(page, { ...opts, closeBeforeReload: true });
      expectReloadedTree(written, {
        mode: 'worker',
        runtime: 'dedicated-worker',
      });
      expect(written.journalMode).toBe('wal');
      expect(written.lockingMode).toBe('exclusive');

      const reopened = await read(page, opts);
      expectReloadedTree(reopened, {
        mode: 'worker',
        runtime: 'dedicated-worker',
      });
      expect(reopened.journalMode).toBe('wal');
      expect(reopened.lockingMode).toBe('exclusive');
    } finally {
      await drop(page, opts).catch(() => {});
    }
  });

  test('opens dedicated-worker OPFS store in write-ahead VFS mode', async ({ page }, testInfo) => {
    if (testInfo.project.name !== 'chromium-dev') test.skip();
    test.setTimeout(120_000);
    page.on('console', (msg) => console.log(`[page][${msg.type()}] ${msg.text()}`));

    const suffix = `${Date.now().toString(36)}-${Math.random().toString(16).slice(2, 8)}`;
    const opts = {
      docId: `lifecycle-write-ahead-${suffix}`,
      filename: `/lifecycle-write-ahead-${suffix}.db`,
      runtime: 'dedicated-worker' as const,
      writeMode: 'opfs-write-ahead' as const,
    };

    await waitForHarness(page);
    const opfsSupport = await support(page);
    if (!opfsSupport.available) test.skip(true, `OPFS unavailable: ${opfsSupport.reason}`);

    try {
      await drop(page, opts);
      const written = await write(page, { ...opts, closeBeforeReload: true });
      expectReloadedTree(written, {
        mode: 'worker',
        runtime: 'dedicated-worker',
      });

      const reopened = await read(page, opts);
      expectReloadedTree(reopened, {
        mode: 'worker',
        runtime: 'dedicated-worker',
      });
    } catch (err) {
      if (isWriteAheadUnsupportedError(err)) {
        test.skip(
          true,
          `OPFSWriteAheadVFS unsupported: ${err instanceof Error ? err.message : err}`,
        );
      }
      throw err;
    } finally {
      await drop(page, opts).catch(() => {});
    }
  });

  test('opens the same write-ahead OPFS store from two dedicated-worker clients', async ({
    page,
  }, testInfo) => {
    if (testInfo.project.name !== 'chromium-dev') test.skip();
    test.setTimeout(120_000);
    page.on('console', (msg) => console.log(`[page][${msg.type()}] ${msg.text()}`));

    const suffix = `${Date.now().toString(36)}-${Math.random().toString(16).slice(2, 8)}`;
    const opts = {
      docId: `lifecycle-write-ahead-two-client-${suffix}`,
      filename: `/lifecycle-write-ahead-two-client-${suffix}.db`,
      runtime: 'dedicated-worker' as const,
      writeMode: 'opfs-write-ahead' as const,
    };

    await waitForHarness(page);
    const opfsSupport = await support(page);
    if (!opfsSupport.available) test.skip(true, `OPFS unavailable: ${opfsSupport.reason}`);

    try {
      await drop(page, opts);
      const result = await page.evaluate(async (twoClientOpts) => {
        const harness = window.__treecrdtLifecycle;
        if (!harness) throw new Error('__treecrdtLifecycle not available');
        return await harness.writeAheadTwoClient(twoClientOpts);
      }, opts);

      expect(result.stateFromBAfterAWrite).toMatchObject({
        mode: 'worker',
        runtime: 'dedicated-worker',
        storage: 'opfs',
        headLamport: 1,
        parentExists: true,
        childExists: false,
        parentPayload: 'browser lifecycle parent',
      });
      expect(result.stateFromBAfterAWrite.rootChildren).toEqual([
        result.stateFromBAfterAWrite.parentId,
      ]);

      expectReloadedTree(result.stateFromAAfterBWrite, {
        mode: 'worker',
        runtime: 'dedicated-worker',
      });
    } catch (err) {
      if (isWriteAheadUnsupportedError(err)) {
        test.skip(
          true,
          `OPFSWriteAheadVFS unsupported: ${err instanceof Error ? err.message : err}`,
        );
      }
      throw err;
    } finally {
      await drop(page, opts).catch(() => {});
    }
  });
});

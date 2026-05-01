import { test, expect, type Page } from '@playwright/test';

async function waitForCrossTabHarness(page: Page) {
  await page.goto('/');
  await page.waitForFunction(
    () =>
      typeof (window as any).__openSharedOpfsCrossTabClient === 'function' &&
      typeof (window as any).__mutateSharedOpfsCrossTabTree === 'function' &&
      typeof (window as any).__sharedOpfsCrossTabState === 'function',
  );
}

type RuntimeChoice = 'auto' | 'dedicated-worker' | 'shared-worker';

async function openClient(page: Page, docId: string, filename: string, runtime: RuntimeChoice) {
  return page.evaluate(
    async ({ docId, filename, runtime }) => {
      const open = (window as any).__openSharedOpfsCrossTabClient;
      if (!open) throw new Error('__openSharedOpfsCrossTabClient not available');
      return await open({ docId, filename, runtime });
    },
    { docId, filename, runtime },
  );
}

async function mutateTree(
  page: Page,
  opts: {
    replicaLabel: string;
    action: 'insert' | 'move' | 'payload' | 'delete';
    nodeInt: number;
    parent?: string;
    newParent?: string;
    payloadText?: string;
  },
) {
  return page.evaluate(async (mutation) => {
    const mutate = (window as any).__mutateSharedOpfsCrossTabTree;
    if (!mutate) throw new Error('__mutateSharedOpfsCrossTabTree not available');
    return await mutate(mutation);
  }, opts);
}

async function state(page: Page, opts: { parents?: string[]; nodes?: string[] } = {}) {
  return page.evaluate(async (stateOpts) => {
    const getState = (window as any).__sharedOpfsCrossTabState;
    if (!getState) throw new Error('__sharedOpfsCrossTabState not available');
    return await getState(stateOpts);
  }, opts);
}

async function closeClient(page: Page) {
  await page.evaluate(async () => {
    const close = (window as any).__closeSharedOpfsCrossTabClient;
    if (close) await close();
  });
}

const scenarios: Array<{
  name: string;
  filePrefix: string;
  runtime: RuntimeChoice;
  expectedRuntime: 'dedicated-worker' | 'shared-worker';
}> = [
  {
    name: 'auto dedicated-worker',
    filePrefix: 'dw',
    runtime: 'auto',
    expectedRuntime: 'dedicated-worker',
  },
  {
    name: 'explicit shared-worker',
    filePrefix: 'sw',
    runtime: 'shared-worker',
    expectedRuntime: 'shared-worker',
  },
];

for (const scenario of scenarios) {
  test(`shared OPFS clients propagate materialization events across tabs (${scenario.name})`, async ({
    context,
  }, testInfo) => {
    if (testInfo.project.name !== 'chromium-dev') test.skip();
    test.setTimeout(120_000);

    const suffix = `${Date.now().toString(36)}-${Math.random().toString(16).slice(2, 8)}`;
    const docId = `e2e-cross-tab-${scenario.filePrefix}-${suffix}`;
    const filename = `/e2e-ct-${scenario.filePrefix}-${suffix}.db`;
    const pageA = await context.newPage();
    const pageB = await context.newPage();
    pageA.on('console', (msg) => console.log(`[pageA][${msg.type()}] ${msg.text()}`));
    pageB.on('console', (msg) => console.log(`[pageB][${msg.type()}] ${msg.text()}`));

    try {
      await Promise.all([waitForCrossTabHarness(pageA), waitForCrossTabHarness(pageB)]);

      const [summaryA, summaryB] = await Promise.all([
        openClient(pageA, docId, filename, scenario.runtime),
        openClient(pageB, docId, filename, scenario.runtime),
      ]);
      expect(summaryA).toEqual({
        mode: 'worker',
        runtime: scenario.expectedRuntime,
        storage: 'opfs',
      });
      expect(summaryB).toEqual({
        mode: 'worker',
        runtime: scenario.expectedRuntime,
        storage: 'opfs',
      });

      const root = '0'.repeat(32);
      const parent = await mutateTree(pageA, {
        replicaLabel: 'cross-tab-a',
        action: 'insert',
        nodeInt: 701,
      });
      const child = await mutateTree(pageA, {
        replicaLabel: 'cross-tab-a',
        action: 'insert',
        nodeInt: 702,
        parent: parent.node,
        payloadText: 'child from A',
      });

      await expect
        .poll(async () => (await state(pageB, { parents: [parent.node] })).eventCount, {
          timeout: 15_000,
        })
        .toBeGreaterThanOrEqual(2);
      const treeBuiltOnB = await state(pageB, {
        parents: [parent.node],
        nodes: [parent.node, child.node],
      });
      expect(treeBuiltOnB.childrenByParent[root]).toContain(parent.node);
      expect(treeBuiltOnB.childrenByParent[parent.node]).toContain(child.node);
      expect(treeBuiltOnB.parentByNode[child.node]).toBe(parent.node);
      expect(treeBuiltOnB.payloadByNode[child.node]).toBe('child from A');
      expect(treeBuiltOnB.events.some((event) => event.nodes.includes(child.node))).toBe(true);

      const eventsBeforeBWriteOnA = (await state(pageA)).eventCount;
      await mutateTree(pageB, {
        replicaLabel: 'cross-tab-b',
        action: 'move',
        nodeInt: 702,
        newParent: root,
      });

      await expect
        .poll(async () => (await state(pageA)).eventCount, { timeout: 15_000 })
        .toBeGreaterThan(eventsBeforeBWriteOnA);
      const childMovedOnA = await state(pageA, { parents: [parent.node], nodes: [child.node] });
      expect(childMovedOnA.childrenByParent[root]).toContain(child.node);
      expect(childMovedOnA.childrenByParent[parent.node]).not.toContain(child.node);
      expect(childMovedOnA.parentByNode[child.node]).toBe(root);

      const eventsBeforePayloadOnB = (await state(pageB)).eventCount;
      await mutateTree(pageA, {
        replicaLabel: 'cross-tab-a',
        action: 'payload',
        nodeInt: 702,
        payloadText: 'renamed from A',
      });
      await expect
        .poll(async () => (await state(pageB)).eventCount, { timeout: 15_000 })
        .toBeGreaterThan(eventsBeforePayloadOnB);
      const payloadUpdatedOnB = await state(pageB, { nodes: [child.node] });
      expect(payloadUpdatedOnB.payloadByNode[child.node]).toBe('renamed from A');

      const eventsBeforeDeleteOnA = (await state(pageA)).eventCount;
      await mutateTree(pageB, {
        replicaLabel: 'cross-tab-b',
        action: 'delete',
        nodeInt: 701,
      });
      await expect
        .poll(async () => (await state(pageA)).eventCount, { timeout: 15_000 })
        .toBeGreaterThan(eventsBeforeDeleteOnA);
      const parentDeletedOnA = await state(pageA, { nodes: [parent.node, child.node] });
      expect(parentDeletedOnA.existsByNode[parent.node]).toBe(false);
      expect(parentDeletedOnA.existsByNode[child.node]).toBe(true);
      expect(parentDeletedOnA.childrenByParent[root]).not.toContain(parent.node);
      expect(parentDeletedOnA.childrenByParent[root]).toContain(child.node);
    } finally {
      await Promise.allSettled([closeClient(pageA), closeClient(pageB)]);
      await Promise.allSettled([pageA.close(), pageB.close()]);
    }
  });
}

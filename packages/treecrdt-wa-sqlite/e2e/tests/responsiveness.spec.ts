import { test, expect, type Page } from '@playwright/test';

type RuntimeChoice = 'dedicated-worker' | 'shared-worker';

type ReadMetrics = {
  ok: true;
  samples: number;
  durationsMs: number[];
  minMs: number;
  p50Ms: number;
  p95Ms: number;
  maxMs: number;
  finalChildCount: number;
};

type WriteMetrics = {
  ok: true;
  totalOps: number;
  batchCount: number;
  batchSize: number;
  durationMs: number;
  batchDurationsMs: number[];
};

const totalOps = Number(process.env.TREECRDT_RESPONSIVENESS_TOTAL_OPS ?? 5_000);
const batchSize = Number(process.env.TREECRDT_RESPONSIVENESS_BATCH_SIZE ?? 500);
const samples = Number(process.env.TREECRDT_RESPONSIVENESS_READ_SAMPLES ?? 20);
const intervalMs = Number(process.env.TREECRDT_RESPONSIVENESS_READ_INTERVAL_MS ?? 0);

async function waitForHarness(page: Page) {
  await page.goto('/');
  await page.waitForFunction(
    () =>
      typeof window.__openTreecrdtResponsivenessClient === 'function' &&
      typeof window.__startTreecrdtResponsivenessWriteBatches === 'function' &&
      typeof window.__sampleTreecrdtResponsivenessReads === 'function' &&
      typeof window.__waitTreecrdtResponsivenessWrites === 'function',
  );
}

async function openClient(
  page: Page,
  opts: { docId: string; filename: string; runtime: RuntimeChoice },
) {
  return page.evaluate(async (openOpts) => {
    const open = window.__openTreecrdtResponsivenessClient;
    if (!open) throw new Error('__openTreecrdtResponsivenessClient not available');
    return await open(openOpts);
  }, opts);
}

async function startWrites(page: Page, opts: { batchCount: number; batchSize: number }) {
  return page.evaluate(async (writeOpts) => {
    const start = window.__startTreecrdtResponsivenessWriteBatches;
    if (!start) throw new Error('__startTreecrdtResponsivenessWriteBatches not available');
    return await start(writeOpts);
  }, opts);
}

async function sampleReads(
  page: Page,
  opts: { samples?: number; intervalMs?: number } = {},
): Promise<ReadMetrics> {
  return page.evaluate(
    async ({ samples, intervalMs }) => {
      const sample = window.__sampleTreecrdtResponsivenessReads;
      if (!sample) throw new Error('__sampleTreecrdtResponsivenessReads not available');
      return await sample({ samples, intervalMs });
    },
    { samples: opts.samples ?? samples, intervalMs: opts.intervalMs ?? intervalMs },
  );
}

async function waitWrites(page: Page): Promise<WriteMetrics> {
  return page.evaluate(async () => {
    const wait = window.__waitTreecrdtResponsivenessWrites;
    if (!wait) throw new Error('__waitTreecrdtResponsivenessWrites not available');
    return await wait();
  });
}

async function closeClient(page: Page) {
  await page.evaluate(async () => {
    const close = window.__closeTreecrdtResponsivenessClient;
    if (close) await close();
  });
}

test.describe('worker read responsiveness under write pressure', () => {
  for (const runtime of ['dedicated-worker', 'shared-worker'] as const) {
    test(`OPFS ${runtime} reads complete while append pressure is active`, async ({
      context,
    }, testInfo) => {
      if (testInfo.project.name !== 'chromium-dev') test.skip();
      test.setTimeout(180_000);

      const suffix = `${Date.now().toString(36)}-${Math.random().toString(16).slice(2, 8)}`;
      const docId = `responsiveness-${runtime}-${suffix}`;
      const filename = `/responsiveness-${runtime}-${suffix}.db`;
      const batchCount = Math.ceil(totalOps / batchSize);
      const writerPage = await context.newPage();
      const readerPage = runtime === 'shared-worker' ? await context.newPage() : writerPage;

      writerPage.on('console', (msg) => console.log(`[writer][${msg.type()}] ${msg.text()}`));
      if (readerPage !== writerPage) {
        readerPage.on('console', (msg) => console.log(`[reader][${msg.type()}] ${msg.text()}`));
      }

      try {
        await waitForHarness(writerPage);
        if (readerPage !== writerPage) await waitForHarness(readerPage);

        const writerSummary = await openClient(writerPage, { docId, filename, runtime });
        const readerSummary =
          readerPage === writerPage
            ? writerSummary
            : await openClient(readerPage, { docId, filename, runtime });
        expect(writerSummary).toEqual({ mode: 'worker', runtime, storage: 'opfs' });
        expect(readerSummary).toEqual({ mode: 'worker', runtime, storage: 'opfs' });

        await startWrites(writerPage, { batchCount, batchSize });
        const [reads, writes] = await Promise.all([
          sampleReads(readerPage),
          waitWrites(writerPage),
        ]);
        const afterWrites = await sampleReads(readerPage, { samples: 1, intervalMs: 0 });

        const batchDurations = writes.batchDurationsMs.slice().sort((a, b) => a - b);
        const p95Batch =
          batchDurations[
            Math.min(batchDurations.length - 1, Math.ceil(batchDurations.length * 0.95) - 1)
          ] ?? 0;
        console.log(
          JSON.stringify({
            runtime,
            totalOps: writes.totalOps,
            writeDurationMs: writes.durationMs,
            writeBatchP95Ms: p95Batch,
            readP50Ms: reads.p50Ms,
            readP95Ms: reads.p95Ms,
            readMaxMs: reads.maxMs,
            readSamples: reads.durationsMs,
            finalChildCount: reads.finalChildCount,
            finalChildCountAfterWrites: afterWrites.finalChildCount,
          }),
        );

        expect(writes.totalOps).toBe(batchCount * batchSize);
        expect(reads.samples).toBe(samples);
        // This is a liveness guard, not a benchmark threshold. Stress runs can tighten it locally.
        expect(reads.maxMs).toBeLessThan(30_000);
        expect(afterWrites.finalChildCount).toBe(writes.totalOps);
      } finally {
        await Promise.allSettled([closeClient(writerPage), closeClient(readerPage)]);
        await Promise.allSettled([writerPage.close(), readerPage.close()]);
      }
    });
  }
});

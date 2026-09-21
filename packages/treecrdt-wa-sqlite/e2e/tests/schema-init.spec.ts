import { expect, test } from '@playwright/test';

for (const kind of ['coop-sync', 'any-context'] as const) {
  test(`schema initialization is deferred and survives reopen: ${kind}`, async ({ page }) => {
    await page.goto('/');
    const result = await page.evaluate(
      (kind) =>
        new Promise((resolve, reject) => {
          const worker = new Worker('/src/schema.worker.ts', { type: 'module' });
          worker.onmessage = ({ data }) => {
            worker.terminate();
            resolve(data);
          };
          worker.onerror = (event) => {
            worker.terminate();
            reject(new Error(event.message));
          };
          worker.postMessage(kind);
        }),
      kind,
    );
    expect(result).toEqual({
      tablesBefore: [],
      docId: 'persisted-document',
      sentinel: 'preserved',
      rootCount: '1',
    });
  });
}

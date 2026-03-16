import { test, expect } from "@playwright/test";

/**
 * Drop OPFS e2e: verifies that client.drop() fully removes OPFS storage.
 * Runs on chromium-dev (same as conformance OPFS) - dev server has COOP/COEP for OPFS.
 *
 * Run locally:
 *   cd packages/treecrdt-wa-sqlite/e2e
 *   pnpm run test:e2e -- tests/drop-opfs.spec.ts
 *
 * Troubleshoot:
 *   - Headed mode (see browser): pnpm exec playwright test tests/drop-opfs.spec.ts --headed --project=chromium-dev
 *   - Debug step-through: pnpm exec playwright test tests/drop-opfs.spec.ts --debug --project=chromium-dev
 *   - Trace (inspect after failure): pnpm exec playwright test tests/drop-opfs.spec.ts --trace=on
 *   - Manual run: pnpm run dev, open http://localhost:4166, DevTools console: await window.__runDropStorageE2E()
 */
test("drop removes all OPFS storage", async ({ page }, testInfo) => {
  if (testInfo.project.name !== "chromium-dev") test.skip();
  test.setTimeout(180_000);
  page.on("console", (msg) => console.log(`[page][${msg.type()}] ${msg.text()}`));

  await page.goto("/");
  await page.waitForFunction(() => typeof (window as any).__runDropStorageE2E === "function");
  const result = await page.evaluate(async () => {
    const runner = (window as any).__runDropStorageE2E;
    if (!runner) throw new Error("__runDropStorageE2E not available");
    return await runner();
  });

  expect(result).toEqual({ ok: true });
});

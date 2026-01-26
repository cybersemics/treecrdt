import { test, expect } from "@playwright/test";

test("sqlite engine conformance (shared suite)", async ({ page }) => {
  test.setTimeout(120_000);

  await page.goto("/");
  await page.waitForFunction(() => typeof window.runTreecrdtSqliteConformanceE2E === "function");
  const result = await page.evaluate(async () => {
    const runner = window.runTreecrdtSqliteConformanceE2E;
    if (!runner) throw new Error("runTreecrdtSqliteConformanceE2E not available");
    return await runner("memory");
  });

  expect(result).toEqual({ ok: true });
});

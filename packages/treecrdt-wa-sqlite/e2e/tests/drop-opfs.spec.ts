import { test, expect } from "@playwright/test";

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

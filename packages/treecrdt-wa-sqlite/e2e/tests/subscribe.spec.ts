import { test, expect } from "@playwright/test";

test("sync v0 live subscription (all + children) e2e", async ({ page }) => {
  test.setTimeout(90_000);
  page.on("console", (msg) => console.log(`[page][${msg.type()}] ${msg.text()}`));

  await page.goto("/");
  await page.waitForFunction(() => typeof window.runTreecrdtSyncSubscribeE2E === "function");
  const result = await page.evaluate(async () => {
    const runner = window.runTreecrdtSyncSubscribeE2E;
    if (!runner) throw new Error("runTreecrdtSyncSubscribeE2E not available");
    return await runner();
  });

  expect(result).toEqual({ ok: true });
});


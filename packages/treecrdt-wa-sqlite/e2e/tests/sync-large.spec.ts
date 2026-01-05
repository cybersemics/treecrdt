import { test, expect } from "@playwright/test";

test.describe("sync v0 large e2e", () => {
  test.skip(process.env.TREECRDT_E2E_LARGE !== "1", "set TREECRDT_E2E_LARGE=1 to run");

  test("sync 100k (fanout=10) from empty peer", async ({ page }) => {
    test.setTimeout(10 * 60_000);
    page.on("console", (msg) => console.log(`[page][${msg.type()}] ${msg.text()}`));

    await page.goto("/");
    await page.waitForFunction(() => typeof window.runTreecrdtSyncLargeFanoutE2E === "function");
    const result = await page.evaluate(async () => {
      const runner = window.runTreecrdtSyncLargeFanoutE2E;
      if (!runner) throw new Error("runTreecrdtSyncLargeFanoutE2E not available");
      return await runner();
    });

    expect(result).toEqual({ ok: true });
  });
});


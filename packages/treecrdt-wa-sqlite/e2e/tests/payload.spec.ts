import { test, expect } from "@playwright/test";

test("append and fetch payload ops", async ({ page }) => {
  await page.goto("/");
  await page.getByRole("button", { name: "Run insert + payload" }).click();
  await expect(page.getByRole("listitem")).toHaveCount(2);
  const items = await page.getByRole("listitem").allInnerTexts();
  const parsed = items.map((t) => JSON.parse(t));

  const payloadOps = parsed.filter((op) => op.kind?.type === "payload");
  expect(payloadOps).toHaveLength(1);
  expect(payloadOps[0].kind.payload).toEqual([104, 101, 108, 108, 111]); // "hello"
});


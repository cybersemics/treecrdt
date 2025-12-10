import { test, expect } from "@playwright/test";

test("wa-sqlite treecrdt extension runs insert+move", async ({ page }) => {
  await page.goto("/");

  const button = page.getByTestId("run-demo");
  await expect(button).toBeEnabled({ timeout: 10000 });

  await button.click();

  const list = page.getByTestId("ops-list").locator("li");
  await expect(list).toHaveCount(2, { timeout: 10000 });
  const texts = await list.allInnerTexts();
  const parsed = texts.map((t) => JSON.parse(t));
  expect(parsed.some((op) => op.kind?.type === "insert")).toBeTruthy();
  expect(parsed.some((op) => op.kind?.type === "move")).toBeTruthy();
});

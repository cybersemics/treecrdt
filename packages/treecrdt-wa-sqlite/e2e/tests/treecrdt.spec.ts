import { test, expect } from "@playwright/test";

test("append and fetch ops", async ({ page }) => {
  await page.goto("/");
  await page.getByRole("button", { name: "Run insert + move" }).click();
  await expect(page.getByRole("listitem")).toHaveCount(2);
  const items = await page.getByRole("listitem").allInnerTexts();
  const parsed = items.map((t) => JSON.parse(t));
  expect(parsed.some((op) => op.kind?.type === "insert")).toBeTruthy();
  expect(parsed.some((op) => op.kind?.type === "move")).toBeTruthy();
});

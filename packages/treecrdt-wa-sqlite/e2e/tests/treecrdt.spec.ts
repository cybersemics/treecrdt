import { test, expect } from "@playwright/test";

test("append and fetch ops", async ({ page }) => {
  await page.goto("/");
  await page.getByRole("button", { name: "Run insert + move" }).click();
  await expect(page.getByRole("listitem")).toHaveCount(2);
  const items = await page.getByRole("listitem").allInnerTexts();
  expect(items[0]).toContain("\"kind\":\"insert\"");
  expect(items[1]).toContain("\"kind\":\"move\"");
});

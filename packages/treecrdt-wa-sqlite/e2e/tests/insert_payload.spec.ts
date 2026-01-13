import { test, expect } from "@playwright/test";

test("append and fetch insert-with-payload op", async ({ page }) => {
  await page.goto("/");
  await page.getByRole("button", { name: "Run insert with payload" }).click();
  await expect(page.getByRole("listitem")).toHaveCount(1);
  const item = await page.getByRole("listitem").innerText();
  const op = JSON.parse(item);

  expect(op.kind?.type).toBe("insert");
  expect(op.kind?.payload).toEqual([104, 101, 108, 108, 111]); // "hello"
});


import { expect, test } from "@playwright/test";

const ROOT_ID = "00000000000000000000000000000000";

function uniqueDocId(prefix: string): string {
  return `${prefix}-${Date.now()}-${Math.random().toString(16).slice(2)}`;
}

async function waitForReady(page: import("@playwright/test").Page, path: string) {
  await page.goto(path);
  await expect(page.getByText("Ready (memory)")).toBeVisible({ timeout: 60_000 });
}

async function expectAuthEnabledByDefault(page: import("@playwright/test").Page) {
  const authToggle = page.getByRole("button", { name: "Auth", exact: true });
  await authToggle.click();
  await expect(page.getByText("Enabled (ops must be signed and authorized)")).toBeVisible({ timeout: 30_000 });
  await authToggle.click();
}

function treeRowByNodeId(page: import("@playwright/test").Page, nodeId: string) {
  return page.locator(`[data-testid="tree-row"][data-node-id="${nodeId}"]`);
}

function treeRowByLabel(page: import("@playwright/test").Page, label: string) {
  return page.getByTestId("tree-row").filter({ hasText: label });
}

async function expandIfCollapsed(row: import("@playwright/test").Locator) {
  const expand = row.getByRole("button", { name: "Expand node" });
  if (await expand.count()) await expand.click();
}

test("insert and delete node", async ({ page }) => {
  test.setTimeout(90_000);

  const doc = uniqueDocId("pw-playground-basic");
  await waitForReady(page, `/?doc=${encodeURIComponent(doc)}&replica=pw-a`);
  await expectAuthEnabledByDefault(page);

  await page.getByPlaceholder("Stored as payload bytes").fill("parent");
  await treeRowByNodeId(page, ROOT_ID).getByRole("button", { name: "Add child" }).click();

  const parentRow = treeRowByLabel(page, "parent");
  await expect(parentRow).toBeVisible({ timeout: 30_000 });

  await page.getByTitle("Toggle operations panel").click();
  const opsPanel = page.locator("aside", { hasText: "Operations" });
  await expect(opsPanel).toBeVisible({ timeout: 30_000 });
  await expect(opsPanel.getByText(/signed/i)).toBeVisible({ timeout: 30_000 });
  await expect(opsPanel.getByText(/signer/i)).toBeVisible({ timeout: 30_000 });
  await expect(opsPanel.getByText(/\(local\)/)).toBeVisible({ timeout: 30_000 });

  await parentRow.getByRole("button", { name: "Delete" }).click();
  await expect(parentRow).toHaveCount(0);
});

test("defensive delete restores parent when unseen child arrives", async ({ browser }) => {
  test.setTimeout(240_000);

  const doc = uniqueDocId("pw-playground-defensive");
  const context = await browser.newContext();
  const pageA = await context.newPage();
  const pageB = await context.newPage();

  try {
    await Promise.all([
      waitForReady(pageA, `/?doc=${encodeURIComponent(doc)}&replica=pw-a`),
      waitForReady(pageB, `/?doc=${encodeURIComponent(doc)}&replica=pw-b`),
    ]);
    await Promise.all([expectAuthEnabledByDefault(pageA), expectAuthEnabledByDefault(pageB)]);

    // Wait for peer discovery (Sync button enabled).
    await Promise.all([
      expect(pageA.getByRole("button", { name: "Sync", exact: true })).toBeEnabled({ timeout: 30_000 }),
      expect(pageB.getByRole("button", { name: "Sync", exact: true })).toBeEnabled({ timeout: 30_000 }),
    ]);

    // Insert parent on A and sync so B knows about it.
    await pageA.getByPlaceholder("Stored as payload bytes").fill("PARENT");
    await treeRowByNodeId(pageA, ROOT_ID).getByRole("button", { name: "Add child" }).click();
    await expect(treeRowByLabel(pageA, "PARENT")).toBeVisible({ timeout: 30_000 });
    await pageA.getByRole("button", { name: "Sync", exact: true }).click();
    const parentOnB = treeRowByLabel(pageB, "PARENT");
    const syncErrorA = pageA.getByTestId("sync-error");
    const syncErrorB = pageB.getByTestId("sync-error");
    await Promise.race([
      parentOnB.waitFor({ state: "visible", timeout: 30_000 }),
      (async () => {
        await syncErrorA.waitFor({ state: "visible", timeout: 30_000 });
        throw new Error(`sync error (A): ${await syncErrorA.textContent()}`);
      })(),
      (async () => {
        await syncErrorB.waitFor({ state: "visible", timeout: 30_000 });
        throw new Error(`sync error (B): ${await syncErrorB.textContent()}`);
      })(),
    ]);

    const parentRowA = treeRowByLabel(pageA, "PARENT");
    const parentRowB = treeRowByLabel(pageB, "PARENT");

    // Ensure parent children are loaded on A (so the next insert becomes visible).
    await expandIfCollapsed(parentRowA);

    // Insert child on A under parent (B is unaware until sync).
    await pageA.getByPlaceholder("Stored as payload bytes").fill("CHILD");
    await parentRowA.getByRole("button", { name: "Add child" }).click();
    await expect(treeRowByLabel(pageA, "CHILD")).toBeVisible({ timeout: 30_000 });

    // Delete parent on B while unaware of the new child.
    await parentRowB.getByRole("button", { name: "Delete" }).click();
    await expect(parentRowB).toHaveCount(0);

    // Sync; defensive deletion should restore parent + child.
    await pageB.getByRole("button", { name: "Sync", exact: true }).click();

    const restoredParentB = treeRowByLabel(pageB, "PARENT");
    await expect(restoredParentB).toBeVisible({ timeout: 30_000 });
    await expandIfCollapsed(restoredParentB);
    await expect(treeRowByLabel(pageB, "CHILD")).toBeVisible({ timeout: 30_000 });
  } finally {
    await context.close();
  }
});

test("invite denies writes to private subtree", async ({ browser }) => {
  test.setTimeout(240_000);

  const doc = uniqueDocId("pw-playground-private");
  const context = await browser.newContext();
  const pageA = await context.newPage();
  const pageB = await context.newPage();

  try {
    await Promise.all([
      waitForReady(pageA, `/?doc=${encodeURIComponent(doc)}&replica=pw-a`),
      waitForReady(pageB, `/?doc=${encodeURIComponent(doc)}&replica=pw-b`),
    ]);
    await Promise.all([expectAuthEnabledByDefault(pageA), expectAuthEnabledByDefault(pageB)]);

    await pageA.getByPlaceholder("Stored as payload bytes").fill("secret-placeholder");
    await treeRowByNodeId(pageA, ROOT_ID).getByRole("button", { name: "Add child" }).click();
    const placeholderRowA = treeRowByLabel(pageA, "secret-placeholder");
    await expect(placeholderRowA).toBeVisible({ timeout: 30_000 });

    const secretNodeId = await placeholderRowA.getAttribute("data-node-id");
    if (!secretNodeId) throw new Error("expected secret node id");
    const secretRowA = treeRowByNodeId(pageA, secretNodeId);

    const privacyToggleA = secretRowA.getByRole("button", { name: "Toggle node privacy" });
    await privacyToggleA.click();
    await expect(privacyToggleA).toHaveAttribute("aria-pressed", "true");

    await pageA.getByRole("button", { name: "Auth", exact: true }).click();
    await pageA.getByRole("button", { name: "Generate" }).click();
    await expect(pageA.locator("textarea[readonly]")).toBeVisible({ timeout: 30_000 });
    const inviteLink = await pageA.locator("textarea[readonly]").inputValue();

    await pageB.getByRole("button", { name: "Auth", exact: true }).click();
    const inviteInput = pageB.getByPlaceholder("Paste an invite URL (or invite=...)");
    await inviteInput.fill(inviteLink);
    await inviteInput.locator("..").getByRole("button", { name: "Import" }).click();

    await Promise.all([
      expect(pageA.getByRole("button", { name: "Sync", exact: true })).toBeEnabled({ timeout: 30_000 }),
      expect(pageB.getByRole("button", { name: "Sync", exact: true })).toBeEnabled({ timeout: 30_000 }),
    ]);

    // Push ops from A to B.
    await pageA.getByRole("button", { name: "Sync", exact: true }).click();

    const secretRowB = treeRowByNodeId(pageB, secretNodeId);
    await expect(secretRowB).toBeVisible({ timeout: 30_000 });

    // Attempt to update payload within the excluded subtree; should be denied by local auth.
    await secretRowB.getByRole("button", { name: "secret-placeholder" }).click();
    await secretRowB.getByRole("textbox").fill("HACKED");
    await secretRowB.getByRole("button", { name: "Save" }).click();

    await expect(pageB.getByText("Failed to append operation (see console)")).toBeVisible({ timeout: 30_000 });
    await expect(treeRowByLabel(pageB, "HACKED")).toHaveCount(0);
    await expect(treeRowByLabel(pageB, "secret-placeholder")).toBeVisible({ timeout: 30_000 });
  } finally {
    await context.close();
  }
});

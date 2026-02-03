import { expect, test } from "@playwright/test";

const ROOT_ID = "00000000000000000000000000000000";

function uniqueDocId(prefix: string): string {
  return `${prefix}-${Date.now()}-${Math.random().toString(16).slice(2)}`;
}

async function waitForReady(page: import("@playwright/test").Page, path: string) {
  await page.goto(path);
  await expect(page.getByText("Ready (memory)")).toBeVisible({ timeout: 60_000 });
  const isJoinMode = new URL(path, "http://localhost").searchParams.get("join") === "1";
  if (!isJoinMode) {
    await expect(page.getByTestId("self-pubkey")).toHaveAttribute("title", /[0-9a-f]{64}/i, { timeout: 60_000 });
  }

  // Composer is hidden by default; open it for tests that rely on the input fields.
  const showComposer = page.getByRole("button", { name: "Show", exact: true });
  if ((await showComposer.count()) > 0) await showComposer.click();

  if (!isJoinMode) {
    await expect(treeRowByNodeId(page, ROOT_ID).getByRole("button", { name: "Add child" })).toBeEnabled({ timeout: 60_000 });
  }
}

async function expectAuthEnabledByDefault(page: import("@playwright/test").Page) {
  const authToggle = page.getByRole("button", { name: "Auth", exact: true });
  await authToggle.click();
  await expect(page.getByText("Enabled (ops must be signed and authorized)")).toBeVisible({ timeout: 30_000 });
  await authToggle.click();
}

async function ensureAuthPanelOpen(page: import("@playwright/test").Page) {
  const marker = page.getByText("Sharing & Auth", { exact: true });
  if ((await marker.count()) > 0) return;
  await page.getByRole("button", { name: "Auth", exact: true }).click();
  await expect(marker).toBeVisible({ timeout: 30_000 });
}

async function ensureAuthAdvancedOpen(page: import("@playwright/test").Page) {
  await ensureAuthPanelOpen(page);
  const toggle = page.getByRole("button", { name: /advanced$/i });
  if ((await toggle.count()) === 0) return;
  const expanded = await toggle.getAttribute("aria-expanded");
  if (expanded !== "true") await toggle.click();
  await expect(toggle).toHaveAttribute("aria-expanded", "true", { timeout: 30_000 });
}

async function waitForLocalAuthTokens(page: import("@playwright/test").Page) {
  await ensureAuthPanelOpen(page);
  const tokenCount = page.getByTestId("auth-token-count");
  await expect(tokenCount).toContainText(/tokens\s*[1-9]/i, { timeout: 30_000 });
  await page.getByRole("button", { name: "Auth", exact: true }).click();
}

async function readReplicaPubkeyHex(page: import("@playwright/test").Page): Promise<string> {
  const title = await page.getByTestId("self-pubkey").getAttribute("title");
  const match = title?.match(/[0-9a-f]{64}/i);
  if (!match) throw new Error(`expected replica pubkey in title: ${title ?? ""}`);
  return match[0]!;
}

async function enableRevealIdentity(page: import("@playwright/test").Page) {
  await ensureAuthAdvancedOpen(page);
  const identityToggle = page.getByRole("button", { name: "Private", exact: true });
  await identityToggle.click();
  await expect(page.getByRole("button", { name: "Revealing", exact: true })).toBeVisible({ timeout: 30_000 });
  await page.getByRole("button", { name: "Auth", exact: true }).click();
}

async function readDeviceWrapKeyB64(page: import("@playwright/test").Page): Promise<string> {
  const marker = page.getByText("Sharing & Auth", { exact: true });
  const authWasOpen = (await marker.count()) > 0;
  await ensureAuthAdvancedOpen(page);

  const title = page.getByText("Device wrap key", { exact: true });

  const card = title.locator("..").locator("..");
  const mono = card.locator("div.font-mono").first();
  await expect(mono).toHaveAttribute("title", /[A-Za-z0-9_-]{43}/, { timeout: 30_000 });
  const wrapKey = await mono.getAttribute("title");

  if (!authWasOpen) await page.getByRole("button", { name: "Auth", exact: true }).click();
  if (!wrapKey) throw new Error("expected device wrap key");
  return wrapKey;
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
  await waitForReady(page, `/?doc=${encodeURIComponent(doc)}`);
  await expectAuthEnabledByDefault(page);
  await waitForLocalAuthTokens(page);

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
  await expect(parentRow).toHaveCount(0, { timeout: 30_000 });
});

test("defensive delete restores parent when unseen child arrives", async ({ browser }) => {
  test.setTimeout(240_000);

  const doc = uniqueDocId("pw-playground-defensive");
  const context = await browser.newContext();
  const pageA = await context.newPage();
  const pageB = await context.newPage();

  try {
    await Promise.all([
      waitForReady(pageA, `/?doc=${encodeURIComponent(doc)}`),
      waitForReady(pageB, `/?doc=${encodeURIComponent(doc)}`),
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

test("invite hides private subtree (excluded roots are not synced)", async ({ browser }) => {
  test.setTimeout(240_000);

  const doc = uniqueDocId("pw-playground-private");
  const context = await browser.newContext();
  const pageA = await context.newPage();
  const pageB = await context.newPage();

  try {
    await Promise.all([
      waitForReady(pageA, `/?doc=${encodeURIComponent(doc)}`),
      waitForReady(pageB, `/?doc=${encodeURIComponent(doc)}`),
    ]);
    await Promise.all([expectAuthEnabledByDefault(pageA), expectAuthEnabledByDefault(pageB)]);

    // Wait for peer discovery (Sync button enabled) so auth/session initialization has time to complete.
    await Promise.all([
      expect(pageA.getByRole("button", { name: "Sync", exact: true })).toBeEnabled({ timeout: 30_000 }),
      expect(pageB.getByRole("button", { name: "Sync", exact: true })).toBeEnabled({ timeout: 30_000 }),
    ]);

    // Create a public node and a private root.
    await pageA.getByPlaceholder("Stored as payload bytes").fill("public");
    await treeRowByNodeId(pageA, ROOT_ID).getByRole("button", { name: "Add child" }).click();
    await expect(treeRowByLabel(pageA, "public")).toBeVisible({ timeout: 30_000 });

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
    const publicRowBLabel = treeRowByLabel(pageB, "public");
    const syncErrorA = pageA.getByTestId("sync-error");
    const syncErrorB = pageB.getByTestId("sync-error");

    const waitForPublicOrError = async () => {
      await Promise.race([
        publicRowBLabel.waitFor({ state: "visible", timeout: 30_000 }),
        (async () => {
          await syncErrorA.waitFor({ state: "visible", timeout: 30_000 });
          throw new Error(`sync error (A): ${await syncErrorA.textContent()}`);
        })(),
        (async () => {
          await syncErrorB.waitFor({ state: "visible", timeout: 30_000 });
          throw new Error(`sync error (B): ${await syncErrorB.textContent()}`);
        })(),
      ]);
    };

    let pushed = false;
    for (let attempt = 0; attempt < 3; attempt++) {
      await pageA.getByRole("button", { name: "Sync", exact: true }).click();
      try {
        await waitForPublicOrError();
        pushed = true;
        break;
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        const initializing = msg.includes("initializing keys/tokens");
        if (msg.startsWith("sync error") && !initializing) throw err;
        if (attempt < 2) {
          await pageA.waitForTimeout(250);
          continue;
        }
        throw err;
      }
    }
    if (!pushed) throw new Error("failed to sync public node");
    const publicNodeId = await publicRowBLabel.getAttribute("data-node-id");
    if (!publicNodeId) throw new Error("expected public node id");
    await expect(treeRowByNodeId(pageB, secretNodeId)).toHaveCount(0, { timeout: 30_000 });

    // Writes to non-excluded nodes should still work.
    const publicRowB = treeRowByNodeId(pageB, publicNodeId);
    await publicRowB.getByRole("button", { name: "public" }).click();
    await publicRowB.getByRole("textbox").fill("PUBLIC-UPDATED");
    await publicRowB.getByRole("button", { name: "Save" }).click();
    await expect(treeRowByLabel(pageB, "PUBLIC-UPDATED")).toBeVisible({ timeout: 30_000 });
  } finally {
    await context.close();
  }
});

test("grant by replica pubkey reveals a private subtree on resync", async ({ browser }) => {
  test.setTimeout(240_000);

  const doc = uniqueDocId("pw-playground-grant");
  const profile = `pw-grant-${Date.now()}-${Math.random().toString(16).slice(2)}`;
  const context = await browser.newContext();
  const pageA = await context.newPage();
  const pageB = await context.newPage();

  try {
    await Promise.all([
      waitForReady(pageA, `/?doc=${encodeURIComponent(doc)}`),
      waitForReady(pageB, `/?doc=${encodeURIComponent(doc)}&profile=${encodeURIComponent(profile)}&join=1`),
    ]);

    await expectAuthEnabledByDefault(pageA);

    // Create a public node and a private root on A.
    await pageA.getByPlaceholder("Stored as payload bytes").fill("public");
    await treeRowByNodeId(pageA, ROOT_ID).getByRole("button", { name: "Add child" }).click();
    await expect(treeRowByLabel(pageA, "public")).toBeVisible({ timeout: 30_000 });

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

    // Invite B with private roots excluded (default behavior).
    await ensureAuthPanelOpen(pageA);
    await pageA.getByRole("button", { name: "Generate" }).click();
    await expect(pageA.locator("textarea[readonly]")).toBeVisible({ timeout: 30_000 });
    const inviteLink = await pageA.locator("textarea[readonly]").inputValue();

    await ensureAuthPanelOpen(pageB);
    const inviteInput = pageB.getByPlaceholder("Paste an invite URL (or invite=...)");
    await inviteInput.fill(inviteLink);
    await inviteInput.locator("..").getByRole("button", { name: "Import" }).click();
    await waitForLocalAuthTokens(pageB);

    // Wait for peer discovery and sync public ops.
    await Promise.all([
      expect(pageA.getByRole("button", { name: "Sync", exact: true })).toBeEnabled({ timeout: 30_000 }),
      expect(pageB.getByRole("button", { name: "Sync", exact: true })).toBeEnabled({ timeout: 30_000 }),
    ]);

    const publicRowB = treeRowByLabel(pageB, "public");
    const syncErrorA = pageA.getByTestId("sync-error");
    const syncErrorB = pageB.getByTestId("sync-error");

    await pageA.getByRole("button", { name: "Sync", exact: true }).click();
    await Promise.race([
      publicRowB.waitFor({ state: "visible", timeout: 30_000 }),
      (async () => {
        await syncErrorA.waitFor({ state: "visible", timeout: 30_000 });
        throw new Error(`sync error (A): ${await syncErrorA.textContent()}`);
      })(),
      (async () => {
        await syncErrorB.waitFor({ state: "visible", timeout: 30_000 });
        throw new Error(`sync error (B): ${await syncErrorB.textContent()}`);
      })(),
    ]);

    await expect(treeRowByNodeId(pageB, secretNodeId)).toHaveCount(0, { timeout: 30_000 });

    // Grant access to B's replica pubkey for the secret subtree.
    const recipientPkHex = await readReplicaPubkeyHex(pageB);
    await ensureAuthPanelOpen(pageA);
    await pageA.getByLabel("Subtree root").selectOption(secretNodeId);
    await pageA.getByPlaceholder("Recipient replica pubkey (hex or base64url)").fill(recipientPkHex);
    await pageA.getByRole("button", { name: "Grant", exact: true }).click();

    await expect(pageB.getByText("Access grant received", { exact: false })).toBeVisible({ timeout: 30_000 });

    // Sync from B so it advertises the new token and pulls the newly authorized ops.
    await expect(pageB.getByRole("button", { name: "Sync", exact: true })).toBeEnabled({ timeout: 30_000 });
    await pageB.getByRole("button", { name: "Sync", exact: true }).click();
    await expect(treeRowByNodeId(pageB, secretNodeId)).toBeVisible({ timeout: 30_000 });
  } finally {
    await context.close();
  }
});

test("identity chain is shown when peers reveal identity", async ({ browser }) => {
  test.setTimeout(240_000);

  const doc = uniqueDocId("pw-playground-identity");
  const context = await browser.newContext();
  const pageA = await context.newPage();
  const pageB = await context.newPage();

  try {
    await Promise.all([
      waitForReady(pageA, `/?doc=${encodeURIComponent(doc)}`),
      waitForReady(pageB, `/?doc=${encodeURIComponent(doc)}`),
    ]);
    await Promise.all([expectAuthEnabledByDefault(pageA), expectAuthEnabledByDefault(pageB)]);
    await Promise.all([enableRevealIdentity(pageA), enableRevealIdentity(pageB)]);

    // Wait for peer discovery (Sync button enabled).
    await Promise.all([
      expect(pageA.getByRole("button", { name: "Sync", exact: true })).toBeEnabled({ timeout: 30_000 }),
      expect(pageB.getByRole("button", { name: "Sync", exact: true })).toBeEnabled({ timeout: 30_000 }),
    ]);

    await pageA.getByPlaceholder("Stored as payload bytes").fill("IDENTITY-TEST");
    await treeRowByNodeId(pageA, ROOT_ID).getByRole("button", { name: "Add child" }).click();
    await expect(treeRowByLabel(pageA, "IDENTITY-TEST")).toBeVisible({ timeout: 30_000 });

    await pageA.getByRole("button", { name: "Sync", exact: true }).click();
    await expect(treeRowByLabel(pageB, "IDENTITY-TEST")).toBeVisible({ timeout: 30_000 });

    await pageB.getByTitle("Toggle operations panel").click();
    const opsPanel = pageB.locator("aside", { hasText: "Operations" });
    await expect(opsPanel).toBeVisible({ timeout: 30_000 });
    await expect(opsPanel.getByText(/identity/i)).toBeVisible({ timeout: 30_000 });
  } finally {
    await context.close();
  }
});

test("identity key blobs can be exported and imported", async ({ browser }) => {
  test.setTimeout(240_000);

  const IDENTITY_SK_SEALED_KEY = "treecrdt-playground-identity-sk-sealed:v1";
  const DEVICE_SIGNING_SK_SEALED_KEY = "treecrdt-playground-device-signing-sk-sealed:v1";

  const doc = uniqueDocId("pw-playground-identity-blobs");
  const context = await browser.newContext();
  const pageA = await context.newPage();
  const pageB = await context.newPage();

  try {
    await Promise.all([
      waitForReady(pageA, `/?doc=${encodeURIComponent(doc)}`),
      waitForReady(pageB, `/?doc=${encodeURIComponent(doc)}`),
    ]);
    await Promise.all([expectAuthEnabledByDefault(pageA), expectAuthEnabledByDefault(pageB)]);
    await enableRevealIdentity(pageA);

    // Wait for peer discovery so the identity chain is computed (which creates the identity keys).
    await Promise.all([
      expect(pageA.getByRole("button", { name: "Sync", exact: true })).toBeEnabled({ timeout: 30_000 }),
      expect(pageB.getByRole("button", { name: "Sync", exact: true })).toBeEnabled({ timeout: 30_000 }),
    ]);

    // Trigger an authenticated sync roundtrip so hello/ack capabilities run.
    await pageA.getByPlaceholder("Stored as payload bytes").fill("BLOB-TEST");
    await treeRowByNodeId(pageA, ROOT_ID).getByRole("button", { name: "Add child" }).click();
    await expect(treeRowByLabel(pageA, "BLOB-TEST")).toBeVisible({ timeout: 30_000 });
    await pageA.getByRole("button", { name: "Sync", exact: true }).click();
    await expect(treeRowByLabel(pageB, "BLOB-TEST")).toBeVisible({ timeout: 30_000 });

    const authToggle = pageA.getByRole("button", { name: "Auth", exact: true });
    await authToggle.click();
    await ensureAuthAdvancedOpen(pageA);

    const identityCard = pageA.getByText("Identity key blob", { exact: true }).locator("..").locator("..");
    const deviceSigningCard = pageA.getByText("Device signing key blob", { exact: true }).locator("..").locator("..");

    await expect(identityCard.getByRole("button", { name: "Copy", exact: true })).toBeEnabled({ timeout: 30_000 });
    await expect(deviceSigningCard.getByRole("button", { name: "Copy", exact: true })).toBeEnabled({ timeout: 30_000 });

    const identityBlob = await identityCard.locator("div.font-mono").first().getAttribute("title");
    if (!identityBlob) throw new Error("expected identity key blob");
    const deviceSigningBlob = await deviceSigningCard.locator("div.font-mono").first().getAttribute("title");
    if (!deviceSigningBlob) throw new Error("expected device signing key blob");

    await pageA.evaluate(
      ({ identityKey, deviceKey }) => {
        window.localStorage.removeItem(identityKey);
        window.localStorage.removeItem(deviceKey);
      },
      { identityKey: IDENTITY_SK_SEALED_KEY, deviceKey: DEVICE_SIGNING_SK_SEALED_KEY }
    );

    // Force a re-render to re-read localStorage-backed values.
    await authToggle.click();
    await authToggle.click();

    await expect(identityCard.getByRole("button", { name: "Copy", exact: true })).toBeDisabled({ timeout: 30_000 });
    await expect(deviceSigningCard.getByRole("button", { name: "Copy", exact: true })).toBeDisabled({ timeout: 30_000 });

    await identityCard.getByPlaceholder("Paste sealed identity key blob (base64url)").fill(identityBlob);
    await identityCard.getByRole("button", { name: "Import", exact: true }).click();
    await expect(identityCard.getByRole("button", { name: "Copy", exact: true })).toBeEnabled({ timeout: 30_000 });

    await deviceSigningCard.getByPlaceholder("Paste sealed device signing key blob (base64url)").fill(deviceSigningBlob);
    await deviceSigningCard.getByRole("button", { name: "Import", exact: true }).click();
    await expect(deviceSigningCard.getByRole("button", { name: "Copy", exact: true })).toBeEnabled({ timeout: 30_000 });
  } finally {
    await context.close();
  }
});

test("isolated peer tab uses separate storage namespace and requires invite", async ({ browser }) => {
  test.setTimeout(240_000);

  const doc = uniqueDocId("pw-playground-isolated");
  const profile = `pw-iso-${Date.now()}-${Math.random().toString(16).slice(2)}`;
  const context = await browser.newContext();
  const pageA = await context.newPage();
  const pageB = await context.newPage();

  try {
    await Promise.all([
      waitForReady(pageA, `/?doc=${encodeURIComponent(doc)}`),
      waitForReady(pageB, `/?doc=${encodeURIComponent(doc)}&profile=${encodeURIComponent(profile)}&join=1`),
    ]);

    await expectAuthEnabledByDefault(pageA);

    const wrapKeyA = await readDeviceWrapKeyB64(pageA);
    const wrapKeyB = await readDeviceWrapKeyB64(pageB);
    expect(wrapKeyB).not.toBe(wrapKeyA);

    // Create a public node and a private root on A.
    await pageA.getByPlaceholder("Stored as payload bytes").fill("public");
    await treeRowByNodeId(pageA, ROOT_ID).getByRole("button", { name: "Add child" }).click();
    await expect(treeRowByLabel(pageA, "public")).toBeVisible({ timeout: 30_000 });

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

    // Generate invite link on A.
    await ensureAuthPanelOpen(pageA);
    await pageA.getByRole("button", { name: "Generate" }).click();
    await expect(pageA.locator("textarea[readonly]")).toBeVisible({ timeout: 30_000 });
    const inviteLink = await pageA.locator("textarea[readonly]").inputValue();

    // Import invite link on B (isolated storage namespace; join-mode requires invite).
    await ensureAuthPanelOpen(pageB);
    const inviteInput = pageB.getByPlaceholder("Paste an invite URL (or invite=...)");
    await inviteInput.fill(inviteLink);
    await inviteInput.locator("..").getByRole("button", { name: "Import" }).click();

    // Wait for peer discovery (Sync button enabled).
    await Promise.all([
      expect(pageA.getByRole("button", { name: "Sync", exact: true })).toBeEnabled({ timeout: 30_000 }),
      expect(pageB.getByRole("button", { name: "Sync", exact: true })).toBeEnabled({ timeout: 30_000 }),
    ]);

    // Push ops from A to B.
    const publicRowBLabel = treeRowByLabel(pageB, "public");
    const syncErrorA = pageA.getByTestId("sync-error");
    const syncErrorB = pageB.getByTestId("sync-error");

    const waitForPublicOrError = async () => {
      await Promise.race([
        publicRowBLabel.waitFor({ state: "visible", timeout: 30_000 }),
        (async () => {
          await syncErrorA.waitFor({ state: "visible", timeout: 30_000 });
          throw new Error(`sync error (A): ${await syncErrorA.textContent()}`);
        })(),
        (async () => {
          await syncErrorB.waitFor({ state: "visible", timeout: 30_000 });
          throw new Error(`sync error (B): ${await syncErrorB.textContent()}`);
        })(),
      ]);
    };

    let pushed = false;
    for (let attempt = 0; attempt < 3; attempt++) {
      await pageA.getByRole("button", { name: "Sync", exact: true }).click();
      try {
        await waitForPublicOrError();
        pushed = true;
        break;
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        const initializing = msg.includes("initializing keys/tokens");
        if (msg.startsWith("sync error") && !initializing) throw err;
        if (attempt < 2) {
          await pageA.waitForTimeout(250);
          continue;
        }
        throw err;
      }
    }
    if (!pushed) throw new Error("failed to sync public node");

    await expect(treeRowByNodeId(pageB, secretNodeId)).toHaveCount(0, { timeout: 30_000 });
  } finally {
    await context.close();
  }
});

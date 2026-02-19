import { expect, test } from "@playwright/test";

const ROOT_ID = "00000000000000000000000000000000";

function uniqueDocId(prefix: string): string {
  // Use crypto for uniqueness to avoid collisions when tests run in quick succession.
  // Date.now() alone can collide (ms resolution) and Math.random can be deterministically seeded.
  const suffix = typeof crypto !== "undefined" && "randomUUID" in crypto ? crypto.randomUUID() : `${Math.random()}`;
  return `${prefix}-${suffix}`;
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
  await ensureAuthAdvancedOpen(page);
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

async function readMembersBadgeCount(
  page: import("@playwright/test").Page,
  nodeId: string
): Promise<number | null> {
  const membersButton = treeRowByNodeId(page, nodeId).getByRole("button", {
    name: "Members and capabilities",
  });
  if ((await membersButton.count()) === 0) return null;
  const text = (await membersButton.locator("span.font-mono").first().textContent())?.trim() ?? "";
  if (!/^\d+$/.test(text)) return null;
  return Number.parseInt(text, 10);
}

async function expectMembersBadgeCount(
  page: import("@playwright/test").Page,
  nodeId: string,
  expected: number
) {
  await expect
    .poll(async () => readMembersBadgeCount(page, nodeId), {
      timeout: 30_000,
      message: `expected members badge count ${expected} for node ${nodeId}`,
    })
    .toBe(expected);
}

async function readLatestIssuedGrantActions(
  page: import("@playwright/test").Page,
  opts: { docId: string; rootNodeId: string; recipientPkHex: string }
): Promise<string[] | null> {
  return await page.evaluate(({ docId, rootNodeId, recipientPkHex }) => {
    const profile = new URLSearchParams(window.location.search).get("profile");
    const keyBase = `treecrdt-playground-issued-grants:${docId}`;
    const storageKey = profile ? `treecrdt-playground-profile:${profile}:${keyBase}` : keyBase;
    const raw = window.localStorage.getItem(storageKey);
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) return null;
    const root = rootNodeId.toLowerCase();
    const recipient = recipientPkHex.toLowerCase();
    const row = parsed.find(
      (entry) =>
        entry &&
        typeof entry === "object" &&
        String((entry as { rootNodeId?: string }).rootNodeId ?? "").toLowerCase() === root &&
        String((entry as { recipientPkHex?: string }).recipientPkHex ?? "").toLowerCase() === recipient
    ) as { actions?: unknown } | undefined;
    if (!row || !Array.isArray(row.actions)) return null;
    return row.actions.filter((value): value is string => typeof value === "string");
  }, opts);
}

async function expandIfCollapsed(row: import("@playwright/test").Locator) {
  const expand = row.getByRole("button", { name: "Expand node" });
  if (await expand.count()) await expand.click();
}

async function shareSubtreeInvite(page: import("@playwright/test").Page, nodeId: string): Promise<string> {
  await treeRowByNodeId(page, nodeId).getByRole("button", { name: "Share subtree (invite)" }).click();
  const textarea = page.getByPlaceholder("Invite link will appear here…");
  await expect(textarea).toHaveValue(/invite=/, { timeout: 30_000 });
  const link = await textarea.inputValue();
  await page.getByRole("button", { name: "Close", exact: true }).click();
  return link;
}

async function joinViaInviteLink(page: import("@playwright/test").Page, inviteLink: string) {
  await waitForReady(page, inviteLink);
  await waitForLocalAuthTokens(page);
}

async function clickSync(page: import("@playwright/test").Page, label: string) {
  const syncBtn = page.getByRole("button", { name: "Sync", exact: true });
  const syncError = page.getByTestId("sync-error");
  await syncBtn.click();
  await Promise.race([
    expect(syncBtn).toBeEnabled({ timeout: 30_000 }),
    (async () => {
      await syncError.waitFor({ state: "visible", timeout: 30_000 });
      throw new Error(`sync error (${label}): ${await syncError.textContent()}`);
    })(),
  ]);
  if (await syncError.isVisible()) {
    throw new Error(`sync error (${label}): ${await syncError.textContent()}`);
  }
}

function isTransientAuthSyncErrorMessage(message: string): boolean {
  return (
    /COSE_Sign1 signature verification failed/i.test(message) ||
    /sync(?:\([^)]+\))?\s+with\s+.+\s+timed out/i.test(message) ||
    /auth enabled but no local capability tokens are recorded/i.test(message) ||
    /initializing keys\/tokens/i.test(message)
  );
}

async function clickSyncWithRetryOnTransientAuthError(
  page: import("@playwright/test").Page,
  label: string,
  opts?: {
    attempts?: number;
    onRetry?: () => Promise<void>;
  }
) {
  const attempts = Math.max(1, opts?.attempts ?? 4);
  for (let attempt = 1; attempt <= attempts; attempt++) {
    try {
      await clickSync(page, label);
      return;
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      const isRetryable = isTransientAuthSyncErrorMessage(message);
      if (!isRetryable || attempt === attempts) throw err;
      await waitForLocalAuthTokens(page);
      if (opts?.onRetry) await opts.onRetry();
      await page.waitForTimeout(300);
    }
  }
}

async function clickSyncBestEffortOnTransientAuthError(page: import("@playwright/test").Page, label: string) {
  try {
    await clickSyncWithRetryOnTransientAuthError(page, label);
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    if (!isTransientAuthSyncErrorMessage(message)) throw err;
  }
}

async function clickSyncAllowRevokedCapabilityTokenError(page: import("@playwright/test").Page, label: string) {
  try {
    await clickSync(page, label);
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    if (/capability token revoked/i.test(message)) return;
    if (/access revoked for this capability/i.test(message)) return;
    if (isTransientAuthSyncErrorMessage(message)) return;
    throw err;
  }
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
  const profile = `pw-private-${Date.now()}-${Math.random().toString(16).slice(2)}`;
  const context = await browser.newContext();
  const pageA = await context.newPage();
  const pageB = await context.newPage();

  try {
    await Promise.all([
      waitForReady(pageA, `/?doc=${encodeURIComponent(doc)}`),
      // Use an isolated (join-only) peer so importing the invite doesn't "swap" an existing peer id mid-test
      // (which can cause A to sync to a stale peer id and burn the per-peer 15s timeout).
      waitForReady(
        pageB,
        `/?doc=${encodeURIComponent(doc)}&profile=${encodeURIComponent(profile)}&join=1`
      ),
    ]);
    await expectAuthEnabledByDefault(pageA);

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

    const inviteLink = await shareSubtreeInvite(pageA, ROOT_ID);
    await joinViaInviteLink(pageB, inviteLink);

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
    await expandIfCollapsed(publicRowB);
    await pageB.getByPlaceholder("Stored as payload bytes").fill("PUBLIC-CHILD");
    await publicRowB.getByRole("button", { name: "Add child" }).click();
    const publicChildB = treeRowByLabel(pageB, "PUBLIC-CHILD");
    await expect(publicChildB).toBeVisible({ timeout: 30_000 });

  } finally {
    await context.close();
  }
});

test("grant by public key reveals a private subtree on resync", async ({ browser }) => {
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
    const inviteLink = await shareSubtreeInvite(pageA, ROOT_ID);
    await joinViaInviteLink(pageB, inviteLink);

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
    await pageB.getByRole("button", { name: "Sync", exact: true }).click();

    // Grant access to B's public key for the secret subtree.
    const recipientPkHex = await readReplicaPubkeyHex(pageB);
    await secretRowA.getByRole("button", { name: "Members and capabilities" }).click();
    const peerRow = pageA
      .getByText(recipientPkHex, { exact: true })
      .first()
      .locator("xpath=ancestor::div[.//button[normalize-space()='Share…']][1]");
    await expect(peerRow).toBeVisible({ timeout: 30_000 });
    const peerShareButton = peerRow.getByRole("button", { name: "Share…", exact: true }).first();
    const peerGrantButton = peerShareButton.locator("xpath=preceding-sibling::button[1]");
    await expect(peerGrantButton).toBeVisible({ timeout: 30_000 });
    await expect(peerGrantButton).toBeEnabled({ timeout: 30_000 });
    await peerGrantButton.evaluate((el) => (el as HTMLButtonElement).click());
    await expect(peerRow.getByText("active")).toBeVisible({ timeout: 30_000 });

    // Sync from B so it advertises the new token and pulls the newly authorized ops.
    const secretRowB = treeRowByNodeId(pageB, secretNodeId);
    let revealed = false;
    for (let attempt = 0; attempt < 4; attempt++) {
      await clickSyncWithRetryOnTransientAuthError(pageA, "A", { attempts: 3 });
      await clickSyncWithRetryOnTransientAuthError(pageB, "B", { attempts: 3 });
      if (await secretRowB.count()) {
        await expect(secretRowB).toBeVisible({ timeout: 5_000 });
        revealed = true;
        break;
      }
      await pageB.waitForTimeout(250);
    }
    if (!revealed) await expect(secretRowB).toBeVisible({ timeout: 30_000 });
  } finally {
    await context.close();
  }
});

test("revoked invited token blocks subsequent writes from that peer", async ({ browser }) => {
  test.setTimeout(300_000);

  const doc = uniqueDocId("pw-playground-revoke-write");
  const profileB = `pw-revoke-b-${Date.now()}-${Math.random().toString(16).slice(2)}`;
  const context = await browser.newContext();
  const pageA = await context.newPage();
  const pageB = await context.newPage();

  try {
    await Promise.all([
      waitForReady(pageA, `/?doc=${encodeURIComponent(doc)}`),
      waitForReady(pageB, `/?doc=${encodeURIComponent(doc)}&profile=${encodeURIComponent(profileB)}&join=1`),
    ]);
    await expectAuthEnabledByDefault(pageA);
    await waitForLocalAuthTokens(pageA);

    // Create a private subtree root on A.
    await pageA.getByPlaceholder("Stored as payload bytes").fill("secret-revoke-root");
    await treeRowByNodeId(pageA, ROOT_ID).getByRole("button", { name: "Add child" }).click();
    const secretRootRowA = treeRowByLabel(pageA, "secret-revoke-root");
    await expect(secretRootRowA).toBeVisible({ timeout: 30_000 });
    const secretNodeId = await secretRootRowA.getAttribute("data-node-id");
    if (!secretNodeId) throw new Error("expected secret node id");

    const privacyToggleA = treeRowByNodeId(pageA, secretNodeId).getByRole("button", { name: "Toggle node privacy" });
    await privacyToggleA.click();
    await expect(privacyToggleA).toHaveAttribute("aria-pressed", "true");

    // Invite B to private subtree and ensure B can access it.
    const inviteToB = await shareSubtreeInvite(pageA, secretNodeId);
    await joinViaInviteLink(pageB, inviteToB);
    await Promise.all([
      expect(pageA.getByRole("button", { name: "Sync", exact: true })).toBeEnabled({ timeout: 30_000 }),
      expect(pageB.getByRole("button", { name: "Sync", exact: true })).toBeEnabled({ timeout: 30_000 }),
    ]);
    await clickSyncWithRetryOnTransientAuthError(pageB, "B");
    const secretRootRowB = treeRowByNodeId(pageB, secretNodeId);
    await expect(secretRootRowB).toBeVisible({ timeout: 30_000 });

    // B writes under secret subtree; A receives it.
    await pageB.getByPlaceholder("Stored as payload bytes").fill("from-B-before-revoke");
    await secretRootRowB.getByRole("button", { name: "Add child" }).click();
    await expect(treeRowByLabel(pageB, "from-B-before-revoke")).toBeVisible({ timeout: 30_000 });

    const beforeRowA = treeRowByLabel(pageA, "from-B-before-revoke");
    await expandIfCollapsed(treeRowByNodeId(pageA, secretNodeId));
    for (let attempt = 0; attempt < 6; attempt++) {
      await clickSyncWithRetryOnTransientAuthError(pageB, "B");
      await clickSyncWithRetryOnTransientAuthError(pageA, "A");
      await expandIfCollapsed(treeRowByNodeId(pageA, secretNodeId));
      if (await beforeRowA.count()) break;
      await pageA.waitForTimeout(250);
    }
    await expect(beforeRowA).toBeVisible({ timeout: 30_000 });

    // A revokes B's currently issued token for this scope (token-level revoke).
    const recipientPkHex = await readReplicaPubkeyHex(pageB);
    await treeRowByNodeId(pageA, secretNodeId).getByRole("button", { name: "Members and capabilities" }).click();
    const peerRow = pageA
      .getByText(recipientPkHex, { exact: true })
      .first()
      .locator("xpath=ancestor::div[.//button[normalize-space()='Share…']][1]");
    await expect(peerRow).toBeVisible({ timeout: 30_000 });
    const revokeButton = peerRow.getByRole("button", { name: "Revoke", exact: true });
    await expect(revokeButton).toBeEnabled({ timeout: 30_000 });
    await revokeButton.click();
    await expect(peerRow.getByText("revoked", { exact: true })).toBeVisible({ timeout: 30_000 });
    await pageA.keyboard.press("Escape");

    // B can still append locally, but A should reject that write after revoke.
    await pageB.getByPlaceholder("Stored as payload bytes").fill("from-B-after-revoke");
    await secretRootRowB.getByRole("button", { name: "Add child" }).click();
    const afterRowB = treeRowByLabel(pageB, "from-B-after-revoke");
    await expect(afterRowB).toBeVisible({ timeout: 30_000 });

    const afterRowA = treeRowByLabel(pageA, "from-B-after-revoke");
    for (let attempt = 0; attempt < 6; attempt++) {
      await clickSyncAllowRevokedCapabilityTokenError(pageB, "B");
      await clickSyncAllowRevokedCapabilityTokenError(pageA, "A");
      if (await afterRowA.count()) break;
      await pageA.waitForTimeout(250);
    }
    await expect(afterRowA).toHaveCount(0);
  } finally {
    await context.close();
  }
});

test("updating a member to read-only keeps sync working and blocks new writes", async ({ browser }) => {
  test.setTimeout(300_000);

  const doc = uniqueDocId("pw-playground-update-read-only");
  const profileB = `pw-update-read-only-b-${Date.now()}-${Math.random().toString(16).slice(2)}`;
  const context = await browser.newContext();
  const pageA = await context.newPage();
  const pageB = await context.newPage();

  try {
    await Promise.all([
      waitForReady(pageA, `/?doc=${encodeURIComponent(doc)}`),
      waitForReady(pageB, `/?doc=${encodeURIComponent(doc)}&profile=${encodeURIComponent(profileB)}&join=1`),
    ]);
    await expectAuthEnabledByDefault(pageA);
    await waitForLocalAuthTokens(pageA);

    // Create a private subtree root on A.
    await pageA.getByPlaceholder("Stored as payload bytes").fill("secret-update-root");
    await treeRowByNodeId(pageA, ROOT_ID).getByRole("button", { name: "Add child" }).click();
    const secretRootRowA = treeRowByLabel(pageA, "secret-update-root");
    await expect(secretRootRowA).toBeVisible({ timeout: 30_000 });
    const secretNodeId = await secretRootRowA.getAttribute("data-node-id");
    if (!secretNodeId) throw new Error("expected secret node id");

    const privacyToggleA = treeRowByNodeId(pageA, secretNodeId).getByRole("button", { name: "Toggle node privacy" });
    await privacyToggleA.click();
    await expect(privacyToggleA).toHaveAttribute("aria-pressed", "true");

    // Invite B and ensure B can access the private subtree.
    const inviteToB = await shareSubtreeInvite(pageA, secretNodeId);
    await joinViaInviteLink(pageB, inviteToB);
    await Promise.all([
      expect(pageA.getByRole("button", { name: "Sync", exact: true })).toBeEnabled({ timeout: 30_000 }),
      expect(pageB.getByRole("button", { name: "Sync", exact: true })).toBeEnabled({ timeout: 30_000 }),
    ]);

    const secretRootRowB = treeRowByNodeId(pageB, secretNodeId);
    for (let attempt = 0; attempt < 6; attempt++) {
      await clickSyncWithRetryOnTransientAuthError(pageB, "B");
      await clickSyncWithRetryOnTransientAuthError(pageA, "A");
      if (await secretRootRowB.count()) break;
      await pageB.waitForTimeout(250);
    }
    await expect(secretRootRowB).toBeVisible({ timeout: 30_000 });

    // Baseline: B can write under the private subtree and A receives it.
    await expandIfCollapsed(secretRootRowB);
    await pageB.getByPlaceholder("Stored as payload bytes").fill("from-B-before-read-only");
    await secretRootRowB.getByRole("button", { name: "Add child" }).click();
    await expect(treeRowByLabel(pageB, "from-B-before-read-only")).toBeVisible({ timeout: 30_000 });

    const beforeRowA = treeRowByLabel(pageA, "from-B-before-read-only");
    await expandIfCollapsed(treeRowByNodeId(pageA, secretNodeId));
    for (let attempt = 0; attempt < 6; attempt++) {
      await clickSyncWithRetryOnTransientAuthError(pageB, "B");
      await clickSyncWithRetryOnTransientAuthError(pageA, "A");
      await expandIfCollapsed(treeRowByNodeId(pageA, secretNodeId));
      if (await beforeRowA.count()) break;
      await pageA.waitForTimeout(250);
    }
    await expect(beforeRowA).toBeVisible({ timeout: 30_000 });

    // A updates B's capability to read-only (removes write actions, keeps read).
    const recipientPkHex = await readReplicaPubkeyHex(pageB);
    await treeRowByNodeId(pageA, secretNodeId).getByRole("button", { name: "Members and capabilities" }).click();
    const peerRow = pageA
      .getByText(recipientPkHex, { exact: true })
      .first()
      .locator("xpath=ancestor::div[.//button[normalize-space()='Share…']][1]");
    await expect(peerRow).toBeVisible({ timeout: 30_000 });

    const readButton = peerRow.getByRole("button", { name: "Read", exact: true });
    const writeStructureButton = peerRow.getByRole("button", { name: "Write structure", exact: true });
    const writePayloadButton = peerRow.getByRole("button", { name: "Write payload", exact: true });
    await expect(readButton).toHaveAttribute("aria-pressed", "true", { timeout: 30_000 });
    await expect(writeStructureButton).toHaveAttribute("aria-pressed", "true", { timeout: 30_000 });
    await expect(writePayloadButton).toHaveAttribute("aria-pressed", "true", { timeout: 30_000 });

    await writeStructureButton.click();
    await writePayloadButton.click();
    await expect(readButton).toHaveAttribute("aria-pressed", "true");
    await expect(writeStructureButton).toHaveAttribute("aria-pressed", "false");
    await expect(writePayloadButton).toHaveAttribute("aria-pressed", "false");

    await peerRow.getByRole("button", { name: "Update", exact: true }).click();

    await expect
      .poll(
        () =>
          readLatestIssuedGrantActions(pageA, {
            docId: doc,
            rootNodeId: secretNodeId,
            recipientPkHex,
          }),
        { timeout: 30_000 }
      )
      .toEqual(expect.arrayContaining(["read_structure", "read_payload"]));
    const latestActions = await readLatestIssuedGrantActions(pageA, {
      docId: doc,
      rootNodeId: secretNodeId,
      recipientPkHex,
    });
    expect(latestActions).not.toEqual(expect.arrayContaining(["write_structure"]));
    expect(latestActions).not.toEqual(expect.arrayContaining(["write_payload"]));

    await pageA.keyboard.press("Escape");

    // Allow one transition round where B might still present a superseded token,
    // but ensure there is no capability-filter auth failure while syncing.
    for (let attempt = 0; attempt < 8; attempt++) {
      await clickSyncAllowRevokedCapabilityTokenError(pageB, "B");
      await clickSyncAllowRevokedCapabilityTokenError(pageA, "A");
      await pageB.waitForTimeout(150);
    }

    // Read still works: A writes, B can sync and see it.
    const secretRootByIdA = treeRowByNodeId(pageA, secretNodeId);
    await expandIfCollapsed(secretRootByIdA);
    await pageA.getByPlaceholder("Stored as payload bytes").fill("from-A-after-read-only");
    await secretRootByIdA.getByRole("button", { name: "Add child" }).click();
    await expect(treeRowByLabel(pageA, "from-A-after-read-only")).toBeVisible({ timeout: 30_000 });

    const fromAOnB = treeRowByLabel(pageB, "from-A-after-read-only");
    for (let attempt = 0; attempt < 8; attempt++) {
      await clickSyncAllowRevokedCapabilityTokenError(pageB, "B");
      await clickSyncAllowRevokedCapabilityTokenError(pageA, "A");
      if (await fromAOnB.count()) break;
      await pageB.waitForTimeout(250);
    }
    await expect(fromAOnB).toBeVisible({ timeout: 30_000 });
  } finally {
    await context.close();
  }
});

test("members counter is consistent across tabs for private subtree invites", async ({ browser }) => {
  test.setTimeout(300_000);

  const doc = uniqueDocId("pw-playground-members-counter");
  const profileB = `pw-members-b-${Date.now()}-${Math.random().toString(16).slice(2)}`;
  const context = await browser.newContext();
  const pageA = await context.newPage();
  const pageB = await context.newPage();

  try {
    await Promise.all([
      waitForReady(pageA, `/?doc=${encodeURIComponent(doc)}`),
      waitForReady(pageB, `/?doc=${encodeURIComponent(doc)}&profile=${encodeURIComponent(profileB)}&join=1`),
    ]);
    await expectAuthEnabledByDefault(pageA);
    await waitForLocalAuthTokens(pageA);

    // Create a private subtree root on A.
    await pageA.getByPlaceholder("Stored as payload bytes").fill("secret-members-root");
    await treeRowByNodeId(pageA, ROOT_ID).getByRole("button", { name: "Add child" }).click();
    const secretRootRowA = treeRowByLabel(pageA, "secret-members-root");
    await expect(secretRootRowA).toBeVisible({ timeout: 30_000 });
    const secretNodeId = await secretRootRowA.getAttribute("data-node-id");
    if (!secretNodeId) throw new Error("expected secret node id");

    const privacyToggleA = treeRowByNodeId(pageA, secretNodeId).getByRole("button", { name: "Toggle node privacy" });
    await privacyToggleA.click();
    await expect(privacyToggleA).toHaveAttribute("aria-pressed", "true");

    // Invite B to private subtree and ensure B can access it.
    const inviteToB = await shareSubtreeInvite(pageA, secretNodeId);
    await joinViaInviteLink(pageB, inviteToB);
    await Promise.all([
      expect(pageA.getByRole("button", { name: "Sync", exact: true })).toBeEnabled({ timeout: 30_000 }),
      expect(pageB.getByRole("button", { name: "Sync", exact: true })).toBeEnabled({ timeout: 30_000 }),
    ]);

    const secretRootRowB = treeRowByNodeId(pageB, secretNodeId);
    for (let attempt = 0; attempt < 6; attempt++) {
      await clickSyncWithRetryOnTransientAuthError(pageB, "B");
      await clickSyncWithRetryOnTransientAuthError(pageA, "A");
      if (await secretRootRowB.count()) break;
      await pageB.waitForTimeout(250);
    }
    await expect(secretRootRowB).toBeVisible({ timeout: 30_000 });

    // Both tabs should show exactly one member in the private-subtree people badge.
    await expectMembersBadgeCount(pageA, secretNodeId, 1);
    await expectMembersBadgeCount(pageB, secretNodeId, 1);

    // B writes under the private subtree; A should receive it after sync.
    await expandIfCollapsed(secretRootRowB);
    await pageB.getByPlaceholder("Stored as payload bytes").fill("from-B-members-counter");
    await secretRootRowB.getByRole("button", { name: "Add child" }).click();
    await expect(treeRowByLabel(pageB, "from-B-members-counter")).toBeVisible({ timeout: 30_000 });

    const secretRootByIdA = treeRowByNodeId(pageA, secretNodeId);
    await expandIfCollapsed(secretRootByIdA);
    const childRowA = treeRowByLabel(pageA, "from-B-members-counter");
    for (let attempt = 0; attempt < 8; attempt++) {
      await clickSyncWithRetryOnTransientAuthError(pageB, "B");
      await clickSyncWithRetryOnTransientAuthError(pageA, "A");
      await expandIfCollapsed(secretRootByIdA);
      if (await childRowA.count()) break;
      await pageA.waitForTimeout(250);
    }
    await expect(childRowA).toBeVisible({ timeout: 30_000 });

    // Counter remains stable after private-subtree writes/sync.
    await expectMembersBadgeCount(pageA, secretNodeId, 1);
    await expectMembersBadgeCount(pageB, secretNodeId, 1);
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
    const inviteLink = await shareSubtreeInvite(pageA, ROOT_ID);
    await joinViaInviteLink(pageB, inviteLink);

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

test("open device auto-syncs so the scoped root label is visible", async ({ browser }) => {
  test.setTimeout(240_000);

  const doc = uniqueDocId("pw-playground-open-device");
  const context = await browser.newContext();
  const pageA = await context.newPage();

  try {
    await waitForReady(pageA, `/?doc=${encodeURIComponent(doc)}`);
    await expectAuthEnabledByDefault(pageA);
    await waitForLocalAuthTokens(pageA);

    await pageA.getByPlaceholder("Stored as payload bytes").fill("AASD");
    await treeRowByNodeId(pageA, ROOT_ID).getByRole("button", { name: "Add child" }).click();
    const rowA = treeRowByLabel(pageA, "AASD");
    await expect(rowA).toBeVisible({ timeout: 30_000 });

    const nodeId = await rowA.getAttribute("data-node-id");
    if (!nodeId) throw new Error("expected node id");

    await treeRowByNodeId(pageA, nodeId).getByRole("button", { name: "Share subtree (invite)" }).click();
    const textarea = pageA.getByPlaceholder("Invite link will appear here…");
    await expect(textarea).toHaveValue(/invite=/, { timeout: 30_000 });

    const [pageB] = await Promise.all([
      pageA.waitForEvent("popup"),
      pageA.getByRole("button", { name: "Open device", exact: true }).click(),
    ]);

    await pageB.bringToFront();
    await expect(pageB.getByText("Ready (memory)")).toBeVisible({ timeout: 60_000 });

    const rowB = treeRowByNodeId(pageB, nodeId);
    await expect(rowB.getByRole("button", { name: "AASD" })).toBeVisible({ timeout: 60_000 });
  } finally {
    await context.close();
  }
});

test("open device sees latest scoped-root label after rename", async ({ browser }) => {
  test.setTimeout(240_000);

  const doc = uniqueDocId("pw-playground-open-device-rename");
  const context = await browser.newContext();
  const pageA = await context.newPage();

  try {
    await waitForReady(pageA, `/?doc=${encodeURIComponent(doc)}`);
    await expectAuthEnabledByDefault(pageA);
    await waitForLocalAuthTokens(pageA);

    await pageA.getByPlaceholder("Stored as payload bytes").fill("OLD-LABEL");
    await treeRowByNodeId(pageA, ROOT_ID).getByRole("button", { name: "Add child" }).click();
    const scopedRowA = treeRowByLabel(pageA, "OLD-LABEL");
    await expect(scopedRowA).toBeVisible({ timeout: 30_000 });

    const scopedNodeId = await scopedRowA.getAttribute("data-node-id");
    if (!scopedNodeId) throw new Error("expected scoped node id");
    const scopedByIdA = treeRowByNodeId(pageA, scopedNodeId);

    const privacyToggleA = scopedByIdA.getByRole("button", { name: "Toggle node privacy" });
    await privacyToggleA.click();
    await expect(privacyToggleA).toHaveAttribute("aria-pressed", "true");

    // Rename and immediately share via Open device. Share controls should wait until local write settles.
    await scopedByIdA.getByRole("button", { name: "OLD-LABEL", exact: true }).click();
    await scopedByIdA.getByRole("textbox").fill("NEW-LABEL");
    await scopedByIdA.getByRole("button", { name: "Save", exact: true }).click();

    await scopedByIdA.getByRole("button", { name: "Share subtree (invite)" }).click();
    const textarea = pageA.getByPlaceholder("Invite link will appear here…");
    await expect(textarea).toHaveValue(/invite=/, { timeout: 30_000 });

    const [pageB] = await Promise.all([
      pageA.waitForEvent("popup"),
      pageA.getByRole("button", { name: "Open device", exact: true }).click(),
    ]);

    await pageB.bringToFront();
    await expect(pageB.getByText("Ready (memory)")).toBeVisible({ timeout: 60_000 });

    const scopedByIdB = treeRowByNodeId(pageB, scopedNodeId);
    await expect(scopedByIdB.getByRole("button", { name: "NEW-LABEL", exact: true })).toBeVisible({
      timeout: 60_000,
    });
  } finally {
    await context.close();
  }
});

test("open device does not create an extra unknown member entry", async ({ browser }) => {
  test.setTimeout(240_000);

  const doc = uniqueDocId("pw-playground-open-device-members");
  const context = await browser.newContext();
  const pageA = await context.newPage();

  try {
    await waitForReady(pageA, `/?doc=${encodeURIComponent(doc)}`);
    await expectAuthEnabledByDefault(pageA);
    await waitForLocalAuthTokens(pageA);

    await pageA.getByPlaceholder("Stored as payload bytes").fill("OPEN-DEVICE-SECRET");
    await treeRowByNodeId(pageA, ROOT_ID).getByRole("button", { name: "Add child" }).click();
    const secretRowA = treeRowByLabel(pageA, "OPEN-DEVICE-SECRET");
    await expect(secretRowA).toBeVisible({ timeout: 30_000 });
    const secretNodeId = await secretRowA.getAttribute("data-node-id");
    if (!secretNodeId) throw new Error("expected secret node id");

    const privacyToggleA = treeRowByNodeId(pageA, secretNodeId).getByRole("button", { name: "Toggle node privacy" });
    await privacyToggleA.click();
    await expect(privacyToggleA).toHaveAttribute("aria-pressed", "true");

    await treeRowByNodeId(pageA, secretNodeId).getByRole("button", { name: "Share subtree (invite)" }).click();
    const textarea = pageA.getByPlaceholder("Invite link will appear here…");
    await expect(textarea).toHaveValue(/invite=/, { timeout: 30_000 });

    const [pageB] = await Promise.all([
      pageA.waitForEvent("popup"),
      pageA.getByRole("button", { name: "Open device", exact: true }).click(),
    ]);
    await pageA.getByRole("button", { name: "Close", exact: true }).click();

    await pageB.bringToFront();
    await expect(pageB.getByText("Ready (memory)")).toBeVisible({ timeout: 60_000 });
    await expect(treeRowByNodeId(pageB, secretNodeId)).toBeVisible({ timeout: 60_000 });

    // When Open device reuses the invite, A should only have one member entry for this subtree.
    await expectMembersBadgeCount(pageA, secretNodeId, 1);
  } finally {
    await context.close();
  }
});

test("delegated invite can be reshared (A → B → C) and sync is bidirectional", async ({ browser }) => {
  test.setTimeout(300_000);

  const doc = uniqueDocId("pw-playground-delegated");
  const profileB = `pw-deleg-b-${Date.now()}-${Math.random().toString(16).slice(2)}`;
  const profileC = `pw-deleg-c-${Date.now()}-${Math.random().toString(16).slice(2)}`;

  const context = await browser.newContext();
  const pageA = await context.newPage();
  const pageB = await context.newPage();
  const pageC = await context.newPage();

  try {
    await Promise.all([
      waitForReady(pageA, `/?doc=${encodeURIComponent(doc)}`),
      waitForReady(pageB, `/?doc=${encodeURIComponent(doc)}&profile=${encodeURIComponent(profileB)}&join=1`),
      waitForReady(pageC, `/?doc=${encodeURIComponent(doc)}&profile=${encodeURIComponent(profileC)}&join=1`),
    ]);

    await expectAuthEnabledByDefault(pageA);
    await waitForLocalAuthTokens(pageA);

    // Create public + private subtree root on A.
    await pageA.getByPlaceholder("Stored as payload bytes").fill("public");
    await treeRowByNodeId(pageA, ROOT_ID).getByRole("button", { name: "Add child" }).click();
    await expect(treeRowByLabel(pageA, "public")).toBeVisible({ timeout: 30_000 });

    await pageA.getByPlaceholder("Stored as payload bytes").fill("secret-root");
    await treeRowByNodeId(pageA, ROOT_ID).getByRole("button", { name: "Add child" }).click();
    const secretRootRowA = treeRowByLabel(pageA, "secret-root");
    await expect(secretRootRowA).toBeVisible({ timeout: 30_000 });

    const secretNodeId = await secretRootRowA.getAttribute("data-node-id");
    if (!secretNodeId) throw new Error("expected secret node id");

    const privacyToggleA = treeRowByNodeId(pageA, secretNodeId).getByRole("button", { name: "Toggle node privacy" });
    await privacyToggleA.click();
    await expect(privacyToggleA).toHaveAttribute("aria-pressed", "true");

    // A shares the secret subtree to B (invite includes grant so B can reshare).
    const inviteToB = await shareSubtreeInvite(pageA, secretNodeId);

    await joinViaInviteLink(pageB, inviteToB);

    await Promise.all([
      expect(pageA.getByRole("button", { name: "Sync", exact: true })).toBeEnabled({ timeout: 30_000 }),
      expect(pageB.getByRole("button", { name: "Sync", exact: true })).toBeEnabled({ timeout: 30_000 }),
    ]);

    // B should be able to pull the scoped root payload by syncing its own `children(scopeRoot)` filter.
    await clickSync(pageB, "B");
    await expect(treeRowByLabel(pageB, "secret-root")).toBeVisible({ timeout: 30_000 });

    const secretRootRowB = treeRowByNodeId(pageB, secretNodeId);
    await expect(secretRootRowB.getByText("scoped access", { exact: true })).toBeVisible({ timeout: 30_000 });
    await expect(secretRootRowB.getByText("private root", { exact: true })).toBeVisible({ timeout: 30_000 });

    // B reshared invite to C (delegated).
    const inviteToC = await shareSubtreeInvite(pageB, secretNodeId);

    await joinViaInviteLink(pageC, inviteToC);

    await Promise.all([
      expect(pageA.getByRole("button", { name: "Sync", exact: true })).toBeEnabled({ timeout: 30_000 }),
      expect(pageC.getByRole("button", { name: "Sync", exact: true })).toBeEnabled({ timeout: 30_000 }),
    ]);

    // C should also be able to pull the scoped root payload by syncing its own scope.
    const secretRootRowC = treeRowByNodeId(pageC, secretNodeId);
    let cRevealed = false;
    for (let attempt = 0; attempt < 24; attempt++) {
      await clickSyncBestEffortOnTransientAuthError(pageA, "A");
      await clickSyncBestEffortOnTransientAuthError(pageB, "B");
      await clickSyncBestEffortOnTransientAuthError(pageC, "C");
      if (await secretRootRowC.count()) {
        await expect(secretRootRowC).toBeVisible({ timeout: 5_000 });
        cRevealed = true;
        break;
      }
      await pageC.waitForTimeout(300);
    }
    if (!cRevealed) await expect(secretRootRowC).toBeVisible({ timeout: 30_000 });
    await expect(secretRootRowC.getByText("scoped access", { exact: true })).toBeVisible({ timeout: 30_000 });
    await expect(secretRootRowC.getByText("private root", { exact: true })).toBeVisible({ timeout: 30_000 });

    // A writes under the secret subtree; B and C should receive it.
    const secretRowAById = treeRowByNodeId(pageA, secretNodeId);
    await expandIfCollapsed(secretRowAById);
    await pageA.getByPlaceholder("Stored as payload bytes").fill("from-A");
    await secretRowAById.getByRole("button", { name: "Add child" }).click();
    await expect(treeRowByLabel(pageA, "from-A")).toBeVisible({ timeout: 30_000 });

    const fromARowB = treeRowByLabel(pageB, "from-A");
    const fromARowC = treeRowByLabel(pageC, "from-A");
    for (let attempt = 0; attempt < 12; attempt++) {
      await clickSyncBestEffortOnTransientAuthError(pageA, "A");
      if ((await fromARowB.isVisible()) && (await fromARowC.isVisible())) break;
      await clickSyncBestEffortOnTransientAuthError(pageB, "B");
      await clickSyncBestEffortOnTransientAuthError(pageC, "C");
      if ((await fromARowB.isVisible()) && (await fromARowC.isVisible())) break;
      await pageA.waitForTimeout(250);
    }
    await expect(fromARowB).toBeVisible({ timeout: 30_000 });
    await expect(fromARowC).toBeVisible({ timeout: 30_000 });

    // C writes under the secret subtree; A (and B) should receive it.
    const secretRowCById = treeRowByNodeId(pageC, secretNodeId);
    await pageC.getByPlaceholder("Stored as payload bytes").fill("from-C");
    await secretRowCById.getByRole("button", { name: "Add child" }).click();
    await expect(treeRowByLabel(pageC, "from-C")).toBeVisible({ timeout: 30_000 });

    await expandIfCollapsed(secretRowAById);
    const fromCRowA = treeRowByLabel(pageA, "from-C");
    const fromCRowB = treeRowByLabel(pageB, "from-C");
    for (let attempt = 0; attempt < 12; attempt++) {
      await clickSyncBestEffortOnTransientAuthError(pageA, "A");
      await expandIfCollapsed(secretRowAById);
      if ((await fromCRowA.isVisible()) && (await fromCRowB.isVisible())) break;
      await clickSyncBestEffortOnTransientAuthError(pageB, "B");
      await clickSyncBestEffortOnTransientAuthError(pageC, "C");
      await expandIfCollapsed(secretRowAById);
      if ((await fromCRowA.isVisible()) && (await fromCRowB.isVisible())) break;
      await pageA.waitForTimeout(250);
    }
    await expect(fromCRowA).toBeVisible({ timeout: 30_000 });
    await expect(fromCRowB).toBeVisible({ timeout: 30_000 });
  } finally {
    await context.close();
  }
});

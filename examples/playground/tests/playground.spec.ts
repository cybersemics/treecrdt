import { expect, test } from "@playwright/test";
import { createHash } from "node:crypto";
import http from "node:http";
import type { Operation } from "@treecrdt/interface";
import { deriveOpRefV0 } from "@treecrdt/sync-protocol";
import { treecrdtSyncV0ProtobufCodec } from "@treecrdt/sync-protocol/protobuf";
import { startWebSocketSyncServer } from "../../../packages/sync-protocol/server/core/dist/index.js";

const ROOT_ID = "00000000000000000000000000000000";
const WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
const REMOTE_PLACEHOLDER = "https://bootstrap-host or ws://localhost:8787";

type TestSyncServer = {
  host: string;
  wsUrl: string;
  close: () => Promise<void>;
};

type InMemorySyncServer = TestSyncServer & {
  waitForDocOps: (docId: string, minCount: number, timeoutMs?: number) => Promise<void>;
  waitForServedOps: (docId: string, minCount: number, timeoutMs?: number) => Promise<void>;
};

async function startMockSyncServer(): Promise<TestSyncServer> {
  const sockets = new Set<import("node:stream").Duplex>();
  const server = http.createServer((req, res) => {
    const url = new URL(req.url ?? "/", "http://localhost");
    if (url.pathname === "/health") {
      res.writeHead(200, { "content-type": "text/plain" });
      res.end("ok");
      return;
    }
    res.writeHead(404, { "content-type": "text/plain" });
    res.end("not found");
  });

  server.on("upgrade", (req, socket) => {
    const url = new URL(req.url ?? "/", "http://localhost");
    if (url.pathname !== "/sync") {
      socket.destroy();
      return;
    }
    if (!url.searchParams.get("docId")) {
      socket.destroy();
      return;
    }
    const key = req.headers["sec-websocket-key"];
    if (typeof key !== "string" || key.length === 0) {
      socket.destroy();
      return;
    }

    const accept = createHash("sha1")
      .update(`${key}${WS_GUID}`)
      .digest("base64");

    socket.write(
      [
        "HTTP/1.1 101 Switching Protocols",
        "Upgrade: websocket",
        "Connection: Upgrade",
        `Sec-WebSocket-Accept: ${accept}`,
        "",
        "",
      ].join("\r\n")
    );
    sockets.add(socket);
    socket.on("close", () => sockets.delete(socket));
    socket.on("error", () => {});
  });

  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => resolve());
  });

  const address = server.address();
  if (!address || typeof address === "string") {
    throw new Error("expected tcp server address");
  }
  const host = `127.0.0.1:${address.port}`;

  return {
    host,
    wsUrl: `ws://${host}/sync`,
    close: async () => {
      for (const socket of sockets) socket.destroy();
      await new Promise<void>((resolve) => server.close(() => resolve()));
    },
  };
}

type InMemoryDocState = {
  opsByRef: Map<string, { opRef: Uint8Array; op: Operation }>;
  maxLamport: number;
  servedOps: number;
  openCount: number;
};

function opRefHex(bytes: Uint8Array): string {
  return Buffer.from(bytes).toString("hex");
}

function opRefFor(docId: string, op: Operation): Uint8Array {
  return deriveOpRefV0(docId, { replica: op.meta.id.replica, counter: op.meta.id.counter });
}

async function startInMemorySyncServer(): Promise<InMemorySyncServer> {
  const docs = new Map<string, InMemoryDocState>();
  const server = await startWebSocketSyncServer<Operation>({
    host: "127.0.0.1",
    port: 0,
    codec: treecrdtSyncV0ProtobufCodec,
    docs: {
      async open(docId) {
        let state = docs.get(docId);
        if (!state) {
          state = {
            opsByRef: new Map(),
            maxLamport: 0,
            servedOps: 0,
            openCount: 0,
          };
          docs.set(docId, state);
        }
        state.openCount += 1;

        return {
          backend: {
            docId,
            maxLamport: async () => BigInt(state.maxLamport),
            listOpRefs: async () => Array.from(state.opsByRef.values(), (entry) => entry.opRef),
            getOpsByOpRefs: async (opRefs) => {
              const found = opRefs
                .map((opRef) => state!.opsByRef.get(opRefHex(opRef))?.op)
                .filter((op): op is Operation => Boolean(op));
              state!.servedOps += found.length;
              return found;
            },
            applyOps: async (ops) => {
              for (const op of ops) {
                const opRef = opRefFor(docId, op);
                const key = opRefHex(opRef);
                if (!state!.opsByRef.has(key)) {
                  state!.opsByRef.set(key, { opRef, op });
                }
                state!.maxLamport = Math.max(state!.maxLamport, op.meta.lamport);
              }
            },
          },
          release: () => {
            state!.openCount = Math.max(0, state!.openCount - 1);
            if (state!.openCount === 0 && state!.opsByRef.size === 0) {
              docs.delete(docId);
            }
          },
        };
      },
    },
  });

  return {
    host: `${server.host}:${server.port}`,
    wsUrl: `ws://${server.host}:${server.port}/sync`,
    waitForDocOps: async (docId, minCount, timeoutMs = 30_000) => {
      const deadline = Date.now() + timeoutMs;
      for (;;) {
        const count = docs.get(docId)?.opsByRef.size ?? 0;
        if (count >= minCount) return;
        if (Date.now() >= deadline) {
          throw new Error(`timed out waiting for ${minCount} ops in ${docId}; saw ${count}`);
        }
        await new Promise((resolve) => setTimeout(resolve, 50));
      }
    },
    waitForServedOps: async (docId, minCount, timeoutMs = 30_000) => {
      const deadline = Date.now() + timeoutMs;
      for (;;) {
        const count = docs.get(docId)?.servedOps ?? 0;
        if (count >= minCount) return;
        if (Date.now() >= deadline) {
          throw new Error(`timed out waiting for ${minCount} served ops in ${docId}; saw ${count}`);
        }
        await new Promise((resolve) => setTimeout(resolve, 50));
      }
    },
    close: server.close,
  };
}

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

async function enableRevealIdentity(page: import("@playwright/test").Page) {
  await ensureAuthAdvancedOpen(page);
  const identityToggle = page.getByRole("button", { name: "Private", exact: true });
  await identityToggle.click();
  await expect(page.getByRole("button", { name: "Revealing", exact: true })).toBeVisible({ timeout: 30_000 });
  await page.getByRole("button", { name: "Auth", exact: true }).click();
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

async function shareSubtreeInvite(page: import("@playwright/test").Page, nodeId: string): Promise<string> {
  await treeRowByNodeId(page, nodeId).getByRole("button", { name: "Share subtree (invite)" }).click();
  const textarea = page.getByPlaceholder("Invite link will appear here…");
  await expect(textarea).toHaveValue(/invite=/, { timeout: 30_000 });
  const link = await textarea.inputValue();
  await page.getByRole("button", { name: "Close", exact: true }).click();
  return link;
}

async function joinViaInviteLink(page: import("@playwright/test").Page, inviteLink: string) {
  const inviteUrl = new URL(inviteLink, "http://localhost");
  // Preserve per-tab storage isolation when the receiving tab was opened with a profile.
  // Without this, invite URLs can collapse tabs into the default profile and race auth token state.
  const currentUrl = page.url();
  if (currentUrl && currentUrl !== "about:blank") {
    const profile = new URL(currentUrl, "http://localhost").searchParams.get("profile");
    if (profile) inviteUrl.searchParams.set("profile", profile);
  }
  await waitForReady(page, inviteUrl.toString());
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

test("playground mints a fresh default doc", async ({ browser }) => {
  const freshProfile = `pw-doc-fresh-${Date.now()}-${Math.random().toString(16).slice(2)}`;
  const freshContext = await browser.newContext();
  const freshPage = await freshContext.newPage();

  try {
    await waitForReady(freshPage, `/?profile=${encodeURIComponent(freshProfile)}`);

    const freshDoc = new URL(freshPage.url()).searchParams.get("doc");
    expect(freshDoc).toMatch(/^treecrdt-playground-[0-9a-f]{16}$/);
    expect(freshDoc).not.toBe("treecrdt-playground");

    const storedFreshDoc = await freshPage.evaluate((profile) => {
      return window.localStorage.getItem(`treecrdt-playground-profile:${profile}:treecrdt-playground-doc`);
    }, freshProfile);
    expect(storedFreshDoc).toBe(freshDoc);
  } finally {
    await freshContext.close();
  }

});

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

test("live payload editing commits each keystroke without clobbering the draft", async ({ page }) => {
  test.setTimeout(90_000);

  const doc = uniqueDocId("pw-playground-live-payload");
  await waitForReady(page, `/?doc=${encodeURIComponent(doc)}`);
  await expectAuthEnabledByDefault(page);
  await waitForLocalAuthTokens(page);

  await page.getByPlaceholder("Stored as payload bytes").fill("seed");
  await treeRowByNodeId(page, ROOT_ID).getByRole("button", { name: "Add child" }).click();

  const seedRow = treeRowByLabel(page, "seed");
  await expect(seedRow).toBeVisible({ timeout: 30_000 });
  const nodeId = await seedRow.getAttribute("data-node-id");
  expect(nodeId).toBeTruthy();

  const row = treeRowByNodeId(page, nodeId!);
  await row.getByRole("button", { name: "seed" }).click();
  const input = row.getByRole("textbox");
  await expect(input).toHaveValue("seed", { timeout: 30_000 });

  await input.fill("");
  await input.pressSequentially("abcdef", { delay: 1 });
  await expect(input).toHaveValue("abcdef");

  await row.getByRole("button", { name: "Save" }).click();
  await expect(treeRowByNodeId(page, nodeId!).getByRole("button", { name: "abcdef" })).toBeVisible({ timeout: 30_000 });

  await page.getByTitle("Toggle operations panel").click();
  const opsPanel = page.locator("aside", { hasText: "Operations" });
  await expect(opsPanel.getByText("Ops: 8")).toBeVisible({ timeout: 30_000 });
});

test("switching remote sync server URL reconnects to the new endpoint", async ({ page }) => {
  test.setTimeout(120_000);

  const doc = uniqueDocId("pw-playground-sync-switch");
  const serverA = await startMockSyncServer();
  const serverB = await startMockSyncServer();

  try {
    await waitForReady(page, `/?doc=${encodeURIComponent(doc)}`);

    await page.getByRole("button", { name: /Connections/ }).click();
    const remoteInput = page.getByPlaceholder(REMOTE_PLACEHOLDER);
    await expect(remoteInput).toBeVisible({ timeout: 30_000 });

    await remoteInput.fill(serverA.wsUrl);
    await expect(page.getByText(`remote(${serverA.host})`)).toBeVisible({ timeout: 30_000 });

    await remoteInput.fill(serverB.wsUrl);
    await expect(page.getByText(`remote(${serverB.host})`)).toBeVisible({ timeout: 30_000 });
    await expect(page.getByText(`remote(${serverA.host})`)).toHaveCount(0, { timeout: 30_000 });

    await page.getByRole("button", { name: "Clear", exact: true }).click();
    await expect(page.getByText(`remote(${serverB.host})`)).toHaveCount(0, { timeout: 30_000 });
  } finally {
    await Promise.all([serverA.close(), serverB.close()]);
  }
});

test("switching to remote transport does not auto-fill a default sync URL", async ({ page }) => {
  const doc = uniqueDocId("pw-playground-sync-no-default");
  await waitForReady(page, `/?doc=${encodeURIComponent(doc)}`);

  await page.getByRole("button", { name: /Connections/ }).click();
  const remoteInput = page.getByPlaceholder(REMOTE_PLACEHOLDER);
  await expect(remoteInput).toHaveValue("");

  await page.getByRole("button", { name: "Remote server", exact: true }).click();
  await expect(remoteInput).toHaveValue("");
  await expect(page.getByText("Missing URL")).toBeVisible({ timeout: 30_000 });

  await expect
    .poll(async () => {
      const url = new URL(page.url());
      return {
        sync: url.searchParams.get("sync"),
        transport: url.searchParams.get("transport"),
      };
    })
    .toEqual({ sync: null, transport: "remote" });
});

test("remote sync settings persist into a shareable URL", async ({ browser, page }) => {
  test.setTimeout(120_000);

  const doc = uniqueDocId("pw-playground-sync-share");
  const server = await startMockSyncServer();
  const sharedContext = await browser.newContext();

  try {
    await waitForReady(page, `/?doc=${encodeURIComponent(doc)}`);

    await page.getByRole("button", { name: /Connections/ }).click();
    const remoteInput = page.getByPlaceholder(REMOTE_PLACEHOLDER);
    await expect(remoteInput).toBeVisible({ timeout: 30_000 });

    await page.getByRole("button", { name: "Remote server", exact: true }).click();
    await remoteInput.fill(server.wsUrl);

    await expect
      .poll(async () => {
        const url = new URL(page.url());
        return {
          sync: url.searchParams.get("sync"),
          transport: url.searchParams.get("transport"),
        };
      })
      .toEqual({ sync: server.wsUrl, transport: "remote" });

    const sharedPage = await sharedContext.newPage();
    await waitForReady(sharedPage, page.url());
    await sharedPage.getByRole("button", { name: /Connections/ }).click();
    await expect(sharedPage.getByPlaceholder(REMOTE_PLACEHOLDER)).toHaveValue(
      server.wsUrl,
      { timeout: 30_000 }
    );
    await expect(sharedPage.getByText(`remote(${server.host})`)).toBeVisible({ timeout: 30_000 });
  } finally {
    await sharedContext.close();
    await server.close();
  }
});

test("invite link preserves auth material and remote sync settings", async ({ browser }) => {
  test.setTimeout(120_000);

  const doc = uniqueDocId("pw-playground-new-device-remote");
  const server = await startMockSyncServer();
  const context = await browser.newContext();
  const pageA = await context.newPage();
  const pageB = await context.newPage();

  try {
    await waitForReady(pageA, `/?doc=${encodeURIComponent(doc)}`);
    await expectAuthEnabledByDefault(pageA);
    await waitForLocalAuthTokens(pageA);

    await pageA.getByRole("button", { name: /Connections/ }).click();
    const remoteInput = pageA.getByPlaceholder(REMOTE_PLACEHOLDER);
    await expect(remoteInput).toBeVisible({ timeout: 30_000 });
    await pageA.getByRole("button", { name: "Remote server", exact: true }).click();
    await remoteInput.fill(server.wsUrl);

    await expect
      .poll(async () => {
        const url = new URL(pageA.url());
        return {
          sync: url.searchParams.get("sync"),
          transport: url.searchParams.get("transport"),
        };
      })
      .toEqual({ sync: server.wsUrl, transport: "remote" });

    const inviteLink = await shareSubtreeInvite(pageA, ROOT_ID);
    const inviteUrl = new URL(inviteLink);
    expect(inviteUrl.searchParams.get("doc")).toBe(doc);
    expect(inviteUrl.searchParams.get("join")).toBe("1");
    expect(inviteUrl.searchParams.get("auth")).toBe("1");
    expect(inviteUrl.searchParams.get("sync")).toBe(server.wsUrl);
    expect(inviteUrl.searchParams.get("transport")).toBe("remote");
    expect(inviteUrl.hash).toMatch(/^#invite=/);

    await joinViaInviteLink(pageB, inviteLink);
    await pageB.getByRole("button", { name: /Connections/ }).click();
    await expect(pageB.getByPlaceholder(REMOTE_PLACEHOLDER)).toHaveValue(server.wsUrl, {
      timeout: 30_000,
    });
    await expect(pageB.getByText(`remote(${server.host})`)).toBeVisible({ timeout: 30_000 });
  } finally {
    await context.close();
    await server.close();
  }
});

test("remote sync server transfers ops between isolated pages", async ({ browser }) => {
  test.setTimeout(120_000);

  const doc = uniqueDocId("pw-playground-remote-sync");
  const server = await startInMemorySyncServer();
  const contextA = await browser.newContext();
  const contextB = await browser.newContext();
  const pageA = await contextA.newPage();
  const pageB = await contextB.newPage();
  const remotePath = `/?doc=${encodeURIComponent(doc)}&auth=0&transport=remote&sync=${encodeURIComponent(server.wsUrl)}`;

  try {
    await Promise.all([waitForReady(pageA, remotePath), waitForReady(pageB, remotePath)]);

    await Promise.all([
      expect(pageA.getByRole("button", { name: "Sync", exact: true })).toBeEnabled({ timeout: 30_000 }),
      expect(pageB.getByRole("button", { name: "Sync", exact: true })).toBeEnabled({ timeout: 30_000 }),
    ]);

    await pageA.getByRole("button", { name: /Connections/ }).click();
    await expect(pageA.getByText(`remote(${server.host})`)).toBeVisible({ timeout: 30_000 });

    await pageB.getByRole("button", { name: /Connections/ }).click();
    await expect(pageB.getByText(`remote(${server.host})`)).toBeVisible({ timeout: 30_000 });

    const nodeLabel = "remote-server-child";
    await pageA.getByPlaceholder("Stored as payload bytes").fill(nodeLabel);
    await treeRowByNodeId(pageA, ROOT_ID).getByRole("button", { name: "Add child" }).click();
    await expect(treeRowByLabel(pageA, nodeLabel)).toBeVisible({ timeout: 30_000 });

    const syncErrorA = pageA.getByTestId("sync-error");
    await pageA.getByRole("button", { name: "Sync", exact: true }).click();
    await Promise.race([
      server.waitForDocOps(doc, 1),
      (async () => {
        await syncErrorA.waitFor({ state: "visible", timeout: 30_000 });
        throw new Error(`sync error (A): ${await syncErrorA.textContent()}`);
      })(),
    ]);

    const syncErrorB = pageB.getByTestId("sync-error");
    await pageB.getByRole("button", { name: "Sync", exact: true }).click();
    await Promise.race([
      server.waitForServedOps(doc, 1),
      (async () => {
        await syncErrorB.waitFor({ state: "visible", timeout: 30_000 });
        throw new Error(`sync error (B): ${await syncErrorB.textContent()}`);
      })(),
    ]);

    await pageB.getByTitle("Toggle operations panel").click();
    await expect(pageB.getByText(/Head lamport:\s*[1-9]/)).toBeVisible({ timeout: 30_000 });
  } finally {
    await Promise.all([contextA.close(), contextB.close(), server.close()]);
  }
});

test("remote sync server handles 1000-node composer fanout between same-device pages", async ({ browser }) => {
  test.setTimeout(240_000);

  const doc = uniqueDocId("pw-playground-remote-fanout");
  const server = await startInMemorySyncServer();
  const context = await browser.newContext();
  const pageA = await context.newPage();
  const pageB = await context.newPage();
  const remotePath = `/?doc=${encodeURIComponent(doc)}&auth=0&transport=remote&sync=${encodeURIComponent(server.wsUrl)}`;

  try {
    await Promise.all([waitForReady(pageA, remotePath), waitForReady(pageB, remotePath)]);

    await pageA.getByPlaceholder("Stored as payload bytes").fill("fanout");
    await pageA.locator('label:has-text("Node count") input[type="number"]').fill("1000");
    await pageA.locator('label:has-text("Fanout") select').selectOption("10");

    await pageA.getByRole("button", { name: "Add nodes", exact: true }).click();
    await expect(pageA.getByText(/1000 nodes/)).toBeVisible({ timeout: 120_000 });

    await clickSync(pageA, "A");
    await clickSync(pageB, "B");

    await expect(pageB.getByText(/1000 nodes/)).toBeVisible({ timeout: 120_000 });
    await pageB.getByTitle("Toggle operations panel").click();
    await expect(pageB.getByText(/Head lamport:\s*1000/)).toBeVisible({ timeout: 120_000 });
    await pageB.getByRole("button", { name: "Expand", exact: true }).click();
    await expect(pageB.getByRole("button", { name: "fanout 1", exact: true })).toBeVisible({ timeout: 30_000 });
    await expect(pageB.getByRole("button", { name: "fanout 10", exact: true })).toBeVisible({ timeout: 30_000 });
  } finally {
    await Promise.all([context.close(), server.close()]);
  }
});

test("remote live sync all pushes new ops without a manual sync click", async ({ browser }) => {
  test.setTimeout(180_000);

  const doc = uniqueDocId("pw-playground-remote-live-all");
  const server = await startInMemorySyncServer();
  const context = await browser.newContext();
  const pageA = await context.newPage();
  const pageB = await context.newPage();
  const remotePath = `/?doc=${encodeURIComponent(doc)}&auth=0&transport=remote&sync=${encodeURIComponent(server.wsUrl)}`;

  try {
    await Promise.all([waitForReady(pageA, remotePath), waitForReady(pageB, remotePath)]);

    await Promise.all([
      expect(pageA.getByRole("button", { name: "Sync", exact: true })).toBeEnabled({ timeout: 30_000 }),
      expect(pageB.getByRole("button", { name: "Sync", exact: true })).toBeEnabled({ timeout: 30_000 }),
    ]);

    const liveA = pageA.getByRole("button", { name: "Live sync all" });
    const liveB = pageB.getByRole("button", { name: "Live sync all" });
    await liveA.click();
    await liveB.click();
    await expect(liveA).toHaveAttribute("aria-pressed", "true");
    await expect(liveB).toHaveAttribute("aria-pressed", "true");
    await expect(liveA).toHaveAttribute("aria-busy", "false", { timeout: 30_000 });
    await expect(liveB).toHaveAttribute("aria-busy", "false", { timeout: 30_000 });

    const nodeLabel = `remote-live-all-${Date.now()}`;
    await pageA.getByPlaceholder("Stored as payload bytes").fill(nodeLabel);
    await treeRowByNodeId(pageA, ROOT_ID).getByRole("button", { name: "Add child" }).click();
    await expect(treeRowByLabel(pageA, nodeLabel)).toBeVisible({ timeout: 30_000 });
    await Promise.race([
      server.waitForDocOps(doc, 1),
      (async () => {
        await pageA.getByTestId("sync-error").waitFor({ state: "visible", timeout: 30_000 });
        throw new Error(`sync error (A): ${await pageA.getByTestId("sync-error").textContent()}`);
      })(),
    ]);
    await Promise.race([
      expect(treeRowByLabel(pageB, nodeLabel)).toBeVisible({ timeout: 60_000 }),
      (async () => {
        await pageB.getByTestId("sync-error").waitFor({ state: "visible", timeout: 60_000 });
        throw new Error(`sync error (B): ${await pageB.getByTestId("sync-error").textContent()}`);
      })(),
    ]);

    await expect(pageA.getByTestId("sync-error")).toBeHidden();
    await expect(pageB.getByTestId("sync-error")).toBeHidden();
  } finally {
    await Promise.all([context.close(), server.close()]);
  }
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

test("scoped invite sync fails closed without revealing private data", async ({ browser }) => {
  test.setTimeout(180_000);

  const doc = uniqueDocId("pw-playground-scoped-fail-closed");
  const profile = `pw-scoped-fail-closed-${Date.now()}-${Math.random().toString(16).slice(2)}`;
  const context = await browser.newContext();
  const owner = await context.newPage();
  const guest = await context.newPage();

  try {
    await Promise.all([
      waitForReady(owner, `/?doc=${encodeURIComponent(doc)}`),
      waitForReady(
        guest,
        `/?doc=${encodeURIComponent(doc)}&profile=${encodeURIComponent(profile)}&join=1`
      ),
    ]);
    await expectAuthEnabledByDefault(owner);

    await owner.getByPlaceholder("Stored as payload bytes").fill("public");
    await treeRowByNodeId(owner, ROOT_ID).getByRole("button", { name: "Add child" }).click();
    await expect(treeRowByLabel(owner, "public")).toBeVisible({ timeout: 30_000 });

    await owner.getByPlaceholder("Stored as payload bytes").fill("private");
    await treeRowByNodeId(owner, ROOT_ID).getByRole("button", { name: "Add child" }).click();
    const privateRow = treeRowByLabel(owner, "private");
    await expect(privateRow).toBeVisible({ timeout: 30_000 });
    const privateNodeId = await privateRow.getAttribute("data-node-id");
    if (!privateNodeId) throw new Error("expected private node id");

    const privacyToggle = privateRow.getByRole("button", { name: "Toggle node privacy" });
    await privacyToggle.click();
    await expect(privacyToggle).toHaveAttribute("aria-pressed", "true");

    const inviteLink = await shareSubtreeInvite(owner, ROOT_ID);
    await joinViaInviteLink(guest, inviteLink);

    const ownerSync = owner.getByRole("button", { name: "Sync", exact: true });
    await expect(ownerSync).toBeEnabled({ timeout: 30_000 });
    await ownerSync.click();
    await expect(owner.getByTestId("sync-error")).toContainText(
      "capability does not allow operation-log projection",
      { timeout: 30_000 }
    );

    await expect(treeRowByLabel(guest, "public")).toHaveCount(0);
    await expect(treeRowByNodeId(guest, privateNodeId)).toHaveCount(0);
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

test("new device can sync historical authors learned by the inviter", async ({ browser }) => {
  test.setTimeout(240_000);

  const doc = uniqueDocId("pw-playground-historical-authors");
  const profileB = `pw-hist-b-${Date.now()}-${Math.random().toString(16).slice(2)}`;
  const profileC = `pw-hist-c-${Date.now()}-${Math.random().toString(16).slice(2)}`;
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

    const inviteToB = await shareSubtreeInvite(pageA, ROOT_ID);
    await joinViaInviteLink(pageB, inviteToB);

    await Promise.all([
      expect(pageA.getByRole("button", { name: "Sync", exact: true })).toBeEnabled({ timeout: 30_000 }),
      expect(pageB.getByRole("button", { name: "Sync", exact: true })).toBeEnabled({ timeout: 30_000 }),
    ]);

    await clickSyncWithRetryOnTransientAuthError(pageA, "A");
    await clickSyncWithRetryOnTransientAuthError(pageB, "B");

    const historicalLabel = `from-b-before-reload-${Date.now()}`;
    await pageB.getByPlaceholder("Stored as payload bytes").fill(historicalLabel);
    await treeRowByNodeId(pageB, ROOT_ID).getByRole("button", { name: "Add child" }).click();
    await expect(treeRowByLabel(pageB, historicalLabel)).toBeVisible({ timeout: 30_000 });

    await clickSyncWithRetryOnTransientAuthError(pageB, "B");
    await clickSyncWithRetryOnTransientAuthError(pageA, "A");
    await expect(treeRowByLabel(pageA, historicalLabel)).toBeVisible({ timeout: 30_000 });

    await pageB.close();

    const inviteToC = await shareSubtreeInvite(pageA, ROOT_ID);
    await joinViaInviteLink(pageC, inviteToC);

    await Promise.all([
      expect(pageA.getByRole("button", { name: "Sync", exact: true })).toBeEnabled({ timeout: 30_000 }),
      expect(pageC.getByRole("button", { name: "Sync", exact: true })).toBeEnabled({ timeout: 30_000 }),
    ]);

    for (let attempt = 0; attempt < 8; attempt += 1) {
      await clickSyncWithRetryOnTransientAuthError(pageA, "A");
      await clickSyncWithRetryOnTransientAuthError(pageC, "C");
      if (await treeRowByLabel(pageC, historicalLabel).count()) break;
      await pageC.waitForTimeout(300);
    }

    await expect(treeRowByLabel(pageC, historicalLabel)).toBeVisible({ timeout: 30_000 });
  } finally {
    await context.close();
  }
});

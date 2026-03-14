import { expect, test, vi } from "vitest";

import type { Operation } from "@treecrdt/interface";
import type { SyncBackend } from "@treecrdt/sync";

import { createPostgresNodeDocStore } from "../dist/server.js";

type Deferred<T> = {
  promise: Promise<T>;
  resolve: (value: T) => void;
  reject: (err: unknown) => void;
};

function deferred<T>(): Deferred<T> {
  let resolve!: (value: T) => void;
  let reject!: (err: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

function makeBackend(docId: string, overrides: Partial<SyncBackend<Operation>> = {}): SyncBackend<Operation> {
  return {
    docId,
    maxLamport: async () => BigInt(0),
    listOpRefs: async () => [],
    getOpsByOpRefs: async () => [],
    applyOps: async () => {},
    ...overrides,
  };
}

test("doc store uses single-flight open for concurrent requests", async () => {
  const gate = deferred<void>();
  let opens = 0;

  const store = createPostgresNodeDocStore({
    idleCloseMs: 60_000,
    backendFactory: {
      open: async (docId) => {
        opens += 1;
        await gate.promise;
        return makeBackend(docId);
      },
    },
  });

  const p1 = store.provider.open("doc-single-flight");
  const p2 = store.provider.open("doc-single-flight");
  await Promise.resolve();
  expect(opens).toBe(1);

  gate.resolve();
  const [h1, h2] = await Promise.all([p1, p2]);
  expect(h1.backend.docId).toBe("doc-single-flight");
  expect(h2.backend.docId).toBe("doc-single-flight");

  await Promise.all([h1.release?.(), h2.release?.()]);
  await store.closeAll();
});

test("apply queue remains usable after a failed append", async () => {
  let applyCalls = 0;
  const store = createPostgresNodeDocStore({
    idleCloseMs: 60_000,
    backendFactory: {
      open: async (docId) =>
        makeBackend(docId, {
          applyOps: async () => {
            applyCalls += 1;
            if (applyCalls === 1) throw new Error("boom");
          },
        }),
    },
  });

  const handle = await store.provider.open("doc-apply-queue");
  await expect(handle.backend.applyOps([{} as Operation])).rejects.toThrow("boom");
  await expect(handle.backend.applyOps([{} as Operation])).resolves.toBeUndefined();
  expect(applyCalls).toBe(2);

  await handle.release?.();
  await store.closeAll();
});

test("notifyDocUpdate triggers peer updates for active docs", async () => {
  const store = createPostgresNodeDocStore({
    idleCloseMs: 60_000,
    backendFactory: {
      open: async (docId) => makeBackend(docId),
    },
  });

  const handle = await store.provider.open("doc-notify");
  const notifyLocalUpdate = vi.fn(() => Promise.resolve());
  handle.onPeerAdded?.({ notifyLocalUpdate } as any);

  store.notifyDocUpdate("doc-notify");
  expect(notifyLocalUpdate).toHaveBeenCalledTimes(1);

  await handle.release?.();
  await store.closeAll();
});

test("doc store passes peer options through to opened handles", async () => {
  const peerOptions = { requireAuthForFilters: false } as const;
  const store = createPostgresNodeDocStore({
    idleCloseMs: 60_000,
    backendFactory: {
      open: async (docId) => makeBackend(docId),
    },
    peerOptionsFactory: async () => peerOptions,
  });

  const handle = await store.provider.open("doc-peer-options");
  expect(handle.peerOptions).toBe(peerOptions);

  await handle.release?.();
  await store.closeAll();
});

test("closeAll waits for in-flight opens and closes resolved backends immediately", async () => {
  const gate = deferred<void>();
  const backendClosed = deferred<void>();
  let closeCalls = 0;

  const store = createPostgresNodeDocStore({
    idleCloseMs: 60_000,
    backendFactory: {
      open: async (docId) => {
        await gate.promise;
        const backend = makeBackend(docId) as SyncBackend<Operation> & { close: () => Promise<void> };
        backend.close = async () => {
          closeCalls += 1;
          backendClosed.resolve();
        };
        return backend;
      },
    },
  });

  const opening = store.provider.open("doc-close-during-open");
  await Promise.resolve();

  let closedAll = false;
  const shutdown = store.closeAll().then(() => {
    closedAll = true;
  });
  await Promise.resolve();
  expect(closedAll).toBe(false);

  gate.resolve();

  await expect(opening).rejects.toThrow("doc store is closing");
  await shutdown;
  await backendClosed.promise;
  expect(closeCalls).toBe(1);
});

import { expect, test, vi } from "vitest";

import type { Operation } from "@treecrdt/interface";
import { bytesToHex } from "@treecrdt/interface/ids";
import { createReplayOnlySyncAuth } from "@treecrdt/sync";
import type { SyncBackend } from "@treecrdt/sync";
import type { Capability, OpAuth, OpRef } from "@treecrdt/sync";
import { deriveOpRefV0 } from "@treecrdt/sync";

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

test("replay-only auth stores verified auth and replays it by op ref", async () => {
  const stored = new Map<string, OpAuth>();
  const capabilities = new Map<string, Capability>();
  const store = {
    storeOpAuth: vi.fn(async (entries: Array<{ opRef: OpRef; auth: OpAuth }>) => {
      for (const entry of entries) {
        stored.set(bytesToHex(entry.opRef), entry.auth);
      }
    }),
    getOpAuthByOpRefs: vi.fn(async (opRefs: OpRef[]) => opRefs.map((opRef) => stored.get(bytesToHex(opRef)) ?? null)),
  };
  const auth = createReplayOnlySyncAuth({
    docId: "doc-replay-auth",
    authMaterialStore: {
      opAuth: store,
      capabilities: {
        storeCapabilities: async (caps) => {
          for (const cap of caps) {
            capabilities.set(`${cap.name}:${cap.value}`, cap);
          }
        },
        listCapabilities: async () => Array.from(capabilities.values()),
      },
    },
  });

  const replica = new Uint8Array(32).fill(7);
  const op: Operation = {
    meta: {
      id: { replica, counter: 1 },
      lamport: 1,
    },
    kind: {
      type: "insert",
      parent: "0".repeat(32),
      node: "1".repeat(32),
      orderKey: new Uint8Array([1]),
    },
  };
  const opAuth: OpAuth = {
    sig: new Uint8Array(64).fill(9),
    proofRef: new Uint8Array(16).fill(3),
  };
  const cap: Capability = {
    name: "auth.capability",
    value: "token-1",
  };

  const ackCaps = await auth.onHello?.(
    { capabilities: [cap], filters: [], maxLamport: 0n },
    { docId: "doc-replay-auth" }
  );
  expect(ackCaps).toEqual([cap]);
  expect(await auth.helloCapabilities?.({ docId: "doc-replay-auth" })).toEqual([cap]);

  await auth.verifyOps?.([op], [opAuth], {
    docId: "doc-replay-auth",
    purpose: "reconcile",
    filterId: "filter-1",
  });
  await auth.onVerifiedOps?.([op], [opAuth], {
    docId: "doc-replay-auth",
    purpose: "reconcile",
    filterId: "filter-1",
  });

  expect(store.storeOpAuth).toHaveBeenCalledTimes(1);
  const opRef = deriveOpRefV0("doc-replay-auth", { replica, counter: 1 });
  const replayed = await auth.signOps?.([op], {
    docId: "doc-replay-auth",
    purpose: "subscribe",
    filterId: "sub-1",
  });
  expect(store.getOpAuthByOpRefs).toHaveBeenCalledWith([opRef]);
  expect(replayed).toEqual([opAuth]);
});

test("replay-only auth fails fast when auth sidecar is missing", async () => {
  const auth = createReplayOnlySyncAuth({
    docId: "doc-missing-auth",
    authMaterialStore: {
      opAuth: {
        storeOpAuth: async () => {},
        getOpAuthByOpRefs: async () => [null],
      },
    },
  });

  const replica = new Uint8Array(32).fill(4);
  const op: Operation = {
    meta: {
      id: { replica, counter: 1 },
      lamport: 1,
    },
    kind: {
      type: "insert",
      parent: "0".repeat(32),
      node: "2".repeat(32),
      orderKey: new Uint8Array([2]),
    },
  };

  await expect(
    auth.signOps?.([op], {
      docId: "doc-missing-auth",
      purpose: "reconcile",
      filterId: "filter-2",
    })
  ).rejects.toThrow("missing op auth for non-local replica");
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

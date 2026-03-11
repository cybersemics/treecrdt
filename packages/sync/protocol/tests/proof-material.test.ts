import { expect, test, vi } from "vitest";

import type { Operation } from "@treecrdt/interface";
import { bytesToHex } from "@treecrdt/interface/ids";

import { createReplayOnlySyncAuth, deriveOpRefV0 } from "../dist/index.js";
import type { Capability, OpAuth, OpRef } from "../dist/types.js";

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

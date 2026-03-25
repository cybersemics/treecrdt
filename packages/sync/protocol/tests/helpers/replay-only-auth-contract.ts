import { randomUUID } from "node:crypto";

import { expect, test } from "vitest";

import type { Operation } from "@treecrdt/interface";
import { bytesToHex } from "@treecrdt/interface/ids";
import {
  createReplayOnlySyncAuth,
  deriveOpRefV0,
  type Filter,
  type OpAuth,
  type OpRef,
  type SyncAuth,
  type SyncBackend,
} from "@treecrdt/sync";
import { createInMemoryConnectedPeers } from "@treecrdt/sync/in-memory";
import { treecrdtSyncV0ProtobufCodec } from "@treecrdt/sync/protobuf";
import type { Capability, SyncAuthMaterialStore } from "@treecrdt/sync";

type ReplayAuthHarness = {
  createDocStores: (
    docId: string
  ) => Promise<Pick<SyncAuthMaterialStore<Operation>, "opAuth" | "capabilities">> | Pick<SyncAuthMaterialStore<Operation>, "opAuth" | "capabilities">;
  close?: () => Promise<void> | void;
};

class SimpleMemoryBackend implements SyncBackend<Operation> {
  readonly docId: string;

  private maxLamportValue = 0n;
  private readonly opsByRefHex = new Map<string, { opRef: OpRef; op: Operation }>();

  constructor(docId: string) {
    this.docId = docId;
  }

  private opRefFor(op: Operation): OpRef {
    return deriveOpRefV0(this.docId, {
      replica: op.meta.id.replica,
      counter: BigInt(op.meta.id.counter),
    });
  }

  hasOp(replicaHex: string, counter: number): boolean {
    return Array.from(this.opsByRefHex.values()).some(
      (entry) => bytesToHex(entry.op.meta.id.replica) === replicaHex && entry.op.meta.id.counter === counter
    );
  }

  async maxLamport(): Promise<bigint> {
    return this.maxLamportValue;
  }

  async listOpRefs(filter: Filter): Promise<OpRef[]> {
    if (!("all" in filter)) throw new Error("SimpleMemoryBackend only supports filter(all)");
    return Array.from(this.opsByRefHex.values(), (entry) => entry.opRef);
  }

  async getOpsByOpRefs(opRefs: OpRef[]): Promise<Operation[]> {
    return opRefs.flatMap((opRef) => {
      const found = this.opsByRefHex.get(bytesToHex(opRef));
      return found ? [found.op] : [];
    });
  }

  async applyOps(ops: Operation[]): Promise<void> {
    for (const op of ops) {
      const opRef = this.opRefFor(op);
      this.opsByRefHex.set(bytesToHex(opRef), { opRef, op });
      const lamport = BigInt(op.meta.lamport);
      if (lamport > this.maxLamportValue) this.maxLamportValue = lamport;
    }
  }
}

function makeReplica(fill: number): Uint8Array {
  return new Uint8Array(32).fill(fill);
}

function makeInsertOp(replicaFill: number, counter: number): Operation {
  return {
    meta: {
      id: { replica: makeReplica(replicaFill), counter },
      lamport: counter,
    },
    kind: {
      type: "insert",
      parent: "0".repeat(32),
      node: counter.toString(16).padStart(32, "0").slice(-32),
      orderKey: new Uint8Array([counter & 0xff]),
    },
  };
}

function normalizeOpAuth(value: OpAuth): OpAuth {
  return {
    sig: Uint8Array.from(value.sig),
    ...(value.proofRef ? { proofRef: Uint8Array.from(value.proofRef) } : {}),
  };
}

async function waitFor(
  check: () => boolean | Promise<boolean>,
  message: string,
  timeoutMs = 2_000
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (true) {
    if (await check()) return;
    if (Date.now() >= deadline) throw new Error(`timed out waiting for ${message}`);
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
}

export function defineReplayOnlyAuthStoreContract(
  label: string,
  createHarness: () => Promise<ReplayAuthHarness> | ReplayAuthHarness
): void {
  test(`${label}: replay-only auth survives restart and relays historical ops`, async () => {
    const harness = await createHarness();
    try {
      const docId = `doc-replay-flow-${randomUUID()}`;
      const relayStores1 = await harness.createDocStores(docId);
      const relayStores2 = await harness.createDocStores(docId);
      const joinerStores = await harness.createDocStores(docId);

      const authorBackend = new SimpleMemoryBackend(docId);
      const relayBackend = new SimpleMemoryBackend(docId);
      const joinerBackend = new SimpleMemoryBackend(docId);
      const op = makeInsertOp(7, 1);
      await authorBackend.applyOps([op]);

      const authorCapability: Capability = { name: "auth.capability", value: "token-author" };
      const ignoredCapability: Capability = { name: "peer.name", value: "author" };
      const authEntry: OpAuth = {
        sig: new Uint8Array(64).fill(9),
        proofRef: new Uint8Array(16).fill(3),
      };

      const authorAuth: SyncAuth<Operation> = {
        helloCapabilities: async () => [authorCapability, ignoredCapability],
        signOps: async (ops) => ops.map(() => authEntry),
      };

      const warmRelayAuth = createReplayOnlySyncAuth({
        docId,
        authMaterialStore: relayStores1,
      });
      const relayJoinerAuth = createReplayOnlySyncAuth({
        docId,
        authMaterialStore: relayStores2,
      });
      const joinerAuth = createReplayOnlySyncAuth({
        docId,
        authMaterialStore: joinerStores,
      });

      const firstHop = createInMemoryConnectedPeers({
        backendA: authorBackend,
        backendB: relayBackend,
        codec: treecrdtSyncV0ProtobufCodec,
        peerAOptions: { auth: authorAuth },
        peerBOptions: { auth: warmRelayAuth },
      });
      try {
        await firstHop.peerA.syncOnce(firstHop.transportA, { all: {} }, { maxCodewords: 10_000, codewordsPerMessage: 256 });
      } finally {
        firstHop.detach();
      }

      await waitFor(
        () => relayBackend.hasOp(bytesToHex(op.meta.id.replica), op.meta.id.counter),
        "relay backend to apply first-hop op"
      );

      const secondHop = createInMemoryConnectedPeers({
        backendA: relayBackend,
        backendB: joinerBackend,
        codec: treecrdtSyncV0ProtobufCodec,
        peerAOptions: { auth: relayJoinerAuth },
        peerBOptions: { auth: joinerAuth },
      });
      try {
        await secondHop.peerA.syncOnce(secondHop.transportA, { all: {} }, { maxCodewords: 10_000, codewordsPerMessage: 256 });
      } finally {
        secondHop.detach();
      }

      await waitFor(
        () => joinerBackend.hasOp(bytesToHex(op.meta.id.replica), op.meta.id.counter),
        "joiner backend to apply replayed op"
      );
      expect(await relayJoinerAuth.helloCapabilities?.({ docId })).toEqual([authorCapability]);
      const replayed = await relayJoinerAuth.signOps?.([op], {
        docId,
        purpose: "reconcile",
        filterId: "all",
      });
      expect(replayed?.map(normalizeOpAuth)).toEqual([normalizeOpAuth(authEntry)]);
    } finally {
      await harness.close?.();
    }
  });
}

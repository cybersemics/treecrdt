import { createHash } from "node:crypto";

import { expect, test } from "vitest";

import type { Operation } from "@treecrdt/interface";
import { bytesToHex, nodeIdToBytes16 } from "@treecrdt/interface/ids";
import { makeOp, nodeIdFromInt } from "@treecrdt/benchmark";

import { treecrdtSyncV0ProtobufCodec } from "../dist/protobuf.js";
import { SyncPeer } from "../dist/sync.js";
import { createInMemoryConnectedPeers } from "../dist/bench.js";
import { wrapDuplexTransportWithCodec } from "../dist/transport.js";
import type { DuplexTransport } from "../dist/transport.js";
import type { Filter, OpRef, SyncBackend, SyncMessage } from "../dist/types.js";

function opRefFor(docId: string, replica: string, counter: number): OpRef {
  const h = createHash("sha256");
  h.update("treecrdt/opref/test");
  h.update(docId);
  h.update(replica);
  h.update(String(counter));
  return new Uint8Array(h.digest()).slice(0, 16);
}

function setHex(opRefs: readonly Uint8Array[]): Set<string> {
  return new Set(opRefs.map((r) => bytesToHex(r)));
}

async function tick(): Promise<void> {
  await new Promise<void>((resolve) => setTimeout(resolve, 0));
}

function createMacrotaskDuplex<M>(): [DuplexTransport<M>, DuplexTransport<M>] {
  const aHandlers = new Set<(msg: M) => void>();
  const bHandlers = new Set<(msg: M) => void>();

  const a: DuplexTransport<M> = {
    async send(msg) {
      setTimeout(() => {
        for (const h of bHandlers) h(msg);
      }, 0);
    },
    onMessage(handler) {
      aHandlers.add(handler);
      return () => aHandlers.delete(handler);
    },
  };

  const b: DuplexTransport<M> = {
    async send(msg) {
      setTimeout(() => {
        for (const h of aHandlers) h(msg);
      }, 0);
    },
    onMessage(handler) {
      bHandlers.add(handler);
      return () => bHandlers.delete(handler);
    },
  };

  return [a, b];
}

function createPeers(a: SyncBackend<Operation>, b: SyncBackend<Operation>) {
  return createInMemoryConnectedPeers({
    backendA: a,
    backendB: b,
    codec: treecrdtSyncV0ProtobufCodec,
  });
}

async function waitUntil(
  predicate: () => Promise<boolean> | boolean,
  opts: { timeoutMs?: number; intervalMs?: number; message?: string } = {}
): Promise<void> {
  const timeoutMs = opts.timeoutMs ?? 2_000;
  const intervalMs = opts.intervalMs ?? 10;
  const start = Date.now();
  // eslint-disable-next-line no-constant-condition
  while (true) {
    const ok = await predicate();
    if (ok) return;
    if (Date.now() - start > timeoutMs) {
      throw new Error(opts.message ?? `waitUntil timeout after ${timeoutMs}ms`);
    }
    await new Promise<void>((resolve) => setTimeout(resolve, intervalMs));
  }
}

const TRASH_HEX = "ff".repeat(16);

class MemoryBackend implements SyncBackend<Operation> {
  readonly docId: string;

  private maxLamportValue = 0n;
  private readonly opsByRefHex = new Map<string, { opRef: OpRef; op: Operation }>();

  constructor(docId: string) {
    this.docId = docId;
  }

  private opRefForOp(op: Operation): OpRef {
    const replica =
      typeof op.meta.id.replica === "string" ? op.meta.id.replica : bytesToHex(op.meta.id.replica);
    return opRefFor(this.docId, replica, op.meta.id.counter);
  }

  hasOp(replica: string, counter: number): boolean {
    return Array.from(this.opsByRefHex.values()).some(
      (v) => v.op.meta.id.replica === replica && v.op.meta.id.counter === counter
    );
  }

  async maxLamport(): Promise<bigint> {
    return this.maxLamportValue;
  }

  async listOpRefs(filter: Filter): Promise<OpRef[]> {
    if ("all" in filter) {
      return Array.from(this.opsByRefHex.values(), (v) => v.opRef);
    }

    const targetParentHex = bytesToHex(filter.children.parent);

    // TODO(sync): `children(parent)` is defined in terms of the *canonical* tree state
    // (needs "old parent == P" for boundary-crossing moves). This naive implementation
    // replays/scans the entire local op log each time the filter is evaluated.
    //
    // Production backends should maintain a materialized parent/children index (or store
    // `old_parent` for move-like ops) so this doesn't become an O(total_ops) scan.
    const entries = Array.from(this.opsByRefHex.values()).sort((a, b) => {
      if (a.op.meta.lamport < b.op.meta.lamport) return -1;
      if (a.op.meta.lamport > b.op.meta.lamport) return 1;
      // Deterministic tie-breaker for the scan: opRef bytes.
      const ah = bytesToHex(a.opRef);
      const bh = bytesToHex(b.opRef);
      return ah < bh ? -1 : ah > bh ? 1 : 0;
    });

    const parentByNodeHex = new Map<string, string>();
    const relevant: OpRef[] = [];
    for (const { opRef, op } of entries) {
      const nodeHex =
        op.kind.type === "insert"
          ? op.kind.node
          : op.kind.type === "move"
            ? op.kind.node
            : op.kind.node;
      const oldParentHex = parentByNodeHex.get(nodeHex);
      const newParentHex =
        op.kind.type === "insert"
          ? op.kind.parent
          : op.kind.type === "move"
            ? op.kind.newParent
            : TRASH_HEX;

      if (oldParentHex === targetParentHex || newParentHex === targetParentHex) {
        relevant.push(opRef);
      }

      parentByNodeHex.set(nodeHex, newParentHex);
    }

  return relevant;
  }

  async getOpsByOpRefs(opRefs: OpRef[]): Promise<Operation[]> {
    return opRefs.map((r) => {
      const stored = this.opsByRefHex.get(bytesToHex(r));
      if (!stored) throw new Error("opRef missing locally");
      return stored.op;
    });
  }

  async applyOps(ops: Operation[]): Promise<void> {
    for (const op of ops) {
      const opRef = this.opRefForOp(op);
      const opRefHex = bytesToHex(opRef);
      if (this.opsByRefHex.has(opRefHex)) continue;

      this.opsByRefHex.set(opRefHex, { opRef, op });
      const lamport = BigInt(op.meta.lamport);
      if (lamport > this.maxLamportValue) this.maxLamportValue = lamport;
    }
  }
}

test("syncOnce does not starve macrotask transports", async () => {
  const docId = "doc-sync-macrotask";
  const root = "0".repeat(32);

  const a = new MemoryBackend(docId);
  const b = new MemoryBackend(docId);

  await a.applyOps([
    makeOp("a", 1, 1, { type: "insert", parent: root, node: nodeIdFromInt(1), position: 0 }),
  ]);

  const [wa, wb] = createMacrotaskDuplex<Uint8Array>();
  const ta = wrapDuplexTransportWithCodec(wa, treecrdtSyncV0ProtobufCodec);
  const tb = wrapDuplexTransportWithCodec(wb, treecrdtSyncV0ProtobufCodec);
  const pa = new SyncPeer(a);
  const pb = new SyncPeer(b);
  pa.attach(ta);
  pb.attach(tb);

  await pa.syncOnce(ta, { all: {} }, { maxCodewords: 10_000, codewordsPerMessage: 256 });

  await waitUntil(() => b.hasOp("a", 1), { message: "expected b to receive a:1 via macrotask duplex" });
});

test("sync all converges union of opRefs", async () => {
  const docId = "doc-sync-all";
  const root = "0".repeat(32);

  const a = new MemoryBackend(docId);
  const b = new MemoryBackend(docId);

  await a.applyOps([
    makeOp("a", 1, 1, { type: "insert", parent: root, node: nodeIdFromInt(1), position: 0 }),
    makeOp("a", 2, 2, { type: "insert", parent: root, node: nodeIdFromInt(2), position: 0 }),
  ]);
  await b.applyOps([
    makeOp("b", 1, 3, { type: "insert", parent: root, node: nodeIdFromInt(3), position: 0 }),
    makeOp("a", 2, 2, { type: "insert", parent: root, node: nodeIdFromInt(2), position: 0 }),
  ]);

  const { peerA: pa, transportA: ta, detach } = createPeers(a, b);
  try {
    await pa.syncOnce(ta, { all: {} }, { maxCodewords: 10_000, codewordsPerMessage: 256 });
    await tick();

    const aAll = await a.listOpRefs({ all: {} });
    const bAll = await b.listOpRefs({ all: {} });
    expect(setHex(aAll)).toEqual(setHex(bAll));
  } finally {
    detach();
  }
});

test("sync all transfers a single missing op (hole in the middle)", async () => {
  const docId = "doc-sync-one-missing";
  const root = "0".repeat(32);

  const a = new MemoryBackend(docId);
  const b = new MemoryBackend(docId);

  const size = 100;
  const missingCounter = Math.ceil(size / 2);
  const ops: Operation[] = [];
  for (let counter = 1; counter <= size; counter += 1) {
    ops.push(
      makeOp("s", counter, counter, {
        type: "insert",
        parent: root,
        node: nodeIdFromInt(counter),
        position: counter - 1,
      })
    );
  }

  await b.applyOps(ops);
  await a.applyOps(ops.filter((op) => op.meta.id.counter !== missingCounter));

  const { peerA: pa, transportA: ta, detach } = createPeers(a, b);
  try {
    await pa.syncOnce(ta, { all: {} }, { maxCodewords: 10_000, codewordsPerMessage: 256 });
    await tick();

    expect(a.hasOp("s", missingCounter)).toBe(true);
    const aAll = await a.listOpRefs({ all: {} });
    const bAll = await b.listOpRefs({ all: {} });
    expect(setHex(aAll)).toEqual(setHex(bAll));
  } finally {
    detach();
  }
});

test("sync children(parent) only transfers those children", async () => {
  const docId = "doc-sync-children";
  const parentAHex = "a0".repeat(16);
  const parentBHex = "b0".repeat(16);
  const parentABytes = nodeIdToBytes16(parentAHex);

  const a = new MemoryBackend(docId);
  const b = new MemoryBackend(docId);

  await a.applyOps([
    makeOp("a", 1, 1, { type: "insert", parent: parentAHex, node: nodeIdFromInt(1), position: 0 }),
    makeOp("a", 2, 2, { type: "insert", parent: parentBHex, node: nodeIdFromInt(2), position: 0 }),
  ]);
  await b.applyOps([
    makeOp("b", 1, 3, { type: "insert", parent: parentAHex, node: nodeIdFromInt(3), position: 0 }),
    makeOp("b", 2, 4, { type: "insert", parent: parentBHex, node: nodeIdFromInt(4), position: 0 }),
  ]);

  const { peerA: pa, transportA: ta, detach } = createPeers(a, b);
  try {
    await pa.syncOnce(
      ta,
      { children: { parent: parentABytes } },
      { maxCodewords: 10_000, codewordsPerMessage: 256 }
    );
    await tick();

    // Converges for the filtered view.
    const aChildrenA = await a.listOpRefs({ children: { parent: parentABytes } });
    const bChildrenA = await b.listOpRefs({ children: { parent: parentABytes } });
    expect(setHex(aChildrenA)).toEqual(setHex(bChildrenA));

    // Does not leak ops outside the filter.
    expect(a.hasOp("b", 1)).toBe(true);
    expect(a.hasOp("b", 2)).toBe(false);
    expect(b.hasOp("a", 1)).toBe(true);
    expect(b.hasOp("a", 2)).toBe(false);
  } finally {
    detach();
  }
});

test("sync children(parent) includes boundary-crossing moves", async () => {
  const docId = "doc-sync-boundary-move";
  const parentAHex = "a0".repeat(16);
  const parentBHex = "b0".repeat(16);
  const parentABytes = nodeIdToBytes16(parentAHex);

  const a = new MemoryBackend(docId);
  const b = new MemoryBackend(docId);

  const node = nodeIdFromInt(0x10);

  await a.applyOps([
    makeOp("a", 1, 1, { type: "insert", parent: parentAHex, node, position: 0 }),
    // Move the node out of the subtree. The move is still relevant to `children(parentA)`
    // because it changes the canonical child set of `parentA`.
    makeOp("a", 2, 2, { type: "move", node, newParent: parentBHex, position: 0 }),
  ]);

  const { peerA: pa, transportA: ta, detach } = createPeers(a, b);
  try {
    await pa.syncOnce(
      ta,
      { children: { parent: parentABytes } },
      { maxCodewords: 10_000, codewordsPerMessage: 256 }
    );
    await tick();

    expect(b.hasOp("a", 2)).toBe(true);

    const aChildrenA = await a.listOpRefs({ children: { parent: parentABytes } });
    const bChildrenA = await b.listOpRefs({ children: { parent: parentABytes } });
    expect(setHex(aChildrenA)).toEqual(setHex(bChildrenA));
  } finally {
    detach();
  }
});

test("subscribe keeps peers converging (push deltas)", async () => {
  const docId = "doc-subscribe";
  const root = "0".repeat(32);

  const a = new MemoryBackend(docId);
  const b = new MemoryBackend(docId);

  await a.applyOps([makeOp("a", 1, 1, { type: "insert", parent: root, node: nodeIdFromInt(1), position: 0 })]);

  const { peerA: pa, peerB: pb, transportA: ta, detach } = createPeers(a, b);
  try {
    const sub = pa.subscribe(ta, { all: {} }, { maxCodewords: 10_000, codewordsPerMessage: 256 });
    try {
      await waitUntil(() => b.hasOp("a", 1), { message: "expected b to receive a:1 via subscription" });

      await b.applyOps([makeOp("b", 1, 2, { type: "insert", parent: root, node: nodeIdFromInt(2), position: 0 })]);
      await pb.notifyLocalUpdate();
      await waitUntil(() => a.hasOp("b", 1), { message: "expected a to receive b:1 via subscription" });
    } finally {
      sub.stop();
      await sub.done;
    }
  } finally {
    detach();
  }
});

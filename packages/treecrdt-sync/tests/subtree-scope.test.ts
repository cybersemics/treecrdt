import { expect, test } from "vitest";

import type { Operation } from "@treecrdt/interface";
import { bytesToHex, nodeIdToBytes16, replicaIdToBytes, ROOT_NODE_ID_HEX, TRASH_NODE_ID_HEX } from "@treecrdt/interface/ids";
import { makeOp, nodeIdFromInt } from "@treecrdt/benchmark";
import { hashes as ed25519Hashes, getPublicKey, utils as ed25519Utils } from "@noble/ed25519";
import { sha512 } from "@noble/hashes/sha512";
import { encode as cborEncode, rfc8949EncodeOptions } from "cborg";

import { deriveOpRefV0 } from "../dist/opref.js";
import { treecrdtSyncV0ProtobufCodec } from "../dist/protobuf.js";
import { createInMemoryConnectedPeers } from "../dist/in-memory.js";
import { coseSign1Ed25519 } from "../dist/cose.js";
import { createTreecrdtCoseCwtAuth } from "../dist/treecrdt-auth.js";
import type { Filter, OpAuth, OpRef, PendingOp, SyncBackend } from "../dist/types.js";

ed25519Hashes.sha512 = sha512;

function orderKeyFromPosition(position: number): Uint8Array {
  if (!Number.isInteger(position) || position < 0) throw new Error(`invalid position: ${position}`);
  const n = position + 1;
  if (n > 0xffff) throw new Error(`position too large for u16 order key: ${position}`);
  const bytes = new Uint8Array(2);
  new DataView(bytes.buffer).setUint16(0, n, false);
  return bytes;
}

function opRefForOp(docId: string, op: Operation): OpRef {
  return deriveOpRefV0(docId, {
    replica: replicaIdToBytes(op.meta.id.replica),
    counter: BigInt(op.meta.id.counter),
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

async function flushMicrotasks(rounds = 8): Promise<void> {
  for (let i = 0; i < rounds; i += 1) {
    await Promise.resolve();
  }
}

class TreeBackend implements SyncBackend<Operation> {
  readonly docId: string;

  private maxLamportValue = 0n;
  private readonly opsByRefHex = new Map<string, { opRef: OpRef; op: Operation; auth?: OpAuth }>();
  private readonly pendingByRefHex = new Map<string, PendingOp<Operation>>();
  private readonly parentByNodeHex = new Map<string, string>();

  constructor(docId: string) {
    this.docId = docId;
  }

  hasOp(replica: string, counter: number): boolean {
    return Array.from(this.opsByRefHex.values()).some(
      (v) => v.op.meta.id.replica === replica && v.op.meta.id.counter === counter
    );
  }

  hasPendingOp(replica: string, counter: number): boolean {
    return Array.from(this.pendingByRefHex.values()).some(
      (v) => v.op.meta.id.replica === replica && v.op.meta.id.counter === counter
    );
  }

  private opRefForOp(op: Operation): OpRef {
    return opRefForOp(this.docId, op);
  }

  private noteParentEdge(op: Operation): void {
    switch (op.kind.type) {
      case "insert":
        this.parentByNodeHex.set(op.kind.node, op.kind.parent);
        break;
      case "move":
        this.parentByNodeHex.set(op.kind.node, op.kind.newParent);
        break;
      case "delete":
      case "tombstone":
        this.parentByNodeHex.set(op.kind.node, TRASH_NODE_ID_HEX);
        break;
      case "payload":
        break;
    }
  }

  getParentHex(nodeHex: string): string | undefined {
    return this.parentByNodeHex.get(nodeHex);
  }

  async maxLamport(): Promise<bigint> {
    return this.maxLamportValue;
  }

  async listOpRefs(filter: Filter): Promise<OpRef[]> {
    if (!("all" in filter)) throw new Error("TreeBackend only supports filter { all: {} } in tests");
    const opRefs = Array.from(this.opsByRefHex.values(), (v) => v.opRef);
    const pendingRefs = Array.from(this.pendingByRefHex.values(), (p) => this.opRefForOp(p.op));
    return opRefs.concat(pendingRefs);
  }

  async getOpsByOpRefs(opRefs: OpRef[]): Promise<Operation[]> {
    const ops: Operation[] = [];
    for (const ref of opRefs) {
      const hex = bytesToHex(ref);
      const entry = this.opsByRefHex.get(hex);
      if (entry) {
        ops.push(entry.op);
        continue;
      }
      const pending = this.pendingByRefHex.get(hex);
      if (!pending) throw new Error(`unknown opRef: ${hex}`);
      ops.push(pending.op);
    }
    return ops;
  }

  async applyOps(ops: Operation[]): Promise<void> {
    for (const op of ops) {
      const opRef = this.opRefForOp(op);
      const opRefHex = bytesToHex(opRef);
      if (this.opsByRefHex.has(opRefHex)) continue;

      this.opsByRefHex.set(opRefHex, { opRef, op });
      const lamport = BigInt(op.meta.lamport);
      if (lamport > this.maxLamportValue) this.maxLamportValue = lamport;
      this.noteParentEdge(op);
    }
  }

  async storePendingOps(pending: PendingOp<Operation>[]): Promise<void> {
    for (const p of pending) {
      const opRefHex = bytesToHex(this.opRefForOp(p.op));
      this.pendingByRefHex.set(opRefHex, p);
    }
  }

  async listPendingOps(): Promise<PendingOp<Operation>[]> {
    return Array.from(this.pendingByRefHex.values());
  }

  async deletePendingOps(ops: Operation[]): Promise<void> {
    for (const op of ops) {
      const opRefHex = bytesToHex(this.opRefForOp(op));
      this.pendingByRefHex.delete(opRefHex);
    }
  }
}

function makeSubtreeCapabilityToken(opts: {
  issuerPrivateKey: Uint8Array;
  subjectPublicKey: Uint8Array;
  docId: string;
  root: string;
  actions: string[];
}): Uint8Array {
  const cnf = new Map<unknown, unknown>([["pub", opts.subjectPublicKey]]);

  const res = new Map<unknown, unknown>([
    ["doc_id", opts.docId],
    ["root", nodeIdToBytes16(opts.root)],
  ]);

  const cap = new Map<unknown, unknown>([
    ["res", res],
    ["actions", opts.actions],
  ]);

  const claims = new Map<unknown, unknown>([
    [3, opts.docId], // CWT `aud`
    [8, cnf], // CWT `cnf`
    [-1, [cap]], // private claim `caps`
  ]);

  const payload = cborEncode(claims, rfc8949EncodeOptions);
  return coseSign1Ed25519({ payload, privateKey: opts.issuerPrivateKey });
}

function makeCapabilityTokenFromCaps(opts: {
  issuerPrivateKey: Uint8Array;
  subjectPublicKey: Uint8Array;
  docId: string;
  caps: Array<{ res: Map<unknown, unknown>; actions: string[] }>;
}): Uint8Array {
  const cnf = new Map<unknown, unknown>([["pub", opts.subjectPublicKey]]);

  const caps = opts.caps.map((c) => new Map<unknown, unknown>([["res", c.res], ["actions", c.actions]]));

  const claims = new Map<unknown, unknown>([
    [3, opts.docId], // CWT `aud`
    [8, cnf], // CWT `cnf`
    [-1, caps], // private claim `caps`
  ]);

  const payload = cborEncode(claims, rfc8949EncodeOptions);
  return coseSign1Ed25519({ payload, privateKey: opts.issuerPrivateKey });
}

test("subtree scope: pending_context ops are stored and later applied when context arrives", async () => {
  const docId = "doc-subtree-pending";
  const root = ROOT_NODE_ID_HEX;
  const subtreeRoot = nodeIdFromInt(1);
  const child = nodeIdFromInt(2);

  const a = new TreeBackend(docId);
  const b = new TreeBackend(docId);

  const issuerSk = ed25519Utils.randomSecretKey();
  const issuerPk = await getPublicKey(issuerSk);

  const aSk = ed25519Utils.randomSecretKey();
  const aPk = await getPublicKey(aSk);
  const bSk = ed25519Utils.randomSecretKey();
  const bPk = await getPublicKey(bSk);

  const tokenA = makeSubtreeCapabilityToken({
    issuerPrivateKey: issuerSk,
    subjectPublicKey: aPk,
    docId,
    root: subtreeRoot,
    actions: ["write_structure", "write_payload"],
  });
  const tokenB = makeSubtreeCapabilityToken({
    issuerPrivateKey: issuerSk,
    subjectPublicKey: bPk,
    docId,
    root,
    actions: ["write_structure", "write_payload"],
  });

  const scopeEvaluator = ({ node, scope }: { node: Uint8Array; scope: { root: Uint8Array } }) => {
    const nodeHex = bytesToHex(node);
    const scopeRootHex = bytesToHex(scope.root);
    if (nodeHex === scopeRootHex) return "allow" as const;
    if (nodeHex === ROOT_NODE_ID_HEX) return "deny" as const;
    if (nodeHex === TRASH_NODE_ID_HEX) return "deny" as const;

    let cur = nodeHex;
    for (let depth = 0; depth < 10_000; depth += 1) {
      const parent = b.getParentHex(cur);
      if (!parent) return "unknown" as const;
      if (parent === scopeRootHex) return "allow" as const;
      if (parent === ROOT_NODE_ID_HEX || parent === TRASH_NODE_ID_HEX) return "deny" as const;
      cur = parent;
    }
    return "unknown" as const;
  };

  const authA = createTreecrdtCoseCwtAuth({
    issuerPublicKeys: [issuerPk],
    localPrivateKey: aSk,
    localPublicKey: aPk,
    localCapabilityTokens: [tokenA],
    requireProofRef: true,
  });

  const authB = createTreecrdtCoseCwtAuth({
    issuerPublicKeys: [issuerPk],
    localPrivateKey: bSk,
    localPublicKey: bPk,
    localCapabilityTokens: [tokenB],
    requireProofRef: true,
    scopeEvaluator,
  });

  const { peerA, peerB, transportB, detach } = createInMemoryConnectedPeers({
    backendA: a,
    backendB: b,
    codec: treecrdtSyncV0ProtobufCodec,
    peerAOptions: { auth: authA, maxOpsPerBatch: 1 },
    peerBOptions: { auth: authB, maxOpsPerBatch: 1 },
  });

  try {
    // Establish capabilities via Hello/HelloAck before using push subscriptions.
    await peerB.syncOnce(transportB, { all: {} }, { maxCodewords: 10_000, codewordsPerMessage: 256 });

    // Start push subscription without running syncOnce again (capabilities already known).
    const sub = peerB.subscribe(transportB, { all: {} }, { immediate: false, intervalMs: 0 });

    // Ensure the subscription is registered on the responder before applying any ops.
    await waitUntil(() => (peerA as any).responderSubscriptions?.size === 1, {
      message: "expected responder subscription to be registered",
    });

    // Deliver the payload op first (out-of-order relative to structure), so it must be
    // stored as pending until the insert arrives and provides ancestry context.
    const payloadOp = makeOp("a", 1, 1, { type: "payload", node: child, payload: new Uint8Array([1, 2, 3]) });
    await a.applyOps([payloadOp]);
    void peerA.notifyLocalUpdate();

    await waitUntil(() => b.hasPendingOp("a", 1), { message: "expected payload op to be stored pending" });
    expect(b.hasPendingOp("a", 1)).toBe(true);
    expect(b.hasOp("a", 1)).toBe(false);
    expect(b.hasOp("a", 2)).toBe(false);

    // Now deliver the structural insert; this should allow the pending payload to be
    // reprocessed and applied.
    const insertChild = makeOp("a", 2, 2, {
      type: "insert",
      parent: subtreeRoot,
      node: child,
      orderKey: orderKeyFromPosition(0),
    });
    await a.applyOps([insertChild]);
    void peerA.notifyLocalUpdate();

    await waitUntil(() => b.hasOp("a", 2), { message: "expected insert op to be applied" });
    await waitUntil(() => b.hasOp("a", 1), { message: "expected pending payload op to be reprocessed and applied" });
    await waitUntil(async () => (await b.listPendingOps?.())?.length === 0, { message: "expected pending store to drain" });

    sub.stop();
    await sub.done;
  } finally {
    detach();
  }
});

test("subtree scope: pending ops are dropped once context proves they are unauthorized", async () => {
  const docId = "doc-subtree-drop";
  const root = ROOT_NODE_ID_HEX;
  const subtreeRoot = nodeIdFromInt(1);
  const node = nodeIdFromInt(2);

  const a = new TreeBackend(docId);
  const b = new TreeBackend(docId);

  const issuerSk = ed25519Utils.randomSecretKey();
  const issuerPk = await getPublicKey(issuerSk);

  const aSk = ed25519Utils.randomSecretKey();
  const aPk = await getPublicKey(aSk);
  const bSk = ed25519Utils.randomSecretKey();
  const bPk = await getPublicKey(bSk);

  const tokenA = makeCapabilityTokenFromCaps({
    issuerPrivateKey: issuerSk,
    subjectPublicKey: aPk,
    docId,
    caps: [
      // Doc-wide: allow structure writes anywhere.
      { res: new Map<unknown, unknown>([["doc_id", docId]]), actions: ["write_structure"] },
      // Subtree-scoped: allow payload writes under `subtreeRoot` only.
      {
        res: new Map<unknown, unknown>([
          ["doc_id", docId],
          ["root", nodeIdToBytes16(subtreeRoot)],
        ]),
        actions: ["write_payload"],
      },
    ],
  });

  const tokenB = makeSubtreeCapabilityToken({
    issuerPrivateKey: issuerSk,
    subjectPublicKey: bPk,
    docId,
    root,
    actions: ["write_structure", "write_payload"],
  });

  const scopeEvaluator = ({ node: n, scope }: { node: Uint8Array; scope: { root: Uint8Array } }) => {
    const nodeHex = bytesToHex(n);
    const scopeRootHex = bytesToHex(scope.root);
    if (nodeHex === scopeRootHex) return "allow" as const;
    if (nodeHex === ROOT_NODE_ID_HEX) return "deny" as const;
    if (nodeHex === TRASH_NODE_ID_HEX) return "deny" as const;

    let cur = nodeHex;
    for (let depth = 0; depth < 10_000; depth += 1) {
      const parent = b.getParentHex(cur);
      if (!parent) return "unknown" as const;
      if (parent === scopeRootHex) return "allow" as const;
      if (parent === ROOT_NODE_ID_HEX || parent === TRASH_NODE_ID_HEX) return "deny" as const;
      cur = parent;
    }
    return "unknown" as const;
  };

  const authA = createTreecrdtCoseCwtAuth({
    issuerPublicKeys: [issuerPk],
    localPrivateKey: aSk,
    localPublicKey: aPk,
    localCapabilityTokens: [tokenA],
    requireProofRef: true,
  });

  const authB = createTreecrdtCoseCwtAuth({
    issuerPublicKeys: [issuerPk],
    localPrivateKey: bSk,
    localPublicKey: bPk,
    localCapabilityTokens: [tokenB],
    requireProofRef: true,
    scopeEvaluator,
  });

  const { peerA, peerB, transportB, detach } = createInMemoryConnectedPeers({
    backendA: a,
    backendB: b,
    codec: treecrdtSyncV0ProtobufCodec,
    peerAOptions: { auth: authA, maxOpsPerBatch: 1 },
    peerBOptions: { auth: authB, maxOpsPerBatch: 1 },
  });

  try {
    // Exchange capabilities first so B can verify A's ops.
    await peerB.syncOnce(transportB, { all: {} }, { maxCodewords: 10_000, codewordsPerMessage: 256 });

    const sub = peerB.subscribe(transportB, { all: {} }, { immediate: false, intervalMs: 0 });
    await waitUntil(() => (peerA as any).responderSubscriptions?.size === 1, {
      message: "expected responder subscription to be registered",
    });

    // A sets a payload on `node` before any ancestry is known: pending_context.
    await a.applyOps([makeOp("a", 1, 1, { type: "payload", node, payload: new Uint8Array([9]) })]);
    void peerA.notifyLocalUpdate();
    await waitUntil(() => b.hasPendingOp("a", 1), { message: "expected payload op to be stored pending" });
    expect(b.hasOp("a", 1)).toBe(false);

    // Later, `node` is inserted under ROOT. This provides enough context to prove
    // the pending payload is outside the authorized subtree, so it is dropped.
    await a.applyOps([makeOp("a", 2, 2, { type: "insert", parent: root, node, orderKey: orderKeyFromPosition(0) })]);
    void peerA.notifyLocalUpdate();
    await waitUntil(() => b.hasOp("a", 2), { message: "expected insert op to be applied" });
    await waitUntil(async () => (await b.listPendingOps?.())?.length === 0, { message: "expected pending store to drain" });
    expect(b.hasOp("a", 1)).toBe(false);

    sub.stop();
    await sub.done;
  } finally {
    detach();
  }
});

test("subtree scope: ops outside scope are rejected (fail closed)", async () => {
  const docId = "doc-subtree-deny";
  const root = ROOT_NODE_ID_HEX;
  const subtreeRoot = nodeIdFromInt(1);
  const node = nodeIdFromInt(2);

  const a = new TreeBackend(docId);
  const b = new TreeBackend(docId);

  const issuerSk = ed25519Utils.randomSecretKey();
  const issuerPk = await getPublicKey(issuerSk);

  const aSk = ed25519Utils.randomSecretKey();
  const aPk = await getPublicKey(aSk);
  const bSk = ed25519Utils.randomSecretKey();
  const bPk = await getPublicKey(bSk);

  const tokenA = makeSubtreeCapabilityToken({
    issuerPrivateKey: issuerSk,
    subjectPublicKey: aPk,
    docId,
    root: subtreeRoot,
    actions: ["write_structure"],
  });
  const tokenB = makeSubtreeCapabilityToken({
    issuerPrivateKey: issuerSk,
    subjectPublicKey: bPk,
    docId,
    root,
    actions: ["write_structure"],
  });

  const scopeEvaluator = ({ node: n, scope }: { node: Uint8Array; scope: { root: Uint8Array } }) => {
    const nodeHex = bytesToHex(n);
    const scopeRootHex = bytesToHex(scope.root);
    if (nodeHex === scopeRootHex) return "allow" as const;
    if (nodeHex === ROOT_NODE_ID_HEX) return "deny" as const;
    return "unknown" as const;
  };

  const authA = createTreecrdtCoseCwtAuth({
    issuerPublicKeys: [issuerPk],
    localPrivateKey: aSk,
    localPublicKey: aPk,
    localCapabilityTokens: [tokenA],
    requireProofRef: true,
  });
  const authB = createTreecrdtCoseCwtAuth({
    issuerPublicKeys: [issuerPk],
    localPrivateKey: bSk,
    localPublicKey: bPk,
    localCapabilityTokens: [tokenB],
    requireProofRef: true,
    scopeEvaluator,
  });

  const { peerA, peerB, transportB, detach } = createInMemoryConnectedPeers({
    backendA: a,
    backendB: b,
    codec: treecrdtSyncV0ProtobufCodec,
    peerAOptions: { auth: authA, maxOpsPerBatch: 1 },
    peerBOptions: { auth: authB, maxOpsPerBatch: 1 },
  });

  try {
    await peerB.syncOnce(transportB, { all: {} }, { maxCodewords: 10_000, codewordsPerMessage: 256 });

    // Attempt an op that touches ROOT (insert under ROOT) with a subtree-root token.
    const sub = peerB.subscribe(transportB, { all: {} }, { immediate: false, intervalMs: 0 });
    await waitUntil(() => (peerA as any).responderSubscriptions?.size === 1, {
      message: "expected responder subscription to be registered",
    });

    await a.applyOps([makeOp("a", 1, 1, { type: "insert", parent: root, node, orderKey: orderKeyFromPosition(0) })]);
    void peerA.notifyLocalUpdate();

    await expect(sub.done).rejects.toThrow(/capability does not allow op/i);
    expect(b.hasOp("a", 1)).toBe(false);
  } finally {
    detach();
  }
});

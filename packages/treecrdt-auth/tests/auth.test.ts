import { createHash } from "node:crypto";

import { expect, test } from "vitest";

import type { Operation } from "@treecrdt/interface";
import { bytesToHex, nodeIdToBytes16 } from "@treecrdt/interface/ids";
import { makeOp, nodeIdFromInt } from "@treecrdt/benchmark";
import { hashes as ed25519Hashes, getPublicKey, utils as ed25519Utils } from "@noble/ed25519";
import { sha512 } from "@noble/hashes/sha512";

import { createInMemoryConnectedPeers } from "@treecrdt/sync/in-memory";
import { treecrdtSyncV0ProtobufCodec } from "@treecrdt/sync/protobuf";
import { deriveTokenIdV1 } from "../dist/cose.js";
import { createTreecrdtIdentityChainCapabilityV1, issueDeviceCertV1, issueReplicaCertV1 } from "../dist/identity.js";
import {
  createTreecrdtCoseCwtAuth,
  describeTreecrdtCapabilityTokenV1,
  issueTreecrdtCapabilityTokenV1,
  issueTreecrdtDelegatedCapabilityTokenV1,
} from "../dist/treecrdt-auth.js";
import type { Filter, OpRef, SyncBackend } from "@treecrdt/sync";

ed25519Hashes.sha512 = sha512;

function orderKeyFromPosition(position: number): Uint8Array {
  if (!Number.isInteger(position) || position < 0) throw new Error(`invalid position: ${position}`);
  const n = position + 1;
  if (n > 0xffff) throw new Error(`position too large for u16 order key: ${position}`);
  const bytes = new Uint8Array(2);
  new DataView(bytes.buffer).setUint16(0, n, false);
  return bytes;
}

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

class MemoryBackend implements SyncBackend<Operation> {
  readonly docId: string;

  private maxLamportValue = 0n;
  private readonly opsByRefHex = new Map<string, { opRef: OpRef; op: Operation }>();

  constructor(docId: string) {
    this.docId = docId;
  }

  private opRefForOp(op: Operation): OpRef {
    const replicaHex = bytesToHex(op.meta.id.replica);
    return opRefFor(this.docId, replicaHex, op.meta.id.counter);
  }

  hasOp(replicaHex: string, counter: number): boolean {
    return Array.from(this.opsByRefHex.values()).some(
      (v) => bytesToHex(v.op.meta.id.replica) === replicaHex && v.op.meta.id.counter === counter
    );
  }

  async maxLamport(): Promise<bigint> {
    return this.maxLamportValue;
  }

  async listOpRefs(filter: Filter): Promise<OpRef[]> {
    if ("all" in filter) {
      return Array.from(this.opsByRefHex.values(), (v) => v.opRef);
    }
    if ("children" in filter) {
      const parentHex = bytesToHex(filter.children.parent);
      return Array.from(this.opsByRefHex.values(), (v) => v).flatMap((v) => {
        switch (v.op.kind.type) {
          case "insert":
            return v.op.kind.parent === parentHex ? [v.opRef] : [];
          case "move":
            return v.op.kind.newParent === parentHex ? [v.opRef] : [];
          default:
            return [];
        }
      });
    }
    throw new Error("MemoryBackend only supports filter { all: {} } and { children: { parent } }");
  }

  async getOpsByOpRefs(opRefs: OpRef[]): Promise<Operation[]> {
    const ops: Operation[] = [];
    for (const ref of opRefs) {
      const hex = bytesToHex(ref);
      const entry = this.opsByRefHex.get(hex);
      if (!entry) throw new Error(`unknown opRef: ${hex}`);
      ops.push(entry.op);
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
    }
  }
}

test("syncOnce with COSE+CWT auth converges and verifies ops", async () => {
  const docId = "doc-auth-happy";
  const root = "0".repeat(32);

  const a = new MemoryBackend(docId);
  const b = new MemoryBackend(docId);

  const issuerSk = ed25519Utils.randomSecretKey();
  const issuerPk = await getPublicKey(issuerSk);

  const aSk = ed25519Utils.randomSecretKey();
  const aPk = await getPublicKey(aSk);
  const bSk = ed25519Utils.randomSecretKey();
  const bPk = await getPublicKey(bSk);

  const aHex = bytesToHex(aPk);
  const bHex = bytesToHex(bPk);

  await a.applyOps([
    makeOp(aPk, 1, 1, { type: "insert", parent: root, node: nodeIdFromInt(1), orderKey: orderKeyFromPosition(0) }),
  ]);
  await b.applyOps([
    makeOp(bPk, 1, 2, { type: "insert", parent: root, node: nodeIdFromInt(2), orderKey: orderKeyFromPosition(0) }),
    makeOp(bPk, 2, 3, { type: "insert", parent: root, node: nodeIdFromInt(3), orderKey: orderKeyFromPosition(0) }),
  ]);

  const tokenA = issueTreecrdtCapabilityTokenV1({
    issuerPrivateKey: issuerSk,
    subjectPublicKey: aPk,
    docId,
    actions: ["write_structure"],
  });
  const tokenB = issueTreecrdtCapabilityTokenV1({
    issuerPrivateKey: issuerSk,
    subjectPublicKey: bPk,
    docId,
    actions: ["write_structure"],
  });

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
  });

  const { peerA: pa, transportA: ta, peerB: pb, detach } = createInMemoryConnectedPeers({
    backendA: a,
    backendB: b,
    codec: treecrdtSyncV0ProtobufCodec,
    peerAOptions: { auth: authA },
    // Keep responder session alive long enough that auth errors would surface deterministically.
    peerBOptions: { auth: authB, maxOpsPerBatch: 1 },
  });

  try {
    await pa.syncOnce(ta, { all: {} }, { maxCodewords: 10_000, codewordsPerMessage: 256 });

    await waitUntil(
      async () => {
        const [aRefs, bRefs] = await Promise.all([a.listOpRefs({ all: {} }), b.listOpRefs({ all: {} })]);
        const aSet = setHex(aRefs);
        const bSet = setHex(bRefs);
        if (aSet.size !== bSet.size) return false;
        for (const v of aSet) if (!bSet.has(v)) return false;
        return true;
      },
      { message: "expected convergence after syncOnce" }
    );

    expect(a.hasOp(aHex, 1)).toBe(true);
    expect(a.hasOp(bHex, 1)).toBe(true);
    expect(a.hasOp(bHex, 2)).toBe(true);
    expect(b.hasOp(aHex, 1)).toBe(true);
    expect(b.hasOp(bHex, 1)).toBe(true);
    expect(b.hasOp(bHex, 2)).toBe(true);
    void pb;
  } finally {
    detach();
  }
});

test("auth: signOps selects proof_ref per op when multiple tokens exist", async () => {
  const docId = "doc-auth-multitoken";
  const root = "0".repeat(32);

  const issuerSk = ed25519Utils.randomSecretKey();
  const issuerPk = await getPublicKey(issuerSk);

  const aSk = ed25519Utils.randomSecretKey();
  const aPk = await getPublicKey(aSk);

  const bSk = ed25519Utils.randomSecretKey();
  const bPk = await getPublicKey(bSk);

  const tokenStructure = issueTreecrdtCapabilityTokenV1({
    issuerPrivateKey: issuerSk,
    subjectPublicKey: aPk,
    docId,
    actions: ["write_structure"],
  });
  const tokenDelete = issueTreecrdtCapabilityTokenV1({
    issuerPrivateKey: issuerSk,
    subjectPublicKey: aPk,
    docId,
    actions: ["delete"],
  });

  const authA = createTreecrdtCoseCwtAuth({
    issuerPublicKeys: [issuerPk],
    localPrivateKey: aSk,
    localPublicKey: aPk,
    localCapabilityTokens: [tokenStructure, tokenDelete],
    requireProofRef: true,
  });

  const authB = createTreecrdtCoseCwtAuth({
    issuerPublicKeys: [issuerPk],
    localPrivateKey: bSk,
    localPublicKey: bPk,
    requireProofRef: true,
  });

  const helloCapsA = await authA.helloCapabilities?.({ docId });
  await authB.onHello?.({ capabilities: helloCapsA ?? [], filters: [], maxLamport: 0n }, { docId });

  const opInsert = makeOp(aPk, 1, 1, {
    type: "insert",
    parent: root,
    node: nodeIdFromInt(1),
    orderKey: orderKeyFromPosition(0),
  });
  const opDelete = makeOp(aPk, 2, 2, {
    type: "delete",
    node: nodeIdFromInt(1),
  });

  const ops = [opInsert, opDelete];
  const ctx = { docId, purpose: "reconcile" as const, filterId: "all" };

  const auth = await authA.signOps?.(ops, ctx);
  expect(auth).toBeTruthy();
  expect(auth?.length).toBe(2);
  expect(auth?.[0]?.proofRef).toBeTruthy();
  expect(auth?.[1]?.proofRef).toBeTruthy();

  const tokenStructureId = deriveTokenIdV1(tokenStructure);
  const tokenDeleteId = deriveTokenIdV1(tokenDelete);

  expect(bytesToHex(auth?.[0]!.proofRef!)).toBe(bytesToHex(tokenStructureId));
  expect(bytesToHex(auth?.[1]!.proofRef!)).toBe(bytesToHex(tokenDeleteId));

  await authB.verifyOps?.(ops, auth, ctx);

  const badAuth = [{ ...auth?.[0]!, proofRef: tokenDeleteId }, auth?.[1]!];
  await expect(authB.verifyOps?.(ops, badAuth, ctx)).rejects.toThrow(/capability does not allow op/i);
});

test("auth: describeTreecrdtCapabilityTokenV1 decodes scope + actions", async () => {
  const docId = "doc-auth-token-describe";

  const issuerSk = ed25519Utils.randomSecretKey();
  const issuerPk = await getPublicKey(issuerSk);

  const subjectSk = ed25519Utils.randomSecretKey();
  const subjectPk = await getPublicKey(subjectSk);

  const rootNodeId = nodeIdFromInt(1);
  const excludeNodeId = nodeIdFromInt(2);

  const tokenBytes = issueTreecrdtCapabilityTokenV1({
    issuerPrivateKey: issuerSk,
    subjectPublicKey: subjectPk,
    docId,
    actions: ["write_structure"],
    rootNodeId,
    maxDepth: 2,
    excludeNodeIds: [excludeNodeId],
  });

  const described = await describeTreecrdtCapabilityTokenV1({
    tokenBytes,
    issuerPublicKeys: [issuerPk],
    docId,
  });

  expect(bytesToHex(described.subjectPublicKey)).toBe(bytesToHex(subjectPk));
  expect(described.caps.length).toBe(1);
  expect(described.caps[0]!.actions).toContain("write_structure");
  expect(described.caps[0]!.res.docId).toBe(docId);
  expect(described.caps[0]!.res.rootNodeId).toBe(rootNodeId);
  expect(described.caps[0]!.res.maxDepth).toBe(2);
  expect(described.caps[0]!.res.excludeNodeIds).toContain(excludeNodeId);

  await expect(
    describeTreecrdtCapabilityTokenV1({
      tokenBytes,
      issuerPublicKeys: [issuerPk],
      docId: "wrong-doc",
    })
  ).rejects.toThrow(/audience mismatch/i);

  const otherIssuerSk = ed25519Utils.randomSecretKey();
  const otherIssuerPk = await getPublicKey(otherIssuerSk);
  await expect(
    describeTreecrdtCapabilityTokenV1({
      tokenBytes,
      issuerPublicKeys: [otherIssuerPk],
      docId,
    })
  ).rejects.toThrow(/verification failed/i);
});

test("syncOnce fails when responder requires auth but initiator sends unsigned ops", async () => {
  const docId = "doc-auth-missing";
  const root = "0".repeat(32);

  const a = new MemoryBackend(docId);
  const b = new MemoryBackend(docId);

  const issuerSk = ed25519Utils.randomSecretKey();
  const issuerPk = await getPublicKey(issuerSk);

  const aSk = ed25519Utils.randomSecretKey();
  const aPk = await getPublicKey(aSk);
  const bSk = ed25519Utils.randomSecretKey();
  const bPk = await getPublicKey(bSk);
  const tokenB = issueTreecrdtCapabilityTokenV1({
    issuerPrivateKey: issuerSk,
    subjectPublicKey: bPk,
    docId,
    actions: ["write_structure"],
  });

  const aHex = bytesToHex(aPk);

  await a.applyOps([
    makeOp(aPk, 1, 1, { type: "insert", parent: root, node: nodeIdFromInt(1), orderKey: orderKeyFromPosition(0) }),
  ]);
  await b.applyOps([
    makeOp(bPk, 1, 2, { type: "insert", parent: root, node: nodeIdFromInt(2), orderKey: orderKeyFromPosition(0) }),
    makeOp(bPk, 2, 3, { type: "insert", parent: root, node: nodeIdFromInt(3), orderKey: orderKeyFromPosition(0) }),
  ]);

  const authB = createTreecrdtCoseCwtAuth({
    issuerPublicKeys: [issuerPk],
    localPrivateKey: bSk,
    localPublicKey: bPk,
    localCapabilityTokens: [tokenB],
    requireProofRef: true,
  });

  const { peerA: pa, transportA: ta, detach } = createInMemoryConnectedPeers({
    backendA: a,
    backendB: b,
    codec: treecrdtSyncV0ProtobufCodec,
    peerAOptions: {}, // no auth => unsigned ops
    peerBOptions: { auth: authB, maxOpsPerBatch: 1 },
  });

  try {
    await expect(pa.syncOnce(ta, { all: {} }, { maxCodewords: 10_000, codewordsPerMessage: 256 })).rejects.toThrow(
      /missing op auth|unauthorized|auth\.capability/i
    );
    await tick();
    expect(b.hasOp(aHex, 1)).toBe(false);
  } finally {
    detach();
  }
});

test("syncOnce fails when op signatures do not match the claimed replica_id", async () => {
  const docId = "doc-auth-badsig";
  const root = "0".repeat(32);

  const a = new MemoryBackend(docId);
  const b = new MemoryBackend(docId);

  const issuerSk = ed25519Utils.randomSecretKey();
  const issuerPk = await getPublicKey(issuerSk);

  const aClaimSk = ed25519Utils.randomSecretKey();
  const aClaimPk = await getPublicKey(aClaimSk);
  const aSignSk = ed25519Utils.randomSecretKey(); // mismatched on purpose

  const bSk = ed25519Utils.randomSecretKey();
  const bPk = await getPublicKey(bSk);

  const aHex = bytesToHex(aClaimPk);

  await a.applyOps([
    makeOp(aClaimPk, 1, 1, { type: "insert", parent: root, node: nodeIdFromInt(1), orderKey: orderKeyFromPosition(0) }),
  ]);
  await b.applyOps([
    makeOp(bPk, 1, 2, { type: "insert", parent: root, node: nodeIdFromInt(2), orderKey: orderKeyFromPosition(0) }),
    makeOp(bPk, 2, 3, { type: "insert", parent: root, node: nodeIdFromInt(3), orderKey: orderKeyFromPosition(0) }),
  ]);

  const tokenA = issueTreecrdtCapabilityTokenV1({
    issuerPrivateKey: issuerSk,
    subjectPublicKey: aClaimPk,
    docId,
    actions: ["write_structure"],
  });
  const tokenB = issueTreecrdtCapabilityTokenV1({
    issuerPrivateKey: issuerSk,
    subjectPublicKey: bPk,
    docId,
    actions: ["write_structure"],
  });

  const authA = createTreecrdtCoseCwtAuth({
    issuerPublicKeys: [issuerPk],
    localPrivateKey: aSignSk,
    localPublicKey: aClaimPk,
    localCapabilityTokens: [tokenA],
    requireProofRef: true,
  });

  const authB = createTreecrdtCoseCwtAuth({
    issuerPublicKeys: [issuerPk],
    localPrivateKey: bSk,
    localPublicKey: bPk,
    localCapabilityTokens: [tokenB],
    requireProofRef: true,
  });

  const { peerA: pa, transportA: ta, detach } = createInMemoryConnectedPeers({
    backendA: a,
    backendB: b,
    codec: treecrdtSyncV0ProtobufCodec,
    peerAOptions: { auth: authA },
    peerBOptions: { auth: authB, maxOpsPerBatch: 1 },
  });

  try {
    await expect(pa.syncOnce(ta, { all: {} }, { maxCodewords: 10_000, codewordsPerMessage: 256 })).rejects.toThrow(
      /invalid op signature|unknown author|capability/i
    );
    await tick();
    expect(b.hasOp(aHex, 1)).toBe(false);
  } finally {
    detach();
  }
});

test("auth: syncOnce rejects filters when capability scope does not allow read access", async () => {
  const docId = "doc-auth-filter-scope";
  const root = "0".repeat(32);

  const a = new MemoryBackend(docId);
  const b = new MemoryBackend(docId);

  const issuerSk = ed25519Utils.randomSecretKey();
  const issuerPk = await getPublicKey(issuerSk);

  const aSk = ed25519Utils.randomSecretKey();
  const aPk = await getPublicKey(aSk);

  const bSk = ed25519Utils.randomSecretKey();
  const bPk = await getPublicKey(bSk);

  const subtreeRoot = nodeIdFromInt(1);

  // Token allows reading structure, but only for a limited subtree.
  const tokenA = issueTreecrdtCapabilityTokenV1({
    issuerPrivateKey: issuerSk,
    subjectPublicKey: aPk,
    docId,
    actions: ["read_structure"],
    rootNodeId: subtreeRoot,
  });

  const scopeEvaluator = ({ node, scope }: { node: Uint8Array; scope: { root: Uint8Array } }) => {
    const nodeHex = bytesToHex(node);
    const rootHex = bytesToHex(scope.root);
    return nodeHex === rootHex ? "allow" : "deny";
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
    requireProofRef: true,
    scopeEvaluator,
  });

  // Insert a no-op op so maxLamport isn't trivially zero (not required, but keeps the setup realistic).
  await a.applyOps([
    makeOp(aPk, 1, 1, { type: "insert", parent: root, node: nodeIdFromInt(2), orderKey: orderKeyFromPosition(0) }),
  ]);

  const { peerA: pa, transportA: ta, detach } = createInMemoryConnectedPeers({
    backendA: a,
    backendB: b,
    codec: treecrdtSyncV0ProtobufCodec,
    peerAOptions: { auth: authA },
    peerBOptions: { auth: authB },
  });

  try {
    await expect(pa.syncOnce(ta, { all: {} }, { maxCodewords: 1_000, codewordsPerMessage: 64 })).rejects.toThrow(
      /UNAUTHORIZED.*capability does not allow filter/i
    );
  } finally {
    detach();
  }
  void bPk;
});

test("auth: subscribe rejects filters when capability scope does not allow read access", async () => {
  const docId = "doc-auth-subscribe-scope";
  const root = "0".repeat(32);

  const a = new MemoryBackend(docId);
  const b = new MemoryBackend(docId);

  const issuerSk = ed25519Utils.randomSecretKey();
  const issuerPk = await getPublicKey(issuerSk);

  const aSk = ed25519Utils.randomSecretKey();
  const aPk = await getPublicKey(aSk);

  const bSk = ed25519Utils.randomSecretKey();
  const bPk = await getPublicKey(bSk);

  const subtreeRoot = nodeIdFromInt(1);

  const tokenA = issueTreecrdtCapabilityTokenV1({
    issuerPrivateKey: issuerSk,
    subjectPublicKey: aPk,
    docId,
    actions: ["read_structure"],
    rootNodeId: subtreeRoot,
  });

  const scopeEvaluator = ({ node, scope }: { node: Uint8Array; scope: { root: Uint8Array } }) => {
    const nodeHex = bytesToHex(node);
    const rootHex = bytesToHex(scope.root);
    return nodeHex === rootHex ? "allow" : "deny";
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
    requireProofRef: true,
    scopeEvaluator,
  });

  await a.applyOps([
    makeOp(aPk, 1, 1, { type: "insert", parent: root, node: nodeIdFromInt(2), orderKey: orderKeyFromPosition(0) }),
  ]);

  const { peerA: pa, transportA: ta, detach } = createInMemoryConnectedPeers({
    backendA: a,
    backendB: b,
    codec: treecrdtSyncV0ProtobufCodec,
    peerAOptions: { auth: authA },
    peerBOptions: { auth: authB },
  });

  try {
    const sub = pa.subscribe(ta, { all: {} }, { immediate: false, intervalMs: 0 });
    await expect(sub.done).rejects.toThrow(/UNAUTHORIZED.*capability does not allow filter/i);
  } finally {
    detach();
  }
  void bPk;
});

test("auth: filters require read_structure action (read_payload alone is insufficient)", async () => {
  const docId = "doc-auth-filter-actions";
  const root = "0".repeat(32);

  const a = new MemoryBackend(docId);
  const b = new MemoryBackend(docId);

  const issuerSk = ed25519Utils.randomSecretKey();
  const issuerPk = await getPublicKey(issuerSk);

  const aSk = ed25519Utils.randomSecretKey();
  const aPk = await getPublicKey(aSk);

  const bSk = ed25519Utils.randomSecretKey();
  const bPk = await getPublicKey(bSk);

  const tokenA = issueTreecrdtCapabilityTokenV1({
    issuerPrivateKey: issuerSk,
    subjectPublicKey: aPk,
    docId,
    actions: ["read_payload"],
    rootNodeId: root,
  });

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
    requireProofRef: true,
  });

  const { peerA: pa, transportA: ta, detach } = createInMemoryConnectedPeers({
    backendA: a,
    backendB: b,
    codec: treecrdtSyncV0ProtobufCodec,
    peerAOptions: { auth: authA },
    peerBOptions: { auth: authB },
  });

  try {
    await expect(pa.syncOnce(ta, { all: {} }, { maxCodewords: 1_000, codewordsPerMessage: 64 })).rejects.toThrow(
      /UNAUTHORIZED.*capability does not allow filter/i
    );
  } finally {
    detach();
  }
  void bPk;
});

test("auth: syncOnce accepts doc-wide read_structure capability for filter(all)", async () => {
  const docId = "doc-auth-filter-allow-all";
  const root = "0".repeat(32);

  const a = new MemoryBackend(docId);
  const b = new MemoryBackend(docId);

  const issuerSk = ed25519Utils.randomSecretKey();
  const issuerPk = await getPublicKey(issuerSk);

  const aSk = ed25519Utils.randomSecretKey();
  const aPk = await getPublicKey(aSk);

  const bSk = ed25519Utils.randomSecretKey();
  const bPk = await getPublicKey(bSk);

  // Initiator: read-only token (must still authorize filters).
  const tokenA = issueTreecrdtCapabilityTokenV1({
    issuerPrivateKey: issuerSk,
    subjectPublicKey: aPk,
    docId,
    actions: ["read_structure"],
    rootNodeId: root,
  });

  // Responder: has a write token so it can sign ops it sends.
  const tokenB = issueTreecrdtCapabilityTokenV1({
    issuerPrivateKey: issuerSk,
    subjectPublicKey: bPk,
    docId,
    actions: ["write_structure"],
    rootNodeId: root,
  });

  // Put one op on B so A has something to fetch (A won't send ops).
  await b.applyOps([
    makeOp(bPk, 1, 1, { type: "insert", parent: root, node: nodeIdFromInt(1), orderKey: orderKeyFromPosition(0) }),
  ]);

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
  });

  const { peerA: pa, transportA: ta, detach } = createInMemoryConnectedPeers({
    backendA: a,
    backendB: b,
    codec: treecrdtSyncV0ProtobufCodec,
    peerAOptions: { auth: authA },
    peerBOptions: { auth: authB },
  });

  try {
    await pa.syncOnce(ta, { all: {} }, { maxCodewords: 1_000, codewordsPerMessage: 64 });
    expect(a.hasOp(bytesToHex(bPk), 1)).toBe(true);
  } finally {
    detach();
  }
});

test("auth: syncOnce accepts filter(children) when capability scope matches the parent", async () => {
  const docId = "doc-auth-filter-allow-children";
  const root = "0".repeat(32);

  const a = new MemoryBackend(docId);
  const b = new MemoryBackend(docId);

  const issuerSk = ed25519Utils.randomSecretKey();
  const issuerPk = await getPublicKey(issuerSk);

  const aSk = ed25519Utils.randomSecretKey();
  const aPk = await getPublicKey(aSk);

  const bSk = ed25519Utils.randomSecretKey();
  const bPk = await getPublicKey(bSk);

  const parent = nodeIdFromInt(1);

  const tokenA = issueTreecrdtCapabilityTokenV1({
    issuerPrivateKey: issuerSk,
    subjectPublicKey: aPk,
    docId,
    actions: ["read_structure"],
    rootNodeId: parent,
  });

  const scopeEvaluator = ({ node, scope }: { node: Uint8Array; scope: { root: Uint8Array } }) => {
    const nodeHex = bytesToHex(node);
    const rootHex = bytesToHex(scope.root);
    return nodeHex === rootHex ? "allow" : "deny";
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
    requireProofRef: true,
    scopeEvaluator,
  });

  const { peerA: pa, transportA: ta, detach } = createInMemoryConnectedPeers({
    backendA: a,
    backendB: b,
    codec: treecrdtSyncV0ProtobufCodec,
    peerAOptions: { auth: authA },
    peerBOptions: { auth: authB },
  });

  try {
    await pa.syncOnce(ta, { children: { parent: nodeIdToBytes16(parent) } }, { maxCodewords: 1_000, codewordsPerMessage: 64 });
  } finally {
    detach();
  }
  void bPk;
});

test("auth: syncOnce rejects filter(children) when capability scope does not match the parent", async () => {
  const docId = "doc-auth-filter-deny-children";
  const root = "0".repeat(32);

  const a = new MemoryBackend(docId);
  const b = new MemoryBackend(docId);

  const issuerSk = ed25519Utils.randomSecretKey();
  const issuerPk = await getPublicKey(issuerSk);

  const aSk = ed25519Utils.randomSecretKey();
  const aPk = await getPublicKey(aSk);

  const bSk = ed25519Utils.randomSecretKey();
  const bPk = await getPublicKey(bSk);

  const tokenScopeRoot = nodeIdFromInt(1);
  const requestedParent = nodeIdFromInt(2);

  const tokenA = issueTreecrdtCapabilityTokenV1({
    issuerPrivateKey: issuerSk,
    subjectPublicKey: aPk,
    docId,
    actions: ["read_structure"],
    rootNodeId: tokenScopeRoot,
  });

  const scopeEvaluator = ({ node, scope }: { node: Uint8Array; scope: { root: Uint8Array } }) => {
    const nodeHex = bytesToHex(node);
    const rootHex = bytesToHex(scope.root);
    return nodeHex === rootHex ? "allow" : "deny";
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
    requireProofRef: true,
    scopeEvaluator,
  });

  const { peerA: pa, transportA: ta, detach } = createInMemoryConnectedPeers({
    backendA: a,
    backendB: b,
    codec: treecrdtSyncV0ProtobufCodec,
    peerAOptions: { auth: authA },
    peerBOptions: { auth: authB },
  });

  try {
    await expect(
      pa.syncOnce(ta, { children: { parent: nodeIdToBytes16(requestedParent) } }, { maxCodewords: 1_000, codewordsPerMessage: 64 })
    ).rejects.toThrow(/UNAUTHORIZED.*capability does not allow filter/i);
  } finally {
    detach();
  }
  void bPk;
});

test("auth: filterOutgoingOps hides move/delete/tombstone for excluded subtrees", async () => {
  const docId = "doc-auth-filter-outgoing-exclude";
  const root = "0".repeat(32);

  const issuerSk = ed25519Utils.randomSecretKey();
  const issuerPk = await getPublicKey(issuerSk);

  const senderSk = ed25519Utils.randomSecretKey();
  const senderPk = await getPublicKey(senderSk);

  const receiverSk = ed25519Utils.randomSecretKey();
  const receiverPk = await getPublicKey(receiverSk);

  const publicNode = nodeIdFromInt(1);
  const privateRoot = nodeIdFromInt(2);
  const privateChild = nodeIdFromInt(3);
  const privateSibling = nodeIdFromInt(4);

  const parentByNodeHex = new Map<string, string | null>([
    [root, null],
    [publicNode, root],
    [privateRoot, root],
    // Current tree state (after the move below): child is under sibling, both under privateRoot.
    [privateSibling, privateRoot],
    [privateChild, privateSibling],
  ]);

  const scopeEvaluator = ({
    node,
    scope,
  }: {
    node: Uint8Array;
    scope: { root: Uint8Array; maxDepth?: number; exclude?: Uint8Array[] };
  }) => {
    const rootHex = bytesToHex(scope.root);
    const excludeHex = new Set((scope.exclude ?? []).map((b) => bytesToHex(b)));
    const maxDepth = scope.maxDepth;

    let curHex = bytesToHex(node);
    let distance = 0;

    for (let hops = 0; hops < 10_000; hops += 1) {
      if (excludeHex.has(curHex)) return "deny" as const;
      if (curHex === rootHex) {
        if (maxDepth !== undefined && distance > maxDepth) return "deny" as const;
        return "allow" as const;
      }

      // Reserved ids terminate the chain (unless they are the scope root, handled above).
      if (curHex === root || curHex === "f".repeat(32)) return "deny" as const;

      // If we already traversed `maxDepth` edges without reaching `root`, the node cannot be within scope.
      if (maxDepth !== undefined && distance >= maxDepth) return "deny" as const;

      const parentHex = parentByNodeHex.get(curHex);
      if (parentHex === undefined) return "unknown" as const;
      if (parentHex === null) return "deny" as const;

      curHex = parentHex;
      distance += 1;
    }

    // Defensive: cycles or extreme depth.
    return "unknown" as const;
  };

  const tokenReceiver = issueTreecrdtCapabilityTokenV1({
    issuerPrivateKey: issuerSk,
    subjectPublicKey: receiverPk,
    docId,
    actions: ["read_structure"],
    rootNodeId: root,
    excludeNodeIds: [privateRoot],
  });

  const authSender = createTreecrdtCoseCwtAuth({
    issuerPublicKeys: [issuerPk],
    localPrivateKey: senderSk,
    localPublicKey: senderPk,
    requireProofRef: true,
    scopeEvaluator,
  });

  const authReceiver = createTreecrdtCoseCwtAuth({
    issuerPublicKeys: [issuerPk],
    localPrivateKey: receiverSk,
    localPublicKey: receiverPk,
    localCapabilityTokens: [tokenReceiver],
    requireProofRef: true,
  });

  const receiverCaps = await authReceiver.helloCapabilities?.({ docId });
  expect(receiverCaps).toBeTruthy();

  const ops: Operation[] = [
    makeOp(senderPk, 1, 1, { type: "insert", parent: root, node: publicNode, orderKey: orderKeyFromPosition(0) }),
    makeOp(senderPk, 2, 2, { type: "insert", parent: root, node: privateRoot, orderKey: orderKeyFromPosition(1) }),
    makeOp(senderPk, 3, 3, { type: "insert", parent: privateRoot, node: privateSibling, orderKey: orderKeyFromPosition(0) }),
    makeOp(senderPk, 4, 4, { type: "insert", parent: privateRoot, node: privateChild, orderKey: orderKeyFromPosition(1) }),
    makeOp(senderPk, 5, 5, { type: "move", node: privateChild, newParent: privateSibling, orderKey: orderKeyFromPosition(0) }),
    makeOp(senderPk, 6, 6, { type: "payload", node: privateChild, payload: new Uint8Array([1, 2, 3]) }),
    makeOp(senderPk, 7, 7, { type: "delete", node: privateChild }),
    makeOp(senderPk, 8, 8, { type: "tombstone", node: privateChild }),
    makeOp(senderPk, 9, 9, { type: "move", node: publicNode, newParent: root, orderKey: orderKeyFromPosition(0) }),
    makeOp(senderPk, 10, 10, { type: "delete", node: publicNode }),
  ];

  const allowed = await authSender.filterOutgoingOps?.(ops, {
    docId,
    purpose: "reconcile",
    filter: { all: {} },
    capabilities: receiverCaps ?? [],
  });

  expect(allowed).toBeTruthy();
  expect(allowed?.length).toBe(ops.length);

  // Allowed: ops for the public subtree.
  expect(allowed?.[0]).toBe(true); // insert(public)
  expect(allowed?.[8]).toBe(true); // move(public)
  expect(allowed?.[9]).toBe(true); // delete(public)

  // Denied: everything under the excluded private root (including move/delete/tombstone).
  for (const i of [1, 2, 3, 4, 5, 6, 7]) {
    expect(allowed?.[i]).toBe(false);
  }
});

test("auth: delegated capability token can be verified via issuer-signed proof", async () => {
  const docId = "doc-auth-delegation-basic";
  const root = "0".repeat(32);

  const issuerSk = ed25519Utils.randomSecretKey();
  const issuerPk = await getPublicKey(issuerSk);

  const delegatorSk = ed25519Utils.randomSecretKey();
  const delegatorPk = await getPublicKey(delegatorSk);

  const recipientSk = ed25519Utils.randomSecretKey();
  const recipientPk = await getPublicKey(recipientSk);

  const proof = issueTreecrdtCapabilityTokenV1({
    issuerPrivateKey: issuerSk,
    subjectPublicKey: delegatorPk,
    docId,
    rootNodeId: root,
    actions: ["write_structure", "grant"],
  });

  const delegated = issueTreecrdtDelegatedCapabilityTokenV1({
    delegatorPrivateKey: delegatorSk,
    delegatorProofToken: proof,
    subjectPublicKey: recipientPk,
    docId,
    rootNodeId: root,
    actions: ["write_structure"],
  });

  const described = await describeTreecrdtCapabilityTokenV1({ tokenBytes: delegated, issuerPublicKeys: [issuerPk], docId });
  expect(bytesToHex(described.subjectPublicKey)).toBe(bytesToHex(recipientPk));

  const verifierSk = ed25519Utils.randomSecretKey();
  const verifierPk = await getPublicKey(verifierSk);
  const authVerifier = createTreecrdtCoseCwtAuth({
    issuerPublicKeys: [issuerPk],
    localPrivateKey: verifierSk,
    localPublicKey: verifierPk,
    requireProofRef: true,
  });

  const authRecipient = createTreecrdtCoseCwtAuth({
    issuerPublicKeys: [issuerPk],
    localPrivateKey: recipientSk,
    localPublicKey: recipientPk,
    localCapabilityTokens: [delegated],
    requireProofRef: true,
  });

  const helloCaps = await authRecipient.helloCapabilities?.({ docId });
  expect(helloCaps).toBeTruthy();
  await authVerifier.onHello?.({ capabilities: helloCaps ?? [], filters: [], maxLamport: 0n }, { docId });

  const node = nodeIdFromInt(1);
  const op: Operation = makeOp(recipientPk, 1, 1, { type: "insert", parent: root, node, orderKey: orderKeyFromPosition(0) });
  const auth = await authRecipient.signOps?.([op], { docId, purpose: "reconcile", filterId: "all" });
  expect(auth).toBeTruthy();

  await authVerifier.verifyOps?.([op], auth ?? undefined, { docId, purpose: "reconcile", filterId: "all" });
});

test("auth: delegation requires grant action in proof token", async () => {
  const docId = "doc-auth-delegation-requires-grant";
  const root = "0".repeat(32);

  const issuerSk = ed25519Utils.randomSecretKey();
  const issuerPk = await getPublicKey(issuerSk);

  const delegatorSk = ed25519Utils.randomSecretKey();
  const delegatorPk = await getPublicKey(delegatorSk);

  const recipientPk = await getPublicKey(ed25519Utils.randomSecretKey());

  const proof = issueTreecrdtCapabilityTokenV1({
    issuerPrivateKey: issuerSk,
    subjectPublicKey: delegatorPk,
    docId,
    rootNodeId: root,
    actions: ["write_structure"],
  });

  const delegated = issueTreecrdtDelegatedCapabilityTokenV1({
    delegatorPrivateKey: delegatorSk,
    delegatorProofToken: proof,
    subjectPublicKey: recipientPk,
    docId,
    rootNodeId: root,
    actions: ["write_structure"],
  });

  await expect(
    describeTreecrdtCapabilityTokenV1({ tokenBytes: delegated, issuerPublicKeys: [issuerPk], docId })
  ).rejects.toThrow(/delegation proof/i);
});

test("auth: delegation proof can itself be delegated (chain)", async () => {
  const docId = "doc-auth-delegation-chain";
  const root = "0".repeat(32);

  const issuerSk = ed25519Utils.randomSecretKey();
  const issuerPk = await getPublicKey(issuerSk);

  const delegatorSk = ed25519Utils.randomSecretKey();
  const delegatorPk = await getPublicKey(delegatorSk);

  const intermediateSk = ed25519Utils.randomSecretKey();
  const intermediatePk = await getPublicKey(intermediateSk);

  const recipientPk = await getPublicKey(ed25519Utils.randomSecretKey());

  // Issuer grants delegator the ability to delegate.
  const proofAtoB = issueTreecrdtCapabilityTokenV1({
    issuerPrivateKey: issuerSk,
    subjectPublicKey: delegatorPk,
    docId,
    rootNodeId: root,
    actions: ["write_structure", "grant"],
  });

  // Delegator grants intermediate the ability to further delegate (chain).
  const proofBtoC = issueTreecrdtDelegatedCapabilityTokenV1({
    delegatorPrivateKey: delegatorSk,
    delegatorProofToken: proofAtoB,
    subjectPublicKey: intermediatePk,
    docId,
    rootNodeId: root,
    actions: ["write_structure", "grant"],
  });

  // Intermediate delegates to recipient using delegated proof token.
  const delegatedCtoD = issueTreecrdtDelegatedCapabilityTokenV1({
    delegatorPrivateKey: intermediateSk,
    delegatorProofToken: proofBtoC,
    subjectPublicKey: recipientPk,
    docId,
    rootNodeId: root,
    actions: ["write_structure"],
  });

  const described = await describeTreecrdtCapabilityTokenV1({ tokenBytes: delegatedCtoD, issuerPublicKeys: [issuerPk], docId });
  expect(bytesToHex(described.subjectPublicKey)).toBe(bytesToHex(recipientPk));
});

test("auth: revoked token id is rejected by describeTreecrdtCapabilityTokenV1", async () => {
  const docId = "doc-auth-revoked-token";

  const issuerSk = ed25519Utils.randomSecretKey();
  const issuerPk = await getPublicKey(issuerSk);
  const subjectPk = await getPublicKey(ed25519Utils.randomSecretKey());

  const token = issueTreecrdtCapabilityTokenV1({
    issuerPrivateKey: issuerSk,
    subjectPublicKey: subjectPk,
    docId,
    actions: ["write_structure"],
  });

  await expect(
    describeTreecrdtCapabilityTokenV1({
      tokenBytes: token,
      issuerPublicKeys: [issuerPk],
      docId,
      revokedCapabilityTokenIds: [deriveTokenIdV1(token)],
    })
  ).rejects.toThrow(/capability token revoked/i);
});

test("auth: delegated token is rejected when proof token is revoked", async () => {
  const docId = "doc-auth-revoked-proof";
  const root = "0".repeat(32);

  const issuerSk = ed25519Utils.randomSecretKey();
  const issuerPk = await getPublicKey(issuerSk);

  const delegatorSk = ed25519Utils.randomSecretKey();
  const delegatorPk = await getPublicKey(delegatorSk);
  const recipientPk = await getPublicKey(ed25519Utils.randomSecretKey());

  const proof = issueTreecrdtCapabilityTokenV1({
    issuerPrivateKey: issuerSk,
    subjectPublicKey: delegatorPk,
    docId,
    rootNodeId: root,
    actions: ["write_structure", "grant"],
  });

  const delegated = issueTreecrdtDelegatedCapabilityTokenV1({
    delegatorPrivateKey: delegatorSk,
    delegatorProofToken: proof,
    subjectPublicKey: recipientPk,
    docId,
    rootNodeId: root,
    actions: ["write_structure"],
  });

  await expect(
    describeTreecrdtCapabilityTokenV1({
      tokenBytes: delegated,
      issuerPublicKeys: [issuerPk],
      docId,
      revokedCapabilityTokenIds: [deriveTokenIdV1(proof)],
    })
  ).rejects.toThrow(/capability token revoked/i);
});

test("auth: onHello rejects revoked peer capability tokens", async () => {
  const docId = "doc-auth-revoked-hello";
  const root = "0".repeat(32);

  const issuerSk = ed25519Utils.randomSecretKey();
  const issuerPk = await getPublicKey(issuerSk);

  const senderSk = ed25519Utils.randomSecretKey();
  const senderPk = await getPublicKey(senderSk);
  const receiverSk = ed25519Utils.randomSecretKey();
  const receiverPk = await getPublicKey(receiverSk);

  const tokenSender = issueTreecrdtCapabilityTokenV1({
    issuerPrivateKey: issuerSk,
    subjectPublicKey: senderPk,
    docId,
    rootNodeId: root,
    actions: ["write_structure"],
  });

  const authSender = createTreecrdtCoseCwtAuth({
    issuerPublicKeys: [issuerPk],
    localPrivateKey: senderSk,
    localPublicKey: senderPk,
    localCapabilityTokens: [tokenSender],
    requireProofRef: true,
  });

  const authReceiver = createTreecrdtCoseCwtAuth({
    issuerPublicKeys: [issuerPk],
    localPrivateKey: receiverSk,
    localPublicKey: receiverPk,
    revokedCapabilityTokenIds: [deriveTokenIdV1(tokenSender)],
    requireProofRef: true,
  });

  const helloCaps = await authSender.helloCapabilities?.({ docId });
  expect(helloCaps).toBeTruthy();

  await expect(
    authReceiver.onHello!({ capabilities: helloCaps ?? [], filters: [], maxLamport: 0n }, { docId })
  ).rejects.toThrow(/capability token revoked/i);
});

test("auth: delegated capability root may be a descendant of proof root (with scope evaluator)", async () => {
  const docId = "doc-auth-delegation-narrow-root";
  const root = nodeIdFromInt(1);
  const child = nodeIdFromInt(2);

  const issuerSk = ed25519Utils.randomSecretKey();
  const issuerPk = await getPublicKey(issuerSk);

  const delegatorSk = ed25519Utils.randomSecretKey();
  const delegatorPk = await getPublicKey(delegatorSk);

  const recipientPk = await getPublicKey(ed25519Utils.randomSecretKey());

  const proof = issueTreecrdtCapabilityTokenV1({
    issuerPrivateKey: issuerSk,
    subjectPublicKey: delegatorPk,
    docId,
    rootNodeId: root,
    actions: ["write_structure", "grant"],
  });

  const delegated = issueTreecrdtDelegatedCapabilityTokenV1({
    delegatorPrivateKey: delegatorSk,
    delegatorProofToken: proof,
    subjectPublicKey: recipientPk,
    docId,
    rootNodeId: child,
    actions: ["write_structure"],
  });

  await expect(describeTreecrdtCapabilityTokenV1({ tokenBytes: delegated, issuerPublicKeys: [issuerPk], docId })).rejects.toThrow(
    /scope evaluator/i
  );

  const parentByNode = new Map<string, string>([[child, root]]);
  const scopeEvaluator = async ({ node, scope }: { node: Uint8Array; scope: { root: Uint8Array } }) => {
    const rootHex = bytesToHex(scope.root);
    let curHex = bytesToHex(node);
    for (let hops = 0; hops < 16; hops += 1) {
      if (curHex === rootHex) return "allow" as const;
      const parent = parentByNode.get(curHex);
      if (!parent) return "deny" as const;
      curHex = parent;
    }
    return "unknown" as const;
  };

  const described = await describeTreecrdtCapabilityTokenV1({
    tokenBytes: delegated,
    issuerPublicKeys: [issuerPk],
    docId,
    scopeEvaluator: scopeEvaluator as any,
  });
  expect(bytesToHex(described.subjectPublicKey)).toBe(bytesToHex(recipientPk));
});

test("auth: records peer identity chain capability via onPeerIdentityChain", async () => {
  const docId = "doc-auth-identity-chain";

  const issuerSk = ed25519Utils.randomSecretKey();
  const issuerPk = await getPublicKey(issuerSk);

  const localSk = ed25519Utils.randomSecretKey();
  const localPk = await getPublicKey(localSk);

  const identitySk = ed25519Utils.randomSecretKey();
  const identityPk = await getPublicKey(identitySk);
  const deviceSk = ed25519Utils.randomSecretKey();
  const devicePk = await getPublicKey(deviceSk);
  const replicaSk = ed25519Utils.randomSecretKey();
  const replicaPk = await getPublicKey(replicaSk);
  void replicaSk;

  const deviceCertBytes = issueDeviceCertV1({ identityPrivateKey: identitySk, devicePublicKey: devicePk });
  const replicaCertBytes = issueReplicaCertV1({ devicePrivateKey: deviceSk, docId, replicaPublicKey: replicaPk });
  const chainCap = createTreecrdtIdentityChainCapabilityV1({ identityPublicKey: identityPk, deviceCertBytes, replicaCertBytes });

  let seen: { identityPkHex: string; devicePkHex: string; replicaPkHex: string } | null = null;

  const auth = createTreecrdtCoseCwtAuth({
    issuerPublicKeys: [issuerPk],
    localPrivateKey: localSk,
    localPublicKey: localPk,
    requireProofRef: true,
    onPeerIdentityChain: (c) => {
      seen = {
        identityPkHex: bytesToHex(c.identityPublicKey),
        devicePkHex: bytesToHex(c.devicePublicKey),
        replicaPkHex: bytesToHex(c.replicaPublicKey),
      };
    },
  });

  await auth.onHello?.({ capabilities: [chainCap], filters: [], maxLamport: 0n }, { docId });

  expect(seen).toEqual({
    identityPkHex: bytesToHex(identityPk),
    devicePkHex: bytesToHex(devicePk),
    replicaPkHex: bytesToHex(replicaPk),
  });
});

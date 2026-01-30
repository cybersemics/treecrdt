import { createHash } from "node:crypto";

import { expect, test } from "vitest";

import type { Operation } from "@treecrdt/interface";
import { bytesToHex } from "@treecrdt/interface/ids";
import { makeOp, nodeIdFromInt } from "@treecrdt/benchmark";
import { hashes as ed25519Hashes, getPublicKey, utils as ed25519Utils } from "@noble/ed25519";
import { sha512 } from "@noble/hashes/sha512";
import { encode as cborEncode, rfc8949EncodeOptions } from "cborg";

import { treecrdtSyncV0ProtobufCodec } from "../dist/protobuf.js";
import { createInMemoryConnectedPeers } from "../dist/in-memory.js";
import { coseSign1Ed25519 } from "../dist/cose.js";
import { createTreecrdtCoseCwtAuth } from "../dist/treecrdt-auth.js";
import type { Filter, OpRef, SyncBackend } from "../dist/types.js";

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
    throw new Error("MemoryBackend only supports filter { all: {} }");
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

function makeCapabilityToken(opts: {
  issuerPrivateKey: Uint8Array;
  subjectPublicKey: Uint8Array;
  docId: string;
}): Uint8Array {
  const cnf = new Map<unknown, unknown>([["pub", opts.subjectPublicKey]]);
  const claims = new Map<unknown, unknown>([
    [3, opts.docId], // CWT `aud`
    [8, cnf], // CWT `cnf`
  ]);
  const payload = cborEncode(claims, rfc8949EncodeOptions);
  return coseSign1Ed25519({ payload, privateKey: opts.issuerPrivateKey });
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

  const tokenA = makeCapabilityToken({ issuerPrivateKey: issuerSk, subjectPublicKey: aPk, docId });
  const tokenB = makeCapabilityToken({ issuerPrivateKey: issuerSk, subjectPublicKey: bPk, docId });

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
  const tokenB = makeCapabilityToken({ issuerPrivateKey: issuerSk, subjectPublicKey: bPk, docId });

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
      /missing op auth/i
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

  const tokenA = makeCapabilityToken({ issuerPrivateKey: issuerSk, subjectPublicKey: aClaimPk, docId });
  const tokenB = makeCapabilityToken({ issuerPrivateKey: issuerSk, subjectPublicKey: bPk, docId });

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

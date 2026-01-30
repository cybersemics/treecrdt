import { expect, test } from "vitest";

import { randomBytes } from "node:crypto";

import type { SqliteRunner } from "@treecrdt/interface/sqlite";
import { createTreecrdtSqliteAdapter, decodeSqliteOpRefs, decodeSqliteOps } from "@treecrdt/interface/sqlite";
import { ROOT_NODE_ID_HEX, bytesToHex, nodeIdToBytes16, replicaIdToBytes } from "@treecrdt/interface/ids";
import { makeOp, maxLamport, nodeIdFromInt } from "@treecrdt/benchmark";
import type { Operation } from "@treecrdt/interface";

import { createInMemoryConnectedPeers, makeQueuedSyncBackend } from "@treecrdt/sync/in-memory";
import { treecrdtSyncV0ProtobufCodec } from "@treecrdt/sync/protobuf";
import {
  createTreecrdtCoseCwtAuth,
  createTreecrdtSqliteSubtreeScopeEvaluator,
  createTreecrdtSyncSqlitePendingOpsStore,
  coseSign1Ed25519,
  deriveOpRefV0,
} from "@treecrdt/sync";

import { hashes as ed25519Hashes, getPublicKey, utils as ed25519Utils } from "@noble/ed25519";
import { sha512 } from "@noble/hashes/sha512";
import { encode as cborEncode, rfc8949EncodeOptions } from "cborg";

ed25519Hashes.sha512 = sha512;

function orderKeyFromPosition(position: number): Uint8Array {
  if (!Number.isInteger(position) || position < 0) throw new Error(`invalid position: ${position}`);
  const n = position + 1;
  if (n > 0xffff) throw new Error(`position too large for u16 order key: ${position}`);
  const bytes = new Uint8Array(2);
  new DataView(bytes.buffer).setUint16(0, n, false);
  return bytes;
}

function replicaFromLabel(label: string): Uint8Array {
  const encoded = new TextEncoder().encode(label);
  if (encoded.length === 0) throw new Error("replica label must not be empty");
  const out = new Uint8Array(32);
  for (let i = 0; i < out.length; i += 1) out[i] = encoded[i % encoded.length]!;
  return out;
}

function createRunner(db: any): SqliteRunner {
  const stmtCache = new Map<string, any>();
  const prepare = (sql: string) => {
    const cached = stmtCache.get(sql);
    if (cached) return cached;
    const stmt = db.prepare(sql);
    stmtCache.set(sql, stmt);
    return stmt;
  };

  const toBindings = (params: unknown[]) =>
    params.reduce<Record<number, unknown>>((acc, val, idx) => {
      acc[idx + 1] = val;
      return acc;
    }, {});

  return {
    exec: (sql) => db.exec(sql),
    getText: (sql, params = []) => {
      const row = prepare(sql).get(toBindings(params));
      if (row === undefined || row === null) return null;
      const val = Object.values(row as Record<string, unknown>)[0];
      if (val === undefined || val === null) return null;
      return String(val);
    },
  };
}

function deriveOpRefForTest(docId: string, op: Operation): Uint8Array {
  return deriveOpRefV0(docId, {
    replica: replicaIdToBytes(op.meta.id.replica),
    counter: BigInt(op.meta.id.counter),
  });
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

test("sqlite sidecar: pending ops store roundtrips ops + auth in the same DB", async () => {
  const { default: Database } = await import("better-sqlite3");
  const { loadTreecrdtExtension, defaultExtensionPath } = await import("../dist/index.js");

  const docId = "doc-sqlite-pending-sidecar";
  const db = new Database(":memory:");
  loadTreecrdtExtension(db, { extensionPath: defaultExtensionPath() });

  const runner = createRunner(db);
  const adapter = createTreecrdtSqliteAdapter(runner);
  await adapter.setDocId(docId);

  const store = createTreecrdtSyncSqlitePendingOpsStore({ runner, docId });
  await store.init();

  const replica = replicaFromLabel("a");
  const op = makeOp(replica, 1, 1, {
    type: "insert",
    parent: ROOT_NODE_ID_HEX,
    node: nodeIdFromInt(1),
    orderKey: orderKeyFromPosition(0),
  });
  const sig = randomBytes(64);
  const proofRef = randomBytes(16);

  await store.storePendingOps([
    { op, auth: { sig, proofRef }, reason: "missing_context", message: "test" },
  ]);

  const listed = await store.listPendingOps();
  expect(listed.length).toBe(1);
  expect(bytesToHex(listed[0]!.op.meta.id.replica)).toBe(bytesToHex(replica));
  expect(listed[0]!.op.meta.id.counter).toBe(1);
  expect(bytesToHex(listed[0]!.auth.sig)).toBe(bytesToHex(sig));
  expect(bytesToHex(listed[0]!.auth.proofRef!)).toBe(bytesToHex(proofRef));
  expect(listed[0]!.message).toBe("test");

  const refs = await store.listPendingOpRefs();
  expect(refs.length).toBe(1);
  expect(bytesToHex(refs[0]!)).toBe(bytesToHex(deriveOpRefForTest(docId, op)));

  await store.deletePendingOps([op]);
  expect((await store.listPendingOps()).length).toBe(0);
});

test("sqlite subtree evaluator: allow/deny/unknown matches materialized tree", async () => {
  const { default: Database } = await import("better-sqlite3");
  const { loadTreecrdtExtension, defaultExtensionPath } = await import("../dist/index.js");

  const docId = "doc-sqlite-scope-eval";
  const db = new Database(":memory:");
  loadTreecrdtExtension(db, { extensionPath: defaultExtensionPath() });

  const runner = createRunner(db);
  const adapter = createTreecrdtSqliteAdapter(runner);
  await adapter.setDocId(docId);

  const evalScope = createTreecrdtSqliteSubtreeScopeEvaluator(runner);
  const subtreeRoot = nodeIdFromInt(1);
  const child = nodeIdFromInt(2);
  const unrelated = nodeIdFromInt(3);

  // Missing node => unknown context.
  expect(
    await evalScope({ docId, node: nodeIdToBytes16(child), scope: { root: nodeIdToBytes16(subtreeRoot) } })
  ).toBe("unknown");

  // Insert child under subtreeRoot (root node itself need not exist as a row).
  await adapter.appendOps?.(
    [
      makeOp(replicaFromLabel("a"), 1, 1, {
        type: "insert",
        parent: subtreeRoot,
        node: child,
        orderKey: orderKeyFromPosition(0),
      }),
    ],
    nodeIdToBytes16,
    replicaIdToBytes
  );
  expect(
    await evalScope({ docId, node: nodeIdToBytes16(child), scope: { root: nodeIdToBytes16(subtreeRoot) } })
  ).toBe("allow");

  // Insert unrelated node under ROOT; should be outside subtreeRoot.
  await adapter.appendOps?.(
    [
      makeOp(replicaFromLabel("a"), 2, 2, {
        type: "insert",
        parent: ROOT_NODE_ID_HEX,
        node: unrelated,
        orderKey: orderKeyFromPosition(0),
      }),
    ],
    nodeIdToBytes16,
    replicaIdToBytes
  );
  expect(
    await evalScope({ docId, node: nodeIdToBytes16(unrelated), scope: { root: nodeIdToBytes16(subtreeRoot) } })
  ).toBe("deny");
});

test("sqlite sync: pending_context ops are stored and later applied when context arrives", async () => {
  const { default: Database } = await import("better-sqlite3");
  const { loadTreecrdtExtension, defaultExtensionPath } = await import("../dist/index.js");

  const docId = "doc-sqlite-sync-pending";
  const subtreeRoot = nodeIdFromInt(1);
  const child = nodeIdFromInt(2);

  const mkDb = () => {
    const db = new Database(":memory:");
    loadTreecrdtExtension(db, { extensionPath: defaultExtensionPath() });
    return db;
  };

  const dbA = mkDb();
  const dbB = mkDb();

  const runnerA = createRunner(dbA);
  const runnerB = createRunner(dbB);
  const adapterA = createTreecrdtSqliteAdapter(runnerA);
  const adapterB = createTreecrdtSqliteAdapter(runnerB);
  await Promise.all([adapterA.setDocId(docId), adapterB.setDocId(docId)]);

  const pendingB = createTreecrdtSyncSqlitePendingOpsStore({ runner: runnerB, docId });
  await pendingB.init();

  const backendA = makeQueuedSyncBackend<Operation>({
    docId,
    initialMaxLamport: 0,
    maxLamportFromOps: maxLamport,
    listOpRefs: async (filter) => {
      if ("all" in filter) return decodeSqliteOpRefs(await adapterA.opRefsAll());
      return decodeSqliteOpRefs(await adapterA.opRefsChildren(filter.children.parent));
    },
    getOpsByOpRefs: async (opRefs) => decodeSqliteOps(await adapterA.opsByOpRefs(opRefs)),
    applyOps: async (ops) => {
      await adapterA.appendOps?.(ops, nodeIdToBytes16, replicaIdToBytes);
    },
  });

  const backendB = Object.assign(
    makeQueuedSyncBackend<Operation>({
      docId,
      initialMaxLamport: 0,
      maxLamportFromOps: maxLamport,
      listOpRefs: async (filter) => {
        if ("all" in filter) return decodeSqliteOpRefs(await adapterB.opRefsAll());
        return decodeSqliteOpRefs(await adapterB.opRefsChildren(filter.children.parent));
      },
      getOpsByOpRefs: async (opRefs) => decodeSqliteOps(await adapterB.opsByOpRefs(opRefs)),
      applyOps: async (ops) => {
        await adapterB.appendOps?.(ops, nodeIdToBytes16, replicaIdToBytes);
      },
    }),
    {
      storePendingOps: pendingB.storePendingOps,
      listPendingOps: pendingB.listPendingOps,
      deletePendingOps: pendingB.deletePendingOps,
    }
  );

  const issuerSk = ed25519Utils.randomSecretKey();
  const issuerPk = await getPublicKey(issuerSk);

  const aSk = ed25519Utils.randomSecretKey();
  const aPk = await getPublicKey(aSk);
  const bSk = ed25519Utils.randomSecretKey();
  const bPk = await getPublicKey(bSk);
  const aHex = bytesToHex(aPk);

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
    root: ROOT_NODE_ID_HEX,
    actions: ["write_structure", "write_payload"],
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
    scopeEvaluator: createTreecrdtSqliteSubtreeScopeEvaluator(runnerB),
  });

  const { peerA, peerB, transportB, detach } = createInMemoryConnectedPeers<Operation>({
    backendA,
    backendB,
    codec: treecrdtSyncV0ProtobufCodec,
    peerAOptions: { auth: authA, maxOpsPerBatch: 1 },
    peerBOptions: { auth: authB, maxOpsPerBatch: 1 },
  });

  try {
    // Exchange capabilities first.
    await peerB.syncOnce(transportB, { all: {} }, { maxCodewords: 10_000, codewordsPerMessage: 256 });

    const sub = peerB.subscribe(transportB, { all: {} }, { immediate: false, intervalMs: 0 });

    // Ensure subscription is registered before applying any ops.
    const start = Date.now();
    while ((peerA as any).responderSubscriptions?.size !== 1) {
      if (Date.now() - start > 2_000) throw new Error("expected responder subscription to register");
      await new Promise((r) => setTimeout(r, 10));
    }

    // Payload arrives before insert => pending_context.
    await backendA.applyOps([
      makeOp(aPk, 1, 1, { type: "payload", node: child, payload: new Uint8Array([1, 2, 3]) }),
    ]);
    void peerA.notifyLocalUpdate();

    const waitPending = async () => {
      const p = await pendingB.listPendingOps();
      return p.some((x) => bytesToHex(x.op.meta.id.replica) === aHex && x.op.meta.id.counter === 1);
    };
    const startPending = Date.now();
    while (!(await waitPending())) {
      if (Date.now() - startPending > 2_000) throw new Error("expected pending payload op");
      await new Promise((r) => setTimeout(r, 10));
    }

    // Now insert arrives; should apply insert and then reprocess pending payload.
    await backendA.applyOps([
      makeOp(aPk, 2, 2, { type: "insert", parent: subtreeRoot, node: child, orderKey: orderKeyFromPosition(0) }),
    ]);
    void peerA.notifyLocalUpdate();

    const waitApplied = async () => {
      const raw = await adapterB.opsSince(0);
      const ops = decodeSqliteOps(raw);
      return (
        ops.some((o) => bytesToHex(o.meta.id.replica) === aHex && o.meta.id.counter === 1) &&
        ops.some((o) => bytesToHex(o.meta.id.replica) === aHex && o.meta.id.counter === 2)
      );
    };
    const startApplied = Date.now();
    while (!(await waitApplied())) {
      if (Date.now() - startApplied > 2_000) throw new Error("expected insert + payload ops to be applied");
      await new Promise((r) => setTimeout(r, 10));
    }

    expect((await pendingB.listPendingOps()).length).toBe(0);

    sub.stop();
    await sub.done;
  } finally {
    detach();
  }
});

import { randomUUID } from "node:crypto";

import { expect, beforeAll, afterAll, beforeEach, describe, test } from "vitest";

import type { Operation } from "@treecrdt/interface";
import { bytesToHex, nodeIdToBytes16 } from "@treecrdt/interface/ids";
import { makeOp, nodeIdFromInt } from "@treecrdt/benchmark";

import { createPostgresNapiSyncBackendFactory } from "../dist/index.js";

const POSTGRES_URL = process.env.TREECRDT_POSTGRES_URL;
const maybeDescribe = POSTGRES_URL ? describe : describe.skip;

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
  if (encoded.length === 0) throw new Error("label must not be empty");
  const out = new Uint8Array(32);
  for (let i = 0; i < out.length; i += 1) out[i] = encoded[i % encoded.length]!;
  return out;
}

maybeDescribe("postgres sync backend conformance (rust napi)", () => {
  const docA = `doc-${randomUUID()}`;
  const docB = `doc-${randomUUID()}`;
  const root = "0".repeat(32);
  const replicas = {
    a: replicaFromLabel("a"),
    b: replicaFromLabel("b"),
    c: replicaFromLabel("c"),
    d: replicaFromLabel("d"),
    seed: replicaFromLabel("seed"),
    z: replicaFromLabel("z"),
  };

  let factory!: ReturnType<typeof createPostgresNapiSyncBackendFactory>;

  beforeAll(async () => {
    factory = createPostgresNapiSyncBackendFactory(POSTGRES_URL!);
    await factory.ensureSchema();
  });

  beforeEach(async () => {
    await factory.resetDocForTests(docA);
    await factory.resetDocForTests(docB);
  });

  afterAll(async () => {
    // no-op: native layer opens per-call connections
  });

  test("applyOps is idempotent and maxLamport is monotonic", async () => {
    const backend = await factory.open(docA);

    const op1 = makeOp(replicas.a, 1, 1, {
      type: "insert",
      parent: root,
      node: nodeIdFromInt(1),
      orderKey: orderKeyFromPosition(0),
    });
    const op2 = makeOp(replicas.a, 2, 7, {
      type: "payload",
      node: nodeIdFromInt(1),
      payload: new Uint8Array([1, 2, 3]),
    });

    await backend.applyOps([op1, op2]);
    await backend.applyOps([op1, op2]);

    const allRefs = await backend.listOpRefs({ all: {} });
    expect(allRefs).toHaveLength(2);

    const maxLamport = await backend.maxLamport();
    expect(maxLamport).toBe(7n);
  });

  test("doc isolation holds across multiple docs", async () => {
    const a = await factory.open(docA);
    const b = await factory.open(docB);

    await a.applyOps([
      makeOp(replicas.a, 1, 1, {
        type: "insert",
        parent: root,
        node: nodeIdFromInt(1),
        orderKey: orderKeyFromPosition(0),
      }),
    ]);

    await b.applyOps([
      makeOp(replicas.b, 1, 1, {
        type: "insert",
        parent: root,
        node: nodeIdFromInt(2),
        orderKey: orderKeyFromPosition(0),
      }),
    ]);

    const refsA = await a.listOpRefs({ all: {} });
    const refsB = await b.listOpRefs({ all: {} });
    expect(refsA).toHaveLength(1);
    expect(refsB).toHaveLength(1);
    expect(bytesToHex(refsA[0]!)).not.toBe(bytesToHex(refsB[0]!));
  });

  test("getOpsByOpRefs preserves caller order and fails on unknown refs", async () => {
    const backend = await factory.open(docA);

    const op1 = makeOp(replicas.a, 1, 1, {
      type: "insert",
      parent: root,
      node: nodeIdFromInt(11),
      orderKey: orderKeyFromPosition(0),
    });
    const op2 = makeOp(replicas.a, 2, 2, {
      type: "insert",
      parent: root,
      node: nodeIdFromInt(12),
      orderKey: orderKeyFromPosition(1),
    });

    await backend.applyOps([op1, op2]);
    const refs = await backend.listOpRefs({ all: {} });
    expect(refs).toHaveLength(2);

    const reversed = [refs[1]!, refs[0]!];
    const ops = await backend.getOpsByOpRefs(reversed);
    expect(ops[0]?.meta.id.counter).toBe(2);
    expect(ops[1]?.meta.id.counter).toBe(1);

    await expect(backend.getOpsByOpRefs([new Uint8Array(16)])).rejects.toThrow(/opRef missing locally/);
  });

  test("children filter includes move boundary ops and payload updates", async () => {
    const backend = await factory.open(docA);

    const p1 = nodeIdFromInt(101);
    const p2 = nodeIdFromInt(102);
    const n = nodeIdFromInt(103);

    await backend.applyOps([
      makeOp(replicas.a, 1, 1, { type: "insert", parent: root, node: p1, orderKey: orderKeyFromPosition(0) }),
      makeOp(replicas.a, 2, 2, { type: "insert", parent: root, node: p2, orderKey: orderKeyFromPosition(1) }),
      makeOp(replicas.a, 3, 3, { type: "insert", parent: p1, node: n, orderKey: orderKeyFromPosition(0) }),
      makeOp(replicas.a, 4, 4, { type: "payload", node: n, payload: new Uint8Array([7]) }),
      makeOp(replicas.a, 5, 5, { type: "move", node: n, newParent: p2, orderKey: orderKeyFromPosition(0) }),
      makeOp(replicas.a, 6, 6, { type: "payload", node: n, payload: new Uint8Array([8]) }),
    ]);

    const refs = await backend.listOpRefs({ children: { parent: nodeIdToBytes16(p2) } });
    const ops = await backend.getOpsByOpRefs(refs);
    const kinds = ops.map((op) => op.kind.type);

    expect(kinds.filter((k) => k === "move")).toHaveLength(1);
    expect(kinds.filter((k) => k === "payload").length).toBeGreaterThanOrEqual(1);
  });

  test("listOpRefs(all) uses canonical op ordering on lamport ties", async () => {
    const backend = await factory.open(docA);

    const ops: Operation[] = [
      makeOp(replicas.z, 1, 5, {
        type: "insert",
        parent: root,
        node: nodeIdFromInt(201),
        orderKey: orderKeyFromPosition(0),
      }),
      makeOp(replicas.a, 2, 5, {
        type: "insert",
        parent: root,
        node: nodeIdFromInt(202),
        orderKey: orderKeyFromPosition(1),
      }),
      makeOp(replicas.a, 1, 5, {
        type: "insert",
        parent: root,
        node: nodeIdFromInt(203),
        orderKey: orderKeyFromPosition(2),
      }),
    ];

    await backend.applyOps(ops);

    const refs = await backend.listOpRefs({ all: {} });
    const ordered = await backend.getOpsByOpRefs(refs);
    const keys = ordered.map((op) => `${bytesToHex(op.meta.id.replica)}:${op.meta.id.counter}`);
    expect(keys).toEqual([
      `${bytesToHex(replicas.a)}:1`,
      `${bytesToHex(replicas.a)}:2`,
      `${bytesToHex(replicas.z)}:1`,
    ]);
  });

  test("children filter scan follows canonical op ordering on lamport ties", async () => {
    const backend = await factory.open(docA);

    const p1 = nodeIdFromInt(301);
    const p2 = nodeIdFromInt(302);
    const n = nodeIdFromInt(303);

    await backend.applyOps([
      makeOp(replicas.seed, 1, 1, { type: "insert", parent: p1, node: n, orderKey: orderKeyFromPosition(0) }),
      makeOp(replicas.seed, 2, 2, { type: "payload", node: n, payload: new Uint8Array([11]) }),
      makeOp(replicas.z, 1, 5, { type: "move", node: n, newParent: p2, orderKey: orderKeyFromPosition(0) }),
      makeOp(replicas.a, 1, 5, { type: "move", node: n, newParent: p1, orderKey: orderKeyFromPosition(0) }),
    ]);

    const refs = await backend.listOpRefs({ children: { parent: nodeIdToBytes16(p1) } });
    const ops = await backend.getOpsByOpRefs(refs);
    const moveReplicas = ops
      .filter((op) => op.kind.type === "move")
      .map((op) => bytesToHex(op.meta.id.replica));
    expect(moveReplicas).toEqual([bytesToHex(replicas.a), bytesToHex(replicas.z)]);
  });

  test(
    "parallel applyOps does not lose writes",
    async () => {
      const backend = await factory.open(docA);

      const ops: Operation[] = [];
      for (let i = 1; i <= 100; i += 1) {
        ops.push(
          makeOp(replicas.c, i, i, {
            type: "insert",
            parent: root,
            node: nodeIdFromInt(10_000 + i),
            orderKey: orderKeyFromPosition(i - 1),
          })
        );
      }

      await Promise.all(ops.map((op) => backend.applyOps([op])));

      const refs = await backend.listOpRefs({ all: {} });
      expect(refs).toHaveLength(100);
    },
    20_000
  );

  test(
    "durability across reconnect to database",
    async () => {
      const factory1 = createPostgresNapiSyncBackendFactory(POSTGRES_URL!);
      await factory1.ensureSchema();
      await factory1.resetForTests();

      const docId = `durability-${randomUUID()}`;
      const backend1 = await factory1.open(docId);
      await backend1.applyOps([
        makeOp(replicas.d, 1, 1, {
          type: "insert",
          parent: root,
          node: nodeIdFromInt(9001),
          orderKey: orderKeyFromPosition(0),
        }),
        makeOp(replicas.d, 2, 2, {
          type: "payload",
          node: nodeIdFromInt(9001),
          payload: new Uint8Array([9]),
        }),
      ]);

      const factory2 = createPostgresNapiSyncBackendFactory(POSTGRES_URL!);
      await factory2.ensureSchema();
      const backend2 = await factory2.open(docId);

      const refs = await backend2.listOpRefs({ all: {} });
      expect(refs).toHaveLength(2);

      const ops = await backend2.getOpsByOpRefs(refs);
      expect(ops.some((op) => op.kind.type === "insert")).toBe(true);
      expect(ops.some((op) => op.kind.type === "payload")).toBe(true);
    },
    20_000
  );
});

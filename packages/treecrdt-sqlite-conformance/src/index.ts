import type { TreecrdtEngine } from "@treecrdt/interface/engine";
import type { Operation, ReplicaId } from "@treecrdt/interface";
import { nodeIdToBytes16, replicaIdToBytes } from "@treecrdt/interface/ids";

export type SqliteConformanceContext = {
  docId: string;
  engine: TreecrdtEngine;
  createEngine: (opts: { docId: string }) => Promise<TreecrdtEngine>;
};

export type SqliteConformanceScenario = {
  name: string;
  run: (ctx: SqliteConformanceContext) => Promise<void>;
};

export function sqliteEngineConformanceScenarios(): SqliteConformanceScenario[] {
  return [
    {
      name: "local ops: insert/move/delete/payload + tree reads",
      run: scenarioLocalOpsBasic,
    },
    {
      name: "local ops: insert with payload sets insert.kind.payload",
      run: scenarioLocalInsertWithPayload,
    },
    {
      name: "tree: childrenPage uses keyset cursor",
      run: scenarioChildrenPagination,
    },
    {
      name: "materialized tree: out-of-order ops rebuild correctly",
      run: scenarioOutOfOrderOpsRebuild,
    },
    {
      name: "materialized tree: dump/children/meta + oprefs_children",
      run: scenarioMaterializedSmokeWithOpRefs,
    },
    {
      name: "oprefs_children: includes move + latest payload",
      run: scenarioOpRefsChildrenIncludesPayloadAfterMove,
    },
    {
      name: "append/appendMany: rejects delete without known_state",
      run: scenarioRejectsDeleteWithoutKnownState,
    },
    {
      name: "defensive delete: delete hides node; move restores it",
      run: scenarioDefensiveDeleteMoveRestores,
    },
    {
      name: "defensive delete: insert under deleted parent restores it",
      run: scenarioDefensiveDeleteReactiveInsert,
    },
    {
      name: "defensive delete: out-of-order child insert restores parent",
      run: scenarioDefensiveDeleteOutOfOrderChildInsert,
    },
    {
      name: "sync: delete known_state propagates (receiver must not recompute)",
      run: scenarioSyncKnownStatePropagation,
    },
  ];
}

function nodeIdFromInt(n: number): string {
  if (!Number.isInteger(n) || n < 0) throw new Error(`invalid node int: ${n}`);
  return n.toString(16).padStart(32, "0");
}

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message);
}

function assertEqual<T>(actual: T, expected: T, message: string): void {
  if (actual !== expected) {
    throw new Error(`${message}: expected ${String(expected)}, got ${String(actual)}`);
  }
}

function assertArrayEqual(actual: string[], expected: string[], message: string): void {
  if (actual.length !== expected.length) {
    throw new Error(`${message}: expected length ${expected.length}, got ${actual.length} (${JSON.stringify(actual)})`);
  }
  for (let i = 0; i < actual.length; i++) {
    if (actual[i] !== expected[i]) {
      throw new Error(`${message}: expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`);
    }
  }
}

function assertBytesEqual(actual: Uint8Array | null, expected: Uint8Array | null, message: string): void {
  if (actual === null || expected === null) {
    if (actual !== expected) throw new Error(`${message}: expected ${String(expected)}, got ${String(actual)}`);
    return;
  }
  if (actual.length !== expected.length) {
    throw new Error(`${message}: expected length ${expected.length}, got ${actual.length}`);
  }
  for (let i = 0; i < actual.length; i++) {
    if (actual[i] !== expected[i]) throw new Error(`${message}: mismatch at byte ${i}`);
  }
}

function orderKeyFromPosition(position: number): Uint8Array {
  if (!Number.isInteger(position) || position < 0) throw new Error(`invalid position: ${position}`);
  const n = position + 1;
  if (n > 0xffff) throw new Error(`position too large for u16 order key: ${position}`);
  const bytes = new Uint8Array(2);
  new DataView(bytes.buffer).setUint16(0, n, false);
  return bytes;
}

function vvBytes(entries: { replica: ReplicaId; frontier: number; ranges?: [number, number][] }[]): Uint8Array {
  const payload = {
    entries: entries.map((e) => ({
      replica: Array.from(replicaIdToBytes(e.replica)),
      frontier: e.frontier,
      ranges: e.ranges ?? [],
    })),
  };
  return new TextEncoder().encode(JSON.stringify(payload));
}

function makeInsertOp(opts: {
  replica: ReplicaId;
  counter: number;
  lamport: number;
  parent: string;
  node: string;
  orderKey: Uint8Array;
  payload?: Uint8Array;
}): Operation {
  return {
    meta: { id: { replica: opts.replica, counter: opts.counter }, lamport: opts.lamport },
    kind: {
      type: "insert",
      parent: opts.parent,
      node: opts.node,
      orderKey: opts.orderKey,
      ...(opts.payload ? { payload: opts.payload } : {}),
    },
  };
}

function makeMoveOp(opts: {
  replica: ReplicaId;
  counter: number;
  lamport: number;
  node: string;
  newParent: string;
  orderKey: Uint8Array;
}): Operation {
  return {
    meta: { id: { replica: opts.replica, counter: opts.counter }, lamport: opts.lamport },
    kind: { type: "move", node: opts.node, newParent: opts.newParent, orderKey: opts.orderKey },
  };
}

function makeDeleteOp(opts: { replica: ReplicaId; counter: number; lamport: number; node: string; knownState?: Uint8Array }): Operation {
  return {
    meta: {
      id: { replica: opts.replica, counter: opts.counter },
      lamport: opts.lamport,
      ...(opts.knownState ? { knownState: opts.knownState } : {}),
    },
    kind: { type: "delete", node: opts.node },
  };
}

function makePayloadOp(opts: { replica: ReplicaId; counter: number; lamport: number; node: string; payload: Uint8Array | null }): Operation {
  return {
    meta: { id: { replica: opts.replica, counter: opts.counter }, lamport: opts.lamport },
    kind: { type: "payload", node: opts.node, payload: opts.payload },
  };
}

async function scenarioLocalOpsBasic(ctx: SqliteConformanceContext): Promise<void> {
  const engine = ctx.engine;
  const replica: ReplicaId = "r1";
  const root = nodeIdFromInt(0);
  const a = nodeIdFromInt(1);
  const b = nodeIdFromInt(2);
  const payload = new TextEncoder().encode("hello");

  const op1 = await engine.local.insert(replica, root, a, { type: "last" }, null);
  assertEqual(op1.kind.type, "insert", "op1.kind.type");
  if (op1.kind.type !== "insert") throw new Error(`expected insert op, got ${op1.kind.type}`);
  assertEqual(op1.kind.parent, root, "op1 insert parent");
  assertEqual(op1.kind.node, a, "op1 insert node");

  const op2 = await engine.local.insert(replica, root, b, { type: "last" }, null);
  assertEqual(op2.kind.type, "insert", "op2.kind.type");
  if (op2.kind.type !== "insert") throw new Error(`expected insert op, got ${op2.kind.type}`);
  assertEqual(op2.kind.parent, root, "op2 insert parent");
  assertEqual(op2.kind.node, b, "op2 insert node");

  let children = await engine.tree.children(root);
  assertArrayEqual(children, [a, b], "children after inserts");

  const op3 = await engine.local.move(replica, b, root, { type: "first" });
  assertEqual(op3.kind.type, "move", "op3.kind.type");
  if (op3.kind.type !== "move") throw new Error(`expected move op, got ${op3.kind.type}`);
  assertEqual(op3.kind.node, b, "op3 move node");
  assertEqual(op3.kind.newParent, root, "op3 move newParent");

  children = await engine.tree.children(root);
  assertArrayEqual(children, [b, a], "children after move(first)");

  const op4 = await engine.local.delete(replica, a);
  assertEqual(op4.kind.type, "delete", "op4.kind.type");
  if (op4.kind.type !== "delete") throw new Error(`expected delete op, got ${op4.kind.type}`);
  assertEqual(op4.kind.node, a, "op4 delete node");

  children = await engine.tree.children(root);
  assertArrayEqual(children, [b], "children after delete");

  const op5 = await engine.local.payload(replica, b, payload);
  assertEqual(op5.kind.type, "payload", "op5.kind.type");
  if (op5.kind.type !== "payload") throw new Error(`expected payload op, got ${op5.kind.type}`);
  assertEqual(op5.kind.node, b, "op5 payload node");
  assertBytesEqual(op5.kind.payload, payload, "op5 payload bytes");

  const allOps = await engine.ops.all();
  assertEqual(allOps.length, 5, "engine.ops.all length");
  const last = allOps[allOps.length - 1]!;
  assertEqual(last.kind.type, "payload", "engine.ops.all last kind");
  if (last.kind.type !== "payload") throw new Error(`expected payload op, got ${last.kind.type}`);
  assertBytesEqual(last.kind.payload, payload, "engine.ops.all last payload bytes");

  const dump = await engine.tree.dump();
  const rowA = dump.find((r) => r.node === a);
  const rowB = dump.find((r) => r.node === b);
  assert(rowA, "tree.dump should include deleted node row");
  assert(rowB, "tree.dump should include live node row");
  assertEqual(rowA.tombstone, true, "tree.dump tombstone for deleted node");
  assertEqual(rowB.tombstone, false, "tree.dump tombstone for live node");

  const maxCounter = await engine.meta.replicaMaxCounter(replica);
  assertEqual(maxCounter, op5.meta.id.counter, "meta.replicaMaxCounter");
}

async function scenarioLocalInsertWithPayload(ctx: SqliteConformanceContext): Promise<void> {
  const engine = ctx.engine;
  const replica: ReplicaId = "r1";
  const root = nodeIdFromInt(0);
  const a = nodeIdFromInt(1);
  const payload = new TextEncoder().encode("hello");

  const op = await engine.local.insert(replica, root, a, { type: "last" }, payload);
  assertEqual(op.kind.type, "insert", "local insert kind");
  if (op.kind.type !== "insert") throw new Error(`expected insert op, got ${op.kind.type}`);
  assertBytesEqual(op.kind.payload ?? null, payload, "insert.kind.payload");

  const all = await engine.ops.all();
  assertEqual(all.length, 1, "ops.all length");
  const first = all[0]!;
  assertEqual(first.kind.type, "insert", "ops.all first kind");
  if (first.kind.type !== "insert") throw new Error(`expected insert op, got ${first.kind.type}`);
  assertBytesEqual(first.kind.payload ?? null, payload, "ops.all insert payload");
}

async function scenarioChildrenPagination(ctx: SqliteConformanceContext): Promise<void> {
  const engine = ctx.engine;
  assert(engine.tree.childrenPage, "engine.tree.childrenPage not implemented");

  const replica: ReplicaId = "r1";
  const root = nodeIdFromInt(0);
  const nodes = Array.from({ length: 10 }, (_, i) => nodeIdFromInt(i + 1));
  for (const node of nodes) {
    await engine.local.insert(replica, root, node, { type: "last" }, null);
  }

  const all = await engine.tree.children(root);
  assertArrayEqual(all, nodes, "tree.children after inserts");

  const p1 = await engine.tree.childrenPage(root, null, 4);
  assertEqual(p1.length, 4, "childrenPage p1 length");
  assertArrayEqual(p1.map((r) => r.node), nodes.slice(0, 4), "childrenPage p1 nodes");

  const c1 = p1[p1.length - 1]!;
  assert(c1.orderKey, "childrenPage cursor orderKey should be present");

  const p2 = await engine.tree.childrenPage(
    root,
    { orderKey: c1.orderKey!, node: nodeIdToBytes16(c1.node) },
    4
  );
  assertEqual(p2.length, 4, "childrenPage p2 length");
  assertArrayEqual(p2.map((r) => r.node), nodes.slice(4, 8), "childrenPage p2 nodes");

  const c2 = p2[p2.length - 1]!;
  assert(c2.orderKey, "childrenPage cursor2 orderKey should be present");

  const p3 = await engine.tree.childrenPage(
    root,
    { orderKey: c2.orderKey!, node: nodeIdToBytes16(c2.node) },
    4
  );
  assertEqual(p3.length, 2, "childrenPage p3 length");
  assertArrayEqual(p3.map((r) => r.node), nodes.slice(8, 10), "childrenPage p3 nodes");

  const c3 = p3[p3.length - 1]!;
  assert(c3.orderKey, "childrenPage cursor3 orderKey should be present");
  const p4 = await engine.tree.childrenPage(
    root,
    { orderKey: c3.orderKey!, node: nodeIdToBytes16(c3.node) },
    4
  );
  assertEqual(p4.length, 0, "childrenPage p4 length");
}

async function scenarioOutOfOrderOpsRebuild(ctx: SqliteConformanceContext): Promise<void> {
  const engine = ctx.engine;
  const replica: ReplicaId = "r1";
  const root = nodeIdFromInt(0);
  const n1 = nodeIdFromInt(1);
  const n2 = nodeIdFromInt(2);

  // Append out-of-order lamports to force a rebuild path.
  await engine.ops.append(
    makeInsertOp({
      replica,
      counter: 1,
      lamport: 2,
      parent: root,
      node: n1,
      orderKey: orderKeyFromPosition(0),
    })
  );
  await engine.ops.append(
    makeInsertOp({
      replica,
      counter: 2,
      lamport: 1,
      parent: root,
      node: n2,
      orderKey: orderKeyFromPosition(0),
    })
  );

  const children = await engine.tree.children(root);
  const sorted = [...children].sort();
  assertArrayEqual(sorted, [n1, n2].sort(), "children after out-of-order inserts");
  assertEqual(await engine.tree.nodeCount(), 2, "tree.nodeCount after out-of-order inserts");
  assertEqual(await engine.meta.headLamport(), 2, "meta.headLamport after out-of-order inserts");
}

async function scenarioMaterializedSmokeWithOpRefs(ctx: SqliteConformanceContext): Promise<void> {
  const engine = ctx.engine;
  const replica: ReplicaId = "r1";
  const root = nodeIdFromInt(0);
  const n1 = nodeIdFromInt(1);
  const n2 = nodeIdFromInt(2);

  await engine.ops.append(
    makeInsertOp({ replica, counter: 1, lamport: 1, parent: root, node: n1, orderKey: orderKeyFromPosition(0) })
  );
  await engine.ops.append(
    makeInsertOp({ replica, counter: 2, lamport: 2, parent: root, node: n2, orderKey: orderKeyFromPosition(0) })
  );
  await engine.ops.append(
    makeMoveOp({ replica, counter: 3, lamport: 3, node: n2, newParent: n1, orderKey: orderKeyFromPosition(0) })
  );

  assertEqual(await engine.meta.headLamport(), 3, "meta.headLamport");
  assertEqual(await engine.meta.replicaMaxCounter(replica), 3, "meta.replicaMaxCounter");
  assertEqual(await engine.tree.nodeCount(), 2, "tree.nodeCount");
  assertArrayEqual(await engine.tree.children(root), [n1], "tree.children(root)");
  assertArrayEqual(await engine.tree.children(n1), [n2], "tree.children(n1)");

  const dump = await engine.tree.dump();
  const byId = new Map(dump.map((row) => [row.node, row]));
  assertEqual(byId.get(root)?.parent ?? null, null, "tree.dump root parent");
  assertEqual(byId.get(n1)?.parent ?? null, root, "tree.dump n1 parent");
  assertEqual(byId.get(n2)?.parent ?? null, n1, "tree.dump n2 parent");

  const refsRoot = await engine.opRefs.children(root);
  assertEqual(refsRoot.length, 3, "opRefs.children(root) length");
  const opsRoot = await engine.ops.get(refsRoot);
  assertArrayEqual(
    opsRoot.map((op) => op.kind.type),
    ["insert", "insert", "move"],
    "opsByOpRefs(root) kinds"
  );

  const refsN1 = await engine.opRefs.children(n1);
  assertEqual(refsN1.length, 1, "opRefs.children(n1) length");
  const opsN1 = await engine.ops.get(refsN1);
  assertArrayEqual(opsN1.map((op) => op.kind.type), ["move"], "opsByOpRefs(n1) kinds");
}

async function scenarioOpRefsChildrenIncludesPayloadAfterMove(ctx: SqliteConformanceContext): Promise<void> {
  const engine = ctx.engine;
  const replica: ReplicaId = "r1";
  const root = nodeIdFromInt(0);
  const p1 = nodeIdFromInt(1);
  const p2 = nodeIdFromInt(2);
  const child = nodeIdFromInt(3);

  await engine.local.insert(replica, root, p1, { type: "last" }, null);
  await engine.local.insert(replica, root, p2, { type: "last" }, null);
  await engine.local.insert(replica, p1, child, { type: "last" }, null);
  await engine.local.payload(replica, child, new TextEncoder().encode("hi"));
  await engine.local.move(replica, child, p2, { type: "last" });

  const refs = await engine.opRefs.children(p2);
  assertEqual(refs.length, 2, "opRefs.children(p2) length");
  const ops = await engine.ops.get(refs);
  const kinds = new Set(ops.map((op) => op.kind.type));
  assert(kinds.has("move"), "opRefs.children(p2) should include move op");
  assert(kinds.has("payload"), "opRefs.children(p2) should include payload op");

  const payloadOp = ops.find((op) => op.kind.type === "payload");
  assert(payloadOp, "expected payload op in opsByOpRefs(p2)");
  if (!payloadOp || payloadOp.kind.type !== "payload") throw new Error("expected payload op");
  assertEqual(new TextDecoder().decode(payloadOp.kind.payload ?? new Uint8Array()), "hi", "payload contents");
}

async function scenarioRejectsDeleteWithoutKnownState(ctx: SqliteConformanceContext): Promise<void> {
  const engine = ctx.engine;
  const replica: ReplicaId = "rA";
  const root = nodeIdFromInt(0);
  const node = nodeIdFromInt(1);

  await engine.local.insert(replica, root, node, { type: "last" }, null);

  let threw = false;
  try {
    await engine.ops.append(makeDeleteOp({ replica, counter: 2, lamport: 2, node }));
  } catch {
    threw = true;
  }
  assert(threw, "append(delete without knownState) should throw");

  threw = false;
  try {
    await engine.ops.appendMany([makeDeleteOp({ replica, counter: 3, lamport: 3, node })]);
  } catch {
    threw = true;
  }
  assert(threw, "appendMany(delete without knownState) should throw");
}

async function scenarioDefensiveDeleteMoveRestores(ctx: SqliteConformanceContext): Promise<void> {
  const engine = ctx.engine;
  const replica: ReplicaId = "r1";
  const root = nodeIdFromInt(0);
  const n1 = nodeIdFromInt(1);

  await engine.local.insert(replica, root, n1, { type: "last" }, null);
  await engine.local.delete(replica, n1);
  assertArrayEqual(await engine.tree.children(root), [], "children after delete");

  await engine.local.move(replica, n1, root, { type: "last" });
  assertArrayEqual(await engine.tree.children(root), [n1], "children after move restores");
}

async function scenarioDefensiveDeleteReactiveInsert(ctx: SqliteConformanceContext): Promise<void> {
  const engine = ctx.engine;
  const replica: ReplicaId = "r1";
  const root = nodeIdFromInt(0);
  const parent = nodeIdFromInt(1);
  const child = nodeIdFromInt(2);

  await engine.ops.append(
    makeInsertOp({ replica, counter: 1, lamport: 1, parent: root, node: parent, orderKey: orderKeyFromPosition(0) })
  );
  await engine.ops.append(
    makeDeleteOp({
      replica,
      counter: 2,
      lamport: 2,
      node: parent,
      knownState: vvBytes([{ replica, frontier: 1 }]),
    })
  );

  assertArrayEqual(await engine.tree.children(root), [], "children after delete");

  await engine.ops.append(
    makeInsertOp({ replica, counter: 3, lamport: 3, parent, node: child, orderKey: orderKeyFromPosition(0) })
  );

  assertArrayEqual(await engine.tree.children(root), [parent], "parent restored after subtree insert");
  assertArrayEqual(await engine.tree.children(parent), [child], "child visible under restored parent");
}

async function scenarioDefensiveDeleteOutOfOrderChildInsert(ctx: SqliteConformanceContext): Promise<void> {
  const engine = ctx.engine;
  const rA: ReplicaId = "rA";
  const rB: ReplicaId = "rB";
  const root = nodeIdFromInt(0);
  const parent = nodeIdFromInt(1);
  const child = nodeIdFromInt(2);

  await engine.ops.append(
    makeInsertOp({ replica: rA, counter: 1, lamport: 1, parent: root, node: parent, orderKey: orderKeyFromPosition(0) })
  );
  await engine.ops.append(
    makeDeleteOp({
      replica: rA,
      counter: 2,
      lamport: 3,
      node: parent,
      knownState: vvBytes([{ replica: rA, frontier: 1 }]),
    })
  );

  assertArrayEqual(await engine.tree.children(root), [], "parent hidden after delete");

  // Later we receive an earlier op (lamport=2) from another replica.
  await engine.ops.append(
    makeInsertOp({ replica: rB, counter: 1, lamport: 2, parent, node: child, orderKey: orderKeyFromPosition(0) })
  );

  assertArrayEqual(await engine.tree.children(root), [parent], "parent restored after out-of-order child insert");
  assertArrayEqual(await engine.tree.children(parent), [child], "child visible under restored parent");
  assertEqual(await engine.tree.nodeCount(), 2, "nodeCount after restore");
}

async function scenarioSyncKnownStatePropagation(ctx: SqliteConformanceContext): Promise<void> {
  const a = ctx.engine;
  const b = await ctx.createEngine({ docId: ctx.docId });

  try {
    const root = nodeIdFromInt(0);
    const parent = nodeIdFromInt(1);
    const child = nodeIdFromInt(2);
    const rA: ReplicaId = "rA";
    const rB: ReplicaId = "rB";

    // Replica B inserts parent, then syncs it to A.
    await b.local.insert(rB, root, parent, { type: "last" }, null);
    await a.ops.appendMany(await b.ops.all());

    // Replica B inserts a child under parent, but A never sees it.
    await b.local.insert(rB, parent, child, { type: "last" }, null);

    // Replica A deletes parent without being aware of B's child insert.
    const del = await a.local.delete(rA, parent);
    assert(del.meta.knownState && del.meta.knownState.length > 0, "local delete must emit knownState");

    // Sync A -> B. The delete MUST carry known_state so B doesn't treat it as aware of the child.
    await b.ops.appendMany(await a.ops.all());

    assertArrayEqual(await b.tree.children(root), [parent], "parent restored after sync delete");
    assertArrayEqual(await b.tree.children(parent), [child], "child still present after sync delete");
  } finally {
    await b.close();
  }
}

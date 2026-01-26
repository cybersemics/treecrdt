import type { TreecrdtEngine } from "@treecrdt/interface/engine";
import type { ReplicaId } from "@treecrdt/interface";

export type SqliteConformanceScenario = {
  name: string;
  run: (engine: TreecrdtEngine) => Promise<void>;
};

export function sqliteEngineConformanceScenarios(): SqliteConformanceScenario[] {
  return [
    {
      name: "local ops: insert/move/delete/payload + tree reads",
      run: scenarioLocalOpsBasic,
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

async function scenarioLocalOpsBasic(engine: TreecrdtEngine): Promise<void> {
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

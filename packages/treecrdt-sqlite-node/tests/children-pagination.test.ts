import { expect, test } from "vitest";

async function loadSqlite(): Promise<any> {
  const { default: Database } = await import("better-sqlite3").catch((err) => {
    throw new Error(
      `better-sqlite3 native binding not available; ensure it is installed/built before running native tests: ${err}`
    );
  });
  return Database;
}

async function loadTreecrdt(): Promise<any> {
  return await import("../dist/index.js");
}

function makeNodeId(lastByte: number): Buffer {
  const b = Buffer.alloc(16, 0);
  b[15] = lastByte;
  return b;
}

function orderKeyFromPosition(position: number): Buffer {
  if (!Number.isInteger(position) || position < 0) throw new Error(`invalid position: ${position}`);
  const n = position + 1;
  if (n > 0xffff) throw new Error(`position too large for u16 order key: ${position}`);
  const b = Buffer.alloc(2);
  b.writeUInt16BE(n, 0);
  return b;
}

test("materialized tree: children pagination uses keyset cursor", async () => {
  const Database = await loadSqlite();
  const { loadTreecrdtExtension, defaultExtensionPath, createSqliteNodeApi } = await loadTreecrdt();

  const docId = "treecrdt-tree-children-pagination";
  const db = new Database(":memory:");
  loadTreecrdtExtension(db, { extensionPath: defaultExtensionPath() });
  const api = createSqliteNodeApi(db);
  await api.setDocId(docId);

  const replica = Buffer.from("r1");
  const root = Buffer.alloc(16, 0);
  const nodes = Array.from({ length: 10 }, (_, i) => makeNodeId(i + 1));

  for (const [idx, node] of nodes.entries()) {
    db.prepare("SELECT treecrdt_append_op(?, ?, ?, ?, ?, ?, NULL, ?, NULL)").get(
      replica,
      idx + 1,
      idx + 1,
      "insert",
      root,
      node,
      orderKeyFromPosition(idx)
    );
  }

  const all = (await api.treeChildren(root)) as string[];
  expect(all).toEqual(nodes.map((n) => n.toString("hex")));

  const p1 = (await api.treeChildrenPage!(root, null, 4)) as Array<{ node: string; order_key: string | null }>;
  expect(p1.length).toBe(4);
  expect(p1.map((r) => r.node)).toEqual(nodes.slice(0, 4).map((n) => n.toString("hex")));

  const c1 = p1[p1.length - 1];
  expect(c1.order_key).toBeTruthy();

  const p2 = (await api.treeChildrenPage!(
    root,
    { orderKey: Buffer.from(c1.order_key!, "hex"), node: Buffer.from(c1.node, "hex") },
    4
  )) as Array<{ node: string; order_key: string | null }>;
  expect(p2.length).toBe(4);
  expect(p2.map((r) => r.node)).toEqual(nodes.slice(4, 8).map((n) => n.toString("hex")));

  const c2 = p2[p2.length - 1];
  const p3 = (await api.treeChildrenPage!(
    root,
    { orderKey: Buffer.from(c2.order_key!, "hex"), node: Buffer.from(c2.node, "hex") },
    4
  )) as Array<{ node: string; order_key: string | null }>;
  expect(p3.length).toBe(2);
  expect(p3.map((r) => r.node)).toEqual(nodes.slice(8, 10).map((n) => n.toString("hex")));

  const c3 = p3[p3.length - 1];
  const p4 = (await api.treeChildrenPage!(
    root,
    { orderKey: Buffer.from(c3.order_key!, "hex"), node: Buffer.from(c3.node, "hex") },
    4
  )) as Array<{ node: string; order_key: string | null }>;
  expect(p4.length).toBe(0);
});

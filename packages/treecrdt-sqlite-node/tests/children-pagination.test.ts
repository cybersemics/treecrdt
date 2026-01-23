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

function parseChildRows(json: string): Array<{ node: Buffer; orderKey: Buffer | null }> {
  const decoded = JSON.parse(json) as Array<{ node: number[]; order_key: number[] | null }>;
  return decoded.map((row) => {
    const node = Buffer.from(row.node);
    const rawOrderKey = row.order_key;
    const orderKey = rawOrderKey === null || rawOrderKey === undefined ? null : Buffer.from(rawOrderKey);
    return { node, orderKey };
  });
}

test("materialized tree: children pagination uses keyset cursor", async () => {
  const Database = await loadSqlite();
  const { loadTreecrdtExtension, defaultExtensionPath } = await loadTreecrdt();

  const docId = "treecrdt-tree-children-pagination";
  const db = new Database(":memory:");
  loadTreecrdtExtension(db, { extensionPath: defaultExtensionPath() });
  db.prepare("SELECT treecrdt_set_doc_id(?)").get(docId);

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

  const allRow: any = db.prepare("SELECT treecrdt_tree_children(?) AS v").get(root);
  const all = (JSON.parse(allRow.v) as number[][]).map((bytes) => Buffer.from(bytes).toString("hex"));
  expect(all).toEqual(nodes.map((n) => n.toString("hex")));

  const pageSql = "SELECT treecrdt_tree_children_page(?, ?, ?, ?) AS v";

  const p1Row: any = db.prepare(pageSql).get(root, null, null, 4);
  const p1 = parseChildRows(p1Row.v);
  expect(p1.length).toBe(4);
  expect(p1.map((r) => r.node.toString("hex"))).toEqual(nodes.slice(0, 4).map((n) => n.toString("hex")));

  const c1 = p1[p1.length - 1];
  expect(c1.orderKey).toBeTruthy();

  const p2Row: any = db.prepare(pageSql).get(root, c1.orderKey, c1.node, 4);
  const p2 = parseChildRows(p2Row.v);
  expect(p2.length).toBe(4);
  expect(p2.map((r) => r.node.toString("hex"))).toEqual(nodes.slice(4, 8).map((n) => n.toString("hex")));

  const c2 = p2[p2.length - 1];
  const p3Row: any = db.prepare(pageSql).get(root, c2.orderKey, c2.node, 4);
  const p3 = parseChildRows(p3Row.v);
  expect(p3.length).toBe(2);
  expect(p3.map((r) => r.node.toString("hex"))).toEqual(nodes.slice(8, 10).map((n) => n.toString("hex")));

  const c3 = p3[p3.length - 1];
  const p4Row: any = db.prepare(pageSql).get(root, c3.orderKey, c3.node, 4);
  const p4 = parseChildRows(p4Row.v);
  expect(p4.length).toBe(0);
});

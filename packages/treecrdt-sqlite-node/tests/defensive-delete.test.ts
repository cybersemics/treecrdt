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

function parseJsonBytes16List(json: string): Buffer[] {
  const decoded = JSON.parse(json) as number[][];
  return decoded.map((bytes) => Buffer.from(bytes));
}

test("materialized tree: delete hides node and move restores it", async () => {
  const Database = await loadSqlite();
  const { loadTreecrdtExtension, defaultExtensionPath } = await loadTreecrdt();

  const docId = "treecrdt-delete-move-restore";
  const db = new Database(":memory:");
  loadTreecrdtExtension(db, { extensionPath: defaultExtensionPath() });
  db.prepare("SELECT treecrdt_set_doc_id(?)").get(docId);

  const replica = Buffer.from("r1");
  const root = Buffer.alloc(16, 0);
  const n1 = makeNodeId(1);

  db.prepare("SELECT treecrdt_append_op(?, ?, ?, ?, ?, ?, NULL, NULL)").get(replica, 1, 1, "insert", root, n1);
  db.prepare("SELECT treecrdt_append_op(?, ?, ?, ?, NULL, ?, NULL, NULL)").get(replica, 2, 2, "delete", n1);

  const afterDeleteRow: any = db.prepare("SELECT treecrdt_tree_children(?) AS v").get(root);
  expect(parseJsonBytes16List(afterDeleteRow.v)).toEqual([]);

  // A move after delete should restore the node (because the delete is no longer aware).
  db.prepare("SELECT treecrdt_append_op(?, ?, ?, ?, NULL, ?, ?, ?)").get(replica, 3, 3, "move", n1, root, 0);
  const afterMoveRow: any = db.prepare("SELECT treecrdt_tree_children(?) AS v").get(root);
  expect(parseJsonBytes16List(afterMoveRow.v).map((b) => b.toString("hex"))).toEqual([n1.toString("hex")]);
});

test("materialized tree: defensive delete restores when earlier child insert arrives out-of-order", async () => {
  const Database = await loadSqlite();
  const { loadTreecrdtExtension, defaultExtensionPath } = await loadTreecrdt();

  const docId = "treecrdt-defensive-delete-out-of-order";
  const db = new Database(":memory:");
  loadTreecrdtExtension(db, { extensionPath: defaultExtensionPath() });
  db.prepare("SELECT treecrdt_set_doc_id(?)").get(docId);

  const rA = Buffer.from("rA");
  const rB = Buffer.from("rB");
  const root = Buffer.alloc(16, 0);
  const parent = makeNodeId(1);
  const child = makeNodeId(2);

  // Replica A inserts parent, then deletes it without having seen B's insert of a child.
  db.prepare("SELECT treecrdt_append_op(?, ?, ?, ?, ?, ?, NULL, NULL)").get(rA, 1, 1, "insert", root, parent);
  db.prepare("SELECT treecrdt_append_op(?, ?, ?, ?, NULL, ?, NULL, NULL)").get(rA, 2, 3, "delete", parent);

  // Parent is tombstoned (hidden from root).
  const afterDeleteRow: any = db.prepare("SELECT treecrdt_tree_children(?) AS v").get(root);
  expect(parseJsonBytes16List(afterDeleteRow.v)).toEqual([]);

  // Later, an earlier op arrives: Replica B had inserted a child under the parent at lamport=2.
  // This is out-of-order (lamport=2 < head=3), forcing a rebuild. The parent should be restored
  // because A's delete was not aware of B's child insert.
  db.prepare("SELECT treecrdt_append_op(?, ?, ?, ?, ?, ?, NULL, NULL)").get(rB, 1, 2, "insert", parent, child);

  const rootChildrenRow: any = db.prepare("SELECT treecrdt_tree_children(?) AS v").get(root);
  expect(parseJsonBytes16List(rootChildrenRow.v).map((b) => b.toString("hex"))).toEqual([parent.toString("hex")]);

  const parentChildrenRow: any = db.prepare("SELECT treecrdt_tree_children(?) AS v").get(parent);
  expect(parseJsonBytes16List(parentChildrenRow.v).map((b) => b.toString("hex"))).toEqual([child.toString("hex")]);

  const nodeCountRow: any = db.prepare("SELECT treecrdt_tree_node_count() AS v").get();
  expect(nodeCountRow.v).toBe(2);
});

test("materialized tree: parent is restored when subtree changes after delete (reactive)", async () => {
  const Database = await loadSqlite();
  const { loadTreecrdtExtension, defaultExtensionPath } = await loadTreecrdt();

  const docId = "treecrdt-defensive-delete-reactive";
  const db = new Database(":memory:");
  loadTreecrdtExtension(db, { extensionPath: defaultExtensionPath() });
  db.prepare("SELECT treecrdt_set_doc_id(?)").get(docId);

  const replica = Buffer.from("r1");
  const root = Buffer.alloc(16, 0);
  const parent = makeNodeId(1);
  const child = makeNodeId(2);

  db.prepare("SELECT treecrdt_append_op(?, ?, ?, ?, ?, ?, NULL, NULL)").get(replica, 1, 1, "insert", root, parent);
  db.prepare("SELECT treecrdt_append_op(?, ?, ?, ?, NULL, ?, NULL, NULL)").get(replica, 2, 2, "delete", parent);

  const afterDeleteRow: any = db.prepare("SELECT treecrdt_tree_children(?) AS v").get(root);
  expect(parseJsonBytes16List(afterDeleteRow.v)).toEqual([]);

  // A subsequent insert under the deleted parent should restore it (because the delete isn't aware).
  db.prepare("SELECT treecrdt_append_op(?, ?, ?, ?, ?, ?, NULL, NULL)").get(replica, 3, 3, "insert", parent, child);

  const rootChildrenRow: any = db.prepare("SELECT treecrdt_tree_children(?) AS v").get(root);
  expect(parseJsonBytes16List(rootChildrenRow.v).map((b) => b.toString("hex"))).toEqual([parent.toString("hex")]);

  const parentChildrenRow: any = db.prepare("SELECT treecrdt_tree_children(?) AS v").get(parent);
  expect(parseJsonBytes16List(parentChildrenRow.v).map((b) => b.toString("hex"))).toEqual([child.toString("hex")]);
});

import { expect, test } from "vitest";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

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

test("materialized tree: dump/children/meta + oprefs_children", async () => {
  const Database = await loadSqlite();
  const { loadTreecrdtExtension, defaultExtensionPath } = await loadTreecrdt();

  const docId = "treecrdt-materialized-tree-smoke";
  const db = new Database(":memory:");
  loadTreecrdtExtension(db, { extensionPath: defaultExtensionPath() });
  db.prepare("SELECT treecrdt_set_doc_id(?)").get(docId);

  const replica = Buffer.from("r1");
  const root = Buffer.alloc(16, 0);
  const n1 = makeNodeId(1);
  const n2 = makeNodeId(2);

  db.prepare("SELECT treecrdt_append_op(?, ?, ?, ?, ?, ?, NULL, NULL, NULL)").get(replica, 1, 1, "insert", root, n1);
  db.prepare("SELECT treecrdt_append_op(?, ?, ?, ?, ?, ?, NULL, NULL, NULL)").get(replica, 2, 2, "insert", root, n2);
  db.prepare("SELECT treecrdt_append_op(?, ?, ?, ?, NULL, ?, ?, ?, NULL)").get(replica, 3, 3, "move", n2, n1, 0);

  const headLamportRow: any = db.prepare("SELECT treecrdt_head_lamport() AS v").get();
  expect(headLamportRow.v).toBe(3);

  const replicaCounterRow: any = db.prepare("SELECT treecrdt_replica_max_counter(?) AS v").get(replica);
  expect(replicaCounterRow.v).toBe(3);

  const nodeCountRow: any = db.prepare("SELECT treecrdt_tree_node_count() AS v").get();
  expect(nodeCountRow.v).toBe(2);

  const rootChildrenRow: any = db.prepare("SELECT treecrdt_tree_children(?) AS v").get(root);
  const rootChildren = parseJsonBytes16List(rootChildrenRow.v).map((b) => b.toString("hex"));
  expect(rootChildren).toEqual([n1.toString("hex")]);

  const n1ChildrenRow: any = db.prepare("SELECT treecrdt_tree_children(?) AS v").get(n1);
  const n1Children = parseJsonBytes16List(n1ChildrenRow.v).map((b) => b.toString("hex"));
  expect(n1Children).toEqual([n2.toString("hex")]);

  const dumpRow: any = db.prepare("SELECT treecrdt_tree_dump() AS v").get();
  const dump = JSON.parse(dumpRow.v) as Array<{
    node: number[];
    parent: number[] | null;
    pos: number | null;
    tombstone: boolean;
  }>;
  const byId = new Map(dump.map((row) => [Buffer.from(row.node).toString("hex"), row]));
  expect(byId.get(root.toString("hex"))?.parent).toBe(null);
  expect(Buffer.from(byId.get(n1.toString("hex"))?.parent ?? []).toString("hex")).toBe(root.toString("hex"));
  expect(Buffer.from(byId.get(n2.toString("hex"))?.parent ?? []).toString("hex")).toBe(n1.toString("hex"));

  const refsRootRow: any = db.prepare("SELECT treecrdt_oprefs_children(?) AS v").get(root);
  const refsRoot = JSON.parse(refsRootRow.v) as number[][];
  expect(refsRoot.length).toBe(3);
  const opsFromRefsRootRow: any = db.prepare("SELECT treecrdt_ops_by_oprefs(?) AS v").get(JSON.stringify(refsRoot));
  const opsFromRefsRoot = JSON.parse(opsFromRefsRootRow.v) as Array<{ kind: string }>;
  expect(opsFromRefsRoot.map((op) => op.kind)).toEqual(["insert", "insert", "move"]);

  const refsN1Row: any = db.prepare("SELECT treecrdt_oprefs_children(?) AS v").get(n1);
  const refsN1 = JSON.parse(refsN1Row.v) as number[][];
  expect(refsN1.length).toBe(1);
  const opsFromRefsN1Row: any = db.prepare("SELECT treecrdt_ops_by_oprefs(?) AS v").get(JSON.stringify(refsN1));
  const opsFromRefsN1 = JSON.parse(opsFromRefsN1Row.v) as Array<{ kind: string }>;
  expect(opsFromRefsN1.map((op) => op.kind)).toEqual(["move"]);
});

test("materialized tree: persists across reopen", async () => {
  const Database = await loadSqlite();
  const { loadTreecrdtExtension, defaultExtensionPath } = await loadTreecrdt();

  const dir = mkdtempSync(join(tmpdir(), "treecrdt-mat-tree-"));
  const path = join(dir, "db.sqlite");
  const docId = "treecrdt-materialized-tree-reopen";
  const replica = Buffer.from("r1");
  const root = Buffer.alloc(16, 0);
  const n1 = makeNodeId(1);

  try {
    {
      const db = new Database(path);
      loadTreecrdtExtension(db, { extensionPath: defaultExtensionPath() });
      db.prepare("SELECT treecrdt_set_doc_id(?)").get(docId);
      db.prepare("SELECT treecrdt_append_op(?, ?, ?, ?, ?, ?, NULL, NULL, NULL)").get(replica, 1, 1, "insert", root, n1);
      const row: any = db.prepare("SELECT treecrdt_tree_children(?) AS v").get(root);
      expect(parseJsonBytes16List(row.v).map((b) => b.toString("hex"))).toEqual([n1.toString("hex")]);
      const countRow: any = db.prepare("SELECT treecrdt_tree_node_count() AS v").get();
      expect(countRow.v).toBe(1);
      db.close();
    }

    {
      const db = new Database(path);
      loadTreecrdtExtension(db, { extensionPath: defaultExtensionPath() });
      db.prepare("SELECT treecrdt_set_doc_id(?)").get(docId);
      const row: any = db.prepare("SELECT treecrdt_tree_children(?) AS v").get(root);
      expect(parseJsonBytes16List(row.v).map((b) => b.toString("hex"))).toEqual([n1.toString("hex")]);
      const countRow: any = db.prepare("SELECT treecrdt_tree_node_count() AS v").get();
      expect(countRow.v).toBe(1);
      db.close();
    }
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("materialized tree: out-of-order ops rebuild correctly", async () => {
  const Database = await loadSqlite();
  const { loadTreecrdtExtension, defaultExtensionPath } = await loadTreecrdt();

  const docId = "treecrdt-materialized-tree-out-of-order";
  const db = new Database(":memory:");
  loadTreecrdtExtension(db, { extensionPath: defaultExtensionPath() });
  db.prepare("SELECT treecrdt_set_doc_id(?)").get(docId);

  const replica = Buffer.from("r1");
  const root = Buffer.alloc(16, 0);
  const n1 = makeNodeId(1);
  const n2 = makeNodeId(2);

  db.prepare("SELECT treecrdt_append_op(?, ?, ?, ?, ?, ?, NULL, NULL, NULL)").get(replica, 1, 2, "insert", root, n1);
  db.prepare("SELECT treecrdt_append_op(?, ?, ?, ?, ?, ?, NULL, NULL, NULL)").get(replica, 2, 1, "insert", root, n2);

  const row: any = db.prepare("SELECT treecrdt_tree_children(?) AS v").get(root);
  const children = parseJsonBytes16List(row.v).map((b) => b.toString("hex"));
  expect(children.sort()).toEqual([n1.toString("hex"), n2.toString("hex")].sort());

  const nodeCountRow: any = db.prepare("SELECT treecrdt_tree_node_count() AS v").get();
  expect(nodeCountRow.v).toBe(2);

  const headLamportRow: any = db.prepare("SELECT treecrdt_head_lamport() AS v").get();
  expect(headLamportRow.v).toBe(2);
});

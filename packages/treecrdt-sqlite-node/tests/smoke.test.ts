import { expect, test } from "vitest";

test("load extension and roundtrip ops", async () => {
  const { default: Database } = await import("better-sqlite3").catch((err) => {
    throw new Error(
      `better-sqlite3 native binding not available; ensure it is installed/built before running native tests: ${err}`
    );
  });

  const { loadTreecrdtExtension, defaultExtensionPath } = await import(
    "../dist/index.js"
  );

  const db = new Database(":memory:");
  // If the extension isn't present, this will throw.
  loadTreecrdtExtension(db, { extensionPath: defaultExtensionPath() });
  db.prepare("SELECT treecrdt_set_doc_id(?)").get("treecrdt-sqlite-node-smoke");

  const versionRow: any = db.prepare("select treecrdt_version() as v").get();
  expect(versionRow.v).toBeTruthy();

  const replica = Buffer.from("r1");
  const parent = Buffer.alloc(16, 0);
  const node = Buffer.from(parent);
  node[15] = 1;

  db.prepare(
    "SELECT treecrdt_append_op(?, ?, ?, ?, ?, ?, NULL, NULL, NULL)"
  ).get(replica, 1, 1, "insert", parent, node);
  db.prepare(
    "SELECT treecrdt_append_op(?, ?, ?, ?, NULL, ?, ?, ?, NULL)"
  ).get(replica, 2, 2, "move", node, parent, 0);

  const row: any = db.prepare("SELECT treecrdt_ops_since(0) AS ops").get();
  const ops = JSON.parse(row.ops);
  expect(ops.length).toBe(2);
  expect(ops[0].kind).toBe("insert");
  expect(ops[1].kind).toBe("move");
});

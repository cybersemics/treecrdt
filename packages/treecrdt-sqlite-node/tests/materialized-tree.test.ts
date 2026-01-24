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

function orderKeyFromPosition(position: number): Buffer {
  if (!Number.isInteger(position) || position < 0) throw new Error(`invalid position: ${position}`);
  const n = position + 1;
  if (n > 0xffff) throw new Error(`position too large for u16 order key: ${position}`);
  const b = Buffer.alloc(2);
  b.writeUInt16BE(n, 0);
  return b;
}

test("materialized tree: dump/children/meta + oprefs_children", async () => {
  const Database = await loadSqlite();
  const { loadTreecrdtExtension, defaultExtensionPath, createSqliteNodeApi } = await loadTreecrdt();

  const docId = "treecrdt-materialized-tree-smoke";
  const db = new Database(":memory:");
  loadTreecrdtExtension(db, { extensionPath: defaultExtensionPath() });
  const api = createSqliteNodeApi(db);
  await api.setDocId(docId);

  const replica = Buffer.from("r1");
  const root = Buffer.alloc(16, 0);
  const n1 = makeNodeId(1);
  const n2 = makeNodeId(2);

  db.prepare("SELECT treecrdt_append_op(?, ?, ?, ?, ?, ?, NULL, ?, NULL)").get(
    replica,
    1,
    1,
    "insert",
    root,
    n1,
    orderKeyFromPosition(0)
  );
  db.prepare("SELECT treecrdt_append_op(?, ?, ?, ?, ?, ?, NULL, ?, NULL)").get(
    replica,
    2,
    2,
    "insert",
    root,
    n2,
    orderKeyFromPosition(0)
  );
  db.prepare("SELECT treecrdt_append_op(?, ?, ?, ?, NULL, ?, ?, ?, NULL)").get(
    replica,
    3,
    3,
    "move",
    n2,
    n1,
    orderKeyFromPosition(0)
  );

  expect(await api.headLamport()).toBe(3);

  expect(await api.replicaMaxCounter(replica)).toBe(3);

  expect(await api.treeNodeCount()).toBe(2);

  expect((await api.treeChildren(root)) as string[]).toEqual([n1.toString("hex")]);

  expect((await api.treeChildren(n1)) as string[]).toEqual([n2.toString("hex")]);

  const dump = (await api.treeDump()) as Array<{ node: string; parent: string | null }>;
  const byId = new Map(dump.map((row) => [row.node, row]));
  expect(byId.get(root.toString("hex"))?.parent).toBe(null);
  expect(byId.get(n1.toString("hex"))?.parent).toBe(root.toString("hex"));
  expect(byId.get(n2.toString("hex"))?.parent).toBe(n1.toString("hex"));

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
  const { loadTreecrdtExtension, defaultExtensionPath, createSqliteNodeApi } = await loadTreecrdt();

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
      const api = createSqliteNodeApi(db);
      await api.setDocId(docId);
      db.prepare("SELECT treecrdt_append_op(?, ?, ?, ?, ?, ?, NULL, ?, NULL)").get(
        replica,
        1,
        1,
        "insert",
        root,
        n1,
        orderKeyFromPosition(0)
      );
      expect((await api.treeChildren(root)) as string[]).toEqual([n1.toString("hex")]);
      expect(await api.treeNodeCount()).toBe(1);
      db.close();
    }

    {
      const db = new Database(path);
      loadTreecrdtExtension(db, { extensionPath: defaultExtensionPath() });
      const api = createSqliteNodeApi(db);
      await api.setDocId(docId);
      expect((await api.treeChildren(root)) as string[]).toEqual([n1.toString("hex")]);
      expect(await api.treeNodeCount()).toBe(1);
      db.close();
    }
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("materialized tree: out-of-order ops rebuild correctly", async () => {
  const Database = await loadSqlite();
  const { loadTreecrdtExtension, defaultExtensionPath, createSqliteNodeApi } = await loadTreecrdt();

  const docId = "treecrdt-materialized-tree-out-of-order";
  const db = new Database(":memory:");
  loadTreecrdtExtension(db, { extensionPath: defaultExtensionPath() });
  const api = createSqliteNodeApi(db);
  await api.setDocId(docId);

  const replica = Buffer.from("r1");
  const root = Buffer.alloc(16, 0);
  const n1 = makeNodeId(1);
  const n2 = makeNodeId(2);

  db.prepare("SELECT treecrdt_append_op(?, ?, ?, ?, ?, ?, NULL, ?, NULL)").get(
    replica,
    1,
    2,
    "insert",
    root,
    n1,
    orderKeyFromPosition(0)
  );
  db.prepare("SELECT treecrdt_append_op(?, ?, ?, ?, ?, ?, NULL, ?, NULL)").get(
    replica,
    2,
    1,
    "insert",
    root,
    n2,
    orderKeyFromPosition(0)
  );

  const children = (await api.treeChildren(root)) as string[];
  expect(children.sort()).toEqual([n1.toString("hex"), n2.toString("hex")].sort());

  expect(await api.treeNodeCount()).toBe(2);

  expect(await api.headLamport()).toBe(2);
});

test("materialized tree: reindexes latest payload across moves for children(parent)", async () => {
  const Database = await loadSqlite();
  const { loadTreecrdtExtension, defaultExtensionPath } = await loadTreecrdt();

  const docId = "treecrdt-materialized-tree-payload-move";
  const db = new Database(":memory:");
  loadTreecrdtExtension(db, { extensionPath: defaultExtensionPath() });
  db.prepare("SELECT treecrdt_set_doc_id(?)").get(docId);

  const replica = Buffer.from("r1");
  const root = Buffer.alloc(16, 0);
  const p1 = makeNodeId(1);
  const p2 = makeNodeId(2);
  const child = makeNodeId(3);

  db.prepare("SELECT treecrdt_append_op(?, ?, ?, ?, ?, ?, NULL, ?, NULL)").get(
    replica,
    1,
    1,
    "insert",
    root,
    p1,
    orderKeyFromPosition(0)
  );
  db.prepare("SELECT treecrdt_append_op(?, ?, ?, ?, ?, ?, NULL, ?, NULL)").get(
    replica,
    2,
    2,
    "insert",
    root,
    p2,
    orderKeyFromPosition(0)
  );
  db.prepare("SELECT treecrdt_append_op(?, ?, ?, ?, ?, ?, NULL, ?, NULL)").get(
    replica,
    3,
    3,
    "insert",
    p1,
    child,
    orderKeyFromPosition(0)
  );
  db.prepare("SELECT treecrdt_append_op(?, ?, ?, ?, NULL, ?, NULL, NULL, ?)").get(
    replica,
    4,
    4,
    "payload",
    child,
    Buffer.from("hi"),
  );
  db.prepare("SELECT treecrdt_append_op(?, ?, ?, ?, NULL, ?, ?, ?, NULL)").get(
    replica,
    5,
    5,
    "move",
    child,
    p2,
    orderKeyFromPosition(0)
  );

  const refsRow: any = db.prepare("SELECT treecrdt_oprefs_children(?) AS v").get(p2);
  const refs = JSON.parse(refsRow.v) as number[][];
  expect(refs.length).toBe(2);

  const opsRow: any = db.prepare("SELECT treecrdt_ops_by_oprefs(?) AS v").get(JSON.stringify(refs));
  const ops = JSON.parse(opsRow.v) as Array<{ kind: string; payload?: number[] | null }>;
  expect(new Set(ops.map((op) => op.kind))).toEqual(new Set(["move", "payload"]));

  const payloadOp = ops.find((op) => op.kind === "payload");
  expect(payloadOp).toBeTruthy();
  expect(Buffer.from(payloadOp?.payload ?? []).toString()).toBe("hi");
});

test("materialized tree: payload persists across reopen", async () => {
  const Database = await loadSqlite();
  const { loadTreecrdtExtension, defaultExtensionPath } = await loadTreecrdt();

  const dir = mkdtempSync(join(tmpdir(), "treecrdt-payload-reopen-"));
  const path = join(dir, "db.sqlite");
  const docId = "treecrdt-materialized-tree-payload-reopen";
  const replica = Buffer.from("r1");
  const root = Buffer.alloc(16, 0);
  const n1 = makeNodeId(1);

  try {
    {
      const db = new Database(path);
      loadTreecrdtExtension(db, { extensionPath: defaultExtensionPath() });
      db.prepare("SELECT treecrdt_set_doc_id(?)").get(docId);
      db.prepare("SELECT treecrdt_append_op(?, ?, ?, ?, ?, ?, NULL, ?, NULL)").get(
        replica,
        1,
        1,
        "insert",
        root,
        n1,
        orderKeyFromPosition(0)
      );
      db.prepare("SELECT treecrdt_append_op(?, ?, ?, ?, NULL, ?, NULL, NULL, ?)").get(
        replica,
        2,
        2,
        "payload",
        n1,
        Buffer.from("hello"),
      );
      db.close();
    }

    {
      const db = new Database(path);
      loadTreecrdtExtension(db, { extensionPath: defaultExtensionPath() });
      db.prepare("SELECT treecrdt_set_doc_id(?)").get(docId);

      const refsRow: any = db.prepare("SELECT treecrdt_oprefs_children(?) AS v").get(root);
      const refs = JSON.parse(refsRow.v) as number[][];
      expect(refs.length).toBe(2);

      const opsRow: any = db.prepare("SELECT treecrdt_ops_by_oprefs(?) AS v").get(JSON.stringify(refs));
      const ops = JSON.parse(opsRow.v) as Array<{ kind: string; payload?: number[] | null }>;
      expect(new Set(ops.map((op) => op.kind))).toEqual(new Set(["insert", "payload"]));

      const payloadOp = ops.find((op) => op.kind === "payload");
      expect(Buffer.from(payloadOp?.payload ?? []).toString()).toBe("hello");

      db.close();
    }
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

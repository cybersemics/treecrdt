import { expect, test } from "vitest";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createTreecrdtClient, defaultExtensionPath, loadTreecrdtExtension } from "../dist/index.js";

function nodeIdFromInt(n: number): string {
  if (!Number.isInteger(n) || n < 0) throw new Error(`invalid node int: ${n}`);
  return n.toString(16).padStart(32, "0");
}

async function loadSqlite(): Promise<any> {
  const { default: Database } = await import("better-sqlite3").catch((err) => {
    throw new Error(
      `better-sqlite3 native binding not available; ensure it is installed/built before running native tests: ${err}`
    );
  });
  return Database;
}

async function openNodeEngine(path: string, docId: string) {
  const Database = await loadSqlite();
  const db = new Database(path);
  loadTreecrdtExtension(db, { extensionPath: defaultExtensionPath() });
  const engine = await createTreecrdtClient(db, { docId });
  return engine;
}

test("materialized tree: persists across reopen", async () => {
  const dir = mkdtempSync(join(tmpdir(), "treecrdt-mat-tree-"));
  const path = join(dir, "db.sqlite");
  const docId = "treecrdt-materialized-tree-reopen";
  const replica = "r1";
  const root = nodeIdFromInt(0);
  const n1 = nodeIdFromInt(1);

  try {
    {
      const engine = await openNodeEngine(path, docId);
      await engine.local.insert(replica, root, n1, { type: "last" }, null);
      expect(await engine.tree.children(root)).toEqual([n1]);
      expect(await engine.tree.nodeCount()).toBe(1);
      await engine.close();
    }

    {
      const engine = await openNodeEngine(path, docId);
      expect(await engine.tree.children(root)).toEqual([n1]);
      expect(await engine.tree.nodeCount()).toBe(1);
      await engine.close();
    }
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("materialized tree: payload persists across reopen", async () => {
  const dir = mkdtempSync(join(tmpdir(), "treecrdt-payload-reopen-"));
  const path = join(dir, "db.sqlite");
  const docId = "treecrdt-materialized-tree-payload-reopen";
  const replica = "r1";
  const root = nodeIdFromInt(0);
  const n1 = nodeIdFromInt(1);

  try {
    {
      const engine = await openNodeEngine(path, docId);
      await engine.local.insert(replica, root, n1, { type: "last" }, null);
      await engine.local.payload(replica, n1, new TextEncoder().encode("hello"));
      await engine.close();
    }

    {
      const engine = await openNodeEngine(path, docId);
      const refs = await engine.opRefs.children(root);
      expect(refs.length).toBe(2);
      const ops = await engine.ops.get(refs);
      expect(new Set(ops.map((op) => op.kind.type))).toEqual(new Set(["insert", "payload"]));

      const payloadOp = ops.find((op) => op.kind.type === "payload");
      expect(payloadOp && payloadOp.kind.type === "payload" ? new TextDecoder().decode(payloadOp.kind.payload ?? []) : "")
        .toBe("hello");

      await engine.close();
    }
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});


import { test } from "vitest";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { sqliteEngineConformanceScenarios } from "@treecrdt/sqlite-conformance";
import { createTreecrdtClient, defaultExtensionPath, loadTreecrdtExtension } from "../dist/index.js";

async function createNodeEngine(opts: { docId: string; path?: string }) {
  const { default: Database } = await import("better-sqlite3").catch((err) => {
    throw new Error(
      `better-sqlite3 native binding not available; ensure it is installed/built before running native tests: ${err}`
    );
  });

  const db = new Database(opts.path ?? ":memory:");
  loadTreecrdtExtension(db, { extensionPath: defaultExtensionPath() });
  return await createTreecrdtClient(db, { docId: opts.docId });
}

function docIdFromScenario(name: string): string {
  const slug = name
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
  return `treecrdt-node-conformance-${slug || "scenario"}`;
}

function slugify(name: string): string {
  return name
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

function track(engine: Awaited<ReturnType<typeof createNodeEngine>>, engines: any[]) {
  const originalClose = engine.close.bind(engine);
  let closed = false;
  engine.close = async () => {
    if (closed) return;
    closed = true;
    await originalClose();
  };
  engines.push(engine);
  return engine;
}

for (const scenario of sqliteEngineConformanceScenarios()) {
  test(`sqlite engine conformance (node): ${scenario.name}`, async () => {
    const docId = docIdFromScenario(scenario.name);
    const engines: any[] = [];
    let persistentDir: string | null = null;
    const persistentPaths = new Map<string, string>();
    const ensurePersistentDir = () => {
      if (persistentDir) return persistentDir;
      persistentDir = mkdtempSync(join(tmpdir(), "treecrdt-node-conformance-"));
      return persistentDir;
    };

    const engine = track(await createNodeEngine({ docId }), engines);
    try {
      await scenario.run({
        docId,
        engine,
        createEngine: ({ docId }) => createNodeEngine({ docId }).then((e) => track(e, engines)),
        createPersistentEngine: ({ docId, name }) => {
          const dir = ensurePersistentDir();
          const key = slugify(name || "db");
          const existing = persistentPaths.get(key);
          const path = existing ?? join(dir, `${key}.sqlite`);
          persistentPaths.set(key, path);
          return createNodeEngine({ docId, path }).then((e) => track(e, engines));
        },
      });
    } finally {
      for (const e of engines.reverse()) {
        try {
          await e.close();
        } catch {
          // ignore close failures during cleanup
        }
      }
      if (persistentDir) rmSync(persistentDir, { recursive: true, force: true });
    }
  });
}

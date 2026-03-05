import { test } from "vitest";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  conformanceSlugify,
  runTreecrdtEngineConformanceScenario,
  treecrdtEngineConformanceScenarios,
} from "@treecrdt/engine-conformance";
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

for (const scenario of treecrdtEngineConformanceScenarios()) {
  test(`sqlite engine conformance (node): ${scenario.name}`, async () => {
    let persistentDir: string | null = null;
    const persistentPaths = new Map<string, string>();
    const ensurePersistentDir = () => {
      if (persistentDir) return persistentDir;
      persistentDir = mkdtempSync(join(tmpdir(), "treecrdt-node-conformance-"));
      return persistentDir;
    };

    await runTreecrdtEngineConformanceScenario(scenario, {
      docIdPrefix: "treecrdt-node-conformance",
      openEngine: ({ docId }) => createNodeEngine({ docId }),
      openPersistentEngine: ({ docId, name }) => {
        const dir = ensurePersistentDir();
        const key = conformanceSlugify(name || "db");
        const existing = persistentPaths.get(key);
        const path = existing ?? join(dir, `${key}.sqlite`);
        persistentPaths.set(key, path);
        return createNodeEngine({ docId, path });
      },
      cleanup: () => {
        if (persistentDir) rmSync(persistentDir, { recursive: true, force: true });
      },
    });
  });
}

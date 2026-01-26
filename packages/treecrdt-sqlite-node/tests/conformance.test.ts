import { test } from "vitest";
import { sqliteEngineConformanceScenarios } from "@treecrdt/sqlite-conformance";
import { createTreecrdtClient, defaultExtensionPath, loadTreecrdtExtension } from "../dist/index.js";

async function createNodeEngine(docId: string) {
  const { default: Database } = await import("better-sqlite3").catch((err) => {
    throw new Error(
      `better-sqlite3 native binding not available; ensure it is installed/built before running native tests: ${err}`
    );
  });

  const db = new Database(":memory:");
  loadTreecrdtExtension(db, { extensionPath: defaultExtensionPath() });
  return await createTreecrdtClient(db, { docId });
}

function docIdFromScenario(name: string): string {
  const slug = name
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
  return `treecrdt-node-conformance-${slug || "scenario"}`;
}

for (const scenario of sqliteEngineConformanceScenarios()) {
  test(`sqlite engine conformance (node): ${scenario.name}`, async () => {
    const engine = await createNodeEngine(docIdFromScenario(scenario.name));
    try {
      await scenario.run(engine);
    } finally {
      await engine.close();
    }
  });
}


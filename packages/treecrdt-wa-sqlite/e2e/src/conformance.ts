import { createTreecrdtClient } from "@treecrdt/wa-sqlite/client";
import { sqliteEngineConformanceScenarios } from "@treecrdt/sqlite-conformance";

type StorageKind = "memory" | "opfs";

function docIdFromScenario(name: string, storage: StorageKind): string {
  const slug = name
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
  return `treecrdt-wa-sqlite-conformance-${storage}-${slug || "scenario"}`;
}

export async function runTreecrdtSqliteConformanceE2E(storage: StorageKind = "memory"): Promise<{ ok: true }> {
  const preferWorker = storage === "opfs";

  for (const scenario of sqliteEngineConformanceScenarios()) {
    const docId = docIdFromScenario(scenario.name, storage);
    const client = await createTreecrdtClient({ storage, preferWorker, docId });
    try {
      await scenario.run({
        docId,
        engine: client,
        createEngine: ({ docId }) => createTreecrdtClient({ storage, preferWorker, docId }),
      });
    } finally {
      await client.close();
    }
  }
  return { ok: true };
}

declare global {
  interface Window {
    runTreecrdtSqliteConformanceE2E?: typeof runTreecrdtSqliteConformanceE2E;
  }
}

if (typeof window !== "undefined") {
  window.runTreecrdtSqliteConformanceE2E = runTreecrdtSqliteConformanceE2E;
}

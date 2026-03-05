import { createTreecrdtClient } from "@treecrdt/wa-sqlite/client";
import {
  conformanceHashKey,
  conformanceSlugify,
  runSqliteEngineConformanceScenario,
  sqliteEngineConformanceScenarios,
} from "@treecrdt/sqlite-conformance";

type StorageKind = "memory" | "opfs";

export async function runTreecrdtSqliteConformanceE2E(storage: StorageKind = "memory"): Promise<{ ok: true }> {
  const runId =
    typeof crypto !== "undefined" && "randomUUID" in crypto
      ? crypto.randomUUID()
      : `${Date.now()}-${Math.random().toString(16).slice(2)}`;

  const preferWorker = storage === "opfs";

  for (const scenario of sqliteEngineConformanceScenarios()) {
    const runKey = runId.replace(/[^a-z0-9]/gi, "").slice(0, 10) || "run";
    const scenarioKey = conformanceHashKey(scenario.name);
    const filenameFor = (name: string) => {
      const nameKey = (conformanceSlugify(name) || "db").slice(0, 12);
      return `/treecrdt-c-${runKey}-${scenarioKey}-${nameKey}.db`;
    };
    const openEngine = async (opts: { docId: string; name?: string }) => {
      const name = opts.name ?? "main";
      const filename = storage === "opfs" ? filenameFor(name) : undefined;
      return await createTreecrdtClient({ storage, preferWorker, docId: opts.docId, filename });
    };

    await runSqliteEngineConformanceScenario(scenario, {
      docIdPrefix: `treecrdt-wa-sqlite-conformance-${storage}`,
      openEngine,
      openPersistentEngine: storage === "opfs" ? ({ docId, name }) => openEngine({ docId, name }) : undefined,
    });
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

import { randomUUID } from "node:crypto";

import { describe, test } from "vitest";

import type { Operation } from "@treecrdt/interface";
import type { TreecrdtEngine } from "@treecrdt/interface/engine";
import { bytesToHex } from "@treecrdt/interface/ids";
import { deriveOpRefV0 } from "@treecrdt/sync";
import {
  conformanceSlugify,
  runTreecrdtEngineConformanceScenario,
  treecrdtEngineConformanceScenarios,
} from "@treecrdt/engine-conformance";

import { createTreecrdtPostgresClient } from "../dist/index.js";

const POSTGRES_URL = process.env.TREECRDT_POSTGRES_URL;
const maybeDescribe = POSTGRES_URL ? describe : describe.skip;

function publicOpRef(docId: string, op: Operation): Uint8Array {
  return deriveOpRefV0(docId, {
    replica: op.meta.id.replica,
    counter: op.meta.id.counter,
  });
}

function wrapDocId(inner: TreecrdtEngine, publicDocId: string): TreecrdtEngine {
  const opRefsAll = async (): Promise<Uint8Array[]> => {
    const all = await inner.ops.all();
    return all.map((op) => publicOpRef(publicDocId, op));
  };

  const opRefsChildren = async (parent: string): Promise<Uint8Array[]> => {
    const ops = await inner.ops.children(parent);
    return ops.map((op) => publicOpRef(publicDocId, op));
  };

  const getByPublicOpRefs = async (opRefs: Uint8Array[]): Promise<Operation[]> => {
    if (opRefs.length === 0) return [];
    const all = await inner.ops.all();
    const byOpRef = new Map<string, Operation>();
    for (const op of all) byOpRef.set(bytesToHex(publicOpRef(publicDocId, op)), op);
    return opRefs.map((opRef) => {
      const op = byOpRef.get(bytesToHex(opRef));
      if (!op) throw new Error("opRef missing locally");
      return op;
    });
  };

  return {
    ...inner,
    docId: publicDocId,
    ops: {
      ...inner.ops,
      get: getByPublicOpRefs,
    },
    opRefs: {
      all: opRefsAll,
      children: opRefsChildren,
    },
  };
}

function internalDocId(publicDocId: string, key: string): string {
  return `${publicDocId}::${key}::${randomUUID()}`;
}

maybeDescribe("engine conformance scenarios (postgres-napi engine)", () => {
  for (const scenario of treecrdtEngineConformanceScenarios()) {
    test(
      `postgres engine conformance: ${scenario.name}`,
      async () => {
        const persistentInternal = new Map<string, string>();
        let ephemeralIndex = 0;

        const openWrapped = async (opts: {
          docId: string;
          persistentName?: string;
        }): Promise<TreecrdtEngine> => {
          const persistentKey =
            opts.persistentName == null
              ? null
              : `${opts.docId}:${conformanceSlugify(opts.persistentName) || "db"}`;
          const existingPersistentDoc = persistentKey ? persistentInternal.get(persistentKey) : undefined;
          const actualDocId =
            existingPersistentDoc ??
            internalDocId(
              opts.docId,
              persistentKey ? `persistent-${conformanceSlugify(opts.persistentName || "db") || "db"}` : `peer-${ephemeralIndex++}`
            );
          if (persistentKey && !existingPersistentDoc) persistentInternal.set(persistentKey, actualDocId);

          const raw = await createTreecrdtPostgresClient(POSTGRES_URL!, { docId: actualDocId });
          return wrapDocId(raw, opts.docId);
        };

        await runTreecrdtEngineConformanceScenario(scenario, {
          docIdPrefix: "treecrdt-postgres-conformance",
          openEngine: ({ docId }) => openWrapped({ docId }),
          openPersistentEngine: ({ docId, name }) => openWrapped({ docId, persistentName: name }),
        });
      },
      90_000
    );
  }
});

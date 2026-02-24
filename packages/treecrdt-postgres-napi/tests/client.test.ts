import { randomUUID } from "node:crypto";

import { expect, describe, test } from "vitest";

import type { ReplicaId } from "@treecrdt/interface";
import { nodeIdFromInt } from "@treecrdt/benchmark";

import { createTreecrdtPostgresClient } from "../dist/index.js";

const POSTGRES_URL = process.env.TREECRDT_POSTGRES_URL;
const maybeDescribe = POSTGRES_URL ? describe : describe.skip;

function replicaFromLabel(label: string): ReplicaId {
  const encoded = new TextEncoder().encode(label);
  if (encoded.length === 0) throw new Error("label must not be empty");
  const out = new Uint8Array(32);
  for (let i = 0; i < out.length; i += 1) out[i] = encoded[i % encoded.length]!;
  return out as ReplicaId;
}

maybeDescribe("postgres TreecrdtEngine (rust napi)", () => {
  const root = "0".repeat(32);

  test("local insert/move/delete/payload update materialized tree", async () => {
    const docId = `doc-${randomUUID()}`;
    const replica = replicaFromLabel("a");

    const engine = await createTreecrdtPostgresClient(POSTGRES_URL!, { docId });
    try {
      const n1 = nodeIdFromInt(1);
      const n2 = nodeIdFromInt(2);

      await engine.local.insert(replica, root, n1, { type: "first" }, new Uint8Array([1]));
      await engine.local.insert(replica, root, n2, { type: "last" }, null);

      expect(await engine.tree.children(root)).toEqual([n1, n2]);

      await engine.local.move(replica, n1, root, { type: "after", after: n2 });
      expect(await engine.tree.children(root)).toEqual([n2, n1]);

      await engine.local.payload(replica, n1, new Uint8Array([9]));

      const del = await engine.local.delete(replica, n2);
      expect(del.kind.type).toBe("delete");
      expect(del.meta.knownState).toBeInstanceOf(Uint8Array);
      expect(del.meta.knownState!.length).toBeGreaterThan(0);

      expect(await engine.tree.children(root)).toEqual([n1]);
      expect(await engine.meta.headLamport()).toBeGreaterThan(0);
    } finally {
      await engine.close();
    }
  });
});


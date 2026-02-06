import React, { useEffect, useState } from "react";
import type { Operation } from "@treecrdt/interface";
import { createTreecrdtClient, type TreecrdtClient } from "@treecrdt/wa-sqlite/client";

type ViewOp = Operation & { asText: string };

function orderKeyFromPosition(position: number): Uint8Array {
  if (!Number.isInteger(position) || position < 0) throw new Error(`invalid position: ${position}`);
  const n = position + 1;
  if (n > 0xffff) throw new Error(`position too large for u16 order key: ${position}`);
  const bytes = new Uint8Array(2);
  new DataView(bytes.buffer).setUint16(0, n, false);
  return bytes;
}

function replicaFromLabel(label: string): Uint8Array {
  const encoded = new TextEncoder().encode(label);
  if (encoded.length === 0) throw new Error("replica label must not be empty");
  const out = new Uint8Array(32);
  for (let i = 0; i < out.length; i += 1) out[i] = encoded[i % encoded.length]!;
  return out;
}

export default function App() {
  const [client, setClient] = useState<TreecrdtClient | null>(null);
  const [ops, setOps] = useState<ViewOp[]>([]);

  useEffect(() => {
    // Expose a small helper for e2e tests to assert client mode.
    if (typeof window !== "undefined") {
      (window as any).__createTreecrdtClient = async (storage: "memory" | "opfs", baseUrl?: string) => {
        const c = await createTreecrdtClient({ storage, baseUrl, preferWorker: storage === "opfs" });
        const summary = { mode: c.mode, storage: c.storage };
        if (c.close) await c.close();
        return summary;
      };
    }

    (async () => {
      try {
        const c = await createTreecrdtClient({ storage: "memory" });
        setClient(c);
      } catch (err) {
        console.error("Failed to init wa-sqlite", err);
      }
    })();
    return () => {
      void client?.close();
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const runDemo = async () => {
    if (!client) return;

    const replica = replicaFromLabel("demo");
    const rootId = "0".padStart(32, "0");
    const childId = "1".padStart(32, "0");

    const insertOp = makeOp("insert", replica, 1, 1, {
      parent: rootId,
      node: childId,
      orderKey: orderKeyFromPosition(0),
    });
    await client.ops.append(insertOp);

    const moveOp = makeOp("move", replica, 2, 2, {
      node: childId,
      newParent: rootId,
      orderKey: orderKeyFromPosition(0),
    });
    await client.ops.append(moveOp);

    const raw = await client.ops.all();
    setOps(raw.map((r) => ({ ...r, asText: JSON.stringify(makeJsonFriendlyOp(r)) })));
  };

  const runPayloadDemo = async () => {
    if (!client) return;

    const replica = replicaFromLabel("demo");
    const rootId = "0".padStart(32, "0");
    const childId = "1".padStart(32, "0");

    const insertOp = makeOp("insert", replica, 1, 1, {
      parent: rootId,
      node: childId,
      orderKey: orderKeyFromPosition(0),
    });
    const payloadOp: Operation = {
      meta: { id: { replica, counter: 2 }, lamport: 2 },
      kind: { type: "payload", node: childId, payload: new TextEncoder().encode("hello") },
    };

    // Exercise the bulk append path (treecrdt_append_ops).
    await client.ops.appendMany([insertOp, payloadOp]);

    const raw = await client.ops.all();
    setOps(raw.map((r) => ({ ...r, asText: JSON.stringify(makeJsonFriendlyOp(r)) })));
  };

  const runInsertWithPayloadDemo = async () => {
    if (!client) return;

    const replica = replicaFromLabel("demo");
    const rootId = "0".padStart(32, "0");
    const childId = "1".padStart(32, "0");

    const insertOp: Operation = {
      meta: { id: { replica, counter: 1 }, lamport: 1 },
      kind: {
        type: "insert",
        parent: rootId,
        node: childId,
        orderKey: orderKeyFromPosition(0),
        payload: new TextEncoder().encode("hello"),
      },
    };
    await client.ops.append(insertOp);

    const raw = await client.ops.all();
    setOps(raw.map((r) => ({ ...r, asText: JSON.stringify(makeJsonFriendlyOp(r)) })));
  };

  return (
    <div style={{ fontFamily: "sans-serif", padding: 24 }}>
      <h1>TreeCRDT wa-sqlite demo</h1>
      <button data-testid="run-demo" onClick={runDemo} disabled={!client}>
        Run insert + move
      </button>
      <button data-testid="run-payload-demo" onClick={runPayloadDemo} disabled={!client} style={{ marginLeft: 12 }}>
        Run insert + payload
      </button>
      <button
        data-testid="run-insert-payload-demo"
        onClick={runInsertWithPayloadDemo}
        disabled={!client}
        style={{ marginLeft: 12 }}
      >
        Run insert with payload
      </button>
      <ul data-testid="ops-list">
        {ops.map((op, idx) => (
          <li key={idx}>{op.asText}</li>
        ))}
      </ul>
    </div>
  );
}

function makeJsonFriendlyOp(op: Operation): unknown {
  const outReplica = Array.from(op.meta.id.replica);
  const kind = op.kind;
  if (kind.type === "payload") {
    return {
      ...op,
      meta: { ...op.meta, id: { ...op.meta.id, replica: outReplica } },
      kind: {
        ...kind,
        payload: kind.payload === null ? null : Array.from(kind.payload),
      },
    };
  }
  if (kind.type === "insert" && kind.payload !== undefined) {
    return {
      ...op,
      meta: { ...op.meta, id: { ...op.meta.id, replica: outReplica } },
      kind: {
        ...kind,
        payload: Array.from(kind.payload),
      },
    };
  }
  return {
    ...op,
    meta: { ...op.meta, id: { ...op.meta.id, replica: outReplica } },
  };
}

function makeOp(
  type: "insert" | "move",
  replica: Uint8Array,
  counter: number,
  lamport: number,
  fields: any
): Operation {
  const base = {
    meta: { id: { replica, counter }, lamport },
  };
  if (type === "insert") {
    return {
      ...base,
      kind: { type: "insert", ...fields },
    } as Operation;
  }
  return {
    ...base,
    kind: { type: "move", ...fields },
  } as Operation;
}

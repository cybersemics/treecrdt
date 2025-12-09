import React, { useEffect, useState } from "react";
import type { Operation } from "@treecrdt/interface";
import { createTreecrdtClient, type TreecrdtClient } from "@treecrdt/wa-sqlite/client";

type ViewOp = Operation & { asText: string };

export default function App() {
  const [client, setClient] = useState<TreecrdtClient | null>(null);
  const [ops, setOps] = useState<ViewOp[]>([]);

  useEffect(() => {
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

    const replica = new TextEncoder().encode("demo");
    const rootId = "0".padStart(32, "0");
    const childId = "1".padStart(32, "0");

    const insertOp = makeOp("insert", replica, 1, 1, {
      parent: rootId,
      node: childId,
      position: 0,
    });
    await client.append(insertOp);

    const moveOp = makeOp("move", replica, 2, 2, {
      node: childId,
      newParent: rootId,
      position: 0,
    });
    await client.append(moveOp);

    const raw = await client.opsSince(0);
    setOps(raw.map((r) => ({ ...r, asText: JSON.stringify(r) })));
  };

  return (
    <div style={{ fontFamily: "sans-serif", padding: 24 }}>
      <h1>TreeCRDT wa-sqlite demo</h1>
      <button data-testid="run-demo" onClick={runDemo} disabled={!client}>
        Run insert + move
      </button>
      <ul data-testid="ops-list">
        {ops.map((op, idx) => (
          <li key={idx}>{op.asText}</li>
        ))}
      </ul>
    </div>
  );
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

import React, { useEffect, useState } from "react";
import SQLiteESMFactory from "wa-sqlite";
import * as SQLite from "wa-sqlite/sqlite-api";
import type { Database } from "wa-sqlite";
import sqliteWasm from "/wa-sqlite/wa-sqlite-async.wasm?url";
import { appendOp, loadTreecrdtExtension, opsSince } from "@treecrdt/wa-sqlite";
import type { Operation } from "@treecrdt/interface";

type ViewOp = Operation & { asText: string };

export default function App() {
  const [db, setDb] = useState<Database | null>(null);
  const [ops, setOps] = useState<ViewOp[]>([]);

  useEffect(() => {
    (async () => {
      try {
        const module = await SQLiteESMFactory({
          locateFile: (file: string) =>
            file.endsWith(".wasm") ? sqliteWasm : file,
        });
        const sqlite3 = SQLite.Factory(module);
        const handle = await sqlite3.open_v2(":memory:");
        const database = makeDbAdapter(sqlite3, handle);
        await loadTreecrdtExtension({ db: database });
        setDb(database);
      } catch (err) {
        console.error("Failed to init wa-sqlite", err);
      }
    })();
  }, []);

  const runDemo = async () => {
    if (!db) return;

    const replica = new TextEncoder().encode("demo");
    const serializeNodeId = (id: string) => hexToBytes(id);
    const serializeReplica = (r: Operation["meta"]["id"]["replica"]) =>
      typeof r === "string" ? new TextEncoder().encode(r) : r;

    const rootId = "0".padStart(32, "0");
    const childId = "1".padStart(32, "0");

    const insertOp = makeOp("insert", replica, 1, 1, {
      parent: rootId,
      node: childId,
      position: 0,
    });
    await appendOp(db, insertOp, serializeNodeId, serializeReplica);

    const moveOp = makeOp("move", replica, 2, 2, {
      node: childId,
      newParent: rootId,
      position: 0,
    });
    await appendOp(db, moveOp, serializeNodeId, serializeReplica);

    const raw = await opsSince(db, { lamport: 0 });
    setOps(
      raw.map((r: any) => ({
        meta: {
          id: { replica: r.replica, counter: r.counter },
          lamport: r.lamport,
        },
        kind: parseKind(r),
        asText: JSON.stringify(r),
      }))
    );
  };

  return (
    <div style={{ fontFamily: "sans-serif", padding: 24 }}>
      <h1>TreeCRDT wa-sqlite demo</h1>
      <button data-testid="run-demo" onClick={runDemo} disabled={!db}>
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

function parseKind(r: any): Operation["kind"] {
  switch (r.kind) {
    case "insert":
      return {
        type: "insert",
        parent: bytesToHex(r.parent),
        node: bytesToHex(r.node),
        position: r.position ?? 0,
      };
    case "move":
      return {
        type: "move",
        node: bytesToHex(r.node),
        newParent: bytesToHex(r.new_parent),
        position: r.position ?? 0,
      };
    case "delete":
      return { type: "delete", node: bytesToHex(r.node) };
    case "tombstone":
      return { type: "tombstone", node: bytesToHex(r.node) };
    default:
      throw new Error("unknown kind");
  }
}

function hexToBytes(hex: string): Uint8Array {
  const clean = hex.startsWith("0x") ? hex.slice(2) : hex;
  const bytes = new Uint8Array(clean.length / 2);
  for (let i = 0; i < clean.length; i += 2) {
    bytes[i / 2] = parseInt(clean.slice(i, i + 2), 16);
  }
  return bytes;
}

function bytesToHex(bytes: number[] | Uint8Array): string {
  const view = bytes instanceof Uint8Array ? bytes : Uint8Array.from(bytes);
  return Array.from(view)
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
}

function makeDbAdapter(sqlite3: any, handle: number): Database {
  const prepare = async (sql: string) => {
    const iter = sqlite3.statements(handle, sql, { unscoped: true });
    const { value } = await iter.next();
    if (iter.return) {
      await iter.return();
    }
    if (!value) {
      throw new Error(`Failed to prepare statement: ${sql}`);
    }
    return value;
  };

  return {
    prepare,
    bind: async (stmt, index, value) => sqlite3.bind(stmt, index, value),
    step: async (stmt) => sqlite3.step(stmt),
    column_text: async (stmt, index) => sqlite3.column_text(stmt, index),
    finalize: async (stmt) => sqlite3.finalize(stmt),
    exec: async (sql: string) => sqlite3.exec(handle, sql),
  } as unknown as Database;
}

import crypto from "node:crypto";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { test, expect } from "vitest";
import WebSocket from "ws";

import type { Operation } from "@treecrdt/interface";
import { ROOT_NODE_ID_HEX, bytesToHex, hexToBytes, nodeIdToBytes16, replicaIdToBytes } from "@treecrdt/interface/ids";
import type { Filter, SyncBackend, SyncMessage } from "@treecrdt/sync";
import { SyncPeer, deriveOpRefV0 } from "@treecrdt/sync";
import { treecrdtSyncV0ProtobufCodec } from "@treecrdt/sync/protobuf";
import type { DuplexTransport } from "@treecrdt/sync/transport";
import { wrapDuplexTransportWithCodec } from "@treecrdt/sync/transport";

import { startSyncServer } from "../dist/server.js";

type Deferred<T> = {
  promise: Promise<T>;
  resolve: (value: T) => void;
  reject: (err: unknown) => void;
};

function deferred<T>(): Deferred<T> {
  let resolve!: (value: T) => void;
  let reject!: (err: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

async function withTimeout<T>(promise: Promise<T>, ms: number, label: string): Promise<T> {
  const timeout = deferred<never>();
  const timer = setTimeout(() => timeout.reject(new Error(`timeout after ${ms}ms: ${label}`)), ms);
  try {
    return await Promise.race([promise, timeout.promise]);
  } finally {
    clearTimeout(timer);
  }
}

function randomNodeId(): string {
  return crypto.randomBytes(16).toString("hex");
}

function testReplicaId(label: string): Uint8Array {
  // Sync v0 ReplicaId is defined as 32 bytes (ed25519 pubkey bytes).
  // For tests we only need stable uniqueness, not a real keypair.
  return crypto.createHash("sha256").update(label, "utf8").digest();
}

function childrenFilter(parentHex: string): Filter {
  return { children: { parent: nodeIdToBytes16(parentHex) } };
}

function toUint8Array(data: WebSocket.RawData): Uint8Array {
  if (data instanceof Uint8Array) return data;
  if (data instanceof ArrayBuffer) return new Uint8Array(data);
  if (Array.isArray(data)) return Buffer.concat(data);
  if (typeof data === "string") return Buffer.from(data);
  return Buffer.from(data);
}

function createWebSocketTransport(ws: WebSocket): DuplexTransport<Uint8Array> {
  return {
    send: (bytes) =>
      new Promise<void>((resolve, reject) => {
        ws.send(bytes, { binary: true }, (err) => (err ? reject(err) : resolve()));
      }),
    onMessage: (handler) => {
      const onMessage = (data: WebSocket.RawData) => handler(toUint8Array(data));
      ws.on("message", onMessage);
      return () => ws.off("message", onMessage);
    },
  };
}

function createMemoryBackend(
  docId: string,
  opts: { onAppliedOps?: (ops: Operation[]) => void } = {}
): SyncBackend<Operation> {
  const opsByOpRef = new Map<string, Operation>();
  let maxLamport = 0;

  const opRefHex = (op: Operation): string => {
    const ref = deriveOpRefV0(docId, {
      replica: replicaIdToBytes(op.meta.id.replica),
      counter: BigInt(op.meta.id.counter),
    });
    return bytesToHex(ref);
  };

  return {
    docId,
    maxLamport: async () => BigInt(maxLamport),
    listOpRefs: async (filter) => {
      if ("all" in filter) {
        return Array.from(opsByOpRef.keys(), (hex) => hexToBytes(hex));
      }

      const parentHex = bytesToHex(filter.children.parent);
      const out: Uint8Array[] = [];
      for (const [refHex, op] of opsByOpRef.entries()) {
        if (op.kind.type === "insert" && bytesToHex(nodeIdToBytes16(op.kind.parent)) === parentHex) {
          out.push(hexToBytes(refHex));
        }
        if (op.kind.type === "move" && bytesToHex(nodeIdToBytes16(op.kind.newParent)) === parentHex) {
          out.push(hexToBytes(refHex));
        }
      }
      return out;
    },
    getOpsByOpRefs: async (opRefs) => {
      const out: Operation[] = [];
      for (const r of opRefs) {
        const op = opsByOpRef.get(bytesToHex(r));
        if (op) out.push(op);
      }
      return out;
    },
    applyOps: async (ops) => {
      const newlyApplied: Operation[] = [];
      for (const op of ops) {
        const key = opRefHex(op);
        if (opsByOpRef.has(key)) continue;
        opsByOpRef.set(key, op);
        maxLamport = Math.max(maxLamport, op.meta.lamport);
        newlyApplied.push(op);
      }
      if (newlyApplied.length > 0) opts.onAppliedOps?.(newlyApplied);
    },
  };
}

async function waitForMessage<T>(transport: DuplexTransport<T>, predicate: (msg: T) => boolean): Promise<T> {
  const d = deferred<T>();
  const detach = transport.onMessage((msg) => {
    if (!predicate(msg)) return;
    detach();
    d.resolve(msg);
  });
  return d.promise;
}

async function connectPeer(docId: string, wsUrl: string, backend: SyncBackend<Operation>) {
  const ws = new WebSocket(wsUrl);
  ws.binaryType = "arraybuffer";
  await new Promise<void>((resolve, reject) => {
    ws.once("open", () => resolve());
    ws.once("error", () => reject(new Error("WebSocket connection failed")));
  });

  const wire = createWebSocketTransport(ws);
  const transport = wrapDuplexTransportWithCodec<Uint8Array, SyncMessage<Operation>>(wire, treecrdtSyncV0ProtobufCodec);
  const peer = new SyncPeer<Operation>(backend);
  const detach = peer.attach(transport);

  const close = async () => {
    detach();
    ws.close();
    await new Promise<void>((resolve) => ws.once("close", () => resolve()));
  };

  return { transport, peer, close };
}
test(
  "sync server live subscription push (children filter)",
  async () => {

  const dbDir = await fs.mkdtemp(path.join(os.tmpdir(), "treecrdt-sync-server-e2e-"));
  const docId = `e2e-${crypto.randomUUID()}`;
  const replicaA = testReplicaId("clientA");
  const replicaB = testReplicaId("clientB");

  const server = await startSyncServer({ host: "127.0.0.1", port: 0, dbDir, idleCloseMs: 0 });
  const wsUrl = `ws://${server.host}:${server.port}/sync?docId=${encodeURIComponent(docId)}`;

  let nodeA1 = "";
  let nodeA1Child = "";

  const gotRootInsertOnB = deferred<Operation>();
  const gotChildInsertOnA = deferred<Operation>();

  const backendA = createMemoryBackend(docId, {
    onAppliedOps: (ops) => {
      for (const op of ops) {
        if (op.kind.type === "insert" && op.kind.parent === nodeA1 && op.kind.node === nodeA1Child) {
          gotChildInsertOnA.resolve(op);
        }
      }
    },
  });

  const backendB = createMemoryBackend(docId, {
    onAppliedOps: (ops) => {
      for (const op of ops) {
        if (op.kind.type === "insert" && op.kind.parent === ROOT_NODE_ID_HEX && op.kind.node === nodeA1) {
          gotRootInsertOnB.resolve(op);
        }
      }
    },
  });

  const a = await connectPeer(docId, wsUrl, backendA);
  const b = await connectPeer(docId, wsUrl, backendB);

  try {
    // Subscribe B to root children (push-only, no polling).
    const subBAck = waitForMessage(b.transport, (m) => (m as any).payload?.case === "subscribeAck");
    const subB = b.peer.subscribe(b.transport, childrenFilter(ROOT_NODE_ID_HEX), { immediate: false, intervalMs: 0 });
    await withTimeout(subBAck, 2_000, "B subscribeAck(root)");

    // A inserts a root child and syncs it to the server; B should receive it via subscription push.
    nodeA1 = randomNodeId();
    const opA1: Operation = {
      meta: { id: { replica: replicaA, counter: 1 }, lamport: 1 },
      kind: { type: "insert", parent: ROOT_NODE_ID_HEX, node: nodeA1, orderKey: Uint8Array.of(0) },
    };
    await backendA.applyOps([opA1]);
    await a.peer.syncOnce(a.transport, childrenFilter(ROOT_NODE_ID_HEX));
    const rootInsertOnB = await withTimeout(gotRootInsertOnB.promise, 5_000, "B received root insert via push");

    // Now subscribe A to nodeA1 children (push-only), then have B add a child under nodeA1.
    const subAAck = waitForMessage(a.transport, (m) => (m as any).payload?.case === "subscribeAck");
    const subA = a.peer.subscribe(a.transport, childrenFilter(nodeA1), { immediate: false, intervalMs: 0 });
    await withTimeout(subAAck, 2_000, "A subscribeAck(children(nodeA1))");

    nodeA1Child = randomNodeId();
    const opB1: Operation = {
      meta: { id: { replica: replicaB, counter: 1 }, lamport: 1 },
      kind: { type: "insert", parent: nodeA1, node: nodeA1Child, orderKey: Uint8Array.of(0) },
    };
    await backendB.applyOps([opB1]);
    await b.peer.syncOnce(b.transport, childrenFilter(nodeA1));
    const childInsertOnA = await withTimeout(gotChildInsertOnA.promise, 5_000, "A received child insert via push");

    subA.stop();
    subB.stop();
    await Promise.allSettled([subA.done, subB.done]);

    expect(rootInsertOnB.kind.type).toBe("insert");
    if (rootInsertOnB.kind.type !== "insert") throw new Error("expected rootInsertOnB to be insert");
    expect(rootInsertOnB.kind.parent).toBe(ROOT_NODE_ID_HEX);
    expect(rootInsertOnB.kind.node).toBe(nodeA1);
    expect(childInsertOnA.kind.type).toBe("insert");
    if (childInsertOnA.kind.type !== "insert") throw new Error("expected childInsertOnA to be insert");
    expect(childInsertOnA.kind.parent).toBe(nodeA1);
    expect(childInsertOnA.kind.node).toBe(nodeA1Child);
  } finally {
    await Promise.allSettled([a.close(), b.close()]);
    await server.close();
    await fs.rm(dbDir, { recursive: true, force: true });
  }
  },
  { timeout: 60_000 }
);

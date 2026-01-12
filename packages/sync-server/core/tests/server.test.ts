import http from "node:http";

import { test, expect } from "vitest";
import WebSocket from "ws";

import type { SyncMessage } from "@treecrdt/sync";
import type { WireCodec } from "@treecrdt/sync/transport";

import { startWebSocketSyncServer } from "../dist/index.js";

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

async function httpGet(url: string): Promise<{ status: number; body: string }> {
  return new Promise((resolve, reject) => {
    const req = http.get(url, (res) => {
      const chunks: Buffer[] = [];
      res.on("data", (chunk) => chunks.push(Buffer.from(chunk)));
      res.on("end", () => {
        resolve({
          status: res.statusCode ?? 0,
          body: Buffer.concat(chunks).toString("utf8"),
        });
      });
    });
    req.once("error", reject);
  });
}

function jsonCodec<Op>(): WireCodec<SyncMessage<Op>, Uint8Array> {
  return {
    encode: (message) => Buffer.from(JSON.stringify(message), "utf8"),
    decode: (wire) => JSON.parse(Buffer.from(wire).toString("utf8")) as SyncMessage<Op>,
  };
}

async function connectWebSocket(url: string): Promise<WebSocket> {
  const ws = new WebSocket(url);
  await new Promise<void>((resolve, reject) => {
    ws.once("open", () => resolve());
    ws.once("error", reject);
  });
  return ws;
}

test("health endpoint returns ok", async () => {
  const server = await startWebSocketSyncServer({
    host: "127.0.0.1",
    port: 0,
    codec: jsonCodec(),
    docs: {
      async open() {
        throw new Error("docs.open should not be called by /health");
      },
    },
  });

  try {
    const base = `http://${server.host}:${server.port}`;

    const health = await httpGet(`${base}/health`);
    expect(health.status).toBe(200);
    expect(health.body).toBe("ok");

    const notFound = await httpGet(`${base}/not-found`);
    expect(notFound.status).toBe(404);
    expect(notFound.body).toBe("not found");
  } finally {
    await server.close();
  }
});

test(
  "opens docId and calls release on close",
  async () => {
    const openedDocIds: string[] = [];
    const released = deferred<void>();

    const server = await startWebSocketSyncServer({
      host: "127.0.0.1",
      port: 0,
      codec: jsonCodec(),
      docs: {
        async open(docId) {
          openedDocIds.push(docId);
          return {
            backend: {
              docId,
              maxLamport: async () => BigInt(0),
              listOpRefs: async () => [],
              getOpsByOpRefs: async () => [],
              applyOps: async () => {},
            },
            release: () => released.resolve(),
          };
        },
      },
    });

    const docId = "core-test-doc";
    const wsUrl = `ws://${server.host}:${server.port}/sync?docId=${encodeURIComponent(docId)}`;

    try {
      const ws = await connectWebSocket(wsUrl);
      expect(openedDocIds).toEqual([docId]);

      const closed = deferred<void>();
      ws.once("close", () => closed.resolve());

      ws.close();
      await withTimeout(closed.promise, 2_000, "ws close");

      await withTimeout(released.promise, 2_000, "server release callback");
    } finally {
      await server.close();
    }
  },
  { timeout: 20_000 }
);

test(
  "closes with 1011 when doc open fails",
  async () => {
    const server = await startWebSocketSyncServer({
      host: "127.0.0.1",
      port: 0,
      codec: jsonCodec(),
      docs: {
        async open() {
          throw new Error("boom");
        },
      },
    });

    const wsUrl = `ws://${server.host}:${server.port}/sync?docId=${encodeURIComponent("fail-open")}`;

    try {
      const closed = deferred<{ code: number; reason: string }>();
      const ws = new WebSocket(wsUrl);
      ws.once("close", (code, reason) => {
        closed.resolve({ code, reason: reason.toString("utf8") });
      });
      ws.once("error", closed.reject);
      await new Promise<void>((resolve, reject) => {
        ws.once("open", () => resolve());
        ws.once("error", reject);
      });

      const info = await withTimeout(closed.promise, 2_000, "ws close after open failure");
      expect(info.code).toBe(1011);
      expect(info.reason).toBe("failed to open doc");
    } finally {
      await server.close();
    }
  },
  { timeout: 20_000 }
);

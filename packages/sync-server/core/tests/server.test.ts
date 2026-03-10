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

async function connectWebSocketRejected(url: string): Promise<{ status: number; body: string }> {
  return new Promise((resolve, reject) => {
    let settled = false;
    const finishResolve = (value: { status: number; body: string }) => {
      if (settled) return;
      settled = true;
      resolve(value);
    };
    const finishReject = (error: unknown) => {
      if (settled) return;
      settled = true;
      reject(error);
    };

    const ws = new WebSocket(url);
    ws.once("open", () => {
      ws.close();
      finishReject(new Error("expected websocket upgrade to be rejected"));
    });
    ws.once("unexpected-response", (_req, res) => {
      const chunks: Buffer[] = [];
      res.on("data", (chunk) => chunks.push(Buffer.from(chunk)));
      res.on("end", () => {
        finishResolve({
          status: res.statusCode ?? 0,
          body: Buffer.concat(chunks).toString("utf8"),
        });
      });
    });
    ws.once("error", (err) => {
      // If `unexpected-response` also fires, it will win via the settled guard.
      queueMicrotask(() => finishReject(err));
    });
  });
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

test("health endpoint uses readiness callback result", async () => {
  const server = await startWebSocketSyncServer({
    host: "127.0.0.1",
    port: 0,
    codec: jsonCodec(),
    docs: {
      async open() {
        throw new Error("docs.open should not be called by /health");
      },
    },
    healthCheck: async () => ({
      ok: false,
      statusCode: 503,
      body: "postgres unavailable",
    }),
  });

  try {
    const health = await httpGet(`http://${server.host}:${server.port}/health`);
    expect(health.status).toBe(503);
    expect(health.body).toBe("postgres unavailable");
  } finally {
    await server.close();
  }
});

test("health endpoint treats readiness callback errors as not ready", async () => {
  const server = await startWebSocketSyncServer({
    host: "127.0.0.1",
    port: 0,
    codec: jsonCodec(),
    docs: {
      async open() {
        throw new Error("docs.open should not be called by /health");
      },
    },
    healthCheck: async () => {
      throw new Error("db ping failed");
    },
  });

  try {
    const health = await httpGet(`http://${server.host}:${server.port}/health`);
    expect(health.status).toBe(503);
    expect(health.body).toBe("not ready");
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
  "releases doc when websocket closes while docs.open is still in flight",
  async () => {
    const openGate = deferred<void>();
    const opened = deferred<void>();
    const released = deferred<void>();
    let opens = 0;
    let releases = 0;
    let peersAdded = 0;
    let peersRemoved = 0;

    const server = await startWebSocketSyncServer({
      host: "127.0.0.1",
      port: 0,
      codec: jsonCodec(),
      docs: {
        async open(docId) {
          opens += 1;
          await openGate.promise;
          opened.resolve();
          return {
            backend: {
              docId,
              maxLamport: async () => BigInt(0),
              listOpRefs: async () => [],
              getOpsByOpRefs: async () => [],
              applyOps: async () => {},
            },
            onPeerAdded: () => {
              peersAdded += 1;
            },
            onPeerRemoved: () => {
              peersRemoved += 1;
            },
            release: () => {
              releases += 1;
              released.resolve();
            },
          };
        },
      },
    });

    const wsUrl = `ws://${server.host}:${server.port}/sync?docId=${encodeURIComponent("in-flight-open-close")}`;

    try {
      const ws = await connectWebSocket(wsUrl);
      const closed = deferred<void>();
      ws.once("close", () => closed.resolve());
      ws.close();
      await withTimeout(closed.promise, 2_000, "client close before open resolves");

      openGate.resolve();
      await withTimeout(opened.promise, 2_000, "docs.open resolves");
      await withTimeout(released.promise, 2_000, "release after in-flight open close");

      expect(opens).toBe(1);
      expect(releases).toBe(1);
      expect(peersAdded <= 1).toBe(true);
      expect(peersRemoved).toBe(peersAdded);
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

test(
  "authenticate hook denies unauthorized upgrades before docs.open",
  async () => {
    let opens = 0;
    const server = await startWebSocketSyncServer({
      host: "127.0.0.1",
      port: 0,
      codec: jsonCodec(),
      hooks: {
        onAuthenticate: ({ url }) => {
          if (url.searchParams.get("token") === "secret") return { allow: true };
          return { allow: false, statusCode: 401, body: "invalid token" };
        },
      },
      docs: {
        async open(docId) {
          opens += 1;
          return {
            backend: {
              docId,
              maxLamport: async () => BigInt(0),
              listOpRefs: async () => [],
              getOpsByOpRefs: async () => [],
              applyOps: async () => {},
            },
          };
        },
      },
    });

    try {
      const unauthorizedUrl = `ws://${server.host}:${server.port}/sync?docId=${encodeURIComponent("hook-auth")}`;
      const denied = await withTimeout(
        connectWebSocketRejected(unauthorizedUrl),
        2_000,
        "unauthorized upgrade rejection"
      );
      expect(denied.status).toBe(401);
      expect(denied.body).toBe("invalid token");
      expect(opens).toBe(0);

      const authorizedUrl = `${unauthorizedUrl}&token=secret`;
      const ws = await connectWebSocket(authorizedUrl);
      ws.close();
      await new Promise<void>((resolve) => ws.once("close", () => resolve()));
      expect(opens).toBe(1);
    } finally {
      await server.close();
    }
  },
  { timeout: 20_000 }
);

test(
  "rate-limit hook rejects excessive upgrades before docs.open",
  async () => {
    let opens = 0;
    let upgrades = 0;
    const server = await startWebSocketSyncServer({
      host: "127.0.0.1",
      port: 0,
      codec: jsonCodec(),
      hooks: {
        onRateLimit: () => {
          upgrades += 1;
          if (upgrades <= 1) return { allow: true };
          return { allow: false, statusCode: 429, body: "too many upgrades" };
        },
      },
      docs: {
        async open(docId) {
          opens += 1;
          return {
            backend: {
              docId,
              maxLamport: async () => BigInt(0),
              listOpRefs: async () => [],
              getOpsByOpRefs: async () => [],
              applyOps: async () => {},
            },
          };
        },
      },
    });

    const wsUrl = `ws://${server.host}:${server.port}/sync?docId=${encodeURIComponent("hook-rate-limit")}`;

    try {
      const first = await connectWebSocket(wsUrl);
      first.close();
      await new Promise<void>((resolve) => first.once("close", () => resolve()));
      expect(opens).toBe(1);

      const denied = await withTimeout(
        connectWebSocketRejected(wsUrl),
        2_000,
        "rate limited upgrade rejection"
      );
      expect(denied.status).toBe(429);
      expect(denied.body).toBe("too many upgrades");
      expect(opens).toBe(1);
    } finally {
      await server.close();
    }
  },
  { timeout: 20_000 }
);

import http from "node:http";

import WebSocket, { WebSocketServer } from "ws";

import type { SyncBackend, SyncMessage, SyncPeerOptions } from "@treecrdt/sync";
import { SyncPeer } from "@treecrdt/sync";
import type { DuplexTransport, WireCodec } from "@treecrdt/sync/transport";
import { wrapDuplexTransportWithCodec } from "@treecrdt/sync/transport";

type Awaitable<T> = T | Promise<T>;
type UpgradeSocket = {
  destroyed?: boolean;
  remoteAddress?: string;
  write(chunk: string | Uint8Array): unknown;
  destroy(error?: Error): void;
};

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
        try {
          ws.send(bytes, { binary: true }, (err) => (err ? reject(err) : resolve()));
        } catch (err) {
          reject(err instanceof Error ? err : new Error(String(err)));
        }
      }),
    onMessage: (handler) => {
      const onMessage = (data: WebSocket.RawData) => handler(toUint8Array(data));
      ws.on("message", onMessage);
      return () => ws.off("message", onMessage);
    },
  };
}

export type WebSocketSyncServerDocHandle<Op> = {
  backend: SyncBackend<Op>;
  peerOptions?: SyncPeerOptions<Op>;
  onPeerAdded?: (peer: SyncPeer<Op>) => void;
  onPeerRemoved?: (peer: SyncPeer<Op>) => void;
  release?: () => void | Promise<void>;
};

export interface WebSocketSyncServerDocProvider<Op> {
  open(docId: string): Promise<WebSocketSyncServerDocHandle<Op>>;
}

export type WebSocketSyncServerUpgradeContext = {
  req: http.IncomingMessage;
  url: URL;
  docId: string;
  remoteAddress: string | null;
};

export type WebSocketSyncServerUpgradeDecision =
  | {
      allow: true;
    }
  | {
      allow: false;
      statusCode?: number;
      body?: string;
    };

export type WebSocketSyncServerUpgradeHook = (
  ctx: WebSocketSyncServerUpgradeContext
) => Awaitable<void | boolean | WebSocketSyncServerUpgradeDecision>;

export type WebSocketSyncServerHooks = {
  onRateLimit?: WebSocketSyncServerUpgradeHook;
  onAuthenticate?: WebSocketSyncServerUpgradeHook;
  onAuthorize?: WebSocketSyncServerUpgradeHook;
  onError?: (
    error: unknown,
    ctx: WebSocketSyncServerUpgradeContext & {
      stage: "rate_limit" | "authenticate" | "authorize";
    }
  ) => void;
};

export type WebSocketSyncServerOptions<Op> = {
  host?: string;
  port?: number;
  syncPath?: string;
  healthPath?: string;
  statusPath?: string;
  healthCheck?: () => Awaitable<WebSocketSyncServerHealthResult>;
  statusInfo?: () => Awaitable<Record<string, unknown>>;
  maxPayloadBytes?: number;
  hooks?: WebSocketSyncServerHooks;
  codec: WireCodec<SyncMessage<Op>, Uint8Array>;
  docs: WebSocketSyncServerDocProvider<Op>;
};

export type WebSocketSyncServerHealthResult =
  | {
      ok: true;
      body?: string;
      contentType?: string;
    }
  | {
      ok: false;
      statusCode?: number;
      body?: string;
      contentType?: string;
    };

export type WebSocketSyncServerHandle = {
  host: string;
  port: number;
  close: () => Promise<void>;
};

function denyUpgrade(socket: UpgradeSocket, statusCode: number, body: string): void {
  if (socket.destroyed) return;
  const safeStatusCode =
    Number.isInteger(statusCode) && statusCode >= 400 && statusCode <= 599 ? statusCode : 403;
  const statusText = http.STATUS_CODES[safeStatusCode] ?? "Forbidden";
  const payload = Buffer.from(body, "utf8");
  const response =
    `HTTP/1.1 ${safeStatusCode} ${statusText}\r\n` +
    "Connection: close\r\n" +
    "Content-Type: text/plain; charset=utf-8\r\n" +
    `Content-Length: ${payload.length}\r\n` +
    "\r\n";
  try {
    socket.write(response);
    socket.write(payload);
  } finally {
    socket.destroy();
  }
}

function defaultDenyForStage(stage: "rate_limit" | "authenticate" | "authorize"): {
  statusCode: number;
  body: string;
} {
  if (stage === "rate_limit") return { statusCode: 429, body: "rate limited" };
  if (stage === "authenticate") return { statusCode: 401, body: "unauthorized" };
  return { statusCode: 403, body: "forbidden" };
}

function normalizeHookDecision(
  stage: "rate_limit" | "authenticate" | "authorize",
  decision: void | boolean | WebSocketSyncServerUpgradeDecision
): { allow: true } | { allow: false; statusCode: number; body: string } {
  if (typeof decision === "undefined" || decision === true) return { allow: true };
  const fallback = defaultDenyForStage(stage);
  if (decision === false) return { allow: false, statusCode: fallback.statusCode, body: fallback.body };
  if (decision.allow) return { allow: true };
  return {
    allow: false,
    statusCode: decision.statusCode ?? fallback.statusCode,
    body: decision.body ?? fallback.body,
  };
}

async function runUpgradeHook(
  stage: "rate_limit" | "authenticate" | "authorize",
  hook: WebSocketSyncServerUpgradeHook | undefined,
  onError: WebSocketSyncServerHooks["onError"] | undefined,
  ctx: WebSocketSyncServerUpgradeContext
): Promise<{ allow: true } | { allow: false; statusCode: number; body: string }> {
  if (!hook) return { allow: true };
  try {
    const result = await hook(ctx);
    return normalizeHookDecision(stage, result);
  } catch (error) {
    try {
      onError?.(error, { ...ctx, stage });
    } catch {
      // ignore hook error callback failures
    }
    const fallback = defaultDenyForStage(stage);
    return { allow: false, statusCode: fallback.statusCode, body: fallback.body };
  }
}

export async function startWebSocketSyncServer<Op>(
  opts: WebSocketSyncServerOptions<Op>
): Promise<WebSocketSyncServerHandle> {
  const host = opts.host ?? "0.0.0.0";
  const port = Number(opts.port ?? 8787);
  const syncPath = opts.syncPath ?? "/sync";
  const healthPath = opts.healthPath ?? "/health";
  const statusPath = opts.statusPath ?? "/status";
  const maxPayloadBytes = Number(opts.maxPayloadBytes ?? 10 * 1024 * 1024);

  if (!Number.isFinite(port) || port < 0) throw new Error(`invalid port: ${opts.port}`);
  if (!syncPath.startsWith("/")) throw new Error(`syncPath must start with "/": ${syncPath}`);
  if (!healthPath.startsWith("/")) throw new Error(`healthPath must start with "/": ${healthPath}`);
  if (!statusPath.startsWith("/")) throw new Error(`statusPath must start with "/": ${statusPath}`);
  if (!Number.isFinite(maxPayloadBytes) || maxPayloadBytes <= 0) {
    throw new Error(`invalid maxPayloadBytes: ${opts.maxPayloadBytes}`);
  }

  const server = http.createServer((req, res) => {
    const url = new URL(req.url ?? "/", `http://${req.headers.host ?? "localhost"}`);
    if (url.pathname === healthPath) {
      void (async () => {
        try {
          const result = (await opts.healthCheck?.()) ?? { ok: true as const };
          if (result.ok) {
            res.writeHead(200, { "content-type": result.contentType ?? "text/plain" });
            res.end(result.body ?? "ok");
            return;
          }

          const requestedStatusCode = result.statusCode;
          const statusCode =
            typeof requestedStatusCode === "number" &&
            Number.isInteger(requestedStatusCode) &&
            requestedStatusCode >= 400 &&
            requestedStatusCode <= 599
              ? requestedStatusCode
              : 503;
          res.writeHead(statusCode, { "content-type": result.contentType ?? "text/plain" });
          res.end(result.body ?? "not ready");
        } catch {
          res.writeHead(503, { "content-type": "text/plain" });
          res.end("not ready");
        }
      })();
      return;
    }

    if (url.pathname === statusPath) {
      void (async () => {
        try {
          const status = (await opts.statusInfo?.()) ?? { ok: true };
          res.writeHead(200, { "content-type": "application/json; charset=utf-8" });
          res.end(JSON.stringify(status));
        } catch {
          res.writeHead(500, { "content-type": "application/json; charset=utf-8" });
          res.end(JSON.stringify({ ok: false, error: "status unavailable" }));
        }
      })();
      return;
    }

    res.writeHead(404, { "content-type": "text/plain" });
    res.end("not found");
  });

  const wss = new WebSocketServer({ noServer: true, maxPayload: maxPayloadBytes });
  const peersByDocId = new Map<string, Set<SyncPeer<Op>>>();

  const addDocPeer = (docId: string, peer: SyncPeer<Op>) => {
    const existing = peersByDocId.get(docId);
    if (existing) {
      existing.add(peer);
      return;
    }
    peersByDocId.set(docId, new Set([peer]));
  };

  const removeDocPeer = (docId: string, peer: SyncPeer<Op>) => {
    const existing = peersByDocId.get(docId);
    if (!existing) return;
    existing.delete(peer);
    if (existing.size === 0) peersByDocId.delete(docId);
  };

  const notifyOtherDocPeers = async (docId: string, source: SyncPeer<Op> | undefined) => {
    const peers = peersByDocId.get(docId);
    if (!peers || peers.size === 0) return;
    const pending: Promise<void>[] = [];
    for (const peer of peers) {
      if (peer === source) continue;
      pending.push(peer.notifyLocalUpdate());
    }
    if (pending.length === 0) return;
    await Promise.allSettled(pending);
  };

  server.on("upgrade", (req, socket, head) => {
    const upgradeSocket = socket as UpgradeSocket;
    const url = new URL(req.url ?? "/", `http://${req.headers.host ?? "localhost"}`);
    if (url.pathname !== syncPath) {
      upgradeSocket.destroy();
      return;
    }
    const docId = url.searchParams.get("docId");
    if (!docId) {
      upgradeSocket.destroy();
      return;
    }

    const ctx: WebSocketSyncServerUpgradeContext = {
      req,
      url,
      docId,
      remoteAddress: upgradeSocket.remoteAddress ?? null,
    };

    void (async () => {
      const rateLimitDecision = await runUpgradeHook(
        "rate_limit",
        opts.hooks?.onRateLimit,
        opts.hooks?.onError,
        ctx
      );
      if (!rateLimitDecision.allow) {
        denyUpgrade(upgradeSocket, rateLimitDecision.statusCode, rateLimitDecision.body);
        return;
      }

      const authDecision = await runUpgradeHook(
        "authenticate",
        opts.hooks?.onAuthenticate,
        opts.hooks?.onError,
        ctx
      );
      if (!authDecision.allow) {
        denyUpgrade(upgradeSocket, authDecision.statusCode, authDecision.body);
        return;
      }

      const authorizeDecision = await runUpgradeHook(
        "authorize",
        opts.hooks?.onAuthorize,
        opts.hooks?.onError,
        ctx
      );
      if (!authorizeDecision.allow) {
        denyUpgrade(upgradeSocket, authorizeDecision.statusCode, authorizeDecision.body);
        return;
      }

      (req as any).treecrdtDocId = docId;
      wss.handleUpgrade(req, socket, head, (ws) => {
        wss.emit("connection", ws, req);
      });
    })();
  });

  wss.on("connection", (ws, req) => {
    void (async () => {
      const docId = String((req as any).treecrdtDocId ?? "");
      if (!docId) {
        ws.close();
        return;
      }

      let cleaned = false;
      let doc: WebSocketSyncServerDocHandle<Op> | undefined;
      let peer: SyncPeer<Op> | undefined;
      let detach: (() => void) | undefined;

      const cleanup = async () => {
        if (cleaned) return;
        cleaned = true;
        try {
          detach?.();
        } catch {}
        try {
          if (peer) removeDocPeer(docId, peer);
        } catch {}
        try {
          if (doc && peer) doc.onPeerRemoved?.(peer);
        } catch {}
        try {
          await doc?.release?.();
        } catch {}
      };

      ws.once("close", () => void cleanup());
      ws.once("error", () => void cleanup());

      let openedDoc: WebSocketSyncServerDocHandle<Op>;
      try {
        openedDoc = await opts.docs.open(docId);
      } catch {
        if (!cleaned && ws.readyState === WebSocket.OPEN) {
          ws.close(1011, "failed to open doc");
        }
        return;
      }

      if (cleaned) {
        try {
          await openedDoc.release?.();
        } catch {}
        return;
      }

      doc = openedDoc;
      const wire = createWebSocketTransport(ws);
      const transport = wrapDuplexTransportWithCodec<Uint8Array, SyncMessage<Op>>(wire, opts.codec);
      const docBackend = doc.backend;
      const backend: SyncBackend<Op> = {
        docId: docBackend.docId,
        maxLamport: () => docBackend.maxLamport(),
        listOpRefs: (filter) => docBackend.listOpRefs(filter),
        getOpsByOpRefs: (opRefs) => docBackend.getOpsByOpRefs(opRefs),
        applyOps: async (ops) => {
          await docBackend.applyOps(ops);
          if (ops.length === 0) return;
          await notifyOtherDocPeers(docId, peer);
        },
        ...(docBackend.storePendingOps ? { storePendingOps: (ops) => docBackend.storePendingOps!(ops) } : {}),
        ...(docBackend.listPendingOps ? { listPendingOps: () => docBackend.listPendingOps!() } : {}),
        ...(docBackend.deletePendingOps ? { deletePendingOps: (ops) => docBackend.deletePendingOps!(ops) } : {}),
      };
      peer = new SyncPeer<Op>(backend, doc.peerOptions);
      addDocPeer(docId, peer);
      detach = peer.attach(transport, {
        onError: () => {
          if (ws.readyState === WebSocket.OPEN) ws.close(1011, "sync handler error");
        },
      });

      doc.onPeerAdded?.(peer);
    })();
  });

  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(port, host, resolve);
  });

  const address = server.address();
  const actualPort = typeof address === "object" && address ? address.port : port;

  const close = async (): Promise<void> => {
    try {
      for (const ws of wss.clients) ws.close();
    } catch {}

    await new Promise<void>((resolve) => wss.close(() => resolve()));
    await new Promise<void>((resolve) => server.close(() => resolve()));
  };

  return { host, port: actualPort, close };
}

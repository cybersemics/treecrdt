import http from "node:http";

import WebSocket, { WebSocketServer } from "ws";

import type { SyncBackend, SyncMessage, SyncPeerOptions } from "@treecrdt/sync";
import { SyncPeer } from "@treecrdt/sync";
import type { DuplexTransport, WireCodec } from "@treecrdt/sync/transport";
import { wrapDuplexTransportWithCodec } from "@treecrdt/sync/transport";

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

export type WebSocketSyncServerPeerErrorContext<Op> = {
  docId: string;
  messageType: SyncMessage<Op>["payload"]["case"];
  message: SyncMessage<Op>;
};

export type WebSocketSyncServerDocHandle<Op> = {
  backend: SyncBackend<Op>;
  peerOptions?: SyncPeerOptions;
  onPeerAdded?: (peer: SyncPeer<Op>) => void;
  onPeerRemoved?: (peer: SyncPeer<Op>) => void;
  release?: () => void | Promise<void>;
};

export interface WebSocketSyncServerDocProvider<Op> {
  open(docId: string): Promise<WebSocketSyncServerDocHandle<Op>>;
}

export type WebSocketSyncServerOptions<Op> = {
  host?: string;
  port?: number;
  syncPath?: string;
  healthPath?: string;
  maxPayloadBytes?: number;
  codec: WireCodec<SyncMessage<Op>, Uint8Array>;
  docs: WebSocketSyncServerDocProvider<Op>;
  onPeerError?: (err: unknown, ctx: WebSocketSyncServerPeerErrorContext<Op>) => void;
};

export type WebSocketSyncServerHandle = {
  host: string;
  port: number;
  close: () => Promise<void>;
};

export async function startWebSocketSyncServer<Op>(
  opts: WebSocketSyncServerOptions<Op>
): Promise<WebSocketSyncServerHandle> {
  const host = opts.host ?? "0.0.0.0";
  const port = Number(opts.port ?? 8787);
  const syncPath = opts.syncPath ?? "/sync";
  const healthPath = opts.healthPath ?? "/health";
  const maxPayloadBytes = Number(opts.maxPayloadBytes ?? 10 * 1024 * 1024);

  if (!Number.isFinite(port) || port < 0) throw new Error(`invalid port: ${opts.port}`);
  if (!syncPath.startsWith("/")) throw new Error(`syncPath must start with "/": ${syncPath}`);
  if (!healthPath.startsWith("/")) throw new Error(`healthPath must start with "/": ${healthPath}`);
  if (!Number.isFinite(maxPayloadBytes) || maxPayloadBytes <= 0) {
    throw new Error(`invalid maxPayloadBytes: ${opts.maxPayloadBytes}`);
  }

  const server = http.createServer((req, res) => {
    const url = new URL(req.url ?? "/", `http://${req.headers.host ?? "localhost"}`);
    if (url.pathname === healthPath) {
      res.writeHead(200, { "content-type": "text/plain" });
      res.end("ok");
      return;
    }

    res.writeHead(404, { "content-type": "text/plain" });
    res.end("not found");
  });

  const wss = new WebSocketServer({ noServer: true, maxPayload: maxPayloadBytes });

  server.on("upgrade", (req, socket, head) => {
    const url = new URL(req.url ?? "/", `http://${req.headers.host ?? "localhost"}`);
    if (url.pathname !== syncPath) {
      socket.destroy();
      return;
    }
    const docId = url.searchParams.get("docId");
    if (!docId) {
      socket.destroy();
      return;
    }

    (req as any).treecrdtDocId = docId;
    wss.handleUpgrade(req, socket, head, (ws) => {
      wss.emit("connection", ws, req);
    });
  });

  wss.on("connection", (ws, req) => {
    void (async () => {
      const docId = String((req as any).treecrdtDocId ?? "");
      if (!docId) {
        ws.close();
        return;
      }

      let doc: WebSocketSyncServerDocHandle<Op>;
      try {
        doc = await opts.docs.open(docId);
      } catch {
        ws.close(1011, "failed to open doc");
        return;
      }

      const wire = createWebSocketTransport(ws);
      const transport = wrapDuplexTransportWithCodec<Uint8Array, SyncMessage<Op>>(wire, opts.codec);
      const peer = new SyncPeer<Op>(doc.backend, doc.peerOptions);

      const detach = peer.attach(transport, {
        onError: (err, ctx) => {
          try {
            opts.onPeerError?.(err, { docId, messageType: ctx.message.payload.case, message: ctx.message });
          } catch {
            // ignore user callback failures
          }
        },
      });

      doc.onPeerAdded?.(peer);

      let cleaned = false;
      const cleanup = async () => {
        if (cleaned) return;
        cleaned = true;
        try {
          detach();
        } catch {}
        try {
          doc.onPeerRemoved?.(peer);
        } catch {}
        try {
          await doc.release?.();
        } catch {}
      };

      ws.once("close", () => void cleanup());
      ws.once("error", () => void cleanup());
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

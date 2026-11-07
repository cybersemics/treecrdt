import http from 'node:http';

import WebSocket, { WebSocketServer } from 'ws';

import type {
  SyncBackend,
  SyncMessage,
  SyncMessagePayload,
  SyncPeerOptions,
} from '@treecrdt/sync-protocol';
import { SyncPeer } from '@treecrdt/sync-protocol';
import type { DuplexTransport, WireCodec } from '@treecrdt/sync-protocol/transport';
import {
  createTransportCloseController,
  wrapDuplexTransportWithCodec,
} from '@treecrdt/sync-protocol/transport';

import type { SyncServerLogFields, SyncServerLogger } from './log.js';
import { silentSyncServerLogger, syncServerErrorMessage } from './log.js';

type Awaitable<T> = T | Promise<T>;
type UpgradeSocket = {
  destroyed?: boolean;
  remoteAddress?: string;
  write(chunk: string | Uint8Array): unknown;
  destroy(error?: Error): void;
};

function summarizeSyncMessage<Op>(message: SyncMessage<Op>): SyncServerLogFields {
  const payload: SyncMessagePayload<Op> = message.payload;
  const fields: SyncServerLogFields = {
    docId: message.docId,
    type: payload.case,
  };
  switch (payload.case) {
    case 'hello':
      fields.filters = payload.value.filters.length;
      fields.capabilities = payload.value.capabilities.length;
      fields.maxLamport = payload.value.maxLamport.toString();
      break;
    case 'helloAck':
      fields.acceptedFilters = payload.value.acceptedFilters.length;
      fields.rejectedFilters = payload.value.rejectedFilters.length;
      fields.maxLamport = payload.value.maxLamport.toString();
      break;
    case 'ribltCodewords':
      fields.filterId = payload.value.filterId;
      fields.round = payload.value.round;
      fields.codewords = payload.value.codewords.length;
      fields.startIndex = payload.value.startIndex.toString();
      break;
    case 'ribltStatus': {
      fields.filterId = payload.value.filterId;
      fields.round = payload.value.round;
      const status = payload.value.payload;
      fields.status = status.case;
      if (status.case === 'decoded') {
        fields.senderMissing = status.value.senderMissing.length;
        fields.receiverMissing = status.value.receiverMissing.length;
      } else if (status.case === 'more') {
        fields.credits = status.value.credits;
      } else {
        fields.reason = status.value.reason;
      }
      break;
    }
    case 'opsBatch':
      fields.filterId = payload.value.filterId;
      fields.ops = payload.value.ops.length;
      fields.done = payload.value.done;
      break;
    case 'subscribe':
      fields.subscriptionId = payload.value.subscriptionId;
      fields.filter = 'all' in payload.value.filter ? 'all' : 'children';
      break;
    case 'subscribeAck':
      fields.subscriptionId = payload.value.subscriptionId;
      fields.currentLamport = payload.value.currentLamport.toString();
      break;
    case 'unsubscribe':
      fields.subscriptionId = payload.value.subscriptionId;
      break;
    case 'error':
      fields.code = payload.value.code;
      fields.error = payload.value.message;
      break;
    default:
      break;
  }
  return fields;
}

function withSyncMessageLogs<Op>(
  transport: DuplexTransport<SyncMessage<Op>>,
  log: SyncServerLogger,
  docId: string,
): DuplexTransport<SyncMessage<Op>> {
  if (log.level !== 'debug') return transport;
  const logMessage = (direction: 'sent' | 'received', message: SyncMessage<Op>) => {
    try {
      log.debug(`sync message ${direction}`, { docId, ...summarizeSyncMessage(message) });
    } catch {
      // logging must never affect server behavior
    }
  };
  return {
    send: async (message) => {
      await transport.send(message);
      logMessage('sent', message);
    },
    onMessage: (handler) =>
      transport.onMessage((message) => {
        logMessage('received', message);
        handler(message);
      }),
    close: (reason) => transport.close(reason),
    closeSignal: transport.closeSignal,
  };
}

function toUint8Array(data: WebSocket.RawData): Uint8Array {
  if (data instanceof Uint8Array) return data;
  if (data instanceof ArrayBuffer) return new Uint8Array(data);
  if (Array.isArray(data)) return Buffer.concat(data);
  if (typeof data === 'string') return Buffer.from(data);
  return Buffer.from(data);
}

/** Close initiated by the remote WebSocket peer, with the status code attached. */
class WebSocketPeerClose extends Error {
  readonly code: number;

  constructor(code: number, reasonText: string) {
    const detail = reasonText.length > 0 ? `: ${reasonText}` : '';
    super(`websocket closed (${code})${detail}`);
    this.name = 'WebSocketPeerClose';
    this.code = code;
  }
}

function isCleanWebSocketClose(error: unknown): boolean {
  return error instanceof WebSocketPeerClose && (error.code === 1000 || error.code === 1001);
}

function createWebSocketTransport(ws: WebSocket): DuplexTransport<Uint8Array> {
  const closeController = createTransportCloseController();

  ws.once('close', (code, reason) => {
    closeController.close(new WebSocketPeerClose(code, reason.toString()));
  });
  ws.once('error', (error) => closeController.close(error));

  if (ws.readyState === WebSocket.CLOSING || ws.readyState === WebSocket.CLOSED) {
    closeController.close(new Error('websocket is not open'));
  }

  return {
    send: (bytes) =>
      new Promise<void>((resolve, reject) => {
        if (closeController.signal.closed) {
          reject(
            closeController.signal.reason instanceof Error
              ? closeController.signal.reason
              : new Error('websocket is closed'),
          );
          return;
        }
        try {
          ws.send(bytes, { binary: true }, (err) => (err ? reject(err) : resolve()));
        } catch (err) {
          reject(err instanceof Error ? err : new Error(String(err)));
        }
      }),
    onMessage: (handler) => {
      if (closeController.signal.closed) return () => {};
      const onMessage = (data: WebSocket.RawData) => {
        if (closeController.signal.closed) return;
        handler(toUint8Array(data));
      };
      ws.on('message', onMessage);
      return () => ws.off('message', onMessage);
    },
    close: (reason) => {
      if (closeController.signal.closed) return;
      closeController.close(reason);
      if (ws.readyState !== WebSocket.OPEN) return;
      if (reason) ws.close(1002, 'invalid sync frame');
      else ws.close();
    },
    closeSignal: closeController.signal,
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
  ctx: WebSocketSyncServerUpgradeContext,
) => Awaitable<void | boolean | WebSocketSyncServerUpgradeDecision>;

export type WebSocketSyncServerHooks = {
  onRateLimit?: WebSocketSyncServerUpgradeHook;
  onAuthenticate?: WebSocketSyncServerUpgradeHook;
  onAuthorize?: WebSocketSyncServerUpgradeHook;
  onError?: (
    error: unknown,
    ctx: WebSocketSyncServerUpgradeContext & {
      stage: 'rate_limit' | 'authenticate' | 'authorize';
    },
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
  /** Defaults to a silent logger. Pass a leveled logger to record server events. */
  log?: SyncServerLogger;
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
  const statusText = http.STATUS_CODES[safeStatusCode] ?? 'Forbidden';
  const payload = Buffer.from(body, 'utf8');
  const response =
    `HTTP/1.1 ${safeStatusCode} ${statusText}\r\n` +
    'Connection: close\r\n' +
    'Content-Type: text/plain; charset=utf-8\r\n' +
    `Content-Length: ${payload.length}\r\n` +
    '\r\n';
  try {
    socket.write(response);
    socket.write(payload);
  } finally {
    socket.destroy();
  }
}

function defaultDenyForStage(stage: 'rate_limit' | 'authenticate' | 'authorize'): {
  statusCode: number;
  body: string;
} {
  if (stage === 'rate_limit') return { statusCode: 429, body: 'rate limited' };
  if (stage === 'authenticate') return { statusCode: 401, body: 'unauthorized' };
  return { statusCode: 403, body: 'forbidden' };
}

function normalizeHookDecision(
  stage: 'rate_limit' | 'authenticate' | 'authorize',
  decision: void | boolean | WebSocketSyncServerUpgradeDecision,
): { allow: true } | { allow: false; statusCode: number; body: string } {
  if (typeof decision === 'undefined' || decision === true) return { allow: true };
  const fallback = defaultDenyForStage(stage);
  if (decision === false)
    return { allow: false, statusCode: fallback.statusCode, body: fallback.body };
  if (decision.allow) return { allow: true };
  return {
    allow: false,
    statusCode: decision.statusCode ?? fallback.statusCode,
    body: decision.body ?? fallback.body,
  };
}

async function runUpgradeHook(
  stage: 'rate_limit' | 'authenticate' | 'authorize',
  hook: WebSocketSyncServerUpgradeHook | undefined,
  onError: WebSocketSyncServerHooks['onError'] | undefined,
  ctx: WebSocketSyncServerUpgradeContext,
  log: SyncServerLogger,
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
    log.error('upgrade hook failed', {
      stage,
      docId: ctx.docId,
      remoteAddress: ctx.remoteAddress,
      error: syncServerErrorMessage(error),
    });
    const fallback = defaultDenyForStage(stage);
    return { allow: false, statusCode: fallback.statusCode, body: fallback.body };
  }
}

export async function startWebSocketSyncServer<Op>(
  opts: WebSocketSyncServerOptions<Op>,
): Promise<WebSocketSyncServerHandle> {
  const host = opts.host ?? '0.0.0.0';
  const port = Number(opts.port ?? 8787);
  const syncPath = opts.syncPath ?? '/sync';
  const healthPath = opts.healthPath ?? '/health';
  const statusPath = opts.statusPath ?? '/status';
  const maxPayloadBytes = Number(opts.maxPayloadBytes ?? 10 * 1024 * 1024);
  const log = opts.log ?? silentSyncServerLogger;

  if (!Number.isFinite(port) || port < 0) throw new Error(`invalid port: ${opts.port}`);
  if (!syncPath.startsWith('/')) throw new Error(`syncPath must start with "/": ${syncPath}`);
  if (!healthPath.startsWith('/')) throw new Error(`healthPath must start with "/": ${healthPath}`);
  if (!statusPath.startsWith('/')) throw new Error(`statusPath must start with "/": ${statusPath}`);
  if (!Number.isFinite(maxPayloadBytes) || maxPayloadBytes <= 0) {
    throw new Error(`invalid maxPayloadBytes: ${opts.maxPayloadBytes}`);
  }

  const server = http.createServer((req, res) => {
    const url = new URL(req.url ?? '/', `http://${req.headers.host ?? 'localhost'}`);
    if (url.pathname === healthPath) {
      void (async () => {
        try {
          const result = (await opts.healthCheck?.()) ?? { ok: true as const };
          if (result.ok) {
            res.writeHead(200, { 'content-type': result.contentType ?? 'text/plain' });
            res.end(result.body ?? 'ok');
            return;
          }

          const requestedStatusCode = result.statusCode;
          const statusCode =
            typeof requestedStatusCode === 'number' &&
            Number.isInteger(requestedStatusCode) &&
            requestedStatusCode >= 400 &&
            requestedStatusCode <= 599
              ? requestedStatusCode
              : 503;
          log.warn('health check failed', { statusCode, body: result.body ?? 'not ready' });
          res.writeHead(statusCode, { 'content-type': result.contentType ?? 'text/plain' });
          res.end(result.body ?? 'not ready');
        } catch (error) {
          log.warn('health check failed', {
            statusCode: 503,
            error: syncServerErrorMessage(error),
          });
          res.writeHead(503, { 'content-type': 'text/plain' });
          res.end('not ready');
        }
      })();
      return;
    }

    if (url.pathname === statusPath) {
      void (async () => {
        try {
          const status = (await opts.statusInfo?.()) ?? { ok: true };
          res.writeHead(200, { 'content-type': 'application/json; charset=utf-8' });
          res.end(JSON.stringify(status));
        } catch (error) {
          log.error('status check failed', { error: syncServerErrorMessage(error) });
          res.writeHead(500, { 'content-type': 'application/json; charset=utf-8' });
          res.end(JSON.stringify({ ok: false, error: 'status unavailable' }));
        }
      })();
      return;
    }

    log.debug('http not found', { method: req.method, path: url.pathname });
    res.writeHead(404, { 'content-type': 'text/plain' });
    res.end('not found');
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

  const notifyOtherDocPeers = async (
    docId: string,
    source: SyncPeer<Op> | undefined,
    ops?: readonly Op[],
  ) => {
    const peers = peersByDocId.get(docId);
    if (!peers || peers.size === 0) return;
    const pending: Promise<void>[] = [];
    for (const peer of peers) {
      if (peer === source) continue;
      pending.push(peer.notifyLocalUpdate(ops));
    }
    if (pending.length === 0) return;
    log.debug('notifying peers', { docId, peers: pending.length, ops: ops?.length });
    const results = await Promise.allSettled(pending);
    for (const result of results) {
      if (result.status === 'rejected') {
        log.warn('peer notify failed', { docId, error: syncServerErrorMessage(result.reason) });
      }
    }
  };

  server.on('upgrade', (req, socket, head) => {
    const upgradeSocket = socket as UpgradeSocket;
    const url = new URL(req.url ?? '/', `http://${req.headers.host ?? 'localhost'}`);
    if (url.pathname !== syncPath) {
      log.debug('upgrade ignored', {
        path: url.pathname,
        remoteAddress: upgradeSocket.remoteAddress ?? null,
      });
      upgradeSocket.destroy();
      return;
    }
    const docId = url.searchParams.get('docId');
    if (!docId) {
      log.warn('upgrade rejected', {
        reason: 'missing docId',
        remoteAddress: upgradeSocket.remoteAddress ?? null,
      });
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
        'rate_limit',
        opts.hooks?.onRateLimit,
        opts.hooks?.onError,
        ctx,
        log,
      );
      if (!rateLimitDecision.allow) {
        log.info('upgrade denied', {
          stage: 'rate_limit',
          docId,
          statusCode: rateLimitDecision.statusCode,
          remoteAddress: ctx.remoteAddress,
        });
        denyUpgrade(upgradeSocket, rateLimitDecision.statusCode, rateLimitDecision.body);
        return;
      }

      const authDecision = await runUpgradeHook(
        'authenticate',
        opts.hooks?.onAuthenticate,
        opts.hooks?.onError,
        ctx,
        log,
      );
      if (!authDecision.allow) {
        log.info('upgrade denied', {
          stage: 'authenticate',
          docId,
          statusCode: authDecision.statusCode,
          remoteAddress: ctx.remoteAddress,
        });
        denyUpgrade(upgradeSocket, authDecision.statusCode, authDecision.body);
        return;
      }

      const authorizeDecision = await runUpgradeHook(
        'authorize',
        opts.hooks?.onAuthorize,
        opts.hooks?.onError,
        ctx,
        log,
      );
      if (!authorizeDecision.allow) {
        log.info('upgrade denied', {
          stage: 'authorize',
          docId,
          statusCode: authorizeDecision.statusCode,
          remoteAddress: ctx.remoteAddress,
        });
        denyUpgrade(upgradeSocket, authorizeDecision.statusCode, authorizeDecision.body);
        return;
      }

      (req as any).treecrdtDocId = docId;
      wss.handleUpgrade(req, socket, head, (ws) => {
        wss.emit('connection', ws, req);
      });
    })();
  });

  wss.on('connection', (ws, req) => {
    void (async () => {
      const docId = String((req as any).treecrdtDocId ?? '');
      if (!docId) {
        ws.close();
        return;
      }

      let cleaned = false;
      let doc: WebSocketSyncServerDocHandle<Op> | undefined;
      let peer: SyncPeer<Op> | undefined;
      let detach: (() => void) | undefined;
      let peerAnnounced = false;

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
          if (doc && peer && peerAnnounced) doc.onPeerRemoved?.(peer);
        } catch {}
        try {
          await doc?.release?.();
        } catch {}
        log.info('peer disconnected', { docId, attached: peerAnnounced });
      };

      // Let the transport's close subscribers reject protocol work before detaching them.
      ws.once('close', () => queueMicrotask(() => void cleanup()));
      ws.once('error', () => queueMicrotask(() => void cleanup()));

      let openedDoc: WebSocketSyncServerDocHandle<Op>;
      try {
        openedDoc = await opts.docs.open(docId);
      } catch (error) {
        log.error('failed to open doc', { docId, error: syncServerErrorMessage(error) });
        if (!cleaned && ws.readyState === WebSocket.OPEN) {
          ws.close(1011, 'failed to open doc');
        }
        return;
      }

      if (cleaned || ws.readyState !== WebSocket.OPEN) {
        try {
          await openedDoc.release?.();
        } catch {}
        return;
      }

      doc = openedDoc;
      const wire = createWebSocketTransport(ws);
      const decoded = wrapDuplexTransportWithCodec<Uint8Array, SyncMessage<Op>>(wire, opts.codec);
      const transport = withSyncMessageLogs(decoded, log, docId);
      const docBackend = doc.backend;
      const backend: SyncBackend<Op> = {
        docId: docBackend.docId,
        maxLamport: () => docBackend.maxLamport(),
        listOpRefs: (filter) => docBackend.listOpRefs(filter),
        getOpsByOpRefs: (opRefs) => docBackend.getOpsByOpRefs(opRefs),
        applyOps: async (ops) => {
          await docBackend.applyOps(ops);
          if (ops.length === 0) return;
          await notifyOtherDocPeers(docId, peer, ops);
        },
        ...(docBackend.storePendingOps
          ? { storePendingOps: (ops) => docBackend.storePendingOps!(ops) }
          : {}),
        ...(docBackend.listPendingOps
          ? { listPendingOps: () => docBackend.listPendingOps!() }
          : {}),
        ...(docBackend.deletePendingOps
          ? { deletePendingOps: (ops) => docBackend.deletePendingOps!(ops) }
          : {}),
      };
      peer = new SyncPeer<Op>(backend, doc.peerOptions);
      try {
        detach = peer.attach(transport, {
          onError: (ctx) => {
            // 1000 and 1001 are peers leaving. Cleanup already logs the disconnect.
            if (isCleanWebSocketClose(ctx.error)) return;
            log.error('sync handler error', { docId, error: syncServerErrorMessage(ctx.error) });
            // Let a codec-triggered protocol close (1002) start before falling back
            // to a generic handler-error close.
            queueMicrotask(() => {
              if (ws.readyState === WebSocket.OPEN) ws.close(1011, 'sync handler error');
            });
          },
        });
      } catch (error) {
        log.error('failed to attach peer', { docId, error: syncServerErrorMessage(error) });
        await cleanup();
        return;
      }

      addDocPeer(docId, peer);
      peerAnnounced = true;
      doc.onPeerAdded?.(peer);
      log.info('peer connected', {
        docId,
        remoteAddress: req.socket.remoteAddress ?? null,
      });
    })();
  });

  await new Promise<void>((resolve, reject) => {
    server.once('error', reject);
    server.listen(port, host, resolve);
  });

  const address = server.address();
  const actualPort = typeof address === 'object' && address ? address.port : port;
  log.info('listening', { host, port: actualPort, syncPath, healthPath, statusPath });

  const close = async (): Promise<void> => {
    log.info('closing', { host, port: actualPort });
    try {
      for (const ws of wss.clients) ws.close();
    } catch {}

    await new Promise<void>((resolve) => wss.close(() => resolve()));
    await new Promise<void>((resolve) => server.close(() => resolve()));
  };

  return { host, port: actualPort, close };
}

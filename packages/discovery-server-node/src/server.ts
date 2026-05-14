import http from 'node:http';

import type { ResolveDocRequest, ResolveDocResponse } from '@justthrowaway/discovery';

type Awaitable<T> = T | Promise<T>;

export type DiscoveryServerResolveContext = {
  req: http.IncomingMessage;
  url: URL;
};

export type DiscoveryServerResolveHandler = (
  request: ResolveDocRequest,
  ctx: DiscoveryServerResolveContext,
) => Awaitable<ResolveDocResponse>;

export type DiscoveryServerHealthResult =
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

export type DiscoveryServerOptions = {
  host?: string;
  port?: number;
  healthPath?: string;
  statusPath?: string;
  resolveDocPath?: string;
  publicHttpBaseUrl?: string;
  publicWebSocketBaseUrl?: string;
  cacheTtlMs?: number;
  resolveDoc?: DiscoveryServerResolveHandler;
  healthCheck?: () => Awaitable<DiscoveryServerHealthResult>;
  statusInfo?: () => Awaitable<Record<string, unknown>>;
  packageName?: string;
  packageVersion?: string;
  gitSha?: string;
  gitDirty?: boolean;
  startedAt?: string;
};

export type DiscoveryServerHandle = {
  host: string;
  port: number;
  close: () => Promise<void>;
};

function normalizeOptionalAbsoluteUrl(
  name: string,
  value: string | undefined,
  allowedProtocols: readonly string[],
): string | undefined {
  if (!value) return undefined;
  const trimmed = value.trim();
  if (trimmed.length === 0) return undefined;
  let url: URL;
  try {
    url = new URL(trimmed);
  } catch {
    throw new Error(`${name} must be a valid absolute URL`);
  }
  if (!allowedProtocols.includes(url.protocol)) {
    throw new Error(`${name} must use ${allowedProtocols.join(', ')}`);
  }
  return url.toString().replace(/\/$/, '');
}

function normalizePath(name: string, value: string | undefined, fallback: string): string {
  const trimmed = value?.trim() || fallback;
  if (!trimmed.startsWith('/')) throw new Error(`${name} must start with "/"`);
  return trimmed;
}

function firstForwardedHeader(value: string | string[] | undefined): string | undefined {
  if (Array.isArray(value)) value = value[0];
  if (!value) return undefined;
  const first = value.split(',')[0]?.trim();
  return first && first.length > 0 ? first : undefined;
}

function derivePublicBaseUrl(req: http.IncomingMessage, fallbackProtocol: 'http' | 'ws'): string {
  const host =
    firstForwardedHeader(req.headers['x-forwarded-host']) ?? req.headers.host ?? 'localhost';
  const forwardedProto = firstForwardedHeader(req.headers['x-forwarded-proto']);
  const protocol =
    forwardedProto && forwardedProto.length > 0
      ? fallbackProtocol === 'ws'
        ? forwardedProto === 'https'
          ? 'wss'
          : 'ws'
        : forwardedProto
      : fallbackProtocol;
  return `${protocol}://${host}`.replace(/\/$/, '');
}

function buildDefaultResolveDocHandler(opts: {
  publicHttpBaseUrl?: string;
  publicWebSocketBaseUrl?: string;
  cacheTtlMs: number;
}): DiscoveryServerResolveHandler {
  return async (request, ctx) => {
    const publicHttpBaseUrl = opts.publicHttpBaseUrl ?? derivePublicBaseUrl(ctx.req, 'http');
    const publicWebSocketBaseUrl =
      opts.publicWebSocketBaseUrl ?? derivePublicBaseUrl(ctx.req, 'ws');
    return {
      docId: request.docId,
      plan: {
        topology: 'relay',
        attachments: [
          {
            protocol: 'websocket',
            role: 'preferred',
            url: `${publicWebSocketBaseUrl}/sync`,
          },
          {
            protocol: 'https',
            role: 'bootstrap',
            url: publicHttpBaseUrl,
          },
        ],
        cacheTtlMs: opts.cacheTtlMs,
      },
    };
  };
}

export async function startDiscoveryServer(
  opts: DiscoveryServerOptions = {},
): Promise<DiscoveryServerHandle> {
  const host = opts.host ?? '0.0.0.0';
  const port = Number(opts.port ?? 8788);
  const healthPath = normalizePath('healthPath', opts.healthPath, '/health');
  const statusPath = normalizePath('statusPath', opts.statusPath, '/status');
  const resolveDocPath = normalizePath('resolveDocPath', opts.resolveDocPath, '/resolve-doc');
  const publicHttpBaseUrl = normalizeOptionalAbsoluteUrl(
    'publicHttpBaseUrl',
    opts.publicHttpBaseUrl,
    ['http:', 'https:'],
  );
  const publicWebSocketBaseUrl = normalizeOptionalAbsoluteUrl(
    'publicWebSocketBaseUrl',
    opts.publicWebSocketBaseUrl,
    ['ws:', 'wss:', 'http:', 'https:'],
  );
  const cacheTtlMs = opts.cacheTtlMs == null ? 60 * 60 * 1000 : Number(opts.cacheTtlMs);
  const packageName = opts.packageName?.trim() || '@justthrowaway/discovery-server-node';
  const packageVersion = opts.packageVersion?.trim() || undefined;
  const gitSha = opts.gitSha?.trim() || undefined;
  const gitDirty = Boolean(opts.gitDirty);
  const startedAt = opts.startedAt?.trim() || new Date().toISOString();
  const startedAtMs = Date.parse(startedAt);

  if (!Number.isFinite(port) || port < 0) throw new Error(`invalid port: ${opts.port}`);
  if (!Number.isFinite(cacheTtlMs) || cacheTtlMs < 0) {
    throw new Error(`invalid cacheTtlMs: ${opts.cacheTtlMs}`);
  }

  const resolveDoc =
    opts.resolveDoc ??
    buildDefaultResolveDocHandler({
      publicHttpBaseUrl,
      publicWebSocketBaseUrl,
      cacheTtlMs,
    });
  const discoveryCorsHeaders = {
    'access-control-allow-origin': '*',
    'access-control-allow-methods': 'GET,OPTIONS',
    'access-control-allow-headers': 'content-type,authorization',
  };

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
          res.writeHead(statusCode, { 'content-type': result.contentType ?? 'text/plain' });
          res.end(result.body ?? 'not ready');
        } catch {
          res.writeHead(503, { 'content-type': 'text/plain' });
          res.end('not ready');
        }
      })();
      return;
    }

    if (url.pathname === statusPath) {
      void (async () => {
        try {
          const status = {
            ok: true,
            service: packageName,
            version: packageVersion ?? null,
            gitSha: gitSha ?? null,
            gitDirty,
            buildRef: gitSha ? `${gitSha}${gitDirty ? '-dirty' : ''}` : null,
            startedAt,
            uptimeMs: Number.isFinite(startedAtMs) ? Math.max(0, Date.now() - startedAtMs) : null,
            resolveDocPath,
            publicHttpBaseUrl: publicHttpBaseUrl ?? null,
            publicWebSocketBaseUrl: publicWebSocketBaseUrl ?? null,
            cacheTtlMs,
            ...((await opts.statusInfo?.()) ?? {}),
          };
          res.writeHead(200, { 'content-type': 'application/json; charset=utf-8' });
          res.end(JSON.stringify(status));
        } catch {
          res.writeHead(500, { 'content-type': 'application/json; charset=utf-8' });
          res.end(JSON.stringify({ ok: false, error: 'status unavailable' }));
        }
      })();
      return;
    }

    if (url.pathname === resolveDocPath) {
      void (async () => {
        try {
          if (req.method === 'OPTIONS') {
            res.writeHead(204, discoveryCorsHeaders);
            res.end();
            return;
          }
          if (req.method && req.method !== 'GET') {
            res.writeHead(405, {
              'content-type': 'application/json; charset=utf-8',
              ...discoveryCorsHeaders,
            });
            res.end(JSON.stringify({ ok: false, error: 'method not allowed' }));
            return;
          }
          const docId = url.searchParams.get('docId')?.trim();
          if (!docId) {
            res.writeHead(400, {
              'content-type': 'application/json; charset=utf-8',
              ...discoveryCorsHeaders,
            });
            res.end(JSON.stringify({ ok: false, error: 'missing docId' }));
            return;
          }
          const response = await resolveDoc({ docId }, { req, url });
          res.writeHead(200, {
            'content-type': 'application/json; charset=utf-8',
            ...discoveryCorsHeaders,
          });
          res.end(JSON.stringify(response));
        } catch {
          res.writeHead(500, {
            'content-type': 'application/json; charset=utf-8',
            ...discoveryCorsHeaders,
          });
          res.end(JSON.stringify({ ok: false, error: 'resolve failed' }));
        }
      })();
      return;
    }

    res.writeHead(404, { 'content-type': 'text/plain' });
    res.end('not found');
  });

  await new Promise<void>((resolve, reject) => {
    const onError = (error: Error) => {
      server.off('listening', onListening);
      reject(error);
    };
    const onListening = () => {
      server.off('error', onError);
      resolve();
    };
    server.once('error', onError);
    server.once('listening', onListening);
    server.listen(port, host);
  });

  const address = server.address();
  if (!address || typeof address === 'string') {
    throw new Error('failed to start discovery server');
  }

  return {
    host: address.address,
    port: address.port,
    close: async () => {
      await new Promise<void>((resolve, reject) => {
        server.close((err) => (err ? reject(err) : resolve()));
      });
    },
  };
}

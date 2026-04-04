import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { base64urlDecode } from '@treecrdt/auth';
import { installHelloTraceSink, type HelloTraceRecord } from '@treecrdt/sync';

import { startSyncServer } from './server.js';

const LOCAL_POSTGRES_URL_EXAMPLE = 'postgres://postgres:postgres@127.0.0.1:5432/postgres';

function parseBooleanEnv(name: string, fallback: boolean): boolean {
  const raw = process.env[name];
  if (!raw || raw.trim().length === 0) return fallback;
  const normalized = raw.trim().toLowerCase();
  if (['1', 'true', 'yes', 'on'].includes(normalized)) return true;
  if (['0', 'false', 'no', 'off'].includes(normalized)) return false;
  throw new Error(`${name} must be a boolean (true/false), got: ${raw}`);
}

function parseIssuerPublicKeysEnv(name: string): Uint8Array[] {
  const raw = process.env[name];
  if (!raw || raw.trim().length === 0) return [];
  return raw
    .split(',')
    .map((entry) => entry.trim())
    .filter((entry) => entry.length > 0)
    .map((entry) => {
      try {
        return base64urlDecode(entry);
      } catch (err) {
        throw new Error(
          `${name} has invalid base64url key "${entry}": ${err instanceof Error ? err.message : String(err)}`,
        );
      }
    });
}

function buildPostgresUrlFromParts(): string | undefined {
  const host = process.env.TREECRDT_POSTGRES_HOST?.trim();
  const db = process.env.TREECRDT_POSTGRES_DB?.trim();
  const user = process.env.TREECRDT_POSTGRES_USER?.trim();
  const password = process.env.TREECRDT_POSTGRES_PASSWORD ?? '';
  const portRaw = process.env.TREECRDT_POSTGRES_PORT?.trim();
  if (!host && !db && !user && !password && !portRaw) return undefined;

  if (!host || !db || !user) {
    throw new Error(
      'TREECRDT_POSTGRES_HOST, TREECRDT_POSTGRES_DB, and TREECRDT_POSTGRES_USER are required when TREECRDT_POSTGRES_URL is not set',
    );
  }
  const port = Number(portRaw ?? '5432');
  if (!Number.isFinite(port) || port <= 0) throw new Error('invalid TREECRDT_POSTGRES_PORT');

  const encodedUser = encodeURIComponent(user);
  const encodedPassword = encodeURIComponent(password);
  return `postgres://${encodedUser}:${encodedPassword}@${host}:${port}/${db}`;
}

function clientHostForBindHost(host: string): string {
  const trimmed = host.trim();
  if (trimmed === '0.0.0.0' || trimmed === '::' || trimmed === '[::]') {
    return 'localhost';
  }
  return trimmed;
}

function readPackageVersion(): string | undefined {
  try {
    const packageJsonUrl = new URL('../package.json', import.meta.url);
    const parsed = JSON.parse(fs.readFileSync(packageJsonUrl, 'utf8')) as { version?: unknown };
    return typeof parsed.version === 'string' && parsed.version.trim().length > 0
      ? parsed.version.trim()
      : undefined;
  } catch {
    return undefined;
  }
}

async function main() {
  const traceHelloToStdout = parseBooleanEnv('TREECRDT_SYNC_TRACE_HELLO', false);
  const disposeHelloTraceSink = traceHelloToStdout
    ? installHelloTraceSink((record: HelloTraceRecord) => {
        try {
          console.log(JSON.stringify(record));
        } catch {
          // hello tracing must never affect server behavior
        }
      })
    : undefined;

  const host = process.env.HOST ?? '0.0.0.0';
  const port = Number(process.env.PORT ?? '8787');
  const postgresUrl = process.env.TREECRDT_POSTGRES_URL?.trim() || buildPostgresUrlFromParts();
  const maxCodewords = Number(process.env.TREECRDT_SYNC_MAX_CODEWORDS ?? '0');
  const directSendThreshold = Number(process.env.TREECRDT_SYNC_DIRECT_SEND_THRESHOLD ?? '0');
  const fastForwardRelaySubscriptions = parseBooleanEnv(
    'TREECRDT_SYNC_FAST_FORWARD_RELAY_SUBSCRIPTIONS',
    false,
  );
  const idleCloseMs = Number(process.env.TREECRDT_IDLE_CLOSE_MS ?? '30000');
  const maxPayloadBytes = Number(
    process.env.TREECRDT_MAX_PAYLOAD_BYTES ?? String(10 * 1024 * 1024),
  );
  const authToken = process.env.TREECRDT_SYNC_AUTH_TOKEN?.trim() || undefined;
  const authCapabilityIssuerPublicKeys = parseIssuerPublicKeysEnv(
    'TREECRDT_SYNC_CWT_ISSUER_PUBKEYS',
  );
  const docIdPattern = process.env.TREECRDT_DOC_ID_PATTERN?.trim() || undefined;
  const allowDocCreate = parseBooleanEnv('TREECRDT_ALLOW_DOC_CREATE', true);
  const enablePgNotify = parseBooleanEnv('TREECRDT_PG_NOTIFY_ENABLED', true);
  const pgNotifyChannel =
    process.env.TREECRDT_PG_NOTIFY_CHANNEL?.trim() || 'treecrdt_sync_doc_updates';
  const rateLimitMaxUpgrades = Number(process.env.TREECRDT_RATE_LIMIT_MAX_UPGRADES ?? '0');
  const rateLimitWindowMs = Number(process.env.TREECRDT_RATE_LIMIT_WINDOW_MS ?? '60000');
  const packageVersion = readPackageVersion();
  const gitSha = process.env.TREECRDT_SYNC_GIT_SHA?.trim() || undefined;
  const gitDirty = parseBooleanEnv('TREECRDT_SYNC_GIT_DIRTY', false);
  const discoveryResolvePath = process.env.TREECRDT_DISCOVERY_RESOLVE_PATH?.trim() || undefined;
  const discoveryPublicHttpBaseUrl =
    process.env.TREECRDT_DISCOVERY_PUBLIC_HTTP_BASE_URL?.trim() || undefined;
  const discoveryPublicWebSocketBaseUrl =
    process.env.TREECRDT_DISCOVERY_PUBLIC_WS_BASE_URL?.trim() || undefined;
  const discoveryCacheTtlMs = Number(
    process.env.TREECRDT_DISCOVERY_CACHE_TTL_MS ?? String(60 * 60 * 1000),
  );
  const discoveryRouteVersion = process.env.TREECRDT_DISCOVERY_ROUTE_VERSION?.trim() || undefined;
  const startedAt = new Date().toISOString();

  const backendModule =
    process.env.TREECRDT_POSTGRES_BACKEND_MODULE ??
    path.resolve(
      path.dirname(fileURLToPath(import.meta.url)),
      '../../../../treecrdt-postgres-napi/dist/index.js',
    );

  if (!postgresUrl || postgresUrl.length === 0) {
    throw new Error(
      [
        'missing Postgres connection for sync-server:postgres',
        'set TREECRDT_POSTGRES_URL or TREECRDT_POSTGRES_HOST/PORT/DB/USER/PASSWORD',
        `local example: TREECRDT_POSTGRES_URL=${LOCAL_POSTGRES_URL_EXAMPLE} pnpm sync-server:postgres`,
        'or use: pnpm sync-server:postgres:local',
      ].join('\n'),
    );
  }
  if (!Number.isFinite(port) || port <= 0) throw new Error(`invalid PORT: ${process.env.PORT}`);
  if (!Number.isFinite(maxCodewords) || maxCodewords < 0)
    throw new Error('invalid TREECRDT_SYNC_MAX_CODEWORDS');
  if (!Number.isFinite(directSendThreshold) || directSendThreshold < 0) {
    throw new Error('invalid TREECRDT_SYNC_DIRECT_SEND_THRESHOLD');
  }
  if (!Number.isFinite(idleCloseMs) || idleCloseMs < 0)
    throw new Error('invalid TREECRDT_IDLE_CLOSE_MS');
  if (!Number.isFinite(maxPayloadBytes) || maxPayloadBytes <= 0) {
    throw new Error('invalid TREECRDT_MAX_PAYLOAD_BYTES');
  }
  if (!Number.isFinite(rateLimitMaxUpgrades) || rateLimitMaxUpgrades < 0) {
    throw new Error('invalid TREECRDT_RATE_LIMIT_MAX_UPGRADES');
  }
  if (!Number.isFinite(rateLimitWindowMs) || rateLimitWindowMs <= 0) {
    throw new Error('invalid TREECRDT_RATE_LIMIT_WINDOW_MS');
  }
  if (!Number.isFinite(discoveryCacheTtlMs) || discoveryCacheTtlMs < 0) {
    throw new Error('invalid TREECRDT_DISCOVERY_CACHE_TTL_MS');
  }

  try {
    const handle = await startSyncServer({
      host,
      port,
      postgresUrl,
      maxCodewords: maxCodewords > 0 ? maxCodewords : undefined,
      directSendThreshold: directSendThreshold > 0 ? directSendThreshold : undefined,
      fastForwardRelaySubscriptions,
      idleCloseMs,
      maxPayloadBytes,
      backendModule,
      authToken,
      authCapabilityIssuerPublicKeys,
      docIdPattern,
      allowDocCreate,
      enablePgNotify,
      pgNotifyChannel,
      rateLimitMaxUpgrades,
      rateLimitWindowMs,
      packageVersion,
      gitSha,
      gitDirty,
      discoveryResolvePath,
      discoveryPublicHttpBaseUrl,
      discoveryPublicWebSocketBaseUrl,
      discoveryCacheTtlMs,
      discoveryRouteVersion,
      startedAt,
    });
    const clientHost = clientHostForBindHost(handle.host);
    console.log(`TreeCRDT sync server listening on ${handle.host}:${handle.port}`);
    console.log(`- bind: http://${handle.host}:${handle.port}`);
    console.log(`- health: http://${clientHost}:${handle.port}/health`);
    console.log(`- status: http://${clientHost}:${handle.port}/status`);
    console.log(
      `- resolve: http://${clientHost}:${handle.port}${discoveryResolvePath ?? '/resolve-doc'}?docId=YOUR_DOC_ID`,
    );
    console.log(`- ws: ws://${clientHost}:${handle.port}`);
    console.log(`- sync endpoint: ws://${clientHost}:${handle.port}/sync?docId=YOUR_DOC_ID`);
    console.log(`- backend module: ${handle.backendModule}`);
    if (packageVersion) console.log(`- version: ${packageVersion}`);
    if (gitSha) console.log(`- git sha: ${gitSha}${gitDirty ? ' (dirty)' : ''}`);
    if (fastForwardRelaySubscriptions)
      console.log('- relay: provisional subscription fast-forward enabled');
    if (authCapabilityIssuerPublicKeys.length > 0) {
      console.log(
        `- auth: capability CWT enabled (${authCapabilityIssuerPublicKeys.length} issuer keys)`,
      );
    } else if (authToken) {
      console.log('- auth: static token enabled (bearer token or ?token=...)');
    }
    if (docIdPattern) console.log(`- docId policy: ${docIdPattern}`);
    if (!allowDocCreate) console.log('- doc creation policy: deny unknown docId');
    if (enablePgNotify) console.log(`- pg notify: enabled on channel ${pgNotifyChannel}`);
    if (rateLimitMaxUpgrades > 0) {
      console.log(
        `- rate limit: ${rateLimitMaxUpgrades} upgrades per ${rateLimitWindowMs}ms per IP`,
      );
    }
  } finally {
    disposeHelloTraceSink?.();
  }
  if (discoveryPublicHttpBaseUrl || discoveryPublicWebSocketBaseUrl) {
    console.log(
      `- discovery public base: ${discoveryPublicHttpBaseUrl ?? '(derived from request)'} / ${
        discoveryPublicWebSocketBaseUrl ?? '(derived from request)'
      }`,
    );
  }
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});

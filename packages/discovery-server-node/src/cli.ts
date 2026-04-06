import fs from 'node:fs';

import { startDiscoveryServer } from './server.js';

function clientHostForBindHost(host: string): string {
  const trimmed = host.trim();
  if (trimmed === '0.0.0.0' || trimmed === '::' || trimmed === '[::]') {
    return 'localhost';
  }
  return trimmed;
}

function parseBooleanEnv(name: string, fallback: boolean): boolean {
  const raw = process.env[name];
  if (!raw || raw.trim().length === 0) return fallback;
  const normalized = raw.trim().toLowerCase();
  if (['1', 'true', 'yes', 'on'].includes(normalized)) return true;
  if (['0', 'false', 'no', 'off'].includes(normalized)) return false;
  throw new Error(`${name} must be a boolean (true/false), got: ${raw}`);
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
  const host = process.env.HOST ?? '0.0.0.0';
  const port = Number(process.env.PORT ?? '8788');
  const resolveDocPath = process.env.TREECRDT_DISCOVERY_RESOLVE_PATH?.trim() || undefined;
  const publicHttpBaseUrl =
    process.env.TREECRDT_DISCOVERY_PUBLIC_HTTP_BASE_URL?.trim() || undefined;
  const publicWebSocketBaseUrl =
    process.env.TREECRDT_DISCOVERY_PUBLIC_WS_BASE_URL?.trim() || undefined;
  const cacheTtlMs = Number(process.env.TREECRDT_DISCOVERY_CACHE_TTL_MS ?? String(60 * 60 * 1000));
  const packageVersion = readPackageVersion();
  const gitSha = process.env.TREECRDT_DISCOVERY_GIT_SHA?.trim() || undefined;
  const gitDirty = parseBooleanEnv('TREECRDT_DISCOVERY_GIT_DIRTY', false);
  const startedAt = new Date().toISOString();

  if (!Number.isFinite(port) || port <= 0) throw new Error(`invalid PORT: ${process.env.PORT}`);
  if (!Number.isFinite(cacheTtlMs) || cacheTtlMs < 0) {
    throw new Error('invalid TREECRDT_DISCOVERY_CACHE_TTL_MS');
  }

  const handle = await startDiscoveryServer({
    host,
    port,
    resolveDocPath,
    publicHttpBaseUrl,
    publicWebSocketBaseUrl,
    cacheTtlMs,
    packageVersion,
    gitSha,
    gitDirty,
    startedAt,
  });
  const clientHost = clientHostForBindHost(handle.host);
  console.log(`TreeCRDT discovery server listening on ${handle.host}:${handle.port}`);
  console.log(`- bind: http://${handle.host}:${handle.port}`);
  console.log(`- health: http://${clientHost}:${handle.port}/health`);
  console.log(`- status: http://${clientHost}:${handle.port}/status`);
  console.log(
    `- resolve: http://${clientHost}:${handle.port}${resolveDocPath ?? '/resolve-doc'}?docId=YOUR_DOC_ID`,
  );
  if (publicHttpBaseUrl || publicWebSocketBaseUrl) {
    console.log(
      `- advertised endpoints: ${publicHttpBaseUrl ?? '(derived from request)'} / ${
        publicWebSocketBaseUrl ?? '(derived from request)'
      }`,
    );
  }
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});

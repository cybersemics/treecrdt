import fs from 'node:fs/promises';
import http from 'node:http';
import https from 'node:https';
import path from 'node:path';
import process from 'node:process';
import { createRequire } from 'node:module';
import { fileURLToPath } from 'node:url';
import { performance } from 'node:perf_hooks';
import {
  medianOrNull,
  percentileNearestRankOrNull,
} from '../packages/treecrdt-benchmark/dist/stats.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(__dirname, '..');
const require = createRequire(import.meta.url);
const WebSocketImpl =
  typeof globalThis.WebSocket === 'function' ? globalThis.WebSocket : require('ws');

function usage() {
  console.log(`Usage:
  node scripts/bench-discovery-connect.mjs [options]

Options:
  --bootstrap-url=https://sync.emhub.net   Discovery/bootstrap base URL (default: env TREECRDT_DISCOVERY_URL)
  --doc-id=bootstrap-probe                 Known doc id to resolve/connect (default: bootstrap-probe)
  --host-map=host=ip[,host=ip...]          Optional hostname override(s) for resolve/connect
  --iterations=N                           Measured samples (default: 5)
  --warmup=N                               Warmup samples (default: 1)
  --out=path.json                          Optional output path under benchmarks/
`);
}

function getArg(name) {
  const prefix = `--${name}=`;
  const found = process.argv.find((arg) => arg.startsWith(prefix));
  return found ? found.slice(prefix.length) : null;
}

function parseIntArg(name, defaultValue) {
  const raw = getArg(name);
  if (raw == null || raw.length === 0) return defaultValue;
  const parsed = Number.parseInt(raw, 10);
  if (!Number.isFinite(parsed) || parsed < 0) throw new Error(`invalid --${name}: ${raw}`);
  return parsed;
}

function parseHostMap(raw) {
  if (!raw || raw.trim().length === 0) return new Map();
  const entries = raw
    .split(',')
    .map((entry) => entry.trim())
    .filter(Boolean)
    .map((entry) => {
      const [host, ip] = entry.split('=', 2).map((part) => part?.trim() ?? '');
      if (!host || !ip) throw new Error(`invalid host map entry: ${entry}`);
      return [host.toLowerCase(), ip];
    });
  return new Map(entries);
}

function normalizeBootstrapUrl(raw) {
  if (!raw || raw.trim().length === 0) throw new Error('missing bootstrap url');
  let input = raw.trim();
  if (!/^[a-zA-Z][a-zA-Z0-9+.-]*:/.test(input)) input = `https://${input}`;
  const url = new URL(input);
  if (url.protocol !== 'http:' && url.protocol !== 'https:') {
    throw new Error('bootstrap url must use http:// or https://');
  }
  url.pathname = '/';
  url.search = '';
  url.hash = '';
  return url;
}

function normalizeDocId(raw) {
  const value = raw?.trim() ?? '';
  return value.length > 0 ? value : 'bootstrap-probe';
}

function attachDocId(rawUrl, docId) {
  const url = new URL(rawUrl);
  if (url.protocol === 'http:') url.protocol = 'ws:';
  if (url.protocol === 'https:') url.protocol = 'wss:';
  url.searchParams.set('docId', docId);
  return url;
}

function createLookup(hostMap) {
  return (hostname, options, callback) => {
    const cb = typeof options === 'function' ? options : callback;
    const opts = typeof options === 'function' ? {} : (options ?? {});
    if (typeof cb !== 'function') {
      throw new Error('lookup callback missing');
    }
    const mapped = hostMap.get(String(hostname).toLowerCase());
    if (mapped) {
      const family = mapped.includes(':') ? 6 : 4;
      if (opts?.all) {
        cb(null, [{ address: mapped, family }]);
      } else {
        cb(null, mapped, family);
      }
      return;
    }
    cb(new Error(`no host-map entry for ${hostname}`));
  };
}

async function requestJson(url, hostMap) {
  const startedAt = performance.now();
  if (hostMap.size === 0) {
    const res = await fetch(url);
    if (!res.ok) {
      throw new Error(`resolve-doc failed (${res.status} ${res.statusText})`);
    }
    return {
      durationMs: performance.now() - startedAt,
      json: await res.json(),
    };
  }

  const transport = url.protocol === 'https:' ? https : http;
  const hostname = url.hostname.toLowerCase();
  const mappedIp = hostMap.get(hostname);
  if (!mappedIp) throw new Error(`missing host-map entry for ${hostname}`);

  const json = await new Promise((resolve, reject) => {
    const req = transport.request(
      {
        protocol: url.protocol,
        hostname: url.hostname,
        port: url.port || (url.protocol === 'https:' ? 443 : 80),
        path: `${url.pathname}${url.search}`,
        method: 'GET',
        lookup: createLookup(hostMap),
        servername: url.hostname,
        headers: {
          accept: '*/*',
          'user-agent': 'treecrdt-bench-discovery/1.0',
        },
      },
      (res) => {
        if ((res.statusCode ?? 500) < 200 || (res.statusCode ?? 500) >= 300) {
          reject(new Error(`resolve-doc failed (${res.statusCode ?? 500})`));
          res.resume();
          return;
        }
        let body = '';
        res.setEncoding('utf8');
        res.on('data', (chunk) => {
          body += chunk;
        });
        res.on('end', () => {
          try {
            resolve(JSON.parse(body));
          } catch (error) {
            reject(error);
          }
        });
      },
    );
    req.on('error', reject);
    req.end();
  });

  return {
    durationMs: performance.now() - startedAt,
    json,
  };
}

async function connectWebSocket(url, hostMap) {
  const startedAt = performance.now();
  const lookup = hostMap.size > 0 ? createLookup(hostMap) : undefined;
  const WsCtor = lookup ? require('ws') : WebSocketImpl;
  const ws = lookup
    ? new WsCtor(url.toString(), {
        lookup,
        headers: { Host: url.host },
        servername: url.hostname,
      })
    : new WsCtor(url.toString());
  await new Promise((resolve, reject) => {
    if (typeof ws.once === 'function') {
      ws.once('open', resolve);
      ws.once('error', reject);
      return;
    }
    const cleanup = () => {
      ws.removeEventListener?.('open', onOpen);
      ws.removeEventListener?.('error', onError);
    };
    const onOpen = () => {
      cleanup();
      resolve();
    };
    const onError = (event) => {
      cleanup();
      reject(event?.error ?? new Error('websocket error'));
    };
    ws.addEventListener?.('open', onOpen);
    ws.addEventListener?.('error', onError);
  });
  const durationMs = performance.now() - startedAt;
  ws.close();
  await new Promise((resolve) => {
    if (typeof ws.once === 'function') {
      ws.once('close', resolve);
      return;
    }
    const onClose = () => {
      ws.removeEventListener?.('close', onClose);
      resolve();
    };
    ws.addEventListener?.('close', onClose);
  });
  return durationMs;
}

async function resolveDoc(bootstrapUrl, docId, hostMap) {
  const url = new URL('/resolve-doc', bootstrapUrl);
  url.searchParams.set('docId', docId);
  const { durationMs, json } = await requestJson(url, hostMap);
  const attachment =
    json?.plan?.attachments?.find?.(
      (entry) => entry.protocol === 'websocket' && entry.role === 'preferred',
    ) ?? json?.plan?.attachments?.find?.((entry) => entry.protocol === 'websocket');
  if (!attachment?.url) {
    throw new Error('resolve-doc response did not include a websocket attachment');
  }
  return { durationMs, response: json, attachmentUrl: attachment.url };
}

async function main() {
  if (process.argv.includes('--help')) {
    usage();
    return;
  }

  const bootstrapUrl = normalizeBootstrapUrl(
    getArg('bootstrap-url') ?? process.env.TREECRDT_DISCOVERY_URL ?? '',
  );
  const docId = normalizeDocId(getArg('doc-id') ?? process.env.TREECRDT_DISCOVERY_DOC_ID ?? '');
  const hostMap = parseHostMap(getArg('host-map') ?? process.env.TREECRDT_BENCH_HOST_MAP ?? '');
  const iterations = parseIntArg('iterations', 5);
  const warmup = parseIntArg('warmup', 1);
  const totalRuns = warmup + iterations;
  const resolveSamplesMs = [];
  const connectSamplesMs = [];
  const totalSamplesMs = [];
  const cachedConnectSamplesMs = [];

  for (let i = 0; i < totalRuns; i += 1) {
    const resolved = await resolveDoc(bootstrapUrl, docId, hostMap);
    const connectUrl = attachDocId(resolved.attachmentUrl, docId);
    const connectMs = await connectWebSocket(connectUrl, hostMap);
    const cachedConnectMs = await connectWebSocket(connectUrl, hostMap);

    if (i >= warmup) {
      resolveSamplesMs.push(resolved.durationMs);
      connectSamplesMs.push(connectMs);
      totalSamplesMs.push(resolved.durationMs + connectMs);
      cachedConnectSamplesMs.push(cachedConnectMs);
    }
  }

  const result = {
    benchmark: 'discovery-bootstrap-connect',
    bootstrapUrl: bootstrapUrl.toString().replace(/\/$/, ''),
    docId,
    hostMap: Object.fromEntries(hostMap),
    iterations,
    warmup,
    resolveSamplesMs,
    connectSamplesMs,
    totalSamplesMs,
    cachedConnectSamplesMs,
    resolveMedianMs: medianOrNull(resolveSamplesMs),
    connectMedianMs: medianOrNull(connectSamplesMs),
    totalMedianMs: medianOrNull(totalSamplesMs),
    cachedConnectMedianMs: medianOrNull(cachedConnectSamplesMs),
    totalP95Ms: percentileNearestRankOrNull(totalSamplesMs, 95),
    cachedConnectP95Ms: percentileNearestRankOrNull(cachedConnectSamplesMs, 95),
    generatedAt: new Date().toISOString(),
  };

  const outPath =
    getArg('out') ??
    path.join(
      repoRoot,
      'benchmarks',
      'discovery-connect',
      `discovery-connect-${bootstrapUrl.host.replace(/[^a-z0-9.-]+/gi, '_')}.json`,
    );

  await fs.mkdir(path.dirname(outPath), { recursive: true });
  await fs.writeFile(outPath, `${JSON.stringify(result, null, 2)}\n`, 'utf8');
  console.log(JSON.stringify(result, null, 2));
  console.log(`wrote ${outPath}`);
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});

import fs from "node:fs/promises";
import path from "node:path";
import process from "node:process";
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";
import { performance } from "node:perf_hooks";

import {
  createTreecrdtClient,
  loadTreecrdtExtension,
} from "../packages/treecrdt-sqlite-node/dist/index.js";
import { createTreecrdtSyncBackendFromClient } from "../packages/sync/material/sqlite/dist/index.js";
import {
  SyncPeer,
  deriveOpRefV0,
  SERVER_INSTANCE_CAPABILITY_NAME,
} from "../packages/sync/protocol/dist/index.js";
import { createBrowserWebSocketTransport } from "../packages/sync/protocol/dist/browser.js";
import { treecrdtSyncV0ProtobufCodec } from "../packages/sync/protocol/dist/protobuf.js";
import { wrapDuplexTransportWithCodec } from "../packages/sync/protocol/dist/transport.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(__dirname, "..");
const sqliteNodeRequire = createRequire(path.join(repoRoot, "packages", "treecrdt-sqlite-node", "package.json"));
const BetterSqlite3 = sqliteNodeRequire("better-sqlite3");
const ROOT_ID = "00000000000000000000000000000000";
const ROOT_BYTES = hexToBytes(ROOT_ID);
const CODEWORDS_PER_MESSAGE = 512;
const MAX_CODEWORDS = 200_000;
const MAX_OPS_PER_BATCH = 500;
const ROUTE_WAIT_TIMEOUT_MS = 10_000;

function hexToBytes(hex) {
  if (typeof hex !== "string" || hex.length % 2 !== 0) {
    throw new Error(`invalid hex string: ${hex}`);
  }
  const out = new Uint8Array(hex.length / 2);
  for (let i = 0; i < out.length; i += 1) {
    const byte = Number.parseInt(hex.slice(i * 2, i * 2 + 2), 16);
    if (!Number.isFinite(byte)) throw new Error(`invalid hex string: ${hex}`);
    out[i] = byte;
  }
  return out;
}

function usage() {
  console.log(`Usage:
  node scripts/bench-sync-route-fanout.mjs [options]

Options:
  --sync-server-url=ws://host/sync     Sync server websocket URL (required; default: env TREECRDT_SYNC_SERVER_URL)
  --source-sync-server-url=ws://host/sync  Optional source/writer websocket URL override
  --target-sync-server-url=ws://host/sync  Optional target/subscriber websocket URL override
  --mode=all|children                  Filter to subscribe on the target (default: all)
  --route=any|same|cross               Desired routing classification per sample (default: any)
  --iterations=N                       Measured samples (default: 5)
  --warmup=N                           Warmup samples (default: 1)
  --max-route-attempts=N               Retries per sample to hit desired route (default: 20)
  --out=path.json                      Optional output path
`);
}

function getArg(name) {
  const prefix = `--${name}=`;
  const found = process.argv.find((arg) => arg.startsWith(prefix));
  return found ? found.slice(prefix.length) : null;
}

function hasFlag(name) {
  return process.argv.includes(`--${name}`);
}

function parseIntArg(name, defaultValue) {
  const raw = getArg(name);
  if (raw == null || raw.length === 0) return defaultValue;
  const parsed = Number.parseInt(raw, 10);
  if (!Number.isFinite(parsed) || parsed < 0) throw new Error(`invalid --${name}: ${raw}`);
  return parsed;
}

function normalizeSyncServerUrl(raw, docId) {
  let input = raw.trim();
  if (input.length === 0) throw new Error("sync server URL is empty");
  if (!/^[a-zA-Z][a-zA-Z0-9+.-]*:/.test(input)) input = `ws://${input}`;
  const url = new URL(input);
  if (url.protocol === "http:") url.protocol = "ws:";
  if (url.protocol === "https:") url.protocol = "wss:";
  if (url.protocol !== "ws:" && url.protocol !== "wss:") {
    throw new Error("sync server URL must use ws://, wss://, http://, or https://");
  }
  if (url.pathname === "/" || url.pathname.length === 0) url.pathname = "/sync";
  url.searchParams.set("docId", docId);
  return url;
}

function percentile(sorted, p) {
  if (sorted.length === 0) return null;
  const index = Math.min(sorted.length - 1, Math.max(0, Math.ceil((p / 100) * sorted.length) - 1));
  return sorted[index];
}

function summarize(values) {
  if (values.length === 0) {
    return { count: 0, minMs: null, medianMs: null, p95Ms: null, maxMs: null, meanMs: null };
  }
  const sorted = [...values].sort((a, b) => a - b);
  const mean = sorted.reduce((sum, value) => sum + value, 0) / sorted.length;
  return {
    count: sorted.length,
    minMs: sorted[0],
    medianMs: percentile(sorted, 50),
    p95Ms: percentile(sorted, 95),
    maxMs: sorted[sorted.length - 1],
    meanMs: mean,
  };
}

function routeLabel(sourceInstanceId, targetInstanceId) {
  if (sourceInstanceId && targetInstanceId) {
    return sourceInstanceId === targetInstanceId ? "same" : "cross";
  }
  return "unknown";
}

function safeEndpointLabel(rawUrl) {
  const normalized = rawUrl.trim().length === 0 ? "unknown" : normalizeSyncServerUrl(rawUrl, "bench").host;
  return normalized.replace(/[^a-z0-9.-]+/gi, "_");
}

function defaultOutPath({ sourceSyncServerUrl, targetSyncServerUrl, mode, route }) {
  const safeHost = `${safeEndpointLabel(sourceSyncServerUrl)}--${safeEndpointLabel(targetSyncServerUrl)}`;
  return path.join(
    repoRoot,
    "benchmarks",
    "sync-route-fanout",
    `sync-route-fanout-${safeHost}-${mode}-${route}.json`
  );
}

function replicaFromLabel(label) {
  const encoded = new TextEncoder().encode(label);
  if (encoded.length === 0) throw new Error("label must not be empty");
  const out = new Uint8Array(32);
  for (let i = 0; i < out.length; i += 1) out[i] = encoded[i % encoded.length];
  return out;
}

function nodeIdFromInt(n) {
  if (!Number.isInteger(n) || n < 0) throw new Error(`invalid node id int: ${n}`);
  return n.toString(16).padStart(32, "0");
}

async function closeBuiltinWebSocket(ws) {
  if (!ws) return;
  if (ws.readyState === globalThis.WebSocket.CLOSED) return;
  await new Promise((resolve) => {
    let done = false;
    const finish = () => {
      if (done) return;
      done = true;
      resolve();
    };
    ws.addEventListener("close", finish, { once: true });
    setTimeout(finish, 2_000);
    try {
      if (ws.readyState !== globalThis.WebSocket.CLOSING) ws.close();
    } catch {
      finish();
    }
  });
}

async function openConnection(baseUrl, docId) {
  const url = normalizeSyncServerUrl(baseUrl, docId);
  const connectStartedAt = performance.now();
  const ws = await new Promise((resolve, reject) => {
    const WebSocketCtor = globalThis.WebSocket;
    if (typeof WebSocketCtor !== "function") {
      reject(new Error("global WebSocket is not available in this Node runtime"));
      return;
    }
    const next = new WebSocketCtor(url.toString());
    const cleanup = () => {
      next.removeEventListener("open", onOpen);
      next.removeEventListener("error", onError);
    };
    const onOpen = () => {
      cleanup();
      resolve(next);
    };
    const onError = () => {
      cleanup();
      reject(new Error(`failed to open websocket ${url.host}`));
    };
    next.addEventListener("open", onOpen, { once: true });
    next.addEventListener("error", onError, { once: true });
  });
  const connectMs = performance.now() - connectStartedAt;
  const wire = createBrowserWebSocketTransport(ws);
  const transport = wrapDuplexTransportWithCodec(wire, treecrdtSyncV0ProtobufCodec);
  return {
    transport,
    connectMs,
    close: async () => {
      await closeBuiltinWebSocket(ws);
    },
  };
}

async function createMemoryClient(docId) {
  const db = new BetterSqlite3(":memory:");
  loadTreecrdtExtension(db);
  const client = await createTreecrdtClient(db, { docId });
  const backend = createTreecrdtSyncBackendFromClient(client, docId, {
    maxLamport: async () => BigInt(await client.meta.headLamport()),
  });
  return { db, client, backend };
}

function syncFilter(mode) {
  return mode === "children" ? { children: { parent: ROOT_BYTES } } : { all: {} };
}

async function waitForRootChildren(client, expectedCount, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  let lastCount = -1;
  while (Date.now() < deadline) {
    const children = await client.tree.children(ROOT_ID);
    lastCount = children.length;
    if (lastCount === expectedCount) return;
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  throw new Error(`timed out waiting for ${expectedCount} root children (last=${lastCount})`);
}

async function setupRouteSample(sourceSyncServerUrl, targetSyncServerUrl, mode, sampleLabel) {
  const docId = `route-fanout-${mode}-${sampleLabel}-${Date.now()}-${Math.random().toString(16).slice(2, 8)}`;
  const source = await createMemoryClient(docId);
  const target = await createMemoryClient(docId);
  const sourcePeer = new SyncPeer(source.backend, {
    maxCodewords: MAX_CODEWORDS,
    maxOpsPerBatch: MAX_OPS_PER_BATCH,
    deriveOpRef: (op) => deriveOpRefV0(docId, { replica: op.meta.id.replica, counter: op.meta.id.counter }),
  });
  const targetPeer = new SyncPeer(target.backend, {
    maxCodewords: MAX_CODEWORDS,
    maxOpsPerBatch: MAX_OPS_PER_BATCH,
    deriveOpRef: (op) => deriveOpRefV0(docId, { replica: op.meta.id.replica, counter: op.meta.id.counter }),
  });

  const sourceConn = await openConnection(sourceSyncServerUrl, docId);
  const targetConn = await openConnection(targetSyncServerUrl, docId);
  const detachSource = sourcePeer.attach(sourceConn.transport);
  const detachTarget = targetPeer.attach(targetConn.transport);
  const filter = syncFilter(mode);

  let targetSub;
  try {
    await sourcePeer.syncOnce(sourceConn.transport, filter, {
      maxCodewords: MAX_CODEWORDS,
      codewordsPerMessage: CODEWORDS_PER_MESSAGE,
      maxOpsPerBatch: MAX_OPS_PER_BATCH,
    });

    targetSub = targetPeer.subscribe(targetConn.transport, filter, {
      immediate: true,
      intervalMs: 0,
      maxCodewords: MAX_CODEWORDS,
      codewordsPerMessage: CODEWORDS_PER_MESSAGE,
      maxOpsPerBatch: MAX_OPS_PER_BATCH,
    });
    await targetSub.ready;

    const sourceInstanceId =
      sourcePeer.getPeerCapabilityValue(sourceConn.transport, SERVER_INSTANCE_CAPABILITY_NAME) ?? null;
    const targetInstanceId =
      targetPeer.getPeerCapabilityValue(targetConn.transport, SERVER_INSTANCE_CAPABILITY_NAME) ?? null;

    return {
      docId,
      source,
      target,
      sourcePeer,
      targetPeer,
      sourceConn,
      targetConn,
      targetSub,
      detachSource,
      detachTarget,
      sourceInstanceId,
      targetInstanceId,
      classification: routeLabel(sourceInstanceId, targetInstanceId),
      connectMs: {
        source: sourceConn.connectMs,
        target: targetConn.connectMs,
      },
    };
  } catch (error) {
    try {
      targetSub?.stop();
    } catch {}
    detachSource();
    detachTarget();
    await Promise.allSettled([
      sourceConn.close(),
      targetConn.close(),
      source.client.close(),
      target.client.close(),
    ]);
    throw error;
  }
}

async function cleanupRouteSample(sample) {
  try {
    sample.targetSub?.stop();
  } catch {}
  try {
    sample.detachSource();
  } catch {}
  try {
    sample.detachTarget();
  } catch {}
  await Promise.allSettled([
    sample.sourceConn.close(),
    sample.targetConn.close(),
    sample.source.client.close(),
    sample.target.client.close(),
  ]);
}

async function main() {
  if (hasFlag("help")) {
    usage();
    return;
  }

  const syncServerUrl = getArg("sync-server-url") ?? process.env.TREECRDT_SYNC_SERVER_URL ?? "";
  const sourceSyncServerUrl = getArg("source-sync-server-url") ?? syncServerUrl;
  const targetSyncServerUrl = getArg("target-sync-server-url") ?? syncServerUrl;
  if (!sourceSyncServerUrl) {
    throw new Error("missing --source-sync-server-url, --sync-server-url, or TREECRDT_SYNC_SERVER_URL");
  }
  if (!targetSyncServerUrl) {
    throw new Error("missing --target-sync-server-url, --sync-server-url, or TREECRDT_SYNC_SERVER_URL");
  }
  const mode = getArg("mode") ?? "all";
  const route = getArg("route") ?? "any";
  const iterations = parseIntArg("iterations", 5);
  const warmup = parseIntArg("warmup", 1);
  const maxRouteAttempts = parseIntArg("max-route-attempts", 20);
  const outPath = getArg("out") ?? defaultOutPath({ sourceSyncServerUrl, targetSyncServerUrl, mode, route });

  if (!["all", "children"].includes(mode)) throw new Error(`--mode must be all or children, got ${mode}`);
  if (!["any", "same", "cross"].includes(route)) throw new Error(`--route must be any, same, or cross, got ${route}`);
  if (iterations <= 0) throw new Error(`--iterations must be > 0, got ${iterations}`);
  if (maxRouteAttempts <= 0) throw new Error(`--max-route-attempts must be > 0, got ${maxRouteAttempts}`);

  const replica = replicaFromLabel("route-writer");
  const samples = [];
  const total = warmup + iterations;

  for (let i = 0; i < total; i += 1) {
    const warmupSample = i < warmup;
    let sampleResult = null;
    let lastRouteError = null;

    for (let attempt = 1; attempt <= maxRouteAttempts; attempt += 1) {
      const sample = await setupRouteSample(sourceSyncServerUrl, targetSyncServerUrl, mode, `${i}-${attempt}`);
      try {
        if (route !== "any" && sample.classification !== route) {
          lastRouteError = new Error(
            `wanted route=${route}, got ${sample.classification} (${sample.sourceInstanceId} -> ${sample.targetInstanceId})`
          );
          await cleanupRouteSample(sample);
          continue;
        }

        const beforeLamport = await sample.source.client.meta.headLamport();
        const nodeId = nodeIdFromInt(10_000 + i * 100 + attempt);
        await sample.source.client.local.insert(replica, ROOT_ID, nodeId, { type: "last" }, null);
        const mintedOps = await sample.source.client.ops.since(beforeLamport);
        const pushStartedAt = performance.now();
        const pushPromise = sample.sourcePeer.pushOps(sample.sourceConn.transport, mintedOps, {
          maxOpsPerBatch: MAX_OPS_PER_BATCH,
        });
        const arrivalPromise = waitForRootChildren(sample.target.client, 1, ROUTE_WAIT_TIMEOUT_MS);
        const pushReturnMs = await pushPromise.then(() => performance.now() - pushStartedAt);
        await arrivalPromise;
        const propagationMs = performance.now() - pushStartedAt;

        sampleResult = {
          index: i - warmup,
          warmup: warmupSample,
          attempt,
          route: sample.classification,
          sourceInstanceId: sample.sourceInstanceId,
          targetInstanceId: sample.targetInstanceId,
          connectMs: sample.connectMs,
          mintedOps: mintedOps.length,
          pushReturnMs,
          propagationMs,
        };
        console.log(
          `[bench-sync-route-fanout] ${warmupSample ? "warmup" : "sample"} ${i + 1}/${total}: ${sample.classification} ${propagationMs.toFixed(1)}ms (${sample.sourceInstanceId} -> ${sample.targetInstanceId})`
        );
        await cleanupRouteSample(sample);
        break;
      } catch (error) {
        lastRouteError = error;
        await cleanupRouteSample(sample);
      }
    }

    if (!sampleResult) {
      throw new Error(`failed to collect sample ${i + 1}/${total}: ${String(lastRouteError)}`);
    }
    if (!warmupSample) samples.push(sampleResult);
  }

  const propagationValues = samples.map((sample) => sample.propagationMs);
  const pushReturnValues = samples.map((sample) => sample.pushReturnMs);
  const result = {
    source: "sync-route-fanout",
    measuredAt: new Date().toISOString(),
    syncServerUrl,
    sourceSyncServerUrl,
    targetSyncServerUrl,
    mode,
    route,
    warmup,
    iterations,
    maxRouteAttempts,
    samples,
    summary: {
      propagation: summarize(propagationValues),
      pushReturn: summarize(pushReturnValues),
      routeCounts: samples.reduce((acc, sample) => {
        acc[sample.route] = (acc[sample.route] ?? 0) + 1;
        return acc;
      }, {}),
    },
  };

  await fs.mkdir(path.dirname(outPath), { recursive: true });
  await fs.writeFile(outPath, `${JSON.stringify(result, null, 2)}\n`, "utf8");
  console.log(`[bench-sync-route-fanout] wrote ${outPath}`);
  console.log(
    `[bench-sync-route-fanout] propagation median=${result.summary.propagation.medianMs?.toFixed(1)}ms p95=${result.summary.propagation.p95Ms?.toFixed(1)}ms`
  );
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});

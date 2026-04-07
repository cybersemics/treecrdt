import { spawn } from 'node:child_process';
import { createRequire } from 'node:module';
import { performance } from 'node:perf_hooks';
import path from 'node:path';
import process from 'node:process';
import { fileURLToPath, pathToFileURL } from 'node:url';
import fs from 'node:fs/promises';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(__dirname, '..');
const playgroundDir = path.join(repoRoot, 'examples', 'playground');
const ROOT_ID = '00000000000000000000000000000000';
const PLAYGROUND_LIVE_WRITE_FIXTURE_VERSION = '2026-03-30-v1';
const playgroundRequire = createRequire(path.join(playgroundDir, 'package.json'));
const { chromium } = playgroundRequire('@playwright/test');
const BENCHMARK_BROWSER_ARGS = [
  '--disable-background-timer-throttling',
  '--disable-renderer-backgrounding',
  '--disable-backgrounding-occluded-windows',
];

function usage() {
  console.log(`Usage:
  node scripts/bench-playground-live-write.mjs [options]

Options:
  --base-url=http://127.0.0.1:5195   Playground base URL (default: env TREECRDT_PLAYGROUND_BASE_URL or http://127.0.0.1:5195)
  --transport=local|remote           Sync transport to benchmark (default: remote)
  --sync-server-url=...              Sync server websocket URL or HTTPS bootstrap URL (required for --transport=remote; default: env TREECRDT_SYNC_SERVER_URL)
  --mode=all|children                Live mode to benchmark (default: all)
  --iterations=N                     Measured samples (default: 5)
  --warmup=N                         Warmup samples (default: 1)
  --tabs=2|3                         Number of tabs/devices (default: 3)
  --auth=0|1                         Enable playground auth (default: 1)
  --headless=0|1                     Launch browser headless (default: 1)
  --host-map=host=ip[,host=ip...]    Optional hostname override(s) for browser resolution
  --label-prefix=foo                 Label prefix for inserted nodes
  --seed-count=N                     Optional number of existing nodes to preseed into a Postgres-backed remote doc before the browser opens
  --seed-fanout=N                    Fanout for seeded balanced trees (default: 10)
  --postgres-url=...                 Postgres URL used to reset/cleanup seeded remote docs (required when --seed-count is set)
  --out=path.json                    Optional output path; defaults under benchmarks/playground-live-write/
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

function parsePositiveIntFlagFromArgv(name, defaultValue) {
  const raw = getArg(name);
  if (raw == null || raw.length === 0) return defaultValue;
  const parsed = Number.parseInt(raw, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) throw new Error(`invalid --${name}: ${raw}`);
  return parsed;
}

function parseBoolArg(name, defaultValue) {
  const raw = getArg(name);
  if (raw == null || raw.length === 0) return defaultValue;
  if (raw === '1' || raw === 'true') return true;
  if (raw === '0' || raw === 'false') return false;
  throw new Error(`invalid --${name}: ${raw}`);
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

function buildHostResolverRules(hostMap) {
  if (hostMap.size === 0) return [];
  const rules = [...hostMap.entries()].map(([host, ip]) => `MAP ${host} ${ip}`);
  return [`--host-resolver-rules=${rules.join(',')}`];
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function nodeIdFromInt(i) {
  if (!Number.isInteger(i) || i < 0) throw new Error(`invalid node id: ${i}`);
  return i.toString(16).padStart(32, '0');
}

function orderKeyFromPosition(position) {
  if (!Number.isInteger(position) || position < 0) {
    throw new Error(`invalid position: ${position}`);
  }
  const n = position + 1;
  if (n > 0xffff) throw new Error(`position too large for u16 order key: ${position}`);
  const bytes = new Uint8Array(2);
  new DataView(bytes.buffer).setUint16(0, n, false);
  return bytes;
}

function replicaFromLabel(label) {
  const encoded = new TextEncoder().encode(label);
  if (encoded.length === 0) throw new Error('label must not be empty');
  const out = new Uint8Array(32);
  for (let i = 0; i < out.length; i += 1) out[i] = encoded[i % encoded.length];
  return out;
}

function buildFanoutInsertTreeOps({ replica, size, fanout, root }) {
  if (!Number.isInteger(size) || size <= 0) throw new Error(`invalid size: ${size}`);
  if (!Number.isInteger(fanout) || fanout <= 0) throw new Error(`invalid fanout: ${fanout}`);
  const ops = [];
  const queue = [{ parent: root, nextChildPosition: 0 }];

  for (let i = 1; i <= size; i += 1) {
    const cursor = queue[0];
    if (!cursor) throw new Error('fanout tree queue empty');

    const parent = cursor.parent;
    const position = cursor.nextChildPosition;
    cursor.nextChildPosition += 1;
    if (cursor.nextChildPosition >= fanout) queue.shift();

    const node = nodeIdFromInt(i);
    ops.push({
      meta: { id: { replica, counter: i }, lamport: i },
      kind: {
        type: 'insert',
        parent,
        node,
        orderKey: orderKeyFromPosition(position),
      },
    });
    queue.push({ parent: node, nextChildPosition: 0 });
  }

  return ops;
}

function buildSeedOps({ size, fanout }) {
  return buildFanoutInsertTreeOps({
    replica: replicaFromLabel('playground-seed'),
    size,
    fanout,
    root: ROOT_ID,
  });
}

let postgresSeedApiPromise = null;
let syncSeedApiPromise = null;
let syncPostgresSeedApiPromise = null;

async function loadPostgresSeedApi() {
  if (!postgresSeedApiPromise) {
    const modulePath = path.join(
      repoRoot,
      'packages',
      'treecrdt-postgres-napi',
      'dist',
      'testing.js',
    );
    postgresSeedApiPromise = import(pathToFileURL(modulePath).href).catch((error) => {
      throw new Error(
        `failed to load Postgres seeding helpers from ${modulePath}; build @treecrdt/postgres-napi first: ${error instanceof Error ? error.message : String(error)}`,
      );
    });
  }
  return await postgresSeedApiPromise;
}

async function loadSyncSeedApi() {
  if (!syncSeedApiPromise) {
    const syncIndexPath = path.join(repoRoot, 'packages', 'sync', 'protocol', 'dist', 'index.js');
    const syncBrowserPath = path.join(
      repoRoot,
      'packages',
      'sync',
      'protocol',
      'dist',
      'browser.js',
    );
    const syncInMemoryPath = path.join(
      repoRoot,
      'packages',
      'sync',
      'protocol',
      'dist',
      'in-memory.js',
    );
    const syncTransportPath = path.join(
      repoRoot,
      'packages',
      'sync',
      'protocol',
      'dist',
      'transport',
      'index.js',
    );
    const syncProtobufPath = path.join(
      repoRoot,
      'packages',
      'sync',
      'protocol',
      'dist',
      'protobuf.js',
    );

    syncSeedApiPromise = Promise.all([
      import(pathToFileURL(syncIndexPath).href),
      import(pathToFileURL(syncBrowserPath).href),
      import(pathToFileURL(syncInMemoryPath).href),
      import(pathToFileURL(syncTransportPath).href),
      import(pathToFileURL(syncProtobufPath).href),
    ]).catch((error) => {
      throw new Error(
        `failed to load sync seeding helpers; build @treecrdt/sync first: ${error instanceof Error ? error.message : String(error)}`,
      );
    });
  }

  const [syncIndex, syncBrowser, syncInMemory, syncTransport, syncProtobuf] =
    await syncSeedApiPromise;
  return {
    SyncPeer: syncIndex.SyncPeer,
    deriveOpRefV0: syncIndex.deriveOpRefV0,
    createBrowserWebSocketTransport: syncBrowser.createBrowserWebSocketTransport,
    makeQueuedSyncBackend: syncInMemory.makeQueuedSyncBackend,
    wrapDuplexTransportWithCodec: syncTransport.wrapDuplexTransportWithCodec,
    treecrdtSyncV0ProtobufCodec: syncProtobuf.treecrdtSyncV0ProtobufCodec,
  };
}

async function loadSyncPostgresSeedApi() {
  if (!syncPostgresSeedApiPromise) {
    const modulePath = path.join(
      repoRoot,
      'packages',
      'sync',
      'material',
      'postgres',
      'dist',
      'index.js',
    );
    syncPostgresSeedApiPromise = import(pathToFileURL(modulePath).href).catch((error) => {
      throw new Error(
        `failed to load Postgres sync proof-material helpers from ${modulePath}; build @treecrdt/sync-postgres first: ${error instanceof Error ? error.message : String(error)}`,
      );
    });
  }
  return await syncPostgresSeedApiPromise;
}

async function canFetch(url) {
  try {
    const res = await fetch(url);
    return res.ok;
  } catch {
    return false;
  }
}

async function waitForHttp(url, timeoutMs = 60_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await canFetch(url)) return;
    await sleep(250);
  }
  throw new Error(`timed out waiting for ${url}`);
}

function isLocalhostUrl(baseUrl) {
  const parsed = new URL(baseUrl);
  return parsed.hostname === '127.0.0.1' || parsed.hostname === 'localhost';
}

async function maybeStartPlaygroundServer(baseUrl) {
  const healthUrl = new URL(baseUrl);
  healthUrl.pathname = '/';
  healthUrl.search = '';
  healthUrl.hash = '';
  const readyUrl = healthUrl.toString();

  if (await canFetch(readyUrl)) return { child: null, started: false };
  if (!isLocalhostUrl(baseUrl)) {
    throw new Error(`playground is not reachable at ${readyUrl} and base-url is not localhost`);
  }

  const parsed = new URL(baseUrl);
  const port = parsed.port || '5195';
  const child = spawn(
    'pnpm',
    ['exec', 'vite', '--host', parsed.hostname, '--port', port, '--strictPort'],
    {
      cwd: playgroundDir,
      stdio: ['ignore', 'pipe', 'pipe'],
      env: process.env,
    },
  );

  child.stdout?.on('data', (chunk) => process.stdout.write(`[playground] ${chunk}`));
  child.stderr?.on('data', (chunk) => process.stderr.write(`[playground] ${chunk}`));

  try {
    await waitForHttp(readyUrl, 90_000);
    return { child, started: true };
  } catch (err) {
    child.kill('SIGINT');
    throw err;
  }
}

async function stopChild(child) {
  if (!child) return;
  child.kill('SIGINT');
  await new Promise((resolve) => child.once('exit', () => resolve()));
}

async function waitReady(page) {
  await page.getByText('Ready (memory)').waitFor({ timeout: 60_000 });
  const show = page.getByRole('button', { name: 'Show', exact: true });
  if ((await show.count()) > 0) await show.click();

  const newDevice = page.getByRole('button', { name: /New device/ });
  const addChild = rootRow(page).getByRole('button', { name: 'Add child' });
  const deadline = Date.now() + 60_000;
  while (Date.now() < deadline) {
    if ((await newDevice.isEnabled()) && (await addChild.isEnabled())) return;
    await sleep(250);
  }
  throw new Error(`timed out waiting for sync controls on ${page.url()}`);
}

async function waitRemoteConnection(page) {
  const connectionsButton = page.getByRole('button', { name: /Connections/ });
  const deadline = Date.now() + 60_000;
  while (Date.now() < deadline) {
    const text = (await connectionsButton.textContent()) ?? '';
    const countMatch = text.match(/Connections\s*(\d+)/i);
    if (countMatch && Number.parseInt(countMatch[1] ?? '0', 10) >= 1) return;
    const syncError = await readSyncError(page);
    if (syncError) throw new Error(`sync error before connection: ${syncError}`);
    await sleep(250);
  }
  throw new Error(`timed out waiting for remote connection on ${page.url()}`);
}

function rootRow(page) {
  return page.locator(`[data-testid="tree-row"][data-node-id="${ROOT_ID}"]`);
}

async function clickAndAssertPressed(button) {
  await button.click();
  await button.waitFor({ state: 'visible', timeout: 30_000 });
  await sleep(150);
}

async function enableLiveMode(page, mode) {
  if (mode === 'all') {
    const button = page.getByRole('button', { name: 'Live sync all' });
    await clickAndAssertPressed(button);
    return;
  }
  if (mode === 'children') {
    const button = rootRow(page).getByRole('button', { name: 'Live sync children' });
    await clickAndAssertPressed(button);
    return;
  }
  throw new Error(`unsupported mode: ${mode}`);
}

async function readSyncError(page) {
  const syncError = page.getByTestId('sync-error');
  if ((await syncError.count()) === 0) return null;
  if (!(await syncError.isVisible())) return null;
  return ((await syncError.textContent()) ?? '').trim();
}

async function openNewDevice(page, opts = {}) {
  const { waitForRemote = false } = opts;
  const [popup] = await Promise.all([
    page.waitForEvent('popup', { timeout: 30_000 }),
    page.getByRole('button', { name: /New device/ }).click(),
  ]);
  await waitReady(popup);
  if (waitForRemote) await waitRemoteConnection(popup);
  return popup;
}

async function addNode(page, label) {
  const input = page.getByPlaceholder('Stored as payload bytes');
  await input.fill(label);
  await rootRow(page).getByRole('button', { name: 'Add child' }).click();
  const row = page.getByTestId('tree-row').filter({ hasText: label }).first();
  await row.waitFor({ state: 'visible', timeout: 30_000 });
  const nodeId = await row.getAttribute('data-node-id');
  if (!nodeId) throw new Error(`missing node id for inserted label "${label}"`);
  return { nodeId };
}

async function resetBenchState(page) {
  await page.evaluate(() => {
    window.__treecrdtPlaygroundBench = { nodes: {} };
  });
}

async function readBenchNodeTiming(page, nodeId) {
  return await page.evaluate((id) => window.__treecrdtPlaygroundBench?.nodes?.[id] ?? null, nodeId);
}

async function waitForNode(page, nodeId, timeoutMs, startedAtMs) {
  const handle = await page.waitForFunction(
    ({ targetNodeId }) => {
      const syncError = document.querySelector('[data-testid="sync-error"]');
      const syncErrorText = syncError?.textContent?.trim();
      if (syncErrorText) return { ok: false, error: syncErrorText };
      const row = document.querySelector(
        `[data-testid="tree-row"][data-node-id="${targetNodeId}"]`,
      );
      return row ? { ok: true } : null;
    },
    { targetNodeId: nodeId },
    { timeout: timeoutMs },
  );
  const result = await handle.jsonValue();
  await handle.dispose();
  if (!result?.ok) {
    throw new Error(
      `sync error on ${page.url()}: ${result?.error ?? `timed out waiting for node ${nodeId}`}`,
    );
  }
  const durationMs = performance.now() - startedAtMs;
  const benchTiming = await readBenchNodeTiming(page, nodeId);
  return { durationMs, benchTiming };
}

async function waitForVisibleRowCount(page, minCount, timeoutMs = 60_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const syncError = await readSyncError(page);
    if (syncError) throw new Error(`sync error before visible rows: ${syncError}`);
    const count = await page.getByTestId('tree-row').count();
    if (count >= minCount) return;
    await sleep(250);
  }
  throw new Error(`timed out waiting for at least ${minCount} visible rows on ${page.url()}`);
}

async function seedBalancedTreeInBrowser(page, { count, fanout }) {
  await page.evaluate(
    async ({ seedCount, seedFanout }) => {
      const bench = window.__treecrdtPlaygroundBench;
      if (!bench?.seedBalancedTree) {
        throw new Error('playground bench seedBalancedTree hook is not available');
      }
      await bench.seedBalancedTree({ count: seedCount, fanout: seedFanout });
    },
    { seedCount: count, seedFanout: fanout },
  );
}

async function waitForBenchHeadLamport(page, minHeadLamport, timeoutMs = 120_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const syncError = await readSyncError(page);
    if (syncError)
      throw new Error(`sync error before head lamport ${minHeadLamport}: ${syncError}`);
    const state = await page.evaluate(() => window.__treecrdtPlaygroundBench?.getState?.() ?? null);
    if (state && typeof state.headLamport === 'number' && state.headLamport >= minHeadLamport) {
      return;
    }
    await sleep(250);
  }
  throw new Error(`timed out waiting for head lamport ${minHeadLamport} on ${page.url()}`);
}

async function waitForBenchIdle(page, { settleMs = 1_000, timeoutMs = 120_000 } = {}) {
  const deadline = Date.now() + timeoutMs;
  let idleStartedAt = null;
  while (Date.now() < deadline) {
    const syncError = await readSyncError(page);
    if (syncError) throw new Error(`sync error before idle state: ${syncError}`);
    const state = await page.evaluate(() => window.__treecrdtPlaygroundBench?.getState?.() ?? null);
    const isIdle =
      state && state.status === 'ready' && state.syncBusy === false && state.liveBusy === false;
    if (isIdle) {
      if (idleStartedAt == null) idleStartedAt = Date.now();
      if (Date.now() - idleStartedAt >= settleMs) return;
    } else {
      idleStartedAt = null;
    }
    await sleep(250);
  }
  throw new Error(`timed out waiting for idle benchmark state on ${page.url()}`);
}

async function waitForSeededRemoteDoc(postgresUrl, docId, expectedHeadLamport, timeoutMs = 60_000) {
  const { createTreecrdtPostgresClient } = await loadPostgresSeedApi();
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const client = await createTreecrdtPostgresClient(postgresUrl, { docId });
    try {
      const headLamport = await client.meta.headLamport();
      if (headLamport >= expectedHeadLamport) return;
    } finally {
      await client.close();
    }
    await sleep(250);
  }
  throw new Error(
    `timed out waiting for remote doc ${docId} to reach head lamport ${expectedHeadLamport}`,
  );
}

function buildSyncWebSocketUrl(baseUrl, docId) {
  let input = baseUrl.trim();
  if (input.length === 0) throw new Error('sync server URL is empty');
  if (!/^[a-zA-Z][a-zA-Z0-9+.-]*:/.test(input)) input = `ws://${input}`;
  const url = new URL(input);
  if (url.protocol === 'http:') url.protocol = 'ws:';
  if (url.protocol === 'https:') url.protocol = 'wss:';
  if (url.protocol !== 'ws:' && url.protocol !== 'wss:') {
    throw new Error('sync server URL must use ws://, wss://, http://, or https://');
  }
  if (url.pathname === '/' || url.pathname.length === 0) {
    url.pathname = '/sync';
  }
  url.searchParams.set('docId', docId);
  return url;
}

async function openSeedWebSocket(url, timeoutMs = 10_000) {
  const WebSocketCtor = globalThis.WebSocket;
  if (typeof WebSocketCtor !== 'function') {
    throw new Error('global WebSocket is not available in this Node runtime');
  }
  return await new Promise((resolve, reject) => {
    const ws = new WebSocketCtor(url.toString());
    ws.binaryType = 'arraybuffer';
    let settled = false;

    const finish = (fn, value) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      ws.removeEventListener('open', onOpen);
      ws.removeEventListener('error', onError);
      fn(value);
    };

    const onOpen = () => finish(resolve, ws);
    const onError = () => finish(reject, new Error(`failed connecting to ${url.toString()}`));
    const timer = setTimeout(() => {
      try {
        ws.close();
      } catch {
        // ignore
      }
      finish(reject, new Error(`timed out connecting to ${url.toString()}`));
    }, timeoutMs);

    ws.addEventListener('open', onOpen);
    ws.addEventListener('error', onError);
  });
}

async function closeSeedWebSocket(ws) {
  if (ws.readyState === globalThis.WebSocket.CLOSED) return;
  await new Promise((resolve) => {
    let settled = false;
    const finish = () => {
      if (settled) return;
      settled = true;
      ws.removeEventListener('close', onClose);
      ws.removeEventListener('error', onError);
      resolve();
    };
    const onClose = () => finish();
    const onError = () => finish();
    ws.addEventListener('close', onClose);
    ws.addEventListener('error', onError);
    try {
      if (ws.readyState !== globalThis.WebSocket.CLOSING) {
        ws.close();
      }
    } catch {
      finish();
    }
    setTimeout(finish, 1_000);
  });
}

function buildSeedSyncBackend({ docId, ops, deriveOpRefV0, makeQueuedSyncBackend }) {
  const opRefs = [];
  const opRefHexToOp = new Map();
  for (const op of ops) {
    const opRef = deriveOpRefV0(docId, op.meta.id);
    const opRefHex = Buffer.from(opRef).toString('hex');
    opRefs.push(opRef);
    opRefHexToOp.set(opRefHex, op);
  }

  return makeQueuedSyncBackend({
    docId,
    initialMaxLamport: ops.reduce((max, op) => Math.max(max, op.meta.lamport), 0),
    maxLamportFromOps: (incomingOps) =>
      incomingOps.reduce((max, op) => Math.max(max, op.meta.lamport), 0),
    listOpRefs: async (filter) => {
      if (!('all' in filter)) {
        throw new Error('seed sync backend only supports { all: {} }');
      }
      return opRefs;
    },
    getOpsByOpRefs: async (requestedOpRefs) =>
      requestedOpRefs
        .map((opRef) => opRefHexToOp.get(Buffer.from(opRef).toString('hex')) ?? null)
        .filter((op) => op != null),
    applyOps: async () => {},
  });
}

async function preseedPostgresPlaygroundDoc({ postgresUrl, docId, size, fanout }) {
  const { createPostgresNapiTestAdapterFactory, createTreecrdtPostgresClient } =
    await loadPostgresSeedApi();
  const factory = createPostgresNapiTestAdapterFactory(postgresUrl);
  await factory.ensureSchema();
  const hotWriteFixtureDocId = [
    'hot-write-seed',
    PLAYGROUND_LIVE_WRITE_FIXTURE_VERSION,
    `fanout${fanout}`,
    'payload32',
    String(size),
  ].join('-');
  try {
    await factory.cloneDocForTests(hotWriteFixtureDocId, docId);
    return {
      fixtureDocId: hotWriteFixtureDocId,
      cleanup: async () => {
        if (process.env.PLAYGROUND_LIVE_WRITE_SKIP_DOC_CLEANUP === '1') return;
        await factory.resetDocForTests(docId);
      },
    };
  } catch {
    // Fall back to building a dedicated zero-payload fixture for the playground bench.
  }

  const fixtureDocId = [
    'playground-live-write-seed',
    PLAYGROUND_LIVE_WRITE_FIXTURE_VERSION,
    `fanout${fanout}`,
    String(size),
  ].join('-');
  const expectedHeadLamport = size;
  const fixtureClient = await createTreecrdtPostgresClient(postgresUrl, { docId: fixtureDocId });
  try {
    const [headLamport, nodeCount] = await Promise.all([
      fixtureClient.meta.headLamport(),
      fixtureClient.tree.nodeCount(),
    ]);
    if (headLamport !== expectedHeadLamport || nodeCount !== size) {
      await factory.primeBalancedFanoutDocForTests(
        fixtureDocId,
        size,
        fanout,
        0,
        'playground-seed',
      );
    }
  } finally {
    await fixtureClient.close();
  }
  await factory.cloneDocForTests(fixtureDocId, docId);

  return {
    fixtureDocId,
    cleanup: async () => {
      if (process.env.PLAYGROUND_LIVE_WRITE_SKIP_DOC_CLEANUP === '1') return;
      if (docId !== fixtureDocId) await factory.resetDocForTests(docId);
    },
  };
}

function percentile(sorted, p) {
  if (sorted.length === 0) return null;
  const index = Math.min(sorted.length - 1, Math.max(0, Math.ceil((p / 100) * sorted.length) - 1));
  return sorted[index];
}

function summarize(values) {
  if (values.length === 0) return null;
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

function phaseExtrema(entries, key, method) {
  const values = entries
    .map((entry) => entry?.[key])
    .filter((value) => typeof value === 'number' && Number.isFinite(value));
  if (values.length === 0) return null;
  return method === 'min' ? Math.min(...values) : Math.max(...values);
}

function derivePhaseOffsets(sample) {
  const source = sample.sourceBenchOffsetsMs ?? {};
  const targets = Array.isArray(sample.targetBenchOffsetsMs)
    ? sample.targetBenchOffsetsMs.filter((entry) => entry && typeof entry === 'object')
    : [];

  return {
    sourceLocalPersistedMs: source.sourceLocalPersistedAtMsAfterMs ?? null,
    sourceLocalPreviewMs: source.sourceLocalPreviewAppliedAtMsAfterMs ?? null,
    sourceRemoteQueuedMs: source.sourceRemoteQueuedAtMsAfterMs ?? null,
    sourceRemotePushStartedMs: source.sourceRemotePushStartedAtMsAfterMs ?? null,
    sourceRemotePushFinishedMs: source.sourceRemotePushFinishedAtMsAfterMs ?? null,
    sourceRowCommittedMs: source.rowCommittedAtMsAfterMs ?? null,
    sourceTreeRefreshMs: source.treeRefreshAppliedAtMsAfterMs ?? null,
    targetSocketMessageMs: phaseExtrema(targets, 'targetSocketMessageAtMsAfterMs', 'min'),
    targetBackendApplyStartedMs: phaseExtrema(
      targets,
      'targetBackendApplyStartedAtMsAfterMs',
      'min',
    ),
    targetBackendApplyFinishedMs: phaseExtrema(
      targets,
      'targetBackendApplyFinishedAtMsAfterMs',
      'max',
    ),
    targetRemoteStartedMs: phaseExtrema(targets, 'remoteOpsAppliedStartedAtMsAfterMs', 'min'),
    targetPayloadsRefreshedMs: phaseExtrema(targets, 'payloadsRefreshedAtMsAfterMs', 'max'),
    targetRemoteFinishedMs: phaseExtrema(targets, 'remoteOpsAppliedFinishedAtMsAfterMs', 'max'),
    targetTreeRefreshMs: phaseExtrema(targets, 'treeRefreshAppliedAtMsAfterMs', 'max'),
    targetRowCommittedMs: phaseExtrema(targets, 'rowCommittedAtMsAfterMs', 'max'),
  };
}

function summarizePhaseOffsets(samples) {
  const phaseKeys = [
    'sourceLocalPersistedMs',
    'sourceLocalPreviewMs',
    'sourceRemoteQueuedMs',
    'sourceRemotePushStartedMs',
    'sourceRemotePushFinishedMs',
    'sourceRowCommittedMs',
    'sourceTreeRefreshMs',
    'targetSocketMessageMs',
    'targetBackendApplyStartedMs',
    'targetBackendApplyFinishedMs',
    'targetRemoteStartedMs',
    'targetPayloadsRefreshedMs',
    'targetRemoteFinishedMs',
    'targetTreeRefreshMs',
    'targetRowCommittedMs',
  ];
  const out = {};
  for (const key of phaseKeys) {
    const values = samples
      .map((sample) => sample.phaseOffsetsMs?.[key])
      .filter((value) => typeof value === 'number' && Number.isFinite(value));
    if (values.length > 0) out[key] = summarize(values);
  }
  return out;
}

function normalizeBenchTiming(benchTiming, startedAtWallClockMs) {
  if (!benchTiming) return null;
  const out = {};
  for (const [key, value] of Object.entries(benchTiming)) {
    if (typeof value !== 'number') continue;
    out[`${key}AfterMs`] = value - startedAtWallClockMs;
  }
  return out;
}

function defaultOutPath({ transport, syncServerUrl, mode, auth, tabs, seedCount }) {
  const safeHost =
    transport === 'local'
      ? 'local-mesh'
      : new URL(syncServerUrl).host.replace(/[^a-z0-9.-]+/gi, '_');
  const seedSuffix = seedCount > 0 ? `-seed${seedCount}` : '';
  return path.join(
    repoRoot,
    'benchmarks',
    'playground-live-write',
    `playground-live-write-${safeHost}-${transport}-${mode}${seedSuffix}-auth${auth ? '1' : '0'}-${tabs}tabs.json`,
  );
}

async function main() {
  if (hasFlag('help')) {
    usage();
    return;
  }

  const baseUrl =
    getArg('base-url') ?? process.env.TREECRDT_PLAYGROUND_BASE_URL ?? 'http://127.0.0.1:5195';
  const transport = getArg('transport') ?? 'remote';
  const syncServerUrl = getArg('sync-server-url') ?? process.env.TREECRDT_SYNC_SERVER_URL ?? '';
  if (!['local', 'remote'].includes(transport)) {
    throw new Error(`--transport must be local or remote, got ${transport}`);
  }
  if (transport === 'remote' && !syncServerUrl) {
    throw new Error('missing --sync-server-url or TREECRDT_SYNC_SERVER_URL');
  }

  const mode = getArg('mode') ?? 'all';
  const iterations = parseIntArg('iterations', 5);
  const warmup = parseIntArg('warmup', 1);
  const tabs = parseIntArg('tabs', 3);
  const auth = parseBoolArg('auth', true);
  const headless = parseBoolArg('headless', true);
  const seedCount = parseIntArg('seed-count', 0);
  const seedFanout = parsePositiveIntFlagFromArgv('seed-fanout', 10);
  const postgresUrl = getArg('postgres-url') ?? process.env.TREECRDT_POSTGRES_URL ?? '';
  const hostMap = parseHostMap(getArg('host-map') ?? process.env.TREECRDT_BENCH_HOST_MAP ?? '');
  const labelPrefix = getArg('label-prefix') ?? `pw-live-write-${mode}`;
  const outPath =
    getArg('out') ?? defaultOutPath({ transport, syncServerUrl, mode, auth, tabs, seedCount });

  if (tabs < 2 || tabs > 3) throw new Error(`--tabs must be 2 or 3, got ${tabs}`);
  if (!['all', 'children'].includes(mode))
    throw new Error(`--mode must be all or children, got ${mode}`);
  if (iterations <= 0) throw new Error(`--iterations must be > 0, got ${iterations}`);
  if (seedCount > 0 && transport !== 'remote') {
    throw new Error('--seed-count currently requires --transport=remote');
  }
  const useRemotePreseed = seedCount > 0 && postgresUrl.length > 0;

  const playground = await maybeStartPlaygroundServer(baseUrl);
  const browser = await chromium.launch({
    headless,
    args: [...BENCHMARK_BROWSER_ARGS, ...buildHostResolverRules(hostMap)],
  });
  const context = await browser.newContext();
  let seedFixture = null;

  try {
    const docId = `${labelPrefix}-doc-${Date.now()}`;
    if (useRemotePreseed) {
      seedFixture = await preseedPostgresPlaygroundDoc({
        postgresUrl,
        docId,
        size: seedCount,
        fanout: seedFanout,
      });
    }
    const rootUrl = new URL(baseUrl);
    rootUrl.searchParams.set('doc', docId);
    rootUrl.searchParams.set('profile', `${labelPrefix}-a`);
    rootUrl.searchParams.set('transport', transport);
    if (transport === 'remote') rootUrl.searchParams.set('sync', syncServerUrl);
    rootUrl.searchParams.set('auth', auth ? '1' : '0');

    const pageA = await context.newPage();
    await pageA.goto(rootUrl.toString());
    await waitReady(pageA);
    if (transport === 'remote') await waitRemoteConnection(pageA);
    await enableLiveMode(pageA, mode);
    const seedPage = pageA;
    const measuredPages = seedCount > 0 && !useRemotePreseed ? [] : [seedPage];
    let openerPage = seedPage;
    while (measuredPages.length < tabs) {
      const nextPage = await openNewDevice(openerPage, { waitForRemote: transport === 'remote' });
      await enableLiveMode(nextPage, mode);
      measuredPages.push(nextPage);
      openerPage = nextPage;
    }

    if (seedCount > 0 && useRemotePreseed) {
      if (mode === 'children') {
        const expectedVisibleRows = 1 + Math.min(seedFanout, seedCount);
        await Promise.all(
          measuredPages.map((page) => waitForVisibleRowCount(page, expectedVisibleRows, 180_000)),
        );
      } else {
        await Promise.all(
          measuredPages.map((page) => waitForBenchHeadLamport(page, seedCount, 180_000)),
        );
      }
      await Promise.all(
        measuredPages.map((page) => waitForBenchIdle(page, { timeoutMs: 180_000 })),
      );
    } else if (seedCount > 0) {
      await seedBalancedTreeInBrowser(seedPage, { count: seedCount, fanout: seedFanout });
      await waitForBenchHeadLamport(seedPage, seedCount);
      if (mode === 'children') {
        const expectedVisibleRows = 1 + Math.min(seedFanout, seedCount);
        await Promise.all(
          measuredPages.map((page) => waitForVisibleRowCount(page, expectedVisibleRows)),
        );
      } else {
        await Promise.all(measuredPages.map((page) => waitForBenchHeadLamport(page, seedCount)));
      }
      await Promise.all(measuredPages.map((page) => waitForBenchIdle(page)));
      await seedPage.close();
    }
    await Promise.all(measuredPages.map((page) => resetBenchState(page)));

    const sourcePage = measuredPages[measuredPages.length - 1];
    const targetPages = measuredPages.slice(0, -1);
    const samples = [];
    const total = warmup + iterations;

    for (let i = 0; i < total; i += 1) {
      const label = `${labelPrefix}-${i}-${Date.now()}`;
      const warmupSample = i < warmup;
      if (targetPages.length > 0) {
        await targetPages[0].bringToFront();
      }
      const startedAtWallClockMs = Date.now();
      const start = performance.now();
      const { nodeId } = await addNode(sourcePage, label);
      const sourceApplyMs = performance.now() - start;
      const sourceBenchTiming = await readBenchNodeTiming(sourcePage, nodeId);
      const targetResults = await Promise.all(
        targetPages.map((page) => waitForNode(page, nodeId, 60_000, start)),
      );
      const targetDurationsMs = targetResults.map((entry) => entry.durationMs);
      const durationMs = Math.max(...targetDurationsMs);
      const sample = {
        index: i - warmup,
        warmup: warmupSample,
        label,
        nodeId,
        startedAtWallClockMs,
        sourceApplyMs,
        sourceBenchTiming,
        sourceBenchOffsetsMs: normalizeBenchTiming(sourceBenchTiming, startedAtWallClockMs),
        durationMs,
        targetDurationsMs,
        targetBenchTimings: targetResults.map((entry) => entry.benchTiming),
        targetBenchOffsetsMs: targetResults.map((entry) =>
          normalizeBenchTiming(entry.benchTiming, startedAtWallClockMs),
        ),
      };
      sample.phaseOffsetsMs = derivePhaseOffsets(sample);
      console.log(
        `[bench-playground-live-write] ${warmupSample ? 'warmup' : 'sample'} ${i + 1}/${total}: ${durationMs.toFixed(1)}ms`,
      );
      if (!warmupSample) {
        samples.push(sample);
      }
    }

    const summary = summarize(samples.map((sample) => sample.durationMs));
    const result = {
      baseUrl,
      transport,
      syncServerUrl,
      mode,
      auth,
      tabs,
      warmup,
      iterations,
      seed:
        seedCount > 0
          ? {
              count: seedCount,
              fanout: seedFanout,
              fixtureDocId: seedFixture?.fixtureDocId ?? null,
            }
          : null,
      source: 'browser-live-write',
      measuredAt: new Date().toISOString(),
      samples,
      summary,
      phaseSummary: summarizePhaseOffsets(samples),
    };

    await fs.mkdir(path.dirname(outPath), { recursive: true });
    await fs.writeFile(outPath, `${JSON.stringify(result, null, 2)}\n`, 'utf8');

    console.log(`[bench-playground-live-write] wrote ${outPath}`);
    console.log(
      `[bench-playground-live-write] summary: median=${summary.medianMs?.toFixed(1)}ms p95=${summary.p95Ms?.toFixed(1)}ms max=${summary.maxMs?.toFixed(1)}ms`,
    );
  } finally {
    await context.close();
    await browser.close();
    await seedFixture?.cleanup?.();
    await stopChild(playground.child);
  }
}

main().catch((err) => {
  console.error(err instanceof Error ? (err.stack ?? err.message) : String(err));
  process.exitCode = 1;
});

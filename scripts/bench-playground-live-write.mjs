import { spawn } from 'node:child_process';
import { createRequire } from 'node:module';
import { performance } from 'node:perf_hooks';
import path from 'node:path';
import process from 'node:process';
import { fileURLToPath } from 'node:url';
import fs from 'node:fs/promises';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(__dirname, '..');
const playgroundDir = path.join(repoRoot, 'examples', 'playground');
const ROOT_ID = '00000000000000000000000000000000';
const playgroundRequire = createRequire(path.join(playgroundDir, 'package.json'));
const { chromium } = playgroundRequire('@playwright/test');

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

function percentile(sorted, p) {
  if (sorted.length === 0) return null;
  const index = Math.min(sorted.length - 1, Math.max(0, Math.ceil((p / 100) * sorted.length) - 1));
  return sorted[index];
}

function summarize(values) {
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

function normalizeBenchTiming(benchTiming, startedAtWallClockMs) {
  if (!benchTiming) return null;
  const out = {};
  for (const [key, value] of Object.entries(benchTiming)) {
    if (typeof value !== 'number') continue;
    out[`${key}AfterMs`] = value - startedAtWallClockMs;
  }
  return out;
}

function defaultOutPath({ transport, syncServerUrl, mode, auth, tabs, contexts }) {
  const safeHost =
    transport === 'local'
      ? 'local-mesh'
      : new URL(syncServerUrl).host.replace(/[^a-z0-9.-]+/gi, '_');
  return path.join(
    repoRoot,
    'benchmarks',
    'playground-live-write',
    `playground-live-write-${safeHost}-${transport}-${mode}-auth${auth ? '1' : '0'}-${tabs}tabs.json`,
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
  const hostMap = parseHostMap(getArg('host-map') ?? process.env.TREECRDT_BENCH_HOST_MAP ?? '');
  const labelPrefix = getArg('label-prefix') ?? `pw-live-write-${mode}`;
  const outPath = getArg('out') ?? defaultOutPath({ transport, syncServerUrl, mode, auth, tabs });

  if (tabs < 2 || tabs > 3) throw new Error(`--tabs must be 2 or 3, got ${tabs}`);
  if (!['all', 'children'].includes(mode))
    throw new Error(`--mode must be all or children, got ${mode}`);
  if (iterations <= 0) throw new Error(`--iterations must be > 0, got ${iterations}`);

  const playground = await maybeStartPlaygroundServer(baseUrl);
  const browser = await chromium.launch({
    headless,
    args: buildHostResolverRules(hostMap),
  });
  const context = await browser.newContext();

  try {
    const docId = `${labelPrefix}-doc-${Date.now()}`;
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
    await resetBenchState(pageA);

    const pageB = await openNewDevice(pageA, { waitForRemote: transport === 'remote' });
    await enableLiveMode(pageB, mode);
    await resetBenchState(pageB);

    const pages = [pageA, pageB];
    if (tabs === 3) {
      const pageC = await openNewDevice(pageB, { waitForRemote: transport === 'remote' });
      await enableLiveMode(pageC, mode);
      await resetBenchState(pageC);
      pages.push(pageC);
    }

    const sourcePage = pages[pages.length - 1];
    const targetPages = pages.slice(0, -1);
    const samples = [];
    const total = warmup + iterations;

    for (let i = 0; i < total; i += 1) {
      const label = `${labelPrefix}-${i}-${Date.now()}`;
      const warmupSample = i < warmup;
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
      source: 'browser-live-write',
      measuredAt: new Date().toISOString(),
      samples,
      summary,
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
    await stopChild(playground.child);
  }
}

main().catch((err) => {
  console.error(err instanceof Error ? (err.stack ?? err.message) : String(err));
  process.exitCode = 1;
});

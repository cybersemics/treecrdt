import fs from "node:fs/promises";
import net from "node:net";
import path from "node:path";
import Database from "better-sqlite3";
import WebSocket from "ws";

import {
  ALL_SYNC_BENCH_WORKLOADS,
  buildSyncBenchCase,
  DEFAULT_SYNC_BENCH_FANOUT,
  DEFAULT_SYNC_BENCH_ROOT_CHILDREN_WORKLOADS,
  DEFAULT_SYNC_BENCH_WORKLOADS,
  SYNC_BENCH_DEFAULT_CODEWORDS_PER_MESSAGE,
  SYNC_BENCH_DEFAULT_MAX_CODEWORDS,
  maxLamport,
  quantile,
  type SyncBenchWorkload,
} from "@treecrdt/benchmark";
import { repoRootFromImportMeta, writeResult } from "@treecrdt/benchmark/node";
import type { Operation } from "@treecrdt/interface";
import { nodeIdToBytes16 } from "@treecrdt/interface/ids";
import { SyncPeer, type Filter } from "@treecrdt/sync";
import {
  createInMemoryConnectedPeers,
  makeQueuedSyncBackend,
  type FlushableSyncBackend,
} from "@treecrdt/sync/in-memory";
import { createTreecrdtSyncBackendFromClient } from "@treecrdt/sync-sqlite/backend";
import { treecrdtSyncV0ProtobufCodec } from "@treecrdt/sync/protobuf";
import {
  wrapDuplexTransportWithCodec,
  type DuplexTransport,
} from "@treecrdt/sync/transport";
import { startSyncServer } from "@treecrdt/sync-server-postgres-node";

import {
  createTreecrdtClient,
  createSqliteNodeApi,
  loadTreecrdtExtension,
} from "../dist/index.js";

type StorageKind = "memory" | "file";
type ConfigEntry = [number, number];
type SyncBenchTargetId =
  | "direct"
  | "local-postgres-sync-server"
  | "remote-sync-server";

type BenchCase = {
  storage: StorageKind;
  target: SyncBenchTargetId;
  workload: SyncBenchWorkload;
  size: number;
  iterations: number;
  fanout: number;
};

type SyncBenchResult = {
  name: string;
  totalOps: number;
  durationMs: number;
  opsPerSec: number;
  extra?: Record<string, unknown>;
};

type SyncBenchSample = {
  totalMs: number;
  syncMs: number;
  firstViewReadMs: number;
};

type SyncBenchConnection = {
  transport: DuplexTransport<any>;
  close: () => Promise<void>;
};

type SyncBenchTargetRuntime = {
  id: Exclude<SyncBenchTargetId, "direct">;
  connect: (docId: string) => Promise<SyncBenchConnection>;
  close: () => Promise<void>;
};

const SYNC_BENCH_CONFIG: ReadonlyArray<ConfigEntry> = [
  [100, 10],
  [1_000, 5],
  [10_000, 10],
];

const SYNC_BENCH_ROOT_CONFIG: ReadonlyArray<ConfigEntry> = [[1110, 10]];

const DEFAULT_TARGETS: readonly SyncBenchTargetId[] = ["direct"];
const ALL_TARGETS: readonly SyncBenchTargetId[] = [
  "direct",
  "local-postgres-sync-server",
  "remote-sync-server",
];
const DEFAULT_STORAGES: readonly StorageKind[] = ["memory", "file"];
const ALL_WORKLOADS: readonly SyncBenchWorkload[] = [
  ...ALL_SYNC_BENCH_WORKLOADS,
  ...DEFAULT_SYNC_BENCH_ROOT_CHILDREN_WORKLOADS,
];
const SERVER_READY_TIMEOUT_MS = 10_000;
const SERVER_READY_POLL_MS = 100;

function envInt(name: string): number | undefined {
  const raw = process.env[name];
  if (raw == null || raw === "") return undefined;
  const n = Number(raw);
  return Number.isFinite(n) ? n : undefined;
}

function parseConfigFromArgv(argv: string[]): Array<ConfigEntry> | null {
  let customConfig: Array<ConfigEntry> | null = null;
  const defaultIterations = Math.max(1, envInt("BENCH_ITERATIONS") ?? 1);
  for (const arg of argv) {
    if (arg.startsWith("--count=")) {
      const val = arg.slice("--count=".length).trim();
      const count = val ? Number(val) : 500;
      customConfig = [[Number.isFinite(count) && count > 0 ? count : 500, defaultIterations]];
      break;
    }
    if (arg.startsWith("--counts=")) {
      const vals = arg
        .slice("--counts=".length)
        .split(",")
        .map((s) => s.trim())
        .filter((s) => s.length > 0);
      const parsed = vals
        .map((s) => {
          const n = Number(s);
          return Number.isFinite(n) && n > 0 ? n : null;
        })
        .filter((n): n is number => n != null)
        .map((c) => [c, defaultIterations] as ConfigEntry);
      if (parsed.length > 0) customConfig = parsed;
      break;
    }
  }
  return customConfig;
}

function parseFlagValue(argv: string[], flag: string): string | undefined {
  const prefix = `${flag}=`;
  const raw = argv.find((arg) => arg.startsWith(prefix));
  return raw ? raw.slice(prefix.length).trim() : undefined;
}

function parsePositiveIntFlag(argv: string[], flag: string, envName: string, fallback: number): number {
  const raw = parseFlagValue(argv, flag) ?? process.env[envName];
  if (!raw) return fallback;
  const value = Number(raw);
  if (!Number.isInteger(value) || value <= 0) {
    throw new Error(`invalid ${flag} value "${raw}", expected a positive integer`);
  }
  return value;
}

function parseBooleanFlag(argv: string[], flag: string, envName: string): boolean {
  const envRaw = process.env[envName];
  if (argv.includes(flag)) return true;
  if (envRaw == null || envRaw === "") return false;
  return ["1", "true", "yes", "on"].includes(envRaw.trim().toLowerCase());
}

function parseCsv(raw: string | undefined): string[] {
  if (!raw) return [];
  return raw
    .split(",")
    .map((value) => value.trim())
    .filter((value) => value.length > 0);
}

function normalizeTargetId(raw: string): SyncBenchTargetId | null {
  const value = raw.trim();
  if (value === "direct") return "direct";
  if (
    value === "local" ||
    value === "local-server" ||
    value === "local-postgres" ||
    value === "local-postgres-sync-server"
  ) {
    return "local-postgres-sync-server";
  }
  if (
    value === "remote" ||
    value === "remote-server" ||
    value === "remote-sync" ||
    value === "remote-sync-server"
  ) {
    return "remote-sync-server";
  }
  return null;
}

function parseTargets(argv: string[]): SyncBenchTargetId[] {
  const raw =
    parseFlagValue(argv, "--targets") ??
    parseFlagValue(argv, "--target") ??
    process.env.SYNC_BENCH_TARGETS ??
    process.env.SYNC_BENCH_TARGET;
  if (!raw) return Array.from(DEFAULT_TARGETS);

  const seen = new Set<SyncBenchTargetId>();
  for (const value of parseCsv(raw)) {
    const normalized = normalizeTargetId(value);
    if (!normalized) {
      throw new Error(
        `invalid sync bench target "${value}", expected one of: direct, local, remote (${ALL_TARGETS.join(", ")})`
      );
    }
    seen.add(normalized);
  }
  return seen.size > 0 ? Array.from(seen) : Array.from(DEFAULT_TARGETS);
}

function parseStorages(argv: string[]): StorageKind[] {
  const raw =
    parseFlagValue(argv, "--storages") ??
    parseFlagValue(argv, "--storage") ??
    process.env.SYNC_BENCH_STORAGES ??
    process.env.SYNC_BENCH_STORAGE;
  if (!raw) return Array.from(DEFAULT_STORAGES);

  const seen = new Set<StorageKind>();
  for (const value of parseCsv(raw)) {
    if (value !== "memory" && value !== "file") {
      throw new Error(`invalid sync bench storage "${value}", expected one of: memory, file`);
    }
    seen.add(value);
  }
  return seen.size > 0 ? Array.from(seen) : Array.from(DEFAULT_STORAGES);
}

function parseWorkloads(argv: string[]): SyncBenchWorkload[] {
  const raw =
    parseFlagValue(argv, "--workloads") ??
    parseFlagValue(argv, "--workload") ??
    process.env.SYNC_BENCH_WORKLOADS ??
    process.env.SYNC_BENCH_WORKLOAD;
  if (!raw) return Array.from(DEFAULT_SYNC_BENCH_WORKLOADS);

  const seen = new Set<SyncBenchWorkload>();
  for (const value of parseCsv(raw)) {
    if (!(ALL_WORKLOADS as readonly string[]).includes(value)) {
      throw new Error(
        `invalid sync bench workload "${value}", expected one of: ${ALL_WORKLOADS.join(", ")}`
      );
    }
    seen.add(value as SyncBenchWorkload);
  }
  return seen.size > 0 ? Array.from(seen) : Array.from(ALL_WORKLOADS);
}

function parseFanout(argv: string[]): number {
  return parsePositiveIntFlag(argv, "--fanout", "SYNC_BENCH_FANOUT", DEFAULT_SYNC_BENCH_FANOUT);
}

function parseFirstView(argv: string[]): boolean {
  return parseBooleanFlag(argv, "--first-view", "SYNC_BENCH_FIRST_VIEW");
}

function normalizeSyncServerUrl(raw: string, docId: string): URL {
  let input = raw.trim();
  if (input.length === 0) throw new Error("Sync server URL is empty");
  if (!/^[a-zA-Z][a-zA-Z0-9+.-]*:/.test(input)) input = `ws://${input}`;

  const url = new URL(input);
  if (url.protocol === "http:") url.protocol = "ws:";
  if (url.protocol === "https:") url.protocol = "wss:";
  if (url.protocol !== "ws:" && url.protocol !== "wss:") {
    throw new Error("Sync server URL must use ws://, wss://, http://, or https://");
  }
  if (url.pathname === "/" || url.pathname.length === 0) {
    url.pathname = "/sync";
  }
  url.searchParams.set("docId", docId);
  return url;
}

function hexToBytes(hex: string): Uint8Array {
  return nodeIdToBytes16(hex);
}

function countOps(db: Database.Database): number {
  return (db.prepare("SELECT COUNT(*) AS cnt FROM ops").get() as { cnt: number }).cnt;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function closeWebSocket(ws: WebSocket): Promise<void> {
  if (ws.readyState === WebSocket.CLOSED) return;
  await new Promise<void>((resolve) => {
    let settled = false;
    const finish = () => {
      if (settled) return;
      settled = true;
      resolve();
    };
    ws.once("close", finish);
    ws.once("error", finish);
    try {
      ws.close();
    } catch {
      finish();
    }
    setTimeout(finish, 1_000);
  });
}

async function openWebSocket(url: URL): Promise<WebSocket> {
  return await new Promise<WebSocket>((resolve, reject) => {
    const ws = new WebSocket(url);
    let settled = false;

    const finish = (
      fn: (value: WebSocket | Error) => void,
      value: WebSocket | Error
    ) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      ws.off("open", onOpen);
      ws.off("error", onError);
      fn(value);
    };

    const onOpen = () => finish(resolve, ws);
    const onError = (error: Error) => {
      void closeWebSocket(ws);
      finish(reject, error);
    };

    const timer = setTimeout(() => {
      void closeWebSocket(ws);
      finish(reject, new Error(`timed out connecting to ${url.toString()}`));
    }, 5_000);

    ws.once("open", onOpen);
    ws.once("error", onError);
  });
}

function createNodeWebSocketTransport(
  ws: WebSocket
): DuplexTransport<Uint8Array> {
  return {
    send: async (bytes) =>
      await new Promise<void>((resolve, reject) => {
        ws.send(bytes, { binary: true }, (error) => {
          if (!error) {
            resolve();
            return;
          }
          reject(error instanceof Error ? error : new Error(String(error)));
        });
      }),
    onMessage: (handler) => {
      const onMessage = (data: WebSocket.RawData) => {
        if (data instanceof Uint8Array) {
          handler(data);
        } else if (data instanceof ArrayBuffer) {
          handler(new Uint8Array(data));
        } else if (Array.isArray(data)) {
          handler(Buffer.concat(data));
        } else {
          handler(Buffer.from(data));
        }
      };
      ws.on("message", onMessage);
      return () => ws.off("message", onMessage);
    },
  };
}

async function makeBackend(opts: {
  db: Database.Database;
  docId: string;
  initialMaxLamport: number;
}): Promise<FlushableSyncBackend<Operation>> {
  const client = await createTreecrdtClient(opts.db, { docId: opts.docId });
  const backend = createTreecrdtSyncBackendFromClient(client, opts.docId);

  return makeQueuedSyncBackend<Operation>({
    docId: opts.docId,
    initialMaxLamport: opts.initialMaxLamport,
    maxLamportFromOps: maxLamport,
    listOpRefs: backend.listOpRefs,
    getOpsByOpRefs: backend.getOpsByOpRefs,
    applyOps: backend.applyOps,
  });
}

async function openDb(opts: {
  storage: StorageKind;
  dbPath?: string;
  docId: string;
}): Promise<Database.Database> {
  const db = new Database(
    opts.storage === "memory" ? ":memory:" : opts.dbPath ?? ":memory:"
  );
  loadTreecrdtExtension(db);
  await createSqliteNodeApi(db).setDocId(opts.docId);
  return db;
}

async function appendInitialOps(
  db: Database.Database,
  ops: Operation[]
): Promise<void> {
  if (ops.length === 0) return;
  const api = createSqliteNodeApi(db);
  await api.appendOps!(
    ops,
    hexToBytes,
    (replica) => (typeof replica === "string" ? Buffer.from(replica) : replica)
  );
}

async function measureFirstViewAfterSync(
  db: Database.Database,
  docId: string,
  firstView: NonNullable<ReturnType<typeof buildSyncBenchCase>["firstView"]>
): Promise<number> {
  const client = await createTreecrdtClient(db, { docId });
  const expectedChildren = Math.min(firstView.expectedChildren, firstView.pageSize);
  const startedAt = performance.now();
  const rows = await client.tree.childrenPage(firstView.parent, null, firstView.pageSize);
  if (!Array.isArray(rows) || rows.length !== expectedChildren) {
    throw new Error(`expected ${expectedChildren} child rows after sync, got ${Array.isArray(rows) ? rows.length : "non-array"}`);
  }

  if (firstView.includePayloads) {
    const parentPayload = await client.tree.getPayload(firstView.parent);
    if (!(parentPayload instanceof Uint8Array) || parentPayload.length !== firstView.payloadBytes) {
      throw new Error("expected scope-root payload to be present after sync");
    }
    const payloads = await Promise.all(rows.map((row: { node: string }) => client.tree.getPayload(row.node)));
    if (payloads.some((payload) => !(payload instanceof Uint8Array) || payload.length !== firstView.payloadBytes)) {
      throw new Error("expected all first-view child payloads to be present after sync");
    }
  }

  return performance.now() - startedAt;
}

async function connectToSyncServer(
  baseUrl: string,
  docId: string
): Promise<SyncBenchConnection> {
  const url = normalizeSyncServerUrl(baseUrl, docId);
  const ws = await openWebSocket(url);
  const wire = createNodeWebSocketTransport(ws);
  const transport = wrapDuplexTransportWithCodec<Uint8Array, any>(
    wire,
    treecrdtSyncV0ProtobufCodec as any
  );
  return {
    transport,
    close: async () => {
      await closeWebSocket(ws);
    },
  };
}

async function createLocalPostgresSyncServerTarget(
  repoRoot: string,
  postgresUrl: string
): Promise<SyncBenchTargetRuntime> {
  const port = await findFreePort();
  const backendModule = path.join(
    repoRoot,
    "packages",
    "treecrdt-postgres-napi",
    "dist",
    "index.js"
  );

  const server = await startSyncServer({
    host: "127.0.0.1",
    port,
    postgresUrl,
    backendModule,
    allowDocCreate: true,
    enablePgNotify: false,
  });

  return {
    id: "local-postgres-sync-server",
    connect: async (docId) =>
      await connectToSyncServer(`ws://127.0.0.1:${server.port}`, docId),
    close: async () => {
      await server.close();
    },
  };
}

async function findFreePort(): Promise<number> {
  return await new Promise<number>((resolve, reject) => {
    const server = net.createServer();
    server.unref();
    server.on("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      if (!address || typeof address === "string") {
        server.close(() => reject(new Error("failed to resolve an ephemeral port")));
        return;
      }
      const { port } = address;
      server.close((error) => {
        if (error) {
          reject(error);
          return;
        }
        resolve(port);
      });
    });
  });
}

function createRemoteSyncServerTarget(
  baseUrl: string
): SyncBenchTargetRuntime {
  return {
    id: "remote-sync-server",
    connect: async (docId) => await connectToSyncServer(baseUrl, docId),
    close: async () => {},
  };
}

async function prepareTargetRuntimes(
  repoRoot: string,
  argv: string[],
  targets: SyncBenchTargetId[]
): Promise<Map<Exclude<SyncBenchTargetId, "direct">, SyncBenchTargetRuntime>> {
  const runtimes = new Map<
    Exclude<SyncBenchTargetId, "direct">,
    SyncBenchTargetRuntime
  >();

  if (targets.includes("local-postgres-sync-server")) {
    const postgresUrl =
      parseFlagValue(argv, "--postgres-url") ?? process.env.TREECRDT_POSTGRES_URL;
    if (!postgresUrl) {
      throw new Error(
        "local-postgres-sync-server target requires TREECRDT_POSTGRES_URL or --postgres-url=..."
      );
    }
    runtimes.set(
      "local-postgres-sync-server",
      await createLocalPostgresSyncServerTarget(repoRoot, postgresUrl)
    );
  }

  if (targets.includes("remote-sync-server")) {
    const remoteUrl =
      parseFlagValue(argv, "--sync-server-url") ??
      process.env.TREECRDT_SYNC_SERVER_URL;
    if (!remoteUrl) {
      throw new Error(
        "remote-sync-server target requires TREECRDT_SYNC_SERVER_URL or --sync-server-url=..."
      );
    }
    runtimes.set("remote-sync-server", createRemoteSyncServerTarget(remoteUrl));
  }

  return runtimes;
}

async function closeTargetRuntimes(
  runtimes: Map<Exclude<SyncBenchTargetId, "direct">, SyncBenchTargetRuntime>
): Promise<void> {
  await Promise.allSettled(Array.from(runtimes.values(), (runtime) => runtime.close()));
}

async function syncBackendThroughServer(
  runtime: SyncBenchTargetRuntime,
  docId: string,
  backend: FlushableSyncBackend<Operation>,
  filter: Filter
): Promise<void> {
  const peer = new SyncPeer<Operation>(backend, {
    maxCodewords: SYNC_BENCH_DEFAULT_MAX_CODEWORDS,
  });
  const connection = await runtime.connect(docId);
  const detach = peer.attach(connection.transport);

  try {
    await peer.syncOnce(connection.transport, filter, {
      maxCodewords: SYNC_BENCH_DEFAULT_MAX_CODEWORDS,
      codewordsPerMessage: SYNC_BENCH_DEFAULT_CODEWORDS_PER_MESSAGE,
    });
    await backend.flush();
  } finally {
    detach();
    await connection.close();
  }
}

async function seedServerState(
  runtime: SyncBenchTargetRuntime,
  docId: string,
  ops: Operation[]
): Promise<void> {
  if (ops.length === 0) return;

  const seedDb = await openDb({ storage: "memory", docId });
  try {
    await appendInitialOps(seedDb, ops);
    const seedBackend = await makeBackend({
      db: seedDb,
      docId,
      initialMaxLamport: maxLamport(ops),
    });
    await syncBackendThroughServer(runtime, docId, seedBackend, { all: {} });
  } finally {
    seedDb.close();
  }
}

async function waitForServerOpCount(
  runtime: SyncBenchTargetRuntime,
  docId: string,
  expectedCount: number
): Promise<void> {
  const deadline = Date.now() + SERVER_READY_TIMEOUT_MS;
  while (true) {
    const verifierDb = await openDb({ storage: "memory", docId });
    try {
      const verifierBackend = await makeBackend({
        db: verifierDb,
        docId,
        initialMaxLamport: 0,
      });
      await syncBackendThroughServer(runtime, docId, verifierBackend, { all: {} });
      if (countOps(verifierDb) === expectedCount) {
        return;
      }
    } finally {
      verifierDb.close();
    }

    if (Date.now() >= deadline) {
      throw new Error(
        `timed out waiting for server doc ${docId} to reach ${expectedCount} ops`
      );
    }
    await sleep(SERVER_READY_POLL_MS);
  }
}

async function openClientDbForRun(
  repoRoot: string,
  storage: StorageKind,
  docId: string,
  runId: string,
  workload: SyncBenchWorkload,
  size: number
): Promise<{ db: Database.Database; cleanup: () => Promise<void> }> {
  const outDir = path.join(repoRoot, "tmp", "sqlite-node-sync-bench");
  const dbPath =
    storage === "file"
      ? path.join(outDir, `${runId}-${workload}-${size}-${docId}.db`)
      : undefined;
  if (storage === "file") {
    await fs.mkdir(outDir, { recursive: true });
  }

  const db = await openDb({ storage, dbPath, docId });
  return {
    db,
    cleanup: async () => {
      db.close();
      if (dbPath) {
        await fs.rm(dbPath).catch(() => {});
      }
    },
  };
}

async function runBenchOnceDirect(
  repoRoot: string,
  { storage, workload, size }: BenchCase,
  bench: ReturnType<typeof buildSyncBenchCase>,
  includeFirstView: boolean
): Promise<SyncBenchSample> {
  const runId = crypto.randomUUID();
  const docId = `sqlite-node-sync-bench-${runId}`;
  const clientA = await openClientDbForRun(
    repoRoot,
    storage,
    docId,
    `${runId}-a`,
    workload,
    size
  );
  const clientB = await openClientDbForRun(
    repoRoot,
    storage,
    docId,
    `${runId}-b`,
    workload,
    size
  );

  try {
    await Promise.all([
      appendInitialOps(clientA.db, bench.opsA),
      appendInitialOps(clientB.db, bench.opsB),
    ]);

    const backendA = await makeBackend({
      db: clientA.db,
      docId,
      initialMaxLamport: maxLamport(bench.opsA),
    });
    const backendB = await makeBackend({
      db: clientB.db,
      docId,
      initialMaxLamport: maxLamport(bench.opsB),
    });

    const { peerA: pa, transportA: ta, detach } = createInMemoryConnectedPeers({
      backendA,
      backendB,
      codec: treecrdtSyncV0ProtobufCodec,
      peerOptions: { maxCodewords: SYNC_BENCH_DEFAULT_MAX_CODEWORDS },
    });

    try {
      const start = performance.now();
      await pa.syncOnce(ta, bench.filter as Filter, {
        maxCodewords: SYNC_BENCH_DEFAULT_MAX_CODEWORDS,
        codewordsPerMessage: SYNC_BENCH_DEFAULT_CODEWORDS_PER_MESSAGE,
      });
      await Promise.all([backendA.flush(), backendB.flush()]);
      const syncedAt = performance.now();

      let firstViewReadMs = 0;
      if (includeFirstView) {
        if (!bench.firstView) {
          throw new Error(`sync bench workload ${bench.name} does not define a first-view read path`);
        }
        firstViewReadMs = await measureFirstViewAfterSync(clientA.db, docId, bench.firstView);
      }

      const countA = countOps(clientA.db);
      const countB = countOps(clientB.db);
      if (
        countA !== bench.expectedFinalOpsA ||
        countB !== bench.expectedFinalOpsB
      ) {
        throw new Error(
          `sync bench mismatch: expected a=${bench.expectedFinalOpsA} b=${bench.expectedFinalOpsB}, got a=${countA} b=${countB}`
        );
      }

      return {
        totalMs: syncedAt - start + firstViewReadMs,
        syncMs: syncedAt - start,
        firstViewReadMs,
      };
    } finally {
      detach();
    }
  } finally {
    await Promise.all([clientA.cleanup(), clientB.cleanup()]);
  }
}

async function runBenchOnceViaServer(
  repoRoot: string,
  runtime: SyncBenchTargetRuntime,
  { storage, workload, size }: BenchCase,
  bench: ReturnType<typeof buildSyncBenchCase>,
  includeFirstView: boolean
): Promise<SyncBenchSample> {
  const runId = crypto.randomUUID();
  const docId = `sqlite-node-sync-bench-${runtime.id}-${runId}`;
  const client = await openClientDbForRun(
    repoRoot,
    storage,
    docId,
    runId,
    workload,
    size
  );

  try {
    await appendInitialOps(client.db, bench.opsA);
    await seedServerState(runtime, docId, bench.opsB);
    await waitForServerOpCount(runtime, docId, bench.opsB.length);

    const clientBackend = await makeBackend({
      db: client.db,
      docId,
      initialMaxLamport: maxLamport(bench.opsA),
    });

    const peer = new SyncPeer<Operation>(clientBackend, {
      maxCodewords: SYNC_BENCH_DEFAULT_MAX_CODEWORDS,
    });
    const connection = await runtime.connect(docId);
    const detach = peer.attach(connection.transport);

    try {
      const start = performance.now();
      await peer.syncOnce(connection.transport, bench.filter as Filter, {
        maxCodewords: SYNC_BENCH_DEFAULT_MAX_CODEWORDS,
        codewordsPerMessage: SYNC_BENCH_DEFAULT_CODEWORDS_PER_MESSAGE,
      });
      await clientBackend.flush();
      const syncedAt = performance.now();

      let firstViewReadMs = 0;
      if (includeFirstView) {
        if (!bench.firstView) {
          throw new Error(`sync bench workload ${bench.name} does not define a first-view read path`);
        }
        firstViewReadMs = await measureFirstViewAfterSync(client.db, docId, bench.firstView);
      }

      const countA = countOps(client.db);
      if (countA !== bench.expectedFinalOpsA) {
        throw new Error(
          `sync bench mismatch: expected client=${bench.expectedFinalOpsA}, got client=${countA}`
        );
      }

      return {
        totalMs: syncedAt - start + firstViewReadMs,
        syncMs: syncedAt - start,
        firstViewReadMs,
      };
    } finally {
      detach();
      await connection.close();
    }
  } finally {
    await client.cleanup();
  }
}

async function runBenchCase(
  repoRoot: string,
  benchCase: BenchCase,
  runtimes: Map<Exclude<SyncBenchTargetId, "direct">, SyncBenchTargetRuntime>,
  includeFirstView: boolean
): Promise<SyncBenchResult> {
  const bench = buildSyncBenchCase({
    workload: benchCase.workload,
    size: benchCase.size,
    fanout: benchCase.fanout,
  });
  const { iterations } = benchCase;

  const runtime =
    benchCase.target === "direct" ? null : runtimes.get(benchCase.target);
  if (benchCase.target !== "direct" && !runtime) {
    throw new Error(`missing runtime for sync bench target ${benchCase.target}`);
  }

  if (includeFirstView && !bench.firstView) {
    throw new Error(`sync bench workload ${bench.name} does not support --first-view`);
  }

  const samples: SyncBenchSample[] = [];
  for (let i = 0; i < iterations; i += 1) {
    samples.push(
      runtime
        ? await runBenchOnceViaServer(repoRoot, runtime, benchCase, bench, includeFirstView)
        : await runBenchOnceDirect(repoRoot, benchCase, bench, includeFirstView)
    );
  }

  const totalSamplesMs = samples.map((sample) => sample.totalMs);
  const syncSamplesMs = samples.map((sample) => sample.syncMs);
  const firstViewReadSamplesMs = samples.map((sample) => sample.firstViewReadMs);
  const durationMs =
    iterations > 1 ? quantile(totalSamplesMs, 0.5) : totalSamplesMs[0] ?? 0;
  const opsPerSec = durationMs > 0 ? (bench.totalOps / durationMs) * 1000 : Infinity;

  return {
    name: includeFirstView ? `${bench.name}-first-view` : bench.name,
    totalOps: bench.totalOps,
    durationMs,
    opsPerSec,
    extra: {
      ...bench.extra,
      count: benchCase.size,
      fanout: benchCase.fanout,
      mode: benchCase.target,
      target: benchCase.target,
      transport: benchCase.target === "direct" ? "in-memory" : "websocket",
      server:
        benchCase.target === "local-postgres-sync-server"
          ? "postgres-local"
          : benchCase.target === "remote-sync-server"
            ? "remote"
            : "none",
      measurement: includeFirstView ? "time-to-first-view" : "sync-only",
      codewordsPerMessage: SYNC_BENCH_DEFAULT_CODEWORDS_PER_MESSAGE,
      maxCodewords: SYNC_BENCH_DEFAULT_MAX_CODEWORDS,
      iterations: iterations > 1 ? iterations : undefined,
      avgDurationMs: iterations > 1 ? durationMs : undefined,
      samplesMs: totalSamplesMs,
      syncSamplesMs,
      firstViewReadSamplesMs: includeFirstView ? firstViewReadSamplesMs : undefined,
      syncMedianMs: quantile(syncSamplesMs, 0.5),
      firstViewReadMedianMs: includeFirstView ? quantile(firstViewReadSamplesMs, 0.5) : undefined,
      p95Ms: quantile(totalSamplesMs, 0.95),
      minMs: Math.min(...totalSamplesMs),
      maxMs: Math.max(...totalSamplesMs),
    },
  };
}

async function main() {
  const argv = process.argv.slice(2);
  const repoRoot = repoRootFromImportMeta(import.meta.url, 3);

  const config = parseConfigFromArgv(argv) ?? [...SYNC_BENCH_CONFIG];
  const rootConfig = [...SYNC_BENCH_ROOT_CONFIG];
  const targets = parseTargets(argv);
  const storages = parseStorages(argv);
  const workloads = parseWorkloads(argv);
  const fanout = parseFanout(argv);
  const includeFirstView = parseFirstView(argv);
  const runtimes = await prepareTargetRuntimes(repoRoot, argv, targets);

  try {
    const cases: BenchCase[] = [];
    for (const target of targets) {
      for (const storage of storages) {
        for (const workload of workloads) {
          const entries =
            workload === "sync-root-children-fanout10" ? rootConfig : config;
          for (const [size, iterations] of entries) {
            cases.push({ target, storage, workload, size, iterations, fanout });
          }
        }
      }
    }

    for (const benchCase of cases) {
      const result = await runBenchCase(repoRoot, benchCase, runtimes, includeFirstView);
      const outFile = path.join(
        repoRoot,
        "benchmarks",
        "sqlite-node-sync",
        `${benchCase.storage}-${benchCase.target}-${result.name}.json`
      );
      const payload = await writeResult(result, {
        implementation: "sqlite-node",
        storage: benchCase.storage,
        workload: result.name,
        outFile,
        extra: {
          count: benchCase.size,
          target: benchCase.target,
          ...result.extra,
        },
      });
      console.log(JSON.stringify(payload));
    }
  } finally {
    await closeTargetRuntimes(runtimes);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});

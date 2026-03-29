import { spawn } from 'node:child_process';
import { createHash } from 'node:crypto';
import { setMaxListeners } from 'node:events';
import fs from 'node:fs/promises';
import net from 'node:net';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import Database from 'better-sqlite3';
import WebSocket from 'ws';

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
} from '@treecrdt/benchmark';
import { repoRootFromImportMeta, writeResult } from '@treecrdt/benchmark/node';
import type { Operation } from '@treecrdt/interface';
import { nodeIdToBytes16 } from '@treecrdt/interface/ids';
import { SyncPeer, type Filter, type SyncBackend } from '@treecrdt/sync';
import { makeQueuedSyncBackend, type FlushableSyncBackend } from '@treecrdt/sync/in-memory';
import { createTreecrdtSyncBackendFromClient } from '@treecrdt/sync-sqlite/backend';
import { treecrdtSyncV0ProtobufCodec } from '@treecrdt/sync/protobuf';
import {
  createInMemoryDuplex,
  wrapDuplexTransportWithCodec,
  type DuplexTransport,
} from '@treecrdt/sync/transport';
import { startSyncServer } from '@treecrdt/sync-server-postgres-node';

import { createTreecrdtClient, createSqliteNodeApi, loadTreecrdtExtension } from '../dist/index.js';
import {
  resetBackendProfiler,
  takeBackendProfilerSnapshot,
  wrapBackendWithProfiler,
} from './backend-profiler.mjs';

type StorageKind = 'memory' | 'file';
type ConfigEntry = [number, number];
type SyncBenchTargetId = 'direct' | 'local-postgres-sync-server' | 'remote-sync-server';
type ServerFixtureCacheMode = 'off' | 'reuse' | 'rebuild';

type BenchCase = {
  storage: StorageKind;
  target: SyncBenchTargetId;
  workload: SyncBenchWorkload;
  size: number;
  iterations: number;
  warmupIterations: number;
  fanout: number;
};

type SyncBenchResult = {
  name: string;
  totalOps: number;
  durationMs: number;
  opsPerSec: number;
  extra?: Record<string, unknown>;
};

type PrimedServerFixtureResult = {
  name: string;
  totalOps: number;
  durationMs: number;
  opsPerSec: number;
  extra?: Record<string, unknown>;
};

type SeedServerStateResult = {
  expectedFilterCount: number;
  uploadMs: number;
  allReadyMs: number;
};

type SyncBenchSample = {
  totalMs: number;
  syncMs: number;
  firstViewReadMs: number;
  backendProfile?: unknown;
  transportProfile?: TransportProfile;
  helloProfile?: HelloTraceProfile;
};

type PreparedServerFixture = {
  docId: string;
  cacheKey?: string;
  cacheStatus: 'disabled' | 'hit' | 'miss' | 'rebuild' | 'assumed';
  seedUploadMs?: number;
  seedAllReadyMs?: number;
  filterReadyMs?: number;
  totalPrepareMs?: number;
};

type SyncBenchConnection = {
  transport: DuplexTransport<any>;
  close: () => Promise<void>;
};

type SyncBenchTargetRuntime = {
  id: Exclude<SyncBenchTargetId, 'direct'>;
  serverProcess: 'child-process' | 'in-process' | 'remote';
  connect: (docId: string) => Promise<SyncBenchConnection>;
  seedOps?: (docId: string, ops: Operation[]) => Promise<void>;
  inspectDoc?: (docId: string) => Promise<{ allCount: number; maxLamport: number }>;
  resetDoc?: (docId: string) => Promise<void>;
  waitForOpCount?: (
    docId: string,
    filter: Filter,
    expectedCount: number,
    opts?: { timeoutMs?: number },
  ) => Promise<void>;
  clearHelloTrace?: (docId: string) => void;
  takeHelloTrace?: (docId: string) => HelloTraceProfile | undefined;
  close: () => Promise<void>;
};

type TransportDirectionProfile = {
  messages: number;
  bytes: number;
  codewords: number;
  ops: number;
  byCase: Record<string, number>;
};

type TransportProfile = {
  sent: TransportDirectionProfile;
  received: TransportDirectionProfile;
  events: TransportProfileEvent[];
};

type TransportProfileEvent = {
  direction: 'sent' | 'received';
  case: string;
  atMs: number;
  bytes: number;
  codewords: number;
  ops: number;
};

type HelloTraceRecord = {
  type: 'sync-hello-trace';
  docId: string;
  stage: string;
  ms: number;
} & Record<string, unknown>;

type HelloTraceProfileEvent = {
  stage: string;
  atMs: number;
  deltaMs: number;
  meta?: Record<string, unknown>;
};

type HelloTraceProfile = {
  totalMs: number;
  events: HelloTraceProfileEvent[];
};

type HelloTraceStageSummary = Record<
  string,
  {
    count: number;
    medianAtMs: number;
    p95AtMs: number;
    medianDeltaMs: number;
    p95DeltaMs: number;
    minDeltaMs: number;
    maxDeltaMs: number;
  }
>;

type HelloTraceStore = {
  clear: (docId: string) => void;
  take: (docId: string) => HelloTraceProfile | undefined;
  dispose?: () => void;
};

const SYNC_BENCH_CONFIG: ReadonlyArray<ConfigEntry> = [
  [100, 10],
  [1_000, 5],
  [10_000, 10],
];

const SYNC_BENCH_ROOT_CONFIG: ReadonlyArray<ConfigEntry> = [[1110, 10]];

const DEFAULT_TARGETS: readonly SyncBenchTargetId[] = ['direct'];
const ALL_TARGETS: readonly SyncBenchTargetId[] = [
  'direct',
  'local-postgres-sync-server',
  'remote-sync-server',
];
const DEFAULT_STORAGES: readonly StorageKind[] = ['memory', 'file'];
const ALL_WORKLOADS: readonly SyncBenchWorkload[] = [
  ...ALL_SYNC_BENCH_WORKLOADS,
  ...DEFAULT_SYNC_BENCH_ROOT_CHILDREN_WORKLOADS,
];
const SYNC_SERVER_START_TIMEOUT_MS = Math.max(
  1_000,
  envInt('SYNC_BENCH_SERVER_START_TIMEOUT_MS') ?? 10_000,
);
const SERVER_READY_TIMEOUT_MS = Math.max(
  1_000,
  envInt('SYNC_BENCH_SERVER_READY_TIMEOUT_MS') ?? 10_000,
);
const SERVER_SEED_READY_TIMEOUT_MS = Math.max(
  SERVER_READY_TIMEOUT_MS,
  envInt('SYNC_BENCH_SEED_READY_TIMEOUT_MS') ?? 60_000,
);
const SYNC_BENCH_SERVER_MAX_CODEWORDS = Math.max(
  SYNC_BENCH_DEFAULT_MAX_CODEWORDS,
  envInt('SYNC_BENCH_SERVER_MAX_CODEWORDS') ?? 1_000_000,
);
const SYNC_BENCH_SEED_MAX_CODEWORDS = Math.max(
  SYNC_BENCH_SERVER_MAX_CODEWORDS,
  envInt('SYNC_BENCH_SEED_MAX_CODEWORDS') ?? SYNC_BENCH_SERVER_MAX_CODEWORDS,
);
const SYNC_BENCH_DIRECT_SEND_THRESHOLD = Math.max(
  0,
  envInt('SYNC_BENCH_DIRECT_SEND_THRESHOLD') ?? 0,
);
const SYNC_BENCH_MAX_OPS_PER_BATCH = envInt('SYNC_BENCH_MAX_OPS_PER_BATCH');
const SYNC_BENCH_POST_SEED_WAIT_MS = Math.max(0, envInt('SYNC_BENCH_POST_SEED_WAIT_MS') ?? 0);
const DEFAULT_SERVER_FIXTURE_CACHE_MODE: ServerFixtureCacheMode = 'reuse';
const SYNC_BENCH_SERVER_FIXTURE_CACHE_VERSION = '2026-03-21-v1';
const SERVER_READY_POLL_MS = 100;

function envInt(name: string): number | undefined {
  const raw = process.env[name];
  if (raw == null || raw === '') return undefined;
  const n = Number(raw);
  return Number.isFinite(n) ? n : undefined;
}

function recommendedIterationsForCustomSize(size: number): number {
  if (size <= 100) return 10;
  if (size <= 1_000) return 7;
  if (size <= 10_000) return 5;
  return 3;
}

function parseIterationsOverride(argv: string[]): number | undefined {
  return parseOptionalPositiveIntFlag(argv, '--iterations', [
    'SYNC_BENCH_ITERATIONS',
    'BENCH_ITERATIONS',
  ]);
}

function parseWarmupIterationsOverride(argv: string[]): number | undefined {
  return parseOptionalNonNegativeIntFlag(argv, '--warmup', ['SYNC_BENCH_WARMUP', 'BENCH_WARMUP']);
}

function resolveWarmupIterations(iterations: number, explicitWarmupIterations?: number): number {
  if (explicitWarmupIterations != null) return explicitWarmupIterations;
  return iterations > 1 ? 1 : 0;
}

function parseConfigFromArgv(
  argv: string[],
  iterationsOverride?: number,
): Array<ConfigEntry> | null {
  let customConfig: Array<ConfigEntry> | null = null;
  for (const arg of argv) {
    if (arg.startsWith('--count=')) {
      const val = arg.slice('--count='.length).trim();
      const count = val ? Number(val) : 500;
      const normalizedCount = Number.isFinite(count) && count > 0 ? count : 500;
      customConfig = [
        [
          normalizedCount,
          iterationsOverride ?? recommendedIterationsForCustomSize(normalizedCount),
        ],
      ];
      break;
    }
    if (arg.startsWith('--counts=')) {
      const vals = arg
        .slice('--counts='.length)
        .split(',')
        .map((s) => s.trim())
        .filter((s) => s.length > 0);
      const parsed = vals
        .map((s) => {
          const n = Number(s);
          return Number.isFinite(n) && n > 0 ? n : null;
        })
        .filter((n): n is number => n != null)
        .map(
          (c) => [c, iterationsOverride ?? recommendedIterationsForCustomSize(c)] as ConfigEntry,
        );
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

function parsePositiveIntFlag(
  argv: string[],
  flag: string,
  envName: string,
  fallback: number,
): number {
  const raw = parseFlagValue(argv, flag) ?? process.env[envName];
  if (!raw) return fallback;
  const value = Number(raw);
  if (!Number.isInteger(value) || value <= 0) {
    throw new Error(`invalid ${flag} value "${raw}", expected a positive integer`);
  }
  return value;
}

function parseOptionalPositiveIntFlag(
  argv: string[],
  flag: string,
  envNames: readonly string[],
): number | undefined {
  const raw =
    parseFlagValue(argv, flag) ??
    envNames.map((name) => process.env[name]).find((value) => value != null && value !== '');
  if (!raw) return undefined;
  const value = Number(raw);
  if (!Number.isInteger(value) || value <= 0) {
    throw new Error(`invalid ${flag} value "${raw}", expected a positive integer`);
  }
  return value;
}

function parseOptionalNonNegativeIntFlag(
  argv: string[],
  flag: string,
  envNames: readonly string[],
): number | undefined {
  const raw =
    parseFlagValue(argv, flag) ??
    envNames.map((name) => process.env[name]).find((value) => value != null && value !== '');
  if (!raw) return undefined;
  const value = Number(raw);
  if (!Number.isInteger(value) || value < 0) {
    throw new Error(`invalid ${flag} value "${raw}", expected a non-negative integer`);
  }
  return value;
}

function parseBooleanFlag(argv: string[], flag: string, envName: string): boolean {
  const envRaw = process.env[envName];
  if (argv.includes(flag)) return true;
  if (envRaw == null || envRaw === '') return false;
  return ['1', 'true', 'yes', 'on'].includes(envRaw.trim().toLowerCase());
}

function parseCsv(raw: string | undefined): string[] {
  if (!raw) return [];
  return raw
    .split(',')
    .map((value) => value.trim())
    .filter((value) => value.length > 0);
}

function normalizeTargetId(raw: string): SyncBenchTargetId | null {
  const value = raw.trim();
  if (value === 'direct') return 'direct';
  if (
    value === 'local' ||
    value === 'local-server' ||
    value === 'local-postgres' ||
    value === 'local-postgres-sync-server'
  ) {
    return 'local-postgres-sync-server';
  }
  if (
    value === 'remote' ||
    value === 'remote-server' ||
    value === 'remote-sync' ||
    value === 'remote-sync-server'
  ) {
    return 'remote-sync-server';
  }
  return null;
}

function parseTargets(argv: string[]): SyncBenchTargetId[] {
  const raw =
    parseFlagValue(argv, '--targets') ??
    parseFlagValue(argv, '--target') ??
    process.env.SYNC_BENCH_TARGETS ??
    process.env.SYNC_BENCH_TARGET;
  if (!raw) return Array.from(DEFAULT_TARGETS);

  const seen = new Set<SyncBenchTargetId>();
  for (const value of parseCsv(raw)) {
    const normalized = normalizeTargetId(value);
    if (!normalized) {
      throw new Error(
        `invalid sync bench target "${value}", expected one of: direct, local, remote (${ALL_TARGETS.join(', ')})`,
      );
    }
    seen.add(normalized);
  }
  return seen.size > 0 ? Array.from(seen) : Array.from(DEFAULT_TARGETS);
}

function parseStorages(argv: string[]): StorageKind[] {
  const raw =
    parseFlagValue(argv, '--storages') ??
    parseFlagValue(argv, '--storage') ??
    process.env.SYNC_BENCH_STORAGES ??
    process.env.SYNC_BENCH_STORAGE;
  if (!raw) return Array.from(DEFAULT_STORAGES);

  const seen = new Set<StorageKind>();
  for (const value of parseCsv(raw)) {
    if (value !== 'memory' && value !== 'file') {
      throw new Error(`invalid sync bench storage "${value}", expected one of: memory, file`);
    }
    seen.add(value);
  }
  return seen.size > 0 ? Array.from(seen) : Array.from(DEFAULT_STORAGES);
}

function parseWorkloads(argv: string[]): SyncBenchWorkload[] {
  const raw =
    parseFlagValue(argv, '--workloads') ??
    parseFlagValue(argv, '--workload') ??
    process.env.SYNC_BENCH_WORKLOADS ??
    process.env.SYNC_BENCH_WORKLOAD;
  if (!raw) return Array.from(DEFAULT_SYNC_BENCH_WORKLOADS);

  const seen = new Set<SyncBenchWorkload>();
  for (const value of parseCsv(raw)) {
    if (!(ALL_WORKLOADS as readonly string[]).includes(value)) {
      throw new Error(
        `invalid sync bench workload "${value}", expected one of: ${ALL_WORKLOADS.join(', ')}`,
      );
    }
    seen.add(value as SyncBenchWorkload);
  }
  return seen.size > 0 ? Array.from(seen) : Array.from(ALL_WORKLOADS);
}

function parseFanout(argv: string[]): number {
  return parsePositiveIntFlag(argv, '--fanout', 'SYNC_BENCH_FANOUT', DEFAULT_SYNC_BENCH_FANOUT);
}

function parseFirstView(argv: string[]): boolean {
  return parseBooleanFlag(argv, '--first-view', 'SYNC_BENCH_FIRST_VIEW');
}

function parseProfileBackend(argv: string[]): boolean {
  return parseBooleanFlag(argv, '--profile-backend', 'SYNC_BENCH_PROFILE_BACKEND');
}

function parseProfileTransport(argv: string[]): boolean {
  return parseBooleanFlag(argv, '--profile-transport', 'SYNC_BENCH_PROFILE_TRANSPORT');
}

function parseProfileHello(argv: string[]): boolean {
  return parseBooleanFlag(argv, '--profile-hello', 'SYNC_BENCH_PROFILE_HELLO');
}

function parseDirectSendThreshold(argv: string[]): number {
  return (
    parseOptionalNonNegativeIntFlag(argv, '--direct-send-threshold', [
      'SYNC_BENCH_DIRECT_SEND_THRESHOLD',
    ]) ?? SYNC_BENCH_DIRECT_SEND_THRESHOLD
  );
}

function parseMaxOpsPerBatch(argv: string[]): number | undefined {
  return (
    parseOptionalPositiveIntFlag(argv, '--max-ops-per-batch', ['SYNC_BENCH_MAX_OPS_PER_BATCH']) ??
    SYNC_BENCH_MAX_OPS_PER_BATCH
  );
}

function parsePostSeedWaitMs(argv: string[]): number {
  return (
    parseOptionalNonNegativeIntFlag(argv, '--post-seed-wait-ms', [
      'SYNC_BENCH_POST_SEED_WAIT_MS',
    ]) ?? SYNC_BENCH_POST_SEED_WAIT_MS
  );
}

function parseServerFixtureCacheMode(argv: string[]): ServerFixtureCacheMode {
  const raw =
    parseFlagValue(argv, '--server-fixture-cache') ?? process.env.SYNC_BENCH_SERVER_FIXTURE_CACHE;
  if (!raw) return DEFAULT_SERVER_FIXTURE_CACHE_MODE;
  const value = raw.trim().toLowerCase();
  if (value === 'off' || value === 'reuse' || value === 'rebuild') {
    return value;
  }
  throw new Error(
    `invalid --server-fixture-cache value "${raw}", expected one of: off, reuse, rebuild`,
  );
}

function parsePrimeServerFixtures(argv: string[]): boolean {
  return parseBooleanFlag(argv, '--prime-server-fixtures', 'SYNC_BENCH_PRIME_SERVER_FIXTURES');
}

function createEmptyTransportDirectionProfile(): TransportDirectionProfile {
  return {
    messages: 0,
    bytes: 0,
    codewords: 0,
    ops: 0,
    byCase: {},
  };
}

function createEmptyTransportProfile(): TransportProfile {
  return {
    sent: createEmptyTransportDirectionProfile(),
    received: createEmptyTransportDirectionProfile(),
    events: [],
  };
}

function snapshotTransportProfile(profile: TransportProfile): TransportProfile {
  return {
    sent: {
      ...profile.sent,
      byCase: { ...profile.sent.byCase },
    },
    received: {
      ...profile.received,
      byCase: { ...profile.received.byCase },
    },
    events: profile.events.map((event) => ({ ...event })),
  };
}

function recordTransportMessage(
  profile: TransportProfile,
  directionName: 'sent' | 'received',
  direction: TransportDirectionProfile,
  message: any,
): void {
  direction.messages += 1;
  let bytes = 0;
  try {
    bytes = treecrdtSyncV0ProtobufCodec.encode(message).byteLength;
    direction.bytes += bytes;
  } catch {
    // best-effort; message counts are still useful if encoding fails
  }

  const payloadCase = typeof message?.payload?.case === 'string' ? message.payload.case : 'unknown';
  direction.byCase[payloadCase] = (direction.byCase[payloadCase] ?? 0) + 1;

  if (payloadCase === 'ribltCodewords') {
    const codewords = message?.payload?.value?.codewords;
    if (Array.isArray(codewords)) {
      direction.codewords += codewords.length;
    }
  }

  if (payloadCase === 'opsBatch') {
    const ops = message?.payload?.value?.ops;
    if (Array.isArray(ops)) {
      direction.ops += ops.length;
    }
  }

  if (profile.events.length < 128) {
    profile.events.push({
      direction: directionName,
      case: payloadCase,
      atMs: performance.now() - transportProfileStartTimes.get(profile)!,
      bytes,
      codewords:
        payloadCase === 'ribltCodewords' && Array.isArray(message?.payload?.value?.codewords)
          ? message.payload.value.codewords.length
          : 0,
      ops:
        payloadCase === 'opsBatch' && Array.isArray(message?.payload?.value?.ops)
          ? message.payload.value.ops.length
          : 0,
    });
  }
}

const transportProfileStartTimes = new WeakMap<TransportProfile, number>();

function createProfiledSyncTransport(transport: DuplexTransport<any>): {
  transport: DuplexTransport<any>;
  snapshot: () => TransportProfile;
} {
  const profile = createEmptyTransportProfile();
  transportProfileStartTimes.set(profile, performance.now());
  return {
    transport: {
      send: async (message) => {
        recordTransportMessage(profile, 'sent', profile.sent, message);
        await transport.send(message);
      },
      onMessage: (handler) =>
        transport.onMessage((message) => {
          recordTransportMessage(profile, 'received', profile.received, message);
          handler(message);
        }),
    },
    snapshot: () => snapshotTransportProfile(profile),
  };
}

const HELLO_TRACE_SINK_KEY = '__TREECRDT_SYNC_HELLO_TRACE_SINK__';

function normalizeHelloTraceRecord(value: unknown): HelloTraceRecord | null {
  if (!value || typeof value !== 'object') return null;
  const record = value as Record<string, unknown>;
  if (record.type !== 'sync-hello-trace') return null;
  if (
    typeof record.docId !== 'string' ||
    typeof record.stage !== 'string' ||
    typeof record.ms !== 'number'
  ) {
    return null;
  }
  return record as HelloTraceRecord;
}

function buildHelloTraceProfile(records: HelloTraceRecord[]): HelloTraceProfile | undefined {
  if (records.length === 0) return undefined;
  const sorted = records.slice().sort((a, b) => a.ms - b.ms);
  let previousMs = 0;
  return {
    totalMs: sorted[sorted.length - 1]!.ms,
    events: sorted.map((record, index) => {
      const { type: _type, docId: _docId, stage, ms, ...meta } = record;
      const deltaMs = index === 0 ? ms : ms - previousMs;
      previousMs = ms;
      return {
        stage,
        atMs: ms,
        deltaMs,
        meta: Object.keys(meta).length > 0 ? meta : undefined,
      };
    }),
  };
}

function summarizeHelloTraceProfiles(
  profiles: HelloTraceProfile[],
): HelloTraceStageSummary | undefined {
  if (profiles.length === 0) return undefined;
  const buckets = new Map<string, { atMs: number[]; deltaMs: number[] }>();
  for (const profile of profiles) {
    for (const event of profile.events) {
      const bucket = buckets.get(event.stage) ?? { atMs: [], deltaMs: [] };
      bucket.atMs.push(event.atMs);
      bucket.deltaMs.push(event.deltaMs);
      buckets.set(event.stage, bucket);
    }
  }

  return Object.fromEntries(
    Array.from(buckets.entries(), ([stage, values]) => [
      stage,
      {
        count: values.atMs.length,
        medianAtMs: quantile(values.atMs, 0.5),
        p95AtMs: quantile(values.atMs, 0.95),
        medianDeltaMs: quantile(values.deltaMs, 0.5),
        p95DeltaMs: quantile(values.deltaMs, 0.95),
        minDeltaMs: Math.min(...values.deltaMs),
        maxDeltaMs: Math.max(...values.deltaMs),
      },
    ]),
  );
}

function createChunkLineParser(onLine: (line: string) => void): {
  push: (chunk: Buffer | string) => void;
  flush: () => void;
} {
  let buffer = '';
  const handleLines = () => {
    while (true) {
      const newlineIndex = buffer.indexOf('\n');
      if (newlineIndex === -1) return;
      const line = buffer.slice(0, newlineIndex).trim();
      buffer = buffer.slice(newlineIndex + 1);
      if (line) onLine(line);
    }
  };

  return {
    push: (chunk) => {
      buffer += chunk.toString();
      handleLines();
    },
    flush: () => {
      const line = buffer.trim();
      buffer = '';
      if (line) onLine(line);
    },
  };
}

function createChildProcessHelloTraceStore(): HelloTraceStore & {
  pushStdout: (chunk: Buffer | string) => void;
  pushStderr: (chunk: Buffer | string) => void;
} {
  const traces = new Map<string, HelloTraceRecord[]>();
  const remember = (record: HelloTraceRecord) => {
    const current = traces.get(record.docId);
    if (current) {
      current.push(record);
      return;
    }
    traces.set(record.docId, [record]);
  };
  const onLine = (line: string) => {
    if (!line.includes('"sync-hello-trace"')) return;
    try {
      const parsed = normalizeHelloTraceRecord(JSON.parse(line));
      if (parsed) remember(parsed);
    } catch {
      // ignore non-JSON or partial debug output
    }
  };
  const stdoutParser = createChunkLineParser(onLine);
  const stderrParser = createChunkLineParser(onLine);

  return {
    pushStdout: stdoutParser.push,
    pushStderr: stderrParser.push,
    clear: (docId) => {
      traces.delete(docId);
    },
    take: (docId) => {
      const records = traces.get(docId) ?? [];
      traces.delete(docId);
      return buildHelloTraceProfile(records);
    },
    dispose: () => {
      stdoutParser.flush();
      stderrParser.flush();
      traces.clear();
    },
  };
}

function createProcessHelloTraceStore(): HelloTraceStore {
  const traces = new Map<string, HelloTraceRecord[]>();
  const root = globalThis as Record<string, unknown>;
  const previousSink = root[HELLO_TRACE_SINK_KEY];
  const nextSink = (record: HelloTraceRecord) => {
    if (typeof previousSink === 'function') {
      (previousSink as (record: HelloTraceRecord) => void)(record);
    }
    const current = traces.get(record.docId);
    if (current) {
      current.push(record);
      return;
    }
    traces.set(record.docId, [record]);
  };
  root[HELLO_TRACE_SINK_KEY] = nextSink;

  return {
    clear: (docId) => {
      traces.delete(docId);
    },
    take: (docId) => {
      const records = traces.get(docId) ?? [];
      traces.delete(docId);
      return buildHelloTraceProfile(records);
    },
    dispose: () => {
      if (previousSink === undefined) {
        delete root[HELLO_TRACE_SINK_KEY];
      } else {
        root[HELLO_TRACE_SINK_KEY] = previousSink;
      }
      traces.clear();
    },
  };
}

function normalizeSyncServerUrl(raw: string, docId: string): URL {
  let input = raw.trim();
  if (input.length === 0) throw new Error('Sync server URL is empty');
  if (!/^[a-zA-Z][a-zA-Z0-9+.-]*:/.test(input)) input = `ws://${input}`;

  const url = new URL(input);
  if (url.protocol === 'http:') url.protocol = 'ws:';
  if (url.protocol === 'https:') url.protocol = 'wss:';
  if (url.protocol !== 'ws:' && url.protocol !== 'wss:') {
    throw new Error('Sync server URL must use ws://, wss://, http://, or https://');
  }
  if (url.pathname === '/' || url.pathname.length === 0) {
    url.pathname = '/sync';
  }
  url.searchParams.set('docId', docId);
  return url;
}

function hexToBytes(hex: string): Uint8Array {
  return nodeIdToBytes16(hex);
}

function countOps(db: Database.Database): number {
  return (db.prepare('SELECT COUNT(*) AS cnt FROM ops').get() as { cnt: number }).cnt;
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
    ws.once('close', finish);
    ws.once('error', finish);
    try {
      ws.close();
    } catch {
      finish();
    }
    setTimeout(finish, 1_000);
  });
}

type BuiltinWebSocket = InstanceType<typeof globalThis.WebSocket>;

async function closeBuiltinWebSocket(ws: BuiltinWebSocket): Promise<void> {
  if (ws.readyState === globalThis.WebSocket.CLOSED) return;
  await new Promise<void>((resolve) => {
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

async function openWebSocket(url: URL): Promise<WebSocket> {
  return await new Promise<WebSocket>((resolve, reject) => {
    const ws = new WebSocket(url);
    let settled = false;

    const finish = (fn: (value: WebSocket | Error) => void, value: WebSocket | Error) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      ws.off('open', onOpen);
      ws.off('error', onError);
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

    ws.once('open', onOpen);
    ws.once('error', onError);
  });
}

async function openBuiltinWebSocket(url: URL): Promise<BuiltinWebSocket> {
  return await new Promise<BuiltinWebSocket>((resolve, reject) => {
    const WebSocketCtor = globalThis.WebSocket;
    if (typeof WebSocketCtor !== 'function') {
      reject(new Error('global WebSocket is not available in this Node runtime'));
      return;
    }

    const ws = new WebSocketCtor(url.toString());
    setMaxListeners(0, ws);
    ws.binaryType = 'arraybuffer';
    let settled = false;

    const finish = (
      fn: (value: BuiltinWebSocket | Error) => void,
      value: BuiltinWebSocket | Error,
    ) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      ws.removeEventListener('open', onOpen);
      ws.removeEventListener('error', onError);
      fn(value);
    };

    const onOpen = () => finish(resolve, ws);
    const onError = () => {
      finish(reject, new Error(`failed connecting to ${url.toString()}`));
    };

    const timer = setTimeout(() => {
      try {
        if (ws.readyState !== globalThis.WebSocket.CLOSING) {
          ws.close();
        }
      } catch {
        // Ignore close failures during connection timeout cleanup.
      }
      finish(reject, new Error(`timed out connecting to ${url.toString()}`));
    }, 5_000);

    ws.addEventListener('open', onOpen);
    ws.addEventListener('error', onError);
  });
}

function createNodeWebSocketTransport(ws: WebSocket): DuplexTransport<Uint8Array> {
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
      ws.on('message', onMessage);
      return () => ws.off('message', onMessage);
    },
  };
}

function createBuiltinWebSocketTransport(ws: BuiltinWebSocket): DuplexTransport<Uint8Array> {
  return {
    send: async (bytes) => {
      try {
        ws.send(bytes);
      } catch (error) {
        throw error instanceof Error ? error : new Error(String(error));
      }
    },
    onMessage: (handler) => {
      const onMessage = (event: { data: unknown }) => {
        const { data } = event;
        if (data instanceof Uint8Array) {
          handler(data);
        } else if (data instanceof ArrayBuffer) {
          handler(new Uint8Array(data));
        } else if (ArrayBuffer.isView(data)) {
          handler(new Uint8Array(data.buffer, data.byteOffset, data.byteLength));
        } else if (typeof data === 'string') {
          handler(Buffer.from(data));
        } else {
          throw new Error(`unsupported builtin WebSocket message type: ${typeof data}`);
        }
      };
      ws.addEventListener('message', onMessage as EventListener);
      return () => ws.removeEventListener('message', onMessage as EventListener);
    },
  };
}

async function makeBackend(opts: {
  db: Database.Database;
  docId: string;
  initialMaxLamport: number;
  profileLabel?: string;
}): Promise<FlushableSyncBackend<Operation>> {
  const client = await createTreecrdtClient(opts.db, { docId: opts.docId });
  const backend = createTreecrdtSyncBackendFromClient(client, opts.docId);
  const queued = makeQueuedSyncBackend<Operation>({
    docId: opts.docId,
    initialMaxLamport: opts.initialMaxLamport,
    maxLamportFromOps: maxLamport,
    listOpRefs: backend.listOpRefs,
    getOpsByOpRefs: backend.getOpsByOpRefs,
    applyOps: backend.applyOps,
  });
  if (!opts.profileLabel) return queued;
  return wrapBackendWithProfiler(queued, {
    docId: opts.docId,
    label: opts.profileLabel,
  }) as FlushableSyncBackend<Operation>;
}

async function openDb(opts: {
  storage: StorageKind;
  dbPath?: string;
  docId: string;
}): Promise<Database.Database> {
  const db = new Database(opts.storage === 'memory' ? ':memory:' : (opts.dbPath ?? ':memory:'));
  loadTreecrdtExtension(db);
  await createSqliteNodeApi(db).setDocId(opts.docId);
  return db;
}

async function appendInitialOps(db: Database.Database, ops: Operation[]): Promise<void> {
  if (ops.length === 0) return;
  const api = createSqliteNodeApi(db);
  await api.appendOps!(ops, hexToBytes, (replica) =>
    typeof replica === 'string' ? Buffer.from(replica) : replica,
  );
}

async function measureFirstViewAfterSync(
  db: Database.Database,
  docId: string,
  firstView: NonNullable<ReturnType<typeof buildSyncBenchCase>['firstView']>,
): Promise<number> {
  const client = await createTreecrdtClient(db, { docId });
  const expectedChildren = Math.min(firstView.expectedChildren, firstView.pageSize);
  const startedAt = performance.now();
  const rows = await client.tree.childrenPage(firstView.parent, null, firstView.pageSize);
  if (!Array.isArray(rows) || rows.length !== expectedChildren) {
    throw new Error(
      `expected ${expectedChildren} child rows after sync, got ${Array.isArray(rows) ? rows.length : 'non-array'}`,
    );
  }

  if (firstView.includePayloads) {
    const parentPayload = await client.tree.getPayload(firstView.parent);
    if (!(parentPayload instanceof Uint8Array) || parentPayload.length !== firstView.payloadBytes) {
      throw new Error('expected scope-root payload to be present after sync');
    }
    const payloads = await Promise.all(
      rows.map((row: { node: string }) => client.tree.getPayload(row.node)),
    );
    if (
      payloads.some(
        (payload) => !(payload instanceof Uint8Array) || payload.length !== firstView.payloadBytes,
      )
    ) {
      throw new Error('expected all first-view child payloads to be present after sync');
    }
  }

  return performance.now() - startedAt;
}

async function connectToSyncServer(
  baseUrl: string,
  docId: string,
  opts?: { client?: 'ws-package' | 'builtin' },
): Promise<SyncBenchConnection> {
  const url = normalizeSyncServerUrl(baseUrl, docId);
  const client = opts?.client ?? 'ws-package';
  const ws = client === 'builtin' ? await openBuiltinWebSocket(url) : await openWebSocket(url);
  const wire =
    client === 'builtin' ? createBuiltinWebSocketTransport(ws) : createNodeWebSocketTransport(ws);
  const transport = wrapDuplexTransportWithCodec<Uint8Array, any>(
    wire,
    treecrdtSyncV0ProtobufCodec as any,
  );
  return {
    transport,
    close: async () => {
      if (client === 'builtin') {
        await closeBuiltinWebSocket(ws);
      } else {
        await closeWebSocket(ws);
      }
    },
  };
}

async function createLocalPostgresSyncServerTarget(
  repoRoot: string,
  postgresUrl: string,
  profileBackend: boolean,
  profileHello: boolean,
  directSendThreshold: number,
): Promise<SyncBenchTargetRuntime> {
  const port = await findFreePort();
  const backendModule = profileBackend
    ? path.join(
        repoRoot,
        'packages',
        'treecrdt-sqlite-node',
        'scripts',
        'instrumented-postgres-backend-module.mjs',
      )
    : path.join(repoRoot, 'packages', 'treecrdt-postgres-napi', 'dist', 'index.js');

  const waitForOpCount = await createDirectPostgresOpCountWaiter(backendModule, postgresUrl);
  const backendFactory = await loadPostgresSyncBackendFactory(backendModule, postgresUrl);
  const inspectDoc = async (docId: string) => {
    const backend = await backendFactory.open(docId);
    const [allRefs, currentMaxLamport] = await Promise.all([
      backend.listOpRefs({ all: {} }),
      backend.maxLamport(),
    ]);
    return {
      allCount: allRefs.length,
      maxLamport: Number(currentMaxLamport),
    };
  };
  const resetDoc = async (docId: string) => {
    await backendFactory.resetDocForTests(docId);
  };
  const seedOps = async (docId: string, ops: Operation[]) => {
    if (ops.length === 0) return;
    const backend = await backendFactory.open(docId);
    await backend.applyOps(ops);
  };

  if (profileBackend) {
    const server = await startSyncServer({
      host: '127.0.0.1',
      port,
      postgresUrl,
      backendModule,
      maxCodewords: SYNC_BENCH_SERVER_MAX_CODEWORDS,
      ...(directSendThreshold > 0 ? { directSendThreshold } : {}),
      allowDocCreate: true,
      enablePgNotify: false,
    });

    return {
      id: 'local-postgres-sync-server',
      serverProcess: 'in-process',
      connect: async (docId) => await connectToSyncServer(`ws://127.0.0.1:${server.port}`, docId),
      seedOps,
      inspectDoc,
      resetDoc,
      waitForOpCount,
      close: async () => {
        await server.close();
      },
    };
  }

  const cliPath = path.join(
    repoRoot,
    'packages',
    'sync',
    'server',
    'postgres-node',
    'dist',
    'cli.js',
  );
  let recentOutput = '';
  const helloTraceStore = profileHello ? createChildProcessHelloTraceStore() : undefined;
  const child = spawn(process.execPath, [cliPath], {
    cwd: repoRoot,
    env: {
      ...process.env,
      HOST: '127.0.0.1',
      PORT: String(port),
      TREECRDT_POSTGRES_URL: postgresUrl,
      TREECRDT_POSTGRES_BACKEND_MODULE: backendModule,
      TREECRDT_SYNC_MAX_CODEWORDS: String(SYNC_BENCH_SERVER_MAX_CODEWORDS),
      TREECRDT_SYNC_DIRECT_SEND_THRESHOLD: String(directSendThreshold),
      TREECRDT_ALLOW_DOC_CREATE: 'true',
      TREECRDT_PG_NOTIFY_ENABLED: 'false',
      ...(profileHello ? { TREECRDT_SYNC_TRACE_HELLO: '1' } : {}),
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  const rememberOutput = (chunk: Buffer | string) => {
    recentOutput += chunk.toString();
    if (recentOutput.length > 8_000) {
      recentOutput = recentOutput.slice(-8_000);
    }
  };
  child.stdout?.on('data', (chunk) => {
    rememberOutput(chunk);
    helloTraceStore?.pushStdout(chunk);
  });
  child.stderr?.on('data', (chunk) => {
    rememberOutput(chunk);
    helloTraceStore?.pushStderr(chunk);
  });

  await waitForSyncServerReady(`http://127.0.0.1:${port}/health`, child, () => recentOutput);

  return {
    id: 'local-postgres-sync-server',
    serverProcess: 'child-process',
    connect: async (docId) => await connectToSyncServer(`ws://127.0.0.1:${port}`, docId),
    seedOps,
    inspectDoc,
    resetDoc,
    waitForOpCount,
    clearHelloTrace: helloTraceStore ? (docId) => helloTraceStore.clear(docId) : undefined,
    takeHelloTrace: helloTraceStore ? (docId) => helloTraceStore.take(docId) : undefined,
    close: async () => {
      helloTraceStore?.dispose?.();
      if (child.exitCode == null && child.signalCode == null) {
        child.kill('SIGTERM');
        await waitForProcessExit(child, 5_000);
      }
    },
  };
}

async function createDirectPostgresOpCountWaiter(
  backendModule: string,
  postgresUrl: string,
): Promise<
  (
    docId: string,
    filter: Filter,
    expectedCount: number,
    opts?: { timeoutMs?: number },
  ) => Promise<void>
> {
  const factory = await loadPostgresSyncBackendFactory(backendModule, postgresUrl);
  return async (
    docId: string,
    filter: Filter,
    expectedCount: number,
    opts?: { timeoutMs?: number },
  ) => {
    const timeoutMs = Math.max(1_000, opts?.timeoutMs ?? SERVER_READY_TIMEOUT_MS);
    const deadline = Date.now() + timeoutMs;
    let lastCount = -1;
    let lastError: unknown;
    while (Date.now() < deadline) {
      try {
        const backend = await factory.open(docId);
        const refs = await backend.listOpRefs(filter);
        lastCount = refs.length;
        if (refs.length === expectedCount) {
          return;
        }
      } catch (error) {
        lastError = error;
      }
      await sleep(SERVER_READY_POLL_MS);
    }
    throw new Error(
      `timed out waiting for server doc ${docId} to reach ${expectedCount} ops within ${timeoutMs}ms` +
        (lastCount >= 0 ? ` (last count=${lastCount})` : '') +
        (lastError ? `: ${String(lastError)}` : ''),
    );
  };
}

async function loadPostgresSyncBackendFactory(
  backendModule: string,
  postgresUrl: string,
): Promise<{
  resetDocForTests: (docId: string) => Promise<void>;
  open: (docId: string) => Promise<SyncBackend<Operation>>;
}> {
  const mod = (await import(pathToFileURL(backendModule).href)) as {
    createPostgresNapiSyncBackendFactory?: (url: string) => {
      resetDocForTests: (docId: string) => Promise<void>;
      open: (docId: string) => Promise<SyncBackend<Operation>>;
    };
  };
  if (typeof mod.createPostgresNapiSyncBackendFactory !== 'function') {
    throw new Error(
      `backend module "${backendModule}" does not export createPostgresNapiSyncBackendFactory(url)`,
    );
  }
  return mod.createPostgresNapiSyncBackendFactory(postgresUrl);
}

async function findFreePort(): Promise<number> {
  return await new Promise<number>((resolve, reject) => {
    const server = net.createServer();
    server.unref();
    server.on('error', reject);
    server.listen(0, '127.0.0.1', () => {
      const address = server.address();
      if (!address || typeof address === 'string') {
        server.close(() => reject(new Error('failed to resolve an ephemeral port')));
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

function createRemoteSyncServerTarget(baseUrl: string): SyncBenchTargetRuntime {
  return {
    id: 'remote-sync-server',
    serverProcess: 'remote',
    connect: async (docId) => await connectToSyncServer(baseUrl, docId, { client: 'builtin' }),
    close: async () => {},
  };
}

async function waitForProcessExit(
  child: ReturnType<typeof spawn>,
  timeoutMs: number,
): Promise<void> {
  if (child.exitCode != null || child.signalCode != null) return;
  await new Promise<void>((resolve) => {
    let settled = false;
    const finish = () => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      resolve();
    };
    const timer = setTimeout(() => {
      if (child.exitCode == null && child.signalCode == null) {
        child.kill('SIGKILL');
      }
      finish();
    }, timeoutMs);
    child.once('exit', finish);
  });
}

async function waitForSyncServerReady(
  healthUrl: string,
  child: ReturnType<typeof spawn>,
  getRecentOutput: () => string,
): Promise<void> {
  const deadline = Date.now() + SYNC_SERVER_START_TIMEOUT_MS;
  while (Date.now() < deadline) {
    if (child.exitCode != null || child.signalCode != null) {
      const recentOutput = getRecentOutput();
      throw new Error(
        `local sync server exited before becoming ready (${healthUrl})${recentOutput ? `\n${recentOutput}` : ''}`,
      );
    }
    try {
      const response = await fetch(healthUrl);
      if (response.ok) return;
    } catch {
      // keep polling until timeout
    }
    await sleep(SERVER_READY_POLL_MS);
  }
  const recentOutput = getRecentOutput();
  throw new Error(
    `timed out waiting for local sync server readiness (${healthUrl})${recentOutput ? `\n${recentOutput}` : ''}`,
  );
}

async function prepareTargetRuntimes(
  repoRoot: string,
  argv: string[],
  targets: SyncBenchTargetId[],
  profileBackend: boolean,
  profileHello: boolean,
  directSendThreshold: number,
): Promise<Map<Exclude<SyncBenchTargetId, 'direct'>, SyncBenchTargetRuntime>> {
  const runtimes = new Map<Exclude<SyncBenchTargetId, 'direct'>, SyncBenchTargetRuntime>();

  if (targets.includes('local-postgres-sync-server')) {
    const postgresUrl = parseFlagValue(argv, '--postgres-url') ?? process.env.TREECRDT_POSTGRES_URL;
    if (!postgresUrl) {
      throw new Error(
        'local-postgres-sync-server target requires TREECRDT_POSTGRES_URL or --postgres-url=...',
      );
    }
    runtimes.set(
      'local-postgres-sync-server',
      await createLocalPostgresSyncServerTarget(
        repoRoot,
        postgresUrl,
        profileBackend,
        profileHello,
        directSendThreshold,
      ),
    );
  }

  if (targets.includes('remote-sync-server')) {
    const remoteUrl =
      parseFlagValue(argv, '--sync-server-url') ?? process.env.TREECRDT_SYNC_SERVER_URL;
    if (!remoteUrl) {
      throw new Error(
        'remote-sync-server target requires TREECRDT_SYNC_SERVER_URL or --sync-server-url=...',
      );
    }
    runtimes.set('remote-sync-server', createRemoteSyncServerTarget(remoteUrl));
  }

  return runtimes;
}

async function closeTargetRuntimes(
  runtimes: Map<Exclude<SyncBenchTargetId, 'direct'>, SyncBenchTargetRuntime>,
): Promise<void> {
  await Promise.allSettled(Array.from(runtimes.values(), (runtime) => runtime.close()));
}

function getRuntimeHelloTraceStore(
  runtime: SyncBenchTargetRuntime,
  profileHello: boolean,
): HelloTraceStore | undefined {
  if (!profileHello) return undefined;
  if (runtime.clearHelloTrace && runtime.takeHelloTrace) {
    return {
      clear: runtime.clearHelloTrace,
      take: runtime.takeHelloTrace,
    };
  }
  if (runtime.serverProcess === 'in-process') {
    return createProcessHelloTraceStore();
  }
  return undefined;
}

async function syncBackendThroughServer(
  runtime: SyncBenchTargetRuntime,
  docId: string,
  backend: FlushableSyncBackend<Operation>,
  filter: Filter,
  opts: {
    maxCodewords?: number;
    codewordsPerMessage?: number;
    directSendThreshold?: number;
    maxOpsPerBatch?: number;
  } = {},
): Promise<void> {
  const peer = new SyncPeer<Operation>(backend, {
    maxCodewords: opts.maxCodewords ?? SYNC_BENCH_DEFAULT_MAX_CODEWORDS,
    directSendThreshold: opts.directSendThreshold ?? 0,
    ...(opts.maxOpsPerBatch != null ? { maxOpsPerBatch: opts.maxOpsPerBatch } : {}),
  });
  const connection = await runtime.connect(docId);
  const detach = peer.attach(connection.transport);

  try {
    await peer.syncOnce(connection.transport, filter, {
      maxCodewords: opts.maxCodewords ?? SYNC_BENCH_DEFAULT_MAX_CODEWORDS,
      codewordsPerMessage: opts.codewordsPerMessage ?? SYNC_BENCH_DEFAULT_CODEWORDS_PER_MESSAGE,
      ...(opts.maxOpsPerBatch != null ? { maxOpsPerBatch: opts.maxOpsPerBatch } : {}),
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
  ops: Operation[],
  filter: Filter,
  maxOpsPerBatch?: number,
): Promise<SeedServerStateResult> {
  if (ops.length === 0) {
    return {
      expectedFilterCount: 0,
      uploadMs: 0,
      allReadyMs: 0,
    };
  }

  const seedDb = await openDb({ storage: 'memory', docId });
  try {
    await appendInitialOps(seedDb, ops);
    const seedBackend = await makeBackend({
      db: seedDb,
      docId,
      initialMaxLamport: maxLamport(ops),
    });
    const expectedFilterCount = (await seedBackend.listOpRefs(filter)).length;
    if (runtime.seedOps) {
      const uploadStartedAt = performance.now();
      await runtime.seedOps(docId, ops);
      const uploadMs = performance.now() - uploadStartedAt;
      const allReadyStartedAt = performance.now();
      if (runtime.waitForOpCount) {
        await runtime.waitForOpCount(docId, { all: {} }, ops.length, {
          timeoutMs: SERVER_SEED_READY_TIMEOUT_MS,
        });
      }
      return {
        expectedFilterCount,
        uploadMs,
        allReadyMs: performance.now() - allReadyStartedAt,
      };
    }
    const peer = new SyncPeer<Operation>(seedBackend, {
      maxCodewords: SYNC_BENCH_SEED_MAX_CODEWORDS,
      directSendThreshold: 0,
      ...(maxOpsPerBatch != null ? { maxOpsPerBatch } : {}),
    });
    const deadline = Date.now() + SERVER_SEED_READY_TIMEOUT_MS;
    let lastError: unknown;
    while (true) {
      const connection = await runtime.connect(docId);
      const detach = peer.attach(connection.transport);
      try {
        const uploadStartedAt = performance.now();
        await peer.syncOnce(
          connection.transport,
          { all: {} },
          {
            maxCodewords: SYNC_BENCH_SEED_MAX_CODEWORDS,
            codewordsPerMessage: SYNC_BENCH_DEFAULT_CODEWORDS_PER_MESSAGE,
            ...(maxOpsPerBatch != null ? { maxOpsPerBatch } : {}),
          },
        );
        await seedBackend.flush();
        const uploadMs = performance.now() - uploadStartedAt;
        const allReadyStartedAt = performance.now();
        if (runtime.waitForOpCount) {
          await runtime.waitForOpCount(docId, { all: {} }, ops.length, {
            timeoutMs: SERVER_SEED_READY_TIMEOUT_MS,
          });
        }
        return {
          expectedFilterCount,
          uploadMs,
          allReadyMs: performance.now() - allReadyStartedAt,
        };
      } catch (error) {
        lastError = error;
        if (Date.now() >= deadline) {
          throw new Error(
            `timed out seeding server doc ${docId} within ${SERVER_SEED_READY_TIMEOUT_MS}ms` +
              (lastError ? `: ${String(lastError)}` : ''),
          );
        }
        await sleep(SERVER_READY_POLL_MS);
      } finally {
        detach();
        await connection.close();
      }
    }
    throw new Error(`failed to seed server doc ${docId}: unexpected retry loop exit`);
  } finally {
    seedDb.close();
  }
}

function canReuseServerFixture(
  runtime: SyncBenchTargetRuntime,
  bench: ReturnType<typeof buildSyncBenchCase>,
): boolean {
  // Reuse is safe when every client-side starting op is already present on the
  // server fixture, so samples only read from the shared doc and never mutate it.
  const serverOpIds = new Set(
    bench.opsB.map(
      (op) => `${Buffer.from(op.meta.id.replica).toString('base64')}:${op.meta.id.counter}`,
    ),
  );
  return bench.opsA.every((op) =>
    serverOpIds.has(`${Buffer.from(op.meta.id.replica).toString('base64')}:${op.meta.id.counter}`),
  );
}

function updateFixtureHashWithBytes(
  hash: ReturnType<typeof createHash>,
  value: Uint8Array | null | undefined,
): void {
  if (value == null) {
    hash.update('-1:');
    return;
  }
  hash.update(`${value.byteLength}:`);
  hash.update(Buffer.from(value));
  hash.update(';');
}

function updateFixtureHashWithString(hash: ReturnType<typeof createHash>, value: string): void {
  hash.update(`${value.length}:`);
  hash.update(value);
  hash.update(';');
}

function updateFixtureHashWithNumber(hash: ReturnType<typeof createHash>, value: number): void {
  hash.update(`${value};`);
}

function createServerFixtureCacheKey(bench: ReturnType<typeof buildSyncBenchCase>): string {
  const hash = createHash('sha256');
  updateFixtureHashWithString(hash, SYNC_BENCH_SERVER_FIXTURE_CACHE_VERSION);
  updateFixtureHashWithString(hash, bench.name);
  if ('all' in bench.filter) {
    updateFixtureHashWithString(hash, 'filter:all');
  } else {
    updateFixtureHashWithString(hash, 'filter:children');
    updateFixtureHashWithBytes(hash, bench.filter.children.parent);
  }
  for (const op of bench.opsB) {
    updateFixtureHashWithBytes(hash, op.meta.id.replica);
    updateFixtureHashWithNumber(hash, op.meta.id.counter);
    updateFixtureHashWithNumber(hash, op.meta.lamport);
    updateFixtureHashWithBytes(hash, op.meta.knownState);
    updateFixtureHashWithString(hash, op.kind.type);
    switch (op.kind.type) {
      case 'insert':
        updateFixtureHashWithString(hash, op.kind.parent);
        updateFixtureHashWithString(hash, op.kind.node);
        updateFixtureHashWithBytes(hash, op.kind.orderKey);
        updateFixtureHashWithBytes(hash, op.kind.payload);
        break;
      case 'move':
        updateFixtureHashWithString(hash, op.kind.node);
        updateFixtureHashWithString(hash, op.kind.newParent);
        updateFixtureHashWithBytes(hash, op.kind.orderKey);
        break;
      case 'delete':
      case 'tombstone':
        updateFixtureHashWithString(hash, op.kind.node);
        break;
      case 'payload':
        updateFixtureHashWithString(hash, op.kind.node);
        updateFixtureHashWithBytes(hash, op.kind.payload);
        break;
    }
  }
  return hash.digest('hex').slice(0, 24);
}

async function prepareServerFixture(
  runtime: SyncBenchTargetRuntime,
  bench: ReturnType<typeof buildSyncBenchCase>,
  directSendThreshold: number,
  cacheMode: ServerFixtureCacheMode,
  maxOpsPerBatch?: number,
): Promise<PreparedServerFixture> {
  const prepareStartedAt = performance.now();
  const cacheKey = cacheMode === 'off' ? undefined : createServerFixtureCacheKey(bench);
  const hasResettableFixture = runtime.resetDoc != null;
  const docId =
    cacheMode === 'off'
      ? `sqlite-node-sync-bench-${runtime.id}-fixture-${crypto.randomUUID()}`
      : cacheMode === 'rebuild' && !hasResettableFixture
        ? `sqlite-node-sync-bench-${runtime.id}-fixture-${cacheKey}-${crypto.randomUUID()}`
        : `sqlite-node-sync-bench-${runtime.id}-fixture-${cacheKey}`;
  if (cacheMode === 'reuse' && runtime.inspectDoc) {
    try {
      const current = await runtime.inspectDoc(docId);
      if (current.allCount === bench.opsB.length && current.maxLamport === maxLamport(bench.opsB)) {
        return {
          docId,
          cacheKey,
          cacheStatus: 'hit',
        };
      }
    } catch {
      // fall through to rebuild the fixture
    }
  }
  if (cacheMode === 'reuse' && !runtime.inspectDoc) {
    return {
      docId,
      cacheKey,
      cacheStatus: 'assumed',
    };
  }
  if (cacheMode !== 'off') {
    await runtime.resetDoc?.(docId);
  }
  const seedState = await seedServerState(
    runtime,
    docId,
    bench.opsB,
    bench.filter as Filter,
    maxOpsPerBatch,
  );
  const filterReadyStartedAt = performance.now();
  if (runtime.waitForOpCount) {
    await runtime.waitForOpCount(docId, bench.filter as Filter, seedState.expectedFilterCount, {
      timeoutMs: SERVER_READY_TIMEOUT_MS,
    });
  } else {
    await waitForServerOpCount(
      runtime,
      docId,
      bench.filter as Filter,
      seedState.expectedFilterCount,
      directSendThreshold,
      maxOpsPerBatch,
      SERVER_SEED_READY_TIMEOUT_MS,
    );
  }
  const filterReadyMs = performance.now() - filterReadyStartedAt;
  return {
    docId,
    cacheKey,
    cacheStatus: cacheMode === 'rebuild' ? 'rebuild' : cacheMode === 'reuse' ? 'miss' : 'disabled',
    seedUploadMs: seedState.uploadMs,
    seedAllReadyMs: seedState.allReadyMs,
    filterReadyMs,
    totalPrepareMs: performance.now() - prepareStartedAt,
  };
}

async function waitForServerOpCount(
  runtime: SyncBenchTargetRuntime,
  docId: string,
  filter: Filter,
  expectedCount: number,
  directSendThreshold: number,
  maxOpsPerBatch?: number,
  timeoutMs = SERVER_READY_TIMEOUT_MS,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  let lastCount = -1;
  let lastError: unknown;
  while (true) {
    const verifierDb = await openDb({ storage: 'memory', docId });
    try {
      const verifierBackend = await makeBackend({
        db: verifierDb,
        docId,
        initialMaxLamport: 0,
      });
      await syncBackendThroughServer(runtime, docId, verifierBackend, filter, {
        maxCodewords: SYNC_BENCH_SEED_MAX_CODEWORDS,
        directSendThreshold,
        ...(maxOpsPerBatch != null ? { maxOpsPerBatch } : {}),
      });
      lastCount = countOps(verifierDb);
      if (lastCount === expectedCount) {
        return;
      }
      lastError = undefined;
    } catch (error) {
      lastError = error;
    } finally {
      verifierDb.close();
    }

    if (Date.now() >= deadline) {
      throw new Error(
        `timed out waiting for server doc ${docId} to reach ${expectedCount} ops within ${timeoutMs}ms` +
          (lastCount >= 0 ? ` (last count=${lastCount})` : '') +
          (lastError ? `: ${String(lastError)}` : ''),
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
  size: number,
): Promise<{ db: Database.Database; cleanup: () => Promise<void> }> {
  const outDir = path.join(repoRoot, 'tmp', 'sqlite-node-sync-bench');
  const dbPath =
    storage === 'file' ? path.join(outDir, `${runId}-${workload}-${size}-${docId}.db`) : undefined;
  if (storage === 'file') {
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
  includeFirstView: boolean,
  profileBackend: boolean,
  profileTransport: boolean,
  profileHello: boolean,
  directSendThreshold: number,
  maxOpsPerBatch?: number,
): Promise<SyncBenchSample> {
  const runId = crypto.randomUUID();
  const docId = `sqlite-node-sync-bench-${runId}`;
  const helloTraceStore = profileHello ? createProcessHelloTraceStore() : undefined;
  const clientA = await openClientDbForRun(repoRoot, storage, docId, `${runId}-a`, workload, size);
  const clientB = await openClientDbForRun(repoRoot, storage, docId, `${runId}-b`, workload, size);

  try {
    await Promise.all([
      appendInitialOps(clientA.db, bench.opsA),
      appendInitialOps(clientB.db, bench.opsB),
    ]);

    const backendA = await makeBackend({
      db: clientA.db,
      docId,
      initialMaxLamport: maxLamport(bench.opsA),
      profileLabel: profileBackend ? 'direct-client-a' : undefined,
    });
    const backendB = await makeBackend({
      db: clientB.db,
      docId,
      initialMaxLamport: maxLamport(bench.opsB),
      profileLabel: profileBackend ? 'direct-client-b' : undefined,
    });

    const [wireA, wireB] = createInMemoryDuplex<Uint8Array>();
    const baseTransportA = wrapDuplexTransportWithCodec(wireA, treecrdtSyncV0ProtobufCodec);
    const transportAProfile = profileTransport
      ? createProfiledSyncTransport(baseTransportA)
      : undefined;
    const transportA = transportAProfile?.transport ?? baseTransportA;
    const transportB = wrapDuplexTransportWithCodec(wireB, treecrdtSyncV0ProtobufCodec);
    const pa = new SyncPeer(backendA, {
      maxCodewords: SYNC_BENCH_DEFAULT_MAX_CODEWORDS,
      directSendThreshold,
      ...(maxOpsPerBatch != null ? { maxOpsPerBatch } : {}),
    });
    const pb = new SyncPeer(backendB, {
      maxCodewords: SYNC_BENCH_DEFAULT_MAX_CODEWORDS,
      directSendThreshold,
      ...(maxOpsPerBatch != null ? { maxOpsPerBatch } : {}),
    });
    const detachA = pa.attach(transportA);
    const detachB = pb.attach(transportB);

    try {
      if (profileBackend) resetBackendProfiler(docId);
      helloTraceStore?.clear(docId);
      const start = performance.now();
      await pa.syncOnce(transportA, bench.filter as Filter, {
        maxCodewords: SYNC_BENCH_DEFAULT_MAX_CODEWORDS,
        codewordsPerMessage: SYNC_BENCH_DEFAULT_CODEWORDS_PER_MESSAGE,
        ...(maxOpsPerBatch != null ? { maxOpsPerBatch } : {}),
      });
      await Promise.all([backendA.flush(), backendB.flush()]);
      const syncedAt = performance.now();

      let firstViewReadMs = 0;
      if (includeFirstView) {
        if (!bench.firstView) {
          throw new Error(
            `sync bench workload ${bench.name} does not define a first-view read path`,
          );
        }
        firstViewReadMs = await measureFirstViewAfterSync(clientA.db, docId, bench.firstView);
      }

      const countA = countOps(clientA.db);
      const countB = countOps(clientB.db);
      if (countA !== bench.expectedFinalOpsA || countB !== bench.expectedFinalOpsB) {
        throw new Error(
          `sync bench mismatch: expected a=${bench.expectedFinalOpsA} b=${bench.expectedFinalOpsB}, got a=${countA} b=${countB}`,
        );
      }

      return {
        totalMs: syncedAt - start + firstViewReadMs,
        syncMs: syncedAt - start,
        firstViewReadMs,
        backendProfile: profileBackend ? takeBackendProfilerSnapshot(docId) : undefined,
        transportProfile: profileTransport ? transportAProfile?.snapshot() : undefined,
        helloProfile: helloTraceStore?.take(docId),
      };
    } finally {
      detachA();
      detachB();
    }
  } finally {
    helloTraceStore?.dispose?.();
    await Promise.all([clientA.cleanup(), clientB.cleanup()]);
  }
}

async function runBenchOnceViaServer(
  repoRoot: string,
  runtime: SyncBenchTargetRuntime,
  { storage, workload, size }: BenchCase,
  bench: ReturnType<typeof buildSyncBenchCase>,
  includeFirstView: boolean,
  profileBackend: boolean,
  profileTransport: boolean,
  profileHello: boolean,
  directSendThreshold: number,
  maxOpsPerBatch: number | undefined,
  postSeedWaitMs: number,
  preparedFixture?: PreparedServerFixture,
): Promise<SyncBenchSample> {
  const runId = crypto.randomUUID();
  const docId = preparedFixture?.docId ?? `sqlite-node-sync-bench-${runtime.id}-${runId}`;
  const helloTraceStore = getRuntimeHelloTraceStore(runtime, profileHello);
  const client = await openClientDbForRun(repoRoot, storage, docId, runId, workload, size);

  try {
    await appendInitialOps(client.db, bench.opsA);
    if (!preparedFixture) {
      const expectedFilterCount = await seedServerState(
        runtime,
        docId,
        bench.opsB,
        bench.filter as Filter,
        maxOpsPerBatch,
      );
      if (runtime.waitForOpCount) {
        await runtime.waitForOpCount(docId, bench.filter as Filter, expectedFilterCount, {
          timeoutMs: SERVER_READY_TIMEOUT_MS,
        });
      } else {
        await waitForServerOpCount(
          runtime,
          docId,
          bench.filter as Filter,
          expectedFilterCount,
          directSendThreshold,
          maxOpsPerBatch,
          SERVER_SEED_READY_TIMEOUT_MS,
        );
      }
      if (postSeedWaitMs > 0) {
        await sleep(postSeedWaitMs);
      }
    }
    helloTraceStore?.clear(docId);

    const clientBackend = await makeBackend({
      db: client.db,
      docId,
      initialMaxLamport: maxLamport(bench.opsA),
      profileLabel: profileBackend ? 'client-sqlite' : undefined,
    });

    const peer = new SyncPeer<Operation>(clientBackend, {
      maxCodewords: SYNC_BENCH_DEFAULT_MAX_CODEWORDS,
      directSendThreshold,
      ...(maxOpsPerBatch != null ? { maxOpsPerBatch } : {}),
    });
    const connection = await runtime.connect(docId);
    const transportProfile = profileTransport
      ? createProfiledSyncTransport(connection.transport)
      : undefined;
    const transport = transportProfile?.transport ?? connection.transport;
    const detach = peer.attach(transport);

    try {
      if (profileBackend) resetBackendProfiler(docId);
      const start = performance.now();
      await peer.syncOnce(transport, bench.filter as Filter, {
        maxCodewords: SYNC_BENCH_DEFAULT_MAX_CODEWORDS,
        codewordsPerMessage: SYNC_BENCH_DEFAULT_CODEWORDS_PER_MESSAGE,
        ...(maxOpsPerBatch != null ? { maxOpsPerBatch } : {}),
      });
      await clientBackend.flush();
      const syncedAt = performance.now();

      let firstViewReadMs = 0;
      if (includeFirstView) {
        if (!bench.firstView) {
          throw new Error(
            `sync bench workload ${bench.name} does not define a first-view read path`,
          );
        }
        firstViewReadMs = await measureFirstViewAfterSync(client.db, docId, bench.firstView);
      }

      const countA = countOps(client.db);
      if (countA !== bench.expectedFinalOpsA) {
        throw new Error(
          `sync bench mismatch: expected client=${bench.expectedFinalOpsA}, got client=${countA}`,
        );
      }

      return {
        totalMs: syncedAt - start + firstViewReadMs,
        syncMs: syncedAt - start,
        firstViewReadMs,
        backendProfile: profileBackend ? takeBackendProfilerSnapshot(docId) : undefined,
        transportProfile: profileTransport ? transportProfile?.snapshot() : undefined,
        helloProfile: helloTraceStore?.take(docId),
      };
    } finally {
      detach();
      await connection.close();
    }
  } finally {
    helloTraceStore?.dispose?.();
    await client.cleanup();
  }
}

async function runBenchCase(
  repoRoot: string,
  benchCase: BenchCase,
  runtimes: Map<Exclude<SyncBenchTargetId, 'direct'>, SyncBenchTargetRuntime>,
  includeFirstView: boolean,
  profileBackend: boolean,
  profileTransport: boolean,
  profileHello: boolean,
  directSendThreshold: number,
  serverFixtureCacheMode: ServerFixtureCacheMode,
  postSeedWaitMs: number,
  maxOpsPerBatch?: number,
): Promise<SyncBenchResult> {
  const bench = buildSyncBenchCase({
    workload: benchCase.workload,
    size: benchCase.size,
    fanout: benchCase.fanout,
  });
  const { iterations, warmupIterations } = benchCase;

  const runtime = benchCase.target === 'direct' ? null : runtimes.get(benchCase.target);
  if (benchCase.target !== 'direct' && !runtime) {
    throw new Error(`missing runtime for sync bench target ${benchCase.target}`);
  }

  if (includeFirstView && !bench.firstView) {
    throw new Error(`sync bench workload ${bench.name} does not support --first-view`);
  }
  const preparedFixture =
    runtime && canReuseServerFixture(runtime, bench)
      ? await prepareServerFixture(
          runtime,
          bench,
          directSendThreshold,
          serverFixtureCacheMode,
          maxOpsPerBatch,
        )
      : undefined;

  for (let i = 0; i < warmupIterations; i += 1) {
    if (runtime) {
      await runBenchOnceViaServer(
        repoRoot,
        runtime,
        benchCase,
        bench,
        includeFirstView,
        profileBackend,
        profileTransport,
        profileHello,
        directSendThreshold,
        maxOpsPerBatch,
        postSeedWaitMs,
        preparedFixture,
      );
    } else {
      await runBenchOnceDirect(
        repoRoot,
        benchCase,
        bench,
        includeFirstView,
        profileBackend,
        profileTransport,
        profileHello,
        directSendThreshold,
        maxOpsPerBatch,
      );
    }
  }

  const samples: SyncBenchSample[] = [];
  for (let i = 0; i < iterations; i += 1) {
    samples.push(
      runtime
        ? await runBenchOnceViaServer(
            repoRoot,
            runtime,
            benchCase,
            bench,
            includeFirstView,
            profileBackend,
            profileTransport,
            profileHello,
            directSendThreshold,
            maxOpsPerBatch,
            postSeedWaitMs,
            preparedFixture,
          )
        : await runBenchOnceDirect(
            repoRoot,
            benchCase,
            bench,
            includeFirstView,
            profileBackend,
            profileTransport,
            profileHello,
            directSendThreshold,
            maxOpsPerBatch,
          ),
    );
  }

  const totalSamplesMs = samples.map((sample) => sample.totalMs);
  const syncSamplesMs = samples.map((sample) => sample.syncMs);
  const firstViewReadSamplesMs = samples.map((sample) => sample.firstViewReadMs);
  const backendProfiles = samples
    .map((sample) => sample.backendProfile)
    .filter((sample): sample is NonNullable<typeof sample> => sample != null);
  const transportProfiles = samples
    .map((sample) => sample.transportProfile)
    .filter((sample): sample is NonNullable<typeof sample> => sample != null);
  const helloProfiles = samples
    .map((sample) => sample.helloProfile)
    .filter((sample): sample is NonNullable<typeof sample> => sample != null);
  const durationMs = iterations > 1 ? quantile(totalSamplesMs, 0.5) : (totalSamplesMs[0] ?? 0);
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
      transport: benchCase.target === 'direct' ? 'in-memory' : 'websocket',
      server:
        benchCase.target === 'local-postgres-sync-server'
          ? 'postgres-local'
          : benchCase.target === 'remote-sync-server'
            ? 'remote'
            : 'none',
      serverProcess: runtime?.serverProcess ?? (benchCase.target === 'direct' ? 'none' : 'unknown'),
      measurement: includeFirstView ? 'time-to-first-view' : 'sync-only',
      backendProfile: profileBackend ? backendProfiles.at(-1) : undefined,
      backendProfileSamples:
        profileBackend && backendProfiles.length > 1 ? backendProfiles : undefined,
      transportProfile: profileTransport ? transportProfiles.at(-1) : undefined,
      transportProfileSamples:
        profileTransport && transportProfiles.length > 1 ? transportProfiles : undefined,
      helloProfile: profileHello ? helloProfiles.at(-1) : undefined,
      helloProfileSamples: profileHello && helloProfiles.length > 1 ? helloProfiles : undefined,
      helloStageSummary: profileHello ? summarizeHelloTraceProfiles(helloProfiles) : undefined,
      helloTotalMsSamples: profileHello
        ? helloProfiles.map((profile) => profile.totalMs)
        : undefined,
      codewordsPerMessage: SYNC_BENCH_DEFAULT_CODEWORDS_PER_MESSAGE,
      maxCodewords: SYNC_BENCH_DEFAULT_MAX_CODEWORDS,
      directSendThreshold: directSendThreshold > 0 ? directSendThreshold : undefined,
      maxOpsPerBatch,
      postSeedWaitMs: postSeedWaitMs > 0 ? postSeedWaitMs : undefined,
      serverFixtureReuse: preparedFixture ? 'per-case' : undefined,
      serverFixtureCacheMode:
        preparedFixture && serverFixtureCacheMode !== DEFAULT_SERVER_FIXTURE_CACHE_MODE
          ? serverFixtureCacheMode
          : undefined,
      serverFixtureCacheStatus:
        preparedFixture && preparedFixture.cacheStatus !== 'disabled'
          ? preparedFixture.cacheStatus
          : undefined,
      serverFixtureCacheKey: preparedFixture?.cacheKey,
      iterations: iterations > 1 ? iterations : undefined,
      warmupIterations: warmupIterations > 0 ? warmupIterations : undefined,
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

async function primeServerFixtureCase(
  benchCase: Omit<BenchCase, 'storage' | 'iterations' | 'warmupIterations'>,
  runtimes: Map<Exclude<SyncBenchTargetId, 'direct'>, SyncBenchTargetRuntime>,
  directSendThreshold: number,
  serverFixtureCacheMode: ServerFixtureCacheMode,
  maxOpsPerBatch?: number,
): Promise<PrimedServerFixtureResult> {
  const bench = buildSyncBenchCase({
    workload: benchCase.workload,
    size: benchCase.size,
    fanout: benchCase.fanout,
  });
  const runtime = benchCase.target === 'direct' ? null : runtimes.get(benchCase.target);
  if (benchCase.target !== 'direct' && !runtime) {
    throw new Error(`missing runtime for sync bench target ${benchCase.target}`);
  }
  if (!runtime) {
    throw new Error('server fixture priming requires a sync-server target');
  }
  if (!canReuseServerFixture(runtime, bench)) {
    throw new Error(
      `sync bench workload ${bench.name} does not support reusable sync-server fixtures`,
    );
  }

  const startedAt = performance.now();
  const preparedFixture = await prepareServerFixture(
    runtime,
    bench,
    directSendThreshold,
    serverFixtureCacheMode,
    maxOpsPerBatch,
  );
  const durationMs = performance.now() - startedAt;
  const totalOps = bench.opsB.length;
  const opsPerSec = durationMs > 0 ? (totalOps / durationMs) * 1000 : Infinity;
  return {
    name: `${bench.name}-server-fixture`,
    totalOps,
    durationMs,
    opsPerSec,
    extra: {
      ...bench.extra,
      count: benchCase.size,
      fanout: benchCase.fanout,
      mode: benchCase.target,
      target: benchCase.target,
      server:
        benchCase.target === 'local-postgres-sync-server'
          ? 'postgres-local'
          : benchCase.target === 'remote-sync-server'
            ? 'remote'
            : 'none',
      serverProcess: runtime.serverProcess,
      measurement: 'server-fixture-prime',
      directSendThreshold: directSendThreshold > 0 ? directSendThreshold : undefined,
      maxOpsPerBatch,
      serverFixtureReuse: 'per-case',
      serverFixtureCacheMode,
      serverFixtureCacheStatus: preparedFixture.cacheStatus,
      serverFixtureCacheKey: preparedFixture.cacheKey,
      docId: preparedFixture.docId,
      seedUploadMs: preparedFixture.seedUploadMs,
      seedAllReadyMs: preparedFixture.seedAllReadyMs,
      filterReadyMs: preparedFixture.filterReadyMs,
      totalPrepareMs: preparedFixture.totalPrepareMs,
      fixtureOpCount: totalOps,
      fixtureMaxLamport: maxLamport(bench.opsB),
    },
  };
}

async function main() {
  const argv = process.argv.slice(2);
  const repoRoot = repoRootFromImportMeta(import.meta.url, 3);

  const iterationsOverride = parseIterationsOverride(argv);
  const warmupIterationsOverride = parseWarmupIterationsOverride(argv);
  const config =
    parseConfigFromArgv(argv, iterationsOverride) ??
    SYNC_BENCH_CONFIG.map(
      ([size, iterations]) => [size, iterationsOverride ?? iterations] as ConfigEntry,
    );
  const rootConfig = SYNC_BENCH_ROOT_CONFIG.map(
    ([size, iterations]) => [size, iterationsOverride ?? iterations] as ConfigEntry,
  );
  const targets = parseTargets(argv);
  const storages = parseStorages(argv);
  const workloads = parseWorkloads(argv);
  const fanout = parseFanout(argv);
  const includeFirstView = parseFirstView(argv);
  const profileBackend = parseProfileBackend(argv);
  const profileTransport = parseProfileTransport(argv);
  const profileHello = parseProfileHello(argv);
  const directSendThreshold = parseDirectSendThreshold(argv);
  const maxOpsPerBatch = parseMaxOpsPerBatch(argv);
  const postSeedWaitMs = parsePostSeedWaitMs(argv);
  const serverFixtureCacheMode = parseServerFixtureCacheMode(argv);
  const primeServerFixtures = parsePrimeServerFixtures(argv);
  const runtimes = await prepareTargetRuntimes(
    repoRoot,
    argv,
    targets,
    profileBackend,
    profileHello,
    directSendThreshold,
  );

  try {
    if (primeServerFixtures) {
      if (targets.some((target) => target === 'direct')) {
        throw new Error('--prime-server-fixtures only supports sync-server targets');
      }
      const primeCases = targets.flatMap((target) =>
        workloads.flatMap((workload) => {
          const entries = workload === 'sync-root-children-fanout10' ? rootConfig : config;
          return entries.map(([size]) => ({
            target,
            workload,
            size,
            fanout,
          }));
        }),
      );

      for (const primeCase of primeCases) {
        const result = await primeServerFixtureCase(
          primeCase,
          runtimes,
          directSendThreshold,
          serverFixtureCacheMode,
          maxOpsPerBatch,
        );
        const outFile = path.join(
          repoRoot,
          'benchmarks',
          'sqlite-node-sync',
          `server-fixture-${primeCase.target}-${result.name}.json`,
        );
        const payload = await writeResult(result, {
          implementation: 'sqlite-node',
          storage: 'server-fixture',
          workload: result.name,
          outFile,
          extra: {
            target: primeCase.target,
            ...result.extra,
          },
        });
        console.log(JSON.stringify(payload));
      }
      return;
    }

    const cases: BenchCase[] = [];
    for (const target of targets) {
      for (const storage of storages) {
        for (const workload of workloads) {
          const entries = workload === 'sync-root-children-fanout10' ? rootConfig : config;
          for (const [size, iterations] of entries) {
            cases.push({
              target,
              storage,
              workload,
              size,
              iterations,
              warmupIterations: resolveWarmupIterations(iterations, warmupIterationsOverride),
              fanout,
            });
          }
        }
      }
    }

    for (const benchCase of cases) {
      const result = await runBenchCase(
        repoRoot,
        benchCase,
        runtimes,
        includeFirstView,
        profileBackend,
        profileTransport,
        profileHello,
        directSendThreshold,
        serverFixtureCacheMode,
        postSeedWaitMs,
        maxOpsPerBatch,
      );
      const outFile = path.join(
        repoRoot,
        'benchmarks',
        'sqlite-node-sync',
        `${benchCase.storage}-${benchCase.target}-${result.name}.json`,
      );
      const payload = await writeResult(result, {
        implementation: 'sqlite-node',
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

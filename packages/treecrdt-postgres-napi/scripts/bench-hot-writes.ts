import { randomUUID } from 'node:crypto';

import {
  DEFAULT_HOT_WRITE_CONFIG,
  DEFAULT_HOT_WRITE_FANOUT,
  DEFAULT_HOT_WRITE_PAYLOAD_BYTES,
  parseHotWriteConfigFromArgv,
  parseHotWriteKinds,
  parseNonNegativeIntFlag,
  parsePositiveIntFlag,
  runHotWriteBenchmarks,
  type HotWriteSeed,
  type HotWriteSeedTargets,
} from '../../treecrdt-benchmark/dist/hot-write.js';
import { repoRootFromImportMeta } from '@treecrdt/benchmark/node';

import { createPostgresNapiTestAdapterFactory } from '../src/testing.js';
import { createTreecrdtPostgresClient } from '../src/client.js';

const POSTGRES_URL = process.env.TREECRDT_POSTGRES_URL;
const HOT_WRITE_FIXTURE_CACHE_VERSION = '2026-03-30-v1';
const HOT_WRITE_SKIP_SAMPLE_CLEANUP = process.env.HOT_WRITE_SKIP_SAMPLE_CLEANUP === '1';

async function main() {
  if (!POSTGRES_URL) {
    console.warn('Skipping postgres hot-write benchmark because TREECRDT_POSTGRES_URL is not set');
    return;
  }

  const repoRoot = repoRootFromImportMeta(import.meta.url, 3);
  const argv = process.argv.slice(2);
  const config = parseHotWriteConfigFromArgv(argv) ?? [...DEFAULT_HOT_WRITE_CONFIG];
  const benches = parseHotWriteKinds(argv);
  const fanout = parsePositiveIntFlag(
    argv,
    '--fanout',
    'HOT_WRITE_BENCH_FANOUT',
    DEFAULT_HOT_WRITE_FANOUT,
  );
  const payloadBytes = parsePositiveIntFlag(
    argv,
    '--payload-bytes',
    'HOT_WRITE_BENCH_PAYLOAD_BYTES',
    DEFAULT_HOT_WRITE_PAYLOAD_BYTES,
  );
  const writesPerSample = parsePositiveIntFlag(
    argv,
    '--writes-per-sample',
    'HOT_WRITE_WRITES_PER_SAMPLE',
    1,
  );
  const warmupWrites = parseNonNegativeIntFlag(
    argv,
    '--warmup-writes',
    'HOT_WRITE_WARMUP_WRITES',
    0,
  );

  const factory = createPostgresNapiTestAdapterFactory(POSTGRES_URL);
  await factory.ensureSchema();
  const seededDocs = new Map<string, string>();

  const outputs = await runHotWriteBenchmarks({
    repoRoot,
    implementation: 'postgres-napi',
    storage: 'postgres',
    config,
    benches,
    fanout,
    payloadBytes,
    writesPerSample,
    warmupWrites,
    openSeededEngine: async ({ bench, size, seed, getSeed, sampleIndex }) =>
      openSeededClient({
        url: POSTGRES_URL,
        factory,
        bench,
        size,
        seed,
        getSeed,
        sampleIndex,
        ensureSeededDocId: async () =>
          ensureSeededDocId({
            url: POSTGRES_URL,
            factory,
            size,
            seed,
            getSeed,
            fanout,
            payloadBytes,
            seededDocs,
          }),
      }),
  });

  for (const output of outputs) console.log(JSON.stringify(output, null, 2));
}

async function openSeededClient(opts: {
  url: string;
  factory: ReturnType<typeof createPostgresNapiTestAdapterFactory>;
  bench: string;
  size: number;
  seed: HotWriteSeedTargets;
  getSeed: () => HotWriteSeed;
  sampleIndex: number;
  ensureSeededDocId: () => Promise<string>;
}) {
  const seededDocId = await opts.ensureSeededDocId();
  const docId = `hot-write-${opts.bench}-${opts.size}-${opts.sampleIndex}-${randomUUID()}`;
  await opts.factory.cloneMaterializedDocForTests(seededDocId, docId);
  const client = await createTreecrdtPostgresClient(opts.url, { docId });
  return {
    ...client,
    close: async () => {
      await client.close();
      if (!HOT_WRITE_SKIP_SAMPLE_CLEANUP) {
        await opts.factory.resetDocForTests(docId);
      }
    },
  };
}

async function ensureSeededDocId(opts: {
  url: string;
  factory: ReturnType<typeof createPostgresNapiTestAdapterFactory>;
  size: number;
  seed: HotWriteSeedTargets;
  getSeed: () => HotWriteSeed;
  fanout: number;
  payloadBytes: number;
  seededDocs: Map<string, string>;
}): Promise<string> {
  const key = `${opts.size}:${opts.fanout}:${opts.payloadBytes}`;
  const cached = opts.seededDocs.get(key);
  if (cached) return cached;
  const expectedHeadLamport = opts.size + 1;

  const docId = [
    'hot-write-seed',
    HOT_WRITE_FIXTURE_CACHE_VERSION,
    `fanout${opts.fanout}`,
    `payload${opts.payloadBytes}`,
    String(opts.size),
  ].join('-');
  const client = await createTreecrdtPostgresClient(opts.url, { docId });
  try {
    const [headLamport, nodeCount] = await Promise.all([
      client.meta.headLamport(),
      client.tree.nodeCount(),
    ]);
    if (headLamport !== expectedHeadLamport || nodeCount !== opts.size) {
      await opts.factory.primeBalancedFanoutDocForTests(
        docId,
        opts.size,
        opts.fanout,
        opts.payloadBytes,
        'bench',
      );
    }
  } finally {
    await client.close();
  }
  opts.seededDocs.set(key, docId);
  return docId;
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});

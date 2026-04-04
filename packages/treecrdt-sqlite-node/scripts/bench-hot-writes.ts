import fs from 'node:fs/promises';
import path from 'node:path';
import { randomUUID } from 'node:crypto';

import Database from 'better-sqlite3';

import {
  DEFAULT_HOT_WRITE_CONFIG,
  DEFAULT_HOT_WRITE_FANOUT,
  DEFAULT_HOT_WRITE_PAYLOAD_BYTES,
  parseHotWriteConfigFromArgv,
  parseHotWriteKinds,
  parseNonNegativeIntFlag,
  parsePositiveIntFlag,
  runHotWriteBenchmarks,
  type HotWriteBenchKind,
  type HotWriteSeedTargets,
} from '../../treecrdt-benchmark/dist/hot-write.js';
import { repoRootFromImportMeta } from '@treecrdt/benchmark/node';
import { nodeIdToBytes16 } from '@treecrdt/interface/ids';

import { createTreecrdtClient, createSqliteNodeApi, loadTreecrdtExtension } from '../dist/index.js';

type StorageKind = 'memory' | 'file';

const STORAGES: readonly StorageKind[] = ['memory', 'file'];

async function openSeededClient(opts: {
  repoRoot: string;
  storage: StorageKind;
  bench: HotWriteBenchKind;
  size: number;
  seed: HotWriteSeedTargets;
  getSeed: () => { ops: import('@treecrdt/interface').Operation[] };
  sampleIndex: number;
}) {
  const dbPath =
    opts.storage === 'memory'
      ? ':memory:'
      : path.join(
          opts.repoRoot,
          'tmp',
          'sqlite-node-hot-write',
          `${opts.bench}-${opts.size}-${opts.sampleIndex}-${randomUUID()}.db`,
        );

  if (opts.storage === 'file') {
    await fs.mkdir(path.dirname(dbPath), { recursive: true });
  }

  const db = new Database(dbPath);
  loadTreecrdtExtension(db);
  const api = createSqliteNodeApi(db);
  await api.setDocId('treecrdt-hot-write-bench');
  await api.appendOps!(opts.getSeed().ops, nodeIdToBytes16, (replica) => replica);

  const client = await createTreecrdtClient(db, { docId: 'treecrdt-hot-write-bench' });
  return {
    ...client,
    close: async () => {
      await client.close();
      if (opts.storage === 'file') {
        await fs.rm(dbPath).catch(() => {});
      }
    },
  };
}

async function main() {
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

  for (const storage of STORAGES) {
    const outputs = await runHotWriteBenchmarks({
      repoRoot,
      implementation: 'sqlite-node',
      storage,
      config,
      benches,
      fanout,
      payloadBytes,
      writesPerSample,
      warmupWrites,
      openSeededEngine: ({ bench, size, seed, getSeed, sampleIndex }) =>
        openSeededClient({
          repoRoot,
          storage,
          bench,
          size,
          seed,
          getSeed,
          sampleIndex,
        }),
    });
    for (const output of outputs) console.log(JSON.stringify(output, null, 2));
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});

import { expect, test, vi } from 'vitest';
import type { MaterializationEvent } from '@treecrdt/interface/engine';
import type { SqliteRunner } from '@treecrdt/interface/sqlite';
import { createTreecrdtPayloadIndex } from '@treecrdt/interface/payload-index';
import type { Operation } from '@treecrdt/interface';
import {
  createTreecrdtClient,
  defaultExtensionPath,
  loadTreecrdtExtension,
} from '../dist/index.js';

const root = '0'.repeat(32);
const replica = Uint8Array.from({ length: 32 }, (_, i) => (i === 31 ? 1 : 7));
const encoder = new TextEncoder();
const id = (n: number) => n.toString(16).padStart(32, '0');
/** A payload shaped like an em thought, keyed by its value. */
const thought = (value: string, created: number) =>
  encoder.encode(
    JSON.stringify({ value, lexeme: value, created, lastUpdated: created, updatedBy: 'perf' }),
  );

async function open() {
  const { default: Database } = await import('better-sqlite3');
  const db = new Database(':memory:');
  loadTreecrdtExtension(db, { extensionPath: defaultExtensionPath() });
  return { db, client: await createTreecrdtClient(db, { docId: 'payload-index' }) };
}

/** `runner` with `getText` replaced by `getText(sql, params, inner)`. */
const wrapGetText = (
  runner: SqliteRunner,
  getText: (
    sql: string,
    params: Parameters<SqliteRunner['getText']>[1],
    inner: SqliteRunner['getText'],
  ) => ReturnType<SqliteRunner['getText']>,
): SqliteRunner => ({ ...runner, getText: (sql, params) => getText(sql, params, runner.getText) });

test('a lookup searches the payload index', async () => {
  const { db, client } = await open();
  const statements: string[] = [];
  const runner = wrapGetText(client.runner, (sql, params, inner) => {
    statements.push(sql);
    return inner(sql, params);
  });
  const index = await createTreecrdtPayloadIndex(
    { onMaterialized: client.onMaterialized, runner },
    { name: 'lexeme', path: '$.lexeme' },
  );
  await index.lookup(['cat', 'dog']);
  const plan = (
    db
      .prepare(`EXPLAIN QUERY PLAN ${statements[statements.length - 1]}`)
      .all({ 1: '["cat","dog"]', 2: 0 }) as { detail: string }[]
  ).map((row) => row.detail);
  expect(plan).toContain('SEARCH p USING INDEX treecrdt_payload_index_lexeme (<expr>=?)');
  await index.close();
  await client.close();
});

test('a write between reading the changed keys and clearing them reports with the next event', async () => {
  const { client } = await open();
  let raced = false;
  const runner = wrapGetText(client.runner, async (sql, params, inner) => {
    const text = await inner(sql, params);
    if (!raced && sql.includes('json_group_array(DISTINCT key)')) {
      raced = true;
      await client.local.payload(replica, id(1), thought('dog', 1));
    }
    return text;
  });
  const index = await createTreecrdtPayloadIndex(
    { onMaterialized: client.onMaterialized, runner },
    { name: 'lexeme', path: '$.lexeme' },
  );
  const reported: string[][] = [];
  index.onChange((keys) => void reported.push(keys));

  await client.local.insert(replica, root, id(1), { type: 'last' }, thought('cat', 1));
  // The edit's event waits behind the delivery it raced, and so does the second wait.
  await index.idle();
  await index.idle();
  expect(reported).toEqual([['cat'], ['cat', 'dog']]);
  await index.close();
  await client.close();
});

test('opens that never close share the triggers, and the log lets go of them', async () => {
  const { db, client } = await open();
  let now = 1_000_000;
  const clock = vi.spyOn(Date, 'now').mockImplementation(() => now);
  try {
    // Opens whose events stop coming, as in tabs that reloaded while others kept their worker.
    // One of them looked keys up, and comes back later, as from a tab the browser froze.
    let wake: (event: MaterializationEvent) => void = () => {};
    const frozen = await createTreecrdtPayloadIndex(
      { onMaterialized: (listener) => ((wake = listener), () => {}), runner: client.runner },
      { name: 'lexeme', path: '$.lexeme' },
    );
    const frozenReports: string[][] = [];
    frozen.onChange((keys) => void frozenReports.push(keys));
    await frozen.lookup(['gone', 'cat']);
    for (let i = 0; i < 3; i += 1) {
      await createTreecrdtPayloadIndex(
        { onMaterialized: () => () => {}, runner: client.runner },
        { name: 'lexeme', path: '$.lexeme' },
      );
    }
    const index = await createTreecrdtPayloadIndex(client, { name: 'lexeme', path: '$.lexeme' });
    const count = (sql: string) => (db.prepare(sql).get() as { n: number }).n;
    expect(count(`SELECT count(*) AS n FROM sqlite_temp_master WHERE type = 'trigger'`)).toBe(4);
    const logged = () => count('SELECT count(*) AS n FROM temp.treecrdt_payload_index_lexeme_log');
    const writer = client.local.forReplica(replica);
    const insert = async (from: number, to: number) => {
      for (let i = from; i <= to; i += 1) {
        await writer.insert(root, id(i), { type: 'last' }, thought(`k${i}`, i));
      }
      await index.idle();
    };

    // The live open reads every key, which the log keeps for the others.
    await insert(1, 150);
    expect(logged()).toBeGreaterThanOrEqual(150);
    // Behind for longer than five minutes, they are taken for gone, and the log lets go of them.
    now += 5 * 60_000 + 1;
    await insert(151, 250);
    expect(logged()).toBeLessThan(64);

    wake({ headSeq: 0, changes: [] });
    await frozen.idle();
    expect(frozenReports).toHaveLength(1);
    expect(frozenReports[0]).toEqual(expect.arrayContaining(['cat', 'gone']));
    await index.close();
  } finally {
    clock.mockRestore();
    await client.close();
  }
});

const perf = process.env.PAYLOAD_INDEX_PERF ? test : test.skip;
const ms = (t: number) => Math.round((performance.now() - t) * 100) / 100;
const median = (values: number[]) => [...values].sort((a, b) => a - b)[values.length >> 1]!;

/** Median milliseconds of `runs` calls of `work`, after one to warm up. */
async function time(runs: number, work: (run: number) => Promise<unknown>): Promise<number> {
  await work(-1);
  const samples: number[] = [];
  for (let run = 0; run < runs; run += 1) {
    const t = performance.now();
    await work(run);
    samples.push(ms(t));
  }
  return median(samples);
}

/**
 * 10k thoughts as the derived-index prototype measured them: 1% share one big group, the rest
 * spread over n / 10 groups, under parents of 100 children.
 */
async function thoughts(n: number): Promise<Operation[]> {
  const { client } = await open();
  const writer = client.local.forReplica(replica);
  const ops: Operation[] = [];
  for (let i = 1; i <= n; i += 1) {
    const parent = id(10_000_000 + Math.floor(i / 100));
    if (i % 100 === 0 || i === 1) {
      ops.push(
        await writer.insert(
          root,
          parent,
          { type: 'last' },
          thought(`p${i}`, 1_600_000_000_000 + i),
        ),
      );
    }
    const value = i % 100 === 0 ? 'big' : `v${i % Math.floor(n / 10)}`;
    ops.push(
      await writer.insert(parent, id(i), { type: 'last' }, thought(value, 1_700_000_000_000 + i)),
    );
  }
  await client.close();
  return ops;
}

perf(
  'payload index at 10k nodes',
  async () => {
    const n = 10_000;
    const ops = await thoughts(n);

    /** Opens of the index whose events never come, as in tabs that reloaded without closing. */
    const orphan = async (client: Awaited<ReturnType<typeof open>>['client'], count: number) => {
      for (let i = 0; i < count; i += 1) {
        await createTreecrdtPayloadIndex(
          { onMaterialized: () => () => {}, runner: client.runner },
          { name: 'lexeme', path: '$.lexeme' },
        );
      }
    };

    // Write cost: a first sync into an empty database without the index, with the SQLite index
    // alone (closed: no triggers), with the index open, and with 20 orphaned opens as well.
    const appendInto = async (listening: boolean | null, orphans = 0) => {
      const { client } = await open();
      await orphan(client, orphans);
      const index =
        listening === null
          ? null
          : await createTreecrdtPayloadIndex(client, { name: 'lexeme', path: '$.lexeme' });
      if (listening === false) await index!.close();
      const t = performance.now();
      await client.ops.appendMany(ops);
      await index?.idle();
      const took = ms(t);
      await client.close();
      return took;
    };
    const appendPlain = await appendInto(null);
    const appendIndexOnly = await appendInto(false);
    const appendIndexed = await appendInto(true);
    const appendOrphans = await appendInto(true, 20);

    // 1000 local inserts, then an edit of each to a new key, until onChange reports the last.
    const localWrites = async (orphans: number) => {
      const { client } = await open();
      await orphan(client, orphans);
      const index = await createTreecrdtPayloadIndex(client, { name: 'lexeme', path: '$.lexeme' });
      const writer = client.local.forReplica(replica);
      const t = performance.now();
      for (let i = 1; i <= 1000; i += 1) {
        await writer.insert(root, id(i), { type: 'last' }, thought(`v${i}`, i));
      }
      for (let i = 1; i <= 1000; i += 1) await writer.payload(id(i), thought(`w${i}`, i));
      await index.idle();
      const took = ms(t);
      await client.close();
      return took;
    };
    const localWritesAlone = await localWrites(0);
    const localWritesOrphans = await localWrites(20);

    // Opening the index on a database that holds 10k thoughts builds it; opening it again does not.
    const { db, client } = await open();
    await client.ops.appendMany(ops);
    const pages = () =>
      (db.prepare('PRAGMA page_count').get() as { page_count: number }).page_count;
    const pagesBefore = pages();
    let t = performance.now();
    let index = await createTreecrdtPayloadIndex(client, { name: 'lexeme', path: '$.lexeme' });
    const create = ms(t);
    const pageSize = (db.prepare('PRAGMA page_size').get() as { page_size: number }).page_size;
    const indexKiB = ((pages() - pagesBefore) * pageSize) / 1024;
    await index.close();
    t = performance.now();
    index = await createTreecrdtPayloadIndex(client, { name: 'lexeme', path: '$.lexeme' });
    const reopen = ms(t);

    const big = (await index.lookup(['big'])).get('big')!;
    const keys = Array.from({ length: 50 }, (_, i) => `v${i + 1}`);
    const lookupBig = await time(20, () => index.lookup(['big'], { payloads: true }));
    const lookup50 = await time(20, () => index.lookup(keys, { payloads: true }));
    const found50 = [...(await index.lookup(keys)).values()].reduce((sum, m) => sum + m.length, 0);

    // A value edit, until onChange reports both keys.
    let reported: () => void = () => {};
    index.onChange((changed) => {
      if (changed.length === 2) reported();
    });
    const writer = client.local.forReplica(replica);
    const edit = await time(20, async (run) => {
      const report = new Promise<void>((resolve) => (reported = resolve));
      // Thought 7 starts as v7, and each run, the warm-up's -1 included, moves it to the other.
      await writer.payload(id(7), thought(Math.abs(run) % 2 ? 'v8' : 'v7', 1_700_000_000_007));
      await report;
    });

    console.log(
      JSON.stringify({
        n,
        bigGroup: big.length,
        appendPlain,
        appendIndexOnly,
        appendIndexed,
        appendOrphans,
        localWritesAlone,
        localWritesOrphans,
        create,
        indexKiB,
        reopen,
        lookupBig,
        lookup50,
        found50,
        edit,
      }),
    );
    expect(big.length).toBe(100);
    await index.close();
    await client.close();
  },
  600_000,
);

import assert from 'node:assert/strict';
import { test } from 'vitest';

import { createOpfsWriteAheadExecutor } from '../src/opfs-write-ahead.ts';

const TREECRDT_WRITE = 'SELECT treecrdt_local_insert(?1)';

function createHarness() {
  let autocommit = true;
  const injected: string[] = [];
  const executed: string[] = [];
  const executor = createOpfsWriteAheadExecutor({
    exec: (sql) => {
      injected.push(sql);
      if (sql === 'BEGIN IMMEDIATE') autocommit = false;
      if (sql === 'COMMIT' || sql === 'ROLLBACK') autocommit = true;
    },
    getAutocommit: () => autocommit,
  });

  return {
    execute: (sql: string) =>
      executor(
        sql,
        () => {
          executed.push(sql);
          if (/^BEGIN(?:\s+(?:DEFERRED|IMMEDIATE|EXCLUSIVE))?(?:\s+TRANSACTION)?$/i.test(sql)) {
            autocommit = false;
          }
          if (/^(?:COMMIT|END|ROLLBACK)(?:\s+TRANSACTION)?$/i.test(sql)) {
            autocommit = true;
          }
        },
        { allowTransactionControlBatch: true },
      ),
    executor,
    executed,
    injected,
    get autocommit() {
      return autocommit;
    },
    setAutocommit(value: boolean) {
      autocommit = value;
    },
  };
}

test('rollback-and-release batches finish the owned transaction instead of stranding its lock', async () => {
  const harness = createHarness();

  await harness.execute('SAVEPOINT outer');
  assert.equal(harness.autocommit, false);

  await harness.execute('ROLLBACK TO outer; RELEASE outer');
  assert.equal(harness.autocommit, true);

  await harness.execute(TREECRDT_WRITE);
  assert.deepEqual(harness.injected, ['BEGIN IMMEDIATE', 'COMMIT', 'BEGIN IMMEDIATE', 'COMMIT']);
});

test('quoted savepoint names and semicolons inside them retain the same ownership', async () => {
  const harness = createHarness();

  await harness.execute('SAVEPOINT "Outer; Savepoint"');
  await harness.execute(
    'ROLLBACK TRANSACTION TO SAVEPOINT "outer; savepoint"; RELEASE SAVEPOINT "OUTER; SAVEPOINT"',
  );

  assert.equal(harness.autocommit, true);
  assert.deepEqual(harness.injected, ['BEGIN IMMEDIATE', 'COMMIT']);
});

test('unsupported transaction-control batches fail before any SQL executes', async () => {
  const harness = createHarness();

  await assert.rejects(
    harness.execute('SAVEPOINT outer; SELECT 1'),
    /cannot safely track unsupported or mixed transaction-control SQL/,
  );
  await assert.rejects(
    harness.execute('SAVEPOINT "unterminated'),
    /cannot safely parse unterminated quoted SQL/,
  );

  assert.deepEqual(harness.executed, []);
  assert.deepEqual(harness.injected, []);
});

test('a synchronous rollback failure does not replace the original write error', async () => {
  const originalError = new Error('write failed');
  const executor = createOpfsWriteAheadExecutor({
    exec: (sql) => {
      if (sql === 'ROLLBACK') throw new Error('rollback failed');
    },
    getAutocommit: () => true,
  });

  await assert.rejects(
    executor(TREECRDT_WRITE, () => {
      throw originalError;
    }),
    (error) => error === originalError,
  );
});

test('caller BEGIN must acquire the write lock up front', async () => {
  for (const sql of ['BEGIN', 'BEGIN DEFERRED', 'BEGIN DEFERRED TRANSACTION']) {
    const harness = createHarness();
    await assert.rejects(
      harness.execute(sql),
      /requires BEGIN IMMEDIATE or BEGIN EXCLUSIVE; BEGIN DEFERRED is unsafe/,
    );
    assert.deepEqual(harness.executed, []);
    assert.deepEqual(harness.injected, []);
  }
});

test('caller-owned immediate and exclusive transactions remain caller-owned', async () => {
  for (const mode of ['IMMEDIATE', 'EXCLUSIVE']) {
    const harness = createHarness();
    await harness.execute(`BEGIN ${mode}`);
    await harness.execute(TREECRDT_WRITE);
    await harness.execute('ROLLBACK');

    assert.deepEqual(harness.injected, []);
    assert.deepEqual(harness.executed, [`BEGIN ${mode}`, TREECRDT_WRITE, 'ROLLBACK']);
  }
});

test('untracked non-autocommit transactions fail closed for TreeCRDT SELECT writes', async () => {
  const harness = createHarness();
  harness.setAutocommit(false);

  await assert.rejects(
    harness.execute(TREECRDT_WRITE),
    /cannot safely execute a TreeCRDT SELECT write in an untracked transaction/,
  );
  assert.deepEqual(harness.executed, []);
  assert.deepEqual(harness.injected, []);
});

test('ordinary SQL writes stay direct so SQLite can provide the VFS write hint', async () => {
  const harness = createHarness();
  const insert = 'INSERT INTO example(value) VALUES (1)';

  await harness.execute(insert);

  assert.deepEqual(harness.executed, [insert]);
  assert.deepEqual(harness.injected, []);
});

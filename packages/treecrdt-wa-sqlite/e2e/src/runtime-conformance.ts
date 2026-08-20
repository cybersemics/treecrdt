import {
  CLIENT_CLOSED_ERROR,
  createTreecrdtClient,
  detectOpfsSupport,
  type TreecrdtClient,
  type TreecrdtRuntime,
} from '@treecrdt/wa-sqlite';
import { nodeIdFromInt } from '@treecrdt/benchmark';
import { replicaFromLabel } from './op-helpers.js';

type Runtime = TreecrdtClient['runtime'];
type Scenario = 'opfs-recovery' | 'terminal-teardown';

type RuntimeConformanceOptions = {
  runtime: Runtime;
  scenario: Scenario;
};

const rootId = '0'.repeat(32);
const replica = replicaFromLabel('runtime-conformance');

function runtimeOptions(runtime: Runtime, sharedWorkerName: string): TreecrdtRuntime {
  return runtime === 'shared-worker'
    ? { type: 'shared-worker', name: sharedWorkerName }
    : { type: runtime };
}

async function assertUsable(client: TreecrdtClient, runtime: Runtime, nodeNumber: number) {
  if (client.runtime !== runtime) {
    throw new Error(`expected ${runtime} runtime, received ${client.runtime}`);
  }

  const node = nodeIdFromInt(nodeNumber);
  await client.local.insert(replica, rootId, node, { type: 'last' }, null);
  if (!(await client.tree.exists(node))) throw new Error('inserted node was not materialized');
}

async function assertClosed(client: TreecrdtClient) {
  let timeout: number | undefined;
  const outcome = await Promise.race([
    client.tree.nodeCount().then(
      () => ({ type: 'resolved' as const }),
      (error) => ({
        type: 'rejected' as const,
        message: error instanceof Error ? error.message : String(error),
      }),
    ),
    new Promise<{ type: 'timeout' }>((resolve) => {
      timeout = window.setTimeout(() => resolve({ type: 'timeout' }), 2_000);
    }),
  ]);
  if (timeout !== undefined) window.clearTimeout(timeout);

  if (outcome.type !== 'rejected' || outcome.message !== CLIENT_CLOSED_ERROR) {
    throw new Error(
      outcome.type === 'rejected'
        ? `expected ${CLIENT_CLOSED_ERROR}, received ${outcome.message}`
        : `call after teardown ${outcome.type}`,
    );
  }
}

async function runTerminalTeardown(runtime: Runtime, runId: string) {
  const sharedWorkerName = `runtime-terminal-${runId}`;
  const open = (label: string) =>
    createTreecrdtClient({
      docId: `runtime-terminal-${label}-${runId}`,
      runtime: runtimeOptions(runtime, sharedWorkerName),
      storage: { type: 'memory' },
    });

  const closed = await open('closed');
  try {
    await assertUsable(closed, runtime, 1);
    await closed.close();
    await closed.close();
    await closed.drop();
    await assertClosed(closed);
  } finally {
    await closed.close().catch(() => {});
  }

  const dropped = await open('dropped');
  try {
    await assertUsable(dropped, runtime, 2);
    await dropped.drop();
    await dropped.drop();
    await dropped.close();
    await assertClosed(dropped);
  } finally {
    await dropped.drop().catch(() => {});
  }

  // With SharedWorker, opening once after drop is insufficient: a retained source port only
  // becomes observable after that next client closes and a third database tries to initialize.
  const firstReuse = await open('first-reuse');
  try {
    await assertUsable(firstReuse, runtime, 3);
  } finally {
    await firstReuse.close();
  }

  const secondReuse = await open('second-reuse');
  try {
    await assertUsable(secondReuse, runtime, 4);
  } finally {
    await secondReuse.close();
  }
}

async function expectOpfsOpenFailure(
  runtime: Runtime,
  sharedWorkerName: string,
  runId: string,
  filename: string,
) {
  const outcome = await createTreecrdtClient({
    docId: `runtime-strict-opfs-${runId}`,
    runtime: runtimeOptions(runtime, sharedWorkerName),
    storage: { type: 'opfs', fallback: 'throw', filename },
  }).then(
    (client) => ({ client }),
    (error) => ({ error }),
  );

  if ('client' in outcome) {
    await outcome.client.drop();
    throw new Error('invalid OPFS path unexpectedly opened');
  }

  const message = outcome.error instanceof Error ? outcome.error.message : String(outcome.error);
  if (!/OPFS requested|sqlite3_open_v2/.test(message)) {
    throw new Error(`unexpected OPFS failure: ${message}`);
  }
}

async function runOpfsRecovery(runtime: Runtime, runId: string) {
  const support = detectOpfsSupport();
  if (!support.available) throw new Error(`OPFS unavailable: ${support.reason ?? 'unknown'}`);

  const invalidFilename = `/${'x'.repeat(512)}.db`;
  if (new TextEncoder().encode(invalidFilename).byteLength <= 512) {
    throw new Error('test filename must exceed the SQLite path limit');
  }

  const strictWorkerName = `runtime-strict-${runId}`;
  await expectOpfsOpenFailure(runtime, strictWorkerName, runId, invalidFilename);

  const openRetry = (label: string) =>
    createTreecrdtClient({
      docId: `runtime-retry-${label}-${runId}`,
      runtime: runtimeOptions(runtime, strictWorkerName),
      storage: { type: 'memory' },
    });

  const firstRetry = await openRetry('first');
  try {
    await assertUsable(firstRetry, runtime, 5);
  } finally {
    await firstRetry.close();
  }

  // A second retry catches a failed SharedWorker init port that remained hidden by the first one.
  const secondRetry = await openRetry('second');
  try {
    await assertUsable(secondRetry, runtime, 6);
  } finally {
    await secondRetry.close();
  }

  const fallback = await createTreecrdtClient({
    docId: `runtime-memory-fallback-${runId}`,
    runtime: runtimeOptions(runtime, `runtime-fallback-${runId}`),
    storage: { type: 'opfs', fallback: 'memory', filename: invalidFilename },
  });
  try {
    if (fallback.storage !== 'memory') {
      throw new Error(`expected memory fallback, received ${fallback.storage}`);
    }
    await assertUsable(fallback, runtime, 7);
  } finally {
    await fallback.close();
  }
}

const scenarioRunners: Record<Scenario, (runtime: Runtime, runId: string) => Promise<void>> = {
  'opfs-recovery': runOpfsRecovery,
  'terminal-teardown': runTerminalTeardown,
};

export function getRuntimeConformanceOpfsSupport(): ReturnType<typeof detectOpfsSupport> {
  return detectOpfsSupport();
}

export async function runRuntimeConformanceE2E(
  opts: RuntimeConformanceOptions,
): Promise<{ ok: true }> {
  const runId = crypto.randomUUID();
  try {
    await scenarioRunners[opts.scenario](opts.runtime, runId);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    throw new Error(`${opts.runtime} ${opts.scenario}: ${message}`);
  }
  return { ok: true };
}

declare global {
  interface Window {
    __treecrdtRuntimeConformance?: {
      support: typeof getRuntimeConformanceOpfsSupport;
      run: typeof runRuntimeConformanceE2E;
    };
  }
}

if (typeof window !== 'undefined') {
  window.__treecrdtRuntimeConformance = {
    support: getRuntimeConformanceOpfsSupport,
    run: runRuntimeConformanceE2E,
  };
}

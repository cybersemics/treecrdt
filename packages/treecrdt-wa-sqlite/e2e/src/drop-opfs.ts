import { createTreecrdtClient } from '@treecrdt/wa-sqlite/client';
import { detectOpfsSupport, opfsStorageExists } from '@treecrdt/wa-sqlite/opfs';
import { makeOp, nodeIdFromInt } from '@treecrdt/benchmark';
import { orderKeyFromPosition, replicaFromLabel } from './op-helpers.js';

export async function runDropStorageE2E(): Promise<{ ok: true } | { ok: false; error: string }> {
  const support = detectOpfsSupport();
  if (!support.available) {
    return { ok: false, error: `OPFS unavailable: ${support.reason ?? 'unknown'}` };
  }

  const baseUrl =
    typeof window !== 'undefined' ? new URL('.', window.location.href).href : undefined;

  const filename = `/drop-test-${crypto.randomUUID()}.db`;
  const client = await createTreecrdtClient({
    storage: { type: 'opfs', filename, fallback: 'throw' },
    runtime: { type: 'dedicated-worker' },
    assets: { baseUrl },
  });

  try {
    const root = '0'.repeat(32);
    const replica = replicaFromLabel('drop-test');
    const op = makeOp(replica, 1, 1, {
      type: 'insert',
      parent: root,
      node: nodeIdFromInt(1),
      orderKey: orderKeyFromPosition(0),
    });
    await client.ops.append(op);

    const existsBefore = await opfsStorageExists(filename);
    if (!existsBefore) {
      return { ok: false, error: 'OPFS storage should exist after append' };
    }

    await client.drop();
    const existsAfter = await opfsStorageExists(filename);
    if (existsAfter) {
      return { ok: false, error: 'OPFS storage should be fully deleted after drop' };
    }

    return { ok: true };
  } finally {
    await client.drop().catch(() => {});
  }
}

declare global {
  interface Window {
    __runDropStorageE2E?: typeof runDropStorageE2E;
  }
}

if (typeof window !== 'undefined') {
  window.__runDropStorageE2E = runDropStorageE2E;
}

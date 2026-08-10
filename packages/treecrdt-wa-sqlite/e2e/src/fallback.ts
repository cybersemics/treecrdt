import { createTreecrdtClient, detectOpfsSupport } from '@treecrdt/wa-sqlite';
import { replicaFromLabel } from './op-helpers.js';

const rootId = '0'.repeat(32);
const nodeId = '1'.repeat(32);
const payload = new TextEncoder().encode('memory fallback works');
const overlongOpfsFilename = `/${'x'.repeat(512)}.db`;

export async function runDedicatedWorkerFallback() {
  const client = await createTreecrdtClient({
    docId: `fallback-${crypto.randomUUID()}`,
    storage: { type: 'opfs', filename: overlongOpfsFilename, fallback: 'memory' },
    runtime: { type: 'dedicated-worker' },
  });

  try {
    await client.local.insert(
      replicaFromLabel('fallback'),
      rootId,
      nodeId,
      { type: 'last' },
      payload,
    );

    const storedPayload = await client.tree.getPayload(nodeId);
    return {
      mode: client.mode,
      runtime: client.runtime,
      storage: client.storage,
      children: await client.tree.children(rootId),
      payload: storedPayload ? new TextDecoder().decode(storedPayload) : null,
    };
  } finally {
    await client.drop();
  }
}

declare global {
  interface Window {
    __treecrdtFallback?: {
      run: typeof runDedicatedWorkerFallback;
      support: typeof detectOpfsSupport;
    };
  }
}

if (typeof window !== 'undefined') {
  window.__treecrdtFallback = {
    run: runDedicatedWorkerFallback,
    support: detectOpfsSupport,
  };
}

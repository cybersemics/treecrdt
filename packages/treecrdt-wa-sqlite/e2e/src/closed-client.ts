import { CLIENT_CLOSED_ERROR, createTreecrdtClient, detectOpfsSupport } from '@treecrdt/wa-sqlite';
import { makeOp, nodeIdFromInt } from '@treecrdt/benchmark';
import { orderKeyFromPosition, replicaFromLabel } from './op-helpers.js';

export async function runClosedClientE2E(): Promise<{ ok: true } | { ok: false; error: string }> {
  const root = '0'.repeat(32);
  const replica = replicaFromLabel('closed-test');
  const op = makeOp(replica, 1, 1, {
    type: 'insert',
    parent: root,
    node: nodeIdFromInt(1),
    orderKey: orderKeyFromPosition(0),
  });

  // --- Direct client (memory)
  const directClient = await createTreecrdtClient({ docId: 'e2e-closed-direct' });
  await directClient.close();

  const directAppendAfterClose = await directClient.ops.append(op).then(
    () => ({ ok: false as const, error: 'append after close should have rejected' }),
    (err) =>
      err instanceof Error && err.message === CLIENT_CLOSED_ERROR
        ? { ok: true as const }
        : { ok: false as const, error: `wrong error: ${err}` },
  );
  if (!directAppendAfterClose.ok) return directAppendAfterClose;

  const directDoubleClose = await directClient.close().then(() => ({ ok: true as const }));
  if (!directDoubleClose.ok) return directDoubleClose;

  // --- Direct client: drop then call
  const directDrop = await createTreecrdtClient({ docId: 'e2e-closed-direct-drop' });
  await directDrop.drop();
  const directAppendAfterDrop = await directDrop.ops.append(op).then(
    () => ({ ok: false as const, error: 'append after drop should have rejected' }),
    (err) =>
      err instanceof Error && err.message === CLIENT_CLOSED_ERROR
        ? { ok: true as const }
        : { ok: false as const, error: `wrong error: ${err}` },
  );
  if (!directAppendAfterDrop.ok) return directAppendAfterDrop;

  const directDoubleDrop = await directDrop.drop().then(() => ({ ok: true as const }));
  if (!directDoubleDrop.ok) return directDoubleDrop;

  // --- Worker client (opfs) if available
  const support = detectOpfsSupport();
  if (support.available) {
    const workerClient = await createTreecrdtClient({
      docId: `closed-test-${crypto.randomUUID()}`,
      persistent: true,
    });
    await workerClient.close();

    const workerAppendAfterClose = await workerClient.ops.append(op).then(
      () => ({ ok: false as const, error: 'worker append after close should have rejected' }),
      (err) =>
        err instanceof Error && err.message === CLIENT_CLOSED_ERROR
          ? { ok: true as const }
          : { ok: false as const, error: `wrong error: ${err}` },
    );
    if (!workerAppendAfterClose.ok) return workerAppendAfterClose;

    const workerDoubleClose = await workerClient.close().then(() => ({ ok: true as const }));
    if (!workerDoubleClose.ok) return workerDoubleClose;
  }

  return { ok: true };
}

declare global {
  interface Window {
    __runClosedClientE2E?: typeof runClosedClientE2E;
  }
}

if (typeof window !== 'undefined') {
  window.__runClosedClientE2E = runClosedClientE2E;
}

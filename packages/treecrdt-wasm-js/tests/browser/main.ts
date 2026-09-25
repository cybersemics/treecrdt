import { createMemoryClient } from '@treecrdt/wasm';
import { createMemorySyncBackend } from '@treecrdt/wasm/sync';

declare global {
  interface Window {
    verifyMemoryClient: () => Promise<unknown>;
  }
}

window.verifyMemoryClient = async () => {
  const client = await createMemoryClient();
  const root = '0'.repeat(32);
  const node = '1'.repeat(32);
  let notifications = 0;
  client.subscribe(() => {
    notifications += 1;
  });
  try {
    const result = client.transact((document) => {
      document.local.insert(root, node, null, new Uint8Array([1, 2, 3]));
      return document.getSnapshot().get(node)?.children.length;
    });
    const backend = createMemorySyncBackend(client, { docId: 'browser-document' });
    const refs = await backend.listOpRefs({ all: {} });
    const synced = await backend.getOpsByOpRefs(refs);
    return {
      value: result.value,
      synchronous: !(result instanceof Promise),
      notifications,
      payload: Array.from(client.getSnapshot().get(node)!.payload!),
      children: client.tree.children(root),
      opCount: result.operations.length,
      syncRefSize: refs[0]?.length,
      syncedKind: synced[0]?.kind.type,
    };
  } finally {
    client.close();
  }
};

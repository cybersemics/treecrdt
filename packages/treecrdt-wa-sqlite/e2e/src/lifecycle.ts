import { createTreecrdtClient, type TreecrdtClient } from '@treecrdt/wa-sqlite/client';
import { detectOpfsSupport } from '@treecrdt/wa-sqlite/opfs';
import { nodeIdFromInt } from '@treecrdt/benchmark';
import { replicaFromLabel } from './op-helpers.js';

export type LifecycleRuntime = 'direct' | 'dedicated-worker';

const rootId = '0'.repeat(32);
const parentId = nodeIdFromInt(901);
const childId = nodeIdFromInt(902);
const replica = replicaFromLabel('lifecycle');
const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

let openClient: TreecrdtClient | null = null;

type LifecycleOptions = {
  docId: string;
  filename: string;
  runtime: LifecycleRuntime;
};

export type LifecycleState = {
  parentId: string;
  childId: string;
  mode: string;
  runtime: string;
  storage: string;
  headLamport: number;
  rootChildren: string[];
  parentChildren: string[];
  parentExists: boolean;
  childExists: boolean;
  childParent: string | null;
  parentPayload: string | null;
  childPayload: string | null;
};

async function createOpfsLifecycleClient(opts: LifecycleOptions): Promise<TreecrdtClient> {
  return createTreecrdtClient({
    docId: opts.docId,
    storage: { type: 'opfs', filename: opts.filename, fallback: 'throw' },
    runtime: { type: opts.runtime },
  });
}

async function summarizeLifecycleState(client: TreecrdtClient): Promise<LifecycleState> {
  const parentPayload = await client.tree.getPayload(parentId);
  const childPayload = await client.tree.getPayload(childId);
  return {
    parentId,
    childId,
    mode: client.mode,
    runtime: client.runtime,
    storage: client.storage,
    headLamport: await client.meta.headLamport(),
    rootChildren: await client.tree.children(rootId),
    parentChildren: await client.tree.children(parentId),
    parentExists: await client.tree.exists(parentId),
    childExists: await client.tree.exists(childId),
    childParent: await client.tree.parent(childId),
    parentPayload: parentPayload ? textDecoder.decode(parentPayload) : null,
    childPayload: childPayload ? textDecoder.decode(childPayload) : null,
  };
}

export function getLifecycleOpfsSupport(): ReturnType<typeof detectOpfsSupport> {
  return detectOpfsSupport();
}

export async function dropLifecycleStore(opts: LifecycleOptions): Promise<void> {
  const clientToClose = openClient;
  openClient = null;
  await clientToClose?.close().catch(() => {});

  const client = await createOpfsLifecycleClient(opts);
  await client.drop();
}

export async function writeLifecycleTree(
  opts: LifecycleOptions & { closeBeforeReload?: boolean },
): Promise<LifecycleState> {
  const prior = openClient;
  openClient = null;
  await prior?.close().catch(() => {});

  const client = await createOpfsLifecycleClient(opts);
  await client.local.insert(
    replica,
    rootId,
    parentId,
    { type: 'last' },
    textEncoder.encode('browser lifecycle parent'),
  );
  await client.local.insert(
    replica,
    parentId,
    childId,
    { type: 'last' },
    textEncoder.encode('browser lifecycle child'),
  );

  const state = await summarizeLifecycleState(client);
  if (opts.closeBeforeReload) {
    await client.close();
  } else {
    openClient = client;
  }
  return state;
}

export async function readLifecycleTree(opts: LifecycleOptions): Promise<LifecycleState> {
  const prior = openClient;
  openClient = null;
  await prior?.close().catch(() => {});

  const client = await createOpfsLifecycleClient(opts);
  try {
    return await summarizeLifecycleState(client);
  } finally {
    await client.close();
  }
}

declare global {
  interface Window {
    __treecrdtLifecycle?: {
      support: typeof getLifecycleOpfsSupport;
      drop: typeof dropLifecycleStore;
      write: typeof writeLifecycleTree;
      read: typeof readLifecycleTree;
    };
  }
}

if (typeof window !== 'undefined') {
  window.__treecrdtLifecycle = {
    support: getLifecycleOpfsSupport,
    drop: dropLifecycleStore,
    write: writeLifecycleTree,
    read: readLifecycleTree,
  };
}

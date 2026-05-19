import { createTreecrdtClient, type TreecrdtClient } from '@treecrdt/wa-sqlite/client';
import { detectOpfsSupport } from '@treecrdt/wa-sqlite/opfs';
import { nodeIdFromInt } from '@treecrdt/benchmark';
import { replicaFromLabel } from './op-helpers.js';

const rootId = '0'.repeat(32);
const parentId = nodeIdFromInt(901);
const childId = nodeIdFromInt(902);
const replica = replicaFromLabel('direct-opfs-reload');
const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

let openClient: TreecrdtClient | null = null;

type DirectOpfsReloadOptions = {
  docId: string;
  filename: string;
};

export type DirectOpfsReloadState = {
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

async function createDirectOpfsClient(opts: DirectOpfsReloadOptions): Promise<TreecrdtClient> {
  return createTreecrdtClient({
    docId: opts.docId,
    storage: { type: 'opfs', filename: opts.filename, fallback: 'throw' },
    runtime: { type: 'direct' },
  });
}

async function summarizeDirectOpfsState(client: TreecrdtClient): Promise<DirectOpfsReloadState> {
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

export function getDirectOpfsSupport(): ReturnType<typeof detectOpfsSupport> {
  return detectOpfsSupport();
}

export async function dropDirectOpfsReloadStore(opts: DirectOpfsReloadOptions): Promise<void> {
  const clientToClose = openClient;
  openClient = null;
  await clientToClose?.close().catch(() => {});

  const client = await createDirectOpfsClient(opts);
  await client.drop();
}

export async function writeDirectOpfsReloadTree(
  opts: DirectOpfsReloadOptions & { closeBeforeReload?: boolean },
): Promise<DirectOpfsReloadState> {
  const prior = openClient;
  openClient = null;
  await prior?.close().catch(() => {});

  const client = await createDirectOpfsClient(opts);
  await client.local.insert(
    replica,
    rootId,
    parentId,
    { type: 'last' },
    textEncoder.encode('direct opfs parent'),
  );
  await client.local.insert(
    replica,
    parentId,
    childId,
    { type: 'last' },
    textEncoder.encode('direct opfs child'),
  );

  const state = await summarizeDirectOpfsState(client);
  if (opts.closeBeforeReload) {
    await client.close();
  } else {
    openClient = client;
  }
  return state;
}

export async function readDirectOpfsReloadTree(
  opts: DirectOpfsReloadOptions,
): Promise<DirectOpfsReloadState> {
  const prior = openClient;
  openClient = null;
  await prior?.close().catch(() => {});

  const client = await createDirectOpfsClient(opts);
  try {
    return await summarizeDirectOpfsState(client);
  } finally {
    await client.close();
  }
}

declare global {
  interface Window {
    __treecrdtDirectOpfsReload?: {
      support: typeof getDirectOpfsSupport;
      drop: typeof dropDirectOpfsReloadStore;
      write: typeof writeDirectOpfsReloadTree;
      read: typeof readDirectOpfsReloadTree;
    };
  }
}

if (typeof window !== 'undefined') {
  window.__treecrdtDirectOpfsReload = {
    support: getDirectOpfsSupport,
    drop: dropDirectOpfsReloadStore,
    write: writeDirectOpfsReloadTree,
    read: readDirectOpfsReloadTree,
  };
}

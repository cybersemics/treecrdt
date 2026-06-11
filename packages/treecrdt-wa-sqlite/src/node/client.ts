import type { ClientOptions, TreecrdtClient, TreecrdtStorage } from '../types.js';
import { buildDirectClient } from '../client.js';
import { openTreecrdtDbNode } from './open.js';

function storageFilename(storage: TreecrdtStorage | undefined): string | undefined {
  if (!storage || storage.type === 'memory') return undefined;
  return storage.filename;
}

function normalizeStorageOptions(opts: ClientOptions) {
  const raw = opts.storage ?? { type: 'auto' };
  if (raw.type === 'opfs') {
    throw new Error(
      'OPFS is not supported in Node; use storage: { type: "memory" } or @treecrdt/sqlite-node for file persistence',
    );
  }
  if (raw.type !== 'memory' && raw.type !== 'auto') {
    throw new Error(
      'createTreecrdtClient on Node supports storage: { type: "memory" } or { type: "auto" }',
    );
  }
}

function normalizeRuntimeOptions(opts: ClientOptions) {
  const runtime = opts.runtime ?? { type: 'auto' };
  if (runtime.type === 'dedicated-worker' || runtime.type === 'shared-worker') {
    throw new Error('Worker runtimes are browser-only');
  }
}

function normalizeAssetsBaseUrl(baseUrl?: string): string | undefined {
  if (baseUrl === undefined) return undefined;
  return baseUrl.endsWith('/') ? baseUrl : `${baseUrl}/`;
}

/** Node entry for createTreecrdtClient (in-memory WASM, direct runtime). */
export async function createTreecrdtClient(opts: ClientOptions = {}): Promise<TreecrdtClient> {
  normalizeStorageOptions(opts);
  normalizeRuntimeOptions(opts);
  const docId = opts.docId ?? 'treecrdt';
  const baseUrl = normalizeAssetsBaseUrl(opts.assets?.baseUrl);
  return buildDirectClient(
    {
      baseUrl,
      filename: storageFilename(opts.storage),
      storage: 'memory',
      docId,
      requireOpfs: false,
    },
    openTreecrdtDbNode,
  );
}

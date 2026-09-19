import type { ClientOptions, TreecrdtClient } from '../types.js';
import { buildDirectClient } from '../client.js';
import { openTreecrdtDbWithLoader } from '../open-core.js';
import { validateDocId } from '../runtime/resolve.js';
import { loadWaSqliteNode } from './load-wa-sqlite.js';

/** Node entry for createTreecrdtClient (in-memory WASM, direct runtime). */
export async function createTreecrdtClient(opts: ClientOptions): Promise<TreecrdtClient> {
  const docId = validateDocId(opts.docId);
  if (opts.persistent) {
    throw new Error(
      'persistent storage is not supported on Node; use @treecrdt/sqlite-node for file persistence',
    );
  }
  return buildDirectClient({ storage: 'memory', docId }, (openOptions) =>
    openTreecrdtDbWithLoader(openOptions, () => loadWaSqliteNode(openOptions.baseUrl)),
  );
}

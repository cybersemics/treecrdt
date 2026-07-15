import {
  openTreecrdtDbFromLoaded,
  type OpenTreecrdtDbOptions,
  type OpenTreecrdtDbResult,
} from '../open-core.js';
import { loadWaSqliteNode } from './load-wa-sqlite.js';

/** Node entry: loads wa-sqlite WASM from the filesystem (in-memory only). */
export async function openTreecrdtDbNode(
  opts: OpenTreecrdtDbOptions,
): Promise<OpenTreecrdtDbResult> {
  if (opts.storage === 'opfs' && opts.requireOpfs) {
    throw new Error('OPFS is not supported in Node');
  }
  const loaded = await loadWaSqliteNode(opts.baseUrl);
  return openTreecrdtDbFromLoaded({ ...opts, storage: 'memory' }, loaded);
}

import {
  openTreecrdtDbWithLoader,
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
  return openTreecrdtDbWithLoader({ ...opts, storage: 'memory' }, () =>
    loadWaSqliteNode(opts.baseUrl),
  );
}

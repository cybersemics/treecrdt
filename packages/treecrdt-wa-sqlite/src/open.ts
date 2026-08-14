import { loadWaSqliteBrowser } from './load-wa-sqlite.browser.js';
import {
  openTreecrdtDbWithLoader,
  type OpenTreecrdtDbOptions,
  type OpenTreecrdtDbResult,
} from './open-core.js';

export type { OpenTreecrdtDbOptions, OpenTreecrdtDbResult };

/** Browser/worker entry: loads wa-sqlite assets from public URLs. */
export async function openTreecrdtDb(opts: OpenTreecrdtDbOptions): Promise<OpenTreecrdtDbResult> {
  const load = (build: 'sync' | 'asyncify') =>
    loadWaSqliteBrowser({ assetsDir: opts.baseUrl, build });
  return openTreecrdtDbWithLoader(
    opts,
    // OPFSAnyContextVFS performs async I/O and requires Asyncify. OPFSCoopSyncVFS and memory
    // databases use the synchronous build.
    () => load(opts.storage === 'opfs' && opts.opfsVfs === 'any-context' ? 'asyncify' : 'sync'),
    () => load('sync'),
  );
}

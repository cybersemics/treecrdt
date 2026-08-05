import { loadWaSqliteBrowser } from './load-wa-sqlite.browser.js';
import {
  openTreecrdtDbWithLoader,
  type OpenTreecrdtDbOptions,
  type OpenTreecrdtDbResult,
} from './open-core.js';

export type { OpenTreecrdtDbOptions, OpenTreecrdtDbResult };

/** Browser/worker entry: loads wa-sqlite assets from public URLs. */
export async function openTreecrdtDb(opts: OpenTreecrdtDbOptions): Promise<OpenTreecrdtDbResult> {
  return openTreecrdtDbWithLoader(opts, () => loadWaSqliteBrowser({ assetsDir: opts.baseUrl }));
}

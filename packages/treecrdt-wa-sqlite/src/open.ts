import { loadWaSqliteBrowser } from './load-wa-sqlite.browser.js';
import {
  openTreecrdtDbFromLoaded,
  type OpenTreecrdtDbOptions,
  type OpenTreecrdtDbResult,
} from './open-core.js';

export type { OpenTreecrdtDbOptions, OpenTreecrdtDbResult };

/** Browser/worker entry: loads wa-sqlite assets from public URLs. */
export async function openTreecrdtDb(opts: OpenTreecrdtDbOptions): Promise<OpenTreecrdtDbResult> {
  const loadFresh = () => loadWaSqliteBrowser({ assetsDir: opts.baseUrl });
  const loaded = await loadFresh();
  return openTreecrdtDbFromLoaded(opts, loaded, loadFresh);
}

import { createBrowserTreecrdtClient } from './client.js';
import { openTreecrdtDb } from './open.js';
import type { ClientOptions, TreecrdtClient } from './types.js';

/** Browser entry for createTreecrdtClient (Vite-hashed WASM + OPFS/worker runtimes). */
export async function createTreecrdtClient(opts: ClientOptions = {}): Promise<TreecrdtClient> {
  return createBrowserTreecrdtClient(opts, openTreecrdtDb);
}

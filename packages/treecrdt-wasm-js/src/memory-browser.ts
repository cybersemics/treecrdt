import { createMemoryClientFactory } from './memory-client.js';
import { loadWasm } from './wasm-browser.js';

export { loadWasm as initializeMemoryWasm } from './wasm-browser.js';

export type {
  MemoryClient,
  MemoryClientOptions,
  MemoryContent,
  MemoryRow,
  MemoryReader,
} from './memory-client.js';

/** Opens a synchronous in-memory client after loading its platform WASM runtime. */
export const createMemoryClient = createMemoryClientFactory(loadWasm);

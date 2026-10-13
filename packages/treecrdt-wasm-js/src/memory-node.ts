import { createMemoryClientFactory } from './memory-client.js';
import type { InitInput } from '../pkg-web/treecrdt_wasm.js';

/** Node loads the adjacent native package; the optional browser initialization input is unused. */
export function initializeMemoryWasm(_input?: InitInput) {
  return import('../pkg/treecrdt_wasm.js');
}

export type {
  MemoryClient,
  MemoryClientOptions,
  MemoryContent,
  MemoryRow,
  MemoryReader,
} from './memory-client.js';

/** Opens a synchronous in-memory client after loading its platform WASM runtime. */
export const createMemoryClient = createMemoryClientFactory(initializeMemoryWasm);

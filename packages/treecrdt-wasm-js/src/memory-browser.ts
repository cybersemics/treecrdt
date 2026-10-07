import { bytesToHex } from '@treecrdt/interface/ids';
import { createInitializedMemoryClient, type MemoryClientOptions } from './memory-client.js';
import { loadWasm } from './wasm-browser.js';

export { loadWasm as initializeMemoryWasm } from './wasm-browser.js';

export type {
  MemoryClient,
  MemoryClientOptions,
  MemoryChanges,
  MemoryRowChange,
  MemoryRow,
  MemoryReader,
  MemoryTransaction,
} from './memory-client.js';

/** Opens a synchronous in-memory client after loading its platform WASM runtime. */
export async function createMemoryClient(options: MemoryClientOptions = {}) {
  const replica = options.replicaId ?? crypto.getRandomValues(new Uint8Array(32));
  if (replica.length !== 32) throw new Error('A memory replica id must contain 32 bytes');
  const replicaHex = bytesToHex(replica);
  const { WasmTree } = await loadWasm();
  return createInitializedMemoryClient(new WasmTree(replicaHex));
}

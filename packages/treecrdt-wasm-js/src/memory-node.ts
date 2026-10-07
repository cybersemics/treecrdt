import { bytesToHex } from '@treecrdt/interface/ids';
import { createInitializedMemoryClient, type MemoryClientOptions } from './memory-client.js';
import type { InitInput } from '../pkg-web/treecrdt_wasm.js';

/** Node loads the adjacent native package; the optional browser initialization input is unused. */
export function initializeMemoryWasm(_input?: InitInput) {
  return import('../pkg/treecrdt_wasm.js');
}

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
  const { WasmTree } = await initializeMemoryWasm();
  return createInitializedMemoryClient(new WasmTree(replicaHex));
}

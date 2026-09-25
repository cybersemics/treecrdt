import init, { type InitInput } from '../pkg-web/treecrdt_wasm.js';
import { createInitializedMemoryClient, type MemoryClientOptions } from './memory-client.js';

export type {
  MemoryClient,
  MemoryClientOptions,
  MemorySnapshot,
  MemorySnapshotRow,
} from './memory-client.js';
export type { MemorySnapshotRow as TreeSnapshotRow } from './memory-client.js';
export type { InitInput } from '../pkg-web/treecrdt_wasm.js';

/** Initializes the browser artifact explicitly; no network or WASM work happens during import. */
let initialized: Promise<void> | undefined;
export function initializeMemoryWasm(
  wasm: InitInput = new URL('../pkg-web/treecrdt_wasm_bg.wasm', import.meta.url),
): Promise<void> {
  initialized ??= init({ module_or_path: wasm })
    .then(() => {})
    .catch((error) => {
      initialized = undefined;
      throw error;
    });
  return initialized;
}

/** Opens an independent synchronous in-memory client after loading the shared WASM module. */
export async function createMemoryClient(options: MemoryClientOptions = {}) {
  await initializeMemoryWasm(options.wasm);
  return createInitializedMemoryClient(options);
}

import { readFile } from 'node:fs/promises';
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

let initialized: Promise<void> | undefined;

/** Loads the same artifact as browsers, using file bytes instead of fetching a file URL. */
export function initializeMemoryWasm(wasm?: InitInput): Promise<void> {
  initialized ??= (
    wasm !== undefined
      ? Promise.resolve(wasm)
      : readFile(new URL('../pkg-web/treecrdt_wasm_bg.wasm', import.meta.url)).then((bytes) =>
          Uint8Array.from(bytes),
        )
  )
    .then((module) => init({ module_or_path: module }))
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

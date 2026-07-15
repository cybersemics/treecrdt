export type {
  ClientMode,
  ClientOptions,
  Database,
  RuntimeMode,
  StorageMode,
  TreecrdtAssets,
  TreecrdtClient,
  TreecrdtRuntime,
  TreecrdtStorage,
} from './types.js';

export type { OpfsSupport, OpfsVfsKind, OpfsVfsOptions, OpenOptions } from './opfs.js';
export {
  clearOpfsStorage,
  createOpfsVfs,
  detectOpfsSupport,
  openWithStorage,
  opfsStorageExists,
} from './opfs.js';

export { CLIENT_CLOSED_ERROR, createTreecrdtClient } from './client.js';

export { createWaSqliteApi } from './adapter.js';
export { initializeTreecrdtExtension } from './extension.js';

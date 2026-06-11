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

export { createTreecrdtClient } from './node/client.js';
export { CLIENT_CLOSED_ERROR } from './client.js';

export { createWaSqliteApi } from './adapter.js';

export { loadWaSqliteNode } from './node/load-wa-sqlite.js';
export { openTreecrdtDbNode } from './node/open.js';

export type {
  ClientMode,
  ClientOptions,
  Database,
  RuntimeMode,
  StorageMode,
  TreecrdtClient,
} from './types.js';

export type { OpfsSupport, OpfsVfsKind, OpfsVfsOptions, OpenOptions } from './opfs.js';
export {
  clearOpfsStorage,
  createOpfsVfs,
  detectOpfsSupport,
  openWithStorage,
  opfsFilenameForDocId,
  opfsStorageExists,
} from './opfs.js';

export { CLIENT_CLOSED_ERROR } from './client.js';
export { createTreecrdtClient } from './client.browser.js';

export { initializeTreecrdtExtension } from './extension.js';

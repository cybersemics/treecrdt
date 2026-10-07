export type {
  ClientMode,
  ClientOptions,
  Database,
  RuntimeMode,
  StorageMode,
  TreecrdtClient,
} from './types.js';

export { createTreecrdtClient } from './node/client.js';
export { CLIENT_CLOSED_ERROR } from './client.js';

export { initializeTreecrdtExtension } from './extension.js';

export { loadWaSqliteNode } from './node/load-wa-sqlite.js';

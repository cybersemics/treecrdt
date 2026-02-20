import type { Database as AdapterDatabase } from '../index.js';

declare module 'wa-sqlite' {
  export type Statement = unknown;

  export interface Database extends AdapterDatabase {}

  export type ModuleFactoryOptions = {
    locateFile?: (path: string, prefix?: string) => string | URL;
    wasm?: string | URL;
  };

  export type ModuleFactory = (opts?: ModuleFactoryOptions) => Promise<any>;

  const factory: ModuleFactory;
  export default factory;
}

declare module 'wa-sqlite/sqlite-api' {
  export type SQLiteAPI = any;
  export function Factory(module: any): SQLiteAPI;
}

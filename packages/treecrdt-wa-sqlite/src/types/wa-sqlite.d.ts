declare module "wa-sqlite" {
  export type Statement = unknown;

  export interface Database {
    prepare(sql: string): Promise<Statement>;
    bind(stmt: Statement, index: number, value: unknown): Promise<void>;
    step(stmt: Statement): Promise<number>;
    column_text(stmt: Statement, index: number): Promise<string>;
    finalize(stmt: Statement): Promise<void>;
    exec(sql: string): Promise<void>;
  }

  export type ModuleFactoryOptions = {
    locateFile?: (path: string, prefix?: string) => string | URL;
    wasm?: string | URL;
  };

  export type ModuleFactory = (opts?: ModuleFactoryOptions) => Promise<any>;

  const factory: ModuleFactory;
  export default factory;
}

declare module "wa-sqlite/sqlite-api" {
  export type SQLiteAPI = any;
  export function Factory(module: any): SQLiteAPI;
}

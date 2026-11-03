import { vi } from 'vitest';

/** Minimal wa-sqlite WASM module with a registration spy. */
export function createFakeModule(initResult = 0) {
  const init = vi.fn(() => initResult);
  return {
    _treecrdt_sqlite_init: init,
    init,
  };
}

export type FakeSqliteOptions = {
  failDocId?: string;
  failOpen?: string;
  failOpenError?: Error;
};

const SQLITE_ROW = 100;
const SQLITE_DONE = 101;
const SQLITE_TEXT = 3;

/** In-memory wa-sqlite API stand-in for open/init tests (no real WASM). */
export function createFakeSqlite(opts: FakeSqliteOptions = {}) {
  let nextHandle = 1;
  let nextStatement = 100;
  const schemaStatements = new Set<number>();
  const stepped = new Set<number>();

  return {
    vfs_register: vi.fn(),
    open_v2: vi.fn(async (filename: string) => {
      if (filename === opts.failOpen) throw opts.failOpenError ?? new Error('OPFS open failed');
      return nextHandle++;
    }),
    statements: vi.fn((_handle: number, sql: string) => {
      const statement = nextStatement++;
      // Kysely compiles tagged templates with lowercase keywords.
      if (/select\s+treecrdt_schema\s*\(\s*\)/i.test(sql)) schemaStatements.add(statement);
      return {
        next: async () => ({ value: statement }),
        return: async () => undefined,
      };
    }),
    bind: vi.fn(async (_statement: number, _index: number, value: unknown) => {
      if (value === opts.failDocId) throw new Error('setDocId failed');
    }),
    step: vi.fn(async (statement: number) => {
      if (!schemaStatements.has(statement)) return SQLITE_DONE;
      if (stepped.has(statement)) return SQLITE_DONE;
      stepped.add(statement);
      return SQLITE_ROW;
    }),
    column_count: vi.fn((statement: number) => (schemaStatements.has(statement) ? 1 : 0)),
    column_name: vi.fn(() => 'treecrdt_schema()'),
    column_type: vi.fn((statement: number) =>
      schemaStatements.has(statement) ? SQLITE_TEXT : 5,
    ),
    column_text: vi.fn((statement: number) =>
      schemaStatements.has(statement) ? 'CREATE TABLE example (id INTEGER)' : null,
    ),
    column_int: vi.fn(() => 0),
    column_double: vi.fn(() => 0),
    column_blob: vi.fn(() => null),
    finalize: vi.fn(),
    exec: vi.fn(),
    close: vi.fn(),
  };
}

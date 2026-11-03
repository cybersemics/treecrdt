import {
  Kysely,
  SqliteAdapter,
  SqliteIntrospector,
  SqliteQueryCompiler,
  type CompiledQuery,
  type DatabaseConnection,
  type Driver,
  type QueryResult,
} from 'kysely';

import type { TreecrdtDb } from './schema.js';

export type SqlValue = string | number | bigint | Uint8Array | null;
export type SqlParam = number | string | Uint8Array | null;

/** Executes compiled SQL against a TreeCRDT SQLite connection. */
export type TreecrdtSqlExecutor = {
  query: (sql: string, params?: SqlParam[]) => Promise<Record<string, SqlValue>[]>;
};

class TreecrdtSqlConnection implements DatabaseConnection {
  constructor(private readonly executor: TreecrdtSqlExecutor) {}

  async executeQuery<R>(compiledQuery: CompiledQuery): Promise<QueryResult<R>> {
    const rows = await this.executor.query(
      compiledQuery.sql,
      compiledQuery.parameters as SqlParam[],
    );
    return { rows: rows as R[] };
  }

  async *streamQuery<R>(): AsyncIterableIterator<QueryResult<R>> {
    throw new Error('TreeCRDT SQL executor does not support streaming');
  }
}

class TreecrdtSqlDriver implements Driver {
  private connection: TreecrdtSqlConnection | null = null;

  constructor(private readonly executor: TreecrdtSqlExecutor) {}

  async init(): Promise<void> {
    this.connection = new TreecrdtSqlConnection(this.executor);
  }

  async acquireConnection(): Promise<DatabaseConnection> {
    if (!this.connection) this.connection = new TreecrdtSqlConnection(this.executor);
    return this.connection;
  }

  async beginTransaction(): Promise<void> {
    throw new Error('TreeCRDT Kysely dialect does not support transactions');
  }

  async commitTransaction(): Promise<void> {
    throw new Error('TreeCRDT Kysely dialect does not support transactions');
  }

  async rollbackTransaction(): Promise<void> {
    throw new Error('TreeCRDT Kysely dialect does not support transactions');
  }

  async releaseConnection(): Promise<void> {}

  async destroy(): Promise<void> {
    this.connection = null;
  }
}

/**
 * Creates a Kysely instance over a TreeCRDT SQL executor.
 * Compiles with the SQLite dialect; execution goes through the executor only.
 */
export function createTreecrdtDb<DB extends TreecrdtDb = TreecrdtDb>(
  executor: TreecrdtSqlExecutor,
): Kysely<DB> {
  return new Kysely<DB>({
    dialect: {
      createAdapter: () => new SqliteAdapter(),
      createDriver: () => new TreecrdtSqlDriver(executor),
      createIntrospector: (db) => new SqliteIntrospector(db),
      createQueryCompiler: () => new SqliteQueryCompiler(),
    },
  });
}

/** Builds a Kysely instance from a SqliteRunner that implements `query`. */
export function createTreecrdtDbFromRunner<DB extends TreecrdtDb = TreecrdtDb>(runner: {
  query: (
    sql: string,
    params?: SqlParam[],
  ) => Promise<Record<string, SqlValue>[]> | Record<string, SqlValue>[];
}): Kysely<DB> {
  return createTreecrdtDb<DB>({
    query: async (sql, params) => Promise.resolve(runner.query(sql, params)),
  });
}

import type { SqliteRunner } from '@treecrdt/interface/sqlite';

type WaSqliteModule = {
  cwrap?: (
    name: string,
    returnType: string,
    argTypes: string[],
    opts?: { async?: boolean },
  ) => (...args: unknown[]) => Promise<number> | number;
};

const initCache = new WeakMap<object, (handle: number) => Promise<number> | number>();
/** Initialize the statically linked TreeCRDT extension on an open wa-sqlite handle. */
export async function initializeTreecrdtExtension(
  module: WaSqliteModule,
  handle: number,
  db: SqliteRunner,
): Promise<void> {
  if (!module || typeof module.cwrap !== 'function') {
    throw new Error('wa-sqlite module does not expose cwrap');
  }

  let init = initCache.get(module as object);
  if (!init) {
    init = module.cwrap('treecrdt_sqlite_init', 'number', ['number'], { async: true }) as (
      handle: number,
    ) => Promise<number> | number;
    initCache.set(module as object, init);
  }

  const rc = await init(handle);
  if (rc !== 0) {
    throw new Error(`TreeCRDT SQLite extension init failed (rc=${rc})`);
  }

  const schema = await db.getText('SELECT treecrdt_schema()');
  if (!schema) throw new Error('TreeCRDT extension did not provide its schema');

  // Let wa-sqlite await each statement's VFS work on the already-open handle.
  // One transaction also prevents a failed initialization leaving a partial schema.
  await db.exec('BEGIN IMMEDIATE');
  try {
    await db.exec(schema);
    await db.exec('COMMIT');
  } catch (error) {
    try {
      await db.exec('ROLLBACK');
    } catch {
      // Preserve the initialization error; the caller owns closing the handle.
    }
    throw error;
  }
}

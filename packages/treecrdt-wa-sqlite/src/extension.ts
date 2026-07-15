type WaSqliteModule = {
  cwrap?: (
    name: string,
    returnType: string,
    argTypes: string[],
    opts?: { async?: boolean },
  ) => (...args: unknown[]) => Promise<number> | number;
  retryOps?: Promise<unknown>[];
  pendingOps?: Promise<unknown>[];
};

const initCache = new WeakMap<object, (handle: number) => Promise<number> | number>();
const SQLITE_OK = 0;
const SQLITE_ERROR = 1;

function pendingErrorCode(error: unknown): number {
  if (typeof error !== 'object' || error === null || !('code' in error)) return SQLITE_ERROR;
  const code = (error as { code?: unknown }).code;
  return typeof code === 'number' && code !== SQLITE_OK ? code : SQLITE_ERROR;
}

async function runWithWaSqliteRetries(run: () => Promise<number> | number, module: WaSqliteModule) {
  while (true) {
    if (module.retryOps?.length) {
      try {
        await Promise.all(module.retryOps);
      } finally {
        module.retryOps = [];
      }
    }

    const rc = await run();
    if (rc === SQLITE_OK || !module.retryOps?.length) {
      if (module.pendingOps?.length) {
        try {
          await Promise.all(module.pendingOps);
        } catch (error) {
          return pendingErrorCode(error);
        } finally {
          module.pendingOps = [];
        }
      }
      return rc;
    }

    // Unlike wa-sqlite's generic retry cap, this idempotent schema initializer can safely
    // continue on the same open handle while each failed attempt queues real VFS work.
  }
}

/** Initialize the statically linked TreeCRDT extension on an open wa-sqlite handle. */
export async function initializeTreecrdtExtension(
  module: WaSqliteModule,
  handle: number,
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

  const rc = await runWithWaSqliteRetries(() => init(handle), module);
  if (rc !== 0) {
    throw new Error(`TreeCRDT SQLite extension init failed (rc=${rc})`);
  }
}

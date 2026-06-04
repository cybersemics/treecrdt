type WaSqliteModule = {
  cwrap?: (
    name: string,
    returnType: string,
    argTypes: string[],
    opts?: { async?: boolean },
  ) => (...args: unknown[]) => Promise<number> | number;
  retryOps?: Promise<unknown>[];
};

const initCache = new WeakMap<object, (handle: number) => Promise<number> | number>();

async function runWithWaSqliteRetries(run: () => Promise<number> | number, module: WaSqliteModule) {
  let rc: number;
  do {
    if (module.retryOps?.length) {
      await Promise.all(module.retryOps);
      module.retryOps = [];
    }
    rc = await run();
  } while (rc !== 0 && module.retryOps?.length);
  return rc;
}

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

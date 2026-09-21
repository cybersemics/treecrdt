import { loadWaSqliteBrowser } from '../../dist/load-wa-sqlite.browser.js';
import { createDatabase } from '../../dist/db.js';
import { initializeTreecrdtExtension } from '../../dist/extension.js';
import { createOpfsVfs, type OpfsVfsKind } from '../../dist/opfs.js';

self.onmessage = async ({ data: kind }: MessageEvent<OpfsVfsKind>) => {
  try {
    const { sqlite3, module } = await loadWaSqliteBrowser({
      assetsDir: '/',
      build: kind === 'coop-sync' ? 'sync' : 'asyncify',
    });
    const vfs = await createOpfsVfs(module, { kind });
    sqlite3.vfs_register(vfs, true);
    const filename = '/deferred-schema.db';
    const handle = await sqlite3.open_v2(filename);
    const register = module.cwrap('treecrdt_sqlite_init', 'number', ['number'], { async: true });
    if ((await register(handle)) !== 0) throw new Error('Registration failed');
    const tablesBefore: unknown[][] = [];
    await sqlite3.exec(handle, 'SELECT treecrdt_version(), treecrdt_schema()');
    await sqlite3.exec(handle, 'SELECT name FROM sqlite_schema', (row: unknown[]) =>
      tablesBefore.push(row),
    );
    const db = createDatabase(sqlite3, handle);
    await initializeTreecrdtExtension(module, handle, db);
    await db.getText("SELECT treecrdt_set_doc_id('persisted-document')");
    await db.exec("INSERT INTO meta VALUES ('sentinel', 'preserved')");
    await db.close?.();

    const reopenedHandle = await sqlite3.open_v2(filename);
    const reopened = createDatabase(sqlite3, reopenedHandle);
    await initializeTreecrdtExtension(module, reopenedHandle, reopened);
    const docId = await reopened.getText('SELECT treecrdt_doc_id()');
    const sentinel = await reopened.getText("SELECT value FROM meta WHERE key = 'sentinel'");
    const rootCount = await reopened.getText('SELECT count(*) FROM tree_nodes');
    await reopened.close?.();
    await vfs.close?.();
    self.postMessage({ tablesBefore, docId, sentinel, rootCount });
  } catch (error) {
    self.postMessage({ error: error instanceof Error ? error.message : String(error) });
  }
};

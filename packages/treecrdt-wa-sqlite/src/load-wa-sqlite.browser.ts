export type LoadWaSqliteOptions = {
  /** Public URL prefix for wa-sqlite assets served by the app. */
  assetsDir?: string;
};

export type LoadWaSqliteResult = {
  sqlite3: any;
  module: any;
};

function defaultBrowserBaseUrl(): string {
  const env = (import.meta as unknown as { env?: { BASE_URL?: string } }).env;
  return env?.BASE_URL ?? '/';
}

export async function loadWaSqliteBrowser(
  opts: LoadWaSqliteOptions = {},
): Promise<LoadWaSqliteResult> {
  const baseUrl = opts.assetsDir ?? defaultBrowserBaseUrl();
  const normalized = baseUrl.endsWith('/') ? baseUrl : `${baseUrl}/`;
  const sqliteModule = await import(/* @vite-ignore */ `${normalized}wa-sqlite/wa-sqlite-async.mjs`);
  const sqliteApi = await import(/* @vite-ignore */ `${normalized}wa-sqlite/sqlite-api.js`);
  const module = await sqliteModule.default({
    locateFile: (file: string) =>
      file.endsWith('.wasm') ? `${normalized}wa-sqlite/wa-sqlite-async.wasm` : file,
  });
  return { sqlite3: sqliteApi.Factory(module), module };
}

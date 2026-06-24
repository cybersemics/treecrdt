# @treecrdt/sqlite-node

Native Node.js SQLite runtime for TreeCRDT.

This package wraps `better-sqlite3` and loads the TreeCRDT SQLite extension for direct Node
process usage. Use `@treecrdt/wa-sqlite` when you need a browser or WASM-only runtime.

```ts
import { createTreecrdtClient } from '@treecrdt/sqlite-node';

const client = await createTreecrdtClient({
  runtime: { type: 'direct' },
  storage: { type: 'memory' },
  docId: 'scratch-doc',
});

await client.close();
```

File-backed storage uses the same direct native runtime:

```ts
const client = await createTreecrdtClient({
  storage: { type: 'file', filename: './treecrdt.sqlite' },
  docId: 'app-doc',
});
```

The package expects a bundled native extension for the current `process.platform` and
`process.arch`. Pass `extensionPath` in `createTreecrdtClient` or `loadTreecrdtExtension` if you
need to provide a custom extension build.

# @treecrdt/sqlite-node

Native Node.js SQLite runtime for TreeCRDT.

This package wraps `better-sqlite3` and loads the TreeCRDT SQLite extension for direct Node
process usage. Use `@treecrdt/wa-sqlite` when you need a browser or WASM-only runtime.

```ts
import { createTreecrdtClient, loadTreecrdtExtension } from '@treecrdt/sqlite-node';

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
`process.arch`. Published packages support:

- `x86_64-unknown-linux-gnu`
- `aarch64-apple-darwin`
- `x86_64-apple-darwin`
- `x86_64-pc-windows-msvc`

Each artifact is smoke-tested with the oldest and newest supported `better-sqlite3` major before
it is published. Other targets can provide their own extension build:

```ts
const client = await createTreecrdtClient({
  extension: { extensionPath: '/path/to/treecrdt_sqlite_ext' },
});

// For an existing better-sqlite3 Database:
loadTreecrdtExtension(db, { extensionPath: '/path/to/treecrdt_sqlite_ext' });
```

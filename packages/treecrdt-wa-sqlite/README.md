# @treecrdt/wa-sqlite (scaffold)

Loader + thin helpers to use the TreeCRDT SQLite extension with wa-sqlite in browser/Node.

## Build the patched wa-sqlite (extension baked in)
```
pnpm --filter @treecrdt/wa-sqlite-vendor build
# builds wa-sqlite/dist (example apps copy these into public/wa-sqlite/)
```

## Use in the demo
The demo imports the local wa-sqlite build and uses the auto-registered TreeCRDT extension:
```ts
import * as SQLite from "wa-sqlite";
import sqliteWasm from "/wa-sqlite/wa-sqlite.wasm?url";
import { appendOp, opsSince } from "@treecrdt/wa-sqlite";

const module = await SQLite.Factory({ wasm: sqliteWasm });
const db = await module.open(":memory:");
```
See `src/index.ts` and `src/ui/App.tsx` for helpers and a simple insert+move demo.

## Playwright
`pnpm --filter @treecrdt/wa-sqlite test:e2e` runs Vite dev + Playwright and asserts the demo can append/fetch ops via the extension.

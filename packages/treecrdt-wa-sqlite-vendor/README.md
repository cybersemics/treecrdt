# @treecrdt/wa-sqlite-vendor

Workspace wrapper around upstream `wa-sqlite` used by TreeCRDT.

This builds upstream `wa-sqlite` with the TreeCRDT SQLite extension baked into the wasm
via Makefile overrides (no TreeCRDT paths hardcoded inside `wa-sqlite`).

- `pnpm --filter @treecrdt/wa-sqlite-vendor build` builds `dist/` (incremental).
- `pnpm --filter @treecrdt/wa-sqlite-vendor rebuild` does a clean rebuild.

The WASM entrypoint registers functions without database I/O. The adapter reads
`treecrdt_schema()` and executes the returned SQL in an awaited transaction, so
wa-sqlite owns OPFS retries and completion. The native `sqlite3_treecrdt_init`
entrypoint still creates the same schema when the extension loads.

# @treecrdt/wa-sqlite-vendor

Workspace wrapper around upstream `wa-sqlite` used by TreeCRDT.

To update an existing checkout's source URL, run `git submodule sync --recursive`
and `git submodule update --init --recursive` from the repository root.

This builds upstream `wa-sqlite` with the TreeCRDT SQLite extension baked into the wasm
via Makefile overrides (no TreeCRDT paths hardcoded inside `wa-sqlite`).

`CFILES_EXTRA` includes the C wrapper and compiled Rust archive; Make's `VPATH`
locates the wrapper. Both inputs participate in dependency tracking. The build
copies upstream's `wa-sqlite/dist/` outputs into this package's `dist/` directory.

- `pnpm --filter @treecrdt/wa-sqlite-vendor build` builds `dist/` (incremental).
- `pnpm --filter @treecrdt/wa-sqlite-vendor rebuild` does a clean rebuild.

Use `rebuild` after changing source/library lists or compiler options.

The WASM entrypoint registers functions without database I/O. The adapter reads
`treecrdt_schema()` and executes the returned SQL in an awaited transaction, so
wa-sqlite owns OPFS retries and completion. The native `sqlite3_treecrdt_init`
entrypoint still creates the same schema when the extension loads.

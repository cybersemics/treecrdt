# @treecrdt/wa-sqlite-vendor

Workspace wrapper around upstream `wa-sqlite` used by TreeCRDT.

This builds upstream `wa-sqlite` with the TreeCRDT SQLite extension baked into the wasm
via Makefile overrides (no TreeCRDT paths hardcoded inside `wa-sqlite`).

- `pnpm --filter @treecrdt/wa-sqlite-vendor build` builds `dist/` (incremental).
- `pnpm --filter @treecrdt/wa-sqlite-vendor rebuild` does a clean rebuild.

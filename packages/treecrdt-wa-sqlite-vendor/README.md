# @justthrowaway/wa-sqlite-vendor

Workspace wrapper around upstream `wa-sqlite` used by TreeCRDT.

This builds upstream `wa-sqlite` with the TreeCRDT SQLite extension baked into the wasm
via Makefile overrides (no TreeCRDT paths hardcoded inside `wa-sqlite`).

- `pnpm --filter @justthrowaway/wa-sqlite-vendor build` builds `dist/` (incremental).
- `pnpm --filter @justthrowaway/wa-sqlite-vendor rebuild` does a clean rebuild.

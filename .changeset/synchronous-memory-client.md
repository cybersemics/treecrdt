---
"@treecrdt/wasm": minor
"@treecrdt/interface": patch
"@treecrdt/sync-protocol": minor
"@treecrdt/sync-sqlite": patch
---

Add a synchronous memory client with current reads, local commands, and a full-document sync backend for exchanging exact operations.

Share the operation-store sync adapter between synchronous memory and persistent clients. SQLite retains its scoped-sync and pending-auth extensions.

Share local command signatures while retaining precise synchronous/asynchronous results and backend-specific write options.

WASM writes, including incoming batches, now roll back on failure without caller-managed transactions.

WASM adapter operation reads now return canonical `Operation` objects with byte fields instead of flat hex-encoded rows. Other adapters retain their existing output formats.

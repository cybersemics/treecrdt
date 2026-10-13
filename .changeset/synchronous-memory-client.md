---
"@treecrdt/wasm": minor
"@treecrdt/interface": patch
---

Add a synchronous memory client with current reads, local commands, and a full-document sync backend for exchanging exact operations.

WASM writes, including incoming batches, now roll back on failure without caller-managed transactions.

WASM adapter operation reads now return canonical `Operation` objects with byte fields instead of flat hex-encoded rows. Other adapters retain their existing output formats.

---
'@treecrdt/sync': minor
'@treecrdt/sync-protocol': patch
---

Add an outbound sync helper for queued local-op upload to one replaceable structural push target.
Flushes explicitly report drained, deferred, failed, or closed outcomes, and failed/offline work
stays queued for retry. Target cleanup is replacement-safe, teardown aborts and awaits active work,
and high-level websocket pushes now accept direct-push cancellation options.

Standard TreeCRDT operations are deduped by operation id by default; custom op shapes may provide
`opKey`. Direct pushes check cancellation before starting Hello, auth, and send side effects.

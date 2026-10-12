---
'@treecrdt/sync': minor
'@treecrdt/sync-protocol': patch
---

Add an outbound sync helper for queued local-op upload to one replaceable structural push target.
`push()` and `flush()` acknowledge a fixed queued prefix without waiting for later submissions.
Failed/offline attempts reject but retain work for retry. Target cleanup is replacement-safe, and
`whenIdle()` waits for owned execution without retrying, and teardown aborts and awaits active work.

Standard TreeCRDT operations are deduped by operation id by default; custom op shapes may provide
`opKey`. Protocol and high-level websocket pushes now wait for receiver application and accept
cancellation. Direct-push acknowledgements require matching updated peers; the protocol is draft.

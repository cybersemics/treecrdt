---
'@treecrdt/auth': patch
'@treecrdt/sync-protocol': patch
---

Fail closed on every outbound operation path: validate replacement capability snapshots before
publishing them, correlate concurrent Hello/Ack authorization barriers by exchange id, preflight
`all` filters before RIBLT disclosure, suppress stale subscription sends, and rescan after validated
capability changes.

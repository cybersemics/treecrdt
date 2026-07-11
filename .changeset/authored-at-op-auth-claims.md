---
'@treecrdt/auth': minor
'@treecrdt/sync-protocol': minor
'@treecrdt/sync-sqlite': minor
'@treecrdt/sync-postgres': minor
---

Add opt-in signed `authoredAtMs` claims to op auth and persist them through sync proof material
stores. Version 2 signatures also bind the operation's defensive-delete `knownState`; the auth
layer refuses legacy v1 delete/tombstone operations because v1 cannot prove whether a relay stripped
those causally meaningful bytes.

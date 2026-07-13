---
'@treecrdt/auth': minor
---

Bind canonical defensive-delete `knownState` to one operation signature format. Require it on
deletes, reject non-empty state on other operation kinds, and sign its explicit presence or absence
on every operation.

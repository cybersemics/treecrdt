---
'@treecrdt/interface': patch
---

Add `createAuthorizedLocalWriter`, the auth-checked local write loop now shared by SQLite clients and `@treecrdt/postgres-napi`: it authorizes the minted op with no transaction open, writes it only if it is still unchanged, and lets writes take turns. Postgres clients no longer hold a row lock while `authorizeLocalOps` runs, which deadlocked any other write to the doc from the same process.

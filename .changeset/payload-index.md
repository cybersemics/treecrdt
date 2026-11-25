---
'@treecrdt/interface': minor
---

Add `createTreecrdtPayloadIndex` (`@treecrdt/interface/payload-index`) for the SQLite-backed engines: it finds live nodes by a string field of their JSON payload through a SQLite expression index on `tree_payload`, which SQLite keeps in the same write as the payload, and reports the keys each materialization event changed, old and new, from TEMP triggers. It reads plaintext JSON payloads only, as SQLite's JSON functions read them, returns a member's payload as that JSON when asked, and reports the writes of its own connection. The opens of a name on a connection share one set of triggers and one log of keys, and opening a name with another path throws while an open of it is left.

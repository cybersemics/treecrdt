---
'@treecrdt/auth': minor
---

Bind canonical defensive-delete `knownState` to one operation signature format. Require it on
deletes, reject the field on other operation kinds, and sign its explicit presence or absence on
every operation. Bound state size and entry count, validate 32-byte replica IDs, and defer canonical
VersionVector decoding until after signature verification. This clean-slate revision invalidates
signatures from the earlier draft format and replaces the public `encodeTreecrdtOpSigInputV1`,
`signTreecrdtOpV1`, and `verifyTreecrdtOpV1` exports with their unsuffixed forms. Use strict RFC 8032
verification for identity signatures and reject unsafe JavaScript operation counters and Lamport
timestamps.

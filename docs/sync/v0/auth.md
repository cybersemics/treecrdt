# TreeCRDT Sync v0: Auth Extension (COSE + CWT) (Draft)

This document defines an optional auth extension for Sync v0, focused on:

- Integrity: prevent forged operations.
- Authorization: subtree-scoped write permissions (and a path to read gating).

It is intentionally **ACL-agnostic at the TreeCRDT core layer**: the CRDT operation
types and merge semantics do not change. Authorization is enforced by the sync layer
and by any server that chooses to validate inbound ops.

Status: draft. Backwards compatibility is not guaranteed.

## Threat model (baseline)

- The sync server is untrusted: it may reorder, drop, replay, and inject messages.
- Peers defend by verifying per-op signatures and per-op authorization proofs.
- This does **not** hide access patterns from the server. Payload confidentiality is
  a separate design (see confidentiality issue/design).

## Identity model

- Each replica uses a **doc-scoped Ed25519 keypair** to sign operations.
- In Sync v0, `replica_id` is the **32-byte Ed25519 public key** (verifying key) for that doc.
- Optionally, a stable cross-doc “global identity” key can exist, but should be exchanged
  end-to-end (not shown to the sync server) to avoid cross-doc correlation.

### Derived `key_id` (optional)

A short doc-scoped key identifier can be derived from the public key for local caches.

Reference derivation (v1):

- `key_id = blake3("treecrdt/keyid/v1" || ed25519_pubkey)[0..16]` (16 bytes)

This is **not transmitted per-op** in v0: verifiers derive it from `replica_id` when needed.

## Wire format: `OpAuth` in `OpsBatch`

Sync v0 already has a `Hello.capabilities` mechanism. This extension uses it to exchange
authorization tokens, and it adds optional per-op auth metadata aligned with each op.

### `Hello.capabilities`

Peers exchange capability tokens at session start:

- `Capability.name = "auth.capability"`
- `Capability.value = base64url(COSE_Sign1_bytes)`

### `OpsBatch.auth[]`

`OpsBatch` gains an optional `auth` field aligned with `ops` by index.

If present, `auth` MUST be either empty (no auth) or exactly the same length as `ops`.

### `OpAuth`

Each op may carry:

- `sig`: Ed25519 signature bytes (64 bytes)
- `proof_ref` (optional but RECOMMENDED): 16-byte reference to the capability token used
  to authorize this op

Rationale:

- `sig` provides integrity and non-repudiation for the op contents.
- `proof_ref` makes authorization deterministic in the presence of rotation/expiry: the
  verifier knows which token (permissions snapshot) the signer intended.

## Capability tokens: COSE_Sign1 + CWT

Authorization proofs are **COSE_Sign1** messages containing a **CWT** (RFC 8392) payload.

### `cnf` binding (proof of possession)

Tokens MUST be bound to the op signing key using the CWT `cnf` claim.

Reference encoding:

- `cnf` is a CBOR map with:
  - `pub`: raw Ed25519 public key bytes (32 bytes)
  - `kid` (optional): derived `key_id` bytes; if present it MUST match `pub`

### Private claim: `caps`

`caps` is a list of capability objects. The reference implementation treats each element as:

```
cap = {
  res: {
    doc_id: "...",
    // `root` is required; doc-wide access is represented by ROOT + no extra scope restrictions.
    root: <NodeId bytes>,        // required (ROOT means doc-wide)
    max_depth: <u32>,            // optional
    exclude: [<NodeId bytes>],   // optional
  },
  actions: ["write_structure", "write_payload", "delete", "tombstone", "grant", ...]
}
```

### Delegation / resharing (optional)

This extension supports delegated (“reshared”) capability tokens.

High-level idea:

- An **issuer** can mint a token for a subject key.
- If that token includes the `grant` action, the subject key can mint a **delegated token** for another subject key.
- The delegated token MUST be a subset of the proof token (same `doc_id`, narrower-or-equal scope, subset of actions, and time bounds within the proof).

The reference verifier establishes that subset without consulting mutable receiver ancestry. A document-wide proof may
delegate a narrower root. A non-document-wide proof must keep the same root, though it may still narrow actions and time
bounds, add `max_depth`, or add exclusions. Re-rooting a scoped proof requires an authenticated causal ancestry witness
that the current protocol does not carry. Runtime scope evaluators remain available for read filtering and subtree checks;
their current tree view is not delegation authority.

Wire format (reference implementation):

- Delegated tokens are still COSE_Sign1 + CWT, but they are signed by the **subject key** of a *proof token*.
- The delegated token carries exactly one proof token in the COSE unprotected header:
  - key: `"treecrdt.delegation_proof_v1"`
  - value: a single `bstr` (or a 1-element array of `bstr`) containing the proof token bytes
- Proof tokens can themselves be delegated (chained), but they must ultimately verify against an issuer key.
  Implementations SHOULD enforce a maximum chain depth and reject cycles.

### Token id (`proof_ref`)

`proof_ref` is a short hash of the COSE token bytes.

Reference derivation (v1):

- `token_id = blake3("treecrdt/tokenid/v1" || cose_sign1_bytes)[0..16]` (16 bytes)

Verifiers SHOULD cache `token_id -> parsed claims` for efficiency.

### Revocation

Implementations SHOULD support token-id-based revocation (denylist) in addition to `exp`/`nbf`.
Revoking a proof token must also invalidate delegated tokens that depend on that proof chain.

Recommended policy modes:

- `hard`: token is invalid for all checks (retroactive if re-verified).
- `write_cutover`: token remains valid for ops strictly before `effective_from`, and is rejected at/after `effective_from`.

`write_cutover` can be anchored by:

- `(replica_id, counter)` only (deterministic and clock-free).
- Lamport-based cutover is intentionally not part of v1.

For convergence across peers, revocations SHOULD be exchanged as signed records (not only local denylist state).

Record shape (reference v1):

- capability: `name="auth.revocation"`, `value=base64url(COSE_Sign1(CWT))`
- claims:
  - `doc_id`
  - `token_id`
  - `mode` (`hard` or `write_cutover`)
  - `effective_from_counter` + optional `effective_from_replica`
  - `iat`
  - monotonic `rev_seq` (per issuer/doc), to avoid ambiguity on conflicting records

Deterministic verifier rule:

- verify record signature against issuer trust roots
- pick highest `rev_seq` for `(doc_id, token_id)`
- enforce by mode:
  - `hard`: reject always
  - `write_cutover`: reject only ops at/after cutover boundary

Reference implementation status:

- supports hard revocation now (`revokedCapabilityTokenIds`)
- supports custom runtime cutover logic via `isCapabilityTokenRevoked` callback with op context (`op`, `purpose`)
- supports `auth.revocation` wire records in `Hello.capabilities` / `HelloAck.capabilities`
- merges by highest `rev_seq` per `(doc_id, token_id)` (deterministic tie-break by lexical record bytes)

## Signed operations

Ops are signed with the doc-scoped Ed25519 key. The signature covers:

- `doc_id`
- `op_id` (`replica_id` + `counter`)
- `lamport`
- explicit defensive-delete `known_state` presence and bytes
- op kind + fields
- payload bytes (ciphertext bytes if payloads are encrypted)

### Canonical signing bytes

All integers are big-endian. Strings are UTF-8 with length prefixes.

```
sig_input = concat(
  "treecrdt/op-sig/v1", 0x00,
  u32_be(len(doc_id)), doc_id_utf8,
  u32_be(len(replica_id)), replica_id_bytes,
  u64_be(counter),
  u64_be(lamport),
  u8(kind_tag),
  kind_fields,
  known_state
)
```

Kind tags and fields:

- Insert: tag=1
  - parent(16) || node(16) || u32_be(len(order_key)) || order_key_bytes
  - payload_flag(u8): 0 or 1
  - if payload_flag=1: u32_be(len(payload)) || payload_bytes
- Move: tag=2
  - node(16) || new_parent(16) || u32_be(len(order_key)) || order_key_bytes
- Delete: tag=3
  - node(16)
- Tombstone: tag=4
  - node(16)
- Payload: tag=5
  - node(16)
  - value_tag(u8): 0=clear, 1=payload
  - if value_tag=1: u32_be(len(payload)) || payload_bytes

Every operation uses this one signature format. `known_state` is encoded after the operation fields:

```
known_state = absent:  u8(0)
              present: u8(1) || u32_be(len(bytes)) || bytes
```

The present form must contain the canonical UTF-8 JSON version-vector encoding used by the
auth-enabled persistent backends: `{"entries":[...]}` without whitespace, with entries sorted
lexicographically by replica bytes and each entry encoded as
`{"replica":[...],"frontier":n,"ranges":[[start,end],...]}`. Signers and verifiers reject other
spellings so persistence cannot change signed bytes.

The v0 cross-language counter limit is `Number.MAX_SAFE_INTEGER` (`9007199254740991`). Frontiers
and range bounds MUST be integers at or below that limit; range bounds MUST be positive. Ranges
MUST be normalized exactly as the Rust version vector stores them: each `start <= end`, starts are
strictly ordered, ranges neither overlap nor touch, and every start is greater than `frontier + 1`.

Policy APIs require non-empty `known_state` on deletes and reject non-empty state on every other
operation; tombstones use the explicit absent-state form. Because every operation signs the
presence tag, a relay cannot strip delete state to bypass this policy. Applications use
`signTreecrdtOp` and `verifyTreecrdtOp`, which enforce these invariants.

## Subtree reads and operation writes

### Reference operation-log reads require document-wide grants

The reference COSE+CWT implementation authorizes `Filter.all` and `Filter.children(parent)` only with a document-wide
`read_structure` grant: ROOT with no `max_depth` or `exclude`. The filter can still reduce synchronization work, but it
does not reduce the authorization scope.

Sync v0 filters reconcile historical operations, while a subtree evaluator sees only current materialized ancestry. If a
node moves into an excluded subtree and later re-enters readable state, a current-state check would expose the historical
move's excluded destination and payload updates authored during the private interval. The same ambiguity applies to
boundary moves, defensive restoration, and superseded payload operations. Per-operation allow/deny flags cannot safely
repair the projection: omitting a selected dependency changes the reconciled op set and can leave stale state.

Until the protocol carries authenticated historical ancestry or redacted snapshot/removal records, the reference auth
layer therefore rejects the entire operation-log projection for non-document-wide read scopes. It does not consult the
stateful scope evaluator for this decision. Applications can still provide a custom `SyncAuth` implementation when their
backend exposes a different, authenticated projection that is safe for scoped reads.

Payload operations (including clears) and inserts with an inline payload additionally require a document-wide
`read_payload` grant. Sync v0 cannot redact their payload state while preserving the selected operation, so a
structure-only projection fails as a whole when it encounters one. Structure-only batches retain the direct fast path,
as do batches whose peer has both document-wide read grants.

### Operation writes require a document-wide grant

The reference implementation does **not** authorize insert, move, payload, delete, or tombstone operations from a
non-document-wide ancestry scope. This includes a non-ROOT `root`, any `max_depth`, and any `exclude` restriction.

Current ancestry is not a stable authorization witness: one peer can accept an operation while a node appears inside the
scope, then receive an earlier move and replay the same operation with that node outside the scope. A peer that received
the move first would reject the operation, so receiver-local ancestry can create both an authorization bypass and
permanent op-set divergence. Insert/move destination checks and defensive delete subtree effects have the same problem.

Until an operation carries an authenticated causal ancestry witness that every peer can verify, only a ROOT scope with no
`max_depth` or `exclude` is state-independent enough to authorize writes. Non-document-wide write grants return `deny`
immediately; they are not placed in `pending_context`.

`pending_context` remains a protocol mechanism for custom auth schemes that have a stable, verifiable way to resolve
missing context. It is not used to make mutable receiver ancestry authoritative for reference operation writes.

## Sidecar storage (same SQLite database)

To avoid changing core op-log storage, implementations SHOULD store auth data in sidecar tables in the same SQLite
database as the CRDT state (not separate files).

Two useful sidecar concepts:

1) **Verified proof cache** (optional): re-verify later without re-downloading proofs.
2) **Pending ops** (recommended): store ops that are validly signed and well-formed but not yet authorizable.
3) **Op auth cache** (recommended for forwarders): persist per-op auth (`sig` + `proof_ref`) for already-applied ops so a peer/server can re-serve ops it did not author after restart.

### Suggested SQLite schema (illustrative)

Implementations may choose different schemas; the important properties are:

- key by `op_ref` (or by `(replica_id, counter)`)
- store the per-op `OpAuth`
- store a reason/message for debugging

Example:

```sql
CREATE TABLE IF NOT EXISTS treecrdt_sync_pending_ops (
  doc_id TEXT NOT NULL,
  op_ref BLOB NOT NULL,              -- 16 bytes
  op BLOB NOT NULL,                  -- encoded sync/v0 Operation protobuf bytes (not a full SyncMessage)
  sig BLOB NOT NULL,
  proof_ref BLOB,
  reason TEXT NOT NULL,              -- e.g. "missing_context"
  message TEXT,
  created_at_ms INTEGER NOT NULL,
  PRIMARY KEY (doc_id, op_ref)
);
```

If a peer/server forwards ops (or needs to re-serve verified ops after restart), it SHOULD also store the `OpAuth`
metadata for already-applied ops:

```sql
CREATE TABLE IF NOT EXISTS treecrdt_sync_op_auth (
  doc_id TEXT NOT NULL,
  op_ref BLOB NOT NULL,              -- 16 bytes
  sig BLOB NOT NULL,                 -- 64 bytes (Ed25519)
  proof_ref BLOB,                    -- 16 bytes (token id), nullable
  created_at_ms INTEGER NOT NULL,
  PRIMARY KEY (doc_id, op_ref)
);
```

### SQLite runner compatibility notes

TreeCRDT’s JS SQLite adapters intentionally use a minimal `SqliteRunner` API (`exec` + `getText`). This has two practical
implications for sidecar tables:

1) **DML via `getText`**: some runners implement `getText` using a “query” primitive (for example `better-sqlite3`’s
   `.get()`), which fails for statements that return no rows. A portable pattern is to use `RETURNING` so the statement
   yields a row:

   - `INSERT ... RETURNING 1`
   - `DELETE ... RETURNING 1`

2) **BLOB reads via text**: if the bridge only returns strings, read `BLOB` columns with `hex(blob_col)` and decode on
   the JS side. For bulk reads, SQLite’s JSON1 functions are convenient (`json_object`, `json_group_array`).

## Subscription note: push vs catch-up

Sync v0 push subscriptions (`Subscribe`) are meant for **new** ops. They do not guarantee delivery of historical ops.

Implementations SHOULD run an initial catch-up (`syncOnce`) and/or periodic catch-up to avoid missing updates due to
transient failures.

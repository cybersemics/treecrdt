use super::materialize::{json_outcome_from_core, JsonMaterializationOutcome};
use super::node_store::SqliteNodeStore;
use super::op_index::SqliteParentOpIndex;
use super::op_storage::SqliteOpStorage;
use super::payload_store::SqlitePayloadStore;
use super::util::{
    read_blob, read_blob16, read_optional_blob16, read_required_blob, read_text,
    sqlite_err_from_core, sqlite_result_json,
};
use super::*;
use treecrdt_core::{
    LamportClock, LocalFinalizePlan, LocalPlacement, MaterializationCursor, Operation,
    OperationKind, PreparedLocalOp, ReplicaId, TreeCrdt,
};

#[derive(serde::Serialize)]
struct JsonOp {
    replica: Vec<u8>,
    counter: u64,
    lamport: Lamport,
    kind: String,
    parent: Option<[u8; 16]>,
    node: [u8; 16],
    new_parent: Option<[u8; 16]>,
    order_key: Option<Vec<u8>>,
    known_state: Option<Vec<u8>>,
    payload: Option<Vec<u8>>,
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct JsonLocalOpResult {
    op: JsonOp,
    outcome: JsonMaterializationOutcome,
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct JsonPreparedLocalOpResult {
    op: JsonOp,
    precondition: String,
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct JsonLocalOpConflict {
    conflict: bool,
}

#[derive(serde::Serialize)]
struct JsonLocalOpTransactionError {
    error: &'static str,
}

enum LocalOpMode {
    Immediate,
    Prepare,
    Commit {
        precondition: String,
        proof: LocalOpProof,
    },
}

struct LocalOpProof {
    sig: Vec<u8>,
    proof_ref: Vec<u8>,
}

enum LocalOpDispatchResult {
    Committed(JsonLocalOpResult),
    Prepared(JsonPreparedLocalOpResult),
    Conflict,
    OuterTransaction,
}

enum OptimisticBeginError {
    Conflict,
    Sql(c_int),
}

type LocalCrdt = TreeCrdt<SqliteOpStorage, LamportClock, SqliteNodeStore, SqlitePayloadStore>;

struct LocalOpSession {
    db: *mut sqlite3,
    doc_id: Vec<u8>,
    crdt: LocalCrdt,
    commit_sql: CString,
    rollback_sql: CString,
}

impl LocalOpSession {
    fn rollback(self, rc: c_int) -> c_int {
        let rollback_rc = sqlite_exec(
            self.db,
            self.rollback_sql.as_ptr(),
            None,
            null_mut(),
            null_mut(),
        );
        if rollback_rc == SQLITE_OK as c_int {
            rc
        } else {
            rollback_rc
        }
    }
}

fn optimistic_conflict(session: LocalOpSession) -> Result<LocalOpDispatchResult, c_int> {
    match session.rollback(SQLITE_OK as c_int) {
        rc if rc == SQLITE_OK as c_int => Ok(LocalOpDispatchResult::Conflict),
        rc => Err(rc),
    }
}

fn parse_local_op_mode(
    argc: c_int,
    args: &[*mut sqlite3_value],
    base_argc: usize,
) -> Result<LocalOpMode, ()> {
    match argc as usize {
        n if n == base_argc => return Ok(LocalOpMode::Immediate),
        n if n == base_argc + 1 => {
            return (read_text(args[base_argc]) == "prepare")
                .then_some(LocalOpMode::Prepare)
                .ok_or(());
        }
        n if n == base_argc + 3 => {}
        _ => return Err(()),
    }

    let precondition = read_text(args[base_argc]);
    if !precondition.starts_with("v1:") {
        return Err(());
    }
    let sig = read_required_blob(args[base_argc + 1])?;
    if sig.len() != 64 {
        return Err(());
    }
    let proof_ref = read_required_blob(args[base_argc + 2])?;
    if proof_ref.len() != 16 {
        return Err(());
    }
    Ok(LocalOpMode::Commit {
        precondition,
        proof: LocalOpProof { sig, proof_ref },
    })
}

fn valid_local_op_argc(argc: c_int, base_argc: usize) -> bool {
    matches!(argc as usize, n if n == base_argc || n == base_argc + 1 || n == base_argc + 3)
}

fn sqlite_busy_or_locked(rc: c_int) -> bool {
    // Keep this independent of optional sqlite bindings: these are stable SQLite result codes.
    matches!(rc & 0xff, 5 | 6)
}

fn rollback_optimistic_begin(
    db: *mut sqlite3,
    rollback: &CString,
    rc: c_int,
) -> OptimisticBeginError {
    let rollback_rc = sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
    if rollback_rc != SQLITE_OK as c_int {
        OptimisticBeginError::Sql(rollback_rc)
    } else if sqlite_busy_or_locked(rc) {
        OptimisticBeginError::Conflict
    } else {
        OptimisticBeginError::Sql(rc)
    }
}

fn json_op_from_operation(op: Operation) -> Result<JsonOp, c_int> {
    let replica = op.meta.id.replica.as_bytes().to_vec();
    let counter = op.meta.id.counter;
    let lamport = op.meta.lamport;
    let known_state = op
        .meta
        .known_state
        .as_ref()
        .map(|vv| serde_json::to_vec(vv).map_err(|_| SQLITE_ERROR as c_int))
        .transpose()?;

    match op.kind {
        OperationKind::Insert {
            parent,
            node,
            order_key,
            payload,
        } => Ok(JsonOp {
            replica,
            counter,
            lamport,
            kind: "insert".to_string(),
            parent: Some(parent.0.to_be_bytes()),
            node: node.0.to_be_bytes(),
            new_parent: None,
            order_key: Some(order_key),
            known_state: None,
            payload,
        }),
        OperationKind::Move {
            node,
            new_parent,
            order_key,
        } => Ok(JsonOp {
            replica,
            counter,
            lamport,
            kind: "move".to_string(),
            parent: None,
            node: node.0.to_be_bytes(),
            new_parent: Some(new_parent.0.to_be_bytes()),
            order_key: Some(order_key),
            known_state: None,
            payload: None,
        }),
        OperationKind::Delete { node } => Ok(JsonOp {
            replica,
            counter,
            lamport,
            kind: "delete".to_string(),
            parent: None,
            node: node.0.to_be_bytes(),
            new_parent: None,
            order_key: None,
            known_state,
            payload: None,
        }),
        OperationKind::Tombstone { node } => Ok(JsonOp {
            replica,
            counter,
            lamport,
            kind: "tombstone".to_string(),
            parent: None,
            node: node.0.to_be_bytes(),
            new_parent: None,
            order_key: None,
            known_state,
            payload: None,
        }),
        OperationKind::Payload { node, payload } => Ok(JsonOp {
            replica,
            counter,
            lamport,
            kind: "payload".to_string(),
            parent: None,
            node: node.0.to_be_bytes(),
            new_parent: None,
            order_key: None,
            known_state: None,
            payload,
        }),
    }
}

fn load_local_crdt(db: *mut sqlite3, doc_id: &[u8], replica: &[u8]) -> Result<LocalCrdt, c_int> {
    let node_store = SqliteNodeStore::prepare(db).map_err(|_| SQLITE_ERROR as c_int)?;
    let payload_store = SqlitePayloadStore::prepare(db).map_err(|_| SQLITE_ERROR as c_int)?;
    let storage = SqliteOpStorage::with_doc_id(db, doc_id.to_vec());
    TreeCrdt::with_stores(
        ReplicaId::new(replica.to_vec()),
        storage,
        LamportClock::default(),
        node_store,
        payload_store,
    )
    .map_err(sqlite_err_from_core)
}

fn load_required_doc_id(db: *mut sqlite3) -> Result<Vec<u8>, c_int> {
    load_doc_id(db)?.ok_or(SQLITE_ERROR as c_int)
}

/// Hash the complete clean materialization revision together with the proposed operation.
///
/// The operation binding is important: two concurrent prepares can mint the same v0 op id while
/// proposing different bodies. A revision-only token would let one authorization commit the
/// other proposal. Requiring a clean replay frontier ensures a pending repair never looks stable.
fn local_precondition(
    doc_id: &[u8],
    meta: &TreeMeta,
    op: &Operation,
) -> Result<Option<String>, c_int> {
    let state = meta.state();
    if state.replay_from.is_some() {
        return Ok(None);
    }

    let mut hasher = blake3::Hasher::new();
    hasher.update(b"treecrdt/sqlite-local-precondition/v1");
    hasher.update(&(doc_id.len() as u64).to_be_bytes());
    hasher.update(doc_id);
    match state.head {
        Some(head) => {
            hasher.update(&[1]);
            hasher.update(&head.at.lamport.to_be_bytes());
            hasher.update(&(head.at.replica.len() as u64).to_be_bytes());
            hasher.update(head.at.replica);
            hasher.update(&head.at.counter.to_be_bytes());
            hasher.update(&head.seq.to_be_bytes());
        }
        None => {
            hasher.update(&[0]);
        }
    };
    let op_bytes = serde_json::to_vec(op).map_err(|_| SQLITE_ERROR as c_int)?;
    hasher.update(&(op_bytes.len() as u64).to_be_bytes());
    hasher.update(&op_bytes);
    Ok(Some(format!("v1:{}", hasher.finalize().to_hex())))
}

fn persist_local_op_proof(
    db: *mut sqlite3,
    doc_id: &[u8],
    op: &Operation,
    proof: &LocalOpProof,
) -> Result<(), c_int> {
    let sql = CString::new(
        "INSERT OR REPLACE INTO treecrdt_sync_op_auth \
         (doc_id, op_ref, sig, proof_ref, created_at_ms) \
         VALUES (?1, ?2, ?3, ?4, CAST(strftime('%s','now') AS INTEGER) * 1000)",
    )
    .expect("local op proof sql");
    let mut stmt: *mut sqlite3_stmt = null_mut();
    let rc = sqlite_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, null_mut());
    if rc != SQLITE_OK as c_int {
        return Err(rc);
    }

    let op_ref = derive_op_ref_v0(doc_id, op.meta.id.replica.as_bytes(), op.meta.id.counter);
    let mut bind_err = false;
    unsafe {
        bind_err |= sqlite_bind_text(
            stmt,
            1,
            doc_id.as_ptr() as *const c_char,
            doc_id.len() as c_int,
            None,
        ) != SQLITE_OK as c_int;
        bind_err |= sqlite_bind_blob(
            stmt,
            2,
            op_ref.as_ptr() as *const c_void,
            op_ref.len() as c_int,
            None,
        ) != SQLITE_OK as c_int;
        bind_err |= sqlite_bind_blob(
            stmt,
            3,
            proof.sig.as_ptr() as *const c_void,
            proof.sig.len() as c_int,
            None,
        ) != SQLITE_OK as c_int;
        bind_err |= sqlite_bind_blob(
            stmt,
            4,
            proof.proof_ref.as_ptr() as *const c_void,
            proof.proof_ref.len() as c_int,
            None,
        ) != SQLITE_OK as c_int;
    }
    if bind_err {
        unsafe { sqlite_finalize(stmt) };
        return Err(SQLITE_ERROR as c_int);
    }

    let step_rc = unsafe { sqlite_step(stmt) };
    let finalize_rc = unsafe { sqlite_finalize(stmt) };
    if step_rc != SQLITE_DONE as c_int {
        return Err(step_rc);
    }
    if finalize_rc != SQLITE_OK as c_int {
        return Err(finalize_rc);
    }
    Ok(())
}

fn begin_local_core_op(
    db: *mut sqlite3,
    doc_id: &[u8],
    replica: &[u8],
    savepoint_name: &str,
) -> Result<LocalOpSession, c_int> {
    let begin = CString::new(format!("SAVEPOINT {savepoint_name}")).expect("savepoint begin");
    let commit_sql = CString::new(format!("RELEASE {savepoint_name}")).expect("savepoint commit");
    let rollback = CString::new(format!(
        "ROLLBACK TO {savepoint_name}; RELEASE {savepoint_name}"
    ))
    .expect("savepoint rollback");
    if sqlite_exec(db, begin.as_ptr(), None, null_mut(), null_mut()) != SQLITE_OK as c_int {
        return Err(SQLITE_ERROR as c_int);
    }

    if let Err(rc) = ensure_materialized(db) {
        sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
        return Err(rc);
    }

    let crdt = match load_local_crdt(db, doc_id, replica) {
        Ok(v) => v,
        Err(rc) => {
            sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
            return Err(rc);
        }
    };
    Ok(LocalOpSession {
        db,
        doc_id: doc_id.to_vec(),
        crdt,
        commit_sql,
        rollback_sql: rollback,
    })
}

fn begin_optimistic_local_core_op(
    db: *mut sqlite3,
    replica: &[u8],
    savepoint_name: &str,
) -> Result<LocalOpSession, OptimisticBeginError> {
    let begin = CString::new(format!("SAVEPOINT {savepoint_name}")).expect("savepoint begin");
    let commit_sql = CString::new(format!("RELEASE {savepoint_name}")).expect("savepoint commit");
    let rollback = CString::new(format!(
        "ROLLBACK TO {savepoint_name}; RELEASE {savepoint_name}"
    ))
    .expect("savepoint rollback");
    let begin_rc = sqlite_exec(db, begin.as_ptr(), None, null_mut(), null_mut());
    if begin_rc != SQLITE_OK as c_int {
        return Err(OptimisticBeginError::Sql(begin_rc));
    }

    // A deferred SAVEPOINT does not serialize this read with writers on other connections.
    // Acquire the write lock before checking the token so no writer can invalidate it between
    // validation and persistence. The assignment is intentionally value-preserving.
    let lock_sql = CString::new("UPDATE tree_meta SET head_seq = head_seq WHERE id = 1")
        .expect("local optimistic write lock sql");
    let lock_rc = sqlite_exec(db, lock_sql.as_ptr(), None, null_mut(), null_mut());
    if lock_rc != SQLITE_OK as c_int {
        return Err(rollback_optimistic_begin(db, &rollback, lock_rc));
    }

    let doc_id = match load_doc_id(db) {
        Ok(Some(v)) => v,
        Ok(None) => {
            return Err(rollback_optimistic_begin(
                db,
                &rollback,
                SQLITE_ERROR as c_int,
            ));
        }
        Err(rc) => {
            return Err(rollback_optimistic_begin(db, &rollback, rc));
        }
    };
    let crdt = match load_local_crdt(db, &doc_id, replica) {
        Ok(v) => v,
        Err(rc) => {
            return Err(rollback_optimistic_begin(db, &rollback, rc));
        }
    };
    Ok(LocalOpSession {
        db,
        doc_id,
        crdt,
        commit_sql,
        rollback_sql: rollback,
    })
}

fn finish_local_core_op(
    mut session: LocalOpSession,
    op: Operation,
    plan: LocalFinalizePlan,
) -> Result<JsonLocalOpResult, c_int> {
    let mut post_materialization_ok = true;
    let mut head_seq = 0u64;
    let mut outcome = treecrdt_core::MaterializationOutcome::empty(0);
    match load_tree_meta(session.db) {
        Ok(meta) => head_seq = meta.state().head_seq(),
        Err(_) => post_materialization_ok = false,
    }
    if post_materialization_ok {
        let finalize_rc = match SqliteParentOpIndex::prepare(session.db, session.doc_id.clone()) {
            Ok(mut op_index) => session
                .crdt
                .finalize_local_with_outcome(&op, &mut op_index, head_seq, &plan)
                .map_err(|_| SQLITE_ERROR as c_int),
            Err(_) => Err(SQLITE_ERROR as c_int),
        };
        match finalize_rc {
            Ok(next) => outcome = next,
            Err(_) => post_materialization_ok = false,
        }
    }
    let head = treecrdt_core::MaterializationHead {
        at: treecrdt_core::MaterializationKey {
            lamport: op.meta.lamport,
            replica: op.meta.id.replica.as_bytes(),
            counter: op.meta.id.counter,
        },
        seq: outcome.head_seq,
    };
    if post_materialization_ok && update_tree_meta_head(session.db, Some(&head)).is_err() {
        post_materialization_ok = false;
    }
    if !post_materialization_ok {
        if let Err(rc) = set_tree_meta_replay_frontier(
            session.db,
            &treecrdt_core::MaterializationFrontier {
                lamport: 0,
                replica: Vec::new(),
                counter: 0,
            },
        ) {
            return Err(session.rollback(rc));
        }
    }

    let op = match json_op_from_operation(op) {
        Ok(v) => v,
        Err(rc) => return Err(session.rollback(rc)),
    };

    let commit_rc = sqlite_exec(
        session.db,
        session.commit_sql.as_ptr(),
        None,
        null_mut(),
        null_mut(),
    );
    if commit_rc != SQLITE_OK as c_int {
        return Err(session.rollback(commit_rc));
    }

    Ok(JsonLocalOpResult {
        op,
        outcome: json_outcome_from_core(&outcome),
    })
}

fn run_local_core_op<F>(
    db: *mut sqlite3,
    replica: Vec<u8>,
    savepoint_name: &str,
    mode: LocalOpMode,
    build: F,
) -> Result<LocalOpDispatchResult, c_int>
where
    F: Fn(&mut LocalCrdt) -> treecrdt_core::Result<PreparedLocalOp>,
{
    if !matches!(&mode, LocalOpMode::Immediate) && sqlite_get_autocommit(db) == 0 {
        return Ok(LocalOpDispatchResult::OuterTransaction);
    }
    match mode {
        LocalOpMode::Prepare => {
            // Loading meta first pins the read snapshot used by the rest of this SELECT. Preparing
            // only reads; a pending replay is surfaced as a retry instead of repairing state here.
            let meta = load_tree_meta(db)?;
            if meta.state().replay_from.is_some() {
                return Ok(LocalOpDispatchResult::Conflict);
            }
            let doc_id = load_required_doc_id(db)?;
            let mut crdt = load_local_crdt(db, &doc_id, &replica)?;
            let prepared = build(&mut crdt).map_err(sqlite_err_from_core)?;
            let Some(precondition) = local_precondition(&doc_id, &meta, &prepared.op)? else {
                return Ok(LocalOpDispatchResult::Conflict);
            };
            let op = json_op_from_operation(prepared.op)?;
            Ok(LocalOpDispatchResult::Prepared(JsonPreparedLocalOpResult {
                op,
                precondition,
            }))
        }
        LocalOpMode::Immediate => {
            let doc_id = load_required_doc_id(db)?;
            let mut session = begin_local_core_op(db, &doc_id, &replica, savepoint_name)?;
            let prepared = match build(&mut session.crdt) {
                Ok(v) => v,
                Err(err) => return Err(session.rollback(sqlite_err_from_core(err))),
            };
            let (op, plan) = match session.crdt.commit_prepared_local(prepared) {
                Ok(v) => v,
                Err(err) => return Err(session.rollback(sqlite_err_from_core(err))),
            };
            finish_local_core_op(session, op, plan).map(LocalOpDispatchResult::Committed)
        }
        LocalOpMode::Commit {
            precondition: expected_precondition,
            proof,
        } => {
            let mut session = match begin_optimistic_local_core_op(db, &replica, savepoint_name) {
                Ok(v) => v,
                Err(OptimisticBeginError::Conflict) => return Ok(LocalOpDispatchResult::Conflict),
                Err(OptimisticBeginError::Sql(rc)) => return Err(rc),
            };
            let meta = match load_tree_meta(session.db) {
                Ok(v) => v,
                Err(rc) => return Err(session.rollback(rc)),
            };
            if meta.state().replay_from.is_some() {
                return optimistic_conflict(session);
            }
            let prepared = match build(&mut session.crdt) {
                Ok(v) => v,
                Err(err) => return Err(session.rollback(sqlite_err_from_core(err))),
            };
            let actual_precondition = match local_precondition(&session.doc_id, &meta, &prepared.op)
            {
                Ok(Some(v)) => v,
                Ok(None) => {
                    return optimistic_conflict(session);
                }
                Err(rc) => return Err(session.rollback(rc)),
            };
            if actual_precondition != expected_precondition {
                return optimistic_conflict(session);
            }
            let (op, plan) = match session.crdt.commit_prepared_local(prepared) {
                Ok(v) => v,
                Err(err) => return Err(session.rollback(sqlite_err_from_core(err))),
            };
            if let Err(rc) = persist_local_op_proof(session.db, &session.doc_id, &op, &proof) {
                return Err(session.rollback(rc));
            }
            finish_local_core_op(session, op, plan).map(LocalOpDispatchResult::Committed)
        }
    }
}

fn sqlite_result_local_dispatch(ctx: *mut sqlite3_context, out: &LocalOpDispatchResult) {
    match out {
        LocalOpDispatchResult::Committed(value) => sqlite_result_json(ctx, value),
        LocalOpDispatchResult::Prepared(value) => sqlite_result_json(ctx, value),
        LocalOpDispatchResult::Conflict => {
            sqlite_result_json(ctx, &JsonLocalOpConflict { conflict: true })
        }
        LocalOpDispatchResult::OuterTransaction => sqlite_result_json(
            ctx,
            &JsonLocalOpTransactionError {
                error: "outerTransaction",
            },
        ),
    }
}

pub(super) unsafe extern "C" fn treecrdt_local_insert(
    ctx: *mut sqlite3_context,
    argc: c_int,
    argv: *mut *mut sqlite3_value,
) {
    if let Err(rc) = ensure_api_initialized() {
        sqlite_result_error_code(ctx, rc);
        return;
    }

    if !valid_local_op_argc(argc, 6) {
        sqlite_result_error(
            ctx,
            b"treecrdt_local_insert expects 6 args, 7 for prepare, or 9 for commit\0".as_ptr()
                as *const c_char,
        );
        return;
    }

    let args = unsafe { slice::from_raw_parts(argv, argc as usize) };
    let mode = match parse_local_op_mode(argc, args, 6) {
        Ok(v) => v,
        Err(_) => {
            sqlite_result_error(
                ctx,
                b"treecrdt_local_insert: invalid prepare or commit arguments\0".as_ptr()
                    as *const c_char,
            );
            return;
        }
    };
    let replica = match read_required_blob(args[0]) {
        Ok(v) => v,
        Err(_) => {
            sqlite_result_error(
                ctx,
                b"treecrdt_local_insert: NULL replica\0".as_ptr() as *const c_char,
            );
            return;
        }
    };
    let parent = match read_blob16(args[1]) {
        Ok(v) => v,
        Err(_) => {
            sqlite_result_error(
                ctx,
                b"treecrdt_local_insert: parent must be 16-byte BLOB\0".as_ptr() as *const c_char,
            );
            return;
        }
    };
    let node = match read_blob16(args[2]) {
        Ok(v) => v,
        Err(_) => {
            sqlite_result_error(
                ctx,
                b"treecrdt_local_insert: node must be 16-byte BLOB\0".as_ptr() as *const c_char,
            );
            return;
        }
    };
    let placement = read_text(args[3]);
    let after = match read_optional_blob16(args[4]) {
        Ok(v) => v,
        Err(_) => {
            sqlite_result_error(
                ctx,
                b"treecrdt_local_insert: after must be 16-byte BLOB or NULL\0".as_ptr()
                    as *const c_char,
            );
            return;
        }
    };
    let payload = read_blob(args[5]);

    let db = sqlite_context_db_handle(ctx);
    let parent_id = NodeId(u128::from_be_bytes(parent));
    let node_id = NodeId(u128::from_be_bytes(node));
    let after_id = after.map(|id| NodeId(u128::from_be_bytes(id)));
    let placement = match LocalPlacement::from_parts(placement.as_str(), after_id) {
        Ok(v) => v,
        Err(err) => {
            sqlite_result_error_code(ctx, sqlite_err_from_core(err));
            return;
        }
    };
    let out = match run_local_core_op(db, replica, "treecrdt_local_insert", mode, |crdt| {
        crdt.prepare_local_insert(parent_id, node_id, placement, payload.clone())
    }) {
        Ok(v) => v,
        Err(rc) => {
            sqlite_result_error_code(ctx, rc);
            return;
        }
    };

    sqlite_result_local_dispatch(ctx, &out);
}

pub(super) unsafe extern "C" fn treecrdt_local_move(
    ctx: *mut sqlite3_context,
    argc: c_int,
    argv: *mut *mut sqlite3_value,
) {
    if let Err(rc) = ensure_api_initialized() {
        sqlite_result_error_code(ctx, rc);
        return;
    }

    if !valid_local_op_argc(argc, 5) {
        sqlite_result_error(
            ctx,
            b"treecrdt_local_move expects 5 args, 6 for prepare, or 8 for commit\0".as_ptr()
                as *const c_char,
        );
        return;
    }

    let args = unsafe { slice::from_raw_parts(argv, argc as usize) };
    let mode = match parse_local_op_mode(argc, args, 5) {
        Ok(v) => v,
        Err(_) => {
            sqlite_result_error(
                ctx,
                b"treecrdt_local_move: invalid prepare or commit arguments\0".as_ptr()
                    as *const c_char,
            );
            return;
        }
    };
    let replica = match read_required_blob(args[0]) {
        Ok(v) => v,
        Err(_) => {
            sqlite_result_error(
                ctx,
                b"treecrdt_local_move: NULL replica\0".as_ptr() as *const c_char,
            );
            return;
        }
    };
    let node = match read_blob16(args[1]) {
        Ok(v) => v,
        Err(_) => {
            sqlite_result_error(
                ctx,
                b"treecrdt_local_move: node must be 16-byte BLOB\0".as_ptr() as *const c_char,
            );
            return;
        }
    };
    let new_parent = match read_blob16(args[2]) {
        Ok(v) => v,
        Err(_) => {
            sqlite_result_error(
                ctx,
                b"treecrdt_local_move: new_parent must be 16-byte BLOB\0".as_ptr() as *const c_char,
            );
            return;
        }
    };
    let placement = read_text(args[3]);
    let after = match read_optional_blob16(args[4]) {
        Ok(v) => v,
        Err(_) => {
            sqlite_result_error(
                ctx,
                b"treecrdt_local_move: after must be 16-byte BLOB or NULL\0".as_ptr()
                    as *const c_char,
            );
            return;
        }
    };

    let db = sqlite_context_db_handle(ctx);
    let node_id = NodeId(u128::from_be_bytes(node));
    let new_parent_id = NodeId(u128::from_be_bytes(new_parent));
    let after_id = after.map(|id| NodeId(u128::from_be_bytes(id)));
    let placement = match LocalPlacement::from_parts(placement.as_str(), after_id) {
        Ok(v) => v,
        Err(err) => {
            sqlite_result_error_code(ctx, sqlite_err_from_core(err));
            return;
        }
    };
    let out = match run_local_core_op(db, replica, "treecrdt_local_move", mode, |crdt| {
        crdt.prepare_local_move(node_id, new_parent_id, placement)
    }) {
        Ok(v) => v,
        Err(rc) => {
            sqlite_result_error_code(ctx, rc);
            return;
        }
    };
    sqlite_result_local_dispatch(ctx, &out);
}

pub(super) unsafe extern "C" fn treecrdt_local_delete(
    ctx: *mut sqlite3_context,
    argc: c_int,
    argv: *mut *mut sqlite3_value,
) {
    if let Err(rc) = ensure_api_initialized() {
        sqlite_result_error_code(ctx, rc);
        return;
    }

    if !valid_local_op_argc(argc, 2) {
        sqlite_result_error(
            ctx,
            b"treecrdt_local_delete expects 2 args, 3 for prepare, or 5 for commit\0".as_ptr()
                as *const c_char,
        );
        return;
    }

    let args = unsafe { slice::from_raw_parts(argv, argc as usize) };
    let mode = match parse_local_op_mode(argc, args, 2) {
        Ok(v) => v,
        Err(_) => {
            sqlite_result_error(
                ctx,
                b"treecrdt_local_delete: invalid prepare or commit arguments\0".as_ptr()
                    as *const c_char,
            );
            return;
        }
    };
    let replica = match read_required_blob(args[0]) {
        Ok(v) => v,
        Err(_) => {
            sqlite_result_error(
                ctx,
                b"treecrdt_local_delete: NULL replica\0".as_ptr() as *const c_char,
            );
            return;
        }
    };
    let node = match read_blob16(args[1]) {
        Ok(v) => v,
        Err(_) => {
            sqlite_result_error(
                ctx,
                b"treecrdt_local_delete: node must be 16-byte BLOB\0".as_ptr() as *const c_char,
            );
            return;
        }
    };

    let db = sqlite_context_db_handle(ctx);
    let node_id = NodeId(u128::from_be_bytes(node));
    let out = match run_local_core_op(db, replica, "treecrdt_local_delete", mode, |crdt| {
        crdt.prepare_local_delete(node_id)
    }) {
        Ok(v) => v,
        Err(rc) => {
            sqlite_result_error_code(ctx, rc);
            return;
        }
    };
    sqlite_result_local_dispatch(ctx, &out);
}

pub(super) unsafe extern "C" fn treecrdt_local_payload(
    ctx: *mut sqlite3_context,
    argc: c_int,
    argv: *mut *mut sqlite3_value,
) {
    if let Err(rc) = ensure_api_initialized() {
        sqlite_result_error_code(ctx, rc);
        return;
    }

    if !valid_local_op_argc(argc, 3) {
        sqlite_result_error(
            ctx,
            b"treecrdt_local_payload expects 3 args, 4 for prepare, or 6 for commit\0".as_ptr()
                as *const c_char,
        );
        return;
    }

    let args = unsafe { slice::from_raw_parts(argv, argc as usize) };
    let mode = match parse_local_op_mode(argc, args, 3) {
        Ok(v) => v,
        Err(_) => {
            sqlite_result_error(
                ctx,
                b"treecrdt_local_payload: invalid prepare or commit arguments\0".as_ptr()
                    as *const c_char,
            );
            return;
        }
    };
    let replica = match read_required_blob(args[0]) {
        Ok(v) => v,
        Err(_) => {
            sqlite_result_error(
                ctx,
                b"treecrdt_local_payload: NULL replica\0".as_ptr() as *const c_char,
            );
            return;
        }
    };
    let node = match read_blob16(args[1]) {
        Ok(v) => v,
        Err(_) => {
            sqlite_result_error(
                ctx,
                b"treecrdt_local_payload: node must be 16-byte BLOB\0".as_ptr() as *const c_char,
            );
            return;
        }
    };
    let payload = read_blob(args[2]);

    let db = sqlite_context_db_handle(ctx);
    let node_id = NodeId(u128::from_be_bytes(node));
    let out = match run_local_core_op(db, replica, "treecrdt_local_payload", mode, |crdt| {
        crdt.prepare_local_payload(node_id, payload.clone())
    }) {
        Ok(v) => v,
        Err(rc) => {
            sqlite_result_error_code(ctx, rc);
            return;
        }
    };
    sqlite_result_local_dispatch(ctx, &out);
}

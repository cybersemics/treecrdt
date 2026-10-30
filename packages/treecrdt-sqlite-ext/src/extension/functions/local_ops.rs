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
    /// `None` when the op was minted but not written.
    outcome: Option<JsonMaterializationOutcome>,
    /// Set when the op was not written: see `run_local_core_op`.
    #[serde(skip_serializing_if = "Option::is_none")]
    token: Option<String>,
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
        sqlite_exec(
            self.db,
            self.rollback_sql.as_ptr(),
            None,
            null_mut(),
            null_mut(),
        );
        rc
    }

    fn release(self) -> Result<(), c_int> {
        let rc = sqlite_exec(
            self.db,
            self.commit_sql.as_ptr(),
            None,
            null_mut(),
            null_mut(),
        );
        if rc != SQLITE_OK as c_int {
            return Err(self.rollback(rc));
        }
        Ok(())
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
        .map(|vv| vv.encode().map_err(|_| SQLITE_ERROR as c_int))
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

    let node_store = match SqliteNodeStore::prepare(db) {
        Ok(store) => store,
        Err(_) => {
            sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
            return Err(SQLITE_ERROR as c_int);
        }
    };
    let payload_store = match SqlitePayloadStore::prepare(db) {
        Ok(store) => store,
        Err(_) => {
            sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
            return Err(SQLITE_ERROR as c_int);
        }
    };
    let storage = SqliteOpStorage::with_doc_id(db, doc_id.to_vec());
    let replica_id = ReplicaId::new(replica.to_vec());
    let crdt = match TreeCrdt::with_stores(
        replica_id,
        storage,
        LamportClock::default(),
        node_store,
        payload_store,
    ) {
        Ok(v) => v,
        Err(_) => {
            sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
            return Err(SQLITE_ERROR as c_int);
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
        set_tree_meta_replay_frontier(
            session.db,
            &treecrdt_core::MaterializationFrontier {
                lamport: 0,
                replica: Vec::new(),
                counter: 0,
            },
        )?;
    }

    let op = match json_op_from_operation(op) {
        Ok(v) => v,
        Err(rc) => return Err(session.rollback(rc)),
    };
    session.release()?;

    Ok(JsonLocalOpResult {
        op,
        outcome: Some(json_outcome_from_core(&outcome)),
        token: None,
    })
}

/// Reads the optional last argument, which selects the authorized form of a local op function:
/// the `token` of an earlier call, or NULL. `None` selects the plain form.
fn read_authorized_token(args: &[*mut sqlite3_value], index: usize) -> Option<Option<String>> {
    let &value = args.get(index)?;
    if unsafe { sqlite_value_type(value) } == SQLITE_NULL as c_int {
        return Some(None);
    }
    Some(Some(read_text(value)))
}

/// Identifies a minted op and the materialized tree it was minted from, so tokens match only for
/// the same op with no op applied in between.
fn local_op_token(db: *mut sqlite3, op: &JsonOp) -> Result<String, c_int> {
    let head_seq = load_tree_meta(db)?.state().head_seq();
    let op_json = serde_json::to_vec(op).map_err(|_| SQLITE_ERROR as c_int)?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(&head_seq.to_be_bytes());
    hasher.update(&op_json);
    Ok(hasher.finalize().to_hex().to_string())
}

/// The plain form (`authorized` is `None`) writes the minted op. The authorized form writes it
/// only if its token equals `authorized`; otherwise it writes nothing (materialization catch-up
/// aside) and returns the op with its token instead of an outcome, for the caller to authorize.
fn run_local_core_op<F>(
    db: *mut sqlite3,
    doc_id: Vec<u8>,
    replica: Vec<u8>,
    savepoint_name: &str,
    authorized: Option<Option<String>>,
    build: F,
) -> Result<JsonLocalOpResult, c_int>
where
    F: FnOnce(&mut LocalCrdt) -> treecrdt_core::Result<PreparedLocalOp>,
{
    let mut session = begin_local_core_op(db, &doc_id, &replica, savepoint_name)?;
    let prepared = match build(&mut session.crdt) {
        Ok(v) => v,
        Err(err) => return Err(session.rollback(sqlite_err_from_core(err))),
    };
    if let Some(authorized) = authorized {
        let op = match json_op_from_operation(prepared.op.clone()) {
            Ok(v) => v,
            Err(rc) => return Err(session.rollback(rc)),
        };
        let token = match local_op_token(db, &op) {
            Ok(v) => v,
            Err(rc) => return Err(session.rollback(rc)),
        };
        if authorized.as_deref() != Some(token.as_str()) {
            session.release()?;
            return Ok(JsonLocalOpResult {
                op,
                outcome: None,
                token: Some(token),
            });
        }
    }
    let (op, plan) = match session.crdt.commit_prepared_local(prepared) {
        Ok(v) => v,
        Err(err) => return Err(session.rollback(sqlite_err_from_core(err))),
    };
    finish_local_core_op(session, op, plan)
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

    if argc != 6 && argc != 7 {
        sqlite_result_error(
            ctx,
            b"treecrdt_local_insert expects 6 or 7 args (replica,parent,node,placement,after,payload[,authorized])\0"
                .as_ptr() as *const c_char,
        );
        return;
    }

    let args = unsafe { slice::from_raw_parts(argv, argc as usize) };
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
    let authorized = read_authorized_token(args, 6);

    let db = sqlite_context_db_handle(ctx);
    let doc_id = match load_doc_id(db) {
        Ok(Some(v)) => v,
        Ok(None) => {
            sqlite_result_error(
                ctx,
                b"treecrdt_local_insert: doc_id not set (call treecrdt_set_doc_id)\0".as_ptr()
                    as *const c_char,
            );
            return;
        }
        Err(rc) => {
            sqlite_result_error_code(ctx, rc);
            return;
        }
    };

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
    let out = match run_local_core_op(
        db,
        doc_id,
        replica,
        "treecrdt_local_insert",
        authorized,
        |crdt| crdt.prepare_local_insert(parent_id, node_id, placement, payload.clone()),
    ) {
        Ok(v) => v,
        Err(rc) => {
            sqlite_result_error_code(ctx, rc);
            return;
        }
    };

    sqlite_result_json(ctx, &out);
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

    if argc != 5 && argc != 6 {
        sqlite_result_error(
            ctx,
            b"treecrdt_local_move expects 5 or 6 args (replica,node,new_parent,placement,after[,authorized])\0"
                .as_ptr() as *const c_char,
        );
        return;
    }

    let args = unsafe { slice::from_raw_parts(argv, argc as usize) };
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
    let authorized = read_authorized_token(args, 5);

    let db = sqlite_context_db_handle(ctx);
    let doc_id = match load_doc_id(db) {
        Ok(Some(v)) => v,
        Ok(None) => {
            sqlite_result_error(
                ctx,
                b"treecrdt_local_move: doc_id not set (call treecrdt_set_doc_id)\0".as_ptr()
                    as *const c_char,
            );
            return;
        }
        Err(rc) => {
            sqlite_result_error_code(ctx, rc);
            return;
        }
    };

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
    let out = match run_local_core_op(
        db,
        doc_id,
        replica,
        "treecrdt_local_move",
        authorized,
        |crdt| crdt.prepare_local_move(node_id, new_parent_id, placement),
    ) {
        Ok(v) => v,
        Err(rc) => {
            sqlite_result_error_code(ctx, rc);
            return;
        }
    };
    sqlite_result_json(ctx, &out);
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

    if argc != 2 && argc != 3 {
        sqlite_result_error(
            ctx,
            b"treecrdt_local_delete expects 2 or 3 args (replica,node[,authorized])\0".as_ptr()
                as *const c_char,
        );
        return;
    }

    let args = unsafe { slice::from_raw_parts(argv, argc as usize) };
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
    let authorized = read_authorized_token(args, 2);

    let db = sqlite_context_db_handle(ctx);
    let doc_id = match load_doc_id(db) {
        Ok(Some(v)) => v,
        Ok(None) => {
            sqlite_result_error(
                ctx,
                b"treecrdt_local_delete: doc_id not set (call treecrdt_set_doc_id)\0".as_ptr()
                    as *const c_char,
            );
            return;
        }
        Err(rc) => {
            sqlite_result_error_code(ctx, rc);
            return;
        }
    };

    let node_id = NodeId(u128::from_be_bytes(node));
    let out = match run_local_core_op(
        db,
        doc_id,
        replica,
        "treecrdt_local_delete",
        authorized,
        |crdt| crdt.prepare_local_delete(node_id),
    ) {
        Ok(v) => v,
        Err(rc) => {
            sqlite_result_error_code(ctx, rc);
            return;
        }
    };
    sqlite_result_json(ctx, &out);
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

    if argc != 3 && argc != 4 {
        sqlite_result_error(
            ctx,
            b"treecrdt_local_payload expects 3 or 4 args (replica,node,payload[,authorized])\0"
                .as_ptr() as *const c_char,
        );
        return;
    }

    let args = unsafe { slice::from_raw_parts(argv, argc as usize) };
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
    let authorized = read_authorized_token(args, 3);

    let db = sqlite_context_db_handle(ctx);
    let doc_id = match load_doc_id(db) {
        Ok(Some(v)) => v,
        Ok(None) => {
            sqlite_result_error(
                ctx,
                b"treecrdt_local_payload: doc_id not set (call treecrdt_set_doc_id)\0".as_ptr()
                    as *const c_char,
            );
            return;
        }
        Err(rc) => {
            sqlite_result_error_code(ctx, rc);
            return;
        }
    };

    let node_id = NodeId(u128::from_be_bytes(node));
    let out = match run_local_core_op(
        db,
        doc_id,
        replica,
        "treecrdt_local_payload",
        authorized,
        |crdt| crdt.prepare_local_payload(node_id, payload.clone()),
    ) {
        Ok(v) => v,
        Err(rc) => {
            sqlite_result_error_code(ctx, rc);
            return;
        }
    };
    sqlite_result_json(ctx, &out);
}

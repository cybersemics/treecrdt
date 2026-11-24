use super::materialize::{json_outcome_from_core, JsonMaterializationOutcome};
use super::node_store::SqliteNodeStore;
use super::op_index::SqliteParentOpIndex;
use super::op_storage::SqliteOpStorage;
use super::payload_store::SqlitePayloadStore;
use super::util::{
    read_blob, read_blob16, read_optional_blob, read_optional_blob16, read_required_blob,
    read_text, sqlite_err_from_core, sqlite_result_json,
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
    /// Written only for create-only inserts.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    if_absent: bool,
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct JsonLocalOpResult {
    op: JsonOp,
    outcome: JsonMaterializationOutcome,
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
            if_absent,
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
            if_absent,
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
            if_absent: false,
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
            if_absent: false,
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
            if_absent: false,
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
            if_absent: false,
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
    if post_materialization_ok {
        if let Err(rc) = record_changed_nodes(session.db, &outcome) {
            sqlite_exec(
                session.db,
                session.rollback_sql.as_ptr(),
                None,
                null_mut(),
                null_mut(),
            );
            return Err(rc);
        }
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
        Err(rc) => {
            sqlite_exec(
                session.db,
                session.rollback_sql.as_ptr(),
                None,
                null_mut(),
                null_mut(),
            );
            return Err(rc);
        }
    };

    let commit_rc = sqlite_exec(
        session.db,
        session.commit_sql.as_ptr(),
        None,
        null_mut(),
        null_mut(),
    );
    if commit_rc != SQLITE_OK as c_int {
        sqlite_exec(
            session.db,
            session.rollback_sql.as_ptr(),
            None,
            null_mut(),
            null_mut(),
        );
        return Err(commit_rc);
    }

    Ok(JsonLocalOpResult {
        op,
        outcome: json_outcome_from_core(&outcome),
    })
}

/// Runs a local op in a savepoint. When `build` prepares no op, it writes nothing, a catch-up
/// included, and returns None.
fn run_local_core_op<F>(
    db: *mut sqlite3,
    doc_id: Vec<u8>,
    replica: Vec<u8>,
    savepoint_name: &str,
    build: F,
) -> Result<Option<JsonLocalOpResult>, c_int>
where
    F: FnOnce(&mut LocalCrdt) -> treecrdt_core::Result<Option<PreparedLocalOp>>,
{
    let mut session = begin_local_core_op(db, &doc_id, &replica, savepoint_name)?;
    let prepared = match build(&mut session.crdt) {
        Ok(Some(v)) => v,
        Ok(None) => {
            session.rollback(SQLITE_OK as c_int);
            return Ok(None);
        }
        Err(err) => return Err(session.rollback(sqlite_err_from_core(err))),
    };
    let (op, plan) = match session.crdt.commit_prepared_local(prepared) {
        Ok(v) => v,
        Err(err) => return Err(session.rollback(sqlite_err_from_core(err))),
    };
    finish_local_core_op(session, op, plan).map(Some)
}

/// Sets a local op function's result: the op and its outcome as JSON, or NULL when it wrote nothing.
fn sqlite_result_local_op(
    ctx: *mut sqlite3_context,
    out: Result<Option<JsonLocalOpResult>, c_int>,
) {
    match out {
        Ok(Some(out)) => sqlite_result_json(ctx, &out),
        // A function that sets no result returns NULL.
        Ok(None) => {}
        Err(rc) => sqlite_result_error_code(ctx, rc),
    }
}

/// Sets the error result `{function}: {reason}`.
fn sqlite_result_function_error(ctx: *mut sqlite3_context, function: &str, reason: &str) {
    // Both parts are static, with no NUL byte to fail on.
    let message = CString::new(format!("{function}: {reason}")).unwrap_or_default();
    sqlite_result_error(ctx, message.as_ptr());
}

/// Reads the placement of a local insert or move from its name, its `after` arg and its trailing
/// args. On a bad placement it sets the error result and returns None.
fn read_placement(
    ctx: *mut sqlite3_context,
    function: &str,
    placement: &str,
    after: Option<NodeId>,
    trailing: &[*mut sqlite3_value],
) -> Option<LocalPlacement> {
    let reason = match (placement, trailing) {
        ("at", &[key]) => match read_optional_blob(key) {
            Some(key) => return Some(LocalPlacement::At { key }),
            None => "placement at needs a key, not NULL",
        },
        // A NULL bound is an open end.
        ("between", &[left, right]) => {
            return Some(LocalPlacement::Between {
                left: read_optional_blob(left),
                right: read_optional_blob(right),
            });
        }
        ("at", _) => "placement at takes one trailing arg (key)",
        ("between", _) => "placement between takes two trailing args (left,right)",
        (placement, []) => match LocalPlacement::from_parts(placement, after) {
            Ok(placement) => return Some(placement),
            Err(err) => {
                sqlite_result_error_code(ctx, sqlite_err_from_core(err));
                return None;
            }
        },
        _ => "only placements at and between take trailing args",
    };
    sqlite_result_function_error(ctx, function, reason);
    None
}

/// `treecrdt_local_insert(replica, parent, node, placement, after, payload, ...)`, where the
/// trailing args are the key of placement 'at', or the left and right bounds of placement
/// 'between' (NULL: an open end). Placements 'first', 'last' and 'after' (the `after` node) take
/// none.
pub(super) unsafe extern "C" fn treecrdt_local_insert(
    ctx: *mut sqlite3_context,
    argc: c_int,
    argv: *mut *mut sqlite3_value,
) {
    local_insert(ctx, argc, argv, "treecrdt_local_insert", false);
}

/// `treecrdt_local_insert_if_absent(...)`, with the args of `treecrdt_local_insert`, writes a
/// create-only insert, which stays a no-op wherever an earlier op placed the node. When the node is
/// already in the tree (ROOT, TRASH, or attached anywhere, deleted included), it returns NULL and
/// writes nothing.
pub(super) unsafe extern "C" fn treecrdt_local_insert_if_absent(
    ctx: *mut sqlite3_context,
    argc: c_int,
    argv: *mut *mut sqlite3_value,
) {
    local_insert(ctx, argc, argv, "treecrdt_local_insert_if_absent", true);
}

fn local_insert(
    ctx: *mut sqlite3_context,
    argc: c_int,
    argv: *mut *mut sqlite3_value,
    function: &str,
    if_absent: bool,
) {
    if let Err(rc) = ensure_api_initialized() {
        sqlite_result_error_code(ctx, rc);
        return;
    }

    if !(6..=8).contains(&argc) {
        // Static, with no NUL byte to fail on.
        let message = CString::new(format!(
            "{function} expects 6 args (replica,parent,node,placement,after,payload), \
             then the key of placement at or the left and right bounds of placement between"
        ))
        .unwrap_or_default();
        sqlite_result_error(ctx, message.as_ptr());
        return;
    }

    let args = unsafe { slice::from_raw_parts(argv, argc as usize) };
    let Ok(replica) = read_required_blob(args[0]) else {
        sqlite_result_function_error(ctx, function, "NULL replica");
        return;
    };
    let Ok(parent) = read_blob16(args[1]) else {
        sqlite_result_function_error(ctx, function, "parent must be 16-byte BLOB");
        return;
    };
    let Ok(node) = read_blob16(args[2]) else {
        sqlite_result_function_error(ctx, function, "node must be 16-byte BLOB");
        return;
    };
    let placement = read_text(args[3]);
    let Ok(after) = read_optional_blob16(args[4]) else {
        sqlite_result_function_error(ctx, function, "after must be 16-byte BLOB or NULL");
        return;
    };
    let payload = read_blob(args[5]);
    let after_id = after.map(|id| NodeId(u128::from_be_bytes(id)));
    let Some(placement) = read_placement(ctx, function, &placement, after_id, &args[6..]) else {
        return;
    };

    let db = sqlite_context_db_handle(ctx);
    let doc_id = match load_doc_id(db) {
        Ok(Some(v)) => v,
        Ok(None) => {
            sqlite_result_function_error(
                ctx,
                function,
                "doc_id not set (call treecrdt_set_doc_id)",
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
    let out = run_local_core_op(db, doc_id, replica, function, |crdt| {
        if if_absent {
            crdt.prepare_local_insert_if_absent(parent_id, node_id, placement, payload)
        } else {
            crdt.prepare_local_insert(parent_id, node_id, placement, payload).map(Some)
        }
    });
    sqlite_result_local_op(ctx, out);
}

/// `treecrdt_local_move(replica, node, new_parent, placement, after, ...)`, with the trailing args
/// of `treecrdt_local_insert`.
pub(super) unsafe extern "C" fn treecrdt_local_move(
    ctx: *mut sqlite3_context,
    argc: c_int,
    argv: *mut *mut sqlite3_value,
) {
    if let Err(rc) = ensure_api_initialized() {
        sqlite_result_error_code(ctx, rc);
        return;
    }

    if !(5..=7).contains(&argc) {
        sqlite_result_error(
            ctx,
            b"treecrdt_local_move expects 5 args (replica,node,new_parent,placement,after), \
              then the key of placement at or the left and right bounds of placement between\0"
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
    let after_id = after.map(|id| NodeId(u128::from_be_bytes(id)));
    let Some(placement) =
        read_placement(ctx, "treecrdt_local_move", &placement, after_id, &args[5..])
    else {
        return;
    };

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
    let out = run_local_core_op(db, doc_id, replica, "treecrdt_local_move", |crdt| {
        crdt.prepare_local_move(node_id, new_parent_id, placement).map(Some)
    });
    sqlite_result_local_op(ctx, out);
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

    if argc != 2 {
        sqlite_result_error(
            ctx,
            b"treecrdt_local_delete expects 2 args (replica,node)\0".as_ptr() as *const c_char,
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
    let out = run_local_core_op(db, doc_id, replica, "treecrdt_local_delete", |crdt| {
        crdt.prepare_local_delete(node_id).map(Some)
    });
    sqlite_result_local_op(ctx, out);
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

    if argc != 3 {
        sqlite_result_error(
            ctx,
            b"treecrdt_local_payload expects 3 args (replica,node,payload)\0".as_ptr()
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
    let out = run_local_core_op(db, doc_id, replica, "treecrdt_local_payload", |crdt| {
        crdt.prepare_local_payload(node_id, payload).map(Some)
    });
    sqlite_result_local_op(ctx, out);
}

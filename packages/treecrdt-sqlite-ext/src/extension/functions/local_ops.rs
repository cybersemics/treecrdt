use super::node_store::SqliteNodeStore;
use super::op_index::SqliteParentOpIndex;
use super::op_storage::SqliteOpStorage;
use super::payload_store::SqlitePayloadStore;
use super::util::sqlite_result_json;
use super::*;
use treecrdt_core::ParentOpIndex;
use treecrdt_core::{LamportClock, Operation, OperationId, OperationKind, ReplicaId, TreeCrdt};

fn read_blob(val: *mut sqlite3_value) -> Option<Vec<u8>> {
    unsafe {
        if sqlite_value_type(val) == SQLITE_NULL as c_int {
            return None;
        }
        let ptr = sqlite_value_blob(val) as *const u8;
        let len = sqlite_value_bytes(val) as usize;
        if ptr.is_null() {
            return None;
        }
        Some(slice::from_raw_parts(ptr, len).to_vec())
    }
}

fn read_required_blob(val: *mut sqlite3_value) -> Result<Vec<u8>, ()> {
    match read_blob(val) {
        Some(v) => Ok(v),
        None => Err(()),
    }
}

fn read_blob16(val: *mut sqlite3_value) -> Result<[u8; 16], ()> {
    let bytes = read_required_blob(val)?;
    if bytes.len() != 16 {
        return Err(());
    }
    let mut out = [0u8; 16];
    out.copy_from_slice(&bytes);
    Ok(out)
}

fn read_optional_blob16(val: *mut sqlite3_value) -> Result<Option<[u8; 16]>, ()> {
    unsafe {
        if sqlite_value_type(val) == SQLITE_NULL as c_int {
            return Ok(None);
        }
    }
    Ok(Some(read_blob16(val)?))
}

fn read_text(val: *mut sqlite3_value) -> String {
    unsafe {
        let ptr = sqlite_value_text(val) as *const u8;
        let len = sqlite_value_bytes(val) as usize;
        if ptr.is_null() || len == 0 {
            return String::new();
        }
        std::str::from_utf8(slice::from_raw_parts(ptr, len)).unwrap_or("").to_string()
    }
}

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

fn sqlite_err_from_core(_: treecrdt_core::Error) -> c_int {
    SQLITE_ERROR as c_int
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

fn invalid_op_error(msg: &str) -> treecrdt_core::Error {
    treecrdt_core::Error::InvalidOperation(msg.to_string())
}

fn resolve_after_node(
    crdt: &LocalCrdt,
    parent: NodeId,
    placement: &str,
    after: Option<[u8; 16]>,
    exclude: Option<NodeId>,
) -> treecrdt_core::Result<Option<NodeId>> {
    let out = match placement {
        "first" => None,
        "after" => {
            let Some(after_bytes) = after else {
                return Err(invalid_op_error("missing after for placement=after"));
            };
            let after_id = NodeId(u128::from_be_bytes(after_bytes));
            if exclude.is_some() && exclude == Some(after_id) {
                return Err(invalid_op_error("after cannot be excluded node"));
            }
            Some(after_id)
        }
        "last" => {
            let mut children = crdt.children(parent)?;
            if let Some(excluded) = exclude {
                children.retain(|c| *c != excluded);
            }
            children.last().copied()
        }
        _ => return Err(invalid_op_error("invalid placement")),
    };
    Ok(out)
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
    let storage = SqliteOpStorage::new(db);
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
    parent_hints: Vec<NodeId>,
    extra_index_records: Vec<(NodeId, OperationId)>,
) -> Result<JsonOp, c_int> {
    let mut post_materialization_ok = true;
    let mut refresh_starts = parent_hints.clone();
    refresh_starts.push(op.kind.node());
    if session.crdt.refresh_tombstones_upward(refresh_starts).is_err() {
        post_materialization_ok = false;
    }

    let mut seq = 0u64;
    match load_tree_meta(session.db) {
        Ok(meta) => seq = meta.head_seq.saturating_add(1),
        Err(_) => post_materialization_ok = false,
    }
    if post_materialization_ok {
        match SqliteParentOpIndex::prepare(session.db, session.doc_id.clone()) {
            Ok(mut op_index) => {
                let mut seen: Vec<NodeId> = Vec::new();
                for parent in parent_hints {
                    if parent == NodeId::TRASH || seen.contains(&parent) {
                        continue;
                    }
                    seen.push(parent);
                    if op_index.record(parent, &op.meta.id, seq).is_err() {
                        post_materialization_ok = false;
                        break;
                    }
                }
                if post_materialization_ok {
                    for (parent, op_id) in extra_index_records {
                        if parent == NodeId::TRASH {
                            continue;
                        }
                        if op_index.record(parent, &op_id, seq).is_err() {
                            post_materialization_ok = false;
                            break;
                        }
                    }
                }
            }
            Err(_) => post_materialization_ok = false,
        }
    }
    if post_materialization_ok
        && update_tree_meta_head(
            session.db,
            op.meta.lamport,
            op.meta.id.replica.as_bytes(),
            op.meta.id.counter,
            seq,
        )
        .is_err()
    {
        post_materialization_ok = false;
    }
    if !post_materialization_ok {
        let _ = set_tree_meta_dirty(session.db, true);
    }

    let out = match json_op_from_operation(op) {
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

    Ok(out)
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

    if argc != 6 {
        sqlite_result_error(
            ctx,
            b"treecrdt_local_insert expects 6 args (replica,parent,node,placement,after,payload)\0"
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

    let mut session = match begin_local_core_op(db, &doc_id, &replica, "treecrdt_local_insert") {
        Ok(v) => v,
        Err(rc) => {
            sqlite_result_error_code(ctx, rc);
            return;
        }
    };
    let parent_id = NodeId(u128::from_be_bytes(parent));
    let node_id = NodeId(u128::from_be_bytes(node));
    let after_id =
        match resolve_after_node(&session.crdt, parent_id, placement.as_str(), after, None) {
            Ok(v) => v,
            Err(err) => {
                sqlite_result_error_code(ctx, session.rollback(sqlite_err_from_core(err)));
                return;
            }
        };
    let op = if let Some(payload) = payload.clone() {
        match session
            .crdt
            .local_insert_after_with_payload(parent_id, node_id, after_id, payload)
        {
            Ok(v) => v,
            Err(err) => {
                sqlite_result_error_code(ctx, session.rollback(sqlite_err_from_core(err)));
                return;
            }
        }
    } else {
        match session.crdt.local_insert_after(parent_id, node_id, after_id) {
            Ok(v) => v,
            Err(err) => {
                sqlite_result_error_code(ctx, session.rollback(sqlite_err_from_core(err)));
                return;
            }
        }
    };
    let out = match finish_local_core_op(session, op, vec![parent_id], Vec::new()) {
        Ok(v) => v,
        Err(rc) => {
            sqlite_result_error_code(ctx, rc);
            return;
        }
    };

    sqlite_result_json(ctx, &vec![out]);
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

    if argc != 5 {
        sqlite_result_error(
            ctx,
            b"treecrdt_local_move expects 5 args (replica,node,new_parent,placement,after)\0"
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

    let mut session = match begin_local_core_op(db, &doc_id, &replica, "treecrdt_local_move") {
        Ok(v) => v,
        Err(rc) => {
            sqlite_result_error_code(ctx, rc);
            return;
        }
    };
    let node_id = NodeId(u128::from_be_bytes(node));
    let new_parent_id = NodeId(u128::from_be_bytes(new_parent));
    let old_parent = match session.crdt.parent(node_id) {
        Ok(v) => v,
        Err(err) => {
            sqlite_result_error_code(ctx, session.rollback(sqlite_err_from_core(err)));
            return;
        }
    };
    let after_id = match resolve_after_node(
        &session.crdt,
        new_parent_id,
        placement.as_str(),
        after,
        Some(node_id),
    ) {
        Ok(v) => v,
        Err(err) => {
            sqlite_result_error_code(ctx, session.rollback(sqlite_err_from_core(err)));
            return;
        }
    };
    let op = match session.crdt.local_move_after(node_id, new_parent_id, after_id) {
        Ok(v) => v,
        Err(err) => {
            sqlite_result_error_code(ctx, session.rollback(sqlite_err_from_core(err)));
            return;
        }
    };
    let mut parent_hints = vec![new_parent_id];
    if let Some(parent) = old_parent {
        parent_hints.push(parent);
    }
    let mut extra_index_records: Vec<(NodeId, OperationId)> = Vec::new();
    if old_parent != Some(new_parent_id) && new_parent_id != NodeId::TRASH {
        match session.crdt.payload_last_writer(node_id) {
            Ok(Some((_lamport, payload_id))) => {
                extra_index_records.push((new_parent_id, payload_id))
            }
            Ok(None) => {}
            Err(err) => {
                sqlite_result_error_code(ctx, session.rollback(sqlite_err_from_core(err)));
                return;
            }
        }
    }
    let out = match finish_local_core_op(session, op, parent_hints, extra_index_records) {
        Ok(v) => v,
        Err(rc) => {
            sqlite_result_error_code(ctx, rc);
            return;
        }
    };
    sqlite_result_json(ctx, &vec![out]);
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

    let mut session = match begin_local_core_op(db, &doc_id, &replica, "treecrdt_local_delete") {
        Ok(v) => v,
        Err(rc) => {
            sqlite_result_error_code(ctx, rc);
            return;
        }
    };
    let node_id = NodeId(u128::from_be_bytes(node));
    let old_parent = match session.crdt.parent(node_id) {
        Ok(v) => v,
        Err(err) => {
            sqlite_result_error_code(ctx, session.rollback(sqlite_err_from_core(err)));
            return;
        }
    };
    let op = match session.crdt.local_delete(node_id) {
        Ok(v) => v,
        Err(err) => {
            sqlite_result_error_code(ctx, session.rollback(sqlite_err_from_core(err)));
            return;
        }
    };
    let mut parent_hints = Vec::new();
    if let Some(parent) = old_parent {
        parent_hints.push(parent);
    }
    let out = match finish_local_core_op(session, op, parent_hints, Vec::new()) {
        Ok(v) => v,
        Err(rc) => {
            sqlite_result_error_code(ctx, rc);
            return;
        }
    };
    sqlite_result_json(ctx, &vec![out]);
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

    let mut session = match begin_local_core_op(db, &doc_id, &replica, "treecrdt_local_payload") {
        Ok(v) => v,
        Err(rc) => {
            sqlite_result_error_code(ctx, rc);
            return;
        }
    };
    let node_id = NodeId(u128::from_be_bytes(node));
    let parent = match session.crdt.parent(node_id) {
        Ok(v) => v,
        Err(err) => {
            sqlite_result_error_code(ctx, session.rollback(sqlite_err_from_core(err)));
            return;
        }
    };
    let op = if let Some(payload) = payload.clone() {
        match session.crdt.local_set_payload(node_id, payload) {
            Ok(v) => v,
            Err(err) => {
                sqlite_result_error_code(ctx, session.rollback(sqlite_err_from_core(err)));
                return;
            }
        }
    } else {
        match session.crdt.local_clear_payload(node_id) {
            Ok(v) => v,
            Err(err) => {
                sqlite_result_error_code(ctx, session.rollback(sqlite_err_from_core(err)));
                return;
            }
        }
    };
    let mut parent_hints = Vec::new();
    if let Some(parent) = parent {
        parent_hints.push(parent);
    }
    let out = match finish_local_core_op(session, op, parent_hints, Vec::new()) {
        Ok(v) => v,
        Err(rc) => {
            sqlite_result_error_code(ctx, rc);
            return;
        }
    };
    sqlite_result_json(ctx, &vec![out]);
}

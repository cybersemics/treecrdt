use super::append::JsonAppendOp;
use super::node_store::SqliteNodeStore;
use super::op_index::SqliteParentOpIndex;
use super::payload_store::SqlitePayloadStore;
use super::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MaterializeKind {
    Insert,
    Move,
    Delete,
    Tombstone,
    Payload,
}

impl MaterializeKind {
    fn parse(s: &str) -> Option<Self> {
        match s {
            "insert" => Some(Self::Insert),
            "move" => Some(Self::Move),
            "delete" => Some(Self::Delete),
            "tombstone" => Some(Self::Tombstone),
            "payload" => Some(Self::Payload),
            _ => None,
        }
    }
}

#[derive(Clone, Debug)]
struct MaterializeOp {
    replica: Vec<u8>,
    counter: u64,
    lamport: Lamport,
    kind: MaterializeKind,
    parent: Option<NodeId>,
    node: NodeId,
    new_parent: Option<NodeId>,
    order_key: Vec<u8>,
    known_state: Option<VersionVector>,
    payload: Option<Vec<u8>>,
}

fn materialize_ops_in_order(
    db: *mut sqlite3,
    doc_id: &[u8],
    meta: &TreeMeta,
    ops: &mut [MaterializeOp],
) -> Result<(), c_int> {
    if ops.is_empty() {
        return Ok(());
    }
    if meta.dirty {
        return Err(SQLITE_ERROR as c_int);
    }

    ops.sort_by(|a, b| {
        treecrdt_core::cmp_op_key(
            a.lamport, &a.replica, a.counter, b.lamport, &b.replica, b.counter,
        )
    });
    if let Some(first) = ops.first() {
        if treecrdt_core::cmp_op_key(
            first.lamport,
            &first.replica,
            first.counter,
            meta.head_lamport,
            &meta.head_replica,
            meta.head_counter,
        ) == std::cmp::Ordering::Less
        {
            return Err(SQLITE_ERROR as c_int);
        }
    }

    use treecrdt_core::{
        LamportClock, Operation, OperationId, OperationKind, OperationMetadata, ReplicaId, TreeCrdt,
    };
    let node_store = SqliteNodeStore::prepare(db).map_err(|_| SQLITE_ERROR as c_int)?;
    let payload_store = SqlitePayloadStore::prepare(db).map_err(|_| SQLITE_ERROR as c_int)?;
    let mut op_index =
        SqliteParentOpIndex::prepare(db, doc_id.to_vec()).map_err(|_| SQLITE_ERROR as c_int)?;
    let mut crdt = TreeCrdt::with_stores(
        ReplicaId::new(b"sqlite-ext"),
        NoopStorage::default(),
        LamportClock::default(),
        node_store,
        payload_store,
    )
    .map_err(|_| SQLITE_ERROR as c_int)?;

    let mut seq = meta.head_seq;

    for op in ops.iter() {
        let op_kind = match op.kind {
            MaterializeKind::Insert => {
                let parent = op.parent.ok_or(SQLITE_ERROR as c_int)?;
                OperationKind::Insert {
                    parent,
                    node: op.node,
                    order_key: op.order_key.clone(),
                    payload: op.payload.clone(),
                }
            }
            MaterializeKind::Move => {
                let new_parent = op.new_parent.ok_or(SQLITE_ERROR as c_int)?;
                OperationKind::Move {
                    node: op.node,
                    new_parent,
                    order_key: op.order_key.clone(),
                }
            }
            MaterializeKind::Delete => OperationKind::Delete { node: op.node },
            MaterializeKind::Tombstone => OperationKind::Tombstone { node: op.node },
            MaterializeKind::Payload => OperationKind::Payload {
                node: op.node,
                payload: op.payload.clone(),
            },
        };

        let operation = Operation {
            meta: OperationMetadata {
                id: OperationId {
                    replica: ReplicaId(op.replica.clone()),
                    counter: op.counter,
                },
                lamport: op.lamport,
                known_state: op.known_state.clone(),
            },
            kind: op_kind,
        };

        let _ = crdt
            .apply_remote_with_materialization_seq(operation, &mut op_index, &mut seq)
            .map_err(|_| SQLITE_ERROR as c_int)?;
    }

    let last = crdt.head_op().ok_or(SQLITE_ERROR as c_int)?;
    update_tree_meta_head(
        db,
        last.meta.lamport,
        last.meta.id.replica.as_bytes(),
        last.meta.id.counter,
        seq,
    )?;
    Ok(())
}

pub(super) fn ensure_materialized(db: *mut sqlite3) -> Result<(), c_int> {
    let meta = load_tree_meta(db)?;
    if !meta.dirty {
        return Ok(());
    }
    rebuild_materialized(db)
}

pub(super) unsafe extern "C" fn treecrdt_ensure_materialized(
    ctx: *mut sqlite3_context,
    argc: c_int,
    _argv: *mut *mut sqlite3_value,
) {
    if argc != 0 {
        sqlite_result_error(
            ctx,
            b"treecrdt_ensure_materialized expects 0 args\0".as_ptr() as *const c_char,
        );
        return;
    }

    let db = sqlite_context_db_handle(ctx);
    match ensure_materialized(db) {
        Ok(()) => sqlite_result_int(ctx, 1),
        Err(rc) => sqlite_result_error_code(ctx, rc),
    }
}

fn rebuild_materialized(db: *mut sqlite3) -> Result<(), c_int> {
    let begin = CString::new("SAVEPOINT treecrdt_materialize").expect("static");
    let commit = CString::new("RELEASE treecrdt_materialize").expect("static");
    let rollback = CString::new("ROLLBACK TO treecrdt_materialize; RELEASE treecrdt_materialize")
        .expect("static");

    if sqlite_exec(db, begin.as_ptr(), None, null_mut(), null_mut()) != SQLITE_OK as c_int {
        return Err(SQLITE_ERROR as c_int);
    }

    let doc_id = load_doc_id(db).unwrap_or(None).unwrap_or_default();

    // Rebuild materialized state by replaying the op-log through core semantics.
    use treecrdt_core::{LamportClock, ReplicaId, TreeCrdt};
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
    let storage = super::op_storage::SqliteOpStorage::new(db);
    let mut op_index = match SqliteParentOpIndex::prepare(db, doc_id.clone()) {
        Ok(v) => v,
        Err(_) => {
            sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
            return Err(SQLITE_ERROR as c_int);
        }
    };
    let mut crdt = TreeCrdt::with_stores(
        ReplicaId::new(b"sqlite-ext"),
        storage,
        LamportClock::default(),
        node_store,
        payload_store,
    )
    .map_err(|_| SQLITE_ERROR as c_int)?;
    if crdt.replay_from_storage_with_materialization(&mut op_index).is_err() {
        sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
        return Err(SQLITE_ERROR as c_int);
    }

    // Update meta head + seq.
    let seq = crdt.log_len() as u64;
    if let Some(last) = crdt.head_op() {
        let head_rc = update_tree_meta_head(
            db,
            last.meta.lamport,
            last.meta.id.replica.as_bytes(),
            last.meta.id.counter,
            seq,
        );
        if head_rc.is_err() {
            sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
            return head_rc;
        }
    } else {
        let head_rc = update_tree_meta_head(db, 0, &[], 0, 0);
        if head_rc.is_err() {
            sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
            return head_rc;
        }
    }

    let commit_rc = sqlite_exec(db, commit.as_ptr(), None, null_mut(), null_mut());
    if commit_rc != SQLITE_OK as c_int {
        sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
        return Err(commit_rc);
    }
    Ok(())
}

pub(super) fn append_ops_impl(
    db: *mut sqlite3,
    doc_id: &[u8],
    savepoint_name: &str,
    ops: &[JsonAppendOp],
) -> Result<i64, c_int> {
    if ops.is_empty() {
        return Ok(0);
    }

    let meta = load_tree_meta(db)?;
    let mut materialize_ops: Vec<MaterializeOp> = Vec::new();
    let mut materialize_ok = !meta.dirty;
    if materialize_ok {
        materialize_ops.reserve(ops.len());
    }

    let begin = CString::new(format!("SAVEPOINT {savepoint_name}")).expect("savepoint begin");
    let commit = CString::new(format!("RELEASE {savepoint_name}")).expect("savepoint commit");
    let rollback = CString::new(format!(
        "ROLLBACK TO {savepoint_name}; RELEASE {savepoint_name}"
    ))
    .expect("savepoint rollback");

    if sqlite_exec(db, begin.as_ptr(), None, null_mut(), null_mut()) != SQLITE_OK as c_int {
        return Err(SQLITE_ERROR as c_int);
    }

    let insert_sql = CString::new(
        "INSERT OR IGNORE INTO ops (replica,counter,lamport,kind,parent,node,new_parent,order_key,known_state,payload,op_ref) \
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",
    )
    .expect("insert ops sql");
    let mut stmt: *mut sqlite3_stmt = null_mut();
    let prep_rc = sqlite_prepare_v2(db, insert_sql.as_ptr(), -1, &mut stmt, null_mut());
    if prep_rc != SQLITE_OK as c_int {
        sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
        return Err(prep_rc);
    }

    let mut inserted: i64 = 0;
    let mut err_rc: c_int = SQLITE_OK as c_int;

    for op in ops {
        unsafe {
            sqlite_clear_bindings(stmt);
            sqlite_reset(stmt);
        }
        let mut bind_err = false;
        unsafe {
            bind_err |= sqlite_bind_blob(
                stmt,
                1,
                op.replica.as_ptr() as *const c_void,
                op.replica.len() as c_int,
                None,
            ) != SQLITE_OK as c_int;
            bind_err |= sqlite_bind_int64(stmt, 2, (op.counter.min(i64::MAX as u64)) as i64)
                != SQLITE_OK as c_int;
            bind_err |= sqlite_bind_int64(stmt, 3, (op.lamport.min(i64::MAX as u64)) as i64)
                != SQLITE_OK as c_int;
        }
        let kind_cstr =
            CString::new(op.kind.as_str()).unwrap_or_else(|_| CString::new("insert").unwrap());
        unsafe {
            bind_err |=
                sqlite_bind_text(stmt, 4, kind_cstr.as_ptr(), -1, None) != SQLITE_OK as c_int;
        }
        unsafe {
            if let Some(parent) = op.parent.as_ref() {
                bind_err |= sqlite_bind_blob(
                    stmt,
                    5,
                    parent.as_ptr() as *const c_void,
                    parent.len() as c_int,
                    None,
                ) != SQLITE_OK as c_int;
            } else {
                bind_err |= sqlite_bind_null(stmt, 5) != SQLITE_OK as c_int;
            }
            bind_err |= sqlite_bind_blob(
                stmt,
                6,
                op.node.as_ptr() as *const c_void,
                op.node.len() as c_int,
                None,
            ) != SQLITE_OK as c_int;
            if let Some(newp) = op.new_parent.as_ref() {
                bind_err |= sqlite_bind_blob(
                    stmt,
                    7,
                    newp.as_ptr() as *const c_void,
                    newp.len() as c_int,
                    None,
                ) != SQLITE_OK as c_int;
            } else {
                bind_err |= sqlite_bind_null(stmt, 7) != SQLITE_OK as c_int;
            }
            if let Some(ref order_key) = op.order_key {
                if order_key.is_empty() {
                    // Distinguish empty key from NULL.
                    let empty: [u8; 0] = [];
                    bind_err |= sqlite_bind_blob(stmt, 8, empty.as_ptr() as *const c_void, 0, None)
                        != SQLITE_OK as c_int;
                } else {
                    bind_err |= sqlite_bind_blob(
                        stmt,
                        8,
                        order_key.as_ptr() as *const c_void,
                        order_key.len() as c_int,
                        None,
                    ) != SQLITE_OK as c_int;
                }
            } else {
                bind_err |= sqlite_bind_null(stmt, 8) != SQLITE_OK as c_int;
            }
            if let Some(ref known_state) = op.known_state {
                if known_state.is_empty() {
                    bind_err |= sqlite_bind_null(stmt, 9) != SQLITE_OK as c_int;
                } else {
                    bind_err |= sqlite_bind_blob(
                        stmt,
                        9,
                        known_state.as_ptr() as *const c_void,
                        known_state.len() as c_int,
                        None,
                    ) != SQLITE_OK as c_int;
                }
            } else {
                bind_err |= sqlite_bind_null(stmt, 9) != SQLITE_OK as c_int;
            }

            if let Some(ref payload) = op.payload {
                if payload.is_empty() {
                    bind_err |= sqlite_bind_null(stmt, 10) != SQLITE_OK as c_int;
                } else {
                    bind_err |= sqlite_bind_blob(
                        stmt,
                        10,
                        payload.as_ptr() as *const c_void,
                        payload.len() as c_int,
                        None,
                    ) != SQLITE_OK as c_int;
                }
            } else {
                bind_err |= sqlite_bind_null(stmt, 10) != SQLITE_OK as c_int;
            }
        }

        let op_ref = derive_op_ref_v0(doc_id, &op.replica, op.counter);
        unsafe {
            bind_err |= sqlite_bind_blob(
                stmt,
                11,
                op_ref.as_ptr() as *const c_void,
                OPREF_V0_WIDTH as c_int,
                None,
            ) != SQLITE_OK as c_int;
        }

        if bind_err {
            err_rc = SQLITE_ERROR as c_int;
            break;
        }

        let step_rc = unsafe { sqlite_step(stmt) };
        if step_rc != SQLITE_DONE as c_int {
            err_rc = step_rc;
            break;
        }

        // Check whether this row was inserted (vs ignored due to duplicate).
        let changed: i64 = sqlite_changes(db) as i64;
        if changed <= 0 {
            continue;
        }

        inserted += 1;
        if materialize_ok {
            let kind_parsed = match MaterializeKind::parse(op.kind.as_str()) {
                Some(k) => k,
                None => {
                    materialize_ok = false;
                    continue;
                }
            };
            if op.node.len() != 16 {
                materialize_ok = false;
                continue;
            }
            let mut node_arr = [0u8; 16];
            node_arr.copy_from_slice(&op.node);
            let node = NodeId(u128::from_be_bytes(node_arr));

            let to_node_id_opt = |bytes: &Option<Vec<u8>>| -> Option<NodeId> {
                let v = bytes.as_ref()?;
                if v.len() != 16 {
                    return None;
                }
                let mut out = [0u8; 16];
                out.copy_from_slice(v);
                Some(NodeId(u128::from_be_bytes(out)))
            };
            let parent_id = to_node_id_opt(&op.parent);
            let new_parent_id = to_node_id_opt(&op.new_parent);
            if kind_parsed == MaterializeKind::Insert && parent_id.is_none() {
                materialize_ok = false;
                continue;
            }
            if kind_parsed == MaterializeKind::Move && new_parent_id.is_none() {
                materialize_ok = false;
                continue;
            }
            if (kind_parsed == MaterializeKind::Insert || kind_parsed == MaterializeKind::Move)
                && op.order_key.is_none()
            {
                materialize_ok = false;
                continue;
            }

            let known_state = match op.known_state.as_ref() {
                Some(bytes) if !bytes.is_empty() => match deserialize_version_vector(bytes) {
                    Ok(vv) => Some(vv),
                    Err(_) => {
                        materialize_ok = false;
                        continue;
                    }
                },
                _ => None,
            };

            materialize_ops.push(MaterializeOp {
                replica: op.replica.clone(),
                counter: op.counter,
                lamport: op.lamport,
                kind: kind_parsed,
                parent: parent_id,
                node,
                new_parent: new_parent_id,
                order_key: op.order_key.clone().unwrap_or_default(),
                known_state,
                payload: op.payload.clone(),
            });
        }
    }

    unsafe { sqlite_finalize(stmt) };

    if err_rc != SQLITE_OK as c_int {
        sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
        return Err(err_rc);
    }

    if inserted > 0 {
        if materialize_ok {
            if materialize_ops_in_order(db, doc_id, &meta, &mut materialize_ops).is_err() {
                let _ = set_tree_meta_dirty(db, true);
            }
        } else {
            let _ = set_tree_meta_dirty(db, true);
        }
    }

    let commit_rc = sqlite_exec(db, commit.as_ptr(), None, null_mut(), null_mut());
    if commit_rc != SQLITE_OK as c_int {
        sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
        return Err(commit_rc);
    }

    Ok(inserted)
}

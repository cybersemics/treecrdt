use super::append::JsonAppendOp;
use super::node_store::SqliteNodeStore;
use super::op_index::SqliteParentOpIndex;
use super::payload_store::SqlitePayloadStore;
use super::util::sqlite_err_from_core;
use super::*;
use treecrdt_core::Storage;

fn parse_node_id(bytes: &[u8]) -> Result<NodeId, c_int> {
    if bytes.len() != 16 {
        return Err(SQLITE_ERROR as c_int);
    }
    let mut arr = [0u8; 16];
    arr.copy_from_slice(bytes);
    Ok(NodeId(u128::from_be_bytes(arr)))
}

fn parse_optional_node_id(bytes: &Option<Vec<u8>>) -> Result<Option<NodeId>, c_int> {
    match bytes {
        Some(v) => Ok(Some(parse_node_id(v)?)),
        None => Ok(None),
    }
}

fn json_append_op_to_operation(op: &JsonAppendOp) -> Result<treecrdt_core::Operation, c_int> {
    use treecrdt_core::{Operation, OperationId, OperationKind, OperationMetadata, ReplicaId};

    let node = parse_node_id(&op.node)?;
    let parent = parse_optional_node_id(&op.parent)?;
    let new_parent = parse_optional_node_id(&op.new_parent)?;

    let parsed_known_state = match op.known_state.as_ref() {
        Some(bytes) if !bytes.is_empty() => Some(deserialize_version_vector(bytes)?),
        _ => None,
    };

    let (kind, known_state) = match op.kind.as_str() {
        "insert" => (
            OperationKind::Insert {
                parent: parent.ok_or(SQLITE_ERROR as c_int)?,
                node,
                order_key: op.order_key.clone().ok_or(SQLITE_ERROR as c_int)?,
                payload: op.payload.clone(),
            },
            None,
        ),
        "move" => (
            OperationKind::Move {
                node,
                new_parent: new_parent.ok_or(SQLITE_ERROR as c_int)?,
                order_key: op.order_key.clone().ok_or(SQLITE_ERROR as c_int)?,
            },
            None,
        ),
        "delete" => (
            OperationKind::Delete { node },
            Some(parsed_known_state.ok_or(SQLITE_ERROR as c_int)?),
        ),
        "tombstone" => (OperationKind::Tombstone { node }, parsed_known_state),
        "payload" => (
            OperationKind::Payload {
                node,
                payload: op.payload.clone(),
            },
            None,
        ),
        _ => return Err(SQLITE_ERROR as c_int),
    };

    Ok(Operation {
        meta: OperationMetadata {
            id: OperationId {
                replica: ReplicaId(op.replica.clone()),
                counter: op.counter,
            },
            lamport: op.lamport,
            known_state,
        },
        kind,
    })
}

impl treecrdt_core::MaterializationCursor for TreeMeta {
    fn dirty(&self) -> bool {
        self.dirty
    }

    fn head_lamport(&self) -> Lamport {
        self.head_lamport
    }

    fn head_replica(&self) -> &[u8] {
        &self.head_replica
    }

    fn head_counter(&self) -> u64 {
        self.head_counter
    }

    fn head_seq(&self) -> u64 {
        self.head_seq
    }
}

fn materialize_ops_in_order(
    db: *mut sqlite3,
    doc_id: &[u8],
    meta: &TreeMeta,
    ops: &[treecrdt_core::Operation],
) -> Result<(), c_int> {
    if ops.is_empty() {
        return Ok(());
    }

    use treecrdt_core::{apply_incremental_ops, LamportClock, ReplicaId, TreeCrdt};
    let node_store = SqliteNodeStore::prepare(db).map_err(|_| SQLITE_ERROR as c_int)?;
    let payload_store = SqlitePayloadStore::prepare(db).map_err(|_| SQLITE_ERROR as c_int)?;
    let mut op_index =
        SqliteParentOpIndex::prepare(db, doc_id.to_vec()).map_err(|_| SQLITE_ERROR as c_int)?;
    let mut crdt = TreeCrdt::with_stores(
        ReplicaId::new(b"sqlite-ext"),
        NoopStorage,
        LamportClock::default(),
        node_store,
        payload_store,
    )
    .map_err(|_| SQLITE_ERROR as c_int)?;

    let next = apply_incremental_ops(&mut crdt, &mut op_index, meta, ops.to_vec())
        .map_err(|_| SQLITE_ERROR as c_int)?
        .ok_or(SQLITE_ERROR as c_int)?;
    update_tree_meta_head(db, next.lamport, &next.replica, next.counter, next.seq)?;
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
    let storage = super::op_storage::SqliteOpStorage::with_doc_id(db, doc_id.clone());
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

    let begin = CString::new(format!("SAVEPOINT {savepoint_name}")).expect("savepoint begin");
    let commit = CString::new(format!("RELEASE {savepoint_name}")).expect("savepoint commit");
    let rollback = CString::new(format!(
        "ROLLBACK TO {savepoint_name}; RELEASE {savepoint_name}"
    ))
    .expect("savepoint rollback");

    if sqlite_exec(db, begin.as_ptr(), None, null_mut(), null_mut()) != SQLITE_OK as c_int {
        return Err(SQLITE_ERROR as c_int);
    }

    let mut storage = super::op_storage::SqliteOpStorage::with_doc_id(db, doc_id.to_vec());
    let mut inserted: i64 = 0;
    let mut materialize_ops: Vec<treecrdt_core::Operation> = Vec::with_capacity(ops.len());

    for op in ops {
        let operation = match json_append_op_to_operation(op) {
            Ok(v) => v,
            Err(rc) => {
                sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
                return Err(rc);
            }
        };

        let inserted_now = match storage.apply(operation.clone()) {
            Ok(v) => v,
            Err(err) => {
                sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
                return Err(sqlite_err_from_core(err));
            }
        };
        if !inserted_now {
            continue;
        }

        inserted += 1;
        materialize_ops.push(operation);
    }

    if inserted > 0 {
        let _ = treecrdt_core::try_incremental_materialization(
            meta.dirty,
            || materialize_ops_in_order(db, doc_id, &meta, &materialize_ops[..]),
            || {
                let _ = set_tree_meta_dirty(db, true);
            },
        );
    }

    let commit_rc = sqlite_exec(db, commit.as_ptr(), None, null_mut(), null_mut());
    if commit_rc != SQLITE_OK as c_int {
        sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
        return Err(commit_rc);
    }

    Ok(inserted)
}

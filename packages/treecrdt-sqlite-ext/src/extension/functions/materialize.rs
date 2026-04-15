use super::append::JsonAppendOp;
use super::node_store::SqliteNodeStore;
use super::op_index::SqliteParentOpIndex;
use super::payload_store::SqlitePayloadStore;
use super::schema::set_tree_meta_replay_frontier;
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

    fn replay_lamport(&self) -> Option<Lamport> {
        self.replay_lamport
    }

    fn replay_replica(&self) -> Option<&[u8]> {
        self.replay_replica.as_deref()
    }

    fn replay_counter(&self) -> Option<u64> {
        self.replay_counter
    }
}

fn materialize_inserted_ops(
    db: *mut sqlite3,
    doc_id: &[u8],
    meta: &TreeMeta,
    ops: &[treecrdt_core::Operation],
) -> Result<treecrdt_core::IncrementalApplyResult, c_int> {
    use treecrdt_core::{
        materialize_persisted_remote_ops_with_delta, LamportClock, PersistedRemoteStores, ReplicaId,
    };

    materialize_persisted_remote_ops_with_delta(
        PersistedRemoteStores {
            // Scratch identity for the temporary TreeCrdt; replayed ops keep their own ids.
            replica_id: ReplicaId::new(b"sqlite-ext"),
            clock: LamportClock::default(),
            nodes: SqliteNodeStore::prepare(db).map_err(|_| SQLITE_ERROR as c_int)?,
            payloads: SqlitePayloadStore::prepare(db).map_err(|_| SQLITE_ERROR as c_int)?,
            index: SqliteParentOpIndex::prepare(db, doc_id.to_vec())
                .map_err(|_| SQLITE_ERROR as c_int)?,
        },
        meta,
        ops.to_vec(),
        |_, _| Ok(()),
        |_| Ok(()),
        |_| Ok(()),
    )
    .map_err(|_| SQLITE_ERROR as c_int)
}

pub(super) fn ensure_materialized(db: *mut sqlite3) -> Result<(), c_int> {
    let meta = load_tree_meta(db)?;
    if !meta.dirty && meta.replay_lamport.is_none() {
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
    use treecrdt_core::{catch_up_materialized_state, LamportClock, ReplicaId};
    let storage = super::op_storage::SqliteOpStorage::with_doc_id(db, doc_id.clone());
    let meta = match load_tree_meta(db) {
        Ok(meta) => meta,
        Err(rc) => {
            sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
            return Err(rc);
        }
    };
    let nodes = match SqliteNodeStore::prepare(db) {
        Ok(store) => store,
        Err(_) => {
            sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
            return Err(SQLITE_ERROR as c_int);
        }
    };
    let payloads = match SqlitePayloadStore::prepare(db) {
        Ok(store) => store,
        Err(_) => {
            sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
            return Err(SQLITE_ERROR as c_int);
        }
    };
    let index = match SqliteParentOpIndex::prepare(db, doc_id.clone()) {
        Ok(index) => index,
        Err(_) => {
            sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
            return Err(SQLITE_ERROR as c_int);
        }
    };
    let head = match catch_up_materialized_state(
        storage,
        treecrdt_core::PersistedRemoteStores {
            replica_id: ReplicaId::new(b"sqlite-ext"),
            clock: LamportClock::default(),
            nodes,
            payloads,
            index,
        },
        &meta,
        |_| Ok(()),
        |_| Ok(()),
    ) {
        Ok(v) => v,
        Err(_) => {
            sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
            return Err(SQLITE_ERROR as c_int);
        }
    };

    if let Some(last) = head {
        let head_rc =
            update_tree_meta_head(db, last.lamport, &last.replica, last.counter, last.seq);
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
) -> Result<Vec<NodeId>, c_int> {
    if ops.is_empty() {
        return Ok(Vec::new());
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
    let mut inserted_ops: Vec<treecrdt_core::Operation> = Vec::with_capacity(ops.len());

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
        if inserted_now {
            inserted_ops.push(operation);
        }
    }

    let apply_result = treecrdt_core::apply_persisted_remote_ops_with_delta(
        &meta,
        inserted_ops,
        |inserted| materialize_inserted_ops(db, doc_id, &meta, &inserted),
        |head| update_tree_meta_head(db, head.lamport, &head.replica, head.counter, head.seq),
        |frontier| set_tree_meta_replay_frontier(db, frontier),
        || set_tree_meta_dirty(db, true),
    );

    let commit_rc = sqlite_exec(db, commit.as_ptr(), None, null_mut(), null_mut());
    if commit_rc != SQLITE_OK as c_int {
        sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
        return Err(commit_rc);
    }

    Ok(apply_result.affected_nodes)
}

use super::append::JsonAppendOp;
use super::node_store::SqliteNodeStore;
use super::op_index::SqliteParentOpIndex;
use super::payload_store::SqlitePayloadStore;
use super::schema::set_tree_meta_replay_frontier;
use super::util::{sqlite_err_from_core, sqlite_result_json};
use super::*;
use treecrdt_core::Storage;
use treecrdt_core::{
    orchestrate_persisted_remote_append, LamportClock, MaterializationChange,
    MaterializationCursor, MaterializationOutcome, MaterializationSource, OperationId, ReplicaId,
};

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct JsonMaterializationOutcome {
    head_seq: u64,
    changes: Vec<JsonMaterializationChange>,
}

#[derive(serde::Serialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
enum JsonMaterializationChange {
    Insert {
        node: String,
        parent_after: String,
        payload: Option<Vec<u8>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        source: Option<JsonMaterializationSource>,
    },
    Move {
        node: String,
        parent_before: Option<String>,
        parent_after: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        source: Option<JsonMaterializationSource>,
    },
    Delete {
        node: String,
        parent_before: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        source: Option<JsonMaterializationSource>,
    },
    Restore {
        node: String,
        parent_after: Option<String>,
        payload: Option<Vec<u8>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        source: Option<JsonMaterializationSource>,
    },
    Payload {
        node: String,
        payload: Option<Vec<u8>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        source: Option<JsonMaterializationSource>,
    },
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct JsonMaterializationSource {
    operation: JsonMaterializationSourceOperation,
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct JsonMaterializationSourceOperation {
    id: JsonOperationId,
    lamport: u64,
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct JsonOperationId {
    replica: Vec<u8>,
    counter: u64,
}

fn json_operation_id(id: &OperationId) -> JsonOperationId {
    JsonOperationId {
        replica: id.replica.as_bytes().to_vec(),
        counter: id.counter,
    }
}

fn node_hex(node: NodeId) -> String {
    format!("{:032x}", node.0)
}

fn json_source(source: &Option<MaterializationSource>) -> Option<JsonMaterializationSource> {
    source.as_ref().map(|source| JsonMaterializationSource {
        operation: JsonMaterializationSourceOperation {
            id: json_operation_id(&source.operation.id),
            lamport: source.operation.lamport,
        },
    })
}

pub(super) fn json_outcome_from_core(
    outcome: &MaterializationOutcome,
) -> JsonMaterializationOutcome {
    let changes = outcome
        .changes
        .iter()
        .map(|change| match change {
            MaterializationChange::Insert {
                node,
                parent_after,
                payload,
                source,
            } => JsonMaterializationChange::Insert {
                node: node_hex(*node),
                parent_after: node_hex(*parent_after),
                payload: payload.clone(),
                source: json_source(source),
            },
            MaterializationChange::Move {
                node,
                parent_before,
                parent_after,
                source,
            } => JsonMaterializationChange::Move {
                node: node_hex(*node),
                parent_before: parent_before.map(node_hex),
                parent_after: node_hex(*parent_after),
                source: json_source(source),
            },
            MaterializationChange::Delete {
                node,
                parent_before,
                source,
            } => JsonMaterializationChange::Delete {
                node: node_hex(*node),
                parent_before: parent_before.map(node_hex),
                source: json_source(source),
            },
            MaterializationChange::Restore {
                node,
                parent_after,
                payload,
                source,
            } => JsonMaterializationChange::Restore {
                node: node_hex(*node),
                parent_after: parent_after.map(node_hex),
                payload: payload.clone(),
                source: json_source(source),
            },
            MaterializationChange::Payload {
                node,
                payload,
                source,
            } => JsonMaterializationChange::Payload {
                node: node_hex(*node),
                payload: payload.clone(),
                source: json_source(source),
            },
        })
        .collect();
    JsonMaterializationOutcome {
        head_seq: outcome.head_seq,
        changes,
    }
}

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

fn with_materialization_savepoint(
    db: *mut sqlite3,
    run: impl FnOnce() -> Result<treecrdt_core::IncrementalApplyResult, c_int>,
) -> Result<treecrdt_core::IncrementalApplyResult, c_int> {
    const BEGIN: &[u8] = b"SAVEPOINT treecrdt_incremental_materialization\0";
    const ROLLBACK: &[u8] = b"ROLLBACK TO treecrdt_incremental_materialization\0";

    let rc = sqlite_exec(db, BEGIN.as_ptr().cast(), None, null_mut(), null_mut());
    if rc != SQLITE_OK as c_int {
        return Err(rc);
    }
    let result = run();
    if !matches!(&result, Ok(result) if result.head.is_some()) {
        let rc = sqlite_exec(db, ROLLBACK.as_ptr().cast(), None, null_mut(), null_mut());
        if rc != SQLITE_OK as c_int {
            return Err(rc);
        }
    }
    // Releasing the outer append savepoint also releases this nested one.
    result
}

fn materialize_inserted_ops(
    db: *mut sqlite3,
    doc_id: &[u8],
    meta: &dyn MaterializationCursor,
    ops: Vec<treecrdt_core::Operation>,
) -> Result<treecrdt_core::IncrementalApplyResult, c_int> {
    use treecrdt_core::{
        materialize_persisted_remote_ops_with_delta, LamportClock, PersistedRemoteStores, ReplicaId,
    };

    with_materialization_savepoint(db, || {
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
            &meta,
            ops,
            |_, _| Ok(()),
            |_| Ok(()),
            |_| Ok(()),
        )
        .map_err(|_| SQLITE_ERROR as c_int)
    })
}

pub(super) fn ensure_materialized(db: *mut sqlite3) -> Result<MaterializationOutcome, c_int> {
    let meta = load_tree_meta(db)?;
    if meta.state().replay_from.is_none() {
        return Ok(MaterializationOutcome::empty(meta.state().head_seq()));
    }
    catch_up_materialized_from_frontier(db)
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
        Ok(outcome) => sqlite_result_json(ctx, &json_outcome_from_core(&outcome)),
        Err(rc) => sqlite_result_error_code(ctx, rc),
    }
}

fn catch_up_materialized_from_frontier(db: *mut sqlite3) -> Result<MaterializationOutcome, c_int> {
    let begin = CString::new("SAVEPOINT treecrdt_materialize").expect("static");
    let commit = CString::new("RELEASE treecrdt_materialize").expect("static");
    let rollback = CString::new("ROLLBACK TO treecrdt_materialize; RELEASE treecrdt_materialize")
        .expect("static");

    if sqlite_exec(db, begin.as_ptr(), None, null_mut(), null_mut()) != SQLITE_OK as c_int {
        return Err(SQLITE_ERROR as c_int);
    }

    let doc_id = load_doc_id(db).unwrap_or(None).unwrap_or_default();

    // Catch materialized state up from the pending frontier by replaying the op-log through core
    // semantics.
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
    let catch_up = match catch_up_materialized_state(
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

    let head_rc = update_tree_meta_head(db, catch_up.head.as_ref());
    if head_rc.is_err() {
        sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
        return Err(head_rc.err().unwrap_or(SQLITE_ERROR as c_int));
    }

    let commit_rc = sqlite_exec(db, commit.as_ptr(), None, null_mut(), null_mut());
    if commit_rc != SQLITE_OK as c_int {
        sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
        return Err(commit_rc);
    }
    Ok(catch_up.outcome)
}

pub(super) fn append_ops_impl(
    db: *mut sqlite3,
    doc_id: &[u8],
    savepoint_name: &str,
    ops: &[JsonAppendOp],
) -> Result<MaterializationOutcome, c_int> {
    if ops.is_empty() {
        let meta = load_tree_meta(db)?;
        return Ok(MaterializationOutcome::empty(meta.state().head_seq()));
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

    let append_result = (|| {
        let mut storage = super::op_storage::SqliteOpStorage::with_doc_id(db, doc_id.to_vec());
        let mut inserted_ops: Vec<treecrdt_core::Operation> = Vec::with_capacity(ops.len());

        for op in ops {
            let operation = json_append_op_to_operation(op)?;
            if storage.apply(operation.clone()).map_err(sqlite_err_from_core)? {
                inserted_ops.push(operation);
            }
        }

        // INSERT OR IGNORE starts SQLite's write transaction even for duplicate-only batches.
        // Read the materialization cursor only after that serialization point so it cannot become
        // stale while this append waits for another connection's writer to commit.
        let meta = load_tree_meta(db)?;

        orchestrate_persisted_remote_append(
            &meta,
            inserted_ops,
            |meta, inserted| materialize_inserted_ops(db, doc_id, meta, inserted),
            |head| update_tree_meta_head(db, Some(head)),
            |frontier| set_tree_meta_replay_frontier(db, frontier),
            || Ok(load_tree_meta(db)?.0),
            |meta| {
                treecrdt_core::catch_up_materialized_state(
                    super::op_storage::SqliteOpStorage::with_doc_id(db, doc_id.to_vec()),
                    treecrdt_core::PersistedRemoteStores {
                        replica_id: ReplicaId::new(b"sqlite-ext"),
                        clock: LamportClock::default(),
                        nodes: SqliteNodeStore::prepare(db).map_err(|_| SQLITE_ERROR as c_int)?,
                        payloads: SqlitePayloadStore::prepare(db)
                            .map_err(|_| SQLITE_ERROR as c_int)?,
                        index: SqliteParentOpIndex::prepare(db, doc_id.to_vec())
                            .map_err(|_| SQLITE_ERROR as c_int)?,
                    },
                    &meta,
                    |_| Ok(()),
                    |_| Ok(()),
                )
                .map_err(|_| SQLITE_ERROR as c_int)
            },
            |_| SQLITE_ERROR as c_int,
        )
    })();

    let apply_result = match append_result {
        Ok(result) => result,
        Err(rc) => {
            sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
            return Err(rc);
        }
    };

    let commit_rc = sqlite_exec(db, commit.as_ptr(), None, null_mut(), null_mut());
    if commit_rc != SQLITE_OK as c_int {
        sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
        return Err(commit_rc);
    }

    Ok(apply_result.outcome)
}

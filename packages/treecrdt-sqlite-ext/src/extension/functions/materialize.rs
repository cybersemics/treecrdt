use super::append::JsonAppendOp;
use super::node_store::SqliteNodeStore;
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
    position: usize,
    known_state: Option<VersionVector>,
    payload: Option<Vec<u8>>,
    op_ref: [u8; OPREF_V0_WIDTH],
}

fn materialize_ops_in_order(
    db: *mut sqlite3,
    doc_id: &[u8],
    meta: &TreeMeta,
    ops: &mut [MaterializeOp],
) -> Result<(), c_int> {
    struct AutoStmt(*mut sqlite3_stmt);
    impl Drop for AutoStmt {
        fn drop(&mut self) {
            if !self.0.is_null() {
                unsafe { sqlite_finalize(self.0) };
            }
        }
    }

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
    let mut crdt = TreeCrdt::with_stores(
        ReplicaId::new(b"sqlite-ext"),
        NoopStorage::default(),
        LamportClock::default(),
        node_store,
        payload_store,
    );

    let parent_sql =
        CString::new("SELECT parent, deleted_at FROM tree_nodes WHERE node = ?1 LIMIT 1")
            .expect("parent lookup sql");
    let mut parent_stmt: *mut sqlite3_stmt = null_mut();
    let parent_rc = sqlite_prepare_v2(db, parent_sql.as_ptr(), -1, &mut parent_stmt, null_mut());
    if parent_rc != SQLITE_OK as c_int {
        return Err(parent_rc);
    }
    let parent_stmt = AutoStmt(parent_stmt);

    let insert_opref_sql = CString::new(
        "INSERT OR IGNORE INTO oprefs_children(parent, op_ref, seq) VALUES (?1, ?2, ?3)",
    )
    .expect("insert oprefs_children sql");
    let mut opref_stmt: *mut sqlite3_stmt = null_mut();
    let opref_rc = sqlite_prepare_v2(
        db,
        insert_opref_sql.as_ptr(),
        -1,
        &mut opref_stmt,
        null_mut(),
    );
    if opref_rc != SQLITE_OK as c_int {
        return Err(opref_rc);
    }
    let opref_stmt = AutoStmt(opref_stmt);

    let update_tombstone_sql = CString::new("UPDATE tree_nodes SET tombstone = ?2 WHERE node = ?1")
        .expect("update tombstone sql");
    let mut tombstone_stmt: *mut sqlite3_stmt = null_mut();
    let tombstone_rc = sqlite_prepare_v2(
        db,
        update_tombstone_sql.as_ptr(),
        -1,
        &mut tombstone_stmt,
        null_mut(),
    );
    if tombstone_rc != SQLITE_OK as c_int {
        return Err(tombstone_rc);
    }
    let tombstone_stmt = AutoStmt(tombstone_stmt);

    let mut seq = meta.head_seq;
    let mut tombstone_starts: HashSet<NodeId> = HashSet::new();

    for op in ops.iter() {
        let op_kind = match op.kind {
            MaterializeKind::Insert => {
                let parent = op.parent.ok_or(SQLITE_ERROR as c_int)?;
                OperationKind::Insert {
                    parent,
                    node: op.node,
                    position: op.position,
                    payload: op.payload.clone(),
                }
            }
            MaterializeKind::Move => {
                let new_parent = op.new_parent.ok_or(SQLITE_ERROR as c_int)?;
                OperationKind::Move {
                    node: op.node,
                    new_parent,
                    position: op.position,
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

        let delta = match crdt.apply_remote_with_delta(operation) {
            Ok(Some(delta)) => delta,
            Ok(None) => continue,
            Err(_) => return Err(SQLITE_ERROR as c_int),
        };

        seq += 1;

        let insert_opref = |parent: NodeId, op_ref: &[u8; OPREF_V0_WIDTH]| -> Result<(), c_int> {
            if parent == NodeId::TRASH {
                return Ok(());
            }
            let parent_bytes = parent.0.to_be_bytes();
            unsafe {
                sqlite_clear_bindings(opref_stmt.0);
                sqlite_reset(opref_stmt.0);
            }
            let mut bind_err = false;
            unsafe {
                bind_err |= sqlite_bind_blob(
                    opref_stmt.0,
                    1,
                    parent_bytes.as_ptr() as *const c_void,
                    parent_bytes.len() as c_int,
                    None,
                ) != SQLITE_OK as c_int;
                bind_err |= sqlite_bind_blob(
                    opref_stmt.0,
                    2,
                    op_ref.as_ptr() as *const c_void,
                    op_ref.len() as c_int,
                    None,
                ) != SQLITE_OK as c_int;
                bind_err |= sqlite_bind_int64(opref_stmt.0, 3, seq as i64) != SQLITE_OK as c_int;
            }
            if bind_err {
                return Err(SQLITE_ERROR as c_int);
            }
            let step_rc = unsafe { sqlite_step(opref_stmt.0) };
            if step_rc != SQLITE_DONE as c_int {
                return Err(step_rc);
            }
            Ok(())
        };

        for parent in delta.affected_parents {
            insert_opref(parent, &op.op_ref)?;
            tombstone_starts.insert(parent);
        }

        // If the node just entered a new parent, ensure the latest payload opRef is discoverable
        // under this parent so late-join subscribers can render current payload without full sync.
        let old_parent = delta.snapshot.parent;
        let next_parent: Option<NodeId> = match op.kind {
            MaterializeKind::Insert => op.parent,
            MaterializeKind::Move => op.new_parent,
            MaterializeKind::Delete | MaterializeKind::Tombstone | MaterializeKind::Payload => {
                old_parent
            }
        };
        if let Some(next_parent) = next_parent {
            if next_parent != NodeId::TRASH && Some(next_parent) != old_parent {
                let writer = crdt
                    .payload_last_writer(op.node)
                    .map_err(|_| SQLITE_ERROR as c_int)?;
                if let Some((_lamport, id)) = writer {
                    let payload_op_ref =
                        derive_op_ref_v0(doc_id, id.replica.as_bytes(), id.counter);
                    insert_opref(next_parent, &payload_op_ref)?;
                }
            }
        }

        tombstone_starts.insert(op.node);
    }

    // Refresh tombstones for affected nodes + ancestors (only for nodes with deleted_at).
    {
        let mut stack: Vec<NodeId> = tombstone_starts.into_iter().collect();
        let mut visited: HashSet<NodeId> = HashSet::new();
        while let Some(node) = stack.pop() {
            if node == NodeId::TRASH {
                continue;
            }
            if !visited.insert(node) {
                continue;
            }

            let node_bytes = node.0.to_be_bytes();
            let (parent, has_deleted_at): (Option<NodeId>, bool) = unsafe {
                sqlite_clear_bindings(parent_stmt.0);
                sqlite_reset(parent_stmt.0);
                let bind_rc = sqlite_bind_blob(
                    parent_stmt.0,
                    1,
                    node_bytes.as_ptr() as *const c_void,
                    node_bytes.len() as c_int,
                    None,
                );
                if bind_rc != SQLITE_OK as c_int {
                    sqlite_reset(parent_stmt.0);
                    Err(bind_rc)
                } else {
                    let step_rc = sqlite_step(parent_stmt.0);
                    let out = if step_rc == SQLITE_ROW as c_int {
                        let parent = match column_blob16(parent_stmt.0, 0)? {
                            Some(bytes) => Some(NodeId(u128::from_be_bytes(bytes))),
                            None => None,
                        };
                        let has_deleted_at = sqlite_column_type(parent_stmt.0, 1)
                            != SQLITE_NULL as c_int
                            && sqlite_column_bytes(parent_stmt.0, 1) > 0;
                        (parent, has_deleted_at)
                    } else if step_rc == SQLITE_DONE as c_int {
                        (None, false)
                    } else {
                        sqlite_reset(parent_stmt.0);
                        return Err(step_rc);
                    };
                    sqlite_reset(parent_stmt.0);
                    Ok(out)
                }
            }?;

            if has_deleted_at {
                let tombstoned = crdt.is_tombstoned(node).map_err(|_| SQLITE_ERROR as c_int)?;
                unsafe {
                    sqlite_clear_bindings(tombstone_stmt.0);
                    sqlite_reset(tombstone_stmt.0);
                }
                let mut bind_err = false;
                unsafe {
                    bind_err |= sqlite_bind_blob(
                        tombstone_stmt.0,
                        1,
                        node_bytes.as_ptr() as *const c_void,
                        node_bytes.len() as c_int,
                        None,
                    ) != SQLITE_OK as c_int;
                    bind_err |=
                        sqlite_bind_int64(tombstone_stmt.0, 2, if tombstoned { 1 } else { 0 })
                            != SQLITE_OK as c_int;
                }
                if bind_err {
                    return Err(SQLITE_ERROR as c_int);
                }
                let step_rc = unsafe { sqlite_step(tombstone_stmt.0) };
                if step_rc != SQLITE_DONE as c_int {
                    return Err(step_rc);
                }
            }

            if let Some(parent) = parent {
                stack.push(parent);
            }
        }
    }

    let last = ops.last().expect("ops non-empty");
    update_tree_meta_head(db, last.lamport, &last.replica, last.counter, seq)?;
    Ok(())
}

pub(super) fn ensure_materialized(db: *mut sqlite3) -> Result<(), c_int> {
    let meta = load_tree_meta(db)?;
    if !meta.dirty {
        return Ok(());
    }
    rebuild_materialized(db)
}

fn rebuild_materialized(db: *mut sqlite3) -> Result<(), c_int> {
    let begin = CString::new("SAVEPOINT treecrdt_materialize").expect("static");
    let commit = CString::new("RELEASE treecrdt_materialize").expect("static");
    let rollback = CString::new("ROLLBACK TO treecrdt_materialize; RELEASE treecrdt_materialize")
        .expect("static");

    if sqlite_exec(db, begin.as_ptr(), None, null_mut(), null_mut()) != SQLITE_OK as c_int {
        return Err(SQLITE_ERROR as c_int);
    }

    let clear_sql = CString::new(
        "DELETE FROM oprefs_children; \
         DELETE FROM tree_payload; \
         UPDATE tree_meta SET dirty = 0, head_lamport = 0, head_replica = X'', head_counter = 0, head_seq = 0 WHERE id = 1;",
    )
    .expect("clear materialized sql");
    let clear_rc = sqlite_exec(db, clear_sql.as_ptr(), None, null_mut(), null_mut());
    if clear_rc != SQLITE_OK as c_int {
        sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
        return Err(clear_rc);
    }

    let doc_id = load_doc_id(db).unwrap_or(None).unwrap_or_default();

    // Rebuild materialized state by replaying the op-log through core semantics.
    use treecrdt_core::{LamportClock, OperationKind, ReplicaId, TreeCrdt};
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
    let mut crdt = TreeCrdt::with_stores(
        ReplicaId::new(b"sqlite-ext"),
        storage,
        LamportClock::default(),
        node_store,
        payload_store,
    );
    if crdt.replay_from_storage().is_err() {
        sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
        return Err(SQLITE_ERROR as c_int);
    }

    let log = crdt.export_log();

    // Refresh tombstone materialization using core semantics.
    {
        let select_sql =
            CString::new("SELECT node FROM tree_nodes").expect("select tree node ids sql");
        let mut select_stmt: *mut sqlite3_stmt = null_mut();
        let rc = sqlite_prepare_v2(db, select_sql.as_ptr(), -1, &mut select_stmt, null_mut());
        if rc != SQLITE_OK as c_int {
            sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
            return Err(rc);
        }

        let mut node_ids: Vec<NodeId> = Vec::new();
        loop {
            let step_rc = unsafe { sqlite_step(select_stmt) };
            if step_rc == SQLITE_ROW as c_int {
                let bytes = match unsafe { column_blob16(select_stmt, 0) } {
                    Ok(Some(bytes)) => bytes,
                    Ok(None) => continue,
                    Err(rc) => {
                        unsafe { sqlite_finalize(select_stmt) };
                        sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
                        return Err(rc);
                    }
                };
                node_ids.push(NodeId(u128::from_be_bytes(bytes)));
            } else if step_rc == SQLITE_DONE as c_int {
                break;
            } else {
                unsafe { sqlite_finalize(select_stmt) };
                sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
                return Err(step_rc);
            }
        }
        let finalize_rc = unsafe { sqlite_finalize(select_stmt) };
        if finalize_rc != SQLITE_OK as c_int {
            sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
            return Err(finalize_rc);
        }

        let update_sql = CString::new("UPDATE tree_nodes SET tombstone = ?2 WHERE node = ?1")
            .expect("update tombstone sql");
        let mut update_stmt: *mut sqlite3_stmt = null_mut();
        let rc = sqlite_prepare_v2(db, update_sql.as_ptr(), -1, &mut update_stmt, null_mut());
        if rc != SQLITE_OK as c_int {
            sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
            return Err(rc);
        }

        for node in node_ids {
            if node == NodeId::TRASH {
                continue;
            }
            let tombstoned = match crdt.is_tombstoned(node) {
                Ok(v) => v,
                Err(_) => {
                    unsafe { sqlite_finalize(update_stmt) };
                    sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
                    return Err(SQLITE_ERROR as c_int);
                }
            };
            let node_bytes = node.0.to_be_bytes();
            unsafe {
                sqlite_clear_bindings(update_stmt);
                sqlite_reset(update_stmt);
            }
            let mut bind_err = false;
            unsafe {
                bind_err |= sqlite_bind_blob(
                    update_stmt,
                    1,
                    node_bytes.as_ptr() as *const c_void,
                    node_bytes.len() as c_int,
                    None,
                ) != SQLITE_OK as c_int;
                bind_err |= sqlite_bind_int64(update_stmt, 2, if tombstoned { 1 } else { 0 })
                    != SQLITE_OK as c_int;
            }
            if bind_err {
                unsafe { sqlite_finalize(update_stmt) };
                sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
                return Err(SQLITE_ERROR as c_int);
            }

            let step_rc = unsafe { sqlite_step(update_stmt) };
            if step_rc != SQLITE_DONE as c_int {
                unsafe { sqlite_finalize(update_stmt) };
                sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
                return Err(step_rc);
            }
        }

        unsafe { sqlite_finalize(update_stmt) };
    }

    // Write oprefs_children.
    {
        let sql = CString::new(
            "INSERT OR IGNORE INTO oprefs_children(parent, op_ref, seq) VALUES (?1, ?2, ?3)",
        )
        .expect("insert oprefs_children sql");
        let mut stmt: *mut sqlite3_stmt = null_mut();
        let rc = sqlite_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, null_mut());
        if rc != SQLITE_OK as c_int {
            sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
            return Err(rc);
        }

        for (idx, entry) in log.iter().enumerate() {
            let seq = (idx as u64) + 1;
            let replica = entry.op.meta.id.replica.as_bytes();
            let counter = entry.op.meta.id.counter;
            let op_ref = derive_op_ref_v0(&doc_id, replica, counter);

            let old_parent = entry.snapshot.parent;
            let next_parent: Option<NodeId> = match &entry.op.kind {
                OperationKind::Insert { parent, .. } => Some(*parent),
                OperationKind::Move { new_parent, .. } => Some(*new_parent),
                OperationKind::Delete { .. } | OperationKind::Tombstone { .. } => old_parent,
                OperationKind::Payload { .. } => old_parent,
            };

            let insert_parent = |parent: NodeId| -> Result<(), c_int> {
                if parent == NodeId::TRASH {
                    return Ok(());
                }
                unsafe {
                    sqlite_clear_bindings(stmt);
                    sqlite_reset(stmt);
                }
                let parent_bytes = parent.0.to_be_bytes();
                let mut bind_err = false;
                unsafe {
                    bind_err |= sqlite_bind_blob(
                        stmt,
                        1,
                        parent_bytes.as_ptr() as *const c_void,
                        parent_bytes.len() as c_int,
                        None,
                    ) != SQLITE_OK as c_int;
                    bind_err |= sqlite_bind_blob(
                        stmt,
                        2,
                        op_ref.as_ptr() as *const c_void,
                        op_ref.len() as c_int,
                        None,
                    ) != SQLITE_OK as c_int;
                    bind_err |= sqlite_bind_int64(stmt, 3, seq as i64) != SQLITE_OK as c_int;
                }
                if bind_err {
                    unsafe { sqlite_finalize(stmt) };
                    sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
                    return Err(SQLITE_ERROR as c_int);
                }
                let step_rc = unsafe { sqlite_step(stmt) };
                if step_rc != SQLITE_DONE as c_int {
                    unsafe { sqlite_finalize(stmt) };
                    sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
                    return Err(step_rc);
                }
                Ok(())
            };

            if let Some(parent) = old_parent {
                insert_parent(parent)?;
            }
            if let Some(parent) = next_parent {
                if Some(parent) != old_parent {
                    insert_parent(parent)?;
                }
            }
        }

        // Ensure the latest payload op for each node is discoverable under its current parent.
        // This mirrors the incremental path that backfills payload visibility on parent changes.
        let payload_seq: u64 = (log.len() as u64).max(1);
        let nodes = match crdt.export_nodes() {
            Ok(v) => v,
            Err(_) => {
                unsafe { sqlite_finalize(stmt) };
                sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
                return Err(SQLITE_ERROR as c_int);
            }
        };
        for node in nodes {
            if node.node == NodeId::ROOT || node.node == NodeId::TRASH {
                continue;
            }
            let Some(parent) = node.parent else {
                continue;
            };
            if parent == NodeId::TRASH {
                continue;
            }
            let writer = match crdt.payload_last_writer(node.node) {
                Ok(v) => v,
                Err(_) => {
                    unsafe { sqlite_finalize(stmt) };
                    sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
                    return Err(SQLITE_ERROR as c_int);
                }
            };
            let Some((_lamport, id)) = writer else {
                continue;
            };
            let payload_op_ref = derive_op_ref_v0(&doc_id, id.replica.as_bytes(), id.counter);

            unsafe {
                sqlite_clear_bindings(stmt);
                sqlite_reset(stmt);
            }
            let parent_bytes = parent.0.to_be_bytes();
            let mut bind_err = false;
            unsafe {
                bind_err |= sqlite_bind_blob(
                    stmt,
                    1,
                    parent_bytes.as_ptr() as *const c_void,
                    parent_bytes.len() as c_int,
                    None,
                ) != SQLITE_OK as c_int;
                bind_err |= sqlite_bind_blob(
                    stmt,
                    2,
                    payload_op_ref.as_ptr() as *const c_void,
                    payload_op_ref.len() as c_int,
                    None,
                ) != SQLITE_OK as c_int;
                bind_err |= sqlite_bind_int64(stmt, 3, payload_seq as i64) != SQLITE_OK as c_int;
            }
            if bind_err {
                unsafe { sqlite_finalize(stmt) };
                sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
                return Err(SQLITE_ERROR as c_int);
            }
            let step_rc = unsafe { sqlite_step(stmt) };
            if step_rc != SQLITE_DONE as c_int {
                unsafe { sqlite_finalize(stmt) };
                sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
                return Err(step_rc);
            }
        }

        unsafe { sqlite_finalize(stmt) };
    }

    // Update meta head + seq.
    if let Some(last) = log.last() {
        let head_rc = update_tree_meta_head(
            db,
            last.op.meta.lamport,
            last.op.meta.id.replica.as_bytes(),
            last.op.meta.id.counter,
            log.len() as u64,
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
        "INSERT OR IGNORE INTO ops (replica,counter,lamport,kind,parent,node,new_parent,position,known_state,payload,op_ref) \
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",
    )
    .expect("insert ops sql");
    let mut stmt: *mut sqlite3_stmt = null_mut();
    let prep_rc = sqlite_prepare_v2(db, insert_sql.as_ptr(), -1, &mut stmt, null_mut());
    if prep_rc != SQLITE_OK as c_int {
        sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
        return Err(prep_rc);
    }

    let changes_sql = CString::new("SELECT changes()").expect("changes sql");
    let mut changes_stmt: *mut sqlite3_stmt = null_mut();
    let changes_rc = sqlite_prepare_v2(db, changes_sql.as_ptr(), -1, &mut changes_stmt, null_mut());
    if changes_rc != SQLITE_OK as c_int {
        unsafe { sqlite_finalize(stmt) };
        sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
        return Err(changes_rc);
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
            if let Some(pos) = op.position {
                bind_err |= sqlite_bind_int64(stmt, 8, (pos.min(i64::MAX as u64)) as i64)
                    != SQLITE_OK as c_int;
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
        let mut changed: i64 = 0;
        unsafe {
            sqlite_reset(changes_stmt);
            let rc = sqlite_step(changes_stmt);
            if rc == SQLITE_ROW as c_int {
                changed = sqlite_column_int64(changes_stmt, 0);
            }
            sqlite_reset(changes_stmt);
        }
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
                position: op.position.unwrap_or(0).min(usize::MAX as u64) as usize,
                known_state,
                payload: op.payload.clone(),
                op_ref,
            });
        }
    }

    unsafe { sqlite_finalize(stmt) };
    unsafe { sqlite_finalize(changes_stmt) };

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

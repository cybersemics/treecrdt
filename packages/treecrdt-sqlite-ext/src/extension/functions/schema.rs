use super::sqlite_api::*;

use std::ffi::CString;
use std::os::raw::{c_int, c_void};
use std::ptr::null_mut;
use std::slice;

use treecrdt_core::{
    should_checkpoint_materialization, Lamport, MaterializationCursor, MaterializationHead,
    MaterializationKey, MaterializationState,
};

pub(super) const ROOT_NODE_ID: [u8; 16] = [0u8; 16];

#[derive(Clone, Debug)]
pub(super) struct TreeMeta(MaterializationState);

impl MaterializationCursor for TreeMeta {
    fn state(&self) -> MaterializationState<&[u8]> {
        self.0.as_borrowed()
    }
}

pub(super) fn load_doc_id(db: *mut sqlite3) -> Result<Option<Vec<u8>>, c_int> {
    let sql =
        CString::new("SELECT value FROM meta WHERE key = 'doc_id' LIMIT 1").expect("doc id sql");
    let mut stmt: *mut sqlite3_stmt = null_mut();
    let rc = sqlite_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, null_mut());
    if rc != SQLITE_OK as c_int {
        return Err(rc);
    }

    let step_rc = unsafe { sqlite_step(stmt) };
    if step_rc == SQLITE_ROW as c_int {
        let ptr = unsafe { sqlite_column_text(stmt, 0) } as *const u8;
        let len = unsafe { sqlite_column_bytes(stmt, 0) } as usize;
        let value = if ptr.is_null() || len == 0 {
            Vec::new()
        } else {
            unsafe { slice::from_raw_parts(ptr, len) }.to_vec()
        };
        let finalize_rc = unsafe { sqlite_finalize(stmt) };
        if finalize_rc != SQLITE_OK as c_int {
            return Err(finalize_rc);
        }
        Ok(Some(value))
    } else if step_rc == SQLITE_DONE as c_int {
        let finalize_rc = unsafe { sqlite_finalize(stmt) };
        if finalize_rc != SQLITE_OK as c_int {
            return Err(finalize_rc);
        }
        Ok(None)
    } else {
        unsafe { sqlite_finalize(stmt) };
        Err(step_rc)
    }
}

pub(super) fn load_tree_meta(db: *mut sqlite3) -> Result<TreeMeta, c_int> {
    let sql = CString::new(
        "SELECT head_lamport, head_replica, head_counter, head_seq, \
                replay_lamport, replay_replica, replay_counter \
         FROM tree_meta WHERE id = 1 LIMIT 1",
    )
    .expect("tree meta sql");
    let mut stmt: *mut sqlite3_stmt = null_mut();
    let rc = sqlite_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, null_mut());
    if rc != SQLITE_OK as c_int {
        return Err(rc);
    }

    let step_rc = unsafe { sqlite_step(stmt) };
    if step_rc != SQLITE_ROW as c_int {
        unsafe { sqlite_finalize(stmt) };
        return Err(SQLITE_ERROR as c_int);
    }

    let head_lamport = unsafe { sqlite_column_int64(stmt, 0) } as Lamport;
    let rep_ptr = unsafe { sqlite_column_blob(stmt, 1) } as *const u8;
    let rep_len = unsafe { sqlite_column_bytes(stmt, 1) } as usize;
    let head_replica = if rep_ptr.is_null() || rep_len == 0 {
        Vec::new()
    } else {
        unsafe { slice::from_raw_parts(rep_ptr, rep_len) }.to_vec()
    };
    let head_counter = unsafe { sqlite_column_int64(stmt, 2) } as u64;
    let head_seq = unsafe { sqlite_column_int64(stmt, 3) } as u64;
    let replay_lamport = if unsafe { sqlite_column_type(stmt, 4) } == SQLITE_NULL as c_int {
        None
    } else {
        Some(unsafe { sqlite_column_int64(stmt, 4).max(0) as Lamport })
    };
    let replay_replica = if unsafe { sqlite_column_type(stmt, 5) } == SQLITE_NULL as c_int {
        None
    } else {
        let ptr = unsafe { sqlite_column_blob(stmt, 5) } as *const u8;
        let len = unsafe { sqlite_column_bytes(stmt, 5) } as usize;
        Some(if ptr.is_null() || len == 0 {
            Vec::new()
        } else {
            unsafe { slice::from_raw_parts(ptr, len) }.to_vec()
        })
    };
    let replay_counter = if unsafe { sqlite_column_type(stmt, 6) } == SQLITE_NULL as c_int {
        None
    } else {
        Some(unsafe { sqlite_column_int64(stmt, 6).max(0) as u64 })
    };

    let finalize_rc = unsafe { sqlite_finalize(stmt) };
    if finalize_rc != SQLITE_OK as c_int {
        return Err(finalize_rc);
    }

    let head = if head_seq == 0 && head_lamport == 0 && head_replica.is_empty() && head_counter == 0
    {
        None
    } else {
        Some(MaterializationHead {
            at: MaterializationKey {
                lamport: head_lamport,
                replica: head_replica,
                counter: head_counter,
            },
            seq: head_seq,
        })
    };
    let replay_from = match (replay_lamport, replay_replica, replay_counter) {
        (Some(lamport), Some(replica), Some(counter)) => Some(MaterializationKey {
            lamport,
            replica,
            counter,
        }),
        _ => None,
    };

    Ok(TreeMeta(MaterializationState { head, replay_from }))
}

pub(super) fn set_tree_meta_replay_frontier(
    db: *mut sqlite3,
    frontier: &treecrdt_core::MaterializationFrontier,
) -> Result<(), c_int> {
    let sql = CString::new(
        "UPDATE tree_meta \
         SET replay_lamport = ?1, replay_replica = ?2, replay_counter = ?3 \
         WHERE id = 1",
    )
    .expect("tree meta replay sql");
    let mut stmt: *mut sqlite3_stmt = null_mut();
    let rc = sqlite_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, null_mut());
    if rc != SQLITE_OK as c_int {
        return Err(rc);
    }

    let mut bind_err = false;
    unsafe {
        bind_err |= sqlite_bind_int64(stmt, 1, frontier.lamport as i64) != SQLITE_OK as c_int;
        bind_err |= sqlite_bind_blob(
            stmt,
            2,
            frontier.replica.as_ptr() as *const c_void,
            frontier.replica.len() as c_int,
            None,
        ) != SQLITE_OK as c_int;
        bind_err |= sqlite_bind_int64(stmt, 3, frontier.counter as i64) != SQLITE_OK as c_int;
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

pub(super) fn update_tree_meta_head<R: AsRef<[u8]>>(
    db: *mut sqlite3,
    head: Option<&MaterializationHead<R>>,
) -> Result<(), c_int> {
    let (lamport, replica, counter, seq): (Lamport, &[u8], u64, u64) = match head {
        Some(head) => (
            head.at.lamport,
            head.at.replica.as_ref(),
            head.at.counter,
            head.seq,
        ),
        None => (0, &[], 0, 0),
    };
    let sql = CString::new(
        "UPDATE tree_meta \
         SET head_lamport = ?1, \
             head_replica = ?2, \
             head_counter = ?3, \
             head_seq = ?4, \
             replay_lamport = NULL, \
             replay_replica = NULL, \
             replay_counter = NULL \
         WHERE id = 1",
    )
    .expect("tree meta head sql");
    let mut stmt: *mut sqlite3_stmt = null_mut();
    let rc = sqlite_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, null_mut());
    if rc != SQLITE_OK as c_int {
        return Err(rc);
    }

    let mut bind_err = false;
    unsafe {
        bind_err |= sqlite_bind_int64(stmt, 1, lamport as i64) != SQLITE_OK as c_int;
        bind_err |= sqlite_bind_blob(
            stmt,
            2,
            replica.as_ptr() as *const c_void,
            replica.len() as c_int,
            None,
        ) != SQLITE_OK as c_int;
        bind_err |= sqlite_bind_int64(stmt, 3, counter as i64) != SQLITE_OK as c_int;
        bind_err |= sqlite_bind_int64(stmt, 4, seq as i64) != SQLITE_OK as c_int;
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

pub(super) fn persist_materialized_head<R: AsRef<[u8]>>(
    db: *mut sqlite3,
    head: Option<&MaterializationHead<R>>,
) -> Result<(), c_int> {
    update_tree_meta_head(db, head)?;
    maybe_save_materialization_checkpoint(db, head)
}

pub(super) fn maybe_save_materialization_checkpoint<R: AsRef<[u8]>>(
    db: *mut sqlite3,
    head: Option<&MaterializationHead<R>>,
) -> Result<(), c_int> {
    let Some(head) = head else {
        return Ok(());
    };
    if !should_checkpoint_materialization(head) {
        return Ok(());
    }

    let checkpoint_seq = head.seq as i64;
    for sql in [
        "DELETE FROM checkpoint_oprefs_children WHERE checkpoint_seq = ?1",
        "DELETE FROM checkpoint_payload WHERE checkpoint_seq = ?1",
        "DELETE FROM checkpoint_nodes WHERE checkpoint_seq = ?1",
        "DELETE FROM materialization_checkpoints WHERE checkpoint_seq = ?1",
    ] {
        let sql = CString::new(sql).expect("checkpoint delete sql");
        let mut stmt: *mut sqlite3_stmt = null_mut();
        let rc = sqlite_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, null_mut());
        if rc != SQLITE_OK as c_int {
            return Err(rc);
        }
        let bind_rc = unsafe { sqlite_bind_int64(stmt, 1, checkpoint_seq) };
        if bind_rc != SQLITE_OK as c_int {
            unsafe { sqlite_finalize(stmt) };
            return Err(bind_rc);
        }
        let step_rc = unsafe { sqlite_step(stmt) };
        let finalize_rc = unsafe { sqlite_finalize(stmt) };
        if step_rc != SQLITE_DONE as c_int {
            return Err(step_rc);
        }
        if finalize_rc != SQLITE_OK as c_int {
            return Err(finalize_rc);
        }
    }

    let insert_meta_sql = CString::new(
        "INSERT INTO materialization_checkpoints(checkpoint_seq, head_lamport, head_replica, head_counter) \
         VALUES (?1, ?2, ?3, ?4)",
    )
    .expect("checkpoint meta sql");
    let mut stmt: *mut sqlite3_stmt = null_mut();
    let rc = sqlite_prepare_v2(db, insert_meta_sql.as_ptr(), -1, &mut stmt, null_mut());
    if rc != SQLITE_OK as c_int {
        return Err(rc);
    }
    let mut bind_err = false;
    unsafe {
        bind_err |= sqlite_bind_int64(stmt, 1, checkpoint_seq) != SQLITE_OK as c_int;
        bind_err |= sqlite_bind_int64(stmt, 2, head.at.lamport as i64) != SQLITE_OK as c_int;
        bind_err |= sqlite_bind_blob(
            stmt,
            3,
            head.at.replica.as_ref().as_ptr() as *const c_void,
            head.at.replica.as_ref().len() as c_int,
            None,
        ) != SQLITE_OK as c_int;
        bind_err |= sqlite_bind_int64(stmt, 4, head.at.counter as i64) != SQLITE_OK as c_int;
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

    let copy_nodes_sql = CString::new(
        "INSERT INTO checkpoint_nodes(checkpoint_seq, node, parent, order_key, tombstone, last_change, deleted_at) \
         SELECT ?1, node, parent, order_key, tombstone, last_change, deleted_at FROM tree_nodes",
    )
    .expect("copy checkpoint nodes sql");
    let mut copy_nodes: *mut sqlite3_stmt = null_mut();
    let rc = sqlite_prepare_v2(db, copy_nodes_sql.as_ptr(), -1, &mut copy_nodes, null_mut());
    if rc != SQLITE_OK as c_int {
        return Err(rc);
    }
    let bind_rc = unsafe { sqlite_bind_int64(copy_nodes, 1, checkpoint_seq) };
    if bind_rc != SQLITE_OK as c_int {
        unsafe { sqlite_finalize(copy_nodes) };
        return Err(bind_rc);
    }
    let step_rc = unsafe { sqlite_step(copy_nodes) };
    let finalize_rc = unsafe { sqlite_finalize(copy_nodes) };
    if step_rc != SQLITE_DONE as c_int {
        return Err(step_rc);
    }
    if finalize_rc != SQLITE_OK as c_int {
        return Err(finalize_rc);
    }

    let copy_payload_sql = CString::new(
        "INSERT INTO checkpoint_payload(checkpoint_seq, node, payload, last_lamport, last_replica, last_counter) \
         SELECT ?1, node, payload, last_lamport, last_replica, last_counter FROM tree_payload",
    )
    .expect("copy checkpoint payload sql");
    let mut copy_payload: *mut sqlite3_stmt = null_mut();
    let rc = sqlite_prepare_v2(
        db,
        copy_payload_sql.as_ptr(),
        -1,
        &mut copy_payload,
        null_mut(),
    );
    if rc != SQLITE_OK as c_int {
        return Err(rc);
    }
    let bind_rc = unsafe { sqlite_bind_int64(copy_payload, 1, checkpoint_seq) };
    if bind_rc != SQLITE_OK as c_int {
        unsafe { sqlite_finalize(copy_payload) };
        return Err(bind_rc);
    }
    let step_rc = unsafe { sqlite_step(copy_payload) };
    let finalize_rc = unsafe { sqlite_finalize(copy_payload) };
    if step_rc != SQLITE_DONE as c_int {
        return Err(step_rc);
    }
    if finalize_rc != SQLITE_OK as c_int {
        return Err(finalize_rc);
    }

    let copy_index_sql = CString::new(
        "INSERT INTO checkpoint_oprefs_children(checkpoint_seq, parent, op_ref, seq) \
         SELECT ?1, parent, op_ref, seq FROM oprefs_children",
    )
    .expect("copy checkpoint index sql");
    let mut copy_index: *mut sqlite3_stmt = null_mut();
    let rc = sqlite_prepare_v2(db, copy_index_sql.as_ptr(), -1, &mut copy_index, null_mut());
    if rc != SQLITE_OK as c_int {
        return Err(rc);
    }
    let bind_rc = unsafe { sqlite_bind_int64(copy_index, 1, checkpoint_seq) };
    if bind_rc != SQLITE_OK as c_int {
        unsafe { sqlite_finalize(copy_index) };
        return Err(bind_rc);
    }
    let step_rc = unsafe { sqlite_step(copy_index) };
    let finalize_rc = unsafe { sqlite_finalize(copy_index) };
    if step_rc != SQLITE_DONE as c_int {
        return Err(step_rc);
    }
    if finalize_rc != SQLITE_OK as c_int {
        return Err(finalize_rc);
    }

    Ok(())
}

pub(super) fn load_materialization_checkpoint_before(
    db: *mut sqlite3,
    frontier: &treecrdt_core::MaterializationFrontier,
) -> Result<Option<treecrdt_core::MaterializationHead>, c_int> {
    let sql = CString::new(
        "SELECT checkpoint_seq, head_lamport, head_replica, head_counter \
         FROM materialization_checkpoints \
         WHERE head_lamport < ?1 \
            OR (head_lamport = ?1 AND head_replica < ?2) \
            OR (head_lamport = ?1 AND head_replica = ?2 AND head_counter < ?3) \
         ORDER BY head_lamport DESC, head_replica DESC, head_counter DESC \
         LIMIT 1",
    )
    .expect("load checkpoint before sql");
    let mut stmt: *mut sqlite3_stmt = null_mut();
    let rc = sqlite_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, null_mut());
    if rc != SQLITE_OK as c_int {
        return Err(rc);
    }
    let mut bind_err = false;
    unsafe {
        bind_err |= sqlite_bind_int64(stmt, 1, frontier.lamport as i64) != SQLITE_OK as c_int;
        bind_err |= sqlite_bind_blob(
            stmt,
            2,
            frontier.replica.as_ptr() as *const c_void,
            frontier.replica.len() as c_int,
            None,
        ) != SQLITE_OK as c_int;
        bind_err |= sqlite_bind_int64(stmt, 3, frontier.counter as i64) != SQLITE_OK as c_int;
    }
    if bind_err {
        unsafe { sqlite_finalize(stmt) };
        return Err(SQLITE_ERROR as c_int);
    }
    let step_rc = unsafe { sqlite_step(stmt) };
    if step_rc == SQLITE_DONE as c_int {
        let finalize_rc = unsafe { sqlite_finalize(stmt) };
        if finalize_rc != SQLITE_OK as c_int {
            return Err(finalize_rc);
        }
        return Ok(None);
    }
    if step_rc != SQLITE_ROW as c_int {
        unsafe { sqlite_finalize(stmt) };
        return Err(step_rc);
    }

    let checkpoint_seq = unsafe { sqlite_column_int64(stmt, 0).max(0) as u64 };
    let head_lamport = unsafe { sqlite_column_int64(stmt, 1).max(0) as Lamport };
    let rep_ptr = unsafe { sqlite_column_blob(stmt, 2) } as *const u8;
    let rep_len = unsafe { sqlite_column_bytes(stmt, 2) } as usize;
    let head_replica = if rep_ptr.is_null() || rep_len == 0 {
        Vec::new()
    } else {
        unsafe { slice::from_raw_parts(rep_ptr, rep_len) }.to_vec()
    };
    let head_counter = unsafe { sqlite_column_int64(stmt, 3).max(0) as u64 };

    let finalize_rc = unsafe { sqlite_finalize(stmt) };
    if finalize_rc != SQLITE_OK as c_int {
        return Err(finalize_rc);
    }

    Ok(Some(treecrdt_core::MaterializationHead {
        at: treecrdt_core::MaterializationKey {
            lamport: head_lamport,
            replica: head_replica,
            counter: head_counter,
        },
        seq: checkpoint_seq,
    }))
}

pub(super) fn restore_materialization_checkpoint<R: AsRef<[u8]>>(
    db: *mut sqlite3,
    checkpoint: Option<&MaterializationHead<R>>,
) -> Result<(), c_int> {
    for sql in [
        "DELETE FROM oprefs_children",
        "DELETE FROM tree_payload",
        "DELETE FROM tree_nodes",
    ] {
        let sql = CString::new(sql).expect("clear live tables sql");
        let rc = sqlite_exec(db, sql.as_ptr(), None, null_mut(), null_mut());
        if rc != SQLITE_OK as c_int {
            return Err(rc);
        }
    }

    if let Some(checkpoint) = checkpoint {
        let checkpoint_seq = checkpoint.seq as i64;
        for sql in [
            "INSERT INTO tree_nodes(node, parent, order_key, tombstone, last_change, deleted_at) \
             SELECT node, parent, order_key, tombstone, last_change, deleted_at \
             FROM checkpoint_nodes WHERE checkpoint_seq = ?1",
            "INSERT INTO tree_payload(node, payload, last_lamport, last_replica, last_counter) \
             SELECT node, payload, last_lamport, last_replica, last_counter \
             FROM checkpoint_payload WHERE checkpoint_seq = ?1",
            "INSERT INTO oprefs_children(parent, op_ref, seq) \
             SELECT parent, op_ref, seq \
             FROM checkpoint_oprefs_children WHERE checkpoint_seq = ?1",
        ] {
            let sql = CString::new(sql).expect("restore checkpoint sql");
            let mut stmt: *mut sqlite3_stmt = null_mut();
            let rc = sqlite_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, null_mut());
            if rc != SQLITE_OK as c_int {
                return Err(rc);
            }
            let bind_rc = unsafe { sqlite_bind_int64(stmt, 1, checkpoint_seq) };
            if bind_rc != SQLITE_OK as c_int {
                unsafe { sqlite_finalize(stmt) };
                return Err(bind_rc);
            }
            let step_rc = unsafe { sqlite_step(stmt) };
            let finalize_rc = unsafe { sqlite_finalize(stmt) };
            if step_rc != SQLITE_DONE as c_int {
                return Err(step_rc);
            }
            if finalize_rc != SQLITE_OK as c_int {
                return Err(finalize_rc);
            }
        }
    } else {
        let root_insert = CString::new(
            "INSERT INTO tree_nodes(node,parent,order_key,tombstone) VALUES (?1,NULL,X'',0)",
        )
        .expect("insert root sql");
        let mut stmt: *mut sqlite3_stmt = null_mut();
        let rc = sqlite_prepare_v2(db, root_insert.as_ptr(), -1, &mut stmt, null_mut());
        if rc != SQLITE_OK as c_int {
            return Err(rc);
        }
        let bind_rc = unsafe {
            sqlite_bind_blob(
                stmt,
                1,
                ROOT_NODE_ID.as_ptr() as *const c_void,
                ROOT_NODE_ID.len() as c_int,
                None,
            )
        };
        if bind_rc != SQLITE_OK as c_int {
            unsafe { sqlite_finalize(stmt) };
            return Err(bind_rc);
        }
        let step_rc = unsafe { sqlite_step(stmt) };
        let finalize_rc = unsafe { sqlite_finalize(stmt) };
        if step_rc != SQLITE_DONE as c_int {
            return Err(step_rc);
        }
        if finalize_rc != SQLITE_OK as c_int {
            return Err(finalize_rc);
        }
    }

    Ok(())
}

pub(super) fn ensure_schema(db: *mut sqlite3) -> Result<(), c_int> {
    ensure_api_initialized()?;

    // Core tables.
    const META: &str = r#"
CREATE TABLE IF NOT EXISTS meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
"#;
    const OPS: &str = r#"
CREATE TABLE IF NOT EXISTS ops (
  replica BLOB NOT NULL,
  counter INTEGER NOT NULL,
  lamport INTEGER NOT NULL,
  kind TEXT NOT NULL,
  parent BLOB,
  node BLOB NOT NULL,
  new_parent BLOB,
  order_key BLOB,
  op_ref BLOB,
  known_state BLOB,
  payload BLOB,
  PRIMARY KEY (replica, counter)
);
"#;
    // Materialized tree state + indexes.
    const TREE_META: &str = r#"
CREATE TABLE IF NOT EXISTS tree_meta (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  head_lamport INTEGER NOT NULL DEFAULT 0,
  head_replica BLOB NOT NULL DEFAULT X'',
  head_counter INTEGER NOT NULL DEFAULT 0,
  head_seq INTEGER NOT NULL DEFAULT 0,
  replay_lamport INTEGER,
  replay_replica BLOB,
  replay_counter INTEGER
);
INSERT OR IGNORE INTO tree_meta(id) VALUES (1);
"#;
    const TREE_NODES: &str = r#"
CREATE TABLE IF NOT EXISTS tree_nodes (
  node BLOB PRIMARY KEY,
  parent BLOB,
  order_key BLOB,
  tombstone INTEGER NOT NULL DEFAULT 0,
  last_change BLOB,
  deleted_at BLOB
);
"#;
    const OPREFS_CHILDREN: &str = r#"
CREATE TABLE IF NOT EXISTS oprefs_children (
  parent BLOB NOT NULL,
  op_ref BLOB NOT NULL,
  seq INTEGER NOT NULL,
  PRIMARY KEY (parent, op_ref)
);
"#;
    const TREE_PAYLOAD: &str = r#"
CREATE TABLE IF NOT EXISTS tree_payload (
  node BLOB PRIMARY KEY,
  payload BLOB,
  last_lamport INTEGER NOT NULL,
  last_replica BLOB NOT NULL,
  last_counter INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS materialization_checkpoints (
  checkpoint_seq INTEGER PRIMARY KEY,
  head_lamport INTEGER NOT NULL,
  head_replica BLOB NOT NULL,
  head_counter INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS checkpoint_nodes (
  checkpoint_seq INTEGER NOT NULL,
  node BLOB NOT NULL,
  parent BLOB,
  order_key BLOB,
  tombstone INTEGER NOT NULL DEFAULT 0,
  last_change BLOB,
  deleted_at BLOB,
  PRIMARY KEY (checkpoint_seq, node)
);

CREATE TABLE IF NOT EXISTS checkpoint_payload (
  checkpoint_seq INTEGER NOT NULL,
  node BLOB NOT NULL,
  payload BLOB,
  last_lamport INTEGER NOT NULL,
  last_replica BLOB NOT NULL,
  last_counter INTEGER NOT NULL,
  PRIMARY KEY (checkpoint_seq, node)
);

CREATE TABLE IF NOT EXISTS checkpoint_oprefs_children (
  checkpoint_seq INTEGER NOT NULL,
  parent BLOB NOT NULL,
  op_ref BLOB NOT NULL,
  seq INTEGER NOT NULL,
  PRIMARY KEY (checkpoint_seq, parent, op_ref)
);
"#;

    let rc_meta = {
        let sql = CString::new(META).expect("meta schema");
        sqlite_exec(db, sql.as_ptr(), None, null_mut(), null_mut())
    };
    if rc_meta != SQLITE_OK as c_int {
        return Err(rc_meta);
    }
    let rc_ops = {
        let sql = CString::new(OPS).expect("ops schema");
        sqlite_exec(db, sql.as_ptr(), None, null_mut(), null_mut())
    };
    if rc_ops != SQLITE_OK as c_int {
        return Err(rc_ops);
    }
    let rc_tree_meta = {
        let sql = CString::new(TREE_META).expect("tree_meta schema");
        sqlite_exec(db, sql.as_ptr(), None, null_mut(), null_mut())
    };
    if rc_tree_meta != SQLITE_OK as c_int {
        return Err(rc_tree_meta);
    }
    let rc_nodes = {
        let sql = CString::new(TREE_NODES).expect("tree_nodes schema");
        sqlite_exec(db, sql.as_ptr(), None, null_mut(), null_mut())
    };
    if rc_nodes != SQLITE_OK as c_int {
        return Err(rc_nodes);
    }
    let rc_oprefs = {
        let sql = CString::new(OPREFS_CHILDREN).expect("oprefs_children schema");
        sqlite_exec(db, sql.as_ptr(), None, null_mut(), null_mut())
    };
    if rc_oprefs != SQLITE_OK as c_int {
        return Err(rc_oprefs);
    }

    let rc_tree_payload = {
        let sql = CString::new(TREE_PAYLOAD).expect("tree_payload schema");
        sqlite_exec(db, sql.as_ptr(), None, null_mut(), null_mut())
    };
    if rc_tree_payload != SQLITE_OK as c_int {
        return Err(rc_tree_payload);
    }

    const INDEXES: &str = r#"
CREATE INDEX IF NOT EXISTS idx_ops_lamport ON ops(lamport, replica, counter);
CREATE INDEX IF NOT EXISTS idx_ops_op_ref ON ops(op_ref);
CREATE INDEX IF NOT EXISTS idx_tree_nodes_parent_order_key_node ON tree_nodes(parent, order_key, node);
CREATE INDEX IF NOT EXISTS idx_tree_nodes_parent_tombstone_order_key_node ON tree_nodes(parent, tombstone, order_key, node);
CREATE INDEX IF NOT EXISTS idx_oprefs_children_parent_seq ON oprefs_children(parent, seq);
CREATE INDEX IF NOT EXISTS idx_materialization_checkpoints_head
  ON materialization_checkpoints(head_lamport, head_replica, head_counter);
CREATE INDEX IF NOT EXISTS idx_checkpoint_oprefs_children_parent_seq
  ON checkpoint_oprefs_children(checkpoint_seq, parent, seq);
"#;
    let rc_idx = {
        let sql = CString::new(INDEXES).expect("index schema");
        sqlite_exec(db, sql.as_ptr(), None, null_mut(), null_mut())
    };
    if rc_idx != SQLITE_OK as c_int {
        return Err(rc_idx);
    }

    // If this is a fresh database with no ops yet, seed the materialized root so appends can
    // maintain state incrementally without a full catch-up pass.
    let mut ops_count: i64 = 0;
    {
        let sql = CString::new("SELECT COUNT(*) FROM ops").expect("count ops sql");
        let mut stmt: *mut sqlite3_stmt = null_mut();
        let rc = sqlite_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, null_mut());
        if rc == SQLITE_OK as c_int {
            let step_rc = unsafe { sqlite_step(stmt) };
            if step_rc == SQLITE_ROW as c_int {
                ops_count = unsafe { sqlite_column_int64(stmt, 0) };
            }
            unsafe { sqlite_finalize(stmt) };
        }
    }
    if ops_count == 0 {
        // Ensure ROOT exists even before first catch-up.
        let _ = {
            let sql = CString::new(
                "INSERT OR IGNORE INTO tree_nodes(node,parent,order_key,tombstone) VALUES (?1,NULL,X'',0)",
            )
            .expect("root insert sql");
            let mut stmt: *mut sqlite3_stmt = null_mut();
            let rc = sqlite_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, null_mut());
            if rc != SQLITE_OK as c_int {
                rc
            } else {
                unsafe {
                    sqlite_bind_blob(
                        stmt,
                        1,
                        ROOT_NODE_ID.as_ptr() as *const c_void,
                        ROOT_NODE_ID.len() as c_int,
                        None,
                    );
                    sqlite_step(stmt);
                    sqlite_finalize(stmt)
                }
            }
        };
    }

    Ok(())
}

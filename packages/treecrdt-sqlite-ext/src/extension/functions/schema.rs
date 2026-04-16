use super::sqlite_api::*;
use super::util::{column_blob_vec, column_nonnegative_i64};

use std::ffi::CString;
use std::os::raw::{c_int, c_void};
use std::ptr::null_mut;
use std::slice;

use treecrdt_core::{
    should_checkpoint_materialization, Lamport, MaterializationCursor, MaterializationHead,
    MaterializationKey, MaterializationState,
};

pub(super) const ROOT_NODE_ID: [u8; 16] = [0u8; 16];

fn exec_sql_text(db: *mut sqlite3, sql: &str) -> Result<(), c_int> {
    let sql = CString::new(sql).expect("sqlite exec sql");
    let rc = sqlite_exec(db, sql.as_ptr(), None, null_mut(), null_mut());
    if rc != SQLITE_OK as c_int {
        return Err(rc);
    }
    Ok(())
}

fn with_stmt<T>(
    db: *mut sqlite3,
    sql: &str,
    run: impl FnOnce(*mut sqlite3_stmt) -> Result<T, c_int>,
) -> Result<T, c_int> {
    let sql = CString::new(sql).expect("sqlite prepared sql");
    let mut stmt: *mut sqlite3_stmt = null_mut();
    let rc = sqlite_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, null_mut());
    if rc != SQLITE_OK as c_int {
        return Err(rc);
    }

    let result = run(stmt);
    let finalize_rc = unsafe { sqlite_finalize(stmt) };
    match result {
        Ok(value) => {
            if finalize_rc != SQLITE_OK as c_int {
                return Err(finalize_rc);
            }
            Ok(value)
        }
        Err(err) => Err(err),
    }
}

fn bind_i64_param(stmt: *mut sqlite3_stmt, idx: c_int, value: i64) -> Result<(), c_int> {
    let rc = unsafe { sqlite_bind_int64(stmt, idx, value) };
    if rc != SQLITE_OK as c_int {
        return Err(rc);
    }
    Ok(())
}

fn bind_blob_param(stmt: *mut sqlite3_stmt, idx: c_int, value: &[u8]) -> Result<(), c_int> {
    let rc = unsafe {
        sqlite_bind_blob(
            stmt,
            idx,
            value.as_ptr() as *const c_void,
            value.len() as c_int,
            None,
        )
    };
    if rc != SQLITE_OK as c_int {
        return Err(rc);
    }
    Ok(())
}

fn step_expect_done(stmt: *mut sqlite3_stmt) -> Result<(), c_int> {
    let rc = unsafe { sqlite_step(stmt) };
    if rc != SQLITE_DONE as c_int {
        return Err(rc);
    }
    Ok(())
}

fn exec_stmt_done(
    db: *mut sqlite3,
    sql: &str,
    bind: impl FnOnce(*mut sqlite3_stmt) -> Result<(), c_int>,
) -> Result<(), c_int> {
    with_stmt(db, sql, |stmt| {
        bind(stmt)?;
        step_expect_done(stmt)
    })
}

fn exec_stmt_i64(db: *mut sqlite3, sql: &str, value: i64) -> Result<(), c_int> {
    exec_stmt_done(db, sql, |stmt| bind_i64_param(stmt, 1, value))
}

fn column_optional_nonnegative_i64(stmt: *mut sqlite3_stmt, idx: c_int) -> Option<i64> {
    if unsafe { sqlite_column_type(stmt, idx) } == SQLITE_NULL as c_int {
        None
    } else {
        Some(column_nonnegative_i64(stmt, idx))
    }
}

fn column_materialization_key(stmt: *mut sqlite3_stmt, idx: c_int) -> MaterializationKey {
    MaterializationKey {
        lamport: column_nonnegative_i64(stmt, idx) as Lamport,
        replica: column_blob_vec(stmt, idx + 1).unwrap_or_default(),
        counter: column_nonnegative_i64(stmt, idx + 2) as u64,
    }
}

fn column_optional_materialization_key(
    stmt: *mut sqlite3_stmt,
    idx: c_int,
) -> Option<MaterializationKey> {
    match (
        column_optional_nonnegative_i64(stmt, idx),
        column_blob_vec(stmt, idx + 1),
        column_optional_nonnegative_i64(stmt, idx + 2),
    ) {
        (Some(lamport), Some(replica), Some(counter)) => Some(MaterializationKey {
            lamport: lamport as Lamport,
            replica,
            counter: counter as u64,
        }),
        _ => None,
    }
}

fn bind_materialization_key<R: AsRef<[u8]>>(
    stmt: *mut sqlite3_stmt,
    idx: c_int,
    key: &MaterializationKey<R>,
) -> Result<(), c_int> {
    bind_i64_param(stmt, idx, key.lamport as i64)?;
    bind_blob_param(stmt, idx + 1, key.replica.as_ref())?;
    bind_i64_param(stmt, idx + 2, key.counter as i64)
}

fn bind_materialization_head<R: AsRef<[u8]>>(
    stmt: *mut sqlite3_stmt,
    idx: c_int,
    head: &MaterializationHead<R>,
) -> Result<(), c_int> {
    bind_materialization_key(stmt, idx, &head.at)?;
    bind_i64_param(stmt, idx + 3, head.seq as i64)
}

fn bind_optional_materialization_head<R: AsRef<[u8]>>(
    stmt: *mut sqlite3_stmt,
    idx: c_int,
    head: Option<&MaterializationHead<R>>,
) -> Result<(), c_int> {
    match head {
        Some(head) => bind_materialization_head(stmt, idx, head),
        None => {
            bind_i64_param(stmt, idx, 0)?;
            bind_blob_param(stmt, idx + 1, &[])?;
            bind_i64_param(stmt, idx + 2, 0)?;
            bind_i64_param(stmt, idx + 3, 0)
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct TreeMeta(MaterializationState);

impl MaterializationCursor for TreeMeta {
    fn state(&self) -> MaterializationState<&[u8]> {
        self.0.as_borrowed()
    }
}

pub(super) fn load_doc_id(db: *mut sqlite3) -> Result<Option<Vec<u8>>, c_int> {
    with_stmt(
        db,
        "SELECT value FROM meta WHERE key = 'doc_id' LIMIT 1",
        |stmt| {
            let step_rc = unsafe { sqlite_step(stmt) };
            if step_rc == SQLITE_ROW as c_int {
                let ptr = unsafe { sqlite_column_text(stmt, 0) } as *const u8;
                let len = unsafe { sqlite_column_bytes(stmt, 0) } as usize;
                let value = if ptr.is_null() || len == 0 {
                    Vec::new()
                } else {
                    unsafe { slice::from_raw_parts(ptr, len) }.to_vec()
                };
                Ok(Some(value))
            } else if step_rc == SQLITE_DONE as c_int {
                Ok(None)
            } else {
                Err(step_rc)
            }
        },
    )
}

pub(super) fn load_tree_meta(db: *mut sqlite3) -> Result<TreeMeta, c_int> {
    with_stmt(
        db,
        "SELECT head_lamport, head_replica, head_counter, head_seq, \
                replay_lamport, replay_replica, replay_counter \
         FROM tree_meta WHERE id = 1 LIMIT 1",
        |stmt| {
            let step_rc = unsafe { sqlite_step(stmt) };
            if step_rc != SQLITE_ROW as c_int {
                return Err(SQLITE_ERROR as c_int);
            }

            let head_seq = column_nonnegative_i64(stmt, 3) as u64;
            let head_at = column_materialization_key(stmt, 0);
            let head = if head_seq == 0
                && head_at.lamport == 0
                && head_at.replica.is_empty()
                && head_at.counter == 0
            {
                None
            } else {
                Some(MaterializationHead {
                    at: head_at,
                    seq: head_seq,
                })
            };
            let replay_from = column_optional_materialization_key(stmt, 4);

            Ok(TreeMeta(MaterializationState { head, replay_from }))
        },
    )
}

pub(super) fn set_tree_meta_replay_frontier(
    db: *mut sqlite3,
    frontier: &treecrdt_core::MaterializationFrontier,
) -> Result<(), c_int> {
    exec_stmt_done(
        db,
        "UPDATE tree_meta \
         SET replay_lamport = ?1, replay_replica = ?2, replay_counter = ?3 \
         WHERE id = 1",
        |stmt| bind_materialization_key(stmt, 1, frontier),
    )
}

pub(super) fn update_tree_meta_head<R: AsRef<[u8]>>(
    db: *mut sqlite3,
    head: Option<&MaterializationHead<R>>,
) -> Result<(), c_int> {
    exec_stmt_done(
        db,
        "UPDATE tree_meta \
         SET head_lamport = ?1, \
             head_replica = ?2, \
             head_counter = ?3, \
             head_seq = ?4, \
             replay_lamport = NULL, \
             replay_replica = NULL, \
             replay_counter = NULL \
         WHERE id = 1",
        |stmt| bind_optional_materialization_head(stmt, 1, head),
    )
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
        exec_stmt_i64(db, sql, checkpoint_seq)?;
    }

    exec_stmt_done(
        db,
        "INSERT INTO materialization_checkpoints(checkpoint_seq, head_lamport, head_replica, head_counter) \
         VALUES (?1, ?2, ?3, ?4)",
        |stmt| {
            bind_i64_param(stmt, 1, checkpoint_seq)?;
            bind_materialization_key(stmt, 2, &head.at)
        },
    )?;

    for sql in [
        "INSERT INTO checkpoint_nodes(checkpoint_seq, node, parent, order_key, tombstone, last_change, deleted_at) \
         SELECT ?1, node, parent, order_key, tombstone, last_change, deleted_at FROM tree_nodes",
        "INSERT INTO checkpoint_payload(checkpoint_seq, node, payload, last_lamport, last_replica, last_counter) \
         SELECT ?1, node, payload, last_lamport, last_replica, last_counter FROM tree_payload",
        "INSERT INTO checkpoint_oprefs_children(checkpoint_seq, parent, op_ref, seq) \
         SELECT ?1, parent, op_ref, seq FROM oprefs_children",
    ] {
        exec_stmt_i64(db, sql, checkpoint_seq)?;
    }

    Ok(())
}

pub(super) fn load_materialization_checkpoint_before(
    db: *mut sqlite3,
    frontier: &treecrdt_core::MaterializationFrontier,
) -> Result<Option<treecrdt_core::MaterializationHead>, c_int> {
    with_stmt(
        db,
        "SELECT checkpoint_seq, head_lamport, head_replica, head_counter \
         FROM materialization_checkpoints \
         WHERE head_lamport < ?1 \
            OR (head_lamport = ?1 AND head_replica < ?2) \
            OR (head_lamport = ?1 AND head_replica = ?2 AND head_counter < ?3) \
         ORDER BY head_lamport DESC, head_replica DESC, head_counter DESC \
         LIMIT 1",
        |stmt| {
            bind_materialization_key(stmt, 1, frontier)?;

            let step_rc = unsafe { sqlite_step(stmt) };
            if step_rc == SQLITE_DONE as c_int {
                return Ok(None);
            }
            if step_rc != SQLITE_ROW as c_int {
                return Err(step_rc);
            }

            let checkpoint_seq = column_nonnegative_i64(stmt, 0) as u64;
            let head_at = column_materialization_key(stmt, 1);

            Ok(Some(treecrdt_core::MaterializationHead {
                at: head_at,
                seq: checkpoint_seq,
            }))
        },
    )
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
        exec_sql_text(db, sql)?;
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
            exec_stmt_i64(db, sql, checkpoint_seq)?;
        }
    } else {
        exec_stmt_done(
            db,
            "INSERT INTO tree_nodes(node,parent,order_key,tombstone) VALUES (?1,NULL,X'',0)",
            |stmt| bind_blob_param(stmt, 1, &ROOT_NODE_ID),
        )?;
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

    for sql in [
        META,
        OPS,
        TREE_META,
        TREE_NODES,
        OPREFS_CHILDREN,
        TREE_PAYLOAD,
    ] {
        exec_sql_text(db, sql)?;
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
    exec_sql_text(db, INDEXES)?;

    // If this is a fresh database with no ops yet, seed the materialized root so appends can
    // maintain state incrementally without a full catch-up pass.
    let ops_count = with_stmt(db, "SELECT COUNT(*) FROM ops", |stmt| {
        let step_rc = unsafe { sqlite_step(stmt) };
        if step_rc == SQLITE_ROW as c_int {
            Ok(column_nonnegative_i64(stmt, 0))
        } else if step_rc == SQLITE_DONE as c_int {
            Ok(0)
        } else {
            Err(step_rc)
        }
    })?;
    if ops_count == 0 {
        // Ensure ROOT exists even before first catch-up.
        let _ = exec_stmt_done(
            db,
            "INSERT OR IGNORE INTO tree_nodes(node,parent,order_key,tombstone) VALUES (?1,NULL,X'',0)",
            |stmt| bind_blob_param(stmt, 1, &ROOT_NODE_ID),
        );
    }

    Ok(())
}

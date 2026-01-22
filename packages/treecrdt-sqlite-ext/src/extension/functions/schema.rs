use super::sqlite_api::*;

use std::ffi::CString;
use std::os::raw::{c_int, c_void};
use std::ptr::null_mut;
use std::slice;

use treecrdt_core::Lamport;

pub(super) const ROOT_NODE_ID: [u8; 16] = [0u8; 16];

#[derive(Clone, Debug)]
pub(super) struct TreeMeta {
    pub(super) dirty: bool,
    pub(super) head_lamport: Lamport,
    pub(super) head_replica: Vec<u8>,
    pub(super) head_counter: u64,
    pub(super) head_seq: u64,
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
        "SELECT dirty, head_lamport, head_replica, head_counter, head_seq \
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

    let dirty = unsafe { sqlite_column_int64(stmt, 0) } != 0;
    let head_lamport = unsafe { sqlite_column_int64(stmt, 1) } as Lamport;
    let rep_ptr = unsafe { sqlite_column_blob(stmt, 2) } as *const u8;
    let rep_len = unsafe { sqlite_column_bytes(stmt, 2) } as usize;
    let head_replica = if rep_ptr.is_null() || rep_len == 0 {
        Vec::new()
    } else {
        unsafe { slice::from_raw_parts(rep_ptr, rep_len) }.to_vec()
    };
    let head_counter = unsafe { sqlite_column_int64(stmt, 3) } as u64;
    let head_seq = unsafe { sqlite_column_int64(stmt, 4) } as u64;

    let finalize_rc = unsafe { sqlite_finalize(stmt) };
    if finalize_rc != SQLITE_OK as c_int {
        return Err(finalize_rc);
    }

    Ok(TreeMeta {
        dirty,
        head_lamport,
        head_replica,
        head_counter,
        head_seq,
    })
}

pub(super) fn set_tree_meta_dirty(db: *mut sqlite3, dirty: bool) -> Result<(), c_int> {
    let sql =
        CString::new("UPDATE tree_meta SET dirty = ?1 WHERE id = 1").expect("tree meta dirty sql");
    let mut stmt: *mut sqlite3_stmt = null_mut();
    let rc = sqlite_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, null_mut());
    if rc != SQLITE_OK as c_int {
        return Err(rc);
    }

    let bind_rc = unsafe { sqlite_bind_int64(stmt, 1, if dirty { 1 } else { 0 }) };
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
    Ok(())
}

pub(super) fn update_tree_meta_head(
    db: *mut sqlite3,
    lamport: Lamport,
    replica: &[u8],
    counter: u64,
    seq: u64,
) -> Result<(), c_int> {
    let sql = CString::new(
        "UPDATE tree_meta \
         SET dirty = 0, head_lamport = ?1, head_replica = ?2, head_counter = ?3, head_seq = ?4 \
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
  position INTEGER,
  op_ref BLOB,
  known_state BLOB,
  payload BLOB,
  PRIMARY KEY (replica, counter)
);
"#;
    // Materialized tree state + indexes (v1).
    const TREE_META: &str = r#"
CREATE TABLE IF NOT EXISTS tree_meta (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  dirty INTEGER NOT NULL DEFAULT 1,
  head_lamport INTEGER NOT NULL DEFAULT 0,
  head_replica BLOB NOT NULL DEFAULT X'',
  head_counter INTEGER NOT NULL DEFAULT 0,
  head_seq INTEGER NOT NULL DEFAULT 0
);
INSERT OR IGNORE INTO tree_meta(id) VALUES (1);
"#;
    const TREE_NODES: &str = r#"
CREATE TABLE IF NOT EXISTS tree_nodes (
  node BLOB PRIMARY KEY,
  parent BLOB,
  pos INTEGER,
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

    // Derived table: migrate legacy payload schema if present.
    {
        let pragma_sql = CString::new("PRAGMA table_info(tree_payload)").expect("tree_payload pragma");
        let mut stmt: *mut sqlite3_stmt = null_mut();
        let rc = sqlite_prepare_v2(db, pragma_sql.as_ptr(), -1, &mut stmt, null_mut());
        if rc == SQLITE_OK as c_int {
            let mut has_op_ref = false;
            let mut has_last_lamport = false;
            loop {
                let step_rc = unsafe { sqlite_step(stmt) };
                if step_rc == SQLITE_ROW as c_int {
                    let name_ptr = unsafe { sqlite_column_text(stmt, 1) } as *const u8;
                    let name_len = unsafe { sqlite_column_bytes(stmt, 1) } as usize;
                    if !name_ptr.is_null() && name_len > 0 {
                        let name = unsafe { slice::from_raw_parts(name_ptr, name_len) };
                        has_op_ref |= name == b"op_ref";
                        has_last_lamport |= name == b"last_lamport";
                    }
                } else if step_rc == SQLITE_DONE as c_int {
                    break;
                } else {
                    break;
                }
            }
            unsafe { sqlite_finalize(stmt) };

            if has_op_ref && !has_last_lamport {
                // Drop/recreate; payload table is derived from ops.
                let drop_sql = CString::new("DROP TABLE IF EXISTS tree_payload").expect("drop tree_payload");
                let _ = sqlite_exec(db, drop_sql.as_ptr(), None, null_mut(), null_mut());
                let recreate_sql = CString::new(
                    "CREATE TABLE tree_payload (\
                      node BLOB PRIMARY KEY,\
                      payload BLOB,\
                      last_lamport INTEGER NOT NULL,\
                      last_replica BLOB NOT NULL,\
                      last_counter INTEGER NOT NULL\
                    );",
                )
                .expect("recreate tree_payload");
                let _ = sqlite_exec(db, recreate_sql.as_ptr(), None, null_mut(), null_mut());
                let _ = set_tree_meta_dirty(db, true);
            }
        }
    }

    const INDEXES: &str = r#"
CREATE INDEX IF NOT EXISTS idx_ops_lamport ON ops(lamport, replica, counter);
CREATE INDEX IF NOT EXISTS idx_ops_op_ref ON ops(op_ref);
CREATE INDEX IF NOT EXISTS idx_tree_nodes_parent_pos ON tree_nodes(parent, pos);
CREATE INDEX IF NOT EXISTS idx_oprefs_children_parent_seq ON oprefs_children(parent, seq);
"#;
    let rc_idx = {
        let sql = CString::new(INDEXES).expect("index schema");
        sqlite_exec(db, sql.as_ptr(), None, null_mut(), null_mut())
    };
    if rc_idx != SQLITE_OK as c_int {
        return Err(rc_idx);
    }

    // If this is a fresh database with no ops yet, mark materialization clean so appends can
    // maintain state incrementally without a full rebuild.
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
        let _ = set_tree_meta_dirty(db, false);
        // Ensure ROOT exists even before first rebuild.
        let _ = {
            let sql = CString::new(
                "INSERT OR IGNORE INTO tree_nodes(node,parent,pos,tombstone) VALUES (?1,NULL,0,0)",
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

use super::materialize::ensure_materialized;
use super::sqlite_api::*;

use std::ffi::CString;
use std::os::raw::{c_int, c_void};
use std::ptr::null_mut;
use std::slice;

fn is_trash_node(bytes: &[u8; 16]) -> bool {
    bytes.iter().all(|b| *b == 0xff)
}

pub(super) fn allocate_order_key(
    db: *mut sqlite3,
    parent: &[u8; 16],
    placement: &str,
    after: Option<&[u8; 16]>,
    exclude: Option<&[u8; 16]>,
    seed: &[u8],
) -> Result<Vec<u8>, c_int> {
    if is_trash_node(parent) {
        return Ok(Vec::new());
    }

    ensure_materialized(db)?;

    let (left, right) = match placement {
        "first" => (None, select_first_child_order_key(db, parent, exclude)?),
        "last" => (select_last_child_order_key(db, parent, exclude)?, None),
        "after" => {
            let after_node =
                after.ok_or_else(|| SQLITE_ERROR as c_int)?;
            if exclude.map_or(false, |ex| ex == after_node) {
                return Err(SQLITE_ERROR as c_int);
            }
            let left_key = select_child_order_key(db, parent, after_node)?;
            let right_key =
                select_next_sibling_order_key(db, parent, &left_key, after_node, exclude)?;
            (Some(left_key), right_key)
        }
        _ => return Err(SQLITE_ERROR as c_int),
    };

    treecrdt_core::order_key::allocate_between(left.as_deref(), right.as_deref(), seed)
        .map_err(|_| SQLITE_ERROR as c_int)
}

fn read_blob_column(stmt: *mut sqlite3_stmt, idx: c_int) -> Result<Vec<u8>, c_int> {
    let ty = unsafe { sqlite_column_type(stmt, idx) };
    if ty == SQLITE_NULL as c_int {
        return Err(SQLITE_ERROR as c_int);
    }
    let len = unsafe { sqlite_column_bytes(stmt, idx) } as usize;
    if len == 0 {
        return Ok(Vec::new());
    }
    let ptr = unsafe { sqlite_column_blob(stmt, idx) } as *const u8;
    if ptr.is_null() {
        return Err(SQLITE_ERROR as c_int);
    }
    Ok(unsafe { slice::from_raw_parts(ptr, len) }.to_vec())
}

fn select_first_child_order_key(
    db: *mut sqlite3,
    parent: &[u8; 16],
    exclude: Option<&[u8; 16]>,
) -> Result<Option<Vec<u8>>, c_int> {
    let sql = if exclude.is_some() {
        "SELECT order_key FROM tree_nodes \
         WHERE parent = ?1 AND tombstone = 0 AND node <> ?2 \
         ORDER BY order_key, node \
         LIMIT 1"
    } else {
        "SELECT order_key FROM tree_nodes \
         WHERE parent = ?1 AND tombstone = 0 \
         ORDER BY order_key, node \
         LIMIT 1"
    };
    let sql = CString::new(sql).expect("first child order_key sql");
    let mut stmt: *mut sqlite3_stmt = null_mut();
    let rc = sqlite_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, null_mut());
    if rc != SQLITE_OK as c_int {
        return Err(rc);
    }
    unsafe {
        sqlite_bind_blob(
            stmt,
            1,
            parent.as_ptr() as *const c_void,
            parent.len() as c_int,
            None,
        );
        if let Some(ex) = exclude {
            sqlite_bind_blob(
                stmt,
                2,
                ex.as_ptr() as *const c_void,
                ex.len() as c_int,
                None,
            );
        }
        let step_rc = sqlite_step(stmt);
        let out = if step_rc == SQLITE_ROW as c_int {
            Some(read_blob_column(stmt, 0)?)
        } else if step_rc == SQLITE_DONE as c_int {
            None
        } else {
            sqlite_finalize(stmt);
            return Err(step_rc);
        };
        sqlite_finalize(stmt);
        Ok(out)
    }
}

fn select_last_child_order_key(
    db: *mut sqlite3,
    parent: &[u8; 16],
    exclude: Option<&[u8; 16]>,
) -> Result<Option<Vec<u8>>, c_int> {
    let sql = if exclude.is_some() {
        "SELECT order_key FROM tree_nodes \
         WHERE parent = ?1 AND tombstone = 0 AND node <> ?2 \
         ORDER BY order_key DESC, node DESC \
         LIMIT 1"
    } else {
        "SELECT order_key FROM tree_nodes \
         WHERE parent = ?1 AND tombstone = 0 \
         ORDER BY order_key DESC, node DESC \
         LIMIT 1"
    };
    let sql = CString::new(sql).expect("last child order_key sql");
    let mut stmt: *mut sqlite3_stmt = null_mut();
    let rc = sqlite_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, null_mut());
    if rc != SQLITE_OK as c_int {
        return Err(rc);
    }
    unsafe {
        sqlite_bind_blob(
            stmt,
            1,
            parent.as_ptr() as *const c_void,
            parent.len() as c_int,
            None,
        );
        if let Some(ex) = exclude {
            sqlite_bind_blob(
                stmt,
                2,
                ex.as_ptr() as *const c_void,
                ex.len() as c_int,
                None,
            );
        }
        let step_rc = sqlite_step(stmt);
        let out = if step_rc == SQLITE_ROW as c_int {
            Some(read_blob_column(stmt, 0)?)
        } else if step_rc == SQLITE_DONE as c_int {
            None
        } else {
            sqlite_finalize(stmt);
            return Err(step_rc);
        };
        sqlite_finalize(stmt);
        Ok(out)
    }
}

fn select_child_order_key(
    db: *mut sqlite3,
    parent: &[u8; 16],
    node: &[u8; 16],
) -> Result<Vec<u8>, c_int> {
    let sql = CString::new(
        "SELECT order_key FROM tree_nodes \
         WHERE node = ?1 AND parent = ?2 AND tombstone = 0 \
         LIMIT 1",
    )
    .expect("child order_key sql");
    let mut stmt: *mut sqlite3_stmt = null_mut();
    let rc = sqlite_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, null_mut());
    if rc != SQLITE_OK as c_int {
        return Err(rc);
    }
    unsafe {
        sqlite_bind_blob(stmt, 1, node.as_ptr() as *const c_void, node.len() as c_int, None);
        sqlite_bind_blob(
            stmt,
            2,
            parent.as_ptr() as *const c_void,
            parent.len() as c_int,
            None,
        );
        let step_rc = sqlite_step(stmt);
        if step_rc != SQLITE_ROW as c_int {
            sqlite_finalize(stmt);
            return Err(if step_rc == SQLITE_DONE as c_int {
                SQLITE_ERROR as c_int
            } else {
                step_rc
            });
        }
        let out = read_blob_column(stmt, 0)?;
        sqlite_finalize(stmt);
        Ok(out)
    }
}

fn select_next_sibling_order_key(
    db: *mut sqlite3,
    parent: &[u8; 16],
    after_order_key: &[u8],
    after_node: &[u8; 16],
    exclude: Option<&[u8; 16]>,
) -> Result<Option<Vec<u8>>, c_int> {
    let sql = if exclude.is_some() {
        "SELECT order_key FROM tree_nodes \
         WHERE parent = ?1 AND tombstone = 0 AND node <> ?4 \
           AND (order_key > ?2 OR (order_key = ?2 AND node > ?3)) \
         ORDER BY order_key, node \
         LIMIT 1"
    } else {
        "SELECT order_key FROM tree_nodes \
         WHERE parent = ?1 AND tombstone = 0 \
           AND (order_key > ?2 OR (order_key = ?2 AND node > ?3)) \
         ORDER BY order_key, node \
         LIMIT 1"
    };
    let sql = CString::new(sql).expect("next sibling order_key sql");
    let mut stmt: *mut sqlite3_stmt = null_mut();
    let rc = sqlite_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, null_mut());
    if rc != SQLITE_OK as c_int {
        return Err(rc);
    }
    unsafe {
        sqlite_bind_blob(
            stmt,
            1,
            parent.as_ptr() as *const c_void,
            parent.len() as c_int,
            None,
        );
        sqlite_bind_blob(
            stmt,
            2,
            after_order_key.as_ptr() as *const c_void,
            after_order_key.len() as c_int,
            None,
        );
        sqlite_bind_blob(
            stmt,
            3,
            after_node.as_ptr() as *const c_void,
            after_node.len() as c_int,
            None,
        );
        if let Some(ex) = exclude {
            sqlite_bind_blob(
                stmt,
                4,
                ex.as_ptr() as *const c_void,
                ex.len() as c_int,
                None,
            );
        }
        let step_rc = sqlite_step(stmt);
        let out = if step_rc == SQLITE_ROW as c_int {
            Some(read_blob_column(stmt, 0)?)
        } else if step_rc == SQLITE_DONE as c_int {
            None
        } else {
            sqlite_finalize(stmt);
            return Err(step_rc);
        };
        sqlite_finalize(stmt);
        Ok(out)
    }
}

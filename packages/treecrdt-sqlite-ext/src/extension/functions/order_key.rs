use super::materialize::ensure_materialized;
use super::sqlite_api::*;
use super::util::sqlite_result_blob_owned;

use std::ffi::CString;
use std::os::raw::{c_char, c_int, c_void};
use std::ptr::null_mut;
use std::slice;

fn is_trash_node(bytes: &[u8; 16]) -> bool {
    bytes.iter().all(|b| *b == 0xff)
}

fn arg_blob16(val: *mut sqlite3_value) -> Result<[u8; 16], &'static [u8]> {
    let ptr = unsafe { sqlite_value_blob(val) } as *const u8;
    let len = unsafe { sqlite_value_bytes(val) } as usize;
    if ptr.is_null() || len != 16 {
        return Err(b"expected 16-byte BLOB\0");
    }
    let mut out = [0u8; 16];
    out.copy_from_slice(unsafe { slice::from_raw_parts(ptr, len) });
    Ok(out)
}

fn arg_optional_blob16(val: *mut sqlite3_value) -> Result<Option<[u8; 16]>, &'static [u8]> {
    let ty = unsafe { sqlite_value_type(val) };
    if ty == SQLITE_NULL as c_int {
        return Ok(None);
    }
    arg_blob16(val).map(Some)
}

fn arg_text(val: *mut sqlite3_value) -> Result<String, &'static [u8]> {
    let ptr = unsafe { sqlite_value_text(val) } as *const u8;
    if ptr.is_null() {
        return Err(b"expected TEXT\0");
    }
    let len = unsafe { sqlite_value_bytes(val) } as usize;
    let bytes = unsafe { slice::from_raw_parts(ptr, len) };
    Ok(String::from_utf8_lossy(bytes).to_string())
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

pub(super) unsafe extern "C" fn treecrdt_allocate_order_key(
    ctx: *mut sqlite3_context,
    argc: c_int,
    argv: *mut *mut sqlite3_value,
) {
    if argc != 5 {
        sqlite_result_error(
            ctx,
            b"treecrdt_allocate_order_key expects 5 args (parent, placement, after, exclude, seed)\0"
                .as_ptr() as *const c_char,
        );
        return;
    }

    let args = unsafe { slice::from_raw_parts(argv, argc as usize) };
    let parent = match arg_blob16(args[0]) {
        Ok(v) => v,
        Err(msg) => {
            sqlite_result_error(ctx, msg.as_ptr() as *const c_char);
            return;
        }
    };
    let placement = match arg_text(args[1]) {
        Ok(v) => v,
        Err(msg) => {
            sqlite_result_error(ctx, msg.as_ptr() as *const c_char);
            return;
        }
    };
    let after = match arg_optional_blob16(args[2]) {
        Ok(v) => v,
        Err(msg) => {
            sqlite_result_error(ctx, msg.as_ptr() as *const c_char);
            return;
        }
    };
    let exclude = match arg_optional_blob16(args[3]) {
        Ok(v) => v,
        Err(msg) => {
            sqlite_result_error(ctx, msg.as_ptr() as *const c_char);
            return;
        }
    };
    let seed_ptr = unsafe { sqlite_value_blob(args[4]) } as *const u8;
    let seed_len = unsafe { sqlite_value_bytes(args[4]) } as usize;
    if seed_ptr.is_null() && seed_len != 0 {
        sqlite_result_error(ctx, b"seed must be a BLOB\0".as_ptr() as *const c_char);
        return;
    }
    let seed = if seed_len == 0 {
        &[]
    } else {
        unsafe { slice::from_raw_parts(seed_ptr, seed_len) }
    };

    if is_trash_node(&parent) {
        sqlite_result_blob_owned(ctx, &[]);
        return;
    }

    let db = sqlite_context_db_handle(ctx);
    if let Err(rc) = ensure_materialized(db) {
        sqlite_result_error_code(ctx, rc);
        return;
    }

    let exclude_ref = exclude.as_ref();
    let (left, right) = match placement.as_str() {
        "first" => match select_first_child_order_key(db, &parent, exclude_ref) {
            Ok(r) => (None, r),
            Err(rc) => {
                sqlite_result_error_code(ctx, rc);
                return;
            }
        },
        "last" => match select_last_child_order_key(db, &parent, exclude_ref) {
            Ok(l) => (l, None),
            Err(rc) => {
                sqlite_result_error_code(ctx, rc);
                return;
            }
        },
        "after" => {
            let Some(after_node) = after else {
                sqlite_result_error(ctx, b"after placement requires after node\0".as_ptr() as *const c_char);
                return;
            };
            if exclude_ref.map_or(false, |ex| ex == &after_node) {
                sqlite_result_error(
                    ctx,
                    b"placement.after must not equal excluded node\0".as_ptr() as *const c_char,
                );
                return;
            }
            let left_key = match select_child_order_key(db, &parent, &after_node) {
                Ok(v) => v,
                Err(rc) => {
                    sqlite_result_error_code(ctx, rc);
                    return;
                }
            };
            let right_key = match select_next_sibling_order_key(
                db,
                &parent,
                &left_key,
                &after_node,
                exclude_ref,
            ) {
                Ok(v) => v,
                Err(rc) => {
                    sqlite_result_error_code(ctx, rc);
                    return;
                }
            };
            (Some(left_key), right_key)
        }
        _ => {
            sqlite_result_error(
                ctx,
                b"placement must be one of: first | last | after\0".as_ptr() as *const c_char,
            );
            return;
        }
    };

    let order_key = match treecrdt_core::order_key::allocate_between(left.as_deref(), right.as_deref(), seed) {
        Ok(v) => v,
        Err(_) => {
            sqlite_result_error_code(ctx, SQLITE_ERROR as c_int);
            return;
        }
    };

    sqlite_result_blob_owned(ctx, &order_key);
}

use super::util::drop_cstring;
use super::*;

pub(super) unsafe extern "C" fn treecrdt_set_doc_id(
    ctx: *mut sqlite3_context,
    argc: c_int,
    argv: *mut *mut sqlite3_value,
) {
    if argc != 1 {
        sqlite_result_error(
            ctx,
            b"treecrdt_set_doc_id expects 1 arg (doc_id)\0".as_ptr() as *const c_char,
        );
        return;
    }

    let args = unsafe { slice::from_raw_parts(argv, argc as usize) };
    let doc_ptr = unsafe { sqlite_value_text(args[0]) } as *const u8;
    let doc_len = unsafe { sqlite_value_bytes(args[0]) } as usize;
    if doc_ptr.is_null() {
        sqlite_result_error(
            ctx,
            b"treecrdt_set_doc_id: NULL doc_id\0".as_ptr() as *const c_char,
        );
        return;
    }
    let doc_bytes = unsafe { slice::from_raw_parts(doc_ptr, doc_len) }.to_vec();

    let db = sqlite_context_db_handle(ctx);

    match load_doc_id(db) {
        Ok(Some(existing)) => {
            if existing != doc_bytes {
                sqlite_result_error(
                    ctx,
                    b"treecrdt_set_doc_id: doc_id already set (cannot change)\0".as_ptr()
                        as *const c_char,
                );
                return;
            }
        }
        Ok(None) => {
            let sql = CString::new("INSERT INTO meta(key,value) VALUES('doc_id', ?1)")
                .expect("insert doc id sql");
            let mut stmt: *mut sqlite3_stmt = null_mut();
            let rc = sqlite_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, null_mut());
            if rc != SQLITE_OK as c_int {
                sqlite_result_error_code(ctx, rc);
                return;
            }
            let bind_rc = unsafe {
                sqlite_bind_text(stmt, 1, doc_ptr as *const c_char, doc_len as c_int, None)
            };
            if bind_rc != SQLITE_OK as c_int {
                unsafe { sqlite_finalize(stmt) };
                sqlite_result_error_code(ctx, bind_rc);
                return;
            }

            let step_rc = unsafe { sqlite_step(stmt) };
            let finalize_rc = unsafe { sqlite_finalize(stmt) };
            if step_rc != SQLITE_DONE as c_int || finalize_rc != SQLITE_OK as c_int {
                sqlite_result_error_code(ctx, SQLITE_ERROR as c_int);
                return;
            }
        }
        Err(rc) => {
            sqlite_result_error_code(ctx, rc);
            return;
        }
    };

    // Ensure the materialized tree state is available for direct SQL reads over `tree_nodes`.
    // This is especially important on reopen, where `tree_meta.dirty = 1` requires a replay.
    if let Err(rc) = ensure_materialized(db) {
        sqlite_result_error_code(ctx, rc);
        return;
    }

    // No backfill/migration: callers must set `doc_id` before appending ops so `op_ref`
    // is always computed at write time.
    sqlite_result_int(ctx, 1);
}

pub(super) unsafe extern "C" fn treecrdt_doc_id(
    ctx: *mut sqlite3_context,
    _argc: c_int,
    _argv: *mut *mut sqlite3_value,
) {
    let db = sqlite_context_db_handle(ctx);
    let doc = match load_doc_id(db) {
        Ok(Some(v)) => v,
        Ok(None) => Vec::new(),
        Err(rc) => {
            sqlite_result_error_code(ctx, rc);
            return;
        }
    };

    let cstr = match CString::new(doc) {
        Ok(v) => v,
        Err(_) => {
            sqlite_result_error_code(ctx, SQLITE_ERROR as c_int);
            return;
        }
    };
    let len = cstr.as_bytes().len() as c_int;
    let ptr = cstr.into_raw();
    sqlite_result_text(ctx, ptr as *const c_char, len, Some(drop_cstring));
}

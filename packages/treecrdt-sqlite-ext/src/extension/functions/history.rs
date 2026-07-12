use super::op_storage::SqliteOpStorage;
use super::util::sqlite_result_json;
use super::*;

pub(super) unsafe extern "C" fn treecrdt_history_invert(
    ctx: *mut sqlite3_context,
    argc: c_int,
    argv: *mut *mut sqlite3_value,
) {
    if argc != 1 {
        sqlite_result_error(
            ctx,
            b"treecrdt_history_invert expects 1 arg (json op ids)\0".as_ptr() as *const c_char,
        );
        return;
    }

    let args = unsafe { slice::from_raw_parts(argv, argc as usize) };
    let json_ptr = unsafe { sqlite_value_text(args[0]) };
    let json_len = unsafe { sqlite_value_bytes(args[0]) } as usize;
    if json_ptr.is_null() || json_len == 0 {
        sqlite_result_error(
            ctx,
            b"treecrdt_history_invert expects non-empty JSON\0".as_ptr() as *const c_char,
        );
        return;
    }

    let db = sqlite_context_db_handle(ctx);
    let doc_id = match load_doc_id(db) {
        Ok(Some(value)) => value,
        Ok(None) => Vec::new(),
        Err(rc) => {
            sqlite_result_error_code(ctx, rc);
            return;
        }
    };
    let storage = SqliteOpStorage::with_doc_id(db, doc_id);
    let json_bytes = unsafe { slice::from_raw_parts(json_ptr as *const u8, json_len) };

    match treecrdt_core::derive_undo_plan_from_history_json(&storage, json_bytes) {
        Ok(plan) => sqlite_result_json(ctx, &plan),
        Err(_) => sqlite_result_error_code(ctx, SQLITE_ERROR as c_int),
    }
}

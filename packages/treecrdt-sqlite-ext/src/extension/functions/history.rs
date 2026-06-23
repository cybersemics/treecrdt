use super::node_store::SqliteNodeStore;
use super::op_storage::SqliteOpStorage;
use super::payload_store::SqlitePayloadStore;
use super::util::sqlite_result_json;
use super::*;

fn exec_static_sql(db: *mut sqlite3, sql: &str) -> Result<(), c_int> {
    let sql = CString::new(sql).expect("history sql");
    let rc = sqlite_exec(db, sql.as_ptr(), None, null_mut(), null_mut());
    if rc == SQLITE_OK as c_int {
        Ok(())
    } else {
        Err(rc)
    }
}

fn try_history_invert_fast(
    db: *mut sqlite3,
    storage: &SqliteOpStorage,
    json_bytes: &[u8],
) -> Result<Option<treecrdt_core::LocalEditPlanWire>, c_int> {
    if exec_static_sql(db, "SAVEPOINT treecrdt_history_invert").is_err() {
        return Ok(None);
    }

    let plan = match (
        load_tree_meta(db),
        SqliteNodeStore::prepare(db),
        SqlitePayloadStore::prepare(db),
    ) {
        (Ok(meta), Ok(mut nodes), Ok(mut payloads)) => {
            treecrdt_core::try_derive_undo_plan_by_rewinding_suffix_json(
                storage,
                &mut nodes,
                &mut payloads,
                &meta,
                json_bytes,
            )
            .ok()
            .flatten()
        }
        _ => None,
    };

    exec_static_sql(
        db,
        "ROLLBACK TO treecrdt_history_invert; RELEASE treecrdt_history_invert",
    )?;
    Ok(plan)
}

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

    let json_bytes = unsafe { slice::from_raw_parts(json_ptr as *const u8, json_len) };
    let db = sqlite_context_db_handle(ctx);
    let doc_id = match load_doc_id(db) {
        Ok(Some(v)) => v,
        Ok(None) => Vec::new(),
        Err(rc) => {
            sqlite_result_error_code(ctx, rc);
            return;
        }
    };
    let storage = SqliteOpStorage::with_doc_id(db, doc_id);

    if ensure_materialized(db).is_ok() {
        match try_history_invert_fast(db, &storage, json_bytes) {
            Ok(Some(plan)) => {
                sqlite_result_json(ctx, &plan);
                return;
            }
            Ok(None) => {}
            Err(rc) => {
                sqlite_result_error_code(ctx, rc);
                return;
            }
        }
    }

    let plan = match treecrdt_core::derive_undo_plan_from_history_json(&storage, json_bytes) {
        Ok(plan) => plan,
        Err(_) => {
            sqlite_result_error_code(ctx, SQLITE_ERROR as c_int);
            return;
        }
    };

    sqlite_result_json(ctx, &plan);
}

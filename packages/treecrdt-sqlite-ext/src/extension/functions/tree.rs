use super::node_store::SqliteNodeStore;
use super::util::sqlite_result_json_string;
use super::*;
use treecrdt_core::{LamportClock, ReplicaId, TreeCrdt};

pub(super) unsafe extern "C" fn treecrdt_subtree_known_state(
    ctx: *mut sqlite3_context,
    argc: c_int,
    argv: *mut *mut sqlite3_value,
) {
    if argc != 1 {
        sqlite_result_error(
            ctx,
            b"treecrdt_subtree_known_state expects 1 arg (node)\0".as_ptr() as *const c_char,
        );
        return;
    }

    let args = unsafe { slice::from_raw_parts(argv, argc as usize) };
    let node_ptr = unsafe { sqlite_value_blob(args[0]) } as *const u8;
    let node_len = unsafe { sqlite_value_bytes(args[0]) } as usize;
    if node_ptr.is_null() || node_len != 16 {
        sqlite_result_error(
            ctx,
            b"treecrdt_subtree_known_state: node must be 16-byte BLOB\0".as_ptr() as *const c_char,
        );
        return;
    }
    let mut node = [0u8; 16];
    node.copy_from_slice(unsafe { slice::from_raw_parts(node_ptr, node_len) });
    let node_id = NodeId(u128::from_be_bytes(node));

    let db = sqlite_context_db_handle(ctx);
    if let Err(rc) = ensure_materialized(db) {
        sqlite_result_error_code(ctx, rc);
        return;
    }

    let node_store = match SqliteNodeStore::prepare(db) {
        Ok(store) => store,
        Err(_) => {
            sqlite_result_error_code(ctx, SQLITE_ERROR as c_int);
            return;
        }
    };

    let tree = TreeCrdt::with_node_store(
        ReplicaId::new(b"sqlite-ext"),
        NoopStorage::default(),
        LamportClock::default(),
        node_store,
    );

    let vv = match tree.subtree_version_vector(node_id) {
        Ok(v) => v,
        Err(_) => {
            sqlite_result_error_code(ctx, SQLITE_ERROR as c_int);
            return;
        }
    };

    match serde_json::to_string(&vv) {
        Ok(json) => sqlite_result_json_string(ctx, json),
        Err(_) => sqlite_result_error_code(ctx, SQLITE_ERROR as c_int),
    }
}

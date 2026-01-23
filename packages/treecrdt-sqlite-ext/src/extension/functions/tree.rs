use super::node_store::SqliteNodeStore;
use super::util::{sqlite_result_json, sqlite_result_json_string};
use super::*;
use treecrdt_core::{LamportClock, ReplicaId, TreeCrdt};

pub(super) unsafe extern "C" fn treecrdt_tree_children(
    ctx: *mut sqlite3_context,
    argc: c_int,
    argv: *mut *mut sqlite3_value,
) {
    if argc != 1 {
        sqlite_result_error(
            ctx,
            b"treecrdt_tree_children expects 1 arg (parent)\0".as_ptr() as *const c_char,
        );
        return;
    }

    let args = unsafe { slice::from_raw_parts(argv, argc as usize) };
    let parent_ptr = unsafe { sqlite_value_blob(args[0]) } as *const u8;
    let parent_len = unsafe { sqlite_value_bytes(args[0]) } as usize;
    if parent_ptr.is_null() || parent_len != 16 {
        sqlite_result_error(
            ctx,
            b"treecrdt_tree_children: parent must be 16-byte BLOB\0".as_ptr() as *const c_char,
        );
        return;
    }
    let mut parent = [0u8; 16];
    parent.copy_from_slice(unsafe { slice::from_raw_parts(parent_ptr, parent_len) });

    let db = sqlite_context_db_handle(ctx);
    if let Err(rc) = ensure_materialized(db) {
        sqlite_result_error_code(ctx, rc);
        return;
    }

    let sql = CString::new(
        "SELECT node FROM tree_nodes WHERE parent = ?1 AND tombstone = 0 ORDER BY order_key, node",
    )
    .expect("static sql");
    let mut stmt: *mut sqlite3_stmt = null_mut();
    let rc = sqlite_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, null_mut());
    if rc != SQLITE_OK as c_int {
        sqlite_result_error_code(ctx, rc);
        return;
    }

    let bind_rc = unsafe {
        sqlite_bind_blob(
            stmt,
            1,
            parent.as_ptr() as *const c_void,
            parent.len() as c_int,
            None,
        )
    };
    if bind_rc != SQLITE_OK as c_int {
        unsafe { sqlite_finalize(stmt) };
        sqlite_result_error_code(ctx, bind_rc);
        return;
    }

    let mut nodes: Vec<Vec<u8>> = Vec::new();
    loop {
        let step_rc = unsafe { sqlite_step(stmt) };
        if step_rc == SQLITE_ROW as c_int {
            let ptr = unsafe { sqlite_column_blob(stmt, 0) } as *const u8;
            let len = unsafe { sqlite_column_bytes(stmt, 0) } as usize;
            if ptr.is_null() || len != 16 {
                unsafe { sqlite_finalize(stmt) };
                sqlite_result_error_code(ctx, SQLITE_ERROR as c_int);
                return;
            }
            nodes.push(unsafe { slice::from_raw_parts(ptr, len) }.to_vec());
        } else if step_rc == SQLITE_DONE as c_int {
            break;
        } else {
            unsafe { sqlite_finalize(stmt) };
            sqlite_result_error_code(ctx, step_rc);
            return;
        }
    }

    let finalize_rc = unsafe { sqlite_finalize(stmt) };
    if finalize_rc != SQLITE_OK as c_int {
        sqlite_result_error_code(ctx, finalize_rc);
        return;
    }

    sqlite_result_json(ctx, &nodes);
}

#[derive(serde::Serialize)]
struct JsonTreeChild {
    node: [u8; 16],
    order_key: Option<Vec<u8>>,
}

pub(super) unsafe extern "C" fn treecrdt_tree_children_page(
    ctx: *mut sqlite3_context,
    argc: c_int,
    argv: *mut *mut sqlite3_value,
) {
    if argc != 4 {
        sqlite_result_error(
            ctx,
            b"treecrdt_tree_children_page expects 4 args (parent, after_order_key, after_node, limit)\0"
                .as_ptr() as *const c_char,
        );
        return;
    }

    let args = unsafe { slice::from_raw_parts(argv, argc as usize) };

    let parent_ptr = unsafe { sqlite_value_blob(args[0]) } as *const u8;
    let parent_len = unsafe { sqlite_value_bytes(args[0]) } as usize;
    if parent_ptr.is_null() || parent_len != 16 {
        sqlite_result_error(
            ctx,
            b"treecrdt_tree_children_page: parent must be 16-byte BLOB\0".as_ptr() as *const c_char,
        );
        return;
    }
    let mut parent = [0u8; 16];
    parent.copy_from_slice(unsafe { slice::from_raw_parts(parent_ptr, parent_len) });

    let after_order_key = if unsafe { sqlite_value_type(args[1]) } == SQLITE_NULL as c_int {
        None
    } else {
        let ptr = unsafe { sqlite_value_blob(args[1]) } as *const u8;
        let len = unsafe { sqlite_value_bytes(args[1]) } as usize;
        if ptr.is_null() {
            None
        } else {
            Some(unsafe { slice::from_raw_parts(ptr, len) }.to_vec())
        }
    };

    let after_node = if unsafe { sqlite_value_type(args[2]) } == SQLITE_NULL as c_int {
        None
    } else {
        let ptr = unsafe { sqlite_value_blob(args[2]) } as *const u8;
        let len = unsafe { sqlite_value_bytes(args[2]) } as usize;
        if ptr.is_null() || len != 16 {
            sqlite_result_error(
                ctx,
                b"treecrdt_tree_children_page: after_node must be NULL or 16-byte BLOB\0"
                    .as_ptr() as *const c_char,
            );
            return;
        }
        let mut buf = [0u8; 16];
        buf.copy_from_slice(unsafe { slice::from_raw_parts(ptr, len) });
        Some(buf)
    };

    if after_order_key.is_some() != after_node.is_some() {
        sqlite_result_error(
            ctx,
            b"treecrdt_tree_children_page: after_order_key and after_node must be both NULL or both non-NULL\0"
                .as_ptr() as *const c_char,
        );
        return;
    }

    let limit = unsafe { sqlite_value_int64(args[3]) };
    if limit <= 0 {
        sqlite_result_error(
            ctx,
            b"treecrdt_tree_children_page: limit must be > 0\0".as_ptr() as *const c_char,
        );
        return;
    }

    let db = sqlite_context_db_handle(ctx);
    if let Err(rc) = ensure_materialized(db) {
        sqlite_result_error_code(ctx, rc);
        return;
    }

    let sql = CString::new(
        "SELECT node, order_key FROM tree_nodes \
         WHERE parent = ?1 AND tombstone = 0 \
         AND (?2 IS NULL OR (order_key > ?2 OR (order_key = ?2 AND node > ?3))) \
         ORDER BY order_key, node \
         LIMIT ?4",
    )
    .expect("static sql");
    let mut stmt: *mut sqlite3_stmt = null_mut();
    let rc = sqlite_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, null_mut());
    if rc != SQLITE_OK as c_int {
        sqlite_result_error_code(ctx, rc);
        return;
    }

    let mut bind_err = false;
    unsafe {
        bind_err |= sqlite_bind_blob(
            stmt,
            1,
            parent.as_ptr() as *const c_void,
            parent.len() as c_int,
            None,
        ) != SQLITE_OK as c_int;

        if let Some(ref ok) = after_order_key {
            bind_err |= sqlite_bind_blob(
                stmt,
                2,
                ok.as_ptr() as *const c_void,
                ok.len() as c_int,
                None,
            ) != SQLITE_OK as c_int;
        } else {
            bind_err |= sqlite_bind_null(stmt, 2) != SQLITE_OK as c_int;
        }

        if let Some(ref an) = after_node {
            bind_err |= sqlite_bind_blob(
                stmt,
                3,
                an.as_ptr() as *const c_void,
                an.len() as c_int,
                None,
            ) != SQLITE_OK as c_int;
        } else {
            bind_err |= sqlite_bind_null(stmt, 3) != SQLITE_OK as c_int;
        }

        bind_err |= sqlite_bind_int64(stmt, 4, limit) != SQLITE_OK as c_int;
    }
    if bind_err {
        unsafe { sqlite_finalize(stmt) };
        sqlite_result_error_code(ctx, SQLITE_ERROR as c_int);
        return;
    }

    let mut rows: Vec<JsonTreeChild> = Vec::new();
    loop {
        let step_rc = unsafe { sqlite_step(stmt) };
        if step_rc == SQLITE_ROW as c_int {
            let node = match unsafe { column_blob16(stmt, 0) } {
                Ok(Some(v)) => v,
                _ => {
                    unsafe { sqlite_finalize(stmt) };
                    sqlite_result_error_code(ctx, SQLITE_ERROR as c_int);
                    return;
                }
            };
            let order_key = if unsafe { sqlite_column_type(stmt, 1) } == SQLITE_NULL as c_int {
                None
            } else {
                let ptr = unsafe { sqlite_column_blob(stmt, 1) } as *const u8;
                let len = unsafe { sqlite_column_bytes(stmt, 1) } as usize;
                if ptr.is_null() {
                    None
                } else {
                    Some(unsafe { slice::from_raw_parts(ptr, len) }.to_vec())
                }
            };
            rows.push(JsonTreeChild { node, order_key });
        } else if step_rc == SQLITE_DONE as c_int {
            break;
        } else {
            unsafe { sqlite_finalize(stmt) };
            sqlite_result_error_code(ctx, step_rc);
            return;
        }
    }

    let finalize_rc = unsafe { sqlite_finalize(stmt) };
    if finalize_rc != SQLITE_OK as c_int {
        sqlite_result_error_code(ctx, finalize_rc);
        return;
    }

    sqlite_result_json(ctx, &rows);
}

#[derive(serde::Serialize)]
struct JsonTreeNode {
    node: [u8; 16],
    parent: Option<[u8; 16]>,
    order_key: Option<Vec<u8>>,
    tombstone: bool,
}

pub(super) unsafe extern "C" fn treecrdt_tree_dump(
    ctx: *mut sqlite3_context,
    argc: c_int,
    _argv: *mut *mut sqlite3_value,
) {
    if argc != 0 {
        sqlite_result_error(
            ctx,
            b"treecrdt_tree_dump expects 0 args\0".as_ptr() as *const c_char,
        );
        return;
    }

    let db = sqlite_context_db_handle(ctx);
    if let Err(rc) = ensure_materialized(db) {
        sqlite_result_error_code(ctx, rc);
        return;
    }

    let sql = CString::new("SELECT node,parent,order_key,tombstone FROM tree_nodes")
        .expect("static sql");
    let mut stmt: *mut sqlite3_stmt = null_mut();
    let rc = sqlite_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, null_mut());
    if rc != SQLITE_OK as c_int {
        sqlite_result_error_code(ctx, rc);
        return;
    }

    let mut rows: Vec<JsonTreeNode> = Vec::new();
    loop {
        let step_rc = unsafe { sqlite_step(stmt) };
        if step_rc == SQLITE_ROW as c_int {
            let node = match unsafe { column_blob16(stmt, 0) } {
                Ok(Some(v)) => v,
                _ => {
                    unsafe { sqlite_finalize(stmt) };
                    sqlite_result_error_code(ctx, SQLITE_ERROR as c_int);
                    return;
                }
            };
            let parent = unsafe { column_blob16(stmt, 1) }.ok().flatten();
            let order_key = if unsafe { sqlite_column_type(stmt, 2) } == SQLITE_NULL as c_int {
                None
            } else {
                let ptr = unsafe { sqlite_column_blob(stmt, 2) } as *const u8;
                let len = unsafe { sqlite_column_bytes(stmt, 2) } as usize;
                if ptr.is_null() {
                    None
                } else {
                    Some(unsafe { slice::from_raw_parts(ptr, len) }.to_vec())
                }
            };
            let tombstone = unsafe { sqlite_column_int64(stmt, 3) } != 0;
            rows.push(JsonTreeNode {
                node,
                parent,
                order_key,
                tombstone,
            });
        } else if step_rc == SQLITE_DONE as c_int {
            break;
        } else {
            unsafe { sqlite_finalize(stmt) };
            sqlite_result_error_code(ctx, step_rc);
            return;
        }
    }

    let finalize_rc = unsafe { sqlite_finalize(stmt) };
    if finalize_rc != SQLITE_OK as c_int {
        sqlite_result_error_code(ctx, finalize_rc);
        return;
    }

    sqlite_result_json(ctx, &rows);
}

pub(super) unsafe extern "C" fn treecrdt_tree_node_count(
    ctx: *mut sqlite3_context,
    argc: c_int,
    _argv: *mut *mut sqlite3_value,
) {
    if argc != 0 {
        sqlite_result_error(
            ctx,
            b"treecrdt_tree_node_count expects 0 args\0".as_ptr() as *const c_char,
        );
        return;
    }

    let db = sqlite_context_db_handle(ctx);
    if let Err(rc) = ensure_materialized(db) {
        sqlite_result_error_code(ctx, rc);
        return;
    }

    let sql = CString::new("SELECT COUNT(*) FROM tree_nodes WHERE tombstone = 0 AND node != ?1")
        .expect("static sql");
    let mut stmt: *mut sqlite3_stmt = null_mut();
    let rc = sqlite_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, null_mut());
    if rc != SQLITE_OK as c_int {
        sqlite_result_error_code(ctx, rc);
        return;
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
        sqlite_result_error_code(ctx, bind_rc);
        return;
    }

    let step_rc = unsafe { sqlite_step(stmt) };
    let count = if step_rc == SQLITE_ROW as c_int {
        unsafe { sqlite_column_int64(stmt, 0) }
    } else {
        0
    };
    let finalize_rc = unsafe { sqlite_finalize(stmt) };
    if finalize_rc != SQLITE_OK as c_int {
        sqlite_result_error_code(ctx, finalize_rc);
        return;
    }
    sqlite_result_int64(ctx, count);
}

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

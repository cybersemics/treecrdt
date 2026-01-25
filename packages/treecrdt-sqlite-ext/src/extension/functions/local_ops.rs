use super::append::JsonAppendOp;
use super::node_store::SqliteNodeStore;
use super::order_key::allocate_order_key;
use super::util::sqlite_result_json;
use super::*;
use treecrdt_core::{LamportClock, ReplicaId, TreeCrdt};

fn read_blob(val: *mut sqlite3_value) -> Option<Vec<u8>> {
    unsafe {
        if sqlite_value_type(val) == SQLITE_NULL as c_int {
            return None;
        }
        let ptr = sqlite_value_blob(val) as *const u8;
        let len = sqlite_value_bytes(val) as usize;
        if ptr.is_null() {
            return None;
        }
        Some(slice::from_raw_parts(ptr, len).to_vec())
    }
}

fn read_required_blob(val: *mut sqlite3_value) -> Result<Vec<u8>, ()> {
    match read_blob(val) {
        Some(v) => Ok(v),
        None => Err(()),
    }
}

fn read_blob16(val: *mut sqlite3_value) -> Result<[u8; 16], ()> {
    let bytes = read_required_blob(val)?;
    if bytes.len() != 16 {
        return Err(());
    }
    let mut out = [0u8; 16];
    out.copy_from_slice(&bytes);
    Ok(out)
}

fn read_optional_blob16(val: *mut sqlite3_value) -> Result<Option<[u8; 16]>, ()> {
    unsafe {
        if sqlite_value_type(val) == SQLITE_NULL as c_int {
            return Ok(None);
        }
    }
    Ok(Some(read_blob16(val)?))
}

fn read_text(val: *mut sqlite3_value) -> String {
    unsafe {
        let ptr = sqlite_value_text(val) as *const u8;
        let len = sqlite_value_bytes(val) as usize;
        if ptr.is_null() || len == 0 {
            return String::new();
        }
        std::str::from_utf8(slice::from_raw_parts(ptr, len)).unwrap_or("").to_string()
    }
}

fn select_head_lamport(db: *mut sqlite3) -> Result<Lamport, c_int> {
    let sql = CString::new("SELECT COALESCE(MAX(lamport), 0) FROM ops").expect("head lamport sql");
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
    let value = unsafe { sqlite_column_int64(stmt, 0) }.max(0) as u64;
    let finalize_rc = unsafe { sqlite_finalize(stmt) };
    if finalize_rc != SQLITE_OK as c_int {
        return Err(finalize_rc);
    }
    Ok(value)
}

fn select_replica_max_counter(db: *mut sqlite3, replica: &[u8]) -> Result<u64, c_int> {
    let sql = CString::new("SELECT COALESCE(MAX(counter), 0) FROM ops WHERE replica = ?1")
        .expect("replica max counter sql");
    let mut stmt: *mut sqlite3_stmt = null_mut();
    let rc = sqlite_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, null_mut());
    if rc != SQLITE_OK as c_int {
        return Err(rc);
    }
    let bind_rc = unsafe {
        sqlite_bind_blob(
            stmt,
            1,
            replica.as_ptr() as *const c_void,
            replica.len() as c_int,
            None,
        )
    };
    if bind_rc != SQLITE_OK as c_int {
        unsafe { sqlite_finalize(stmt) };
        return Err(bind_rc);
    }
    let step_rc = unsafe { sqlite_step(stmt) };
    if step_rc != SQLITE_ROW as c_int {
        unsafe { sqlite_finalize(stmt) };
        return Err(SQLITE_ERROR as c_int);
    }
    let value = unsafe { sqlite_column_int64(stmt, 0) }.max(0) as u64;
    let finalize_rc = unsafe { sqlite_finalize(stmt) };
    if finalize_rc != SQLITE_OK as c_int {
        return Err(finalize_rc);
    }
    Ok(value)
}

fn next_local_meta(db: *mut sqlite3, replica: &[u8]) -> Result<(Lamport, u64), c_int> {
    let head_lamport = select_head_lamport(db)?;
    let counter = select_replica_max_counter(db, replica)?;
    Ok((head_lamport.saturating_add(1), counter.saturating_add(1)))
}

fn make_seed(replica: &[u8], counter: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(replica.len() + 8);
    out.extend_from_slice(replica);
    out.extend_from_slice(&counter.to_be_bytes());
    out
}

fn subtree_known_state_bytes(db: *mut sqlite3, node: [u8; 16]) -> Result<Vec<u8>, c_int> {
    ensure_materialized(db)?;

    let node_store = SqliteNodeStore::prepare(db).map_err(|_| SQLITE_ERROR as c_int)?;
    let tree = TreeCrdt::with_node_store(
        ReplicaId::new(b"sqlite-ext"),
        NoopStorage::default(),
        LamportClock::default(),
        node_store,
    )
    .map_err(|_| SQLITE_ERROR as c_int)?;

    let node_id = NodeId(u128::from_be_bytes(node));
    let vv = tree.subtree_version_vector(node_id).map_err(|_| SQLITE_ERROR as c_int)?;

    serde_json::to_vec(&vv).map_err(|_| SQLITE_ERROR as c_int)
}

#[derive(serde::Serialize)]
struct JsonOp {
    replica: Vec<u8>,
    counter: u64,
    lamport: Lamport,
    kind: String,
    parent: Option<[u8; 16]>,
    node: [u8; 16],
    new_parent: Option<[u8; 16]>,
    order_key: Option<Vec<u8>>,
    known_state: Option<Vec<u8>>,
    payload: Option<Vec<u8>>,
}

pub(super) unsafe extern "C" fn treecrdt_local_insert(
    ctx: *mut sqlite3_context,
    argc: c_int,
    argv: *mut *mut sqlite3_value,
) {
    if let Err(rc) = ensure_api_initialized() {
        sqlite_result_error_code(ctx, rc);
        return;
    }

    if argc != 6 {
        sqlite_result_error(
            ctx,
            b"treecrdt_local_insert expects 6 args (replica,parent,node,placement,after,payload)\0"
                .as_ptr() as *const c_char,
        );
        return;
    }

    let args = unsafe { slice::from_raw_parts(argv, argc as usize) };
    let replica = match read_required_blob(args[0]) {
        Ok(v) => v,
        Err(_) => {
            sqlite_result_error(
                ctx,
                b"treecrdt_local_insert: NULL replica\0".as_ptr() as *const c_char,
            );
            return;
        }
    };
    let parent = match read_blob16(args[1]) {
        Ok(v) => v,
        Err(_) => {
            sqlite_result_error(
                ctx,
                b"treecrdt_local_insert: parent must be 16-byte BLOB\0".as_ptr() as *const c_char,
            );
            return;
        }
    };
    let node = match read_blob16(args[2]) {
        Ok(v) => v,
        Err(_) => {
            sqlite_result_error(
                ctx,
                b"treecrdt_local_insert: node must be 16-byte BLOB\0".as_ptr() as *const c_char,
            );
            return;
        }
    };
    let placement = read_text(args[3]);
    let after = match read_optional_blob16(args[4]) {
        Ok(v) => v,
        Err(_) => {
            sqlite_result_error(
                ctx,
                b"treecrdt_local_insert: after must be 16-byte BLOB or NULL\0".as_ptr()
                    as *const c_char,
            );
            return;
        }
    };
    let payload = read_blob(args[5]);

    let db = sqlite_context_db_handle(ctx);
    let doc_id = match load_doc_id(db) {
        Ok(Some(v)) => v,
        Ok(None) => {
            sqlite_result_error(
                ctx,
                b"treecrdt_local_insert: doc_id not set (call treecrdt_set_doc_id)\0".as_ptr()
                    as *const c_char,
            );
            return;
        }
        Err(rc) => {
            sqlite_result_error_code(ctx, rc);
            return;
        }
    };

    let (lamport, counter) = match next_local_meta(db, &replica) {
        Ok(v) => v,
        Err(rc) => {
            sqlite_result_error_code(ctx, rc);
            return;
        }
    };
    let seed = make_seed(&replica, counter);

    let after_ref = after.as_ref();
    let order_key =
        match allocate_order_key(db, &parent, placement.as_str(), after_ref, None, &seed) {
            Ok(v) => v,
            Err(rc) => {
                sqlite_result_error_code(ctx, rc);
                return;
            }
        };

    let op = JsonAppendOp {
        replica: replica.clone(),
        counter,
        lamport,
        kind: "insert".to_string(),
        parent: Some(parent.to_vec()),
        node: node.to_vec(),
        new_parent: None,
        order_key: Some(order_key.clone()),
        known_state: None,
        payload: payload.clone(),
    };

    if let Err(rc) = append_ops_impl(
        db,
        &doc_id,
        "treecrdt_local_insert",
        std::slice::from_ref(&op),
    ) {
        sqlite_result_error_code(ctx, rc);
        return;
    }

    let out = JsonOp {
        replica,
        counter,
        lamport,
        kind: "insert".to_string(),
        parent: Some(parent),
        node,
        new_parent: None,
        order_key: Some(order_key),
        known_state: None,
        payload,
    };
    sqlite_result_json(ctx, &vec![out]);
}

pub(super) unsafe extern "C" fn treecrdt_local_move(
    ctx: *mut sqlite3_context,
    argc: c_int,
    argv: *mut *mut sqlite3_value,
) {
    if let Err(rc) = ensure_api_initialized() {
        sqlite_result_error_code(ctx, rc);
        return;
    }

    if argc != 5 {
        sqlite_result_error(
            ctx,
            b"treecrdt_local_move expects 5 args (replica,node,new_parent,placement,after)\0"
                .as_ptr() as *const c_char,
        );
        return;
    }

    let args = unsafe { slice::from_raw_parts(argv, argc as usize) };
    let replica = match read_required_blob(args[0]) {
        Ok(v) => v,
        Err(_) => {
            sqlite_result_error(
                ctx,
                b"treecrdt_local_move: NULL replica\0".as_ptr() as *const c_char,
            );
            return;
        }
    };
    let node = match read_blob16(args[1]) {
        Ok(v) => v,
        Err(_) => {
            sqlite_result_error(
                ctx,
                b"treecrdt_local_move: node must be 16-byte BLOB\0".as_ptr() as *const c_char,
            );
            return;
        }
    };
    let new_parent = match read_blob16(args[2]) {
        Ok(v) => v,
        Err(_) => {
            sqlite_result_error(
                ctx,
                b"treecrdt_local_move: new_parent must be 16-byte BLOB\0".as_ptr() as *const c_char,
            );
            return;
        }
    };
    let placement = read_text(args[3]);
    let after = match read_optional_blob16(args[4]) {
        Ok(v) => v,
        Err(_) => {
            sqlite_result_error(
                ctx,
                b"treecrdt_local_move: after must be 16-byte BLOB or NULL\0".as_ptr()
                    as *const c_char,
            );
            return;
        }
    };

    let db = sqlite_context_db_handle(ctx);
    let doc_id = match load_doc_id(db) {
        Ok(Some(v)) => v,
        Ok(None) => {
            sqlite_result_error(
                ctx,
                b"treecrdt_local_move: doc_id not set (call treecrdt_set_doc_id)\0".as_ptr()
                    as *const c_char,
            );
            return;
        }
        Err(rc) => {
            sqlite_result_error_code(ctx, rc);
            return;
        }
    };

    let (lamport, counter) = match next_local_meta(db, &replica) {
        Ok(v) => v,
        Err(rc) => {
            sqlite_result_error_code(ctx, rc);
            return;
        }
    };
    let seed = make_seed(&replica, counter);

    let after_ref = after.as_ref();
    let order_key = match allocate_order_key(
        db,
        &new_parent,
        placement.as_str(),
        after_ref,
        Some(&node),
        &seed,
    ) {
        Ok(v) => v,
        Err(rc) => {
            sqlite_result_error_code(ctx, rc);
            return;
        }
    };

    let op = JsonAppendOp {
        replica: replica.clone(),
        counter,
        lamport,
        kind: "move".to_string(),
        parent: None,
        node: node.to_vec(),
        new_parent: Some(new_parent.to_vec()),
        order_key: Some(order_key.clone()),
        known_state: None,
        payload: None,
    };

    if let Err(rc) = append_ops_impl(
        db,
        &doc_id,
        "treecrdt_local_move",
        std::slice::from_ref(&op),
    ) {
        sqlite_result_error_code(ctx, rc);
        return;
    }

    let out = JsonOp {
        replica,
        counter,
        lamport,
        kind: "move".to_string(),
        parent: None,
        node,
        new_parent: Some(new_parent),
        order_key: Some(order_key),
        known_state: None,
        payload: None,
    };
    sqlite_result_json(ctx, &vec![out]);
}

pub(super) unsafe extern "C" fn treecrdt_local_delete(
    ctx: *mut sqlite3_context,
    argc: c_int,
    argv: *mut *mut sqlite3_value,
) {
    if let Err(rc) = ensure_api_initialized() {
        sqlite_result_error_code(ctx, rc);
        return;
    }

    if argc != 2 {
        sqlite_result_error(
            ctx,
            b"treecrdt_local_delete expects 2 args (replica,node)\0".as_ptr() as *const c_char,
        );
        return;
    }

    let args = unsafe { slice::from_raw_parts(argv, argc as usize) };
    let replica = match read_required_blob(args[0]) {
        Ok(v) => v,
        Err(_) => {
            sqlite_result_error(
                ctx,
                b"treecrdt_local_delete: NULL replica\0".as_ptr() as *const c_char,
            );
            return;
        }
    };
    let node = match read_blob16(args[1]) {
        Ok(v) => v,
        Err(_) => {
            sqlite_result_error(
                ctx,
                b"treecrdt_local_delete: node must be 16-byte BLOB\0".as_ptr() as *const c_char,
            );
            return;
        }
    };

    let db = sqlite_context_db_handle(ctx);
    let doc_id = match load_doc_id(db) {
        Ok(Some(v)) => v,
        Ok(None) => {
            sqlite_result_error(
                ctx,
                b"treecrdt_local_delete: doc_id not set (call treecrdt_set_doc_id)\0".as_ptr()
                    as *const c_char,
            );
            return;
        }
        Err(rc) => {
            sqlite_result_error_code(ctx, rc);
            return;
        }
    };

    let (lamport, counter) = match next_local_meta(db, &replica) {
        Ok(v) => v,
        Err(rc) => {
            sqlite_result_error_code(ctx, rc);
            return;
        }
    };

    let known_state = match subtree_known_state_bytes(db, node) {
        Ok(v) => v,
        Err(rc) => {
            sqlite_result_error_code(ctx, rc);
            return;
        }
    };

    let op = JsonAppendOp {
        replica: replica.clone(),
        counter,
        lamport,
        kind: "delete".to_string(),
        parent: None,
        node: node.to_vec(),
        new_parent: None,
        order_key: None,
        known_state: Some(known_state.clone()),
        payload: None,
    };

    if let Err(rc) = append_ops_impl(
        db,
        &doc_id,
        "treecrdt_local_delete",
        std::slice::from_ref(&op),
    ) {
        sqlite_result_error_code(ctx, rc);
        return;
    }

    let out = JsonOp {
        replica,
        counter,
        lamport,
        kind: "delete".to_string(),
        parent: None,
        node,
        new_parent: None,
        order_key: None,
        known_state: Some(known_state),
        payload: None,
    };
    sqlite_result_json(ctx, &vec![out]);
}

pub(super) unsafe extern "C" fn treecrdt_local_payload(
    ctx: *mut sqlite3_context,
    argc: c_int,
    argv: *mut *mut sqlite3_value,
) {
    if let Err(rc) = ensure_api_initialized() {
        sqlite_result_error_code(ctx, rc);
        return;
    }

    if argc != 3 {
        sqlite_result_error(
            ctx,
            b"treecrdt_local_payload expects 3 args (replica,node,payload)\0".as_ptr()
                as *const c_char,
        );
        return;
    }

    let args = unsafe { slice::from_raw_parts(argv, argc as usize) };
    let replica = match read_required_blob(args[0]) {
        Ok(v) => v,
        Err(_) => {
            sqlite_result_error(
                ctx,
                b"treecrdt_local_payload: NULL replica\0".as_ptr() as *const c_char,
            );
            return;
        }
    };
    let node = match read_blob16(args[1]) {
        Ok(v) => v,
        Err(_) => {
            sqlite_result_error(
                ctx,
                b"treecrdt_local_payload: node must be 16-byte BLOB\0".as_ptr() as *const c_char,
            );
            return;
        }
    };
    let payload = read_blob(args[2]);

    let db = sqlite_context_db_handle(ctx);
    let doc_id = match load_doc_id(db) {
        Ok(Some(v)) => v,
        Ok(None) => {
            sqlite_result_error(
                ctx,
                b"treecrdt_local_payload: doc_id not set (call treecrdt_set_doc_id)\0".as_ptr()
                    as *const c_char,
            );
            return;
        }
        Err(rc) => {
            sqlite_result_error_code(ctx, rc);
            return;
        }
    };

    let (lamport, counter) = match next_local_meta(db, &replica) {
        Ok(v) => v,
        Err(rc) => {
            sqlite_result_error_code(ctx, rc);
            return;
        }
    };

    let op = JsonAppendOp {
        replica: replica.clone(),
        counter,
        lamport,
        kind: "payload".to_string(),
        parent: None,
        node: node.to_vec(),
        new_parent: None,
        order_key: None,
        known_state: None,
        payload: payload.clone(),
    };

    if let Err(rc) = append_ops_impl(
        db,
        &doc_id,
        "treecrdt_local_payload",
        std::slice::from_ref(&op),
    ) {
        sqlite_result_error_code(ctx, rc);
        return;
    }

    let out = JsonOp {
        replica,
        counter,
        lamport,
        kind: "payload".to_string(),
        parent: None,
        node,
        new_parent: None,
        order_key: None,
        known_state: None,
        payload,
    };
    sqlite_result_json(ctx, &vec![out]);
}

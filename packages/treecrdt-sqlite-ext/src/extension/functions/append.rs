use super::*;

/// Append an operation row to the `ops` table. Args:
/// replica BLOB, counter INT, lamport INT, kind TEXT, parent BLOB|null, node BLOB, new_parent BLOB|null, order_key BLOB|null, known_state_or_payload BLOB|null
pub(super) unsafe extern "C" fn treecrdt_append_op(
    ctx: *mut sqlite3_context,
    argc: c_int,
    argv: *mut *mut sqlite3_value,
) {
    if let Err(rc) = ensure_api_initialized() {
        sqlite_result_error_code(ctx, rc);
        return;
    }

    if argc != 9 {
        sqlite_result_error(
            ctx,
            b"treecrdt_append_op expects 9 args\0".as_ptr() as *const c_char,
        );
        return;
    }

    let db = sqlite_context_db_handle(ctx);
    let args = unsafe { std::slice::from_raw_parts(argv, argc as usize) };

    let doc_id = match load_doc_id(db) {
        Ok(Some(v)) => v,
        Ok(None) => {
            sqlite_result_error(
                ctx,
                b"treecrdt_append_op: doc_id not set (call treecrdt_set_doc_id)\0".as_ptr()
                    as *const c_char,
            );
            return;
        }
        Err(rc) => {
            sqlite_result_error_code(ctx, rc);
            return;
        }
    };

    let replica_ptr = unsafe { sqlite_value_blob(args[0]) } as *const u8;
    let replica_len = unsafe { sqlite_value_bytes(args[0]) } as usize;
    if replica_ptr.is_null() {
        sqlite_result_error(
            ctx,
            b"treecrdt_append_op: NULL replica\0".as_ptr() as *const c_char,
        );
        return;
    }
    let replica = unsafe { slice::from_raw_parts(replica_ptr, replica_len) }.to_vec();
    let counter_i64 = unsafe { sqlite_value_int64(args[1]) };
    if counter_i64 < 0 {
        sqlite_result_error(
            ctx,
            b"treecrdt_append_op: counter must be >= 0\0".as_ptr() as *const c_char,
        );
        return;
    }
    let counter = counter_i64 as u64;
    let lamport = unsafe { sqlite_value_int64(args[2]).max(0) as Lamport };

    let kind_ptr = unsafe { sqlite_value_text(args[3]) } as *const u8;
    let kind_len = unsafe { sqlite_value_bytes(args[3]) } as usize;
    let kind = if kind_ptr.is_null() {
        ""
    } else {
        std::str::from_utf8(unsafe { slice::from_raw_parts(kind_ptr, kind_len) }).unwrap_or("")
    };
    let kind = kind.to_string();

    let read_opt_blob = |val: *mut sqlite3_value| -> Option<Vec<u8>> {
        unsafe {
            if sqlite_value_type(val) == SQLITE_NULL as c_int {
                return None;
            }
            let ptr = sqlite_value_blob(val) as *const u8;
            let len = sqlite_value_bytes(val) as usize;
            if ptr.is_null() {
                None
            } else {
                Some(slice::from_raw_parts(ptr, len).to_vec())
            }
        }
    };

    let parent = read_opt_blob(args[4]);
    let node = match read_opt_blob(args[5]) {
        Some(v) => v,
        None => {
            sqlite_result_error(
                ctx,
                b"treecrdt_append_op: NULL node\0".as_ptr() as *const c_char,
            );
            return;
        }
    };
    let new_parent = read_opt_blob(args[6]);
    let order_key = read_opt_blob(args[7]);
    let known_state_or_payload = read_opt_blob(args[8]);

    let (known_state, payload) = match kind.as_str() {
        // Deletes must carry known_state (writer-side subtree version vector).
        "delete" => (known_state_or_payload, None),
        // Payload ops are represented as `kind = "payload"` plus an optional payload blob.
        // NULL payload clears.
        "payload" => (None, known_state_or_payload),
        // Inserts can carry an optional initial payload in the last arg.
        "insert" => (None, known_state_or_payload),
        _ => (known_state_or_payload, None),
    };

    if kind == "delete" {
        if known_state.as_ref().map_or(true, |bytes| bytes.is_empty()) {
            sqlite_result_error(
                ctx,
                b"treecrdt_append_op: delete op missing known_state\0".as_ptr() as *const c_char,
            );
            return;
        }
        if node.len() != 16 {
            sqlite_result_error(
                ctx,
                b"treecrdt_append_op: delete node must be 16-byte BLOB\0".as_ptr() as *const c_char,
            );
            return;
        }
    }

    if (kind == "insert" || kind == "move") && order_key.is_none() {
        sqlite_result_error(
            ctx,
            b"treecrdt_append_op: insert/move op missing order_key\0".as_ptr() as *const c_char,
        );
        return;
    }

    let op = JsonAppendOp {
        replica,
        counter,
        lamport,
        kind,
        parent,
        node,
        new_parent,
        order_key,
        known_state,
        payload,
    };

    match append_ops_impl(db, &doc_id, "treecrdt_append_op", std::slice::from_ref(&op)) {
        Ok(_) => sqlite_result_int(ctx, 1),
        Err(rc) => sqlite_result_error_code(ctx, rc),
    }
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct JsonAppendOp {
    pub(super) replica: Vec<u8>,
    pub(super) counter: u64,
    pub(super) lamport: Lamport,
    pub(super) kind: String,
    pub(super) parent: Option<Vec<u8>>,
    pub(super) node: Vec<u8>,
    pub(super) new_parent: Option<Vec<u8>>,
    pub(super) order_key: Option<Vec<u8>>,
    pub(super) known_state: Option<Vec<u8>>,
    #[serde(default)]
    pub(super) payload: Option<Vec<u8>>,
}

/// Batch append: accepts a single JSON array argument with fields matching the ops table.
pub(super) unsafe extern "C" fn treecrdt_append_ops(
    ctx: *mut sqlite3_context,
    argc: c_int,
    argv: *mut *mut sqlite3_value,
) {
    if argc != 1 {
        sqlite_result_error(
            ctx,
            b"treecrdt_append_ops expects a single JSON array argument\0".as_ptr() as *const c_char,
        );
        return;
    }

    let args = unsafe { std::slice::from_raw_parts(argv, argc as usize) };
    let json_ptr = unsafe { sqlite_value_text(args[0]) };
    let json_len = unsafe { sqlite_value_bytes(args[0]) } as usize;
    if json_ptr.is_null() || json_len == 0 {
        sqlite_result_error(
            ctx,
            b"treecrdt_append_ops expects non-empty JSON\0".as_ptr() as *const c_char,
        );
        return;
    }

    let json_bytes = unsafe { std::slice::from_raw_parts(json_ptr as *const u8, json_len) };
    let json_str = match std::str::from_utf8(json_bytes) {
        Ok(s) => s,
        Err(_) => {
            sqlite_result_error(
                ctx,
                b"treecrdt_append_ops invalid UTF-8\0".as_ptr() as *const c_char,
            );
            return;
        }
    };
    let ops: Vec<JsonAppendOp> = match serde_json::from_str(json_str) {
        Ok(v) => v,
        Err(_) => {
            sqlite_result_error(
                ctx,
                b"treecrdt_append_ops failed to parse JSON array\0".as_ptr() as *const c_char,
            );
            return;
        }
    };
    if ops.is_empty() {
        sqlite_result_int(ctx, 0);
        return;
    }

    let db = sqlite_context_db_handle(ctx);
    let doc_id = match load_doc_id(db) {
        Ok(Some(v)) => v,
        Ok(None) => {
            sqlite_result_error(
                ctx,
                b"treecrdt_append_ops: doc_id not set (call treecrdt_set_doc_id)\0".as_ptr()
                    as *const c_char,
            );
            return;
        }
        Err(rc) => {
            sqlite_result_error_code(ctx, rc);
            return;
        }
    };

    // Defensive deletion requires the writer's causal "known_state" so receivers don't invent
    // awareness from their own history (which breaks revival semantics).
    for op in &ops {
        if op.kind == "delete" && op.known_state.as_ref().map_or(true, |bytes| bytes.is_empty()) {
            sqlite_result_error(
                ctx,
                b"treecrdt_append_ops: delete op missing known_state\0".as_ptr() as *const c_char,
            );
            return;
        }
        if (op.kind == "insert" || op.kind == "move") && op.order_key.is_none() {
            sqlite_result_error(
                ctx,
                b"treecrdt_append_ops: insert/move op missing order_key\0".as_ptr() as *const c_char,
            );
            return;
        }
    }

    match append_ops_impl(db, &doc_id, "treecrdt_append_ops", &ops) {
        Ok(inserted) => sqlite_result_int(ctx, inserted as c_int),
        Err(rc) => sqlite_result_error_code(ctx, rc),
    }
}

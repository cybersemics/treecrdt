//! SQLite extension implementation implemented against the SQLite C API.
//! This is intentionally minimal: it proves the cross-target build (native SQLite + wa-sqlite)
//! and registers a basic function to verify loading. Additional virtual tables/functions will
//! bridge to `treecrdt-core`.

#![allow(non_snake_case)]

mod materialize;
mod node_store;
mod schema;
mod sqlite_api;
use materialize::{append_ops_impl, ensure_materialized};
use schema::*;
use sqlite_api::*;

use std::collections::HashSet;
use std::ffi::CString;
use std::os::raw::{c_char, c_int, c_void};
use std::ptr::null_mut;
use std::slice;

use treecrdt_core::{Lamport, NodeId, NodeStore, VersionVector};

#[cfg(any(feature = "ext-sqlite", feature = "static-link"))]
use serde_json;

const OPREF_V0_DOMAIN: &[u8] = b"treecrdt/opref/v0";
const OPREF_V0_WIDTH: usize = 16;

fn derive_op_ref_v0(doc_id: &[u8], replica: &[u8], counter: u64) -> [u8; OPREF_V0_WIDTH] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(OPREF_V0_DOMAIN);
    hasher.update(doc_id);
    hasher.update(&(replica.len() as u32).to_be_bytes());
    hasher.update(replica);
    hasher.update(&counter.to_be_bytes());
    let hash = hasher.finalize();
    let mut out = [0u8; OPREF_V0_WIDTH];
    out.copy_from_slice(&hash.as_bytes()[0..OPREF_V0_WIDTH]);
    out
}

#[cfg(any(feature = "ext-sqlite", feature = "static-link"))]
fn deserialize_version_vector(bytes: &[u8]) -> Result<VersionVector, c_int> {
    match serde_json::from_slice(bytes) {
        Ok(vv) => Ok(vv),
        Err(_) => Err(SQLITE_ERROR as c_int),
    }
}

#[derive(Default)]
struct NoopStorage;

impl treecrdt_core::Storage for NoopStorage {
    fn apply(&mut self, _op: treecrdt_core::Operation) -> treecrdt_core::Result<()> {
        Ok(())
    }

    fn load_since(
        &self,
        _lamport: Lamport,
    ) -> treecrdt_core::Result<Vec<treecrdt_core::Operation>> {
        Ok(Vec::new())
    }

    fn latest_lamport(&self) -> Lamport {
        0
    }

    fn snapshot(&self) -> treecrdt_core::Result<treecrdt_core::Snapshot> {
        Ok(treecrdt_core::Snapshot { head: 0 })
    }
}

/// A simple scalar function that returns the crate version string. Useful to confirm
/// the extension is loaded in native SQLite or wa-sqlite builds.
unsafe extern "C" fn treecrdt_version_fn(
    ctx: *mut sqlite3_context,
    _argc: c_int,
    _argv: *mut *mut sqlite3_value,
) {
    let ver = CString::new(env!("CARGO_PKG_VERSION")).expect("static version");
    let len = ver.as_bytes().len() as c_int;
    // Transfer ownership to SQLite; we provide a destructor to free the CString.
    let ptr = ver.into_raw();
    sqlite_result_text(ctx, ptr as *const c_char, len, Some(drop_cstring));
}

unsafe extern "C" fn drop_cstring(ptr: *mut c_void) {
    if !ptr.is_null() {
        unsafe {
            drop(CString::from_raw(ptr as *mut c_char));
        }
    }
}

#[no_mangle]
pub extern "C" fn sqlite3_treecrdt_init(
    db: *mut sqlite3,
    pz_err_msg: *mut *mut c_char,
    p_api: *const sqlite3_api_routines,
) -> c_int {
    if let Err(rc) = unsafe { set_sqlite3_api(p_api) } {
        return rc;
    }

    // Create op-log schema if missing.
    if let Err(rc) = ensure_schema(db) {
        unsafe {
            if !pz_err_msg.is_null() {
                let msg = CString::new("treecrdt schema init failed")
                    .unwrap_or_else(|_| CString::new("treecrdt init failed").unwrap());
                *pz_err_msg = msg.into_raw();
            }
        }
        return rc;
    }

    let rc = {
        let name = CString::new("treecrdt_version").expect("static name");
        sqlite_create_function_v2(
            db,
            name.as_ptr(),
            0,
            SQLITE_UTF8 as c_int,
            null_mut(),
            Some(treecrdt_version_fn),
            None,
            None,
            None,
        )
    };

    let rc_append = {
        let name = CString::new("treecrdt_append_op").expect("static name");
        sqlite_create_function_v2(
            db,
            name.as_ptr(),
            9,
            SQLITE_UTF8 as c_int,
            null_mut(),
            Some(treecrdt_append_op),
            None,
            None,
            None,
        )
    };
    let _rc_append_batch = {
        let name = CString::new("treecrdt_append_ops").expect("static name");
        sqlite_create_function_v2(
            db,
            name.as_ptr(),
            1,
            SQLITE_UTF8 as c_int,
            null_mut(),
            Some(treecrdt_append_ops),
            None,
            None,
            None,
        )
    };

    let rc_set_doc_id = {
        let name = CString::new("treecrdt_set_doc_id").expect("static name");
        sqlite_create_function_v2(
            db,
            name.as_ptr(),
            1,
            SQLITE_UTF8 as c_int,
            null_mut(),
            Some(treecrdt_set_doc_id),
            None,
            None,
            None,
        )
    };
    let rc_doc_id = {
        let name = CString::new("treecrdt_doc_id").expect("static name");
        sqlite_create_function_v2(
            db,
            name.as_ptr(),
            0,
            SQLITE_UTF8 as c_int,
            null_mut(),
            Some(treecrdt_doc_id),
            None,
            None,
            None,
        )
    };

    let rc_oprefs_all = {
        let name = CString::new("treecrdt_oprefs_all").expect("static name");
        sqlite_create_function_v2(
            db,
            name.as_ptr(),
            0,
            SQLITE_UTF8 as c_int,
            null_mut(),
            Some(treecrdt_oprefs_all),
            None,
            None,
            None,
        )
    };
    let rc_oprefs_children = {
        let name = CString::new("treecrdt_oprefs_children").expect("static name");
        sqlite_create_function_v2(
            db,
            name.as_ptr(),
            1,
            SQLITE_UTF8 as c_int,
            null_mut(),
            Some(treecrdt_oprefs_children),
            None,
            None,
            None,
        )
    };
    let rc_tree_children = {
        let name = CString::new("treecrdt_tree_children").expect("static name");
        sqlite_create_function_v2(
            db,
            name.as_ptr(),
            1,
            SQLITE_UTF8 as c_int,
            null_mut(),
            Some(treecrdt_tree_children),
            None,
            None,
            None,
        )
    };
    let rc_tree_dump = {
        let name = CString::new("treecrdt_tree_dump").expect("static name");
        sqlite_create_function_v2(
            db,
            name.as_ptr(),
            0,
            SQLITE_UTF8 as c_int,
            null_mut(),
            Some(treecrdt_tree_dump),
            None,
            None,
            None,
        )
    };
    let rc_tree_node_count = {
        let name = CString::new("treecrdt_tree_node_count").expect("static name");
        sqlite_create_function_v2(
            db,
            name.as_ptr(),
            0,
            SQLITE_UTF8 as c_int,
            null_mut(),
            Some(treecrdt_tree_node_count),
            None,
            None,
            None,
        )
    };
    let rc_head_lamport = {
        let name = CString::new("treecrdt_head_lamport").expect("static name");
        sqlite_create_function_v2(
            db,
            name.as_ptr(),
            0,
            SQLITE_UTF8 as c_int,
            null_mut(),
            Some(treecrdt_head_lamport),
            None,
            None,
            None,
        )
    };
    let rc_replica_max_counter = {
        let name = CString::new("treecrdt_replica_max_counter").expect("static name");
        sqlite_create_function_v2(
            db,
            name.as_ptr(),
            1,
            SQLITE_UTF8 as c_int,
            null_mut(),
            Some(treecrdt_replica_max_counter),
            None,
            None,
            None,
        )
    };
    let rc_ops_by_oprefs = {
        let name = CString::new("treecrdt_ops_by_oprefs").expect("static name");
        sqlite_create_function_v2(
            db,
            name.as_ptr(),
            1,
            SQLITE_UTF8 as c_int,
            null_mut(),
            Some(treecrdt_ops_by_oprefs),
            None,
            None,
            None,
        )
    };

    let rc_since = {
        let name = CString::new("treecrdt_ops_since").expect("static name");
        // -1 allows 1 or 2 args (lamport, optional root node filter)
        sqlite_create_function_v2(
            db,
            name.as_ptr(),
            -1,
            SQLITE_UTF8 as c_int,
            null_mut(),
            Some(treecrdt_ops_since),
            None,
            None,
            None,
        )
    };

    if rc != SQLITE_OK as c_int
        || rc_append != SQLITE_OK as c_int
        || rc_set_doc_id != SQLITE_OK as c_int
        || rc_doc_id != SQLITE_OK as c_int
        || rc_oprefs_all != SQLITE_OK as c_int
        || rc_oprefs_children != SQLITE_OK as c_int
        || rc_tree_children != SQLITE_OK as c_int
        || rc_tree_dump != SQLITE_OK as c_int
        || rc_tree_node_count != SQLITE_OK as c_int
        || rc_head_lamport != SQLITE_OK as c_int
        || rc_replica_max_counter != SQLITE_OK as c_int
        || rc_ops_by_oprefs != SQLITE_OK as c_int
        || rc_since != SQLITE_OK as c_int
    {
        unsafe {
            if !pz_err_msg.is_null() {
                let msg = CString::new("treecrdt extension init failed")
                    .unwrap_or_else(|_| CString::new("treecrdt init failed").unwrap());
                *pz_err_msg = msg.into_raw();
            }
        }
        return if rc != SQLITE_OK as c_int {
            rc
        } else if rc_append != SQLITE_OK as c_int {
            rc_append
        } else if rc_set_doc_id != SQLITE_OK as c_int {
            rc_set_doc_id
        } else if rc_doc_id != SQLITE_OK as c_int {
            rc_doc_id
        } else if rc_oprefs_all != SQLITE_OK as c_int {
            rc_oprefs_all
        } else if rc_oprefs_children != SQLITE_OK as c_int {
            rc_oprefs_children
        } else if rc_tree_children != SQLITE_OK as c_int {
            rc_tree_children
        } else if rc_tree_dump != SQLITE_OK as c_int {
            rc_tree_dump
        } else if rc_tree_node_count != SQLITE_OK as c_int {
            rc_tree_node_count
        } else if rc_head_lamport != SQLITE_OK as c_int {
            rc_head_lamport
        } else if rc_replica_max_counter != SQLITE_OK as c_int {
            rc_replica_max_counter
        } else if rc_ops_by_oprefs != SQLITE_OK as c_int {
            rc_ops_by_oprefs
        } else {
            rc_since
        };
    }

    SQLITE_OK as c_int
}

// Keep the init symbol alive in static-link builds even if it appears unreferenced.
#[cfg(feature = "static-link")]
#[used]
static _TREECRDT_INIT_REF: unsafe extern "C" fn(
    *mut sqlite3,
    *mut *mut c_char,
    *const sqlite3_api_routines,
) -> c_int = sqlite3_treecrdt_init;

unsafe extern "C" fn treecrdt_set_doc_id(
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
    }

    // No backfill/migration: callers must set `doc_id` before appending ops so `op_ref`
    // is always computed at write time.
    sqlite_result_int(ctx, 1);
}

unsafe extern "C" fn treecrdt_doc_id(
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

/// Append an operation row to the `ops` table. Args:
/// replica BLOB, counter INT, lamport INT, kind TEXT, parent BLOB|null, node BLOB, new_parent BLOB|null, position INT|null, known_state BLOB|null
unsafe extern "C" fn treecrdt_append_op(
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
    let position = unsafe {
        if sqlite_value_type(args[7]) == SQLITE_NULL as c_int {
            None
        } else {
            let v = sqlite_value_int64(args[7]);
            if v < 0 {
                None
            } else {
                Some(v as u64)
            }
        }
    };
    let known_state = read_opt_blob(args[8]);

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

    let op = JsonAppendOp {
        replica,
        counter,
        lamport,
        kind,
        parent,
        node,
        new_parent,
        position,
        known_state,
    };

    match append_ops_impl(db, &doc_id, "treecrdt_append_op", std::slice::from_ref(&op)) {
        Ok(_) => sqlite_result_int(ctx, 1),
        Err(rc) => sqlite_result_error_code(ctx, rc),
    }
}

#[derive(serde::Deserialize)]
struct JsonAppendOp {
    replica: Vec<u8>,
    counter: u64,
    lamport: Lamport,
    kind: String,
    parent: Option<Vec<u8>>,
    node: Vec<u8>,
    new_parent: Option<Vec<u8>>,
    position: Option<u64>,
    known_state: Option<Vec<u8>>,
}

/// Batch append: accepts a single JSON array argument with fields matching the ops table.
unsafe extern "C" fn treecrdt_append_ops(
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
    }

    match append_ops_impl(db, &doc_id, "treecrdt_append_ops", &ops) {
        Ok(inserted) => sqlite_result_int(ctx, inserted as c_int),
        Err(rc) => sqlite_result_error_code(ctx, rc),
    }
}

unsafe extern "C" fn treecrdt_oprefs_all(
    ctx: *mut sqlite3_context,
    argc: c_int,
    _argv: *mut *mut sqlite3_value,
) {
    if argc != 0 {
        sqlite_result_error(
            ctx,
            b"treecrdt_oprefs_all expects 0 args\0".as_ptr() as *const c_char,
        );
        return;
    }

    let db = sqlite_context_db_handle(ctx);
    let sql = CString::new(
        "SELECT op_ref FROM ops WHERE op_ref IS NOT NULL ORDER BY lamport, replica, counter",
    )
    .expect("static sql");
    let mut stmt: *mut sqlite3_stmt = null_mut();
    let rc = sqlite_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, null_mut());
    if rc != SQLITE_OK as c_int {
        sqlite_result_error_code(ctx, rc);
        return;
    }

    let mut refs: Vec<Vec<u8>> = Vec::new();
    loop {
        let step_rc = unsafe { sqlite_step(stmt) };
        if step_rc == SQLITE_ROW as c_int {
            let ptr = unsafe { sqlite_column_blob(stmt, 0) } as *const u8;
            let len = unsafe { sqlite_column_bytes(stmt, 0) } as usize;
            if ptr.is_null() || len != OPREF_V0_WIDTH {
                unsafe { sqlite_finalize(stmt) };
                sqlite_result_error(
                    ctx,
                    b"treecrdt_oprefs_all: invalid op_ref (call treecrdt_set_doc_id)\0".as_ptr()
                        as *const c_char,
                );
                return;
            }
            refs.push(unsafe { slice::from_raw_parts(ptr, len) }.to_vec());
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

    let json = match serde_json::to_string(&refs) {
        Ok(v) => v,
        Err(_) => {
            sqlite_result_error_code(ctx, SQLITE_ERROR as c_int);
            return;
        }
    };
    if let Ok(cstr) = CString::new(json) {
        let len = cstr.as_bytes().len() as c_int;
        let ptr = cstr.into_raw();
        sqlite_result_text(ctx, ptr as *const c_char, len, Some(drop_cstring));
    } else {
        sqlite_result_error_code(ctx, SQLITE_ERROR as c_int);
    }
}

unsafe extern "C" fn treecrdt_oprefs_children(
    ctx: *mut sqlite3_context,
    argc: c_int,
    argv: *mut *mut sqlite3_value,
) {
    if argc != 1 {
        sqlite_result_error(
            ctx,
            b"treecrdt_oprefs_children expects 1 arg (parent)\0".as_ptr() as *const c_char,
        );
        return;
    }

    let args = unsafe { slice::from_raw_parts(argv, argc as usize) };
    let parent_ptr = unsafe { sqlite_value_blob(args[0]) } as *const u8;
    let parent_len = unsafe { sqlite_value_bytes(args[0]) } as usize;
    if parent_ptr.is_null() || parent_len != 16 {
        sqlite_result_error(
            ctx,
            b"treecrdt_oprefs_children: parent must be 16-byte BLOB\0".as_ptr() as *const c_char,
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

    let sql = CString::new("SELECT op_ref FROM oprefs_children WHERE parent = ?1 ORDER BY seq")
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

    let mut refs: Vec<Vec<u8>> = Vec::new();
    loop {
        let step_rc = unsafe { sqlite_step(stmt) };
        if step_rc == SQLITE_ROW as c_int {
            let ptr = unsafe { sqlite_column_blob(stmt, 0) } as *const u8;
            let len = unsafe { sqlite_column_bytes(stmt, 0) } as usize;
            if ptr.is_null() || len != OPREF_V0_WIDTH {
                unsafe { sqlite_finalize(stmt) };
                sqlite_result_error_code(ctx, SQLITE_ERROR as c_int);
                return;
            }
            refs.push(unsafe { slice::from_raw_parts(ptr, len) }.to_vec());
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

    let json = match serde_json::to_string(&refs) {
        Ok(v) => v,
        Err(_) => {
            sqlite_result_error_code(ctx, SQLITE_ERROR as c_int);
            return;
        }
    };
    if let Ok(cstr) = CString::new(json) {
        let len = cstr.as_bytes().len() as c_int;
        let ptr = cstr.into_raw();
        sqlite_result_text(ctx, ptr as *const c_char, len, Some(drop_cstring));
    } else {
        sqlite_result_error_code(ctx, SQLITE_ERROR as c_int);
    }
}

unsafe extern "C" fn treecrdt_tree_children(
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
        "SELECT node FROM tree_nodes WHERE parent = ?1 AND tombstone = 0 ORDER BY pos",
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

    let json = match serde_json::to_string(&nodes) {
        Ok(v) => v,
        Err(_) => {
            sqlite_result_error_code(ctx, SQLITE_ERROR as c_int);
            return;
        }
    };
    if let Ok(cstr) = CString::new(json) {
        let len = cstr.as_bytes().len() as c_int;
        let ptr = cstr.into_raw();
        sqlite_result_text(ctx, ptr as *const c_char, len, Some(drop_cstring));
    } else {
        sqlite_result_error_code(ctx, SQLITE_ERROR as c_int);
    }
}

#[derive(serde::Serialize)]
struct JsonTreeNode {
    node: [u8; 16],
    parent: Option<[u8; 16]>,
    pos: Option<u64>,
    tombstone: bool,
}

unsafe extern "C" fn treecrdt_tree_dump(
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

    let sql = CString::new("SELECT node,parent,pos,tombstone FROM tree_nodes").expect("static sql");
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
            let pos = unsafe { column_int_opt(stmt, 2) };
            let tombstone = unsafe { sqlite_column_int64(stmt, 3) } != 0;
            rows.push(JsonTreeNode {
                node,
                parent,
                pos,
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

    let json = match serde_json::to_string(&rows) {
        Ok(v) => v,
        Err(_) => {
            sqlite_result_error_code(ctx, SQLITE_ERROR as c_int);
            return;
        }
    };
    if let Ok(cstr) = CString::new(json) {
        let len = cstr.as_bytes().len() as c_int;
        let ptr = cstr.into_raw();
        sqlite_result_text(ctx, ptr as *const c_char, len, Some(drop_cstring));
    } else {
        sqlite_result_error_code(ctx, SQLITE_ERROR as c_int);
    }
}

unsafe extern "C" fn treecrdt_tree_node_count(
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

unsafe extern "C" fn treecrdt_head_lamport(
    ctx: *mut sqlite3_context,
    argc: c_int,
    _argv: *mut *mut sqlite3_value,
) {
    if argc != 0 {
        sqlite_result_error(
            ctx,
            b"treecrdt_head_lamport expects 0 args\0".as_ptr() as *const c_char,
        );
        return;
    }

    let db = sqlite_context_db_handle(ctx);
    let sql = CString::new(
        "SELECT lamport FROM ops ORDER BY lamport DESC, replica DESC, counter DESC LIMIT 1",
    )
    .expect("static sql");
    let mut stmt: *mut sqlite3_stmt = null_mut();
    let rc = sqlite_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, null_mut());
    if rc != SQLITE_OK as c_int {
        sqlite_result_error_code(ctx, rc);
        return;
    }

    let step_rc = unsafe { sqlite_step(stmt) };
    let lamport = if step_rc == SQLITE_ROW as c_int {
        unsafe { sqlite_column_int64(stmt, 0) }
    } else {
        0
    };
    let finalize_rc = unsafe { sqlite_finalize(stmt) };
    if finalize_rc != SQLITE_OK as c_int {
        sqlite_result_error_code(ctx, finalize_rc);
        return;
    }
    sqlite_result_int64(ctx, lamport);
}

unsafe extern "C" fn treecrdt_replica_max_counter(
    ctx: *mut sqlite3_context,
    argc: c_int,
    argv: *mut *mut sqlite3_value,
) {
    if argc != 1 {
        sqlite_result_error(
            ctx,
            b"treecrdt_replica_max_counter expects 1 arg (replica)\0".as_ptr() as *const c_char,
        );
        return;
    }

    let args = unsafe { slice::from_raw_parts(argv, argc as usize) };
    let replica_ptr = unsafe { sqlite_value_blob(args[0]) } as *const u8;
    let replica_len = unsafe { sqlite_value_bytes(args[0]) } as usize;
    if replica_ptr.is_null() {
        sqlite_result_error(
            ctx,
            b"treecrdt_replica_max_counter: replica must be BLOB\0".as_ptr() as *const c_char,
        );
        return;
    }

    let db = sqlite_context_db_handle(ctx);
    let sql = CString::new("SELECT COALESCE(MAX(counter), 0) FROM ops WHERE replica = ?1")
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
            replica_ptr as *const c_void,
            replica_len as c_int,
            None,
        )
    };
    if bind_rc != SQLITE_OK as c_int {
        unsafe { sqlite_finalize(stmt) };
        sqlite_result_error_code(ctx, bind_rc);
        return;
    }

    let step_rc = unsafe { sqlite_step(stmt) };
    let max_counter = if step_rc == SQLITE_ROW as c_int {
        unsafe { sqlite_column_int64(stmt, 0) }
    } else {
        0
    };
    let finalize_rc = unsafe { sqlite_finalize(stmt) };
    if finalize_rc != SQLITE_OK as c_int {
        sqlite_result_error_code(ctx, finalize_rc);
        return;
    }
    sqlite_result_int64(ctx, max_counter);
}

unsafe extern "C" fn treecrdt_ops_by_oprefs(
    ctx: *mut sqlite3_context,
    argc: c_int,
    argv: *mut *mut sqlite3_value,
) {
    if argc != 1 {
        sqlite_result_error(
            ctx,
            b"treecrdt_ops_by_oprefs expects 1 arg (json)\0".as_ptr() as *const c_char,
        );
        return;
    }
    let args = unsafe { slice::from_raw_parts(argv, argc as usize) };
    let json_ptr = unsafe { sqlite_value_text(args[0]) };
    let json_len = unsafe { sqlite_value_bytes(args[0]) } as usize;
    if json_ptr.is_null() || json_len == 0 {
        sqlite_result_error(
            ctx,
            b"treecrdt_ops_by_oprefs expects non-empty JSON\0".as_ptr() as *const c_char,
        );
        return;
    }
    let json_bytes = unsafe { slice::from_raw_parts(json_ptr as *const u8, json_len) };
    let json_str = match std::str::from_utf8(json_bytes) {
        Ok(s) => s,
        Err(_) => {
            sqlite_result_error(
                ctx,
                b"treecrdt_ops_by_oprefs invalid UTF-8\0".as_ptr() as *const c_char,
            );
            return;
        }
    };

    let op_refs: Vec<Vec<u8>> = match serde_json::from_str(json_str) {
        Ok(v) => v,
        Err(_) => {
            sqlite_result_error(
                ctx,
                b"treecrdt_ops_by_oprefs failed to parse JSON array\0".as_ptr() as *const c_char,
            );
            return;
        }
    };
    if op_refs.is_empty() {
        sqlite_result_text(ctx, b"[]\0".as_ptr() as *const c_char, 2, None);
        return;
    }

    let db = sqlite_context_db_handle(ctx);
    let sql = CString::new(
        "SELECT replica,counter,lamport,kind,parent,node,new_parent,position,known_state \
         FROM ops \
         WHERE op_ref = ?1",
    )
    .expect("static sql");
    let mut stmt: *mut sqlite3_stmt = null_mut();
    let rc = sqlite_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, null_mut());
    if rc != SQLITE_OK as c_int {
        sqlite_result_error_code(ctx, rc);
        return;
    }

    let mut ops: Vec<JsonOp> = Vec::new();
    for op_ref in op_refs {
        if op_ref.len() != OPREF_V0_WIDTH {
            unsafe { sqlite_finalize(stmt) };
            sqlite_result_error(
                ctx,
                b"treecrdt_ops_by_oprefs: op_ref must be 16 bytes\0".as_ptr() as *const c_char,
            );
            return;
        }
        unsafe {
            sqlite_clear_bindings(stmt);
            sqlite_reset(stmt);
        }
        let bind_rc = unsafe {
            sqlite_bind_blob(
                stmt,
                1,
                op_ref.as_ptr() as *const c_void,
                op_ref.len() as c_int,
                None,
            )
        };
        if bind_rc != SQLITE_OK as c_int {
            unsafe { sqlite_finalize(stmt) };
            sqlite_result_error_code(ctx, bind_rc);
            return;
        }

        let step_rc = unsafe { sqlite_step(stmt) };
        if step_rc == SQLITE_ROW as c_int {
            match read_row(stmt) {
                Ok(op) => ops.push(op),
                Err(rc) => {
                    unsafe { sqlite_finalize(stmt) };
                    sqlite_result_error_code(ctx, rc);
                    return;
                }
            }
        } else if step_rc == SQLITE_DONE as c_int {
            unsafe { sqlite_finalize(stmt) };
            sqlite_result_error(
                ctx,
                b"treecrdt_ops_by_oprefs: op_ref not found\0".as_ptr() as *const c_char,
            );
            return;
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

    let json = match serde_json::to_string(&ops) {
        Ok(j) => j,
        Err(_) => {
            sqlite_result_error_code(ctx, SQLITE_ERROR as c_int);
            return;
        }
    };

    if let Ok(cstr) = CString::new(json) {
        let len = cstr.as_bytes().len() as c_int;
        let ptr = cstr.into_raw();
        sqlite_result_text(ctx, ptr as *const c_char, len, Some(drop_cstring));
    } else {
        sqlite_result_error_code(ctx, SQLITE_ERROR as c_int);
    }
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
    position: Option<u64>,
    known_state: Option<Vec<u8>>,
}

unsafe extern "C" fn treecrdt_ops_since(
    ctx: *mut sqlite3_context,
    argc: c_int,
    argv: *mut *mut sqlite3_value,
) {
    if !(argc == 1 || argc == 2) {
        sqlite_result_error(
            ctx,
            b"treecrdt_ops_since expects lamport [, root]\0".as_ptr() as *const c_char,
        );
        return;
    }

    let args = unsafe { std::slice::from_raw_parts(argv, argc as usize) };
    let lamport: Lamport = unsafe { sqlite_value_int64(args[0]) as Lamport };
    let root_filter = if argc == 2 { Some(args[1]) } else { None };

    let db = sqlite_context_db_handle(ctx);
    let sql = CString::new(
        "SELECT replica,counter,lamport,kind,parent,node,new_parent,position,known_state \
         FROM ops \
         WHERE lamport > ?1 \
         AND (?2 IS NULL OR parent = ?2 OR node = ?2 OR new_parent = ?2) \
         ORDER BY lamport, replica, counter",
    )
    .expect("static sql");

    let mut stmt: *mut sqlite3_stmt = null_mut();
    let rc = sqlite_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, null_mut());
    if rc != SQLITE_OK as c_int {
        sqlite_result_error_code(ctx, rc);
        return;
    }

    let bind_rc1 = unsafe { sqlite_bind_int64(stmt, 1, lamport as i64) };
    let bind_rc2 = if let Some(filter_val) = root_filter {
        unsafe { bind_optional_blob(stmt, 2, filter_val) }
    } else {
        unsafe { sqlite_bind_null(stmt, 2) != SQLITE_OK as c_int }
    };

    if bind_rc1 != SQLITE_OK as c_int || bind_rc2 {
        unsafe { sqlite_finalize(stmt) };
        sqlite_result_error_code(ctx, SQLITE_ERROR as c_int);
        return;
    }

    let mut ops: Vec<JsonOp> = Vec::new();
    loop {
        let step_rc = unsafe { sqlite_step(stmt) };
        if step_rc == SQLITE_ROW as c_int {
            match read_row(stmt) {
                Ok(op) => ops.push(op),
                Err(rc) => {
                    unsafe { sqlite_finalize(stmt) };
                    sqlite_result_error_code(ctx, rc);
                    return;
                }
            }
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

    let json = match serde_json::to_string(&ops) {
        Ok(j) => j,
        Err(_) => {
            sqlite_result_error_code(ctx, SQLITE_ERROR as c_int);
            return;
        }
    };

    if let Ok(cstr) = CString::new(json) {
        let len = cstr.as_bytes().len() as c_int;
        let ptr = cstr.into_raw();
        sqlite_result_text(ctx, ptr as *const c_char, len, Some(drop_cstring));
    } else {
        sqlite_result_error_code(ctx, SQLITE_ERROR as c_int);
    }
}

fn read_row(stmt: *mut sqlite3_stmt) -> Result<JsonOp, c_int> {
    unsafe {
        let replica_ptr = sqlite_column_blob(stmt, 0);
        let replica_len = sqlite_column_bytes(stmt, 0);
        let counter = sqlite_column_int64(stmt, 1) as u64;
        let lamport = sqlite_column_int64(stmt, 2) as Lamport;
        let kind_ptr = sqlite_column_text(stmt, 3);
        let kind_len = sqlite_column_bytes(stmt, 3);

        let replica =
            std::slice::from_raw_parts(replica_ptr as *const u8, replica_len as usize).to_vec();
        let kind = std::str::from_utf8(std::slice::from_raw_parts(
            kind_ptr as *const u8,
            kind_len as usize,
        ))
        .unwrap_or("")
        .to_string();

        let parent = column_blob16(stmt, 4)?;
        let node = match column_blob16(stmt, 5)? {
            Some(v) => v,
            None => return Err(SQLITE_ERROR as c_int),
        };
        let new_parent = column_blob16(stmt, 6)?;
        let position = column_int_opt(stmt, 7);
        let known_state = if sqlite_column_type(stmt, 8) == SQLITE_NULL as c_int {
            None
        } else {
            let ptr = sqlite_column_blob(stmt, 8) as *const u8;
            let len = sqlite_column_bytes(stmt, 8) as usize;
            if ptr.is_null() || len == 0 {
                None
            } else {
                Some(slice::from_raw_parts(ptr, len).to_vec())
            }
        };

        Ok(JsonOp {
            replica,
            counter,
            lamport,
            kind,
            parent,
            node,
            new_parent,
            position,
            known_state,
        })
    }
}

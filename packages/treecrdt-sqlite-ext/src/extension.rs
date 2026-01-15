//! SQLite extension entrypoint implemented against the SQLite C API.
//! This is intentionally minimal: it proves the cross-target build (native SQLite + wa-sqlite)
//! and registers a basic function to verify loading. Additional virtual tables/functions will
//! bridge to `treecrdt-core`.

#![allow(non_snake_case)]

use std::ffi::CString;
use std::os::raw::{c_char, c_int, c_void};
#[cfg(feature = "ext-sqlite")]
use std::ptr::null;
use std::ptr::null_mut;
use std::{
    collections::{HashMap, HashSet},
    slice,
};

use treecrdt_core::{Lamport, VersionVector};

#[cfg(any(feature = "ext-sqlite", feature = "static-link"))]
use serde_json;

#[cfg(feature = "ext-sqlite")]
use sqlite3ext_sys::{
    sqlite3, sqlite3_api_routines, sqlite3_context, sqlite3_stmt, sqlite3_value, SQLITE_DONE,
    SQLITE_ERROR, SQLITE_NULL, SQLITE_OK, SQLITE_ROW, SQLITE_UTF8,
};

#[cfg(feature = "static-link")]
#[allow(non_camel_case_types)]
type sqlite3_api_routines = c_void;

#[cfg(feature = "static-link")]
#[allow(non_camel_case_types, dead_code, improper_ctypes)]
mod ffi {
    use super::{c_char, c_int, c_void};

    #[repr(C)]
    pub struct sqlite3;
    #[repr(C)]
    pub struct sqlite3_stmt;
    #[repr(C)]
    pub struct sqlite3_context;
    #[repr(C)]
    pub struct sqlite3_value;

    extern "C" {
        pub fn sqlite3_create_function_v2(
            db: *mut sqlite3,
            zFunctionName: *const c_char,
            nArg: c_int,
            eTextRep: c_int,
            pApp: *mut c_void,
            xFunc: Option<
                unsafe extern "C" fn(
                    ctx: *mut sqlite3_context,
                    argc: c_int,
                    argv: *mut *mut sqlite3_value,
                ),
            >,
            xStep: Option<
                unsafe extern "C" fn(
                    ctx: *mut sqlite3_context,
                    argc: c_int,
                    argv: *mut *mut sqlite3_value,
                ),
            >,
            xFinal: Option<unsafe extern "C" fn(ctx: *mut sqlite3_context)>,
            xDestroy: Option<unsafe extern "C" fn(*mut c_void)>,
        ) -> c_int;

        pub fn sqlite3_exec(
            db: *mut sqlite3,
            sql: *const c_char,
            callback: Option<
                unsafe extern "C" fn(
                    arg1: *mut c_void,
                    arg2: c_int,
                    arg3: *mut *mut c_char,
                    arg4: *mut *mut c_char,
                ) -> c_int,
            >,
            arg: *mut c_void,
            errmsg: *mut *mut c_char,
        ) -> c_int;

        pub fn sqlite3_prepare_v2(
            db: *mut sqlite3,
            zSql: *const c_char,
            nByte: c_int,
            ppStmt: *mut *mut sqlite3_stmt,
            pzTail: *mut *const c_char,
        ) -> c_int;

        pub fn sqlite3_bind_blob(
            stmt: *mut sqlite3_stmt,
            idx: c_int,
            value: *const c_void,
            n: c_int,
            destructor: Option<unsafe extern "C" fn(*mut c_void)>,
        ) -> c_int;
        pub fn sqlite3_bind_text(
            stmt: *mut sqlite3_stmt,
            idx: c_int,
            value: *const c_char,
            n: c_int,
            destructor: Option<unsafe extern "C" fn(*mut c_void)>,
        ) -> c_int;
        pub fn sqlite3_bind_int64(stmt: *mut sqlite3_stmt, idx: c_int, value: i64) -> c_int;
        pub fn sqlite3_bind_null(stmt: *mut sqlite3_stmt, idx: c_int) -> c_int;

        pub fn sqlite3_step(stmt: *mut sqlite3_stmt) -> c_int;
        pub fn sqlite3_reset(stmt: *mut sqlite3_stmt) -> c_int;
        pub fn sqlite3_clear_bindings(stmt: *mut sqlite3_stmt) -> c_int;
        pub fn sqlite3_finalize(stmt: *mut sqlite3_stmt) -> c_int;

        pub fn sqlite3_value_blob(val: *mut sqlite3_value) -> *const c_void;
        pub fn sqlite3_value_bytes(val: *mut sqlite3_value) -> c_int;
        pub fn sqlite3_value_int64(val: *mut sqlite3_value) -> i64;
        pub fn sqlite3_value_text(val: *mut sqlite3_value) -> *const c_char;
        pub fn sqlite3_value_type(val: *mut sqlite3_value) -> c_int;

        pub fn sqlite3_result_text(
            ctx: *mut sqlite3_context,
            val: *const c_char,
            n: c_int,
            destructor: Option<unsafe extern "C" fn(*mut c_void)>,
        );
        pub fn sqlite3_result_error_code(ctx: *mut sqlite3_context, code: c_int);
        pub fn sqlite3_result_int(ctx: *mut sqlite3_context, value: c_int);
        pub fn sqlite3_result_int64(ctx: *mut sqlite3_context, value: i64);
        pub fn sqlite3_result_error(ctx: *mut sqlite3_context, msg: *const c_char, n: c_int);

        pub fn sqlite3_context_db_handle(ctx: *mut sqlite3_context) -> *mut sqlite3;

        pub fn sqlite3_column_blob(stmt: *mut sqlite3_stmt, idx: c_int) -> *const c_void;
        pub fn sqlite3_column_bytes(stmt: *mut sqlite3_stmt, idx: c_int) -> c_int;
        pub fn sqlite3_column_int64(stmt: *mut sqlite3_stmt, idx: c_int) -> i64;
        pub fn sqlite3_column_text(stmt: *mut sqlite3_stmt, idx: c_int) -> *const c_char;
        pub fn sqlite3_column_type(stmt: *mut sqlite3_stmt, idx: c_int) -> c_int;

        pub fn sqlite3_auto_extension(xEntryPoint: Option<unsafe extern "C" fn()>) -> c_int;
    }

    pub const SQLITE_DONE: c_int = 101;
    pub const SQLITE_ERROR: c_int = 1;
    pub const SQLITE_OK: c_int = 0;
    pub const SQLITE_ROW: c_int = 100;
    pub const SQLITE_UTF8: c_int = 1;
    pub const SQLITE_NULL: c_int = 5;
}

#[cfg(feature = "static-link")]
use ffi::*;

#[cfg(feature = "ext-sqlite")]
// Pointer to the SQLite API jump table. The host sets this when the extension loads.
static mut SQLITE3_API: *const sqlite3_api_routines = null();

const OPREF_V0_DOMAIN: &[u8] = b"treecrdt/opref/v0";
const OPREF_V0_WIDTH: usize = 16;
const ROOT_NODE_ID: [u8; 16] = [0u8; 16];
#[cfg(feature = "ext-sqlite")]
fn api<'a>() -> Option<&'a sqlite3_api_routines> {
    unsafe { SQLITE3_API.as_ref() }
}

#[derive(Clone, Debug)]
struct TreeMeta {
    dirty: bool,
    head_lamport: Lamport,
    head_replica: Vec<u8>,
    head_counter: u64,
    head_seq: u64,
}

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
fn serialize_version_vector(vv: &VersionVector) -> Result<Vec<u8>, c_int> {
    match serde_json::to_vec(vv) {
        Ok(bytes) => Ok(bytes),
        Err(_) => Err(SQLITE_ERROR as c_int),
    }
}

#[cfg(any(feature = "ext-sqlite", feature = "static-link"))]
fn deserialize_version_vector(bytes: &[u8]) -> Result<VersionVector, c_int> {
    match serde_json::from_slice(bytes) {
        Ok(vv) => Ok(vv),
        Err(_) => Err(SQLITE_ERROR as c_int),
    }
}

// Calculate subtree version vector from in-memory state (used during materialization)
#[cfg(any(feature = "ext-sqlite", feature = "static-link"))]
fn calculate_subtree_version_vector_mem(
    node: &[u8; 16],
    last_change_by_node: &HashMap<[u8; 16], VersionVector>,
    children_by_parent: &HashMap<[u8; 16], Vec<[u8; 16]>>,
) -> VersionVector {
    let mut subtree_vv = last_change_by_node.get(node).cloned().unwrap_or_else(VersionVector::new);

    if let Some(children) = children_by_parent.get(node) {
        for child in children {
            let child_vv = calculate_subtree_version_vector_mem(
                child,
                last_change_by_node,
                children_by_parent,
            );
            subtree_vv.merge(&child_vv);
        }
    }

    subtree_vv
}

// Calculate subtree version vector from database
#[cfg(any(feature = "ext-sqlite", feature = "static-link"))]
fn calculate_subtree_version_vector(
    db: *mut sqlite3,
    node: &[u8; 16],
) -> Result<VersionVector, c_int> {
    // Load node's last_change version vector
    let sql = CString::new("SELECT last_change FROM tree_nodes WHERE node = ?1 LIMIT 1")
        .expect("select last_change sql");
    let mut stmt: *mut sqlite3_stmt = null_mut();
    let rc = sqlite_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, null_mut());
    if rc != SQLITE_OK as c_int {
        return Err(rc);
    }

    let mut subtree_vv = VersionVector::new();
    unsafe {
        sqlite_bind_blob(
            stmt,
            1,
            node.as_ptr() as *const c_void,
            node.len() as c_int,
            None,
        );
    }

    let step_rc = unsafe { sqlite_step(stmt) };
    if step_rc == SQLITE_ROW as c_int {
        let last_change_ptr = unsafe { sqlite_column_blob(stmt, 0) } as *const u8;
        let last_change_len = unsafe { sqlite_column_bytes(stmt, 0) } as usize;
        if !last_change_ptr.is_null() && last_change_len > 0 {
            let last_change_bytes =
                unsafe { slice::from_raw_parts(last_change_ptr, last_change_len) };
            if let Ok(last_change_vv) = deserialize_version_vector(last_change_bytes) {
                subtree_vv.merge(&last_change_vv);
            }
        }
    }
    unsafe {
        sqlite_finalize(stmt);
    }

    // Get all children (including tombstoned ones for defensive deletion)
    let children_sql = CString::new("SELECT node FROM tree_nodes WHERE parent = ?1 ORDER BY pos")
        .expect("select children sql");
    let mut children_stmt: *mut sqlite3_stmt = null_mut();
    let rc = sqlite_prepare_v2(
        db,
        children_sql.as_ptr(),
        -1,
        &mut children_stmt,
        null_mut(),
    );
    if rc != SQLITE_OK as c_int {
        return Err(rc);
    }

    unsafe {
        sqlite_bind_blob(
            children_stmt,
            1,
            node.as_ptr() as *const c_void,
            node.len() as c_int,
            None,
        );
    }

    loop {
        let step_rc = unsafe { sqlite_step(children_stmt) };
        if step_rc == SQLITE_ROW as c_int {
            let child_ptr = unsafe { sqlite_column_blob(children_stmt, 0) } as *const u8;
            let child_len = unsafe { sqlite_column_bytes(children_stmt, 0) } as usize;
            if child_ptr.is_null() || child_len != 16 {
                unsafe { sqlite_finalize(children_stmt) };
                return Err(SQLITE_ERROR as c_int);
            }
            let mut child = [0u8; 16];
            child.copy_from_slice(unsafe { slice::from_raw_parts(child_ptr, child_len) });

            // Recursively calculate child's subtree version vector
            match calculate_subtree_version_vector(db, &child) {
                Ok(child_vv) => {
                    subtree_vv.merge(&child_vv);
                }
                Err(e) => {
                    unsafe { sqlite_finalize(children_stmt) };
                    return Err(e);
                }
            }
        } else if step_rc == SQLITE_DONE as c_int {
            break;
        } else {
            unsafe { sqlite_finalize(children_stmt) };
            return Err(step_rc);
        }
    }

    unsafe {
        sqlite_finalize(children_stmt);
    }

    Ok(subtree_vv)
}

fn load_doc_id(db: *mut sqlite3) -> Result<Option<Vec<u8>>, c_int> {
    let sql =
        CString::new("SELECT value FROM meta WHERE key = 'doc_id' LIMIT 1").expect("doc id sql");
    let mut stmt: *mut sqlite3_stmt = null_mut();
    let rc = sqlite_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, null_mut());
    if rc != SQLITE_OK as c_int {
        return Err(rc);
    }

    let step_rc = unsafe { sqlite_step(stmt) };
    if step_rc == SQLITE_ROW as c_int {
        let ptr = unsafe { sqlite_column_text(stmt, 0) } as *const u8;
        let len = unsafe { sqlite_column_bytes(stmt, 0) } as usize;
        let value = if ptr.is_null() || len == 0 {
            Vec::new()
        } else {
            unsafe { slice::from_raw_parts(ptr, len) }.to_vec()
        };
        let finalize_rc = unsafe { sqlite_finalize(stmt) };
        if finalize_rc != SQLITE_OK as c_int {
            return Err(finalize_rc);
        }
        Ok(Some(value))
    } else if step_rc == SQLITE_DONE as c_int {
        let finalize_rc = unsafe { sqlite_finalize(stmt) };
        if finalize_rc != SQLITE_OK as c_int {
            return Err(finalize_rc);
        }
        Ok(None)
    } else {
        unsafe { sqlite_finalize(stmt) };
        Err(step_rc)
    }
}

fn load_tree_meta(db: *mut sqlite3) -> Result<TreeMeta, c_int> {
    let sql = CString::new(
        "SELECT dirty, head_lamport, head_replica, head_counter, head_seq \
         FROM tree_meta WHERE id = 1 LIMIT 1",
    )
    .expect("tree meta sql");
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

    let dirty = unsafe { sqlite_column_int64(stmt, 0) } != 0;
    let head_lamport = unsafe { sqlite_column_int64(stmt, 1) } as Lamport;
    let rep_ptr = unsafe { sqlite_column_blob(stmt, 2) } as *const u8;
    let rep_len = unsafe { sqlite_column_bytes(stmt, 2) } as usize;
    let head_replica = if rep_ptr.is_null() || rep_len == 0 {
        Vec::new()
    } else {
        unsafe { slice::from_raw_parts(rep_ptr, rep_len) }.to_vec()
    };
    let head_counter = unsafe { sqlite_column_int64(stmt, 3) } as u64;
    let head_seq = unsafe { sqlite_column_int64(stmt, 4) } as u64;

    let finalize_rc = unsafe { sqlite_finalize(stmt) };
    if finalize_rc != SQLITE_OK as c_int {
        return Err(finalize_rc);
    }

    Ok(TreeMeta {
        dirty,
        head_lamport,
        head_replica,
        head_counter,
        head_seq,
    })
}

fn set_tree_meta_dirty(db: *mut sqlite3, dirty: bool) -> Result<(), c_int> {
    let sql =
        CString::new("UPDATE tree_meta SET dirty = ?1 WHERE id = 1").expect("tree meta dirty sql");
    let mut stmt: *mut sqlite3_stmt = null_mut();
    let rc = sqlite_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, null_mut());
    if rc != SQLITE_OK as c_int {
        return Err(rc);
    }

    let bind_rc = unsafe { sqlite_bind_int64(stmt, 1, if dirty { 1 } else { 0 }) };
    if bind_rc != SQLITE_OK as c_int {
        unsafe { sqlite_finalize(stmt) };
        return Err(bind_rc);
    }

    let step_rc = unsafe { sqlite_step(stmt) };
    let finalize_rc = unsafe { sqlite_finalize(stmt) };
    if step_rc != SQLITE_DONE as c_int {
        return Err(step_rc);
    }
    if finalize_rc != SQLITE_OK as c_int {
        return Err(finalize_rc);
    }
    Ok(())
}

fn last_changes(db: *mut sqlite3) -> Result<i64, c_int> {
    let sql = CString::new("SELECT changes()").expect("changes sql");
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
    let value = unsafe { sqlite_column_int64(stmt, 0) };
    let finalize_rc = unsafe { sqlite_finalize(stmt) };
    if finalize_rc != SQLITE_OK as c_int {
        return Err(finalize_rc);
    }
    Ok(value)
}

fn update_tree_meta_head(
    db: *mut sqlite3,
    lamport: Lamport,
    replica: &[u8],
    counter: u64,
    seq: u64,
) -> Result<(), c_int> {
    let sql = CString::new(
        "UPDATE tree_meta \
         SET dirty = 0, head_lamport = ?1, head_replica = ?2, head_counter = ?3, head_seq = ?4 \
         WHERE id = 1",
    )
    .expect("tree meta head sql");
    let mut stmt: *mut sqlite3_stmt = null_mut();
    let rc = sqlite_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, null_mut());
    if rc != SQLITE_OK as c_int {
        return Err(rc);
    }

    let mut bind_err = false;
    unsafe {
        bind_err |= sqlite_bind_int64(stmt, 1, lamport as i64) != SQLITE_OK as c_int;
        bind_err |= sqlite_bind_blob(
            stmt,
            2,
            replica.as_ptr() as *const c_void,
            replica.len() as c_int,
            None,
        ) != SQLITE_OK as c_int;
        bind_err |= sqlite_bind_int64(stmt, 3, counter as i64) != SQLITE_OK as c_int;
        bind_err |= sqlite_bind_int64(stmt, 4, seq as i64) != SQLITE_OK as c_int;
    }
    if bind_err {
        unsafe { sqlite_finalize(stmt) };
        return Err(SQLITE_ERROR as c_int);
    }

    let step_rc = unsafe { sqlite_step(stmt) };
    let finalize_rc = unsafe { sqlite_finalize(stmt) };
    if step_rc != SQLITE_DONE as c_int {
        return Err(step_rc);
    }
    if finalize_rc != SQLITE_OK as c_int {
        return Err(finalize_rc);
    }
    Ok(())
}

fn op_key_lt(
    lamport: Lamport,
    replica: &[u8],
    counter: u64,
    head_lamport: Lamport,
    head_replica: &[u8],
    head_counter: u64,
) -> bool {
    if lamport != head_lamport {
        return lamport < head_lamport;
    }
    if replica != head_replica {
        return replica < head_replica;
    }
    counter < head_counter
}

fn ensure_materialized(db: *mut sqlite3) -> Result<(), c_int> {
    let meta = load_tree_meta(db)?;
    if !meta.dirty {
        return Ok(());
    }
    rebuild_materialized(db)
}

fn rebuild_materialized(db: *mut sqlite3) -> Result<(), c_int> {
    let begin = CString::new("SAVEPOINT treecrdt_materialize").expect("static");
    let commit = CString::new("RELEASE treecrdt_materialize").expect("static");
    let rollback = CString::new("ROLLBACK TO treecrdt_materialize; RELEASE treecrdt_materialize")
        .expect("static");

    if sqlite_exec(db, begin.as_ptr(), None, null_mut(), null_mut()) != SQLITE_OK as c_int {
        return Err(SQLITE_ERROR as c_int);
    }

    let clear_sql = CString::new(
        "DELETE FROM tree_nodes; \
         DELETE FROM oprefs_children; \
         UPDATE tree_meta SET dirty = 0, head_lamport = 0, head_replica = X'', head_counter = 0, head_seq = 0 WHERE id = 1;",
    )
    .expect("clear materialized sql");
    let clear_rc = sqlite_exec(db, clear_sql.as_ptr(), None, null_mut(), null_mut());
    if clear_rc != SQLITE_OK as c_int {
        sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
        return Err(clear_rc);
    }

    // Always ensure ROOT exists in the materialized table.
    {
        let sql = CString::new(
            "INSERT OR IGNORE INTO tree_nodes(node,parent,pos,tombstone) VALUES (?1,NULL,0,0)",
        )
        .expect("root insert sql");
        let mut stmt: *mut sqlite3_stmt = null_mut();
        let rc = sqlite_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, null_mut());
        if rc != SQLITE_OK as c_int {
            sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
            return Err(rc);
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
            sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
            return Err(bind_rc);
        }
        let step_rc = unsafe { sqlite_step(stmt) };
        let finalize_rc = unsafe { sqlite_finalize(stmt) };
        if step_rc != SQLITE_DONE as c_int || finalize_rc != SQLITE_OK as c_int {
            sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
            return Err(SQLITE_ERROR as c_int);
        }
    }

    // Scan ops in canonical order and build tree + opref indexes in-memory.
    let scan_sql = CString::new(
        "SELECT replica,counter,lamport,kind,parent,node,new_parent,position,op_ref,known_state \
         FROM ops ORDER BY lamport, replica, counter",
    )
    .expect("scan ops sql");
    let mut scan_stmt: *mut sqlite3_stmt = null_mut();
    let prep_rc = sqlite_prepare_v2(db, scan_sql.as_ptr(), -1, &mut scan_stmt, null_mut());
    if prep_rc != SQLITE_OK as c_int {
        sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
        return Err(prep_rc);
    }

    let mut children_by_parent: HashMap<[u8; 16], Vec<[u8; 16]>> = HashMap::new();
    let mut parent_by_node: HashMap<[u8; 16], [u8; 16]> = HashMap::new();
    // Track version vectors for defensive deletion
    let mut last_change_by_node: HashMap<[u8; 16], VersionVector> = HashMap::new();
    let mut deleted_at_by_node: HashMap<[u8; 16], VersionVector> = HashMap::new();

    #[derive(Clone)]
    struct ChildOpRefRow {
        parent: [u8; 16],
        op_ref: [u8; OPREF_V0_WIDTH],
        seq: u64,
    }
    let mut opref_rows: Vec<ChildOpRefRow> = Vec::new();

    let mut seq: u64 = 0;
    let mut head: Option<(Lamport, Vec<u8>, u64)> = None;

    loop {
        let step_rc = unsafe { sqlite_step(scan_stmt) };
        if step_rc == SQLITE_ROW as c_int {
            seq += 1;
            unsafe {
                let replica_ptr = sqlite_column_blob(scan_stmt, 0) as *const u8;
                let replica_len = sqlite_column_bytes(scan_stmt, 0) as usize;
                let replica = if replica_ptr.is_null() || replica_len == 0 {
                    Vec::new()
                } else {
                    slice::from_raw_parts(replica_ptr, replica_len).to_vec()
                };

                let counter = sqlite_column_int64(scan_stmt, 1) as u64;
                let lamport = sqlite_column_int64(scan_stmt, 2) as Lamport;

                let kind_ptr = sqlite_column_text(scan_stmt, 3) as *const u8;
                let kind_len = sqlite_column_bytes(scan_stmt, 3) as usize;
                let kind = if kind_ptr.is_null() {
                    ""
                } else {
                    std::str::from_utf8(slice::from_raw_parts(kind_ptr, kind_len)).unwrap_or("")
                };

                let parent = column_blob16(scan_stmt, 4).ok().flatten();
                let node = match column_blob16(scan_stmt, 5).ok().flatten() {
                    Some(v) => v,
                    None => continue,
                };
                let new_parent = column_blob16(scan_stmt, 6).ok().flatten();
                let position = column_int_opt(scan_stmt, 7).map(|v| v as usize);

                let op_ref_ptr = sqlite_column_blob(scan_stmt, 8) as *const u8;
                let op_ref_len = sqlite_column_bytes(scan_stmt, 8) as usize;
                if op_ref_ptr.is_null() || op_ref_len != OPREF_V0_WIDTH {
                    continue;
                }
                let mut op_ref = [0u8; OPREF_V0_WIDTH];
                op_ref.copy_from_slice(slice::from_raw_parts(op_ref_ptr, op_ref_len));

                // Read known_state (column 9) - may be NULL for old operations
                let known_state_from_db =
                    if sqlite_column_type(scan_stmt, 9) == SQLITE_NULL as c_int {
                        None
                    } else {
                        let ks_ptr = sqlite_column_blob(scan_stmt, 9) as *const u8;
                        let ks_len = sqlite_column_bytes(scan_stmt, 9) as usize;
                        if !ks_ptr.is_null() && ks_len > 0 {
                            let ks_bytes = slice::from_raw_parts(ks_ptr, ks_len);
                            deserialize_version_vector(ks_bytes).ok()
                        } else {
                            None
                        }
                    };

                let old_parent = parent_by_node.get(&node).copied();

                // Create operation version vector
                use treecrdt_core::{ReplicaId, VersionVector};
                let mut op_vv = VersionVector::new();
                op_vv.observe(&ReplicaId(replica.clone()), counter);

                let mut next_parent: Option<[u8; 16]> = None;
                let mut known_state: Option<VersionVector> = None;
                let mut should_reposition = false;

                if kind == "insert" {
                    should_reposition = true;
                    if let Some(p) = parent {
                        next_parent = Some(p);
                    }
                    // Update last_change for node and parent
                    last_change_by_node
                        .entry(node)
                        .or_insert_with(VersionVector::new)
                        .merge(&op_vv);
                    if let Some(p) = parent {
                        last_change_by_node
                            .entry(p)
                            .or_insert_with(VersionVector::new)
                            .merge(&op_vv);
                    }
                } else if kind == "move" {
                    should_reposition = true;
                    if let Some(p) = new_parent {
                        next_parent = Some(p);
                    }
                    // Update last_change for node, old parent, and new parent
                    last_change_by_node
                        .entry(node)
                        .or_insert_with(VersionVector::new)
                        .merge(&op_vv);
                    if let Some(old_p) = old_parent {
                        last_change_by_node
                            .entry(old_p)
                            .or_insert_with(VersionVector::new)
                            .merge(&op_vv);
                    }
                    if let Some(new_p) = new_parent {
                        last_change_by_node
                            .entry(new_p)
                            .or_insert_with(VersionVector::new)
                            .merge(&op_vv);
                    }
                } else if kind == "delete" || kind == "tombstone" {
                    // Phase 1: Capture - Calculate subtree version vector BEFORE applying delete
                    // Use known_state from database if available (for existing operations),
                    // otherwise calculate it from current state (for new operations during materialization)
                    if kind == "delete" {
                        known_state = known_state_from_db.or_else(|| {
                            Some(calculate_subtree_version_vector_mem(
                                &node,
                                &last_change_by_node,
                                &children_by_parent,
                            ))
                        });
                    }

                    // Keep parent reference - don't detach on delete
                    next_parent = old_parent;
                    // Tombstone status will be computed later based on awareness

                    // Phase 2: Tombstone - Merge operation version vector with known_state
                    let mut delete_vv = op_vv.clone();
                    if let Some(ref ks) = known_state {
                        delete_vv.merge(ks);
                    }

                    // Update deleted_at (merge if multiple deletes)
                    if let Some(existing) = deleted_at_by_node.get_mut(&node) {
                        existing.merge(&delete_vv);
                    } else {
                        deleted_at_by_node.insert(node, delete_vv);
                    }

                    // Note: Delete operations do NOT update last_change (matching core behavior)
                    // In core, apply_delete does not call update_last_change
                } else {
                    // Unknown kind; ignore.
                    continue;
                }

                // Update parent/children structure (delete operations keep parent + position).
                if should_reposition {
                    if let Some(op) = old_parent {
                        if let Some(children) = children_by_parent.get_mut(&op) {
                            children.retain(|c| c != &node);
                        }
                    }

                    if let Some(p) = next_parent {
                        let children = children_by_parent.entry(p).or_default();
                        let mut pos = position.unwrap_or(children.len());
                        if pos > children.len() {
                            pos = children.len();
                        }
                        children.insert(pos, node);
                        parent_by_node.insert(node, p);
                    } else {
                        parent_by_node.remove(&node);
                    }
                } else if let Some(p) = next_parent {
                    // Ensure the node is present under its existing parent.
                    let children = children_by_parent.entry(p).or_default();
                    if !children.contains(&node) {
                        children.push(node);
                    }
                    parent_by_node.insert(node, p);
                }
                // Note: tombstone status will be computed after all operations are processed

                if let Some(p) = old_parent {
                    opref_rows.push(ChildOpRefRow {
                        parent: p,
                        op_ref,
                        seq,
                    });
                }
                if let Some(p) = next_parent {
                    if old_parent != Some(p) {
                        opref_rows.push(ChildOpRefRow {
                            parent: p,
                            op_ref,
                            seq,
                        });
                    } else {
                        // reorder within parent: still relevant once
                        // (already pushed old_parent)
                    }
                }

                head = Some((lamport, replica, counter));
            }
        } else if step_rc == SQLITE_DONE as c_int {
            break;
        } else {
            unsafe { sqlite_finalize(scan_stmt) };
            sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
            return Err(step_rc);
        }
    }

    let finalize_rc = unsafe { sqlite_finalize(scan_stmt) };
    if finalize_rc != SQLITE_OK as c_int {
        sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
        return Err(finalize_rc);
    }

    // Phase 3: Validate - Compute actual tombstone status for all nodes based on awareness
    // Collect all nodes that need to be written
    let mut all_nodes: Vec<([u8; 16], Option<[u8; 16]>, usize, bool)> = Vec::new();

    // Add nodes from children_by_parent (non-tombstoned or tombstoned but keeping parent)
    for (parent, children) in &children_by_parent {
        for (pos, node) in children.iter().enumerate() {
            // Compute tombstone status: check if deleted_at is aware of current subtree
            let is_tombstoned = if let Some(deleted_at_vv) = deleted_at_by_node.get(node) {
                let current_subtree = calculate_subtree_version_vector_mem(
                    node,
                    &last_change_by_node,
                    &children_by_parent,
                );
                deleted_at_vv.is_aware_of(&current_subtree)
            } else {
                false
            };
            all_nodes.push((*node, Some(*parent), pos, is_tombstoned));
        }
    }

    // Add nodes that have deleted_at but might not be in children_by_parent
    for (node, deleted_at_vv) in &deleted_at_by_node {
        if !all_nodes.iter().any(|(n, _, _, _)| n == node) {
            let parent = parent_by_node.get(node).copied();
            let current_subtree = calculate_subtree_version_vector_mem(
                node,
                &last_change_by_node,
                &children_by_parent,
            );
            let is_tombstoned = deleted_at_vv.is_aware_of(&current_subtree);
            all_nodes.push((*node, parent, 0, is_tombstoned));
        }
    }

    // Write all tree_nodes with computed tombstone status
    {
        let sql = CString::new(
            "INSERT OR REPLACE INTO tree_nodes(node,parent,pos,tombstone,last_change,deleted_at) VALUES (?1,?2,?3,?4,?5,?6)",
        )
        .expect("insert tree node sql");
        let mut stmt: *mut sqlite3_stmt = null_mut();
        let rc = sqlite_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, null_mut());
        if rc != SQLITE_OK as c_int {
            sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
            return Err(rc);
        }

        for (node, parent, pos, is_tombstoned) in all_nodes {
            unsafe {
                sqlite_clear_bindings(stmt);
                sqlite_reset(stmt);
            }
            let mut bind_err = false;
            let last_change_bytes =
                last_change_by_node.get(&node).and_then(|vv| serialize_version_vector(vv).ok());
            let deleted_at_bytes =
                deleted_at_by_node.get(&node).and_then(|vv| serialize_version_vector(vv).ok());

            unsafe {
                bind_err |= sqlite_bind_blob(
                    stmt,
                    1,
                    node.as_ptr() as *const c_void,
                    node.len() as c_int,
                    None,
                ) != SQLITE_OK as c_int;
                if let Some(p) = parent {
                    bind_err |= sqlite_bind_blob(
                        stmt,
                        2,
                        p.as_ptr() as *const c_void,
                        p.len() as c_int,
                        None,
                    ) != SQLITE_OK as c_int;
                } else {
                    bind_err |= sqlite_bind_null(stmt, 2) != SQLITE_OK as c_int;
                }
                bind_err |= sqlite_bind_int64(stmt, 3, pos as i64) != SQLITE_OK as c_int;
                bind_err |= sqlite_bind_int64(stmt, 4, if is_tombstoned { 1 } else { 0 })
                    != SQLITE_OK as c_int;
                if let Some(ref bytes) = last_change_bytes {
                    bind_err |= sqlite_bind_blob(
                        stmt,
                        5,
                        bytes.as_ptr() as *const c_void,
                        bytes.len() as c_int,
                        None,
                    ) != SQLITE_OK as c_int;
                } else {
                    bind_err |= sqlite_bind_null(stmt, 5) != SQLITE_OK as c_int;
                }
                if let Some(ref bytes) = deleted_at_bytes {
                    bind_err |= sqlite_bind_blob(
                        stmt,
                        6,
                        bytes.as_ptr() as *const c_void,
                        bytes.len() as c_int,
                        None,
                    ) != SQLITE_OK as c_int;
                } else {
                    bind_err |= sqlite_bind_null(stmt, 6) != SQLITE_OK as c_int;
                }
            }
            if bind_err {
                unsafe { sqlite_finalize(stmt) };
                sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
                return Err(SQLITE_ERROR as c_int);
            }
            let step_rc = unsafe { sqlite_step(stmt) };
            if step_rc != SQLITE_DONE as c_int {
                unsafe { sqlite_finalize(stmt) };
                sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
                return Err(step_rc);
            }
        }

        unsafe { sqlite_finalize(stmt) };
    }

    // Write oprefs_children.
    {
        let sql = CString::new(
            "INSERT OR IGNORE INTO oprefs_children(parent, op_ref, seq) VALUES (?1, ?2, ?3)",
        )
        .expect("insert oprefs_children sql");
        let mut stmt: *mut sqlite3_stmt = null_mut();
        let rc = sqlite_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, null_mut());
        if rc != SQLITE_OK as c_int {
            sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
            return Err(rc);
        }
        for row in opref_rows {
            unsafe {
                sqlite_clear_bindings(stmt);
                sqlite_reset(stmt);
            }
            let mut bind_err = false;
            unsafe {
                bind_err |= sqlite_bind_blob(
                    stmt,
                    1,
                    row.parent.as_ptr() as *const c_void,
                    row.parent.len() as c_int,
                    None,
                ) != SQLITE_OK as c_int;
                bind_err |= sqlite_bind_blob(
                    stmt,
                    2,
                    row.op_ref.as_ptr() as *const c_void,
                    row.op_ref.len() as c_int,
                    None,
                ) != SQLITE_OK as c_int;
                bind_err |= sqlite_bind_int64(stmt, 3, row.seq as i64) != SQLITE_OK as c_int;
            }
            if bind_err {
                unsafe { sqlite_finalize(stmt) };
                sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
                return Err(SQLITE_ERROR as c_int);
            }
            let step_rc = unsafe { sqlite_step(stmt) };
            if step_rc != SQLITE_DONE as c_int {
                unsafe { sqlite_finalize(stmt) };
                sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
                return Err(step_rc);
            }
        }
        unsafe { sqlite_finalize(stmt) };
    }

    // Update meta head + seq.
    if let Some((lamport, replica, counter)) = head {
        let head_rc = update_tree_meta_head(db, lamport, &replica, counter, seq);
        if head_rc.is_err() {
            sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
            return head_rc;
        }
    } else {
        let head_rc = update_tree_meta_head(db, 0, &[], 0, 0);
        if head_rc.is_err() {
            sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
            return head_rc;
        }
    }

    let commit_rc = sqlite_exec(db, commit.as_ptr(), None, null_mut(), null_mut());
    if commit_rc != SQLITE_OK as c_int {
        sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
        return Err(commit_rc);
    }
    Ok(())
}

struct MaterializeCtx {
    ensure_node: *mut sqlite3_stmt,
    select_node: *mut sqlite3_stmt,
    shift_down: *mut sqlite3_stmt,
    shift_up: *mut sqlite3_stmt,
    max_pos: *mut sqlite3_stmt,
    clear_parent_pos: *mut sqlite3_stmt,
    detach: *mut sqlite3_stmt,
    attach: *mut sqlite3_stmt,
    update_last_change: *mut sqlite3_stmt,
    update_tombstone: *mut sqlite3_stmt,
    update_pos: *mut sqlite3_stmt,
    insert_opref: *mut sqlite3_stmt,
}

impl MaterializeCtx {
    fn prepare(db: *mut sqlite3) -> Result<Self, c_int> {
        let ensure_node_sql = CString::new(
            "INSERT OR IGNORE INTO tree_nodes(node,parent,pos,tombstone) VALUES (?1,NULL,NULL,0)",
        )
        .expect("ensure node sql");
        let select_node_sql =
            CString::new("SELECT parent,pos,tombstone,last_change,deleted_at FROM tree_nodes WHERE node = ?1 LIMIT 1")
                .expect("select node sql");
        let shift_down_sql = CString::new(
            "UPDATE tree_nodes SET pos = pos - 1 WHERE parent = ?1 AND tombstone = 0 AND pos > ?2",
        )
        .expect("shift down sql");
        let shift_up_sql = CString::new(
            "UPDATE tree_nodes SET pos = pos + 1 WHERE parent = ?1 AND tombstone = 0 AND pos >= ?2",
        )
        .expect("shift up sql");
        let max_pos_sql = CString::new(
            "SELECT COALESCE(MAX(pos) + 1, 0) FROM tree_nodes WHERE parent = ?1 AND tombstone = 0",
        )
        .expect("max pos sql");
        let clear_parent_pos_sql =
            CString::new("UPDATE tree_nodes SET parent = NULL, pos = NULL WHERE node = ?1")
                .expect("clear parent pos sql");
        let detach_sql = CString::new(
            "UPDATE tree_nodes SET tombstone = ?2, last_change = ?3, deleted_at = ?4 WHERE node = ?1",
        )
        .expect("detach sql");
        let attach_sql = CString::new(
            "UPDATE tree_nodes SET parent = ?2, pos = ?3, tombstone = 0, last_change = ?4, deleted_at = ?5 WHERE node = ?1",
        )
        .expect("attach sql");
        let update_last_change_sql =
            CString::new("UPDATE tree_nodes SET last_change = ?2 WHERE node = ?1")
                .expect("update last_change sql");
        let update_tombstone_sql =
            CString::new("UPDATE tree_nodes SET tombstone = ?2 WHERE node = ?1")
                .expect("update tombstone sql");
        let update_pos_sql =
            CString::new("UPDATE tree_nodes SET pos = ?2 WHERE node = ?1").expect("update pos sql");
        let insert_opref_sql = CString::new(
            "INSERT OR IGNORE INTO oprefs_children(parent, op_ref, seq) VALUES (?1, ?2, ?3)",
        )
        .expect("insert opref sql");

        let mut ensure_node: *mut sqlite3_stmt = null_mut();
        let mut select_node: *mut sqlite3_stmt = null_mut();
        let mut shift_down: *mut sqlite3_stmt = null_mut();
        let mut shift_up: *mut sqlite3_stmt = null_mut();
        let mut max_pos: *mut sqlite3_stmt = null_mut();
        let mut clear_parent_pos: *mut sqlite3_stmt = null_mut();
        let mut detach: *mut sqlite3_stmt = null_mut();
        let mut attach: *mut sqlite3_stmt = null_mut();
        let mut update_last_change: *mut sqlite3_stmt = null_mut();
        let mut update_tombstone: *mut sqlite3_stmt = null_mut();
        let mut update_pos: *mut sqlite3_stmt = null_mut();
        let mut insert_opref: *mut sqlite3_stmt = null_mut();

        let prep = |sql: &CString, stmt: &mut *mut sqlite3_stmt| -> Result<(), c_int> {
            let rc = sqlite_prepare_v2(db, sql.as_ptr(), -1, stmt, null_mut());
            if rc != SQLITE_OK as c_int {
                return Err(rc);
            }
            Ok(())
        };

        prep(&ensure_node_sql, &mut ensure_node)?;
        prep(&select_node_sql, &mut select_node)?;
        prep(&shift_down_sql, &mut shift_down)?;
        prep(&shift_up_sql, &mut shift_up)?;
        prep(&max_pos_sql, &mut max_pos)?;
        prep(&clear_parent_pos_sql, &mut clear_parent_pos)?;
        prep(&detach_sql, &mut detach)?;
        prep(&attach_sql, &mut attach)?;
        prep(&update_last_change_sql, &mut update_last_change)?;
        prep(&update_tombstone_sql, &mut update_tombstone)?;
        prep(&update_pos_sql, &mut update_pos)?;
        prep(&insert_opref_sql, &mut insert_opref)?;

        Ok(Self {
            ensure_node,
            select_node,
            shift_down,
            shift_up,
            max_pos,
            clear_parent_pos,
            detach,
            attach,
            update_last_change,
            update_tombstone,
            update_pos,
            insert_opref,
        })
    }

    unsafe fn finalize(&mut self) {
        unsafe {
            sqlite_finalize(self.ensure_node);
            sqlite_finalize(self.select_node);
            sqlite_finalize(self.shift_down);
            sqlite_finalize(self.shift_up);
            sqlite_finalize(self.max_pos);
            sqlite_finalize(self.clear_parent_pos);
            sqlite_finalize(self.detach);
            sqlite_finalize(self.attach);
            sqlite_finalize(self.update_last_change);
            sqlite_finalize(self.update_tombstone);
            sqlite_finalize(self.update_pos);
            sqlite_finalize(self.insert_opref);
        }
    }
}

fn merge_last_change_delta(
    _db: *mut sqlite3,
    ctx: &mut MaterializeCtx,
    node: &[u8; 16],
    delta: &VersionVector,
) -> Result<(), c_int> {
    unsafe {
        sqlite_clear_bindings(ctx.ensure_node);
        sqlite_reset(ctx.ensure_node);
        sqlite_bind_blob(
            ctx.ensure_node,
            1,
            node.as_ptr() as *const c_void,
            node.len() as c_int,
            None,
        );
        sqlite_step(ctx.ensure_node);
    }

    let mut last_change = VersionVector::new();
    unsafe {
        sqlite_clear_bindings(ctx.select_node);
        sqlite_reset(ctx.select_node);
        sqlite_bind_blob(
            ctx.select_node,
            1,
            node.as_ptr() as *const c_void,
            node.len() as c_int,
            None,
        );
        let step_rc = sqlite_step(ctx.select_node);
        if step_rc == SQLITE_ROW as c_int {
            if sqlite_column_type(ctx.select_node, 3) != SQLITE_NULL as c_int {
                let lc_ptr = sqlite_column_blob(ctx.select_node, 3) as *const u8;
                let lc_len = sqlite_column_bytes(ctx.select_node, 3) as usize;
                if !lc_ptr.is_null() && lc_len > 0 {
                    let lc_bytes = slice::from_raw_parts(lc_ptr, lc_len);
                    if let Ok(vv) = deserialize_version_vector(lc_bytes) {
                        last_change = vv;
                    }
                }
            }
        }
    }

    last_change.merge(delta);
    let bytes = serialize_version_vector(&last_change)?;

    unsafe {
        sqlite_clear_bindings(ctx.update_last_change);
        sqlite_reset(ctx.update_last_change);
        sqlite_bind_blob(
            ctx.update_last_change,
            1,
            node.as_ptr() as *const c_void,
            node.len() as c_int,
            None,
        );
        sqlite_bind_blob(
            ctx.update_last_change,
            2,
            bytes.as_ptr() as *const c_void,
            bytes.len() as c_int,
            None,
        );
        sqlite_step(ctx.update_last_change);
        sqlite_reset(ctx.update_last_change);
    }

    Ok(())
}

fn refresh_tombstones_upward(
    db: *mut sqlite3,
    ctx: &mut MaterializeCtx,
    starts: &[Option<[u8; 16]>],
) -> Result<(), c_int> {
    let mut visited: HashSet<[u8; 16]> = HashSet::new();
    let mut stack: Vec<[u8; 16]> = starts.iter().copied().flatten().collect();

    while let Some(node) = stack.pop() {
        if !visited.insert(node) {
            continue;
        }

        let (parent, old_tombstone, deleted_at) = unsafe {
            sqlite_clear_bindings(ctx.select_node);
            sqlite_reset(ctx.select_node);
            sqlite_bind_blob(
                ctx.select_node,
                1,
                node.as_ptr() as *const c_void,
                node.len() as c_int,
                None,
            );
            let step_rc = sqlite_step(ctx.select_node);
            if step_rc != SQLITE_ROW as c_int {
                continue;
            }

            let parent = column_blob16(ctx.select_node, 0).ok().flatten();
            let old_tombstone = sqlite_column_int64(ctx.select_node, 2) != 0;

            let mut deleted_at: Option<VersionVector> = None;
            if sqlite_column_type(ctx.select_node, 4) != SQLITE_NULL as c_int {
                let da_ptr = sqlite_column_blob(ctx.select_node, 4) as *const u8;
                let da_len = sqlite_column_bytes(ctx.select_node, 4) as usize;
                if !da_ptr.is_null() && da_len > 0 {
                    let da_bytes = slice::from_raw_parts(da_ptr, da_len);
                    if let Ok(vv) = deserialize_version_vector(da_bytes) {
                        deleted_at = Some(vv);
                    }
                }
            }

            (parent, old_tombstone, deleted_at)
        };

        let new_tombstone = if let Some(ref deleted_at_vv) = deleted_at {
            let current_subtree = calculate_subtree_version_vector(db, &node)?;
            deleted_at_vv.is_aware_of(&current_subtree)
        } else {
            false
        };

        if new_tombstone != old_tombstone {
            unsafe {
                sqlite_clear_bindings(ctx.update_tombstone);
                sqlite_reset(ctx.update_tombstone);
                sqlite_bind_blob(
                    ctx.update_tombstone,
                    1,
                    node.as_ptr() as *const c_void,
                    node.len() as c_int,
                    None,
                );
                sqlite_bind_int64(ctx.update_tombstone, 2, if new_tombstone { 1 } else { 0 });
                sqlite_step(ctx.update_tombstone);
                sqlite_reset(ctx.update_tombstone);
            }

            // If restoring a node, ensure it has a non-conflicting position among active siblings.
            if old_tombstone && !new_tombstone {
                if let Some(p) = parent {
                    let mut len: i64 = 0;
                    unsafe {
                        sqlite_clear_bindings(ctx.max_pos);
                        sqlite_reset(ctx.max_pos);
                        sqlite_bind_blob(
                            ctx.max_pos,
                            1,
                            p.as_ptr() as *const c_void,
                            p.len() as c_int,
                            None,
                        );
                        let step_rc = sqlite_step(ctx.max_pos);
                        if step_rc == SQLITE_ROW as c_int {
                            len = sqlite_column_int64(ctx.max_pos, 0);
                        }
                        sqlite_reset(ctx.max_pos);
                    }

                    unsafe {
                        sqlite_clear_bindings(ctx.update_pos);
                        sqlite_reset(ctx.update_pos);
                        sqlite_bind_blob(
                            ctx.update_pos,
                            1,
                            node.as_ptr() as *const c_void,
                            node.len() as c_int,
                            None,
                        );
                        sqlite_bind_int64(ctx.update_pos, 2, len);
                        sqlite_step(ctx.update_pos);
                        sqlite_reset(ctx.update_pos);
                    }
                }
            }
        }

        if let Some(p) = parent {
            stack.push(p);
        }
    }

    Ok(())
}

fn materialize_inserted_op(
    db: *mut sqlite3,
    ctx: &mut MaterializeCtx,
    meta: &mut TreeMeta,
    kind: &str,
    node: [u8; 16],
    parent: Option<[u8; 16]>,
    new_parent: Option<[u8; 16]>,
    position: Option<u64>,
    op_ref: [u8; OPREF_V0_WIDTH],
    lamport: Lamport,
    replica: &[u8],
    counter: u64,
) -> Result<(), c_int> {
    if meta.dirty {
        return Ok(());
    }

    if op_key_lt(
        lamport,
        replica,
        counter,
        meta.head_lamport,
        &meta.head_replica,
        meta.head_counter,
    ) {
        meta.dirty = true;
        return Ok(());
    }

    let seq = meta.head_seq + 1;

    // ROOT is stable and never moves/deletes; still advance seq/head.
    if node == ROOT_NODE_ID {
        meta.head_lamport = lamport;
        meta.head_replica = replica.to_vec();
        meta.head_counter = counter;
        meta.head_seq = seq;
        return Ok(());
    }

    unsafe {
        sqlite_clear_bindings(ctx.ensure_node);
        sqlite_reset(ctx.ensure_node);
        sqlite_bind_blob(
            ctx.ensure_node,
            1,
            node.as_ptr() as *const c_void,
            node.len() as c_int,
            None,
        );
        sqlite_step(ctx.ensure_node);
    }

    // Load current node state (old parent/pos/version vectors) before applying.
    let mut old_parent: Option<[u8; 16]> = None;
    let mut old_pos: Option<i64> = None;
    let mut old_tombstone = false;
    let mut old_last_change = VersionVector::new();
    let mut old_deleted_at: Option<VersionVector> = None;
    unsafe {
        sqlite_clear_bindings(ctx.select_node);
        sqlite_reset(ctx.select_node);
        sqlite_bind_blob(
            ctx.select_node,
            1,
            node.as_ptr() as *const c_void,
            node.len() as c_int,
            None,
        );
        let step_rc = sqlite_step(ctx.select_node);
        if step_rc == SQLITE_ROW as c_int {
            old_parent = column_blob16(ctx.select_node, 0).ok().flatten();
            old_pos = if sqlite_column_type(ctx.select_node, 1) == SQLITE_NULL as c_int {
                None
            } else {
                Some(sqlite_column_int64(ctx.select_node, 1))
            };
            old_tombstone = sqlite_column_int64(ctx.select_node, 2) != 0;
            // Load last_change (column 3)
            if sqlite_column_type(ctx.select_node, 3) != SQLITE_NULL as c_int {
                let lc_ptr = sqlite_column_blob(ctx.select_node, 3) as *const u8;
                let lc_len = sqlite_column_bytes(ctx.select_node, 3) as usize;
                if !lc_ptr.is_null() && lc_len > 0 {
                    let lc_bytes = slice::from_raw_parts(lc_ptr, lc_len);
                    if let Ok(vv) = deserialize_version_vector(lc_bytes) {
                        old_last_change = vv;
                    }
                }
            }
            // Load deleted_at (column 4)
            if sqlite_column_type(ctx.select_node, 4) != SQLITE_NULL as c_int {
                let da_ptr = sqlite_column_blob(ctx.select_node, 4) as *const u8;
                let da_len = sqlite_column_bytes(ctx.select_node, 4) as usize;
                if !da_ptr.is_null() && da_len > 0 {
                    let da_bytes = slice::from_raw_parts(da_ptr, da_len);
                    if let Ok(vv) = deserialize_version_vector(da_bytes) {
                        old_deleted_at = Some(vv);
                    }
                }
            }
        }
    }

    // Create operation version vector
    use treecrdt_core::{ReplicaId, VersionVector};
    let mut op_vv = VersionVector::new();
    op_vv.observe(&ReplicaId(replica.to_vec()), counter);

    // Calculate known_state for delete operations (Phase 1: Capture)
    let known_state: Option<VersionVector> = if kind == "delete" {
        match calculate_subtree_version_vector(db, &node) {
            Ok(vv) => Some(vv),
            Err(_) => None, // If calculation fails, proceed without known_state
        }
    } else {
        None
    };

    // Update last_change version vector
    // Note: Delete operations do NOT update last_change (matching core behavior)
    // In core, apply_delete does not call update_last_change
    let mut new_last_change = old_last_change.clone();
    if kind != "delete" && kind != "tombstone" {
        new_last_change.merge(&op_vv);
    }

    // Update deleted_at for delete operations (Phase 2: Tombstone)
    let mut new_deleted_at = old_deleted_at.clone();
    if kind == "delete" || kind == "tombstone" {
        let mut delete_vv = op_vv.clone();
        if let Some(ref ks) = known_state {
            delete_vv.merge(ks);
        }
        if let Some(ref mut existing) = new_deleted_at {
            existing.merge(&delete_vv);
        } else {
            new_deleted_at = Some(delete_vv);
        }
    }

    // Phase 3: Validate - Compute actual tombstone status based on awareness
    let computed_tombstone = if let Some(ref deleted_at_vv) = new_deleted_at {
        let current_subtree = match calculate_subtree_version_vector(db, &node) {
            Ok(vv) => vv,
            Err(_) => VersionVector::new(), // If calculation fails, assume not aware
        };
        deleted_at_vv.is_aware_of(&current_subtree)
    } else {
        false
    };

    let (next_parent, next_tombstone) = if kind == "insert" {
        (parent, false)
    } else if kind == "move" {
        (new_parent, false)
    } else if kind == "delete" || kind == "tombstone" {
        // Keep parent reference - don't detach on delete
        (old_parent, computed_tombstone)
    } else {
        // Unknown kind: treat as a no-op for tree materialization, but still advance head/seq.
        meta.head_lamport = lamport;
        meta.head_replica = replica.to_vec();
        meta.head_counter = counter;
        meta.head_seq = seq;
        return Ok(());
    };

    // Track opRef relevance for children(parent) filter.
    if let Some(p) = old_parent {
        unsafe {
            sqlite_clear_bindings(ctx.insert_opref);
            sqlite_reset(ctx.insert_opref);
            sqlite_bind_blob(
                ctx.insert_opref,
                1,
                p.as_ptr() as *const c_void,
                p.len() as c_int,
                None,
            );
            sqlite_bind_blob(
                ctx.insert_opref,
                2,
                op_ref.as_ptr() as *const c_void,
                op_ref.len() as c_int,
                None,
            );
            sqlite_bind_int64(ctx.insert_opref, 3, seq as i64);
            sqlite_step(ctx.insert_opref);
        }
    }
    if let Some(p) = next_parent {
        if old_parent != Some(p) {
            unsafe {
                sqlite_clear_bindings(ctx.insert_opref);
                sqlite_reset(ctx.insert_opref);
                sqlite_bind_blob(
                    ctx.insert_opref,
                    1,
                    p.as_ptr() as *const c_void,
                    p.len() as c_int,
                    None,
                );
                sqlite_bind_blob(
                    ctx.insert_opref,
                    2,
                    op_ref.as_ptr() as *const c_void,
                    op_ref.len() as c_int,
                    None,
                );
                sqlite_bind_int64(ctx.insert_opref, 3, seq as i64);
                sqlite_step(ctx.insert_opref);
            }
        }
    }

    // Update node with new state (parent, pos, tombstone, version vectors)
    // For delete operations, we keep the parent reference and only update tombstone flag
    let last_change_bytes = serialize_version_vector(&new_last_change).ok();
    let deleted_at_bytes = new_deleted_at.as_ref().and_then(|vv| serialize_version_vector(vv).ok());

    if kind == "delete" || kind == "tombstone" {
        // Keep parent reference - don't detach on delete.
        unsafe {
            sqlite_clear_bindings(ctx.detach);
            sqlite_reset(ctx.detach);
            sqlite_bind_blob(
                ctx.detach,
                1,
                node.as_ptr() as *const c_void,
                node.len() as c_int,
                None,
            );
            sqlite_bind_int64(ctx.detach, 2, if next_tombstone { 1 } else { 0 });
            if let Some(ref bytes) = last_change_bytes {
                sqlite_bind_blob(
                    ctx.detach,
                    3,
                    bytes.as_ptr() as *const c_void,
                    bytes.len() as c_int,
                    None,
                );
            } else {
                sqlite_bind_null(ctx.detach, 3);
            }
            if let Some(ref bytes) = deleted_at_bytes {
                sqlite_bind_blob(
                    ctx.detach,
                    4,
                    bytes.as_ptr() as *const c_void,
                    bytes.len() as c_int,
                    None,
                );
            } else {
                sqlite_bind_null(ctx.detach, 4);
            }
            sqlite_step(ctx.detach);
        }
    } else {
        // Insert/move: always detach then attach, even within the same parent (reordering).
        if !old_tombstone {
            if let (Some(p), Some(pos)) = (old_parent, old_pos) {
                unsafe {
                    sqlite_clear_bindings(ctx.shift_down);
                    sqlite_reset(ctx.shift_down);
                    sqlite_bind_blob(
                        ctx.shift_down,
                        1,
                        p.as_ptr() as *const c_void,
                        p.len() as c_int,
                        None,
                    );
                    sqlite_bind_int64(ctx.shift_down, 2, pos);
                    sqlite_step(ctx.shift_down);
                }
            }
        }

        unsafe {
            sqlite_clear_bindings(ctx.clear_parent_pos);
            sqlite_reset(ctx.clear_parent_pos);
            sqlite_bind_blob(
                ctx.clear_parent_pos,
                1,
                node.as_ptr() as *const c_void,
                node.len() as c_int,
                None,
            );
            sqlite_step(ctx.clear_parent_pos);
        }

        if let Some(p) = next_parent {
            // Determine final insert position (clamped).
            let mut len: i64 = 0;
            unsafe {
                sqlite_clear_bindings(ctx.max_pos);
                sqlite_reset(ctx.max_pos);
                sqlite_bind_blob(
                    ctx.max_pos,
                    1,
                    p.as_ptr() as *const c_void,
                    p.len() as c_int,
                    None,
                );
                let step_rc = sqlite_step(ctx.max_pos);
                if step_rc == SQLITE_ROW as c_int {
                    len = sqlite_column_int64(ctx.max_pos, 0);
                }
            }

            let mut pos = position.map(|v| v as i64).unwrap_or(len);
            if pos < 0 {
                pos = 0;
            }
            if pos > len {
                pos = len;
            }

            unsafe {
                sqlite_clear_bindings(ctx.shift_up);
                sqlite_reset(ctx.shift_up);
                sqlite_bind_blob(
                    ctx.shift_up,
                    1,
                    p.as_ptr() as *const c_void,
                    p.len() as c_int,
                    None,
                );
                sqlite_bind_int64(ctx.shift_up, 2, pos);
                sqlite_step(ctx.shift_up);

                sqlite_clear_bindings(ctx.attach);
                sqlite_reset(ctx.attach);
                sqlite_bind_blob(
                    ctx.attach,
                    1,
                    node.as_ptr() as *const c_void,
                    node.len() as c_int,
                    None,
                );
                sqlite_bind_blob(
                    ctx.attach,
                    2,
                    p.as_ptr() as *const c_void,
                    p.len() as c_int,
                    None,
                );
                sqlite_bind_int64(ctx.attach, 3, pos);
                if let Some(ref bytes) = last_change_bytes {
                    sqlite_bind_blob(
                        ctx.attach,
                        4,
                        bytes.as_ptr() as *const c_void,
                        bytes.len() as c_int,
                        None,
                    );
                } else {
                    sqlite_bind_null(ctx.attach, 4);
                }
                if let Some(ref bytes) = deleted_at_bytes {
                    sqlite_bind_blob(
                        ctx.attach,
                        5,
                        bytes.as_ptr() as *const c_void,
                        bytes.len() as c_int,
                        None,
                    );
                } else {
                    sqlite_bind_null(ctx.attach, 5);
                }
                sqlite_step(ctx.attach);
            }
        }

        // Update last_change for affected parents (matching core behavior).
        if kind == "insert" {
            if let Some(p) = parent {
                let _ = merge_last_change_delta(db, ctx, &p, &op_vv);
            }
            let _ = refresh_tombstones_upward(db, ctx, &[parent]);
        } else if kind == "move" {
            if let Some(p) = old_parent {
                let _ = merge_last_change_delta(db, ctx, &p, &op_vv);
            }
            if let Some(p) = new_parent {
                if old_parent != Some(p) {
                    let _ = merge_last_change_delta(db, ctx, &p, &op_vv);
                }
            }
            let _ = refresh_tombstones_upward(db, ctx, &[old_parent, new_parent]);
        }
    }

    meta.head_lamport = lamport;
    meta.head_replica = replica.to_vec();
    meta.head_counter = counter;
    meta.head_seq = seq;
    Ok(())
}

// Wrapper functions to abstract over the two build modes.
fn sqlite_create_function_v2(
    db: *mut sqlite3,
    name: *const c_char,
    n_arg: c_int,
    text_rep: c_int,
    p_app: *mut c_void,
    x_func: Option<unsafe extern "C" fn(*mut sqlite3_context, c_int, *mut *mut sqlite3_value)>,
    x_step: Option<unsafe extern "C" fn(*mut sqlite3_context, c_int, *mut *mut sqlite3_value)>,
    x_final: Option<unsafe extern "C" fn(*mut sqlite3_context)>,
    x_destroy: Option<unsafe extern "C" fn(*mut c_void)>,
) -> c_int {
    #[cfg(feature = "ext-sqlite")]
    {
        let api = api().expect("api table");
        unsafe {
            (api.create_function_v2.unwrap())(
                db, name, n_arg, text_rep, p_app, x_func, x_step, x_final, x_destroy,
            )
        }
    }
    #[cfg(feature = "static-link")]
    unsafe {
        ffi::sqlite3_create_function_v2(
            db, name, n_arg, text_rep, p_app, x_func, x_step, x_final, x_destroy,
        )
    }
}

fn sqlite_exec(
    db: *mut sqlite3,
    sql: *const c_char,
    cb: Option<
        unsafe extern "C" fn(*mut c_void, c_int, *mut *mut c_char, *mut *mut c_char) -> c_int,
    >,
    arg: *mut c_void,
    errmsg: *mut *mut c_char,
) -> c_int {
    #[cfg(feature = "ext-sqlite")]
    {
        let api = api().expect("api table");
        unsafe { (api.exec.unwrap())(db, sql, cb, arg, errmsg) }
    }
    #[cfg(feature = "static-link")]
    unsafe {
        ffi::sqlite3_exec(db, sql, cb, arg, errmsg)
    }
}

fn sqlite_prepare_v2(
    db: *mut sqlite3,
    sql: *const c_char,
    n_byte: c_int,
    stmt: *mut *mut sqlite3_stmt,
    tail: *mut *const c_char,
) -> c_int {
    #[cfg(feature = "ext-sqlite")]
    {
        let api = api().expect("api table");
        unsafe { (api.prepare_v2.unwrap())(db, sql, n_byte, stmt, tail) }
    }
    #[cfg(feature = "static-link")]
    unsafe {
        ffi::sqlite3_prepare_v2(db, sql, n_byte, stmt, tail)
    }
}

unsafe fn sqlite_bind_blob(
    stmt: *mut sqlite3_stmt,
    idx: c_int,
    ptr: *const c_void,
    len: c_int,
    destructor: Option<unsafe extern "C" fn(*mut c_void)>,
) -> c_int {
    #[cfg(feature = "ext-sqlite")]
    {
        let api = api().expect("api table");
        unsafe { (api.bind_blob.unwrap())(stmt, idx, ptr, len, destructor) }
    }
    #[cfg(feature = "static-link")]
    {
        unsafe { ffi::sqlite3_bind_blob(stmt, idx, ptr, len, destructor) }
    }
}

unsafe fn sqlite_bind_text(
    stmt: *mut sqlite3_stmt,
    idx: c_int,
    ptr: *const c_char,
    len: c_int,
    destructor: Option<unsafe extern "C" fn(*mut c_void)>,
) -> c_int {
    #[cfg(feature = "ext-sqlite")]
    {
        let api = api().expect("api table");
        unsafe { (api.bind_text.unwrap())(stmt, idx, ptr, len, destructor) }
    }
    #[cfg(feature = "static-link")]
    {
        unsafe { ffi::sqlite3_bind_text(stmt, idx, ptr, len, destructor) }
    }
}

unsafe fn sqlite_bind_int64(stmt: *mut sqlite3_stmt, idx: c_int, val: i64) -> c_int {
    #[cfg(feature = "ext-sqlite")]
    {
        let api = api().expect("api table");
        unsafe { (api.bind_int64.unwrap())(stmt, idx, val) }
    }
    #[cfg(feature = "static-link")]
    {
        unsafe { ffi::sqlite3_bind_int64(stmt, idx, val) }
    }
}

unsafe fn sqlite_bind_null(stmt: *mut sqlite3_stmt, idx: c_int) -> c_int {
    #[cfg(feature = "ext-sqlite")]
    {
        let api = api().expect("api table");
        unsafe { (api.bind_null.unwrap())(stmt, idx) }
    }
    #[cfg(feature = "static-link")]
    {
        unsafe { ffi::sqlite3_bind_null(stmt, idx) }
    }
}

unsafe fn sqlite_step(stmt: *mut sqlite3_stmt) -> c_int {
    #[cfg(feature = "ext-sqlite")]
    {
        let api = api().expect("api table");
        unsafe { (api.step.unwrap())(stmt) }
    }
    #[cfg(feature = "static-link")]
    {
        unsafe { ffi::sqlite3_step(stmt) }
    }
}

unsafe fn sqlite_reset(stmt: *mut sqlite3_stmt) -> c_int {
    #[cfg(feature = "ext-sqlite")]
    {
        let api = api().expect("api table");
        unsafe { (api.reset.unwrap())(stmt) }
    }
    #[cfg(feature = "static-link")]
    {
        unsafe { ffi::sqlite3_reset(stmt) }
    }
}

unsafe fn sqlite_clear_bindings(stmt: *mut sqlite3_stmt) -> c_int {
    #[cfg(feature = "ext-sqlite")]
    {
        let api = api().expect("api table");
        unsafe { (api.clear_bindings.unwrap())(stmt) }
    }
    #[cfg(feature = "static-link")]
    {
        unsafe { ffi::sqlite3_clear_bindings(stmt) }
    }
}

unsafe fn sqlite_finalize(stmt: *mut sqlite3_stmt) -> c_int {
    #[cfg(feature = "ext-sqlite")]
    {
        let api = api().expect("api table");
        unsafe { (api.finalize.unwrap())(stmt) }
    }
    #[cfg(feature = "static-link")]
    {
        unsafe { ffi::sqlite3_finalize(stmt) }
    }
}

unsafe fn sqlite_value_blob(val: *mut sqlite3_value) -> *const c_void {
    #[cfg(feature = "ext-sqlite")]
    {
        let api = api().expect("api table");
        unsafe { (api.value_blob.unwrap())(val) }
    }
    #[cfg(feature = "static-link")]
    {
        unsafe { ffi::sqlite3_value_blob(val) }
    }
}

unsafe fn sqlite_value_bytes(val: *mut sqlite3_value) -> c_int {
    #[cfg(feature = "ext-sqlite")]
    {
        let api = api().expect("api table");
        unsafe { (api.value_bytes.unwrap())(val) }
    }
    #[cfg(feature = "static-link")]
    {
        unsafe { ffi::sqlite3_value_bytes(val) }
    }
}

unsafe fn sqlite_value_int64(val: *mut sqlite3_value) -> i64 {
    #[cfg(feature = "ext-sqlite")]
    {
        let api = api().expect("api table");
        unsafe { (api.value_int64.unwrap())(val) }
    }
    #[cfg(feature = "static-link")]
    {
        unsafe { ffi::sqlite3_value_int64(val) }
    }
}

unsafe fn sqlite_value_text(val: *mut sqlite3_value) -> *const c_char {
    #[cfg(feature = "ext-sqlite")]
    {
        let api = api().expect("api table");
        unsafe { (api.value_text.unwrap())(val) as *const c_char }
    }
    #[cfg(feature = "static-link")]
    {
        unsafe { ffi::sqlite3_value_text(val) }
    }
}

unsafe fn sqlite_value_type(val: *mut sqlite3_value) -> c_int {
    #[cfg(feature = "ext-sqlite")]
    {
        let api = api().expect("api table");
        unsafe { (api.value_type.unwrap())(val) }
    }
    #[cfg(feature = "static-link")]
    {
        unsafe { ffi::sqlite3_value_type(val) }
    }
}

fn sqlite_result_text(
    ctx: *mut sqlite3_context,
    val: *const c_char,
    len: c_int,
    destructor: Option<unsafe extern "C" fn(*mut c_void)>,
) {
    #[cfg(feature = "ext-sqlite")]
    {
        let api = api().expect("api table");
        unsafe {
            (api.result_text.unwrap())(ctx, val, len, destructor);
        }
    }
    #[cfg(feature = "static-link")]
    unsafe {
        ffi::sqlite3_result_text(ctx, val, len, destructor);
    }
}

fn sqlite_result_error_code(ctx: *mut sqlite3_context, code: c_int) {
    #[cfg(feature = "ext-sqlite")]
    {
        if let Some(api) = api() {
            unsafe {
                (api.result_error_code.unwrap())(ctx, code);
            }
        }
    }
    #[cfg(feature = "static-link")]
    unsafe {
        ffi::sqlite3_result_error_code(ctx, code);
    }
}

fn sqlite_result_int(ctx: *mut sqlite3_context, val: c_int) {
    #[cfg(feature = "ext-sqlite")]
    {
        if let Some(api) = api() {
            unsafe {
                (api.result_int.unwrap())(ctx, val);
            }
        }
    }
    #[cfg(feature = "static-link")]
    unsafe {
        ffi::sqlite3_result_int(ctx, val);
    }
}

fn sqlite_result_int64(ctx: *mut sqlite3_context, val: i64) {
    #[cfg(feature = "ext-sqlite")]
    {
        if let Some(api) = api() {
            unsafe {
                (api.result_int64.unwrap())(ctx, val);
            }
        }
    }
    #[cfg(feature = "static-link")]
    unsafe {
        ffi::sqlite3_result_int64(ctx, val);
    }
}

fn sqlite_result_error(ctx: *mut sqlite3_context, msg: *const c_char) {
    #[cfg(feature = "ext-sqlite")]
    {
        if let Some(api) = api() {
            unsafe {
                (api.result_error.unwrap())(ctx, msg, -1);
            }
        }
    }
    #[cfg(feature = "static-link")]
    unsafe {
        ffi::sqlite3_result_error(ctx, msg, -1);
    }
}

fn sqlite_context_db_handle(ctx: *mut sqlite3_context) -> *mut sqlite3 {
    #[cfg(feature = "ext-sqlite")]
    {
        let api = api().expect("api table");
        unsafe { (api.context_db_handle.unwrap())(ctx) }
    }
    #[cfg(feature = "static-link")]
    unsafe {
        ffi::sqlite3_context_db_handle(ctx)
    }
}

unsafe fn sqlite_column_blob(stmt: *mut sqlite3_stmt, idx: c_int) -> *const c_void {
    #[cfg(feature = "ext-sqlite")]
    {
        let api = api().expect("api table");
        unsafe { (api.column_blob.unwrap())(stmt, idx) }
    }
    #[cfg(feature = "static-link")]
    {
        unsafe { ffi::sqlite3_column_blob(stmt, idx) }
    }
}

unsafe fn sqlite_column_bytes(stmt: *mut sqlite3_stmt, idx: c_int) -> c_int {
    #[cfg(feature = "ext-sqlite")]
    {
        let api = api().expect("api table");
        unsafe { (api.column_bytes.unwrap())(stmt, idx) }
    }
    #[cfg(feature = "static-link")]
    {
        unsafe { ffi::sqlite3_column_bytes(stmt, idx) }
    }
}

unsafe fn sqlite_column_int64(stmt: *mut sqlite3_stmt, idx: c_int) -> i64 {
    #[cfg(feature = "ext-sqlite")]
    {
        let api = api().expect("api table");
        unsafe { (api.column_int64.unwrap())(stmt, idx) }
    }
    #[cfg(feature = "static-link")]
    {
        unsafe { ffi::sqlite3_column_int64(stmt, idx) }
    }
}

unsafe fn sqlite_column_text(stmt: *mut sqlite3_stmt, idx: c_int) -> *const c_char {
    #[cfg(feature = "ext-sqlite")]
    {
        let api = api().expect("api table");
        unsafe { (api.column_text.unwrap())(stmt, idx) as *const c_char }
    }
    #[cfg(feature = "static-link")]
    {
        unsafe { ffi::sqlite3_column_text(stmt, idx) }
    }
}

unsafe fn sqlite_column_type(stmt: *mut sqlite3_stmt, idx: c_int) -> c_int {
    #[cfg(feature = "ext-sqlite")]
    {
        let api = api().expect("api table");
        unsafe { (api.column_type.unwrap())(stmt, idx) }
    }
    #[cfg(feature = "static-link")]
    {
        unsafe { ffi::sqlite3_column_type(stmt, idx) }
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
    #[cfg(feature = "static-link")]
    let _ = p_api;
    #[cfg(feature = "ext-sqlite")]
    unsafe {
        if p_api.is_null() {
            return SQLITE_ERROR as c_int;
        }
        SQLITE3_API = p_api;
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
            8,
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

fn ensure_schema(db: *mut sqlite3) -> Result<(), c_int> {
    #[cfg(feature = "ext-sqlite")]
    {
        if api().is_none() {
            return Err(SQLITE_ERROR as c_int);
        }
    }

    // Core tables.
    const META: &str = r#"
CREATE TABLE IF NOT EXISTS meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
"#;
    const OPS: &str = r#"
CREATE TABLE IF NOT EXISTS ops (
  replica BLOB NOT NULL,
  counter INTEGER NOT NULL,
  lamport INTEGER NOT NULL,
  kind TEXT NOT NULL,
  parent BLOB,
  node BLOB NOT NULL,
  new_parent BLOB,
  position INTEGER,
  op_ref BLOB,
  known_state BLOB,
  PRIMARY KEY (replica, counter)
);
"#;
    // Materialized tree state + indexes (v1).
    const TREE_META: &str = r#"
CREATE TABLE IF NOT EXISTS tree_meta (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  dirty INTEGER NOT NULL DEFAULT 1,
  head_lamport INTEGER NOT NULL DEFAULT 0,
  head_replica BLOB NOT NULL DEFAULT X'',
  head_counter INTEGER NOT NULL DEFAULT 0,
  head_seq INTEGER NOT NULL DEFAULT 0
);
INSERT OR IGNORE INTO tree_meta(id) VALUES (1);
"#;
    const TREE_NODES: &str = r#"
CREATE TABLE IF NOT EXISTS tree_nodes (
  node BLOB PRIMARY KEY,
  parent BLOB,
  pos INTEGER,
  tombstone INTEGER NOT NULL DEFAULT 0,
  last_change BLOB,
  deleted_at BLOB
);
"#;
    const OPREFS_CHILDREN: &str = r#"
CREATE TABLE IF NOT EXISTS oprefs_children (
  parent BLOB NOT NULL,
  op_ref BLOB NOT NULL,
  seq INTEGER NOT NULL,
  PRIMARY KEY (parent, op_ref)
);
"#;

    let rc_meta = {
        let sql = CString::new(META).expect("meta schema");
        sqlite_exec(db, sql.as_ptr(), None, null_mut(), null_mut())
    };
    if rc_meta != SQLITE_OK as c_int {
        return Err(rc_meta);
    }

    let rc_ops = {
        let sql = CString::new(OPS).expect("ops schema");
        sqlite_exec(db, sql.as_ptr(), None, null_mut(), null_mut())
    };
    if rc_ops != SQLITE_OK as c_int {
        return Err(rc_ops);
    }

    let rc_tree_meta = {
        let sql = CString::new(TREE_META).expect("tree meta schema");
        sqlite_exec(db, sql.as_ptr(), None, null_mut(), null_mut())
    };
    if rc_tree_meta != SQLITE_OK as c_int {
        return Err(rc_tree_meta);
    }

    let rc_tree_nodes = {
        let sql = CString::new(TREE_NODES).expect("tree nodes schema");
        sqlite_exec(db, sql.as_ptr(), None, null_mut(), null_mut())
    };
    if rc_tree_nodes != SQLITE_OK as c_int {
        return Err(rc_tree_nodes);
    }

    let rc_oprefs_children = {
        let sql = CString::new(OPREFS_CHILDREN).expect("oprefs children schema");
        sqlite_exec(db, sql.as_ptr(), None, null_mut(), null_mut())
    };
    if rc_oprefs_children != SQLITE_OK as c_int {
        return Err(rc_oprefs_children);
    }

    // Indexes.
    const INDEXES: &str = r#"
CREATE INDEX IF NOT EXISTS idx_ops_lamport ON ops(lamport, replica, counter);
CREATE INDEX IF NOT EXISTS idx_ops_op_ref ON ops(op_ref);
CREATE INDEX IF NOT EXISTS idx_tree_nodes_parent_pos ON tree_nodes(parent, pos);
CREATE INDEX IF NOT EXISTS idx_oprefs_children_parent_seq ON oprefs_children(parent, seq);
"#;
    let rc_idx = {
        let sql = CString::new(INDEXES).expect("index schema");
        sqlite_exec(db, sql.as_ptr(), None, null_mut(), null_mut())
    };
    if rc_idx != SQLITE_OK as c_int {
        return Err(rc_idx);
    }

    // If this is a fresh database with no ops yet, mark materialization clean so appends can
    // maintain state incrementally without a full rebuild.
    let mut ops_count: i64 = 0;
    {
        let sql = CString::new("SELECT COUNT(*) FROM ops").expect("count ops sql");
        let mut stmt: *mut sqlite3_stmt = null_mut();
        let rc = sqlite_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, null_mut());
        if rc == SQLITE_OK as c_int {
            let step_rc = unsafe { sqlite_step(stmt) };
            if step_rc == SQLITE_ROW as c_int {
                ops_count = unsafe { sqlite_column_int64(stmt, 0) };
            }
            unsafe { sqlite_finalize(stmt) };
        }
    }
    if ops_count == 0 {
        let _ = set_tree_meta_dirty(db, false);
        // Ensure ROOT exists even before first rebuild.
        let _ = {
            let sql = CString::new(
                "INSERT OR IGNORE INTO tree_nodes(node,parent,pos,tombstone) VALUES (?1,NULL,0,0)",
            )
            .expect("root insert sql");
            let mut stmt: *mut sqlite3_stmt = null_mut();
            let rc = sqlite_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, null_mut());
            if rc != SQLITE_OK as c_int {
                rc
            } else {
                unsafe {
                    sqlite_bind_blob(
                        stmt,
                        1,
                        ROOT_NODE_ID.as_ptr() as *const c_void,
                        ROOT_NODE_ID.len() as c_int,
                        None,
                    );
                    sqlite_step(stmt);
                    sqlite_finalize(stmt)
                }
            }
        };
    }

    Ok(())
}

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
/// replica BLOB, counter INT, lamport INT, kind TEXT, parent BLOB|null, node BLOB, new_parent BLOB|null, position INT|null
unsafe extern "C" fn treecrdt_append_op(
    ctx: *mut sqlite3_context,
    argc: c_int,
    argv: *mut *mut sqlite3_value,
) {
    #[cfg(feature = "ext-sqlite")]
    {
        if api().is_none() {
            sqlite_result_error_code(ctx, SQLITE_ERROR as c_int);
            return;
        }
    }

    if argc != 8 {
        sqlite_result_error(
            ctx,
            b"treecrdt_append_op expects 8 args\0".as_ptr() as *const c_char,
        );
        return;
    }

    let db = sqlite_context_db_handle(ctx);
    let sql = CString::new(
        "INSERT OR IGNORE INTO ops (replica,counter,lamport,kind,parent,node,new_parent,position,op_ref) \
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)",
    )
    .expect("static sql");

    let mut stmt: *mut sqlite3_stmt = null_mut();
    let rc = sqlite_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, null_mut());
    if rc != SQLITE_OK as c_int {
        sqlite_result_error_code(ctx, rc);
        return;
    }

    let args = unsafe { std::slice::from_raw_parts(argv, argc as usize) };

    let doc_id = match load_doc_id(db) {
        Ok(Some(v)) => v,
        Ok(None) => {
            unsafe { sqlite_finalize(stmt) };
            sqlite_result_error(
                ctx,
                b"treecrdt_append_op: doc_id not set (call treecrdt_set_doc_id)\0".as_ptr()
                    as *const c_char,
            );
            return;
        }
        Err(rc) => {
            unsafe { sqlite_finalize(stmt) };
            sqlite_result_error_code(ctx, rc);
            return;
        }
    };

    let replica_ptr = unsafe { sqlite_value_blob(args[0]) } as *const u8;
    let replica_len = unsafe { sqlite_value_bytes(args[0]) } as usize;
    if replica_ptr.is_null() {
        unsafe { sqlite_finalize(stmt) };
        sqlite_result_error(
            ctx,
            b"treecrdt_append_op: NULL replica\0".as_ptr() as *const c_char,
        );
        return;
    }
    let replica = unsafe { slice::from_raw_parts(replica_ptr, replica_len) };
    let counter_i64 = unsafe { sqlite_value_int64(args[1]) };
    if counter_i64 < 0 {
        unsafe { sqlite_finalize(stmt) };
        sqlite_result_error(
            ctx,
            b"treecrdt_append_op: counter must be >= 0\0".as_ptr() as *const c_char,
        );
        return;
    }
    let op_ref = derive_op_ref_v0(&doc_id, replica, counter_i64 as u64);

    let mut bind_err = false;
    bind_err |= unsafe { bind_blob(stmt, 1, args[0]) };
    bind_err |= unsafe { bind_int64(stmt, 2, args[1]) };
    bind_err |= unsafe { bind_int64(stmt, 3, args[2]) };
    bind_err |= unsafe { bind_text(stmt, 4, args[3]) };
    bind_err |= unsafe { bind_optional_blob(stmt, 5, args[4]) };
    bind_err |= unsafe { bind_blob(stmt, 6, args[5]) };
    bind_err |= unsafe { bind_optional_blob(stmt, 7, args[6]) };
    bind_err |= unsafe { bind_optional_int(stmt, 8, args[7]) };
    bind_err |= unsafe {
        sqlite_bind_blob(
            stmt,
            9,
            op_ref.as_ptr() as *const c_void,
            OPREF_V0_WIDTH as c_int,
            None,
        ) != SQLITE_OK as c_int
    };

    if bind_err {
        unsafe { sqlite_finalize(stmt) };
        sqlite_result_error_code(ctx, SQLITE_ERROR as c_int);
        return;
    }

    let step_rc = unsafe { sqlite_step(stmt) };
    let finalize_rc = unsafe { sqlite_finalize(stmt) };
    if step_rc != SQLITE_DONE as c_int || finalize_rc != SQLITE_OK as c_int {
        let rc = if step_rc != SQLITE_DONE as c_int {
            step_rc
        } else {
            finalize_rc
        };
        sqlite_result_error_code(ctx, rc);
        return;
    }

    // Determine whether the op row was newly inserted (vs ignored due to duplicate op_id).
    let inserted = match last_changes(db) {
        Ok(v) => v > 0,
        Err(_) => false,
    };

    if !inserted {
        // Backfill op_ref if this is an older row created before doc_id support.
        let upd_sql = CString::new(
            "UPDATE ops SET op_ref = ?3 WHERE replica = ?1 AND counter = ?2 AND op_ref IS NULL",
        )
        .expect("update op_ref sql");
        let mut upd: *mut sqlite3_stmt = null_mut();
        let upd_rc = sqlite_prepare_v2(db, upd_sql.as_ptr(), -1, &mut upd, null_mut());
        if upd_rc == SQLITE_OK as c_int {
            unsafe {
                sqlite_bind_blob(
                    upd,
                    1,
                    replica.as_ptr() as *const c_void,
                    replica.len() as c_int,
                    None,
                );
                sqlite_bind_int64(upd, 2, counter_i64);
                sqlite_bind_blob(
                    upd,
                    3,
                    op_ref.as_ptr() as *const c_void,
                    op_ref.len() as c_int,
                    None,
                );
                sqlite_step(upd);
                sqlite_finalize(upd);
            }
        }
        sqlite_result_int(ctx, 1);
        return;
    }

    // Materialize incrementally (best-effort). If out-of-order, mark dirty; next query rebuilds.
    let mut meta = match load_tree_meta(db) {
        Ok(v) => v,
        Err(rc) => {
            sqlite_result_error_code(ctx, rc);
            return;
        }
    };

    // Parse op fields needed for materialization.
    let kind_ptr = unsafe { sqlite_value_text(args[3]) } as *const u8;
    let kind_len = unsafe { sqlite_value_bytes(args[3]) } as usize;
    let kind = if kind_ptr.is_null() {
        ""
    } else {
        std::str::from_utf8(unsafe { slice::from_raw_parts(kind_ptr, kind_len) }).unwrap_or("")
    };

    let node_ptr = unsafe { sqlite_value_blob(args[5]) } as *const u8;
    let node_len = unsafe { sqlite_value_bytes(args[5]) } as usize;
    if node_ptr.is_null() || node_len != 16 {
        let _ = set_tree_meta_dirty(db, true);
        sqlite_result_int(ctx, 1);
        return;
    }
    let mut node = [0u8; 16];
    node.copy_from_slice(unsafe { slice::from_raw_parts(node_ptr, node_len) });

    let parent = unsafe {
        if sqlite_value_type(args[4]) == SQLITE_NULL as c_int {
            None
        } else {
            let ptr = sqlite_value_blob(args[4]) as *const u8;
            let len = sqlite_value_bytes(args[4]) as usize;
            if ptr.is_null() || len != 16 {
                None
            } else {
                let mut out = [0u8; 16];
                out.copy_from_slice(slice::from_raw_parts(ptr, len));
                Some(out)
            }
        }
    };

    let new_parent = unsafe {
        if sqlite_value_type(args[6]) == SQLITE_NULL as c_int {
            None
        } else {
            let ptr = sqlite_value_blob(args[6]) as *const u8;
            let len = sqlite_value_bytes(args[6]) as usize;
            if ptr.is_null() || len != 16 {
                None
            } else {
                let mut out = [0u8; 16];
                out.copy_from_slice(slice::from_raw_parts(ptr, len));
                Some(out)
            }
        }
    };

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

    let lamport_val = unsafe { sqlite_value_int64(args[2]) as Lamport };

    // Capture and persist delete known_state (subtree version vector) so defensive deletion
    // remains correct across out-of-order inserts and rebuilds.
    if kind == "delete" && !meta.dirty {
        let known_state_vv = calculate_subtree_version_vector(db, &node).ok();
        if let Some(vv) = known_state_vv {
            if let Ok(bytes) = serialize_version_vector(&vv) {
                let upd_sql = CString::new(
                    "UPDATE ops SET known_state = ?3 WHERE replica = ?1 AND counter = ?2",
                )
                .expect("update known_state sql");
                let mut upd_stmt: *mut sqlite3_stmt = null_mut();
                let upd_rc = sqlite_prepare_v2(db, upd_sql.as_ptr(), -1, &mut upd_stmt, null_mut());
                if upd_rc == SQLITE_OK as c_int {
                    unsafe {
                        sqlite_bind_blob(
                            upd_stmt,
                            1,
                            replica.as_ptr() as *const c_void,
                            replica.len() as c_int,
                            None,
                        );
                        sqlite_bind_int64(upd_stmt, 2, counter_i64);
                        sqlite_bind_blob(
                            upd_stmt,
                            3,
                            bytes.as_ptr() as *const c_void,
                            bytes.len() as c_int,
                            None,
                        );
                        sqlite_step(upd_stmt);
                        sqlite_finalize(upd_stmt);
                    }
                } else {
                    let _ = unsafe { sqlite_finalize(upd_stmt) };
                }
            }
        }
    }

    let mut ctxm = match MaterializeCtx::prepare(db) {
        Ok(v) => v,
        Err(rc) => {
            sqlite_result_error_code(ctx, rc);
            return;
        }
    };
    let _ = materialize_inserted_op(
        db,
        &mut ctxm,
        &mut meta,
        kind,
        node,
        parent,
        new_parent,
        position,
        op_ref,
        lamport_val,
        replica,
        counter_i64 as u64,
    );
    unsafe { ctxm.finalize() };

    if meta.dirty {
        let _ = set_tree_meta_dirty(db, true);
    } else {
        let _ = update_tree_meta_head(
            db,
            meta.head_lamport,
            &meta.head_replica,
            meta.head_counter,
            meta.head_seq,
        );
    }

    sqlite_result_int(ctx, 1);
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
    let begin = CString::new("SAVEPOINT treecrdt_append_ops").expect("static");
    let commit = CString::new("RELEASE treecrdt_append_ops").expect("static");
    let rollback = CString::new("ROLLBACK TO treecrdt_append_ops; RELEASE treecrdt_append_ops")
        .expect("static");

    if sqlite_exec(db, begin.as_ptr(), None, null_mut(), null_mut()) != SQLITE_OK as c_int {
        sqlite_result_error_code(ctx, SQLITE_ERROR as c_int);
        return;
    }

    let insert_sql = CString::new(
        "INSERT OR IGNORE INTO ops (replica,counter,lamport,kind,parent,node,new_parent,position,op_ref) \
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)",
    )
    .expect("static sql");
    let mut stmt: *mut sqlite3_stmt = null_mut();
    let prep_rc = sqlite_prepare_v2(db, insert_sql.as_ptr(), -1, &mut stmt, null_mut());
    if prep_rc != SQLITE_OK as c_int {
        sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
        sqlite_result_error_code(ctx, prep_rc);
        return;
    }

    let update_opref_sql = CString::new(
        "UPDATE ops SET op_ref = ?3 WHERE replica = ?1 AND counter = ?2 AND op_ref IS NULL",
    )
    .expect("update op_ref sql");
    let mut upd_stmt: *mut sqlite3_stmt = null_mut();
    let upd_prep_rc =
        sqlite_prepare_v2(db, update_opref_sql.as_ptr(), -1, &mut upd_stmt, null_mut());
    if upd_prep_rc != SQLITE_OK as c_int {
        unsafe { sqlite_finalize(stmt) };
        sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
        sqlite_result_error_code(ctx, upd_prep_rc);
        return;
    }

    let update_known_state_sql =
        CString::new("UPDATE ops SET known_state = ?3 WHERE replica = ?1 AND counter = ?2")
            .expect("update known_state sql");
    let mut known_state_stmt: *mut sqlite3_stmt = null_mut();
    let known_state_rc = sqlite_prepare_v2(
        db,
        update_known_state_sql.as_ptr(),
        -1,
        &mut known_state_stmt,
        null_mut(),
    );
    if known_state_rc != SQLITE_OK as c_int {
        unsafe { sqlite_finalize(stmt) };
        unsafe { sqlite_finalize(upd_stmt) };
        sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
        sqlite_result_error_code(ctx, known_state_rc);
        return;
    }

    let changes_sql = CString::new("SELECT changes()").expect("static sql");
    let mut changes_stmt: *mut sqlite3_stmt = null_mut();
    let changes_rc = sqlite_prepare_v2(db, changes_sql.as_ptr(), -1, &mut changes_stmt, null_mut());
    if changes_rc != SQLITE_OK as c_int {
        unsafe { sqlite_finalize(stmt) };
        unsafe { sqlite_finalize(upd_stmt) };
        unsafe { sqlite_finalize(known_state_stmt) };
        sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
        sqlite_result_error_code(ctx, changes_rc);
        return;
    }

    let mut materialize_ctx = match MaterializeCtx::prepare(db) {
        Ok(v) => v,
        Err(rc) => {
            unsafe { sqlite_finalize(stmt) };
            unsafe { sqlite_finalize(upd_stmt) };
            unsafe { sqlite_finalize(changes_stmt) };
            sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
            sqlite_result_error_code(ctx, rc);
            return;
        }
    };
    let mut tree_meta = match load_tree_meta(db) {
        Ok(v) => v,
        Err(rc) => {
            unsafe { sqlite_finalize(stmt) };
            unsafe { sqlite_finalize(upd_stmt) };
            unsafe { sqlite_finalize(changes_stmt) };
            unsafe { materialize_ctx.finalize() };
            sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
            sqlite_result_error_code(ctx, rc);
            return;
        }
    };

    let mut inserted: i64 = 0;
    let mut err_rc: c_int = SQLITE_OK as c_int;
    for op in ops {
        unsafe {
            sqlite_clear_bindings(stmt);
            sqlite_reset(stmt);
        }
        let mut bind_err = false;
        unsafe {
            bind_err |= sqlite_bind_blob(
                stmt,
                1,
                op.replica.as_ptr() as *const c_void,
                op.replica.len() as c_int,
                None,
            ) != SQLITE_OK as c_int;
            bind_err |= sqlite_bind_int64(stmt, 2, op.counter as i64) != SQLITE_OK as c_int;
            bind_err |= sqlite_bind_int64(stmt, 3, op.lamport as i64) != SQLITE_OK as c_int;
        }
        let kind_cstr =
            CString::new(op.kind.as_str()).unwrap_or_else(|_| CString::new("insert").unwrap());
        unsafe {
            bind_err |=
                sqlite_bind_text(stmt, 4, kind_cstr.as_ptr(), -1, None) != SQLITE_OK as c_int;
        }
        unsafe {
            if let Some(parent) = op.parent.as_ref() {
                bind_err |= sqlite_bind_blob(
                    stmt,
                    5,
                    parent.as_ptr() as *const c_void,
                    parent.len() as c_int,
                    None,
                ) != SQLITE_OK as c_int;
            } else {
                bind_err |= sqlite_bind_null(stmt, 5) != SQLITE_OK as c_int;
            }
            bind_err |= sqlite_bind_blob(
                stmt,
                6,
                op.node.as_ptr() as *const c_void,
                op.node.len() as c_int,
                None,
            ) != SQLITE_OK as c_int;
            if let Some(newp) = op.new_parent.as_ref() {
                bind_err |= sqlite_bind_blob(
                    stmt,
                    7,
                    newp.as_ptr() as *const c_void,
                    newp.len() as c_int,
                    None,
                ) != SQLITE_OK as c_int;
            } else {
                bind_err |= sqlite_bind_null(stmt, 7) != SQLITE_OK as c_int;
            }
            if let Some(pos) = op.position {
                bind_err |= sqlite_bind_int64(stmt, 8, pos as i64) != SQLITE_OK as c_int;
            } else {
                bind_err |= sqlite_bind_null(stmt, 8) != SQLITE_OK as c_int;
            }
        }
        let op_ref = derive_op_ref_v0(&doc_id, &op.replica, op.counter);
        unsafe {
            bind_err |= sqlite_bind_blob(
                stmt,
                9,
                op_ref.as_ptr() as *const c_void,
                OPREF_V0_WIDTH as c_int,
                None,
            ) != SQLITE_OK as c_int;
        }

        if bind_err {
            err_rc = SQLITE_ERROR as c_int;
            break;
        }

        let step_rc = unsafe { sqlite_step(stmt) };
        if step_rc != SQLITE_DONE as c_int {
            err_rc = step_rc;
            break;
        }

        // Check whether this row was inserted (vs ignored due to duplicate).
        let mut changed: i64 = 0;
        unsafe {
            sqlite_reset(changes_stmt);
            let rc = sqlite_step(changes_stmt);
            if rc == SQLITE_ROW as c_int {
                changed = sqlite_column_int64(changes_stmt, 0);
            }
            sqlite_reset(changes_stmt);
        }

        if changed > 0 {
            inserted += 1;

            if !tree_meta.dirty {
                // Convert op fields for materialization.
                let node = if op.node.len() == 16 {
                    let mut out = [0u8; 16];
                    out.copy_from_slice(&op.node);
                    out
                } else {
                    tree_meta.dirty = true;
                    continue;
                };

                if op.kind == "delete" {
                    if let Ok(vv) = calculate_subtree_version_vector(db, &node) {
                        if let Ok(bytes) = serialize_version_vector(&vv) {
                            unsafe {
                                sqlite_clear_bindings(known_state_stmt);
                                sqlite_reset(known_state_stmt);
                                sqlite_bind_blob(
                                    known_state_stmt,
                                    1,
                                    op.replica.as_ptr() as *const c_void,
                                    op.replica.len() as c_int,
                                    None,
                                );
                                sqlite_bind_int64(known_state_stmt, 2, op.counter as i64);
                                sqlite_bind_blob(
                                    known_state_stmt,
                                    3,
                                    bytes.as_ptr() as *const c_void,
                                    bytes.len() as c_int,
                                    None,
                                );
                                sqlite_step(known_state_stmt);
                                sqlite_reset(known_state_stmt);
                            }
                        }
                    }
                }

                let parent = op.parent.as_ref().and_then(|bytes| {
                    if bytes.len() != 16 {
                        None
                    } else {
                        let mut out = [0u8; 16];
                        out.copy_from_slice(bytes);
                        Some(out)
                    }
                });
                let new_parent = op.new_parent.as_ref().and_then(|bytes| {
                    if bytes.len() != 16 {
                        None
                    } else {
                        let mut out = [0u8; 16];
                        out.copy_from_slice(bytes);
                        Some(out)
                    }
                });
                let _ = materialize_inserted_op(
                    db,
                    &mut materialize_ctx,
                    &mut tree_meta,
                    &op.kind,
                    node,
                    parent,
                    new_parent,
                    op.position,
                    op_ref,
                    op.lamport,
                    &op.replica,
                    op.counter,
                );
            }
        } else {
            // Backfill op_ref for older rows that lack it.
            unsafe {
                sqlite_clear_bindings(upd_stmt);
                sqlite_reset(upd_stmt);
                sqlite_bind_blob(
                    upd_stmt,
                    1,
                    op.replica.as_ptr() as *const c_void,
                    op.replica.len() as c_int,
                    None,
                );
                sqlite_bind_int64(upd_stmt, 2, op.counter as i64);
                sqlite_bind_blob(
                    upd_stmt,
                    3,
                    op_ref.as_ptr() as *const c_void,
                    op_ref.len() as c_int,
                    None,
                );
                sqlite_step(upd_stmt);
            }
        }
    }

    unsafe { sqlite_finalize(stmt) };
    unsafe { sqlite_finalize(upd_stmt) };
    unsafe { sqlite_finalize(changes_stmt) };
    unsafe { sqlite_finalize(known_state_stmt) };
    unsafe { materialize_ctx.finalize() };

    if err_rc == SQLITE_OK as c_int {
        if tree_meta.dirty {
            let _ = set_tree_meta_dirty(db, true);
        } else {
            let _ = update_tree_meta_head(
                db,
                tree_meta.head_lamport,
                &tree_meta.head_replica,
                tree_meta.head_counter,
                tree_meta.head_seq,
            );
        }
        let commit_rc = sqlite_exec(db, commit.as_ptr(), None, null_mut(), null_mut());
        if commit_rc == SQLITE_OK as c_int {
            sqlite_result_int(ctx, inserted as c_int);
            return;
        } else {
            err_rc = commit_rc;
        }
    }

    sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
    sqlite_result_error_code(ctx, err_rc);
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

unsafe fn column_blob16(stmt: *mut sqlite3_stmt, idx: c_int) -> Result<Option<[u8; 16]>, c_int> {
    let ty = unsafe { sqlite_column_type(stmt, idx) };
    if ty == SQLITE_NULL as c_int {
        return Ok(None);
    }
    let ptr = unsafe { sqlite_column_blob(stmt, idx) };
    let len = unsafe { sqlite_column_bytes(stmt, idx) };
    if ptr.is_null() || len != 16 {
        return Err(SQLITE_ERROR as c_int);
    }
    let bytes = unsafe { slice::from_raw_parts(ptr as *const u8, len as usize) };
    let mut out = [0u8; 16];
    out.copy_from_slice(bytes);
    Ok(Some(out))
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
        "SELECT replica,counter,lamport,kind,parent,node,new_parent,position \
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

unsafe fn bind_blob(stmt: *mut sqlite3_stmt, idx: c_int, val: *mut sqlite3_value) -> bool {
    let ptr = unsafe { sqlite_value_blob(val) };
    let len = unsafe { sqlite_value_bytes(val) };
    unsafe { sqlite_bind_blob(stmt, idx, ptr, len, None) != SQLITE_OK as c_int }
}

unsafe fn bind_optional_blob(stmt: *mut sqlite3_stmt, idx: c_int, val: *mut sqlite3_value) -> bool {
    let ty = unsafe { sqlite_value_type(val) };
    if ty == SQLITE_NULL as c_int {
        unsafe { sqlite_bind_null(stmt, idx) != SQLITE_OK as c_int }
    } else {
        unsafe { bind_blob(stmt, idx, val) }
    }
}

unsafe fn bind_text(stmt: *mut sqlite3_stmt, idx: c_int, val: *mut sqlite3_value) -> bool {
    let ptr = unsafe { sqlite_value_text(val) };
    let len = unsafe { sqlite_value_bytes(val) };
    unsafe { sqlite_bind_text(stmt, idx, ptr as *const c_char, len, None) != SQLITE_OK as c_int }
}

unsafe fn bind_int64(stmt: *mut sqlite3_stmt, idx: c_int, val: *mut sqlite3_value) -> bool {
    let v = unsafe { sqlite_value_int64(val) };
    unsafe { sqlite_bind_int64(stmt, idx, v) != SQLITE_OK as c_int }
}

unsafe fn bind_optional_int(stmt: *mut sqlite3_stmt, idx: c_int, val: *mut sqlite3_value) -> bool {
    let ty = unsafe { sqlite_value_type(val) };
    if ty == SQLITE_NULL as c_int {
        unsafe { sqlite_bind_null(stmt, idx) != SQLITE_OK as c_int }
    } else {
        let v = unsafe { sqlite_value_int64(val) };
        unsafe { sqlite_bind_int64(stmt, idx, v) != SQLITE_OK as c_int }
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
        "SELECT replica,counter,lamport,kind,parent,node,new_parent,position \
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

        Ok(JsonOp {
            replica,
            counter,
            lamport,
            kind,
            parent,
            node,
            new_parent,
            position,
        })
    }
}

unsafe fn column_int_opt(stmt: *mut sqlite3_stmt, idx: c_int) -> Option<u64> {
    let ty = unsafe { sqlite_column_type(stmt, idx) };
    if ty == SQLITE_NULL as c_int {
        None
    } else {
        Some(unsafe { sqlite_column_int64(stmt, idx) as u64 })
    }
}

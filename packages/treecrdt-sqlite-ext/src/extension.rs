//! SQLite extension entrypoint implemented against the SQLite C API.
//! This is intentionally minimal: it proves the cross-target build (native SQLite + wa-sqlite)
//! and registers a basic function to verify loading. Additional virtual tables/functions will
//! bridge to `treecrdt-core`.

#![allow(non_snake_case)]

use std::ffi::CString;
use std::collections::HashSet;
use std::os::raw::{c_char, c_int, c_void};
#[cfg(feature = "ext-sqlite")]
use std::ptr::null;
use std::ptr::null_mut;
use std::slice;

use treecrdt_core::{Lamport, NodeId, NodeStore, VersionVector};

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

fn sqlite_node_id_bytes(node: NodeId) -> [u8; 16] {
    node.0.to_be_bytes()
}

fn sqlite_bytes_to_node_id(bytes: [u8; 16]) -> NodeId {
    NodeId(u128::from_be_bytes(bytes))
}

fn sqlite_rc_error(rc: c_int, context: &str) -> treecrdt_core::Error {
    treecrdt_core::Error::Storage(format!("{context} (rc={rc})"))
}

fn vv_to_bytes(vv: &VersionVector) -> treecrdt_core::Result<Vec<u8>> {
    serde_json::to_vec(vv).map_err(|e| treecrdt_core::Error::Storage(e.to_string()))
}

fn vv_from_bytes(bytes: &[u8]) -> treecrdt_core::Result<VersionVector> {
    serde_json::from_slice(bytes).map_err(|e| treecrdt_core::Error::Storage(e.to_string()))
}

struct SqliteNodeStore {
    db: *mut sqlite3,
    ensure_node: *mut sqlite3_stmt,
    exists: *mut sqlite3_stmt,
    select_node: *mut sqlite3_stmt,
    select_children: *mut sqlite3_stmt,
    all_nodes: *mut sqlite3_stmt,
    shift_down: *mut sqlite3_stmt,
    shift_up: *mut sqlite3_stmt,
    max_pos: *mut sqlite3_stmt,
    clear_parent_pos: *mut sqlite3_stmt,
    set_parent_pos: *mut sqlite3_stmt,
    update_last_change: *mut sqlite3_stmt,
    update_deleted_at: *mut sqlite3_stmt,
}

impl SqliteNodeStore {
    fn prepare(db: *mut sqlite3) -> treecrdt_core::Result<Self> {
        let ensure_node_sql = CString::new(
            "INSERT OR IGNORE INTO tree_nodes(node,parent,pos,tombstone) VALUES (?1,NULL,NULL,0)",
        )
        .expect("ensure node sql");
        let exists_sql = CString::new("SELECT 1 FROM tree_nodes WHERE node = ?1 LIMIT 1")
            .expect("exists sql");
        let select_node_sql = CString::new(
            "SELECT parent,pos,last_change,deleted_at FROM tree_nodes WHERE node = ?1 LIMIT 1",
        )
        .expect("select node sql");
        let select_children_sql =
            CString::new("SELECT node FROM tree_nodes WHERE parent = ?1 ORDER BY pos")
                .expect("select children sql");
        let all_nodes_sql =
            CString::new("SELECT node FROM tree_nodes").expect("select all nodes sql");
        let shift_down_sql = CString::new(
            "UPDATE tree_nodes SET pos = pos - 1 WHERE parent = ?1 AND pos > ?2",
        )
        .expect("shift down sql");
        let shift_up_sql = CString::new(
            "UPDATE tree_nodes SET pos = pos + 1 WHERE parent = ?1 AND pos >= ?2",
        )
        .expect("shift up sql");
        let max_pos_sql = CString::new(
            "SELECT COALESCE(MAX(pos) + 1, 0) FROM tree_nodes WHERE parent = ?1",
        )
        .expect("max pos sql");
        let clear_parent_pos_sql =
            CString::new("UPDATE tree_nodes SET parent = NULL, pos = NULL WHERE node = ?1")
                .expect("clear parent pos sql");
        let set_parent_pos_sql =
            CString::new("UPDATE tree_nodes SET parent = ?2, pos = ?3 WHERE node = ?1")
                .expect("set parent pos sql");
        let update_last_change_sql =
            CString::new("UPDATE tree_nodes SET last_change = ?2 WHERE node = ?1")
                .expect("update last_change sql");
        let update_deleted_at_sql =
            CString::new("UPDATE tree_nodes SET deleted_at = ?2 WHERE node = ?1")
                .expect("update deleted_at sql");

        let mut ensure_node: *mut sqlite3_stmt = null_mut();
        let mut exists: *mut sqlite3_stmt = null_mut();
        let mut select_node: *mut sqlite3_stmt = null_mut();
        let mut select_children: *mut sqlite3_stmt = null_mut();
        let mut all_nodes: *mut sqlite3_stmt = null_mut();
        let mut shift_down: *mut sqlite3_stmt = null_mut();
        let mut shift_up: *mut sqlite3_stmt = null_mut();
        let mut max_pos: *mut sqlite3_stmt = null_mut();
        let mut clear_parent_pos: *mut sqlite3_stmt = null_mut();
        let mut set_parent_pos: *mut sqlite3_stmt = null_mut();
        let mut update_last_change: *mut sqlite3_stmt = null_mut();
        let mut update_deleted_at: *mut sqlite3_stmt = null_mut();

        let prep = |sql: &CString, stmt: &mut *mut sqlite3_stmt| -> treecrdt_core::Result<()> {
            let rc = sqlite_prepare_v2(db, sql.as_ptr(), -1, stmt, null_mut());
            if rc != SQLITE_OK as c_int {
                return Err(sqlite_rc_error(rc, "sqlite_prepare_v2 failed"));
            }
            Ok(())
        };

        prep(&ensure_node_sql, &mut ensure_node)?;
        prep(&exists_sql, &mut exists)?;
        prep(&select_node_sql, &mut select_node)?;
        prep(&select_children_sql, &mut select_children)?;
        prep(&all_nodes_sql, &mut all_nodes)?;
        prep(&shift_down_sql, &mut shift_down)?;
        prep(&shift_up_sql, &mut shift_up)?;
        prep(&max_pos_sql, &mut max_pos)?;
        prep(&clear_parent_pos_sql, &mut clear_parent_pos)?;
        prep(&set_parent_pos_sql, &mut set_parent_pos)?;
        prep(&update_last_change_sql, &mut update_last_change)?;
        prep(&update_deleted_at_sql, &mut update_deleted_at)?;

        Ok(Self {
            db,
            ensure_node,
            exists,
            select_node,
            select_children,
            all_nodes,
            shift_down,
            shift_up,
            max_pos,
            clear_parent_pos,
            set_parent_pos,
            update_last_change,
            update_deleted_at,
        })
    }
}

impl Drop for SqliteNodeStore {
    fn drop(&mut self) {
        unsafe {
            sqlite_finalize(self.ensure_node);
            sqlite_finalize(self.exists);
            sqlite_finalize(self.select_node);
            sqlite_finalize(self.select_children);
            sqlite_finalize(self.all_nodes);
            sqlite_finalize(self.shift_down);
            sqlite_finalize(self.shift_up);
            sqlite_finalize(self.max_pos);
            sqlite_finalize(self.clear_parent_pos);
            sqlite_finalize(self.set_parent_pos);
            sqlite_finalize(self.update_last_change);
            sqlite_finalize(self.update_deleted_at);
        }
    }
}

impl treecrdt_core::NodeStore for SqliteNodeStore {
    fn reset(&mut self) -> treecrdt_core::Result<()> {
        let clear_sql = CString::new("DELETE FROM tree_nodes").expect("clear nodes sql");
        let rc = sqlite_exec(self.db, clear_sql.as_ptr(), None, null_mut(), null_mut());
        if rc != SQLITE_OK as c_int {
            return Err(sqlite_rc_error(rc, "sqlite_exec reset tree_nodes failed"));
        }

        let root = sqlite_node_id_bytes(NodeId::ROOT);
        self.ensure_node(NodeId::ROOT)?;
        unsafe {
            sqlite_clear_bindings(self.set_parent_pos);
            sqlite_reset(self.set_parent_pos);
            sqlite_bind_blob(
                self.set_parent_pos,
                1,
                root.as_ptr() as *const c_void,
                root.len() as c_int,
                None,
            );
            sqlite_bind_null(self.set_parent_pos, 2);
            sqlite_bind_int64(self.set_parent_pos, 3, 0);
            let step_rc = sqlite_step(self.set_parent_pos);
            sqlite_reset(self.set_parent_pos);
            if step_rc != SQLITE_DONE as c_int {
                return Err(sqlite_rc_error(step_rc, "reset root row failed"));
            }
        }
        Ok(())
    }

    fn ensure_node(&mut self, node: NodeId) -> treecrdt_core::Result<()> {
        let bytes = sqlite_node_id_bytes(node);
        unsafe {
            sqlite_clear_bindings(self.ensure_node);
            sqlite_reset(self.ensure_node);
            let bind_rc = sqlite_bind_blob(
                self.ensure_node,
                1,
                bytes.as_ptr() as *const c_void,
                bytes.len() as c_int,
                None,
            );
            if bind_rc != SQLITE_OK as c_int {
                return Err(sqlite_rc_error(bind_rc, "bind ensure_node failed"));
            }
            let step_rc = sqlite_step(self.ensure_node);
            sqlite_reset(self.ensure_node);
            if step_rc != SQLITE_DONE as c_int {
                return Err(sqlite_rc_error(step_rc, "ensure_node step failed"));
            }
        }
        Ok(())
    }

    fn exists(&self, node: NodeId) -> treecrdt_core::Result<bool> {
        let bytes = sqlite_node_id_bytes(node);
        unsafe {
            sqlite_clear_bindings(self.exists);
            sqlite_reset(self.exists);
            let bind_rc = sqlite_bind_blob(
                self.exists,
                1,
                bytes.as_ptr() as *const c_void,
                bytes.len() as c_int,
                None,
            );
            if bind_rc != SQLITE_OK as c_int {
                return Err(sqlite_rc_error(bind_rc, "bind exists failed"));
            }
            let step_rc = sqlite_step(self.exists);
            sqlite_reset(self.exists);
            match step_rc {
                rc if rc == SQLITE_ROW as c_int => Ok(true),
                rc if rc == SQLITE_DONE as c_int => Ok(false),
                rc => Err(sqlite_rc_error(rc, "exists step failed")),
            }
        }
    }

    fn parent(&self, node: NodeId) -> treecrdt_core::Result<Option<NodeId>> {
        let bytes = sqlite_node_id_bytes(node);
        unsafe {
            sqlite_clear_bindings(self.select_node);
            sqlite_reset(self.select_node);
            let bind_rc = sqlite_bind_blob(
                self.select_node,
                1,
                bytes.as_ptr() as *const c_void,
                bytes.len() as c_int,
                None,
            );
            if bind_rc != SQLITE_OK as c_int {
                return Err(sqlite_rc_error(bind_rc, "bind select_node failed"));
            }
            let step_rc = sqlite_step(self.select_node);
            let parent = if step_rc == SQLITE_ROW as c_int {
                match column_blob16(self.select_node, 0) {
                    Ok(Some(p)) => Some(sqlite_bytes_to_node_id(p)),
                    Ok(None) => None,
                    Err(rc) => return Err(sqlite_rc_error(rc, "read parent failed")),
                }
            } else if step_rc == SQLITE_DONE as c_int {
                None
            } else {
                return Err(sqlite_rc_error(step_rc, "select_node step failed"));
            };
            sqlite_reset(self.select_node);
            Ok(parent)
        }
    }

    fn children(&self, parent: NodeId) -> treecrdt_core::Result<Vec<NodeId>> {
        if parent == NodeId::TRASH {
            return Ok(Vec::new());
        }
        let parent_bytes = sqlite_node_id_bytes(parent);
        let mut out = Vec::new();
        unsafe {
            sqlite_clear_bindings(self.select_children);
            sqlite_reset(self.select_children);
            let bind_rc = sqlite_bind_blob(
                self.select_children,
                1,
                parent_bytes.as_ptr() as *const c_void,
                parent_bytes.len() as c_int,
                None,
            );
            if bind_rc != SQLITE_OK as c_int {
                return Err(sqlite_rc_error(bind_rc, "bind select_children failed"));
            }
            loop {
                let step_rc = sqlite_step(self.select_children);
                if step_rc == SQLITE_ROW as c_int {
                    match column_blob16(self.select_children, 0) {
                        Ok(Some(id)) => out.push(sqlite_bytes_to_node_id(id)),
                        Ok(None) => {}
                        Err(rc) => return Err(sqlite_rc_error(rc, "read child id failed")),
                    }
                } else if step_rc == SQLITE_DONE as c_int {
                    break;
                } else {
                    return Err(sqlite_rc_error(step_rc, "select_children step failed"));
                }
            }
            sqlite_reset(self.select_children);
        }
        Ok(out)
    }

    fn detach(&mut self, node: NodeId) -> treecrdt_core::Result<()> {
        if node == NodeId::ROOT {
            return Ok(());
        }
        self.ensure_node(node)?;
        let bytes = sqlite_node_id_bytes(node);

        let (parent, pos) = unsafe {
            sqlite_clear_bindings(self.select_node);
            sqlite_reset(self.select_node);
            let bind_rc = sqlite_bind_blob(
                self.select_node,
                1,
                bytes.as_ptr() as *const c_void,
                bytes.len() as c_int,
                None,
            );
            if bind_rc != SQLITE_OK as c_int {
                return Err(sqlite_rc_error(bind_rc, "bind select_node failed"));
            }
            let step_rc = sqlite_step(self.select_node);
            if step_rc != SQLITE_ROW as c_int {
                sqlite_reset(self.select_node);
                return Ok(());
            }
            let parent = column_blob16(self.select_node, 0).map_err(|rc| sqlite_rc_error(rc, "read parent failed"))?;
            let pos = column_int_opt(self.select_node, 1).map(|v| v as usize);
            sqlite_reset(self.select_node);
            (parent, pos)
        };

        if let (Some(parent_bytes), Some(pos)) = (parent, pos) {
            let parent_id = sqlite_bytes_to_node_id(parent_bytes);
            if parent_id != NodeId::TRASH {
                unsafe {
                    sqlite_clear_bindings(self.shift_down);
                    sqlite_reset(self.shift_down);
                    let bind_parent = sqlite_bind_blob(
                        self.shift_down,
                        1,
                        parent_bytes.as_ptr() as *const c_void,
                        parent_bytes.len() as c_int,
                        None,
                    );
                    if bind_parent != SQLITE_OK as c_int {
                        return Err(sqlite_rc_error(bind_parent, "bind shift_down parent failed"));
                    }
                    let bind_pos = sqlite_bind_int64(self.shift_down, 2, pos as i64);
                    if bind_pos != SQLITE_OK as c_int {
                        return Err(sqlite_rc_error(bind_pos, "bind shift_down pos failed"));
                    }
                    let step_rc = sqlite_step(self.shift_down);
                    sqlite_reset(self.shift_down);
                    if step_rc != SQLITE_DONE as c_int {
                        return Err(sqlite_rc_error(step_rc, "shift_down step failed"));
                    }
                }
            }
        }

        unsafe {
            sqlite_clear_bindings(self.clear_parent_pos);
            sqlite_reset(self.clear_parent_pos);
            let bind_rc = sqlite_bind_blob(
                self.clear_parent_pos,
                1,
                bytes.as_ptr() as *const c_void,
                bytes.len() as c_int,
                None,
            );
            if bind_rc != SQLITE_OK as c_int {
                return Err(sqlite_rc_error(bind_rc, "bind clear_parent_pos failed"));
            }
            let step_rc = sqlite_step(self.clear_parent_pos);
            sqlite_reset(self.clear_parent_pos);
            if step_rc != SQLITE_DONE as c_int {
                return Err(sqlite_rc_error(step_rc, "clear_parent_pos step failed"));
            }
        }

        Ok(())
    }

    fn attach(&mut self, node: NodeId, parent: NodeId, position: usize) -> treecrdt_core::Result<()> {
        if node == NodeId::ROOT {
            return Ok(());
        }
        self.ensure_node(node)?;
        self.ensure_node(parent)?;

        let node_bytes = sqlite_node_id_bytes(node);
        let parent_bytes = sqlite_node_id_bytes(parent);

        if parent == NodeId::TRASH {
            unsafe {
                sqlite_clear_bindings(self.set_parent_pos);
                sqlite_reset(self.set_parent_pos);
                sqlite_bind_blob(
                    self.set_parent_pos,
                    1,
                    node_bytes.as_ptr() as *const c_void,
                    node_bytes.len() as c_int,
                    None,
                );
                sqlite_bind_blob(
                    self.set_parent_pos,
                    2,
                    parent_bytes.as_ptr() as *const c_void,
                    parent_bytes.len() as c_int,
                    None,
                );
                sqlite_bind_null(self.set_parent_pos, 3);
                let step_rc = sqlite_step(self.set_parent_pos);
                sqlite_reset(self.set_parent_pos);
                if step_rc != SQLITE_DONE as c_int {
                    return Err(sqlite_rc_error(step_rc, "attach to trash failed"));
                }
            }
            return Ok(());
        }

        let max_pos = unsafe {
            sqlite_clear_bindings(self.max_pos);
            sqlite_reset(self.max_pos);
            let bind_rc = sqlite_bind_blob(
                self.max_pos,
                1,
                parent_bytes.as_ptr() as *const c_void,
                parent_bytes.len() as c_int,
                None,
            );
            if bind_rc != SQLITE_OK as c_int {
                return Err(sqlite_rc_error(bind_rc, "bind max_pos failed"));
            }
            let step_rc = sqlite_step(self.max_pos);
            if step_rc != SQLITE_ROW as c_int {
                sqlite_reset(self.max_pos);
                return Err(sqlite_rc_error(step_rc, "max_pos step failed"));
            }
            let val = sqlite_column_int64(self.max_pos, 0) as usize;
            sqlite_reset(self.max_pos);
            val
        };

        let pos = position.min(max_pos);

        unsafe {
            sqlite_clear_bindings(self.shift_up);
            sqlite_reset(self.shift_up);
            let bind_parent = sqlite_bind_blob(
                self.shift_up,
                1,
                parent_bytes.as_ptr() as *const c_void,
                parent_bytes.len() as c_int,
                None,
            );
            if bind_parent != SQLITE_OK as c_int {
                return Err(sqlite_rc_error(bind_parent, "bind shift_up parent failed"));
            }
            let bind_pos = sqlite_bind_int64(self.shift_up, 2, pos as i64);
            if bind_pos != SQLITE_OK as c_int {
                return Err(sqlite_rc_error(bind_pos, "bind shift_up pos failed"));
            }
            let step_rc = sqlite_step(self.shift_up);
            sqlite_reset(self.shift_up);
            if step_rc != SQLITE_DONE as c_int {
                return Err(sqlite_rc_error(step_rc, "shift_up step failed"));
            }
        }

        unsafe {
            sqlite_clear_bindings(self.set_parent_pos);
            sqlite_reset(self.set_parent_pos);
            let bind_node = sqlite_bind_blob(
                self.set_parent_pos,
                1,
                node_bytes.as_ptr() as *const c_void,
                node_bytes.len() as c_int,
                None,
            );
            if bind_node != SQLITE_OK as c_int {
                return Err(sqlite_rc_error(bind_node, "bind set_parent_pos node failed"));
            }
            let bind_parent = sqlite_bind_blob(
                self.set_parent_pos,
                2,
                parent_bytes.as_ptr() as *const c_void,
                parent_bytes.len() as c_int,
                None,
            );
            if bind_parent != SQLITE_OK as c_int {
                return Err(sqlite_rc_error(bind_parent, "bind set_parent_pos parent failed"));
            }
            let bind_pos = sqlite_bind_int64(self.set_parent_pos, 3, pos as i64);
            if bind_pos != SQLITE_OK as c_int {
                return Err(sqlite_rc_error(bind_pos, "bind set_parent_pos pos failed"));
            }
            let step_rc = sqlite_step(self.set_parent_pos);
            sqlite_reset(self.set_parent_pos);
            if step_rc != SQLITE_DONE as c_int {
                return Err(sqlite_rc_error(step_rc, "set_parent_pos step failed"));
            }
        }

        Ok(())
    }

    fn last_change(&self, node: NodeId) -> treecrdt_core::Result<VersionVector> {
        let bytes = sqlite_node_id_bytes(node);
        unsafe {
            sqlite_clear_bindings(self.select_node);
            sqlite_reset(self.select_node);
            sqlite_bind_blob(
                self.select_node,
                1,
                bytes.as_ptr() as *const c_void,
                bytes.len() as c_int,
                None,
            );
            let step_rc = sqlite_step(self.select_node);
            if step_rc != SQLITE_ROW as c_int {
                sqlite_reset(self.select_node);
                return Err(sqlite_rc_error(step_rc, "select_node last_change failed"));
            }
            let vv = if sqlite_column_type(self.select_node, 2) == SQLITE_NULL as c_int {
                VersionVector::new()
            } else {
                let ptr = sqlite_column_blob(self.select_node, 2) as *const u8;
                let len = sqlite_column_bytes(self.select_node, 2) as usize;
                if ptr.is_null() || len == 0 {
                    VersionVector::new()
                } else {
                    vv_from_bytes(slice::from_raw_parts(ptr, len))?
                }
            };
            sqlite_reset(self.select_node);
            Ok(vv)
        }
    }

    fn merge_last_change(&mut self, node: NodeId, delta: &VersionVector) -> treecrdt_core::Result<()> {
        self.ensure_node(node)?;
        let mut vv = self.last_change(node)?;
        vv.merge(delta);
        let bytes = vv_to_bytes(&vv)?;

        let node_bytes = sqlite_node_id_bytes(node);
        unsafe {
            sqlite_clear_bindings(self.update_last_change);
            sqlite_reset(self.update_last_change);
            sqlite_bind_blob(
                self.update_last_change,
                1,
                node_bytes.as_ptr() as *const c_void,
                node_bytes.len() as c_int,
                None,
            );
            sqlite_bind_blob(
                self.update_last_change,
                2,
                bytes.as_ptr() as *const c_void,
                bytes.len() as c_int,
                None,
            );
            let step_rc = sqlite_step(self.update_last_change);
            sqlite_reset(self.update_last_change);
            if step_rc != SQLITE_DONE as c_int {
                return Err(sqlite_rc_error(step_rc, "update_last_change failed"));
            }
        }

        Ok(())
    }

    fn deleted_at(&self, node: NodeId) -> treecrdt_core::Result<Option<VersionVector>> {
        let bytes = sqlite_node_id_bytes(node);
        unsafe {
            sqlite_clear_bindings(self.select_node);
            sqlite_reset(self.select_node);
            sqlite_bind_blob(
                self.select_node,
                1,
                bytes.as_ptr() as *const c_void,
                bytes.len() as c_int,
                None,
            );
            let step_rc = sqlite_step(self.select_node);
            if step_rc != SQLITE_ROW as c_int {
                sqlite_reset(self.select_node);
                return Ok(None);
            }
            let vv = if sqlite_column_type(self.select_node, 3) == SQLITE_NULL as c_int {
                None
            } else {
                let ptr = sqlite_column_blob(self.select_node, 3) as *const u8;
                let len = sqlite_column_bytes(self.select_node, 3) as usize;
                if ptr.is_null() || len == 0 {
                    None
                } else {
                    Some(vv_from_bytes(slice::from_raw_parts(ptr, len))?)
                }
            };
            sqlite_reset(self.select_node);
            Ok(vv)
        }
    }

    fn merge_deleted_at(&mut self, node: NodeId, delta: &VersionVector) -> treecrdt_core::Result<()> {
        self.ensure_node(node)?;
        let mut vv = self.deleted_at(node)?.unwrap_or_else(VersionVector::new);
        vv.merge(delta);
        let bytes = vv_to_bytes(&vv)?;

        let node_bytes = sqlite_node_id_bytes(node);
        unsafe {
            sqlite_clear_bindings(self.update_deleted_at);
            sqlite_reset(self.update_deleted_at);
            sqlite_bind_blob(
                self.update_deleted_at,
                1,
                node_bytes.as_ptr() as *const c_void,
                node_bytes.len() as c_int,
                None,
            );
            sqlite_bind_blob(
                self.update_deleted_at,
                2,
                bytes.as_ptr() as *const c_void,
                bytes.len() as c_int,
                None,
            );
            let step_rc = sqlite_step(self.update_deleted_at);
            sqlite_reset(self.update_deleted_at);
            if step_rc != SQLITE_DONE as c_int {
                return Err(sqlite_rc_error(step_rc, "update_deleted_at failed"));
            }
        }

        Ok(())
    }

    fn all_nodes(&self) -> treecrdt_core::Result<Vec<NodeId>> {
        let mut out = Vec::new();
        unsafe {
            sqlite_clear_bindings(self.all_nodes);
            sqlite_reset(self.all_nodes);
            loop {
                let step_rc = sqlite_step(self.all_nodes);
                if step_rc == SQLITE_ROW as c_int {
                    match column_blob16(self.all_nodes, 0) {
                        Ok(Some(id)) => out.push(sqlite_bytes_to_node_id(id)),
                        Ok(None) => {}
                        Err(rc) => return Err(sqlite_rc_error(rc, "read node id failed")),
                    }
                } else if step_rc == SQLITE_DONE as c_int {
                    break;
                } else {
                    return Err(sqlite_rc_error(step_rc, "all_nodes step failed"));
                }
            }
            sqlite_reset(self.all_nodes);
        }
        Ok(out)
    }
}

#[derive(Default)]
struct NoopStorage;

impl treecrdt_core::Storage for NoopStorage {
    fn apply(&mut self, _op: treecrdt_core::Operation) -> treecrdt_core::Result<()> {
        Ok(())
    }

    fn load_since(&self, _lamport: Lamport) -> treecrdt_core::Result<Vec<treecrdt_core::Operation>> {
        Ok(Vec::new())
    }

    fn latest_lamport(&self) -> Lamport {
        0
    }

    fn snapshot(&self) -> treecrdt_core::Result<treecrdt_core::Snapshot> {
        Ok(treecrdt_core::Snapshot { head: 0 })
    }
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MaterializeKind {
    Insert,
    Move,
    Delete,
    Tombstone,
}

impl MaterializeKind {
    fn parse(s: &str) -> Option<Self> {
        match s {
            "insert" => Some(Self::Insert),
            "move" => Some(Self::Move),
            "delete" => Some(Self::Delete),
            "tombstone" => Some(Self::Tombstone),
            _ => None,
        }
    }
}

#[derive(Clone, Debug)]
struct MaterializeOp {
    replica: Vec<u8>,
    counter: u64,
    lamport: Lamport,
    kind: MaterializeKind,
    parent: Option<NodeId>,
    node: NodeId,
    new_parent: Option<NodeId>,
    position: usize,
    known_state: Option<VersionVector>,
    op_ref: [u8; OPREF_V0_WIDTH],
}

fn materialize_ops_in_order(
    db: *mut sqlite3,
    meta: &TreeMeta,
    ops: &mut [MaterializeOp],
) -> Result<(), c_int> {
    struct AutoStmt(*mut sqlite3_stmt);
    impl Drop for AutoStmt {
        fn drop(&mut self) {
            if !self.0.is_null() {
                unsafe { sqlite_finalize(self.0) };
            }
        }
    }

    if ops.is_empty() {
        return Ok(());
    }
    if meta.dirty {
        return Err(SQLITE_ERROR as c_int);
    }

    ops.sort_by(|a, b| {
        (a.lamport, &a.replica, a.counter).cmp(&(b.lamport, &b.replica, b.counter))
    });
    if let Some(first) = ops.first() {
        if op_key_lt(
            first.lamport,
            &first.replica,
            first.counter,
            meta.head_lamport,
            &meta.head_replica,
            meta.head_counter,
        ) {
            return Err(SQLITE_ERROR as c_int);
        }
    }

    use treecrdt_core::{
        LamportClock, Operation, OperationId, OperationKind, OperationMetadata, ReplicaId, TreeCrdt,
    };
    let node_store = SqliteNodeStore::prepare(db).map_err(|_| SQLITE_ERROR as c_int)?;
    let mut crdt = TreeCrdt::with_node_store(
        ReplicaId::new(b"sqlite-ext"),
        NoopStorage::default(),
        LamportClock::default(),
        node_store,
    );

    let parent_sql = CString::new("SELECT parent, deleted_at FROM tree_nodes WHERE node = ?1 LIMIT 1")
        .expect("parent lookup sql");
    let mut parent_stmt: *mut sqlite3_stmt = null_mut();
    let parent_rc = sqlite_prepare_v2(db, parent_sql.as_ptr(), -1, &mut parent_stmt, null_mut());
    if parent_rc != SQLITE_OK as c_int {
        return Err(parent_rc);
    }
    let parent_stmt = AutoStmt(parent_stmt);

    let insert_opref_sql = CString::new(
        "INSERT OR IGNORE INTO oprefs_children(parent, op_ref, seq) VALUES (?1, ?2, ?3)",
    )
    .expect("insert oprefs_children sql");
    let mut opref_stmt: *mut sqlite3_stmt = null_mut();
    let opref_rc =
        sqlite_prepare_v2(db, insert_opref_sql.as_ptr(), -1, &mut opref_stmt, null_mut());
    if opref_rc != SQLITE_OK as c_int {
        return Err(opref_rc);
    }
    let opref_stmt = AutoStmt(opref_stmt);

    let update_tombstone_sql =
        CString::new("UPDATE tree_nodes SET tombstone = ?2 WHERE node = ?1")
            .expect("update tombstone sql");
    let mut tombstone_stmt: *mut sqlite3_stmt = null_mut();
    let tombstone_rc = sqlite_prepare_v2(
        db,
        update_tombstone_sql.as_ptr(),
        -1,
        &mut tombstone_stmt,
        null_mut(),
    );
    if tombstone_rc != SQLITE_OK as c_int {
        return Err(tombstone_rc);
    }
    let tombstone_stmt = AutoStmt(tombstone_stmt);

    let mut seq = meta.head_seq;
    let mut tombstone_starts: HashSet<NodeId> = HashSet::new();

    for op in ops.iter() {
        seq += 1;

        let node_bytes = op.node.0.to_be_bytes();
        let old_parent: Option<NodeId> = unsafe {
            sqlite_clear_bindings(parent_stmt.0);
            sqlite_reset(parent_stmt.0);
            let bind_rc = sqlite_bind_blob(
                parent_stmt.0,
                1,
                node_bytes.as_ptr() as *const c_void,
                node_bytes.len() as c_int,
                None,
            );
            if bind_rc != SQLITE_OK as c_int {
                sqlite_reset(parent_stmt.0);
                Err(bind_rc)
            } else {
                let step_rc = sqlite_step(parent_stmt.0);
                let out = if step_rc == SQLITE_ROW as c_int {
                    match column_blob16(parent_stmt.0, 0)? {
                        Some(bytes) => Some(NodeId(u128::from_be_bytes(bytes))),
                        None => None,
                    }
                } else if step_rc == SQLITE_DONE as c_int {
                    None
                } else {
                    sqlite_reset(parent_stmt.0);
                    return Err(step_rc);
                };
                sqlite_reset(parent_stmt.0);
                Ok(out)
            }
        }?;

        let next_parent: Option<NodeId> = match op.kind {
            MaterializeKind::Insert => op.parent,
            MaterializeKind::Move => op.new_parent,
            MaterializeKind::Delete | MaterializeKind::Tombstone => old_parent,
        };

        let op_kind = match op.kind {
            MaterializeKind::Insert => {
                let parent = op.parent.ok_or(SQLITE_ERROR as c_int)?;
                OperationKind::Insert {
                    parent,
                    node: op.node,
                    position: op.position,
                }
            }
            MaterializeKind::Move => {
                let new_parent = op.new_parent.ok_or(SQLITE_ERROR as c_int)?;
                OperationKind::Move {
                    node: op.node,
                    new_parent,
                    position: op.position,
                }
            }
            MaterializeKind::Delete => OperationKind::Delete { node: op.node },
            MaterializeKind::Tombstone => OperationKind::Tombstone { node: op.node },
        };

        let operation = Operation {
            meta: OperationMetadata {
                id: OperationId {
                    replica: ReplicaId(op.replica.clone()),
                    counter: op.counter,
                },
                lamport: op.lamport,
                known_state: op.known_state.clone(),
            },
            kind: op_kind,
        };

        if crdt.apply_remote(operation).is_err() {
            return Err(SQLITE_ERROR as c_int);
        }

        let insert_parent = |parent: NodeId| -> Result<(), c_int> {
            if parent == NodeId::TRASH {
                return Ok(());
            }
            let parent_bytes = parent.0.to_be_bytes();
            unsafe {
                sqlite_clear_bindings(opref_stmt.0);
                sqlite_reset(opref_stmt.0);
            }
            let mut bind_err = false;
            unsafe {
                bind_err |= sqlite_bind_blob(
                    opref_stmt.0,
                    1,
                    parent_bytes.as_ptr() as *const c_void,
                    parent_bytes.len() as c_int,
                    None,
                ) != SQLITE_OK as c_int;
                bind_err |= sqlite_bind_blob(
                    opref_stmt.0,
                    2,
                    op.op_ref.as_ptr() as *const c_void,
                    op.op_ref.len() as c_int,
                    None,
                ) != SQLITE_OK as c_int;
                bind_err |= sqlite_bind_int64(opref_stmt.0, 3, seq as i64) != SQLITE_OK as c_int;
            }
            if bind_err {
                return Err(SQLITE_ERROR as c_int);
            }
            let step_rc = unsafe { sqlite_step(opref_stmt.0) };
            if step_rc != SQLITE_DONE as c_int {
                return Err(step_rc);
            }
            Ok(())
        };

        if let Some(parent) = old_parent {
            insert_parent(parent)?;
        }
        if let Some(parent) = next_parent {
            if Some(parent) != old_parent {
                insert_parent(parent)?;
            }
        }

        tombstone_starts.insert(op.node);
        if let Some(parent) = old_parent {
            tombstone_starts.insert(parent);
        }
        if let Some(parent) = next_parent {
            tombstone_starts.insert(parent);
        }
    }

    // Refresh tombstones for affected nodes + ancestors (only for nodes with deleted_at).
    {
        let mut stack: Vec<NodeId> = tombstone_starts.into_iter().collect();
        let mut visited: HashSet<NodeId> = HashSet::new();
        while let Some(node) = stack.pop() {
            if node == NodeId::TRASH {
                continue;
            }
            if !visited.insert(node) {
                continue;
            }

            let node_bytes = node.0.to_be_bytes();
            let (parent, has_deleted_at): (Option<NodeId>, bool) = unsafe {
                sqlite_clear_bindings(parent_stmt.0);
                sqlite_reset(parent_stmt.0);
                let bind_rc = sqlite_bind_blob(
                    parent_stmt.0,
                    1,
                    node_bytes.as_ptr() as *const c_void,
                    node_bytes.len() as c_int,
                    None,
                );
                if bind_rc != SQLITE_OK as c_int {
                    sqlite_reset(parent_stmt.0);
                    Err(bind_rc)
                } else {
                    let step_rc = sqlite_step(parent_stmt.0);
                    let out = if step_rc == SQLITE_ROW as c_int {
                        let parent = match column_blob16(parent_stmt.0, 0)? {
                            Some(bytes) => Some(NodeId(u128::from_be_bytes(bytes))),
                            None => None,
                        };
                        let has_deleted_at =
                            sqlite_column_type(parent_stmt.0, 1) != SQLITE_NULL as c_int
                                && sqlite_column_bytes(parent_stmt.0, 1) > 0;
                        (parent, has_deleted_at)
                    } else if step_rc == SQLITE_DONE as c_int {
                        (None, false)
                    } else {
                        sqlite_reset(parent_stmt.0);
                        return Err(step_rc);
                    };
                    sqlite_reset(parent_stmt.0);
                    Ok(out)
                }
            }?;

            if has_deleted_at {
                let tombstoned = crdt.is_tombstoned(node).map_err(|_| SQLITE_ERROR as c_int)?;
                unsafe {
                    sqlite_clear_bindings(tombstone_stmt.0);
                    sqlite_reset(tombstone_stmt.0);
                }
                let mut bind_err = false;
                unsafe {
                    bind_err |= sqlite_bind_blob(
                        tombstone_stmt.0,
                        1,
                        node_bytes.as_ptr() as *const c_void,
                        node_bytes.len() as c_int,
                        None,
                    ) != SQLITE_OK as c_int;
                    bind_err |= sqlite_bind_int64(
                        tombstone_stmt.0,
                        2,
                        if tombstoned { 1 } else { 0 },
                    ) != SQLITE_OK as c_int;
                }
                if bind_err {
                    return Err(SQLITE_ERROR as c_int);
                }
                let step_rc = unsafe { sqlite_step(tombstone_stmt.0) };
                if step_rc != SQLITE_DONE as c_int {
                    return Err(step_rc);
                }
            }

            if let Some(parent) = parent {
                stack.push(parent);
            }
        }
    }

    let last = ops.last().expect("ops non-empty");
    update_tree_meta_head(db, last.lamport, &last.replica, last.counter, seq)?;
    Ok(())
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
        "DELETE FROM oprefs_children; \
         UPDATE tree_meta SET dirty = 0, head_lamport = 0, head_replica = X'', head_counter = 0, head_seq = 0 WHERE id = 1;",
    )
    .expect("clear materialized sql");
    let clear_rc = sqlite_exec(db, clear_sql.as_ptr(), None, null_mut(), null_mut());
    if clear_rc != SQLITE_OK as c_int {
        sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
        return Err(clear_rc);
    }

    let doc_id = load_doc_id(db).unwrap_or(None).unwrap_or_default();

    // Rebuild materialized tree state using core TreeCrdt semantics + SQLite-backed NodeStore.
    use treecrdt_core::{
        LamportClock, Operation, OperationId, OperationKind, OperationMetadata, ReplicaId, TreeCrdt,
    };
    let mut node_store = match SqliteNodeStore::prepare(db) {
        Ok(store) => store,
        Err(_) => {
            sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
            return Err(SQLITE_ERROR as c_int);
        }
    };
    if node_store.reset().is_err() {
        sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
        return Err(SQLITE_ERROR as c_int);
    }
    let mut crdt = TreeCrdt::with_node_store(
        ReplicaId::new(b"sqlite-ext"),
        NoopStorage::default(),
        LamportClock::default(),
        node_store,
    );

    let scan_sql = CString::new(
        "SELECT replica,counter,lamport,kind,parent,node,new_parent,position,known_state \
         FROM ops ORDER BY lamport, replica, counter",
    )
    .expect("scan ops sql");
    let mut scan_stmt: *mut sqlite3_stmt = null_mut();
    let prep_rc = sqlite_prepare_v2(db, scan_sql.as_ptr(), -1, &mut scan_stmt, null_mut());
    if prep_rc != SQLITE_OK as c_int {
        sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
        return Err(prep_rc);
    }

    let mut scan_err: Option<c_int> = None;
    loop {
        let step_rc = unsafe { sqlite_step(scan_stmt) };
        if step_rc == SQLITE_ROW as c_int {
            unsafe {
                let replica_ptr = sqlite_column_blob(scan_stmt, 0) as *const u8;
                let replica_len = sqlite_column_bytes(scan_stmt, 0) as usize;
                if replica_ptr.is_null() {
                    continue;
                }
                let replica_bytes = slice::from_raw_parts(replica_ptr, replica_len).to_vec();
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
                let position = column_int_opt(scan_stmt, 7);

                // Read known_state (column 8) - may be NULL for older operations
                let known_state_from_db =
                    if sqlite_column_type(scan_stmt, 8) == SQLITE_NULL as c_int {
                        None
                    } else {
                        let ks_ptr = sqlite_column_blob(scan_stmt, 8) as *const u8;
                        let ks_len = sqlite_column_bytes(scan_stmt, 8) as usize;
                        if !ks_ptr.is_null() && ks_len > 0 {
                            let ks_bytes = slice::from_raw_parts(ks_ptr, ks_len);
                            deserialize_version_vector(ks_bytes).ok()
                        } else {
                            None
                        }
                    };

                let to_node_id = |bytes: [u8; 16]| NodeId(u128::from_be_bytes(bytes));
                let node_id = to_node_id(node);

                let kind_parsed = if kind == "insert" {
                    let Some(p) = parent else { continue };
                    OperationKind::Insert {
                        parent: to_node_id(p),
                        node: node_id,
                        position: position.unwrap_or(0) as usize,
                    }
                } else if kind == "move" {
                    let Some(p) = new_parent else { continue };
                    OperationKind::Move {
                        node: node_id,
                        new_parent: to_node_id(p),
                        position: position.unwrap_or(0) as usize,
                    }
                } else if kind == "delete" {
                    OperationKind::Delete { node: node_id }
                } else if kind == "tombstone" {
                    OperationKind::Tombstone { node: node_id }
                } else {
                    continue;
                };

                let op = Operation {
                    meta: OperationMetadata {
                        id: OperationId {
                            replica: ReplicaId(replica_bytes),
                            counter,
                        },
                        lamport,
                        known_state: known_state_from_db,
                    },
                    kind: kind_parsed,
                };

                if crdt.apply_remote(op).is_err() {
                    scan_err = Some(SQLITE_ERROR as c_int);
                    break;
                }
            }
        } else if step_rc == SQLITE_DONE as c_int {
            break;
        } else {
            scan_err = Some(step_rc);
            break;
        }
    }

    let finalize_rc = unsafe { sqlite_finalize(scan_stmt) };
    if scan_err.is_some() || finalize_rc != SQLITE_OK as c_int {
        sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
        return Err(scan_err.unwrap_or(finalize_rc));
    }

    let log = crdt.export_log();

    // Refresh tombstone materialization using core semantics.
    {
        let select_sql =
            CString::new("SELECT node FROM tree_nodes").expect("select tree node ids sql");
        let mut select_stmt: *mut sqlite3_stmt = null_mut();
        let rc = sqlite_prepare_v2(db, select_sql.as_ptr(), -1, &mut select_stmt, null_mut());
        if rc != SQLITE_OK as c_int {
            sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
            return Err(rc);
        }

        let mut node_ids: Vec<NodeId> = Vec::new();
        loop {
            let step_rc = unsafe { sqlite_step(select_stmt) };
            if step_rc == SQLITE_ROW as c_int {
                let bytes = match unsafe { column_blob16(select_stmt, 0) } {
                    Ok(Some(bytes)) => bytes,
                    Ok(None) => continue,
                    Err(rc) => {
                        unsafe { sqlite_finalize(select_stmt) };
                        sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
                        return Err(rc);
                    }
                };
                node_ids.push(NodeId(u128::from_be_bytes(bytes)));
            } else if step_rc == SQLITE_DONE as c_int {
                break;
            } else {
                unsafe { sqlite_finalize(select_stmt) };
                sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
                return Err(step_rc);
            }
        }
        let finalize_rc = unsafe { sqlite_finalize(select_stmt) };
        if finalize_rc != SQLITE_OK as c_int {
            sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
            return Err(finalize_rc);
        }

        let update_sql = CString::new("UPDATE tree_nodes SET tombstone = ?2 WHERE node = ?1")
            .expect("update tombstone sql");
        let mut update_stmt: *mut sqlite3_stmt = null_mut();
        let rc = sqlite_prepare_v2(db, update_sql.as_ptr(), -1, &mut update_stmt, null_mut());
        if rc != SQLITE_OK as c_int {
            sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
            return Err(rc);
        }

        for node in node_ids {
            if node == NodeId::TRASH {
                continue;
            }
            let tombstoned = match crdt.is_tombstoned(node) {
                Ok(v) => v,
                Err(_) => {
                    unsafe { sqlite_finalize(update_stmt) };
                    sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
                    return Err(SQLITE_ERROR as c_int);
                }
            };
            let node_bytes = node.0.to_be_bytes();
            unsafe {
                sqlite_clear_bindings(update_stmt);
                sqlite_reset(update_stmt);
            }
            let mut bind_err = false;
            unsafe {
                bind_err |= sqlite_bind_blob(
                    update_stmt,
                    1,
                    node_bytes.as_ptr() as *const c_void,
                    node_bytes.len() as c_int,
                    None,
                ) != SQLITE_OK as c_int;
                bind_err |= sqlite_bind_int64(update_stmt, 2, if tombstoned { 1 } else { 0 })
                    != SQLITE_OK as c_int;
            }
            if bind_err {
                unsafe { sqlite_finalize(update_stmt) };
                sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
                return Err(SQLITE_ERROR as c_int);
            }

            let step_rc = unsafe { sqlite_step(update_stmt) };
            if step_rc != SQLITE_DONE as c_int {
                unsafe { sqlite_finalize(update_stmt) };
                sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
                return Err(step_rc);
            }
        }

        unsafe { sqlite_finalize(update_stmt) };
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

        for (idx, entry) in log.iter().enumerate() {
            let seq = (idx as u64) + 1;
            let replica = entry.op.meta.id.replica.as_bytes();
            let counter = entry.op.meta.id.counter;
            let op_ref = derive_op_ref_v0(&doc_id, replica, counter);

            let old_parent = entry.snapshot.parent;
            let next_parent: Option<NodeId> = match &entry.op.kind {
                OperationKind::Insert { parent, .. } => Some(*parent),
                OperationKind::Move { new_parent, .. } => Some(*new_parent),
                OperationKind::Delete { .. } | OperationKind::Tombstone { .. } => old_parent,
            };

            let insert_parent = |parent: NodeId| -> Result<(), c_int> {
                if parent == NodeId::TRASH {
                    return Ok(());
                }
                unsafe {
                    sqlite_clear_bindings(stmt);
                    sqlite_reset(stmt);
                }
                let parent_bytes = parent.0.to_be_bytes();
                let mut bind_err = false;
                unsafe {
                    bind_err |= sqlite_bind_blob(
                        stmt,
                        1,
                        parent_bytes.as_ptr() as *const c_void,
                        parent_bytes.len() as c_int,
                        None,
                    ) != SQLITE_OK as c_int;
                    bind_err |= sqlite_bind_blob(
                        stmt,
                        2,
                        op_ref.as_ptr() as *const c_void,
                        op_ref.len() as c_int,
                        None,
                    ) != SQLITE_OK as c_int;
                    bind_err |= sqlite_bind_int64(stmt, 3, seq as i64) != SQLITE_OK as c_int;
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
                Ok(())
            };

            if let Some(parent) = old_parent {
                insert_parent(parent)?;
            }
            if let Some(parent) = next_parent {
                if Some(parent) != old_parent {
                    insert_parent(parent)?;
                }
            }
        }

        unsafe { sqlite_finalize(stmt) };
    }

    // Update meta head + seq.
    if let Some(last) = log.last() {
        let head_rc = update_tree_meta_head(
            db,
            last.op.meta.lamport,
            last.op.meta.id.replica.as_bytes(),
            last.op.meta.id.counter,
            log.len() as u64,
        );
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
/// replica BLOB, counter INT, lamport INT, kind TEXT, parent BLOB|null, node BLOB, new_parent BLOB|null, position INT|null, known_state BLOB|null
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

    if argc != 9 {
        sqlite_result_error(
            ctx,
            b"treecrdt_append_op expects 9 args\0".as_ptr() as *const c_char,
        );
        return;
    }

    let db = sqlite_context_db_handle(ctx);
    let sql = CString::new(
        "INSERT OR IGNORE INTO ops (replica,counter,lamport,kind,parent,node,new_parent,position,known_state,op_ref) \
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
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

    // Compute delete `known_state` (subtree version vector) when the caller doesn't provide it.
    // This is only correct for the writer creating the delete op.
    let kind_ptr = unsafe { sqlite_value_text(args[3]) } as *const u8;
    let kind_len = unsafe { sqlite_value_bytes(args[3]) } as usize;
    let kind = if kind_ptr.is_null() {
        ""
    } else {
        std::str::from_utf8(unsafe { slice::from_raw_parts(kind_ptr, kind_len) }).unwrap_or("")
    };

    let node_ptr = unsafe { sqlite_value_blob(args[5]) } as *const u8;
    let node_len = unsafe { sqlite_value_bytes(args[5]) } as usize;
    let node_bytes: Option<[u8; 16]> = if !node_ptr.is_null() && node_len == 16 {
        let mut out = [0u8; 16];
        out.copy_from_slice(unsafe { slice::from_raw_parts(node_ptr, node_len) });
        Some(out)
    } else {
        None
    };

    let known_state_blob: Option<Vec<u8>> = if kind == "delete" {
        let provided_known_state = unsafe {
            if sqlite_value_type(args[8]) == SQLITE_NULL as c_int {
                None
            } else {
                let ptr = sqlite_value_blob(args[8]) as *const u8;
                let len = sqlite_value_bytes(args[8]) as usize;
                if ptr.is_null() {
                    None
                } else {
                    Some(slice::from_raw_parts(ptr, len).to_vec())
                }
            }
        };
        if let Some(bytes) = provided_known_state {
            if bytes.is_empty() {
                unsafe { sqlite_finalize(stmt) };
                sqlite_result_error(
                    ctx,
                    b"treecrdt_append_op: delete known_state must not be empty\0".as_ptr()
                        as *const c_char,
                );
                return;
            }
            Some(bytes)
        } else {
            let Some(node_bytes) = node_bytes else {
                unsafe { sqlite_finalize(stmt) };
                sqlite_result_error(
                    ctx,
                    b"treecrdt_append_op: delete node must be 16-byte BLOB\0".as_ptr()
                        as *const c_char,
                );
                return;
            };
            if let Err(rc) = ensure_materialized(db) {
                unsafe { sqlite_finalize(stmt) };
                sqlite_result_error_code(ctx, rc);
                return;
            }

            let node_id = NodeId(u128::from_be_bytes(node_bytes));
            let node_store = match SqliteNodeStore::prepare(db) {
                Ok(store) => store,
                Err(_) => {
                    unsafe { sqlite_finalize(stmt) };
                    sqlite_result_error_code(ctx, SQLITE_ERROR as c_int);
                    return;
                }
            };

            let crdt = treecrdt_core::TreeCrdt::with_node_store(
                treecrdt_core::ReplicaId::new(b"sqlite-ext"),
                NoopStorage::default(),
                treecrdt_core::LamportClock::default(),
                node_store,
            );

            let vv = match crdt.subtree_version_vector(node_id) {
                Ok(vv) => vv,
                Err(_) => {
                    unsafe { sqlite_finalize(stmt) };
                    sqlite_result_error_code(ctx, SQLITE_ERROR as c_int);
                    return;
                }
            };

            match serialize_version_vector(&vv) {
                Ok(bytes) => Some(bytes),
                Err(rc) => {
                    unsafe { sqlite_finalize(stmt) };
                    sqlite_result_error_code(ctx, rc);
                    return;
                }
            }
        }
    } else {
        None
    };

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
        if let Some(ref bytes) = known_state_blob {
            sqlite_bind_blob(
                stmt,
                9,
                bytes.as_ptr() as *const c_void,
                bytes.len() as c_int,
                None,
            ) != SQLITE_OK as c_int
        } else {
            sqlite_bind_null(stmt, 9) != SQLITE_OK as c_int
        }
    };
    bind_err |= unsafe {
        sqlite_bind_blob(
            stmt,
            10,
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
        // Backfill known_state for deletes inserted via legacy entrypoint.
        if let Some(ref bytes) = known_state_blob {
            let upd_sql = CString::new(
                "UPDATE ops SET known_state = ?3 WHERE replica = ?1 AND counter = ?2 AND known_state IS NULL",
            )
            .expect("update known_state sql");
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
                        bytes.as_ptr() as *const c_void,
                        bytes.len() as c_int,
                        None,
                    );
                    sqlite_step(upd);
                    sqlite_finalize(upd);
                }
            }
        }
        sqlite_result_int(ctx, 1);
        return;
    }

    // Incremental materialization fast-path (best-effort). If we can't apply in-order, mark dirty
    // and let the next reader trigger a full rebuild.
    let meta = match load_tree_meta(db) {
        Ok(v) => v,
        Err(_) => {
            let _ = set_tree_meta_dirty(db, true);
            sqlite_result_int(ctx, 1);
            return;
        }
    };
    if meta.dirty {
        let _ = set_tree_meta_dirty(db, true);
        sqlite_result_int(ctx, 1);
        return;
    }

    let kind_parsed = match MaterializeKind::parse(kind) {
        Some(k) => k,
        None => {
            let _ = set_tree_meta_dirty(db, true);
            sqlite_result_int(ctx, 1);
            return;
        }
    };

    let Some(node_bytes) = node_bytes else {
        let _ = set_tree_meta_dirty(db, true);
        sqlite_result_int(ctx, 1);
        return;
    };
    let node_id = NodeId(u128::from_be_bytes(node_bytes));

    let parse_opt_node_id = |val: *mut sqlite3_value| -> Option<NodeId> {
        unsafe {
            if sqlite_value_type(val) == SQLITE_NULL as c_int {
                return None;
            }
            let ptr = sqlite_value_blob(val) as *const u8;
            let len = sqlite_value_bytes(val) as usize;
            if ptr.is_null() || len != 16 {
                return None;
            }
            let mut out = [0u8; 16];
            out.copy_from_slice(slice::from_raw_parts(ptr, len));
            Some(NodeId(u128::from_be_bytes(out)))
        }
    };

    let parent = parse_opt_node_id(args[4]);
    let new_parent = parse_opt_node_id(args[6]);
    let position = unsafe {
        if sqlite_value_type(args[7]) == SQLITE_NULL as c_int {
            0usize
        } else {
            let v = sqlite_value_int64(args[7]);
            if v < 0 {
                0usize
            } else {
                (v as u64).min(usize::MAX as u64) as usize
            }
        }
    };

    let known_state = if let Some(ref bytes) = known_state_blob {
        if bytes.is_empty() {
            None
        } else {
            match deserialize_version_vector(bytes) {
                Ok(vv) => Some(vv),
                Err(_) => {
                    let _ = set_tree_meta_dirty(db, true);
                    sqlite_result_int(ctx, 1);
                    return;
                }
            }
        }
    } else {
        None
    };

    let mut ops = [MaterializeOp {
        replica: replica.to_vec(),
        counter: counter_i64 as u64,
        lamport: unsafe { sqlite_value_int64(args[2]).max(0) as Lamport },
        kind: kind_parsed,
        parent,
        node: node_id,
        new_parent,
        position,
        known_state,
        op_ref,
    }];
    if materialize_ops_in_order(db, &meta, &mut ops).is_err() {
        let _ = set_tree_meta_dirty(db, true);
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
        if op.kind == "delete"
            && op.known_state.as_ref().map_or(true, |bytes| bytes.is_empty())
        {
            sqlite_result_error(
                ctx,
                b"treecrdt_append_ops: delete op missing known_state\0".as_ptr() as *const c_char,
            );
            return;
        }
    }

    let meta = match load_tree_meta(db) {
        Ok(v) => v,
        Err(rc) => {
            sqlite_result_error_code(ctx, rc);
            return;
        }
    };
    let mut materialize_ops: Vec<MaterializeOp> = Vec::new();
    let mut materialize_ok = !meta.dirty;
    if materialize_ok {
        materialize_ops.reserve(ops.len());
    }

    let begin = CString::new("SAVEPOINT treecrdt_append_ops").expect("static");
    let commit = CString::new("RELEASE treecrdt_append_ops").expect("static");
    let rollback = CString::new("ROLLBACK TO treecrdt_append_ops; RELEASE treecrdt_append_ops")
        .expect("static");

    if sqlite_exec(db, begin.as_ptr(), None, null_mut(), null_mut()) != SQLITE_OK as c_int {
        sqlite_result_error_code(ctx, SQLITE_ERROR as c_int);
        return;
    }

    let insert_sql = CString::new(
        "INSERT OR IGNORE INTO ops (replica,counter,lamport,kind,parent,node,new_parent,position,op_ref,known_state) \
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
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

    let update_known_state_sql = CString::new(
        "UPDATE ops SET known_state = ?3 WHERE replica = ?1 AND counter = ?2 AND known_state IS NULL",
    )
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
        unsafe {
            if let Some(ref known_state) = op.known_state {
                if known_state.is_empty() {
                    bind_err |= sqlite_bind_null(stmt, 10) != SQLITE_OK as c_int;
                } else {
                    bind_err |= sqlite_bind_blob(
                        stmt,
                        10,
                        known_state.as_ptr() as *const c_void,
                        known_state.len() as c_int,
                        None,
                    ) != SQLITE_OK as c_int;
                }
            } else {
                bind_err |= sqlite_bind_null(stmt, 10) != SQLITE_OK as c_int;
            }
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
            if materialize_ok {
                let kind_parsed = match MaterializeKind::parse(op.kind.as_str()) {
                    Some(k) => k,
                    None => {
                        materialize_ok = false;
                        continue;
                    }
                };
                if op.node.len() != 16 {
                    materialize_ok = false;
                    continue;
                }
                let mut node_arr = [0u8; 16];
                node_arr.copy_from_slice(&op.node);
                let node = NodeId(u128::from_be_bytes(node_arr));

                let to_node_id_opt = |bytes: &Option<Vec<u8>>| -> Option<NodeId> {
                    let v = bytes.as_ref()?;
                    if v.len() != 16 {
                        return None;
                    }
                    let mut out = [0u8; 16];
                    out.copy_from_slice(v);
                    Some(NodeId(u128::from_be_bytes(out)))
                };
                let parent_id = to_node_id_opt(&op.parent);
                let new_parent_id = to_node_id_opt(&op.new_parent);
                if kind_parsed == MaterializeKind::Insert && parent_id.is_none() {
                    materialize_ok = false;
                    continue;
                }
                if kind_parsed == MaterializeKind::Move && new_parent_id.is_none() {
                    materialize_ok = false;
                    continue;
                }

                let known_state = match op.known_state.as_ref() {
                    Some(bytes) if !bytes.is_empty() => match deserialize_version_vector(bytes) {
                        Ok(vv) => Some(vv),
                        Err(_) => {
                            materialize_ok = false;
                            continue;
                        }
                    },
                    _ => None,
                };

                materialize_ops.push(MaterializeOp {
                    replica: op.replica.clone(),
                    counter: op.counter,
                    lamport: op.lamport,
                    kind: kind_parsed,
                    parent: parent_id,
                    node,
                    new_parent: new_parent_id,
                    position: op
                        .position
                        .unwrap_or(0)
                        .min(usize::MAX as u64) as usize,
                    known_state,
                    op_ref,
                });
            }
            continue;
        }

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
            sqlite_reset(upd_stmt);
        }

        // Backfill known_state when supplied (e.g. sync upgrades older rows).
        if let Some(ref known_state) = op.known_state {
            if !known_state.is_empty() {
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
                        known_state.as_ptr() as *const c_void,
                        known_state.len() as c_int,
                        None,
                    );
                    sqlite_step(known_state_stmt);
                    sqlite_reset(known_state_stmt);
                }
            }
        }
    }

    unsafe { sqlite_finalize(stmt) };
    unsafe { sqlite_finalize(upd_stmt) };
    unsafe { sqlite_finalize(changes_stmt) };
    unsafe { sqlite_finalize(known_state_stmt) };

    if err_rc == SQLITE_OK as c_int {
        if inserted > 0 {
            if materialize_ok {
                if materialize_ops_in_order(db, &meta, &mut materialize_ops).is_err() {
                    let _ = set_tree_meta_dirty(db, true);
                }
            } else {
                let _ = set_tree_meta_dirty(db, true);
            }
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

unsafe fn column_int_opt(stmt: *mut sqlite3_stmt, idx: c_int) -> Option<u64> {
    let ty = unsafe { sqlite_column_type(stmt, idx) };
    if ty == SQLITE_NULL as c_int {
        None
    } else {
        Some(unsafe { sqlite_column_int64(stmt, idx) as u64 })
    }
}

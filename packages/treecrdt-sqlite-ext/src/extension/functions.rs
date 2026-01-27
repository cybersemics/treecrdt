//! SQLite extension implementation implemented against the SQLite C API.
//! This is intentionally minimal: it proves the cross-target build (native SQLite + wa-sqlite)
//! and registers a basic function to verify loading. Additional virtual tables/functions will
//! bridge to `treecrdt-core`.

#![allow(non_snake_case)]

mod append;
mod doc_id;
mod local_ops;
mod materialize;
mod node_store;
mod op_index;
mod op_storage;
mod oprefs;
mod ops;
mod order_key;
mod payload_store;
mod schema;
mod sqlite_api;
mod util;

use append::{treecrdt_append_op, treecrdt_append_ops};
use doc_id::{treecrdt_doc_id, treecrdt_set_doc_id};
use local_ops::{
    treecrdt_local_delete, treecrdt_local_insert, treecrdt_local_move, treecrdt_local_payload,
};
use materialize::{append_ops_impl, ensure_materialized, treecrdt_ensure_materialized};
use oprefs::{treecrdt_oprefs_all, treecrdt_oprefs_children};
use ops::{treecrdt_ops_by_oprefs, treecrdt_ops_since};
use schema::*;
use sqlite_api::*;
use util::drop_cstring;

use std::ffi::CString;
use std::os::raw::{c_char, c_int, c_void};
use std::ptr::null_mut;
use std::slice;

pub(super) use treecrdt_core::{Lamport, NodeId, VersionVector};

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
    fn apply(&mut self, _op: treecrdt_core::Operation) -> treecrdt_core::Result<bool> {
        Ok(true)
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
    let rc_ensure_materialized = {
        let name = CString::new("treecrdt_ensure_materialized").expect("static name");
        sqlite_create_function_v2(
            db,
            name.as_ptr(),
            0,
            SQLITE_UTF8 as c_int,
            null_mut(),
            Some(treecrdt_ensure_materialized),
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

    let rc_local_insert = {
        let name = CString::new("treecrdt_local_insert").expect("static name");
        sqlite_create_function_v2(
            db,
            name.as_ptr(),
            6,
            SQLITE_UTF8 as c_int,
            null_mut(),
            Some(treecrdt_local_insert),
            None,
            None,
            None,
        )
    };
    let rc_local_move = {
        let name = CString::new("treecrdt_local_move").expect("static name");
        sqlite_create_function_v2(
            db,
            name.as_ptr(),
            5,
            SQLITE_UTF8 as c_int,
            null_mut(),
            Some(treecrdt_local_move),
            None,
            None,
            None,
        )
    };
    let rc_local_delete = {
        let name = CString::new("treecrdt_local_delete").expect("static name");
        sqlite_create_function_v2(
            db,
            name.as_ptr(),
            2,
            SQLITE_UTF8 as c_int,
            null_mut(),
            Some(treecrdt_local_delete),
            None,
            None,
            None,
        )
    };
    let rc_local_payload = {
        let name = CString::new("treecrdt_local_payload").expect("static name");
        sqlite_create_function_v2(
            db,
            name.as_ptr(),
            3,
            SQLITE_UTF8 as c_int,
            null_mut(),
            Some(treecrdt_local_payload),
            None,
            None,
            None,
        )
    };

    if rc != SQLITE_OK as c_int
        || rc_append != SQLITE_OK as c_int
        || rc_set_doc_id != SQLITE_OK as c_int
        || rc_doc_id != SQLITE_OK as c_int
        || rc_ensure_materialized != SQLITE_OK as c_int
        || rc_oprefs_all != SQLITE_OK as c_int
        || rc_oprefs_children != SQLITE_OK as c_int
        || rc_ops_by_oprefs != SQLITE_OK as c_int
        || rc_since != SQLITE_OK as c_int
        || rc_local_insert != SQLITE_OK as c_int
        || rc_local_move != SQLITE_OK as c_int
        || rc_local_delete != SQLITE_OK as c_int
        || rc_local_payload != SQLITE_OK as c_int
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
        } else if rc_ensure_materialized != SQLITE_OK as c_int {
            rc_ensure_materialized
        } else if rc_oprefs_all != SQLITE_OK as c_int {
            rc_oprefs_all
        } else if rc_oprefs_children != SQLITE_OK as c_int {
            rc_oprefs_children
        } else if rc_ops_by_oprefs != SQLITE_OK as c_int {
            rc_ops_by_oprefs
        } else if rc_local_insert != SQLITE_OK as c_int {
            rc_local_insert
        } else if rc_local_move != SQLITE_OK as c_int {
            rc_local_move
        } else if rc_local_delete != SQLITE_OK as c_int {
            rc_local_delete
        } else if rc_local_payload != SQLITE_OK as c_int {
            rc_local_payload
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

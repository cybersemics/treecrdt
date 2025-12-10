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

use treecrdt_core::Lamport;

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

#[cfg(feature = "ext-sqlite")]
fn api<'a>() -> Option<&'a sqlite3_api_routines> {
    unsafe { SQLITE3_API.as_ref() }
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

    if rc != SQLITE_OK as c_int || rc_append != SQLITE_OK as c_int || rc_since != SQLITE_OK as c_int
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
    const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS ops (
  replica BLOB NOT NULL,
  counter INTEGER NOT NULL,
  lamport INTEGER NOT NULL,
  kind TEXT NOT NULL,
  parent BLOB,
  node BLOB NOT NULL,
  new_parent BLOB,
  position INTEGER,
  PRIMARY KEY (replica, counter)
);
CREATE INDEX IF NOT EXISTS idx_ops_lamport ON ops(lamport, replica, counter);
"#;

    #[cfg(feature = "ext-sqlite")]
    {
        if api().is_none() {
            return Err(SQLITE_ERROR as c_int);
        }
    }
    let sql = CString::new(SCHEMA).expect("static schema");
    let rc = sqlite_exec(db, sql.as_ptr(), None, null_mut(), null_mut());
    if rc == SQLITE_OK as c_int {
        Ok(())
    } else {
        Err(rc)
    }
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
        "INSERT OR IGNORE INTO ops (replica,counter,lamport,kind,parent,node,new_parent,position) VALUES (?1,?2,?3,?4,?5,?6,?7,?8)",
    )
    .expect("static sql");

    let mut stmt: *mut sqlite3_stmt = null_mut();
    let rc = sqlite_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, null_mut());
    if rc != SQLITE_OK as c_int {
        sqlite_result_error_code(ctx, rc);
        return;
    }

    let args = unsafe { std::slice::from_raw_parts(argv, argc as usize) };

    let mut bind_err = false;
    bind_err |= unsafe { bind_blob(stmt, 1, args[0]) };
    bind_err |= unsafe { bind_int64(stmt, 2, args[1]) };
    bind_err |= unsafe { bind_int64(stmt, 3, args[2]) };
    bind_err |= unsafe { bind_text(stmt, 4, args[3]) };
    bind_err |= unsafe { bind_optional_blob(stmt, 5, args[4]) };
    bind_err |= unsafe { bind_blob(stmt, 6, args[5]) };
    bind_err |= unsafe { bind_optional_blob(stmt, 7, args[6]) };
    bind_err |= unsafe { bind_optional_int(stmt, 8, args[7]) };

    if bind_err {
        unsafe { sqlite_finalize(stmt) };
        sqlite_result_error_code(ctx, SQLITE_ERROR as c_int);
        return;
    }

    let step_rc = unsafe { sqlite_step(stmt) };
    let finalize_rc = unsafe { sqlite_finalize(stmt) };
    if step_rc == SQLITE_DONE as c_int && finalize_rc == SQLITE_OK as c_int {
        sqlite_result_int(ctx, 1);
    } else {
        let rc = if step_rc != SQLITE_DONE as c_int {
            step_rc
        } else {
            finalize_rc
        };
        sqlite_result_error_code(ctx, rc);
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
    let begin = CString::new("BEGIN").expect("static");
    let commit = CString::new("COMMIT").expect("static");
    let rollback = CString::new("ROLLBACK").expect("static");

    if sqlite_exec(db, begin.as_ptr(), None, null_mut(), null_mut()) != SQLITE_OK as c_int {
        sqlite_result_error_code(ctx, SQLITE_ERROR as c_int);
        return;
    }

    let insert_sql = CString::new(
        "INSERT OR IGNORE INTO ops (replica,counter,lamport,kind,parent,node,new_parent,position) VALUES (?1,?2,?3,?4,?5,?6,?7,?8)",
    )
    .expect("static sql");
    let mut stmt: *mut sqlite3_stmt = null_mut();
    let prep_rc = sqlite_prepare_v2(db, insert_sql.as_ptr(), -1, &mut stmt, null_mut());
    if prep_rc != SQLITE_OK as c_int {
        sqlite_exec(db, rollback.as_ptr(), None, null_mut(), null_mut());
        sqlite_result_error_code(ctx, prep_rc);
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
        let kind_cstr = CString::new(op.kind).unwrap_or_else(|_| CString::new("insert").unwrap());
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

        if bind_err {
            err_rc = SQLITE_ERROR as c_int;
            break;
        }

        let step_rc = unsafe { sqlite_step(stmt) };
        if step_rc == SQLITE_DONE as c_int {
            inserted += 1;
        } else {
            err_rc = step_rc;
            break;
        }
    }

    unsafe { sqlite_finalize(stmt) };

    if err_rc == SQLITE_OK as c_int {
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
    parent: Option<u128>,
    node: u128,
    new_parent: Option<u128>,
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

        let parent = column_node(stmt, 4)?;
        let node = column_node(stmt, 5)?.unwrap_or(0);
        let new_parent = column_node(stmt, 6)?;
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

unsafe fn column_node(stmt: *mut sqlite3_stmt, idx: c_int) -> Result<Option<u128>, c_int> {
    let ty = unsafe { sqlite_column_type(stmt, idx) };
    if ty == SQLITE_NULL as c_int {
        return Ok(None);
    }
    let ptr = unsafe { sqlite_column_blob(stmt, idx) };
    let len = unsafe { sqlite_column_bytes(stmt, idx) };
    if len != 16 {
        return Err(SQLITE_ERROR as c_int);
    }
    let bytes = unsafe { std::slice::from_raw_parts(ptr as *const u8, len as usize) };
    let mut buf = [0u8; 16];
    buf.copy_from_slice(bytes);
    Ok(Some(u128::from_be_bytes(buf)))
}

unsafe fn column_int_opt(stmt: *mut sqlite3_stmt, idx: c_int) -> Option<u64> {
    let ty = unsafe { sqlite_column_type(stmt, idx) };
    if ty == SQLITE_NULL as c_int {
        None
    } else {
        Some(unsafe { sqlite_column_int64(stmt, idx) as u64 })
    }
}

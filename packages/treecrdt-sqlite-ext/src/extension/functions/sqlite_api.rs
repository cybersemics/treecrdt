use std::os::raw::{c_char, c_int, c_void};
use std::slice;

#[cfg(feature = "ext-sqlite")]
use std::ptr::null;

#[cfg(feature = "ext-sqlite")]
pub(super) use sqlite3ext_sys::{
    sqlite3, sqlite3_api_routines, sqlite3_context, sqlite3_stmt, sqlite3_value, SQLITE_DONE,
    SQLITE_ERROR, SQLITE_NULL, SQLITE_OK, SQLITE_ROW, SQLITE_UTF8,
};

#[cfg(feature = "static-link")]
#[allow(non_camel_case_types)]
pub(super) type sqlite3_api_routines = c_void;

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
        pub fn sqlite3_result_blob(
            ctx: *mut sqlite3_context,
            val: *const c_void,
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

        pub fn sqlite3_changes(db: *mut sqlite3) -> c_int;

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
pub(super) use ffi::{
    sqlite3, sqlite3_context, sqlite3_stmt, sqlite3_value, SQLITE_DONE, SQLITE_ERROR, SQLITE_NULL,
    SQLITE_OK, SQLITE_ROW, SQLITE_UTF8,
};

#[cfg(feature = "ext-sqlite")]
static mut SQLITE3_API: *const sqlite3_api_routines = null();

#[cfg(feature = "ext-sqlite")]
fn api<'a>() -> Option<&'a sqlite3_api_routines> {
    unsafe { SQLITE3_API.as_ref() }
}

#[cfg(feature = "ext-sqlite")]
pub(super) unsafe fn set_sqlite3_api(p_api: *const sqlite3_api_routines) -> Result<(), c_int> {
    if p_api.is_null() {
        return Err(SQLITE_ERROR as c_int);
    }
    unsafe { SQLITE3_API = p_api };
    Ok(())
}

#[cfg(feature = "static-link")]
pub(super) unsafe fn set_sqlite3_api(_p_api: *const sqlite3_api_routines) -> Result<(), c_int> {
    Ok(())
}

pub(super) fn ensure_api_initialized() -> Result<(), c_int> {
    #[cfg(feature = "ext-sqlite")]
    {
        if api().is_none() {
            return Err(SQLITE_ERROR as c_int);
        }
    }
    Ok(())
}

// Wrapper functions to abstract over the two build modes.
pub(super) fn sqlite_create_function_v2(
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

pub(super) fn sqlite_exec(
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

pub(super) fn sqlite_prepare_v2(
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

pub(super) fn sqlite_changes(db: *mut sqlite3) -> c_int {
    #[cfg(feature = "ext-sqlite")]
    {
        let api = api().expect("api table");
        unsafe { (api.changes.unwrap())(db) }
    }
    #[cfg(feature = "static-link")]
    unsafe {
        ffi::sqlite3_changes(db)
    }
}

pub(super) unsafe fn sqlite_bind_blob(
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

pub(super) unsafe fn sqlite_bind_text(
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

pub(super) unsafe fn sqlite_bind_int64(stmt: *mut sqlite3_stmt, idx: c_int, val: i64) -> c_int {
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

pub(super) unsafe fn sqlite_bind_null(stmt: *mut sqlite3_stmt, idx: c_int) -> c_int {
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

pub(super) unsafe fn sqlite_step(stmt: *mut sqlite3_stmt) -> c_int {
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

pub(super) unsafe fn sqlite_reset(stmt: *mut sqlite3_stmt) -> c_int {
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

pub(super) unsafe fn sqlite_clear_bindings(stmt: *mut sqlite3_stmt) -> c_int {
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

pub(super) unsafe fn sqlite_finalize(stmt: *mut sqlite3_stmt) -> c_int {
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

pub(super) unsafe fn sqlite_value_blob(val: *mut sqlite3_value) -> *const c_void {
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

pub(super) unsafe fn sqlite_value_bytes(val: *mut sqlite3_value) -> c_int {
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

pub(super) unsafe fn sqlite_value_int64(val: *mut sqlite3_value) -> i64 {
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

pub(super) unsafe fn sqlite_value_text(val: *mut sqlite3_value) -> *const c_char {
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

pub(super) unsafe fn sqlite_value_type(val: *mut sqlite3_value) -> c_int {
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

pub(super) fn sqlite_result_text(
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

pub(super) fn sqlite_result_error_code(ctx: *mut sqlite3_context, code: c_int) {
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

pub(super) fn sqlite_result_int(ctx: *mut sqlite3_context, val: c_int) {
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

pub(super) fn sqlite_result_error(ctx: *mut sqlite3_context, msg: *const c_char) {
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

pub(super) fn sqlite_context_db_handle(ctx: *mut sqlite3_context) -> *mut sqlite3 {
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

pub(super) unsafe fn sqlite_column_blob(stmt: *mut sqlite3_stmt, idx: c_int) -> *const c_void {
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

pub(super) unsafe fn sqlite_column_bytes(stmt: *mut sqlite3_stmt, idx: c_int) -> c_int {
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

pub(super) unsafe fn sqlite_column_int64(stmt: *mut sqlite3_stmt, idx: c_int) -> i64 {
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

pub(super) unsafe fn sqlite_column_text(stmt: *mut sqlite3_stmt, idx: c_int) -> *const c_char {
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

pub(super) unsafe fn sqlite_column_type(stmt: *mut sqlite3_stmt, idx: c_int) -> c_int {
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

pub(super) unsafe fn column_blob16(
    stmt: *mut sqlite3_stmt,
    idx: c_int,
) -> Result<Option<[u8; 16]>, c_int> {
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

pub(super) unsafe fn bind_blob(
    stmt: *mut sqlite3_stmt,
    idx: c_int,
    val: *mut sqlite3_value,
) -> bool {
    let ptr = unsafe { sqlite_value_blob(val) };
    let len = unsafe { sqlite_value_bytes(val) };
    unsafe { sqlite_bind_blob(stmt, idx, ptr, len, None) != SQLITE_OK as c_int }
}

pub(super) unsafe fn bind_optional_blob(
    stmt: *mut sqlite3_stmt,
    idx: c_int,
    val: *mut sqlite3_value,
) -> bool {
    let ty = unsafe { sqlite_value_type(val) };
    if ty == SQLITE_NULL as c_int {
        unsafe { sqlite_bind_null(stmt, idx) != SQLITE_OK as c_int }
    } else {
        unsafe { bind_blob(stmt, idx, val) }
    }
}

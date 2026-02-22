use super::sqlite_api::*;

use std::ffi::CString;
use std::os::raw::{c_char, c_int, c_void};
use std::slice;

pub(super) fn sqlite_result_json<T>(ctx: *mut sqlite3_context, value: &T)
where
    T: serde::Serialize,
{
    let json = match serde_json::to_string(value) {
        Ok(v) => v,
        Err(_) => {
            sqlite_result_error_code(ctx, SQLITE_ERROR as c_int);
            return;
        }
    };
    sqlite_result_json_string(ctx, json);
}

pub(super) fn sqlite_result_json_string(ctx: *mut sqlite3_context, json: String) {
    if let Ok(cstr) = CString::new(json) {
        let len = cstr.as_bytes().len() as c_int;
        let ptr = cstr.into_raw();
        sqlite_result_text(ctx, ptr as *const c_char, len, Some(drop_cstring));
    } else {
        sqlite_result_error_code(ctx, SQLITE_ERROR as c_int);
    }
}

pub(super) unsafe extern "C" fn drop_cstring(ptr: *mut c_void) {
    if !ptr.is_null() {
        unsafe {
            drop(CString::from_raw(ptr as *mut c_char));
        }
    }
}

pub(super) fn read_blob(val: *mut sqlite3_value) -> Option<Vec<u8>> {
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

pub(super) fn read_required_blob(val: *mut sqlite3_value) -> Result<Vec<u8>, ()> {
    match read_blob(val) {
        Some(v) => Ok(v),
        None => Err(()),
    }
}

pub(super) fn read_blob16(val: *mut sqlite3_value) -> Result<[u8; 16], ()> {
    let bytes = read_required_blob(val)?;
    if bytes.len() != 16 {
        return Err(());
    }
    let mut out = [0u8; 16];
    out.copy_from_slice(&bytes);
    Ok(out)
}

pub(super) fn read_optional_blob16(val: *mut sqlite3_value) -> Result<Option<[u8; 16]>, ()> {
    unsafe {
        if sqlite_value_type(val) == SQLITE_NULL as c_int {
            return Ok(None);
        }
    }
    Ok(Some(read_blob16(val)?))
}

pub(super) fn read_text(val: *mut sqlite3_value) -> String {
    unsafe {
        let ptr = sqlite_value_text(val) as *const u8;
        let len = sqlite_value_bytes(val) as usize;
        if ptr.is_null() || len == 0 {
            return String::new();
        }
        std::str::from_utf8(slice::from_raw_parts(ptr, len)).unwrap_or("").to_string()
    }
}

pub(super) fn sqlite_err_from_core(_: treecrdt_core::Error) -> c_int {
    SQLITE_ERROR as c_int
}

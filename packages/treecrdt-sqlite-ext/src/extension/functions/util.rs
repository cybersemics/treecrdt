use super::sqlite_api::*;

use std::ffi::CString;
use std::os::raw::{c_char, c_int, c_void};

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

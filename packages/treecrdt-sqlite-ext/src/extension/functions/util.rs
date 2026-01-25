use super::sqlite_api::*;

use std::alloc::{alloc, dealloc, Layout};
use std::ffi::CString;
use std::os::raw::{c_char, c_int, c_void};
use std::ptr;

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

const USIZE_BYTES: usize = std::mem::size_of::<usize>();

pub(super) fn sqlite_result_blob_owned(ctx: *mut sqlite3_context, bytes: &[u8]) {
    let total_len = USIZE_BYTES
        .checked_add(bytes.len())
        .expect("blob allocation size overflow");
    let layout =
        Layout::from_size_align(total_len, std::mem::align_of::<usize>()).expect("blob layout");
    let base = unsafe { alloc(layout) };
    if base.is_null() {
        sqlite_result_error_code(ctx, SQLITE_ERROR as c_int);
        return;
    }
    unsafe {
        (base as *mut usize).write(bytes.len());
        let data_ptr = base.add(USIZE_BYTES);
        if !bytes.is_empty() {
            ptr::copy_nonoverlapping(bytes.as_ptr(), data_ptr, bytes.len());
        }
        sqlite_result_blob(
            ctx,
            data_ptr as *const c_void,
            bytes.len() as c_int,
            Some(drop_allocated_blob),
        );
    }
}

pub(super) unsafe extern "C" fn drop_allocated_blob(ptr: *mut c_void) {
    if ptr.is_null() {
        return;
    }
    unsafe {
        let data_ptr = ptr as *mut u8;
        let base = data_ptr.sub(USIZE_BYTES);
        let len = (base as *const usize).read();
        let total_len = USIZE_BYTES
            .checked_add(len)
            .expect("blob deallocation size overflow");
        let layout =
            Layout::from_size_align(total_len, std::mem::align_of::<usize>()).expect("blob layout");
        dealloc(base, layout);
    }
}

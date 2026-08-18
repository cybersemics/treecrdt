use super::*;

use std::{cell::Cell, ffi::CStr};

/// Lazily prepares a SQLite statement and finalizes it with its owning operation-scoped store.
///
/// Statements intentionally do not outlive the store: keeping them across local UDF calls would
/// leave the SQLite connection busy when its owner tries to close it.
pub(super) struct LazyStatement {
    db: *mut sqlite3,
    sql: &'static CStr,
    stmt: Cell<*mut sqlite3_stmt>,
}

impl LazyStatement {
    pub(super) fn new(db: *mut sqlite3, sql: &'static CStr) -> Self {
        Self {
            db,
            sql,
            stmt: Cell::new(null_mut()),
        }
    }

    pub(super) fn get(&self) -> treecrdt_core::Result<*mut sqlite3_stmt> {
        let cached = self.stmt.get();
        if !cached.is_null() {
            return Ok(cached);
        }

        let mut stmt = null_mut();
        let rc = sqlite_prepare_v2(self.db, self.sql.as_ptr(), -1, &mut stmt, null_mut());
        if rc != SQLITE_OK as c_int || stmt.is_null() {
            if !stmt.is_null() {
                unsafe { sqlite_finalize(stmt) };
            }
            return Err(treecrdt_core::Error::Storage(format!(
                "sqlite_prepare_v2 failed (rc={rc})"
            )));
        }
        self.stmt.set(stmt);
        Ok(stmt)
    }
}

impl Drop for LazyStatement {
    fn drop(&mut self) {
        let stmt = self.stmt.get();
        if !stmt.is_null() {
            unsafe { sqlite_finalize(stmt) };
        }
    }
}

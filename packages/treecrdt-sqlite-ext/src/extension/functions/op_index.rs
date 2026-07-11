use super::*;

fn sqlite_rc_error(rc: c_int, context: &str) -> treecrdt_core::Error {
    treecrdt_core::Error::Storage(format!("{context} (rc={rc})"))
}

pub(super) struct SqliteParentOpIndex {
    db: *mut sqlite3,
    doc_id: Vec<u8>,
    insert: *mut sqlite3_stmt,
}

impl SqliteParentOpIndex {
    pub(super) fn prepare(db: *mut sqlite3, doc_id: Vec<u8>) -> treecrdt_core::Result<Self> {
        let insert_sql = CString::new(
            "INSERT OR IGNORE INTO oprefs_children(parent, op_ref, seq) VALUES (?1, ?2, ?3)",
        )
        .expect("oprefs_children insert sql");
        let mut insert: *mut sqlite3_stmt = null_mut();
        let rc = sqlite_prepare_v2(db, insert_sql.as_ptr(), -1, &mut insert, null_mut());
        if rc != SQLITE_OK as c_int {
            return Err(sqlite_rc_error(
                rc,
                "sqlite_prepare_v2 oprefs_children insert failed",
            ));
        }
        Ok(Self { db, doc_id, insert })
    }
}

impl Drop for SqliteParentOpIndex {
    fn drop(&mut self) {
        unsafe { sqlite_finalize(self.insert) };
    }
}

impl treecrdt_core::ParentOpIndex for SqliteParentOpIndex {
    fn reset(&mut self) -> treecrdt_core::Result<()> {
        let sql = CString::new("DELETE FROM oprefs_children").expect("clear oprefs_children sql");
        let rc = sqlite_exec(self.db, sql.as_ptr(), None, null_mut(), null_mut());
        if rc != SQLITE_OK as c_int {
            return Err(sqlite_rc_error(
                rc,
                "sqlite_exec clear oprefs_children failed",
            ));
        }
        Ok(())
    }

    fn record(
        &mut self,
        parent: NodeId,
        op_id: &treecrdt_core::OperationId,
        seq: u64,
    ) -> treecrdt_core::Result<()> {
        if parent == NodeId::TRASH {
            return Ok(());
        }

        let parent_bytes = parent.0.to_be_bytes();
        let op_ref = derive_op_ref_v0(&self.doc_id, op_id.replica.as_bytes(), op_id.counter);

        unsafe {
            sqlite_clear_bindings(self.insert);
            sqlite_reset(self.insert);
        }
        let mut bind_err = false;
        unsafe {
            bind_err |= sqlite_bind_blob(
                self.insert,
                1,
                parent_bytes.as_ptr() as *const c_void,
                parent_bytes.len() as c_int,
                None,
            ) != SQLITE_OK as c_int;
            bind_err |= sqlite_bind_blob(
                self.insert,
                2,
                op_ref.as_ptr() as *const c_void,
                op_ref.len() as c_int,
                None,
            ) != SQLITE_OK as c_int;
            bind_err |= sqlite_bind_int64(self.insert, 3, seq.min(i64::MAX as u64) as i64)
                != SQLITE_OK as c_int;
        }
        if bind_err {
            unsafe { sqlite_reset(self.insert) };
            return Err(sqlite_rc_error(
                SQLITE_ERROR as c_int,
                "bind oprefs_children insert failed",
            ));
        }

        let step_rc = unsafe { sqlite_step(self.insert) };
        unsafe { sqlite_reset(self.insert) };
        if step_rc != SQLITE_DONE as c_int {
            return Err(sqlite_rc_error(
                step_rc,
                "oprefs_children insert step failed",
            ));
        }
        Ok(())
    }
}

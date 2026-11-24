use super::statement::LazyStatement;
use super::*;

fn sqlite_rc_error(rc: c_int, context: &str) -> treecrdt_core::Error {
    treecrdt_core::Error::Storage(format!("{context} (rc={rc})"))
}

pub(super) struct SqliteParentOpIndex {
    db: *mut sqlite3,
    doc_id: Vec<u8>,
    insert: LazyStatement,
    placement: LazyStatement,
}

impl SqliteParentOpIndex {
    pub(super) fn prepare(db: *mut sqlite3, doc_id: Vec<u8>) -> treecrdt_core::Result<Self> {
        Ok(Self {
            db,
            doc_id,
            insert: LazyStatement::new(
                db,
                c"INSERT OR IGNORE INTO oprefs_children(parent, op_ref, seq) VALUES (?1, ?2, ?3)",
            ),
            placement: LazyStatement::new(
                db,
                c"SELECT replica, counter FROM ops \
                  WHERE node = ?1 \
                    AND ((kind = 'insert' AND parent = ?2) OR (kind = 'move' AND new_parent = ?2)) \
                    AND (lamport < ?3 OR (lamport = ?3 AND (replica < ?4 OR (replica = ?4 AND counter < ?5)))) \
                  ORDER BY lamport DESC, replica DESC, counter DESC \
                  LIMIT 1",
            ),
        })
    }

    /// The latest insert or move of `node` that names `parent` and sorts before `before`.
    fn latest_placement(
        &self,
        node: NodeId,
        parent: NodeId,
        before: &treecrdt_core::Operation,
    ) -> treecrdt_core::Result<Option<treecrdt_core::OperationId>> {
        let node_bytes = node.0.to_be_bytes();
        let parent_bytes = parent.0.to_be_bytes();
        let replica = before.meta.id.replica.as_bytes();
        let stmt = self.placement.get()?;
        unsafe {
            sqlite_clear_bindings(stmt);
            sqlite_reset(stmt);
        }
        let mut bind_err = false;
        unsafe {
            bind_err |= sqlite_bind_blob(
                stmt,
                1,
                node_bytes.as_ptr() as *const c_void,
                node_bytes.len() as c_int,
                None,
            ) != SQLITE_OK as c_int;
            bind_err |= sqlite_bind_blob(
                stmt,
                2,
                parent_bytes.as_ptr() as *const c_void,
                parent_bytes.len() as c_int,
                None,
            ) != SQLITE_OK as c_int;
            bind_err |= sqlite_bind_int64(stmt, 3, before.meta.lamport.min(i64::MAX as u64) as i64)
                != SQLITE_OK as c_int;
            bind_err |= sqlite_bind_blob(
                stmt,
                4,
                replica.as_ptr() as *const c_void,
                replica.len() as c_int,
                None,
            ) != SQLITE_OK as c_int;
            bind_err |=
                sqlite_bind_int64(stmt, 5, before.meta.id.counter.min(i64::MAX as u64) as i64)
                    != SQLITE_OK as c_int;
        }
        if bind_err {
            unsafe { sqlite_reset(stmt) };
            return Err(sqlite_rc_error(
                SQLITE_ERROR as c_int,
                "bind latest placement failed",
            ));
        }

        let step_rc = unsafe { sqlite_step(stmt) };
        let placement = if step_rc == SQLITE_ROW as c_int {
            let (ptr, len) = unsafe {
                (
                    sqlite_column_blob(stmt, 0) as *const u8,
                    sqlite_column_bytes(stmt, 0) as usize,
                )
            };
            let replica = if ptr.is_null() || len == 0 {
                Vec::new()
            } else {
                unsafe { slice::from_raw_parts(ptr, len) }.to_vec()
            };
            let counter = unsafe { sqlite_column_int64(stmt, 1) }.max(0) as u64;
            Some(treecrdt_core::OperationId {
                replica: treecrdt_core::ReplicaId(replica),
                counter,
            })
        } else if step_rc == SQLITE_DONE as c_int {
            None
        } else {
            unsafe { sqlite_reset(stmt) };
            return Err(sqlite_rc_error(step_rc, "latest placement step failed"));
        };
        unsafe { sqlite_reset(stmt) };
        Ok(placement)
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
        let stmt = self.insert.get()?;

        unsafe {
            sqlite_clear_bindings(stmt);
            sqlite_reset(stmt);
        }
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
            bind_err |=
                sqlite_bind_int64(stmt, 3, seq.min(i64::MAX as u64) as i64) != SQLITE_OK as c_int;
        }
        if bind_err {
            unsafe { sqlite_reset(stmt) };
            return Err(sqlite_rc_error(
                SQLITE_ERROR as c_int,
                "bind oprefs_children insert failed",
            ));
        }

        let step_rc = unsafe { sqlite_step(stmt) };
        unsafe { sqlite_reset(stmt) };
        if step_rc != SQLITE_DONE as c_int {
            return Err(sqlite_rc_error(
                step_rc,
                "oprefs_children insert step failed",
            ));
        }
        Ok(())
    }

    fn record_placement(
        &mut self,
        parent: NodeId,
        node: NodeId,
        placed_under: NodeId,
        before: &treecrdt_core::Operation,
        seq: u64,
    ) -> treecrdt_core::Result<()> {
        match self.latest_placement(node, placed_under, before)? {
            Some(op_id) => self.record(parent, &op_id, seq),
            None => Ok(()),
        }
    }
}

impl treecrdt_core::TruncatingParentOpIndex for SqliteParentOpIndex {
    fn truncate_from(&mut self, seq: u64) -> treecrdt_core::Result<()> {
        let sql =
            CString::new("DELETE FROM oprefs_children WHERE seq >= ?1").expect("truncate oprefs");
        let mut stmt: *mut sqlite3_stmt = null_mut();
        let prep_rc = sqlite_prepare_v2(self.db, sql.as_ptr(), -1, &mut stmt, null_mut());
        if prep_rc != SQLITE_OK as c_int {
            return Err(sqlite_rc_error(prep_rc, "prepare truncate oprefs failed"));
        }
        unsafe {
            sqlite_bind_int64(stmt, 1, seq.min(i64::MAX as u64) as i64);
            let step_rc = sqlite_step(stmt);
            sqlite_finalize(stmt);
            if step_rc != SQLITE_DONE as c_int {
                return Err(sqlite_rc_error(step_rc, "truncate oprefs step failed"));
            }
        }
        Ok(())
    }
}

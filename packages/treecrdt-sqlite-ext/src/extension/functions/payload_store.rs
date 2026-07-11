use super::statement::LazyStatement;
use super::*;

fn sqlite_node_id_bytes(node: NodeId) -> [u8; 16] {
    node.0.to_be_bytes()
}

fn sqlite_rc_error(rc: c_int, context: &str) -> treecrdt_core::Error {
    treecrdt_core::Error::Storage(format!("{context} (rc={rc})"))
}

pub(super) struct SqlitePayloadStore {
    db: *mut sqlite3,
    select: LazyStatement,
    upsert: LazyStatement,
    delete: LazyStatement,
}

impl SqlitePayloadStore {
    pub(super) fn prepare(db: *mut sqlite3) -> treecrdt_core::Result<Self> {
        Ok(Self {
            db,
            select: LazyStatement::new(
                db,
                c"SELECT payload, last_lamport, last_replica, last_counter FROM tree_payload WHERE node = ?1 LIMIT 1",
            ),
            upsert: LazyStatement::new(
                db,
                c"INSERT INTO tree_payload(node,payload,last_lamport,last_replica,last_counter) VALUES (?1,?2,?3,?4,?5) ON CONFLICT(node) DO UPDATE SET payload = excluded.payload, last_lamport = excluded.last_lamport, last_replica = excluded.last_replica, last_counter = excluded.last_counter",
            ),
            delete: LazyStatement::new(db, c"DELETE FROM tree_payload WHERE node = ?1"),
        })
    }
}

impl treecrdt_core::PayloadStore for SqlitePayloadStore {
    fn reset(&mut self) -> treecrdt_core::Result<()> {
        let clear_sql = CString::new("DELETE FROM tree_payload").expect("clear payload sql");
        let rc = sqlite_exec(self.db, clear_sql.as_ptr(), None, null_mut(), null_mut());
        if rc != SQLITE_OK as c_int {
            return Err(sqlite_rc_error(rc, "sqlite_exec reset tree_payload failed"));
        }
        Ok(())
    }

    fn payload(&self, node: NodeId) -> treecrdt_core::Result<Option<Vec<u8>>> {
        let bytes = sqlite_node_id_bytes(node);
        let stmt = self.select.get()?;
        unsafe {
            sqlite_clear_bindings(stmt);
            sqlite_reset(stmt);
            let bind_rc = sqlite_bind_blob(
                stmt,
                1,
                bytes.as_ptr() as *const c_void,
                bytes.len() as c_int,
                None,
            );
            if bind_rc != SQLITE_OK as c_int {
                sqlite_reset(stmt);
                return Err(sqlite_rc_error(bind_rc, "bind select payload failed"));
            }

            let step_rc = sqlite_step(stmt);
            let payload = if step_rc == SQLITE_ROW as c_int {
                if sqlite_column_type(stmt, 0) == SQLITE_NULL as c_int {
                    None
                } else {
                    let ptr = sqlite_column_blob(stmt, 0) as *const u8;
                    let len = sqlite_column_bytes(stmt, 0) as usize;
                    if ptr.is_null() {
                        None
                    } else {
                        Some(slice::from_raw_parts(ptr, len).to_vec())
                    }
                }
            } else if step_rc == SQLITE_DONE as c_int {
                None
            } else {
                sqlite_reset(stmt);
                return Err(sqlite_rc_error(step_rc, "select payload step failed"));
            };
            sqlite_reset(stmt);
            Ok(payload)
        }
    }

    fn last_writer(
        &self,
        node: NodeId,
    ) -> treecrdt_core::Result<Option<(Lamport, treecrdt_core::OperationId)>> {
        let bytes = sqlite_node_id_bytes(node);
        let stmt = self.select.get()?;
        unsafe {
            sqlite_clear_bindings(stmt);
            sqlite_reset(stmt);
            let bind_rc = sqlite_bind_blob(
                stmt,
                1,
                bytes.as_ptr() as *const c_void,
                bytes.len() as c_int,
                None,
            );
            if bind_rc != SQLITE_OK as c_int {
                sqlite_reset(stmt);
                return Err(sqlite_rc_error(
                    bind_rc,
                    "bind select payload writer failed",
                ));
            }

            let step_rc = sqlite_step(stmt);
            let writer = if step_rc == SQLITE_ROW as c_int {
                let lamport = sqlite_column_int64(stmt, 1).max(0) as Lamport;
                let rep_ptr = sqlite_column_blob(stmt, 2) as *const u8;
                let rep_len = sqlite_column_bytes(stmt, 2) as usize;
                let replica = if rep_ptr.is_null() || rep_len == 0 {
                    Vec::new()
                } else {
                    slice::from_raw_parts(rep_ptr, rep_len).to_vec()
                };
                let counter = sqlite_column_int64(stmt, 3).max(0) as u64;
                Some((
                    lamport,
                    treecrdt_core::OperationId {
                        replica: treecrdt_core::ReplicaId(replica),
                        counter,
                    },
                ))
            } else if step_rc == SQLITE_DONE as c_int {
                None
            } else {
                sqlite_reset(stmt);
                return Err(sqlite_rc_error(
                    step_rc,
                    "select payload writer step failed",
                ));
            };
            sqlite_reset(stmt);
            Ok(writer)
        }
    }

    fn set_payload(
        &mut self,
        node: NodeId,
        payload: Option<Vec<u8>>,
        writer: (Lamport, treecrdt_core::OperationId),
    ) -> treecrdt_core::Result<()> {
        let node_bytes = sqlite_node_id_bytes(node);
        let (lamport, id) = writer;
        let stmt = self.upsert.get()?;
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

            if let Some(ref bytes) = payload {
                bind_err |= sqlite_bind_blob(
                    stmt,
                    2,
                    bytes.as_ptr() as *const c_void,
                    bytes.len() as c_int,
                    None,
                ) != SQLITE_OK as c_int;
            } else {
                bind_err |= sqlite_bind_null(stmt, 2) != SQLITE_OK as c_int;
            }

            bind_err |= sqlite_bind_int64(stmt, 3, lamport as i64) != SQLITE_OK as c_int;
            bind_err |= sqlite_bind_blob(
                stmt,
                4,
                id.replica.as_bytes().as_ptr() as *const c_void,
                id.replica.as_bytes().len() as c_int,
                None,
            ) != SQLITE_OK as c_int;
            bind_err |= sqlite_bind_int64(stmt, 5, id.counter as i64) != SQLITE_OK as c_int;
        }
        if bind_err {
            unsafe { sqlite_reset(stmt) };
            return Err(sqlite_rc_error(
                SQLITE_ERROR as c_int,
                "bind upsert payload failed",
            ));
        }
        let step_rc = unsafe { sqlite_step(stmt) };
        unsafe { sqlite_reset(stmt) };
        if step_rc != SQLITE_DONE as c_int {
            return Err(sqlite_rc_error(step_rc, "upsert payload step failed"));
        }
        Ok(())
    }
}

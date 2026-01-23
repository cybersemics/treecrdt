use super::*;

fn sqlite_rc_error(rc: c_int, context: &str) -> treecrdt_core::Error {
    treecrdt_core::Error::Storage(format!("{context} (rc={rc})"))
}

fn vv_to_bytes(vv: &VersionVector) -> treecrdt_core::Result<Vec<u8>> {
    serde_json::to_vec(vv).map_err(|e| treecrdt_core::Error::Storage(e.to_string()))
}

fn vv_from_bytes(bytes: &[u8]) -> treecrdt_core::Result<VersionVector> {
    serde_json::from_slice(bytes).map_err(|e| treecrdt_core::Error::Storage(e.to_string()))
}

fn sqlite_bytes_to_node_id(bytes: [u8; 16]) -> NodeId {
    NodeId(u128::from_be_bytes(bytes))
}

fn sqlite_node_id_bytes(node: NodeId) -> [u8; 16] {
    node.0.to_be_bytes()
}

pub(super) struct SqliteOpStorage {
    db: *mut sqlite3,
    doc_id: Option<Vec<u8>>,
}

impl SqliteOpStorage {
    pub(super) fn new(db: *mut sqlite3) -> Self {
        Self { db, doc_id: None }
    }

    fn ensure_doc_id(&mut self) -> treecrdt_core::Result<&[u8]> {
        if self.doc_id.is_none() {
            self.doc_id =
                load_doc_id(self.db).map_err(|rc| sqlite_rc_error(rc, "load_doc_id failed"))?;
        }
        self.doc_id
            .as_deref()
            .ok_or_else(|| treecrdt_core::Error::Storage("doc_id not set".into()))
    }
}

impl treecrdt_core::Storage for SqliteOpStorage {
    fn apply(&mut self, op: treecrdt_core::Operation) -> treecrdt_core::Result<bool> {
        let doc_id = self.ensure_doc_id()?;

        let (kind, parent, node, new_parent, position, known_state, payload) = match op.kind {
            treecrdt_core::OperationKind::Insert {
                parent,
                node,
                position,
                payload,
            } => (
                "insert",
                Some(sqlite_node_id_bytes(parent).to_vec()),
                sqlite_node_id_bytes(node).to_vec(),
                None,
                Some(position as u64),
                None,
                payload,
            ),
            treecrdt_core::OperationKind::Move {
                node,
                new_parent,
                position,
            } => (
                "move",
                None,
                sqlite_node_id_bytes(node).to_vec(),
                Some(sqlite_node_id_bytes(new_parent).to_vec()),
                Some(position as u64),
                None,
                None,
            ),
            treecrdt_core::OperationKind::Delete { node } => (
                "delete",
                None,
                sqlite_node_id_bytes(node).to_vec(),
                None,
                None,
                op.meta.known_state.clone(),
                None,
            ),
            treecrdt_core::OperationKind::Tombstone { node } => (
                "tombstone",
                None,
                sqlite_node_id_bytes(node).to_vec(),
                None,
                None,
                op.meta.known_state.clone(),
                None,
            ),
            treecrdt_core::OperationKind::Payload { node, payload } => (
                "payload",
                None,
                sqlite_node_id_bytes(node).to_vec(),
                None,
                None,
                None,
                payload,
            ),
        };

        let known_state_bytes = known_state.as_ref().map(vv_to_bytes).transpose()?;
        let op_ref = derive_op_ref_v0(doc_id, op.meta.id.replica.as_bytes(), op.meta.id.counter);

        let insert_sql = CString::new(
            "INSERT OR IGNORE INTO ops \
             (replica,counter,lamport,kind,parent,node,new_parent,position,known_state,payload,op_ref) \
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",
        )
        .expect("insert op sql");
        let mut stmt: *mut sqlite3_stmt = null_mut();
        let prep_rc = sqlite_prepare_v2(self.db, insert_sql.as_ptr(), -1, &mut stmt, null_mut());
        if prep_rc != SQLITE_OK as c_int {
            return Err(sqlite_rc_error(
                prep_rc,
                "sqlite_prepare_v2 insert op failed",
            ));
        }

        let mut bind_err = false;
        unsafe {
            bind_err |= sqlite_bind_blob(
                stmt,
                1,
                op.meta.id.replica.as_bytes().as_ptr() as *const c_void,
                op.meta.id.replica.as_bytes().len() as c_int,
                None,
            ) != SQLITE_OK as c_int;
            bind_err |= sqlite_bind_int64(stmt, 2, op.meta.id.counter as i64) != SQLITE_OK as c_int;
            bind_err |= sqlite_bind_int64(stmt, 3, op.meta.lamport as i64) != SQLITE_OK as c_int;
        }
        let kind_cstr = CString::new(kind).unwrap_or_else(|_| CString::new("insert").unwrap());
        unsafe {
            bind_err |=
                sqlite_bind_text(stmt, 4, kind_cstr.as_ptr(), -1, None) != SQLITE_OK as c_int;
        }
        unsafe {
            if let Some(ref p) = parent {
                bind_err |=
                    sqlite_bind_blob(stmt, 5, p.as_ptr() as *const c_void, p.len() as c_int, None)
                        != SQLITE_OK as c_int;
            } else {
                bind_err |= sqlite_bind_null(stmt, 5) != SQLITE_OK as c_int;
            }
            bind_err |= sqlite_bind_blob(
                stmt,
                6,
                node.as_ptr() as *const c_void,
                node.len() as c_int,
                None,
            ) != SQLITE_OK as c_int;
            if let Some(ref np) = new_parent {
                bind_err |= sqlite_bind_blob(
                    stmt,
                    7,
                    np.as_ptr() as *const c_void,
                    np.len() as c_int,
                    None,
                ) != SQLITE_OK as c_int;
            } else {
                bind_err |= sqlite_bind_null(stmt, 7) != SQLITE_OK as c_int;
            }
            if let Some(pos) = position {
                bind_err |= sqlite_bind_int64(stmt, 8, pos as i64) != SQLITE_OK as c_int;
            } else {
                bind_err |= sqlite_bind_null(stmt, 8) != SQLITE_OK as c_int;
            }
            if let Some(ref ks) = known_state_bytes {
                bind_err |= sqlite_bind_blob(
                    stmt,
                    9,
                    ks.as_ptr() as *const c_void,
                    ks.len() as c_int,
                    None,
                ) != SQLITE_OK as c_int;
            } else {
                bind_err |= sqlite_bind_null(stmt, 9) != SQLITE_OK as c_int;
            }
            if let Some(ref pl) = payload {
                bind_err |= sqlite_bind_blob(
                    stmt,
                    10,
                    pl.as_ptr() as *const c_void,
                    pl.len() as c_int,
                    None,
                ) != SQLITE_OK as c_int;
            } else {
                bind_err |= sqlite_bind_null(stmt, 10) != SQLITE_OK as c_int;
            }
            bind_err |= sqlite_bind_blob(
                stmt,
                11,
                op_ref.as_ptr() as *const c_void,
                OPREF_V0_WIDTH as c_int,
                None,
            ) != SQLITE_OK as c_int;
        }

        if bind_err {
            unsafe { sqlite_finalize(stmt) };
            return Err(sqlite_rc_error(
                SQLITE_ERROR as c_int,
                "bind insert op failed",
            ));
        }
        let step_rc = unsafe { sqlite_step(stmt) };
        let inserted = sqlite_changes(self.db) > 0;
        let finalize_rc = unsafe { sqlite_finalize(stmt) };
        if step_rc != SQLITE_DONE as c_int {
            return Err(sqlite_rc_error(step_rc, "insert op step failed"));
        }
        if finalize_rc != SQLITE_OK as c_int {
            return Err(sqlite_rc_error(finalize_rc, "finalize insert op failed"));
        }
        Ok(inserted)
    }

    fn load_since(&self, lamport: Lamport) -> treecrdt_core::Result<Vec<treecrdt_core::Operation>> {
        let sql = CString::new(
            "SELECT replica,counter,lamport,kind,parent,node,new_parent,position,known_state,payload \
             FROM ops \
             WHERE lamport > ?1 \
             ORDER BY lamport, replica, counter",
        )
        .expect("ops since sql");
        let mut stmt: *mut sqlite3_stmt = null_mut();
        let rc = sqlite_prepare_v2(self.db, sql.as_ptr(), -1, &mut stmt, null_mut());
        if rc != SQLITE_OK as c_int {
            return Err(sqlite_rc_error(rc, "sqlite_prepare_v2 ops since failed"));
        }
        let bind_rc = unsafe { sqlite_bind_int64(stmt, 1, lamport as i64) };
        if bind_rc != SQLITE_OK as c_int {
            unsafe { sqlite_finalize(stmt) };
            return Err(sqlite_rc_error(bind_rc, "bind ops since failed"));
        }

        let mut out: Vec<treecrdt_core::Operation> = Vec::new();
        loop {
            let step_rc = unsafe { sqlite_step(stmt) };
            if step_rc == SQLITE_ROW as c_int {
                let replica_ptr = unsafe { sqlite_column_blob(stmt, 0) } as *const u8;
                let replica_len = unsafe { sqlite_column_bytes(stmt, 0) } as usize;
                if replica_ptr.is_null() {
                    continue;
                }
                let replica = unsafe { slice::from_raw_parts(replica_ptr, replica_len) }.to_vec();
                let counter = unsafe { sqlite_column_int64(stmt, 1).max(0) as u64 };
                let lamport_val = unsafe { sqlite_column_int64(stmt, 2).max(0) as Lamport };

                let kind_ptr = unsafe { sqlite_column_text(stmt, 3) } as *const u8;
                let kind_len = unsafe { sqlite_column_bytes(stmt, 3) } as usize;
                let kind = if kind_ptr.is_null() {
                    ""
                } else {
                    std::str::from_utf8(unsafe { slice::from_raw_parts(kind_ptr, kind_len) })
                        .unwrap_or("")
                };

                let parent = unsafe { column_blob16(stmt, 4) }
                    .map_err(|rc| sqlite_rc_error(rc, "read parent failed"))?;
                let node = unsafe { column_blob16(stmt, 5) }
                    .map_err(|rc| sqlite_rc_error(rc, "read node failed"))?
                    .ok_or_else(|| sqlite_rc_error(SQLITE_ERROR as c_int, "node missing"))?;
                let new_parent = unsafe { column_blob16(stmt, 6) }
                    .map_err(|rc| sqlite_rc_error(rc, "read new_parent failed"))?;
                let position =
                    unsafe { column_int_opt(stmt, 7) }.map(|v| v.min(usize::MAX as u64) as usize);

                let known_state = if unsafe { sqlite_column_type(stmt, 8) } == SQLITE_NULL as c_int
                {
                    None
                } else {
                    let ptr = unsafe { sqlite_column_blob(stmt, 8) } as *const u8;
                    let len = unsafe { sqlite_column_bytes(stmt, 8) } as usize;
                    if ptr.is_null() || len == 0 {
                        None
                    } else {
                        Some(vv_from_bytes(unsafe { slice::from_raw_parts(ptr, len) })?)
                    }
                };

                let payload = if unsafe { sqlite_column_type(stmt, 9) } == SQLITE_NULL as c_int {
                    None
                } else {
                    let ptr = unsafe { sqlite_column_blob(stmt, 9) } as *const u8;
                    let len = unsafe { sqlite_column_bytes(stmt, 9) } as usize;
                    if ptr.is_null() {
                        None
                    } else {
                        Some(unsafe { slice::from_raw_parts(ptr, len) }.to_vec())
                    }
                };

                let op_kind = match kind {
                    "insert" => {
                        let parent = parent.ok_or_else(|| {
                            sqlite_rc_error(SQLITE_ERROR as c_int, "insert missing parent")
                        })?;
                        treecrdt_core::OperationKind::Insert {
                            parent: sqlite_bytes_to_node_id(parent),
                            node: sqlite_bytes_to_node_id(node),
                            position: position.unwrap_or(0),
                            payload,
                        }
                    }
                    "move" => {
                        let new_parent = new_parent.ok_or_else(|| {
                            sqlite_rc_error(SQLITE_ERROR as c_int, "move missing new_parent")
                        })?;
                        treecrdt_core::OperationKind::Move {
                            node: sqlite_bytes_to_node_id(node),
                            new_parent: sqlite_bytes_to_node_id(new_parent),
                            position: position.unwrap_or(0),
                        }
                    }
                    "delete" => treecrdt_core::OperationKind::Delete {
                        node: sqlite_bytes_to_node_id(node),
                    },
                    "tombstone" => treecrdt_core::OperationKind::Tombstone {
                        node: sqlite_bytes_to_node_id(node),
                    },
                    "payload" => treecrdt_core::OperationKind::Payload {
                        node: sqlite_bytes_to_node_id(node),
                        payload,
                    },
                    _ => {
                        unsafe { sqlite_finalize(stmt) };
                        return Err(sqlite_rc_error(SQLITE_ERROR as c_int, "unknown op kind"));
                    }
                };

                out.push(treecrdt_core::Operation {
                    meta: treecrdt_core::OperationMetadata {
                        id: treecrdt_core::OperationId {
                            replica: treecrdt_core::ReplicaId(replica),
                            counter,
                        },
                        lamport: lamport_val,
                        known_state,
                    },
                    kind: op_kind,
                });
            } else if step_rc == SQLITE_DONE as c_int {
                break;
            } else {
                unsafe { sqlite_finalize(stmt) };
                return Err(sqlite_rc_error(step_rc, "ops since step failed"));
            }
        }

        let finalize_rc = unsafe { sqlite_finalize(stmt) };
        if finalize_rc != SQLITE_OK as c_int {
            return Err(sqlite_rc_error(finalize_rc, "finalize ops since failed"));
        }
        Ok(out)
    }

    fn scan_since(
        &self,
        lamport: Lamport,
        visit: &mut dyn FnMut(treecrdt_core::Operation) -> treecrdt_core::Result<()>,
    ) -> treecrdt_core::Result<()> {
        let sql = CString::new(
            "SELECT replica,counter,lamport,kind,parent,node,new_parent,position,known_state,payload \
             FROM ops \
             WHERE lamport > ?1 \
             ORDER BY lamport, replica, counter",
        )
        .expect("ops since sql");
        let mut stmt: *mut sqlite3_stmt = null_mut();
        let rc = sqlite_prepare_v2(self.db, sql.as_ptr(), -1, &mut stmt, null_mut());
        if rc != SQLITE_OK as c_int {
            return Err(sqlite_rc_error(rc, "sqlite_prepare_v2 ops since failed"));
        }
        let bind_rc = unsafe { sqlite_bind_int64(stmt, 1, lamport as i64) };
        if bind_rc != SQLITE_OK as c_int {
            unsafe { sqlite_finalize(stmt) };
            return Err(sqlite_rc_error(bind_rc, "bind ops since failed"));
        }

        loop {
            let step_rc = unsafe { sqlite_step(stmt) };
            if step_rc == SQLITE_ROW as c_int {
                let replica_ptr = unsafe { sqlite_column_blob(stmt, 0) } as *const u8;
                let replica_len = unsafe { sqlite_column_bytes(stmt, 0) } as usize;
                if replica_ptr.is_null() {
                    continue;
                }
                let replica = unsafe { slice::from_raw_parts(replica_ptr, replica_len) }.to_vec();
                let counter = unsafe { sqlite_column_int64(stmt, 1).max(0) as u64 };
                let lamport_val = unsafe { sqlite_column_int64(stmt, 2).max(0) as Lamport };

                let kind_ptr = unsafe { sqlite_column_text(stmt, 3) } as *const u8;
                let kind_len = unsafe { sqlite_column_bytes(stmt, 3) } as usize;
                let kind = if kind_ptr.is_null() {
                    ""
                } else {
                    std::str::from_utf8(unsafe { slice::from_raw_parts(kind_ptr, kind_len) })
                        .unwrap_or("")
                };

                let parent = unsafe { column_blob16(stmt, 4) }
                    .map_err(|rc| sqlite_rc_error(rc, "read parent failed"))?;
                let node = unsafe { column_blob16(stmt, 5) }
                    .map_err(|rc| sqlite_rc_error(rc, "read node failed"))?
                    .ok_or_else(|| sqlite_rc_error(SQLITE_ERROR as c_int, "node missing"))?;
                let new_parent = unsafe { column_blob16(stmt, 6) }
                    .map_err(|rc| sqlite_rc_error(rc, "read new_parent failed"))?;
                let position =
                    unsafe { column_int_opt(stmt, 7) }.map(|v| v.min(usize::MAX as u64) as usize);

                let known_state = if unsafe { sqlite_column_type(stmt, 8) } == SQLITE_NULL as c_int
                {
                    None
                } else {
                    let ptr = unsafe { sqlite_column_blob(stmt, 8) } as *const u8;
                    let len = unsafe { sqlite_column_bytes(stmt, 8) } as usize;
                    if ptr.is_null() || len == 0 {
                        None
                    } else {
                        Some(vv_from_bytes(unsafe { slice::from_raw_parts(ptr, len) })?)
                    }
                };

                let payload = if unsafe { sqlite_column_type(stmt, 9) } == SQLITE_NULL as c_int {
                    None
                } else {
                    let ptr = unsafe { sqlite_column_blob(stmt, 9) } as *const u8;
                    let len = unsafe { sqlite_column_bytes(stmt, 9) } as usize;
                    if ptr.is_null() {
                        None
                    } else {
                        Some(unsafe { slice::from_raw_parts(ptr, len) }.to_vec())
                    }
                };

                let op_kind = match kind {
                    "insert" => {
                        let parent = parent.ok_or_else(|| {
                            sqlite_rc_error(SQLITE_ERROR as c_int, "insert missing parent")
                        })?;
                        treecrdt_core::OperationKind::Insert {
                            parent: sqlite_bytes_to_node_id(parent),
                            node: sqlite_bytes_to_node_id(node),
                            position: position.unwrap_or(0),
                            payload,
                        }
                    }
                    "move" => {
                        let new_parent = new_parent.ok_or_else(|| {
                            sqlite_rc_error(SQLITE_ERROR as c_int, "move missing new_parent")
                        })?;
                        treecrdt_core::OperationKind::Move {
                            node: sqlite_bytes_to_node_id(node),
                            new_parent: sqlite_bytes_to_node_id(new_parent),
                            position: position.unwrap_or(0),
                        }
                    }
                    "delete" => treecrdt_core::OperationKind::Delete {
                        node: sqlite_bytes_to_node_id(node),
                    },
                    "tombstone" => treecrdt_core::OperationKind::Tombstone {
                        node: sqlite_bytes_to_node_id(node),
                    },
                    "payload" => treecrdt_core::OperationKind::Payload {
                        node: sqlite_bytes_to_node_id(node),
                        payload,
                    },
                    _ => {
                        unsafe { sqlite_finalize(stmt) };
                        return Err(sqlite_rc_error(SQLITE_ERROR as c_int, "unknown op kind"));
                    }
                };

                let op = treecrdt_core::Operation {
                    meta: treecrdt_core::OperationMetadata {
                        id: treecrdt_core::OperationId {
                            replica: treecrdt_core::ReplicaId(replica),
                            counter,
                        },
                        lamport: lamport_val,
                        known_state,
                    },
                    kind: op_kind,
                };

                if let Err(err) = visit(op) {
                    unsafe { sqlite_finalize(stmt) };
                    return Err(err);
                }
            } else if step_rc == SQLITE_DONE as c_int {
                break;
            } else {
                unsafe { sqlite_finalize(stmt) };
                return Err(sqlite_rc_error(step_rc, "ops since step failed"));
            }
        }

        let finalize_rc = unsafe { sqlite_finalize(stmt) };
        if finalize_rc != SQLITE_OK as c_int {
            return Err(sqlite_rc_error(finalize_rc, "finalize ops since failed"));
        }
        Ok(())
    }

    fn latest_lamport(&self) -> Lamport {
        let sql =
            CString::new("SELECT COALESCE(MAX(lamport), 0) FROM ops").expect("max lamport sql");
        let mut stmt: *mut sqlite3_stmt = null_mut();
        let rc = sqlite_prepare_v2(self.db, sql.as_ptr(), -1, &mut stmt, null_mut());
        if rc != SQLITE_OK as c_int {
            return 0;
        }
        let step_rc = unsafe { sqlite_step(stmt) };
        let val = if step_rc == SQLITE_ROW as c_int {
            unsafe { sqlite_column_int64(stmt, 0).max(0) as Lamport }
        } else {
            0
        };
        unsafe { sqlite_finalize(stmt) };
        val
    }
}

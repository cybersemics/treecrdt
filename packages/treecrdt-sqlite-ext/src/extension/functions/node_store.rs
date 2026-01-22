use super::*;

fn sqlite_node_id_bytes(node: NodeId) -> [u8; 16] {
    node.0.to_be_bytes()
}

fn sqlite_bytes_to_node_id(bytes: [u8; 16]) -> NodeId {
    NodeId(u128::from_be_bytes(bytes))
}

fn sqlite_rc_error(rc: c_int, context: &str) -> treecrdt_core::Error {
    treecrdt_core::Error::Storage(format!("{context} (rc={rc})"))
}

fn vv_to_bytes(vv: &VersionVector) -> treecrdt_core::Result<Vec<u8>> {
    serde_json::to_vec(vv).map_err(|e| treecrdt_core::Error::Storage(e.to_string()))
}

fn vv_from_bytes(bytes: &[u8]) -> treecrdt_core::Result<VersionVector> {
    serde_json::from_slice(bytes).map_err(|e| treecrdt_core::Error::Storage(e.to_string()))
}

pub(super) struct SqliteNodeStore {
    db: *mut sqlite3,
    ensure_node: *mut sqlite3_stmt,
    exists: *mut sqlite3_stmt,
    select_node: *mut sqlite3_stmt,
    select_tombstone: *mut sqlite3_stmt,
    select_children: *mut sqlite3_stmt,
    all_nodes: *mut sqlite3_stmt,
    shift_down: *mut sqlite3_stmt,
    shift_up: *mut sqlite3_stmt,
    max_pos: *mut sqlite3_stmt,
    clear_parent_pos: *mut sqlite3_stmt,
    set_parent_pos: *mut sqlite3_stmt,
    update_tombstone: *mut sqlite3_stmt,
    update_last_change: *mut sqlite3_stmt,
    update_deleted_at: *mut sqlite3_stmt,
}

impl SqliteNodeStore {
    pub(super) fn prepare(db: *mut sqlite3) -> treecrdt_core::Result<Self> {
        let ensure_node_sql = CString::new(
            "INSERT OR IGNORE INTO tree_nodes(node,parent,pos,tombstone) VALUES (?1,NULL,NULL,0)",
        )
        .expect("ensure node sql");
        let exists_sql =
            CString::new("SELECT 1 FROM tree_nodes WHERE node = ?1 LIMIT 1").expect("exists sql");
        let select_node_sql = CString::new(
            "SELECT parent,pos,last_change,deleted_at FROM tree_nodes WHERE node = ?1 LIMIT 1",
        )
        .expect("select node sql");
        let select_tombstone_sql =
            CString::new("SELECT tombstone FROM tree_nodes WHERE node = ?1 LIMIT 1")
                .expect("select tombstone sql");
        let select_children_sql =
            CString::new("SELECT node FROM tree_nodes WHERE parent = ?1 ORDER BY pos")
                .expect("select children sql");
        let all_nodes_sql =
            CString::new("SELECT node FROM tree_nodes").expect("select all nodes sql");
        let shift_down_sql =
            CString::new("UPDATE tree_nodes SET pos = pos - 1 WHERE parent = ?1 AND pos > ?2")
                .expect("shift down sql");
        let shift_up_sql =
            CString::new("UPDATE tree_nodes SET pos = pos + 1 WHERE parent = ?1 AND pos >= ?2")
                .expect("shift up sql");
        let max_pos_sql =
            CString::new("SELECT COALESCE(MAX(pos) + 1, 0) FROM tree_nodes WHERE parent = ?1")
                .expect("max pos sql");
        let clear_parent_pos_sql =
            CString::new("UPDATE tree_nodes SET parent = NULL, pos = NULL WHERE node = ?1")
                .expect("clear parent pos sql");
        let set_parent_pos_sql =
            CString::new("UPDATE tree_nodes SET parent = ?2, pos = ?3 WHERE node = ?1")
                .expect("set parent pos sql");
        let update_tombstone_sql =
            CString::new("UPDATE tree_nodes SET tombstone = ?2 WHERE node = ?1")
                .expect("update tombstone sql");
        let update_last_change_sql =
            CString::new("UPDATE tree_nodes SET last_change = ?2 WHERE node = ?1")
                .expect("update last_change sql");
        let update_deleted_at_sql =
            CString::new("UPDATE tree_nodes SET deleted_at = ?2 WHERE node = ?1")
                .expect("update deleted_at sql");

        let mut ensure_node: *mut sqlite3_stmt = null_mut();
        let mut exists: *mut sqlite3_stmt = null_mut();
        let mut select_node: *mut sqlite3_stmt = null_mut();
        let mut select_tombstone: *mut sqlite3_stmt = null_mut();
        let mut select_children: *mut sqlite3_stmt = null_mut();
        let mut all_nodes: *mut sqlite3_stmt = null_mut();
        let mut shift_down: *mut sqlite3_stmt = null_mut();
        let mut shift_up: *mut sqlite3_stmt = null_mut();
        let mut max_pos: *mut sqlite3_stmt = null_mut();
        let mut clear_parent_pos: *mut sqlite3_stmt = null_mut();
        let mut set_parent_pos: *mut sqlite3_stmt = null_mut();
        let mut update_tombstone: *mut sqlite3_stmt = null_mut();
        let mut update_last_change: *mut sqlite3_stmt = null_mut();
        let mut update_deleted_at: *mut sqlite3_stmt = null_mut();

        let prep = |sql: &CString, stmt: &mut *mut sqlite3_stmt| -> treecrdt_core::Result<()> {
            let rc = sqlite_prepare_v2(db, sql.as_ptr(), -1, stmt, null_mut());
            if rc != SQLITE_OK as c_int {
                return Err(sqlite_rc_error(rc, "sqlite_prepare_v2 failed"));
            }
            Ok(())
        };

        prep(&ensure_node_sql, &mut ensure_node)?;
        prep(&exists_sql, &mut exists)?;
        prep(&select_node_sql, &mut select_node)?;
        prep(&select_tombstone_sql, &mut select_tombstone)?;
        prep(&select_children_sql, &mut select_children)?;
        prep(&all_nodes_sql, &mut all_nodes)?;
        prep(&shift_down_sql, &mut shift_down)?;
        prep(&shift_up_sql, &mut shift_up)?;
        prep(&max_pos_sql, &mut max_pos)?;
        prep(&clear_parent_pos_sql, &mut clear_parent_pos)?;
        prep(&set_parent_pos_sql, &mut set_parent_pos)?;
        prep(&update_tombstone_sql, &mut update_tombstone)?;
        prep(&update_last_change_sql, &mut update_last_change)?;
        prep(&update_deleted_at_sql, &mut update_deleted_at)?;

        Ok(Self {
            db,
            ensure_node,
            exists,
            select_node,
            select_tombstone,
            select_children,
            all_nodes,
            shift_down,
            shift_up,
            max_pos,
            clear_parent_pos,
            set_parent_pos,
            update_tombstone,
            update_last_change,
            update_deleted_at,
        })
    }
}

impl Drop for SqliteNodeStore {
    fn drop(&mut self) {
        unsafe {
            sqlite_finalize(self.ensure_node);
            sqlite_finalize(self.exists);
            sqlite_finalize(self.select_node);
            sqlite_finalize(self.select_tombstone);
            sqlite_finalize(self.select_children);
            sqlite_finalize(self.all_nodes);
            sqlite_finalize(self.shift_down);
            sqlite_finalize(self.shift_up);
            sqlite_finalize(self.max_pos);
            sqlite_finalize(self.clear_parent_pos);
            sqlite_finalize(self.set_parent_pos);
            sqlite_finalize(self.update_tombstone);
            sqlite_finalize(self.update_last_change);
            sqlite_finalize(self.update_deleted_at);
        }
    }
}

impl treecrdt_core::NodeStore for SqliteNodeStore {
    fn reset(&mut self) -> treecrdt_core::Result<()> {
        let clear_sql = CString::new("DELETE FROM tree_nodes").expect("clear nodes sql");
        let rc = sqlite_exec(self.db, clear_sql.as_ptr(), None, null_mut(), null_mut());
        if rc != SQLITE_OK as c_int {
            return Err(sqlite_rc_error(rc, "sqlite_exec reset tree_nodes failed"));
        }

        let root = sqlite_node_id_bytes(NodeId::ROOT);
        self.ensure_node(NodeId::ROOT)?;
        unsafe {
            sqlite_clear_bindings(self.set_parent_pos);
            sqlite_reset(self.set_parent_pos);
            sqlite_bind_blob(
                self.set_parent_pos,
                1,
                root.as_ptr() as *const c_void,
                root.len() as c_int,
                None,
            );
            sqlite_bind_null(self.set_parent_pos, 2);
            sqlite_bind_int64(self.set_parent_pos, 3, 0);
            let step_rc = sqlite_step(self.set_parent_pos);
            sqlite_reset(self.set_parent_pos);
            if step_rc != SQLITE_DONE as c_int {
                return Err(sqlite_rc_error(step_rc, "reset root row failed"));
            }
        }
        Ok(())
    }

    fn ensure_node(&mut self, node: NodeId) -> treecrdt_core::Result<()> {
        let bytes = sqlite_node_id_bytes(node);
        unsafe {
            sqlite_clear_bindings(self.ensure_node);
            sqlite_reset(self.ensure_node);
            let bind_rc = sqlite_bind_blob(
                self.ensure_node,
                1,
                bytes.as_ptr() as *const c_void,
                bytes.len() as c_int,
                None,
            );
            if bind_rc != SQLITE_OK as c_int {
                return Err(sqlite_rc_error(bind_rc, "bind ensure_node failed"));
            }
            let step_rc = sqlite_step(self.ensure_node);
            sqlite_reset(self.ensure_node);
            if step_rc != SQLITE_DONE as c_int {
                return Err(sqlite_rc_error(step_rc, "ensure_node step failed"));
            }
        }
        Ok(())
    }

    fn exists(&self, node: NodeId) -> treecrdt_core::Result<bool> {
        let bytes = sqlite_node_id_bytes(node);
        unsafe {
            sqlite_clear_bindings(self.exists);
            sqlite_reset(self.exists);
            let bind_rc = sqlite_bind_blob(
                self.exists,
                1,
                bytes.as_ptr() as *const c_void,
                bytes.len() as c_int,
                None,
            );
            if bind_rc != SQLITE_OK as c_int {
                return Err(sqlite_rc_error(bind_rc, "bind exists failed"));
            }
            let step_rc = sqlite_step(self.exists);
            sqlite_reset(self.exists);
            match step_rc {
                rc if rc == SQLITE_ROW as c_int => Ok(true),
                rc if rc == SQLITE_DONE as c_int => Ok(false),
                rc => Err(sqlite_rc_error(rc, "exists step failed")),
            }
        }
    }

    fn parent(&self, node: NodeId) -> treecrdt_core::Result<Option<NodeId>> {
        let bytes = sqlite_node_id_bytes(node);
        unsafe {
            sqlite_clear_bindings(self.select_node);
            sqlite_reset(self.select_node);
            let bind_rc = sqlite_bind_blob(
                self.select_node,
                1,
                bytes.as_ptr() as *const c_void,
                bytes.len() as c_int,
                None,
            );
            if bind_rc != SQLITE_OK as c_int {
                return Err(sqlite_rc_error(bind_rc, "bind select_node failed"));
            }
            let step_rc = sqlite_step(self.select_node);
            let parent = if step_rc == SQLITE_ROW as c_int {
                match column_blob16(self.select_node, 0) {
                    Ok(Some(p)) => Some(sqlite_bytes_to_node_id(p)),
                    Ok(None) => None,
                    Err(rc) => return Err(sqlite_rc_error(rc, "read parent failed")),
                }
            } else if step_rc == SQLITE_DONE as c_int {
                None
            } else {
                return Err(sqlite_rc_error(step_rc, "select_node step failed"));
            };
            sqlite_reset(self.select_node);
            Ok(parent)
        }
    }

    fn children(&self, parent: NodeId) -> treecrdt_core::Result<Vec<NodeId>> {
        if parent == NodeId::TRASH {
            return Ok(Vec::new());
        }
        let parent_bytes = sqlite_node_id_bytes(parent);
        let mut out = Vec::new();
        unsafe {
            sqlite_clear_bindings(self.select_children);
            sqlite_reset(self.select_children);
            let bind_rc = sqlite_bind_blob(
                self.select_children,
                1,
                parent_bytes.as_ptr() as *const c_void,
                parent_bytes.len() as c_int,
                None,
            );
            if bind_rc != SQLITE_OK as c_int {
                return Err(sqlite_rc_error(bind_rc, "bind select_children failed"));
            }
            loop {
                let step_rc = sqlite_step(self.select_children);
                if step_rc == SQLITE_ROW as c_int {
                    match column_blob16(self.select_children, 0) {
                        Ok(Some(id)) => out.push(sqlite_bytes_to_node_id(id)),
                        Ok(None) => {}
                        Err(rc) => return Err(sqlite_rc_error(rc, "read child id failed")),
                    }
                } else if step_rc == SQLITE_DONE as c_int {
                    break;
                } else {
                    return Err(sqlite_rc_error(step_rc, "select_children step failed"));
                }
            }
            sqlite_reset(self.select_children);
        }
        Ok(out)
    }

    fn detach(&mut self, node: NodeId) -> treecrdt_core::Result<()> {
        if node == NodeId::ROOT {
            return Ok(());
        }
        self.ensure_node(node)?;
        let bytes = sqlite_node_id_bytes(node);

        let (parent, pos) = unsafe {
            sqlite_clear_bindings(self.select_node);
            sqlite_reset(self.select_node);
            let bind_rc = sqlite_bind_blob(
                self.select_node,
                1,
                bytes.as_ptr() as *const c_void,
                bytes.len() as c_int,
                None,
            );
            if bind_rc != SQLITE_OK as c_int {
                return Err(sqlite_rc_error(bind_rc, "bind select_node failed"));
            }
            let step_rc = sqlite_step(self.select_node);
            if step_rc != SQLITE_ROW as c_int {
                sqlite_reset(self.select_node);
                return Ok(());
            }
            let parent = column_blob16(self.select_node, 0)
                .map_err(|rc| sqlite_rc_error(rc, "read parent failed"))?;
            let pos = column_int_opt(self.select_node, 1).map(|v| v as usize);
            sqlite_reset(self.select_node);
            (parent, pos)
        };

        if let (Some(parent_bytes), Some(pos)) = (parent, pos) {
            let parent_id = sqlite_bytes_to_node_id(parent_bytes);
            if parent_id != NodeId::TRASH {
                unsafe {
                    sqlite_clear_bindings(self.shift_down);
                    sqlite_reset(self.shift_down);
                    let bind_parent = sqlite_bind_blob(
                        self.shift_down,
                        1,
                        parent_bytes.as_ptr() as *const c_void,
                        parent_bytes.len() as c_int,
                        None,
                    );
                    if bind_parent != SQLITE_OK as c_int {
                        return Err(sqlite_rc_error(
                            bind_parent,
                            "bind shift_down parent failed",
                        ));
                    }
                    let bind_pos = sqlite_bind_int64(self.shift_down, 2, pos as i64);
                    if bind_pos != SQLITE_OK as c_int {
                        return Err(sqlite_rc_error(bind_pos, "bind shift_down pos failed"));
                    }
                    let step_rc = sqlite_step(self.shift_down);
                    sqlite_reset(self.shift_down);
                    if step_rc != SQLITE_DONE as c_int {
                        return Err(sqlite_rc_error(step_rc, "shift_down step failed"));
                    }
                }
            }
        }

        unsafe {
            sqlite_clear_bindings(self.clear_parent_pos);
            sqlite_reset(self.clear_parent_pos);
            let bind_rc = sqlite_bind_blob(
                self.clear_parent_pos,
                1,
                bytes.as_ptr() as *const c_void,
                bytes.len() as c_int,
                None,
            );
            if bind_rc != SQLITE_OK as c_int {
                return Err(sqlite_rc_error(bind_rc, "bind clear_parent_pos failed"));
            }
            let step_rc = sqlite_step(self.clear_parent_pos);
            sqlite_reset(self.clear_parent_pos);
            if step_rc != SQLITE_DONE as c_int {
                return Err(sqlite_rc_error(step_rc, "clear_parent_pos step failed"));
            }
        }

        Ok(())
    }

    fn attach(
        &mut self,
        node: NodeId,
        parent: NodeId,
        position: usize,
    ) -> treecrdt_core::Result<()> {
        if node == NodeId::ROOT {
            return Ok(());
        }
        self.ensure_node(node)?;
        self.ensure_node(parent)?;

        let node_bytes = sqlite_node_id_bytes(node);
        let parent_bytes = sqlite_node_id_bytes(parent);

        if parent == NodeId::TRASH {
            unsafe {
                sqlite_clear_bindings(self.set_parent_pos);
                sqlite_reset(self.set_parent_pos);
                sqlite_bind_blob(
                    self.set_parent_pos,
                    1,
                    node_bytes.as_ptr() as *const c_void,
                    node_bytes.len() as c_int,
                    None,
                );
                sqlite_bind_blob(
                    self.set_parent_pos,
                    2,
                    parent_bytes.as_ptr() as *const c_void,
                    parent_bytes.len() as c_int,
                    None,
                );
                sqlite_bind_null(self.set_parent_pos, 3);
                let step_rc = sqlite_step(self.set_parent_pos);
                sqlite_reset(self.set_parent_pos);
                if step_rc != SQLITE_DONE as c_int {
                    return Err(sqlite_rc_error(step_rc, "attach to trash failed"));
                }
            }
            return Ok(());
        }

        let max_pos = unsafe {
            sqlite_clear_bindings(self.max_pos);
            sqlite_reset(self.max_pos);
            let bind_rc = sqlite_bind_blob(
                self.max_pos,
                1,
                parent_bytes.as_ptr() as *const c_void,
                parent_bytes.len() as c_int,
                None,
            );
            if bind_rc != SQLITE_OK as c_int {
                return Err(sqlite_rc_error(bind_rc, "bind max_pos failed"));
            }
            let step_rc = sqlite_step(self.max_pos);
            if step_rc != SQLITE_ROW as c_int {
                sqlite_reset(self.max_pos);
                return Err(sqlite_rc_error(step_rc, "max_pos step failed"));
            }
            let val = sqlite_column_int64(self.max_pos, 0) as usize;
            sqlite_reset(self.max_pos);
            val
        };

        let pos = position.min(max_pos);

        unsafe {
            sqlite_clear_bindings(self.shift_up);
            sqlite_reset(self.shift_up);
            let bind_parent = sqlite_bind_blob(
                self.shift_up,
                1,
                parent_bytes.as_ptr() as *const c_void,
                parent_bytes.len() as c_int,
                None,
            );
            if bind_parent != SQLITE_OK as c_int {
                return Err(sqlite_rc_error(bind_parent, "bind shift_up parent failed"));
            }
            let bind_pos = sqlite_bind_int64(self.shift_up, 2, pos as i64);
            if bind_pos != SQLITE_OK as c_int {
                return Err(sqlite_rc_error(bind_pos, "bind shift_up pos failed"));
            }
            let step_rc = sqlite_step(self.shift_up);
            sqlite_reset(self.shift_up);
            if step_rc != SQLITE_DONE as c_int {
                return Err(sqlite_rc_error(step_rc, "shift_up step failed"));
            }
        }

        unsafe {
            sqlite_clear_bindings(self.set_parent_pos);
            sqlite_reset(self.set_parent_pos);
            let bind_node = sqlite_bind_blob(
                self.set_parent_pos,
                1,
                node_bytes.as_ptr() as *const c_void,
                node_bytes.len() as c_int,
                None,
            );
            if bind_node != SQLITE_OK as c_int {
                return Err(sqlite_rc_error(
                    bind_node,
                    "bind set_parent_pos node failed",
                ));
            }
            let bind_parent = sqlite_bind_blob(
                self.set_parent_pos,
                2,
                parent_bytes.as_ptr() as *const c_void,
                parent_bytes.len() as c_int,
                None,
            );
            if bind_parent != SQLITE_OK as c_int {
                return Err(sqlite_rc_error(
                    bind_parent,
                    "bind set_parent_pos parent failed",
                ));
            }
            let bind_pos = sqlite_bind_int64(self.set_parent_pos, 3, pos as i64);
            if bind_pos != SQLITE_OK as c_int {
                return Err(sqlite_rc_error(bind_pos, "bind set_parent_pos pos failed"));
            }
            let step_rc = sqlite_step(self.set_parent_pos);
            sqlite_reset(self.set_parent_pos);
            if step_rc != SQLITE_DONE as c_int {
                return Err(sqlite_rc_error(step_rc, "set_parent_pos step failed"));
            }
        }

        Ok(())
    }

    fn tombstone(&self, node: NodeId) -> treecrdt_core::Result<bool> {
        let bytes = sqlite_node_id_bytes(node);
        unsafe {
            sqlite_clear_bindings(self.select_tombstone);
            sqlite_reset(self.select_tombstone);
            let bind_rc = sqlite_bind_blob(
                self.select_tombstone,
                1,
                bytes.as_ptr() as *const c_void,
                bytes.len() as c_int,
                None,
            );
            if bind_rc != SQLITE_OK as c_int {
                sqlite_reset(self.select_tombstone);
                return Err(sqlite_rc_error(bind_rc, "bind select_tombstone failed"));
            }

            let step_rc = sqlite_step(self.select_tombstone);
            let out = if step_rc == SQLITE_ROW as c_int {
                sqlite_column_int64(self.select_tombstone, 0) != 0
            } else if step_rc == SQLITE_DONE as c_int {
                false
            } else {
                sqlite_reset(self.select_tombstone);
                return Err(sqlite_rc_error(step_rc, "select_tombstone step failed"));
            };
            sqlite_reset(self.select_tombstone);
            Ok(out)
        }
    }

    fn set_tombstone(&mut self, node: NodeId, tombstone: bool) -> treecrdt_core::Result<()> {
        self.ensure_node(node)?;
        let bytes = sqlite_node_id_bytes(node);
        unsafe {
            sqlite_clear_bindings(self.update_tombstone);
            sqlite_reset(self.update_tombstone);
            let mut bind_err = false;
            bind_err |= sqlite_bind_blob(
                self.update_tombstone,
                1,
                bytes.as_ptr() as *const c_void,
                bytes.len() as c_int,
                None,
            ) != SQLITE_OK as c_int;
            bind_err |= sqlite_bind_int64(
                self.update_tombstone,
                2,
                if tombstone { 1 } else { 0 },
            ) != SQLITE_OK as c_int;
            if bind_err {
                sqlite_reset(self.update_tombstone);
                return Err(sqlite_rc_error(
                    SQLITE_ERROR as c_int,
                    "bind update_tombstone failed",
                ));
            }
            let step_rc = sqlite_step(self.update_tombstone);
            sqlite_reset(self.update_tombstone);
            if step_rc != SQLITE_DONE as c_int {
                return Err(sqlite_rc_error(step_rc, "update_tombstone step failed"));
            }
        }
        Ok(())
    }

    fn has_deleted_at(&self, node: NodeId) -> treecrdt_core::Result<bool> {
        let bytes = sqlite_node_id_bytes(node);
        unsafe {
            sqlite_clear_bindings(self.select_node);
            sqlite_reset(self.select_node);
            let bind_rc = sqlite_bind_blob(
                self.select_node,
                1,
                bytes.as_ptr() as *const c_void,
                bytes.len() as c_int,
                None,
            );
            if bind_rc != SQLITE_OK as c_int {
                sqlite_reset(self.select_node);
                return Err(sqlite_rc_error(bind_rc, "bind select_node failed"));
            }

            let step_rc = sqlite_step(self.select_node);
            let has = if step_rc == SQLITE_ROW as c_int {
                sqlite_column_type(self.select_node, 3) != SQLITE_NULL as c_int
                    && sqlite_column_bytes(self.select_node, 3) > 0
            } else if step_rc == SQLITE_DONE as c_int {
                false
            } else {
                sqlite_reset(self.select_node);
                return Err(sqlite_rc_error(step_rc, "select_node step failed"));
            };
            sqlite_reset(self.select_node);
            Ok(has)
        }
    }

    fn parent_and_has_deleted_at(
        &self,
        node: NodeId,
    ) -> treecrdt_core::Result<Option<(Option<NodeId>, bool)>> {
        let bytes = sqlite_node_id_bytes(node);
        unsafe {
            sqlite_clear_bindings(self.select_node);
            sqlite_reset(self.select_node);
            let bind_rc = sqlite_bind_blob(
                self.select_node,
                1,
                bytes.as_ptr() as *const c_void,
                bytes.len() as c_int,
                None,
            );
            if bind_rc != SQLITE_OK as c_int {
                sqlite_reset(self.select_node);
                return Err(sqlite_rc_error(bind_rc, "bind select_node failed"));
            }

            let step_rc = sqlite_step(self.select_node);
            let out = if step_rc == SQLITE_ROW as c_int {
                let parent = match column_blob16(self.select_node, 0) {
                    Ok(Some(p)) => Some(sqlite_bytes_to_node_id(p)),
                    Ok(None) => None,
                    Err(rc) => {
                        sqlite_reset(self.select_node);
                        return Err(sqlite_rc_error(rc, "read parent failed"));
                    }
                };
                let has_deleted_at = sqlite_column_type(self.select_node, 3) != SQLITE_NULL as c_int
                    && sqlite_column_bytes(self.select_node, 3) > 0;
                Some((parent, has_deleted_at))
            } else if step_rc == SQLITE_DONE as c_int {
                None
            } else {
                sqlite_reset(self.select_node);
                return Err(sqlite_rc_error(step_rc, "select_node step failed"));
            };
            sqlite_reset(self.select_node);
            Ok(out)
        }
    }

    fn last_change(&self, node: NodeId) -> treecrdt_core::Result<VersionVector> {
        let bytes = sqlite_node_id_bytes(node);
        unsafe {
            sqlite_clear_bindings(self.select_node);
            sqlite_reset(self.select_node);
            sqlite_bind_blob(
                self.select_node,
                1,
                bytes.as_ptr() as *const c_void,
                bytes.len() as c_int,
                None,
            );
            let step_rc = sqlite_step(self.select_node);
            if step_rc != SQLITE_ROW as c_int {
                sqlite_reset(self.select_node);
                return Err(sqlite_rc_error(step_rc, "select_node last_change failed"));
            }
            let vv = if sqlite_column_type(self.select_node, 2) == SQLITE_NULL as c_int {
                VersionVector::new()
            } else {
                let ptr = sqlite_column_blob(self.select_node, 2) as *const u8;
                let len = sqlite_column_bytes(self.select_node, 2) as usize;
                if ptr.is_null() || len == 0 {
                    VersionVector::new()
                } else {
                    vv_from_bytes(slice::from_raw_parts(ptr, len))?
                }
            };
            sqlite_reset(self.select_node);
            Ok(vv)
        }
    }

    fn merge_last_change(
        &mut self,
        node: NodeId,
        delta: &VersionVector,
    ) -> treecrdt_core::Result<()> {
        self.ensure_node(node)?;
        let mut vv = self.last_change(node)?;
        vv.merge(delta);
        let bytes = vv_to_bytes(&vv)?;

        let node_bytes = sqlite_node_id_bytes(node);
        unsafe {
            sqlite_clear_bindings(self.update_last_change);
            sqlite_reset(self.update_last_change);
            sqlite_bind_blob(
                self.update_last_change,
                1,
                node_bytes.as_ptr() as *const c_void,
                node_bytes.len() as c_int,
                None,
            );
            sqlite_bind_blob(
                self.update_last_change,
                2,
                bytes.as_ptr() as *const c_void,
                bytes.len() as c_int,
                None,
            );
            let step_rc = sqlite_step(self.update_last_change);
            sqlite_reset(self.update_last_change);
            if step_rc != SQLITE_DONE as c_int {
                return Err(sqlite_rc_error(step_rc, "update_last_change failed"));
            }
        }

        Ok(())
    }

    fn deleted_at(&self, node: NodeId) -> treecrdt_core::Result<Option<VersionVector>> {
        let bytes = sqlite_node_id_bytes(node);
        unsafe {
            sqlite_clear_bindings(self.select_node);
            sqlite_reset(self.select_node);
            sqlite_bind_blob(
                self.select_node,
                1,
                bytes.as_ptr() as *const c_void,
                bytes.len() as c_int,
                None,
            );
            let step_rc = sqlite_step(self.select_node);
            if step_rc != SQLITE_ROW as c_int {
                sqlite_reset(self.select_node);
                return Ok(None);
            }
            let vv = if sqlite_column_type(self.select_node, 3) == SQLITE_NULL as c_int {
                None
            } else {
                let ptr = sqlite_column_blob(self.select_node, 3) as *const u8;
                let len = sqlite_column_bytes(self.select_node, 3) as usize;
                if ptr.is_null() || len == 0 {
                    None
                } else {
                    Some(vv_from_bytes(slice::from_raw_parts(ptr, len))?)
                }
            };
            sqlite_reset(self.select_node);
            Ok(vv)
        }
    }

    fn merge_deleted_at(
        &mut self,
        node: NodeId,
        delta: &VersionVector,
    ) -> treecrdt_core::Result<()> {
        self.ensure_node(node)?;
        let mut vv = self.deleted_at(node)?.unwrap_or_else(VersionVector::new);
        vv.merge(delta);
        let bytes = vv_to_bytes(&vv)?;

        let node_bytes = sqlite_node_id_bytes(node);
        unsafe {
            sqlite_clear_bindings(self.update_deleted_at);
            sqlite_reset(self.update_deleted_at);
            sqlite_bind_blob(
                self.update_deleted_at,
                1,
                node_bytes.as_ptr() as *const c_void,
                node_bytes.len() as c_int,
                None,
            );
            sqlite_bind_blob(
                self.update_deleted_at,
                2,
                bytes.as_ptr() as *const c_void,
                bytes.len() as c_int,
                None,
            );
            let step_rc = sqlite_step(self.update_deleted_at);
            sqlite_reset(self.update_deleted_at);
            if step_rc != SQLITE_DONE as c_int {
                return Err(sqlite_rc_error(step_rc, "update_deleted_at failed"));
            }
        }

        Ok(())
    }

    fn all_nodes(&self) -> treecrdt_core::Result<Vec<NodeId>> {
        let mut out = Vec::new();
        unsafe {
            sqlite_clear_bindings(self.all_nodes);
            sqlite_reset(self.all_nodes);
            loop {
                let step_rc = sqlite_step(self.all_nodes);
                if step_rc == SQLITE_ROW as c_int {
                    match column_blob16(self.all_nodes, 0) {
                        Ok(Some(id)) => out.push(sqlite_bytes_to_node_id(id)),
                        Ok(None) => {}
                        Err(rc) => return Err(sqlite_rc_error(rc, "read node id failed")),
                    }
                } else if step_rc == SQLITE_DONE as c_int {
                    break;
                } else {
                    return Err(sqlite_rc_error(step_rc, "all_nodes step failed"));
                }
            }
            sqlite_reset(self.all_nodes);
        }
        Ok(out)
    }
}

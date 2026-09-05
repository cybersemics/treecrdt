use super::statement::LazyStatement;
use super::*;
use std::slice;
use treecrdt_core::NodeStore;

fn sqlite_node_id_bytes(node: NodeId) -> [u8; 16] {
    node.0.to_be_bytes()
}

fn sqlite_bytes_to_node_id(bytes: [u8; 16]) -> NodeId {
    NodeId(u128::from_be_bytes(bytes))
}

fn sqlite_rc_error(rc: c_int, context: &str) -> treecrdt_core::Error {
    treecrdt_core::Error::Storage(format!("{context} (rc={rc})"))
}

fn read_optional_version_vector_v0_column(
    stmt: *mut sqlite3_stmt,
    column: c_int,
    context: &str,
) -> treecrdt_core::Result<Option<VersionVector>> {
    unsafe {
        if sqlite_column_type(stmt, column) == SQLITE_NULL as c_int {
            return Ok(None);
        }

        let ptr = sqlite_column_blob(stmt, column) as *const u8;
        let len = sqlite_column_bytes(stmt, column) as usize;
        let bytes = if len == 0 {
            &[]
        } else if ptr.is_null() {
            return Err(sqlite_rc_error(
                SQLITE_ERROR as c_int,
                &format!("{context} blob pointer is null"),
            ));
        } else {
            slice::from_raw_parts(ptr, len)
        };
        VersionVector::decode_v0(bytes).map(Some)
    }
}

pub(super) struct SqliteNodeStore {
    db: *mut sqlite3,
    ensure_node: LazyStatement,
    exists: LazyStatement,
    select_node: LazyStatement,
    select_tombstone: LazyStatement,
    select_children: LazyStatement,
    all_nodes: LazyStatement,
    clear_parent_order_key: LazyStatement,
    set_parent_order_key: LazyStatement,
    update_tombstone: LazyStatement,
    update_last_change: LazyStatement,
    update_deleted_at: LazyStatement,
}

impl SqliteNodeStore {
    pub(super) fn prepare(db: *mut sqlite3) -> treecrdt_core::Result<Self> {
        Ok(Self {
            db,
            ensure_node: LazyStatement::new(
                db,
                c"INSERT OR IGNORE INTO tree_nodes(node,parent,order_key,tombstone) VALUES (?1,NULL,NULL,0)",
            ),
            exists: LazyStatement::new(
                db,
                c"SELECT 1 FROM tree_nodes WHERE node = ?1 LIMIT 1",
            ),
            select_node: LazyStatement::new(
                db,
                c"SELECT parent,order_key,last_change,deleted_at FROM tree_nodes WHERE node = ?1 LIMIT 1",
            ),
            select_tombstone: LazyStatement::new(
                db,
                c"SELECT tombstone FROM tree_nodes WHERE node = ?1 LIMIT 1",
            ),
            select_children: LazyStatement::new(
                db,
                c"SELECT node FROM tree_nodes WHERE parent = ?1 ORDER BY order_key, node",
            ),
            all_nodes: LazyStatement::new(db, c"SELECT node FROM tree_nodes"),
            clear_parent_order_key: LazyStatement::new(
                db,
                c"UPDATE tree_nodes SET parent = NULL, order_key = NULL WHERE node = ?1",
            ),
            set_parent_order_key: LazyStatement::new(
                db,
                c"UPDATE tree_nodes SET parent = ?2, order_key = ?3 WHERE node = ?1",
            ),
            update_tombstone: LazyStatement::new(
                db,
                c"UPDATE tree_nodes SET tombstone = ?2 WHERE node = ?1",
            ),
            update_last_change: LazyStatement::new(
                db,
                c"UPDATE tree_nodes SET last_change = ?2 WHERE node = ?1",
            ),
            update_deleted_at: LazyStatement::new(
                db,
                c"UPDATE tree_nodes SET deleted_at = ?2 WHERE node = ?1",
            ),
        })
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
        let stmt = self.set_parent_order_key.get()?;
        unsafe {
            sqlite_clear_bindings(stmt);
            sqlite_reset(stmt);
            sqlite_bind_blob(
                stmt,
                1,
                root.as_ptr() as *const c_void,
                root.len() as c_int,
                None,
            );
            sqlite_bind_null(stmt, 2);
            let empty: [u8; 0] = [];
            sqlite_bind_blob(stmt, 3, empty.as_ptr() as *const c_void, 0, None);
            let step_rc = sqlite_step(stmt);
            sqlite_reset(stmt);
            if step_rc != SQLITE_DONE as c_int {
                return Err(sqlite_rc_error(step_rc, "reset root row failed"));
            }
        }
        Ok(())
    }

    fn ensure_node(&mut self, node: NodeId) -> treecrdt_core::Result<()> {
        let bytes = sqlite_node_id_bytes(node);
        let stmt = self.ensure_node.get()?;
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
                return Err(sqlite_rc_error(bind_rc, "bind ensure_node failed"));
            }
            let step_rc = sqlite_step(stmt);
            sqlite_reset(stmt);
            if step_rc != SQLITE_DONE as c_int {
                return Err(sqlite_rc_error(step_rc, "ensure_node step failed"));
            }
        }
        Ok(())
    }

    fn exists(&self, node: NodeId) -> treecrdt_core::Result<bool> {
        let bytes = sqlite_node_id_bytes(node);
        let stmt = self.exists.get()?;
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
                return Err(sqlite_rc_error(bind_rc, "bind exists failed"));
            }
            let step_rc = sqlite_step(stmt);
            sqlite_reset(stmt);
            match step_rc {
                rc if rc == SQLITE_ROW as c_int => Ok(true),
                rc if rc == SQLITE_DONE as c_int => Ok(false),
                rc => Err(sqlite_rc_error(rc, "exists step failed")),
            }
        }
    }

    fn parent(&self, node: NodeId) -> treecrdt_core::Result<Option<NodeId>> {
        let bytes = sqlite_node_id_bytes(node);
        let stmt = self.select_node.get()?;
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
                return Err(sqlite_rc_error(bind_rc, "bind select_node failed"));
            }
            let step_rc = sqlite_step(stmt);
            let parent = if step_rc == SQLITE_ROW as c_int {
                match column_blob16(stmt, 0) {
                    Ok(Some(p)) => Some(sqlite_bytes_to_node_id(p)),
                    Ok(None) => None,
                    Err(rc) => return Err(sqlite_rc_error(rc, "read parent failed")),
                }
            } else if step_rc == SQLITE_DONE as c_int {
                None
            } else {
                return Err(sqlite_rc_error(step_rc, "select_node step failed"));
            };
            sqlite_reset(stmt);
            Ok(parent)
        }
    }

    fn order_key(&self, node: NodeId) -> treecrdt_core::Result<Option<Vec<u8>>> {
        let bytes = sqlite_node_id_bytes(node);
        let stmt = self.select_node.get()?;
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
                return Err(sqlite_rc_error(bind_rc, "bind select_node failed"));
            }

            let step_rc = sqlite_step(stmt);
            let out = if step_rc == SQLITE_ROW as c_int {
                if sqlite_column_type(stmt, 1) == SQLITE_NULL as c_int {
                    None
                } else {
                    let ptr = sqlite_column_blob(stmt, 1) as *const u8;
                    let len = sqlite_column_bytes(stmt, 1) as usize;
                    if ptr.is_null() {
                        None
                    } else {
                        Some(slice::from_raw_parts(ptr, len).to_vec())
                    }
                }
            } else if step_rc == SQLITE_DONE as c_int {
                None
            } else {
                return Err(sqlite_rc_error(step_rc, "select_node step failed"));
            };

            sqlite_reset(stmt);
            Ok(out)
        }
    }

    fn children(&self, parent: NodeId) -> treecrdt_core::Result<Vec<NodeId>> {
        if parent == NodeId::TRASH {
            return Ok(Vec::new());
        }
        let parent_bytes = sqlite_node_id_bytes(parent);
        let mut out = Vec::new();
        let stmt = self.select_children.get()?;
        unsafe {
            sqlite_clear_bindings(stmt);
            sqlite_reset(stmt);
            let bind_rc = sqlite_bind_blob(
                stmt,
                1,
                parent_bytes.as_ptr() as *const c_void,
                parent_bytes.len() as c_int,
                None,
            );
            if bind_rc != SQLITE_OK as c_int {
                return Err(sqlite_rc_error(bind_rc, "bind select_children failed"));
            }
            loop {
                let step_rc = sqlite_step(stmt);
                if step_rc == SQLITE_ROW as c_int {
                    match column_blob16(stmt, 0) {
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
            sqlite_reset(stmt);
        }
        Ok(out)
    }

    fn detach(&mut self, node: NodeId) -> treecrdt_core::Result<()> {
        if node == NodeId::ROOT {
            return Ok(());
        }
        self.ensure_node(node)?;
        let bytes = sqlite_node_id_bytes(node);
        let stmt = self.clear_parent_order_key.get()?;

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
                return Err(sqlite_rc_error(
                    bind_rc,
                    "bind clear_parent_order_key failed",
                ));
            }
            let step_rc = sqlite_step(stmt);
            sqlite_reset(stmt);
            if step_rc != SQLITE_DONE as c_int {
                return Err(sqlite_rc_error(
                    step_rc,
                    "clear_parent_order_key step failed",
                ));
            }
        }

        Ok(())
    }

    fn attach(
        &mut self,
        node: NodeId,
        parent: NodeId,
        order_key: Vec<u8>,
    ) -> treecrdt_core::Result<()> {
        if node == NodeId::ROOT {
            return Ok(());
        }
        self.ensure_node(node)?;
        self.ensure_node(parent)?;

        let node_bytes = sqlite_node_id_bytes(node);
        let parent_bytes = sqlite_node_id_bytes(parent);
        let stmt = self.set_parent_order_key.get()?;

        if parent == NodeId::TRASH {
            unsafe {
                sqlite_clear_bindings(stmt);
                sqlite_reset(stmt);
                sqlite_bind_blob(
                    stmt,
                    1,
                    node_bytes.as_ptr() as *const c_void,
                    node_bytes.len() as c_int,
                    None,
                );
                sqlite_bind_blob(
                    stmt,
                    2,
                    parent_bytes.as_ptr() as *const c_void,
                    parent_bytes.len() as c_int,
                    None,
                );
                sqlite_bind_null(stmt, 3);
                let step_rc = sqlite_step(stmt);
                sqlite_reset(stmt);
                if step_rc != SQLITE_DONE as c_int {
                    return Err(sqlite_rc_error(step_rc, "attach to trash failed"));
                }
            }
            return Ok(());
        }

        unsafe {
            sqlite_clear_bindings(stmt);
            sqlite_reset(stmt);
            let bind_node = sqlite_bind_blob(
                stmt,
                1,
                node_bytes.as_ptr() as *const c_void,
                node_bytes.len() as c_int,
                None,
            );
            if bind_node != SQLITE_OK as c_int {
                return Err(sqlite_rc_error(
                    bind_node,
                    "bind set_parent_order_key node failed",
                ));
            }
            let bind_parent = sqlite_bind_blob(
                stmt,
                2,
                parent_bytes.as_ptr() as *const c_void,
                parent_bytes.len() as c_int,
                None,
            );
            if bind_parent != SQLITE_OK as c_int {
                return Err(sqlite_rc_error(
                    bind_parent,
                    "bind set_parent_order_key parent failed",
                ));
            }
            let bind_pos = sqlite_bind_blob(
                stmt,
                3,
                order_key.as_ptr() as *const c_void,
                order_key.len() as c_int,
                None,
            );
            if bind_pos != SQLITE_OK as c_int {
                return Err(sqlite_rc_error(
                    bind_pos,
                    "bind set_parent_order_key order_key failed",
                ));
            }
            let step_rc = sqlite_step(stmt);
            sqlite_reset(stmt);
            if step_rc != SQLITE_DONE as c_int {
                return Err(sqlite_rc_error(step_rc, "set_parent_order_key step failed"));
            }
        }

        Ok(())
    }

    fn tombstone(&self, node: NodeId) -> treecrdt_core::Result<bool> {
        let bytes = sqlite_node_id_bytes(node);
        let stmt = self.select_tombstone.get()?;
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
                return Err(sqlite_rc_error(bind_rc, "bind select_tombstone failed"));
            }

            let step_rc = sqlite_step(stmt);
            let out = if step_rc == SQLITE_ROW as c_int {
                sqlite_column_int64(stmt, 0) != 0
            } else if step_rc == SQLITE_DONE as c_int {
                false
            } else {
                sqlite_reset(stmt);
                return Err(sqlite_rc_error(step_rc, "select_tombstone step failed"));
            };
            sqlite_reset(stmt);
            Ok(out)
        }
    }

    fn set_tombstone(&mut self, node: NodeId, tombstone: bool) -> treecrdt_core::Result<()> {
        self.ensure_node(node)?;
        let bytes = sqlite_node_id_bytes(node);
        let stmt = self.update_tombstone.get()?;
        unsafe {
            sqlite_clear_bindings(stmt);
            sqlite_reset(stmt);
            let mut bind_err = false;
            bind_err |= sqlite_bind_blob(
                stmt,
                1,
                bytes.as_ptr() as *const c_void,
                bytes.len() as c_int,
                None,
            ) != SQLITE_OK as c_int;
            bind_err |=
                sqlite_bind_int64(stmt, 2, if tombstone { 1 } else { 0 }) != SQLITE_OK as c_int;
            if bind_err {
                sqlite_reset(stmt);
                return Err(sqlite_rc_error(
                    SQLITE_ERROR as c_int,
                    "bind update_tombstone failed",
                ));
            }
            let step_rc = sqlite_step(stmt);
            sqlite_reset(stmt);
            if step_rc != SQLITE_DONE as c_int {
                return Err(sqlite_rc_error(step_rc, "update_tombstone step failed"));
            }
        }
        Ok(())
    }

    fn has_deleted_at(&self, node: NodeId) -> treecrdt_core::Result<bool> {
        let bytes = sqlite_node_id_bytes(node);
        let stmt = self.select_node.get()?;
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
                return Err(sqlite_rc_error(bind_rc, "bind select_node failed"));
            }

            let step_rc = sqlite_step(stmt);
            let has = if step_rc == SQLITE_ROW as c_int {
                match read_optional_version_vector_v0_column(stmt, 3, "deleted_at") {
                    Ok(value) => value.is_some(),
                    Err(error) => {
                        sqlite_reset(stmt);
                        return Err(error);
                    }
                }
            } else if step_rc == SQLITE_DONE as c_int {
                false
            } else {
                sqlite_reset(stmt);
                return Err(sqlite_rc_error(step_rc, "select_node step failed"));
            };
            sqlite_reset(stmt);
            Ok(has)
        }
    }

    fn parent_and_has_deleted_at(
        &self,
        node: NodeId,
    ) -> treecrdt_core::Result<Option<(Option<NodeId>, bool)>> {
        let bytes = sqlite_node_id_bytes(node);
        let stmt = self.select_node.get()?;
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
                return Err(sqlite_rc_error(bind_rc, "bind select_node failed"));
            }

            let step_rc = sqlite_step(stmt);
            let out = if step_rc == SQLITE_ROW as c_int {
                let parent = match column_blob16(stmt, 0) {
                    Ok(Some(p)) => Some(sqlite_bytes_to_node_id(p)),
                    Ok(None) => None,
                    Err(rc) => {
                        sqlite_reset(stmt);
                        return Err(sqlite_rc_error(rc, "read parent failed"));
                    }
                };
                let has_deleted_at =
                    match read_optional_version_vector_v0_column(stmt, 3, "deleted_at") {
                        Ok(value) => value.is_some(),
                        Err(error) => {
                            sqlite_reset(stmt);
                            return Err(error);
                        }
                    };
                Some((parent, has_deleted_at))
            } else if step_rc == SQLITE_DONE as c_int {
                None
            } else {
                sqlite_reset(stmt);
                return Err(sqlite_rc_error(step_rc, "select_node step failed"));
            };
            sqlite_reset(stmt);
            Ok(out)
        }
    }

    fn last_change(&self, node: NodeId) -> treecrdt_core::Result<VersionVector> {
        let bytes = sqlite_node_id_bytes(node);
        let stmt = self.select_node.get()?;
        unsafe {
            sqlite_clear_bindings(stmt);
            sqlite_reset(stmt);
            sqlite_bind_blob(
                stmt,
                1,
                bytes.as_ptr() as *const c_void,
                bytes.len() as c_int,
                None,
            );
            let step_rc = sqlite_step(stmt);
            if step_rc != SQLITE_ROW as c_int {
                sqlite_reset(stmt);
                return Err(sqlite_rc_error(step_rc, "select_node last_change failed"));
            }
            let vv = match read_optional_version_vector_v0_column(stmt, 2, "last_change") {
                Ok(value) => value.unwrap_or_default(),
                Err(error) => {
                    sqlite_reset(stmt);
                    return Err(error);
                }
            };
            sqlite_reset(stmt);
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
        let bytes = vv.encode_v0()?;

        let node_bytes = sqlite_node_id_bytes(node);
        let stmt = self.update_last_change.get()?;
        unsafe {
            sqlite_clear_bindings(stmt);
            sqlite_reset(stmt);
            sqlite_bind_blob(
                stmt,
                1,
                node_bytes.as_ptr() as *const c_void,
                node_bytes.len() as c_int,
                None,
            );
            sqlite_bind_blob(
                stmt,
                2,
                bytes.as_ptr() as *const c_void,
                bytes.len() as c_int,
                None,
            );
            let step_rc = sqlite_step(stmt);
            sqlite_reset(stmt);
            if step_rc != SQLITE_DONE as c_int {
                return Err(sqlite_rc_error(step_rc, "update_last_change failed"));
            }
        }

        Ok(())
    }

    fn deleted_at(&self, node: NodeId) -> treecrdt_core::Result<Option<VersionVector>> {
        let bytes = sqlite_node_id_bytes(node);
        let stmt = self.select_node.get()?;
        unsafe {
            sqlite_clear_bindings(stmt);
            sqlite_reset(stmt);
            sqlite_bind_blob(
                stmt,
                1,
                bytes.as_ptr() as *const c_void,
                bytes.len() as c_int,
                None,
            );
            let step_rc = sqlite_step(stmt);
            if step_rc != SQLITE_ROW as c_int {
                sqlite_reset(stmt);
                return Ok(None);
            }
            let vv = match read_optional_version_vector_v0_column(stmt, 3, "deleted_at") {
                Ok(value) => value,
                Err(error) => {
                    sqlite_reset(stmt);
                    return Err(error);
                }
            };
            sqlite_reset(stmt);
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
        let bytes = vv.encode_v0()?;

        let node_bytes = sqlite_node_id_bytes(node);
        let stmt = self.update_deleted_at.get()?;
        unsafe {
            sqlite_clear_bindings(stmt);
            sqlite_reset(stmt);
            sqlite_bind_blob(
                stmt,
                1,
                node_bytes.as_ptr() as *const c_void,
                node_bytes.len() as c_int,
                None,
            );
            sqlite_bind_blob(
                stmt,
                2,
                bytes.as_ptr() as *const c_void,
                bytes.len() as c_int,
                None,
            );
            let step_rc = sqlite_step(stmt);
            sqlite_reset(stmt);
            if step_rc != SQLITE_DONE as c_int {
                return Err(sqlite_rc_error(step_rc, "update_deleted_at failed"));
            }
        }

        Ok(())
    }

    fn all_nodes(&self) -> treecrdt_core::Result<Vec<NodeId>> {
        let mut out = Vec::new();
        let stmt = self.all_nodes.get()?;
        unsafe {
            sqlite_clear_bindings(stmt);
            sqlite_reset(stmt);
            loop {
                let step_rc = sqlite_step(stmt);
                if step_rc == SQLITE_ROW as c_int {
                    match column_blob16(stmt, 0) {
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
            sqlite_reset(stmt);
        }
        Ok(out)
    }
}

impl treecrdt_core::ExactNodeStore for SqliteNodeStore {
    fn set_last_change_exact(
        &mut self,
        node: NodeId,
        vv: &VersionVector,
    ) -> treecrdt_core::Result<()> {
        self.ensure_node(node)?;
        let node_bytes = sqlite_node_id_bytes(node);
        let vv_bytes = (!vv.is_empty()).then(|| vv.encode_v0()).transpose()?;
        let stmt = self.update_last_change.get()?;
        unsafe {
            sqlite_clear_bindings(stmt);
            sqlite_reset(stmt);
            sqlite_bind_blob(
                stmt,
                1,
                node_bytes.as_ptr() as *const c_void,
                node_bytes.len() as c_int,
                None,
            );
            if let Some(bytes) = vv_bytes.as_ref() {
                sqlite_bind_blob(
                    stmt,
                    2,
                    bytes.as_ptr() as *const c_void,
                    bytes.len() as c_int,
                    None,
                );
            } else {
                sqlite_bind_null(stmt, 2);
            }
            let step_rc = sqlite_step(stmt);
            let reset_rc = sqlite_reset(stmt);
            // sqlite_reset preserves SQLITE_STATIC bindings, so clear them before node_bytes and
            // vv_bytes are dropped.
            let clear_rc = sqlite_clear_bindings(stmt);
            if step_rc != SQLITE_DONE as c_int {
                return Err(sqlite_rc_error(step_rc, "set exact last_change failed"));
            }
            if reset_rc != SQLITE_OK as c_int {
                return Err(sqlite_rc_error(reset_rc, "reset exact last_change failed"));
            }
            if clear_rc != SQLITE_OK as c_int {
                return Err(sqlite_rc_error(
                    clear_rc,
                    "clear exact last_change bindings failed",
                ));
            }
        }
        Ok(())
    }

    fn set_deleted_at_exact(
        &mut self,
        node: NodeId,
        vv: Option<&VersionVector>,
    ) -> treecrdt_core::Result<()> {
        self.ensure_node(node)?;
        let node_bytes = sqlite_node_id_bytes(node);
        let vv_bytes = vv.filter(|vv| !vv.is_empty()).map(VersionVector::encode_v0).transpose()?;
        let stmt = self.update_deleted_at.get()?;
        unsafe {
            sqlite_clear_bindings(stmt);
            sqlite_reset(stmt);
            sqlite_bind_blob(
                stmt,
                1,
                node_bytes.as_ptr() as *const c_void,
                node_bytes.len() as c_int,
                None,
            );
            if let Some(bytes) = vv_bytes.as_ref() {
                sqlite_bind_blob(
                    stmt,
                    2,
                    bytes.as_ptr() as *const c_void,
                    bytes.len() as c_int,
                    None,
                );
            } else {
                sqlite_bind_null(stmt, 2);
            }
            let step_rc = sqlite_step(stmt);
            let reset_rc = sqlite_reset(stmt);
            // sqlite_reset preserves SQLITE_STATIC bindings, so clear them before node_bytes and
            // vv_bytes are dropped.
            let clear_rc = sqlite_clear_bindings(stmt);
            if step_rc != SQLITE_DONE as c_int {
                return Err(sqlite_rc_error(step_rc, "set exact deleted_at failed"));
            }
            if reset_rc != SQLITE_OK as c_int {
                return Err(sqlite_rc_error(reset_rc, "reset exact deleted_at failed"));
            }
            if clear_rc != SQLITE_OK as c_int {
                return Err(sqlite_rc_error(
                    clear_rc,
                    "clear exact deleted_at bindings failed",
                ));
            }
        }
        Ok(())
    }
}

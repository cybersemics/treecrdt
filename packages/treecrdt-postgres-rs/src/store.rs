use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::time::Instant;

use postgres::{Client, Row, Statement};

use treecrdt_core::{
    apply_incremental_ops, try_incremental_materialization, Error, Lamport, LamportClock,
    MaterializationCursor, NodeId, NodeStore, NoopStorage, Operation, OperationId, OperationKind,
    ParentOpIndex, PayloadStore, ReplicaId, Result, Storage, TreeCrdt, VersionVector,
};

use crate::opref::{derive_op_ref_v0, OPREF_V0_WIDTH};
use crate::profile::{append_profile_enabled, PgAppendProfile};
use crate::schema::{reset_doc_for_tests, reset_doc_for_tests_within_tx};

pub(crate) fn storage_debug<E: std::fmt::Debug>(e: E) -> Error {
    Error::Storage(format!("{e:?}"))
}

pub(crate) fn node_to_bytes(node: NodeId) -> [u8; 16] {
    node.0.to_be_bytes()
}

pub(crate) fn bytes_to_node(bytes: &[u8]) -> Result<NodeId> {
    if bytes.len() != 16 {
        return Err(Error::Storage("expected 16-byte node id".into()));
    }
    let mut arr = [0u8; 16];
    arr.copy_from_slice(bytes);
    Ok(NodeId(u128::from_be_bytes(arr)))
}

fn order_key_from_position(position: usize) -> Result<Vec<u8>> {
    if position >= u16::MAX as usize {
        return Err(Error::Storage(format!(
            "position too large for u16 order key: {position}"
        )));
    }
    Ok(((position + 1) as u16).to_be_bytes().to_vec())
}

fn repeated_replica_from_label(label: &str) -> Result<ReplicaId> {
    if label.is_empty() {
        return Err(Error::Storage("replica label must not be empty".into()));
    }
    let encoded = label.as_bytes();
    let mut out = vec![0u8; 32];
    for (index, byte) in out.iter_mut().enumerate() {
        *byte = encoded[index % encoded.len()];
    }
    Ok(ReplicaId::new(out))
}

fn payload_bytes_from_seed(seed: usize, size: usize) -> Vec<u8> {
    let mut out = vec![0u8; size];
    for (index, byte) in out.iter_mut().enumerate() {
        *byte = ((seed + index * 31) % 251) as u8;
    }
    out
}

fn single_counter_vv_bytes(replica: &ReplicaId, counter: u64) -> Result<Vec<u8>> {
    let mut vv = VersionVector::new();
    vv.observe(replica, counter);
    vv_to_bytes(&vv)
}

fn balanced_parent_and_position(node_index: usize, fanout: usize) -> (NodeId, usize) {
    if node_index <= fanout {
        return (NodeId::ROOT, (node_index - 1) % fanout);
    }
    (
        NodeId((((node_index - (fanout + 1)) / fanout) + 1) as u128),
        (node_index - 1) % fanout,
    )
}

fn balanced_direct_child_max_counter(node_index: usize, size: usize, fanout: usize) -> Option<u64> {
    let start = node_index.saturating_mul(fanout).saturating_add(1);
    if start > size {
        return None;
    }
    Some((node_index.saturating_mul(fanout).saturating_add(fanout).min(size)) as u64)
}

pub(crate) fn op_ref_from_bytes(bytes: &[u8]) -> Result<[u8; OPREF_V0_WIDTH]> {
    if bytes.len() != OPREF_V0_WIDTH {
        return Err(Error::Storage("expected 16-byte op_ref".into()));
    }
    let mut arr = [0u8; OPREF_V0_WIDTH];
    arr.copy_from_slice(bytes);
    Ok(arr)
}

fn vv_to_bytes(vv: &VersionVector) -> Result<Vec<u8>> {
    serde_json::to_vec(vv).map_err(|e| Error::Storage(e.to_string()))
}

pub(crate) fn vv_from_bytes(bytes: &[u8]) -> Result<VersionVector> {
    serde_json::from_slice(bytes).map_err(|e| Error::Storage(e.to_string()))
}

#[derive(Clone, Debug)]
pub(crate) struct TreeMeta {
    dirty: bool,
    head_lamport: Lamport,
    head_replica: Vec<u8>,
    head_counter: u64,
    head_seq: u64,
}

impl MaterializationCursor for TreeMeta {
    fn dirty(&self) -> bool {
        self.dirty
    }

    fn head_lamport(&self) -> Lamport {
        self.head_lamport
    }

    fn head_replica(&self) -> &[u8] {
        &self.head_replica
    }

    fn head_counter(&self) -> u64 {
        self.head_counter
    }

    fn head_seq(&self) -> u64 {
        self.head_seq
    }
}

pub(crate) fn ensure_doc_meta(client: &Rc<RefCell<Client>>, doc_id: &str) -> Result<()> {
    let mut c = client.borrow_mut();
    c.execute(
        // Default new docs to "clean" so we can incrementally maintain materialized state.
        "INSERT INTO treecrdt_meta(doc_id, dirty) VALUES ($1, FALSE) ON CONFLICT (doc_id) DO NOTHING",
        &[&doc_id],
    )
    .map_err(storage_debug)?;
    Ok(())
}

fn load_tree_meta_row(
    client: &Rc<RefCell<Client>>,
    doc_id: &str,
    for_update: bool,
) -> Result<TreeMeta> {
    let ctx = PgCtx::new_assume_doc_meta(client.clone(), doc_id)?;
    let mut c = client.borrow_mut();
    let stmt = if for_update {
        ctx.stmt(
            &mut c,
            "SELECT dirty, head_lamport, head_replica, head_counter, head_seq \
             FROM treecrdt_meta WHERE doc_id = $1 FOR UPDATE",
        )?
    } else {
        ctx.stmt(
            &mut c,
            "SELECT dirty, head_lamport, head_replica, head_counter, head_seq \
             FROM treecrdt_meta WHERE doc_id = $1 LIMIT 1",
        )?
    };
    let rows = c.query(&stmt, &[&doc_id]).map_err(storage_debug)?;

    let row = rows.first().ok_or_else(|| Error::Storage("missing treecrdt_meta row".into()))?;

    Ok(TreeMeta {
        dirty: row.get::<_, bool>(0),
        head_lamport: row.get::<_, i64>(1).max(0) as Lamport,
        head_replica: row.get::<_, Vec<u8>>(2),
        head_counter: row.get::<_, i64>(3).max(0) as u64,
        head_seq: row.get::<_, i64>(4).max(0) as u64,
    })
}

fn load_tree_meta(client: &Rc<RefCell<Client>>, doc_id: &str) -> Result<TreeMeta> {
    ensure_doc_meta(client, doc_id)?;
    load_tree_meta_row(client, doc_id, false)
}

pub(crate) fn load_tree_meta_for_update(
    client: &Rc<RefCell<Client>>,
    doc_id: &str,
) -> Result<TreeMeta> {
    ensure_doc_meta(client, doc_id)?;
    load_tree_meta_row(client, doc_id, true)
}

pub(crate) fn set_tree_meta_dirty(
    client: &Rc<RefCell<Client>>,
    doc_id: &str,
    dirty: bool,
) -> Result<()> {
    ensure_doc_meta(client, doc_id)?;
    let mut c = client.borrow_mut();
    c.execute(
        "UPDATE treecrdt_meta SET dirty = $2 WHERE doc_id = $1",
        &[&doc_id, &dirty],
    )
    .map_err(|e| Error::Storage(e.to_string()))?;
    Ok(())
}

pub(crate) fn update_tree_meta_head(
    client: &Rc<RefCell<Client>>,
    doc_id: &str,
    lamport: Lamport,
    replica: &[u8],
    counter: u64,
    seq: u64,
) -> Result<()> {
    ensure_doc_meta(client, doc_id)?;
    let mut c = client.borrow_mut();
    c.execute(
        "UPDATE treecrdt_meta SET dirty = FALSE, head_lamport = $2, head_replica = $3, head_counter = $4, head_seq = $5 WHERE doc_id = $1",
        &[&doc_id, &(lamport as i64), &replica, &(counter as i64), &(seq as i64)],
    )
    .map_err(|e| Error::Storage(e.to_string()))?;
    Ok(())
}

#[derive(Clone)]
pub(crate) struct PgCtx {
    pub(crate) doc_id: String,
    pub(crate) client: Rc<RefCell<Client>>,
    stmts: Rc<RefCell<HashMap<&'static str, Statement>>>,
    append_profile: Option<Rc<RefCell<PgAppendProfile>>>,
}

impl PgCtx {
    pub(crate) fn new(client: Rc<RefCell<Client>>, doc_id: &str) -> Result<Self> {
        Self::new_with_profile(client, doc_id, None)
    }

    pub(crate) fn new_assume_doc_meta(client: Rc<RefCell<Client>>, doc_id: &str) -> Result<Self> {
        Self::new_with_profile_assume_doc_meta(client, doc_id, None)
    }

    fn new_with_profile(
        client: Rc<RefCell<Client>>,
        doc_id: &str,
        append_profile: Option<Rc<RefCell<PgAppendProfile>>>,
    ) -> Result<Self> {
        ensure_doc_meta(&client, doc_id)?;
        Self::new_with_profile_assume_doc_meta(client, doc_id, append_profile)
    }

    fn new_with_profile_assume_doc_meta(
        client: Rc<RefCell<Client>>,
        doc_id: &str,
        append_profile: Option<Rc<RefCell<PgAppendProfile>>>,
    ) -> Result<Self> {
        Ok(Self {
            doc_id: doc_id.to_string(),
            client,
            stmts: Rc::new(RefCell::new(HashMap::new())),
            append_profile,
        })
    }

    pub(crate) fn stmt(&self, c: &mut Client, sql: &'static str) -> Result<Statement> {
        if let Some(stmt) = self.stmts.borrow().get(sql) {
            return Ok(stmt.clone());
        }
        let stmt = c.prepare(sql).map_err(storage_debug)?;
        self.stmts.borrow_mut().insert(sql, stmt.clone());
        Ok(stmt)
    }
}

#[derive(Clone, Debug)]
struct CachedNodeRow {
    parent: Option<NodeId>,
    order_key: Option<Vec<u8>>,
    tombstone: bool,
    last_change: Option<Vec<u8>>,
    deleted_at: Option<Vec<u8>>,
}

#[derive(Clone, Debug)]
struct CachedPayloadRow {
    payload: Option<Vec<u8>>,
    last_lamport: Lamport,
    last_replica: Vec<u8>,
    last_counter: u64,
}

#[derive(Clone)]
pub(crate) struct PgNodeStore {
    ctx: PgCtx,
    cache: Rc<RefCell<HashMap<NodeId, Option<CachedNodeRow>>>>,
    pending_last_change: Rc<RefCell<HashSet<NodeId>>>,
}

impl PgNodeStore {
    pub(crate) fn new(ctx: PgCtx) -> Self {
        Self {
            ctx,
            cache: Rc::new(RefCell::new(HashMap::new())),
            pending_last_change: Rc::new(RefCell::new(HashSet::new())),
        }
    }

    fn load_node_row(&self, node: NodeId) -> Result<Option<CachedNodeRow>> {
        let started_at = Instant::now();
        let node_bytes = node_to_bytes(node);
        let mut c = self.ctx.client.borrow_mut();
        let stmt = self.ctx.stmt(
            &mut c,
            "SELECT parent, order_key, tombstone, last_change, deleted_at \
             FROM treecrdt_nodes WHERE doc_id = $1 AND node = $2 LIMIT 1",
        )?;
        let rows = c
            .query(&stmt, &[&self.ctx.doc_id, &node_bytes.as_slice()])
            .map_err(storage_debug)?;

        let Some(row) = rows.first() else {
            return Ok(None);
        };

        let parent_bytes: Option<Vec<u8>> = row.get(0);
        let parent = match parent_bytes {
            None => None,
            Some(b) => Some(bytes_to_node(&b)?),
        };
        let order_key: Option<Vec<u8>> = row.get(1);
        let tombstone: bool = row.get(2);
        let last_change: Option<Vec<u8>> = row.get(3);
        let deleted_at: Option<Vec<u8>> = row.get(4);
        if let Some(profile) = &self.ctx.append_profile {
            let elapsed_ms = started_at.elapsed().as_secs_f64() * 1000.0;
            let mut profile = profile.borrow_mut();
            profile.node_load_count += 1;
            profile.node_load_ms += elapsed_ms;
        }
        Ok(Some(CachedNodeRow {
            parent,
            order_key,
            tombstone,
            last_change,
            deleted_at,
        }))
    }

    fn node_row(&self, node: NodeId) -> Result<Option<CachedNodeRow>> {
        if let Some(cached) = self.cache.borrow().get(&node) {
            return Ok(cached.clone());
        }
        let loaded = self.load_node_row(node)?;
        self.cache.borrow_mut().insert(node, loaded.clone());
        Ok(loaded)
    }

    // Payload application may need the node's current parent / deleted_at state before core can
    // decide payload visibility and defensive-delete effects. Preload those node rows in one SQL
    // pass so incremental materialization does not degrade into per-op lookups.
    fn preload_for_ops(&self, ops: &[Operation]) -> Result<()> {
        let mut referenced: HashSet<NodeId> = HashSet::new();
        for op in ops {
            // Always preload the op's own node. Payload / delete / move semantics all consult the
            // current cached row, not just the incoming operation payload.
            let node = op.kind.node();
            if node != NodeId::TRASH {
                referenced.insert(node);
            }
            match &op.kind {
                OperationKind::Insert { parent, .. } => {
                    // Inserts can immediately affect the parent subtree's tombstone / visibility
                    // state, so preload the current parent row too.
                    if *parent != NodeId::TRASH {
                        referenced.insert(*parent);
                    }
                }
                OperationKind::Move { new_parent, .. } => {
                    // Moves can revive or reindex under the destination parent, so preload it.
                    if *new_parent != NodeId::TRASH {
                        referenced.insert(*new_parent);
                    }
                }
                OperationKind::Delete { .. }
                | OperationKind::Tombstone { .. }
                | OperationKind::Payload { .. } => {}
            }
        }

        if referenced.is_empty() {
            return Ok(());
        }

        let mut requested_nodes: Vec<NodeId> = referenced.iter().copied().collect();
        requested_nodes.sort();
        let requested_bytes: Vec<Vec<u8>> =
            requested_nodes.iter().map(|node| node_to_bytes(*node).to_vec()).collect();

        let load_started_at = Instant::now();
        let mut loaded_nodes: HashSet<NodeId> = HashSet::with_capacity(requested_nodes.len());
        {
            let mut c = self.ctx.client.borrow_mut();
            // Fetch every currently-existing node row in one query so the cache reflects the
            // pre-apply materialized state before core starts mutating it.
            let stmt = self.ctx.stmt(
                &mut c,
                "SELECT node, parent, order_key, tombstone, last_change, deleted_at \
                 FROM treecrdt_nodes \
                 WHERE doc_id = $1 \
                   AND node IN (SELECT DISTINCT i.node FROM unnest($2::bytea[]) AS i(node))",
            )?;
            let rows =
                c.query(&stmt, &[&self.ctx.doc_id, &requested_bytes]).map_err(storage_debug)?;
            let mut cache = self.cache.borrow_mut();
            for row in rows {
                let node_bytes: Vec<u8> = row.get(0);
                let node = bytes_to_node(&node_bytes)?;
                let parent_bytes: Option<Vec<u8>> = row.get(1);
                let parent = match parent_bytes {
                    None => None,
                    Some(b) => Some(bytes_to_node(&b)?),
                };
                let order_key: Option<Vec<u8>> = row.get(2);
                let tombstone: bool = row.get(3);
                let last_change: Option<Vec<u8>> = row.get(4);
                let deleted_at: Option<Vec<u8>> = row.get(5);
                loaded_nodes.insert(node);
                cache.insert(
                    node,
                    Some(CachedNodeRow {
                        parent,
                        order_key,
                        tombstone,
                        last_change,
                        deleted_at,
                    }),
                );
            }
        }
        if let Some(profile) = &self.ctx.append_profile {
            let elapsed_ms = load_started_at.elapsed().as_secs_f64() * 1000.0;
            let mut profile = profile.borrow_mut();
            profile.node_load_count += loaded_nodes.len() as u64;
            profile.node_load_ms += elapsed_ms;
        }

        let missing: Vec<NodeId> = requested_nodes
            .into_iter()
            .filter(|node| !loaded_nodes.contains(node))
            .collect();
        if missing.is_empty() {
            return Ok(());
        }

        let ensure_started_at = Instant::now();
        let missing_bytes: Vec<Vec<u8>> =
            missing.iter().map(|node| node_to_bytes(*node).to_vec()).collect();
        {
            let mut c = self.ctx.client.borrow_mut();
            // Core assumes referenced nodes exist in the NodeStore. For brand-new nodes that have
            // never been materialized before, insert placeholder rows now and let later attach /
            // payload / delete steps fill in the real state.
            let stmt = self.ctx.stmt(
                &mut c,
                "INSERT INTO treecrdt_nodes(doc_id, node) \
                 SELECT $1, src.node \
                 FROM unnest($2::bytea[]) AS src(node) \
                 ON CONFLICT (doc_id, node) DO NOTHING",
            )?;
            c.execute(&stmt, &[&self.ctx.doc_id, &missing_bytes]).map_err(storage_debug)?;
        }
        {
            let mut cache = self.cache.borrow_mut();
            for node in missing {
                cache.insert(
                    node,
                    Some(CachedNodeRow {
                        parent: None,
                        order_key: None,
                        tombstone: false,
                        last_change: None,
                        deleted_at: None,
                    }),
                );
            }
        }
        if let Some(profile) = &self.ctx.append_profile {
            let elapsed_ms = ensure_started_at.elapsed().as_secs_f64() * 1000.0;
            let mut profile = profile.borrow_mut();
            profile.node_ensure_count += missing_bytes.len() as u64;
            profile.node_ensure_ms += elapsed_ms;
        }

        Ok(())
    }

    // last_change is updated many times while core applies a batch. Buffer the touched nodes in
    // memory and flush once at the end so we avoid one UPDATE per node mutation.
    pub(crate) fn flush_last_change(&self) -> Result<()> {
        let dirty_nodes = {
            let mut pending = self.pending_last_change.borrow_mut();
            std::mem::take(&mut *pending)
        };
        if dirty_nodes.is_empty() {
            return Ok(());
        }

        let started_at = Instant::now();
        let mut nodes: Vec<Vec<u8>> = Vec::with_capacity(dirty_nodes.len());
        let mut values: Vec<Vec<u8>> = Vec::with_capacity(dirty_nodes.len());
        {
            let cache = self.cache.borrow();
            for node in dirty_nodes {
                let Some(Some(row)) = cache.get(&node) else {
                    continue;
                };
                let Some(last_change) = &row.last_change else {
                    continue;
                };
                nodes.push(node_to_bytes(node).to_vec());
                values.push(last_change.clone());
            }
        }
        if nodes.is_empty() {
            return Ok(());
        }

        let mut c = self.ctx.client.borrow_mut();
        // Write the buffered last_change vectors back in one batched UPDATE so the persisted
        // subtree version vectors stay aligned with the in-memory cache core just mutated.
        let stmt = self.ctx.stmt(
            &mut c,
            "UPDATE treecrdt_nodes AS dst \
             SET last_change = src.last_change \
             FROM unnest($2::bytea[], $3::bytea[]) AS src(node, last_change) \
             WHERE dst.doc_id = $1 AND dst.node = src.node",
        )?;
        c.execute(&stmt, &[&self.ctx.doc_id, &nodes, &values]).map_err(storage_debug)?;

        if let Some(profile) = &self.ctx.append_profile {
            profile.borrow_mut().node_last_change_ms += started_at.elapsed().as_secs_f64() * 1000.0;
        }

        Ok(())
    }
}

impl treecrdt_core::NodeStore for PgNodeStore {
    fn reset(&mut self) -> Result<()> {
        self.cache.borrow_mut().clear();
        self.pending_last_change.borrow_mut().clear();
        let mut c = self.ctx.client.borrow_mut();
        let stmt = self.ctx.stmt(&mut c, "DELETE FROM treecrdt_nodes WHERE doc_id = $1")?;
        c.execute(&stmt, &[&self.ctx.doc_id]).map_err(storage_debug)?;

        let root_bytes = node_to_bytes(NodeId::ROOT);
        let empty: &[u8] = &[];
        let stmt = self.ctx.stmt(
            &mut c,
            "INSERT INTO treecrdt_nodes(doc_id, node, parent, order_key, tombstone) VALUES ($1, $2, NULL, $3, FALSE) ON CONFLICT (doc_id, node) DO UPDATE SET parent = NULL, order_key = EXCLUDED.order_key, tombstone = FALSE",
        )?;
        c.execute(&stmt, &[&self.ctx.doc_id, &root_bytes.as_slice(), &empty])
            .map_err(storage_debug)?;

        self.cache.borrow_mut().insert(
            NodeId::ROOT,
            Some(CachedNodeRow {
                parent: None,
                order_key: Some(Vec::new()),
                tombstone: false,
                last_change: None,
                deleted_at: None,
            }),
        );
        Ok(())
    }

    fn ensure_node(&mut self, node: NodeId) -> Result<()> {
        if matches!(self.cache.borrow().get(&node), Some(Some(_))) {
            return Ok(());
        }

        let started_at = Instant::now();
        let node_bytes = node_to_bytes(node);
        let mut c = self.ctx.client.borrow_mut();
        let stmt = self.ctx.stmt(
            &mut c,
            "INSERT INTO treecrdt_nodes(doc_id, node) VALUES ($1, $2) \
             ON CONFLICT (doc_id, node) DO NOTHING",
        )?;
        let inserted = c
            .execute(&stmt, &[&self.ctx.doc_id, &node_bytes.as_slice()])
            .map_err(storage_debug)?;

        if inserted > 0 {
            self.cache.borrow_mut().insert(
                node,
                Some(CachedNodeRow {
                    parent: None,
                    order_key: None,
                    tombstone: false,
                    last_change: None,
                    deleted_at: None,
                }),
            );
        }
        if let Some(profile) = &self.ctx.append_profile {
            let elapsed_ms = started_at.elapsed().as_secs_f64() * 1000.0;
            let mut profile = profile.borrow_mut();
            profile.node_ensure_count += 1;
            profile.node_ensure_ms += elapsed_ms;
        }
        Ok(())
    }

    fn exists(&self, node: NodeId) -> Result<bool> {
        Ok(self.node_row(node)?.is_some())
    }

    fn parent(&self, node: NodeId) -> Result<Option<NodeId>> {
        let Some(row) = self.node_row(node)? else {
            return Ok(None);
        };
        Ok(row.parent)
    }

    fn order_key(&self, node: NodeId) -> Result<Option<Vec<u8>>> {
        let Some(row) = self.node_row(node)? else {
            return Ok(None);
        };
        Ok(row.order_key)
    }

    fn children(&self, parent: NodeId) -> Result<Vec<NodeId>> {
        if parent == NodeId::TRASH {
            return Ok(Vec::new());
        }
        let parent_bytes = node_to_bytes(parent);
        let mut c = self.ctx.client.borrow_mut();
        let stmt = self.ctx.stmt(
            &mut c,
            "SELECT node FROM treecrdt_nodes \
             WHERE doc_id = $1 AND parent = $2 \
             ORDER BY order_key, node",
        )?;
        let rows = c
            .query(&stmt, &[&self.ctx.doc_id, &parent_bytes.as_slice()])
            .map_err(storage_debug)?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let node: Vec<u8> = row.get(0);
            out.push(bytes_to_node(&node)?);
        }
        Ok(out)
    }

    fn detach(&mut self, node: NodeId) -> Result<()> {
        if node == NodeId::ROOT {
            return Ok(());
        }
        self.ensure_node(node)?;
        let started_at = Instant::now();
        let node_bytes = node_to_bytes(node);
        let mut c = self.ctx.client.borrow_mut();
        let stmt = self.ctx.stmt(
            &mut c,
            "UPDATE treecrdt_nodes \
             SET parent = NULL, order_key = NULL \
             WHERE doc_id = $1 AND node = $2",
        )?;
        c.execute(&stmt, &[&self.ctx.doc_id, &node_bytes.as_slice()])
            .map_err(storage_debug)?;

        if let Some(Some(row)) = self.cache.borrow_mut().get_mut(&node) {
            row.parent = None;
            row.order_key = None;
        }
        if let Some(profile) = &self.ctx.append_profile {
            let elapsed_ms = started_at.elapsed().as_secs_f64() * 1000.0;
            let mut profile = profile.borrow_mut();
            profile.node_detach_count += 1;
            profile.node_detach_ms += elapsed_ms;
        }
        Ok(())
    }

    fn attach(&mut self, node: NodeId, parent: NodeId, order_key: Vec<u8>) -> Result<()> {
        if node == NodeId::ROOT {
            return Ok(());
        }
        self.ensure_node(node)?;
        self.ensure_node(parent)?;

        let started_at = Instant::now();
        let node_bytes = node_to_bytes(node);
        let parent_bytes = node_to_bytes(parent);
        let mut c = self.ctx.client.borrow_mut();

        if parent == NodeId::TRASH {
            let stmt = self.ctx.stmt(
                &mut c,
                "UPDATE treecrdt_nodes \
                 SET parent = $3, order_key = NULL \
                 WHERE doc_id = $1 AND node = $2",
            )?;
            c.execute(
                &stmt,
                &[
                    &self.ctx.doc_id,
                    &node_bytes.as_slice(),
                    &parent_bytes.as_slice(),
                ],
            )
            .map_err(storage_debug)?;

            if let Some(Some(row)) = self.cache.borrow_mut().get_mut(&node) {
                row.parent = Some(parent);
                row.order_key = None;
            }
            if let Some(profile) = &self.ctx.append_profile {
                let elapsed_ms = started_at.elapsed().as_secs_f64() * 1000.0;
                let mut profile = profile.borrow_mut();
                profile.node_attach_count += 1;
                profile.node_attach_ms += elapsed_ms;
            }
            return Ok(());
        }

        let stmt = self.ctx.stmt(
            &mut c,
            "UPDATE treecrdt_nodes \
             SET parent = $3, order_key = $4 \
             WHERE doc_id = $1 AND node = $2",
        )?;
        c.execute(
            &stmt,
            &[
                &self.ctx.doc_id,
                &node_bytes.as_slice(),
                &parent_bytes.as_slice(),
                &order_key,
            ],
        )
        .map_err(storage_debug)?;

        if let Some(Some(row)) = self.cache.borrow_mut().get_mut(&node) {
            row.parent = Some(parent);
            row.order_key = Some(order_key);
        }
        if let Some(profile) = &self.ctx.append_profile {
            let elapsed_ms = started_at.elapsed().as_secs_f64() * 1000.0;
            let mut profile = profile.borrow_mut();
            profile.node_attach_count += 1;
            profile.node_attach_ms += elapsed_ms;
        }
        Ok(())
    }

    fn tombstone(&self, node: NodeId) -> Result<bool> {
        let Some(row) = self.node_row(node)? else {
            return Ok(false);
        };
        Ok(row.tombstone)
    }

    fn set_tombstone(&mut self, node: NodeId, tombstone: bool) -> Result<()> {
        self.ensure_node(node)?;
        let started_at = Instant::now();
        let node_bytes = node_to_bytes(node);
        let mut c = self.ctx.client.borrow_mut();
        let stmt = self.ctx.stmt(
            &mut c,
            "UPDATE treecrdt_nodes \
             SET tombstone = $3 \
             WHERE doc_id = $1 AND node = $2",
        )?;
        c.execute(
            &stmt,
            &[&self.ctx.doc_id, &node_bytes.as_slice(), &tombstone],
        )
        .map_err(storage_debug)?;

        if let Some(Some(row)) = self.cache.borrow_mut().get_mut(&node) {
            row.tombstone = tombstone;
        }
        if let Some(profile) = &self.ctx.append_profile {
            let elapsed_ms = started_at.elapsed().as_secs_f64() * 1000.0;
            let mut profile = profile.borrow_mut();
            profile.node_tombstone_count += 1;
            profile.node_tombstone_ms += elapsed_ms;
        }
        Ok(())
    }

    fn last_change(&self, node: NodeId) -> Result<VersionVector> {
        let Some(row) = self.node_row(node)? else {
            return Ok(VersionVector::new());
        };
        match row.last_change {
            None => Ok(VersionVector::new()),
            Some(b) if b.is_empty() => Ok(VersionVector::new()),
            Some(b) => vv_from_bytes(&b),
        }
    }

    fn merge_last_change(&mut self, node: NodeId, delta: &VersionVector) -> Result<()> {
        self.ensure_node(node)?;
        let started_at = Instant::now();
        let mut vv = self.last_change(node)?;
        vv.merge(delta);
        let bytes = vv_to_bytes(&vv)?;

        if let Some(Some(row)) = self.cache.borrow_mut().get_mut(&node) {
            row.last_change = Some(bytes);
        }
        self.pending_last_change.borrow_mut().insert(node);
        if let Some(profile) = &self.ctx.append_profile {
            let elapsed_ms = started_at.elapsed().as_secs_f64() * 1000.0;
            let mut profile = profile.borrow_mut();
            profile.node_last_change_count += 1;
            profile.node_last_change_ms += elapsed_ms;
        }
        Ok(())
    }

    fn deleted_at(&self, node: NodeId) -> Result<Option<VersionVector>> {
        let Some(row) = self.node_row(node)? else {
            return Ok(None);
        };
        match row.deleted_at {
            None => Ok(None),
            Some(b) if b.is_empty() => Ok(None),
            Some(b) => vv_from_bytes(&b).map(Some),
        }
    }

    fn merge_deleted_at(&mut self, node: NodeId, delta: &VersionVector) -> Result<()> {
        self.ensure_node(node)?;
        let started_at = Instant::now();
        let mut vv = self.deleted_at(node)?.unwrap_or_else(VersionVector::new);
        vv.merge(delta);
        let bytes = vv_to_bytes(&vv)?;

        let node_bytes = node_to_bytes(node);
        let mut c = self.ctx.client.borrow_mut();
        let stmt = self.ctx.stmt(
            &mut c,
            "UPDATE treecrdt_nodes \
             SET deleted_at = $3 \
             WHERE doc_id = $1 AND node = $2",
        )?;
        c.execute(&stmt, &[&self.ctx.doc_id, &node_bytes.as_slice(), &bytes])
            .map_err(storage_debug)?;

        if let Some(Some(row)) = self.cache.borrow_mut().get_mut(&node) {
            row.deleted_at = Some(bytes);
        }
        if let Some(profile) = &self.ctx.append_profile {
            let elapsed_ms = started_at.elapsed().as_secs_f64() * 1000.0;
            let mut profile = profile.borrow_mut();
            profile.node_deleted_at_count += 1;
            profile.node_deleted_at_ms += elapsed_ms;
        }
        Ok(())
    }

    fn all_nodes(&self) -> Result<Vec<NodeId>> {
        let mut c = self.ctx.client.borrow_mut();
        let stmt = self.ctx.stmt(&mut c, "SELECT node FROM treecrdt_nodes WHERE doc_id = $1")?;
        let rows = c.query(&stmt, &[&self.ctx.doc_id]).map_err(storage_debug)?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let node: Vec<u8> = row.get(0);
            out.push(bytes_to_node(&node)?);
        }
        Ok(out)
    }
}

pub(crate) struct PgPayloadStore {
    ctx: PgCtx,
    cache: RefCell<HashMap<NodeId, Option<CachedPayloadRow>>>,
}

impl PgPayloadStore {
    pub(crate) fn new(ctx: PgCtx) -> Self {
        Self {
            ctx,
            cache: RefCell::new(HashMap::new()),
        }
    }

    fn load_payload_row(&self, node: NodeId) -> Result<Option<CachedPayloadRow>> {
        let started_at = Instant::now();
        let node_bytes = node_to_bytes(node);
        let mut c = self.ctx.client.borrow_mut();
        let stmt = self.ctx.stmt(
            &mut c,
            "SELECT payload, last_lamport, last_replica, last_counter \
             FROM treecrdt_payload WHERE doc_id = $1 AND node = $2 LIMIT 1",
        )?;
        let rows = c
            .query(&stmt, &[&self.ctx.doc_id, &node_bytes.as_slice()])
            .map_err(storage_debug)?;

        let Some(row) = rows.first() else {
            return Ok(None);
        };
        let payload: Option<Vec<u8>> = row.get(0);
        let last_lamport = row.get::<_, i64>(1).max(0) as Lamport;
        let last_replica: Vec<u8> = row.get(2);
        let last_counter = row.get::<_, i64>(3).max(0) as u64;
        if let Some(profile) = &self.ctx.append_profile {
            let elapsed_ms = started_at.elapsed().as_secs_f64() * 1000.0;
            let mut profile = profile.borrow_mut();
            profile.payload_load_count += 1;
            profile.payload_load_ms += elapsed_ms;
        }
        Ok(Some(CachedPayloadRow {
            payload,
            last_lamport,
            last_replica,
            last_counter,
        }))
    }

    fn payload_row(&self, node: NodeId) -> Result<Option<CachedPayloadRow>> {
        if let Some(cached) = self.cache.borrow().get(&node) {
            return Ok(cached.clone());
        }
        let loaded = self.load_payload_row(node)?;
        self.cache.borrow_mut().insert(node, loaded.clone());
        Ok(loaded)
    }
}

impl treecrdt_core::PayloadStore for PgPayloadStore {
    fn reset(&mut self) -> Result<()> {
        self.cache.borrow_mut().clear();
        let mut c = self.ctx.client.borrow_mut();
        let stmt = self.ctx.stmt(&mut c, "DELETE FROM treecrdt_payload WHERE doc_id = $1")?;
        c.execute(&stmt, &[&self.ctx.doc_id]).map_err(storage_debug)?;
        Ok(())
    }

    fn payload(&self, node: NodeId) -> Result<Option<Vec<u8>>> {
        let Some(row) = self.payload_row(node)? else {
            return Ok(None);
        };
        Ok(row.payload)
    }

    fn last_writer(&self, node: NodeId) -> Result<Option<(Lamport, OperationId)>> {
        let Some(row) = self.payload_row(node)? else {
            return Ok(None);
        };
        Ok(Some((
            row.last_lamport,
            OperationId {
                replica: ReplicaId(row.last_replica),
                counter: row.last_counter,
            },
        )))
    }

    fn set_payload(
        &mut self,
        node: NodeId,
        payload: Option<Vec<u8>>,
        writer: (Lamport, OperationId),
    ) -> Result<()> {
        let started_at = Instant::now();
        let row_exists = matches!(self.cache.borrow().get(&node), Some(Some(_)));
        let node_bytes = node_to_bytes(node);
        let (lamport, id) = writer;
        let OperationId { replica, counter } = id;
        let ReplicaId(replica_bytes) = replica;
        let mut c = self.ctx.client.borrow_mut();
        if row_exists {
            let stmt = self.ctx.stmt(
                &mut c,
                "UPDATE treecrdt_payload \
                 SET payload = $3, last_lamport = $4, last_replica = $5, last_counter = $6 \
                 WHERE doc_id = $1 AND node = $2",
            )?;
            c.execute(
                &stmt,
                &[
                    &self.ctx.doc_id,
                    &node_bytes.as_slice(),
                    &payload,
                    &(lamport as i64),
                    &replica_bytes,
                    &(counter as i64),
                ],
            )
            .map_err(storage_debug)?;
        } else {
            let stmt = self.ctx.stmt(
                &mut c,
                "INSERT INTO treecrdt_payload(doc_id, node, payload, last_lamport, last_replica, last_counter) VALUES ($1,$2,$3,$4,$5,$6)",
            )?;
            c.execute(
                &stmt,
                &[
                    &self.ctx.doc_id,
                    &node_bytes.as_slice(),
                    &payload,
                    &(lamport as i64),
                    &replica_bytes,
                    &(counter as i64),
                ],
            )
            .map_err(storage_debug)?;
        }

        self.cache.borrow_mut().insert(
            node,
            Some(CachedPayloadRow {
                payload,
                last_lamport: lamport,
                last_replica: replica_bytes,
                last_counter: counter,
            }),
        );
        if let Some(profile) = &self.ctx.append_profile {
            let elapsed_ms = started_at.elapsed().as_secs_f64() * 1000.0;
            let mut profile = profile.borrow_mut();
            profile.payload_set_count += 1;
            profile.payload_set_ms += elapsed_ms;
        }
        Ok(())
    }
}

pub(crate) struct PgParentOpIndex {
    ctx: PgCtx,
    pending: Vec<PendingParentOpRefRow>,
}

impl PgParentOpIndex {
    pub(crate) fn new(ctx: PgCtx) -> Self {
        Self {
            ctx,
            pending: Vec::new(),
        }
    }

    // Core records parent->op_ref relationships as it applies ops, but we buffer those rows here
    // and insert them once per batch to avoid churning treecrdt_oprefs_children on every op.
    pub(crate) fn flush(&mut self) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }

        let started_at = Instant::now();
        let mut parents: Vec<Vec<u8>> = Vec::with_capacity(self.pending.len());
        let mut op_refs: Vec<Vec<u8>> = Vec::with_capacity(self.pending.len());
        let mut seqs: Vec<i64> = Vec::with_capacity(self.pending.len());
        for row in self.pending.drain(..) {
            parents.push(row.parent);
            op_refs.push(row.op_ref);
            seqs.push(row.seq);
        }

        let mut c = self.ctx.client.borrow_mut();
        // INSERT .. DO NOTHING keeps this flush idempotent if the same parent/op_ref pair was
        // buffered more than once while applying a batch.
        let stmt = self.ctx.stmt(
            &mut c,
            "INSERT INTO treecrdt_oprefs_children(doc_id, parent, op_ref, seq) \
             SELECT $1, src.parent, src.op_ref, src.seq \
             FROM unnest($2::bytea[], $3::bytea[], $4::bigint[]) AS src(parent, op_ref, seq) \
             ON CONFLICT (doc_id, parent, op_ref) DO NOTHING",
        )?;
        c.execute(&stmt, &[&self.ctx.doc_id, &parents, &op_refs, &seqs])
            .map_err(storage_debug)?;

        if let Some(profile) = &self.ctx.append_profile {
            let elapsed_ms = started_at.elapsed().as_secs_f64() * 1000.0;
            let mut profile = profile.borrow_mut();
            profile.index_record_count += parents.len() as u64;
            profile.index_record_ms += elapsed_ms;
        }

        Ok(())
    }
}

impl treecrdt_core::ParentOpIndex for PgParentOpIndex {
    fn reset(&mut self) -> Result<()> {
        self.pending.clear();
        let mut c = self.ctx.client.borrow_mut();
        let stmt = self.ctx.stmt(
            &mut c,
            "DELETE FROM treecrdt_oprefs_children WHERE doc_id = $1",
        )?;
        c.execute(&stmt, &[&self.ctx.doc_id]).map_err(storage_debug)?;
        Ok(())
    }

    fn record(&mut self, parent: NodeId, op_id: &OperationId, seq: u64) -> Result<()> {
        if parent == NodeId::TRASH {
            return Ok(());
        }
        self.pending.push(PendingParentOpRefRow {
            parent: node_to_bytes(parent).to_vec(),
            op_ref: derive_op_ref_v0(&self.ctx.doc_id, op_id.replica.as_bytes(), op_id.counter)
                .to_vec(),
            seq: seq as i64,
        });
        if self.pending.len() >= PARENT_OP_INDEX_FLUSH_SIZE {
            self.flush()?;
        }
        Ok(())
    }
}

const PARENT_OP_INDEX_FLUSH_SIZE: usize = 4096;

struct PendingParentOpRefRow {
    parent: Vec<u8>,
    op_ref: Vec<u8>,
    seq: i64,
}

pub(crate) struct PgOpStorage {
    ctx: PgCtx,
}

impl PgOpStorage {
    pub(crate) fn new(ctx: PgCtx) -> Self {
        Self { ctx }
    }
}

impl Storage for PgOpStorage {
    fn apply(&mut self, op: Operation) -> Result<bool> {
        let mut c = self.ctx.client.borrow_mut();
        let inserted = insert_op_in_tx(&self.ctx, &mut c, &op)?;
        drop(c);
        if inserted {
            upsert_replica_counters_in_tx(
                &self.ctx.client,
                &self.ctx.doc_id,
                std::slice::from_ref(&op),
            )?;
        }
        Ok(inserted)
    }

    fn load_since(&self, lamport: Lamport) -> Result<Vec<Operation>> {
        let mut c = self.ctx.client.borrow_mut();
        let stmt = self.ctx.stmt(
            &mut c,
            "SELECT lamport, replica, counter, kind, parent, node, new_parent, order_key, payload, known_state \
             FROM treecrdt_ops WHERE doc_id = $1 AND lamport > $2 ORDER BY lamport, replica, counter",
        )?;
        let rows = c.query(&stmt, &[&self.ctx.doc_id, &(lamport as i64)]).map_err(storage_debug)?;
        rows.into_iter().map(row_to_op).collect()
    }

    fn latest_lamport(&self) -> Lamport {
        let mut c = self.ctx.client.borrow_mut();
        let stmt = match self.ctx.stmt(
            &mut c,
            "SELECT COALESCE(MAX(lamport), 0) FROM treecrdt_ops WHERE doc_id = $1",
        ) {
            Ok(stmt) => stmt,
            Err(_) => return 0,
        };
        let rows = c.query(&stmt, &[&self.ctx.doc_id]).ok();
        match rows.and_then(|r| r.first().map(|row| row.get::<_, i64>(0))) {
            Some(v) => v.max(0) as Lamport,
            None => 0,
        }
    }

    fn latest_counter(&self, replica: &ReplicaId) -> Result<u64> {
        let mut c = self.ctx.client.borrow_mut();
        let stmt = self.ctx.stmt(
            &mut c,
            "SELECT COALESCE(MAX(counter), 0) \
             FROM treecrdt_ops WHERE doc_id = $1 AND replica = $2",
        )?;
        let rows = c.query(&stmt, &[&self.ctx.doc_id, &replica.0]).map_err(storage_debug)?;
        let row = rows.first().ok_or_else(|| Error::Storage("missing MAX(counter) row".into()))?;
        Ok(row.get::<_, i64>(0).max(0) as u64)
    }

    fn scan_since(
        &self,
        lamport: Lamport,
        visit: &mut dyn FnMut(Operation) -> Result<()>,
    ) -> Result<()> {
        let ops = self.load_since(lamport)?;
        for op in ops {
            visit(op)?;
        }
        Ok(())
    }
}

pub(crate) fn row_to_op(row: Row) -> Result<Operation> {
    let lamport = row.get::<_, i64>(0).max(0) as Lamport;
    let replica: Vec<u8> = row.get(1);
    let counter = row.get::<_, i64>(2).max(0) as u64;
    let kind: String = row.get(3);
    let parent: Option<Vec<u8>> = row.get(4);
    let node: Vec<u8> = row.get(5);
    let new_parent: Option<Vec<u8>> = row.get(6);
    let order_key: Option<Vec<u8>> = row.get(7);
    let payload: Option<Vec<u8>> = row.get(8);
    let known_state_bytes: Option<Vec<u8>> = row.get(9);
    let known_state = match known_state_bytes {
        None => None,
        Some(b) if b.is_empty() => None,
        Some(b) => Some(vv_from_bytes(&b)?),
    };

    let replica_id = ReplicaId(replica);
    let op_id = OperationId::new(&replica_id, counter);
    let meta = treecrdt_core::OperationMetadata {
        id: op_id,
        lamport,
        known_state,
    };

    let kind = match kind.as_str() {
        "insert" => {
            let parent = parent.ok_or_else(|| Error::Storage("insert op missing parent".into()))?;
            let order_key =
                order_key.ok_or_else(|| Error::Storage("insert op missing order_key".into()))?;
            OperationKind::Insert {
                parent: bytes_to_node(&parent)?,
                node: bytes_to_node(&node)?,
                order_key,
                payload,
            }
        }
        "move" => {
            let new_parent =
                new_parent.ok_or_else(|| Error::Storage("move op missing new_parent".into()))?;
            let order_key =
                order_key.ok_or_else(|| Error::Storage("move op missing order_key".into()))?;
            OperationKind::Move {
                node: bytes_to_node(&node)?,
                new_parent: bytes_to_node(&new_parent)?,
                order_key,
            }
        }
        "delete" => OperationKind::Delete {
            node: bytes_to_node(&node)?,
        },
        "tombstone" => OperationKind::Tombstone {
            node: bytes_to_node(&node)?,
        },
        "payload" => OperationKind::Payload {
            node: bytes_to_node(&node)?,
            payload,
        },
        other => return Err(Error::Storage(format!("unknown op kind: {other}"))),
    };

    Ok(Operation { meta, kind })
}

pub(crate) fn row_to_op_at(row: &Row, base: usize) -> Result<Operation> {
    let lamport = row.get::<_, i64>(base).max(0) as Lamport;
    let replica: Vec<u8> = row.get(base + 1);
    let counter = row.get::<_, i64>(base + 2).max(0) as u64;
    let kind: String = row.get(base + 3);
    let parent: Option<Vec<u8>> = row.get(base + 4);
    let node: Vec<u8> = row.get(base + 5);
    let new_parent: Option<Vec<u8>> = row.get(base + 6);
    let order_key: Option<Vec<u8>> = row.get(base + 7);
    let payload: Option<Vec<u8>> = row.get(base + 8);
    let known_state_bytes: Option<Vec<u8>> = row.get(base + 9);
    let known_state = match known_state_bytes {
        None => None,
        Some(b) if b.is_empty() => None,
        Some(b) => Some(vv_from_bytes(&b)?),
    };

    let replica_id = ReplicaId(replica);
    let op_id = OperationId::new(&replica_id, counter);
    let meta = treecrdt_core::OperationMetadata {
        id: op_id,
        lamport,
        known_state,
    };

    let kind = match kind.as_str() {
        "insert" => {
            let parent = parent.ok_or_else(|| Error::Storage("insert op missing parent".into()))?;
            let order_key =
                order_key.ok_or_else(|| Error::Storage("insert op missing order_key".into()))?;
            OperationKind::Insert {
                parent: bytes_to_node(&parent)?,
                node: bytes_to_node(&node)?,
                order_key,
                payload,
            }
        }
        "move" => {
            let new_parent =
                new_parent.ok_or_else(|| Error::Storage("move op missing new_parent".into()))?;
            let order_key =
                order_key.ok_or_else(|| Error::Storage("move op missing order_key".into()))?;
            OperationKind::Move {
                node: bytes_to_node(&node)?,
                new_parent: bytes_to_node(&new_parent)?,
                order_key,
            }
        }
        "delete" => OperationKind::Delete {
            node: bytes_to_node(&node)?,
        },
        "tombstone" => OperationKind::Tombstone {
            node: bytes_to_node(&node)?,
        },
        "payload" => OperationKind::Payload {
            node: bytes_to_node(&node)?,
            payload,
        },
        other => return Err(Error::Storage(format!("unknown op kind: {other}"))),
    };

    Ok(Operation { meta, kind })
}

struct OpDbFields {
    kind: &'static str,
    parent: Option<Vec<u8>>,
    node: Vec<u8>,
    new_parent: Option<Vec<u8>>,
    order_key: Option<Vec<u8>>,
    payload: Option<Vec<u8>>,
    known_state: Option<Vec<u8>>,
}

fn op_kind_to_db(op: &Operation) -> Result<OpDbFields> {
    let known_state = match op.meta.known_state.as_ref() {
        None => None,
        Some(vv) => Some(vv_to_bytes(vv)?),
    };

    match &op.kind {
        OperationKind::Insert {
            parent,
            node,
            order_key,
            payload,
        } => Ok(OpDbFields {
            kind: "insert",
            parent: Some(node_to_bytes(*parent).to_vec()),
            node: node_to_bytes(*node).to_vec(),
            new_parent: None,
            order_key: Some(order_key.clone()),
            payload: payload.clone(),
            known_state,
        }),
        OperationKind::Move {
            node,
            new_parent,
            order_key,
        } => Ok(OpDbFields {
            kind: "move",
            parent: None,
            node: node_to_bytes(*node).to_vec(),
            new_parent: Some(node_to_bytes(*new_parent).to_vec()),
            order_key: Some(order_key.clone()),
            payload: None,
            known_state,
        }),
        OperationKind::Delete { node } => {
            let Some(vv) = op.meta.known_state.as_ref() else {
                return Err(Error::InvalidOperation(
                    "treecrdt: delete operations require meta.known_state".into(),
                ));
            };
            let bytes = vv_to_bytes(vv)?;
            if bytes.is_empty() {
                return Err(Error::InvalidOperation(
                    "treecrdt: delete known_state must not be empty".into(),
                ));
            }
            Ok(OpDbFields {
                kind: "delete",
                parent: None,
                node: node_to_bytes(*node).to_vec(),
                new_parent: None,
                order_key: None,
                payload: None,
                known_state: Some(bytes),
            })
        }
        OperationKind::Tombstone { node } => Ok(OpDbFields {
            kind: "tombstone",
            parent: None,
            node: node_to_bytes(*node).to_vec(),
            new_parent: None,
            order_key: None,
            payload: None,
            known_state,
        }),
        OperationKind::Payload { node, payload } => Ok(OpDbFields {
            kind: "payload",
            parent: None,
            node: node_to_bytes(*node).to_vec(),
            new_parent: None,
            order_key: None,
            payload: payload.clone(),
            known_state,
        }),
    }
}

fn upsert_replica_counters_in_tx(
    client: &Rc<RefCell<Client>>,
    doc_id: &str,
    ops: &[Operation],
) -> Result<()> {
    if ops.is_empty() {
        return Ok(());
    }

    let mut per_replica: HashMap<Vec<u8>, i64> = HashMap::new();
    for op in ops {
        let replica = op.meta.id.replica.as_bytes().to_vec();
        let counter = (op.meta.id.counter.min(i64::MAX as u64)) as i64;
        per_replica
            .entry(replica)
            .and_modify(|current| *current = (*current).max(counter))
            .or_insert(counter);
    }

    let mut replicas = Vec::with_capacity(per_replica.len());
    let mut counters = Vec::with_capacity(per_replica.len());
    for (replica, counter) in per_replica {
        replicas.push(replica);
        counters.push(counter);
    }

    let mut c = client.borrow_mut();
    c.execute(
        "INSERT INTO treecrdt_replica_meta (doc_id, replica, max_counter) \
         SELECT $1, src.replica, src.max_counter \
         FROM unnest($2::bytea[], $3::bigint[]) AS src(replica, max_counter) \
         ON CONFLICT (doc_id, replica) DO UPDATE \
         SET max_counter = GREATEST(treecrdt_replica_meta.max_counter, EXCLUDED.max_counter)",
        &[&doc_id, &replicas, &counters],
    )
    .map_err(storage_debug)?;
    Ok(())
}

fn insert_op_in_tx(ctx: &PgCtx, c: &mut Client, op: &Operation) -> Result<bool> {
    let replica = op.meta.id.replica.as_bytes();
    let counter = op.meta.id.counter;
    let op_ref = derive_op_ref_v0(&ctx.doc_id, replica, counter);
    let row = op_kind_to_db(op)?;

    let stmt = ctx.stmt(
        c,
        "INSERT INTO treecrdt_ops (doc_id, op_ref, lamport, replica, counter, kind, parent, node, new_parent, order_key, payload, known_state) \
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) \
         ON CONFLICT (doc_id, op_ref) DO NOTHING",
    )?;
    let inserted = c
        .execute(
            &stmt,
            &[
                &ctx.doc_id,
                &op_ref.as_slice(),
                &(op.meta.lamport as i64),
                &replica,
                &(counter as i64),
                &row.kind,
                &row.parent,
                &row.node,
                &row.new_parent,
                &row.order_key,
                &row.payload,
                &row.known_state,
            ],
        )
        .map_err(storage_debug)?;
    Ok(inserted > 0)
}

fn bulk_insert_ops_in_tx(ctx: &PgCtx, c: &mut Client, ops: &[Operation]) -> Result<Vec<Vec<u8>>> {
    if ops.is_empty() {
        return Ok(Vec::new());
    }

    let mut op_refs: Vec<Vec<u8>> = Vec::with_capacity(ops.len());
    let mut lamports: Vec<i64> = Vec::with_capacity(ops.len());
    let mut replicas: Vec<Vec<u8>> = Vec::with_capacity(ops.len());
    let mut counters: Vec<i64> = Vec::with_capacity(ops.len());
    let mut kinds: Vec<String> = Vec::with_capacity(ops.len());
    let mut parents: Vec<Option<Vec<u8>>> = Vec::with_capacity(ops.len());
    let mut nodes: Vec<Vec<u8>> = Vec::with_capacity(ops.len());
    let mut new_parents: Vec<Option<Vec<u8>>> = Vec::with_capacity(ops.len());
    let mut order_keys: Vec<Option<Vec<u8>>> = Vec::with_capacity(ops.len());
    let mut payloads: Vec<Option<Vec<u8>>> = Vec::with_capacity(ops.len());
    let mut known_states: Vec<Option<Vec<u8>>> = Vec::with_capacity(ops.len());

    for op in ops {
        let replica = op.meta.id.replica.as_bytes();
        let counter = op.meta.id.counter;
        let op_ref = derive_op_ref_v0(&ctx.doc_id, replica, counter);
        let row = op_kind_to_db(op)?;

        op_refs.push(op_ref.to_vec());
        lamports.push(op.meta.lamport as i64);
        replicas.push(replica.to_vec());
        counters.push(counter as i64);
        kinds.push(row.kind.to_string());
        parents.push(row.parent);
        nodes.push(row.node);
        new_parents.push(row.new_parent);
        order_keys.push(row.order_key);
        payloads.push(row.payload);
        known_states.push(row.known_state);
    }

    let stmt = ctx.stmt(
        c,
        "INSERT INTO treecrdt_ops (doc_id, op_ref, lamport, replica, counter, kind, parent, node, new_parent, order_key, payload, known_state) \
         SELECT \
           $1, \
           src.op_ref, src.lamport, src.replica, src.counter, src.kind, src.parent, src.node, src.new_parent, src.order_key, src.payload, src.known_state \
         FROM unnest( \
           $2::bytea[], \
           $3::bigint[], \
           $4::bytea[], \
           $5::bigint[], \
           $6::text[], \
           $7::bytea[], \
           $8::bytea[], \
           $9::bytea[], \
           $10::bytea[], \
           $11::bytea[], \
           $12::bytea[] \
         ) AS src(op_ref, lamport, replica, counter, kind, parent, node, new_parent, order_key, payload, known_state) \
         ON CONFLICT (doc_id, op_ref) DO NOTHING \
         RETURNING op_ref",
    )?;

    let rows = c
        .query(
            &stmt,
            &[
                &ctx.doc_id,
                &op_refs,
                &lamports,
                &replicas,
                &counters,
                &kinds,
                &parents,
                &nodes,
                &new_parents,
                &order_keys,
                &payloads,
                &known_states,
            ],
        )
        .map_err(storage_debug)?;

    let mut inserted = Vec::with_capacity(rows.len());
    for row in rows {
        inserted.push(row.get::<_, Vec<u8>>(0));
    }
    Ok(inserted)
}

fn bulk_insert_fixture_nodes_in_tx(
    ctx: &PgCtx,
    c: &mut Client,
    nodes: &[Vec<u8>],
    parents: &[Option<Vec<u8>>],
    order_keys: &[Option<Vec<u8>>],
    last_changes: &[Option<Vec<u8>>],
) -> Result<()> {
    if nodes.is_empty() {
        return Ok(());
    }

    let stmt = ctx.stmt(
        c,
        "INSERT INTO treecrdt_nodes(doc_id, node, parent, order_key, tombstone, last_change, deleted_at) \
         SELECT $1, src.node, src.parent, src.order_key, FALSE, src.last_change, NULL::bytea \
         FROM unnest($2::bytea[], $3::bytea[], $4::bytea[], $5::bytea[]) \
           AS src(node, parent, order_key, last_change)",
    )?;
    c.execute(
        &stmt,
        &[&ctx.doc_id, &nodes, &parents, &order_keys, &last_changes],
    )
    .map_err(storage_debug)?;
    Ok(())
}

fn bulk_insert_fixture_index_rows_in_tx(
    ctx: &PgCtx,
    c: &mut Client,
    parents: &[Vec<u8>],
    op_refs: &[Vec<u8>],
    seqs: &[i64],
) -> Result<()> {
    if parents.is_empty() {
        return Ok(());
    }

    let stmt = ctx.stmt(
        c,
        "INSERT INTO treecrdt_oprefs_children(doc_id, parent, op_ref, seq) \
         SELECT $1, src.parent, src.op_ref, src.seq \
         FROM unnest($2::bytea[], $3::bytea[], $4::bigint[]) AS src(parent, op_ref, seq)",
    )?;
    c.execute(&stmt, &[&ctx.doc_id, &parents, &op_refs, &seqs])
        .map_err(storage_debug)?;
    Ok(())
}

fn upsert_replica_counter_in_tx(
    client: &Rc<RefCell<Client>>,
    doc_id: &str,
    replica: &[u8],
    max_counter: u64,
) -> Result<()> {
    let mut c = client.borrow_mut();
    c.execute(
        "INSERT INTO treecrdt_replica_meta (doc_id, replica, max_counter) \
         VALUES ($1, $2, $3) \
         ON CONFLICT (doc_id, replica) DO UPDATE \
         SET max_counter = GREATEST(treecrdt_replica_meta.max_counter, EXCLUDED.max_counter)",
        &[
            &doc_id,
            &replica,
            &((max_counter.min(i64::MAX as u64)) as i64),
        ],
    )
    .map_err(storage_debug)?;
    Ok(())
}

fn select_inserted_ops(
    ctx: &PgCtx,
    ops: &[Operation],
    inserted_op_refs: Vec<Vec<u8>>,
) -> Vec<Operation> {
    if inserted_op_refs.is_empty() {
        return Vec::new();
    }
    if inserted_op_refs.len() == ops.len() {
        return ops.to_vec();
    }

    // bulk_insert_ops_in_tx returns exactly the op_refs Postgres accepted.
    // Preserve multiplicity here so a batch like [opA, opA, opB] only
    // materializes [opA, opB] instead of replaying opA twice.
    let mut remaining_by_ref: HashMap<Vec<u8>, usize> = HashMap::new();
    for op_ref in inserted_op_refs {
        *remaining_by_ref.entry(op_ref).or_insert(0) += 1;
    }

    let mut inserted_ops = Vec::new();
    for op in ops {
        let replica = op.meta.id.replica.as_bytes();
        let counter = op.meta.id.counter;
        let op_ref = derive_op_ref_v0(&ctx.doc_id, replica, counter);
        let Some(remaining) = remaining_by_ref.get_mut(op_ref.as_slice()) else {
            continue;
        };
        if *remaining == 0 {
            continue;
        }
        *remaining -= 1;
        inserted_ops.push(op.clone());
    }

    inserted_ops
}

fn materialize_ops_in_order(ctx: PgCtx, meta: &TreeMeta, ops: Vec<Operation>) -> Result<()> {
    if ops.is_empty() {
        return Ok(());
    }

    // At this point treecrdt_ops already contains the inserted operations. This temporary
    // TreeCrdt exists only to replay those ops through core semantics and update derived tables.
    let nodes = PgNodeStore::new(ctx.clone());
    if ops.iter().any(|op| matches!(op.kind, OperationKind::Payload { .. })) {
        // Payload ops can depend on the current node row, so front-load the reads here.
        nodes.preload_for_ops(&ops)?;
    }
    let node_flush = nodes.clone();
    let payloads = PgPayloadStore::new(ctx.clone());
    let mut index = PgParentOpIndex::new(ctx.clone());

    // NoopStorage is intentional: append_ops_in_tx already persisted the op log, and this pass is
    // only responsible for materialized state (nodes/payload/index/head) through treecrdt-core.
    let mut crdt = TreeCrdt::with_stores(
        ReplicaId::new(b"postgres"),
        NoopStorage,
        LamportClock::default(),
        nodes,
        payloads,
    )?;

    let materialize_started_at = Instant::now();
    // This is the main handoff into treecrdt-core for remote/bulk append batches:
    // defensive delete + revival, payload LWW, tombstone refresh, and oprefs_children updates.
    let next = apply_incremental_ops(&mut crdt, &mut index, meta, ops)?
        .ok_or_else(|| Error::Storage("expected head op after materialization".into()))?;
    node_flush.flush_last_change()?;
    index.flush()?;
    if let Some(profile) = &ctx.append_profile {
        profile.borrow_mut().materialize_ms +=
            materialize_started_at.elapsed().as_secs_f64() * 1000.0;
    }
    let update_head_started_at = Instant::now();
    update_tree_meta_head(
        &ctx.client,
        &ctx.doc_id,
        next.lamport,
        &next.replica,
        next.counter,
        next.seq,
    )?;
    if let Some(profile) = &ctx.append_profile {
        profile.borrow_mut().update_head_ms +=
            update_head_started_at.elapsed().as_secs_f64() * 1000.0;
    }
    Ok(())
}

pub fn append_ops(client: &Rc<RefCell<Client>>, doc_id: &str, ops: &[Operation]) -> Result<u64> {
    {
        let mut c = client.borrow_mut();
        c.batch_execute("BEGIN").map_err(|e| Error::Storage(e.to_string()))?;
    }

    let res = append_ops_in_tx(client, doc_id, ops);

    match res {
        Ok(v) => {
            let mut c = client.borrow_mut();
            c.batch_execute("COMMIT").map_err(|e| Error::Storage(e.to_string()))?;
            Ok(v)
        }
        Err(e) => {
            let mut c = client.borrow_mut();
            let _ = c.batch_execute("ROLLBACK");
            Err(e)
        }
    }
}

fn run_in_tx<T>(client: &Rc<RefCell<Client>>, f: impl FnOnce() -> Result<T>) -> Result<T> {
    {
        let mut c = client.borrow_mut();
        c.batch_execute("BEGIN").map_err(|e| Error::Storage(e.to_string()))?;
    }

    let res = f();

    match res {
        Ok(v) => {
            let mut c = client.borrow_mut();
            c.batch_execute("COMMIT").map_err(|e| Error::Storage(e.to_string()))?;
            Ok(v)
        }
        Err(e) => {
            let mut c = client.borrow_mut();
            let _ = c.batch_execute("ROLLBACK");
            Err(e)
        }
    }
}

pub fn prime_doc_for_tests(
    client: &Rc<RefCell<Client>>,
    doc_id: &str,
    ops: &[Operation],
) -> Result<()> {
    run_in_tx(client, || {
        {
            let mut c = client.borrow_mut();
            reset_doc_for_tests_within_tx(&mut c, doc_id)?;
        }

        if ops.is_empty() {
            return Ok(());
        }

        ensure_doc_meta(client, doc_id)?;
        set_tree_meta_dirty(client, doc_id, true)?;

        let ctx = PgCtx::new_assume_doc_meta(client.clone(), doc_id)?;
        {
            let mut c = client.borrow_mut();
            bulk_insert_ops_in_tx(&ctx, &mut c, ops)?;
        }
        upsert_replica_counters_in_tx(client, doc_id, ops)?;
        Ok(())
    })?;

    if ops.is_empty() {
        return Ok(());
    }

    // Fixture import wants one bulk append followed by one rebuild, rather than eager
    // incremental materialization during the append path.
    ensure_materialized(client, doc_id)
}

pub fn prime_balanced_fanout_doc_for_tests(
    client: &Rc<RefCell<Client>>,
    doc_id: &str,
    size: usize,
    fanout: usize,
    payload_bytes: usize,
    replica_label: &str,
) -> Result<()> {
    if size == 0 {
        return Err(Error::Storage("fixture size must be positive".into()));
    }
    if fanout == 0 {
        return Err(Error::Storage("fixture fanout must be positive".into()));
    }
    if payload_bytes > 0 && size <= fanout {
        return Err(Error::Storage(format!(
            "payload fixture requires size > fanout ({fanout})"
        )));
    }

    const BATCH_SIZE: usize = 50_000;

    {
        let mut c = client.borrow_mut();
        reset_doc_for_tests(&mut c, doc_id)?;
    }

    ensure_doc_meta(client, doc_id)?;

    let replica = repeated_replica_from_label(replica_label)?;
    let replica_bytes = replica.as_bytes().to_vec();
    let payload_counter = (size + 1) as u64;
    let total_ops = if payload_bytes > 0 {
        payload_counter
    } else {
        size as u64
    };

    run_in_tx(client, || {
        let ctx = PgCtx::new_assume_doc_meta(client.clone(), doc_id)?;
        set_tree_meta_dirty(client, doc_id, true)?;

        let root_last_change = if size == 0 {
            None
        } else {
            Some(single_counter_vv_bytes(&replica, size.min(fanout) as u64)?)
        };

        {
            let mut c = client.borrow_mut();
            bulk_insert_fixture_nodes_in_tx(
                &ctx,
                &mut c,
                &[node_to_bytes(NodeId::ROOT).to_vec()],
                &[None],
                &[Some(Vec::new())],
                &[root_last_change],
            )?;
        }

        let mut batch_ops = Vec::with_capacity(BATCH_SIZE);
        let mut node_rows: Vec<Vec<u8>> = Vec::with_capacity(BATCH_SIZE);
        let mut node_parents: Vec<Option<Vec<u8>>> = Vec::with_capacity(BATCH_SIZE);
        let mut node_order_keys: Vec<Option<Vec<u8>>> = Vec::with_capacity(BATCH_SIZE);
        let mut node_last_changes: Vec<Option<Vec<u8>>> = Vec::with_capacity(BATCH_SIZE);
        let mut index_parents: Vec<Vec<u8>> = Vec::with_capacity(BATCH_SIZE);
        let mut index_op_refs: Vec<Vec<u8>> = Vec::with_capacity(BATCH_SIZE);
        let mut index_seqs: Vec<i64> = Vec::with_capacity(BATCH_SIZE);

        let flush_batch = |batch_ops: &mut Vec<Operation>,
                           node_rows: &mut Vec<Vec<u8>>,
                           node_parents: &mut Vec<Option<Vec<u8>>>,
                           node_order_keys: &mut Vec<Option<Vec<u8>>>,
                           node_last_changes: &mut Vec<Option<Vec<u8>>>,
                           index_parents: &mut Vec<Vec<u8>>,
                           index_op_refs: &mut Vec<Vec<u8>>,
                           index_seqs: &mut Vec<i64>|
         -> Result<()> {
            if batch_ops.is_empty() {
                return Ok(());
            }
            {
                let mut c = client.borrow_mut();
                bulk_insert_ops_in_tx(&ctx, &mut c, batch_ops)?;
                bulk_insert_fixture_nodes_in_tx(
                    &ctx,
                    &mut c,
                    node_rows,
                    node_parents,
                    node_order_keys,
                    node_last_changes,
                )?;
                bulk_insert_fixture_index_rows_in_tx(
                    &ctx,
                    &mut c,
                    index_parents,
                    index_op_refs,
                    index_seqs,
                )?;
            }
            batch_ops.clear();
            node_rows.clear();
            node_parents.clear();
            node_order_keys.clear();
            node_last_changes.clear();
            index_parents.clear();
            index_op_refs.clear();
            index_seqs.clear();
            Ok(())
        };

        for node_index in 1..=size {
            let (parent, position) = balanced_parent_and_position(node_index, fanout);
            let node = NodeId(node_index as u128);
            let order_key = order_key_from_position(position)?;
            let op = Operation::insert(
                &replica,
                node_index as u64,
                node_index as Lamport,
                parent,
                node,
                order_key.clone(),
            );
            let op_ref = derive_op_ref_v0(doc_id, replica.as_bytes(), node_index as u64).to_vec();
            let mut last_change_counter = node_index as u64;
            if let Some(child_max) = balanced_direct_child_max_counter(node_index, size, fanout) {
                last_change_counter = last_change_counter.max(child_max);
            }
            if payload_bytes > 0 && node_index == fanout + 1 {
                last_change_counter = last_change_counter.max(payload_counter);
            }

            batch_ops.push(op);
            node_rows.push(node_to_bytes(node).to_vec());
            node_parents.push(Some(node_to_bytes(parent).to_vec()));
            node_order_keys.push(Some(order_key));
            node_last_changes.push(Some(single_counter_vv_bytes(
                &replica,
                last_change_counter,
            )?));
            index_parents.push(node_to_bytes(parent).to_vec());
            index_op_refs.push(op_ref);
            index_seqs.push(node_index as i64);

            if batch_ops.len() >= BATCH_SIZE {
                flush_batch(
                    &mut batch_ops,
                    &mut node_rows,
                    &mut node_parents,
                    &mut node_order_keys,
                    &mut node_last_changes,
                    &mut index_parents,
                    &mut index_op_refs,
                    &mut index_seqs,
                )?;
            }
        }

        flush_batch(
            &mut batch_ops,
            &mut node_rows,
            &mut node_parents,
            &mut node_order_keys,
            &mut node_last_changes,
            &mut index_parents,
            &mut index_op_refs,
            &mut index_seqs,
        )?;

        if payload_bytes > 0 {
            let payload_node = NodeId((fanout + 1) as u128);
            let payload_node_bytes = node_to_bytes(payload_node);
            let payload_op = Operation::set_payload(
                &replica,
                payload_counter,
                payload_counter as Lamport,
                payload_node,
                payload_bytes_from_seed(10_000, payload_bytes),
            );
            let payload_parent = NodeId(1);
            let payload_op_ref =
                derive_op_ref_v0(doc_id, replica.as_bytes(), payload_counter).to_vec();
            let payload_bytes_value = payload_bytes_from_seed(10_000, payload_bytes);

            {
                let mut c = client.borrow_mut();
                bulk_insert_ops_in_tx(&ctx, &mut c, &[payload_op])?;
                let stmt = ctx.stmt(
                    &mut c,
                    "INSERT INTO treecrdt_payload(doc_id, node, payload, last_lamport, last_replica, last_counter) \
                     VALUES ($1, $2, $3, $4, $5, $6)",
                )?;
                c.execute(
                    &stmt,
                    &[
                        &ctx.doc_id,
                        &payload_node_bytes.as_slice(),
                        &payload_bytes_value,
                        &(payload_counter as i64),
                        &replica_bytes,
                        &(payload_counter as i64),
                    ],
                )
                .map_err(storage_debug)?;
                bulk_insert_fixture_index_rows_in_tx(
                    &ctx,
                    &mut c,
                    &[node_to_bytes(payload_parent).to_vec()],
                    &[payload_op_ref],
                    &[payload_counter as i64],
                )?;
            }
        }

        upsert_replica_counter_in_tx(client, doc_id, replica.as_bytes(), total_ops)?;
        update_tree_meta_head(
            client,
            doc_id,
            total_ops as Lamport,
            replica.as_bytes(),
            total_ops,
            total_ops,
        )?;
        Ok(())
    })?;

    Ok(())
}

fn append_ops_in_tx(client: &Rc<RefCell<Client>>, doc_id: &str, ops: &[Operation]) -> Result<u64> {
    // Serialize per-doc writers across all server instances (incremental materialization updates
    // derived tables + head_seq and is not safe to run concurrently for the same doc_id).
    let meta = load_tree_meta_for_update(client, doc_id)?;
    // This profiler is only for large-upload benchmark/debug runs. The normal
    // append path keeps the hook disabled and pays only the `OnceLock` check.
    let append_profile = append_profile_enabled().then(|| {
        Rc::new(RefCell::new(PgAppendProfile::new(
            ops.len(),
            meta.dirty,
            meta.head_seq(),
        )))
    });
    let ctx =
        PgCtx::new_with_profile_assume_doc_meta(client.clone(), doc_id, append_profile.clone())?;

    // treecrdt_ops is the source of truth. This function first appends/dedupes the op log in SQL
    // and only then decides whether it can replay the inserted subset through core materialization.
    //
    // If the doc is already dirty, derived tables are stale, so we only append to the op log and
    // leave full reconstruction to ensure_materialized_in_tx on a later read.
    if meta.dirty {
        let bulk_insert_started_at = Instant::now();
        let inserted = {
            let mut c = client.borrow_mut();
            bulk_insert_ops_in_tx(&ctx, &mut c, ops)?
        };
        if let Some(profile) = &append_profile {
            let mut profile = profile.borrow_mut();
            profile.bulk_insert_ms += bulk_insert_started_at.elapsed().as_secs_f64() * 1000.0;
            profile.bulk_inserted_ops += inserted.len();
        }
        let inserted_count = inserted.len();
        if inserted_count > 0 {
            let inserted_op_refs: HashSet<Vec<u8>> = inserted.into_iter().collect();
            let inserted_ops: Vec<Operation> = ops
                .iter()
                .filter(|op| {
                    let replica = op.meta.id.replica.as_bytes();
                    let counter = op.meta.id.counter;
                    inserted_op_refs
                        .contains(derive_op_ref_v0(&ctx.doc_id, replica, counter).as_slice())
                })
                .cloned()
                .collect();
            upsert_replica_counters_in_tx(client, doc_id, &inserted_ops)?;
            set_tree_meta_dirty(client, doc_id, true)?;
            if let Some(profile) = &append_profile {
                profile.borrow_mut().fallback_mark_dirty = true;
            }
        }
        let inserted_count = inserted_count.min(u64::MAX as usize) as u64;
        if let Some(profile) = &append_profile {
            profile.borrow().log(doc_id, inserted_count as usize);
        }
        return Ok(inserted_count);
    }

    let bulk_insert_started_at = Instant::now();
    let inserted_op_refs = {
        let mut c = client.borrow_mut();
        bulk_insert_ops_in_tx(&ctx, &mut c, ops)?
    };
    if let Some(profile) = &append_profile {
        let mut profile = profile.borrow_mut();
        profile.bulk_insert_ms += bulk_insert_started_at.elapsed().as_secs_f64() * 1000.0;
        profile.bulk_inserted_ops += inserted_op_refs.len();
    }

    if inserted_op_refs.is_empty() {
        if let Some(profile) = &append_profile {
            profile.borrow().log(doc_id, 0);
        }
        return Ok(0);
    }

    let dedupe_filter_started_at = Instant::now();
    // Only materialize the ops Postgres actually inserted. This keeps duplicate opRefs in the
    // input batch from being replayed twice through core materialization.
    let inserted_ops = select_inserted_ops(&ctx, ops, inserted_op_refs);
    if let Some(profile) = &append_profile {
        profile.borrow_mut().dedupe_filter_ms +=
            dedupe_filter_started_at.elapsed().as_secs_f64() * 1000.0;
    }

    if inserted_ops.is_empty() {
        if let Some(profile) = &append_profile {
            profile.borrow().log(doc_id, 0);
        }
        return Ok(0);
    }

    upsert_replica_counters_in_tx(client, doc_id, &inserted_ops)?;

    let inserted = inserted_ops.len();
    // If incremental materialization fails, keep the op-log append and mark the doc dirty so the
    // next rebuild replays the full log through the same core semantics.
    let _ = try_incremental_materialization(
        false,
        || materialize_ops_in_order(ctx, &meta, inserted_ops),
        || {
            let _ = set_tree_meta_dirty(client, doc_id, true);
            if let Some(profile) = &append_profile {
                profile.borrow_mut().fallback_mark_dirty = true;
            }
        },
    );

    if let Some(profile) = &append_profile {
        profile.borrow().log(doc_id, inserted);
    }

    Ok(inserted.min(u64::MAX as usize) as u64)
}

fn clear_materialized(client: &Rc<RefCell<Client>>, doc_id: &str) -> Result<()> {
    let mut c = client.borrow_mut();
    c.execute(
        "DELETE FROM treecrdt_oprefs_children WHERE doc_id = $1",
        &[&doc_id],
    )
    .map_err(|e| Error::Storage(e.to_string()))?;
    c.execute("DELETE FROM treecrdt_payload WHERE doc_id = $1", &[&doc_id])
        .map_err(|e| Error::Storage(e.to_string()))?;
    c.execute("DELETE FROM treecrdt_nodes WHERE doc_id = $1", &[&doc_id])
        .map_err(|e| Error::Storage(e.to_string()))?;
    Ok(())
}

pub fn ensure_materialized(client: &Rc<RefCell<Client>>, doc_id: &str) -> Result<()> {
    {
        let mut c = client.borrow_mut();
        c.batch_execute("BEGIN").map_err(|e| Error::Storage(e.to_string()))?;
    }

    let res = ensure_materialized_in_tx(client, doc_id);

    match res {
        Ok(()) => {
            let mut c = client.borrow_mut();
            c.batch_execute("COMMIT").map_err(|e| Error::Storage(e.to_string()))?;
            Ok(())
        }
        Err(e) => {
            let mut c = client.borrow_mut();
            let _ = c.batch_execute("ROLLBACK");
            Err(e)
        }
    }
}

fn ensure_materialized_in_tx(client: &Rc<RefCell<Client>>, doc_id: &str) -> Result<()> {
    let meta = load_tree_meta(client, doc_id)?;
    if !meta.dirty {
        return Ok(());
    }

    // Take a per-doc lock so rebuild can't race with concurrent append/materialization.
    let meta = load_tree_meta_for_update(client, doc_id)?;
    if !meta.dirty {
        return Ok(());
    }

    clear_materialized(client, doc_id)?;

    let ctx = PgCtx::new(client.clone(), doc_id)?;
    let storage = PgOpStorage::new(ctx.clone());
    let mut nodes = PgNodeStore::new(ctx.clone());
    let node_flush = nodes.clone();
    let mut payloads = PgPayloadStore::new(ctx.clone());
    let mut index = PgParentOpIndex::new(ctx.clone());

    nodes.reset()?;
    payloads.reset()?;
    index.reset()?;

    let mut crdt = TreeCrdt::with_stores(
        ReplicaId::new(b"postgres"),
        storage,
        LamportClock::default(),
        nodes,
        payloads,
    )?;
    // Full rebuild path: replay the persisted op log through treecrdt-core to reconstruct
    // nodes, payloads, subtree opRef index rows, and cached tombstone state from scratch.
    crdt.replay_from_storage_with_materialization(&mut index)?;
    node_flush.flush_last_change()?;
    index.flush()?;

    let seq = crdt.log_len().min(u64::MAX as usize) as u64;
    if let Some(last) = crdt.head_op() {
        update_tree_meta_head(
            client,
            doc_id,
            last.meta.lamport,
            last.meta.id.replica.as_bytes(),
            last.meta.id.counter,
            seq,
        )?;
    } else {
        update_tree_meta_head(client, doc_id, 0, &[], 0, 0)?;
    }

    Ok(())
}

pub(crate) fn ensure_materialized_and_load_meta_for_update_in_tx(
    client: &Rc<RefCell<Client>>,
    doc_id: &str,
) -> Result<TreeMeta> {
    ensure_doc_meta(client, doc_id)?;
    let meta = load_tree_meta_row(client, doc_id, true)?;
    if !meta.dirty {
        return Ok(meta);
    }

    clear_materialized(client, doc_id)?;

    let ctx = PgCtx::new_assume_doc_meta(client.clone(), doc_id)?;
    let storage = PgOpStorage::new(ctx.clone());
    let mut nodes = PgNodeStore::new(ctx.clone());
    let node_flush = nodes.clone();
    let mut payloads = PgPayloadStore::new(ctx.clone());
    let mut index = PgParentOpIndex::new(ctx.clone());

    nodes.reset()?;
    payloads.reset()?;
    index.reset()?;

    let mut crdt = TreeCrdt::with_stores(
        ReplicaId::new(b"postgres"),
        storage,
        LamportClock::default(),
        nodes,
        payloads,
    )?;
    crdt.replay_from_storage_with_materialization(&mut index)?;
    node_flush.flush_last_change()?;
    index.flush()?;

    let seq = crdt.log_len().min(u64::MAX as usize) as u64;
    if let Some(last) = crdt.head_op() {
        update_tree_meta_head(
            client,
            doc_id,
            last.meta.lamport,
            last.meta.id.replica.as_bytes(),
            last.meta.id.counter,
            seq,
        )?;
        return Ok(TreeMeta {
            dirty: false,
            head_lamport: last.meta.lamport,
            head_replica: last.meta.id.replica.as_bytes().to_vec(),
            head_counter: last.meta.id.counter,
            head_seq: seq,
        });
    }

    update_tree_meta_head(client, doc_id, 0, &[], 0, 0)?;
    Ok(TreeMeta {
        dirty: false,
        head_lamport: 0,
        head_replica: Vec::new(),
        head_counter: 0,
        head_seq: 0,
    })
}

pub(crate) fn replica_max_counter_in_tx(
    client: &Rc<RefCell<Client>>,
    doc_id: &str,
    replica: &[u8],
) -> Result<u64> {
    let ctx = PgCtx::new_assume_doc_meta(client.clone(), doc_id)?;
    let mut c = client.borrow_mut();
    let stmt = ctx.stmt(
        &mut c,
        "SELECT COALESCE(MAX(max_counter), 0) FROM treecrdt_replica_meta WHERE doc_id = $1 AND replica = $2",
    )?;
    let rows = c.query(&stmt, &[&doc_id, &replica]).map_err(storage_debug)?;
    let row = rows.first().ok_or_else(|| Error::Storage("missing MAX(counter) row".into()))?;
    Ok(row.get::<_, i64>(0).max(0) as u64)
}

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use postgres::{Client, Statement};

use treecrdt_core::{
    Error, Lamport, MaterializationCursor, MaterializationFrontier, MaterializationHead,
    MaterializationKey, MaterializationState, Result,
};

use crate::profile::PgAppendProfile;

use super::storage_debug;

#[derive(Clone, Debug)]
pub(crate) struct TreeMeta(pub(crate) MaterializationState);

impl MaterializationCursor for TreeMeta {
    fn state(&self) -> MaterializationState<&[u8]> {
        self.0.as_borrowed()
    }
}

pub(crate) fn ensure_doc_meta(client: &Rc<RefCell<Client>>, doc_id: &str) -> Result<()> {
    let mut c = client.borrow_mut();
    c.execute(
        "INSERT INTO treecrdt_meta(doc_id) VALUES ($1) ON CONFLICT (doc_id) DO NOTHING",
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
    let ctx = PgCtx::new(client.clone(), doc_id)?;
    let mut c = client.borrow_mut();
    let stmt = if for_update {
        ctx.stmt(
            &mut c,
            "SELECT head_lamport, head_replica, head_counter, head_seq, \
                    replay_lamport, replay_replica, replay_counter \
             FROM treecrdt_meta WHERE doc_id = $1 FOR UPDATE",
        )?
    } else {
        ctx.stmt(
            &mut c,
            "SELECT head_lamport, head_replica, head_counter, head_seq, \
                    replay_lamport, replay_replica, replay_counter \
             FROM treecrdt_meta WHERE doc_id = $1 LIMIT 1",
        )?
    };
    let rows = c.query(&stmt, &[&doc_id]).map_err(storage_debug)?;

    let row = rows.first().ok_or_else(|| Error::Storage("missing treecrdt_meta row".into()))?;

    let head_lamport = row.get::<_, i64>(0).max(0) as Lamport;
    let head_replica = row.get::<_, Vec<u8>>(1);
    let head_counter = row.get::<_, i64>(2).max(0) as u64;
    let head_seq = row.get::<_, i64>(3).max(0) as u64;
    let replay_lamport = row.get::<_, Option<i64>>(4).map(|v| v.max(0) as Lamport);
    let replay_replica = row.get::<_, Option<Vec<u8>>>(5);
    let replay_counter = row.get::<_, Option<i64>>(6).map(|v| v.max(0) as u64);

    let head = if head_seq == 0 && head_lamport == 0 && head_replica.is_empty() && head_counter == 0
    {
        None
    } else {
        Some(MaterializationHead {
            at: MaterializationKey {
                lamport: head_lamport,
                replica: head_replica,
                counter: head_counter,
            },
            seq: head_seq,
        })
    };
    let replay_from = match (replay_lamport, replay_replica, replay_counter) {
        (Some(lamport), Some(replica), Some(counter)) => Some(MaterializationKey {
            lamport,
            replica,
            counter,
        }),
        _ => None,
    };

    Ok(TreeMeta(MaterializationState { head, replay_from }))
}

pub(super) fn load_tree_meta(client: &Rc<RefCell<Client>>, doc_id: &str) -> Result<TreeMeta> {
    load_tree_meta_row(client, doc_id, false)
}

pub(crate) fn load_tree_meta_for_update(
    client: &Rc<RefCell<Client>>,
    doc_id: &str,
) -> Result<TreeMeta> {
    load_tree_meta_row(client, doc_id, true)
}

pub(crate) fn set_tree_meta_replay_frontier(
    client: &Rc<RefCell<Client>>,
    doc_id: &str,
    frontier: &MaterializationFrontier,
) -> Result<()> {
    ensure_doc_meta(client, doc_id)?;
    let mut c = client.borrow_mut();
    c.execute(
        "UPDATE treecrdt_meta \
         SET replay_lamport = $2, replay_replica = $3, replay_counter = $4 \
         WHERE doc_id = $1",
        &[
            &doc_id,
            &(frontier.lamport as i64),
            &frontier.replica,
            &(frontier.counter as i64),
        ],
    )
    .map_err(|e| Error::Storage(e.to_string()))?;
    Ok(())
}

pub(crate) fn update_tree_meta_head<R: AsRef<[u8]>>(
    client: &Rc<RefCell<Client>>,
    doc_id: &str,
    head: Option<&MaterializationHead<R>>,
) -> Result<()> {
    ensure_doc_meta(client, doc_id)?;
    let (lamport, replica, counter, seq): (Lamport, &[u8], u64, u64) = match head {
        Some(head) => (
            head.at.lamport,
            head.at.replica.as_ref(),
            head.at.counter,
            head.seq,
        ),
        None => (0, &[], 0, 0),
    };
    let mut c = client.borrow_mut();
    c.execute(
        "UPDATE treecrdt_meta \
         SET head_lamport = $2, \
             head_replica = $3, \
             head_counter = $4, \
             head_seq = $5, \
             replay_lamport = NULL, \
             replay_replica = NULL, \
             replay_counter = NULL \
         WHERE doc_id = $1",
        &[
            &doc_id,
            &(lamport as i64),
            &replica,
            &(counter as i64),
            &(seq as i64),
        ],
    )
    .map_err(|e| Error::Storage(e.to_string()))?;
    Ok(())
}

#[derive(Clone)]
pub(crate) struct PgCtx {
    pub(crate) doc_id: String,
    pub(crate) client: Rc<RefCell<Client>>,
    stmts: Rc<RefCell<HashMap<&'static str, Statement>>>,
    pub(super) append_profile: Option<Rc<RefCell<PgAppendProfile>>>,
}

impl PgCtx {
    pub(crate) fn new(client: Rc<RefCell<Client>>, doc_id: &str) -> Result<Self> {
        Self::new_with_profile(client, doc_id, None)
    }

    pub(super) fn new_with_profile(
        client: Rc<RefCell<Client>>,
        doc_id: &str,
        append_profile: Option<Rc<RefCell<PgAppendProfile>>>,
    ) -> Result<Self> {
        ensure_doc_meta(&client, doc_id)?;
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

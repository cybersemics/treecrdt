use crate::ops::{cmp_op_key, cmp_ops, Operation};
use crate::traits::{Clock, NodeStore, ParentOpIndex, PayloadStore, Storage};
use crate::tree::TreeCrdt;
use crate::{Error, Lamport, Result};

/// Snapshot of adapter-maintained materialization metadata.
pub trait MaterializationCursor {
    fn dirty(&self) -> bool;
    fn head_lamport(&self) -> Lamport;
    fn head_replica(&self) -> &[u8];
    fn head_counter(&self) -> u64;
    fn head_seq(&self) -> u64;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MaterializationHead {
    pub lamport: Lamport,
    pub replica: Vec<u8>,
    pub counter: u64,
    pub seq: u64,
}

/// Apply an incremental batch through core materialization semantics.
///
/// The batch is sorted in canonical op-key order, validated against the materialized head,
/// and applied with parent-op index + tombstone maintenance.
pub fn apply_incremental_ops<S, C, N, P, I, M>(
    crdt: &mut TreeCrdt<S, C, N, P>,
    index: &mut I,
    meta: &M,
    mut ops: Vec<Operation>,
) -> Result<Option<MaterializationHead>>
where
    S: Storage,
    C: Clock,
    N: NodeStore,
    P: PayloadStore,
    I: ParentOpIndex,
    M: MaterializationCursor,
{
    if ops.is_empty() {
        return Ok(None);
    }
    if meta.dirty() {
        return Err(Error::Storage("materialize called while dirty".into()));
    }

    ops.sort_by(cmp_ops);

    if let Some(first) = ops.first() {
        if cmp_op_key(
            first.meta.lamport,
            first.meta.id.replica.as_bytes(),
            first.meta.id.counter,
            meta.head_lamport(),
            meta.head_replica(),
            meta.head_counter(),
        ) == std::cmp::Ordering::Less
        {
            return Err(Error::Storage(
                "out-of-order op before materialized head".into(),
            ));
        }
    }

    let mut seq = meta.head_seq();
    for op in ops {
        let _ = crdt.apply_remote_with_materialization_seq(op, index, &mut seq)?;
    }

    let last = crdt
        .head_op()
        .ok_or_else(|| Error::Storage("expected head op after materialization".into()))?;

    Ok(Some(MaterializationHead {
        lamport: last.meta.lamport,
        replica: last.meta.id.replica.as_bytes().to_vec(),
        counter: last.meta.id.counter,
        seq,
    }))
}

/// Run incremental materialization when possible; otherwise mark the document as dirty.
///
/// Returns `true` when incremental materialization succeeded, `false` when the caller
/// should rely on a full rebuild path later.
pub fn try_incremental_materialization<E>(
    already_dirty: bool,
    incremental: impl FnOnce() -> std::result::Result<(), E>,
    mut mark_dirty: impl FnMut(),
) -> bool {
    if already_dirty {
        mark_dirty();
        return false;
    }

    if incremental().is_err() {
        mark_dirty();
        return false;
    }

    true
}

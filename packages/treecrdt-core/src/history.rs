use std::cmp::Ordering;
use std::collections::HashSet;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::ids::{Lamport, NodeId, OperationId, ReplicaId};
use crate::materialization::{
    cmp_frontiers, frontier_from_op, op_requires_full_replay, rewind_op_in_place,
    FrontierRewindStorage, MaterializationCursor, MaterializationFrontier,
};
use crate::ops::{cmp_ops, Operation, OperationKind};
use crate::traits::{
    Clock, ExactPayloadStore, LamportClock, MemoryNodeStore, MemoryPayloadStore, NodeStore,
    NoopParentOpIndex, NoopStorage, PayloadStore, Storage,
};
use crate::tree::TreeCrdt;
use crate::types::LocalPlacement;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LocalEditAction {
    Delete {
        node: NodeId,
    },
    Move {
        node: NodeId,
        parent: NodeId,
        placement: LocalPlacement,
    },
    Payload {
        node: NodeId,
        payload: Option<Vec<u8>>,
    },
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct LocalEditPlan {
    pub actions: Vec<LocalEditAction>,
}

#[cfg(feature = "serde")]
#[derive(Deserialize)]
struct LocalEditOperationIdWire {
    replica: Vec<u8>,
    counter: u64,
    lamport: Option<Lamport>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalEditOperationRef {
    pub id: OperationId,
    pub lamport: Lamport,
}

#[cfg(feature = "serde")]
#[derive(Serialize)]
pub struct LocalEditPlanWire {
    actions: Vec<LocalEditActionWire>,
}

#[cfg(feature = "serde")]
#[derive(Serialize)]
#[serde(tag = "type", rename_all = "camelCase")]
enum LocalEditActionWire {
    Delete {
        node: [u8; 16],
    },
    Move {
        node: [u8; 16],
        parent: [u8; 16],
        placement: LocalEditPlacementWire,
    },
    Payload {
        node: [u8; 16],
        payload: Option<Vec<u8>>,
    },
}

#[cfg(feature = "serde")]
#[derive(Serialize)]
#[serde(tag = "type", rename_all = "camelCase")]
enum LocalEditPlacementWire {
    First,
    After { after: [u8; 16] },
}

#[cfg(feature = "serde")]
fn node_bytes(node: NodeId) -> [u8; 16] {
    node.0.to_be_bytes()
}

#[cfg(feature = "serde")]
fn wire_placement(placement: LocalPlacement) -> Result<LocalEditPlacementWire> {
    match placement {
        LocalPlacement::First => Ok(LocalEditPlacementWire::First),
        LocalPlacement::After(after) => Ok(LocalEditPlacementWire::After {
            after: node_bytes(after),
        }),
        LocalPlacement::Last => Err(Error::InvalidOperation(
            "history undo plan cannot serialize last placement".into(),
        )),
    }
}

#[cfg(feature = "serde")]
fn wire_action(action: LocalEditAction) -> Result<LocalEditActionWire> {
    match action {
        LocalEditAction::Delete { node } => Ok(LocalEditActionWire::Delete {
            node: node_bytes(node),
        }),
        LocalEditAction::Move {
            node,
            parent,
            placement,
        } => Ok(LocalEditActionWire::Move {
            node: node_bytes(node),
            parent: node_bytes(parent),
            placement: wire_placement(placement)?,
        }),
        LocalEditAction::Payload { node, payload } => Ok(LocalEditActionWire::Payload {
            node: node_bytes(node),
            payload,
        }),
    }
}

#[cfg(feature = "serde")]
impl TryFrom<LocalEditPlan> for LocalEditPlanWire {
    type Error = Error;

    fn try_from(plan: LocalEditPlan) -> Result<Self> {
        Ok(Self {
            actions: plan.actions.into_iter().map(wire_action).collect::<Result<Vec<_>>>()?,
        })
    }
}

#[cfg(feature = "serde")]
pub fn derive_undo_plan_from_history_json<H>(
    history: &H,
    edit_ids_json: &[u8],
) -> Result<LocalEditPlanWire>
where
    H: LocalEditHistory + ?Sized,
{
    let edit_ids = serde_json::from_slice::<Vec<LocalEditOperationIdWire>>(edit_ids_json)
        .map_err(|err| Error::InvalidOperation(format!("invalid local edit id JSON: {err}")))?
        .into_iter()
        .map(|id| OperationId {
            replica: ReplicaId::new(id.replica),
            counter: id.counter,
        })
        .collect::<Vec<_>>();
    derive_undo_plan_from_history(history, edit_ids)?.try_into()
}

#[cfg(feature = "serde")]
pub fn try_derive_undo_plan_by_rewinding_suffix_json<S, N, P, M>(
    history: &S,
    nodes: &mut N,
    payloads: &mut P,
    meta: &M,
    edit_ids_json: &[u8],
) -> Result<Option<LocalEditPlanWire>>
where
    S: FrontierRewindStorage,
    N: NodeStore,
    P: ExactPayloadStore,
    M: MaterializationCursor,
{
    let edit_refs = serde_json::from_slice::<Vec<LocalEditOperationIdWire>>(edit_ids_json)
        .map_err(|err| Error::InvalidOperation(format!("invalid local edit id JSON: {err}")))?
        .into_iter()
        .map(|id| {
            let id_ref = OperationId {
                replica: ReplicaId::new(id.replica),
                counter: id.counter,
            };
            match id.lamport {
                Some(lamport) => Ok(LocalEditOperationRef {
                    id: id_ref,
                    lamport,
                }),
                None => Err(id_ref),
            }
        })
        .collect::<std::result::Result<Vec<_>, _>>();

    let Ok(edit_refs) = edit_refs else {
        return Ok(None);
    };

    try_derive_undo_plan_by_rewinding_suffix(history, nodes, payloads, meta, edit_refs)?
        .map(LocalEditPlanWire::try_from)
        .transpose()
}

/// Canonical operation history used to derive local edit inverses.
///
/// Backends own only the storage access here. The undo semantics stay in this module.
pub trait LocalEditHistory {
    fn scan_history(&self, visit: &mut dyn FnMut(Operation) -> Result<()>) -> Result<()>;
}

impl<T> LocalEditHistory for T
where
    T: Storage,
{
    fn scan_history(&self, visit: &mut dyn FnMut(Operation) -> Result<()>) -> Result<()> {
        self.scan_since(0, visit)
    }
}

impl LocalEditHistory for [Operation] {
    fn scan_history(&self, visit: &mut dyn FnMut(Operation) -> Result<()>) -> Result<()> {
        let mut ops = self.to_vec();
        ops.sort_by(cmp_ops);
        for op in ops {
            visit(op)?;
        }
        Ok(())
    }
}

/// Materialized tree state at a point in history.
///
/// This is the state surface the inversion rules need. Today the replay implementation below
/// satisfies it with an in-memory core materializer; a future backend can satisfy it from a
/// persisted historical snapshot without changing the local edit API.
trait LocalEditState {
    fn parent(&self, node: NodeId) -> Result<Option<NodeId>>;
    fn children(&self, parent: NodeId) -> Result<Vec<NodeId>>;
    fn payload(&self, node: NodeId) -> Result<Option<Vec<u8>>>;
}

struct StoreLocalEditState<'a, N, P> {
    nodes: &'a N,
    payloads: &'a P,
}

impl<N, P> LocalEditState for StoreLocalEditState<'_, N, P>
where
    N: NodeStore,
    P: PayloadStore,
{
    fn parent(&self, node: NodeId) -> Result<Option<NodeId>> {
        if !self.nodes.exists(node)? {
            return Ok(None);
        }
        if self.nodes.tombstone(node)? {
            return Ok(Some(NodeId::TRASH));
        }
        Ok(self.nodes.parent(node)?.filter(|parent| *parent != NodeId::TRASH))
    }

    fn children(&self, parent: NodeId) -> Result<Vec<NodeId>> {
        if !self.nodes.exists(parent)? {
            return Ok(Vec::new());
        }
        let children = self.nodes.children(parent)?;
        let mut visible = Vec::with_capacity(children.len());
        for child in children {
            if !self.nodes.tombstone(child)? {
                visible.push(child);
            }
        }
        Ok(visible)
    }

    fn payload(&self, node: NodeId) -> Result<Option<Vec<u8>>> {
        self.payloads.payload(node)
    }
}

impl<S, C, N, P> LocalEditState for TreeCrdt<S, C, N, P>
where
    S: Storage,
    C: Clock,
    N: NodeStore,
    P: PayloadStore,
{
    fn parent(&self, node: NodeId) -> Result<Option<NodeId>> {
        TreeCrdt::parent(self, node)
    }

    fn children(&self, parent: NodeId) -> Result<Vec<NodeId>> {
        TreeCrdt::children(self, parent)
    }

    fn payload(&self, node: NodeId) -> Result<Option<Vec<u8>>> {
        TreeCrdt::payload(self, node)
    }
}

fn visible_placement_before(
    state: &impl LocalEditState,
    node: NodeId,
) -> Result<Option<(NodeId, LocalPlacement)>> {
    let Some(parent) = state.parent(node)? else {
        return Ok(None);
    };
    if parent == NodeId::TRASH {
        return Ok(None);
    }

    let children = state.children(parent)?;
    let Some(index) = children.iter().position(|child| *child == node) else {
        return Ok(None);
    };
    let placement = if index == 0 {
        LocalPlacement::First
    } else {
        LocalPlacement::After(children[index - 1])
    };
    Ok(Some((parent, placement)))
}

fn undo_action_before(state: &impl LocalEditState, op: &Operation) -> Result<LocalEditAction> {
    match &op.kind {
        OperationKind::Insert { node, .. } => Ok(LocalEditAction::Delete { node: *node }),
        OperationKind::Move { node, .. } => match visible_placement_before(state, *node)? {
            Some((parent, placement)) => Ok(LocalEditAction::Move {
                node: *node,
                parent,
                placement,
            }),
            None => Ok(LocalEditAction::Delete { node: *node }),
        },
        OperationKind::Delete { node } | OperationKind::Tombstone { node } => {
            match visible_placement_before(state, *node)? {
                Some((parent, placement)) => Ok(LocalEditAction::Move {
                    node: *node,
                    parent,
                    placement,
                }),
                None => Ok(LocalEditAction::Delete { node: *node }),
            }
        }
        OperationKind::Payload { node, .. } => Ok(LocalEditAction::Payload {
            node: *node,
            payload: state.payload(*node)?,
        }),
    }
}

pub fn derive_undo_plan_from_history<H>(
    history: &H,
    edit_ids: impl IntoIterator<Item = OperationId>,
) -> Result<LocalEditPlan>
where
    H: LocalEditHistory + ?Sized,
{
    let mut pending: HashSet<OperationId> = edit_ids.into_iter().collect();

    let mut crdt = TreeCrdt::with_stores(
        ReplicaId::new(b"history-invert"),
        NoopStorage,
        LamportClock::default(),
        MemoryNodeStore::default(),
        MemoryPayloadStore::default(),
    )?;
    let mut index = NoopParentOpIndex;
    let mut seq = 0u64;
    let mut actions = Vec::new();

    history.scan_history(&mut |op| {
        if pending.remove(&op.meta.id) {
            actions.insert(0, undo_action_before(&crdt, &op)?);
        }

        seq = seq.saturating_add(1);
        crdt.apply_sorted_remote_with_materialization(op, &mut index, seq)?;
        Ok(())
    })?;

    if !pending.is_empty() {
        return Err(Error::MissingDependency(format!(
            "missing {} edit operation(s) from history",
            pending.len()
        )));
    }

    Ok(LocalEditPlan { actions })
}

fn frontier_from_edit_ref(edit_ref: &LocalEditOperationRef) -> MaterializationFrontier {
    MaterializationFrontier {
        lamport: edit_ref.lamport,
        replica: edit_ref.id.replica.as_bytes().to_vec(),
        counter: edit_ref.id.counter,
    }
}

/// Try to derive an undo plan by rewinding the current materialized suffix in place.
///
/// This is intended for rollbackable scratch contexts. The supplied node/payload stores are
/// mutated while deriving the plan and are left rewound; callers that use persistent stores should
/// run this inside a transaction/savepoint and roll it back after reading the plan. Returns
/// `Ok(None)` when the suffix is not safe for this fast path, so callers can fall back to full
/// canonical replay.
pub fn try_derive_undo_plan_by_rewinding_suffix<S, N, P, M>(
    history: &S,
    nodes: &mut N,
    payloads: &mut P,
    meta: &M,
    edit_refs: impl IntoIterator<Item = LocalEditOperationRef>,
) -> Result<Option<LocalEditPlan>>
where
    S: FrontierRewindStorage,
    N: NodeStore,
    P: ExactPayloadStore,
    M: MaterializationCursor,
{
    let edit_refs = edit_refs.into_iter().collect::<Vec<_>>();
    if edit_refs.is_empty() {
        return Ok(Some(LocalEditPlan::default()));
    }

    let state = meta.state();
    let Some(head) = state.head.as_ref() else {
        return Ok(None);
    };
    if state.replay_from.is_some() {
        return Ok(None);
    }

    let target_ids = edit_refs.iter().map(|edit| edit.id.clone()).collect::<HashSet<_>>();
    let Some(earliest) = edit_refs.iter().map(frontier_from_edit_ref).min_by(cmp_frontiers) else {
        return Ok(Some(LocalEditPlan::default()));
    };
    if cmp_frontiers(&earliest, &head.at) == Ordering::Greater {
        return Ok(None);
    }

    let mut suffix = Vec::new();
    let mut seen_targets = HashSet::new();
    let mut requires_full_replay = false;
    history.scan_frontier_range(&earliest.as_borrowed(), &mut |op| {
        if cmp_frontiers(&frontier_from_op(&op), &head.at) == Ordering::Greater {
            return Ok(());
        }
        if target_ids.contains(&op.meta.id) {
            seen_targets.insert(op.meta.id.clone());
        }
        requires_full_replay |= op_requires_full_replay(&op);
        suffix.push(op);
        Ok(())
    })?;

    if requires_full_replay || seen_targets.len() != target_ids.len() {
        return Ok(None);
    }

    let mut pending = target_ids;
    let mut actions = Vec::new();
    for op in suffix.iter().rev() {
        rewind_op_in_place(nodes, payloads, history, op)?;
        if pending.remove(&op.meta.id) {
            let state = StoreLocalEditState {
                nodes: &*nodes,
                payloads: &*payloads,
            };
            actions.push(undo_action_before(&state, op)?);
        }
    }

    if !pending.is_empty() {
        return Ok(None);
    }

    Ok(Some(LocalEditPlan { actions }))
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use super::*;
    use crate::ids::Lamport;
    use crate::materialization::{MaterializationHead, MaterializationKey, MaterializationState};
    use crate::ops::cmp_op_key;

    fn node(n: u128) -> NodeId {
        NodeId(n)
    }

    fn key(position: u8) -> Vec<u8> {
        vec![position]
    }

    fn meta_from_op(op: &Operation, seq: u64) -> MaterializationState {
        MaterializationState {
            head: Some(MaterializationHead {
                at: MaterializationKey {
                    lamport: op.meta.lamport,
                    replica: op.meta.id.replica.as_bytes().to_vec(),
                    counter: op.meta.id.counter,
                },
                seq,
            }),
            replay_from: None,
        }
    }

    struct FastOnlyStorage {
        ops: Vec<Operation>,
        full_history_calls: Cell<usize>,
    }

    impl FastOnlyStorage {
        fn new(mut ops: Vec<Operation>) -> Self {
            ops.sort_by(cmp_ops);
            Self {
                ops,
                full_history_calls: Cell::new(0),
            }
        }

        fn op_before(
            op: &Operation,
            before: &crate::materialization::MaterializationFrontierRef<'_>,
        ) -> bool {
            cmp_op_key(
                op.meta.lamport,
                op.meta.id.replica.as_bytes(),
                op.meta.id.counter,
                before.lamport,
                before.replica,
                before.counter,
            ) == Ordering::Less
        }
    }

    impl Storage for FastOnlyStorage {
        fn apply(&mut self, _op: Operation) -> Result<bool> {
            Err(Error::Storage(
                "fast-only storage does not apply ops".into(),
            ))
        }

        fn load_since(&self, _lamport: Lamport) -> Result<Vec<Operation>> {
            self.full_history_calls.set(self.full_history_calls.get().saturating_add(1));
            Err(Error::Storage(
                "full history load should not be used".into(),
            ))
        }

        fn latest_lamport(&self) -> Lamport {
            self.ops.iter().map(|op| op.meta.lamport).max().unwrap_or_default()
        }

        fn scan_since(
            &self,
            _lamport: Lamport,
            _visit: &mut dyn FnMut(Operation) -> Result<()>,
        ) -> Result<()> {
            self.full_history_calls.set(self.full_history_calls.get().saturating_add(1));
            Err(Error::Storage(
                "full history scan should not be used".into(),
            ))
        }
    }

    impl FrontierRewindStorage for FastOnlyStorage {
        fn scan_frontier_range(
            &self,
            start: &crate::materialization::MaterializationFrontierRef<'_>,
            visit: &mut dyn FnMut(Operation) -> Result<()>,
        ) -> Result<()> {
            for op in &self.ops {
                if !Self::op_before(op, start) {
                    visit(op.clone())?;
                }
            }
            Ok(())
        }

        fn latest_structural_before(
            &self,
            node: NodeId,
            before: &crate::materialization::MaterializationFrontierRef<'_>,
        ) -> Result<Option<Operation>> {
            Ok(self
                .ops
                .iter()
                .rev()
                .find(|op| {
                    Self::op_before(op, before)
                        && matches!(
                            op.kind,
                            OperationKind::Insert { node: n, .. }
                                | OperationKind::Move { node: n, .. } if n == node
                        )
                })
                .cloned())
        }

        fn latest_payload_before(
            &self,
            node: NodeId,
            before: &crate::materialization::MaterializationFrontierRef<'_>,
        ) -> Result<Option<Operation>> {
            Ok(self
                .ops
                .iter()
                .rev()
                .find(|op| {
                    Self::op_before(op, before)
                        && match &op.kind {
                            OperationKind::Insert {
                                node: n, payload, ..
                            } => *n == node && payload.is_some(),
                            OperationKind::Payload { node: n, .. } => *n == node,
                            _ => false,
                        }
                })
                .cloned())
        }
    }

    #[test]
    fn rewind_suffix_derives_move_payload_plan_without_full_history_scan() {
        let replica = ReplicaId::new(b"history-fast");
        let root = NodeId::ROOT;
        let parent_a = node(1);
        let parent_b = node(2);
        let sibling = node(3);
        let target = node(4);
        let unrelated = node(5);
        let original_payload = b"original".to_vec();
        let next_payload = b"next".to_vec();

        let ops = vec![
            Operation::insert(&replica, 1, 1, root, parent_a, key(0)),
            Operation::insert(&replica, 2, 2, root, parent_b, key(1)),
            Operation::insert(&replica, 3, 3, parent_a, sibling, key(0)),
            Operation::insert_with_payload(
                &replica,
                4,
                4,
                parent_a,
                target,
                key(1),
                original_payload.clone(),
            ),
            Operation::move_node(&replica, 5, 5, target, parent_b, key(0)),
            Operation::set_payload(&replica, 6, 6, target, next_payload.clone()),
            Operation::set_payload(&replica, 7, 7, unrelated, b"later".to_vec()),
        ];
        let storage = FastOnlyStorage::new(ops.clone());

        let mut nodes = MemoryNodeStore::default();
        nodes.attach(parent_a, root, key(0)).unwrap();
        nodes.attach(parent_b, root, key(1)).unwrap();
        nodes.attach(sibling, parent_a, key(0)).unwrap();
        nodes.attach(target, parent_b, key(0)).unwrap();
        nodes.ensure_node(unrelated).unwrap();

        let mut payloads = MemoryPayloadStore::default();
        payloads
            .set_payload(
                target,
                Some(next_payload),
                (ops[5].meta.lamport, ops[5].meta.id.clone()),
            )
            .unwrap();
        payloads
            .set_payload(
                unrelated,
                Some(b"later".to_vec()),
                (ops[6].meta.lamport, ops[6].meta.id.clone()),
            )
            .unwrap();

        let plan = try_derive_undo_plan_by_rewinding_suffix(
            &storage,
            &mut nodes,
            &mut payloads,
            &meta_from_op(ops.last().unwrap(), ops.len() as u64),
            [
                LocalEditOperationRef {
                    id: ops[4].meta.id.clone(),
                    lamport: ops[4].meta.lamport,
                },
                LocalEditOperationRef {
                    id: ops[5].meta.id.clone(),
                    lamport: ops[5].meta.lamport,
                },
            ],
        )
        .unwrap()
        .expect("fast path plan");

        assert_eq!(storage.full_history_calls.get(), 0);
        assert_eq!(
            plan.actions,
            vec![
                LocalEditAction::Payload {
                    node: target,
                    payload: Some(original_payload)
                },
                LocalEditAction::Move {
                    node: target,
                    parent: parent_a,
                    placement: LocalPlacement::After(sibling)
                }
            ]
        );
    }

    #[test]
    fn rewind_suffix_declines_delete_suffix_for_replay_fallback() {
        let replica = ReplicaId::new(b"history-delete");
        let parent = node(11);
        let child = node(12);
        let ops = vec![
            Operation::insert(&replica, 1, 1, NodeId::ROOT, parent, key(0)),
            Operation::insert(&replica, 2, 2, parent, child, key(0)),
            Operation::delete(&replica, 3, 3, parent, None),
        ];
        let storage = FastOnlyStorage::new(ops.clone());
        let mut nodes = MemoryNodeStore::default();
        let mut payloads = MemoryPayloadStore::default();

        let plan = try_derive_undo_plan_by_rewinding_suffix(
            &storage,
            &mut nodes,
            &mut payloads,
            &meta_from_op(ops.last().unwrap(), ops.len() as u64),
            [LocalEditOperationRef {
                id: ops[1].meta.id.clone(),
                lamport: ops[1].meta.lamport,
            }],
        )
        .unwrap();

        assert!(plan.is_none());
        assert_eq!(storage.full_history_calls.get(), 0);
    }
}

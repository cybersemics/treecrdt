use std::collections::HashSet;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::ids::{NodeId, OperationId, ReplicaId};
use crate::ops::{cmp_ops, Operation, OperationKind};
use crate::traits::{
    Clock, LamportClock, MemoryNodeStore, MemoryPayloadStore, NodeStore, NoopParentOpIndex,
    NoopStorage, PayloadStore, Storage,
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
        /// The node's zero-based position under `parent` before the edit.
        ///
        /// Callers can use this to rebase the action when an `After` anchor is stale.
        index: usize,
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
        index: usize,
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
            index,
        } => Ok(LocalEditActionWire::Move {
            node: node_bytes(node),
            parent: node_bytes(parent),
            placement: wire_placement(placement)?,
            index,
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
        });
    derive_undo_plan_from_history(history, edit_ids)?.try_into()
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

fn visible_placement_before<S, C, N, P>(
    state: &TreeCrdt<S, C, N, P>,
    node: NodeId,
) -> Result<Option<(NodeId, LocalPlacement, usize)>>
where
    S: Storage,
    C: Clock,
    N: NodeStore,
    P: PayloadStore,
{
    if state.is_tombstoned(node)? {
        return Ok(None);
    }
    let Some(parent) = state.raw_parent(node)? else {
        return Ok(None);
    };
    if parent == NodeId::TRASH {
        return Ok(Some((parent, LocalPlacement::First, 0)));
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
    Ok(Some((parent, placement, index)))
}

fn restore_placement_action<S, C, N, P>(
    state: &TreeCrdt<S, C, N, P>,
    node: NodeId,
) -> Result<LocalEditAction>
where
    S: Storage,
    C: Clock,
    N: NodeStore,
    P: PayloadStore,
{
    match visible_placement_before(state, node)? {
        Some((parent, placement, index)) => Ok(LocalEditAction::Move {
            node,
            parent,
            placement,
            index,
        }),
        None if state.is_known(node)? && !state.is_tombstoned(node)? => Err(
            Error::InvalidOperation("cannot restore a detached node during undo".into()),
        ),
        None => Ok(LocalEditAction::Delete { node }),
    }
}

fn restore_deleted_node_action<S, C, N, P>(
    state: &TreeCrdt<S, C, N, P>,
    node: NodeId,
) -> Result<LocalEditAction>
where
    S: Storage,
    C: Clock,
    N: NodeStore,
    P: PayloadStore,
{
    if state.is_known(node)? && !state.is_tombstoned(node)? && state.raw_parent(node)?.is_none() {
        return Ok(LocalEditAction::Payload {
            node,
            payload: state.payload(node)?,
        });
    }
    restore_placement_action(state, node)
}

fn undo_actions_before<S, C, N, P>(
    state: &TreeCrdt<S, C, N, P>,
    op: &Operation,
) -> Result<Vec<LocalEditAction>>
where
    S: Storage,
    C: Clock,
    N: NodeStore,
    P: PayloadStore,
{
    match &op.kind {
        OperationKind::Insert { node, payload, .. } => {
            let restore = restore_placement_action(state, *node)?;
            let mut actions = Vec::with_capacity(2);
            if payload.is_some() {
                actions.push(LocalEditAction::Payload {
                    node: *node,
                    payload: state.payload(*node)?,
                });
            }
            actions.push(restore);
            Ok(actions)
        }
        OperationKind::Move { node, .. } => Ok(vec![restore_placement_action(state, *node)?]),
        OperationKind::Delete { node } | OperationKind::Tombstone { node } => {
            Ok(vec![restore_deleted_node_action(state, *node)?])
        }
        OperationKind::Payload { node, .. } => {
            let was_known = state.is_known(*node)?;
            let mut actions = vec![LocalEditAction::Payload {
                node: *node,
                payload: state.payload(*node)?,
            }];
            if !was_known || state.is_tombstoned(*node)? {
                // A payload write advances last_change and can revive a defensively deleted node.
                // Restore the payload first, then issue a causally aware delete to hide it again.
                actions.push(LocalEditAction::Delete { node: *node });
            }
            Ok(actions)
        }
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
    if pending.is_empty() {
        return Ok(LocalEditPlan::default());
    }

    let mut crdt = TreeCrdt::with_stores(
        ReplicaId::new(b"history-invert"),
        NoopStorage,
        LamportClock::default(),
        MemoryNodeStore::default(),
        MemoryPayloadStore::default(),
    )?;
    let mut index = NoopParentOpIndex;
    let mut seq = 0u64;
    let mut inverse_groups = Vec::with_capacity(pending.len());
    let mut plan_complete = false;

    history.scan_history(&mut |op| {
        if plan_complete {
            return Ok(());
        }
        if pending.remove(&op.meta.id) {
            inverse_groups.push(undo_actions_before(&crdt, &op)?);
        }

        crdt.apply_remote_with_materialization_seq(op, &mut index, &mut seq)?;
        plan_complete = pending.is_empty();
        Ok(())
    })?;

    if !pending.is_empty() {
        return Err(Error::MissingDependency(format!(
            "missing {} edit operation(s) from history",
            pending.len()
        )));
    }

    let mut actions = Vec::new();
    for action in inverse_groups.into_iter().rev().flatten() {
        let replaces_last = matches!(
            (&action, actions.last()),
            (
                LocalEditAction::Payload { node, .. },
                Some(LocalEditAction::Payload { node: previous, .. })
            ) if node == previous
        );
        if replaces_last {
            actions.pop();
        }
        actions.push(action);
    }
    Ok(LocalEditPlan { actions })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::MemoryStorage;
    use crate::version_vector::VersionVector;

    fn node(n: u128) -> NodeId {
        NodeId(n)
    }

    #[test]
    fn canonical_plan_restores_reinserted_node_and_payload() {
        let replica = ReplicaId::new(b"history");
        let sibling = node(1);
        let target = node(2);
        let destination = node(3);
        let original_payload = b"original".to_vec();
        let ops = vec![
            Operation::insert(&replica, 1, 1, NodeId::ROOT, sibling, vec![0x10]),
            Operation::insert_with_payload(
                &replica,
                2,
                2,
                NodeId::ROOT,
                target,
                vec![0x20],
                original_payload.clone(),
            ),
            Operation::insert(&replica, 3, 3, NodeId::ROOT, destination, vec![0x30]),
            Operation::insert_with_payload(
                &replica,
                4,
                4,
                destination,
                target,
                vec![0x10],
                b"updated".to_vec(),
            ),
        ];

        let plan = derive_undo_plan_from_history(ops.as_slice(), [ops[3].meta.id.clone()]).unwrap();

        assert_eq!(
            plan.actions,
            vec![
                LocalEditAction::Payload {
                    node: target,
                    payload: Some(original_payload),
                },
                LocalEditAction::Move {
                    node: target,
                    parent: NodeId::ROOT,
                    placement: LocalPlacement::After(sibling),
                    index: 1,
                },
            ]
        );
    }

    #[test]
    fn canonical_plan_rejects_insert_over_preexisting_detached_node() {
        let replica = ReplicaId::new(b"history");
        let target = node(4);
        let ops = vec![
            Operation::set_payload(&replica, 1, 1, target, b"original".to_vec()),
            Operation::insert_with_payload(
                &replica,
                2,
                2,
                NodeId::ROOT,
                target,
                vec![0x10],
                b"updated".to_vec(),
            ),
        ];

        let error =
            derive_undo_plan_from_history(ops.as_slice(), [ops[1].meta.id.clone()]).unwrap_err();

        assert!(matches!(
            error,
            Error::InvalidOperation(message)
                if message == "cannot restore a detached node during undo"
        ));
    }

    #[test]
    fn canonical_plan_restores_move_to_trash() {
        let replica = ReplicaId::new(b"history");
        let target = node(5);
        let ops = vec![
            Operation::insert(&replica, 1, 1, NodeId::ROOT, target, vec![0x10]),
            Operation::move_node(&replica, 2, 2, target, NodeId::TRASH, Vec::new()),
            Operation::move_node(&replica, 3, 3, target, NodeId::ROOT, vec![0x10]),
        ];

        let plan = derive_undo_plan_from_history(ops.as_slice(), [ops[2].meta.id.clone()]).unwrap();

        assert_eq!(
            plan.actions,
            vec![LocalEditAction::Move {
                node: target,
                parent: NodeId::TRASH,
                placement: LocalPlacement::First,
                index: 0,
            }]
        );
    }

    #[test]
    fn payload_inverse_hides_previously_unknown_node() {
        let replica = ReplicaId::new(b"history");
        let target = node(6);
        let payload = Operation::set_payload(&replica, 1, 1, target, b"created".to_vec());

        let plan = derive_undo_plan_from_history(
            std::slice::from_ref(&payload),
            [payload.meta.id.clone()],
        )
        .unwrap();

        assert_eq!(
            plan.actions,
            vec![
                LocalEditAction::Payload {
                    node: target,
                    payload: None,
                },
                LocalEditAction::Delete { node: target },
            ]
        );
    }

    #[test]
    fn payload_inverse_restores_tombstoned_state() {
        let author = ReplicaId::new(b"author");
        let node = node(10);
        let original_payload = b"original".to_vec();
        let insert = Operation::insert_with_payload(
            &author,
            1,
            1,
            NodeId::ROOT,
            node,
            vec![0x10],
            original_payload.clone(),
        );
        let mut known_state = VersionVector::new();
        known_state.observe(&author, 1);
        let delete = Operation::delete(&author, 2, 2, node, Some(known_state));
        let payload = Operation::set_payload(&author, 3, 3, node, b"revived".to_vec());
        let ops = vec![insert, delete, payload.clone()];

        let plan = derive_undo_plan_from_history(ops.as_slice(), [payload.meta.id]).unwrap();
        assert_eq!(
            plan.actions,
            vec![
                LocalEditAction::Payload {
                    node,
                    payload: Some(original_payload.clone()),
                },
                LocalEditAction::Delete { node },
            ]
        );

        let mut crdt = TreeCrdt::new(
            ReplicaId::new(b"undo"),
            MemoryStorage::default(),
            LamportClock::default(),
        )
        .unwrap();
        for op in ops {
            crdt.apply_remote(op).unwrap();
        }
        assert!(!crdt.is_tombstoned(node).unwrap());

        for action in plan.actions {
            match action {
                LocalEditAction::Payload { node, payload } => {
                    crdt.local_payload(node, payload).unwrap();
                }
                LocalEditAction::Delete { node } => {
                    crdt.local_delete(node).unwrap();
                }
                LocalEditAction::Move {
                    node,
                    parent,
                    placement,
                    ..
                } => {
                    crdt.local_move(node, parent, placement).unwrap();
                }
            }
        }

        assert!(crdt.is_tombstoned(node).unwrap());
        assert_eq!(crdt.payload(node).unwrap(), Some(original_payload));
    }

    #[cfg(feature = "serde")]
    #[test]
    fn json_refs_need_only_replica_and_counter() {
        let replica = ReplicaId::new(b"wire");
        let sibling = node(20);
        let target = node(21);
        let destination = node(22);
        let ops = vec![
            Operation::insert(&replica, 1, 1, NodeId::ROOT, sibling, vec![0x10]),
            Operation::insert(&replica, 2, 2, NodeId::ROOT, target, vec![0x20]),
            Operation::insert(&replica, 3, 3, NodeId::ROOT, destination, vec![0x30]),
            Operation::move_node(&replica, 4, 4, target, destination, vec![0x10]),
        ];
        let refs = serde_json::to_vec(&serde_json::json!([{
            "replica": replica.as_bytes(),
            "counter": 4,
        }]))
        .unwrap();

        let wire = derive_undo_plan_from_history_json(ops.as_slice(), &refs).unwrap();
        let value = serde_json::to_value(wire).unwrap();
        assert_eq!(value["actions"][0]["type"], "move");
        assert_eq!(value["actions"][0]["index"], 1);
    }
}

use crate::ids::NodeId;
use crate::ops::OperationKind;
use crate::types::MaterializationChange;

#[derive(Clone, Debug)]
pub(crate) struct TombstoneDelta {
    pub(crate) node: NodeId,
    pub(crate) parent: Option<NodeId>,
    pub(crate) previous: bool,
    pub(crate) tombstoned: bool,
    pub(crate) payload_after: Option<Vec<u8>>,
}

pub(crate) fn affected_parents(
    snapshot_parent: Option<NodeId>,
    kind: &OperationKind,
) -> Vec<NodeId> {
    let mut parents = Vec::new();
    if let Some(p) = snapshot_parent {
        parents.push(p);
    }
    match kind {
        OperationKind::Insert { parent, .. } => parents.push(*parent),
        OperationKind::Move { new_parent, .. } => parents.push(*new_parent),
        OperationKind::Delete { .. }
        | OperationKind::Tombstone { .. }
        | OperationKind::Payload { .. } => {}
    }
    parents.sort();
    parents.dedup();
    parents
}

pub(crate) fn parent_hints_from(parent: Option<NodeId>) -> Vec<NodeId> {
    parent.into_iter().collect()
}

pub(crate) fn direct_materialization_changes(
    snapshot_parent: Option<NodeId>,
    kind: &OperationKind,
) -> Vec<MaterializationChange> {
    match kind {
        OperationKind::Insert {
            parent,
            node,
            payload,
            ..
        } => {
            vec![MaterializationChange::Insert {
                node: *node,
                parent_after: *parent,
                payload: payload.clone(),
            }]
        }
        OperationKind::Move {
            node, new_parent, ..
        } => vec![MaterializationChange::Move {
            node: *node,
            parent_before: snapshot_parent.filter(|parent| *parent != NodeId::TRASH),
            parent_after: *new_parent,
        }],
        OperationKind::Payload { node, payload } => vec![MaterializationChange::Payload {
            node: *node,
            payload: payload.clone(),
        }],
        OperationKind::Delete { .. } | OperationKind::Tombstone { .. } => Vec::new(),
    }
}

pub(crate) fn materialization_change_from_tombstone_delta(
    delta: TombstoneDelta,
) -> Option<MaterializationChange> {
    if delta.node == NodeId::TRASH {
        return None;
    }

    let parent = delta.parent.filter(|parent| *parent != NodeId::TRASH);
    match (delta.previous, delta.tombstoned) {
        (true, false) => Some(MaterializationChange::Restore {
            node: delta.node,
            parent_after: parent,
            payload: delta.payload_after,
        }),
        (false, true) => Some(MaterializationChange::Delete {
            node: delta.node,
            parent_before: parent,
        }),
        _ => None,
    }
}

#[derive(Clone, Debug)]
struct StructuralChange {
    first_parent: Option<NodeId>,
    final_parent: NodeId,
    inserted: bool,
    payload_after: Option<Vec<u8>>,
}

#[derive(Clone, Debug)]
struct TombstoneChange {
    first_is_restore: bool,
    last_parent: Option<NodeId>,
    payload_after: Option<Vec<u8>>,
    count: usize,
}

pub(crate) fn coalesce_materialization_changes(
    changes: Vec<MaterializationChange>,
) -> Vec<MaterializationChange> {
    use std::collections::{BTreeMap, BTreeSet};

    let mut structural: BTreeMap<NodeId, StructuralChange> = BTreeMap::new();
    let mut tombstone: BTreeMap<NodeId, TombstoneChange> = BTreeMap::new();
    let mut payload: BTreeMap<NodeId, Option<Vec<u8>>> = BTreeMap::new();

    for change in changes {
        match change {
            MaterializationChange::Insert {
                node,
                parent_after,
                payload,
            } => {
                structural
                    .entry(node)
                    .and_modify(|existing| {
                        existing.final_parent = parent_after;
                        existing.inserted = true;
                        existing.payload_after = payload.clone();
                    })
                    .or_insert(StructuralChange {
                        first_parent: None,
                        final_parent: parent_after,
                        inserted: true,
                        payload_after: payload,
                    });
            }
            MaterializationChange::Move {
                node,
                parent_before,
                parent_after,
            } => {
                structural
                    .entry(node)
                    .and_modify(|existing| existing.final_parent = parent_after)
                    .or_insert(StructuralChange {
                        first_parent: parent_before,
                        final_parent: parent_after,
                        inserted: false,
                        payload_after: None,
                    });
            }
            MaterializationChange::Delete {
                node,
                parent_before,
            } => {
                tombstone
                    .entry(node)
                    .and_modify(|existing| {
                        existing.last_parent = parent_before;
                        existing.count += 1;
                    })
                    .or_insert(TombstoneChange {
                        first_is_restore: false,
                        last_parent: parent_before,
                        payload_after: None,
                        count: 1,
                    });
            }
            MaterializationChange::Restore {
                node,
                parent_after,
                payload,
            } => {
                tombstone
                    .entry(node)
                    .and_modify(|existing| {
                        existing.last_parent = parent_after;
                        existing.payload_after = payload.clone();
                        existing.count += 1;
                    })
                    .or_insert(TombstoneChange {
                        first_is_restore: true,
                        last_parent: parent_after,
                        payload_after: payload,
                        count: 1,
                    });
            }
            MaterializationChange::Payload {
                node,
                payload: payload_after,
            } => {
                payload.insert(node, payload_after);
            }
        }
    }

    let deleted_nodes: BTreeSet<NodeId> = tombstone
        .iter()
        .filter_map(|(node, change)| {
            if change.count % 2 == 1 && !change.first_is_restore {
                Some(*node)
            } else {
                None
            }
        })
        .collect();

    let mut coalesced = Vec::new();
    for (node, change) in structural {
        if change.inserted {
            let payload_after = payload.remove(&node).unwrap_or(change.payload_after);
            coalesced.push(MaterializationChange::Insert {
                node,
                parent_after: change.final_parent,
                payload: payload_after,
            });
        } else {
            coalesced.push(MaterializationChange::Move {
                node,
                parent_before: change.first_parent,
                parent_after: change.final_parent,
            });
        }
    }

    for (node, change) in tombstone {
        if change.count % 2 == 0 {
            continue;
        }
        if change.first_is_restore {
            let payload_after = payload.remove(&node).unwrap_or(change.payload_after);
            coalesced.push(MaterializationChange::Restore {
                node,
                parent_after: change.last_parent,
                payload: payload_after,
            });
        } else {
            coalesced.push(MaterializationChange::Delete {
                node,
                parent_before: change.last_parent,
            });
        }
    }

    for (node, payload_after) in payload {
        if !deleted_nodes.contains(&node) {
            coalesced.push(MaterializationChange::Payload {
                node,
                payload: payload_after,
            });
        }
    }

    coalesced
}

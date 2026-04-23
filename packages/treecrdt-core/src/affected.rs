use crate::ids::NodeId;
use crate::ops::OperationKind;
use crate::types::MaterializationChange;

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
            let mut changes = vec![MaterializationChange::Insert {
                node: *node,
                parent_after: *parent,
            }];
            if payload.is_some() {
                changes.push(MaterializationChange::Payload { node: *node });
            }
            changes
        }
        OperationKind::Move {
            node, new_parent, ..
        } => vec![MaterializationChange::Move {
            node: *node,
            parent_before: snapshot_parent.filter(|parent| *parent != NodeId::TRASH),
            parent_after: *new_parent,
        }],
        OperationKind::Payload { node, .. } => vec![MaterializationChange::Payload { node: *node }],
        OperationKind::Delete { .. } | OperationKind::Tombstone { .. } => Vec::new(),
    }
}

#[derive(Clone, Debug)]
struct StructuralChange {
    first_parent: Option<NodeId>,
    final_parent: NodeId,
    inserted: bool,
}

#[derive(Clone, Debug)]
struct TombstoneChange {
    first_is_restore: bool,
    last_parent: Option<NodeId>,
    count: usize,
}

pub(crate) fn coalesce_materialization_changes(
    changes: Vec<MaterializationChange>,
) -> Vec<MaterializationChange> {
    use std::collections::{BTreeMap, BTreeSet};

    let mut structural: BTreeMap<NodeId, StructuralChange> = BTreeMap::new();
    let mut tombstone: BTreeMap<NodeId, TombstoneChange> = BTreeMap::new();
    let mut payload: BTreeSet<NodeId> = BTreeSet::new();

    for change in changes {
        match change {
            MaterializationChange::Insert { node, parent_after } => {
                structural
                    .entry(node)
                    .and_modify(|existing| {
                        existing.final_parent = parent_after;
                        existing.inserted = true;
                    })
                    .or_insert(StructuralChange {
                        first_parent: None,
                        final_parent: parent_after,
                        inserted: true,
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
                        count: 1,
                    });
            }
            MaterializationChange::Restore { node, parent_after } => {
                tombstone
                    .entry(node)
                    .and_modify(|existing| {
                        existing.last_parent = parent_after;
                        existing.count += 1;
                    })
                    .or_insert(TombstoneChange {
                        first_is_restore: true,
                        last_parent: parent_after,
                        count: 1,
                    });
            }
            MaterializationChange::Payload { node } => {
                payload.insert(node);
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
            coalesced.push(MaterializationChange::Insert {
                node,
                parent_after: change.final_parent,
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
            coalesced.push(MaterializationChange::Restore {
                node,
                parent_after: change.last_parent,
            });
        } else {
            coalesced.push(MaterializationChange::Delete {
                node,
                parent_before: change.last_parent,
            });
        }
    }

    for node in payload {
        if !deleted_nodes.contains(&node) {
            coalesced.push(MaterializationChange::Payload { node });
        }
    }

    coalesced
}

use crate::ids::NodeId;
use crate::ops::{Operation, OperationKind};
use crate::types::{MaterializationChange, MaterializationSource};

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
    op: &Operation,
) -> Vec<MaterializationChange> {
    let source = Some(MaterializationSource::from_op(op));
    let kind = &op.kind;
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
                source,
            }]
        }
        OperationKind::Move {
            node, new_parent, ..
        } => vec![MaterializationChange::Move {
            node: *node,
            parent_before: snapshot_parent.filter(|parent| *parent != NodeId::TRASH),
            parent_after: *new_parent,
            source,
        }],
        OperationKind::Payload { node, payload } => vec![MaterializationChange::Payload {
            node: *node,
            payload: payload.clone(),
            source,
        }],
        OperationKind::Delete { .. } | OperationKind::Tombstone { .. } => Vec::new(),
    }
}

pub(crate) fn materialization_change_from_tombstone_delta(
    delta: TombstoneDelta,
    source: Option<MaterializationSource>,
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
            source,
        }),
        (false, true) => Some(MaterializationChange::Delete {
            node: delta.node,
            parent_before: parent,
            source,
        }),
        _ => None,
    }
}

struct StructuralChange {
    first_parent: Option<NodeId>,
    final_parent: NodeId,
    inserted: bool,
    source: Option<MaterializationSource>,
}

struct VisibilityChange {
    visible_before: bool,
    visible_after: bool,
    last_parent: Option<NodeId>,
    source: Option<MaterializationSource>,
}

/// One node's changes, folded in the order they were made.
#[derive(Default)]
struct NodeChanges {
    structure: Option<StructuralChange>,
    visibility: Option<VisibilityChange>,
    /// The payload carried by the latest change that carried one: what the node stored after it.
    /// An insert without a payload leaves the stored payload as it was.
    payload: Option<Option<Vec<u8>>>,
    /// Whether an insert or payload change wrote the payload. A restore carries the stored payload
    /// without writing it.
    payload_written: bool,
    /// The source of the latest write.
    payload_source: Option<MaterializationSource>,
}

impl NodeChanges {
    fn restructure(
        &mut self,
        parent_before: Option<NodeId>,
        parent_after: NodeId,
        inserted: bool,
        source: Option<MaterializationSource>,
    ) {
        match &mut self.structure {
            Some(existing) => {
                existing.final_parent = parent_after;
                existing.inserted |= inserted;
                existing.source = latest_source(existing.source.take(), source);
            }
            None => {
                self.structure = Some(StructuralChange {
                    first_parent: parent_before,
                    final_parent: parent_after,
                    inserted,
                    source,
                })
            }
        }
    }

    fn set_visible(
        &mut self,
        visible: bool,
        parent: Option<NodeId>,
        source: Option<MaterializationSource>,
    ) {
        match &mut self.visibility {
            Some(existing) => {
                existing.visible_after = visible;
                existing.last_parent = parent;
                existing.source = latest_source(existing.source.take(), source);
            }
            None => {
                self.visibility = Some(VisibilityChange {
                    visible_before: !visible,
                    visible_after: visible,
                    last_parent: parent,
                    source,
                })
            }
        }
    }

    fn write_payload(&mut self, payload: Option<Vec<u8>>, source: Option<MaterializationSource>) {
        self.payload = Some(payload);
        self.payload_written = true;
        self.payload_source = source;
    }
}

fn source_key(source: &MaterializationSource) -> (u64, &[u8], u64) {
    (
        source.operation.lamport,
        source.operation.id.replica.as_bytes(),
        source.operation.id.counter,
    )
}

fn latest_source(
    left: Option<MaterializationSource>,
    right: Option<MaterializationSource>,
) -> Option<MaterializationSource> {
    match (left, right) {
        (None, None) => None,
        (Some(source), None) | (None, Some(source)) => Some(source),
        (Some(left), Some(right)) => {
            if source_key(&right) >= source_key(&left) {
                Some(right)
            } else {
                Some(left)
            }
        }
    }
}

/// Collapse changes, given in the order they were made, into each node's net visible change.
///
/// A change that carries a payload carries what the node stores after the last change. Coalesce a
/// batch's raw changes at once: coalescing each op's changes first turns a payload write and the
/// restore it causes into one restore, which an earlier delete in the batch then cancels, hiding
/// the write.
pub(crate) fn coalesce_materialization_changes(
    changes: Vec<MaterializationChange>,
) -> Vec<MaterializationChange> {
    use std::collections::BTreeMap;

    let mut nodes: BTreeMap<NodeId, NodeChanges> = BTreeMap::new();
    for change in changes {
        let folded = nodes.entry(change.node()).or_default();
        match change {
            MaterializationChange::Insert {
                parent_after,
                payload,
                source,
                ..
            } => {
                if payload.is_some() {
                    folded.write_payload(payload, source.clone());
                }
                folded.restructure(None, parent_after, true, source);
            }
            MaterializationChange::Move {
                parent_before,
                parent_after,
                source,
                ..
            } => folded.restructure(parent_before, parent_after, false, source),
            MaterializationChange::Delete {
                parent_before,
                source,
                ..
            } => folded.set_visible(false, parent_before, source),
            MaterializationChange::Restore {
                parent_after,
                payload,
                source,
                ..
            } => {
                folded.payload = Some(payload);
                folded.set_visible(true, parent_after, source);
            }
            MaterializationChange::Payload {
                payload, source, ..
            } => folded.write_payload(payload, source),
        }
    }

    let mut structural = Vec::new();
    let mut visibility = Vec::new();
    let mut payloads = Vec::new();
    for (node, folded) in nodes {
        let payload = folded.payload.flatten();
        let payload_source = folded.payload_source;
        let mut payload_reported = false;
        if let Some(change) = folded.structure {
            if change.inserted {
                payload_reported = true;
                structural.push(MaterializationChange::Insert {
                    node,
                    parent_after: change.final_parent,
                    payload: payload.clone(),
                    source: latest_source(change.source, payload_source.clone()),
                });
            } else {
                structural.push(MaterializationChange::Move {
                    node,
                    parent_before: change.first_parent,
                    parent_after: change.final_parent,
                    source: change.source,
                });
            }
        }
        let visible_after = folded.visibility.as_ref().is_none_or(|change| change.visible_after);
        if let Some(change) = folded.visibility {
            if change.visible_before != change.visible_after {
                if change.visible_after {
                    payload_reported = true;
                    visibility.push(MaterializationChange::Restore {
                        node,
                        parent_after: change.last_parent,
                        payload: payload.clone(),
                        source: latest_source(change.source, payload_source.clone()),
                    });
                } else {
                    visibility.push(MaterializationChange::Delete {
                        node,
                        parent_before: change.last_parent,
                        source: change.source,
                    });
                }
            }
        }
        if folded.payload_written && visible_after && !payload_reported {
            payloads.push(MaterializationChange::Payload {
                node,
                payload,
                source: payload_source,
            });
        }
    }

    structural.extend(visibility);
    structural.extend(payloads);
    structural
}

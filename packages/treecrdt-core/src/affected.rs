use crate::ids::NodeId;
use crate::ops::OperationKind;

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

pub(crate) fn sorted_node_ids(nodes: impl IntoIterator<Item = NodeId>) -> Vec<NodeId> {
    let mut ids: Vec<NodeId> = nodes.into_iter().collect();
    ids.sort();
    ids.dedup();
    ids
}

pub(crate) fn parent_hints_from(parent: Option<NodeId>) -> Vec<NodeId> {
    parent.into_iter().collect()
}

fn push_if_live(nodes: &mut Vec<NodeId>, id: NodeId) {
    if id != NodeId::TRASH {
        nodes.push(id);
    }
}

fn push_snapshot_parent(nodes: &mut Vec<NodeId>, snapshot_parent: Option<NodeId>) {
    if let Some(p) = snapshot_parent {
        push_if_live(nodes, p);
    }
}

pub(crate) fn direct_affected_nodes(
    snapshot_parent: Option<NodeId>,
    kind: &OperationKind,
) -> Vec<NodeId> {
    let mut nodes = Vec::new();
    push_if_live(&mut nodes, kind.node());
    match kind {
        OperationKind::Insert { parent, .. } => {
            push_snapshot_parent(&mut nodes, snapshot_parent);
            push_if_live(&mut nodes, *parent);
        }
        OperationKind::Move { new_parent, .. } => {
            push_snapshot_parent(&mut nodes, snapshot_parent);
            push_if_live(&mut nodes, *new_parent);
        }
        OperationKind::Delete { .. } | OperationKind::Tombstone { .. } => {
            push_snapshot_parent(&mut nodes, snapshot_parent);
        }
        OperationKind::Payload { .. } => {}
    }
    sorted_node_ids(nodes)
}

//! History-derived force-revert. Adapted from the canonical inversion in TreeCRDT #175.
//! Plans stay private: callers receive only newly authored operations, not replay instructions.

use std::collections::{HashMap, HashSet, VecDeque};

use crate::{
    Error, LamportClock, LocalPlacement, MemoryStorage, NodeId, NodeStore, NoopParentOpIndex,
    NoopStorage, Operation, OperationId, OperationKind, ReplicaId, Result, Storage, TreeCrdt,
};

pub(crate) enum Inverse {
    Delete(NodeId),
    Move {
        node: NodeId,
        parent: NodeId,
        placement: LocalPlacement,
    },
    Payload(NodeId, Option<Vec<u8>>),
}

impl Inverse {
    fn node(&self) -> NodeId {
        match self {
            Self::Delete(node) | Self::Payload(node, _) | Self::Move { node, .. } => *node,
        }
    }
}

type HistoryState = TreeCrdt<NoopStorage, LamportClock>;

fn restore_placement(state: &HistoryState, node: NodeId) -> Result<Inverse> {
    if !state.is_known(node)? || state.is_tombstoned(node)? {
        return Ok(Inverse::Delete(node));
    }
    let parent = state.node_store().parent(node)?.ok_or_else(|| {
        Error::InvalidOperation("cannot restore a detached node during revert".into())
    })?;
    let placement = if parent == NodeId::TRASH {
        LocalPlacement::First
    } else {
        let children = state.children(parent)?;
        let index = children.iter().position(|child| *child == node).ok_or_else(|| {
            Error::InconsistentState("revert target is absent from its parent's children".into())
        })?;
        if index == 0 {
            LocalPlacement::First
        } else {
            LocalPlacement::After(children[index - 1])
        }
    };
    Ok(Inverse::Move {
        node,
        parent,
        placement,
    })
}

fn inverse_before(state: &HistoryState, op: &Operation) -> Result<Vec<Inverse>> {
    let node = op.kind.node();
    match &op.kind {
        OperationKind::Insert { payload, .. } => {
            let restore = restore_placement(state, node)?;
            let mut actions = Vec::with_capacity(2);
            if payload.is_some() {
                actions.push(Inverse::Payload(node, state.payload(node)?));
            }
            actions.push(restore);
            Ok(actions)
        }
        OperationKind::Move { .. } => Ok(vec![restore_placement(state, node)?]),
        OperationKind::Delete { .. } | OperationKind::Tombstone { .. } => {
            if state.is_known(node)?
                && !state.is_tombstoned(node)?
                && state.node_store().parent(node)?.is_none()
            {
                Ok(vec![Inverse::Payload(node, state.payload(node)?)])
            } else {
                Ok(vec![restore_placement(state, node)?])
            }
        }
        OperationKind::Payload { .. } => {
            let mut actions = vec![Inverse::Payload(node, state.payload(node)?)];
            if !state.is_known(node)? || state.is_tombstoned(node)? {
                // Restoring a payload can revive a defensive deletion. Hide the node again only
                // after writing its previous payload, so the compensating delete observes it.
                actions.push(Inverse::Delete(node));
            }
            Ok(actions)
        }
    }
}

/// Reconstructs each selected operation's canonical predecessor with the normal materializer.
/// Receipt order and arrival order do not affect inversion; duplicate IDs are selected once.
pub(crate) fn derive_inverse(history: &impl Storage, ids: &[OperationId]) -> Result<Vec<Inverse>> {
    let mut pending: HashSet<_> = ids.iter().cloned().collect();
    if pending.is_empty() {
        return Ok(Vec::new());
    }
    let mut state = TreeCrdt::new(
        ReplicaId::new(b"history-invert"),
        NoopStorage,
        LamportClock::default(),
    )?;
    let mut index = NoopParentOpIndex;
    let mut seq = 0;
    let mut groups = Vec::with_capacity(pending.len());
    history.scan_since(0, &mut |op| {
        if pending.is_empty() {
            return Ok(());
        }
        if pending.remove(&op.meta.id) {
            groups.push(inverse_before(&state, &op)?);
        }
        state.apply_remote_with_materialization_seq(op, &mut index, &mut seq)?;
        Ok(())
    })?;
    if !pending.is_empty() {
        return Err(Error::MissingDependency(format!(
            "missing {} edit operation(s) from history",
            pending.len()
        )));
    }
    let mut actions = Vec::new();
    for action in groups.into_iter().rev().flatten() {
        if matches!(
            (&action, actions.last()),
            (Inverse::Payload(node, _), Some(Inverse::Payload(previous, _))) if node == previous
        ) {
            actions.pop();
        }
        actions.push(action);
    }
    Ok(actions)
}

pub(crate) fn apply_inverse(
    tree: &mut TreeCrdt<MemoryStorage, LamportClock>,
    actions: Vec<Inverse>,
) -> Result<Vec<Operation>> {
    let mut operations = Vec::with_capacity(actions.len());
    let mut previous = HashMap::new();
    let mut predecessors = Vec::with_capacity(actions.len());
    let mut restorations: HashMap<_, VecDeque<usize>> = HashMap::new();
    for (index, action) in actions.iter().enumerate() {
        predecessors.push(previous.insert(action.node(), index));
        if let Inverse::Move { node, parent, .. } = action {
            restorations.entry((*node, None)).or_default().push_back(index);
            restorations.entry((*node, Some(*parent))).or_default().push_back(index);
        }
    }
    let mut pending: Vec<_> = actions.into_iter().map(Some).collect();
    let mut visiting = vec![false; pending.len()];
    for first in 0..pending.len() {
        // Iterative DFS avoids repeatedly walking the entire pending group (and avoids the
        // native stack limit for deep trees). Only selected restoration dependencies can run
        // early; each node's predecessor preserves its own canonical inverse ordering.
        let mut stack = vec![first];
        while let Some(&index) = stack.last() {
            let Some(action) = &pending[index] else {
                stack.pop();
                continue;
            };
            visiting[index] = true;
            let dependency = if let Some(previous) =
                predecessors[index].filter(|&i| pending[i].is_some())
            {
                Some(previous)
            } else if let Some((node, anchor_parent, message)) = missing_dependency(tree, action)? {
                let candidates = restorations.entry((node, anchor_parent)).or_default();
                while candidates.front().is_some_and(|&i| pending[i].is_none()) {
                    candidates.pop_front();
                }
                Some(*candidates.front().ok_or_else(|| Error::MissingDependency(message.into()))?)
            } else {
                None
            };
            if let Some(dependency) = dependency {
                if visiting[dependency] {
                    return Err(Error::InvalidOperation(
                        "revert restoration dependencies form a cycle".into(),
                    ));
                }
                stack.push(dependency);
                continue;
            }
            operations.push(apply_action(
                tree,
                pending[index].take().expect("pending inverse"),
            )?);
            visiting[index] = false;
            stack.pop();
        }
    }
    Ok(operations)
}

/// Only dependencies with an explicit selected restoration can be deferred. External stale
/// destinations/anchors must fail, rather than being implicitly revived or assigned a fallback.
fn missing_dependency(
    tree: &TreeCrdt<MemoryStorage, LamportClock>,
    action: &Inverse,
) -> Result<Option<(NodeId, Option<NodeId>, &'static str)>> {
    let Inverse::Move {
        node,
        parent,
        placement,
    } = *action
    else {
        return Ok(None);
    };
    if parent != NodeId::TRASH && (!tree.is_known(parent)? || tree.is_tombstoned(parent)?) {
        return Ok(Some((
            parent,
            None,
            "revert destination is missing or deleted",
        )));
    }
    if let LocalPlacement::After(anchor) = placement {
        if !tree.children(parent)?.contains(&anchor) || anchor == node {
            return Ok(Some((
                anchor,
                Some(parent),
                "revert anchor is missing, deleted, or moved",
            )));
        }
    }
    Ok(None)
}

fn apply_action(
    tree: &mut TreeCrdt<MemoryStorage, LamportClock>,
    action: Inverse,
) -> Result<Operation> {
    let (op, _) = match action {
        Inverse::Delete(node) => tree.local_delete(node)?,
        Inverse::Payload(node, payload) => tree.local_payload(node, payload)?,
        Inverse::Move {
            node,
            parent,
            placement,
        } => {
            // Ordinary CRDT moves may be ignored to preserve convergence, or revive a
            // deleted destination. Explicit user restoration must not report such a no-op
            // as success or silently resurrect a destination outside the selected receipt.
            let mut current = Some(parent);
            let mut visited = HashSet::new();
            while let Some(ancestor) = current {
                if ancestor == node || !visited.insert(ancestor) {
                    return Err(Error::InvalidOperation(
                        "revert would create a cycle".into(),
                    ));
                }
                if ancestor == NodeId::ROOT || ancestor == NodeId::TRASH {
                    break;
                }
                current = tree.node_store().parent(ancestor)?;
            }
            tree.local_move(node, parent, placement)?
        }
    };
    Ok(op)
}

use crate::error::{Error, Result};
use crate::ids::{NodeId, OperationId};
use crate::version_vector::VersionVector;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug)]
pub struct NodeExport {
    pub node: NodeId,
    pub parent: Option<NodeId>,
    pub children: Vec<NodeId>,
    pub last_change: VersionVector,
    pub deleted_at: Option<VersionVector>,
}

#[derive(Clone, Debug)]
pub struct NodeSnapshotExport {
    pub parent: Option<NodeId>,
    pub order_key: Option<Vec<u8>>,
}

/// A coalesced visible change produced while advancing materialized state.
///
/// This is intentionally higher-level than raw operations: a replay pass may collapse multiple
/// operations for the same node into one final visible insert/move/delete/restore/payload change.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(tag = "kind", rename_all = "camelCase"))]
pub enum MaterializationChange {
    Insert {
        node: NodeId,
        parent_after: NodeId,
    },
    Move {
        node: NodeId,
        parent_before: Option<NodeId>,
        parent_after: NodeId,
    },
    Delete {
        node: NodeId,
        parent_before: Option<NodeId>,
    },
    Restore {
        node: NodeId,
        parent_after: Option<NodeId>,
    },
    Payload {
        node: NodeId,
    },
}

impl MaterializationChange {
    pub fn node(&self) -> NodeId {
        match self {
            Self::Insert { node, .. }
            | Self::Move { node, .. }
            | Self::Delete { node, .. }
            | Self::Restore { node, .. }
            | Self::Payload { node } => *node,
        }
    }

    /// Convert a structured change to the node ids that may need storage row patching.
    ///
    /// Adapters should expose `MaterializationChange`/`MaterializationOutcome` to consumers rather
    /// than this derived helper. The helper exists so storage backends can keep their existing
    /// "patch rows for these ids" machinery without making affected ids part of the public event API.
    pub fn affected_nodes(&self) -> Vec<NodeId> {
        let mut nodes = vec![self.node()];
        match self {
            Self::Insert { parent_after, .. } => {
                nodes.push(*parent_after);
            }
            Self::Move {
                parent_before,
                parent_after,
                ..
            } => {
                nodes.extend(parent_before.iter().copied());
                nodes.push(*parent_after);
            }
            Self::Delete { parent_before, .. } => nodes.extend(parent_before.iter().copied()),
            Self::Restore { parent_after, .. } => nodes.extend(parent_after.iter().copied()),
            Self::Payload { .. } => {}
        }
        nodes.retain(|node| *node != NodeId::TRASH);
        nodes
    }
}

/// The result of one materialization pass.
///
/// `head_seq` is the materialized op-log frontier after the pass. Empty `changes` means the pass
/// did not change any visible materialized state and should not emit a public event.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "camelCase"))]
pub struct MaterializationOutcome {
    pub head_seq: u64,
    pub changes: Vec<MaterializationChange>,
}

impl MaterializationOutcome {
    pub fn empty(head_seq: u64) -> Self {
        Self {
            head_seq,
            changes: Vec::new(),
        }
    }

    pub fn affected_nodes(&self) -> Vec<NodeId> {
        let mut nodes: Vec<NodeId> =
            self.changes.iter().flat_map(MaterializationChange::affected_nodes).collect();
        nodes.sort();
        nodes.dedup();
        nodes
    }
}

#[derive(Clone, Debug)]
pub struct ApplyDelta {
    pub snapshot: NodeSnapshotExport,
    pub changes: Vec<MaterializationChange>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LocalPlacement {
    First,
    Last,
    After(NodeId),
}

impl LocalPlacement {
    pub fn from_parts(placement: &str, after: Option<NodeId>) -> Result<Self> {
        match placement {
            "first" => Ok(Self::First),
            "last" => Ok(Self::Last),
            "after" => {
                let Some(after_id) = after else {
                    return Err(Error::InvalidOperation(
                        "missing after for placement=after".into(),
                    ));
                };
                Ok(Self::After(after_id))
            }
            _ => Err(Error::InvalidOperation("invalid placement".into())),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct LocalFinalizePlan {
    pub parent_hints: Vec<NodeId>,
    pub extra_index_records: Vec<(NodeId, OperationId)>,
    pub changes: Vec<MaterializationChange>,
}

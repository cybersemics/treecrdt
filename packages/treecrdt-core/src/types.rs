use crate::error::{Error, Result};
use crate::ids::{NodeId, OperationId};
use crate::version_vector::VersionVector;

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

#[derive(Clone, Debug)]
pub struct ApplyDelta {
    pub snapshot: NodeSnapshotExport,
    pub affected_nodes: Vec<NodeId>,
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
}

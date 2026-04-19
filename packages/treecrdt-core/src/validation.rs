use std::collections::HashSet;

use crate::error::{Error, Result};
use crate::ids::NodeId;
use crate::traits::{Clock, NodeStore, PayloadStore, Storage};
use crate::tree::TreeCrdt;

impl<S, C, N, P> TreeCrdt<S, C, N, P>
where
    S: Storage,
    C: Clock,
    N: NodeStore,
    P: PayloadStore,
{
    pub fn validate_invariants(&self) -> Result<()> {
        let nodes = self.node_store();
        for pid in nodes.all_nodes()? {
            let pchildren = nodes.children(pid)?;
            let mut seen = HashSet::new();
            for child in pchildren {
                if !seen.insert(child) {
                    return Err(Error::InvalidOperation("duplicate child entry".into()));
                }
                if !nodes.exists(child)? {
                    return Err(Error::InvalidOperation("child not present in nodes".into()));
                }
                if nodes.parent(child)? != Some(pid) {
                    return Err(Error::InvalidOperation("child parent mismatch".into()));
                }
            }
        }

        for node in nodes.all_nodes()? {
            if self.has_cycle_from(node)? {
                return Err(Error::InvalidOperation("cycle detected".into()));
            }
        }
        Ok(())
    }

    fn has_cycle_from(&self, start: NodeId) -> Result<bool> {
        if start == NodeId::ROOT || start == NodeId::TRASH {
            return Ok(false);
        }
        let nodes = self.node_store();
        let mut visited = HashSet::new();
        let mut current = Some(start);
        while let Some(n) = current {
            if !visited.insert(n) {
                return Ok(true);
            }
            if n == NodeId::ROOT || n == NodeId::TRASH {
                return Ok(false);
            }
            current = nodes.parent(n)?;
        }
        Ok(false)
    }
}

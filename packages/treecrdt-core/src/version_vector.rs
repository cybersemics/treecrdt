use std::collections::HashMap;

use crate::ids::{Lamport, ReplicaId};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// Dotted version vector tracks causal knowledge per replica.
/// Each entry maps a replica ID to the highest Lamport timestamp seen from that replica.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct VersionVector {
    entries: HashMap<ReplicaId, Lamport>,
}

impl VersionVector {
    /// Create a new empty version vector.
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    pub fn observe(&mut self, replica: &ReplicaId, lamport: Lamport) {
        let entry = self.entries.entry(replica.clone()).or_insert(0);
        *entry = (*entry).max(lamport);
    }

    pub fn merge(&mut self, other: &VersionVector) {
        for (replica, lamport) in &other.entries {
            self.observe(replica, *lamport);
        }
    }

    pub fn is_aware_of(&self, other: &VersionVector) -> bool {
        for (replica, other_lamport) in &other.entries {
            let self_lamport = self.entries.get(replica).copied().unwrap_or(0);
            if self_lamport < *other_lamport {
                return false;
            }
        }
        true
    }

    /// Get the Lamport timestamp for a specific replica, or 0 if not present.
    pub fn get(&self, replica: &ReplicaId) -> Lamport {
        self.entries.get(replica).copied().unwrap_or(0)
    }

    /// Check if this version vector is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Get all entries as a reference.
    pub fn entries(&self) -> &HashMap<ReplicaId, Lamport> {
        &self.entries
    }
}

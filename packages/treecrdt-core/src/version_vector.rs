use std::collections::HashMap;

use crate::ids::ReplicaId;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
struct ReplicaVersion {
    /// Highest contiguous counter observed (i.e. we've seen `1..=frontier`).
    frontier: u64,
    /// Additional observed counters beyond the contiguous frontier, stored as disjoint inclusive ranges.
    ///
    /// Invariant: sorted by start, non-overlapping, and every range has `start > frontier + 1`.
    ranges: Vec<(u64, u64)>,
}

impl ReplicaVersion {
    fn max_seen(&self) -> u64 {
        self.ranges
            .last()
            .map(|&(_, end)| end.max(self.frontier))
            .unwrap_or(self.frontier)
    }

    fn observe(&mut self, counter: u64) {
        if counter == 0 {
            return;
        }

        if counter <= self.frontier {
            return;
        }

        if counter == self.frontier + 1 {
            self.frontier = counter;
            self.absorb_frontier_ranges();
            return;
        }

        // Insert as a single-point range, merging with neighbors as needed.
        let mut idx = 0;
        while idx < self.ranges.len() && self.ranges[idx].0 < counter {
            idx += 1;
        }

        // If the previous range already covers it, done.
        if idx > 0 {
            let (prev_start, prev_end) = self.ranges[idx - 1];
            if counter >= prev_start && counter <= prev_end {
                return;
            }
            if counter == prev_end + 1 {
                self.ranges[idx - 1].1 = counter;
                // Merge with next if now adjacent/overlapping.
                self.merge_with_next(idx - 1);
                return;
            }
        }

        // If the next range is adjacent, extend it backwards.
        if idx < self.ranges.len() {
            let (next_start, next_end) = self.ranges[idx];
            if counter == next_start.saturating_sub(1) {
                self.ranges[idx].0 = counter;
                // Possibly merge with previous now.
                if idx > 0 {
                    self.merge_with_next(idx - 1);
                }
                return;
            }
            if counter >= next_start && counter <= next_end {
                return;
            }
        }

        self.ranges.insert(idx, (counter, counter));
    }

    fn merge_with_next(&mut self, idx: usize) {
        if idx + 1 >= self.ranges.len() {
            return;
        }
        let (a_start, a_end) = self.ranges[idx];
        let (b_start, b_end) = self.ranges[idx + 1];
        if b_start <= a_end + 1 {
            self.ranges[idx] = (a_start, a_end.max(b_end));
            self.ranges.remove(idx + 1);
        }
    }

    fn absorb_frontier_ranges(&mut self) {
        loop {
            let Some(&(start, end)) = self.ranges.first() else {
                break;
            };
            if start == self.frontier + 1 {
                self.frontier = end;
                self.ranges.remove(0);
            } else {
                break;
            }
        }
    }

    fn contains_range(&self, start: u64, end: u64) -> bool {
        if start == 0 || end == 0 || start > end {
            return false;
        }
        if end <= self.frontier {
            return true;
        }
        if start <= self.frontier {
            // This range would require `frontier + 1`, which by definition is missing.
            return false;
        }

        // Need a single extra range that covers [start, end].
        for &(rs, re) in &self.ranges {
            if rs > start {
                return false;
            }
            if rs <= start && re >= end {
                return true;
            }
        }
        false
    }

    fn is_superset_of(&self, other: &ReplicaVersion) -> bool {
        if self.frontier < other.frontier {
            return false;
        }
        for &(start, end) in &other.ranges {
            if !self.contains_range(start, end) {
                return false;
            }
        }
        true
    }

    fn union(&mut self, other: &ReplicaVersion) {
        if other.frontier == 0 && other.ranges.is_empty() {
            return;
        }

        let mut all: Vec<(u64, u64)> = Vec::new();
        if self.frontier > 0 {
            all.push((1, self.frontier));
        }
        all.extend(self.ranges.iter().copied());
        if other.frontier > 0 {
            all.push((1, other.frontier));
        }
        all.extend(other.ranges.iter().copied());

        all.sort_by_key(|&(s, _)| s);

        let mut merged: Vec<(u64, u64)> = Vec::new();
        for (start, end) in all {
            if start == 0 || end == 0 || start > end {
                continue;
            }
            if let Some(last) = merged.last_mut() {
                if start <= last.1 + 1 {
                    last.1 = last.1.max(end);
                    continue;
                }
            }
            merged.push((start, end));
        }

        // The first merged range determines the new contiguous frontier if it starts at 1.
        if merged.first().map(|&(s, _)| s) == Some(1) {
            self.frontier = merged[0].1;
            merged.remove(0);
        } else {
            self.frontier = 0;
        }

        self.ranges = merged;
        self.absorb_frontier_ranges();

        // Enforce invariant: no range starts at frontier+1 (it would be absorbed).
        if self.ranges.first().map(|&(s, _)| s) == Some(self.frontier + 1) {
            self.absorb_frontier_ranges();
        }
    }
}

/// Gap-aware version vector (frontier + ranges) keyed by per-replica operation counters.
///
/// This represents causal knowledge without assuming "contiguous time" (i.e. it can represent holes).
#[derive(Clone, Debug, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct VersionVector {
    entries: HashMap<ReplicaId, ReplicaVersion>,
}

impl VersionVector {
    /// Create a new empty version vector.
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    pub fn observe(&mut self, replica: &ReplicaId, counter: u64) {
        self.entries
            .entry(replica.clone())
            .or_default()
            .observe(counter);
    }

    pub fn merge(&mut self, other: &VersionVector) {
        for (replica, other_replica) in &other.entries {
            self.entries
                .entry(replica.clone())
                .or_default()
                .union(other_replica);
        }
    }

    pub fn is_aware_of(&self, other: &VersionVector) -> bool {
        for (replica, other_replica) in &other.entries {
            let self_replica = self.entries.get(replica).cloned().unwrap_or_default();
            if !self_replica.is_superset_of(other_replica) {
                return false;
            }
        }
        true
    }

    /// Get the maximum observed counter for a specific replica, or 0 if not present.
    ///
    /// Note: this is NOT the contiguous frontier; use `frontier()` when you need gap-aware semantics.
    pub fn get(&self, replica: &ReplicaId) -> u64 {
        self.entries.get(replica).map(|v| v.max_seen()).unwrap_or(0)
    }

    /// Get the contiguous frontier (i.e. we've observed `1..=frontier`) for a replica.
    pub fn frontier(&self, replica: &ReplicaId) -> u64 {
        self.entries.get(replica).map(|v| v.frontier).unwrap_or(0)
    }

    /// Check if this version vector is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Get the maximum observed counter for each replica.
    pub fn entries(&self) -> HashMap<ReplicaId, u64> {
        self.entries
            .iter()
            .map(|(replica, v)| (replica.clone(), v.max_seen()))
            .collect()
    }

    /// Get the contiguous frontier for each replica.
    pub fn frontiers(&self) -> HashMap<ReplicaId, u64> {
        self.entries
            .iter()
            .map(|(replica, v)| (replica.clone(), v.frontier))
            .collect()
    }
}

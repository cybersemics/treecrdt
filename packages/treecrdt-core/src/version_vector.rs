use std::collections::HashMap;

use crate::ids::ReplicaId;
use crate::{Error, Result};

const VERSION_VECTOR_V0_MAGIC: &[u8; 4] = b"TCVV";
const VERSION_VECTOR_V0_VERSION: u8 = 0;
const VERSION_VECTOR_V0_HEADER_LEN: usize = VERSION_VECTOR_V0_MAGIC.len() + 1 + 4;
const VERSION_VECTOR_V0_MIN_ENTRY_LEN: usize = 4 + 8 + 4;
const VERSION_VECTOR_V0_RANGE_LEN: usize = 16;

#[derive(Clone, Debug, PartialEq, Eq, Default)]
struct ReplicaVersion {
    /// Highest contiguous counter observed (i.e. we've seen `1..=frontier`).
    frontier: u64,
    /// Additional observed counters beyond the contiguous frontier, stored as disjoint inclusive ranges.
    ///
    /// Invariant: sorted by start, non-overlapping, and every range has `start > frontier + 1`.
    ranges: Vec<(u64, u64)>,
}

impl ReplicaVersion {
    fn is_empty(&self) -> bool {
        self.frontier == 0 && self.ranges.is_empty()
    }

    fn validate_ranges(&self) -> std::result::Result<(), &'static str> {
        let mut previous_end = self.frontier;
        for &(start, end) in &self.ranges {
            validate_next_range(previous_end, start, end)?;
            previous_end = end;
        }
        Ok(())
    }

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

        if Some(counter) == self.frontier.checked_add(1) {
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
            if Some(counter) == prev_end.checked_add(1) {
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
        if b_start <= a_end.saturating_add(1) {
            self.ranges[idx] = (a_start, a_end.max(b_end));
            self.ranges.remove(idx + 1);
        }
    }

    fn absorb_frontier_ranges(&mut self) {
        while let Some(&(start, end)) = self.ranges.first() {
            if Some(start) == self.frontier.checked_add(1) {
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
                if start <= last.1.saturating_add(1) {
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
        if self.ranges.first().map(|&(s, _)| s) == self.frontier.checked_add(1) {
            self.absorb_frontier_ranges();
        }
    }
}

fn validate_next_range(
    previous_end: u64,
    start: u64,
    end: u64,
) -> std::result::Result<(), &'static str> {
    if start == 0 || start > end {
        return Err("ranges must have positive inclusive bounds");
    }
    if start <= previous_end.saturating_add(1) {
        return Err("ranges must be sorted, separated, and strictly beyond the frontier");
    }
    Ok(())
}

/// Gap-aware version vector (frontier + ranges) keyed by per-replica operation counters.
///
/// This represents causal knowledge without assuming "contiguous time" (i.e. it can represent holes).
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct VersionVector {
    entries: HashMap<ReplicaId, ReplicaVersion>,
}

#[cfg(feature = "serde")]
mod serde_impl {
    //! Generic serde support for diagnostics and snapshots only.
    //! Protocol and storage boundaries must use the explicit v0 binary codec.

    use super::{ReplicaVersion, VersionVector};
    use crate::ids::ReplicaId;
    use serde::{de::Error as _, Deserialize, Deserializer, Serialize, Serializer};
    use std::collections::HashMap;

    #[derive(Clone, Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct VersionVectorEntry {
        replica: Vec<u8>,
        frontier: u64,
        ranges: Vec<(u64, u64)>,
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct VersionVectorRepr {
        entries: Vec<VersionVectorEntry>,
    }

    impl Serialize for VersionVector {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            let mut entries: Vec<VersionVectorEntry> = self
                .entries
                .iter()
                .map(|(replica, version)| VersionVectorEntry {
                    replica: replica.0.clone(),
                    frontier: version.frontier,
                    ranges: version.ranges.clone(),
                })
                .collect();
            entries.sort_by(|a, b| a.replica.cmp(&b.replica));
            VersionVectorRepr { entries }.serialize(serializer)
        }
    }

    impl<'de> Deserialize<'de> for VersionVector {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            let repr = VersionVectorRepr::deserialize(deserializer)?;
            if repr.entries.windows(2).any(|pair| pair[0].replica >= pair[1].replica) {
                return Err(D::Error::custom(
                    "version vector replicas must be unique and strictly sorted",
                ));
            }
            let mut entries = HashMap::with_capacity(repr.entries.len());
            for entry in repr.entries {
                let version = ReplicaVersion {
                    frontier: entry.frontier,
                    ranges: entry.ranges,
                };
                if version.is_empty() {
                    return Err(D::Error::custom(
                        "version vector entries must not be semantically empty",
                    ));
                }
                version.validate_ranges().map_err(D::Error::custom)?;
                entries.insert(ReplicaId(entry.replica), version);
            }
            Ok(VersionVector { entries })
        }
    }
}

struct VersionVectorV0Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> VersionVectorV0Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.offset)
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8]> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or_else(|| invalid_v0("field length overflow"))?;
        let value =
            self.bytes.get(self.offset..end).ok_or_else(|| invalid_v0("truncated input"))?;
        self.offset = end;
        Ok(value)
    }

    fn read_u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn read_u32(&mut self) -> Result<u32> {
        let bytes: [u8; 4] =
            self.take(4)?.try_into().map_err(|_| invalid_v0("invalid u32 field"))?;
        Ok(u32::from_be_bytes(bytes))
    }

    fn read_u64(&mut self) -> Result<u64> {
        let bytes: [u8; 8] =
            self.take(8)?.try_into().map_err(|_| invalid_v0("invalid u64 field"))?;
        Ok(u64::from_be_bytes(bytes))
    }
}

fn invalid_v0(message: impl Into<String>) -> Error {
    Error::InvalidVersionVector(format!("v0: {}", message.into()))
}

fn checked_u32(value: usize, field: &str) -> Result<u32> {
    value.try_into().map_err(|_| invalid_v0(format!("{field} exceeds u32")))
}

fn checked_add_size(total: &mut usize, value: usize) -> Result<()> {
    *total = total.checked_add(value).ok_or_else(|| invalid_v0("encoded size overflow"))?;
    Ok(())
}

impl VersionVector {
    /// Create a new empty version vector.
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    pub fn observe(&mut self, replica: &ReplicaId, counter: u64) {
        if counter == 0 {
            return;
        }
        self.entries.entry(replica.clone()).or_default().observe(counter);
    }

    pub fn merge(&mut self, other: &VersionVector) {
        for (replica, other_replica) in &other.entries {
            if other_replica.is_empty() {
                continue;
            }
            self.entries.entry(replica.clone()).or_default().union(other_replica);
        }
    }

    /// Encode this value with the canonical TreeCRDT VersionVector v0 binary codec.
    ///
    /// The encoding is stable across storage adapters and runtimes. Entries are sorted by raw
    /// replica bytes, counters are unsigned 64-bit big-endian integers, and semantically empty
    /// entries are omitted.
    pub fn encode_v0(&self) -> Result<Vec<u8>> {
        let mut entries: Vec<(&ReplicaId, &ReplicaVersion)> =
            self.entries.iter().filter(|(_, version)| !version.is_empty()).collect();
        entries.sort_by(|(left, _), (right, _)| left.as_bytes().cmp(right.as_bytes()));

        let entry_count = checked_u32(entries.len(), "entry count")?;
        let mut encoded_len = VERSION_VECTOR_V0_HEADER_LEN;
        for (replica, version) in &entries {
            checked_u32(replica.as_bytes().len(), "replica id length")?;
            checked_u32(version.ranges.len(), "range count")?;
            version.validate_ranges().map_err(invalid_v0)?;
            checked_add_size(&mut encoded_len, 4)?;
            checked_add_size(&mut encoded_len, replica.as_bytes().len())?;
            checked_add_size(&mut encoded_len, 8 + 4)?;
            let ranges_len = version
                .ranges
                .len()
                .checked_mul(VERSION_VECTOR_V0_RANGE_LEN)
                .ok_or_else(|| invalid_v0("encoded size overflow"))?;
            checked_add_size(&mut encoded_len, ranges_len)?;
        }

        let mut bytes = Vec::with_capacity(encoded_len);
        bytes.extend_from_slice(VERSION_VECTOR_V0_MAGIC);
        bytes.push(VERSION_VECTOR_V0_VERSION);
        bytes.extend_from_slice(&entry_count.to_be_bytes());
        for (replica, version) in entries {
            let replica_len = checked_u32(replica.as_bytes().len(), "replica id length")?;
            let range_count = checked_u32(version.ranges.len(), "range count")?;
            bytes.extend_from_slice(&replica_len.to_be_bytes());
            bytes.extend_from_slice(replica.as_bytes());
            bytes.extend_from_slice(&version.frontier.to_be_bytes());
            bytes.extend_from_slice(&range_count.to_be_bytes());
            for &(start, end) in &version.ranges {
                bytes.extend_from_slice(&start.to_be_bytes());
                bytes.extend_from_slice(&end.to_be_bytes());
            }
        }
        Ok(bytes)
    }

    /// Decode the canonical TreeCRDT VersionVector v0 binary format.
    ///
    /// Non-canonical values fail closed: replica entries must be unique and sorted, ranges must be
    /// normalized, empty semantic entries are forbidden, and the input must contain no trailing
    /// bytes.
    pub fn decode_v0(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < VERSION_VECTOR_V0_HEADER_LEN {
            return Err(invalid_v0("truncated header"));
        }

        let mut cursor = VersionVectorV0Cursor::new(bytes);
        if cursor.take(VERSION_VECTOR_V0_MAGIC.len())? != VERSION_VECTOR_V0_MAGIC {
            return Err(invalid_v0("invalid magic"));
        }
        let version = cursor.read_u8()?;
        if version != VERSION_VECTOR_V0_VERSION {
            return Err(invalid_v0(format!("unsupported version {version}")));
        }

        let entry_count = cursor.read_u32()? as usize;
        if entry_count > cursor.remaining() / VERSION_VECTOR_V0_MIN_ENTRY_LEN {
            return Err(invalid_v0("entry count exceeds remaining input"));
        }

        let mut entries = HashMap::with_capacity(entry_count);
        let mut previous_replica: Option<&[u8]> = None;
        for _ in 0..entry_count {
            let replica_len = cursor.read_u32()? as usize;
            let replica = cursor.take(replica_len)?;
            if previous_replica.is_some_and(|previous| previous >= replica) {
                return Err(invalid_v0("replica ids must be unique and strictly sorted"));
            }
            previous_replica = Some(replica);

            let frontier = cursor.read_u64()?;
            let range_count = cursor.read_u32()? as usize;
            if range_count > cursor.remaining() / VERSION_VECTOR_V0_RANGE_LEN {
                return Err(invalid_v0("range count exceeds remaining input"));
            }

            let mut ranges = Vec::with_capacity(range_count);
            let mut previous_end = frontier;
            for _ in 0..range_count {
                let start = cursor.read_u64()?;
                let end = cursor.read_u64()?;
                validate_next_range(previous_end, start, end).map_err(invalid_v0)?;
                previous_end = end;
                ranges.push((start, end));
            }

            let replica_version = ReplicaVersion { frontier, ranges };
            if replica_version.is_empty() {
                return Err(invalid_v0("entries must not be semantically empty"));
            }
            entries.insert(ReplicaId(replica.to_vec()), replica_version);
        }

        if cursor.remaining() != 0 {
            return Err(invalid_v0("trailing bytes"));
        }
        Ok(Self { entries })
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
        self.entries.iter().map(|(replica, v)| (replica.clone(), v.frontier)).collect()
    }
}

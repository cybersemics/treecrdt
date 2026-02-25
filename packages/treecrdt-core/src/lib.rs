#![forbid(unsafe_code)]
//! Core primitives for a Tree CRDT with pluggable storage, indexing, and access control.
//! This crate stays independent of concrete storage engines so it can be embedded in SQLite,
//! WASM, or any host that can satisfy the traits defined here.

pub mod error;
pub mod ids;
pub mod materialization;
pub mod ops;
pub mod order_key;
pub mod traits;
pub mod tree;
pub mod version_vector;

pub use error::{Error, Result};
pub use ids::{Lamport, NodeId, OperationId, ReplicaId};
pub use materialization::{
    apply_incremental_ops, try_incremental_materialization, MaterializationCursor,
    MaterializationHead,
};
pub use ops::{cmp_op_key, cmp_ops, Operation, OperationKind, OperationMetadata};
pub use traits::{
    Clock, IndexProvider, LamportClock, MemoryNodeStore, MemoryPayloadStore, MemoryStorage,
    NodeStore, NoopParentOpIndex, NoopStorage, ParentOpIndex, PayloadStore, Storage,
};
pub use tree::{ApplyDelta, NodeExport, NodeSnapshotExport, TreeCrdt};
pub use version_vector::VersionVector;

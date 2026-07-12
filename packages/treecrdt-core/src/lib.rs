#![forbid(unsafe_code)]
//! Core primitives for a Tree CRDT with pluggable storage, indexing, and access control.
//! This crate stays independent of concrete storage engines so it can be embedded in SQLite,
//! WASM, or any host that can satisfy the traits defined here.

pub(crate) mod affected;
pub mod error;
pub mod history;
pub mod ids;
pub mod materialization;
pub mod ops;
pub mod order_key;
pub mod traits;
pub mod tree;
pub mod types;
mod validation;
pub mod version_vector;

pub use error::{Error, Result};
pub use history::{
    derive_undo_plan_from_history, LocalEditAction, LocalEditHistory, LocalEditPlan,
};
#[cfg(feature = "serde")]
pub use history::{derive_undo_plan_from_history_json, LocalEditPlanWire};
pub use ids::{Lamport, NodeId, OperationId, ReplicaId};
pub use materialization::{
    apply_incremental_ops_with_delta, apply_persisted_remote_ops_with_delta,
    catch_up_materialized_state, materialize_persisted_remote_ops_with_delta,
    orchestrate_persisted_remote_append, CatchUpResult, IncrementalApplyResult,
    MaterializationCursor, MaterializationFrontier, MaterializationHead, MaterializationKey,
    MaterializationState, MaterializationStateRef, PersistedRemoteApplyResult,
    PersistedRemoteStores,
};
pub use ops::{cmp_op_key, cmp_ops, Operation, OperationKind, OperationMetadata};
pub use traits::{
    Clock, IndexProvider, LamportClock, MemoryNodeStore, MemoryPayloadStore, MemoryStorage,
    NodeStore, NoopParentOpIndex, NoopStorage, ParentOpIndex, PayloadStore, Storage,
};
pub use tree::TreeCrdt;
pub use types::{
    ApplyDelta, LocalFinalizePlan, LocalPlacement, MaterializationChange, MaterializationOutcome,
    MaterializationSource, MaterializationSourceOperation, NodeExport, NodeSnapshotExport,
    PreparedLocalOp,
};
pub use version_vector::VersionVector;

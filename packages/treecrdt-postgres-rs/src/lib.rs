#![forbid(unsafe_code)]
//! Postgres-backed persistence + materialization for `treecrdt-core`.
//!
//! Goal: keep all CRDT semantics in `treecrdt-core` (defensive delete, payload LWW, oprefs_children),
//! while storing state in vanilla PostgreSQL so it works on Aurora Postgres / Supabase / self-hosted.

mod opref;
mod schema;
mod store;

pub use schema::{ensure_schema, reset_doc_for_tests};
pub use store::{
    append_ops, ensure_materialized, get_ops_by_op_refs, list_op_refs_all, list_op_refs_children,
    local_delete, local_insert, local_move, local_payload, max_lamport, ops_since,
    replica_max_counter, tree_children, tree_children_page, tree_dump, tree_node_count, TreeChildRow,
    TreeRow,
};

#![forbid(unsafe_code)]
//! Postgres-backed persistence + materialization for `treecrdt-core`.
//!
//! Goal: keep all CRDT semantics in `treecrdt-core` (defensive delete, payload LWW, oprefs_children),
//! while storing state in vanilla PostgreSQL so it works on Aurora Postgres / Supabase / self-hosted.

mod local_ops;
mod opref;
mod profile;
mod reads;
mod schema;
mod store;
mod testing;

pub use local_ops::{local_delete, local_insert, local_move, local_payload};
pub use reads::{
    get_ops_by_op_refs, list_op_refs_all, list_op_refs_children,
    list_op_refs_children_with_parent_payload, max_lamport, ops_since, replica_max_counter,
    tree_children, tree_children_page, tree_dump, tree_exists, tree_node_count, tree_parent,
    tree_payload, TreeChildRow, TreeRow,
};
pub use schema::{
    clone_doc_for_tests, clone_materialized_doc_for_tests, ensure_schema, reset_doc_for_tests,
};
pub use store::{append_ops, append_ops_with_affected_nodes, ensure_materialized};
pub use testing::{prime_balanced_fanout_doc_for_tests, prime_doc_for_tests};

#![deny(unsafe_op_in_unsafe_fn)]
//! SQLite / wa-sqlite extension scaffolding for TreeCRDT.
//! The extension entrypoint is implemented against the SQLite C API (via sqlite3ext-sys)
//! so it can be built for both native SQLite and wa-sqlite. A rusqlite-backed storage
//! adapter is available for native testing and prototyping behind a feature flag.

#[cfg(feature = "rusqlite-storage")]
mod storage;
#[cfg(feature = "rusqlite-storage")]
pub use storage::SqliteStorage;

#[cfg(any(feature = "ext-sqlite", feature = "static-link"))]
pub mod extension;
#[cfg(any(feature = "ext-sqlite", feature = "static-link"))]
pub use extension::*;

#[cfg(feature = "rusqlite-storage")]
use treecrdt_core::{LamportClock, ReplicaId, TreeCrdt};

/// Temporary helper to ensure the extension crate links and can be used in tests/examples.
#[cfg(feature = "rusqlite-storage")]
pub fn demo_instance_with_sqlite() -> TreeCrdt<SqliteStorage, LamportClock> {
    TreeCrdt::new(
        ReplicaId::new(b"sqlite-ext"),
        SqliteStorage::new_in_memory().expect("in-memory sqlite"),
        LamportClock::default(),
    )
    .unwrap()
}

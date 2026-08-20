#![deny(unsafe_op_in_unsafe_fn)]
//! SQLite / wa-sqlite extension scaffolding for TreeCRDT.
//! The extension entrypoint is implemented against the SQLite C API (via sqlite3ext-sys)
//! so it can be built for both native SQLite and wa-sqlite.

#[cfg(any(feature = "ext-sqlite", feature = "static-link"))]
pub mod extension;
#[cfg(any(feature = "ext-sqlite", feature = "static-link"))]
pub use extension::*;

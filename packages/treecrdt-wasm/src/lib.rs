#![forbid(unsafe_code)]
//! WASM-friendly bridge for TreeCRDT. The actual wasm-bindgen surface will be added later.

use treecrdt_core::{AllowAllAccess, LamportClock, MemoryStorage, ReplicaId, TreeCrdt};

pub fn demo_instance() -> TreeCrdt<MemoryStorage, AllowAllAccess, LamportClock> {
    TreeCrdt::new(
        ReplicaId::new(b"wasm"),
        MemoryStorage::default(),
        AllowAllAccess,
        LamportClock::default(),
    )
}

pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

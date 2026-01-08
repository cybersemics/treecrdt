#![cfg(all(feature = "rusqlite-storage", feature = "ext-sqlite"))]
use std::env;
use std::path::PathBuf;

use rusqlite::Connection;
use serde::Deserialize;

#[derive(Deserialize)]
struct JsonOp {
    replica: Vec<u8>,
    counter: u64,
    lamport: u64,
    kind: String,
    parent: Option<u128>,
    node: u128,
    new_parent: Option<u128>,
    position: Option<u64>,
}

#[test]
fn append_and_fetch_ops_via_extension() {
    let ext_path = find_extension().expect("extension dylib path");
    let conn = Connection::open_in_memory().unwrap();
    unsafe {
        conn.load_extension_enable().unwrap();
        conn.load_extension(ext_path, Some("sqlite3_treecrdt_init")).unwrap();
    }
    conn.query_row(
        "SELECT treecrdt_set_doc_id('treecrdt-sqlite-ext-test')",
        [],
        |row| row.get::<_, i64>(0),
    )
    .unwrap();

    let replica = b"r1".to_vec();
    let parent = node_bytes(0);
    let node = node_bytes(1);

    let _: i64 = conn
        .query_row(
            "SELECT treecrdt_append_op(?1, ?2, ?3, ?4, ?5, ?6, NULL, NULL)",
            rusqlite::params![replica, 1i64, 1i64, "insert", parent, node],
            |row| row.get(0),
        )
        .unwrap();

    // Move node to the end again
    let _: i64 = conn
        .query_row(
            "SELECT treecrdt_append_op(?1, ?2, ?3, ?4, NULL, ?5, ?6, ?7)",
            rusqlite::params![b"r1".to_vec(), 2i64, 2i64, "move", node, parent, 0i64],
            |row| row.get(0),
        )
        .unwrap();

    let json: String =
        conn.query_row("SELECT treecrdt_ops_since(0)", [], |row| row.get(0)).unwrap();

    let ops: Vec<JsonOp> = serde_json::from_str(&json).unwrap();
    assert_eq!(ops.len(), 2);
    assert_eq!(ops[0].kind, "insert");
    assert_eq!(ops[1].kind, "move");
    assert_eq!(ops[0].parent, Some(0));
    assert_eq!(ops[0].node, 1);
    assert_eq!(ops[1].new_parent, Some(0));

    // With root filter we still see the same ops when filtering by root.
    let json_filtered: String = conn
        .query_row(
            "SELECT treecrdt_ops_since(0, ?1)",
            rusqlite::params![parent],
            |row| row.get(0),
        )
        .unwrap();
    let filtered: Vec<JsonOp> = serde_json::from_str(&json_filtered).unwrap();
    assert_eq!(filtered.len(), 2);
}

fn node_bytes(id: u128) -> Vec<u8> {
    id.to_be_bytes().to_vec()
}

fn find_extension() -> Option<PathBuf> {
    let manifest = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    let target_dir = env::var("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| manifest.join("..").join("..").join("target"));

    let (name, alt_name) = extension_filenames();
    let candidates = [
        target_dir.join("debug").join(&name),
        target_dir.join("debug").join("deps").join(&name),
        target_dir.join("debug").join(&alt_name),
        target_dir.join("debug").join("deps").join(&alt_name),
    ];
    candidates.into_iter().find(|p| p.exists())
}

fn extension_filenames() -> (String, String) {
    #[cfg(target_os = "macos")]
    return (
        "libtreecrdt_sqlite_ext.dylib".into(),
        "libtreecrdt_sqlite_ext.dylib".into(),
    );
    #[cfg(target_os = "linux")]
    return (
        "libtreecrdt_sqlite_ext.so".into(),
        "libtreecrdt_sqlite_ext.so".into(),
    );
    #[cfg(target_os = "windows")]
    return (
        "treecrdt_sqlite_ext.dll".into(),
        "libtreecrdt_sqlite_ext.dll".into(),
    );
}

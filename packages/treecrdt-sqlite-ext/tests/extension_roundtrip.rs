#![cfg(all(feature = "rusqlite-storage", feature = "ext-sqlite"))]
use std::env;
use std::path::PathBuf;

use rusqlite::Connection;
use serde::Deserialize;
use treecrdt_core::order_key::allocate_between;

#[derive(Deserialize)]
struct JsonOp {
    replica: Vec<u8>,
    counter: u64,
    lamport: u64,
    kind: String,
    parent: Option<[u8; 16]>,
    node: [u8; 16],
    new_parent: Option<[u8; 16]>,
    order_key: Option<Vec<u8>>,
}

#[test]
fn append_and_fetch_ops_via_extension() {
    let conn = setup_conn();

    let replica = b"r1".to_vec();
    let parent = node_bytes(0);
    let node = node_bytes(1);
    let order_key = (1u16).to_be_bytes().to_vec();

    let _: i64 = conn
        .query_row(
            "SELECT treecrdt_append_op(?1, ?2, ?3, ?4, ?5, ?6, NULL, ?7, NULL)",
            rusqlite::params![
                replica,
                1i64,
                1i64,
                "insert",
                parent,
                node,
                order_key.clone()
            ],
            |row| row.get(0),
        )
        .unwrap();

    // Move node to the end again
    let _: i64 = conn
        .query_row(
            "SELECT treecrdt_append_op(?1, ?2, ?3, ?4, NULL, ?5, ?6, ?7, NULL)",
            rusqlite::params![
                b"r1".to_vec(),
                2i64,
                2i64,
                "move",
                node,
                parent,
                order_key.clone()
            ],
            |row| row.get(0),
        )
        .unwrap();

    let json: String =
        conn.query_row("SELECT treecrdt_ops_since(0)", [], |row| row.get(0)).unwrap();

    let ops: Vec<JsonOp> = serde_json::from_str(&json).unwrap();
    assert_eq!(ops.len(), 2);
    assert_eq!(ops[0].kind, "insert");
    assert_eq!(ops[1].kind, "move");
    assert_eq!(ops[0].order_key, Some(order_key.clone()));
    assert_eq!(ops[1].order_key, Some(order_key.clone()));
    assert_eq!(
        ops[0].parent,
        Some(<[u8; 16]>::try_from(parent.as_slice()).unwrap())
    );
    assert_eq!(ops[0].node, <[u8; 16]>::try_from(node.as_slice()).unwrap());
    assert_eq!(
        ops[1].new_parent,
        Some(<[u8; 16]>::try_from(parent.as_slice()).unwrap())
    );

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

#[test]
fn allocate_order_key_after_is_deterministic_for_single_gap() {
    let conn = setup_conn();

    let parent = node_bytes(0);
    let node_a = node_bytes(1);
    let node_b = node_bytes(2);

    let key_a = (1u16).to_be_bytes().to_vec();
    let key_b = (3u16).to_be_bytes().to_vec();

    // A(1), B(3)
    for (counter, (node, order_key)) in [(1i64, (&node_a, &key_a)), (2i64, (&node_b, &key_b))] {
        let _: i64 = conn
            .query_row(
                "SELECT treecrdt_append_op(?1, ?2, ?3, ?4, ?5, ?6, NULL, ?7, NULL)",
                rusqlite::params![b"r1".to_vec(), counter, counter, "insert", parent, node, order_key],
                |row| row.get(0),
            )
            .unwrap();
    }

    // Ensure tree_nodes is available for boundary lookup.
    let _: i64 = conn
        .query_row("SELECT treecrdt_ensure_materialized()", [], |row| row.get(0))
        .unwrap();

    // after(A) between 1 and 3 => 2 (deterministic)
    let seed = b"seed".to_vec();
    let allocated_after: Vec<u8> = conn
        .query_row(
            "SELECT treecrdt_allocate_order_key(?1, 'after', ?2, NULL, ?3)",
            rusqlite::params![parent, node_a, seed.clone()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(allocated_after, (2u16).to_be_bytes().to_vec());
}

#[test]
fn allocate_order_key_first_is_deterministic_for_single_gap() {
    let conn = setup_conn();

    let parent = node_bytes(0);
    let node_a = node_bytes(1);
    let node_b = node_bytes(2);
    let key_a = (2u16).to_be_bytes().to_vec();
    let key_b = (4u16).to_be_bytes().to_vec();
    for (counter, (node, order_key)) in [(1i64, (&node_a, &key_a)), (2i64, (&node_b, &key_b))] {
        let _: i64 = conn
            .query_row(
                "SELECT treecrdt_append_op(?1, ?2, ?3, ?4, ?5, ?6, NULL, ?7, NULL)",
                rusqlite::params![b"r1".to_vec(), counter, counter, "insert", parent, node, order_key],
                |row| row.get(0),
            )
            .unwrap();
    }
    let _: i64 = conn
        .query_row("SELECT treecrdt_ensure_materialized()", [], |row| row.get(0))
        .unwrap();
    let seed = b"seed".to_vec();
    let allocated_first: Vec<u8> = conn
        .query_row(
            "SELECT treecrdt_allocate_order_key(?1, 'first', NULL, NULL, ?2)",
            rusqlite::params![parent, seed],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(allocated_first, (1u16).to_be_bytes().to_vec());
}

#[test]
fn allocate_order_key_last_is_deterministic_for_single_gap() {
    let conn = setup_conn();

    let parent = node_bytes(0);
    let node_a = node_bytes(1);
    let key_a = (0xfffdu16).to_be_bytes().to_vec();
    let _: i64 = conn
        .query_row(
            "SELECT treecrdt_append_op(?1, ?2, ?3, ?4, ?5, ?6, NULL, ?7, NULL)",
            rusqlite::params![b"r1".to_vec(), 1i64, 1i64, "insert", parent, node_a, key_a],
            |row| row.get(0),
        )
        .unwrap();
    let _: i64 = conn
        .query_row("SELECT treecrdt_ensure_materialized()", [], |row| row.get(0))
        .unwrap();
    let allocated_last: Vec<u8> = conn
        .query_row(
            "SELECT treecrdt_allocate_order_key(?1, 'last', NULL, NULL, ?2)",
            rusqlite::params![parent, b"seed".to_vec()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(allocated_last, (0xfffeu16).to_be_bytes().to_vec());
}

#[test]
fn allocate_order_key_after_excludes_a_node() {
    let conn = setup_conn();

    let parent = node_bytes(0);
    let node_a = node_bytes(1);
    let node_b = node_bytes(2);
    let node_c = node_bytes(3);

    let key_a = (1u16).to_be_bytes().to_vec();
    let key_b = (3u16).to_be_bytes().to_vec();
    let key_c = (5u16).to_be_bytes().to_vec();

    // A(1), B(3), C(5)
    for (counter, (node, order_key)) in [
        (1i64, (&node_a, &key_a)),
        (2i64, (&node_b, &key_b)),
        (3i64, (&node_c, &key_c)),
    ] {
        let _: i64 = conn
            .query_row(
                "SELECT treecrdt_append_op(?1, ?2, ?3, ?4, ?5, ?6, NULL, ?7, NULL)",
                rusqlite::params![b"r1".to_vec(), counter, counter, "insert", parent, node, order_key],
                |row| row.get(0),
            )
            .unwrap();
    }
    let _: i64 = conn
        .query_row("SELECT treecrdt_ensure_materialized()", [], |row| row.get(0))
        .unwrap();

    // exclude() should skip B when placing after A.
    let seed = b"seed".to_vec();
    let expected_excluding_b =
        allocate_between(Some(&key_a), Some(&key_c), &seed).expect("allocate_between");
    let allocated_excluding_b: Vec<u8> = conn
        .query_row(
            "SELECT treecrdt_allocate_order_key(?1, 'after', ?2, ?3, ?4)",
            rusqlite::params![parent, node_a, node_b, seed.clone()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(allocated_excluding_b, expected_excluding_b);
}

#[test]
fn allocate_order_key_after_rejects_excluding_the_after_node() {
    let conn = setup_conn();

    let parent = node_bytes(0);
    let node_a = node_bytes(1);
    let node_b = node_bytes(2);
    let key_a = (1u16).to_be_bytes().to_vec();
    let key_b = (3u16).to_be_bytes().to_vec();
    for (counter, (node, order_key)) in [(1i64, (&node_a, &key_a)), (2i64, (&node_b, &key_b))] {
        let _: i64 = conn
            .query_row(
                "SELECT treecrdt_append_op(?1, ?2, ?3, ?4, ?5, ?6, NULL, ?7, NULL)",
                rusqlite::params![b"r1".to_vec(), counter, counter, "insert", parent, node, order_key],
                |row| row.get(0),
            )
            .unwrap();
    }
    let _: i64 = conn
        .query_row("SELECT treecrdt_ensure_materialized()", [], |row| row.get(0))
        .unwrap();

    // after == exclude should error.
    let seed = b"seed".to_vec();
    let bad: rusqlite::Result<Vec<u8>> = conn.query_row(
        "SELECT treecrdt_allocate_order_key(?1, 'after', ?2, ?2, ?3)",
        rusqlite::params![parent, node_a, seed.clone()],
        |row| row.get(0),
    );
    assert!(bad.is_err());
}

#[test]
fn allocate_order_key_trash_parent_returns_empty() {
    let conn = setup_conn();

    // TRASH parent returns empty order_key.
    let trash = node_bytes(u128::MAX);
    let allocated_trash: Vec<u8> = conn
        .query_row(
            "SELECT treecrdt_allocate_order_key(?1, 'first', NULL, NULL, ?2)",
            rusqlite::params![trash, b"seed".to_vec()],
            |row| row.get(0),
        )
        .unwrap();
    assert!(allocated_trash.is_empty());
}

fn setup_conn() -> Connection {
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
    conn
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

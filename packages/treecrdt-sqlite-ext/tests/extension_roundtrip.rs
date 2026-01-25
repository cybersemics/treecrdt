#![cfg(all(feature = "rusqlite-storage", feature = "ext-sqlite"))]
use std::env;
use std::path::PathBuf;

use rusqlite::Connection;
use serde::Deserialize;
use treecrdt_core::{order_key::allocate_between, ReplicaId, VersionVector};

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
    known_state: Option<Vec<u8>>,
    payload: Option<Vec<u8>>,
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
fn local_insert_returns_appended_insert_op() {
    let conn = setup_conn();

    let replica = b"r1".to_vec();
    let parent = node_bytes(0);
    let node = node_bytes(1);

    let json: String = conn
        .query_row(
            "SELECT treecrdt_local_insert(?1, ?2, ?3, 'first', NULL, NULL)",
            rusqlite::params![replica.clone(), parent.clone(), node.clone()],
            |row| row.get(0),
        )
        .unwrap();

    let ops: Vec<JsonOp> = serde_json::from_str(&json).unwrap();
    assert_eq!(ops.len(), 1);
    let op = &ops[0];
    assert_eq!(op.kind, "insert");
    assert_eq!(op.replica, replica);
    assert_eq!(op.counter, 1);
    assert_eq!(op.lamport, 1);
    assert_eq!(
        op.parent,
        Some(<[u8; 16]>::try_from(parent.as_slice()).unwrap())
    );
    assert_eq!(op.node, <[u8; 16]>::try_from(node.as_slice()).unwrap());
    assert!(op.order_key.is_some());

    let seed = {
        let mut s = Vec::new();
        s.extend_from_slice(&replica);
        s.extend_from_slice(&1u64.to_be_bytes());
        s
    };
    let expected = allocate_between(None, None, &seed).expect("allocate_between");
    assert_eq!(op.order_key.as_ref().unwrap(), &expected);

    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM ops", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 1);
}

#[test]
fn local_insert_after_is_deterministic_for_single_gap() {
    let conn = setup_conn();

    let replica = b"r1".to_vec();
    let parent = node_bytes(0);
    let node_a = node_bytes(1);
    let node_b = node_bytes(2);
    let node_c = node_bytes(3);

    let key_a = (1u16).to_be_bytes().to_vec();
    let key_b = (3u16).to_be_bytes().to_vec();

    // A(1), B(3)
    for (counter, (node, order_key)) in [(1i64, (&node_a, &key_a)), (2i64, (&node_b, &key_b))] {
        let _: i64 = conn
            .query_row(
                "SELECT treecrdt_append_op(?1, ?2, ?3, ?4, ?5, ?6, NULL, ?7, NULL)",
                rusqlite::params![replica.clone(), counter, counter, "insert", parent, node, order_key],
                |row| row.get(0),
            )
            .unwrap();
    }

    // after(A) between 1 and 3 => 2 (deterministic)
    let json: String = conn
        .query_row(
            "SELECT treecrdt_local_insert(?1, ?2, ?3, 'after', ?4, NULL)",
            rusqlite::params![replica, parent, node_c, node_a],
            |row| row.get(0),
        )
        .unwrap();
    let ops: Vec<JsonOp> = serde_json::from_str(&json).unwrap();
    assert_eq!(ops.len(), 1);
    let op = &ops[0];
    assert_eq!(op.kind, "insert");
    assert_eq!(op.order_key.as_ref().unwrap(), &(2u16).to_be_bytes().to_vec());
}

#[test]
fn local_insert_last_is_deterministic_for_single_gap() {
    let conn = setup_conn();

    let replica = b"r1".to_vec();
    let parent = node_bytes(0);
    let node_a = node_bytes(1);
    let node_b = node_bytes(2);

    let key_a = (0xfffdu16).to_be_bytes().to_vec();
    let _: i64 = conn
        .query_row(
            "SELECT treecrdt_append_op(?1, ?2, ?3, ?4, ?5, ?6, NULL, ?7, NULL)",
            rusqlite::params![replica.clone(), 1i64, 1i64, "insert", parent, node_a, key_a],
            |row| row.get(0),
        )
        .unwrap();

    let json: String = conn
        .query_row(
            "SELECT treecrdt_local_insert(?1, ?2, ?3, 'last', NULL, NULL)",
            rusqlite::params![replica, parent, node_b],
            |row| row.get(0),
        )
        .unwrap();
    let ops: Vec<JsonOp> = serde_json::from_str(&json).unwrap();
    assert_eq!(ops.len(), 1);
    let op = &ops[0];
    assert_eq!(op.kind, "insert");
    assert_eq!(op.order_key.as_ref().unwrap(), &(0xfffeu16).to_be_bytes().to_vec());
}

#[test]
fn local_move_allocates_key_excluding_self() {
    let conn = setup_conn();

    let replica = b"r1".to_vec();
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
                rusqlite::params![replica.clone(), counter, counter, "insert", parent, node, order_key],
                |row| row.get(0),
            )
            .unwrap();
    }

    // Move B after A, excluding B from sibling scan => allocate between A and C.
    let json: String = conn
        .query_row(
            "SELECT treecrdt_local_move(?1, ?2, ?3, 'after', ?4)",
            rusqlite::params![replica.clone(), node_b.clone(), parent.clone(), node_a.clone()],
            |row| row.get(0),
        )
        .unwrap();

    let ops: Vec<JsonOp> = serde_json::from_str(&json).unwrap();
    assert_eq!(ops.len(), 1);
    let op = &ops[0];
    assert_eq!(op.kind, "move");
    assert_eq!(op.counter, 4);
    assert_eq!(op.lamport, 4);
    assert_eq!(
        op.new_parent,
        Some(<[u8; 16]>::try_from(parent.as_slice()).unwrap())
    );
    assert_eq!(op.node, <[u8; 16]>::try_from(node_b.as_slice()).unwrap());

    let seed = {
        let mut s = Vec::new();
        s.extend_from_slice(&replica);
        s.extend_from_slice(&4u64.to_be_bytes());
        s
    };
    let expected = allocate_between(Some(&key_a), Some(&key_c), &seed).expect("allocate_between");
    assert_eq!(op.order_key.as_ref().unwrap(), &expected);
}

#[test]
fn local_move_to_trash_returns_empty_order_key() {
    let conn = setup_conn();

    let replica = b"r1".to_vec();
    let parent = node_bytes(0);
    let node = node_bytes(1);
    let trash = node_bytes(u128::MAX);

    let _: String = conn
        .query_row(
            "SELECT treecrdt_local_insert(?1, ?2, ?3, 'first', NULL, NULL)",
            rusqlite::params![replica.clone(), parent, node.clone()],
            |row| row.get(0),
        )
        .unwrap();

    let json: String = conn
        .query_row(
            "SELECT treecrdt_local_move(?1, ?2, ?3, 'first', NULL)",
            rusqlite::params![replica, node, trash],
            |row| row.get(0),
        )
        .unwrap();
    let ops: Vec<JsonOp> = serde_json::from_str(&json).unwrap();
    assert_eq!(ops.len(), 1);
    let op = &ops[0];
    assert_eq!(op.kind, "move");
    assert!(op.order_key.as_ref().unwrap().is_empty());
}

#[test]
fn local_delete_includes_known_state_bytes() {
    let conn = setup_conn();

    let replica = b"r1".to_vec();
    let parent = node_bytes(0);
    let node = node_bytes(1);

    // Insert first so subtree_known_state has something to report.
    let _: String = conn
        .query_row(
            "SELECT treecrdt_local_insert(?1, ?2, ?3, 'first', NULL, NULL)",
            rusqlite::params![replica.clone(), parent, node.clone()],
            |row| row.get(0),
        )
        .unwrap();

    let json: String = conn
        .query_row(
            "SELECT treecrdt_local_delete(?1, ?2)",
            rusqlite::params![replica.clone(), node],
            |row| row.get(0),
        )
        .unwrap();
    let ops: Vec<JsonOp> = serde_json::from_str(&json).unwrap();
    assert_eq!(ops.len(), 1);
    let op = &ops[0];
    assert_eq!(op.kind, "delete");
    let bytes = op.known_state.as_ref().unwrap();
    assert!(!bytes.is_empty());
    let vv: VersionVector = serde_json::from_slice(bytes).unwrap();
    assert!(vv.get(&ReplicaId::new(replica)) >= 1);
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

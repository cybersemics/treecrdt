#[cfg(feature = "serde")]
#[test]
fn version_vector_json_uses_sorted_entry_list_and_roundtrips() {
    use treecrdt_core::{ReplicaId, VersionVector};

    let mut vv = VersionVector::new();
    vv.observe(&ReplicaId::new(b"rB"), 1);
    vv.observe(&ReplicaId::new(b"rB"), 2);
    vv.observe(&ReplicaId::new(b"rA"), 1);

    let bytes = serde_json::to_vec(&vv).expect("serialize VersionVector");
    let json = std::str::from_utf8(&bytes).expect("VersionVector JSON must be UTF-8");

    assert_eq!(
        json,
        r#"{"entries":[{"replica":[114,65],"frontier":1,"ranges":[]},{"replica":[114,66],"frontier":2,"ranges":[]}]}"#
    );

    let roundtrip: VersionVector =
        serde_json::from_slice(&bytes).expect("deserialize VersionVector");
    assert_eq!(roundtrip, vv);
}

#[cfg(feature = "serde")]
#[test]
fn version_vector_json_rejects_map_shaped_entries() {
    use treecrdt_core::VersionVector;

    let legacy = r#"{"entries":{"rA":{"frontier":1,"ranges":[]}}}"#;
    assert!(serde_json::from_str::<VersionVector>(legacy).is_err());
}

#[cfg(feature = "serde")]
#[test]
fn version_vector_json_rejects_duplicate_or_unsorted_replicas() {
    use treecrdt_core::VersionVector;

    let duplicate = r#"{"entries":[{"replica":[114,65],"frontier":1,"ranges":[]},{"replica":[114,65],"frontier":2,"ranges":[]}]}"#;
    assert!(serde_json::from_str::<VersionVector>(duplicate).is_err());

    let unsorted = r#"{"entries":[{"replica":[114,66],"frontier":2,"ranges":[]},{"replica":[114,65],"frontier":1,"ranges":[]}]}"#;
    assert!(serde_json::from_str::<VersionVector>(unsorted).is_err());
}

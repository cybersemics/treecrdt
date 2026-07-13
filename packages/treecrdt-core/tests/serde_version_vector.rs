#[cfg(feature = "serde")]
#[test]
fn version_vector_debug_json_roundtrips() {
    use treecrdt_core::{ReplicaId, VersionVector};

    let mut vv = VersionVector::new();
    vv.observe(&ReplicaId::new(b"rB"), 1);
    vv.observe(&ReplicaId::new(b"rB"), 2);
    vv.observe(&ReplicaId::new(b"rA"), 1);

    let bytes = serde_json::to_vec(&vv).expect("serialize VersionVector");
    let roundtrip: VersionVector =
        serde_json::from_slice(&bytes).expect("deserialize VersionVector");
    assert_eq!(roundtrip, vv);
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

#[cfg(feature = "serde")]
#[test]
fn version_vector_json_rejects_invalid_semantics_and_unknown_fields() {
    use treecrdt_core::VersionVector;

    let empty_entry = r#"{"entries":[{"replica":[114,65],"frontier":0,"ranges":[]}]}"#;
    assert!(serde_json::from_str::<VersionVector>(empty_entry).is_err());

    let adjacent_range = r#"{"entries":[{"replica":[114,65],"frontier":1,"ranges":[[2,3]]}]}"#;
    assert!(serde_json::from_str::<VersionVector>(adjacent_range).is_err());

    let unknown = r#"{"entries":[{"replica":[114,65],"frontier":1,"ranges":[],"extra":true}]}"#;
    assert!(serde_json::from_str::<VersionVector>(unknown).is_err());
}

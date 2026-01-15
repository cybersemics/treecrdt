#[cfg(feature = "serde")]
#[test]
fn version_vector_json_roundtrips_and_is_nonempty() {
    use treecrdt_core::{ReplicaId, VersionVector};

    let mut vv = VersionVector::new();
    vv.observe(&ReplicaId::new(b"rB"), 1);
    vv.observe(&ReplicaId::new(b"rB"), 2);
    vv.observe(&ReplicaId::new(b"rA"), 1);

    let bytes = serde_json::to_vec(&vv).expect("serialize VersionVector");
    let json = std::str::from_utf8(&bytes).expect("VersionVector JSON must be UTF-8");

    // If this ever regresses, the sqlite extension will silently store NULL/empty vectors.
    assert!(
        json.contains("\"entries\"") && json.contains('['),
        "expected VersionVector to serialize as an entries list, got: {json}"
    );

    let roundtrip: VersionVector =
        serde_json::from_slice(&bytes).expect("deserialize VersionVector");
    assert_eq!(roundtrip, vv);
}

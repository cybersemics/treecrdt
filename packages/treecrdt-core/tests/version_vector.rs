use treecrdt_core::{ReplicaId, VersionVector};

#[test]
fn test_version_vector_observe() {
    let mut vv = VersionVector::new();
    let replica_a = ReplicaId::new(b"a");
    let replica_b = ReplicaId::new(b"b");

    vv.observe(&replica_a, 5);
    assert_eq!(vv.get(&replica_a), 5);
    assert_eq!(vv.get(&replica_b), 0);

    vv.observe(&replica_a, 3);
    assert_eq!(vv.get(&replica_a), 5); // Should keep max

    vv.observe(&replica_a, 7);
    assert_eq!(vv.get(&replica_a), 7);
}

#[test]
fn test_version_vector_merge() {
    let mut vv1 = VersionVector::new();
    let mut vv2 = VersionVector::new();
    let replica_a = ReplicaId::new(b"a");
    let replica_b = ReplicaId::new(b"b");

    vv1.observe(&replica_a, 5);
    vv2.observe(&replica_b, 3);
    vv2.observe(&replica_a, 2);

    vv1.merge(&vv2);
    assert_eq!(vv1.get(&replica_a), 5); // Max of 5 and 2
    assert_eq!(vv1.get(&replica_b), 3);
}

#[test]
fn test_version_vector_awareness() {
    let mut vv1 = VersionVector::new();
    let mut vv2 = VersionVector::new();
    let replica_a = ReplicaId::new(b"a");
    let replica_b = ReplicaId::new(b"b");

    vv1.observe(&replica_a, 5);
    vv1.observe(&replica_b, 3);

    vv2.observe(&replica_a, 3);
    vv2.observe(&replica_b, 2);

    assert!(vv1.is_aware_of(&vv2)); // vv1 has seen more
    assert!(!vv2.is_aware_of(&vv1)); // vv2 hasn't seen replica_a:5 or replica_b:3

    vv2.observe(&replica_a, 5);
    vv2.observe(&replica_b, 3);
    assert!(vv2.is_aware_of(&vv1)); // Now vv2 has seen everything
}

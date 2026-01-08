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
    assert_eq!(vv.get(&replica_a), 5); // Max seen remains 5

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
fn test_version_vector_awareness_contiguous() {
    let mut vv1 = VersionVector::new();
    let mut vv2 = VersionVector::new();
    let replica_a = ReplicaId::new(b"a");
    let replica_b = ReplicaId::new(b"b");

    // vv1 has seen contiguous counters for both replicas.
    for c in 1..=5 {
        vv1.observe(&replica_a, c);
    }
    for c in 1..=3 {
        vv1.observe(&replica_b, c);
    }

    for c in 1..=3 {
        vv2.observe(&replica_a, c);
    }
    for c in 1..=2 {
        vv2.observe(&replica_b, c);
    }

    assert!(vv1.is_aware_of(&vv2)); // vv1 has seen everything vv2 has
    assert!(!vv2.is_aware_of(&vv1)); // vv2 is missing a:4..5 and b:3

    // vv2 fills in the missing counters.
    vv2.observe(&replica_a, 4);
    vv2.observe(&replica_a, 5);
    vv2.observe(&replica_b, 3);
    assert!(vv2.is_aware_of(&vv1));
}

#[test]
fn test_version_vector_awareness_holes() {
    let mut vv = VersionVector::new();
    let replica = ReplicaId::new(b"r");

    // Seeing counter 2 does not imply seeing counter 1.
    vv.observe(&replica, 2);
    assert_eq!(vv.frontier(&replica), 0);

    let mut needs_1 = VersionVector::new();
    needs_1.observe(&replica, 1);
    assert!(!vv.is_aware_of(&needs_1));

    // Once 1 arrives, frontier advances and vv becomes aware.
    vv.observe(&replica, 1);
    assert_eq!(vv.frontier(&replica), 2);
    assert!(vv.is_aware_of(&needs_1));
}

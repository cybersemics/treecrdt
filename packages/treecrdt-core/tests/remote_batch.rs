use std::cell::RefCell;
use std::rc::Rc;

use proptest::prelude::*;
use treecrdt_core::{
    Error, Lamport, LamportClock, LocalPlacement, MemoryNodeStore, MemoryPayloadStore,
    MemoryStorage, NodeId, NodeStore, Operation, OperationId, OperationKind, PayloadStore,
    ReplicaId, Result, Storage, TreeCrdt, VersionVector,
};

#[derive(Default)]
struct StorageTrace {
    attempts: Vec<Operation>,
    scans: usize,
    owned_loads: usize,
    borrowed_visits: usize,
    fail_on_attempt: Option<usize>,
}

struct CountingStorage {
    inner: MemoryStorage,
    trace: Rc<RefCell<StorageTrace>>,
}

impl Storage for CountingStorage {
    fn apply(&mut self, op: Operation) -> Result<bool> {
        let mut trace = self.trace.borrow_mut();
        trace.attempts.push(op.clone());
        if trace.fail_on_attempt == Some(trace.attempts.len()) {
            return Err(Error::Storage("injected append failure".into()));
        }
        self.inner.apply(op)
    }

    fn load_since(&self, lamport: Lamport) -> Result<Vec<Operation>> {
        self.trace.borrow_mut().owned_loads += 1;
        self.inner.load_since(lamport)
    }

    fn try_visit_since(
        &self,
        lamport: Lamport,
        visit: &mut dyn FnMut(&Operation) -> Result<()>,
    ) -> Result<bool> {
        self.trace.borrow_mut().borrowed_visits += 1;
        self.inner.try_visit_since(lamport, visit)
    }

    fn latest_lamport(&self) -> Lamport {
        self.inner.latest_lamport()
    }

    fn latest_counter(&self, replica: &ReplicaId) -> Result<u64> {
        self.inner.latest_counter(replica)
    }

    fn scan_since(
        &self,
        lamport: Lamport,
        visit: &mut dyn FnMut(Operation) -> Result<()>,
    ) -> Result<()> {
        self.trace.borrow_mut().scans += 1;
        self.inner.scan_since(lamport, visit)
    }
}

fn counting_tree(
    replica: &ReplicaId,
) -> (
    TreeCrdt<CountingStorage, LamportClock>,
    Rc<RefCell<StorageTrace>>,
) {
    let trace = Rc::new(RefCell::new(StorageTrace::default()));
    let storage = CountingStorage {
        inner: MemoryStorage::default(),
        trace: trace.clone(),
    };
    let tree = TreeCrdt::new(replica.clone(), storage, LamportClock::default()).unwrap();
    (tree, trace)
}

fn insert(replica: &ReplicaId, counter: u64, lamport: Lamport) -> Operation {
    Operation::insert(
        replica,
        counter,
        lamport,
        NodeId::ROOT,
        NodeId(counter as u128),
        vec![counter as u8],
    )
}

#[test]
fn memory_storage_visits_borrowed_suffix_and_propagates_callback_errors() {
    let replica = ReplicaId::new(b"remote");
    let mut storage = MemoryStorage::default();
    let mut payload_addresses = Vec::new();
    // Arrival order is deliberately different from canonical order.
    for lamport in [30, 10, 20, 21] {
        let payload = vec![lamport as u8; 8];
        payload_addresses.push((lamport, payload.as_ptr()));
        storage
            .apply(Operation::set_payload(
                &replica,
                lamport,
                lamport,
                NodeId(1),
                payload,
            ))
            .unwrap();
    }
    for (since, expected) in [(0, vec![10, 20, 21, 30]), (20, vec![21, 30]), (30, vec![])] {
        let mut visited = Vec::new();
        assert!(storage
            .try_visit_since(since, &mut |op| {
                let OperationKind::Payload {
                    payload: Some(payload),
                    ..
                } = &op.kind
                else {
                    return Err(Error::Storage("expected stored payload".into()));
                };
                let (_, address) = payload_addresses
                    .iter()
                    .find(|(lamport, _)| *lamport == op.meta.lamport)
                    .unwrap();
                assert_eq!(
                    payload.as_ptr(),
                    *address,
                    "visitor must borrow stored bytes"
                );
                assert_eq!(payload.as_slice(), &[op.meta.lamport as u8; 8]);
                visited.push(op.meta.lamport);
                Ok(())
            })
            .unwrap());
        visited.sort_unstable();
        assert_eq!(visited, expected);
    }

    let mut calls = 0;
    let error = storage
        .try_visit_since(20, &mut |_| {
            calls += 1;
            Err(Error::Storage("injected visitor failure".into()))
        })
        .unwrap_err();
    assert!(matches!(error, Error::Storage(message) if message == "injected visitor failure"));
    assert_eq!(calls, 1);
}

#[test]
fn ignored_operations_initialize_nodes_before_and_after_replay() {
    let replica = ReplicaId::new(b"remote");
    let (mut tree, _) = counting_tree(&replica);
    tree.apply_remote_batch([
        Operation::insert(&replica, 1, 1, NodeId(1), NodeId(1), vec![1]),
        Operation::delete(&replica, 2, 2, NodeId::TRASH, None),
    ])
    .unwrap();
    assert!(tree.is_known(NodeId(1)).unwrap());
    assert!(tree.is_known(NodeId::TRASH).unwrap());

    tree.replay_from_storage().unwrap();
    assert!(tree.is_known(NodeId(1)).unwrap());
    assert!(tree.is_known(NodeId::TRASH).unwrap());
}

#[test]
fn empty_duplicate_and_newer_batches_do_not_scan_history() {
    let replica = ReplicaId::new(b"remote");
    let (mut tree, trace) = counting_tree(&replica);

    tree.apply_remote_batch(Vec::new()).unwrap();
    assert!(trace.borrow().attempts.is_empty());
    assert!(tree.head_op().is_none());

    let first = insert(&replica, 10, 1);
    tree.apply_remote(first.clone()).unwrap();
    let second = insert(&replica, 20, 2);
    let third = insert(&replica, 30, 3);
    tree.apply_remote_batch(vec![third.clone(), second.clone()]).unwrap();

    assert_eq!(tree.head_op(), Some(&third));
    assert_eq!(
        tree.children(NodeId::ROOT).unwrap(),
        vec![NodeId(10), NodeId(20), NodeId(30)]
    );
    assert_eq!(
        tree.operations_since(0).unwrap(),
        vec![first.clone(), third.clone(), second.clone()]
    );
    tree.apply_remote_batch(vec![third.clone(), first, second]).unwrap();
    tree.apply_remote_batch(Vec::new()).unwrap();
    assert_eq!(tree.head_op(), Some(&third));
    assert_eq!(trace.borrow().scans, 0);

    let next = tree.prepare_local_delete(NodeId(30)).unwrap().op;
    assert_eq!((next.meta.id.counter, next.meta.lamport), (31, 4));
    let mut observed = VersionVector::new();
    observed.observe(&replica, 30);
    assert_eq!(next.meta.known_state, Some(observed));
}

#[test]
fn equal_lamport_batches_order_by_replica_then_counter_without_replay() {
    let a = ReplicaId::new(b"a");
    let z = ReplicaId::new(b"z");
    for (earlier, later) in [
        (insert(&a, 1, 20), insert(&a, 2, 20)),
        (insert(&a, 2, 20), insert(&z, 1, 20)),
    ] {
        let (mut tree, trace) = counting_tree(&ReplicaId::new(b"local"));
        tree.apply_remote_batch(vec![later.clone(), earlier]).unwrap();
        assert_eq!(tree.head_op(), Some(&later));
        assert_eq!(trace.borrow().scans, 0);
    }
}

#[test]
fn duplicate_identity_keeps_first_arriving_envelope_before_sorting() {
    let replica = ReplicaId::new(b"remote");
    let (mut tree, trace) = counting_tree(&ReplicaId::new(b"local"));
    let first = Operation::set_payload(&replica, 1, 20, NodeId(10), b"first");
    let later_duplicate = Operation::set_payload(&replica, 1, 2, NodeId(11), b"duplicate");
    tree.apply_remote_batch(vec![first.clone(), later_duplicate]).unwrap();

    assert_eq!(tree.operations_since(0).unwrap(), vec![first.clone()]);
    assert_eq!(tree.head_op(), Some(&first));
    assert_eq!(tree.payload(NodeId(10)).unwrap(), Some(b"first".to_vec()));
    assert!(!tree.is_known(NodeId(11)).unwrap());
    assert_eq!(trace.borrow().scans, 0);
}

#[test]
fn duplicate_high_lamport_advances_clock_with_or_without_replay() {
    for replay in [false, true] {
        let replica = ReplicaId::new(b"remote");
        let (mut tree, trace) = counting_tree(&ReplicaId::new(b"local"));
        let original = insert(&replica, 10, 10);
        tree.apply_remote(original.clone()).unwrap();
        let mut duplicate = original.clone();
        duplicate.meta.lamport = 100;
        let mut received = vec![duplicate];
        let mut accepted = vec![original.clone()];
        if replay {
            let older = insert(&replica, 5, 5);
            received.push(older.clone());
            accepted.push(older);
        }
        tree.apply_remote_batch(received).unwrap();

        assert_eq!(tree.lamport(), 100);
        assert_eq!(tree.head_op(), Some(&original));
        assert_eq!(tree.operations_since(0).unwrap(), accepted);
        assert_eq!(trace.borrow().scans, usize::from(replay));
        let (next, _) = tree.local_payload(NodeId(10), Some(vec![1])).unwrap();
        assert_eq!(next.meta.lamport, 101);
    }
}

fn assert_same_state<S: Storage, T: Storage>(
    left: &TreeCrdt<S, LamportClock>,
    right: &TreeCrdt<T, LamportClock>,
) {
    assert_eq!(left.head_op(), right.head_op());
    assert_eq!(left.lamport(), right.lamport());
    let mut left_exports = left.export_nodes().unwrap();
    let mut right_exports = right.export_nodes().unwrap();
    left_exports.sort_by_key(|node| node.node.0);
    right_exports.sort_by_key(|node| node.node.0);
    assert_eq!(left_exports.len(), right_exports.len());
    for (left_node, right_node) in left_exports.iter().zip(&right_exports) {
        assert_eq!(left_node.node, right_node.node);
        assert_eq!(left_node.parent, right_node.parent);
        assert_eq!(left_node.children, right_node.children);
        assert_eq!(left_node.last_change, right_node.last_change);
        assert_eq!(left_node.deleted_at, right_node.deleted_at);
        let node = left_node.node;
        assert_eq!(left.payload(node).unwrap(), right.payload(node).unwrap());
        assert_eq!(
            left.payload_last_writer(node).unwrap(),
            right.payload_last_writer(node).unwrap()
        );
    }
    left.validate_invariants().unwrap();
    right.validate_invariants().unwrap();
}

#[test]
fn late_payload_batches_match_replay_without_scanning_history() {
    for newer_head in [false, true] {
        let local = ReplicaId::new(b"local");
        let remote = ReplicaId::new(b"remote");
        let (mut tree, trace) = counting_tree(&local);
        tree.local_insert(NodeId::ROOT, NodeId(1), LocalPlacement::Last, None).unwrap();
        tree.local_insert(NodeId(1), NodeId(2), LocalPlacement::Last, Some(vec![0]))
            .unwrap();
        tree.local_delete(NodeId(1)).unwrap();
        // A newer unrelated operation makes the concurrent payload writes out of order.
        tree.apply_remote(insert(&local, 100, 100)).unwrap();
        assert!(tree.is_tombstoned(NodeId(1)).unwrap());
        let (mut replay, _) = counting_tree(&local);
        replay.apply_remote_batch(tree.operations_since(0).unwrap()).unwrap();

        let mut batch = vec![
            Operation::set_payload(&remote, 2, 10, NodeId(2), b"winner"),
            Operation::set_payload(&remote, 1, 10, NodeId(2), b"older"),
            Operation::set_payload(&local, 101, 10, NodeId(2), b"losing replica"),
            // A payload can precede the node's insert, and a clear must retain its writer.
            Operation::set_payload(&remote, 3, 11, NodeId(99), b"unplaced"),
            Operation::clear_payload(&remote, 4, 12, NodeId(99)),
        ];
        if newer_head {
            batch.push(Operation::set_payload(
                &remote,
                5,
                101,
                NodeId(2),
                b"new head",
            ));
        }
        tree.apply_remote_batch(batch.clone()).unwrap();
        // The single-op delta path independently replays late operations.
        for op in batch {
            replay.apply_remote_with_delta(op).unwrap();
        }

        assert_eq!(trace.borrow().scans, 0);
        assert!(!tree.is_tombstoned(NodeId(1)).unwrap());
        assert_same_state(&tree, &replay);
        assert_eq!(
            tree.prepare_local_delete(NodeId(1)).unwrap().op,
            replay.prepare_local_delete(NodeId(1)).unwrap().op
        );
        // An older move of the node inserted by the newer placement still requires replay.
        let moved = Operation::move_node(&remote, 6, 50, NodeId(100), NodeId::ROOT, vec![2]);
        tree.apply_remote(moved.clone()).unwrap();
        replay.apply_remote_with_delta(moved).unwrap();
        assert_eq!(trace.borrow().scans, 1);
        assert_same_state(&tree, &replay);
    }
}

#[test]
fn late_moves_after_structural_history_match_replay_without_scanning() {
    let local = ReplicaId::new(b"local");
    let remote = ReplicaId::new(b"remote");
    let (mut tree, trace) = counting_tree(&local);
    for (parent, node) in [
        (NodeId::ROOT, NodeId(1)),
        (NodeId::ROOT, NodeId(2)),
        (NodeId(1), NodeId(3)),
    ] {
        tree.local_insert(parent, node, LocalPlacement::Last, Some(vec![0])).unwrap();
    }
    tree.local_delete(NodeId(1)).unwrap();
    let head = Operation::clear_payload(&remote, 20, 100, NodeId(3));
    tree.apply_remote(head.clone()).unwrap();
    let (mut replay, _) = counting_tree(&local);
    replay.apply_remote_batch(tree.operations_since(0).unwrap()).unwrap();

    let moved = Operation::move_node(&remote, 1, 10, NodeId(3), NodeId(2), vec![1]);
    let reordered = Operation::move_node(&remote, 2, 11, NodeId(1), NodeId::ROOT, vec![0]);
    // Rejected in canonical order after `moved`, but still part of structural history.
    let cycle = Operation::move_node(&remote, 3, 14, NodeId(2), NodeId(3), vec![2]);
    let batch = vec![cycle, reordered, moved];
    tree.apply_remote_batch(batch.clone()).unwrap();
    for op in batch {
        replay.apply_remote_with_delta(op).unwrap();
    }
    assert_eq!(trace.borrow().scans, 0);
    assert_eq!(tree.head_op(), Some(&head));
    assert_eq!(
        tree.children(NodeId::ROOT).unwrap(),
        vec![NodeId(1), NodeId(2)]
    );
    assert_eq!(tree.children(NodeId(2)).unwrap(), vec![NodeId(3)]);
    assert_same_state(&tree, &replay);
    assert_eq!(
        tree.prepare_local_delete(NodeId(2)).unwrap().op,
        replay.prepare_local_delete(NodeId(2)).unwrap().op
    );

    // This earlier move makes the formerly rejected cycle operation valid on replay.
    let earlier = Operation::move_node(
        &ReplicaId::new(b"late"),
        1,
        13,
        NodeId(3),
        NodeId::ROOT,
        vec![3],
    );
    tree.apply_remote(earlier.clone()).unwrap();
    replay.apply_remote_with_delta(earlier).unwrap();
    assert_eq!(trace.borrow().scans, 1);
    assert_eq!(tree.parent(NodeId(2)).unwrap(), Some(NodeId(3)));
    assert_same_state(&tree, &replay);

    // An old payload does not force replay for a new, in-order structural operation.
    let batch = vec![
        Operation::set_payload(&ReplicaId::new(b"payload"), 1, 5, NodeId(3), b"older"),
        insert(&ReplicaId::new(b"insert"), 4, 101),
    ];
    tree.apply_remote_batch(batch.clone()).unwrap();
    for op in batch {
        replay.apply_remote_with_delta(op).unwrap();
    }
    assert_eq!(trace.borrow().scans, 1);
    assert_same_state(&tree, &replay);
}

#[test]
fn late_moves_compare_the_complete_placement_key() {
    let author = ReplicaId::new(b"a");
    for (replica, counter, lamport, scans) in [
        (b"0".as_slice(), 1, 20, 1),
        (b"a", 4, 20, 1),
        (b"a", 6, 20, 0),
        (b"z", 1, 20, 0),
        (b"z", 1, 19, 1),
    ] {
        let (mut tree, trace) = counting_tree(&ReplicaId::new(b"local"));
        tree.apply_remote_batch(vec![insert(&author, 1, 5), insert(&author, 5, 20)])
            .unwrap();
        tree.apply_remote(Operation::set_payload(&author, 7, 100, NodeId(5), b"newer"))
            .unwrap();
        let (mut replay, _) = counting_tree(&ReplicaId::new(b"local"));
        replay.apply_remote_batch(tree.operations_since(0).unwrap()).unwrap();
        let op = Operation::move_node(
            &ReplicaId::new(replica),
            counter,
            lamport,
            NodeId(5),
            NodeId::ROOT,
            vec![3],
        );
        tree.apply_remote(op.clone()).unwrap();
        replay.apply_remote_with_delta(op).unwrap();
        assert_eq!(trace.borrow().scans, scans);
        assert_same_state(&tree, &replay);
    }
}

#[test]
fn independent_late_moves_avoid_replay() {
    for shared_parent in [false, true] {
        for suffix_insert in [false, true] {
            let local = ReplicaId::new(b"local");
            let remote = ReplicaId::new(b"remote");
            let (mut tree, trace) = counting_tree(&local);
            for (parent, node) in [(0, 1), (0, 2), (0, 3), (0, 4), (1, 10), (2, 11)] {
                tree.local_insert(
                    NodeId(parent),
                    NodeId(node),
                    LocalPlacement::Last,
                    Some(vec![1]),
                )
                .unwrap();
            }
            let parent = NodeId(if shared_parent { 3 } else { 4 });
            let newer = if suffix_insert {
                Operation::insert_with_payload(&remote, 2, 30, parent, NodeId(11), vec![2], b"new")
            } else {
                Operation::move_node(&remote, 2, 30, NodeId(11), parent, vec![2])
            };
            tree.apply_remote(newer.clone()).unwrap();
            let (mut replay, _) = counting_tree(&local);
            replay.apply_remote_batch(tree.operations_since(0).unwrap()).unwrap();
            trace.borrow_mut().owned_loads = 0;

            // Both incoming moves are persisted before the independence check. Their
            // IDs must be excluded from the stored suffix, including the repeated target.
            let batch = vec![
                Operation::move_node(&remote, 3, 21, NodeId(10), NodeId(3), vec![1]),
                Operation::move_node(&remote, 1, 20, NodeId(10), NodeId(3), vec![3]),
            ];
            tree.apply_remote_batch(batch.clone()).unwrap();
            for op in batch {
                replay.apply_remote_with_delta(op).unwrap();
            }

            assert_eq!(trace.borrow().scans, 0);
            assert_eq!(trace.borrow().owned_loads, 0);
            assert_eq!(trace.borrow().borrowed_visits, 1);
            assert_eq!(tree.head_op(), Some(&newer));
            assert_eq!(tree.parent(NodeId(10)).unwrap(), Some(NodeId(3)));
            if shared_parent {
                assert_eq!(
                    tree.children(NodeId(3)).unwrap(),
                    vec![NodeId(10), NodeId(11)]
                );
            }
            assert_same_state(&tree, &replay);
        }
    }
}

#[test]
fn unsupported_borrowed_visitation_falls_back_to_replay() {
    struct UnsupportedStorage(CountingStorage);

    impl Storage for UnsupportedStorage {
        fn apply(&mut self, op: Operation) -> Result<bool> {
            self.0.apply(op)
        }

        fn load_since(&self, lamport: Lamport) -> Result<Vec<Operation>> {
            self.0.load_since(lamport)
        }

        fn latest_lamport(&self) -> Lamport {
            self.0.latest_lamport()
        }

        fn latest_counter(&self, replica: &ReplicaId) -> Result<u64> {
            self.0.latest_counter(replica)
        }

        fn scan_since(
            &self,
            lamport: Lamport,
            visit: &mut dyn FnMut(Operation) -> Result<()>,
        ) -> Result<()> {
            self.0.scan_since(lamport, visit)
        }
    }

    let local = ReplicaId::new(b"local");
    let remote = ReplicaId::new(b"remote");
    let trace = Rc::new(RefCell::new(StorageTrace::default()));
    let mut storage = UnsupportedStorage(CountingStorage {
        inner: MemoryStorage::default(),
        trace: trace.clone(),
    });
    let history = vec![
        insert(&local, 1, 1),
        insert(&local, 2, 2),
        Operation::move_node(&remote, 2, 30, NodeId(2), NodeId::ROOT, vec![2]),
    ];
    for op in &history {
        storage.apply(op.clone()).unwrap();
    }
    assert!(!storage
        .try_visit_since(0, &mut |_| {
            Err(Error::Storage(
                "unsupported visitor must not call back".into(),
            ))
        })
        .unwrap());
    let mut tree = TreeCrdt::new(local.clone(), storage, LamportClock::default()).unwrap();
    tree.replay_from_storage().unwrap();
    trace.borrow_mut().scans = 0;
    let (mut replay, _) = counting_tree(&local);
    replay.apply_remote_batch(history).unwrap();

    let late = Operation::move_node(&remote, 1, 20, NodeId(1), NodeId::ROOT, vec![3]);
    tree.apply_remote(late.clone()).unwrap();
    replay.apply_remote_with_delta(late).unwrap();
    assert_eq!(trace.borrow().scans, 1);
    assert_eq!(trace.borrow().owned_loads, 0);
    assert_eq!(trace.borrow().borrowed_visits, 0);
    assert_same_state(&tree, &replay);
}

#[test]
fn independent_move_suffix_filters_the_complete_equal_lamport_key() {
    for same_replica in [false, true] {
        let a = ReplicaId::new(b"a");
        let m = if same_replica {
            a.clone()
        } else {
            ReplicaId::new(b"m")
        };
        let z = if same_replica {
            a.clone()
        } else {
            ReplicaId::new(b"z")
        };
        let local = ReplicaId::new(b"local");
        let (mut tree, trace) = counting_tree(&local);
        tree.apply_remote_batch(vec![insert(&local, 1, 1), insert(&local, 2, 2)])
            .unwrap();
        tree.apply_remote_batch(vec![
            Operation::move_node(&a, 10, 20, NodeId(1), NodeId::ROOT, vec![1]),
            Operation::move_node(&z, 30, 20, NodeId(2), NodeId::ROOT, vec![2]),
        ])
        .unwrap();
        let (mut replay, _) = counting_tree(&local);
        replay.apply_remote_batch(tree.operations_since(0).unwrap()).unwrap();
        trace.borrow_mut().owned_loads = 0;

        // The earlier equal-Lamport placement touches this target, but is not
        // part of the overlapping suffix. The later placement must be included.
        let late = Operation::move_node(&m, 20, 20, NodeId(1), NodeId::ROOT, vec![3]);
        tree.apply_remote(late.clone()).unwrap();
        replay.apply_remote_with_delta(late).unwrap();
        assert_eq!(trace.borrow().scans, 0);
        assert_eq!(trace.borrow().owned_loads, 0);
        assert_eq!(trace.borrow().borrowed_visits, 1);
        assert_same_state(&tree, &replay);
    }
}

#[test]
fn dependent_late_moves_replay_cycle_decisions_and_defensive_delete_metadata() {
    for case in ["ancestor", "rejected_cycle", "defensive_delete"] {
        let local = ReplicaId::new(b"local");
        let early = ReplicaId::new(b"early");
        let later = ReplicaId::new(b"later");
        let (mut tree, trace) = counting_tree(&local);
        let initial = match case {
            "ancestor" => vec![(0, 1), (1, 3), (0, 2), (2, 4)],
            "rejected_cycle" => vec![(0, 1), (1, 2)],
            _ => vec![(0, 1), (0, 2)],
        };
        for (parent, node) in initial {
            tree.local_insert(NodeId(parent), NodeId(node), LocalPlacement::Last, None)
                .unwrap();
        }
        let (destination, newer_parent) = match case {
            "ancestor" => (NodeId(4), NodeId(3)),
            "rejected_cycle" => (NodeId(2), NodeId::ROOT),
            _ => (NodeId(2), NodeId(1)),
        };
        tree.apply_remote(Operation::move_node(
            &later,
            1,
            30,
            NodeId(2),
            newer_parent,
            vec![1],
        ))
        .unwrap();
        if case == "defensive_delete" {
            tree.apply_remote(Operation::move_node(
                &later,
                2,
                40,
                NodeId(2),
                NodeId::ROOT,
                vec![2],
            ))
            .unwrap();
            let mut known_state = VersionVector::new();
            known_state.observe(&local, 1);
            known_state.observe(&local, 2);
            known_state.observe(&early, 1);
            tree.apply_remote(Operation::delete(
                &ReplicaId::new(b"deleter"),
                1,
                50,
                NodeId(1),
                Some(known_state),
            ))
            .unwrap();
        }
        let (mut replay, _) = counting_tree(&local);
        replay.apply_remote_batch(tree.operations_since(0).unwrap()).unwrap();
        trace.borrow_mut().owned_loads = 0;
        let late = Operation::move_node(&early, 1, 20, NodeId(1), destination, vec![3]);
        tree.apply_remote(late.clone()).unwrap();
        replay.apply_remote_with_delta(late).unwrap();

        assert_eq!(trace.borrow().scans, 1, "{case}");
        assert_eq!(trace.borrow().owned_loads, 0, "{case}");
        assert_eq!(trace.borrow().borrowed_visits, 1, "{case}");
        assert_same_state(&tree, &replay);
        let expected = if case == "rejected_cycle" {
            NodeId::ROOT
        } else {
            destination
        };
        let raw_parent = tree
            .export_nodes()
            .unwrap()
            .into_iter()
            .find(|node| node.node == NodeId(1))
            .unwrap()
            .parent;
        assert_eq!(raw_parent, Some(expected), "{case}");
        if case == "defensive_delete" {
            // Final topology alone would agree without replay, but the rejected
            // newer move must not leave parent last_change metadata behind.
            assert!(tree.is_tombstoned(NodeId(1)).unwrap());
        }
    }
}

#[test]
fn hidden_sorted_placement_still_disables_independence_after_a_stored_head() {
    let local = ReplicaId::new(b"local");
    let remote = ReplicaId::new(b"remote");
    let (mut tree, trace) = counting_tree(&local);
    tree.apply_remote_batch((1..=3).map(|id| insert(&local, id, id))).unwrap();
    let hidden = Operation::move_node(&remote, 2, 20, NodeId(1), NodeId(2), vec![2]);
    tree.apply_sorted_remote_with_materialization(
        hidden.clone(),
        &mut treecrdt_core::NoopParentOpIndex,
        4,
    )
    .unwrap();
    let stored_head = Operation::move_node(&remote, 3, 30, NodeId(3), NodeId::ROOT, vec![3]);
    tree.apply_remote(stored_head.clone()).unwrap();
    let history = tree.operations_since(0).unwrap();
    assert!(history.contains(&stored_head));
    assert!(!history.contains(&hidden));
    let (mut replay, _) = counting_tree(&local);
    replay.apply_remote_batch(history).unwrap();

    // A suffix containing the current placement head can still omit an earlier
    // sorted placement. Reconstruct from the persisted log, including metadata.
    let late = Operation::move_node(&remote, 1, 10, NodeId(1), NodeId::ROOT, vec![4]);
    tree.apply_remote(late.clone()).unwrap();
    replay.apply_remote_with_delta(late).unwrap();
    assert_eq!(trace.borrow().scans, 1);
    assert_same_state(&tree, &replay);
}

#[test]
fn placement_bound_survives_other_apply_paths_and_opaque_stores() {
    for path in ["local", "delta", "sorted", "replay", "stored", "opaque"] {
        for lamport in [15, 20, 30] {
            let local = ReplicaId::new(b"local");
            let remote = ReplicaId::new(b"remote");
            let (mut tree, trace) = counting_tree(&local);
            tree.apply_remote(insert(&remote, 1, 10)).unwrap();
            let structural = Operation::move_node(&remote, 2, 20, NodeId(1), NodeId::ROOT, vec![2]);
            match path {
                "local" => {
                    tree.apply_remote(Operation::set_payload(&remote, 2, 19, NodeId(1), b"clock"))
                        .unwrap();
                    tree.local_move(NodeId(1), NodeId::ROOT, LocalPlacement::Last).unwrap();
                }
                "delta" => {
                    tree.apply_remote_with_delta(structural).unwrap();
                }
                "sorted" => {
                    tree.apply_sorted_remote_with_materialization(
                        structural,
                        &mut treecrdt_core::NoopParentOpIndex,
                        2,
                    )
                    .unwrap();
                }
                "replay" => {
                    tree.apply_remote(structural).unwrap();
                    tree.replay_from_storage().unwrap();
                }
                "stored" => {
                    let mut inner = MemoryStorage::default();
                    inner
                        .apply(Operation::insert_with_payload(
                            &remote,
                            1,
                            10,
                            NodeId::ROOT,
                            NodeId(2),
                            vec![1],
                            b"retained",
                        ))
                        .unwrap();
                    inner.apply(structural).unwrap();
                    tree = TreeCrdt::new(
                        local.clone(),
                        CountingStorage {
                            inner,
                            trace: trace.clone(),
                        },
                        LamportClock::default(),
                    )
                    .unwrap();
                    tree.replay_from_storage().unwrap();
                }
                "opaque" => {
                    let mut nodes = MemoryNodeStore::default();
                    nodes.attach(NodeId(1), NodeId::ROOT, vec![1]).unwrap();
                    // A supplied materialized prefix need not have a corresponding operation log.
                    tree = TreeCrdt::with_stores(
                        local.clone(),
                        CountingStorage {
                            inner: MemoryStorage::default(),
                            trace: trace.clone(),
                        },
                        LamportClock::default(),
                        nodes,
                        MemoryPayloadStore::default(),
                    )
                    .unwrap();
                }
                _ => unreachable!(),
            }
            tree.apply_remote(Operation::set_payload(&remote, 3, 100, NodeId(1), b"head"))
                .unwrap();
            trace.borrow_mut().scans = 0;
            tree.apply_remote(Operation::move_node(
                &remote,
                4,
                lamport,
                NodeId(1),
                NodeId::ROOT,
                vec![3],
            ))
            .unwrap();
            let scans = usize::from(lamport < 20 || path == "opaque");
            assert_eq!(trace.borrow().scans, scans, "{path} at {lamport}");
            if path == "stored" {
                let (mut replay, _) = counting_tree(&local);
                replay.apply_remote_batch(tree.operations_since(0).unwrap()).unwrap();
                assert_same_state(&tree, &replay);
            }
            if path == "opaque" {
                // Replay reconstructs a known prefix and enables the shortcut again.
                trace.borrow_mut().scans = 0;
                tree.apply_remote(Operation::move_node(
                    &remote,
                    5,
                    lamport + 1,
                    NodeId(1),
                    NodeId::ROOT,
                    vec![4],
                ))
                .unwrap();
                assert_eq!(trace.borrow().scans, 0);
            }
        }
    }
}

#[test]
fn late_deletes_and_placements_match_replay_without_scanning() {
    let local = ReplicaId::new(b"local");
    let remote = ReplicaId::new(b"remote");
    let (mut tree, trace) = counting_tree(&local);
    tree.local_insert(NodeId::ROOT, NodeId(1), LocalPlacement::Last, None).unwrap();
    tree.local_insert(NodeId(1), NodeId(2), LocalPlacement::Last, None).unwrap();
    let delete = tree.prepare_local_delete(NodeId(1)).unwrap().op;
    tree.local_move(NodeId(2), NodeId::ROOT, LocalPlacement::Last).unwrap();
    tree.apply_remote(Operation::delete(&remote, 1, 80, NodeId(99), None)).unwrap();
    tree.apply_remote(Operation::clear_payload(&remote, 2, 100, NodeId(99)))
        .unwrap();
    let (mut replay, _) = counting_tree(&local);
    replay.apply_remote_batch(tree.operations_since(0).unwrap()).unwrap();

    // These placements cross the newer deletion; the older delete crosses a move.
    let batch = vec![
        Operation::move_node(&remote, 4, 70, NodeId(2), NodeId(99), vec![1]),
        Operation::insert_with_payload(&remote, 3, 60, NodeId(1), NodeId(99), vec![1], b"older"),
        Operation::tombstone(&remote, 5, 75, NodeId(1)),
        delete,
    ];
    tree.apply_remote_batch(batch.clone()).unwrap();
    for op in batch {
        replay.apply_remote_with_delta(op).unwrap();
    }
    assert_eq!(trace.borrow().scans, 0);
    assert_same_state(&tree, &replay);
    assert_eq!(
        tree.prepare_local_delete(NodeId(1)).unwrap().op,
        replay.prepare_local_delete(NodeId(1)).unwrap().op
    );
}

#[test]
fn mixed_history_matches_sequential_ingestion_and_cold_batch() {
    let replica = ReplicaId::new(b"author");
    let (mut author, _) = counting_tree(&replica);
    let parent = NodeId(10);
    let sibling = NodeId(11);
    let child = NodeId(12);
    for (parent, node, payload) in [
        (NodeId::ROOT, parent, Some(b"parent".to_vec())),
        (NodeId::ROOT, sibling, None),
        (parent, child, Some(b"child".to_vec())),
    ] {
        author.local_insert(parent, node, LocalPlacement::Last, payload).unwrap();
    }
    author.local_move(child, sibling, LocalPlacement::First).unwrap();
    author.local_delete(sibling).unwrap();
    author.local_payload(child, Some(b"restored".to_vec())).unwrap();
    author.local_delete(parent).unwrap();
    author.local_payload(child, None).unwrap();
    let mut history = author.operations_since(0).unwrap();
    // A persisted semantic no-op must still become the materialized head.
    let no_op = Operation::move_node(&ReplicaId::new(b"remote"), 1, 20, sibling, sibling, vec![1]);
    history.push(no_op.clone());

    let (mut batch, trace) = counting_tree(&replica);
    let (mut sequential, _) = counting_tree(&replica);
    let mut arrival = Vec::new();
    // Seed the late payload clear before the older moves and deletion metadata arrive.
    for index in [0, 1, 2, 7] {
        let op = history[index].clone();
        batch.apply_remote(op.clone()).unwrap();
        sequential.apply_remote_with_delta(op.clone()).unwrap();
        arrival.push(op);
    }
    let received: Vec<_> = history[3..].iter().rev().cloned().collect();
    for op in &received {
        sequential.apply_remote_with_delta(op.clone()).unwrap();
    }
    batch.apply_remote_batch(received.iter().cloned()).unwrap();
    arrival.extend(received);
    let (mut cold, cold_trace) = counting_tree(&replica);
    cold.apply_remote_batch(arrival).unwrap();

    assert_eq!(trace.borrow().scans, 0);
    assert_eq!(cold_trace.borrow().scans, 0);
    assert_same_state(&batch, &sequential);
    assert_same_state(&batch, &cold);
    assert_eq!(batch.head_op(), Some(&no_op));

    let next_batch = batch.prepare_local_delete(sibling).unwrap().op;
    let next_sequential = sequential.prepare_local_delete(sibling).unwrap().op;
    let next_cold = cold.prepare_local_delete(sibling).unwrap().op;
    assert_eq!(next_batch, next_sequential);
    assert_eq!(next_batch, next_cold);
}

proptest! {
    #[test]
    fn mixed_operation_batches_match_canonical_replay(
        generated in prop::collection::vec(
            (0u8..5, 7u64..=12, 0usize..3, 1u128..=6, 0u128..=6,
             any::<u8>(), any::<u16>(), any::<bool>()),
            5..=40,
        ),
    ) {
        let local = ReplicaId::new(b"local");
        let replicas = [ReplicaId::new(b"a"), ReplicaId::new(b"b"), ReplicaId::new(b"c")];
        let (mut tree, _) = counting_tree(&local);
        let mut history: Vec<_> = (1..=6).map(|id| insert(&local, id, id)).collect();
        tree.apply_remote_batch(history.clone()).unwrap();
        let mut deliveries: Vec<_> = generated.into_iter().enumerate().map(
            |(index, (kind, lamport, author, node, parent, value, priority, batch_end))| {
                // IDs stay unique across arbitrary delivery order and equal Lamport keys.
                let counter = index as u64 + 1;
                let replica = &replicas[author];
                let op = match kind {
                    0 => Operation::insert_with_payload(
                        replica, counter, lamport, NodeId(parent), NodeId(node), vec![value], vec![value],
                    ),
                    1 => Operation::move_node(
                        replica, counter, lamport, NodeId(node), NodeId(parent), vec![value],
                    ),
                    2 => Operation::set_payload(replica, counter, lamport, NodeId(node), vec![value]),
                    3 => {
                        // This fixed envelope is independent of each receiver's delivery state.
                        let mut known_state = VersionVector::new();
                        for id in 1..=6 {
                            known_state.observe(&local, id);
                        }
                        known_state.observe(&replicas[(author + 1) % replicas.len()], counter / 2);
                        Operation::delete(replica, counter, lamport, NodeId(node), Some(known_state))
                    }
                    _ => Operation::clear_payload(replica, counter, lamport, NodeId(node)),
                };
                (priority, batch_end, op)
            },
        ).collect();
        deliveries.sort_by_key(|(priority, _, _)| *priority);
        let mut batch = Vec::new();
        let delivery_count = deliveries.len();
        for (index, (_, batch_end, op)) in deliveries.into_iter().enumerate() {
            batch.push(op);
            if !batch_end && index + 1 < delivery_count {
                continue;
            }
            tree.apply_remote_batch(batch.clone()).unwrap();
            history.append(&mut batch);

            // Rebuild a fresh oracle after every batch; do not use the optimized ingestion path.
            let mut storage = MemoryStorage::default();
            for op in &history {
                storage.apply(op.clone()).unwrap();
            }
            let mut replay = TreeCrdt::new(local.clone(), storage, LamportClock::default()).unwrap();
            replay.replay_from_storage().unwrap();
            assert_same_state(&tree, &replay);
        }
    }
}

#[test]
fn storage_failure_stops_at_failed_envelope_and_requires_replay() {
    let replica = ReplicaId::new(b"local");
    let (mut tree, trace) = counting_tree(&replica);
    trace.borrow_mut().fail_on_attempt = Some(2);
    let first = insert(&replica, 10, 10);
    let failed = insert(&replica, 20, 20);
    let unattempted = insert(&replica, 100, 100);
    let error = tree
        .apply_remote_batch(vec![first.clone(), failed.clone(), unattempted])
        .unwrap_err();

    assert!(matches!(error, Error::Storage(message) if message == "injected append failure"));
    assert_eq!(trace.borrow().attempts, vec![first.clone(), failed]);
    assert_eq!(tree.operations_since(0).unwrap(), vec![first]);
    assert_eq!(tree.lamport(), 20);
    assert_eq!(trace.borrow().scans, 0);
    // Persistence precedes materialization. Do not retry/continue against this stale state.
    assert!(tree.head_op().is_none());
    assert!(!tree.is_known(NodeId(10)).unwrap());

    trace.borrow_mut().fail_on_attempt = None;
    tree.replay_from_storage().unwrap();
    assert_eq!(tree.children(NodeId::ROOT).unwrap(), vec![NodeId(10)]);
    let (next, _) = tree.local_payload(NodeId(10), Some(vec![1])).unwrap();
    assert_eq!(next.meta.id.counter, 21);
    assert_eq!(next.meta.lamport, 21);
}

struct FailingPayloadStore {
    inner: MemoryPayloadStore,
    fail_next_write: bool,
}

impl PayloadStore for FailingPayloadStore {
    fn reset(&mut self) -> Result<()> {
        self.inner.reset()
    }

    fn payload(&self, node: NodeId) -> Result<Option<Vec<u8>>> {
        self.inner.payload(node)
    }

    fn last_writer(&self, node: NodeId) -> Result<Option<(Lamport, OperationId)>> {
        self.inner.last_writer(node)
    }

    fn set_payload(
        &mut self,
        node: NodeId,
        payload: Option<Vec<u8>>,
        writer: (Lamport, OperationId),
    ) -> Result<()> {
        if std::mem::take(&mut self.fail_next_write) {
            return Err(Error::Storage("injected payload failure".into()));
        }
        self.inner.set_payload(node, payload, writer)
    }
}

#[test]
fn materialization_failure_leaves_persisted_batch_for_replay() {
    let replica = ReplicaId::new(b"local");
    let mut tree = TreeCrdt::with_stores(
        replica.clone(),
        MemoryStorage::default(),
        LamportClock::default(),
        MemoryNodeStore::default(),
        FailingPayloadStore {
            inner: MemoryPayloadStore::default(),
            fail_next_write: true,
        },
    )
    .unwrap();
    let first = insert(&replica, 1, 1);
    let second = Operation::set_payload(&replica, 2, 2, NodeId(1), b"payload");
    let batch = vec![first.clone(), second.clone()];
    let error = tree.apply_remote_batch(batch.clone()).unwrap_err();

    assert!(matches!(error, Error::Storage(message) if message == "injected payload failure"));
    assert_eq!(tree.operations_since(0).unwrap(), batch);
    assert_eq!(tree.head_op(), Some(&first));
    assert_eq!(tree.children(NodeId::ROOT).unwrap(), vec![NodeId(1)]);
    assert_eq!(tree.payload(NodeId(1)).unwrap(), None);

    tree.replay_from_storage().unwrap();
    assert_eq!(tree.head_op(), Some(&second));
    assert_eq!(tree.payload(NodeId(1)).unwrap(), Some(b"payload".to_vec()));
}

use std::cell::RefCell;
use std::rc::Rc;

use treecrdt_core::{
    Error, Lamport, LamportClock, LocalPlacement, MemoryNodeStore, MemoryPayloadStore,
    MemoryStorage, NodeId, NodeStore, Operation, OperationId, PayloadStore, ReplicaId, Result,
    Storage, TreeCrdt, VersionVector,
};

#[derive(Default)]
struct StorageTrace {
    attempts: Vec<Operation>,
    scans: usize,
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
        self.inner.load_since(lamport)
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
        // Retaining the newer head must still trigger replay for a later-arriving old move.
        let moved = Operation::move_node(&remote, 6, 50, NodeId(2), NodeId::ROOT, vec![2]);
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

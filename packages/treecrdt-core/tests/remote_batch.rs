use std::cell::{Cell, RefCell};
use std::rc::Rc;

use treecrdt_core::{
    Error, Lamport, LamportClock, LocalPlacement, MemoryNodeStore, MemoryPayloadStore,
    MemoryStorage, NodeId, Operation, OperationId, PayloadStore, ReplicaId, Result, Storage,
    TreeCrdt, VersionVector,
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
    let (mut tree, trace) = counting_tree(&ReplicaId::new(b"local"));

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
    let first = Operation::insert_with_payload(
        &replica,
        1,
        20,
        NodeId::ROOT,
        NodeId(10),
        vec![10],
        b"first",
    );
    let later_duplicate = Operation::insert_with_payload(
        &replica,
        1,
        2,
        NodeId::ROOT,
        NodeId(11),
        vec![11],
        b"duplicate",
    );
    tree.apply_remote_batch(vec![first.clone(), later_duplicate]).unwrap();

    assert_eq!(tree.operations_since(0).unwrap(), vec![first.clone()]);
    assert_eq!(tree.head_op(), Some(&first));
    assert_eq!(tree.children(NodeId::ROOT).unwrap(), vec![NodeId(10)]);
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

#[test]
fn own_replica_counters_advance_without_filling_unobserved_causal_gaps() {
    let replica = ReplicaId::new(b"local");
    let (mut tree, _) = counting_tree(&replica);
    tree.apply_remote_batch(vec![insert(&replica, 5, 50)]).unwrap();
    let (next, _) = tree.local_move(NodeId(5), NodeId::ROOT, LocalPlacement::First).unwrap();

    assert_eq!(next.meta.id.counter, 6);
    assert_eq!(next.meta.lamport, 51);
    let mut observed = VersionVector::new();
    observed.observe(&replica, 5);
    observed.observe(&replica, 6);
    assert_eq!(tree.subtree_version_vector(NodeId(5)).unwrap(), observed);
    let prepared_delete = tree.prepare_local_delete(NodeId(5)).unwrap();
    assert_eq!(prepared_delete.op.meta.known_state, Some(observed));
    assert_eq!(prepared_delete.op.meta.id.counter, 7);
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
    // Seed the late payload clear so older structural operations require a replay.
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

    assert_eq!(trace.borrow().scans, 1);
    assert_eq!(cold_trace.borrow().scans, 0);
    assert_same_state(&batch, &sequential);
    assert_same_state(&batch, &cold);
    assert!(batch.is_tombstoned(parent).unwrap());
    assert!(!batch.is_tombstoned(sibling).unwrap());
    assert_eq!(batch.parent(child).unwrap(), Some(sibling));
    assert_eq!(batch.payload(child).unwrap(), None);
    assert_eq!(batch.head_op(), Some(&no_op));

    let (next_batch, _) = batch.local_delete(sibling).unwrap();
    let (next_sequential, _) = sequential.local_delete(sibling).unwrap();
    let (next_cold, _) = cold.local_delete(sibling).unwrap();
    assert_eq!(next_batch, next_sequential);
    assert_eq!(next_batch, next_cold);
    assert_same_state(&batch, &sequential);
    assert_same_state(&batch, &cold);
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
    fail: Rc<Cell<bool>>,
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
        if self.fail.get() {
            return Err(Error::Storage("injected payload failure".into()));
        }
        self.inner.set_payload(node, payload, writer)
    }
}

#[test]
fn materialization_failure_leaves_persisted_batch_for_replay() {
    let replica = ReplicaId::new(b"local");
    let fail = Rc::new(Cell::new(true));
    let mut tree = TreeCrdt::with_stores(
        replica.clone(),
        MemoryStorage::default(),
        LamportClock::default(),
        MemoryNodeStore::default(),
        FailingPayloadStore {
            inner: MemoryPayloadStore::default(),
            fail: fail.clone(),
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

    fail.set(false);
    tree.replay_from_storage().unwrap();
    assert_eq!(tree.head_op(), Some(&second));
    assert_eq!(tree.payload(NodeId(1)).unwrap(), Some(b"payload".to_vec()));
    let (next, _) = tree.local_payload(NodeId(1), None).unwrap();
    assert_eq!(next.meta.id.counter, 3);
    assert_eq!(next.meta.lamport, 3);
}

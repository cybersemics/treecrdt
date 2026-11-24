//! Create-only inserts (`OperationKind::Insert { if_absent: true, .. }`): an insert that does
//! nothing when its node is already in the tree at that point in the total order, so an automatic
//! create can never move a node that someone already placed.

use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use treecrdt_core::{
    catch_up_materialized_state, cmp_ops, materialize_persisted_remote_ops_with_delta,
    orchestrate_persisted_remote_append, try_direct_rewind_catch_up_materialized_state, Error,
    ExactPayloadStore, Lamport, LamportClock, LocalPlacement, MaterializationChange,
    MaterializationHead, MaterializationKey, MaterializationSource, MaterializationState,
    MemoryNodeStore, MemoryPayloadStore, MemoryStorage, NodeId, NodeStore, Operation, OperationId,
    OperationKind, ParentOpIndex, PayloadStore, PersistedRemoteStores, ReplicaId, Storage,
    TreeCrdt, TruncatingParentOpIndex, VersionVector,
};

type Crdt = TreeCrdt<MemoryStorage, LamportClock>;

const P1: NodeId = NodeId(1);
const P2: NodeId = NodeId(2);
const P3: NodeId = NodeId(3);
const N: NodeId = NodeId(10);

fn replica(name: &str) -> Crdt {
    TreeCrdt::new(
        ReplicaId::new(name.as_bytes()),
        MemoryStorage::default(),
        LamportClock::default(),
    )
    .unwrap()
}

fn rid(name: &str) -> ReplicaId {
    ReplicaId::new(name.as_bytes())
}

/// A one-digit order key.
fn key(digit: u16) -> Vec<u8> {
    digit.to_be_bytes().to_vec()
}

fn at(digit: u16) -> LocalPlacement {
    LocalPlacement::At { key: key(digit) }
}

fn vv_of(op: &Operation) -> VersionVector {
    let mut vv = VersionVector::new();
    vv.observe(&op.meta.id.replica, op.meta.id.counter);
    vv
}

fn source(op: &Operation) -> Option<MaterializationSource> {
    Some(MaterializationSource::from_op(op))
}

fn materialization_key(op: &Operation) -> MaterializationKey {
    MaterializationKey {
        lamport: op.meta.lamport,
        replica: op.meta.id.replica.as_bytes().to_vec(),
        counter: op.meta.id.counter,
    }
}

/// Inserts of P1, P2 and P3 under ROOT by a setup replica, stamped before everything else.
fn parents() -> Vec<Operation> {
    let s = rid("s");
    [P1, P2, P3]
        .into_iter()
        .enumerate()
        .map(|(i, p)| {
            let n = i as u64 + 1;
            Operation::insert(&s, n, n, NodeId::ROOT, p, key(0x1000 * n as u16))
        })
        .collect()
}

/// Everything a replica derives for one node.
#[derive(Debug, PartialEq)]
struct NodeRow {
    node: NodeId,
    parent: Option<NodeId>,
    order_key: Option<Vec<u8>>,
    children: Vec<NodeId>,
    last_change: VersionVector,
    deleted_at: Option<VersionVector>,
    tombstoned: bool,
    payload: Option<Vec<u8>>,
    writer: Option<(Lamport, OperationId)>,
}

impl NodeRow {
    /// A row that only holds the place of a node named before its own insert arrived.
    fn is_placeholder(&self) -> bool {
        self.parent.is_none()
            && self.children.is_empty()
            && self.last_change.is_empty()
            && self.deleted_at.is_none()
            && !self.tombstoned
            && self.writer.is_none()
    }
}

/// A replica's full derived state, sorted, so replicas compare exactly.
fn rows(c: &Crdt) -> Vec<NodeRow> {
    let mut rows: Vec<NodeRow> = c
        .export_nodes()
        .unwrap()
        .into_iter()
        .map(|e| NodeRow {
            node: e.node,
            parent: e.parent,
            order_key: e.order_key,
            children: e.children,
            last_change: e.last_change,
            deleted_at: e.deleted_at,
            tombstoned: c.is_tombstoned(e.node).unwrap(),
            payload: c.payload(e.node).unwrap(),
            writer: c.payload_last_writer(e.node).unwrap(),
        })
        .collect();
    rows.sort_by_key(|row| row.node);
    rows
}

/// A node's raw placement (TRASH and deleted included) and payload.
fn placement(c: &Crdt, node: NodeId) -> (Option<NodeId>, Option<Vec<u8>>, Option<Vec<u8>>) {
    let export = c.export_nodes().unwrap().into_iter().find(|e| e.node == node);
    let (parent, order_key) = export.map_or((None, None), |e| (e.parent, e.order_key));
    (parent, order_key, c.payload(node).unwrap())
}

fn permutations<T: Clone>(items: &[T]) -> Vec<Vec<T>> {
    if items.len() <= 1 {
        return vec![items.to_vec()];
    }
    let mut out = Vec::new();
    for i in 0..items.len() {
        let mut rest = items.to_vec();
        let first = rest.remove(i);
        for mut tail in permutations(&rest) {
            tail.insert(0, first.clone());
            out.push(tail);
        }
    }
    out
}

/// A fresh replica that received `setup`, then `ops` one at a time in the given order.
fn deliver(setup: &[Operation], ops: &[Operation]) -> Crdt {
    let mut c = replica("observer");
    for op in setup.iter().chain(ops) {
        c.apply_remote(op.clone()).unwrap();
    }
    c.validate_invariants().unwrap();
    c
}

/// A parent-op index that writes through to shared records: core takes stores by value, but a
/// backend's index outlives each call.
#[derive(Clone, Default)]
struct RecordingIndex(Rc<RefCell<Vec<(NodeId, OperationId, u64)>>>);

impl RecordingIndex {
    fn ops_under(&self, parent: NodeId) -> Vec<OperationId> {
        let records = self.0.borrow();
        records
            .iter()
            .filter(|(p, _, _)| *p == parent)
            .map(|(_, id, _)| id.clone())
            .collect()
    }
}

impl ParentOpIndex for RecordingIndex {
    fn reset(&mut self) -> treecrdt_core::Result<()> {
        self.0.borrow_mut().clear();
        Ok(())
    }

    fn record(
        &mut self,
        parent: NodeId,
        op_id: &OperationId,
        seq: u64,
    ) -> treecrdt_core::Result<()> {
        self.0.borrow_mut().push((parent, op_id.clone(), seq));
        Ok(())
    }
}

impl TruncatingParentOpIndex for RecordingIndex {
    fn truncate_from(&mut self, seq: u64) -> treecrdt_core::Result<()> {
        self.0.borrow_mut().retain(|(_, _, existing)| *existing < seq);
        Ok(())
    }
}

/// A payload store that writes through to a shared one, like `RecordingIndex`.
#[derive(Clone, Default)]
struct SharedPayloads(Rc<RefCell<MemoryPayloadStore>>);

impl PayloadStore for SharedPayloads {
    fn reset(&mut self) -> treecrdt_core::Result<()> {
        self.0.borrow_mut().reset()
    }

    fn payload(&self, node: NodeId) -> treecrdt_core::Result<Option<Vec<u8>>> {
        self.0.borrow().payload(node)
    }

    fn last_writer(&self, node: NodeId) -> treecrdt_core::Result<Option<(Lamport, OperationId)>> {
        self.0.borrow().last_writer(node)
    }

    fn set_payload(
        &mut self,
        node: NodeId,
        payload: Option<Vec<u8>>,
        writer: (Lamport, OperationId),
    ) -> treecrdt_core::Result<()> {
        self.0.borrow_mut().set_payload(node, payload, writer)
    }
}

impl ExactPayloadStore for SharedPayloads {
    fn clear_payload(&mut self, node: NodeId) -> treecrdt_core::Result<()> {
        self.0.borrow_mut().clear_payload(node)
    }
}

/// A backend as an adapter keeps one: an op log, node and payload rows, a parent-op index and the
/// materialized head, appended to through `orchestrate_persisted_remote_append` as the SQLite
/// extension does.
struct Backend {
    log: RefCell<Vec<Operation>>,
    nodes: RefCell<MemoryNodeStore>,
    payloads: SharedPayloads,
    index: RecordingIndex,
    meta: RefCell<MaterializationState>,
    /// Whether an out-of-order append tries direct rewind before catch-up, as adapters do.
    direct_rewind: bool,
    /// Whether the last append ran the full catch-up.
    caught_up: Cell<bool>,
}

type BackendStores =
    PersistedRemoteStores<LamportClock, MemoryNodeStore, SharedPayloads, RecordingIndex>;

impl Backend {
    fn new() -> Self {
        Self {
            log: RefCell::default(),
            nodes: RefCell::default(),
            payloads: SharedPayloads::default(),
            index: RecordingIndex::default(),
            meta: RefCell::new(MaterializationState {
                head: None,
                replay_from: None,
            }),
            direct_rewind: true,
            caught_up: Cell::new(false),
        }
    }

    /// A backend that received `ops` in one batch, so materialized them in canonical order.
    fn from_scratch(ops: &[Operation]) -> Self {
        let backend = Self::new();
        backend.append(ops).unwrap();
        backend
    }

    fn storage(&self) -> MemoryStorage {
        let mut storage = MemoryStorage::default();
        for op in self.log.borrow().iter() {
            storage.apply(op.clone()).unwrap();
        }
        storage
    }

    fn stores(&self) -> BackendStores {
        PersistedRemoteStores {
            replica_id: rid("backend"),
            clock: LamportClock::default(),
            nodes: self.nodes.borrow().clone(),
            payloads: self.payloads.clone(),
            index: self.index.clone(),
        }
    }

    /// Persists the ops it lacks and materializes them, returning the changes it reports.
    fn append(&self, ops: &[Operation]) -> treecrdt_core::Result<Vec<MaterializationChange>> {
        self.caught_up.set(false);
        let mut inserted = Vec::new();
        for op in ops {
            let mut log = self.log.borrow_mut();
            if !log.iter().any(|logged| logged.meta.id == op.meta.id) {
                log.push(op.clone());
                inserted.push(op.clone());
            }
        }
        let flush = |nodes: &mut MemoryNodeStore| {
            *self.nodes.borrow_mut() = nodes.clone();
            Ok(())
        };
        let meta = self.meta.borrow().clone();
        let result = orchestrate_persisted_remote_append(
            &meta,
            inserted,
            |node| self.payloads.last_writer(node),
            |meta, ops| {
                materialize_persisted_remote_ops_with_delta(
                    self.stores(),
                    &meta,
                    ops,
                    |_, _| Ok(()),
                    flush,
                    |_| Ok(()),
                )
            },
            |head| {
                *self.meta.borrow_mut() = MaterializationState {
                    head: Some(head.clone()),
                    replay_from: None,
                };
                Ok(())
            },
            |frontier| {
                self.meta.borrow_mut().replay_from = Some(frontier.clone());
                Ok(())
            },
            || Ok(self.meta.borrow().clone()),
            |meta, inserted| {
                if !self.direct_rewind {
                    return Ok(None);
                }
                try_direct_rewind_catch_up_materialized_state(
                    &self.storage(),
                    inserted,
                    self.stores(),
                    &meta,
                    flush,
                    |_| Ok(()),
                )
            },
            |meta| {
                self.caught_up.set(true);
                catch_up_materialized_state(self.storage(), self.stores(), &meta, flush, |_| Ok(()))
            },
            |message| Error::Storage(message.into()),
        )?;
        Ok(result.outcome.changes)
    }

    /// The backend's rows, as `rows` reads a replica's, without placeholder rows: catch-up keeps
    /// one where a voided op named a parent that nothing else created.
    fn rows(&self) -> Vec<NodeRow> {
        let nodes = self.nodes.borrow();
        let payloads = self.payloads.0.borrow();
        let mut ids = nodes.all_nodes().unwrap();
        ids.sort();
        ids.into_iter()
            .map(|node| NodeRow {
                node,
                parent: nodes.parent(node).unwrap(),
                order_key: nodes.order_key(node).unwrap(),
                children: nodes.children(node).unwrap(),
                last_change: nodes.last_change(node).unwrap(),
                deleted_at: nodes.deleted_at(node).unwrap(),
                tombstoned: nodes.tombstone(node).unwrap(),
                payload: payloads.payload(node).unwrap(),
                writer: payloads.last_writer(node).unwrap(),
            })
            .filter(|row| !row.is_placeholder())
            .collect()
    }

    /// Appends `ops` and checks the rows against a backend that materialized everything it has
    /// received from scratch, returning the changes.
    fn append_and_check(&self, ops: &[Operation]) -> Vec<MaterializationChange> {
        let changes = self.append(ops).unwrap();
        let log = self.log.borrow().clone();
        assert_eq!(self.rows(), Backend::from_scratch(&log).rows(), "{ops:?}");
        changes
    }
}

#[test]
fn create_only_insert_of_a_placed_node_changes_nothing() {
    let mut a = replica("a");
    a.local_insert(NodeId::ROOT, P1, LocalPlacement::Last, None).unwrap();
    a.local_insert(NodeId::ROOT, P2, LocalPlacement::Last, None).unwrap();
    let (insert, _) = a.local_insert(P1, N, LocalPlacement::Last, Some(b"kept".to_vec())).unwrap();
    let before = rows(&a);
    let lamport = a.lamport();
    let logged = a.operations_since(0).unwrap().len();

    // Locally: the node is present, so nothing is written, not even a counter or clock tick.
    for placement in [LocalPlacement::First, at(0x4000)] {
        let local = a.local_insert_if_absent(P2, N, placement, Some(b"new".to_vec()));
        assert!(local.unwrap().is_none());
    }
    assert_eq!(a.lamport(), lamport);
    assert_eq!(a.operations_since(0).unwrap().len(), logged);

    // Remotely: a later create-only insert from a replica that never saw the node is a no-op. It
    // still joins the log and advances the sequence, but reports and indexes nothing.
    let remote = Operation::insert_if_absent(
        &rid("b"),
        1,
        lamport + 1,
        P2,
        N,
        key(0x4000),
        Some(b"new".to_vec()),
    );
    let mut index = RecordingIndex::default();
    let mut seq = 7;
    let delta = a
        .apply_remote_with_materialization_seq(remote, &mut index, &mut seq)
        .unwrap()
        .expect("an in-order op applies");
    assert!(delta.changes.is_empty());
    assert!(index.0.borrow().is_empty());
    assert_eq!(seq, 8);
    assert_eq!(rows(&a), before);
    assert_eq!(a.parent(N).unwrap(), Some(P1));
    assert_eq!(a.payload(N).unwrap(), Some(b"kept".to_vec()));

    // The skipped local creates took no counter.
    let (next, _) = a.local_insert(NodeId::ROOT, P3, LocalPlacement::Last, None).unwrap();
    assert_eq!(next.meta.id.counter, insert.meta.id.counter + 1);
}

#[test]
fn create_only_insert_creates_an_absent_node() {
    let mut a = replica("a");
    let mut b = replica("b");

    let (op, plan) = a
        .local_insert_if_absent(NodeId::ROOT, N, LocalPlacement::Last, Some(b"ref".to_vec()))
        .unwrap()
        .expect("an absent node is created");
    assert!(matches!(
        op.kind,
        OperationKind::Insert {
            if_absent: true,
            ..
        }
    ));
    assert_eq!(
        plan.changes,
        vec![MaterializationChange::Insert {
            node: N,
            parent_after: NodeId::ROOT,
            payload: Some(b"ref".to_vec()),
            source: None,
        }]
    );
    assert_eq!(a.parent(N).unwrap(), Some(NodeId::ROOT));
    assert_eq!(a.payload(N).unwrap(), Some(b"ref".to_vec()));

    let delta = b.apply_remote_with_delta(op.clone()).unwrap().expect("an in-order op applies");
    assert_eq!(
        delta.changes,
        vec![MaterializationChange::Insert {
            node: N,
            parent_after: NodeId::ROOT,
            payload: Some(b"ref".to_vec()),
            source: source(&op),
        }]
    );
    assert_eq!(rows(&a), rows(&b));
}

#[test]
fn local_create_only_insert_takes_any_placement() {
    let mut a = replica("a");
    let [x, y, z] = [21, 22, 23].map(NodeId);
    let (insert_x, _) = a
        .local_insert_if_absent(NodeId::ROOT, x, at(0x0005), None)
        .unwrap()
        .expect("absent");
    let (insert_y, _) = a
        .local_insert_if_absent(
            NodeId::ROOT,
            y,
            LocalPlacement::Between {
                left: None,
                right: Some(key(0x0005)),
            },
            None,
        )
        .unwrap()
        .expect("absent");
    a.local_insert_if_absent(NodeId::ROOT, z, LocalPlacement::After(x), None)
        .unwrap()
        .expect("absent");
    for op in [&insert_x, &insert_y] {
        assert!(matches!(
            op.kind,
            OperationKind::Insert {
                if_absent: true,
                ..
            }
        ));
    }
    let OperationKind::Insert { order_key, .. } = &insert_x.kind else {
        unreachable!("an insert")
    };
    assert_eq!(order_key, &key(0x0005));
    assert_eq!(a.children(NodeId::ROOT).unwrap(), [y, x, z]);

    // A bad key fails like a plain insert's, before the op takes a counter.
    let logged = a.operations_since(0).unwrap().len();
    let bad = a.local_insert_if_absent(NodeId::ROOT, N, at(0), None);
    assert!(matches!(bad, Err(Error::InvalidOperation(_))));
    assert_eq!(a.operations_since(0).unwrap().len(), logged);
}

/// Devices that derive a reference's parent and key the same way write identical creates, apart
/// from their ids, and the earliest one wins wherever they meet.
#[test]
fn concurrent_creates_at_one_exact_key_converge() {
    let (mut a, mut b) = (replica("a"), replica("b"));
    for c in [&mut a, &mut b] {
        for op in parents() {
            c.apply_remote(op).unwrap();
        }
    }
    let (create_a, _) = a
        .local_insert_if_absent(P1, N, at(0x0007), Some(b"a".to_vec()))
        .unwrap()
        .expect("absent on a");
    let (create_b, _) = b
        .local_insert_if_absent(P1, N, at(0x0007), Some(b"b".to_vec()))
        .unwrap()
        .expect("absent on b");
    let strip_payload = |op: &Operation| match &op.kind {
        OperationKind::Insert {
            parent,
            node,
            order_key,
            if_absent,
            ..
        } => (*parent, *node, order_key.clone(), *if_absent),
        _ => unreachable!("an insert"),
    };
    assert_eq!(strip_payload(&create_a), strip_payload(&create_b));

    a.apply_remote(create_b.clone()).unwrap();
    b.apply_remote(create_a.clone()).unwrap();
    assert_eq!(rows(&a), rows(&b));
    let first = if cmp_ops(&create_a, &create_b).is_lt() {
        b"a".to_vec()
    } else {
        b"b".to_vec()
    };
    assert_eq!(placement(&a, N), (Some(P1), Some(key(0x0007)), Some(first)));
    assert_eq!(a.children(P1).unwrap(), [N]);
}

#[test]
fn create_only_insert_placed_between_prepare_and_commit_reports_nothing() {
    let mut a = replica("a");
    a.apply_remote(Operation::insert(
        &rid("s"),
        1,
        5,
        NodeId::ROOT,
        P1,
        key(0x1000),
    ))
    .unwrap();
    let prepared = a
        .prepare_local_insert_if_absent(NodeId::ROOT, N, LocalPlacement::Last, None)
        .unwrap()
        .expect("absent when prepared");
    // The node arrives before the prepared op commits, ordered just before it.
    let lamport = prepared.op.meta.lamport;
    a.apply_remote(Operation::insert(&rid("0"), 1, lamport, P1, N, key(0x1000)))
        .unwrap();
    let before = rows(&a);

    let (_, plan) = a.commit_prepared_local(prepared).unwrap();
    assert!(plan.changes.is_empty());
    assert!(plan.parent_hints.is_empty());
    assert!(plan.extra_index_records.is_empty());
    assert_eq!(rows(&a), before);
}

#[test]
fn concurrent_creates_converge_on_the_move_in_every_order() {
    let setup = parents();
    let (a, b, m) = (rid("a"), rid("b"), rid("m"));
    // (create by a, create by b, move by m) Lamports: the move after both creates, b's create
    // after the move, and b's create first.
    for (la, lb, lm) in [(10, 11, 12), (10, 13, 12), (11, 10, 12)] {
        let create_a =
            Operation::insert_if_absent(&a, 1, la, P1, N, key(0x4000), Some(b"a".to_vec()));
        let create_b =
            Operation::insert_if_absent(&b, 1, lb, P2, N, key(0x5000), Some(b"b".to_vec()));
        let move_n = Operation::move_node(&m, 1, lm, N, P3, key(0x6000));
        // The earliest create places the node and sets its payload; the move then wins the place.
        let first = if la < lb {
            b"a".to_vec()
        } else {
            b"b".to_vec()
        };
        let expected = (Some(P3), Some(key(0x6000)), Some(first));

        let ops = [create_a, create_b, move_n];
        let canonical = {
            let mut sorted = ops.to_vec();
            sorted.sort_by(cmp_ops);
            rows(&deliver(&setup, &sorted))
        };
        for order in permutations(&ops) {
            let c = deliver(&setup, &order);
            assert_eq!(
                placement(&c, N),
                expected,
                "stamps {la} {lb} {lm}, order {order:?}"
            );
            assert_eq!(
                rows(&c),
                canonical,
                "stamps {la} {lb} {lm}, order {order:?}"
            );

            let mut batched = replica("batched");
            batched.apply_remote_batch(setup.iter().chain(&order).cloned()).unwrap();
            assert_eq!(rows(&batched), canonical);
        }
    }
}

#[test]
fn concurrent_creates_without_a_move_converge_on_the_earliest_create() {
    let setup = parents();
    let (a, b) = (rid("a"), rid("b"));
    for (la, lb) in [(10, 11), (11, 10), (10, 10)] {
        let create_a =
            Operation::insert_if_absent(&a, 1, la, P1, N, key(0x4000), Some(b"a".to_vec()));
        let create_b =
            Operation::insert_if_absent(&b, 1, lb, P2, N, key(0x5000), Some(b"b".to_vec()));
        let earliest = [&create_a, &create_b].into_iter().min_by(|x, y| cmp_ops(x, y)).unwrap();
        let OperationKind::Insert {
            parent,
            order_key,
            payload,
            ..
        } = &earliest.kind
        else {
            unreachable!("an insert")
        };
        let expected = (Some(*parent), Some(order_key.clone()), payload.clone());

        let ops = [create_a.clone(), create_b.clone()];
        let orders = permutations(&ops);
        let first = rows(&deliver(&setup, &orders[0]));
        for order in &orders {
            let c = deliver(&setup, order);
            assert_eq!(placement(&c, N), expected, "stamps {la} {lb}");
            assert_eq!(rows(&c), first, "stamps {la} {lb}");
            // The losing create leaves no trace on its parent.
            let other = if parent == &P1 { P2 } else { P1 };
            assert!(c.children(other).unwrap().is_empty());
        }
    }
}

#[test]
fn deleted_or_trashed_node_is_neither_revived_nor_moved_by_a_later_create() {
    let deleted = NodeId(20);
    let trashed = NodeId(21);
    let mut a = replica("a");
    a.local_insert(NodeId::ROOT, P1, LocalPlacement::Last, None).unwrap();
    a.local_insert(NodeId::ROOT, P2, LocalPlacement::Last, None).unwrap();
    a.local_insert(P1, deleted, LocalPlacement::Last, Some(b"x".to_vec())).unwrap();
    a.local_insert(P1, trashed, LocalPlacement::Last, None).unwrap();
    a.local_delete(deleted).unwrap();
    a.local_move(trashed, NodeId::TRASH, LocalPlacement::First).unwrap();
    assert!(a.is_tombstoned(deleted).unwrap());
    let before = rows(&a);

    // Both are in the tree, as are ROOT and TRASH, so the local API writes nothing.
    for node in [deleted, trashed, NodeId::ROOT, NodeId::TRASH] {
        let local = a.local_insert_if_absent(P2, node, LocalPlacement::Last, None).unwrap();
        assert!(local.is_none());
    }

    // Later creates from a replica that never saw them change nothing either.
    let b = rid("b");
    for (counter, node) in [(1, deleted), (2, trashed)] {
        let lamport = a.lamport() + 1;
        let op = Operation::insert_if_absent(
            &b,
            counter,
            lamport,
            P2,
            node,
            key(0x4000),
            Some(b"y".to_vec()),
        );
        let delta = a.apply_remote_with_delta(op).unwrap().expect("an in-order op applies");
        assert!(delta.changes.is_empty());
    }
    assert_eq!(rows(&a), before);
    assert!(a.is_tombstoned(deleted).unwrap());
    assert_eq!(a.parent(deleted).unwrap(), Some(NodeId::TRASH));
    assert_eq!(a.payload(deleted).unwrap(), Some(b"x".to_vec()));
    assert!(a.children(P2).unwrap().is_empty());
}

#[test]
fn no_op_create_does_not_revive_a_concurrently_deleted_subtree() {
    let a = rid("a");
    let b = rid("b");
    let p = NodeId(30);
    let q = NodeId(31);
    let insert_p = Operation::insert(&a, 1, 1, NodeId::ROOT, p, key(0x1000));
    let insert_q = Operation::insert(&a, 2, 2, NodeId::ROOT, q, key(0x2000));
    let insert_n = Operation::insert(&a, 3, 3, p, N, key(0x1000));
    // The deleter knew all of a's ops so far.
    let mut known = VersionVector::new();
    for counter in 1..=3 {
        known.observe(&a, counter);
    }
    let delete_p = Operation::delete(&a, 4, 4, p, Some(known));
    let setup = vec![insert_p, insert_q, insert_n];

    // b never saw N and creates it under Q after N's insert, concurrently with the delete (stamped
    // before it, then after it): as a no-op it leaves P's subtree as the deleter knew it, so P
    // stays deleted with N inside. A plain insert would have pulled N out of P instead.
    for lamport in [3, 5] {
        let create = Operation::insert_if_absent(&b, 1, lamport, q, N, key(0x4000), None);
        let ops = [create.clone(), delete_p.clone()];
        let first = rows(&deliver(&setup, &ops));
        for order in permutations(&ops) {
            let c = deliver(&setup, &order);
            assert!(
                c.is_tombstoned(p).unwrap(),
                "create at {lamport}, order {order:?}"
            );
            assert_eq!(placement(&c, N).0, Some(p));
            assert!(c.children(q).unwrap().is_empty());
            assert_eq!(rows(&c), first);
        }

        let plain = Operation::insert(&b, 1, lamport, q, N, key(0x4000));
        let c = deliver(&setup, &[plain, delete_p.clone()]);
        assert_eq!(c.parent(N).unwrap(), Some(q));
    }
}

#[test]
fn create_under_a_concurrently_deleted_parent_behaves_like_a_plain_insert() {
    let run = |create_only: bool| {
        let mut a = replica("a");
        let mut b = replica("b");
        let (insert_p, _) = a.local_insert(NodeId::ROOT, P1, LocalPlacement::Last, None).unwrap();
        b.apply_remote(insert_p).unwrap();

        let (create, _) = if create_only {
            b.local_insert_if_absent(P1, N, LocalPlacement::Last, Some(b"n".to_vec()))
                .unwrap()
                .expect("absent on b")
        } else {
            b.local_insert(P1, N, LocalPlacement::Last, Some(b"n".to_vec())).unwrap()
        };
        let (delete, _) = a.local_delete(P1).unwrap();
        assert!(a.is_tombstoned(P1).unwrap());

        a.apply_remote(create).unwrap();
        b.apply_remote(delete).unwrap();
        assert_eq!(rows(&a), rows(&b));
        rows(&a)
    };

    let created = run(true);
    assert_eq!(created, run(false));
    let parent = created.iter().find(|row| row.node == P1).unwrap();
    assert!(
        !parent.tombstoned,
        "the create revives the parent, as an insert does"
    );
    assert_eq!(parent.children, vec![N]);
}

#[test]
fn node_with_only_a_placeholder_row_is_created() {
    let r = rid("r");
    let child = NodeId(40);
    let payload_only = NodeId(41);
    let early_ops = [
        // N named as a parent before its own insert arrived.
        Operation::insert(&r, 1, 1, N, child, key(0x1000)),
        // A payload write for a node whose insert has not arrived.
        Operation::set_payload(&r, 2, 2, payload_only, b"early".to_vec()),
    ];

    let mut a = replica("a");
    for op in &early_ops {
        a.apply_remote(op.clone()).unwrap();
    }
    for node in [N, payload_only] {
        assert!(a.is_known(node).unwrap(), "the node has a row");
        assert_eq!(placement(&a, node).0, None, "but no place in the tree");
    }

    let create =
        Operation::insert_if_absent(&r, 3, 3, NodeId::ROOT, N, key(0x2000), Some(b"n".to_vec()));
    let delta = a.apply_remote_with_delta(create.clone()).unwrap().expect("in order");
    assert_eq!(
        delta.changes,
        vec![MaterializationChange::Insert {
            node: N,
            parent_after: NodeId::ROOT,
            payload: Some(b"n".to_vec()),
            source: source(&create),
        }]
    );
    assert_eq!(a.parent(N).unwrap(), Some(NodeId::ROOT));
    assert_eq!(a.children(N).unwrap(), vec![child]);

    // The local API also treats a placeholder row as absent.
    let (op, _) = a
        .local_insert_if_absent(NodeId::ROOT, payload_only, LocalPlacement::Last, None)
        .unwrap()
        .expect("a placeholder row is not a place in the tree");
    assert!(matches!(
        op.kind,
        OperationKind::Insert {
            if_absent: true,
            ..
        }
    ));
    assert_eq!(a.parent(payload_only).unwrap(), Some(NodeId::ROOT));
    assert_eq!(a.payload(payload_only).unwrap(), Some(b"early".to_vec()));
}

#[test]
fn plain_insert_after_a_create_only_insert_still_moves() {
    let setup = parents();
    let create =
        Operation::insert_if_absent(&rid("a"), 1, 10, P1, N, key(0x4000), Some(b"c".to_vec()));
    let plain = Operation::insert_with_payload(&rid("b"), 1, 11, P2, N, key(0x5000), b"p".to_vec());
    for order in permutations(&[create, plain]) {
        let c = deliver(&setup, &order);
        assert_eq!(
            placement(&c, N),
            (Some(P2), Some(key(0x5000)), Some(b"p".to_vec()))
        );
        assert!(c.children(P1).unwrap().is_empty());
    }
}

/// An earlier-stamped create arriving late turns the published create into a no-op. Catch-up
/// reports the move away from the published parent and the payload left, and leaves the backend
/// rows exactly as a replay from scratch would, the published parent's history and index included.
#[test]
fn catch_up_reports_a_late_earlier_create_as_a_move_from_the_published_parent() {
    let late =
        Operation::insert_if_absent(&rid("a"), 1, 10, P1, N, key(0x4000), Some(b"a".to_vec()));
    let published =
        Operation::insert_if_absent(&rid("b"), 1, 11, P2, N, key(0x5000), Some(b"b".to_vec()));
    let backend = Backend::from_scratch(&parents());
    backend.append_and_check(std::slice::from_ref(&published));
    let published_vv = vv_of(&published);
    assert!(backend.nodes.borrow().last_change(P2).unwrap().is_aware_of(&published_vv));
    assert!(backend.index.ops_under(P2).contains(&published.meta.id));

    let changes = backend.append_and_check(std::slice::from_ref(&late));
    assert!(backend.caught_up.get());
    assert_eq!(
        changes,
        vec![
            MaterializationChange::Move {
                node: N,
                parent_before: Some(P2),
                parent_after: P1,
                source: source(&late),
            },
            MaterializationChange::Payload {
                node: N,
                payload: Some(b"a".to_vec()),
                source: None,
            },
        ]
    );
    assert!(!backend.nodes.borrow().last_change(P2).unwrap().is_aware_of(&published_vv));
    assert!(!backend.index.ops_under(P2).contains(&published.meta.id));
    assert!(backend.index.ops_under(P1).contains(&late.meta.id));
}

/// N's create under P reached the backend before P's own insert, so P has only a placeholder row.
/// A late, earlier create of N under ROOT voids it, and replay never creates P: catch-up must
/// still patch P's row back to a placeholder, not fail on a node the replay lacks.
#[test]
fn catch_up_patches_a_published_parent_that_replay_never_creates() {
    let p = NodeId(1);
    let late = Operation::insert_if_absent(&rid("a"), 1, 10, NodeId::ROOT, N, key(0x1000), None);
    let published = Operation::insert_if_absent(&rid("b"), 1, 11, p, N, key(0x2000), None);
    let backend = Backend::from_scratch(std::slice::from_ref(&published));
    assert_eq!(backend.nodes.borrow().parent(N).unwrap(), Some(p));

    let changes = backend.append_and_check(std::slice::from_ref(&late));
    assert_eq!(
        changes,
        vec![MaterializationChange::Move {
            node: N,
            parent_before: Some(p),
            parent_after: NodeId::ROOT,
            source: source(&late),
        }]
    );
    assert!(backend.nodes.borrow().children(p).unwrap().is_empty());
}

/// When a late op voids a published create-only insert that set the payload, the payload falls
/// back to an older write, or to none, which no replayed op writes. Catch-up still reports it.
#[test]
fn catch_up_reports_the_payload_left_when_a_create_is_voided() {
    let published =
        Operation::insert_if_absent(&rid("b"), 1, 11, P1, N, key(0x2000), Some(b"b".to_vec()));

    // A late move places N first, and nothing else wrote a payload.
    let late = Operation::move_node(&rid("a"), 1, 10, N, P2, key(0x1000));
    let backend = Backend::from_scratch(&parents());
    backend.append_and_check(std::slice::from_ref(&published));
    assert_eq!(
        backend.append_and_check(std::slice::from_ref(&late)),
        vec![
            MaterializationChange::Move {
                node: N,
                parent_before: Some(P1),
                parent_after: P2,
                source: source(&late),
            },
            MaterializationChange::Payload {
                node: N,
                payload: None,
                source: None,
            },
        ]
    );

    // A late create without a payload places N first, in the same parent: an older write is left.
    let early = Operation::set_payload(&rid("s"), 4, 4, N, b"z".to_vec());
    let late = Operation::insert_if_absent(&rid("a"), 1, 10, P1, N, key(0x1000), None);
    let backend = Backend::from_scratch(&parents());
    backend.append_and_check(&[early, published]);
    assert_eq!(
        backend.append_and_check(std::slice::from_ref(&late)),
        vec![
            MaterializationChange::Move {
                node: N,
                parent_before: Some(P1),
                parent_after: P1,
                source: source(&late),
            },
            MaterializationChange::Payload {
                node: N,
                payload: Some(b"z".to_vec()),
                source: None,
            },
        ]
    );
}

/// Two devices create the same node in the same parent, and the earlier create reaches the backend
/// after it published the later one. The node moved only within its parent, or not at all, so
/// catch-up reports a move within the parent (as for a drag), or nothing, never a second insert.
#[test]
fn catch_up_reports_a_create_already_shown_in_its_parent_as_a_reorder_or_not_at_all() {
    let payload = Some(b"member".to_vec());
    let published =
        Operation::insert_if_absent(&rid("b"), 1, 11, P1, N, key(0x5000), payload.clone());
    for late_key in [key(0x4000), key(0x5000)] {
        let late =
            Operation::insert_if_absent(&rid("a"), 1, 10, P1, N, late_key.clone(), payload.clone());
        let backend = Backend::from_scratch(&parents());
        backend.append_and_check(std::slice::from_ref(&published));
        let changes = backend.append_and_check(std::slice::from_ref(&late));
        assert_eq!(
            backend.nodes.borrow().order_key(N).unwrap(),
            Some(late_key.clone())
        );
        if late_key == key(0x5000) {
            assert_eq!(changes, vec![]);
        } else {
            assert_eq!(
                changes,
                vec![MaterializationChange::Move {
                    node: N,
                    parent_before: Some(P1),
                    parent_after: P1,
                    source: source(&late),
                }]
            );
        }
    }
}

fn rewind_meta(head: &Operation, seq: u64, replay_from: &Operation) -> MaterializationState {
    MaterializationState {
        head: Some(MaterializationHead {
            at: materialization_key(head),
            seq,
        }),
        replay_from: Some(materialization_key(replay_from)),
    }
}

/// Whether direct rewind catches up a backend that materialized `prefix` in order, after `late`
/// arrived, or leaves it to full catch-up.
fn try_direct_rewind(prefix: &[Operation], late: &Operation) -> bool {
    let backend = Backend::from_scratch(prefix);
    backend.log.borrow_mut().push(late.clone());
    let head = prefix.iter().max_by(|a, b| cmp_ops(a, b)).unwrap();
    try_direct_rewind_catch_up_materialized_state(
        &backend.storage(),
        &HashSet::from([late.meta.id.clone()]),
        backend.stores(),
        &rewind_meta(head, prefix.len() as u64, late),
        |_| Ok(()),
        |_| Ok(()),
    )
    .unwrap()
    .is_some()
}

#[test]
fn direct_rewind_leaves_create_only_suffixes_to_catch_up() {
    let late = Operation::insert_if_absent(&rid("a"), 1, 10, P1, N, key(0x4000), None);
    let published = Operation::insert_if_absent(&rid("b"), 1, 11, P2, N, key(0x5000), None);
    let mut prefix = parents();
    prefix.push(published);
    assert!(!try_direct_rewind(&prefix, &late));
}

/// Under a materialized suffix of payload writes, the tree is as it was at the frontier, so a late
/// create-only insert replays against the right tree, whether it places its node or not.
#[test]
fn direct_rewind_replays_a_late_create_only_insert() {
    let r = rid("r");
    let placed = Operation::insert(&r, 4, 4, P1, N, key(0x1000));
    let suffix = Operation::set_payload(&rid("m"), 1, 8, P3, b"m".to_vec());
    let late_noop = Operation::insert_if_absent(&rid("a"), 1, 6, P2, N, key(0x4000), None);
    let late_create = Operation::insert_if_absent(
        &rid("a"),
        1,
        6,
        P2,
        NodeId(11),
        key(0x4000),
        Some(b"c".to_vec()),
    );
    for late in [late_noop, late_create] {
        let mut prefix = parents();
        prefix.extend([placed.clone(), suffix.clone()]);
        assert!(try_direct_rewind(&prefix, &late));

        let backend = Backend::from_scratch(&prefix);
        backend.append_and_check(std::slice::from_ref(&late));
        assert!(!backend.caught_up.get(), "direct rewind, not catch-up, ran");
    }
}

/// Direct rewind restores a payload written in the materialized suffix to its winner before the
/// frontier. A create-only insert there may have been a no-op that wrote nothing, so the suffix goes
/// to catch-up: restoring its payload would also put its id in the parent-op index under the parent
/// a late move takes the node to.
#[test]
fn direct_rewind_leaves_a_payload_last_written_by_a_create_only_insert_to_catch_up() {
    let insert = Operation::insert_with_payload(&rid("r"), 4, 4, P1, N, key(0x1000), b"x".to_vec());
    let no_op_create =
        Operation::insert_if_absent(&rid("c"), 1, 5, P2, N, key(0x2000), Some(b"y".to_vec()));
    let suffix = Operation::set_payload(&rid("m"), 1, 8, N, b"z".to_vec());
    let late = Operation::move_node(&rid("x"), 1, 6, N, P3, key(0x3000));
    let mut prefix = parents();
    prefix.extend([insert.clone(), no_op_create.clone(), suffix]);
    assert!(!try_direct_rewind(&prefix, &late));

    let backend = Backend::from_scratch(&prefix);
    backend.append_and_check(std::slice::from_ref(&late));
    assert!(backend.caught_up.get());
    let under_p3 = backend.index.ops_under(P3);
    assert!(under_p3.contains(&insert.meta.id), "{under_p3:?}");
    assert!(!under_p3.contains(&no_op_create.meta.id), "{under_p3:?}");
}

#[cfg(feature = "serde")]
#[test]
fn serde_writes_the_flag_only_for_create_only_inserts() {
    let r = rid("r");
    let plain = Operation::insert(&r, 1, 1, NodeId::ROOT, N, key(0x1000));
    let create = Operation::insert_if_absent(&r, 2, 2, NodeId::ROOT, N, key(0x1000), None);

    let plain_json = serde_json::to_string(&plain).unwrap();
    assert!(!plain_json.contains("if_absent"), "{plain_json}");
    let create_json = serde_json::to_string(&create).unwrap();
    assert!(create_json.contains("\"if_absent\":true"), "{create_json}");

    assert_eq!(
        serde_json::from_str::<Operation>(&plain_json).unwrap(),
        plain
    );
    assert_eq!(
        serde_json::from_str::<Operation>(&create_json).unwrap(),
        create
    );
}

struct Rng(u64);

impl Rng {
    fn below(&mut self, n: usize) -> usize {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        (x.wrapping_mul(0x2545_F491_4F6C_DD1D) % n as u64) as usize
    }

    fn shuffle<T>(&mut self, items: &mut [T]) {
        for i in (1..items.len()).rev() {
            items.swap(i, self.below(i + 1));
        }
    }
}

/// Ops from three replicas that create a few shared ids create-only, so their creates race, and
/// move them (into cycles too), write payloads and delete, syncing partially between steps. Creates
/// and moves place by sibling, between keys, or at a key derived from the node, as automatic
/// writes do.
fn random_history(rng: &mut Rng, steps: usize) -> Vec<Operation> {
    let mut replicas = ["a", "b", "c"].map(replica);
    let mut log: Vec<Operation> = Vec::new();
    for _ in 0..steps {
        let c = &mut replicas[rng.below(3)];
        let pool = (1..=6).map(NodeId);
        let placed: Vec<NodeId> = pool.filter(|n| placement(c, *n).0.is_some()).collect();
        let node = NodeId(1 + rng.below(6) as u128);
        let parent = match rng.below(10) {
            0 => NodeId::TRASH,
            1..=3 => NodeId::ROOT,
            _ if placed.is_empty() => NodeId::ROOT,
            _ => placed[rng.below(placed.len())],
        };
        let at = match rng.below(4) {
            0 => LocalPlacement::First,
            1 => LocalPlacement::Last,
            2 => LocalPlacement::Between {
                left: None,
                right: None,
            },
            _ => LocalPlacement::At {
                key: key(node.0 as u16),
            },
        };
        let value = Some(vec![b'v', rng.below(4) as u8]);
        let op = match rng.below(10) {
            0..=3 if parent != NodeId::TRASH => {
                let payload = value.filter(|_| rng.below(2) == 0);
                c.local_insert_if_absent(parent, node, at, payload).unwrap().map(|(op, _)| op)
            }
            4..=5 if placed.contains(&node) => Some(c.local_move(node, parent, at).unwrap().0),
            6 => Some(c.local_payload(node, value).unwrap().0),
            7 if placed.contains(&node) => Some(c.local_delete(node).unwrap().0),
            _ => {
                let have: HashSet<OperationId> =
                    c.operations_since(0).unwrap().into_iter().map(|op| op.meta.id).collect();
                let mut missing: Vec<Operation> = log
                    .iter()
                    .filter(|op| !have.contains(&op.meta.id) && rng.below(2) == 0)
                    .cloned()
                    .collect();
                rng.shuffle(&mut missing);
                for op in missing {
                    c.apply_remote(op).unwrap();
                }
                None
            }
        };
        log.extend(op);
    }
    log
}

/// What a backend shows for each node: its parent, order key, payload and tombstone.
type Shown = HashMap<NodeId, (Option<NodeId>, Option<Vec<u8>>, Option<Vec<u8>>, bool)>;

fn shown(backend: &Backend) -> Shown {
    let rows = backend.rows().into_iter();
    rows.map(|row| {
        (
            row.node,
            (row.parent, row.order_key, row.payload, row.tombstoned),
        )
    })
    .collect()
}

/// The changes describe the step from `before` to `after`: an insert is only for a node that was
/// not in the tree, and a move starts where the node was. After a catch-up, every visible change
/// is reported too: a move or insert to a node's new place, the payload a live node now has, and
/// every payload reported is the final one. (In order, a rejected move is still reported, and an
/// earlier payload write can win over a later insert's payload when one batch holds both.)
fn assert_changes_describe(
    before: &Shown,
    after: &Shown,
    changes: &[MaterializationChange],
    caught_up: bool,
) {
    let get = |shown: &Shown, node| shown.get(&node).cloned().unwrap_or_default();
    let in_tree = |parent: Option<NodeId>| parent.filter(|parent| *parent != NodeId::TRASH);
    for change in changes {
        match change {
            MaterializationChange::Insert { node, .. } => {
                assert_eq!(in_tree(get(before, *node).0), None, "{change:?}")
            }
            MaterializationChange::Move {
                node,
                parent_before,
                ..
            } => assert_eq!(*parent_before, in_tree(get(before, *node).0), "{change:?}"),
            _ => {}
        }
    }
    if !caught_up {
        return;
    }
    for change in changes {
        if let MaterializationChange::Insert { node, payload, .. }
        | MaterializationChange::Restore { node, payload, .. }
        | MaterializationChange::Payload { node, payload, .. } = change
        {
            assert_eq!(*payload, get(after, *node).2, "{change:?}");
        }
    }
    for node in before.keys().chain(after.keys()) {
        let ((parent, key, payload, tombstoned), was) = (get(after, *node), get(before, *node));
        let Some(parent) = in_tree(parent) else {
            continue;
        };
        if (Some(parent), &key) != (was.0, &was.1) {
            let moved = |change: &&MaterializationChange| {
                matches!(change,
                    MaterializationChange::Insert { node: n, parent_after, .. }
                    | MaterializationChange::Move { node: n, parent_after, .. }
                    if n == node && *parent_after == parent)
            };
            assert!(
                changes.iter().any(|change| moved(&change)),
                "{node:?}: {changes:?}"
            );
        }
        if payload != was.2 && !tombstoned {
            let carried = |change: &MaterializationChange| match change {
                MaterializationChange::Insert {
                    node: n,
                    payload: p,
                    ..
                }
                | MaterializationChange::Restore {
                    node: n,
                    payload: p,
                    ..
                }
                | MaterializationChange::Payload {
                    node: n,
                    payload: p,
                    ..
                } => n == node && *p == payload,
                _ => false,
            };
            assert!(changes.iter().any(carried), "{node:?}: {changes:?}");
        }
    }
}

/// Backends that receive racing create-only inserts in any order and batching, catching up with
/// and without direct rewind, hold exactly what a replay from scratch holds, and report the
/// changes.
fn backend_appends_match_a_replay_from_scratch(seeds: std::ops::RangeInclusive<u64>) {
    for seed in seeds {
        for direct_rewind in [false, true] {
            let mut rng = Rng(seed);
            let mut ops = random_history(&mut rng, 24);
            rng.shuffle(&mut ops);
            let mut backend = Backend::new();
            backend.direct_rewind = direct_rewind;
            let mut rest = ops.as_slice();
            while !rest.is_empty() {
                let (batch, tail) = rest.split_at((1 + rng.below(3)).min(rest.len()));
                let before = shown(&backend);
                let changes = backend.append_and_check(batch);
                let caught_up = backend.caught_up.get();
                assert_changes_describe(&before, &shown(&backend), &changes, caught_up);
                rest = tail;
            }
        }
    }
}

#[test]
fn backend_appends_in_any_order_match_a_replay_from_scratch() {
    backend_appends_match_a_replay_from_scratch(1..=300);
}

/// The same at more seeds:
/// `cargo test --release -p treecrdt-core --test create_only_insert -- --ignored`.
#[test]
#[ignore = "slow; run in release"]
fn backend_appends_in_any_order_match_a_replay_from_scratch_at_many_seeds() {
    backend_appends_match_a_replay_from_scratch(1..=20_000);
}

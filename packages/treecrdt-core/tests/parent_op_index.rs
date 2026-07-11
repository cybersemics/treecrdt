use std::collections::BTreeSet;

use treecrdt_core::{
    Lamport, LamportClock, LocalPlacement, MemoryStorage, NodeId, Operation, OperationId,
    ParentOpIndex, ReplicaId, Storage, TreeCrdt, VersionVector,
};

#[derive(Default)]
struct RecordingIndex {
    records: Vec<(NodeId, OperationId)>,
}

impl ParentOpIndex for RecordingIndex {
    fn reset(&mut self) -> treecrdt_core::Result<()> {
        self.records.clear();
        Ok(())
    }

    fn record(
        &mut self,
        parent: NodeId,
        op_id: &OperationId,
        _seq: u64,
    ) -> treecrdt_core::Result<()> {
        self.records.push((parent, op_id.clone()));
        Ok(())
    }
}

fn node(value: u128) -> NodeId {
    NodeId(value)
}

fn key(position: u16) -> Vec<u8> {
    position.wrapping_add(1).to_be_bytes().to_vec()
}

fn materialize(ops: &[Operation]) -> (TreeCrdt<MemoryStorage, LamportClock>, RecordingIndex) {
    let mut tree = TreeCrdt::new(
        ReplicaId::new(b"materializer"),
        MemoryStorage::default(),
        LamportClock::default(),
    )
    .unwrap();
    let mut index = RecordingIndex::default();
    let mut seq = 0;
    for op in ops {
        tree.apply_remote_with_materialization_seq(op.clone(), &mut index, &mut seq)
            .unwrap();
    }
    (tree, index)
}

fn replay_children_filter(
    ops: &[Operation],
    index: &RecordingIndex,
    parent: NodeId,
) -> TreeCrdt<MemoryStorage, LamportClock> {
    let selected: BTreeSet<_> = index
        .records
        .iter()
        .filter_map(|(indexed_parent, op_id)| (*indexed_parent == parent).then_some(op_id))
        .collect();
    let mut filtered = TreeCrdt::new(
        ReplicaId::new(b"filtered"),
        MemoryStorage::default(),
        LamportClock::default(),
    )
    .unwrap();
    for op in ops {
        if selected.contains(&op.meta.id) {
            filtered.apply_remote(op.clone()).unwrap();
        }
    }
    filtered
}

fn indexed_counters(index: &RecordingIndex, parent: NodeId) -> Vec<u64> {
    let mut counters: Vec<_> = index
        .records
        .iter()
        .filter_map(|(indexed_parent, op_id)| (*indexed_parent == parent).then_some(op_id.counter))
        .collect();
    counters.sort_unstable();
    counters.dedup();
    counters
}

#[derive(Default)]
struct DuplicateStorage;

impl Storage for DuplicateStorage {
    fn apply(&mut self, _op: Operation) -> treecrdt_core::Result<bool> {
        Ok(false)
    }

    fn load_since(&self, _lamport: Lamport) -> treecrdt_core::Result<Vec<Operation>> {
        Ok(Vec::new())
    }

    fn latest_lamport(&self) -> Lamport {
        0
    }
}

#[test]
fn duplicate_local_commit_does_not_create_a_materialized_node() {
    let target = node(99);
    let mut tree = TreeCrdt::new(
        ReplicaId::new(b"duplicate"),
        DuplicateStorage,
        LamportClock::default(),
    )
    .unwrap();
    let prepared = tree
        .prepare_local_insert(NodeId::ROOT, target, LocalPlacement::Last, None)
        .unwrap();

    let (_op, plan) = tree.commit_prepared_local(prepared).unwrap();

    assert!(!tree.is_known(target).unwrap());
    assert!(plan.parent_hints.is_empty());
    assert!(plan.changes.is_empty());
}

#[test]
fn descendant_restore_trigger_is_included_in_parent_filter() {
    let replica = ReplicaId::new(b"restore");
    let parent = node(1);
    let child = node(2);
    let mut known_state = VersionVector::new();
    known_state.observe(&replica, 1);
    let ops = vec![
        Operation::insert(&replica, 1, 1, NodeId::ROOT, parent, key(0)),
        Operation::delete(&replica, 2, 2, parent, Some(known_state)),
        Operation::insert(&replica, 3, 3, parent, child, key(0)),
    ];

    let (full, index) = materialize(&ops);
    let filtered = replay_children_filter(&ops, &index, NodeId::ROOT);

    assert_eq!(indexed_counters(&index, NodeId::ROOT), vec![1, 2, 3]);
    assert_eq!(full.children(NodeId::ROOT).unwrap(), vec![parent]);
    assert_eq!(filtered.children(NodeId::ROOT).unwrap(), vec![parent]);
    assert_eq!(filtered.children(parent).unwrap(), vec![child]);
}

#[test]
fn rejected_cycle_is_not_indexed_under_requested_parent() {
    let replica = ReplicaId::new(b"cycle");
    let parent = node(1);
    let child = node(2);
    let ops = vec![
        Operation::insert(&replica, 1, 1, NodeId::ROOT, parent, key(0)),
        Operation::insert(&replica, 2, 2, parent, child, key(0)),
        Operation::move_node(&replica, 3, 3, parent, child, key(0)),
    ];

    let (full, index) = materialize(&ops);
    let filtered = replay_children_filter(&ops, &index, child);

    assert!(indexed_counters(&index, child).is_empty());
    assert_eq!(full.children(child).unwrap(), Vec::<NodeId>::new());
    assert_eq!(filtered.children(child).unwrap(), Vec::<NodeId>::new());
}

#[test]
fn move_leaving_parent_is_included_in_source_filter() {
    let replica = ReplicaId::new(b"leave");
    let source = node(1);
    let destination = node(2);
    let child = node(3);
    let ops = vec![
        Operation::insert(&replica, 1, 1, NodeId::ROOT, source, key(0)),
        Operation::insert(&replica, 2, 2, source, child, key(0)),
        Operation::insert(&replica, 3, 3, NodeId::ROOT, destination, key(1)),
        Operation::move_node(&replica, 4, 4, child, destination, key(0)),
    ];

    let (full, index) = materialize(&ops);
    let filtered = replay_children_filter(&ops, &index, source);

    assert_eq!(indexed_counters(&index, source), vec![2, 4]);
    assert_eq!(full.children(source).unwrap(), Vec::<NodeId>::new());
    assert_eq!(filtered.children(source).unwrap(), Vec::<NodeId>::new());
}

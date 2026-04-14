use treecrdt_core::{
    apply_incremental_ops, apply_incremental_ops_with_delta, apply_persisted_remote_ops_with_delta,
    materialize_persisted_remote_ops_with_delta, try_incremental_materialization, LamportClock,
    MaterializationCursor, MaterializationHead, MemoryNodeStore, MemoryPayloadStore, MemoryStorage,
    NodeId, NoopParentOpIndex, Operation, OperationId, ParentOpIndex, PersistedRemoteStores,
    ReplicaId, TreeCrdt,
};

#[derive(Default)]
struct RecordingIndex {
    records: Vec<(NodeId, OperationId, u64)>,
}

#[derive(Default)]
struct Cursor {
    dirty: bool,
    head_lamport: u64,
    head_replica: Vec<u8>,
    head_counter: u64,
    head_seq: u64,
}

impl MaterializationCursor for Cursor {
    fn dirty(&self) -> bool {
        self.dirty
    }

    fn head_lamport(&self) -> u64 {
        self.head_lamport
    }

    fn head_replica(&self) -> &[u8] {
        &self.head_replica
    }

    fn head_counter(&self) -> u64 {
        self.head_counter
    }

    fn head_seq(&self) -> u64 {
        self.head_seq
    }
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
        seq: u64,
    ) -> treecrdt_core::Result<()> {
        self.records.push((parent, op_id.clone(), seq));
        Ok(())
    }
}

#[test]
fn try_incremental_materialization_marks_dirty_on_failure() {
    let mut marked_dirty = 0u64;
    let ok = try_incremental_materialization(
        false,
        || -> Result<(), ()> { Err(()) },
        || marked_dirty += 1,
    );
    assert!(!ok);
    assert_eq!(marked_dirty, 1);
}

#[test]
fn try_incremental_materialization_short_circuits_when_already_dirty() {
    let mut incremental_runs = 0u64;
    let mut marked_dirty = 0u64;

    let ok = try_incremental_materialization(
        true,
        || -> Result<(), ()> {
            incremental_runs += 1;
            Ok(())
        },
        || marked_dirty += 1,
    );
    assert!(!ok);
    assert_eq!(incremental_runs, 0);
    assert_eq!(marked_dirty, 1);
}

#[test]
fn finalize_local_materialization_records_unique_hints_and_extras() {
    let mut crdt = TreeCrdt::new(
        ReplicaId::new(b"local"),
        MemoryStorage::default(),
        LamportClock::default(),
    )
    .unwrap();

    let parent = NodeId(10);
    let node = NodeId(11);
    crdt.local_insert_after(NodeId::ROOT, parent, None).unwrap();
    let op = crdt.local_insert_after(parent, node, None).unwrap();

    let extra_op_id = OperationId {
        replica: ReplicaId::new(b"extra"),
        counter: 7,
    };

    let mut index = RecordingIndex::default();
    crdt.finalize_local_materialization(
        &op,
        &mut index,
        42,
        &[parent, parent, NodeId::TRASH],
        &[
            (parent, extra_op_id.clone()),
            (NodeId::TRASH, extra_op_id.clone()),
        ],
    )
    .unwrap();

    assert_eq!(index.records.len(), 2);
    assert_eq!(index.records[0], (parent, op.meta.id.clone(), 42));
    assert_eq!(index.records[1], (parent, extra_op_id, 42));
}

#[test]
fn apply_incremental_ops_sorts_and_returns_head() {
    let mut crdt = TreeCrdt::new(
        ReplicaId::new(b"local"),
        MemoryStorage::default(),
        LamportClock::default(),
    )
    .unwrap();
    let mut index = RecordingIndex::default();
    let cursor = Cursor::default();
    let replica = ReplicaId::new(b"remote");

    let first = Operation::insert(&replica, 1, 1, NodeId::ROOT, NodeId(1), vec![0x10]);
    let second = Operation::insert(&replica, 2, 2, NodeId::ROOT, NodeId(2), vec![0x20]);

    let next = apply_incremental_ops(&mut crdt, &mut index, &cursor, vec![second, first])
        .unwrap()
        .unwrap();

    assert_eq!(next.lamport, 2);
    assert_eq!(next.replica, replica.as_bytes());
    assert_eq!(next.counter, 2);
    assert_eq!(next.seq, 2);
    assert_eq!(index.records.len(), 2);
    assert_eq!(index.records[0].2, 1);
    assert_eq!(index.records[1].2, 2);
}

#[test]
fn apply_incremental_ops_rejects_before_materialized_head() {
    let mut crdt = TreeCrdt::new(
        ReplicaId::new(b"local"),
        MemoryStorage::default(),
        LamportClock::default(),
    )
    .unwrap();
    let mut index = RecordingIndex::default();
    let cursor = Cursor {
        dirty: false,
        head_lamport: 5,
        head_replica: b"r".to_vec(),
        head_counter: 3,
        head_seq: 12,
    };

    let op = Operation::insert(
        &ReplicaId::new(b"r"),
        2,
        4,
        NodeId::ROOT,
        NodeId(9),
        vec![0x10],
    );

    let res = apply_incremental_ops(&mut crdt, &mut index, &cursor, vec![op]);
    assert!(res.is_err());
}

#[test]
fn apply_incremental_ops_with_delta_returns_affected_union() {
    let mut crdt = TreeCrdt::new(
        ReplicaId::new(b"local"),
        MemoryStorage::default(),
        LamportClock::default(),
    )
    .unwrap();
    let mut index = RecordingIndex::default();
    let cursor = Cursor::default();
    let replica = ReplicaId::new(b"remote");

    let parent = NodeId(10);
    let child = NodeId(11);
    let parent_insert = Operation::insert(&replica, 1, 1, NodeId::ROOT, parent, vec![0x10]);
    let child_insert = Operation::insert(&replica, 2, 2, parent, child, vec![0x20]);

    let res = apply_incremental_ops_with_delta(
        &mut crdt,
        &mut index,
        &cursor,
        vec![child_insert, parent_insert],
    )
    .unwrap();

    let head = res.head.expect("expected materialization head");
    assert_eq!(head.counter, 2);
    assert_eq!(res.affected_nodes, vec![NodeId::ROOT, parent, child],);
}

#[test]
fn apply_persisted_remote_ops_materializes_only_inserted_entries() {
    let cursor = Cursor::default();
    let replica = ReplicaId::new(b"remote");
    let op2 = Operation::insert(&replica, 2, 2, NodeId::ROOT, NodeId(2), vec![0x20]);
    let op3 = Operation::set_payload(&replica, 3, 3, NodeId(2), vec![9]);

    let mut seen_counters = Vec::new();
    let mut updated_head = None;
    let result = apply_persisted_remote_ops_with_delta(
        &cursor,
        vec![op2.clone(), op3.clone()],
        |ops| {
            seen_counters = ops.iter().map(|op| op.meta.id.counter).collect();
            Ok::<_, ()>(treecrdt_core::IncrementalApplyResult {
                head: Some(MaterializationHead {
                    lamport: op3.meta.lamport,
                    replica: op3.meta.id.replica.as_bytes().to_vec(),
                    counter: op3.meta.id.counter,
                    seq: 2,
                }),
                affected_nodes: vec![NodeId(2)],
            })
        },
        |head| {
            updated_head = Some(head.clone());
            Ok::<_, ()>(())
        },
        || Ok::<_, ()>(()),
    );

    assert_eq!(seen_counters, vec![2, 3]);
    assert_eq!(result.inserted_count, 2);
    assert_eq!(result.affected_nodes, vec![NodeId(2)]);
    assert!(!result.dirty_fallback);
    assert_eq!(
        updated_head,
        Some(MaterializationHead {
            lamport: 3,
            replica: replica.as_bytes().to_vec(),
            counter: 3,
            seq: 2,
        })
    );
}

#[test]
fn apply_persisted_remote_ops_short_circuits_to_dirty_when_cursor_dirty() {
    let cursor = Cursor {
        dirty: true,
        ..Cursor::default()
    };
    let replica = ReplicaId::new(b"remote");
    let op = Operation::insert(&replica, 1, 1, NodeId::ROOT, NodeId(1), vec![0x10]);
    let mut runs = 0u64;
    let mut marked_dirty = 0u64;

    let result = apply_persisted_remote_ops_with_delta(
        &cursor,
        vec![op],
        |_| {
            runs += 1;
            Ok::<_, ()>(treecrdt_core::IncrementalApplyResult {
                head: None,
                affected_nodes: Vec::new(),
            })
        },
        |_| Ok::<_, ()>(()),
        || {
            marked_dirty += 1;
            Ok::<_, ()>(())
        },
    );

    assert_eq!(runs, 0);
    assert_eq!(marked_dirty, 1);
    assert_eq!(result.inserted_count, 1);
    assert_eq!(result.affected_nodes, Vec::<NodeId>::new());
    assert!(result.dirty_fallback);
}

#[test]
fn apply_persisted_remote_ops_clears_affected_nodes_when_update_head_fails() {
    let cursor = Cursor::default();
    let replica = ReplicaId::new(b"remote");
    let op = Operation::insert(&replica, 1, 1, NodeId::ROOT, NodeId(1), vec![0x10]);
    let mut marked_dirty = 0u64;

    let result = apply_persisted_remote_ops_with_delta(
        &cursor,
        vec![op],
        |_| {
            Ok::<_, ()>(treecrdt_core::IncrementalApplyResult {
                head: Some(MaterializationHead {
                    lamport: 7,
                    replica: b"r".to_vec(),
                    counter: 4,
                    seq: 9,
                }),
                affected_nodes: vec![NodeId(1), NodeId(2)],
            })
        },
        |_| Err::<(), ()>(()),
        || {
            marked_dirty += 1;
            Ok::<(), ()>(())
        },
    );

    assert_eq!(marked_dirty, 1);
    assert_eq!(result.inserted_count, 1);
    assert!(result.affected_nodes.is_empty());
    assert!(result.dirty_fallback);
}

#[test]
fn materialize_persisted_remote_ops_with_delta_runs_prepare_and_flush_hooks() {
    let cursor = Cursor::default();
    let replica = ReplicaId::new(b"remote");
    let parent = Operation::insert(&replica, 1, 1, NodeId::ROOT, NodeId(10), vec![0x10]);
    let child = Operation::insert(&replica, 2, 2, NodeId(10), NodeId(11), vec![0x20]);

    let mut prepared = 0u64;
    let mut flushed_nodes = 0u64;
    let mut flushed_index = 0u64;

    let result = materialize_persisted_remote_ops_with_delta(
        PersistedRemoteStores {
            replica_id: ReplicaId::new(b"adapter"),
            clock: LamportClock::default(),
            nodes: MemoryNodeStore::default(),
            payloads: MemoryPayloadStore::default(),
            index: NoopParentOpIndex,
        },
        &cursor,
        vec![child, parent],
        |_, ops| {
            prepared += ops.len() as u64;
            Ok(())
        },
        |_| {
            flushed_nodes += 1;
            Ok(())
        },
        |_| {
            flushed_index += 1;
            Ok(())
        },
    )
    .unwrap();

    let head = result.head.expect("expected head");
    assert_eq!(prepared, 2);
    assert_eq!(flushed_nodes, 1);
    assert_eq!(flushed_index, 1);
    assert_eq!(head.counter, 2);
    assert_eq!(
        result.affected_nodes,
        vec![NodeId::ROOT, NodeId(10), NodeId(11)]
    );
}

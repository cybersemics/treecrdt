use treecrdt_core::{
    apply_incremental_ops, try_incremental_materialization, LamportClock, MaterializationCursor,
    MemoryStorage, NodeId, Operation, OperationId, ParentOpIndex, ReplicaId, TreeCrdt,
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

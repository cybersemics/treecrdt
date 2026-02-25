use treecrdt_core::{
    try_incremental_materialization, LamportClock, MemoryStorage, NodeId, OperationId,
    ParentOpIndex, ReplicaId, TreeCrdt,
};

#[derive(Default)]
struct RecordingIndex {
    records: Vec<(NodeId, OperationId, u64)>,
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

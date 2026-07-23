use treecrdt_core::{
    Lamport, LamportClock, LocalPlacement, MemoryStorage, NodeId, Operation, OperationKind,
    ReplicaId, Result, Storage, TreeCrdt,
};

#[test]
fn structural_operation_validation_is_state_independent() {
    let replica = ReplicaId::new(b"remote");
    let invalid = [
        Operation::insert(&replica, 1, 1, NodeId::ROOT, NodeId(1), vec![]),
        Operation::insert(&replica, 2, 2, NodeId::ROOT, NodeId(2), vec![1]),
        Operation::insert(&replica, 3, 3, NodeId::ROOT, NodeId(3), vec![0, 1, 0, 0]),
        Operation::insert(&replica, 4, 4, NodeId::TRASH, NodeId(4), vec![0, 1]),
        Operation::move_node(&replica, 5, 5, NodeId(5), NodeId::ROOT, vec![]),
        Operation::move_node(&replica, 6, 6, NodeId(6), NodeId::TRASH, vec![0, 1]),
    ];
    for op in invalid {
        assert!(
            op.validate().is_err(),
            "operation should be rejected: {op:?}"
        );
    }

    for op in [
        Operation::insert(&replica, 7, 7, NodeId::ROOT, NodeId(7), vec![0, 1]),
        Operation::move_node(&replica, 8, 8, NodeId(8), NodeId::ROOT, vec![0xff, 0xff]),
        Operation::insert(&replica, 9, 9, NodeId::TRASH, NodeId(9), vec![]),
        Operation::move_node(&replica, 10, 10, NodeId(10), NodeId::TRASH, vec![]),
    ] {
        op.validate().unwrap();
    }
}

#[derive(Clone)]
struct UncheckedStorage(Vec<Operation>);

impl Storage for UncheckedStorage {
    fn apply(&mut self, op: Operation) -> Result<bool> {
        self.0.push(op);
        Ok(true)
    }

    fn load_since(&self, lamport: Lamport) -> Result<Vec<Operation>> {
        Ok(self.0.iter().filter(|op| op.meta.lamport > lamport).cloned().collect())
    }

    fn latest_lamport(&self) -> Lamport {
        self.0.iter().map(|op| op.meta.lamport).max().unwrap_or(0)
    }
}

#[test]
fn replay_revalidates_persisted_operations() {
    let invalid = Operation::insert(
        &ReplicaId::new(b"remote"),
        1,
        1,
        NodeId::ROOT,
        NodeId(10),
        vec![],
    );
    let mut tree = TreeCrdt::new(
        ReplicaId::new(b"local"),
        UncheckedStorage(vec![invalid]),
        LamportClock::default(),
    )
    .unwrap();

    let error = tree.replay_from_storage().unwrap_err();
    assert!(error.to_string().contains("non-empty"));
    assert!(tree.children(NodeId::ROOT).unwrap().is_empty());
}

#[test]
fn local_move_to_trash_uses_the_empty_sentinel_key() {
    let mut tree = TreeCrdt::new(
        ReplicaId::new(b"local"),
        MemoryStorage::default(),
        LamportClock::default(),
    )
    .unwrap();
    let node = NodeId(10);
    tree.local_insert(NodeId::ROOT, node, LocalPlacement::First, None).unwrap();

    let (operation, _) = tree.local_move(node, NodeId::TRASH, LocalPlacement::First).unwrap();
    let OperationKind::Move { order_key, .. } = operation.kind else {
        unreachable!();
    };
    assert!(order_key.is_empty());
}

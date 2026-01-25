use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use treecrdt_core::{LamportClock, MemoryStorage, NodeId, ReplicaId, TreeCrdt};

fn bench_insert_chain(c: &mut Criterion) {
    let sizes = [100u64, 1_000, 10_000];
    let mut group = c.benchmark_group("insert_chain");

    for size in sizes {
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &n| {
            b.iter_batched(
                || {
                    TreeCrdt::new(
                        ReplicaId::new(b"bench"),
                        MemoryStorage::default(),
                        LamportClock::default(),
                    )
                    .unwrap()
                },
                |mut crdt| {
                    let mut parent = NodeId::ROOT;
                    for i in 0..n {
                        let node = NodeId(i as u128 + 1);
                        crdt.local_insert_after(parent, node, None).unwrap();
                        parent = node;
                    }
                },
                BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

fn bench_move_siblings(c: &mut Criterion) {
    let mut group = c.benchmark_group("move_siblings");
    group.bench_function("move_first_to_last", |b| {
        b.iter_batched(
            || {
                let mut crdt = TreeCrdt::new(
                    ReplicaId::new(b"bench"),
                    MemoryStorage::default(),
                    LamportClock::default(),
                )
                .unwrap();
                let mut last: Option<NodeId> = None;
                for i in 0..1_000u64 {
                    let node = NodeId(i as u128 + 1);
                    crdt.local_insert_after(NodeId::ROOT, node, last).unwrap();
                    last = Some(node);
                }
                crdt
            },
            |mut crdt| {
                let first = NodeId(1);
                crdt.local_move_after(first, NodeId::ROOT, Some(NodeId(1_000))).unwrap();
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

criterion_group!(core, bench_insert_chain, bench_move_siblings);
criterion_main!(core);

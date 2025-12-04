use std::env;
use std::fs;
use std::path::PathBuf;
use std::time::Instant;

use treecrdt_core::{AllowAllAccess, Lamport, LamportClock, MemoryStorage, NodeId, ReplicaId, TreeCrdt};

#[derive(serde::Serialize)]
struct Output {
    implementation: &'static str,
    storage: &'static str,
    workload: String,
    timestamp: String,
    name: String,
    total_ops: u64,
    duration_ms: f64,
    ops_per_sec: f64,
    extra: Extra,
    source_file: Option<String>,
}

#[derive(serde::Serialize)]
struct Extra {
    count: u64,
}

fn hex_id(n: u64) -> NodeId {
    let mut bytes = [0u8; 16];
    bytes[8..].copy_from_slice(&n.to_be_bytes());
    NodeId(u128::from_be_bytes(bytes))
}

fn main() {
    let mut count: u64 = 200;
    let mut out_file: Option<PathBuf> = None;
    for arg in env::args().skip(1) {
        if let Some(val) = arg.strip_prefix("--count=") {
            count = val.parse().unwrap_or(count);
        } else if let Some(val) = arg.strip_prefix("--out=") {
            out_file = Some(PathBuf::from(val));
        }
    }

    let replica = ReplicaId::new(b"core");
    let storage = MemoryStorage::default();
    let mut tree = TreeCrdt::new(replica, storage, AllowAllAccess, LamportClock::default());

    let start = Instant::now();
    for i in 0..count {
        let node = hex_id(i + 1);
        let _ = tree.local_insert(NodeId::ROOT, node, i as usize).unwrap();
    }
    for i in 0..count {
        let node = hex_id(i + 1);
        let _ = tree.local_move(node, NodeId::ROOT, 0).unwrap();
    }
    let _ = tree.operations_since(0 as Lamport).unwrap();
    let duration_ms = start.elapsed().as_secs_f64() * 1000.0;

    let output = Output {
        implementation: "core-memory",
        storage: "memory",
        workload: format!("insert-move-{}", count),
        timestamp: chrono::Utc::now().to_rfc3339(),
        name: format!("insert-move-{}", count),
        total_ops: count * 2,
        duration_ms,
        ops_per_sec: if duration_ms > 0.0 {
            (count as f64 * 2.0) / duration_ms * 1000.0
        } else {
            f64::INFINITY
        },
        extra: Extra { count },
        source_file: out_file.as_ref().map(|p| p.display().to_string()),
    };

    let json = serde_json::to_string_pretty(&output).expect("serialize");
    if let Some(path) = out_file {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("mkdirs");
        }
        fs::write(&path, &json).expect("write output");
    }
    println!("{}", json);
}

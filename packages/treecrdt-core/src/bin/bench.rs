use std::env;
use std::fs;
use std::path::PathBuf;
use std::time::Instant;

use treecrdt_core::{Lamport, LamportClock, MemoryStorage, NodeId, ReplicaId, TreeCrdt};

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
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
#[serde(rename_all = "camelCase")]
struct Extra {
    count: u64,
    mode: &'static str,
}

fn hex_id(n: u64) -> NodeId {
    let mut bytes = [0u8; 16];
    bytes[8..].copy_from_slice(&n.to_be_bytes());
    NodeId(u128::from_be_bytes(bytes))
}

fn main() {
    let mut counts: Vec<u64> = vec![1, 10, 100, 1_000, 10_000];
    let mut out_dir: Option<PathBuf> = None;
    for arg in env::args().skip(1) {
        if let Some(val) = arg.strip_prefix("--count=") {
            counts = vec![val.parse().unwrap_or(500)];
        } else if let Some(val) = arg.strip_prefix("--counts=") {
            let parsed: Vec<u64> =
                val.split(',').filter_map(|s| s.trim().parse::<u64>().ok()).collect();
            if !parsed.is_empty() {
                counts = parsed;
            }
        } else if let Some(val) = arg.strip_prefix("--out-dir=") {
            out_dir = Some(PathBuf::from(val));
        }
    }

    let out_dir = out_dir.unwrap_or_else(|| PathBuf::from("benchmarks/core"));
    fs::create_dir_all(&out_dir).expect("mkdirs");

    let replica = ReplicaId::new(b"core");
    for count in counts {
        let storage = MemoryStorage::default();
        let mut tree = TreeCrdt::new(replica.clone(), storage, LamportClock::default());

        let start = Instant::now();
        let mut last: Option<NodeId> = None;
        for i in 0..count {
            let node = hex_id(i + 1);
            let _ = tree.local_insert_after(NodeId::ROOT, node, last).unwrap();
            last = Some(node);
        }
        for i in 0..count {
            let node = hex_id(i + 1);
            let _ = tree.local_move_after(node, NodeId::ROOT, None).unwrap();
        }
        let _ = tree.operations_since(0 as Lamport).unwrap();
        let duration_ms = start.elapsed().as_secs_f64() * 1000.0;

        let workload_name = format!("insert-move-{}", count);
        let out_path = out_dir.join(format!("memory-{}.json", workload_name));

        let output = Output {
            implementation: "core-inmem-crdt",
            storage: "memory",
            workload: workload_name.clone(),
            timestamp: chrono::Utc::now().to_rfc3339(),
            name: workload_name.clone(),
            total_ops: count * 2,
            duration_ms,
            ops_per_sec: if duration_ms > 0.0 {
                (count as f64 * 2.0) / duration_ms * 1000.0
            } else {
                f64::INFINITY
            },
            extra: Extra {
                count,
                mode: "sequential",
            },
            source_file: Some(out_path.display().to_string()),
        };

        let json = serde_json::to_string_pretty(&output).expect("serialize");
        fs::write(&out_path, &json).expect("write output");
        println!("{}", json);
    }
}

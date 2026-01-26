use std::env;
use std::fs;
use std::path::PathBuf;
use std::time::Instant;

use treecrdt_core::{Lamport, LamportClock, MemoryStorage, NodeId, ReplicaId, TreeCrdt};

const CI_CONFIG: &[(u64, u64)] = &[
    (100, 5),
    (1_000, 1),
    (10_000, 1),
];

const LOCAL_CONFIG: &[(u64, u64)] = &[
    (1, 1),
    (10, 1),
    (100, 1),
    (1_000, 1),
    (10_000, 1),
];

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
    #[serde(skip_serializing_if = "Option::is_none")]
    iterations: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    avg_duration_ms: Option<f64>,
}

fn hex_id(n: u64) -> NodeId {
    let mut bytes = [0u8; 16];
    bytes[8..].copy_from_slice(&n.to_be_bytes());
    NodeId(u128::from_be_bytes(bytes))
}

fn is_ci() -> bool {
    env::var("CI").map(|v| v == "true").unwrap_or(false)
}

fn run_benchmark(replica: &ReplicaId, count: u64) -> f64 {
    let storage = MemoryStorage::default();
    let mut tree = TreeCrdt::new(replica.clone(), storage, LamportClock::default()).unwrap();

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
    start.elapsed().as_secs_f64() * 1000.0
}

fn main() {
    let is_ci_env = is_ci();
    let config: &[(u64, u64)] = if is_ci_env {
        CI_CONFIG
    } else {
        LOCAL_CONFIG
    };

    let mut out_dir: Option<PathBuf> = None;
    let mut custom_config: Option<Vec<(u64, u64)>> = None;
    for arg in env::args().skip(1) {
        if let Some(val) = arg.strip_prefix("--count=") {
            let count = val.parse().unwrap_or(500);
            custom_config = Some(vec![(count, 1)]);
        } else if let Some(val) = arg.strip_prefix("--counts=") {
            let parsed: Vec<(u64, u64)> = val
                .split(',')
                .filter_map(|s| s.trim().parse::<u64>().ok())
                .map(|c| (c, 1))
                .collect();
            if !parsed.is_empty() {
                custom_config = Some(parsed);
            }
        } else if let Some(val) = arg.strip_prefix("--out-dir=") {
            out_dir = Some(PathBuf::from(val));
        }
    }

    let config = custom_config.as_deref().unwrap_or(config);
    let out_dir = out_dir.unwrap_or_else(|| PathBuf::from("benchmarks/core"));
    fs::create_dir_all(&out_dir).expect("mkdirs");

    let replica = ReplicaId::new(b"core");
    for &(count, iterations) in config {
        let (duration_ms, iterations_opt, avg_duration_ms) = if iterations > 1 {
            // Run multiple iterations and average
            let mut durations = Vec::new();
            for _ in 0..iterations {
                durations.push(run_benchmark(&replica, count));
            }
            let avg = durations.iter().sum::<f64>() / durations.len() as f64;
            (avg, Some(iterations), Some(avg))
        } else {
            // Single run
            let duration = run_benchmark(&replica, count);
            (duration, None, None)
        };

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
                iterations: iterations_opt,
                avg_duration_ms,
            },
            source_file: Some(out_path.display().to_string()),
        };

        let json = serde_json::to_string_pretty(&output).expect("serialize");
        fs::write(&out_path, &json).expect("write output");
        println!("{}", json);
    }
}

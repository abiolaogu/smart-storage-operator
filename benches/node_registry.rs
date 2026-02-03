//! Benchmark for the sharded node registry
//!
//! Target: 10K registry updates/sec

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use smart_storage_operator::hardware::registry::{NodeRegistry, NodeId};
use smart_storage_operator::crd::StorageNodeStatus;
use std::sync::Arc;

fn bench_register_nodes(c: &mut Criterion) {
    let mut group = c.benchmark_group("node_registry");
    group.throughput(Throughput::Elements(1));

    group.bench_function("register_single_node", |b| {
        let registry = NodeRegistry::new();
        let mut counter = 0u64;

        b.iter(|| {
            counter += 1;
            let node_id = format!("node-{}", counter);
            let status = StorageNodeStatus::default();
            let _ = registry.register(
                black_box(&node_id),
                format!("host-{}.local", counter),
                status,
            );
        });
    });

    group.finish();
}

fn bench_update_status(c: &mut Criterion) {
    let mut group = c.benchmark_group("node_registry");
    group.throughput(Throughput::Elements(1));

    // Pre-register nodes
    let registry = NodeRegistry::new();
    for i in 0..1000 {
        let node_id = format!("node-{:04}", i);
        let status = StorageNodeStatus::default();
        let _ = registry.register(&node_id, format!("host-{}.local", i), status);
    }

    group.bench_function("update_status", |b| {
        let mut counter = 0u64;
        b.iter(|| {
            counter += 1;
            let node_id = format!("node-{:04}", counter % 1000);
            let status = StorageNodeStatus::default();
            let _ = registry.update_status(black_box(&node_id), status);
        });
    });

    group.finish();
}

fn bench_concurrent_updates(c: &mut Criterion) {
    let mut group = c.benchmark_group("node_registry");
    group.throughput(Throughput::Elements(100));

    // Pre-register nodes
    let registry = NodeRegistry::new();
    for i in 0..1000 {
        let node_id = format!("node-{:04}", i);
        let status = StorageNodeStatus::default();
        let _ = registry.register(&node_id, format!("host-{}.local", i), status);
    }

    let rt = tokio::runtime::Runtime::new().unwrap();

    group.bench_function("concurrent_100_updates", |b| {
        b.iter(|| {
            rt.block_on(async {
                let mut handles = Vec::new();
                for i in 0..100 {
                    let reg = registry.clone();
                    handles.push(tokio::spawn(async move {
                        let node_id = format!("node-{:04}", i % 1000);
                        let status = StorageNodeStatus::default();
                        let _ = reg.update_status(&node_id, status);
                    }));
                }
                for handle in handles {
                    let _ = handle.await;
                }
            });
        });
    });

    group.finish();
}

fn bench_drive_metrics_update(c: &mut Criterion) {
    let mut group = c.benchmark_group("drive_metrics");
    group.throughput(Throughput::Elements(1));

    // Pre-register nodes with drives
    let registry = NodeRegistry::new();
    let mut status = StorageNodeStatus::default();
    status.drives = vec![
        smart_storage_operator::crd::DriveStatus {
            id: "nvme0n1".to_string(),
            device_path: "/dev/nvme0n1".to_string(),
            drive_type: smart_storage_operator::crd::DriveType::Nvme,
            model: "Test Drive".to_string(),
            serial: "TEST123".to_string(),
            firmware: "1.0".to_string(),
            capacity_bytes: 1_000_000_000_000,
            used_bytes: 0,
            namespaces: vec![],
            classification: Default::default(),
            metrics: None,
            smart: None,
            pool_ref: None,
            healthy: true,
        },
    ];
    let _ = registry.register("node-001", "host.local".to_string(), status);

    group.bench_function("update_drive_metrics", |b| {
        b.iter(|| {
            let _ = registry.update_drive_metrics(
                black_box("node-001"),
                black_box("nvme0n1"),
                black_box(50000),
                black_box(500_000_000),
                black_box(150),
                black_box(75.0),
                black_box(42),
                black_box(5),
            );
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_register_nodes,
    bench_update_status,
    bench_concurrent_updates,
    bench_drive_metrics_update,
);
criterion_main!(benches);

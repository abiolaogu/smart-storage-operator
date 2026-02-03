# Smart Storage Operator

[![Rust](https://img.shields.io/badge/Rust-1.76+-orange?logo=rust)](https://www.rust-lang.org)
[![Kubernetes](https://img.shields.io/badge/Kubernetes-1.29+-326CE5?logo=kubernetes)](https://kubernetes.io)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

**Unified Control Plane Orchestrator for Block/File/Object Storage** — A high-performance Kubernetes operator that provides unified storage management across multiple backends and platforms.

## Overview

Smart Storage Operator is a "Ceph-killer" architecture that orchestrates:
- **Block Storage** via [OpenEBS Mayastor](https://mayastor.gitbook.io/)
- **File Storage** via [SeaweedFS](https://github.com/seaweedfs/seaweedfs)
- **Object Storage** via [RustFS](https://github.com/rustfs/rustfs) (S3-compatible)

With platform adapters for:
- **Harvester HCI** (Longhorn CSI)
- **OpenStack** (Cinder/Manila/Swift)

## Key Features

| Feature | Description |
|---------|-------------|
| **Unified API** | Single REST API for Block/File/Object provisioning |
| **Hardware-Aware Allocation** | Automatic NVMe/SSD/HDD classification and tiering |
| **High Performance** | 256-way sharded registry achieving 4M+ ops/sec |
| **Platform Adapters** | Native integration with Harvester HCI and OpenStack |
| **DOD Architecture** | Data-Oriented Design with cache-line aligned structures |

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     Unified Control Plane Orchestrator                       │
├─────────────────────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────────────────┐  │
│  │   Unified API   │  │    Hardware     │  │      Allocation             │  │
│  │   (gRPC/REST)   │  │    Discovery    │  │      Engine                 │  │
│  └────────┬────────┘  └────────┬────────┘  └─────────────┬───────────────┘  │
│           │                    │                         │                   │
│           └────────────────────┼─────────────────────────┘                   │
│                                │                                             │
│                    ┌───────────┴───────────┐                                │
│                    │  Node Registry (DOD)  │                                │
│                    │  (256-way sharded)    │                                │
│                    └───────────────────────┘                                │
├─────────────────────────────────────────────────────────────────────────────┤
│                          Storage Backends                                    │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────────────────┐  │
│  │     Block       │  │      File       │  │         Object              │  │
│  │   (Mayastor)    │  │   (SeaweedFS)   │  │        (RustFS)             │  │
│  └─────────────────┘  └─────────────────┘  └─────────────────────────────┘  │
├─────────────────────────────────────────────────────────────────────────────┤
│                        Platform Adapters                                     │
│  ┌─────────────────────────────┐  ┌─────────────────────────────────────┐   │
│  │      Harvester HCI          │  │           OpenStack                 │   │
│  │   (Longhorn CSI)            │  │   (Cinder/Manila/Swift)             │   │
│  └─────────────────────────────┘  └─────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Quick Start

### Prerequisites

- Rust 1.76+ (`rustup update stable`)
- Kubernetes 1.29+
- OpenEBS Mayastor (for block storage)

### Build & Run

```bash
# Build release binary
cargo build --release

# Run tests
cargo test

# Run benchmarks
cargo bench --bench node_registry

# Run locally
cargo run -- --api-addr 0.0.0.0:8090
```

### Deploy to Kubernetes

```bash
# Build container image
docker build -t smart-storage-operator:latest .

# Install CRDs
kubectl apply -f deploy/crds/

# Deploy operator
kubectl apply -f deploy/operator.yaml
```

## REST API

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/v1/storage` | POST | Provision storage |
| `/v1/storage/:id` | GET | Get storage info |
| `/v1/storage/:id` | DELETE | Delete storage |
| `/v1/nodes` | GET | List nodes with hardware |
| `/v1/nodes/:name` | GET | Get node details |
| `/v1/nodes/:name/classify` | POST | Classify node drives |
| `/v1/pools` | GET | List unified pools |
| `/v1/capacity` | GET | Cluster capacity summary |
| `/health` | GET | Health check |

### Example: Provision Block Storage

```bash
curl -X POST http://localhost:8090/v1/storage \
  -H "Content-Type: application/json" \
  -d '{
    "name": "my-volume",
    "storage_type": "block",
    "capacity_bytes": 107374182400,
    "tier": "hot"
  }'
```

## Custom Resource Definitions

### UnifiedStorageClass

```yaml
apiVersion: storage.billyronks.io/v1
kind: UnifiedStorageClass
metadata:
  name: enterprise-tiered
spec:
  storageType: block    # block | file | object | auto
  tier: hot             # hot | warm | cold | auto
  capacity:
    requested: "100Gi"
    maxIOPS: 50000
  redundancy:
    type: replication
    replicationFactor: 3
  hardwarePreference:
    driveType: nvme     # nvme | ssd | hdd | auto
    minDriveCount: 3
  platformOverrides:
    harvester:
      storageClass: "longhorn-nvme"
    openstack:
      volumeType: "high-iops"
```

### StorageNode

```yaml
apiVersion: storage.billyronks.io/v1
kind: StorageNode
metadata:
  name: node-001
spec:
  nodeName: worker-1
  autoDiscover: true
status:
  phase: Ready
  drives:
    - id: "nvme0n1"
      driveType: nvme
      capacityBytes: 3840000000000
      classification:
        tier: fastNvme
        score: 95
        suitableFor: [block, cache]
```

### UnifiedPool

```yaml
apiVersion: storage.billyronks.io/v1
kind: UnifiedPool
metadata:
  name: hot-nvme-pool
spec:
  poolType: block
  backend:
    type: mayastor
  driveSelector:
    driveTypes: [nvme]
    minScore: 80
  capacity:
    targetBytes: 10000000000000
```

## Project Structure

```
src/
├── main.rs                      # Entry point, CLI, servers
├── lib.rs                       # Library exports
├── error.rs                     # Error types
├── domain/
│   └── ports.rs                 # Core traits (hexagonal architecture)
├── crd/
│   ├── unified_storage.rs       # UnifiedStorageClass CRD
│   ├── storage_node.rs          # StorageNode CRD
│   └── unified_pool.rs          # UnifiedPool CRD
├── controlplane/
│   ├── orchestrator.rs          # Main orchestrator
│   ├── api/
│   │   ├── server.rs            # API server setup
│   │   └── rest.rs              # REST handlers
│   ├── backends/
│   │   ├── mayastor.rs          # Block storage adapter
│   │   ├── seaweedfs.rs         # File storage adapter
│   │   └── rustfs.rs            # Object storage adapter
│   └── platform/
│       ├── harvester.rs         # Harvester HCI adapter
│       └── openstack.rs         # OpenStack adapter
└── hardware/
    ├── registry/
    │   ├── node_registry.rs     # 256-way sharded registry
    │   └── events.rs            # Registry events
    ├── discovery/
    │   ├── scanner.rs           # Block device scanner
    │   ├── nvme.rs              # NVMe discovery
    │   └── sas_sata.rs          # SAS/SATA discovery
    ├── classification/
    │   ├── classifier.rs        # Device classifier
    │   └── fingerprint.rs       # Model fingerprinting
    └── allocation/
        ├── allocator.rs         # Drive allocator
        ├── policy.rs            # Allocation policies
        └── placement.rs         # Placement engine
```

## Performance

Benchmark results (Apple M-series, single thread):

| Operation | Throughput | Latency |
|-----------|------------|---------|
| Node Registration | 1.4M ops/sec | ~700 ns |
| Status Update | 4.0M ops/sec | ~250 ns |
| Drive Metrics Update | 2.0M ops/sec | ~490 ns |
| 100 Concurrent Updates | 1.8M ops/sec | ~55 µs |

The 256-way sharded registry with cache-line aligned structures exceeds the 10K ops/sec target by **400x**.

## Configuration

### Command-Line Options

```
smart-storage-operator [OPTIONS]

OPTIONS:
    --api-addr <ADDR>           REST API address [default: 0.0.0.0:8090]
    --grpc-addr <ADDR>          gRPC API address [default: 0.0.0.0:8091]
    --health-addr <ADDR>        Health endpoint [default: 0.0.0.0:8081]
    --metrics-addr <ADDR>       Metrics endpoint [default: 0.0.0.0:8080]
    --mayastor-namespace <NS>   Mayastor namespace [default: mayastor]
    --auto-discover             Enable hardware auto-discovery
    --discover-interval <SECS>  Discovery interval [default: 300]
    --log-level <LEVEL>         Log level [default: info]
    --log-json                  Output logs as JSON
    --standalone                Run without Kubernetes
```

### Environment Variables

```bash
API_ADDR=0.0.0.0:8090
GRPC_ADDR=0.0.0.0:8091
HEALTH_ADDR=0.0.0.0:8081
METRICS_ADDR=0.0.0.0:8080
MAYASTOR_NAMESPACE=mayastor
AUTO_DISCOVER=true
DISCOVER_INTERVAL=300
LOG_LEVEL=info
LOG_JSON=false
```

## Hardware Classification

Drives are automatically classified into performance tiers:

| Tier | Description | Use Case |
|------|-------------|----------|
| **UltraFast** | Intel Optane, PMem | Cache, hot block storage |
| **FastNvme** | High-performance NVMe | Block storage, databases |
| **StandardSsd** | SATA/SAS SSDs | Mixed workloads |
| **Hdd** | Spinning disks | Object storage, archives |

Classification considers:
- Device type (NVMe/SSD/HDD)
- Model fingerprinting (known enterprise models)
- ZNS support (for object-optimized storage)
- SMART health data
- Capacity tier (Small/Medium/Large)

## Development

```bash
# Format code
cargo fmt

# Run lints
cargo clippy

# Run tests
cargo test

# Run benchmarks
cargo bench

# Generate docs
cargo doc --open
```

## License

Apache License 2.0

---

**BillyRonks Global Limited** | Built with Rust

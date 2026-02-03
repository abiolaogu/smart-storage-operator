# Smart Storage Operator (Rust)

[![Rust](https://img.shields.io/badge/Rust-1.76+-orange?logo=rust)](https://www.rust-lang.org)
[![Kubernetes](https://img.shields.io/badge/Kubernetes-1.28+-326CE5?logo=kubernetes)](https://kubernetes.io)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

**Intelligent storage tiering for OpenEBS Mayastor** — written in Rust for maximum performance and safety.

## Why Rust?

| Aspect | Benefit |
|--------|---------|
| **Memory Safety** | Zero-cost abstractions, no garbage collector pauses |
| **Performance** | Native speed, minimal resource footprint (~10MB binary) |
| **Reliability** | Compile-time guarantees prevent entire classes of bugs |
| **Mayastor Alignment** | Same language as Mayastor itself (potential for deeper integration) |
| **Type Safety** | Strong typing catches errors at compile time, not runtime |

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     Smart Storage Operator                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐       │
│  │   Metrics    │    │  Controller  │    │   Migrator   │       │
│  │   Watcher    │───▶│    (Brain)   │───▶│   (Hands)    │       │
│  │   (Eyes)     │    │  Reconciler  │    │              │       │
│  └──────────────┘    └──────────────┘    └──────────────┘       │
│         │                   │                    │               │
│         ▼                   ▼                    ▼               │
│    Prometheus         StoragePolicy       Mayastor CRDs          │
│     (metrics)            (CRD)            (volumes, pools)       │
└─────────────────────────────────────────────────────────────────┘
```

## Quick Start

### Prerequisites

- Rust 1.76+ (`rustup update stable`)
- Kubernetes 1.28+
- [OpenEBS Mayastor](https://mayastor.gitbook.io/)
- Prometheus with Mayastor metrics

### Build & Run

```bash
# Build release binary
cargo build --release

# Run locally (with port-forwarded Prometheus)
kubectl port-forward svc/prometheus 9090:9090 -n monitoring &
cargo run -- --prometheus-url=http://localhost:9090

# Build & push Docker image
make docker-push IMG=myregistry/smart-storage-operator TAG=v1.0.0
```

### Deploy to Kubernetes

```bash
# Install CRDs
kubectl apply -f deploy/crds/

# Deploy operator
kubectl apply -f deploy/operator.yaml

# Create a policy
kubectl apply -f deploy/examples/storagepolicy-examples.yaml

# Check status
kubectl get storagepolicies
```

## Project Structure

```
src/
├── main.rs              # Entry point, CLI, servers
├── lib.rs               # Library exports
├── error.rs             # Error types (thiserror)
├── crd/
│   ├── mod.rs           # CRD exports
│   ├── storage_policy.rs # StoragePolicy CRD (kube-derive)
│   └── mayastor.rs      # Mayastor CRD mirrors
├── metrics/
│   ├── mod.rs           # Metrics exports
│   └── watcher.rs       # Prometheus query client
├── migrator/
│   ├── mod.rs           # Migrator exports
│   └── engine.rs        # Safe migration state machine
└── controller/
    ├── mod.rs           # Controller exports
    └── storage_policy.rs # Reconciliation logic
```

## Key Dependencies

| Crate | Purpose |
|-------|---------|
| `kube` | Kubernetes client & controller runtime |
| `k8s-openapi` | Kubernetes API types |
| `tokio` | Async runtime |
| `reqwest` | HTTP client (Prometheus) |
| `serde` | Serialization |
| `thiserror` | Error handling |
| `tracing` | Logging & observability |
| `clap` | CLI argument parsing |
| `prometheus` | Metrics exposition |

## Configuration

### Command-Line Options

```
USAGE:
    smart-storage-operator [OPTIONS]

OPTIONS:
    --prometheus-url <URL>
        Prometheus server URL [default: http://prometheus.monitoring.svc.cluster.local:9090]

    --max-concurrent-migrations <N>
        Maximum parallel migrations [default: 2]

    --migration-timeout-minutes <N>
        Timeout per migration [default: 30]

    --dry-run
        Log migrations without executing

    --preservation-mode
        Never remove old replicas (safest)

    --log-level <LEVEL>
        trace, debug, info, warn, error [default: info]

    --log-json
        Output logs as JSON

    --metrics-addr <ADDR>
        Metrics endpoint [default: 0.0.0.0:8080]

    --health-addr <ADDR>
        Health endpoint [default: 0.0.0.0:8081]
```

### Environment Variables

All CLI options can be set via environment variables:

```bash
PROMETHEUS_URL=http://prometheus:9090
MAX_CONCURRENT_MIGRATIONS=2
MIGRATION_TIMEOUT_MINUTES=30
DRY_RUN=true
LOG_LEVEL=debug
```

## StoragePolicy CRD

```yaml
apiVersion: storage.billyronks.io/v1
kind: StoragePolicy
metadata:
  name: production-tiering
spec:
  highWatermarkIOPS: 5000    # → NVMe when exceeded
  lowWatermarkIOPS: 500      # → SATA when below
  samplingWindow: "1h"       # IOPS averaging window
  cooldownPeriod: "24h"      # Anti-thrashing delay
  storageClassName: "mayastor"
  
  nvmePoolSelector:
    matchLabels:
      storage-tier: hot
  
  sataPoolSelector:
    matchLabels:
      storage-tier: cold
  
  maxConcurrentMigrations: 2
  migrationTimeout: "30m"
  enabled: true
  dryRun: false
```

## Safety Guarantees

The Rust implementation provides additional safety through:

### Compile-Time Safety
- **Ownership model** prevents data races
- **Result types** force explicit error handling
- **Lifetime annotations** prevent use-after-free

### Migration Safety (Same as Go version)
```
1. ANALYZE    → Verify current state
2. SCALE UP   → Add replica on target pool
3. WAIT SYNC  → Poll until Online AND Synced
4. SCALE DOWN → Remove old replica ONLY if sync succeeded
```

Data is **never lost** because:
- Old replica preserved if sync fails
- Old replica preserved on timeout
- Old replica preserved on any error
- Preservation mode option never removes old replicas

## Observability

### Metrics (`:8080/metrics`)

```
storage_operator_reconcile_total
storage_operator_migrations_total{status="success|failed|aborted"}
storage_operator_active_migrations
```

### Health Endpoints (`:8081`)

- `/healthz` - Liveness probe
- `/readyz` - Readiness probe

### Logs

```bash
# Stream logs
kubectl logs -n smart-storage-system -l app.kubernetes.io/name=smart-storage-operator -f

# Debug level
RUST_LOG=debug ./smart-storage-operator
```

## Development

### Prerequisites

```bash
# Install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Add musl target for static builds
rustup target add x86_64-unknown-linux-musl
```

### Commands

```bash
# Format code
cargo fmt

# Run lints
cargo clippy

# Run tests
cargo test

# Generate docs
cargo doc --open

# Build release
cargo build --release
```

### Testing Locally

```bash
# 1. Port-forward Prometheus
kubectl port-forward svc/prometheus 9090:9090 -n monitoring &

# 2. Run with debug logging
RUST_LOG=debug cargo run -- \
    --prometheus-url=http://localhost:9090 \
    --dry-run
```

## Comparison: Rust vs Go

| Aspect | Rust | Go |
|--------|------|-----|
| Binary Size | ~10MB | ~30MB |
| Memory Usage | Lower (no GC) | Higher (GC overhead) |
| Startup Time | Instant | ~100ms |
| Compile Time | Slower | Faster |
| Error Handling | `Result<T, E>` (explicit) | `error` (easy to ignore) |
| Concurrency | Ownership-based safety | Goroutines + channels |
| Ecosystem | Growing (kube-rs) | Mature (controller-runtime) |

**When to choose Rust:**
- Resource-constrained environments
- Maximum reliability requirements
- Deep Mayastor integration plans

**When to choose Go:**
- Faster iteration
- Larger team familiarity
- Simpler operator logic

## License

Apache License 2.0

---

**BillyRonks Global Limited** | Built with 🦀

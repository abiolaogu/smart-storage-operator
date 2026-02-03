//! Smart Storage Operator - Unified Control Plane
//!
//! A Kubernetes operator providing unified storage management across
//! Block (Mayastor), File (SeaweedFS), and Object (RustFS) storage backends.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────────┐
//! │                     Unified Control Plane Orchestrator                       │
//! ├─────────────────────────────────────────────────────────────────────────────┤
//! │  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────────────────┐  │
//! │  │   Unified API   │  │    Hardware     │  │      Allocation             │  │
//! │  │   (gRPC/REST)   │  │    Discovery    │  │      Engine                 │  │
//! │  └────────┬────────┘  └────────┬────────┘  └─────────────┬───────────────┘  │
//! │           │                    │                         │                   │
//! │           └────────────────────┼─────────────────────────┘                   │
//! │                                │                                             │
//! │                    ┌───────────┴───────────┐                                │
//! │                    │  Node Registry (DOD)  │                                │
//! │                    │  (256-way sharded)    │                                │
//! │                    └───────────────────────┘                                │
//! ├─────────────────────────────────────────────────────────────────────────────┤
//! │                          Storage Backends                                    │
//! │  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────────────────┐  │
//! │  │     Block       │  │      File       │  │         Object              │  │
//! │  │   (Mayastor)    │  │   (SeaweedFS)   │  │        (RustFS)             │  │
//! │  └─────────────────┘  └─────────────────┘  └─────────────────────────────┘  │
//! ├─────────────────────────────────────────────────────────────────────────────┤
//! │                        Platform Adapters                                     │
//! │  ┌─────────────────────────────┐  ┌─────────────────────────────────────┐   │
//! │  │      Harvester HCI          │  │           OpenStack                 │   │
//! │  │   (Longhorn CSI)            │  │   (Cinder/Manila/Swift)             │   │
//! │  └─────────────────────────────┘  └─────────────────────────────────────┘   │
//! └─────────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Modules
//!
//! - [`controlplane`]: Unified control plane orchestrator and APIs
//! - [`hardware`]: Hardware discovery, classification, and allocation
//! - [`crd`]: Custom Resource Definitions
//! - [`domain`]: Core domain types and traits
//! - [`error`]: Error types and handling

pub mod controlplane;
pub mod crd;
pub mod domain;
pub mod error;
pub mod hardware;

// Re-export commonly used types
pub use controlplane::{
    Orchestrator, OrchestratorConfig, OrchestratorStatus,
    ApiServer, ApiServerConfig,
    BackendConfig, BackendFactory,
    PlatformConfig, PlatformFactory,
};

pub use crd::{
    UnifiedStorageClass, UnifiedStorageClassSpec, UnifiedStorageClassStatus,
    StorageNode, StorageNodeSpec, StorageNodeStatus,
    UnifiedPool, UnifiedPoolSpec, UnifiedPoolStatus,
    BackendType, UnifiedStorageType, UnifiedTier,
    DriveTier, DriveType, WorkloadSuitability,
};

pub use domain::ports::{
    StorageType, StorageTier, DriveInfo, NodeHardwareInfo,
    ProvisionRequest, ProvisionResponse,
    StorageProvisioner, HardwareDiscoverer, PlatformAdapter, AllocationEngine,
};

pub use error::{Error, Result, ErrorAction};

pub use hardware::{
    NodeRegistry, NodeId, DriveMetrics, GlobalStatsSnapshot,
    HardwareScanner, ScannerConfig,
    DeviceClassifier, DeviceClassification,
    DriveAllocator, AllocationPolicy, PlacementPolicy,
};

/// Library version
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Library name
pub const NAME: &str = env!("CARGO_PKG_NAME");

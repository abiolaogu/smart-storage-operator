//! Custom Resource Definitions for the Unified Control Plane
//!
//! This module contains all CRD types:
//! - UnifiedStorageClass: Storage class with cross-platform support
//! - StorageNode: Node hardware inventory and status
//! - UnifiedPool: Storage pool spanning multiple backends

pub mod unified_storage;
pub mod storage_node;
pub mod unified_pool;

pub use unified_storage::*;
pub use storage_node::*;
pub use unified_pool::*;

// Re-export common types for convenience
pub use chrono::{DateTime, Utc};
pub use std::collections::BTreeMap;

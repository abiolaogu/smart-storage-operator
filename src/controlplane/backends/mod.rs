//! Storage Backend Adapters
//!
//! Provides adapters for different storage backends:
//! - Mayastor: Block storage
//! - SeaweedFS: File storage
//! - RustFS: Object storage

pub mod mayastor;
pub mod seaweedfs;
pub mod rustfs;

pub use mayastor::*;
pub use seaweedfs::*;
pub use rustfs::*;

use crate::domain::ports::{StorageProvisioner, StorageType};
use crate::error::Result;
use std::sync::Arc;

/// Factory for creating storage backend adapters
pub struct BackendFactory;

impl BackendFactory {
    /// Create a backend adapter by name
    pub fn create(name: &str, config: BackendConfig) -> Result<Arc<dyn StorageProvisioner>> {
        match name.to_lowercase().as_str() {
            "mayastor" | "block" => Ok(Arc::new(MayastorAdapter::new(config.mayastor))),
            "seaweedfs" | "file" => Ok(Arc::new(SeaweedFSAdapter::new(config.seaweedfs))),
            "rustfs" | "object" => Ok(Arc::new(RustFSAdapter::new(config.rustfs))),
            _ => Err(crate::error::Error::BackendUnavailable {
                backend: name.to_string(),
            }),
        }
    }

    /// Create a backend for a storage type
    pub fn for_storage_type(
        storage_type: StorageType,
        config: BackendConfig,
    ) -> Result<Arc<dyn StorageProvisioner>> {
        match storage_type {
            StorageType::Block => Self::create("mayastor", config),
            StorageType::File => Self::create("seaweedfs", config),
            StorageType::Object => Self::create("rustfs", config),
        }
    }
}

/// Combined backend configuration
#[derive(Debug, Clone, Default)]
pub struct BackendConfig {
    pub mayastor: MayastorConfig,
    pub seaweedfs: SeaweedFSConfig,
    pub rustfs: RustFSConfig,
}

//! Platform Adapters
//!
//! Provides platform-specific adapters for:
//! - Harvester HCI (Longhorn CSI)
//! - OpenStack (Cinder, Manila, Swift)

pub mod harvester;
pub mod openstack;

pub use harvester::*;
pub use openstack::*;

use crate::domain::ports::{Platform, PlatformAdapter, PlatformStorageClass, StorageTier, StorageType};
use crate::error::Result;
use std::sync::Arc;

/// Factory for creating platform adapters
pub struct PlatformFactory;

impl PlatformFactory {
    /// Create a platform adapter by name
    pub fn create(platform: Platform, config: PlatformConfig) -> Result<Arc<dyn PlatformAdapter>> {
        match platform {
            Platform::Harvester => Ok(Arc::new(HarvesterAdapter::new(config.harvester))),
            Platform::OpenStack => Ok(Arc::new(OpenStackAdapter::new(config.openstack))),
            Platform::Kubernetes => {
                // Default to Harvester for basic Kubernetes
                Ok(Arc::new(HarvesterAdapter::new(config.harvester)))
            }
        }
    }
}

/// Combined platform configuration
#[derive(Debug, Clone, Default)]
pub struct PlatformConfig {
    pub harvester: HarvesterConfig,
    pub openstack: OpenStackConfig,
}

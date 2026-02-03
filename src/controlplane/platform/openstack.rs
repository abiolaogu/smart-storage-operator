//! OpenStack Platform Adapter
//!
//! Provides integration with OpenStack services:
//! - Cinder: Block storage
//! - Manila: File storage (shares)
//! - Swift: Object storage

use crate::domain::ports::{Platform, PlatformAdapter, PlatformStorageClass, StorageTier, StorageType};
use crate::error::{Error, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

// =============================================================================
// Configuration
// =============================================================================

/// Configuration for OpenStack adapter
#[derive(Debug, Clone)]
pub struct OpenStackConfig {
    /// Keystone auth URL
    pub auth_url: String,
    /// Username
    pub username: String,
    /// Password (should use secrets in production)
    pub password: String,
    /// Project name
    pub project_name: String,
    /// User domain name
    pub user_domain_name: String,
    /// Project domain name
    pub project_domain_name: String,
    /// Region name
    pub region: String,
    /// Default availability zone
    pub availability_zone: Option<String>,
}

impl Default for OpenStackConfig {
    fn default() -> Self {
        Self {
            auth_url: "http://keystone:5000/v3".to_string(),
            username: "admin".to_string(),
            password: String::new(),
            project_name: "admin".to_string(),
            user_domain_name: "Default".to_string(),
            project_domain_name: "Default".to_string(),
            region: "RegionOne".to_string(),
            availability_zone: None,
        }
    }
}

// =============================================================================
// OpenStack Resource Types
// =============================================================================

/// Cinder volume state
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CinderVolume {
    id: String,
    name: String,
    size_gb: u64,
    volume_type: String,
    availability_zone: String,
    status: String,
}

/// Manila share state
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ManilaShare {
    id: String,
    name: String,
    size_gb: u64,
    share_type: String,
    share_protocol: String,
    export_location: Option<String>,
    status: String,
}

/// Swift container state
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SwiftContainer {
    name: String,
    storage_policy: String,
    object_count: u64,
    bytes_used: u64,
}

// =============================================================================
// Volume Types
// =============================================================================

/// OpenStack volume types (storage classes)
#[derive(Debug, Clone, Serialize, Deserialize)]
struct VolumeType {
    name: String,
    description: String,
    is_public: bool,
    extra_specs: BTreeMap<String, String>,
}

// =============================================================================
// OpenStack Adapter
// =============================================================================

/// Adapter for OpenStack platform
pub struct OpenStackAdapter {
    config: OpenStackConfig,
    /// Cinder volumes
    cinder_volumes: RwLock<BTreeMap<String, CinderVolume>>,
    /// Manila shares
    manila_shares: RwLock<BTreeMap<String, ManilaShare>>,
    /// Swift containers
    swift_containers: RwLock<BTreeMap<String, SwiftContainer>>,
    /// Volume types (storage classes)
    volume_types: RwLock<BTreeMap<String, VolumeType>>,
}

impl OpenStackAdapter {
    /// Create a new OpenStack adapter
    pub fn new(config: OpenStackConfig) -> Self {
        Self {
            config,
            cinder_volumes: RwLock::new(BTreeMap::new()),
            manila_shares: RwLock::new(BTreeMap::new()),
            swift_containers: RwLock::new(BTreeMap::new()),
            volume_types: RwLock::new(BTreeMap::new()),
        }
    }

    /// Create a Cinder volume
    async fn create_cinder_volume(
        &self,
        name: &str,
        capacity_bytes: u64,
        volume_type: &str,
    ) -> Result<String> {
        let volume_id = format!("vol-{}", generate_id());
        let size_gb = (capacity_bytes + (1024 * 1024 * 1024 - 1)) / (1024 * 1024 * 1024); // Round up

        info!(
            "Creating Cinder volume: {} ({} GB, type: {})",
            name, size_gb, volume_type
        );

        let volume = CinderVolume {
            id: volume_id.clone(),
            name: name.to_string(),
            size_gb,
            volume_type: volume_type.to_string(),
            availability_zone: self.config.availability_zone.clone().unwrap_or_else(|| "nova".to_string()),
            status: "available".to_string(),
        };

        self.cinder_volumes.write().await.insert(volume_id.clone(), volume);

        Ok(volume_id)
    }

    /// Create a Manila share
    async fn create_manila_share(
        &self,
        name: &str,
        capacity_bytes: u64,
        share_type: &str,
    ) -> Result<String> {
        let share_id = format!("share-{}", generate_id());
        let size_gb = (capacity_bytes + (1024 * 1024 * 1024 - 1)) / (1024 * 1024 * 1024);

        info!(
            "Creating Manila share: {} ({} GB, type: {})",
            name, size_gb, share_type
        );

        let share = ManilaShare {
            id: share_id.clone(),
            name: name.to_string(),
            size_gb,
            share_type: share_type.to_string(),
            share_protocol: "NFS".to_string(),
            export_location: Some(format!("10.0.0.1:/shares/{}", share_id)),
            status: "available".to_string(),
        };

        self.manila_shares.write().await.insert(share_id.clone(), share);

        Ok(share_id)
    }

    /// Create a Swift container
    async fn create_swift_container(
        &self,
        name: &str,
        storage_policy: &str,
    ) -> Result<String> {
        info!(
            "Creating Swift container: {} (policy: {})",
            name, storage_policy
        );

        // Container name is the ID in Swift
        let container = SwiftContainer {
            name: name.to_string(),
            storage_policy: storage_policy.to_string(),
            object_count: 0,
            bytes_used: 0,
        };

        self.swift_containers.write().await.insert(name.to_string(), container);

        Ok(name.to_string())
    }

    /// Delete a Cinder volume
    async fn delete_cinder_volume(&self, volume_id: &str) -> Result<()> {
        let mut volumes = self.cinder_volumes.write().await;
        if volumes.remove(volume_id).is_some() {
            Ok(())
        } else {
            Err(Error::ResourceNotFound {
                kind: "CinderVolume".into(),
                name: volume_id.into(),
            })
        }
    }

    /// Delete a Manila share
    async fn delete_manila_share(&self, share_id: &str) -> Result<()> {
        let mut shares = self.manila_shares.write().await;
        if shares.remove(share_id).is_some() {
            Ok(())
        } else {
            Err(Error::ResourceNotFound {
                kind: "ManilaShare".into(),
                name: share_id.into(),
            })
        }
    }

    /// Delete a Swift container
    async fn delete_swift_container(&self, name: &str) -> Result<()> {
        let mut containers = self.swift_containers.write().await;
        if let Some(container) = containers.get(name) {
            if container.object_count > 0 {
                return Err(Error::OpenStackApi {
                    service: "swift".into(),
                    reason: "Container is not empty".into(),
                });
            }
        }

        if containers.remove(name).is_some() {
            Ok(())
        } else {
            Err(Error::ResourceNotFound {
                kind: "SwiftContainer".into(),
                name: name.into(),
            })
        }
    }

    /// Get volume type name for storage tier
    fn get_volume_type(&self, tier: StorageTier) -> &'static str {
        match tier {
            StorageTier::Hot => "high-iops",
            StorageTier::Warm => "standard",
            StorageTier::Cold => "archive",
        }
    }

    /// Get share type name for storage tier
    fn get_share_type(&self, tier: StorageTier) -> &'static str {
        match tier {
            StorageTier::Hot => "high-performance",
            StorageTier::Warm => "general",
            StorageTier::Cold => "backup",
        }
    }

    /// Get Swift storage policy for tier
    fn get_storage_policy(&self, tier: StorageTier) -> &'static str {
        match tier {
            StorageTier::Hot => "Policy-0",
            StorageTier::Warm => "Policy-0",
            StorageTier::Cold => "erasure-coded",
        }
    }
}

#[async_trait]
impl PlatformAdapter for OpenStackAdapter {
    fn platform(&self) -> Platform {
        Platform::OpenStack
    }

    async fn create_storage_class(
        &self,
        name: &str,
        storage_type: StorageType,
        tier: StorageTier,
        params: BTreeMap<String, String>,
    ) -> Result<PlatformStorageClass> {
        info!(
            "Creating OpenStack volume type: {} (type: {:?}, tier: {:?})",
            name, storage_type, tier
        );

        // Build extra specs based on storage type and tier
        let mut extra_specs = BTreeMap::new();

        match storage_type {
            StorageType::Block => {
                extra_specs.insert("volume_backend_name".to_string(),
                    match tier {
                        StorageTier::Hot => "nvme-backend".to_string(),
                        StorageTier::Warm => "ssd-backend".to_string(),
                        StorageTier::Cold => "hdd-backend".to_string(),
                    });
            }
            StorageType::File => {
                extra_specs.insert("share_backend_name".to_string(), "manila-generic".to_string());
                extra_specs.insert("snapshot_support".to_string(), "true".to_string());
            }
            StorageType::Object => {
                extra_specs.insert("storage_policy".to_string(), self.get_storage_policy(tier).to_string());
            }
        }

        // Merge user parameters
        for (k, v) in params.iter() {
            extra_specs.insert(k.clone(), v.clone());
        }

        let volume_type = VolumeType {
            name: name.to_string(),
            description: format!("{:?} storage, {:?} tier", storage_type, tier),
            is_public: true,
            extra_specs: extra_specs.clone(),
        };

        self.volume_types.write().await.insert(name.to_string(), volume_type);

        Ok(PlatformStorageClass {
            name: name.to_string(),
            platform: Platform::OpenStack,
            is_default: false,
            parameters: extra_specs,
        })
    }

    async fn delete_storage_class(&self, name: &str) -> Result<()> {
        info!("Deleting OpenStack volume type: {}", name);

        let mut types = self.volume_types.write().await;
        if types.remove(name).is_some() {
            Ok(())
        } else {
            Err(Error::ResourceNotFound {
                kind: "VolumeType".into(),
                name: name.into(),
            })
        }
    }

    async fn list_storage_classes(&self) -> Result<Vec<PlatformStorageClass>> {
        let types = self.volume_types.read().await;

        Ok(types
            .values()
            .map(|vt| PlatformStorageClass {
                name: vt.name.clone(),
                platform: Platform::OpenStack,
                is_default: false,
                parameters: vt.extra_specs.clone(),
            })
            .collect())
    }

    async fn provision(
        &self,
        name: &str,
        storage_type: StorageType,
        capacity_bytes: u64,
        storage_class: &str,
    ) -> Result<String> {
        match storage_type {
            StorageType::Block => {
                self.create_cinder_volume(name, capacity_bytes, storage_class).await
            }
            StorageType::File => {
                self.create_manila_share(name, capacity_bytes, storage_class).await
            }
            StorageType::Object => {
                // For object storage, storage_class is the storage policy
                self.create_swift_container(name, storage_class).await
            }
        }
    }

    async fn delete_storage(&self, storage_id: &str) -> Result<()> {
        // Try to delete from each service
        // In a real implementation, we'd track the type

        // Try Cinder first
        if self.delete_cinder_volume(storage_id).await.is_ok() {
            return Ok(());
        }

        // Try Manila
        if self.delete_manila_share(storage_id).await.is_ok() {
            return Ok(());
        }

        // Try Swift
        if self.delete_swift_container(storage_id).await.is_ok() {
            return Ok(());
        }

        Err(Error::ResourceNotFound {
            kind: "Storage".into(),
            name: storage_id.into(),
        })
    }

    async fn health_check(&self) -> Result<bool> {
        // In a real implementation, check Keystone and service endpoints
        Ok(true)
    }
}

/// Generate a simple unique ID
fn generate_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{:016x}", now)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_create_cinder_volume() {
        let adapter = OpenStackAdapter::new(OpenStackConfig::default());

        let volume_id = adapter
            .provision(
                "test-volume",
                StorageType::Block,
                10 * 1024 * 1024 * 1024,
                "high-iops",
            )
            .await
            .unwrap();

        assert!(!volume_id.is_empty());
        assert!(volume_id.starts_with("vol-"));
    }

    #[tokio::test]
    async fn test_create_manila_share() {
        let adapter = OpenStackAdapter::new(OpenStackConfig::default());

        let share_id = adapter
            .provision(
                "test-share",
                StorageType::File,
                100 * 1024 * 1024 * 1024,
                "general",
            )
            .await
            .unwrap();

        assert!(!share_id.is_empty());
        assert!(share_id.starts_with("share-"));
    }

    #[tokio::test]
    async fn test_create_swift_container() {
        let adapter = OpenStackAdapter::new(OpenStackConfig::default());

        let container = adapter
            .provision(
                "test-container",
                StorageType::Object,
                0, // Object storage doesn't have fixed size
                "Policy-0",
            )
            .await
            .unwrap();

        assert_eq!(container, "test-container");
    }

    #[tokio::test]
    async fn test_create_volume_type() {
        let adapter = OpenStackAdapter::new(OpenStackConfig::default());

        let class = adapter
            .create_storage_class(
                "high-iops",
                StorageType::Block,
                StorageTier::Hot,
                BTreeMap::new(),
            )
            .await
            .unwrap();

        assert_eq!(class.name, "high-iops");
        assert_eq!(class.platform, Platform::OpenStack);
        assert!(class.parameters.contains_key("volume_backend_name"));
    }
}

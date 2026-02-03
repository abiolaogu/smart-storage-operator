//! Harvester HCI Platform Adapter
//!
//! Provides integration with Harvester HCI using Longhorn CSI
//! for block storage provisioning.

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

/// Configuration for Harvester adapter
#[derive(Debug, Clone)]
pub struct HarvesterConfig {
    /// Harvester API endpoint
    pub api_endpoint: Option<String>,
    /// Longhorn namespace
    pub longhorn_namespace: String,
    /// Default number of replicas
    pub default_replicas: u32,
    /// Enable data locality
    pub data_locality: String,
    /// Storage class prefix
    pub storage_class_prefix: String,
}

impl Default for HarvesterConfig {
    fn default() -> Self {
        Self {
            api_endpoint: None,
            longhorn_namespace: "longhorn-system".to_string(),
            default_replicas: 3,
            data_locality: "disabled".to_string(),
            storage_class_prefix: "unified-".to_string(),
        }
    }
}

// =============================================================================
// Storage Class State
// =============================================================================

/// Internal tracking of created storage classes
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StorageClassState {
    name: String,
    storage_type: String,
    tier: String,
    replicas: u32,
    is_default: bool,
    parameters: BTreeMap<String, String>,
}

// =============================================================================
// Harvester Adapter
// =============================================================================

/// Adapter for Harvester HCI platform
pub struct HarvesterAdapter {
    config: HarvesterConfig,
    /// Track created storage classes
    storage_classes: RwLock<BTreeMap<String, StorageClassState>>,
    /// Track provisioned volumes
    volumes: RwLock<BTreeMap<String, VolumeState>>,
}

#[derive(Debug, Clone)]
struct VolumeState {
    id: String,
    name: String,
    storage_class: String,
    capacity_bytes: u64,
}

impl HarvesterAdapter {
    /// Create a new Harvester adapter
    pub fn new(config: HarvesterConfig) -> Self {
        Self {
            config,
            storage_classes: RwLock::new(BTreeMap::new()),
            volumes: RwLock::new(BTreeMap::new()),
        }
    }

    /// Get the Longhorn storage class name for a tier
    fn get_longhorn_class(&self, tier: StorageTier) -> String {
        match tier {
            StorageTier::Hot => format!("{}longhorn-nvme", self.config.storage_class_prefix),
            StorageTier::Warm => format!("{}longhorn-ssd", self.config.storage_class_prefix),
            StorageTier::Cold => format!("{}longhorn-hdd", self.config.storage_class_prefix),
        }
    }

    /// Create a Longhorn volume via PVC
    async fn create_pvc(
        &self,
        name: &str,
        capacity_bytes: u64,
        storage_class: &str,
    ) -> Result<String> {
        let volume_id = format!("pvc-{}", generate_id());

        info!(
            "Creating Harvester PVC: {} ({} bytes, class: {})",
            name, capacity_bytes, storage_class
        );

        // In a real implementation, this would create a PVC resource
        let state = VolumeState {
            id: volume_id.clone(),
            name: name.to_string(),
            storage_class: storage_class.to_string(),
            capacity_bytes,
        };

        self.volumes.write().await.insert(volume_id.clone(), state);

        Ok(volume_id)
    }
}

#[async_trait]
impl PlatformAdapter for HarvesterAdapter {
    fn platform(&self) -> Platform {
        Platform::Harvester
    }

    async fn create_storage_class(
        &self,
        name: &str,
        storage_type: StorageType,
        tier: StorageTier,
        params: BTreeMap<String, String>,
    ) -> Result<PlatformStorageClass> {
        let class_name = format!("{}{}", self.config.storage_class_prefix, name);

        info!(
            "Creating Harvester storage class: {} (type: {:?}, tier: {:?})",
            class_name, storage_type, tier
        );

        // Determine replicas based on tier
        let replicas = match tier {
            StorageTier::Hot => self.config.default_replicas,
            StorageTier::Warm => 2,
            StorageTier::Cold => 1,
        };

        // Build Longhorn parameters
        let mut parameters = BTreeMap::new();
        parameters.insert("numberOfReplicas".to_string(), replicas.to_string());
        parameters.insert("dataLocality".to_string(), self.config.data_locality.clone());
        parameters.insert("staleReplicaTimeout".to_string(), "30".to_string());

        // Merge user parameters
        for (k, v) in params {
            parameters.insert(k, v);
        }

        let state = StorageClassState {
            name: class_name.clone(),
            storage_type: format!("{:?}", storage_type).to_lowercase(),
            tier: format!("{:?}", tier).to_lowercase(),
            replicas,
            is_default: false,
            parameters: parameters.clone(),
        };

        self.storage_classes.write().await.insert(class_name.clone(), state);

        Ok(PlatformStorageClass {
            name: class_name,
            platform: Platform::Harvester,
            is_default: false,
            parameters,
        })
    }

    async fn delete_storage_class(&self, name: &str) -> Result<()> {
        info!("Deleting Harvester storage class: {}", name);

        let mut classes = self.storage_classes.write().await;
        if classes.remove(name).is_some() {
            Ok(())
        } else {
            Err(Error::ResourceNotFound {
                kind: "StorageClass".into(),
                name: name.into(),
            })
        }
    }

    async fn list_storage_classes(&self) -> Result<Vec<PlatformStorageClass>> {
        let classes = self.storage_classes.read().await;

        Ok(classes
            .values()
            .map(|state| PlatformStorageClass {
                name: state.name.clone(),
                platform: Platform::Harvester,
                is_default: state.is_default,
                parameters: state.parameters.clone(),
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
        // Only block storage is supported via Longhorn
        if storage_type != StorageType::Block {
            return Err(Error::PlatformAdapter {
                platform: "harvester".into(),
                reason: format!("{:?} storage not supported, use block", storage_type),
            });
        }

        self.create_pvc(name, capacity_bytes, storage_class).await
    }

    async fn delete_storage(&self, storage_id: &str) -> Result<()> {
        info!("Deleting Harvester volume: {}", storage_id);

        let mut volumes = self.volumes.write().await;
        if volumes.remove(storage_id).is_some() {
            Ok(())
        } else {
            Err(Error::ResourceNotFound {
                kind: "PersistentVolumeClaim".into(),
                name: storage_id.into(),
            })
        }
    }

    async fn health_check(&self) -> Result<bool> {
        // In a real implementation, check Longhorn manager status
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
    async fn test_create_storage_class() {
        let adapter = HarvesterAdapter::new(HarvesterConfig::default());

        let class = adapter
            .create_storage_class(
                "test-class",
                StorageType::Block,
                StorageTier::Hot,
                BTreeMap::new(),
            )
            .await
            .unwrap();

        assert!(class.name.contains("test-class"));
        assert_eq!(class.platform, Platform::Harvester);
        assert!(class.parameters.contains_key("numberOfReplicas"));
    }

    #[tokio::test]
    async fn test_provision_block() {
        let adapter = HarvesterAdapter::new(HarvesterConfig::default());

        let volume_id = adapter
            .provision(
                "test-volume",
                StorageType::Block,
                10 * 1024 * 1024 * 1024,
                "longhorn",
            )
            .await
            .unwrap();

        assert!(!volume_id.is_empty());
    }

    #[tokio::test]
    async fn test_provision_file_fails() {
        let adapter = HarvesterAdapter::new(HarvesterConfig::default());

        let result = adapter
            .provision(
                "test-share",
                StorageType::File,
                10 * 1024 * 1024 * 1024,
                "longhorn",
            )
            .await;

        assert!(result.is_err());
    }
}

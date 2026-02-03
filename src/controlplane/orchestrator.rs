//! Main Orchestrator - The "Brain"
//!
//! Coordinates all control plane operations including:
//! - Storage provisioning across backends
//! - Hardware discovery and classification
//! - Platform adapter management
//! - Pool lifecycle management

use crate::controlplane::backends::{BackendConfig, BackendFactory};
use crate::controlplane::platform::{PlatformConfig, PlatformFactory};
use crate::domain::ports::{
    Platform, PlatformAdapter, ProvisionRequest, ProvisionResponse, StorageProvisioner,
    StorageTier, StorageType,
};
use crate::error::{Error, Result};
use crate::hardware::allocation::{AllocationPolicy, DriveAllocator};
use crate::hardware::classification::DeviceClassifier;
use crate::hardware::registry::NodeRegistry;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

// =============================================================================
// Pool Info
// =============================================================================

/// Information about a storage pool
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolInfo {
    pub name: String,
    pub pool_type: String,
    pub backend: String,
    pub drive_count: u32,
    pub node_count: u32,
    pub total_capacity_bytes: u64,
    pub available_capacity_bytes: u64,
    pub utilization_percent: u32,
}

// =============================================================================
// Orchestrator Configuration
// =============================================================================

/// Configuration for the orchestrator
#[derive(Debug, Clone)]
pub struct OrchestratorConfig {
    /// Backend configurations
    pub backends: BackendConfig,
    /// Platform configurations
    pub platforms: PlatformConfig,
    /// Default platform
    pub default_platform: Platform,
    /// Enable auto-classification
    pub auto_classify: bool,
    /// Classification interval in seconds
    pub classify_interval_secs: u64,
}

impl Default for OrchestratorConfig {
    fn default() -> Self {
        Self {
            backends: BackendConfig::default(),
            platforms: PlatformConfig::default(),
            default_platform: Platform::Kubernetes,
            auto_classify: true,
            classify_interval_secs: 300,
        }
    }
}

// =============================================================================
// Provisioned Storage Record
// =============================================================================

/// Record of provisioned storage
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StorageRecord {
    id: String,
    name: String,
    storage_type: StorageType,
    capacity_bytes: u64,
    backend: String,
    pool_name: String,
    platform: Platform,
    created_at: chrono::DateTime<chrono::Utc>,
}

// =============================================================================
// Orchestrator
// =============================================================================

/// Main orchestrator that coordinates all control plane operations
pub struct Orchestrator {
    config: OrchestratorConfig,
    /// Node registry
    registry: Arc<NodeRegistry>,
    /// Drive allocator
    allocator: Arc<DriveAllocator>,
    /// Device classifier
    classifier: DeviceClassifier,
    /// Backend adapters by name
    backends: RwLock<BTreeMap<String, Arc<dyn StorageProvisioner>>>,
    /// Platform adapters by name
    platforms: RwLock<BTreeMap<Platform, Arc<dyn PlatformAdapter>>>,
    /// Provisioned storage records
    storage_records: RwLock<BTreeMap<String, StorageRecord>>,
    /// Pool records
    pools: RwLock<BTreeMap<String, PoolInfo>>,
}

impl Orchestrator {
    /// Create a new orchestrator
    pub fn new(
        config: OrchestratorConfig,
        registry: Arc<NodeRegistry>,
    ) -> Arc<Self> {
        let allocator = DriveAllocator::new(registry.clone());

        Arc::new(Self {
            config,
            registry,
            allocator,
            classifier: DeviceClassifier::new(),
            backends: RwLock::new(BTreeMap::new()),
            platforms: RwLock::new(BTreeMap::new()),
            storage_records: RwLock::new(BTreeMap::new()),
            pools: RwLock::new(BTreeMap::new()),
        })
    }

    /// Initialize the orchestrator with default backends and platforms
    pub async fn initialize(&self) -> Result<()> {
        info!("Initializing orchestrator");

        // Initialize backends
        self.register_backend("mayastor", &self.config.backends).await?;
        self.register_backend("seaweedfs", &self.config.backends).await?;
        self.register_backend("rustfs", &self.config.backends).await?;

        // Initialize platforms
        self.register_platform(Platform::Harvester, &self.config.platforms).await?;
        self.register_platform(Platform::OpenStack, &self.config.platforms).await?;

        // Create default pools
        self.create_default_pools().await?;

        info!("Orchestrator initialized successfully");
        Ok(())
    }

    /// Register a storage backend
    async fn register_backend(&self, name: &str, config: &BackendConfig) -> Result<()> {
        info!("Registering backend: {}", name);

        let backend = BackendFactory::create(name, config.clone())?;
        self.backends.write().await.insert(name.to_string(), backend);

        Ok(())
    }

    /// Register a platform adapter
    async fn register_platform(&self, platform: Platform, config: &PlatformConfig) -> Result<()> {
        info!("Registering platform: {:?}", platform);

        let adapter = PlatformFactory::create(platform, config.clone())?;
        self.platforms.write().await.insert(platform, adapter);

        Ok(())
    }

    /// Create default storage pools
    async fn create_default_pools(&self) -> Result<()> {
        // Create hot pool for block storage
        let hot_pool = PoolInfo {
            name: "hot-nvme-pool".to_string(),
            pool_type: "block".to_string(),
            backend: "mayastor".to_string(),
            drive_count: 0,
            node_count: 0,
            total_capacity_bytes: 0,
            available_capacity_bytes: 0,
            utilization_percent: 0,
        };

        // Create object storage pool
        let object_pool = PoolInfo {
            name: "object-pool".to_string(),
            pool_type: "object".to_string(),
            backend: "rustfs".to_string(),
            drive_count: 0,
            node_count: 0,
            total_capacity_bytes: 0,
            available_capacity_bytes: 0,
            utilization_percent: 0,
        };

        // Create file storage pool
        let file_pool = PoolInfo {
            name: "file-pool".to_string(),
            pool_type: "file".to_string(),
            backend: "seaweedfs".to_string(),
            drive_count: 0,
            node_count: 0,
            total_capacity_bytes: 0,
            available_capacity_bytes: 0,
            utilization_percent: 0,
        };

        let mut pools = self.pools.write().await;
        pools.insert(hot_pool.name.clone(), hot_pool);
        pools.insert(object_pool.name.clone(), object_pool);
        pools.insert(file_pool.name.clone(), file_pool);

        Ok(())
    }

    /// Provision storage
    pub async fn provision(&self, request: ProvisionRequest) -> Result<ProvisionResponse> {
        info!(
            "Provisioning storage: {} ({:?}, {} bytes)",
            request.name, request.storage_type, request.capacity_bytes
        );

        // Select backend based on storage type
        let backend_name = match request.storage_type {
            StorageType::Block => "mayastor",
            StorageType::File => "seaweedfs",
            StorageType::Object => "rustfs",
        };

        let backends = self.backends.read().await;
        let backend = backends.get(backend_name).ok_or_else(|| {
            Error::BackendUnavailable {
                backend: backend_name.to_string(),
            }
        })?;

        // Provision via backend
        let response = backend.provision(request.clone()).await?;

        // Record the provisioning
        let record = StorageRecord {
            id: response.storage_id.clone(),
            name: response.name.clone(),
            storage_type: request.storage_type,
            capacity_bytes: response.capacity_bytes,
            backend: backend_name.to_string(),
            pool_name: response.pool_name.clone(),
            platform: self.config.default_platform,
            created_at: chrono::Utc::now(),
        };

        self.storage_records.write().await.insert(response.storage_id.clone(), record);

        info!("Provisioned storage: {} -> {}", request.name, response.storage_id);

        Ok(response)
    }

    /// Get storage by ID
    pub async fn get_storage(&self, storage_id: &str) -> Result<Option<ProvisionResponse>> {
        // Check our records
        let records = self.storage_records.read().await;
        let record = match records.get(storage_id) {
            Some(r) => r.clone(),
            None => return Ok(None),
        };
        drop(records);

        // Get from backend
        let backends = self.backends.read().await;
        let backend = backends.get(&record.backend).ok_or_else(|| {
            Error::BackendUnavailable {
                backend: record.backend.clone(),
            }
        })?;

        backend.get(storage_id).await
    }

    /// Delete storage
    pub async fn delete_storage(&self, storage_id: &str) -> Result<()> {
        info!("Deleting storage: {}", storage_id);

        // Get record
        let records = self.storage_records.read().await;
        let record = records.get(storage_id).cloned().ok_or_else(|| {
            Error::ResourceNotFound {
                kind: "Storage".into(),
                name: storage_id.into(),
            }
        })?;
        drop(records);

        // Delete from backend
        let backends = self.backends.read().await;
        let backend = backends.get(&record.backend).ok_or_else(|| {
            Error::BackendUnavailable {
                backend: record.backend.clone(),
            }
        })?;

        backend.delete(storage_id).await?;

        // Remove record
        self.storage_records.write().await.remove(storage_id);

        info!("Deleted storage: {}", storage_id);

        Ok(())
    }

    /// List all pools
    pub async fn list_pools(&self) -> Result<Vec<PoolInfo>> {
        let pools = self.pools.read().await;
        Ok(pools.values().cloned().collect())
    }

    /// Get pool by name
    pub async fn get_pool(&self, name: &str) -> Result<Option<PoolInfo>> {
        let pools = self.pools.read().await;
        Ok(pools.get(name).cloned())
    }

    /// Classify drives on a node
    pub async fn classify_node_drives(&self, node_id: &str) -> Result<()> {
        info!("Classifying drives on node: {}", node_id);

        let entry = self.registry.get(node_id).ok_or_else(|| {
            Error::NodeNotFound {
                node_id: node_id.to_string(),
            }
        })?;

        for drive in &entry.status.drives {
            // Create a DriveInfo from DriveStatus for classification
            let drive_info = crate::domain::ports::DriveInfo {
                device_path: drive.device_path.clone(),
                device_id: drive.id.clone(),
                drive_type: match drive.drive_type {
                    crate::crd::DriveType::Nvme => crate::domain::ports::DriveType::Nvme,
                    crate::crd::DriveType::Ssd => crate::domain::ports::DriveType::Ssd,
                    crate::crd::DriveType::Hdd => crate::domain::ports::DriveType::Hdd,
                    crate::crd::DriveType::Unknown => crate::domain::ports::DriveType::Unknown,
                },
                model: drive.model.clone(),
                serial: drive.serial.clone(),
                firmware: drive.firmware.clone(),
                capacity_bytes: drive.capacity_bytes,
                block_size: 4096, // Default
                zns_supported: drive.namespaces.iter().any(|ns| ns.is_zns),
                nvme_namespaces: vec![],
                smart_data: None,
            };

            let classification = self.classifier.classify(&drive_info);

            debug!(
                "Drive {} classified: {:?}, score: {}",
                drive.id,
                classification.performance,
                classification.confidence_percent()
            );
        }

        Ok(())
    }

    /// Get backend health status
    pub async fn backends_health(&self) -> BTreeMap<String, bool> {
        let backends = self.backends.read().await;
        let mut health = BTreeMap::new();

        for (name, backend) in backends.iter() {
            let is_healthy = backend.health_check().await.unwrap_or(false);
            health.insert(name.clone(), is_healthy);
        }

        health
    }

    /// Get platform health status
    pub async fn platforms_health(&self) -> BTreeMap<String, bool> {
        let platforms = self.platforms.read().await;
        let mut health = BTreeMap::new();

        for (platform, adapter) in platforms.iter() {
            let is_healthy = adapter.health_check().await.unwrap_or(false);
            health.insert(format!("{:?}", platform), is_healthy);
        }

        health
    }

    /// Get overall orchestrator status
    pub async fn status(&self) -> OrchestratorStatus {
        let registry_stats = self.registry.stats();
        let backends_health = self.backends_health().await;
        let platforms_health = self.platforms_health().await;
        let storage_count = self.storage_records.read().await.len();
        let pool_count = self.pools.read().await.len();

        OrchestratorStatus {
            healthy: backends_health.values().all(|&h| h),
            node_count: registry_stats.total_nodes,
            online_nodes: registry_stats.online_nodes,
            total_drives: registry_stats.total_drives,
            total_capacity_bytes: registry_stats.total_capacity_bytes,
            available_capacity_bytes: registry_stats.available_capacity_bytes,
            storage_count: storage_count as u64,
            pool_count: pool_count as u64,
            backends_health,
            platforms_health,
        }
    }
}

/// Orchestrator status summary
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrchestratorStatus {
    pub healthy: bool,
    pub node_count: u64,
    pub online_nodes: u64,
    pub total_drives: u64,
    pub total_capacity_bytes: u64,
    pub available_capacity_bytes: u64,
    pub storage_count: u64,
    pub pool_count: u64,
    pub backends_health: BTreeMap<String, bool>,
    pub platforms_health: BTreeMap<String, bool>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_orchestrator_creation() {
        let registry = NodeRegistry::new();
        let config = OrchestratorConfig::default();
        let orchestrator = Orchestrator::new(config, registry);

        orchestrator.initialize().await.unwrap();

        let status = orchestrator.status().await;
        assert_eq!(status.node_count, 0);
        assert!(status.pool_count > 0);
    }

    #[tokio::test]
    async fn test_provision_block_storage() {
        let registry = NodeRegistry::new();
        let config = OrchestratorConfig::default();
        let orchestrator = Orchestrator::new(config, registry);

        orchestrator.initialize().await.unwrap();

        let request = ProvisionRequest {
            request_id: "test-1".into(),
            name: "test-volume".into(),
            storage_type: StorageType::Block,
            capacity_bytes: 10 * 1024 * 1024 * 1024,
            tier: Some(StorageTier::Hot),
            max_iops: None,
            labels: BTreeMap::new(),
            platform_params: BTreeMap::new(),
        };

        let response = orchestrator.provision(request).await.unwrap();

        assert!(!response.storage_id.is_empty());
        assert_eq!(response.name, "test-volume");
        assert_eq!(response.storage_type, StorageType::Block);

        // Verify we can get it back
        let fetched = orchestrator.get_storage(&response.storage_id).await.unwrap();
        assert!(fetched.is_some());
    }

    #[tokio::test]
    async fn test_list_pools() {
        let registry = NodeRegistry::new();
        let config = OrchestratorConfig::default();
        let orchestrator = Orchestrator::new(config, registry);

        orchestrator.initialize().await.unwrap();

        let pools = orchestrator.list_pools().await.unwrap();
        assert!(pools.len() >= 3); // hot, object, file pools
    }
}

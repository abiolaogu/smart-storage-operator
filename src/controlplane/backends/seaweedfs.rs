//! SeaweedFS File Storage Adapter
//!
//! Provides file storage provisioning via SeaweedFS.

use crate::domain::ports::{ProvisionRequest, ProvisionResponse, StorageProvisioner, StorageType};
use crate::error::{Error, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

// =============================================================================
// Configuration
// =============================================================================

/// Configuration for SeaweedFS adapter
#[derive(Debug, Clone)]
pub struct SeaweedFSConfig {
    /// Master server endpoint
    pub master_endpoint: String,
    /// Filer endpoint
    pub filer_endpoint: String,
    /// Default replication scheme
    pub default_replication: String,
    /// Default TTL (empty for no TTL)
    pub default_ttl: Option<String>,
    /// Data center name
    pub data_center: Option<String>,
}

impl Default for SeaweedFSConfig {
    fn default() -> Self {
        Self {
            master_endpoint: "http://seaweedfs-master:9333".to_string(),
            filer_endpoint: "http://seaweedfs-filer:8888".to_string(),
            default_replication: "001".to_string(), // 1 copy on different rack
            default_ttl: None,
            data_center: None,
        }
    }
}

// =============================================================================
// Volume State
// =============================================================================

/// Internal tracking of provisioned file volumes
#[derive(Debug, Clone, Serialize, Deserialize)]
struct FileVolumeState {
    id: String,
    name: String,
    path: String,
    capacity_bytes: u64,
    replication: String,
    created_at: chrono::DateTime<chrono::Utc>,
}

// =============================================================================
// SeaweedFS Adapter
// =============================================================================

/// Adapter for SeaweedFS file storage
pub struct SeaweedFSAdapter {
    config: SeaweedFSConfig,
    /// Track provisioned volumes
    volumes: RwLock<BTreeMap<String, FileVolumeState>>,
}

impl SeaweedFSAdapter {
    /// Create a new SeaweedFS adapter
    pub fn new(config: SeaweedFSConfig) -> Self {
        Self {
            config,
            volumes: RwLock::new(BTreeMap::new()),
        }
    }

    /// Create a file volume (directory in SeaweedFS filer)
    async fn create_volume(
        &self,
        name: &str,
        capacity_bytes: u64,
        replication: &str,
    ) -> Result<String> {
        let volume_id = format!("fvol-{}", generate_id());
        let path = format!("/volumes/{}", name);

        info!(
            "Creating SeaweedFS volume: {} at {} ({} bytes)",
            name, path, capacity_bytes
        );

        // In a real implementation, this would:
        // 1. Create directory in filer
        // 2. Set collection/replication options
        // 3. Configure quota if supported

        let state = FileVolumeState {
            id: volume_id.clone(),
            name: name.to_string(),
            path,
            capacity_bytes,
            replication: replication.to_string(),
            created_at: chrono::Utc::now(),
        };

        self.volumes.write().await.insert(volume_id.clone(), state);

        debug!("Created SeaweedFS volume: {}", volume_id);

        Ok(volume_id)
    }

    /// Delete a file volume
    async fn delete_volume(&self, volume_id: &str) -> Result<()> {
        info!("Deleting SeaweedFS volume: {}", volume_id);

        let mut volumes = self.volumes.write().await;
        if volumes.remove(volume_id).is_some() {
            Ok(())
        } else {
            Err(Error::ResourceNotFound {
                kind: "SeaweedFSVolume".into(),
                name: volume_id.into(),
            })
        }
    }

    /// Get volume state
    async fn get_volume(&self, volume_id: &str) -> Option<FileVolumeState> {
        self.volumes.read().await.get(volume_id).cloned()
    }
}

#[async_trait]
impl StorageProvisioner for SeaweedFSAdapter {
    async fn provision(&self, request: ProvisionRequest) -> Result<ProvisionResponse> {
        // Determine replication based on tier
        let replication = match request.tier {
            Some(crate::domain::ports::StorageTier::Hot) => "010", // 1 replica on different server
            Some(crate::domain::ports::StorageTier::Cold) => "001", // 1 replica on different rack
            _ => &self.config.default_replication,
        };

        let volume_id = self
            .create_volume(&request.name, request.capacity_bytes, replication)
            .await?;

        let state = self.get_volume(&volume_id).await.ok_or_else(|| {
            Error::BackendOperationFailed {
                backend: "seaweedfs".into(),
                operation: "provision".into(),
                reason: "Volume not found after creation".into(),
            }
        })?;

        let mut platform_details = BTreeMap::new();
        platform_details.insert("backend".to_string(), "seaweedfs".to_string());
        platform_details.insert("path".to_string(), state.path.clone());
        platform_details.insert("replication".to_string(), state.replication.clone());
        platform_details.insert("filer".to_string(), self.config.filer_endpoint.clone());

        Ok(ProvisionResponse {
            storage_id: volume_id,
            name: request.name,
            storage_type: StorageType::File,
            capacity_bytes: request.capacity_bytes,
            pool_name: "seaweedfs-default".to_string(),
            primary_node: None,
            platform_details,
        })
    }

    async fn delete(&self, storage_id: &str) -> Result<()> {
        self.delete_volume(storage_id).await
    }

    async fn get(&self, storage_id: &str) -> Result<Option<ProvisionResponse>> {
        let state = match self.get_volume(storage_id).await {
            Some(s) => s,
            None => return Ok(None),
        };

        let mut platform_details = BTreeMap::new();
        platform_details.insert("backend".to_string(), "seaweedfs".to_string());
        platform_details.insert("path".to_string(), state.path.clone());
        platform_details.insert("replication".to_string(), state.replication.clone());

        Ok(Some(ProvisionResponse {
            storage_id: state.id,
            name: state.name,
            storage_type: StorageType::File,
            capacity_bytes: state.capacity_bytes,
            pool_name: "seaweedfs-default".to_string(),
            primary_node: None,
            platform_details,
        }))
    }

    async fn list(&self) -> Result<Vec<ProvisionResponse>> {
        let volumes = self.volumes.read().await;
        let mut responses = Vec::new();

        for state in volumes.values() {
            let mut platform_details = BTreeMap::new();
            platform_details.insert("backend".to_string(), "seaweedfs".to_string());
            platform_details.insert("path".to_string(), state.path.clone());

            responses.push(ProvisionResponse {
                storage_id: state.id.clone(),
                name: state.name.clone(),
                storage_type: StorageType::File,
                capacity_bytes: state.capacity_bytes,
                pool_name: "seaweedfs-default".to_string(),
                primary_node: None,
                platform_details,
            });
        }

        Ok(responses)
    }

    async fn health_check(&self) -> Result<bool> {
        // In a real implementation, check SeaweedFS master/filer status
        Ok(true)
    }

    fn backend_name(&self) -> &str {
        "seaweedfs"
    }

    fn supported_types(&self) -> Vec<StorageType> {
        vec![StorageType::File]
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
    async fn test_provision_file_volume() {
        let adapter = SeaweedFSAdapter::new(SeaweedFSConfig::default());

        let request = ProvisionRequest {
            request_id: "test-req".into(),
            name: "test-share".into(),
            storage_type: StorageType::File,
            capacity_bytes: 100 * 1024 * 1024 * 1024, // 100GB
            tier: None,
            max_iops: None,
            labels: BTreeMap::new(),
            platform_params: BTreeMap::new(),
        };

        let response = adapter.provision(request).await.unwrap();

        assert!(!response.storage_id.is_empty());
        assert_eq!(response.name, "test-share");
        assert_eq!(response.storage_type, StorageType::File);
        assert!(response.platform_details.contains_key("path"));
    }
}

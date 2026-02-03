//! Mayastor Block Storage Adapter
//!
//! Provides block storage provisioning via OpenEBS Mayastor.

use crate::domain::ports::{ProvisionRequest, ProvisionResponse, StorageProvisioner, StorageType};
use crate::error::{Error, Result};
use async_trait::async_trait;
use kube::Client;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

// =============================================================================
// Configuration
// =============================================================================

/// Configuration for Mayastor adapter
#[derive(Debug, Clone)]
pub struct MayastorConfig {
    /// Mayastor namespace
    pub namespace: String,
    /// REST API endpoint (if available)
    pub api_endpoint: Option<String>,
    /// Default replication factor
    pub default_replicas: u32,
    /// Default pool label for hot tier
    pub hot_pool_label: String,
    /// Default pool label for cold tier
    pub cold_pool_label: String,
}

impl Default for MayastorConfig {
    fn default() -> Self {
        Self {
            namespace: "mayastor".to_string(),
            api_endpoint: None,
            default_replicas: 3,
            hot_pool_label: "tier=hot".to_string(),
            cold_pool_label: "tier=cold".to_string(),
        }
    }
}

// =============================================================================
// Volume State
// =============================================================================

/// Internal tracking of provisioned volumes
#[derive(Debug, Clone, Serialize, Deserialize)]
struct VolumeState {
    id: String,
    name: String,
    capacity_bytes: u64,
    pool_name: String,
    replicas: u32,
    created_at: chrono::DateTime<chrono::Utc>,
}

// =============================================================================
// Mayastor Adapter
// =============================================================================

/// Adapter for Mayastor block storage
pub struct MayastorAdapter {
    config: MayastorConfig,
    client: Option<Client>,
    /// Track provisioned volumes
    volumes: RwLock<BTreeMap<String, VolumeState>>,
}

impl MayastorAdapter {
    /// Create a new Mayastor adapter
    pub fn new(config: MayastorConfig) -> Self {
        Self {
            config,
            client: None,
            volumes: RwLock::new(BTreeMap::new()),
        }
    }

    /// Initialize with Kubernetes client
    pub async fn with_client(mut self, client: Client) -> Self {
        self.client = Some(client);
        self
    }

    /// Create a MayastorVolume resource
    async fn create_volume(
        &self,
        name: &str,
        capacity_bytes: u64,
        replicas: u32,
        pool_labels: &str,
    ) -> Result<String> {
        let volume_id = format!("vol-{}", generate_id());

        info!(
            "Creating Mayastor volume: {} ({} bytes, {} replicas)",
            name, capacity_bytes, replicas
        );

        // In a real implementation, this would create the MayastorVolume CRD
        // For now, we track it internally
        let state = VolumeState {
            id: volume_id.clone(),
            name: name.to_string(),
            capacity_bytes,
            pool_name: format!("pool-{}", pool_labels.replace('=', "-")),
            replicas,
            created_at: chrono::Utc::now(),
        };

        self.volumes.write().await.insert(volume_id.clone(), state);

        debug!("Created Mayastor volume: {}", volume_id);

        Ok(volume_id)
    }

    /// Delete a MayastorVolume resource
    async fn delete_volume(&self, volume_id: &str) -> Result<()> {
        info!("Deleting Mayastor volume: {}", volume_id);

        let mut volumes = self.volumes.write().await;
        if volumes.remove(volume_id).is_some() {
            Ok(())
        } else {
            Err(Error::ResourceNotFound {
                kind: "MayastorVolume".into(),
                name: volume_id.into(),
            })
        }
    }

    /// Get volume state
    async fn get_volume(&self, volume_id: &str) -> Option<VolumeState> {
        self.volumes.read().await.get(volume_id).cloned()
    }
}

#[async_trait]
impl StorageProvisioner for MayastorAdapter {
    async fn provision(&self, request: ProvisionRequest) -> Result<ProvisionResponse> {
        // Determine pool labels based on tier
        let pool_labels = match request.tier {
            Some(crate::domain::ports::StorageTier::Hot) => &self.config.hot_pool_label,
            Some(crate::domain::ports::StorageTier::Cold) => &self.config.cold_pool_label,
            _ => &self.config.hot_pool_label, // Default to hot
        };

        // Create the volume
        let volume_id = self
            .create_volume(
                &request.name,
                request.capacity_bytes,
                self.config.default_replicas,
                pool_labels,
            )
            .await?;

        // Get the created state
        let state = self.get_volume(&volume_id).await.ok_or_else(|| {
            Error::BackendOperationFailed {
                backend: "mayastor".into(),
                operation: "provision".into(),
                reason: "Volume not found after creation".into(),
            }
        })?;

        let mut platform_details = BTreeMap::new();
        platform_details.insert("backend".to_string(), "mayastor".to_string());
        platform_details.insert("replicas".to_string(), state.replicas.to_string());
        platform_details.insert("namespace".to_string(), self.config.namespace.clone());

        Ok(ProvisionResponse {
            storage_id: volume_id,
            name: request.name,
            storage_type: StorageType::Block,
            capacity_bytes: request.capacity_bytes,
            pool_name: state.pool_name,
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
        platform_details.insert("backend".to_string(), "mayastor".to_string());
        platform_details.insert("replicas".to_string(), state.replicas.to_string());

        Ok(Some(ProvisionResponse {
            storage_id: state.id,
            name: state.name,
            storage_type: StorageType::Block,
            capacity_bytes: state.capacity_bytes,
            pool_name: state.pool_name,
            primary_node: None,
            platform_details,
        }))
    }

    async fn list(&self) -> Result<Vec<ProvisionResponse>> {
        let volumes = self.volumes.read().await;
        let mut responses = Vec::new();

        for state in volumes.values() {
            let mut platform_details = BTreeMap::new();
            platform_details.insert("backend".to_string(), "mayastor".to_string());
            platform_details.insert("replicas".to_string(), state.replicas.to_string());

            responses.push(ProvisionResponse {
                storage_id: state.id.clone(),
                name: state.name.clone(),
                storage_type: StorageType::Block,
                capacity_bytes: state.capacity_bytes,
                pool_name: state.pool_name.clone(),
                primary_node: None,
                platform_details,
            });
        }

        Ok(responses)
    }

    async fn health_check(&self) -> Result<bool> {
        // In a real implementation, check Mayastor API or CRD status
        Ok(true)
    }

    fn backend_name(&self) -> &str {
        "mayastor"
    }

    fn supported_types(&self) -> Vec<StorageType> {
        vec![StorageType::Block]
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
    async fn test_provision_volume() {
        let adapter = MayastorAdapter::new(MayastorConfig::default());

        let request = ProvisionRequest {
            request_id: "test-req".into(),
            name: "test-volume".into(),
            storage_type: StorageType::Block,
            capacity_bytes: 10 * 1024 * 1024 * 1024, // 10GB
            tier: Some(crate::domain::ports::StorageTier::Hot),
            max_iops: None,
            labels: BTreeMap::new(),
            platform_params: BTreeMap::new(),
        };

        let response = adapter.provision(request).await.unwrap();

        assert!(!response.storage_id.is_empty());
        assert_eq!(response.name, "test-volume");
        assert_eq!(response.storage_type, StorageType::Block);
    }

    #[tokio::test]
    async fn test_delete_volume() {
        let adapter = MayastorAdapter::new(MayastorConfig::default());

        // Create a volume
        let request = ProvisionRequest {
            request_id: "test-req".into(),
            name: "test-volume".into(),
            storage_type: StorageType::Block,
            capacity_bytes: 10 * 1024 * 1024 * 1024,
            tier: None,
            max_iops: None,
            labels: BTreeMap::new(),
            platform_params: BTreeMap::new(),
        };

        let response = adapter.provision(request).await.unwrap();

        // Delete it
        adapter.delete(&response.storage_id).await.unwrap();

        // Should not find it
        assert!(adapter.get(&response.storage_id).await.unwrap().is_none());
    }
}

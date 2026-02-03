//! RustFS Object Storage Adapter
//!
//! Provides S3-compatible object storage provisioning via RustFS.

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

/// Configuration for RustFS adapter
#[derive(Debug, Clone)]
pub struct RustFSConfig {
    /// RustFS API endpoint
    pub api_endpoint: String,
    /// Default erasure coding data shards
    pub ec_data_shards: u32,
    /// Default erasure coding parity shards
    pub ec_parity_shards: u32,
    /// Enable versioning by default
    pub default_versioning: bool,
    /// Access key for management
    pub access_key: Option<String>,
    /// Secret key for management
    pub secret_key: Option<String>,
}

impl Default for RustFSConfig {
    fn default() -> Self {
        Self {
            api_endpoint: "http://rustfs:9000".to_string(),
            ec_data_shards: 4,
            ec_parity_shards: 2,
            default_versioning: false,
            access_key: None,
            secret_key: None,
        }
    }
}

// =============================================================================
// Bucket State
// =============================================================================

/// Internal tracking of provisioned buckets
#[derive(Debug, Clone, Serialize, Deserialize)]
struct BucketState {
    id: String,
    name: String,
    capacity_bytes: u64,
    ec_policy: String,
    versioning: bool,
    created_at: chrono::DateTime<chrono::Utc>,
    objects_count: u64,
    used_bytes: u64,
}

// =============================================================================
// RustFS Adapter
// =============================================================================

/// Adapter for RustFS object storage
pub struct RustFSAdapter {
    config: RustFSConfig,
    /// Track provisioned buckets
    buckets: RwLock<BTreeMap<String, BucketState>>,
}

impl RustFSAdapter {
    /// Create a new RustFS adapter
    pub fn new(config: RustFSConfig) -> Self {
        Self {
            config,
            buckets: RwLock::new(BTreeMap::new()),
        }
    }

    /// Create a bucket
    async fn create_bucket(
        &self,
        name: &str,
        capacity_bytes: u64,
        versioning: bool,
    ) -> Result<String> {
        // Validate bucket name (S3 rules)
        if !is_valid_bucket_name(name) {
            return Err(Error::ApiValidation(format!(
                "Invalid bucket name: {}. Must be 3-63 characters, lowercase, no underscores",
                name
            )));
        }

        let bucket_id = format!("bucket-{}", generate_id());
        let ec_policy = format!("EC:{}:{}", self.config.ec_data_shards, self.config.ec_parity_shards);

        info!(
            "Creating RustFS bucket: {} ({} bytes, EC: {}, versioning: {})",
            name, capacity_bytes, ec_policy, versioning
        );

        // In a real implementation, this would:
        // 1. Call RustFS API to create bucket
        // 2. Configure erasure coding policy
        // 3. Set up lifecycle rules
        // 4. Enable versioning if requested

        let state = BucketState {
            id: bucket_id.clone(),
            name: name.to_string(),
            capacity_bytes,
            ec_policy,
            versioning,
            created_at: chrono::Utc::now(),
            objects_count: 0,
            used_bytes: 0,
        };

        self.buckets.write().await.insert(bucket_id.clone(), state);

        debug!("Created RustFS bucket: {}", bucket_id);

        Ok(bucket_id)
    }

    /// Delete a bucket
    async fn delete_bucket(&self, bucket_id: &str) -> Result<()> {
        info!("Deleting RustFS bucket: {}", bucket_id);

        let mut buckets = self.buckets.write().await;

        // Check if bucket is empty
        if let Some(state) = buckets.get(bucket_id) {
            if state.objects_count > 0 {
                return Err(Error::BackendOperationFailed {
                    backend: "rustfs".into(),
                    operation: "delete".into(),
                    reason: format!(
                        "Bucket is not empty ({} objects)",
                        state.objects_count
                    ),
                });
            }
        }

        if buckets.remove(bucket_id).is_some() {
            Ok(())
        } else {
            Err(Error::ResourceNotFound {
                kind: "Bucket".into(),
                name: bucket_id.into(),
            })
        }
    }

    /// Get bucket state
    async fn get_bucket(&self, bucket_id: &str) -> Option<BucketState> {
        self.buckets.read().await.get(bucket_id).cloned()
    }

    /// Get bucket by name
    async fn get_bucket_by_name(&self, name: &str) -> Option<BucketState> {
        self.buckets
            .read()
            .await
            .values()
            .find(|b| b.name == name)
            .cloned()
    }
}

#[async_trait]
impl StorageProvisioner for RustFSAdapter {
    async fn provision(&self, request: ProvisionRequest) -> Result<ProvisionResponse> {
        // Check if bucket with same name exists
        if self.get_bucket_by_name(&request.name).await.is_some() {
            return Err(Error::ResourceExists {
                kind: "Bucket".into(),
                name: request.name.clone(),
            });
        }

        // Determine versioning based on tier
        let versioning = match request.tier {
            Some(crate::domain::ports::StorageTier::Cold) => false, // Archive doesn't need versioning
            _ => self.config.default_versioning,
        };

        let bucket_id = self
            .create_bucket(&request.name, request.capacity_bytes, versioning)
            .await?;

        let state = self.get_bucket(&bucket_id).await.ok_or_else(|| {
            Error::BackendOperationFailed {
                backend: "rustfs".into(),
                operation: "provision".into(),
                reason: "Bucket not found after creation".into(),
            }
        })?;

        let mut platform_details = BTreeMap::new();
        platform_details.insert("backend".to_string(), "rustfs".to_string());
        platform_details.insert("bucket_name".to_string(), state.name.clone());
        platform_details.insert("ec_policy".to_string(), state.ec_policy.clone());
        platform_details.insert("versioning".to_string(), state.versioning.to_string());
        platform_details.insert("endpoint".to_string(), self.config.api_endpoint.clone());

        Ok(ProvisionResponse {
            storage_id: bucket_id,
            name: request.name,
            storage_type: StorageType::Object,
            capacity_bytes: request.capacity_bytes,
            pool_name: "rustfs-default".to_string(),
            primary_node: None,
            platform_details,
        })
    }

    async fn delete(&self, storage_id: &str) -> Result<()> {
        self.delete_bucket(storage_id).await
    }

    async fn get(&self, storage_id: &str) -> Result<Option<ProvisionResponse>> {
        let state = match self.get_bucket(storage_id).await {
            Some(s) => s,
            None => return Ok(None),
        };

        let mut platform_details = BTreeMap::new();
        platform_details.insert("backend".to_string(), "rustfs".to_string());
        platform_details.insert("bucket_name".to_string(), state.name.clone());
        platform_details.insert("ec_policy".to_string(), state.ec_policy.clone());
        platform_details.insert("objects_count".to_string(), state.objects_count.to_string());
        platform_details.insert("used_bytes".to_string(), state.used_bytes.to_string());

        Ok(Some(ProvisionResponse {
            storage_id: state.id,
            name: state.name,
            storage_type: StorageType::Object,
            capacity_bytes: state.capacity_bytes,
            pool_name: "rustfs-default".to_string(),
            primary_node: None,
            platform_details,
        }))
    }

    async fn list(&self) -> Result<Vec<ProvisionResponse>> {
        let buckets = self.buckets.read().await;
        let mut responses = Vec::new();

        for state in buckets.values() {
            let mut platform_details = BTreeMap::new();
            platform_details.insert("backend".to_string(), "rustfs".to_string());
            platform_details.insert("bucket_name".to_string(), state.name.clone());
            platform_details.insert("ec_policy".to_string(), state.ec_policy.clone());

            responses.push(ProvisionResponse {
                storage_id: state.id.clone(),
                name: state.name.clone(),
                storage_type: StorageType::Object,
                capacity_bytes: state.capacity_bytes,
                pool_name: "rustfs-default".to_string(),
                primary_node: None,
                platform_details,
            });
        }

        Ok(responses)
    }

    async fn health_check(&self) -> Result<bool> {
        // In a real implementation, check RustFS service status
        Ok(true)
    }

    fn backend_name(&self) -> &str {
        "rustfs"
    }

    fn supported_types(&self) -> Vec<StorageType> {
        vec![StorageType::Object]
    }
}

/// Validate S3-compatible bucket name
fn is_valid_bucket_name(name: &str) -> bool {
    // Must be 3-63 characters
    if name.len() < 3 || name.len() > 63 {
        return false;
    }

    // Must start with lowercase letter or number
    if !name.chars().next().map(|c| c.is_ascii_lowercase() || c.is_ascii_digit()).unwrap_or(false) {
        return false;
    }

    // Must end with lowercase letter or number
    if !name.chars().last().map(|c| c.is_ascii_lowercase() || c.is_ascii_digit()).unwrap_or(false) {
        return false;
    }

    // Can only contain lowercase letters, numbers, and hyphens
    for c in name.chars() {
        if !c.is_ascii_lowercase() && !c.is_ascii_digit() && c != '-' {
            return false;
        }
    }

    // Cannot have consecutive periods
    if name.contains("..") {
        return false;
    }

    // Cannot be formatted as IP address
    if name.split('.').count() == 4
        && name.split('.').all(|p| p.parse::<u8>().is_ok())
    {
        return false;
    }

    true
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

    #[test]
    fn test_valid_bucket_names() {
        assert!(is_valid_bucket_name("my-bucket"));
        assert!(is_valid_bucket_name("bucket123"));
        assert!(is_valid_bucket_name("a-1-b"));
        assert!(is_valid_bucket_name("abc"));

        // Invalid names
        assert!(!is_valid_bucket_name("ab")); // Too short
        assert!(!is_valid_bucket_name("MyBucket")); // Uppercase
        assert!(!is_valid_bucket_name("my_bucket")); // Underscore
        assert!(!is_valid_bucket_name("-bucket")); // Starts with hyphen
        assert!(!is_valid_bucket_name("bucket-")); // Ends with hyphen
        assert!(!is_valid_bucket_name("192.168.1.1")); // IP address
    }

    #[tokio::test]
    async fn test_provision_bucket() {
        let adapter = RustFSAdapter::new(RustFSConfig::default());

        let request = ProvisionRequest {
            request_id: "test-req".into(),
            name: "test-bucket".into(),
            storage_type: StorageType::Object,
            capacity_bytes: 1024 * 1024 * 1024 * 1024, // 1TB
            tier: None,
            max_iops: None,
            labels: BTreeMap::new(),
            platform_params: BTreeMap::new(),
        };

        let response = adapter.provision(request).await.unwrap();

        assert!(!response.storage_id.is_empty());
        assert_eq!(response.name, "test-bucket");
        assert_eq!(response.storage_type, StorageType::Object);
        assert!(response.platform_details.contains_key("ec_policy"));
    }

    #[tokio::test]
    async fn test_duplicate_bucket_name() {
        let adapter = RustFSAdapter::new(RustFSConfig::default());

        let request = ProvisionRequest {
            request_id: "test-req".into(),
            name: "duplicate-test".into(),
            storage_type: StorageType::Object,
            capacity_bytes: 1024 * 1024 * 1024,
            tier: None,
            max_iops: None,
            labels: BTreeMap::new(),
            platform_params: BTreeMap::new(),
        };

        // First should succeed
        adapter.provision(request.clone()).await.unwrap();

        // Second should fail
        let result = adapter.provision(request).await;
        assert!(result.is_err());
    }
}

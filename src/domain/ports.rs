//! Domain Ports - Core trait definitions for the storage operator
//!
//! These traits define the boundaries between the domain logic and external systems.
//! Adapters implement these traits to provide concrete functionality.

use crate::error::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;

// =============================================================================
// Storage Types
// =============================================================================

/// Storage types supported by the unified control plane
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StorageType {
    Block,
    File,
    Object,
}

impl std::fmt::Display for StorageType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageType::Block => write!(f, "block"),
            StorageType::File => write!(f, "file"),
            StorageType::Object => write!(f, "object"),
        }
    }
}

/// Storage tier for performance classification
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StorageTier {
    Hot,
    Warm,
    Cold,
}

impl std::fmt::Display for StorageTier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageTier::Hot => write!(f, "hot"),
            StorageTier::Warm => write!(f, "warm"),
            StorageTier::Cold => write!(f, "cold"),
        }
    }
}

// =============================================================================
// Provisioning Request/Response
// =============================================================================

/// Request to provision storage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProvisionRequest {
    /// Unique identifier for this request
    pub request_id: String,
    /// Name for the storage resource
    pub name: String,
    /// Type of storage to provision
    pub storage_type: StorageType,
    /// Desired capacity in bytes
    pub capacity_bytes: u64,
    /// Optional tier preference
    pub tier: Option<StorageTier>,
    /// Optional IOPS requirement
    pub max_iops: Option<u64>,
    /// Labels for the provisioned resource
    pub labels: BTreeMap<String, String>,
    /// Platform-specific parameters
    pub platform_params: BTreeMap<String, String>,
}

/// Response from storage provisioning
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProvisionResponse {
    /// ID of the provisioned storage
    pub storage_id: String,
    /// Name of the storage resource
    pub name: String,
    /// Type of storage provisioned
    pub storage_type: StorageType,
    /// Actual capacity provisioned
    pub capacity_bytes: u64,
    /// Pool or backend where storage was allocated
    pub pool_name: String,
    /// Node where storage is primary
    pub primary_node: Option<String>,
    /// Platform-specific details
    pub platform_details: BTreeMap<String, String>,
}

// =============================================================================
// Hardware Discovery Types
// =============================================================================

/// Drive type classification
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DriveType {
    Nvme,
    Ssd,
    Hdd,
    Unknown,
}

impl std::fmt::Display for DriveType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DriveType::Nvme => write!(f, "nvme"),
            DriveType::Ssd => write!(f, "ssd"),
            DriveType::Hdd => write!(f, "hdd"),
            DriveType::Unknown => write!(f, "unknown"),
        }
    }
}

/// Information about a discovered drive
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DriveInfo {
    /// Device path (e.g., /dev/nvme0n1)
    pub device_path: String,
    /// Device ID (e.g., nvme0n1)
    pub device_id: String,
    /// Drive type
    pub drive_type: DriveType,
    /// Model name
    pub model: String,
    /// Serial number
    pub serial: String,
    /// Firmware version
    pub firmware: String,
    /// Total capacity in bytes
    pub capacity_bytes: u64,
    /// Logical block size
    pub block_size: u32,
    /// Whether drive supports ZNS (Zoned Namespace)
    pub zns_supported: bool,
    /// NVMe namespace info (if applicable)
    pub nvme_namespaces: Vec<NvmeNamespaceInfo>,
    /// SMART health data
    pub smart_data: Option<SmartData>,
}

/// NVMe namespace information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NvmeNamespaceInfo {
    /// Namespace ID
    pub nsid: u32,
    /// Namespace capacity in bytes
    pub capacity_bytes: u64,
    /// Whether namespace is active
    pub active: bool,
    /// Whether this is a ZNS namespace
    pub is_zns: bool,
}

/// SMART health data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SmartData {
    /// Temperature in Celsius
    pub temperature_celsius: i32,
    /// Percentage of drive life used
    pub percentage_used: u8,
    /// Data units read (in 512KB units)
    pub data_units_read: u64,
    /// Data units written (in 512KB units)
    pub data_units_written: u64,
    /// Power-on hours
    pub power_on_hours: u64,
    /// Critical warning flags
    pub critical_warning: u8,
}

/// Node hardware information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeHardwareInfo {
    /// Node name/ID
    pub node_id: String,
    /// Hostname
    pub hostname: String,
    /// Discovered drives
    pub drives: Vec<DriveInfo>,
    /// Total memory in bytes
    pub memory_bytes: u64,
    /// CPU count
    pub cpu_count: u32,
    /// Discovery timestamp
    pub discovered_at: chrono::DateTime<chrono::Utc>,
}

// =============================================================================
// Storage Provisioner Port
// =============================================================================

/// Port for storage provisioning operations
#[async_trait]
pub trait StorageProvisioner: Send + Sync {
    /// Provision new storage
    async fn provision(&self, request: ProvisionRequest) -> Result<ProvisionResponse>;

    /// Delete provisioned storage
    async fn delete(&self, storage_id: &str) -> Result<()>;

    /// Get storage info
    async fn get(&self, storage_id: &str) -> Result<Option<ProvisionResponse>>;

    /// List all provisioned storage
    async fn list(&self) -> Result<Vec<ProvisionResponse>>;

    /// Check if backend is healthy
    async fn health_check(&self) -> Result<bool>;

    /// Get backend name
    fn backend_name(&self) -> &str;

    /// Get supported storage types
    fn supported_types(&self) -> Vec<StorageType>;
}

// =============================================================================
// Hardware Discoverer Port
// =============================================================================

/// Port for hardware discovery operations
#[async_trait]
pub trait HardwareDiscoverer: Send + Sync {
    /// Discover all hardware on the local node
    async fn discover_local(&self) -> Result<NodeHardwareInfo>;

    /// Discover specific device
    async fn discover_device(&self, device_path: &str) -> Result<DriveInfo>;

    /// Refresh SMART data for a device
    async fn refresh_smart(&self, device_path: &str) -> Result<SmartData>;

    /// Check if a device supports ZNS
    async fn check_zns_support(&self, device_path: &str) -> Result<bool>;
}

// =============================================================================
// Platform Adapter Port
// =============================================================================

/// Platform types supported
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Platform {
    Kubernetes,
    Harvester,
    OpenStack,
}

/// Platform-specific storage class info
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlatformStorageClass {
    /// Name of the storage class
    pub name: String,
    /// Platform
    pub platform: Platform,
    /// Whether this is the default class
    pub is_default: bool,
    /// Parameters
    pub parameters: BTreeMap<String, String>,
}

/// Port for platform-specific operations
#[async_trait]
pub trait PlatformAdapter: Send + Sync {
    /// Get platform type
    fn platform(&self) -> Platform;

    /// Create storage class for this platform
    async fn create_storage_class(
        &self,
        name: &str,
        storage_type: StorageType,
        tier: StorageTier,
        params: BTreeMap<String, String>,
    ) -> Result<PlatformStorageClass>;

    /// Delete storage class
    async fn delete_storage_class(&self, name: &str) -> Result<()>;

    /// List storage classes
    async fn list_storage_classes(&self) -> Result<Vec<PlatformStorageClass>>;

    /// Provision storage using platform APIs
    async fn provision(
        &self,
        name: &str,
        storage_type: StorageType,
        capacity_bytes: u64,
        storage_class: &str,
    ) -> Result<String>;

    /// Delete provisioned storage
    async fn delete_storage(&self, storage_id: &str) -> Result<()>;

    /// Check platform connectivity
    async fn health_check(&self) -> Result<bool>;
}

// =============================================================================
// Allocation Engine Port
// =============================================================================

/// Allocation result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AllocationResult {
    /// Allocated drive
    pub drive_id: String,
    /// Node where drive is located
    pub node_id: String,
    /// Allocated capacity in bytes
    pub capacity_bytes: u64,
    /// Pool assigned to
    pub pool_name: Option<String>,
}

/// Allocation constraints
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AllocationConstraints {
    /// Minimum capacity required
    pub min_capacity_bytes: u64,
    /// Required drive type
    pub drive_type: Option<DriveType>,
    /// Required storage tier
    pub tier: Option<StorageTier>,
    /// Nodes to exclude
    pub exclude_nodes: Vec<String>,
    /// Nodes to prefer
    pub prefer_nodes: Vec<String>,
    /// Minimum fault domains
    pub min_fault_domains: Option<u32>,
}

/// Port for drive allocation operations
#[async_trait]
pub trait AllocationEngine: Send + Sync {
    /// Allocate drives for storage
    async fn allocate(
        &self,
        storage_type: StorageType,
        constraints: AllocationConstraints,
        count: usize,
    ) -> Result<Vec<AllocationResult>>;

    /// Release allocated drives
    async fn release(&self, allocation_ids: &[String]) -> Result<()>;

    /// Get allocation info
    async fn get_allocation(&self, allocation_id: &str) -> Result<Option<AllocationResult>>;

    /// Get total available capacity for constraints
    async fn available_capacity(&self, constraints: AllocationConstraints) -> Result<u64>;
}

// =============================================================================
// Type Aliases for Arc'd Traits
// =============================================================================

pub type StorageProvisionerRef = Arc<dyn StorageProvisioner>;
pub type HardwareDiscovererRef = Arc<dyn HardwareDiscoverer>;
pub type PlatformAdapterRef = Arc<dyn PlatformAdapter>;
pub type AllocationEngineRef = Arc<dyn AllocationEngine>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_storage_type_display() {
        assert_eq!(format!("{}", StorageType::Block), "block");
        assert_eq!(format!("{}", StorageType::File), "file");
        assert_eq!(format!("{}", StorageType::Object), "object");
    }

    #[test]
    fn test_drive_type_display() {
        assert_eq!(format!("{}", DriveType::Nvme), "nvme");
        assert_eq!(format!("{}", DriveType::Ssd), "ssd");
        assert_eq!(format!("{}", DriveType::Hdd), "hdd");
    }
}

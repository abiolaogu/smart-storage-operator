//! UnifiedStorageClass CRD
//!
//! Defines storage classes that work across Block, File, and Object storage
//! with platform-specific overrides for Harvester and OpenStack.

use chrono::{DateTime, Utc};
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

// =============================================================================
// UnifiedStorageClass CRD
// =============================================================================

/// UnifiedStorageClass provides a single abstraction for provisioning
/// Block, File, or Object storage across different backends and platforms.
#[derive(CustomResource, Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[kube(
    group = "storage.billyronks.io",
    version = "v1",
    kind = "UnifiedStorageClass",
    plural = "unifiedstorageclasses",
    shortname = "usc",
    status = "UnifiedStorageClassStatus",
    printcolumn = r#"{"name": "Type", "type": "string", "jsonPath": ".spec.storageType"}"#,
    printcolumn = r#"{"name": "Tier", "type": "string", "jsonPath": ".spec.tier"}"#,
    printcolumn = r#"{"name": "Backend", "type": "string", "jsonPath": ".spec.backend"}"#,
    printcolumn = r#"{"name": "Phase", "type": "string", "jsonPath": ".status.phase"}"#,
    printcolumn = r#"{"name": "Age", "type": "date", "jsonPath": ".metadata.creationTimestamp"}"#,
    namespaced = false
)]
#[serde(rename_all = "camelCase")]
pub struct UnifiedStorageClassSpec {
    /// Type of storage: block, file, object, or auto (let system decide)
    #[serde(default = "default_storage_type")]
    pub storage_type: UnifiedStorageType,

    /// Performance tier: hot, warm, cold, or auto
    #[serde(default = "default_tier")]
    pub tier: UnifiedTier,

    /// Specific backend to use (mayastor, seaweedfs, rustfs)
    /// If not specified, chosen based on storage type
    #[serde(default)]
    pub backend: Option<BackendType>,

    /// Capacity settings
    #[serde(default)]
    pub capacity: CapacitySpec,

    /// Redundancy settings
    #[serde(default)]
    pub redundancy: RedundancySpec,

    /// Hardware preferences for allocation
    #[serde(default)]
    pub hardware_preference: HardwarePreference,

    /// Platform-specific overrides
    #[serde(default)]
    pub platform_overrides: PlatformOverrides,

    /// Whether this is the default storage class
    #[serde(default)]
    pub is_default: bool,

    /// Additional parameters passed to backend
    #[serde(default)]
    pub parameters: BTreeMap<String, String>,
}

// =============================================================================
// Sub-Types
// =============================================================================

/// Unified storage type
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum UnifiedStorageType {
    Block,
    File,
    Object,
    #[default]
    Auto,
}

impl std::fmt::Display for UnifiedStorageType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UnifiedStorageType::Block => write!(f, "block"),
            UnifiedStorageType::File => write!(f, "file"),
            UnifiedStorageType::Object => write!(f, "object"),
            UnifiedStorageType::Auto => write!(f, "auto"),
        }
    }
}

/// Performance tier
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum UnifiedTier {
    Hot,
    Warm,
    Cold,
    #[default]
    Auto,
}

impl std::fmt::Display for UnifiedTier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UnifiedTier::Hot => write!(f, "hot"),
            UnifiedTier::Warm => write!(f, "warm"),
            UnifiedTier::Cold => write!(f, "cold"),
            UnifiedTier::Auto => write!(f, "auto"),
        }
    }
}

/// Backend storage system
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum BackendType {
    Mayastor,
    SeaweedFS,
    RustFS,
}

impl std::fmt::Display for BackendType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BackendType::Mayastor => write!(f, "mayastor"),
            BackendType::SeaweedFS => write!(f, "seaweedfs"),
            BackendType::RustFS => write!(f, "rustfs"),
        }
    }
}

/// Capacity specification
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CapacitySpec {
    /// Requested capacity (e.g., "100Gi")
    #[serde(default)]
    pub requested: Option<String>,

    /// Maximum IOPS requirement
    #[serde(default)]
    pub max_iops: Option<u64>,

    /// Maximum throughput in MB/s
    #[serde(default)]
    pub max_throughput_mbps: Option<u64>,

    /// Whether to allow expansion
    #[serde(default = "default_true")]
    pub allow_expansion: bool,
}

/// Redundancy configuration
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RedundancySpec {
    /// Redundancy type
    #[serde(default)]
    pub redundancy_type: RedundancyType,

    /// Replication factor (for replication type)
    #[serde(default = "default_replication_factor")]
    pub replication_factor: u32,

    /// Erasure coding policy name (for EC type)
    #[serde(default)]
    pub ec_policy: Option<String>,

    /// Minimum fault domains for replica spread
    #[serde(default)]
    pub min_fault_domains: Option<u32>,
}

impl Default for RedundancySpec {
    fn default() -> Self {
        Self {
            redundancy_type: RedundancyType::default(),
            replication_factor: default_replication_factor(),
            ec_policy: None,
            min_fault_domains: None,
        }
    }
}

/// Redundancy type
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum RedundancyType {
    #[default]
    Replication,
    ErasureCoding,
    None,
}

/// Hardware preferences for drive allocation
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct HardwarePreference {
    /// Preferred drive type
    #[serde(default)]
    pub drive_type: Option<DriveTypePreference>,

    /// Minimum number of drives for placement
    #[serde(default)]
    pub min_drive_count: Option<u32>,

    /// Prefer ZNS-enabled drives
    #[serde(default)]
    pub prefer_zns: bool,

    /// Required node labels
    #[serde(default)]
    pub node_selector: BTreeMap<String, String>,
}

/// Drive type preference
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum DriveTypePreference {
    Nvme,
    Ssd,
    Hdd,
    #[default]
    Auto,
}

/// Platform-specific overrides
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PlatformOverrides {
    /// Harvester HCI specific settings
    #[serde(default)]
    pub harvester: Option<HarvesterOverrides>,

    /// OpenStack specific settings
    #[serde(default)]
    pub openstack: Option<OpenStackOverrides>,
}

/// Harvester HCI overrides
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct HarvesterOverrides {
    /// Longhorn storage class to use
    pub storage_class: Option<String>,

    /// Number of replicas for Longhorn
    pub replicas: Option<u32>,

    /// Data locality setting
    pub data_locality: Option<String>,
}

/// OpenStack overrides
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct OpenStackOverrides {
    /// Cinder volume type
    pub volume_type: Option<String>,

    /// Manila share type
    pub share_type: Option<String>,

    /// Swift container policy
    pub container_policy: Option<String>,

    /// Availability zone
    pub availability_zone: Option<String>,
}

// =============================================================================
// Status
// =============================================================================

/// Status of the UnifiedStorageClass
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct UnifiedStorageClassStatus {
    /// Current phase
    #[serde(default)]
    pub phase: StorageClassPhase,

    /// Resolved backend for this class
    #[serde(default)]
    pub resolved_backend: Option<BackendType>,

    /// Platform storage classes created
    #[serde(default)]
    pub platform_classes: Vec<PlatformClassRef>,

    /// Observed capacity available
    #[serde(default)]
    pub available_capacity_bytes: u64,

    /// Number of volumes using this class
    #[serde(default)]
    pub volume_count: u32,

    /// Last reconcile time
    #[serde(default)]
    #[schemars(with = "Option<String>")]
    pub last_reconcile_time: Option<DateTime<Utc>>,

    /// Conditions
    #[serde(default)]
    pub conditions: Vec<StorageClassCondition>,
}

/// Storage class lifecycle phase
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum StorageClassPhase {
    #[default]
    Pending,
    Ready,
    Degraded,
    Error,
}

impl std::fmt::Display for StorageClassPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageClassPhase::Pending => write!(f, "Pending"),
            StorageClassPhase::Ready => write!(f, "Ready"),
            StorageClassPhase::Degraded => write!(f, "Degraded"),
            StorageClassPhase::Error => write!(f, "Error"),
        }
    }
}

/// Reference to platform-specific storage class
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PlatformClassRef {
    /// Platform name
    pub platform: String,
    /// Storage class name on that platform
    pub class_name: String,
    /// Whether creation succeeded
    pub ready: bool,
}

/// Condition for storage class status
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct StorageClassCondition {
    /// Type of condition
    pub r#type: String,
    /// Status: True, False, Unknown
    pub status: ConditionStatus,
    /// Last transition time
    #[serde(default)]
    #[schemars(with = "Option<String>")]
    pub last_transition_time: Option<DateTime<Utc>>,
    /// Machine-readable reason
    #[serde(default)]
    pub reason: Option<String>,
    /// Human-readable message
    #[serde(default)]
    pub message: Option<String>,
}

/// Condition status values
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum ConditionStatus {
    True,
    False,
    Unknown,
}

// =============================================================================
// Default Value Functions
// =============================================================================

fn default_storage_type() -> UnifiedStorageType {
    UnifiedStorageType::Auto
}

fn default_tier() -> UnifiedTier {
    UnifiedTier::Auto
}

fn default_replication_factor() -> u32 {
    3
}

fn default_true() -> bool {
    true
}

// =============================================================================
// Implementations
// =============================================================================

impl UnifiedStorageClass {
    /// Get the name of this storage class
    pub fn name(&self) -> &str {
        self.metadata.name.as_deref().unwrap_or("unknown")
    }

    /// Check if this is a block storage class
    pub fn is_block(&self) -> bool {
        matches!(self.spec.storage_type, UnifiedStorageType::Block)
    }

    /// Check if this is a file storage class
    pub fn is_file(&self) -> bool {
        matches!(self.spec.storage_type, UnifiedStorageType::File)
    }

    /// Check if this is an object storage class
    pub fn is_object(&self) -> bool {
        matches!(self.spec.storage_type, UnifiedStorageType::Object)
    }

    /// Get the resolved backend for this class
    pub fn resolved_backend(&self) -> BackendType {
        self.spec.backend.unwrap_or_else(|| match self.spec.storage_type {
            UnifiedStorageType::Block => BackendType::Mayastor,
            UnifiedStorageType::File => BackendType::SeaweedFS,
            UnifiedStorageType::Object => BackendType::RustFS,
            UnifiedStorageType::Auto => {
                // Default to block if auto
                BackendType::Mayastor
            }
        })
    }

    /// Get replication factor
    pub fn replication_factor(&self) -> u32 {
        self.spec.redundancy.replication_factor
    }
}

impl UnifiedStorageClassStatus {
    /// Set a condition, replacing existing if same type
    pub fn set_condition(&mut self, condition: StorageClassCondition) {
        if let Some(existing) = self
            .conditions
            .iter_mut()
            .find(|c| c.r#type == condition.r#type)
        {
            *existing = condition;
        } else {
            self.conditions.push(condition);
        }
    }

    /// Check if class is ready
    pub fn is_ready(&self) -> bool {
        self.phase == StorageClassPhase::Ready
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_storage_type_display() {
        assert_eq!(format!("{}", UnifiedStorageType::Block), "block");
        assert_eq!(format!("{}", UnifiedStorageType::Auto), "auto");
    }

    #[test]
    fn test_resolved_backend() {
        let mut class = UnifiedStorageClass {
            metadata: Default::default(),
            spec: UnifiedStorageClassSpec {
                storage_type: UnifiedStorageType::Object,
                ..Default::default()
            },
            status: None,
        };
        assert_eq!(class.resolved_backend(), BackendType::RustFS);

        class.spec.backend = Some(BackendType::Mayastor);
        assert_eq!(class.resolved_backend(), BackendType::Mayastor);
    }

    #[test]
    fn test_default_values() {
        let spec = UnifiedStorageClassSpec::default();
        assert_eq!(spec.storage_type, UnifiedStorageType::Auto);
        assert_eq!(spec.tier, UnifiedTier::Auto);
        assert_eq!(spec.redundancy.replication_factor, 3);
    }
}

impl Default for UnifiedStorageClassSpec {
    fn default() -> Self {
        Self {
            storage_type: default_storage_type(),
            tier: default_tier(),
            backend: None,
            capacity: CapacitySpec::default(),
            redundancy: RedundancySpec::default(),
            hardware_preference: HardwarePreference::default(),
            platform_overrides: PlatformOverrides::default(),
            is_default: false,
            parameters: BTreeMap::new(),
        }
    }
}

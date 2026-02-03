//! UnifiedPool CRD
//!
//! Represents a storage pool that spans multiple drives and nodes,
//! backed by a specific storage backend (Mayastor, SeaweedFS, RustFS).

use chrono::{DateTime, Utc};
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use super::storage_node::{DriveTier, DriveType};
use super::unified_storage::BackendType;

// =============================================================================
// UnifiedPool CRD
// =============================================================================

/// UnifiedPool represents a storage pool that aggregates drives from one or more
/// nodes into a unified storage resource backed by a specific backend.
#[derive(CustomResource, Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[kube(
    group = "storage.billyronks.io",
    version = "v1",
    kind = "UnifiedPool",
    plural = "unifiedpools",
    shortname = "up",
    status = "UnifiedPoolStatus",
    printcolumn = r#"{"name": "Type", "type": "string", "jsonPath": ".spec.poolType"}"#,
    printcolumn = r#"{"name": "Backend", "type": "string", "jsonPath": ".spec.backend.type"}"#,
    printcolumn = r#"{"name": "Drives", "type": "integer", "jsonPath": ".status.driveCount"}"#,
    printcolumn = r#"{"name": "Capacity", "type": "string", "jsonPath": ".status.totalCapacity"}"#,
    printcolumn = r#"{"name": "Phase", "type": "string", "jsonPath": ".status.phase"}"#,
    printcolumn = r#"{"name": "Age", "type": "date", "jsonPath": ".metadata.creationTimestamp"}"#,
    namespaced = false
)]
#[serde(rename_all = "camelCase")]
pub struct UnifiedPoolSpec {
    /// Type of storage this pool provides
    pub pool_type: PoolType,

    /// Backend configuration
    pub backend: BackendSpec,

    /// Drive selection criteria
    #[serde(default)]
    pub drive_selector: DriveSelector,

    /// Capacity targets
    #[serde(default)]
    pub capacity: PoolCapacitySpec,

    /// Node selection criteria
    #[serde(default)]
    pub node_selector: BTreeMap<String, String>,

    /// Topology constraints
    #[serde(default)]
    pub topology: TopologySpec,

    /// Labels for this pool
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
}

// =============================================================================
// Sub-Types
// =============================================================================

/// Pool type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum PoolType {
    Block,
    File,
    Object,
}

impl std::fmt::Display for PoolType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PoolType::Block => write!(f, "block"),
            PoolType::File => write!(f, "file"),
            PoolType::Object => write!(f, "object"),
        }
    }
}

/// Backend specification
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BackendSpec {
    /// Backend type
    pub r#type: BackendType,

    /// Backend-specific configuration
    #[serde(default)]
    pub config: BTreeMap<String, String>,

    /// Namespace where backend is deployed (for Mayastor)
    #[serde(default = "default_mayastor_namespace")]
    pub namespace: String,
}

impl Default for BackendSpec {
    fn default() -> Self {
        Self {
            r#type: BackendType::Mayastor,
            config: BTreeMap::new(),
            namespace: default_mayastor_namespace(),
        }
    }
}

/// Drive selection criteria
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DriveSelector {
    /// Required drive types
    #[serde(default)]
    pub drive_types: Vec<DriveType>,

    /// Minimum performance tier
    #[serde(default)]
    pub min_tier: Option<DriveTier>,

    /// Minimum classification score (0-100)
    #[serde(default)]
    pub min_score: Option<u32>,

    /// Require ZNS support
    #[serde(default)]
    pub require_zns: bool,

    /// Minimum drive capacity in bytes
    #[serde(default)]
    pub min_capacity_bytes: Option<u64>,

    /// Maximum drive capacity in bytes
    #[serde(default)]
    pub max_capacity_bytes: Option<u64>,

    /// Match labels on drives
    #[serde(default)]
    pub match_labels: BTreeMap<String, String>,
}

/// Pool capacity specification
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PoolCapacitySpec {
    /// Target total capacity in bytes
    #[serde(default)]
    pub target_bytes: Option<u64>,

    /// Target capacity as human-readable string (e.g., "10Ti")
    #[serde(default)]
    pub target: Option<String>,

    /// Minimum drives to include
    #[serde(default)]
    pub min_drives: Option<u32>,

    /// Maximum drives to include
    #[serde(default)]
    pub max_drives: Option<u32>,

    /// Enable auto-expansion when capacity is low
    #[serde(default)]
    pub auto_expand: bool,

    /// Threshold for auto-expansion (percentage)
    #[serde(default = "default_expand_threshold")]
    pub expand_threshold_percent: u32,
}

/// Topology constraints
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TopologySpec {
    /// Spread drives across different fault domains
    #[serde(default)]
    pub spread_across_fault_domains: bool,

    /// Maximum drives per node
    #[serde(default)]
    pub max_drives_per_node: Option<u32>,

    /// Minimum nodes for the pool
    #[serde(default)]
    pub min_nodes: Option<u32>,

    /// Preferred nodes (node names)
    #[serde(default)]
    pub preferred_nodes: Vec<String>,

    /// Excluded nodes (node names)
    #[serde(default)]
    pub excluded_nodes: Vec<String>,
}

// =============================================================================
// Status
// =============================================================================

/// Status of the UnifiedPool
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct UnifiedPoolStatus {
    /// Current phase
    #[serde(default)]
    pub phase: PoolPhase,

    /// Number of drives in pool
    #[serde(default)]
    pub drive_count: u32,

    /// Number of nodes in pool
    #[serde(default)]
    pub node_count: u32,

    /// Total capacity (human-readable)
    #[serde(default)]
    pub total_capacity: String,

    /// Total capacity in bytes
    #[serde(default)]
    pub total_capacity_bytes: u64,

    /// Used capacity in bytes
    #[serde(default)]
    pub used_capacity_bytes: u64,

    /// Available capacity in bytes
    #[serde(default)]
    pub available_capacity_bytes: u64,

    /// Utilization percentage
    #[serde(default)]
    pub utilization_percent: u32,

    /// Drives in this pool
    #[serde(default)]
    pub drives: Vec<PoolDriveRef>,

    /// Backend-specific pool ID
    #[serde(default)]
    pub backend_pool_id: Option<String>,

    /// Backend-specific status
    #[serde(default)]
    pub backend_status: BTreeMap<String, String>,

    /// Last reconcile time
    #[serde(default)]
    #[schemars(with = "Option<String>")]
    pub last_reconcile_time: Option<DateTime<Utc>>,

    /// Conditions
    #[serde(default)]
    pub conditions: Vec<PoolCondition>,
}

/// Pool lifecycle phase
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum PoolPhase {
    #[default]
    Pending,
    Creating,
    Ready,
    Expanding,
    Degraded,
    Error,
    Deleting,
}

impl std::fmt::Display for PoolPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PoolPhase::Pending => write!(f, "Pending"),
            PoolPhase::Creating => write!(f, "Creating"),
            PoolPhase::Ready => write!(f, "Ready"),
            PoolPhase::Expanding => write!(f, "Expanding"),
            PoolPhase::Degraded => write!(f, "Degraded"),
            PoolPhase::Error => write!(f, "Error"),
            PoolPhase::Deleting => write!(f, "Deleting"),
        }
    }
}

/// Reference to a drive in the pool
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PoolDriveRef {
    /// Node name where drive is located
    pub node_name: String,

    /// Drive ID
    pub drive_id: String,

    /// Device path
    pub device_path: String,

    /// Drive capacity in bytes
    pub capacity_bytes: u64,

    /// Used capacity in bytes
    #[serde(default)]
    pub used_bytes: u64,

    /// Drive status in pool
    pub status: PoolDriveStatus,

    /// When drive was added to pool
    #[serde(default)]
    #[schemars(with = "Option<String>")]
    pub added_at: Option<DateTime<Utc>>,
}

/// Status of a drive in a pool
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum PoolDriveStatus {
    Pending,
    Online,
    Degraded,
    Offline,
    Removing,
}

impl std::fmt::Display for PoolDriveStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PoolDriveStatus::Pending => write!(f, "pending"),
            PoolDriveStatus::Online => write!(f, "online"),
            PoolDriveStatus::Degraded => write!(f, "degraded"),
            PoolDriveStatus::Offline => write!(f, "offline"),
            PoolDriveStatus::Removing => write!(f, "removing"),
        }
    }
}

/// Pool condition
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PoolCondition {
    /// Type of condition
    pub r#type: String,
    /// Status: True, False, Unknown
    pub status: String,
    /// Last transition time
    #[serde(default)]
    #[schemars(with = "Option<String>")]
    pub last_transition_time: Option<DateTime<Utc>>,
    /// Reason
    #[serde(default)]
    pub reason: Option<String>,
    /// Message
    #[serde(default)]
    pub message: Option<String>,
}

// =============================================================================
// Default Value Functions
// =============================================================================

fn default_mayastor_namespace() -> String {
    "mayastor".to_string()
}

fn default_expand_threshold() -> u32 {
    80
}

// =============================================================================
// Implementations
// =============================================================================

impl UnifiedPool {
    /// Get the pool name
    pub fn name(&self) -> &str {
        self.metadata.name.as_deref().unwrap_or("unknown")
    }

    /// Check if pool is ready
    pub fn is_ready(&self) -> bool {
        self.status
            .as_ref()
            .map(|s| s.phase == PoolPhase::Ready)
            .unwrap_or(false)
    }

    /// Get total capacity in bytes
    pub fn total_capacity_bytes(&self) -> u64 {
        self.status
            .as_ref()
            .map(|s| s.total_capacity_bytes)
            .unwrap_or(0)
    }

    /// Get available capacity in bytes
    pub fn available_capacity_bytes(&self) -> u64 {
        self.status
            .as_ref()
            .map(|s| s.available_capacity_bytes)
            .unwrap_or(0)
    }

    /// Get utilization percentage
    pub fn utilization_percent(&self) -> u32 {
        self.status
            .as_ref()
            .map(|s| s.utilization_percent)
            .unwrap_or(0)
    }

    /// Check if pool needs expansion
    pub fn needs_expansion(&self) -> bool {
        if !self.spec.capacity.auto_expand {
            return false;
        }
        self.utilization_percent() >= self.spec.capacity.expand_threshold_percent
    }

    /// Get the backend type
    pub fn backend_type(&self) -> BackendType {
        self.spec.backend.r#type
    }

    /// Check if a drive matches the selector
    pub fn drive_matches_selector(
        &self,
        drive_type: DriveType,
        tier: Option<DriveTier>,
        score: u32,
        capacity_bytes: u64,
        is_zns: bool,
    ) -> bool {
        let selector = &self.spec.drive_selector;

        // Check drive type
        if !selector.drive_types.is_empty() && !selector.drive_types.contains(&drive_type) {
            return false;
        }

        // Check tier
        if let (Some(min_tier), Some(actual_tier)) = (selector.min_tier, tier) {
            if !tier_meets_minimum(actual_tier, min_tier) {
                return false;
            }
        }

        // Check score
        if let Some(min_score) = selector.min_score {
            if score < min_score {
                return false;
            }
        }

        // Check ZNS
        if selector.require_zns && !is_zns {
            return false;
        }

        // Check capacity bounds
        if let Some(min) = selector.min_capacity_bytes {
            if capacity_bytes < min {
                return false;
            }
        }
        if let Some(max) = selector.max_capacity_bytes {
            if capacity_bytes > max {
                return false;
            }
        }

        true
    }
}

/// Check if a tier meets the minimum requirement
fn tier_meets_minimum(actual: DriveTier, minimum: DriveTier) -> bool {
    let tier_order = |t: DriveTier| -> u8 {
        match t {
            DriveTier::UltraFast => 4,
            DriveTier::FastNvme => 3,
            DriveTier::StandardSsd => 2,
            DriveTier::Hdd => 1,
        }
    };
    tier_order(actual) >= tier_order(minimum)
}

impl UnifiedPoolStatus {
    /// Update capacity fields
    pub fn update_capacity(&mut self) {
        self.total_capacity_bytes = self.drives.iter().map(|d| d.capacity_bytes).sum();
        self.used_capacity_bytes = self.drives.iter().map(|d| d.used_bytes).sum();
        self.available_capacity_bytes = self.total_capacity_bytes.saturating_sub(self.used_capacity_bytes);

        if self.total_capacity_bytes > 0 {
            self.utilization_percent =
                ((self.used_capacity_bytes as f64 / self.total_capacity_bytes as f64) * 100.0) as u32;
        } else {
            self.utilization_percent = 0;
        }

        self.total_capacity = format_bytes(self.total_capacity_bytes);
        self.drive_count = self.drives.len() as u32;

        // Count unique nodes
        let mut nodes: std::collections::HashSet<&str> = std::collections::HashSet::new();
        for drive in &self.drives {
            nodes.insert(&drive.node_name);
        }
        self.node_count = nodes.len() as u32;
    }

    /// Set a condition
    pub fn set_condition(&mut self, condition: PoolCondition) {
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

    /// Get online drive count
    pub fn online_drives(&self) -> usize {
        self.drives
            .iter()
            .filter(|d| d.status == PoolDriveStatus::Online)
            .count()
    }
}

/// Format bytes as human-readable string
fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;
    const TB: u64 = GB * 1024;
    const PB: u64 = TB * 1024;

    if bytes >= PB {
        format!("{:.2}PB", bytes as f64 / PB as f64)
    } else if bytes >= TB {
        format!("{:.2}TB", bytes as f64 / TB as f64)
    } else if bytes >= GB {
        format!("{:.2}GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2}MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2}KB", bytes as f64 / KB as f64)
    } else {
        format!("{}B", bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pool_type_display() {
        assert_eq!(format!("{}", PoolType::Block), "block");
        assert_eq!(format!("{}", PoolType::Object), "object");
    }

    #[test]
    fn test_tier_meets_minimum() {
        assert!(tier_meets_minimum(DriveTier::UltraFast, DriveTier::FastNvme));
        assert!(tier_meets_minimum(DriveTier::FastNvme, DriveTier::FastNvme));
        assert!(!tier_meets_minimum(DriveTier::StandardSsd, DriveTier::FastNvme));
        assert!(!tier_meets_minimum(DriveTier::Hdd, DriveTier::StandardSsd));
    }

    #[test]
    fn test_update_capacity() {
        let mut status = UnifiedPoolStatus::default();
        status.drives = vec![
            PoolDriveRef {
                node_name: "node-1".into(),
                drive_id: "nvme0n1".into(),
                device_path: "/dev/nvme0n1".into(),
                capacity_bytes: 1_000_000_000_000,
                used_bytes: 500_000_000_000,
                status: PoolDriveStatus::Online,
                added_at: None,
            },
            PoolDriveRef {
                node_name: "node-2".into(),
                drive_id: "nvme0n1".into(),
                device_path: "/dev/nvme0n1".into(),
                capacity_bytes: 1_000_000_000_000,
                used_bytes: 200_000_000_000,
                status: PoolDriveStatus::Online,
                added_at: None,
            },
        ];
        status.update_capacity();

        assert_eq!(status.total_capacity_bytes, 2_000_000_000_000);
        assert_eq!(status.used_capacity_bytes, 700_000_000_000);
        assert_eq!(status.available_capacity_bytes, 1_300_000_000_000);
        assert_eq!(status.utilization_percent, 35);
        assert_eq!(status.drive_count, 2);
        assert_eq!(status.node_count, 2);
    }
}

//! StorageNode CRD
//!
//! Represents a node's storage hardware inventory including drives,
//! their classification, and real-time metrics.

use chrono::{DateTime, Utc};
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

// =============================================================================
// StorageNode CRD
// =============================================================================

/// StorageNode tracks the storage hardware inventory and status for a cluster node.
/// It includes drive discovery results, performance classification, and real-time metrics.
#[derive(CustomResource, Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[kube(
    group = "storage.billyronks.io",
    version = "v1",
    kind = "StorageNode",
    plural = "storagenodes",
    shortname = "sn",
    status = "StorageNodeStatus",
    printcolumn = r#"{"name": "Node", "type": "string", "jsonPath": ".spec.nodeName"}"#,
    printcolumn = r#"{"name": "Drives", "type": "integer", "jsonPath": ".status.driveCount"}"#,
    printcolumn = r#"{"name": "NVMe", "type": "integer", "jsonPath": ".status.nvmeCount"}"#,
    printcolumn = r#"{"name": "Capacity", "type": "string", "jsonPath": ".status.totalCapacity"}"#,
    printcolumn = r#"{"name": "Phase", "type": "string", "jsonPath": ".status.phase"}"#,
    printcolumn = r#"{"name": "Age", "type": "date", "jsonPath": ".metadata.creationTimestamp"}"#,
    namespaced = false
)]
#[serde(rename_all = "camelCase")]
pub struct StorageNodeSpec {
    /// Name of the Kubernetes node
    pub node_name: String,

    /// Hostname of the node
    #[serde(default)]
    pub hostname: Option<String>,

    /// Node labels for scheduling
    #[serde(default)]
    pub labels: BTreeMap<String, String>,

    /// Fault domain for this node (rack, zone, etc.)
    #[serde(default)]
    pub fault_domain: Option<String>,

    /// Enable automatic hardware discovery
    #[serde(default = "default_true")]
    pub auto_discover: bool,

    /// Discovery interval in seconds
    #[serde(default = "default_discovery_interval")]
    pub discovery_interval_secs: u64,

    /// Drives to exclude from management (by device path)
    #[serde(default)]
    pub excluded_drives: Vec<String>,

    /// Manual drive overrides (for classification hints)
    #[serde(default)]
    pub drive_overrides: BTreeMap<String, DriveOverride>,
}

// =============================================================================
// Sub-Types
// =============================================================================

/// Override settings for a specific drive
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DriveOverride {
    /// Force a specific tier classification
    pub force_tier: Option<DriveTier>,

    /// Force a specific workload suitability
    pub force_workload: Option<WorkloadSuitability>,

    /// Exclude this drive from allocation
    #[serde(default)]
    pub exclude: bool,

    /// Custom labels for this drive
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
}

/// Drive performance tier
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum DriveTier {
    UltraFast,
    FastNvme,
    StandardSsd,
    Hdd,
}

impl std::fmt::Display for DriveTier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DriveTier::UltraFast => write!(f, "ultra-fast"),
            DriveTier::FastNvme => write!(f, "fast-nvme"),
            DriveTier::StandardSsd => write!(f, "standard-ssd"),
            DriveTier::Hdd => write!(f, "hdd"),
        }
    }
}

/// Workload suitability classification
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum WorkloadSuitability {
    BlockOptimized,
    ObjectOptimized,
    Mixed,
}

impl std::fmt::Display for WorkloadSuitability {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WorkloadSuitability::BlockOptimized => write!(f, "block-optimized"),
            WorkloadSuitability::ObjectOptimized => write!(f, "object-optimized"),
            WorkloadSuitability::Mixed => write!(f, "mixed"),
        }
    }
}

// =============================================================================
// Status
// =============================================================================

/// Status of the StorageNode
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct StorageNodeStatus {
    /// Current phase of the node
    #[serde(default)]
    pub phase: NodePhase,

    /// Total number of discovered drives
    #[serde(default)]
    pub drive_count: u32,

    /// Number of NVMe drives
    #[serde(default)]
    pub nvme_count: u32,

    /// Number of SSD drives
    #[serde(default)]
    pub ssd_count: u32,

    /// Number of HDD drives
    #[serde(default)]
    pub hdd_count: u32,

    /// Total capacity (human readable)
    #[serde(default)]
    pub total_capacity: String,

    /// Total capacity in bytes
    #[serde(default)]
    pub total_capacity_bytes: u64,

    /// Available capacity in bytes
    #[serde(default)]
    pub available_capacity_bytes: u64,

    /// Discovered drives with details
    #[serde(default)]
    pub drives: Vec<DriveStatus>,

    /// Node system information
    #[serde(default)]
    pub system_info: Option<SystemInfo>,

    /// Last discovery time
    #[serde(default)]
    #[schemars(with = "Option<String>")]
    pub last_discovery_time: Option<DateTime<Utc>>,

    /// Last heartbeat from node agent
    #[serde(default)]
    #[schemars(with = "Option<String>")]
    pub last_heartbeat_time: Option<DateTime<Utc>>,

    /// Conditions
    #[serde(default)]
    pub conditions: Vec<NodeCondition>,
}

/// Node lifecycle phase
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum NodePhase {
    #[default]
    Pending,
    Discovering,
    Ready,
    Degraded,
    Offline,
}

impl std::fmt::Display for NodePhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodePhase::Pending => write!(f, "Pending"),
            NodePhase::Discovering => write!(f, "Discovering"),
            NodePhase::Ready => write!(f, "Ready"),
            NodePhase::Degraded => write!(f, "Degraded"),
            NodePhase::Offline => write!(f, "Offline"),
        }
    }
}

/// Status of a single drive
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DriveStatus {
    /// Device ID (e.g., nvme0n1)
    pub id: String,

    /// Device path (e.g., /dev/nvme0n1)
    pub device_path: String,

    /// Drive type
    pub drive_type: DriveType,

    /// Model name
    pub model: String,

    /// Serial number
    pub serial: String,

    /// Firmware version
    #[serde(default)]
    pub firmware: String,

    /// Total capacity in bytes
    pub capacity_bytes: u64,

    /// Used capacity in bytes
    #[serde(default)]
    pub used_bytes: u64,

    /// NVMe namespaces (if NVMe drive)
    #[serde(default)]
    pub namespaces: Vec<NamespaceStatus>,

    /// Classification results
    pub classification: DriveClassification,

    /// Real-time metrics
    #[serde(default)]
    pub metrics: Option<DriveMetricsStatus>,

    /// SMART health data
    #[serde(default)]
    pub smart: Option<SmartStatus>,

    /// Pool this drive is assigned to (if any)
    #[serde(default)]
    pub pool_ref: Option<String>,

    /// Whether drive is healthy
    #[serde(default = "default_true")]
    pub healthy: bool,
}

/// Drive type from discovery
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
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

/// NVMe namespace status
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct NamespaceStatus {
    /// Namespace ID
    pub nsid: u32,

    /// Capacity in bytes
    pub capacity_bytes: u64,

    /// Whether namespace is active
    pub active: bool,

    /// Whether this is a ZNS namespace
    #[serde(default)]
    pub is_zns: bool,

    /// Pool reference if allocated
    #[serde(default)]
    pub pool_ref: Option<String>,
}

/// Drive classification results
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DriveClassification {
    /// Performance tier
    pub tier: Option<DriveTier>,

    /// Capacity tier
    pub capacity_tier: Option<CapacityTier>,

    /// Workload suitability
    pub workload: Option<WorkloadSuitability>,

    /// Suitable storage types
    #[serde(default)]
    pub suitable_for: Vec<String>,

    /// Classification confidence score (0-100)
    #[serde(default)]
    pub confidence_score: u32,

    /// Classification timestamp
    #[serde(default)]
    #[schemars(with = "Option<String>")]
    pub classified_at: Option<DateTime<Utc>>,
}

/// Capacity tier classification
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum CapacityTier {
    Small,
    Medium,
    Large,
}

impl std::fmt::Display for CapacityTier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CapacityTier::Small => write!(f, "small"),
            CapacityTier::Medium => write!(f, "medium"),
            CapacityTier::Large => write!(f, "large"),
        }
    }
}

/// Real-time drive metrics
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DriveMetricsStatus {
    /// Current IOPS
    #[serde(default)]
    pub iops: u64,

    /// Current throughput in bytes/sec
    #[serde(default)]
    pub throughput_bps: u64,

    /// P99 latency in microseconds
    #[serde(default)]
    pub latency_us_p99: u32,

    /// Utilization percentage
    #[serde(default)]
    pub utilization_percent: u32,

    /// Last update timestamp
    #[serde(default)]
    #[schemars(with = "Option<String>")]
    pub last_update: Option<DateTime<Utc>>,
}

/// SMART health status
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SmartStatus {
    /// Temperature in Celsius
    #[serde(default)]
    pub temperature_celsius: i32,

    /// Percentage of drive life used
    #[serde(default)]
    pub percentage_used: u8,

    /// Power-on hours
    #[serde(default)]
    pub power_on_hours: u64,

    /// Critical warning flags
    #[serde(default)]
    pub critical_warning: u8,

    /// Overall health status
    #[serde(default = "default_true")]
    pub healthy: bool,
}

/// System information
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SystemInfo {
    /// Total memory in bytes
    #[serde(default)]
    pub memory_bytes: u64,

    /// Available memory in bytes
    #[serde(default)]
    pub available_memory_bytes: u64,

    /// CPU count
    #[serde(default)]
    pub cpu_count: u32,

    /// Kernel version
    #[serde(default)]
    pub kernel_version: String,

    /// OS version
    #[serde(default)]
    pub os_version: String,
}

/// Node condition
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct NodeCondition {
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

fn default_true() -> bool {
    true
}

fn default_discovery_interval() -> u64 {
    300 // 5 minutes
}

// =============================================================================
// Implementations
// =============================================================================

impl StorageNode {
    /// Get the node name
    pub fn node_name(&self) -> &str {
        &self.spec.node_name
    }

    /// Check if node is ready
    pub fn is_ready(&self) -> bool {
        self.status
            .as_ref()
            .map(|s| s.phase == NodePhase::Ready)
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

    /// Get drives by type
    pub fn drives_by_type(&self, drive_type: DriveType) -> Vec<&DriveStatus> {
        self.status
            .as_ref()
            .map(|s| {
                s.drives
                    .iter()
                    .filter(|d| d.drive_type == drive_type)
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Get drives by tier
    pub fn drives_by_tier(&self, tier: DriveTier) -> Vec<&DriveStatus> {
        self.status
            .as_ref()
            .map(|s| {
                s.drives
                    .iter()
                    .filter(|d| d.classification.tier == Some(tier))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Get unallocated drives
    pub fn unallocated_drives(&self) -> Vec<&DriveStatus> {
        self.status
            .as_ref()
            .map(|s| {
                s.drives
                    .iter()
                    .filter(|d| d.pool_ref.is_none())
                    .collect()
            })
            .unwrap_or_default()
    }
}

impl StorageNodeStatus {
    /// Update drive counts from drives list
    pub fn update_counts(&mut self) {
        self.drive_count = self.drives.len() as u32;
        self.nvme_count = self.drives.iter().filter(|d| d.drive_type == DriveType::Nvme).count() as u32;
        self.ssd_count = self.drives.iter().filter(|d| d.drive_type == DriveType::Ssd).count() as u32;
        self.hdd_count = self.drives.iter().filter(|d| d.drive_type == DriveType::Hdd).count() as u32;
        self.total_capacity_bytes = self.drives.iter().map(|d| d.capacity_bytes).sum();
        self.available_capacity_bytes = self.drives
            .iter()
            .filter(|d| d.pool_ref.is_none())
            .map(|d| d.capacity_bytes)
            .sum();
        self.total_capacity = format_bytes(self.total_capacity_bytes);
    }

    /// Set a condition
    pub fn set_condition(&mut self, condition: NodeCondition) {
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
    fn test_format_bytes() {
        assert_eq!(format_bytes(500), "500B");
        assert_eq!(format_bytes(1024), "1.00KB");
        assert_eq!(format_bytes(1073741824), "1.00GB");
        assert_eq!(format_bytes(1099511627776), "1.00TB");
    }

    #[test]
    fn test_drive_tier_display() {
        assert_eq!(format!("{}", DriveTier::UltraFast), "ultra-fast");
        assert_eq!(format!("{}", DriveTier::FastNvme), "fast-nvme");
    }

    #[test]
    fn test_update_counts() {
        let mut status = StorageNodeStatus::default();
        status.drives = vec![
            DriveStatus {
                id: "nvme0n1".into(),
                device_path: "/dev/nvme0n1".into(),
                drive_type: DriveType::Nvme,
                model: "Samsung 980 Pro".into(),
                serial: "S123".into(),
                firmware: "1.0".into(),
                capacity_bytes: 1_000_000_000_000,
                used_bytes: 0,
                namespaces: vec![],
                classification: DriveClassification::default(),
                metrics: None,
                smart: None,
                pool_ref: None,
                healthy: true,
            },
            DriveStatus {
                id: "sda".into(),
                device_path: "/dev/sda".into(),
                drive_type: DriveType::Hdd,
                model: "WD Red".into(),
                serial: "W123".into(),
                firmware: "1.0".into(),
                capacity_bytes: 18_000_000_000_000,
                used_bytes: 0,
                namespaces: vec![],
                classification: DriveClassification::default(),
                metrics: None,
                smart: None,
                pool_ref: Some("cold-pool".into()),
                healthy: true,
            },
        ];
        status.update_counts();

        assert_eq!(status.drive_count, 2);
        assert_eq!(status.nvme_count, 1);
        assert_eq!(status.hdd_count, 1);
        assert_eq!(status.total_capacity_bytes, 19_000_000_000_000);
        assert_eq!(status.available_capacity_bytes, 1_000_000_000_000);
    }
}

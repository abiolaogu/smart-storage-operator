//! Allocation Policies
//!
//! Defines policies for how drives should be allocated to different
//! storage backends based on requirements.

use crate::crd::{DriveTier, DriveType, WorkloadSuitability};
use serde::{Deserialize, Serialize};

// =============================================================================
// Allocation Target
// =============================================================================

/// Target storage system for allocation
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AllocationTarget {
    /// Block storage (Mayastor/Couchestor)
    Block,
    /// Object storage (RustFS)
    Object,
    /// File storage (SeaweedFS)
    File,
    /// Cache tier
    Cache,
    /// General purpose (any)
    General,
}

impl std::fmt::Display for AllocationTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AllocationTarget::Block => write!(f, "block"),
            AllocationTarget::Object => write!(f, "object"),
            AllocationTarget::File => write!(f, "file"),
            AllocationTarget::Cache => write!(f, "cache"),
            AllocationTarget::General => write!(f, "general"),
        }
    }
}

// =============================================================================
// Placement Policy
// =============================================================================

/// Policy for spreading allocations across topology
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PlacementPolicy {
    /// Spread across nodes
    SpreadNodes,
    /// Spread across fault domains (racks)
    SpreadFaultDomains,
    /// Prefer same node (data locality)
    PreferSameNode,
    /// No preference (best fit)
    BestFit,
}

impl Default for PlacementPolicy {
    fn default() -> Self {
        PlacementPolicy::SpreadNodes
    }
}

// =============================================================================
// Fault Domain Policy
// =============================================================================

/// Policy for fault domain distribution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FaultDomainPolicy {
    /// Minimum number of distinct fault domains
    pub min_domains: u32,
    /// Require all replicas in different domains
    pub strict: bool,
    /// Domain level (node, rack, zone, region)
    pub domain_level: DomainLevel,
}

impl Default for FaultDomainPolicy {
    fn default() -> Self {
        Self {
            min_domains: 1,
            strict: false,
            domain_level: DomainLevel::Node,
        }
    }
}

/// Fault domain level
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DomainLevel {
    Node,
    Rack,
    Zone,
    Region,
}

// =============================================================================
// Allocation Policy
// =============================================================================

/// Complete allocation policy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AllocationPolicy {
    /// Target storage system
    pub target: AllocationTarget,

    /// Minimum performance tier required
    pub min_performance: Option<DriveTier>,

    /// Minimum capacity per drive (bytes)
    pub min_capacity_bytes: u64,

    /// Maximum capacity per drive (bytes)
    pub max_capacity_bytes: Option<u64>,

    /// Required drive types
    pub drive_types: Vec<DriveType>,

    /// Required workload suitability
    pub workload: Option<WorkloadSuitability>,

    /// Placement policy
    pub placement: PlacementPolicy,

    /// Fault domain requirements
    pub fault_domains: FaultDomainPolicy,

    /// Require ZNS support
    pub require_zns: bool,

    /// Minimum drive score (0-100)
    pub min_score: u32,

    /// Prefer enterprise drives
    pub prefer_enterprise: bool,

    /// Node affinity (prefer these nodes)
    pub node_affinity: Vec<String>,

    /// Node anti-affinity (avoid these nodes)
    pub node_anti_affinity: Vec<String>,
}

impl Default for AllocationPolicy {
    fn default() -> Self {
        Self {
            target: AllocationTarget::General,
            min_performance: None,
            min_capacity_bytes: 0,
            max_capacity_bytes: None,
            drive_types: Vec::new(),
            workload: None,
            placement: PlacementPolicy::SpreadNodes,
            fault_domains: FaultDomainPolicy::default(),
            require_zns: false,
            min_score: 0,
            prefer_enterprise: false,
            node_affinity: Vec::new(),
            node_anti_affinity: Vec::new(),
        }
    }
}

impl AllocationPolicy {
    /// Create a policy for block storage (Mayastor/Couchestor)
    pub fn for_block() -> Self {
        Self {
            target: AllocationTarget::Block,
            min_performance: Some(DriveTier::FastNvme),
            drive_types: vec![DriveType::Nvme],
            placement: PlacementPolicy::SpreadFaultDomains,
            fault_domains: FaultDomainPolicy {
                min_domains: 3,
                strict: true,
                domain_level: DomainLevel::Node,
            },
            workload: Some(WorkloadSuitability::BlockOptimized),
            min_score: 70,
            ..Default::default()
        }
    }

    /// Create a policy for object storage (RustFS)
    pub fn for_object() -> Self {
        Self {
            target: AllocationTarget::Object,
            min_performance: None, // Object storage accepts all tiers including HDD
            drive_types: vec![DriveType::Nvme, DriveType::Ssd, DriveType::Hdd],
            placement: PlacementPolicy::SpreadNodes,
            fault_domains: FaultDomainPolicy {
                min_domains: 1,
                strict: false,
                domain_level: DomainLevel::Node,
            },
            workload: Some(WorkloadSuitability::ObjectOptimized),
            min_score: 30,
            ..Default::default()
        }
    }

    /// Create a policy for file storage (SeaweedFS)
    pub fn for_file() -> Self {
        Self {
            target: AllocationTarget::File,
            min_performance: Some(DriveTier::StandardSsd),
            drive_types: vec![DriveType::Nvme, DriveType::Ssd],
            placement: PlacementPolicy::SpreadNodes,
            fault_domains: FaultDomainPolicy {
                min_domains: 2,
                strict: false,
                domain_level: DomainLevel::Node,
            },
            workload: Some(WorkloadSuitability::Mixed),
            min_score: 50,
            ..Default::default()
        }
    }

    /// Create a policy for cache tier
    pub fn for_cache() -> Self {
        Self {
            target: AllocationTarget::Cache,
            min_performance: Some(DriveTier::UltraFast),
            drive_types: vec![DriveType::Nvme],
            placement: PlacementPolicy::BestFit,
            fault_domains: FaultDomainPolicy::default(),
            workload: Some(WorkloadSuitability::BlockOptimized),
            min_score: 90,
            prefer_enterprise: true,
            ..Default::default()
        }
    }

    /// Create a policy for ZNS object storage
    pub fn for_zns_object() -> Self {
        Self {
            target: AllocationTarget::Object,
            min_performance: Some(DriveTier::FastNvme),
            drive_types: vec![DriveType::Nvme],
            placement: PlacementPolicy::SpreadNodes,
            fault_domains: FaultDomainPolicy {
                min_domains: 1,
                strict: false,
                domain_level: DomainLevel::Node,
            },
            require_zns: true,
            workload: Some(WorkloadSuitability::ObjectOptimized),
            min_score: 60,
            ..Default::default()
        }
    }

    /// Create a policy for high-capacity cold storage
    pub fn for_cold_storage() -> Self {
        Self {
            target: AllocationTarget::Object,
            min_performance: None,
            drive_types: vec![DriveType::Hdd],
            placement: PlacementPolicy::SpreadFaultDomains,
            fault_domains: FaultDomainPolicy {
                min_domains: 3,
                strict: true,
                domain_level: DomainLevel::Rack,
            },
            min_capacity_bytes: 10_000_000_000_000, // 10TB minimum
            workload: Some(WorkloadSuitability::ObjectOptimized),
            min_score: 20,
            ..Default::default()
        }
    }

    /// Check if a drive meets this policy's requirements
    pub fn matches_drive(
        &self,
        drive_type: DriveType,
        tier: Option<DriveTier>,
        workload: Option<WorkloadSuitability>,
        capacity_bytes: u64,
        score: u32,
        is_zns: bool,
    ) -> bool {
        // Check drive type
        if !self.drive_types.is_empty() && !self.drive_types.contains(&drive_type) {
            return false;
        }

        // Check performance tier
        if let (Some(min_tier), Some(actual_tier)) = (self.min_performance, tier) {
            if !tier_meets_minimum(actual_tier, min_tier) {
                return false;
            }
        }

        // Check workload suitability
        if let (Some(required), Some(actual)) = (self.workload, workload) {
            if !workload_compatible(actual, required) {
                return false;
            }
        }

        // Check capacity
        if capacity_bytes < self.min_capacity_bytes {
            return false;
        }
        if let Some(max) = self.max_capacity_bytes {
            if capacity_bytes > max {
                return false;
            }
        }

        // Check score
        if score < self.min_score {
            return false;
        }

        // Check ZNS
        if self.require_zns && !is_zns {
            return false;
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

/// Check if workloads are compatible
fn workload_compatible(actual: WorkloadSuitability, required: WorkloadSuitability) -> bool {
    match required {
        WorkloadSuitability::Mixed => true, // Mixed accepts anything
        WorkloadSuitability::BlockOptimized => {
            matches!(actual, WorkloadSuitability::BlockOptimized | WorkloadSuitability::Mixed)
        }
        WorkloadSuitability::ObjectOptimized => {
            matches!(actual, WorkloadSuitability::ObjectOptimized | WorkloadSuitability::Mixed)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_block_policy() {
        let policy = AllocationPolicy::for_block();

        // Fast NVMe should match
        assert!(policy.matches_drive(
            DriveType::Nvme,
            Some(DriveTier::FastNvme),
            Some(WorkloadSuitability::BlockOptimized),
            1_000_000_000_000,
            80,
            false
        ));

        // HDD should not match
        assert!(!policy.matches_drive(
            DriveType::Hdd,
            Some(DriveTier::Hdd),
            Some(WorkloadSuitability::ObjectOptimized),
            1_000_000_000_000,
            30,
            false
        ));
    }

    #[test]
    fn test_object_policy() {
        let policy = AllocationPolicy::for_object();

        // HDD should match for object storage
        assert!(policy.matches_drive(
            DriveType::Hdd,
            Some(DriveTier::Hdd),
            Some(WorkloadSuitability::ObjectOptimized),
            18_000_000_000_000,
            30,
            false
        ));
    }

    #[test]
    fn test_zns_policy() {
        let policy = AllocationPolicy::for_zns_object();

        // Non-ZNS should not match
        assert!(!policy.matches_drive(
            DriveType::Nvme,
            Some(DriveTier::FastNvme),
            Some(WorkloadSuitability::ObjectOptimized),
            8_000_000_000_000,
            70,
            false
        ));

        // ZNS should match
        assert!(policy.matches_drive(
            DriveType::Nvme,
            Some(DriveTier::FastNvme),
            Some(WorkloadSuitability::ObjectOptimized),
            8_000_000_000_000,
            70,
            true
        ));
    }

    #[test]
    fn test_tier_meets_minimum() {
        assert!(tier_meets_minimum(DriveTier::UltraFast, DriveTier::FastNvme));
        assert!(tier_meets_minimum(DriveTier::FastNvme, DriveTier::FastNvme));
        assert!(!tier_meets_minimum(DriveTier::StandardSsd, DriveTier::FastNvme));
        assert!(!tier_meets_minimum(DriveTier::Hdd, DriveTier::StandardSsd));
    }
}

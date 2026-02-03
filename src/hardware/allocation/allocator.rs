//! Main Allocation Engine
//!
//! Coordinates drive allocation across the cluster based on policies,
//! available hardware, and placement constraints.

use super::placement::{PlacementCandidate, PlacementEngine, PlacementResult};
use super::policy::AllocationPolicy;
use crate::crd::DriveStatus;
use crate::domain::ports::{AllocationConstraints, AllocationEngine, AllocationResult, StorageType};
use crate::error::{Error, Result};
use crate::hardware::classification::DeviceClassifier;
use crate::hardware::registry::NodeRegistry;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

// =============================================================================
// Allocation Record
// =============================================================================

/// Record of an allocation
#[derive(Debug, Clone)]
pub struct AllocationRecord {
    /// Allocation ID
    pub id: String,
    /// Node ID
    pub node_id: String,
    /// Drive ID
    pub drive_id: String,
    /// Pool name (if assigned to a pool)
    pub pool_name: Option<String>,
    /// Capacity allocated
    pub capacity_bytes: u64,
    /// Timestamp
    pub allocated_at: chrono::DateTime<chrono::Utc>,
}

// =============================================================================
// Drive Allocator
// =============================================================================

/// Main allocation engine implementation
pub struct DriveAllocator {
    /// Reference to node registry
    registry: Arc<NodeRegistry>,
    /// Device classifier
    classifier: DeviceClassifier,
    /// Active allocations
    allocations: RwLock<HashMap<String, AllocationRecord>>,
    /// Allocation counter for generating IDs
    allocation_counter: std::sync::atomic::AtomicU64,
}

impl DriveAllocator {
    /// Create a new allocator
    pub fn new(registry: Arc<NodeRegistry>) -> Arc<Self> {
        Arc::new(Self {
            registry,
            classifier: DeviceClassifier::new(),
            allocations: RwLock::new(HashMap::new()),
            allocation_counter: std::sync::atomic::AtomicU64::new(0),
        })
    }

    /// Generate a unique allocation ID
    fn generate_allocation_id(&self) -> String {
        let counter = self.allocation_counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        format!("alloc-{:016x}", counter)
    }

    /// Allocate drives based on policy
    pub async fn allocate_with_policy(
        &self,
        policy: &AllocationPolicy,
        count: usize,
    ) -> Result<Vec<AllocationResult>> {
        info!(
            "Allocating {} drives for target {:?} with policy",
            count, policy.target
        );

        // Gather candidates from all online nodes
        let candidates = self.gather_candidates(policy).await?;

        debug!("Found {} candidate drives", candidates.len());

        if candidates.is_empty() {
            return Err(Error::NoDrivesMatchPolicy {
                policy: format!("{:?}", policy.target),
            });
        }

        // Run placement algorithm
        let placement = PlacementEngine::select(
            &candidates,
            count,
            &policy.placement,
            &policy.fault_domains,
        )?;

        info!(
            "Placement selected {} drives across {} nodes",
            placement.selected.len(),
            placement.nodes_used.len()
        );

        // Convert to allocation results
        let mut results = Vec::new();
        let mut allocations = self.allocations.write().await;

        for candidate in placement.selected {
            let alloc_id = self.generate_allocation_id();

            let record = AllocationRecord {
                id: alloc_id.clone(),
                node_id: candidate.node_id.clone(),
                drive_id: candidate.drive_id.clone(),
                pool_name: None,
                capacity_bytes: candidate.capacity_bytes,
                allocated_at: chrono::Utc::now(),
            };

            allocations.insert(alloc_id.clone(), record);

            results.push(AllocationResult {
                drive_id: candidate.drive_id,
                node_id: candidate.node_id,
                capacity_bytes: candidate.capacity_bytes,
                pool_name: None,
            });
        }

        Ok(results)
    }

    /// Gather candidate drives that match a policy
    async fn gather_candidates(&self, policy: &AllocationPolicy) -> Result<Vec<PlacementCandidate>> {
        let mut candidates = Vec::new();

        // Get all online nodes
        let node_ids = self.registry.online_node_ids();

        for node_id in node_ids {
            // Check node affinity/anti-affinity
            let node_str = node_id.to_string();

            if !policy.node_anti_affinity.is_empty()
                && policy.node_anti_affinity.contains(&node_str)
            {
                continue;
            }

            if !policy.node_affinity.is_empty()
                && !policy.node_affinity.contains(&node_str)
            {
                continue;
            }

            // Get node entry
            if let Some(entry) = self.registry.get(node_id.clone()) {
                for drive in entry.drives() {
                    // Skip already allocated drives
                    if drive.pool_ref.is_some() {
                        continue;
                    }

                    // Skip unhealthy drives
                    if !drive.healthy {
                        continue;
                    }

                    // Get classification
                    let tier = drive.classification.tier;
                    let workload = drive.classification.workload;
                    let score = drive.classification.confidence_score;

                    // Check ZNS if available
                    let is_zns = drive.namespaces.iter().any(|ns| ns.is_zns);

                    // Check if drive matches policy
                    if policy.matches_drive(
                        drive.drive_type,
                        tier,
                        workload,
                        drive.capacity_bytes,
                        score,
                        is_zns,
                    ) {
                        candidates.push(PlacementCandidate {
                            node_id: node_str.clone(),
                            drive_id: drive.id.clone(),
                            fault_domain: entry.fault_domain.clone(),
                            capacity_bytes: drive.capacity_bytes,
                            score,
                            enterprise: is_enterprise_model(&drive.model),
                        });
                    }
                }
            }
        }

        // Sort by score and enterprise preference
        candidates.sort_by(|a, b| {
            // Enterprise first if preferred
            if policy.prefer_enterprise && a.enterprise != b.enterprise {
                return b.enterprise.cmp(&a.enterprise);
            }
            // Then by score
            b.score.cmp(&a.score)
        });

        Ok(candidates)
    }

    /// Get an allocation by ID
    pub async fn get_allocation(&self, alloc_id: &str) -> Option<AllocationRecord> {
        self.allocations.read().await.get(alloc_id).cloned()
    }

    /// Release an allocation
    pub async fn release_allocation(&self, alloc_id: &str) -> Result<()> {
        let mut allocations = self.allocations.write().await;

        if allocations.remove(alloc_id).is_some() {
            info!("Released allocation {}", alloc_id);
            Ok(())
        } else {
            Err(Error::AllocationFailed(format!(
                "Allocation not found: {}",
                alloc_id
            )))
        }
    }

    /// Get total available capacity matching constraints
    pub async fn available_capacity(&self, constraints: &AllocationConstraints) -> Result<u64> {
        let node_ids = self.registry.online_node_ids();
        let mut total = 0u64;

        for node_id in node_ids {
            // Check node exclusions
            let node_str = node_id.to_string();
            if constraints.exclude_nodes.contains(&node_str) {
                continue;
            }

            if let Some(entry) = self.registry.get(node_id) {
                for drive in entry.drives() {
                    // Skip allocated drives
                    if drive.pool_ref.is_some() {
                        continue;
                    }

                    // Skip unhealthy
                    if !drive.healthy {
                        continue;
                    }

                    // Check capacity
                    if drive.capacity_bytes < constraints.min_capacity_bytes {
                        continue;
                    }

                    // Check drive type
                    if let Some(required_type) = constraints.drive_type {
                        let actual_type = match drive.drive_type {
                            crate::crd::DriveType::Nvme => crate::domain::ports::DriveType::Nvme,
                            crate::crd::DriveType::Ssd => crate::domain::ports::DriveType::Ssd,
                            crate::crd::DriveType::Hdd => crate::domain::ports::DriveType::Hdd,
                            crate::crd::DriveType::Unknown => crate::domain::ports::DriveType::Unknown,
                        };
                        if actual_type != required_type {
                            continue;
                        }
                    }

                    total += drive.capacity_bytes;
                }
            }
        }

        Ok(total)
    }

    /// Get allocation statistics
    pub async fn stats(&self) -> AllocationStats {
        let allocations = self.allocations.read().await;

        let mut total_allocated = 0u64;
        let mut nodes_with_allocations = std::collections::HashSet::new();

        for record in allocations.values() {
            total_allocated += record.capacity_bytes;
            nodes_with_allocations.insert(record.node_id.clone());
        }

        AllocationStats {
            total_allocations: allocations.len(),
            total_allocated_bytes: total_allocated,
            nodes_with_allocations: nodes_with_allocations.len(),
        }
    }
}

/// Allocation statistics
#[derive(Debug, Clone)]
pub struct AllocationStats {
    pub total_allocations: usize,
    pub total_allocated_bytes: u64,
    pub nodes_with_allocations: usize,
}

/// Check if a model is enterprise-grade
fn is_enterprise_model(model: &str) -> bool {
    let model_upper = model.to_uppercase();
    model_upper.contains("PM1733")
        || model_upper.contains("PM1735")
        || model_upper.contains("P5510")
        || model_upper.contains("P5520")
        || model_upper.contains("ULTRASTAR")
        || model_upper.contains("EXOS")
        || model_upper.contains("OPTANE")
        || model_upper.contains("9400")
}

// =============================================================================
// AllocationEngine Implementation
// =============================================================================

#[async_trait]
impl AllocationEngine for DriveAllocator {
    async fn allocate(
        &self,
        storage_type: StorageType,
        constraints: AllocationConstraints,
        count: usize,
    ) -> Result<Vec<AllocationResult>> {
        // Convert storage type to policy
        let mut policy = match storage_type {
            StorageType::Block => AllocationPolicy::for_block(),
            StorageType::File => AllocationPolicy::for_file(),
            StorageType::Object => AllocationPolicy::for_object(),
        };

        // Apply constraints to policy
        policy.min_capacity_bytes = constraints.min_capacity_bytes;
        if let Some(drive_type) = constraints.drive_type {
            let crd_type = match drive_type {
                crate::domain::ports::DriveType::Nvme => crate::crd::DriveType::Nvme,
                crate::domain::ports::DriveType::Ssd => crate::crd::DriveType::Ssd,
                crate::domain::ports::DriveType::Hdd => crate::crd::DriveType::Hdd,
                crate::domain::ports::DriveType::Unknown => crate::crd::DriveType::Unknown,
            };
            policy.drive_types = vec![crd_type];
        }
        policy.node_anti_affinity = constraints.exclude_nodes;
        policy.node_affinity = constraints.prefer_nodes;
        if let Some(min_fd) = constraints.min_fault_domains {
            policy.fault_domains.min_domains = min_fd;
        }

        self.allocate_with_policy(&policy, count).await
    }

    async fn release(&self, allocation_ids: &[String]) -> Result<()> {
        for id in allocation_ids {
            self.release_allocation(id).await?;
        }
        Ok(())
    }

    async fn get_allocation(&self, allocation_id: &str) -> Result<Option<AllocationResult>> {
        Ok(self.get_allocation(allocation_id).await.map(|record| {
            AllocationResult {
                drive_id: record.drive_id,
                node_id: record.node_id,
                capacity_bytes: record.capacity_bytes,
                pool_name: record.pool_name,
            }
        }))
    }

    async fn available_capacity(&self, constraints: AllocationConstraints) -> Result<u64> {
        self.available_capacity(&constraints).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_enterprise_model() {
        assert!(is_enterprise_model("Samsung PM1733"));
        assert!(is_enterprise_model("Intel Optane P5800X"));
        assert!(is_enterprise_model("Seagate Exos X18"));
        assert!(!is_enterprise_model("Samsung 980 PRO"));
        assert!(!is_enterprise_model("WD Red"));
    }
}

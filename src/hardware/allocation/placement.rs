//! Placement Strategy
//!
//! Implements placement algorithms for distributing allocations
//! across nodes and fault domains.

use super::policy::{DomainLevel, FaultDomainPolicy, PlacementPolicy};
use crate::error::{Error, Result};
use std::collections::{HashMap, HashSet};

// =============================================================================
// Placement Candidate
// =============================================================================

/// A candidate drive for allocation
#[derive(Debug, Clone)]
pub struct PlacementCandidate {
    /// Node ID
    pub node_id: String,
    /// Drive ID
    pub drive_id: String,
    /// Fault domain (rack, zone, etc.)
    pub fault_domain: Option<String>,
    /// Capacity in bytes
    pub capacity_bytes: u64,
    /// Score (0-100)
    pub score: u32,
    /// Is enterprise grade
    pub enterprise: bool,
}

// =============================================================================
// Placement Result
// =============================================================================

/// Result of placement calculation
#[derive(Debug, Clone)]
pub struct PlacementResult {
    /// Selected candidates
    pub selected: Vec<PlacementCandidate>,
    /// Reason for selection
    pub reason: String,
    /// Fault domains covered
    pub fault_domains: HashSet<String>,
    /// Nodes used
    pub nodes_used: HashSet<String>,
}

// =============================================================================
// Placement Engine
// =============================================================================

/// Engine for calculating optimal placement
pub struct PlacementEngine;

impl PlacementEngine {
    /// Select drives based on policy
    pub fn select(
        candidates: &[PlacementCandidate],
        count: usize,
        policy: &PlacementPolicy,
        fault_domain_policy: &FaultDomainPolicy,
    ) -> Result<PlacementResult> {
        if candidates.is_empty() {
            return Err(Error::NoDrivesMatchPolicy {
                policy: "no candidates available".into(),
            });
        }

        if count == 0 {
            return Ok(PlacementResult {
                selected: Vec::new(),
                reason: "zero count requested".into(),
                fault_domains: HashSet::new(),
                nodes_used: HashSet::new(),
            });
        }

        let selected = match policy {
            PlacementPolicy::SpreadNodes => {
                Self::spread_across_nodes(candidates, count, fault_domain_policy)?
            }
            PlacementPolicy::SpreadFaultDomains => {
                Self::spread_across_fault_domains(candidates, count, fault_domain_policy)?
            }
            PlacementPolicy::PreferSameNode => {
                Self::prefer_same_node(candidates, count)?
            }
            PlacementPolicy::BestFit => {
                Self::best_fit(candidates, count)?
            }
        };

        // Collect metadata
        let mut fault_domains = HashSet::new();
        let mut nodes_used = HashSet::new();
        for candidate in &selected {
            nodes_used.insert(candidate.node_id.clone());
            if let Some(fd) = &candidate.fault_domain {
                fault_domains.insert(fd.clone());
            }
        }

        // Verify fault domain requirements
        if fault_domain_policy.strict {
            let required = fault_domain_policy.min_domains as usize;
            let actual = fault_domains.len().max(nodes_used.len()); // Use nodes if no FDs
            if actual < required && selected.len() >= required {
                return Err(Error::PlacementConstraintViolated {
                    constraint: format!(
                        "need {} fault domains, only {} available",
                        required, actual
                    ),
                });
            }
        }

        let reason = format!(
            "selected {} drives across {} nodes",
            selected.len(),
            nodes_used.len()
        );

        Ok(PlacementResult {
            selected,
            reason,
            fault_domains,
            nodes_used,
        })
    }

    /// Spread allocations across different nodes
    fn spread_across_nodes(
        candidates: &[PlacementCandidate],
        count: usize,
        fault_domain_policy: &FaultDomainPolicy,
    ) -> Result<Vec<PlacementCandidate>> {
        // Group candidates by node
        let mut by_node: HashMap<&str, Vec<&PlacementCandidate>> = HashMap::new();
        for candidate in candidates {
            by_node.entry(&candidate.node_id).or_default().push(candidate);
        }

        // Sort nodes by number of candidates (prefer nodes with more options)
        let mut nodes: Vec<_> = by_node.keys().cloned().collect();
        nodes.sort_by(|a, b| {
            by_node[b].len().cmp(&by_node[a].len())
        });

        let mut selected = Vec::new();
        let mut used_nodes: HashSet<&str> = HashSet::new();
        let mut node_idx = 0;

        while selected.len() < count {
            if nodes.is_empty() {
                break;
            }

            let node = nodes[node_idx % nodes.len()];

            // Find best unused candidate from this node
            if let Some(candidates) = by_node.get_mut(node) {
                if let Some(pos) = candidates.iter().position(|c| {
                    !selected.iter().any(|s: &PlacementCandidate| {
                        s.node_id == c.node_id && s.drive_id == c.drive_id
                    })
                }) {
                    let candidate = candidates.remove(pos);
                    selected.push(candidate.clone());
                    used_nodes.insert(node);
                }

                // Remove empty nodes
                if candidates.is_empty() {
                    let node_str = node.to_string();
                    nodes.retain(|n| *n != node);
                    by_node.remove(node_str.as_str());
                }
            }

            node_idx += 1;

            // Avoid infinite loop
            if node_idx > count * 10 {
                break;
            }
        }

        // Fill remaining if needed (allow multiple per node)
        if selected.len() < count {
            for candidate in candidates {
                if selected.len() >= count {
                    break;
                }
                if !selected.iter().any(|s| {
                    s.node_id == candidate.node_id && s.drive_id == candidate.drive_id
                }) {
                    selected.push(candidate.clone());
                }
            }
        }

        Ok(selected)
    }

    /// Spread allocations across different fault domains
    fn spread_across_fault_domains(
        candidates: &[PlacementCandidate],
        count: usize,
        fault_domain_policy: &FaultDomainPolicy,
    ) -> Result<Vec<PlacementCandidate>> {
        // Group candidates by fault domain
        let mut by_domain: HashMap<String, Vec<&PlacementCandidate>> = HashMap::new();
        for candidate in candidates {
            let domain = candidate.fault_domain.clone()
                .unwrap_or_else(|| candidate.node_id.clone());
            by_domain.entry(domain).or_default().push(candidate);
        }

        // Sort domains by number of candidates
        let mut domains: Vec<_> = by_domain.keys().cloned().collect();
        domains.sort_by(|a, b| {
            by_domain[b].len().cmp(&by_domain[a].len())
        });

        let mut selected = Vec::new();
        let mut domain_idx = 0;

        while selected.len() < count && !domains.is_empty() {
            let domain = &domains[domain_idx % domains.len()];
            let domain_clone = domain.clone();
            let mut removed_domain = false;

            if let Some(candidates) = by_domain.get_mut(&domain_clone) {
                if let Some(pos) = candidates.iter().position(|c| {
                    !selected.iter().any(|s: &PlacementCandidate| {
                        s.node_id == c.node_id && s.drive_id == c.drive_id
                    })
                }) {
                    let candidate = candidates.remove(pos);
                    selected.push(candidate.clone());
                }

                if candidates.is_empty() {
                    domains.retain(|d| d != &domain_clone);
                    by_domain.remove(&domain_clone);
                    removed_domain = true;
                }
            }

            // Only increment if we didn't remove the current domain
            // This ensures we try the next domain in sequence
            if !removed_domain || domains.is_empty() {
                domain_idx += 1;
            }

            if domain_idx > count * 10 {
                break;
            }
        }

        Ok(selected)
    }

    /// Prefer allocations on the same node (data locality)
    fn prefer_same_node(
        candidates: &[PlacementCandidate],
        count: usize,
    ) -> Result<Vec<PlacementCandidate>> {
        // Group by node and find node with most candidates
        let mut by_node: HashMap<&str, Vec<&PlacementCandidate>> = HashMap::new();
        for candidate in candidates {
            by_node.entry(&candidate.node_id).or_default().push(candidate);
        }

        // Sort nodes by candidate count (descending) then by total score
        let mut nodes: Vec<_> = by_node.iter().collect();
        nodes.sort_by(|(_, a), (_, b)| {
            let a_count = a.len();
            let b_count = b.len();
            if a_count != b_count {
                b_count.cmp(&a_count)
            } else {
                let a_score: u32 = a.iter().map(|c| c.score).sum();
                let b_score: u32 = b.iter().map(|c| c.score).sum();
                b_score.cmp(&a_score)
            }
        });

        let mut selected = Vec::new();

        // Try to fill from single node first
        if let Some((_, node_candidates)) = nodes.first() {
            for candidate in node_candidates.iter().take(count) {
                selected.push((*candidate).clone());
            }
        }

        // Fill remaining from other nodes if needed
        if selected.len() < count {
            for candidate in candidates {
                if selected.len() >= count {
                    break;
                }
                if !selected.iter().any(|s| {
                    s.node_id == candidate.node_id && s.drive_id == candidate.drive_id
                }) {
                    selected.push(candidate.clone());
                }
            }
        }

        Ok(selected)
    }

    /// Best fit - select highest scoring candidates
    fn best_fit(
        candidates: &[PlacementCandidate],
        count: usize,
    ) -> Result<Vec<PlacementCandidate>> {
        let mut sorted: Vec<_> = candidates.iter().collect();

        // Sort by score (descending), then by capacity (descending)
        sorted.sort_by(|a, b| {
            if a.score != b.score {
                b.score.cmp(&a.score)
            } else {
                b.capacity_bytes.cmp(&a.capacity_bytes)
            }
        });

        let selected: Vec<PlacementCandidate> = sorted
            .into_iter()
            .take(count)
            .cloned()
            .collect();

        Ok(selected)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_candidate(node: &str, drive: &str, domain: Option<&str>, score: u32) -> PlacementCandidate {
        PlacementCandidate {
            node_id: node.to_string(),
            drive_id: drive.to_string(),
            fault_domain: domain.map(|s| s.to_string()),
            capacity_bytes: 1_000_000_000_000,
            score,
            enterprise: false,
        }
    }

    #[test]
    fn test_spread_across_nodes() {
        let candidates = vec![
            make_candidate("node-1", "nvme0n1", Some("rack-1"), 80),
            make_candidate("node-1", "nvme1n1", Some("rack-1"), 75),
            make_candidate("node-2", "nvme0n1", Some("rack-2"), 85),
            make_candidate("node-2", "nvme1n1", Some("rack-2"), 70),
            make_candidate("node-3", "nvme0n1", Some("rack-3"), 90),
        ];

        let result = PlacementEngine::select(
            &candidates,
            3,
            &PlacementPolicy::SpreadNodes,
            &FaultDomainPolicy::default(),
        ).unwrap();

        // Should select from 3 different nodes
        assert_eq!(result.selected.len(), 3);
        assert_eq!(result.nodes_used.len(), 3);
    }

    #[test]
    fn test_spread_across_fault_domains() {
        let candidates = vec![
            make_candidate("node-1", "nvme0n1", Some("rack-1"), 80),
            make_candidate("node-2", "nvme0n1", Some("rack-1"), 75),
            make_candidate("node-3", "nvme0n1", Some("rack-2"), 85),
            make_candidate("node-4", "nvme0n1", Some("rack-3"), 90),
        ];

        let result = PlacementEngine::select(
            &candidates,
            3,
            &PlacementPolicy::SpreadFaultDomains,
            &FaultDomainPolicy {
                min_domains: 3,
                strict: true,
                domain_level: DomainLevel::Rack,
            },
        ).unwrap();

        // Should select from 3 different fault domains
        assert_eq!(result.selected.len(), 3);
        assert_eq!(result.fault_domains.len(), 3);
    }

    #[test]
    fn test_prefer_same_node() {
        let candidates = vec![
            make_candidate("node-1", "nvme0n1", None, 80),
            make_candidate("node-1", "nvme1n1", None, 75),
            make_candidate("node-1", "nvme2n1", None, 70),
            make_candidate("node-2", "nvme0n1", None, 90),
        ];

        let result = PlacementEngine::select(
            &candidates,
            3,
            &PlacementPolicy::PreferSameNode,
            &FaultDomainPolicy::default(),
        ).unwrap();

        // Should prefer node-1 which has the most candidates
        assert_eq!(result.selected.len(), 3);
        let node1_count = result.selected.iter()
            .filter(|c| c.node_id == "node-1")
            .count();
        assert_eq!(node1_count, 3);
    }

    #[test]
    fn test_best_fit() {
        let candidates = vec![
            make_candidate("node-1", "nvme0n1", None, 60),
            make_candidate("node-2", "nvme0n1", None, 90),
            make_candidate("node-3", "nvme0n1", None, 80),
            make_candidate("node-4", "nvme0n1", None, 70),
        ];

        let result = PlacementEngine::select(
            &candidates,
            2,
            &PlacementPolicy::BestFit,
            &FaultDomainPolicy::default(),
        ).unwrap();

        // Should select highest scoring (90 and 80)
        assert_eq!(result.selected.len(), 2);
        assert!(result.selected.iter().any(|c| c.score == 90));
        assert!(result.selected.iter().any(|c| c.score == 80));
    }
}

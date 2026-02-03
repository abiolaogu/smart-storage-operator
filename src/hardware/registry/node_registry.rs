//! Sharded Node Registry
//!
//! A 256-way sharded registry for tracking storage nodes and their hardware.
//! Optimized for high-throughput updates using Data-Oriented Design principles.

use crate::crd::{DriveStatus, StorageNodeStatus};
use crate::error::{Error, Result};
use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::broadcast;

// =============================================================================
// Constants
// =============================================================================

/// Number of shards in the registry (256 for good distribution)
pub const REGISTRY_SHARD_COUNT: usize = 256;

/// Cache line size for alignment
const CACHE_LINE_SIZE: usize = 64;

// =============================================================================
// Node ID
// =============================================================================

/// Unique identifier for a node
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NodeId(pub String);

impl NodeId {
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }

    /// Get the shard index for this node ID
    #[inline]
    pub fn shard_index(&self) -> usize {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        self.0.hash(&mut hasher);
        (hasher.finish() as usize) % REGISTRY_SHARD_COUNT
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for NodeId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for NodeId {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

impl From<&String> for NodeId {
    fn from(s: &String) -> Self {
        Self(s.clone())
    }
}

// =============================================================================
// Cache-Line Aligned Drive Metrics (DOD)
// =============================================================================

/// Drive metrics optimized for cache-line access
/// Aligned to 64 bytes to prevent false sharing
#[repr(C, align(64))]
#[derive(Debug)]
pub struct DriveMetrics {
    /// Current IOPS
    pub iops: AtomicU64,
    /// Throughput in bytes per second
    pub throughput_bps: AtomicU64,
    /// P99 latency in microseconds (stored as u64 for atomicity)
    pub latency_us_p99: AtomicU64,
    /// Utilization percentage * 100 (for precision without floats)
    pub utilization_permille: AtomicU64,
    /// Temperature in Celsius * 10 (for precision)
    pub temperature_decidegrees: AtomicU64,
    /// Wear level percentage * 100
    pub wear_level_permille: AtomicU64,
    /// Last update timestamp (Unix millis)
    pub last_update_ms: AtomicU64,
    /// Padding to fill cache line
    _padding: [u8; 8],
}

impl Default for DriveMetrics {
    fn default() -> Self {
        Self {
            iops: AtomicU64::new(0),
            throughput_bps: AtomicU64::new(0),
            latency_us_p99: AtomicU64::new(0),
            utilization_permille: AtomicU64::new(0),
            temperature_decidegrees: AtomicU64::new(0),
            wear_level_permille: AtomicU64::new(0),
            last_update_ms: AtomicU64::new(0),
            _padding: [0; 8],
        }
    }
}

impl DriveMetrics {
    /// Update all metrics atomically
    pub fn update(
        &self,
        iops: u64,
        throughput_bps: u64,
        latency_us_p99: u32,
        utilization_percent: f32,
        temperature_celsius: i32,
        wear_level_percent: u8,
    ) {
        self.iops.store(iops, Ordering::Relaxed);
        self.throughput_bps.store(throughput_bps, Ordering::Relaxed);
        self.latency_us_p99.store(latency_us_p99 as u64, Ordering::Relaxed);
        self.utilization_permille.store((utilization_percent * 10.0) as u64, Ordering::Relaxed);
        self.temperature_decidegrees.store((temperature_celsius * 10) as u64, Ordering::Relaxed);
        self.wear_level_permille.store((wear_level_percent as u64) * 10, Ordering::Relaxed);
        self.last_update_ms.store(
            Utc::now().timestamp_millis() as u64,
            Ordering::Release,
        );
    }

    /// Get IOPS
    #[inline]
    pub fn get_iops(&self) -> u64 {
        self.iops.load(Ordering::Relaxed)
    }

    /// Get throughput in bytes/sec
    #[inline]
    pub fn get_throughput_bps(&self) -> u64 {
        self.throughput_bps.load(Ordering::Relaxed)
    }

    /// Get P99 latency in microseconds
    #[inline]
    pub fn get_latency_us_p99(&self) -> u32 {
        self.latency_us_p99.load(Ordering::Relaxed) as u32
    }

    /// Get utilization percentage
    #[inline]
    pub fn get_utilization_percent(&self) -> f32 {
        self.utilization_permille.load(Ordering::Relaxed) as f32 / 10.0
    }

    /// Get temperature in Celsius
    #[inline]
    pub fn get_temperature_celsius(&self) -> i32 {
        (self.temperature_decidegrees.load(Ordering::Relaxed) as i32) / 10
    }

    /// Get wear level percentage
    #[inline]
    pub fn get_wear_level_percent(&self) -> u8 {
        (self.wear_level_permille.load(Ordering::Relaxed) / 10) as u8
    }

    /// Check if metrics are stale (no update in last N seconds)
    pub fn is_stale(&self, max_age_secs: u64) -> bool {
        let last_update = self.last_update_ms.load(Ordering::Acquire);
        if last_update == 0 {
            return true;
        }
        let now_ms = Utc::now().timestamp_millis() as u64;
        let age_ms = now_ms.saturating_sub(last_update);
        age_ms > (max_age_secs * 1000)
    }
}

// =============================================================================
// Node Entry
// =============================================================================

/// Entry for a single node in the registry
#[derive(Debug)]
pub struct NodeEntry {
    /// Node ID
    pub node_id: NodeId,
    /// Hostname
    pub hostname: String,
    /// Node status from CRD
    pub status: StorageNodeStatus,
    /// Per-drive metrics (indexed by drive ID)
    pub drive_metrics: HashMap<String, Arc<DriveMetrics>>,
    /// Registration timestamp
    pub registered_at: DateTime<Utc>,
    /// Last heartbeat timestamp
    pub last_heartbeat: DateTime<Utc>,
    /// Is node online
    pub online: bool,
    /// Node labels
    pub labels: HashMap<String, String>,
    /// Fault domain
    pub fault_domain: Option<String>,
}

impl NodeEntry {
    /// Create a new node entry
    pub fn new(node_id: NodeId, hostname: String, status: StorageNodeStatus) -> Self {
        let now = Utc::now();
        let mut drive_metrics = HashMap::new();

        // Initialize metrics for each drive
        for drive in &status.drives {
            drive_metrics.insert(drive.id.clone(), Arc::new(DriveMetrics::default()));
        }

        Self {
            node_id,
            hostname,
            status,
            drive_metrics,
            registered_at: now,
            last_heartbeat: now,
            online: true,
            labels: HashMap::new(),
            fault_domain: None,
        }
    }

    /// Update node status
    pub fn update_status(&mut self, status: StorageNodeStatus) {
        // Add metrics for any new drives
        for drive in &status.drives {
            if !self.drive_metrics.contains_key(&drive.id) {
                self.drive_metrics.insert(drive.id.clone(), Arc::new(DriveMetrics::default()));
            }
        }

        // Remove metrics for removed drives
        let current_ids: std::collections::HashSet<_> = status.drives.iter().map(|d| &d.id).collect();
        self.drive_metrics.retain(|id, _| current_ids.contains(id));

        self.status = status;
        self.last_heartbeat = Utc::now();
    }

    /// Record heartbeat
    pub fn heartbeat(&mut self) {
        self.last_heartbeat = Utc::now();
        self.online = true;
    }

    /// Mark node as offline
    pub fn mark_offline(&mut self) {
        self.online = false;
    }

    /// Get drive metrics by ID
    pub fn get_drive_metrics(&self, drive_id: &str) -> Option<Arc<DriveMetrics>> {
        self.drive_metrics.get(drive_id).cloned()
    }

    /// Get total capacity in bytes
    pub fn total_capacity_bytes(&self) -> u64 {
        self.status.total_capacity_bytes
    }

    /// Get available capacity in bytes
    pub fn available_capacity_bytes(&self) -> u64 {
        self.status.available_capacity_bytes
    }

    /// Get drives
    pub fn drives(&self) -> &[DriveStatus] {
        &self.status.drives
    }
}

// =============================================================================
// Shard Statistics
// =============================================================================

/// Statistics for a single shard
#[repr(C, align(64))]
#[derive(Debug)]
pub struct ShardStats {
    /// Number of nodes in shard
    pub node_count: AtomicU64,
    /// Total updates to this shard
    pub update_count: AtomicU64,
    /// Lock contention count
    pub contention_count: AtomicU64,
}

impl Default for ShardStats {
    fn default() -> Self {
        Self {
            node_count: AtomicU64::new(0),
            update_count: AtomicU64::new(0),
            contention_count: AtomicU64::new(0),
        }
    }
}

// =============================================================================
// Registry Shard
// =============================================================================

/// A single shard of the registry
#[repr(C, align(64))]
pub struct RegistryShard {
    /// Nodes in this shard
    nodes: RwLock<HashMap<NodeId, NodeEntry>>,
    /// Shard statistics
    stats: ShardStats,
}

impl std::fmt::Debug for RegistryShard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegistryShard")
            .field("node_count", &self.stats.node_count.load(Ordering::Relaxed))
            .finish()
    }
}

impl RegistryShard {
    fn new() -> Self {
        Self {
            nodes: RwLock::new(HashMap::new()),
            stats: ShardStats::default(),
        }
    }

    /// Get node count
    fn node_count(&self) -> usize {
        self.stats.node_count.load(Ordering::Relaxed) as usize
    }

    /// Try to insert a node
    fn insert(&self, node_id: NodeId, entry: NodeEntry) -> Result<()> {
        let mut nodes = self.nodes.write();
        if nodes.contains_key(&node_id) {
            return Err(Error::NodeAlreadyRegistered {
                node_id: node_id.to_string(),
            });
        }
        nodes.insert(node_id, entry);
        self.stats.node_count.fetch_add(1, Ordering::Relaxed);
        self.stats.update_count.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Get a node
    fn get(&self, node_id: &NodeId) -> Option<NodeEntry> {
        self.nodes.read().get(node_id).cloned()
    }

    /// Update a node's status
    fn update_status(&self, node_id: &NodeId, status: StorageNodeStatus) -> Result<()> {
        let mut nodes = self.nodes.write();
        if let Some(entry) = nodes.get_mut(node_id) {
            entry.update_status(status);
            self.stats.update_count.fetch_add(1, Ordering::Relaxed);
            Ok(())
        } else {
            Err(Error::NodeNotFound {
                node_id: node_id.to_string(),
            })
        }
    }

    /// Record heartbeat for a node
    fn heartbeat(&self, node_id: &NodeId) -> Result<()> {
        let mut nodes = self.nodes.write();
        if let Some(entry) = nodes.get_mut(node_id) {
            entry.heartbeat();
            Ok(())
        } else {
            Err(Error::NodeNotFound {
                node_id: node_id.to_string(),
            })
        }
    }

    /// Remove a node
    fn remove(&self, node_id: &NodeId) -> Option<NodeEntry> {
        let mut nodes = self.nodes.write();
        let result = nodes.remove(node_id);
        if result.is_some() {
            self.stats.node_count.fetch_sub(1, Ordering::Relaxed);
        }
        result
    }

    /// Get all node IDs in this shard
    fn node_ids(&self) -> Vec<NodeId> {
        self.nodes.read().keys().cloned().collect()
    }

    /// Mark stale nodes as offline
    fn mark_stale_offline(&self, max_heartbeat_age_secs: u64) -> usize {
        let mut nodes = self.nodes.write();
        let now = Utc::now();
        let mut count = 0;

        for entry in nodes.values_mut() {
            if entry.online {
                let age = now.signed_duration_since(entry.last_heartbeat);
                if age.num_seconds() > max_heartbeat_age_secs as i64 {
                    entry.mark_offline();
                    count += 1;
                }
            }
        }

        count
    }
}

// Clone is needed for returning node entries
impl Clone for NodeEntry {
    fn clone(&self) -> Self {
        Self {
            node_id: self.node_id.clone(),
            hostname: self.hostname.clone(),
            status: self.status.clone(),
            drive_metrics: self.drive_metrics.clone(),
            registered_at: self.registered_at,
            last_heartbeat: self.last_heartbeat,
            online: self.online,
            labels: self.labels.clone(),
            fault_domain: self.fault_domain.clone(),
        }
    }
}

// =============================================================================
// Global Statistics
// =============================================================================

/// Global statistics across all shards
#[derive(Debug, Default)]
pub struct GlobalStats {
    /// Total nodes across all shards
    pub total_nodes: AtomicU64,
    /// Total online nodes
    pub online_nodes: AtomicU64,
    /// Total drives across all nodes
    pub total_drives: AtomicU64,
    /// Total capacity in bytes
    pub total_capacity_bytes: AtomicU64,
    /// Total available capacity in bytes
    pub available_capacity_bytes: AtomicU64,
    /// Registration events
    pub registrations: AtomicU64,
    /// Deregistration events
    pub deregistrations: AtomicU64,
}

impl GlobalStats {
    /// Create a snapshot of current stats
    pub fn snapshot(&self) -> GlobalStatsSnapshot {
        GlobalStatsSnapshot {
            total_nodes: self.total_nodes.load(Ordering::Relaxed),
            online_nodes: self.online_nodes.load(Ordering::Relaxed),
            total_drives: self.total_drives.load(Ordering::Relaxed),
            total_capacity_bytes: self.total_capacity_bytes.load(Ordering::Relaxed),
            available_capacity_bytes: self.available_capacity_bytes.load(Ordering::Relaxed),
            registrations: self.registrations.load(Ordering::Relaxed),
            deregistrations: self.deregistrations.load(Ordering::Relaxed),
        }
    }
}

/// Snapshot of global statistics
#[derive(Debug, Clone)]
pub struct GlobalStatsSnapshot {
    pub total_nodes: u64,
    pub online_nodes: u64,
    pub total_drives: u64,
    pub total_capacity_bytes: u64,
    pub available_capacity_bytes: u64,
    pub registrations: u64,
    pub deregistrations: u64,
}

// =============================================================================
// Node Registry
// =============================================================================

/// High-performance node registry with 256-way sharding
pub struct NodeRegistry {
    /// Shards for node storage
    shards: Box<[RegistryShard; REGISTRY_SHARD_COUNT]>,
    /// Global statistics
    global_stats: GlobalStats,
    /// Event broadcaster
    event_sender: broadcast::Sender<super::RegistryEvent>,
}

impl NodeRegistry {
    /// Create a new node registry
    pub fn new() -> Arc<Self> {
        // Initialize shards
        let shards: Vec<RegistryShard> = (0..REGISTRY_SHARD_COUNT)
            .map(|_| RegistryShard::new())
            .collect();

        let shards: Box<[RegistryShard; REGISTRY_SHARD_COUNT]> =
            shards.into_boxed_slice().try_into().unwrap();

        let (event_sender, _) = broadcast::channel(1024);

        Arc::new(Self {
            shards,
            global_stats: GlobalStats::default(),
            event_sender,
        })
    }

    /// Get an event receiver
    pub fn subscribe(&self) -> broadcast::Receiver<super::RegistryEvent> {
        self.event_sender.subscribe()
    }

    /// Register a new node
    pub fn register(
        &self,
        node_id: impl Into<NodeId>,
        hostname: String,
        status: StorageNodeStatus,
    ) -> Result<()> {
        let node_id = node_id.into();
        let shard_idx = node_id.shard_index();

        let entry = NodeEntry::new(node_id.clone(), hostname.clone(), status.clone());
        let drive_count = entry.status.drives.len() as u64;
        let capacity = entry.total_capacity_bytes();
        let available = entry.available_capacity_bytes();

        self.shards[shard_idx].insert(node_id.clone(), entry)?;

        // Update global stats
        self.global_stats.total_nodes.fetch_add(1, Ordering::Relaxed);
        self.global_stats.online_nodes.fetch_add(1, Ordering::Relaxed);
        self.global_stats.total_drives.fetch_add(drive_count, Ordering::Relaxed);
        self.global_stats.total_capacity_bytes.fetch_add(capacity, Ordering::Relaxed);
        self.global_stats.available_capacity_bytes.fetch_add(available, Ordering::Relaxed);
        self.global_stats.registrations.fetch_add(1, Ordering::Relaxed);

        // Send event
        let _ = self.event_sender.send(super::RegistryEvent::NodeRegistered {
            node_id: node_id.to_string(),
            hostname,
            drive_count: drive_count as u32,
        });

        Ok(())
    }

    /// Deregister a node
    pub fn deregister(&self, node_id: impl Into<NodeId>) -> Result<()> {
        let node_id = node_id.into();
        let shard_idx = node_id.shard_index();

        if let Some(entry) = self.shards[shard_idx].remove(&node_id) {
            let drive_count = entry.status.drives.len() as u64;
            let capacity = entry.total_capacity_bytes();
            let available = entry.available_capacity_bytes();

            // Update global stats
            self.global_stats.total_nodes.fetch_sub(1, Ordering::Relaxed);
            if entry.online {
                self.global_stats.online_nodes.fetch_sub(1, Ordering::Relaxed);
            }
            self.global_stats.total_drives.fetch_sub(drive_count, Ordering::Relaxed);
            self.global_stats.total_capacity_bytes.fetch_sub(capacity, Ordering::Relaxed);
            self.global_stats.available_capacity_bytes.fetch_sub(available, Ordering::Relaxed);
            self.global_stats.deregistrations.fetch_add(1, Ordering::Relaxed);

            // Send event
            let _ = self.event_sender.send(super::RegistryEvent::NodeDeregistered {
                node_id: node_id.to_string(),
            });

            Ok(())
        } else {
            Err(Error::NodeNotFound {
                node_id: node_id.to_string(),
            })
        }
    }

    /// Update a node's status
    pub fn update_status(&self, node_id: impl Into<NodeId>, status: StorageNodeStatus) -> Result<()> {
        let node_id = node_id.into();
        let shard_idx = node_id.shard_index();

        self.shards[shard_idx].update_status(&node_id, status)?;

        // Send event
        let _ = self.event_sender.send(super::RegistryEvent::NodeUpdated {
            node_id: node_id.to_string(),
        });

        Ok(())
    }

    /// Record a heartbeat from a node
    pub fn heartbeat(&self, node_id: impl Into<NodeId>) -> Result<()> {
        let node_id = node_id.into();
        let shard_idx = node_id.shard_index();
        self.shards[shard_idx].heartbeat(&node_id)
    }

    /// Get a node by ID
    pub fn get(&self, node_id: impl Into<NodeId>) -> Option<NodeEntry> {
        let node_id = node_id.into();
        let shard_idx = node_id.shard_index();
        self.shards[shard_idx].get(&node_id)
    }

    /// Check if a node exists
    pub fn contains(&self, node_id: impl Into<NodeId>) -> bool {
        self.get(node_id).is_some()
    }

    /// Get all node IDs
    pub fn all_node_ids(&self) -> Vec<NodeId> {
        let mut ids = Vec::new();
        for shard in self.shards.iter() {
            ids.extend(shard.node_ids());
        }
        ids
    }

    /// Get all online node IDs
    pub fn online_node_ids(&self) -> Vec<NodeId> {
        let mut ids = Vec::new();
        for shard in self.shards.iter() {
            for node_id in shard.node_ids() {
                if let Some(entry) = shard.get(&node_id) {
                    if entry.online {
                        ids.push(node_id);
                    }
                }
            }
        }
        ids
    }

    /// Get drive metrics for a specific drive
    pub fn get_drive_metrics(&self, node_id: impl Into<NodeId>, drive_id: &str) -> Option<Arc<DriveMetrics>> {
        self.get(node_id).and_then(|entry| entry.get_drive_metrics(drive_id))
    }

    /// Update drive metrics
    pub fn update_drive_metrics(
        &self,
        node_id: impl Into<NodeId>,
        drive_id: &str,
        iops: u64,
        throughput_bps: u64,
        latency_us_p99: u32,
        utilization_percent: f32,
        temperature_celsius: i32,
        wear_level_percent: u8,
    ) -> Result<()> {
        let node_id = node_id.into();
        if let Some(metrics) = self.get_drive_metrics(node_id.clone(), drive_id) {
            metrics.update(
                iops,
                throughput_bps,
                latency_us_p99,
                utilization_percent,
                temperature_celsius,
                wear_level_percent,
            );
            Ok(())
        } else {
            Err(Error::DeviceNotFound {
                device: format!("{}:{}", node_id, drive_id),
            })
        }
    }

    /// Get global statistics
    pub fn stats(&self) -> GlobalStatsSnapshot {
        self.global_stats.snapshot()
    }

    /// Mark stale nodes as offline
    pub fn mark_stale_offline(&self, max_heartbeat_age_secs: u64) -> usize {
        let mut total = 0;
        for shard in self.shards.iter() {
            total += shard.mark_stale_offline(max_heartbeat_age_secs);
        }

        if total > 0 {
            self.global_stats.online_nodes.fetch_sub(total as u64, Ordering::Relaxed);
        }

        total
    }

    /// Get shard statistics for debugging
    pub fn shard_stats(&self) -> Vec<(usize, usize, u64)> {
        self.shards
            .iter()
            .enumerate()
            .map(|(idx, shard)| {
                (
                    idx,
                    shard.node_count(),
                    shard.stats.update_count.load(Ordering::Relaxed),
                )
            })
            .filter(|(_, count, _)| *count > 0)
            .collect()
    }
}

impl Default for NodeRegistry {
    fn default() -> Self {
        Arc::try_unwrap(Self::new()).unwrap_or_else(|arc| {
            // This shouldn't happen, but handle it gracefully
            let shards: Vec<RegistryShard> = (0..REGISTRY_SHARD_COUNT)
                .map(|_| RegistryShard::new())
                .collect();
            let shards: Box<[RegistryShard; REGISTRY_SHARD_COUNT]> =
                shards.into_boxed_slice().try_into().unwrap();
            let (event_sender, _) = broadcast::channel(1024);
            Self {
                shards,
                global_stats: GlobalStats::default(),
                event_sender,
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_id_sharding() {
        let id1 = NodeId::new("node-001");
        let id2 = NodeId::new("node-002");
        let id3 = NodeId::new("node-001"); // Same as id1

        // Same ID should always hash to same shard
        assert_eq!(id1.shard_index(), id3.shard_index());

        // Different IDs may hash to different shards (probabilistic)
        // Just verify they're within bounds
        assert!(id1.shard_index() < REGISTRY_SHARD_COUNT);
        assert!(id2.shard_index() < REGISTRY_SHARD_COUNT);
    }

    #[test]
    fn test_drive_metrics_cache_line_alignment() {
        // Verify DriveMetrics is cache-line aligned
        assert_eq!(std::mem::align_of::<DriveMetrics>(), CACHE_LINE_SIZE);
        // Should be exactly one cache line
        assert!(std::mem::size_of::<DriveMetrics>() <= CACHE_LINE_SIZE);
    }

    #[test]
    fn test_drive_metrics_update_and_read() {
        let metrics = DriveMetrics::default();

        metrics.update(10000, 500_000_000, 150, 75.5, 42, 5);

        assert_eq!(metrics.get_iops(), 10000);
        assert_eq!(metrics.get_throughput_bps(), 500_000_000);
        assert_eq!(metrics.get_latency_us_p99(), 150);
        assert!((metrics.get_utilization_percent() - 75.5).abs() < 0.2);
        assert_eq!(metrics.get_temperature_celsius(), 42);
        assert_eq!(metrics.get_wear_level_percent(), 5);
    }

    #[test]
    fn test_registry_register_and_get() {
        let registry = NodeRegistry::new();
        let status = StorageNodeStatus::default();

        registry
            .register("node-001", "host-001.local".to_string(), status.clone())
            .unwrap();

        let entry = registry.get("node-001").unwrap();
        assert_eq!(entry.hostname, "host-001.local");
        assert!(entry.online);

        let stats = registry.stats();
        assert_eq!(stats.total_nodes, 1);
        assert_eq!(stats.online_nodes, 1);
    }

    #[test]
    fn test_registry_deregister() {
        let registry = NodeRegistry::new();
        let status = StorageNodeStatus::default();

        registry
            .register("node-001", "host-001.local".to_string(), status)
            .unwrap();

        assert!(registry.contains("node-001"));

        registry.deregister("node-001").unwrap();

        assert!(!registry.contains("node-001"));

        let stats = registry.stats();
        assert_eq!(stats.total_nodes, 0);
        assert_eq!(stats.deregistrations, 1);
    }

    #[test]
    fn test_registry_duplicate_registration() {
        let registry = NodeRegistry::new();
        let status = StorageNodeStatus::default();

        registry
            .register("node-001", "host-001.local".to_string(), status.clone())
            .unwrap();

        let result = registry.register("node-001", "host-001.local".to_string(), status);
        assert!(result.is_err());
    }
}

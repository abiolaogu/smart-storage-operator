//! Cache Metrics
//!
//! Cache-line aligned metrics for high-performance concurrent access.
//! Follows Data-Oriented Design (DOD) patterns from node_registry.rs.

use crate::cache::tier::CacheTier;
use chrono::Utc;
use std::sync::atomic::{AtomicU64, Ordering};

// =============================================================================
// Cache Line Size
// =============================================================================

/// Cache line size for alignment (64 bytes on most modern CPUs)
pub const CACHE_LINE_SIZE: usize = 64;

// =============================================================================
// Per-Tier Metrics (Cache-Line Aligned)
// =============================================================================

/// Metrics for a single cache tier, aligned to prevent false sharing
#[repr(C, align(64))]
#[derive(Debug)]
pub struct CacheTierMetrics {
    /// Number of cache hits
    pub hits: AtomicU64,
    /// Number of cache misses
    pub misses: AtomicU64,
    /// Total bytes currently stored
    pub bytes_stored: AtomicU64,
    /// Number of entries currently stored
    pub entry_count: AtomicU64,
    /// Number of entries evicted
    pub evictions: AtomicU64,
    /// Number of entries demoted to lower tier
    pub demotions: AtomicU64,
    /// Last update timestamp (Unix millis)
    pub last_update_ms: AtomicU64,
    /// Padding to fill cache line
    _padding: [u8; 8],
}

// Verify size at compile time
const _: () = assert!(std::mem::size_of::<CacheTierMetrics>() <= CACHE_LINE_SIZE);

impl Default for CacheTierMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl CacheTierMetrics {
    /// Create new zeroed metrics
    pub fn new() -> Self {
        Self {
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            bytes_stored: AtomicU64::new(0),
            entry_count: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
            demotions: AtomicU64::new(0),
            last_update_ms: AtomicU64::new(0),
            _padding: [0; 8],
        }
    }

    /// Record a cache hit
    #[inline]
    pub fn record_hit(&self) {
        self.hits.fetch_add(1, Ordering::Relaxed);
        self.touch();
    }

    /// Record a cache miss
    #[inline]
    pub fn record_miss(&self) {
        self.misses.fetch_add(1, Ordering::Relaxed);
        self.touch();
    }

    /// Record an entry being added
    #[inline]
    pub fn record_put(&self, size_bytes: u64) {
        self.entry_count.fetch_add(1, Ordering::Relaxed);
        self.bytes_stored.fetch_add(size_bytes, Ordering::Relaxed);
        self.touch();
    }

    /// Record an entry being removed
    #[inline]
    pub fn record_remove(&self, size_bytes: u64) {
        self.entry_count.fetch_sub(1, Ordering::Relaxed);
        self.bytes_stored.fetch_sub(size_bytes, Ordering::Relaxed);
        self.touch();
    }

    /// Record an eviction
    #[inline]
    pub fn record_eviction(&self, size_bytes: u64) {
        self.evictions.fetch_add(1, Ordering::Relaxed);
        self.record_remove(size_bytes);
    }

    /// Record a demotion to lower tier
    #[inline]
    pub fn record_demotion(&self, size_bytes: u64) {
        self.demotions.fetch_add(1, Ordering::Relaxed);
        self.record_remove(size_bytes);
    }

    /// Reset storage metrics (entry_count and bytes_stored) to zero
    /// Used when clearing a tier
    #[inline]
    pub fn reset_storage(&self) {
        self.entry_count.store(0, Ordering::Relaxed);
        self.bytes_stored.store(0, Ordering::Relaxed);
        self.touch();
    }

    /// Update last update timestamp
    #[inline]
    fn touch(&self) {
        self.last_update_ms
            .store(Utc::now().timestamp_millis() as u64, Ordering::Release);
    }

    /// Get total requests (hits + misses)
    #[inline]
    pub fn total_requests(&self) -> u64 {
        self.hits.load(Ordering::Relaxed) + self.misses.load(Ordering::Relaxed)
    }

    /// Calculate hit ratio (0.0 to 1.0)
    pub fn hit_ratio(&self) -> f64 {
        let total = self.total_requests();
        if total == 0 {
            0.0
        } else {
            self.hits.load(Ordering::Relaxed) as f64 / total as f64
        }
    }

    /// Get current bytes stored
    #[inline]
    pub fn get_bytes_stored(&self) -> u64 {
        self.bytes_stored.load(Ordering::Relaxed)
    }

    /// Get current entry count
    #[inline]
    pub fn get_entry_count(&self) -> u64 {
        self.entry_count.load(Ordering::Relaxed)
    }

    /// Create a snapshot of current metrics
    pub fn snapshot(&self) -> TierMetricsSnapshot {
        TierMetricsSnapshot {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            bytes_stored: self.bytes_stored.load(Ordering::Relaxed),
            entry_count: self.entry_count.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
            demotions: self.demotions.load(Ordering::Relaxed),
        }
    }
}

// =============================================================================
// Tier Metrics Snapshot
// =============================================================================

/// Point-in-time snapshot of tier metrics
#[derive(Debug, Clone, Default)]
pub struct TierMetricsSnapshot {
    pub hits: u64,
    pub misses: u64,
    pub bytes_stored: u64,
    pub entry_count: u64,
    pub evictions: u64,
    pub demotions: u64,
}

impl TierMetricsSnapshot {
    /// Calculate hit ratio
    pub fn hit_ratio(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            self.hits as f64 / total as f64
        }
    }

    /// Get total requests
    pub fn total_requests(&self) -> u64 {
        self.hits + self.misses
    }
}

// =============================================================================
// Aggregate Cache Statistics
// =============================================================================

/// Aggregate statistics across all tiers
#[derive(Debug, Clone, Default)]
pub struct CacheStatsSnapshot {
    /// Stats per tier
    pub tiers: [(CacheTier, TierMetricsSnapshot); 3],
    /// Total hits across all tiers
    pub total_hits: u64,
    /// Total misses (lookup miss = miss on all tiers)
    pub total_misses: u64,
    /// Total bytes stored across all tiers
    pub total_bytes_stored: u64,
    /// Total entries across all tiers
    pub total_entry_count: u64,
    /// Prefetch requests
    pub prefetch_requests: u64,
    /// Successful prefetches
    pub prefetch_hits: u64,
}

impl CacheStatsSnapshot {
    /// Create a new stats snapshot from tier metrics
    pub fn from_tier_metrics(
        l1: TierMetricsSnapshot,
        l2: TierMetricsSnapshot,
        l3: TierMetricsSnapshot,
        prefetch_requests: u64,
        prefetch_hits: u64,
    ) -> Self {
        let total_hits = l1.hits + l2.hits + l3.hits;
        let total_bytes_stored = l1.bytes_stored + l2.bytes_stored + l3.bytes_stored;
        let total_entry_count = l1.entry_count + l2.entry_count + l3.entry_count;
        // Total misses = L3 misses (since L1/L2 misses cascade down)
        let total_misses = l3.misses;

        Self {
            tiers: [
                (CacheTier::L1Memory, l1),
                (CacheTier::L2Local, l2),
                (CacheTier::L3Persistent, l3),
            ],
            total_hits,
            total_misses,
            total_bytes_stored,
            total_entry_count,
            prefetch_requests,
            prefetch_hits,
        }
    }

    /// Get overall hit ratio
    pub fn hit_ratio(&self) -> f64 {
        let total = self.total_hits + self.total_misses;
        if total == 0 {
            0.0
        } else {
            self.total_hits as f64 / total as f64
        }
    }

    /// Get tier statistics by tier
    pub fn get_tier_stats(&self, tier: CacheTier) -> &TierMetricsSnapshot {
        match tier {
            CacheTier::L1Memory => &self.tiers[0].1,
            CacheTier::L2Local => &self.tiers[1].1,
            CacheTier::L3Persistent => &self.tiers[2].1,
        }
    }

    /// Get prefetch hit ratio
    pub fn prefetch_hit_ratio(&self) -> f64 {
        if self.prefetch_requests == 0 {
            0.0
        } else {
            self.prefetch_hits as f64 / self.prefetch_requests as f64
        }
    }
}

// =============================================================================
// Global Cache Metrics
// =============================================================================

/// Global cache metrics container
#[derive(Debug)]
pub struct CacheMetrics {
    /// L1 Memory tier metrics
    pub l1: CacheTierMetrics,
    /// L2 Local tier metrics
    pub l2: CacheTierMetrics,
    /// L3 Persistent tier metrics
    pub l3: CacheTierMetrics,
    /// Prefetch statistics
    pub prefetch_requests: AtomicU64,
    pub prefetch_hits: AtomicU64,
}

impl Default for CacheMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl CacheMetrics {
    /// Create new metrics
    pub fn new() -> Self {
        Self {
            l1: CacheTierMetrics::new(),
            l2: CacheTierMetrics::new(),
            l3: CacheTierMetrics::new(),
            prefetch_requests: AtomicU64::new(0),
            prefetch_hits: AtomicU64::new(0),
        }
    }

    /// Get metrics for a specific tier
    pub fn tier(&self, tier: CacheTier) -> &CacheTierMetrics {
        match tier {
            CacheTier::L1Memory => &self.l1,
            CacheTier::L2Local => &self.l2,
            CacheTier::L3Persistent => &self.l3,
        }
    }

    /// Record a prefetch request
    pub fn record_prefetch_request(&self) {
        self.prefetch_requests.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a successful prefetch
    pub fn record_prefetch_hit(&self) {
        self.prefetch_hits.fetch_add(1, Ordering::Relaxed);
    }

    /// Create a snapshot of all metrics
    pub fn snapshot(&self) -> CacheStatsSnapshot {
        CacheStatsSnapshot::from_tier_metrics(
            self.l1.snapshot(),
            self.l2.snapshot(),
            self.l3.snapshot(),
            self.prefetch_requests.load(Ordering::Relaxed),
            self.prefetch_hits.load(Ordering::Relaxed),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_line_alignment() {
        // Verify CacheTierMetrics is properly aligned
        assert_eq!(std::mem::align_of::<CacheTierMetrics>(), CACHE_LINE_SIZE);
        assert!(std::mem::size_of::<CacheTierMetrics>() <= CACHE_LINE_SIZE);
    }

    #[test]
    fn test_tier_metrics_operations() {
        let metrics = CacheTierMetrics::new();

        metrics.record_hit();
        metrics.record_hit();
        metrics.record_miss();

        assert_eq!(metrics.hits.load(Ordering::Relaxed), 2);
        assert_eq!(metrics.misses.load(Ordering::Relaxed), 1);
        assert!((metrics.hit_ratio() - 0.666).abs() < 0.01);

        metrics.record_put(1000);
        assert_eq!(metrics.get_bytes_stored(), 1000);
        assert_eq!(metrics.get_entry_count(), 1);

        metrics.record_eviction(1000);
        assert_eq!(metrics.get_bytes_stored(), 0);
        assert_eq!(metrics.get_entry_count(), 0);
        assert_eq!(metrics.evictions.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_cache_stats_snapshot() {
        let l1 = TierMetricsSnapshot {
            hits: 100,
            misses: 10,
            bytes_stored: 1000,
            entry_count: 10,
            evictions: 5,
            demotions: 3,
        };
        let l2 = TierMetricsSnapshot {
            hits: 50,
            misses: 20,
            bytes_stored: 5000,
            entry_count: 20,
            evictions: 2,
            demotions: 1,
        };
        let l3 = TierMetricsSnapshot {
            hits: 20,
            misses: 30,
            bytes_stored: 10000,
            entry_count: 30,
            evictions: 1,
            demotions: 0,
        };

        let stats = CacheStatsSnapshot::from_tier_metrics(l1, l2, l3, 100, 80);

        assert_eq!(stats.total_hits, 170);
        assert_eq!(stats.total_misses, 30); // L3 misses only
        assert_eq!(stats.total_bytes_stored, 16000);
        assert_eq!(stats.total_entry_count, 60);
        assert!((stats.prefetch_hit_ratio() - 0.8).abs() < 0.001);
    }

    #[test]
    fn test_global_cache_metrics() {
        let metrics = CacheMetrics::new();

        metrics.tier(CacheTier::L1Memory).record_hit();
        metrics.tier(CacheTier::L2Local).record_miss();
        metrics.tier(CacheTier::L3Persistent).record_put(500);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.get_tier_stats(CacheTier::L1Memory).hits, 1);
        assert_eq!(snapshot.get_tier_stats(CacheTier::L2Local).misses, 1);
        assert_eq!(snapshot.get_tier_stats(CacheTier::L3Persistent).bytes_stored, 500);
    }
}

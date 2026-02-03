//! LRU Tracker with Tier Demotion
//!
//! A 64-way sharded LRU tracker that supports tier demotion instead of
//! simple eviction. When entries are evicted from L1, they're demoted to L2;
//! from L2 to L3; from L3 they're deleted entirely.

use crate::cache::entry::{CacheKey, EntryMetadata};
use crate::cache::tier::CacheTier;
use chrono::Utc;
use indexmap::IndexMap;
use parking_lot::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};

// =============================================================================
// Constants
// =============================================================================

/// Number of shards for LRU tracking (64 for good distribution)
pub const LRU_SHARD_COUNT: usize = 64;

// =============================================================================
// Eviction Policy
// =============================================================================

/// Policy for selecting eviction candidates
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvictionPolicy {
    /// Least Recently Used - evict entries that haven't been accessed recently
    Lru,
    /// Least Frequently Used - evict entries with lowest access count
    Lfu,
    /// Size-based - evict largest entries first
    LargestFirst,
    /// Combined score based on age, frequency, and size
    Adaptive,
}

impl Default for EvictionPolicy {
    fn default() -> Self {
        Self::Lru
    }
}

// =============================================================================
// Eviction Candidate
// =============================================================================

/// Candidate for eviction with relevant metadata
#[derive(Debug, Clone)]
pub struct EvictionCandidate {
    /// The cache key
    pub key: CacheKey,
    /// Size in bytes
    pub size_bytes: u64,
    /// Current tier
    pub tier: CacheTier,
    /// Target tier after demotion (None if should be deleted)
    pub demotion_target: Option<CacheTier>,
    /// Eviction score (higher = more likely to evict)
    pub score: f64,
}

// =============================================================================
// LRU Shard
// =============================================================================

/// A single shard of the LRU tracker
#[derive(Debug)]
struct LruShard {
    /// Entries in LRU order (front = oldest, back = newest)
    entries: IndexMap<String, EntryMetadata>,
    /// Total bytes tracked in this shard
    total_bytes: u64,
}

impl LruShard {
    fn new() -> Self {
        Self {
            entries: IndexMap::new(),
            total_bytes: 0,
        }
    }

    /// Track a new or updated entry (moves to back = most recently used)
    fn track(&mut self, metadata: EntryMetadata) {
        let key = metadata.key.to_storage_key();
        let new_size = metadata.size_bytes;

        // Remove old entry if exists
        if let Some(old) = self.entries.shift_remove(&key) {
            self.total_bytes = self.total_bytes.saturating_sub(old.size_bytes);
        }

        // Add new entry at back (most recently used)
        self.total_bytes += new_size;
        self.entries.insert(key, metadata);
    }

    /// Record access (moves to back = most recently used)
    fn access(&mut self, key: &CacheKey) -> bool {
        let storage_key = key.to_storage_key();
        if let Some(mut metadata) = self.entries.shift_remove(&storage_key) {
            metadata.record_access();
            self.entries.insert(storage_key, metadata);
            true
        } else {
            false
        }
    }

    /// Remove an entry
    fn remove(&mut self, key: &CacheKey) -> Option<EntryMetadata> {
        let storage_key = key.to_storage_key();
        if let Some(metadata) = self.entries.shift_remove(&storage_key) {
            self.total_bytes = self.total_bytes.saturating_sub(metadata.size_bytes);
            Some(metadata)
        } else {
            None
        }
    }

    /// Get candidates for eviction (from front = least recently used)
    fn get_eviction_candidates(
        &self,
        tier: CacheTier,
        count: usize,
        policy: EvictionPolicy,
    ) -> Vec<EvictionCandidate> {
        let mut candidates: Vec<_> = self
            .entries
            .iter()
            .filter(|(_, m)| m.tier == tier)
            .map(|(_, metadata)| {
                let score = Self::calculate_score(metadata, policy);
                EvictionCandidate {
                    key: metadata.key.clone(),
                    size_bytes: metadata.size_bytes,
                    tier: metadata.tier,
                    demotion_target: metadata.tier.demotion_target(),
                    score,
                }
            })
            .collect();

        // Sort by score (highest first = most likely to evict)
        candidates.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
        candidates.truncate(count);
        candidates
    }

    /// Calculate eviction score (higher = more likely to evict)
    fn calculate_score(metadata: &EntryMetadata, policy: EvictionPolicy) -> f64 {
        let now_ms = Utc::now().timestamp_millis() as u64;
        let age_ms = now_ms.saturating_sub(metadata.last_accessed_ms);
        let age_secs = age_ms as f64 / 1000.0;

        match policy {
            EvictionPolicy::Lru => {
                // Score based purely on age (older = higher score)
                age_secs
            }
            EvictionPolicy::Lfu => {
                // Score based on inverse of access frequency
                1.0 / (metadata.access_count as f64 + 1.0)
            }
            EvictionPolicy::LargestFirst => {
                // Score based on size
                metadata.size_bytes as f64
            }
            EvictionPolicy::Adaptive => {
                // Combined score: age * (1/frequency) * size_factor
                let freq_factor = 1.0 / (metadata.access_count as f64 + 1.0);
                let size_factor = (metadata.size_bytes as f64).sqrt() / 1000.0;
                age_secs * freq_factor * (1.0 + size_factor)
            }
        }
    }

    /// Get entry count
    fn len(&self) -> usize {
        self.entries.len()
    }

    /// Clear all entries
    fn clear(&mut self) {
        self.entries.clear();
        self.total_bytes = 0;
    }
}

// =============================================================================
// Sharded LRU Tracker
// =============================================================================

/// 64-way sharded LRU tracker for concurrent access
pub struct ShardedLruTracker {
    /// Shards for distributed tracking
    shards: Box<[RwLock<LruShard>; LRU_SHARD_COUNT]>,
    /// Total entries tracked
    entry_count: AtomicU64,
    /// Eviction policy
    policy: EvictionPolicy,
}

impl ShardedLruTracker {
    /// Create a new sharded LRU tracker
    pub fn new() -> Self {
        Self::with_policy(EvictionPolicy::default())
    }

    /// Create a new tracker with specified policy
    pub fn with_policy(policy: EvictionPolicy) -> Self {
        let shards: Vec<RwLock<LruShard>> = (0..LRU_SHARD_COUNT)
            .map(|_| RwLock::new(LruShard::new()))
            .collect();

        let shards: Box<[RwLock<LruShard>; LRU_SHARD_COUNT]> =
            shards.into_boxed_slice().try_into().unwrap();

        Self {
            shards,
            entry_count: AtomicU64::new(0),
            policy,
        }
    }

    /// Get shard for a key
    #[inline]
    fn shard_for(&self, key: &CacheKey) -> &RwLock<LruShard> {
        &self.shards[key.shard_index()]
    }

    /// Track a new or updated entry
    pub fn track(&self, metadata: EntryMetadata) {
        let shard = self.shard_for(&metadata.key);
        let mut shard = shard.write();

        let was_new = !shard.entries.contains_key(&metadata.key.to_storage_key());
        shard.track(metadata);

        if was_new {
            self.entry_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record access to an entry (updates LRU position)
    pub fn access(&self, key: &CacheKey) -> bool {
        let shard = self.shard_for(key);
        shard.write().access(key)
    }

    /// Remove an entry from tracking
    pub fn remove(&self, key: &CacheKey) -> Option<EntryMetadata> {
        let shard = self.shard_for(key);
        let result = shard.write().remove(key);
        if result.is_some() {
            self.entry_count.fetch_sub(1, Ordering::Relaxed);
        }
        result
    }

    /// Get eviction candidates for a tier
    ///
    /// Returns candidates across all shards, sorted by eviction score.
    pub fn get_eviction_candidates(
        &self,
        tier: CacheTier,
        bytes_needed: u64,
    ) -> Vec<EvictionCandidate> {
        let mut all_candidates = Vec::new();

        // Collect candidates from all shards
        for shard in self.shards.iter() {
            let shard = shard.read();
            // Get more candidates than strictly needed to allow for good selection
            let candidates = shard.get_eviction_candidates(tier, 100, self.policy);
            all_candidates.extend(candidates);
        }

        // Sort all candidates by score
        all_candidates.sort_by(|a, b| {
            b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal)
        });

        // Select enough candidates to cover bytes_needed
        let mut selected = Vec::new();
        let mut total_bytes = 0u64;

        for candidate in all_candidates {
            selected.push(candidate.clone());
            total_bytes += candidate.size_bytes;
            if total_bytes >= bytes_needed {
                break;
            }
        }

        selected
    }

    /// Get single eviction candidate (least recently used in tier)
    pub fn get_lru_candidate(&self, tier: CacheTier) -> Option<EvictionCandidate> {
        self.get_eviction_candidates(tier, 1)
            .into_iter()
            .next()
    }

    /// Get total entry count
    pub fn entry_count(&self) -> u64 {
        self.entry_count.load(Ordering::Relaxed)
    }

    /// Get entry count for a specific tier
    pub fn tier_entry_count(&self, tier: CacheTier) -> u64 {
        let mut count = 0u64;
        for shard in self.shards.iter() {
            let shard = shard.read();
            count += shard.entries.values().filter(|m| m.tier == tier).count() as u64;
        }
        count
    }

    /// Get total bytes tracked for a tier
    pub fn tier_bytes(&self, tier: CacheTier) -> u64 {
        let mut bytes = 0u64;
        for shard in self.shards.iter() {
            let shard = shard.read();
            bytes += shard
                .entries
                .values()
                .filter(|m| m.tier == tier)
                .map(|m| m.size_bytes)
                .sum::<u64>();
        }
        bytes
    }

    /// Clear all entries
    pub fn clear(&self) {
        for shard in self.shards.iter() {
            shard.write().clear();
        }
        self.entry_count.store(0, Ordering::Relaxed);
    }

    /// Clear entries for a specific tier
    pub fn clear_tier(&self, tier: CacheTier) {
        let mut removed = 0u64;
        for shard in self.shards.iter() {
            let mut shard = shard.write();
            let keys_to_remove: Vec<String> = shard
                .entries
                .iter()
                .filter(|(_, m)| m.tier == tier)
                .map(|(k, _)| k.clone())
                .collect();

            for key in keys_to_remove {
                if let Some(metadata) = shard.entries.shift_remove(&key) {
                    shard.total_bytes = shard.total_bytes.saturating_sub(metadata.size_bytes);
                    removed += 1;
                }
            }
        }
        self.entry_count.fetch_sub(removed, Ordering::Relaxed);
    }

    /// Get current policy
    pub fn policy(&self) -> EvictionPolicy {
        self.policy
    }
}

impl Default for ShardedLruTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_metadata(id: &str, tier: CacheTier, size: u64) -> EntryMetadata {
        EntryMetadata {
            key: CacheKey::new("test", id),
            size_bytes: size,
            tier,
            last_accessed_ms: Utc::now().timestamp_millis() as u64,
            access_count: 1,
        }
    }

    #[test]
    fn test_track_and_access() {
        let tracker = ShardedLruTracker::new();

        let meta = test_metadata("file1", CacheTier::L1Memory, 1000);
        tracker.track(meta);

        assert_eq!(tracker.entry_count(), 1);
        assert_eq!(tracker.tier_entry_count(CacheTier::L1Memory), 1);
        assert_eq!(tracker.tier_bytes(CacheTier::L1Memory), 1000);

        let key = CacheKey::new("test", "file1");
        assert!(tracker.access(&key));
        assert!(!tracker.access(&CacheKey::new("test", "nonexistent")));
    }

    #[test]
    fn test_remove() {
        let tracker = ShardedLruTracker::new();

        tracker.track(test_metadata("file1", CacheTier::L1Memory, 1000));
        tracker.track(test_metadata("file2", CacheTier::L1Memory, 2000));

        assert_eq!(tracker.entry_count(), 2);

        let key = CacheKey::new("test", "file1");
        let removed = tracker.remove(&key).unwrap();
        assert_eq!(removed.size_bytes, 1000);
        assert_eq!(tracker.entry_count(), 1);
    }

    #[test]
    fn test_eviction_candidates() {
        let tracker = ShardedLruTracker::new();

        // Add entries with different ages
        for i in 0..10 {
            let mut meta = test_metadata(&format!("file{}", i), CacheTier::L1Memory, 100);
            // Older entries have lower timestamps
            meta.last_accessed_ms -= i as u64 * 1000;
            tracker.track(meta);
        }

        // Get eviction candidates
        let candidates = tracker.get_eviction_candidates(CacheTier::L1Memory, 500);

        // Should get 5 entries (500 bytes / 100 bytes each)
        assert_eq!(candidates.len(), 5);

        // All should be L1 entries
        for c in &candidates {
            assert_eq!(c.tier, CacheTier::L1Memory);
            assert_eq!(c.demotion_target, Some(CacheTier::L2Local));
        }
    }

    #[test]
    fn test_clear_tier() {
        let tracker = ShardedLruTracker::new();

        tracker.track(test_metadata("l1-1", CacheTier::L1Memory, 100));
        tracker.track(test_metadata("l1-2", CacheTier::L1Memory, 100));
        tracker.track(test_metadata("l2-1", CacheTier::L2Local, 200));

        assert_eq!(tracker.entry_count(), 3);
        assert_eq!(tracker.tier_entry_count(CacheTier::L1Memory), 2);
        assert_eq!(tracker.tier_entry_count(CacheTier::L2Local), 1);

        tracker.clear_tier(CacheTier::L1Memory);

        assert_eq!(tracker.entry_count(), 1);
        assert_eq!(tracker.tier_entry_count(CacheTier::L1Memory), 0);
        assert_eq!(tracker.tier_entry_count(CacheTier::L2Local), 1);
    }

    #[test]
    fn test_eviction_policies() {
        // Test LFU - low access count should have higher score
        let meta_low_access = EntryMetadata {
            key: CacheKey::new("test", "low"),
            size_bytes: 100,
            tier: CacheTier::L1Memory,
            last_accessed_ms: Utc::now().timestamp_millis() as u64,
            access_count: 1,
        };

        let meta_high_access = EntryMetadata {
            key: CacheKey::new("test", "high"),
            size_bytes: 100,
            tier: CacheTier::L1Memory,
            last_accessed_ms: Utc::now().timestamp_millis() as u64,
            access_count: 100,
        };

        let score_low = LruShard::calculate_score(&meta_low_access, EvictionPolicy::Lfu);
        let score_high = LruShard::calculate_score(&meta_high_access, EvictionPolicy::Lfu);

        assert!(score_low > score_high);
    }
}

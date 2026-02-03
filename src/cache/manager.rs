//! Multi-Tier Cache Manager
//!
//! Main implementation of the StorageCache trait, coordinating all tiers,
//! LRU tracking, compression, and prefetching.

use crate::cache::compression::{CompressionConfig, CompressionManager};
use crate::cache::entry::{CacheData, CacheEntry, CacheKey, EntryMetadata};
use crate::cache::events::{CacheEvent, EvictionReason};
use crate::cache::lru::{EvictionPolicy, ShardedLruTracker};
use crate::cache::metrics::{CacheMetrics, CacheStatsSnapshot};
use crate::cache::prefetch::{PrefetchConfig, PrefetchRequest, Prefetcher};
use crate::cache::storage::{LocalStorage, MemoryStorage, PersistentStorage, TierStorage};
use crate::cache::tier::{CacheTier, TierConfig, CACHE_BYPASS_SIZE_BYTES};
use crate::cache::{CacheLookupResult, StorageCache};
use crate::error::{Error, Result};
use async_trait::async_trait;
use bytes::Bytes;
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::{debug, info, warn};

// =============================================================================
// Configuration
// =============================================================================

/// Configuration for the multi-tier cache
#[derive(Debug, Clone)]
pub struct MultiTierCacheConfig {
    /// L1 Memory tier configuration
    pub l1: TierConfig,
    /// L2 Local tier configuration
    pub l2: TierConfig,
    /// L3 Persistent tier configuration
    pub l3: TierConfig,
    /// Compression configuration
    pub compression: CompressionConfig,
    /// Prefetch configuration
    pub prefetch: PrefetchConfig,
    /// Eviction policy
    pub eviction_policy: EvictionPolicy,
    /// Whether to auto-promote on access
    pub auto_promote: bool,
    /// Event channel capacity
    pub event_channel_capacity: usize,
    /// L2 storage path (for local disk)
    pub l2_path: Option<String>,
}

impl Default for MultiTierCacheConfig {
    fn default() -> Self {
        Self {
            l1: TierConfig::l1_default(),
            l2: TierConfig::l2_default(),
            l3: TierConfig::l3_default(),
            compression: CompressionConfig::default(),
            prefetch: PrefetchConfig::default(),
            eviction_policy: EvictionPolicy::Lru,
            auto_promote: true,
            event_channel_capacity: 1024,
            l2_path: None,
        }
    }
}

// =============================================================================
// Multi-Tier Cache
// =============================================================================

/// Main multi-tier cache implementation
pub struct MultiTierCache {
    /// L1 Memory storage
    l1: MemoryStorage,
    /// L2 Local storage
    l2: Arc<LocalStorage>,
    /// L3 Persistent storage
    l3: PersistentStorage,
    /// LRU tracker
    lru: ShardedLruTracker,
    /// Compression manager
    compression: CompressionManager,
    /// Prefetcher
    prefetcher: Prefetcher,
    /// Metrics
    metrics: CacheMetrics,
    /// Configuration
    config: MultiTierCacheConfig,
    /// Event broadcaster
    event_tx: broadcast::Sender<CacheEvent>,
}

impl MultiTierCache {
    /// Create a new multi-tier cache with default configuration
    pub async fn new() -> Result<Arc<Self>> {
        Self::with_config(MultiTierCacheConfig::default()).await
    }

    /// Create a new multi-tier cache with custom configuration
    pub async fn with_config(config: MultiTierCacheConfig) -> Result<Arc<Self>> {
        // Create L1 memory storage
        let l1 = MemoryStorage::with_capacity(config.l1.capacity_bytes);

        // Create L2 local storage
        let l2_path = config.l2_path.clone().unwrap_or_else(|| {
            std::env::temp_dir()
                .join("smart-storage-cache")
                .to_string_lossy()
                .to_string()
        });
        let l2 = Arc::new(LocalStorage::with_path(&l2_path).await?);

        // Create L3 persistent storage
        let l3 = PersistentStorage::new();

        // Create LRU tracker
        let lru = ShardedLruTracker::with_policy(config.eviction_policy);

        // Create compression manager
        let compression = CompressionManager::with_config(config.compression.clone());

        // Create prefetcher
        let prefetcher = Prefetcher::with_config(config.prefetch.clone());

        // Create event channel
        let (event_tx, _) = broadcast::channel(config.event_channel_capacity);

        let cache = Arc::new(Self {
            l1,
            l2,
            l3,
            lru,
            compression,
            prefetcher,
            metrics: CacheMetrics::new(),
            config,
            event_tx,
        });

        info!("Multi-tier cache initialized");
        Ok(cache)
    }

    /// Subscribe to cache events
    pub fn subscribe(&self) -> broadcast::Receiver<CacheEvent> {
        self.event_tx.subscribe()
    }

    /// Emit a cache event
    fn emit_event(&self, event: CacheEvent) {
        let _ = self.event_tx.send(event);
    }

    /// Get storage for a tier
    fn storage(&self, tier: CacheTier) -> &dyn TierStorage {
        match tier {
            CacheTier::L1Memory => &self.l1,
            CacheTier::L2Local => self.l2.as_ref(),
            CacheTier::L3Persistent => &self.l3,
        }
    }

    /// Promote entry to a higher tier
    async fn promote(&self, entry: &CacheEntry, target_tier: CacheTier) -> Result<bool> {
        // Check if promotion is beneficial
        if entry.stored_size() > target_tier.max_object_size() {
            return Ok(false);
        }

        // Check tier capacity and evict if needed
        self.ensure_capacity(target_tier, entry.stored_size()).await?;

        // Create entry for target tier
        let mut promoted_entry = entry.clone();
        promoted_entry.tier = target_tier;

        // Store in target tier
        match target_tier {
            CacheTier::L1Memory => self.l1.put(promoted_entry.clone()).await?,
            CacheTier::L2Local => self.l2.put(promoted_entry.clone()).await?,
            CacheTier::L3Persistent => self.l3.put(promoted_entry.clone()).await?,
        }

        // Update LRU tracker
        self.lru.track(EntryMetadata::from_entry(&promoted_entry));

        // Update metrics
        self.metrics.tier(target_tier).record_put(entry.stored_size());

        // Emit event
        self.emit_event(CacheEvent::promote(
            &entry.key,
            entry.tier,
            target_tier,
            entry.stored_size(),
        ));

        debug!(
            key = %entry.key,
            from = %entry.tier,
            to = %target_tier,
            "Promoted cache entry"
        );

        Ok(true)
    }

    /// Demote entry to a lower tier
    async fn demote(&self, entry: &CacheEntry) -> Result<bool> {
        let target_tier = match entry.tier.demotion_target() {
            Some(t) => t,
            None => return Ok(false), // Nothing to demote to
        };

        // Ensure capacity in target tier
        self.ensure_capacity(target_tier, entry.stored_size()).await?;

        // Create entry for target tier
        let mut demoted_entry = entry.clone();
        demoted_entry.tier = target_tier;

        // Store in target tier
        match target_tier {
            CacheTier::L1Memory => self.l1.put(demoted_entry.clone()).await?,
            CacheTier::L2Local => self.l2.put(demoted_entry.clone()).await?,
            CacheTier::L3Persistent => self.l3.put(demoted_entry.clone()).await?,
        }

        // Update LRU tracker
        self.lru.track(EntryMetadata::from_entry(&demoted_entry));

        // Update metrics
        self.metrics.tier(entry.tier).record_demotion(entry.stored_size());
        self.metrics.tier(target_tier).record_put(entry.stored_size());

        // Emit event
        self.emit_event(CacheEvent::demote(
            &entry.key,
            entry.tier,
            target_tier,
            entry.stored_size(),
        ));

        debug!(
            key = %entry.key,
            from = %entry.tier,
            to = %target_tier,
            "Demoted cache entry"
        );

        Ok(true)
    }

    /// Ensure a tier has capacity for new data
    async fn ensure_capacity(&self, tier: CacheTier, needed: u64) -> Result<()> {
        let config = match tier {
            CacheTier::L1Memory => &self.config.l1,
            CacheTier::L2Local => &self.config.l2,
            CacheTier::L3Persistent => &self.config.l3,
        };

        let current = self.metrics.tier(tier).get_bytes_stored();
        let threshold = config.eviction_watermark();

        if current + needed <= threshold {
            return Ok(());
        }

        // Calculate how much to evict
        let to_evict = (current + needed).saturating_sub(threshold) + (threshold / 10); // Evict 10% extra

        self.evict(tier, to_evict).await?;
        Ok(())
    }

    /// Get tier configuration
    fn tier_config(&self, tier: CacheTier) -> &TierConfig {
        match tier {
            CacheTier::L1Memory => &self.config.l1,
            CacheTier::L2Local => &self.config.l2,
            CacheTier::L3Persistent => &self.config.l3,
        }
    }
}

#[async_trait]
impl StorageCache for MultiTierCache {
    async fn get(&self, key: &CacheKey) -> Result<CacheLookupResult> {
        // Search tiers in order: L1 -> L2 -> L3
        for tier in CacheTier::lookup_order() {
            let storage = self.storage(*tier);

            match storage.get(key).await {
                Ok(Some(entry)) => {
                    // Check if expired
                    if entry.is_expired() {
                        // Delete expired entry
                        let _ = storage.delete(key).await;
                        self.lru.remove(key);
                        self.metrics.tier(*tier).record_eviction(entry.stored_size());
                        continue;
                    }

                    // Record hit
                    self.metrics.tier(*tier).record_hit();
                    self.lru.access(key);

                    // Decompress if needed
                    let data = if entry.data.compressed {
                        self.compression.decompress(
                            &entry.data.bytes,
                            entry.data.compression_algorithm.unwrap_or_default(),
                        )?
                    } else {
                        entry.data.bytes.clone()
                    };

                    // Maybe promote to higher tier
                    let promoted = if self.config.auto_promote && *tier != CacheTier::L1Memory {
                        if let Some(target) = tier.promotion_target() {
                            // Only promote small enough entries
                            if entry.stored_size() <= target.max_object_size() {
                                self.promote(&entry, target).await.unwrap_or(false)
                            } else {
                                false
                            }
                        } else {
                            false
                        }
                    } else {
                        false
                    };

                    // Emit event
                    self.emit_event(CacheEvent::hit(key, *tier, promoted));

                    return Ok(CacheLookupResult::Hit {
                        data,
                        tier: *tier,
                        promoted,
                    });
                }
                Ok(None) => {
                    self.metrics.tier(*tier).record_miss();
                    continue;
                }
                Err(e) => {
                    warn!(tier = %tier, error = %e, "Tier lookup failed");
                    continue;
                }
            }
        }

        // Cache miss
        self.emit_event(CacheEvent::miss(key));
        Ok(CacheLookupResult::Miss)
    }

    async fn put(&self, key: CacheKey, data: Bytes) -> Result<CacheTier> {
        let size = data.len() as u64;

        // Check if should bypass cache
        if size > CACHE_BYPASS_SIZE_BYTES {
            return Err(Error::Internal(format!(
                "Object size {} exceeds cache bypass threshold {}",
                size, CACHE_BYPASS_SIZE_BYTES
            )));
        }

        // Determine target tier based on size
        let tier = CacheTier::for_size(size).ok_or_else(|| {
            Error::Internal(format!("No suitable tier for size {}", size))
        })?;

        self.put_with_tier(key, data, tier).await
    }

    async fn put_with_tier(&self, key: CacheKey, data: Bytes, tier: CacheTier) -> Result<CacheTier> {
        let original_size = data.len() as u64;

        // Compress if tier supports it
        let tier_config = self.tier_config(tier);
        let (stored_data, algorithm) = if tier_config.enable_compression {
            self.compression.compress(&data)
        } else {
            (data.clone(), crate::cache::entry::CompressionAlgorithm::None)
        };

        let cache_data = if algorithm != crate::cache::entry::CompressionAlgorithm::None {
            CacheData::compressed(stored_data, original_size, algorithm)
        } else {
            CacheData::uncompressed(stored_data)
        };

        let entry = CacheEntry::new(key.clone(), cache_data, tier);
        let stored_size = entry.stored_size();

        // Ensure capacity
        self.ensure_capacity(tier, stored_size).await?;

        // Store in appropriate tier
        match tier {
            CacheTier::L1Memory => self.l1.put(entry.clone()).await?,
            CacheTier::L2Local => self.l2.put(entry.clone()).await?,
            CacheTier::L3Persistent => self.l3.put(entry.clone()).await?,
        }

        // Update LRU tracker
        self.lru.track(EntryMetadata::from_entry(&entry));

        // Update metrics
        self.metrics.tier(tier).record_put(stored_size);

        // Emit event
        self.emit_event(CacheEvent::put(
            &key,
            tier,
            stored_size,
            algorithm != crate::cache::entry::CompressionAlgorithm::None,
        ));

        debug!(
            key = %key,
            tier = %tier,
            size = stored_size,
            compressed = algorithm != crate::cache::entry::CompressionAlgorithm::None,
            "Stored cache entry"
        );

        Ok(tier)
    }

    async fn delete(&self, key: &CacheKey) -> Result<bool> {
        let mut deleted = false;

        // Delete from all tiers
        for tier in CacheTier::lookup_order() {
            let storage = self.storage(*tier);
            if let Ok(Some(entry)) = storage.delete(key).await {
                self.metrics.tier(*tier).record_remove(entry.stored_size());
                deleted = true;

                self.emit_event(CacheEvent::Delete {
                    key: key.to_storage_key(),
                    tier: *tier,
                });
            }
        }

        // Remove from LRU tracker
        self.lru.remove(key);

        Ok(deleted)
    }

    async fn prefetch(&self, keys: Vec<CacheKey>) -> Result<()> {
        if keys.is_empty() {
            return Ok(());
        }

        self.metrics.record_prefetch_request();

        let request = PrefetchRequest::new(keys);
        self.prefetcher.submit(request).await
    }

    fn stats(&self) -> CacheStatsSnapshot {
        self.metrics.snapshot()
    }

    async fn health_check(&self) -> Result<bool> {
        let l1_ok = self.l1.health_check().await.unwrap_or(false);
        let l2_ok = self.l2.health_check().await.unwrap_or(false);
        let l3_ok = self.l3.health_check().await.unwrap_or(false);

        Ok(l1_ok && l2_ok && l3_ok)
    }

    async fn evict(&self, tier: CacheTier, bytes_to_free: u64) -> Result<u64> {
        let mut freed = 0u64;

        // Get eviction candidates
        let candidates = self.lru.get_eviction_candidates(tier, bytes_to_free);

        for candidate in candidates {
            if freed >= bytes_to_free {
                break;
            }

            // Get the entry
            let storage = self.storage(tier);
            if let Ok(Some(entry)) = storage.delete(&candidate.key).await {
                let size = entry.stored_size();

                // Try to demote if enabled
                let demoted = if self.tier_config(tier).enable_demotion {
                    self.demote(&entry).await.unwrap_or(false)
                } else {
                    false
                };

                if !demoted {
                    // Entry was deleted, not demoted
                    self.emit_event(CacheEvent::evict(
                        &candidate.key,
                        tier,
                        size,
                        EvictionReason::Capacity,
                    ));
                }

                // Update metrics
                if demoted {
                    self.metrics.tier(tier).record_demotion(size);
                } else {
                    self.metrics.tier(tier).record_eviction(size);
                }

                // Remove from LRU (or update if demoted)
                if !demoted {
                    self.lru.remove(&candidate.key);
                }

                freed += size;
            }
        }

        debug!(tier = %tier, freed = freed, "Evicted entries from cache tier");
        Ok(freed)
    }

    async fn clear_tier(&self, tier: CacheTier) -> Result<()> {
        let storage = self.storage(tier);
        let entries = storage.entry_count();
        let bytes = storage.size_bytes();

        storage.clear().await?;
        self.lru.clear_tier(tier);
        self.metrics.tier(tier).reset_storage();

        self.emit_event(CacheEvent::TierCleared {
            tier,
            entries_removed: entries,
            bytes_freed: bytes,
        });

        info!(tier = %tier, entries = entries, bytes = bytes, "Cleared cache tier");
        Ok(())
    }

    async fn clear_all(&self) -> Result<()> {
        for tier in CacheTier::lookup_order() {
            self.clear_tier(*tier).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    async fn test_cache() -> (Arc<MultiTierCache>, TempDir) {
        let tmp = TempDir::new().unwrap();
        let config = MultiTierCacheConfig {
            l2_path: Some(tmp.path().to_string_lossy().to_string()),
            ..Default::default()
        };
        let cache = MultiTierCache::with_config(config).await.unwrap();
        (cache, tmp)
    }

    #[tokio::test]
    async fn test_put_and_get() {
        let (cache, _tmp) = test_cache().await;

        let key = CacheKey::new("test", "file1");
        let data = Bytes::from("hello world");

        // Put
        let tier = cache.put(key.clone(), data.clone()).await.unwrap();
        assert_eq!(tier, CacheTier::L1Memory); // Small data goes to L1

        // Get
        let result = cache.get(&key).await.unwrap();
        match result {
            CacheLookupResult::Hit { data: retrieved, tier, .. } => {
                assert_eq!(retrieved, data);
                assert_eq!(tier, CacheTier::L1Memory);
            }
            CacheLookupResult::Miss => panic!("Expected hit"),
        }
    }

    #[tokio::test]
    async fn test_delete() {
        let (cache, _tmp) = test_cache().await;

        let key = CacheKey::new("test", "file1");
        let data = Bytes::from("hello world");

        cache.put(key.clone(), data).await.unwrap();
        assert!(cache.get(&key).await.unwrap().is_hit());

        let deleted = cache.delete(&key).await.unwrap();
        assert!(deleted);

        assert!(cache.get(&key).await.unwrap().is_miss());
    }

    #[tokio::test]
    async fn test_stats() {
        let (cache, _tmp) = test_cache().await;

        let key = CacheKey::new("test", "file1");
        let data = Bytes::from("hello world");

        cache.put(key.clone(), data).await.unwrap();

        let stats = cache.stats();
        assert_eq!(stats.total_entry_count, 1);
        assert!(stats.total_bytes_stored > 0);
    }

    #[tokio::test]
    async fn test_tier_selection() {
        let (cache, _tmp) = test_cache().await;

        // Small data -> L1
        let small = Bytes::from("small");
        let tier = cache.put(CacheKey::new("test", "small"), small).await.unwrap();
        assert_eq!(tier, CacheTier::L1Memory);
    }

    #[tokio::test]
    async fn test_cache_miss() {
        let (cache, _tmp) = test_cache().await;

        let key = CacheKey::new("test", "nonexistent");
        let result = cache.get(&key).await.unwrap();
        assert!(result.is_miss());
    }

    #[tokio::test]
    async fn test_clear_tier() {
        let (cache, _tmp) = test_cache().await;

        cache.put(CacheKey::new("test", "file1"), Bytes::from("data1")).await.unwrap();
        cache.put(CacheKey::new("test", "file2"), Bytes::from("data2")).await.unwrap();

        let stats = cache.stats();
        assert_eq!(stats.total_entry_count, 2);

        cache.clear_tier(CacheTier::L1Memory).await.unwrap();

        let stats = cache.stats();
        assert_eq!(stats.get_tier_stats(CacheTier::L1Memory).entry_count, 0);
    }
}

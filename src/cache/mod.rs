//! Multi-Tier Caching System
//!
//! A three-tier caching system optimized for storage workloads:
//! - **L1 Memory**: In-memory cache for hot data (<100MB objects)
//! - **L2 Local**: Local SSD cache for warm data (<1GB objects)
//! - **L3 Persistent**: Persistent storage for cold data (<10GB objects)
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────┐
//! │                        Multi-Tier Cache Manager                          │
//! ├─────────────────────────────────────────────────────────────────────────┤
//! │  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────────────┐   │
//! │  │  L1 Memory   │  │  L2 Local    │  │      L3 Persistent           │   │
//! │  │  (DashMap)   │  │  (Local SSD) │  │   (Backend Storage)          │   │
//! │  │  <100MB      │  │  <1GB        │  │      <10GB                   │   │
//! │  └──────────────┘  └──────────────┘  └──────────────────────────────┘   │
//! │         │                  │                       │                     │
//! │         └──────────────────┼───────────────────────┘                     │
//! │                            │                                             │
//! │                    ┌───────┴────────┐                                    │
//! │                    │  LRU Tracker   │                                    │
//! │                    │  (64-sharded)  │                                    │
//! │                    └────────────────┘                                    │
//! ├─────────────────────────────────────────────────────────────────────────┤
//! │                         Features                                         │
//! │  • Size-based tier placement    • LRU eviction with tier demotion       │
//! │  • Async prefetching            • Multiple compression algorithms        │
//! │  • Cache-line aligned metrics   • Concurrent access (lock-free reads)   │
//! └─────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Usage
//!
//! ```ignore
//! use smart_storage_operator::cache::{MultiTierCache, MultiTierCacheConfig, CacheKey};
//! use bytes::Bytes;
//!
//! let config = MultiTierCacheConfig::default();
//! let cache = MultiTierCache::new(config).await?;
//!
//! // Store data (automatically placed in appropriate tier)
//! let key = CacheKey::new("objects", "my-bucket/my-file.txt");
//! let tier = cache.put(key.clone(), Bytes::from("hello world")).await?;
//!
//! // Retrieve data (searches L1 -> L2 -> L3)
//! match cache.get(&key).await? {
//!     CacheLookupResult::Hit { data, tier } => {
//!         println!("Found in {}: {:?}", tier, data);
//!     }
//!     CacheLookupResult::Miss => {
//!         println!("Not in cache");
//!     }
//! }
//!
//! // Prefetch for anticipated access
//! cache.prefetch(vec![key1, key2, key3]).await?;
//!
//! // Get statistics
//! let stats = cache.stats();
//! println!("Hit ratio: {:.2}%", stats.hit_ratio() * 100.0);
//! ```

pub mod compression;
pub mod entry;
pub mod events;
pub mod lru;
pub mod manager;
pub mod metrics;
pub mod prefetch;
pub mod storage;
pub mod tier;

// Re-export main types
pub use compression::{CompressionAlgorithm, Compressor, CompressionConfig};
pub use entry::{CacheData, CacheEntry, CacheKey, EntryMetadata};
pub use events::CacheEvent;
pub use lru::{EvictionCandidate, EvictionPolicy, ShardedLruTracker};
pub use manager::{MultiTierCache, MultiTierCacheConfig};
pub use metrics::{CacheMetrics, CacheStatsSnapshot, CacheTierMetrics, TierMetricsSnapshot};
pub use prefetch::{Prefetcher, PrefetchConfig, PrefetchRequest};
pub use storage::{TierStorage, MemoryStorage, LocalStorage, PersistentStorage};
pub use tier::{CacheTier, TierConfig, L1_MAX_SIZE_BYTES, L2_MAX_SIZE_BYTES, CACHE_BYPASS_SIZE_BYTES};

use crate::error::Result;
use async_trait::async_trait;
use bytes::Bytes;
use std::sync::Arc;

// =============================================================================
// Cache Lookup Result
// =============================================================================

/// Result of a cache lookup operation
#[derive(Debug, Clone)]
pub enum CacheLookupResult {
    /// Cache hit with data and source tier
    Hit {
        /// The cached data (decompressed)
        data: Bytes,
        /// Tier where the data was found
        tier: CacheTier,
        /// Whether data was promoted to a higher tier
        promoted: bool,
    },
    /// Cache miss (not found in any tier)
    Miss,
}

impl CacheLookupResult {
    /// Check if this is a cache hit
    pub fn is_hit(&self) -> bool {
        matches!(self, CacheLookupResult::Hit { .. })
    }

    /// Check if this is a cache miss
    pub fn is_miss(&self) -> bool {
        matches!(self, CacheLookupResult::Miss)
    }

    /// Get the data if this is a hit
    pub fn data(&self) -> Option<&Bytes> {
        match self {
            CacheLookupResult::Hit { data, .. } => Some(data),
            CacheLookupResult::Miss => None,
        }
    }

    /// Get the tier if this is a hit
    pub fn tier(&self) -> Option<CacheTier> {
        match self {
            CacheLookupResult::Hit { tier, .. } => Some(*tier),
            CacheLookupResult::Miss => None,
        }
    }
}

// =============================================================================
// StorageCache Trait (Port)
// =============================================================================

/// Main trait for cache operations
///
/// This trait defines the port for cache implementations, following the
/// hexagonal architecture pattern used throughout the operator.
#[async_trait]
pub trait StorageCache: Send + Sync {
    /// Look up a key in the cache
    ///
    /// Searches tiers in order: L1 -> L2 -> L3
    /// On hit, may promote data to higher tier based on access patterns.
    async fn get(&self, key: &CacheKey) -> Result<CacheLookupResult>;

    /// Store data in the cache
    ///
    /// Data is automatically placed in the appropriate tier based on size.
    /// Returns the tier where data was stored.
    async fn put(&self, key: CacheKey, data: Bytes) -> Result<CacheTier>;

    /// Store data with explicit tier preference
    ///
    /// If the preferred tier cannot accommodate the data (size constraints),
    /// falls back to an appropriate tier.
    async fn put_with_tier(&self, key: CacheKey, data: Bytes, tier: CacheTier) -> Result<CacheTier>;

    /// Delete a key from all tiers
    ///
    /// Returns true if the key was found and deleted.
    async fn delete(&self, key: &CacheKey) -> Result<bool>;

    /// Prefetch keys into cache
    ///
    /// Asynchronously loads keys into cache for anticipated access.
    /// Keys are loaded from backend storage if not already cached.
    async fn prefetch(&self, keys: Vec<CacheKey>) -> Result<()>;

    /// Get current cache statistics
    fn stats(&self) -> CacheStatsSnapshot;

    /// Check if cache is healthy
    async fn health_check(&self) -> Result<bool>;

    /// Evict entries to free up space
    ///
    /// Evicts least-recently-used entries until the specified bytes are freed.
    /// Returns the actual number of bytes freed.
    async fn evict(&self, tier: CacheTier, bytes_to_free: u64) -> Result<u64>;

    /// Clear all entries from a specific tier
    async fn clear_tier(&self, tier: CacheTier) -> Result<()>;

    /// Clear all entries from all tiers
    async fn clear_all(&self) -> Result<()>;
}

/// Type alias for Arc'd StorageCache
pub type StorageCacheRef = Arc<dyn StorageCache>;

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lookup_result_accessors() {
        let hit = CacheLookupResult::Hit {
            data: Bytes::from("test"),
            tier: CacheTier::L1Memory,
            promoted: false,
        };
        assert!(hit.is_hit());
        assert!(!hit.is_miss());
        assert_eq!(hit.data(), Some(&Bytes::from("test")));
        assert_eq!(hit.tier(), Some(CacheTier::L1Memory));

        let miss = CacheLookupResult::Miss;
        assert!(!miss.is_hit());
        assert!(miss.is_miss());
        assert_eq!(miss.data(), None);
        assert_eq!(miss.tier(), None);
    }
}

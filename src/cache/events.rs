//! Cache Events
//!
//! Events emitted by the cache for monitoring and observability.

use crate::cache::entry::CacheKey;
use crate::cache::tier::CacheTier;
use serde::{Deserialize, Serialize};

/// Events emitted by the cache system
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CacheEvent {
    /// Entry was stored in cache
    Put {
        key: String,
        tier: CacheTier,
        size_bytes: u64,
        compressed: bool,
    },

    /// Cache hit
    Hit {
        key: String,
        tier: CacheTier,
        promoted: bool,
    },

    /// Cache miss (not found in any tier)
    Miss {
        key: String,
    },

    /// Entry was deleted
    Delete {
        key: String,
        tier: CacheTier,
    },

    /// Entry was evicted due to capacity
    Evict {
        key: String,
        tier: CacheTier,
        size_bytes: u64,
        reason: EvictionReason,
    },

    /// Entry was demoted to lower tier
    Demote {
        key: String,
        from_tier: CacheTier,
        to_tier: CacheTier,
        size_bytes: u64,
    },

    /// Entry was promoted to higher tier
    Promote {
        key: String,
        from_tier: CacheTier,
        to_tier: CacheTier,
        size_bytes: u64,
    },

    /// Prefetch request completed
    PrefetchComplete {
        keys_requested: usize,
        keys_loaded: usize,
        bytes_loaded: u64,
    },

    /// Tier became unavailable
    TierUnavailable {
        tier: CacheTier,
        reason: String,
    },

    /// Tier recovered
    TierRecovered {
        tier: CacheTier,
    },

    /// Tier cleared
    TierCleared {
        tier: CacheTier,
        entries_removed: u64,
        bytes_freed: u64,
    },

    /// Compression error (fell back to uncompressed)
    CompressionFailed {
        key: String,
        algorithm: String,
        error: String,
    },

    /// Cache statistics snapshot
    StatsSnapshot {
        total_entries: u64,
        total_bytes: u64,
        hit_ratio: f64,
        l1_entries: u64,
        l2_entries: u64,
        l3_entries: u64,
    },
}

/// Reason for eviction
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum EvictionReason {
    /// Capacity limit reached
    Capacity,
    /// Entry expired (TTL)
    Expired,
    /// Manual eviction request
    Manual,
    /// Entry corrupted
    Corrupted,
}

impl std::fmt::Display for EvictionReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EvictionReason::Capacity => write!(f, "capacity"),
            EvictionReason::Expired => write!(f, "expired"),
            EvictionReason::Manual => write!(f, "manual"),
            EvictionReason::Corrupted => write!(f, "corrupted"),
        }
    }
}

impl CacheEvent {
    /// Create a Put event
    pub fn put(key: &CacheKey, tier: CacheTier, size_bytes: u64, compressed: bool) -> Self {
        CacheEvent::Put {
            key: key.to_storage_key(),
            tier,
            size_bytes,
            compressed,
        }
    }

    /// Create a Hit event
    pub fn hit(key: &CacheKey, tier: CacheTier, promoted: bool) -> Self {
        CacheEvent::Hit {
            key: key.to_storage_key(),
            tier,
            promoted,
        }
    }

    /// Create a Miss event
    pub fn miss(key: &CacheKey) -> Self {
        CacheEvent::Miss {
            key: key.to_storage_key(),
        }
    }

    /// Create an Evict event
    pub fn evict(key: &CacheKey, tier: CacheTier, size_bytes: u64, reason: EvictionReason) -> Self {
        CacheEvent::Evict {
            key: key.to_storage_key(),
            tier,
            size_bytes,
            reason,
        }
    }

    /// Create a Demote event
    pub fn demote(key: &CacheKey, from_tier: CacheTier, to_tier: CacheTier, size_bytes: u64) -> Self {
        CacheEvent::Demote {
            key: key.to_storage_key(),
            from_tier,
            to_tier,
            size_bytes,
        }
    }

    /// Create a Promote event
    pub fn promote(key: &CacheKey, from_tier: CacheTier, to_tier: CacheTier, size_bytes: u64) -> Self {
        CacheEvent::Promote {
            key: key.to_storage_key(),
            from_tier,
            to_tier,
            size_bytes,
        }
    }

    /// Get the key associated with this event (if any)
    pub fn key(&self) -> Option<&str> {
        match self {
            CacheEvent::Put { key, .. } => Some(key),
            CacheEvent::Hit { key, .. } => Some(key),
            CacheEvent::Miss { key } => Some(key),
            CacheEvent::Delete { key, .. } => Some(key),
            CacheEvent::Evict { key, .. } => Some(key),
            CacheEvent::Demote { key, .. } => Some(key),
            CacheEvent::Promote { key, .. } => Some(key),
            CacheEvent::CompressionFailed { key, .. } => Some(key),
            _ => None,
        }
    }

    /// Get the tier associated with this event (if any)
    pub fn tier(&self) -> Option<CacheTier> {
        match self {
            CacheEvent::Put { tier, .. } => Some(*tier),
            CacheEvent::Hit { tier, .. } => Some(*tier),
            CacheEvent::Delete { tier, .. } => Some(*tier),
            CacheEvent::Evict { tier, .. } => Some(*tier),
            CacheEvent::TierUnavailable { tier, .. } => Some(*tier),
            CacheEvent::TierRecovered { tier } => Some(*tier),
            CacheEvent::TierCleared { tier, .. } => Some(*tier),
            _ => None,
        }
    }

    /// Check if this is an error event
    pub fn is_error(&self) -> bool {
        matches!(
            self,
            CacheEvent::TierUnavailable { .. } | CacheEvent::CompressionFailed { .. }
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_creation() {
        let key = CacheKey::new("objects", "my-file.txt");

        let hit = CacheEvent::hit(&key, CacheTier::L1Memory, false);
        assert_eq!(hit.key(), Some("objects:my-file.txt"));
        assert_eq!(hit.tier(), Some(CacheTier::L1Memory));
        assert!(!hit.is_error());

        let unavailable = CacheEvent::TierUnavailable {
            tier: CacheTier::L2Local,
            reason: "disk full".to_string(),
        };
        assert!(unavailable.is_error());
    }

    #[test]
    fn test_eviction_reason_display() {
        assert_eq!(format!("{}", EvictionReason::Capacity), "capacity");
        assert_eq!(format!("{}", EvictionReason::Expired), "expired");
    }
}

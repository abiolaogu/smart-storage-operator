//! Cache Tier Definitions
//!
//! Defines the three-tier caching hierarchy with size-based placement rules.

use serde::{Deserialize, Serialize};
use std::fmt;

// =============================================================================
// Size Thresholds
// =============================================================================

/// Maximum size for L1 (memory) tier: 100 MB
pub const L1_MAX_SIZE_BYTES: u64 = 100 * 1024 * 1024;

/// Maximum size for L2 (local SSD) tier: 1 GB
pub const L2_MAX_SIZE_BYTES: u64 = 1024 * 1024 * 1024;

/// Objects larger than this bypass the cache entirely: 10 GB
pub const CACHE_BYPASS_SIZE_BYTES: u64 = 10 * 1024 * 1024 * 1024;

// =============================================================================
// Cache Tier
// =============================================================================

/// Cache tier representing the storage hierarchy
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CacheTier {
    /// L1: In-memory cache (fastest, smallest)
    #[default]
    L1Memory,
    /// L2: Local SSD cache (fast, medium capacity)
    L2Local,
    /// L3: Persistent storage cache (slower, largest capacity)
    L3Persistent,
}

impl CacheTier {
    /// Determine the appropriate tier for an object based on its size
    pub fn for_size(size_bytes: u64) -> Option<Self> {
        if size_bytes > CACHE_BYPASS_SIZE_BYTES {
            None // Too large, bypass cache
        } else if size_bytes <= L1_MAX_SIZE_BYTES {
            Some(CacheTier::L1Memory)
        } else if size_bytes <= L2_MAX_SIZE_BYTES {
            Some(CacheTier::L2Local)
        } else {
            Some(CacheTier::L3Persistent)
        }
    }

    /// Get the maximum object size for this tier
    pub fn max_object_size(&self) -> u64 {
        match self {
            CacheTier::L1Memory => L1_MAX_SIZE_BYTES,
            CacheTier::L2Local => L2_MAX_SIZE_BYTES,
            CacheTier::L3Persistent => CACHE_BYPASS_SIZE_BYTES,
        }
    }

    /// Get the demotion target tier (where entries go when evicted)
    pub fn demotion_target(&self) -> Option<CacheTier> {
        match self {
            CacheTier::L1Memory => Some(CacheTier::L2Local),
            CacheTier::L2Local => Some(CacheTier::L3Persistent),
            CacheTier::L3Persistent => None, // Evicted from cache entirely
        }
    }

    /// Get the promotion target tier (where entries can be promoted to)
    pub fn promotion_target(&self) -> Option<CacheTier> {
        match self {
            CacheTier::L1Memory => None, // Already at top tier
            CacheTier::L2Local => Some(CacheTier::L1Memory),
            CacheTier::L3Persistent => Some(CacheTier::L2Local),
        }
    }

    /// Check if this tier is higher priority than another
    pub fn is_higher_than(&self, other: &CacheTier) -> bool {
        match (self, other) {
            (CacheTier::L1Memory, CacheTier::L2Local | CacheTier::L3Persistent) => true,
            (CacheTier::L2Local, CacheTier::L3Persistent) => true,
            _ => false,
        }
    }

    /// Get tier priority (lower is higher priority)
    pub fn priority(&self) -> u8 {
        match self {
            CacheTier::L1Memory => 0,
            CacheTier::L2Local => 1,
            CacheTier::L3Persistent => 2,
        }
    }

    /// Get all tiers in lookup order (L1 -> L2 -> L3)
    pub fn lookup_order() -> &'static [CacheTier] {
        &[CacheTier::L1Memory, CacheTier::L2Local, CacheTier::L3Persistent]
    }
}

impl fmt::Display for CacheTier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CacheTier::L1Memory => write!(f, "L1-Memory"),
            CacheTier::L2Local => write!(f, "L2-Local"),
            CacheTier::L3Persistent => write!(f, "L3-Persistent"),
        }
    }
}

// =============================================================================
// Tier Configuration
// =============================================================================

/// Configuration for a cache tier
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TierConfig {
    /// Tier this config applies to
    pub tier: CacheTier,
    /// Maximum capacity in bytes for this tier
    pub capacity_bytes: u64,
    /// Threshold (0.0-1.0) at which eviction starts
    pub eviction_threshold: f32,
    /// Whether to demote entries instead of deleting on eviction
    pub enable_demotion: bool,
    /// Whether compression is enabled for this tier
    pub enable_compression: bool,
    /// Target compression ratio (if compression enabled)
    pub target_compression_ratio: f32,
}

impl Default for TierConfig {
    fn default() -> Self {
        Self {
            tier: CacheTier::L1Memory,
            capacity_bytes: 512 * 1024 * 1024, // 512 MB default
            eviction_threshold: 0.85,
            enable_demotion: true,
            enable_compression: false,
            target_compression_ratio: 0.5,
        }
    }
}

impl TierConfig {
    /// Create a default config for L1 Memory tier
    pub fn l1_default() -> Self {
        Self {
            tier: CacheTier::L1Memory,
            capacity_bytes: 512 * 1024 * 1024, // 512 MB
            eviction_threshold: 0.90,
            enable_demotion: true,
            enable_compression: false,
            target_compression_ratio: 1.0, // No compression
        }
    }

    /// Create a default config for L2 Local tier
    pub fn l2_default() -> Self {
        Self {
            tier: CacheTier::L2Local,
            capacity_bytes: 10 * 1024 * 1024 * 1024, // 10 GB
            eviction_threshold: 0.85,
            enable_demotion: true,
            enable_compression: true,
            target_compression_ratio: 0.6,
        }
    }

    /// Create a default config for L3 Persistent tier
    pub fn l3_default() -> Self {
        Self {
            tier: CacheTier::L3Persistent,
            capacity_bytes: 100 * 1024 * 1024 * 1024, // 100 GB
            eviction_threshold: 0.80,
            enable_demotion: false, // Nothing to demote to
            enable_compression: true,
            target_compression_ratio: 0.5,
        }
    }

    /// Calculate the eviction watermark in bytes
    pub fn eviction_watermark(&self) -> u64 {
        (self.capacity_bytes as f64 * self.eviction_threshold as f64) as u64
    }

    /// Check if the tier is above eviction threshold
    pub fn should_evict(&self, current_bytes: u64) -> bool {
        current_bytes > self.eviction_watermark()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tier_for_size() {
        // Small objects go to L1
        assert_eq!(CacheTier::for_size(1024), Some(CacheTier::L1Memory));
        assert_eq!(CacheTier::for_size(50 * 1024 * 1024), Some(CacheTier::L1Memory));
        assert_eq!(CacheTier::for_size(L1_MAX_SIZE_BYTES), Some(CacheTier::L1Memory));

        // Medium objects go to L2
        assert_eq!(CacheTier::for_size(L1_MAX_SIZE_BYTES + 1), Some(CacheTier::L2Local));
        assert_eq!(CacheTier::for_size(500 * 1024 * 1024), Some(CacheTier::L2Local));
        assert_eq!(CacheTier::for_size(L2_MAX_SIZE_BYTES), Some(CacheTier::L2Local));

        // Large objects go to L3
        assert_eq!(CacheTier::for_size(L2_MAX_SIZE_BYTES + 1), Some(CacheTier::L3Persistent));
        assert_eq!(CacheTier::for_size(5 * 1024 * 1024 * 1024), Some(CacheTier::L3Persistent));

        // Very large objects bypass cache
        assert_eq!(CacheTier::for_size(CACHE_BYPASS_SIZE_BYTES + 1), None);
        assert_eq!(CacheTier::for_size(15 * 1024 * 1024 * 1024), None);
    }

    #[test]
    fn test_tier_demotion() {
        assert_eq!(CacheTier::L1Memory.demotion_target(), Some(CacheTier::L2Local));
        assert_eq!(CacheTier::L2Local.demotion_target(), Some(CacheTier::L3Persistent));
        assert_eq!(CacheTier::L3Persistent.demotion_target(), None);
    }

    #[test]
    fn test_tier_promotion() {
        assert_eq!(CacheTier::L1Memory.promotion_target(), None);
        assert_eq!(CacheTier::L2Local.promotion_target(), Some(CacheTier::L1Memory));
        assert_eq!(CacheTier::L3Persistent.promotion_target(), Some(CacheTier::L2Local));
    }

    #[test]
    fn test_tier_priority() {
        assert!(CacheTier::L1Memory.is_higher_than(&CacheTier::L2Local));
        assert!(CacheTier::L1Memory.is_higher_than(&CacheTier::L3Persistent));
        assert!(CacheTier::L2Local.is_higher_than(&CacheTier::L3Persistent));
        assert!(!CacheTier::L2Local.is_higher_than(&CacheTier::L1Memory));
    }

    #[test]
    fn test_tier_config_eviction() {
        let config = TierConfig {
            tier: CacheTier::L1Memory,
            capacity_bytes: 1000,
            eviction_threshold: 0.8,
            enable_demotion: true,
            enable_compression: false,
            target_compression_ratio: 1.0,
        };

        assert_eq!(config.eviction_watermark(), 800);
        assert!(!config.should_evict(700));
        assert!(!config.should_evict(800));
        assert!(config.should_evict(801));
    }
}

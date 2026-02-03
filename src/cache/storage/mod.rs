//! Cache Storage Backends
//!
//! Implementations for each cache tier's storage layer.

mod local;
mod memory;
mod persistent;

pub use local::LocalStorage;
pub use memory::MemoryStorage;
pub use persistent::PersistentStorage;

use crate::cache::entry::{CacheEntry, CacheKey};
use crate::cache::tier::CacheTier;
use crate::error::Result;
use async_trait::async_trait;

// =============================================================================
// TierStorage Trait
// =============================================================================

/// Trait for tier-specific storage operations
///
/// Each tier implements this trait to provide storage operations.
/// The trait is intentionally simple to allow different implementations
/// (in-memory, disk-based, remote storage) to be used interchangeably.
#[async_trait]
pub trait TierStorage: Send + Sync {
    /// Get the tier this storage serves
    fn tier(&self) -> CacheTier;

    /// Get an entry by key
    async fn get(&self, key: &CacheKey) -> Result<Option<CacheEntry>>;

    /// Store an entry
    async fn put(&self, entry: CacheEntry) -> Result<()>;

    /// Delete an entry by key
    ///
    /// Returns the deleted entry if it existed.
    async fn delete(&self, key: &CacheKey) -> Result<Option<CacheEntry>>;

    /// Check if a key exists
    async fn contains(&self, key: &CacheKey) -> Result<bool>;

    /// Get current storage size in bytes
    fn size_bytes(&self) -> u64;

    /// Get current entry count
    fn entry_count(&self) -> u64;

    /// Get all keys in this storage
    ///
    /// Note: This may be expensive for large storages.
    async fn keys(&self) -> Result<Vec<CacheKey>>;

    /// Clear all entries
    async fn clear(&self) -> Result<()>;

    /// Check if storage is available/healthy
    async fn health_check(&self) -> Result<bool>;
}

/// Type alias for boxed tier storage
pub type BoxedTierStorage = Box<dyn TierStorage>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::entry::CacheData;
    use bytes::Bytes;

    /// Helper to create a test entry
    pub fn test_entry(namespace: &str, id: &str, data: &[u8]) -> CacheEntry {
        let key = CacheKey::new(namespace, id);
        let cache_data = CacheData::uncompressed(Bytes::copy_from_slice(data));
        CacheEntry::new(key, cache_data, CacheTier::L1Memory)
    }
}

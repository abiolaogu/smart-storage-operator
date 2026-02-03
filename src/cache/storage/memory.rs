//! L1 In-Memory Storage
//!
//! High-performance in-memory cache using DashMap for concurrent access.

use crate::cache::entry::{CacheEntry, CacheKey};
use crate::cache::storage::TierStorage;
use crate::cache::tier::CacheTier;
use crate::error::Result;
use async_trait::async_trait;
use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};

// =============================================================================
// Memory Storage Configuration
// =============================================================================

/// Configuration for memory storage
#[derive(Debug, Clone)]
pub struct MemoryStorageConfig {
    /// Maximum capacity in bytes
    pub capacity_bytes: u64,
    /// Number of shards for DashMap (0 = auto)
    pub shard_count: usize,
}

impl Default for MemoryStorageConfig {
    fn default() -> Self {
        Self {
            capacity_bytes: 512 * 1024 * 1024, // 512 MB
            shard_count: 0,                     // Auto
        }
    }
}

// =============================================================================
// Memory Storage
// =============================================================================

/// L1 in-memory cache storage backed by DashMap
pub struct MemoryStorage {
    /// The actual storage map
    entries: DashMap<String, CacheEntry>,
    /// Current total size in bytes
    size_bytes: AtomicU64,
    /// Current entry count
    entry_count: AtomicU64,
    /// Maximum capacity
    capacity_bytes: u64,
}

impl MemoryStorage {
    /// Create new memory storage with default config
    pub fn new() -> Self {
        Self::with_config(MemoryStorageConfig::default())
    }

    /// Create new memory storage with specified capacity
    pub fn with_capacity(capacity_bytes: u64) -> Self {
        Self::with_config(MemoryStorageConfig {
            capacity_bytes,
            ..Default::default()
        })
    }

    /// Create new memory storage with full config
    pub fn with_config(config: MemoryStorageConfig) -> Self {
        let entries = if config.shard_count > 0 {
            DashMap::with_shard_amount(config.shard_count)
        } else {
            DashMap::new()
        };

        Self {
            entries,
            size_bytes: AtomicU64::new(0),
            entry_count: AtomicU64::new(0),
            capacity_bytes: config.capacity_bytes,
        }
    }

    /// Get current capacity
    pub fn capacity_bytes(&self) -> u64 {
        self.capacity_bytes
    }

    /// Get available space
    pub fn available_bytes(&self) -> u64 {
        self.capacity_bytes.saturating_sub(self.size_bytes.load(Ordering::Relaxed))
    }

    /// Check if storage has space for new entry
    pub fn has_space_for(&self, size_bytes: u64) -> bool {
        self.size_bytes.load(Ordering::Relaxed) + size_bytes <= self.capacity_bytes
    }

    /// Update an entry in place (for access tracking)
    pub fn touch(&self, key: &CacheKey) -> bool {
        let storage_key = key.to_storage_key();
        if let Some(mut entry) = self.entries.get_mut(&storage_key) {
            entry.record_access();
            true
        } else {
            false
        }
    }
}

impl Default for MemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl TierStorage for MemoryStorage {
    fn tier(&self) -> CacheTier {
        CacheTier::L1Memory
    }

    async fn get(&self, key: &CacheKey) -> Result<Option<CacheEntry>> {
        let storage_key = key.to_storage_key();
        Ok(self.entries.get(&storage_key).map(|r| r.value().clone()))
    }

    async fn put(&self, entry: CacheEntry) -> Result<()> {
        let storage_key = entry.key.to_storage_key();
        let new_size = entry.stored_size();

        // Check for existing entry to update size tracking correctly
        let old_size = self
            .entries
            .get(&storage_key)
            .map(|e| e.stored_size())
            .unwrap_or(0);

        if old_size == 0 {
            // New entry
            self.entry_count.fetch_add(1, Ordering::Relaxed);
            self.size_bytes.fetch_add(new_size, Ordering::Relaxed);
        } else {
            // Replacement - adjust size delta
            if new_size > old_size {
                self.size_bytes.fetch_add(new_size - old_size, Ordering::Relaxed);
            } else {
                self.size_bytes.fetch_sub(old_size - new_size, Ordering::Relaxed);
            }
        }

        self.entries.insert(storage_key, entry);
        Ok(())
    }

    async fn delete(&self, key: &CacheKey) -> Result<Option<CacheEntry>> {
        let storage_key = key.to_storage_key();
        if let Some((_, entry)) = self.entries.remove(&storage_key) {
            self.entry_count.fetch_sub(1, Ordering::Relaxed);
            self.size_bytes.fetch_sub(entry.stored_size(), Ordering::Relaxed);
            Ok(Some(entry))
        } else {
            Ok(None)
        }
    }

    async fn contains(&self, key: &CacheKey) -> Result<bool> {
        Ok(self.entries.contains_key(&key.to_storage_key()))
    }

    fn size_bytes(&self) -> u64 {
        self.size_bytes.load(Ordering::Relaxed)
    }

    fn entry_count(&self) -> u64 {
        self.entry_count.load(Ordering::Relaxed)
    }

    async fn keys(&self) -> Result<Vec<CacheKey>> {
        let keys: Vec<CacheKey> = self
            .entries
            .iter()
            .filter_map(|r| CacheKey::from_storage_key(r.key()))
            .collect();
        Ok(keys)
    }

    async fn clear(&self) -> Result<()> {
        self.entries.clear();
        self.size_bytes.store(0, Ordering::Relaxed);
        self.entry_count.store(0, Ordering::Relaxed);
        Ok(())
    }

    async fn health_check(&self) -> Result<bool> {
        // Memory storage is always healthy if we got here
        Ok(true)
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::entry::CacheData;
    use bytes::Bytes;

    fn test_entry(id: &str, data: &[u8]) -> CacheEntry {
        let key = CacheKey::new("test", id);
        let cache_data = CacheData::uncompressed(Bytes::copy_from_slice(data));
        CacheEntry::new(key, cache_data, CacheTier::L1Memory)
    }

    #[tokio::test]
    async fn test_basic_operations() {
        let storage = MemoryStorage::new();

        // Put
        let entry = test_entry("file1", b"hello world");
        storage.put(entry.clone()).await.unwrap();

        assert_eq!(storage.entry_count(), 1);
        assert_eq!(storage.size_bytes(), 11);

        // Get
        let key = CacheKey::new("test", "file1");
        let retrieved = storage.get(&key).await.unwrap().unwrap();
        assert_eq!(retrieved.key, key);

        // Contains
        assert!(storage.contains(&key).await.unwrap());
        assert!(!storage.contains(&CacheKey::new("test", "nonexistent")).await.unwrap());

        // Delete
        let deleted = storage.delete(&key).await.unwrap().unwrap();
        assert_eq!(deleted.key, key);
        assert_eq!(storage.entry_count(), 0);
        assert_eq!(storage.size_bytes(), 0);
    }

    #[tokio::test]
    async fn test_update_existing() {
        let storage = MemoryStorage::new();

        // Initial entry
        let entry1 = test_entry("file1", b"short");
        storage.put(entry1).await.unwrap();
        assert_eq!(storage.size_bytes(), 5);
        assert_eq!(storage.entry_count(), 1);

        // Update with larger data
        let entry2 = test_entry("file1", b"much longer data here");
        storage.put(entry2).await.unwrap();
        assert_eq!(storage.size_bytes(), 21);
        assert_eq!(storage.entry_count(), 1); // Still 1 entry
    }

    #[tokio::test]
    async fn test_clear() {
        let storage = MemoryStorage::new();

        storage.put(test_entry("file1", b"data1")).await.unwrap();
        storage.put(test_entry("file2", b"data2")).await.unwrap();
        storage.put(test_entry("file3", b"data3")).await.unwrap();

        assert_eq!(storage.entry_count(), 3);

        storage.clear().await.unwrap();

        assert_eq!(storage.entry_count(), 0);
        assert_eq!(storage.size_bytes(), 0);
    }

    #[tokio::test]
    async fn test_keys() {
        let storage = MemoryStorage::new();

        storage.put(test_entry("file1", b"data")).await.unwrap();
        storage.put(test_entry("file2", b"data")).await.unwrap();

        let keys = storage.keys().await.unwrap();
        assert_eq!(keys.len(), 2);
    }

    #[tokio::test]
    async fn test_capacity() {
        let storage = MemoryStorage::with_capacity(100);

        assert_eq!(storage.capacity_bytes(), 100);
        assert_eq!(storage.available_bytes(), 100);

        storage.put(test_entry("file1", b"0123456789")).await.unwrap(); // 10 bytes

        assert_eq!(storage.available_bytes(), 90);
        assert!(storage.has_space_for(90));
        assert!(!storage.has_space_for(91));
    }

    #[tokio::test]
    async fn test_touch() {
        let storage = MemoryStorage::new();

        let entry = test_entry("file1", b"data");
        storage.put(entry).await.unwrap();

        let key = CacheKey::new("test", "file1");

        // Touch existing
        assert!(storage.touch(&key));

        // Touch non-existing
        assert!(!storage.touch(&CacheKey::new("test", "nonexistent")));
    }
}

//! L3 Persistent Storage
//!
//! Backend-agnostic persistent storage layer that delegates to actual storage backends.
//! This tier is typically backed by object storage or distributed filesystems.

use crate::cache::entry::{CacheData, CacheEntry, CacheKey, CompressionAlgorithm};
use crate::cache::storage::TierStorage;
use crate::cache::tier::CacheTier;
use crate::error::{Error, Result};
use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

// =============================================================================
// Persistent Storage Configuration
// =============================================================================

/// Configuration for persistent storage
#[derive(Debug, Clone)]
pub struct PersistentStorageConfig {
    /// Maximum capacity in bytes (for quota enforcement)
    pub capacity_bytes: u64,
    /// Backend type
    pub backend: PersistentBackend,
    /// Prefix for all stored keys
    pub key_prefix: String,
}

/// Backend type for persistent storage
#[derive(Debug, Clone)]
pub enum PersistentBackend {
    /// In-memory mock (for testing)
    InMemory,
    /// Object storage (S3-compatible)
    ObjectStorage {
        endpoint: String,
        bucket: String,
        region: String,
    },
    /// File system path
    FileSystem { root_path: String },
}

impl Default for PersistentStorageConfig {
    fn default() -> Self {
        Self {
            capacity_bytes: 100 * 1024 * 1024 * 1024, // 100 GB
            backend: PersistentBackend::InMemory,
            key_prefix: "cache/l3/".to_string(),
        }
    }
}

// =============================================================================
// Stored Entry Metadata
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredEntry {
    data: Vec<u8>,
    original_size: u64,
    compressed: bool,
    compression_algorithm: Option<String>,
    created_at: i64,
    last_accessed: i64,
    access_count: u64,
    ttl_seconds: Option<u64>,
    content_type: Option<String>,
    etag: Option<String>,
}

// =============================================================================
// Persistent Storage
// =============================================================================

/// L3 persistent storage backend
///
/// This implementation uses an in-memory store for testing purposes.
/// In production, this would delegate to object storage or other persistent backends.
pub struct PersistentStorage {
    /// In-memory store (mock backend)
    store: RwLock<HashMap<String, StoredEntry>>,
    /// Current total size in bytes
    size_bytes: AtomicU64,
    /// Current entry count
    entry_count: AtomicU64,
    /// Maximum capacity
    capacity_bytes: u64,
    /// Key prefix
    key_prefix: String,
    /// Is backend available
    available: AtomicBool,
}

impl PersistentStorage {
    /// Create new persistent storage with default config
    pub fn new() -> Self {
        Self::with_config(PersistentStorageConfig::default())
    }

    /// Create new persistent storage with config
    pub fn with_config(config: PersistentStorageConfig) -> Self {
        Self {
            store: RwLock::new(HashMap::new()),
            size_bytes: AtomicU64::new(0),
            entry_count: AtomicU64::new(0),
            capacity_bytes: config.capacity_bytes,
            key_prefix: config.key_prefix,
            available: AtomicBool::new(true),
        }
    }

    /// Get the full storage key with prefix
    fn prefixed_key(&self, key: &CacheKey) -> String {
        format!("{}{}", self.key_prefix, key.to_storage_key())
    }

    /// Get capacity
    pub fn capacity_bytes(&self) -> u64 {
        self.capacity_bytes
    }

    /// Get available space
    pub fn available_bytes(&self) -> u64 {
        self.capacity_bytes
            .saturating_sub(self.size_bytes.load(Ordering::Relaxed))
    }

    /// Set availability (for testing)
    pub fn set_available(&self, available: bool) {
        self.available.store(available, Ordering::Relaxed);
    }

    /// Check if available
    pub fn is_available(&self) -> bool {
        self.available.load(Ordering::Relaxed)
    }
}

impl Default for PersistentStorage {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl TierStorage for PersistentStorage {
    fn tier(&self) -> CacheTier {
        CacheTier::L3Persistent
    }

    async fn get(&self, key: &CacheKey) -> Result<Option<CacheEntry>> {
        if !self.is_available() {
            return Err(Error::BackendUnavailable {
                backend: "persistent-cache".to_string(),
            });
        }

        let prefixed = self.prefixed_key(key);
        let store = self.store.read();

        let stored = match store.get(&prefixed) {
            Some(s) => s.clone(),
            None => return Ok(None),
        };

        drop(store);

        // Reconstruct entry
        let compression_algo = stored.compression_algorithm.as_deref().and_then(|s| {
            match s {
                "lz4" => Some(CompressionAlgorithm::Lz4),
                "zstd" => Some(CompressionAlgorithm::Zstd),
                "snappy" => Some(CompressionAlgorithm::Snappy),
                _ => None,
            }
        });

        let cache_data = if stored.compressed {
            CacheData::compressed(
                Bytes::from(stored.data),
                stored.original_size,
                compression_algo.unwrap_or(CompressionAlgorithm::None),
            )
        } else {
            CacheData::uncompressed(Bytes::from(stored.data))
        };

        let entry = CacheEntry {
            key: key.clone(),
            data: Arc::new(cache_data),
            tier: CacheTier::L3Persistent,
            created_at: DateTime::from_timestamp(stored.created_at, 0)
                .unwrap_or_else(Utc::now),
            last_accessed: Utc::now(), // Update access time
            access_count: stored.access_count + 1,
            ttl_seconds: stored.ttl_seconds,
            content_type: stored.content_type,
            etag: stored.etag,
        };

        // Update access metadata
        {
            let mut store = self.store.write();
            if let Some(stored) = store.get_mut(&prefixed) {
                stored.last_accessed = Utc::now().timestamp();
                stored.access_count += 1;
            }
        }

        Ok(Some(entry))
    }

    async fn put(&self, entry: CacheEntry) -> Result<()> {
        if !self.is_available() {
            return Err(Error::BackendUnavailable {
                backend: "persistent-cache".to_string(),
            });
        }

        let prefixed = self.prefixed_key(&entry.key);
        let new_size = entry.stored_size();

        let stored = StoredEntry {
            data: entry.data.bytes.to_vec(),
            original_size: entry.data.original_size,
            compressed: entry.data.compressed,
            compression_algorithm: entry
                .data
                .compression_algorithm
                .map(|a| a.to_string()),
            created_at: entry.created_at.timestamp(),
            last_accessed: entry.last_accessed.timestamp(),
            access_count: entry.access_count,
            ttl_seconds: entry.ttl_seconds,
            content_type: entry.content_type,
            etag: entry.etag,
        };

        let old_size = {
            let mut store = self.store.write();
            let old = store.insert(prefixed, stored);
            old.map(|s| s.data.len() as u64).unwrap_or(0)
        };

        // Update stats
        if old_size == 0 {
            self.entry_count.fetch_add(1, Ordering::Relaxed);
            self.size_bytes.fetch_add(new_size, Ordering::Relaxed);
        } else if new_size > old_size {
            self.size_bytes.fetch_add(new_size - old_size, Ordering::Relaxed);
        } else {
            self.size_bytes.fetch_sub(old_size - new_size, Ordering::Relaxed);
        }

        Ok(())
    }

    async fn delete(&self, key: &CacheKey) -> Result<Option<CacheEntry>> {
        if !self.is_available() {
            return Err(Error::BackendUnavailable {
                backend: "persistent-cache".to_string(),
            });
        }

        let prefixed = self.prefixed_key(key);

        let stored = {
            let mut store = self.store.write();
            store.remove(&prefixed)
        };

        let stored = match stored {
            Some(s) => s,
            None => return Ok(None),
        };

        // Update stats
        let size = stored.data.len() as u64;
        self.entry_count.fetch_sub(1, Ordering::Relaxed);
        self.size_bytes.fetch_sub(size, Ordering::Relaxed);

        // Reconstruct entry for return
        let compression_algo = stored.compression_algorithm.as_deref().and_then(|s| {
            match s {
                "lz4" => Some(CompressionAlgorithm::Lz4),
                "zstd" => Some(CompressionAlgorithm::Zstd),
                "snappy" => Some(CompressionAlgorithm::Snappy),
                _ => None,
            }
        });

        let cache_data = if stored.compressed {
            CacheData::compressed(
                Bytes::from(stored.data),
                stored.original_size,
                compression_algo.unwrap_or(CompressionAlgorithm::None),
            )
        } else {
            CacheData::uncompressed(Bytes::from(stored.data))
        };

        let entry = CacheEntry {
            key: key.clone(),
            data: Arc::new(cache_data),
            tier: CacheTier::L3Persistent,
            created_at: DateTime::from_timestamp(stored.created_at, 0)
                .unwrap_or_else(Utc::now),
            last_accessed: DateTime::from_timestamp(stored.last_accessed, 0)
                .unwrap_or_else(Utc::now),
            access_count: stored.access_count,
            ttl_seconds: stored.ttl_seconds,
            content_type: stored.content_type,
            etag: stored.etag,
        };

        Ok(Some(entry))
    }

    async fn contains(&self, key: &CacheKey) -> Result<bool> {
        if !self.is_available() {
            return Err(Error::BackendUnavailable {
                backend: "persistent-cache".to_string(),
            });
        }

        let prefixed = self.prefixed_key(key);
        Ok(self.store.read().contains_key(&prefixed))
    }

    fn size_bytes(&self) -> u64 {
        self.size_bytes.load(Ordering::Relaxed)
    }

    fn entry_count(&self) -> u64 {
        self.entry_count.load(Ordering::Relaxed)
    }

    async fn keys(&self) -> Result<Vec<CacheKey>> {
        if !self.is_available() {
            return Err(Error::BackendUnavailable {
                backend: "persistent-cache".to_string(),
            });
        }

        let store = self.store.read();
        let prefix_len = self.key_prefix.len();
        let keys: Vec<CacheKey> = store
            .keys()
            .filter_map(|k| {
                if k.starts_with(&self.key_prefix) {
                    CacheKey::from_storage_key(&k[prefix_len..])
                } else {
                    None
                }
            })
            .collect();
        Ok(keys)
    }

    async fn clear(&self) -> Result<()> {
        if !self.is_available() {
            return Err(Error::BackendUnavailable {
                backend: "persistent-cache".to_string(),
            });
        }

        {
            let mut store = self.store.write();
            store.clear();
        }

        self.size_bytes.store(0, Ordering::Relaxed);
        self.entry_count.store(0, Ordering::Relaxed);

        Ok(())
    }

    async fn health_check(&self) -> Result<bool> {
        Ok(self.is_available())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_entry(id: &str, data: &[u8]) -> CacheEntry {
        let key = CacheKey::new("test", id);
        let cache_data = CacheData::uncompressed(Bytes::copy_from_slice(data));
        CacheEntry::new(key, cache_data, CacheTier::L3Persistent)
    }

    #[tokio::test]
    async fn test_basic_operations() {
        let storage = PersistentStorage::new();

        // Put
        let entry = test_entry("file1", b"hello world");
        storage.put(entry.clone()).await.unwrap();

        assert_eq!(storage.entry_count(), 1);
        assert_eq!(storage.size_bytes(), 11);

        // Get
        let key = CacheKey::new("test", "file1");
        let retrieved = storage.get(&key).await.unwrap().unwrap();
        assert_eq!(retrieved.key, key);
        assert_eq!(retrieved.data.bytes.as_ref(), b"hello world");

        // Contains
        assert!(storage.contains(&key).await.unwrap());
        assert!(!storage
            .contains(&CacheKey::new("test", "nonexistent"))
            .await
            .unwrap());

        // Delete
        let deleted = storage.delete(&key).await.unwrap();
        assert!(deleted.is_some());
        assert_eq!(storage.entry_count(), 0);
    }

    #[tokio::test]
    async fn test_unavailable() {
        let storage = PersistentStorage::new();
        storage.set_available(false);

        let key = CacheKey::new("test", "file1");
        let result = storage.get(&key).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_access_tracking() {
        let storage = PersistentStorage::new();

        let entry = test_entry("file1", b"data");
        storage.put(entry).await.unwrap();

        let key = CacheKey::new("test", "file1");

        // First access
        let entry1 = storage.get(&key).await.unwrap().unwrap();
        assert_eq!(entry1.access_count, 2); // 1 from creation + 1 from get

        // Second access
        let entry2 = storage.get(&key).await.unwrap().unwrap();
        assert_eq!(entry2.access_count, 3);
    }

    #[tokio::test]
    async fn test_clear() {
        let storage = PersistentStorage::new();

        storage.put(test_entry("file1", b"data1")).await.unwrap();
        storage.put(test_entry("file2", b"data2")).await.unwrap();

        assert_eq!(storage.entry_count(), 2);

        storage.clear().await.unwrap();

        assert_eq!(storage.entry_count(), 0);
        assert_eq!(storage.size_bytes(), 0);
    }
}

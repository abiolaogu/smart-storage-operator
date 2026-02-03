//! L2 Local Disk Storage
//!
//! File-based cache storage for local SSD tier.

use crate::cache::entry::{CacheData, CacheEntry, CacheKey, CompressionAlgorithm};
use crate::cache::storage::TierStorage;
use crate::cache::tier::CacheTier;
use crate::error::Result;
use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::fs;
use tokio::io::AsyncWriteExt;

// =============================================================================
// Local Storage Configuration
// =============================================================================

/// Configuration for local storage
#[derive(Debug, Clone)]
pub struct LocalStorageConfig {
    /// Root directory for cache files
    pub root_path: PathBuf,
    /// Maximum capacity in bytes
    pub capacity_bytes: u64,
    /// Whether to sync writes to disk
    pub sync_writes: bool,
}

impl Default for LocalStorageConfig {
    fn default() -> Self {
        Self {
            root_path: PathBuf::from("/var/cache/smart-storage"),
            capacity_bytes: 10 * 1024 * 1024 * 1024, // 10 GB
            sync_writes: false,
        }
    }
}

// =============================================================================
// Entry Metadata (stored alongside data)
// =============================================================================

/// Metadata stored with each cache file
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredMetadata {
    key: SerializableCacheKey,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SerializableCacheKey {
    namespace: String,
    id: String,
    version: Option<u64>,
}

impl From<&CacheKey> for SerializableCacheKey {
    fn from(key: &CacheKey) -> Self {
        Self {
            namespace: key.namespace.clone(),
            id: key.id.clone(),
            version: key.version,
        }
    }
}

impl From<SerializableCacheKey> for CacheKey {
    fn from(key: SerializableCacheKey) -> Self {
        CacheKey {
            namespace: key.namespace,
            id: key.id,
            version: key.version,
        }
    }
}

// =============================================================================
// Local Storage
// =============================================================================

/// L2 local disk cache storage
pub struct LocalStorage {
    /// Root directory for cache files
    root_path: PathBuf,
    /// Index of cached files (key -> file path)
    index: RwLock<HashMap<String, PathBuf>>,
    /// Current total size in bytes
    size_bytes: AtomicU64,
    /// Current entry count
    entry_count: AtomicU64,
    /// Maximum capacity
    capacity_bytes: u64,
    /// Whether to sync writes
    sync_writes: bool,
}

impl LocalStorage {
    /// Create new local storage with default config
    pub async fn new() -> Result<Self> {
        Self::with_config(LocalStorageConfig::default()).await
    }

    /// Create new local storage with specified root path
    pub async fn with_path(root_path: impl Into<PathBuf>) -> Result<Self> {
        Self::with_config(LocalStorageConfig {
            root_path: root_path.into(),
            ..Default::default()
        })
        .await
    }

    /// Create new local storage with full config
    pub async fn with_config(config: LocalStorageConfig) -> Result<Self> {
        // Create root directory if it doesn't exist
        fs::create_dir_all(&config.root_path).await?;

        let storage = Self {
            root_path: config.root_path,
            index: RwLock::new(HashMap::new()),
            size_bytes: AtomicU64::new(0),
            entry_count: AtomicU64::new(0),
            capacity_bytes: config.capacity_bytes,
            sync_writes: config.sync_writes,
        };

        // Scan existing files to rebuild index
        storage.rebuild_index().await?;

        Ok(storage)
    }

    /// Rebuild index from disk
    async fn rebuild_index(&self) -> Result<()> {
        let mut index = self.index.write();
        index.clear();

        let mut total_size = 0u64;
        let mut entry_count = 0u64;

        // Scan root directory for shard subdirectories
        let mut root_entries = fs::read_dir(&self.root_path).await?;
        while let Some(root_entry) = root_entries.next_entry().await? {
            let shard_path = root_entry.path();
            if !shard_path.is_dir() {
                continue;
            }

            // Scan shard directory for meta files
            let mut shard_entries = match fs::read_dir(&shard_path).await {
                Ok(e) => e,
                Err(_) => continue,
            };

            while let Some(entry) = shard_entries.next_entry().await? {
                let path = entry.path();
                if path.extension().map(|e| e == "meta").unwrap_or(false) {
                    // Read metadata to get key
                    if let Ok(metadata_json) = fs::read_to_string(&path).await {
                        if let Ok(metadata) = serde_json::from_str::<StoredMetadata>(&metadata_json) {
                            let key: CacheKey = metadata.key.into();
                            let storage_key = key.to_storage_key();

                            // Get data file path (remove .meta extension)
                            let data_path = path.with_extension("");
                            if data_path.exists() {
                                if let Ok(file_meta) = fs::metadata(&data_path).await {
                                    total_size += file_meta.len();
                                    entry_count += 1;
                                    index.insert(storage_key, data_path);
                                }
                            }
                        }
                    }
                }
            }
        }

        drop(index);
        self.size_bytes.store(total_size, Ordering::Relaxed);
        self.entry_count.store(entry_count, Ordering::Relaxed);

        Ok(())
    }

    /// Get file path for a key
    fn get_file_path(&self, key: &CacheKey) -> PathBuf {
        // Use hash-based subdirectories to avoid too many files in one dir
        let shard = key.shard_index();
        let shard_dir = self.root_path.join(format!("{:02x}", shard));
        let filename = self.safe_filename(&key.to_storage_key());
        shard_dir.join(filename)
    }

    /// Convert key to safe filename
    fn safe_filename(&self, key: &str) -> String {
        // Hash the key to create a safe filename
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        format!("{:016x}", hasher.finish())
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
}

#[async_trait]
impl TierStorage for LocalStorage {
    fn tier(&self) -> CacheTier {
        CacheTier::L2Local
    }

    async fn get(&self, key: &CacheKey) -> Result<Option<CacheEntry>> {
        let storage_key = key.to_storage_key();

        // Check index
        let data_path = {
            let index = self.index.read();
            match index.get(&storage_key) {
                Some(path) => path.clone(),
                None => return Ok(None),
            }
        };

        let meta_path = data_path.with_extension("meta");

        // Read metadata
        let metadata_json = match fs::read_to_string(&meta_path).await {
            Ok(json) => json,
            Err(_) => return Ok(None),
        };

        let metadata: StoredMetadata = match serde_json::from_str(&metadata_json) {
            Ok(m) => m,
            Err(_) => return Ok(None),
        };

        // Read data
        let data_bytes = match fs::read(&data_path).await {
            Ok(bytes) => bytes,
            Err(_) => return Ok(None),
        };

        // Reconstruct entry
        let cache_key: CacheKey = metadata.key.clone().into();
        let compression_algo = metadata.compression_algorithm.as_deref().and_then(|s| {
            match s {
                "lz4" => Some(CompressionAlgorithm::Lz4),
                "zstd" => Some(CompressionAlgorithm::Zstd),
                "snappy" => Some(CompressionAlgorithm::Snappy),
                _ => None,
            }
        });

        let cache_data = if metadata.compressed {
            CacheData::compressed(
                Bytes::from(data_bytes),
                metadata.original_size,
                compression_algo.unwrap_or(CompressionAlgorithm::None),
            )
        } else {
            CacheData::uncompressed(Bytes::from(data_bytes))
        };

        let entry = CacheEntry {
            key: cache_key,
            data: Arc::new(cache_data),
            tier: CacheTier::L2Local,
            created_at: DateTime::from_timestamp(metadata.created_at, 0)
                .unwrap_or_else(Utc::now),
            last_accessed: DateTime::from_timestamp(metadata.last_accessed, 0)
                .unwrap_or_else(Utc::now),
            access_count: metadata.access_count,
            ttl_seconds: metadata.ttl_seconds,
            content_type: metadata.content_type.clone(),
            etag: metadata.etag.clone(),
        };

        // Update access time (async, fire and forget)
        let meta_path_clone = meta_path.clone();
        let mut updated_metadata = metadata;
        updated_metadata.last_accessed = Utc::now().timestamp();
        updated_metadata.access_count += 1;
        tokio::spawn(async move {
            let _ = fs::write(
                &meta_path_clone,
                serde_json::to_string(&updated_metadata).unwrap_or_default(),
            )
            .await;
        });

        Ok(Some(entry))
    }

    async fn put(&self, entry: CacheEntry) -> Result<()> {
        let storage_key = entry.key.to_storage_key();
        let data_path = self.get_file_path(&entry.key);
        let meta_path = data_path.with_extension("meta");

        // Create shard directory if needed
        if let Some(parent) = data_path.parent() {
            fs::create_dir_all(parent).await?;
        }

        // Check for existing entry
        let existing_path = {
            let index = self.index.read();
            index.get(&storage_key).cloned()
        };
        let old_size = if let Some(path) = existing_path {
            fs::metadata(&path).await.map(|m| m.len()).unwrap_or(0)
        } else {
            0
        };

        // Write data file
        let data_bytes = entry.data.bytes.as_ref();
        let mut file = fs::File::create(&data_path).await?;
        file.write_all(data_bytes).await?;
        if self.sync_writes {
            file.sync_all().await?;
        }

        // Write metadata
        let metadata = StoredMetadata {
            key: (&entry.key).into(),
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

        let metadata_json = serde_json::to_string(&metadata)?;
        fs::write(&meta_path, metadata_json).await?;

        // Update index and stats
        let new_size = data_bytes.len() as u64;
        {
            let mut index = self.index.write();
            if index.insert(storage_key, data_path).is_none() {
                // New entry
                self.entry_count.fetch_add(1, Ordering::Relaxed);
            }
        }

        // Update size
        if old_size == 0 {
            self.size_bytes.fetch_add(new_size, Ordering::Relaxed);
        } else if new_size > old_size {
            self.size_bytes.fetch_add(new_size - old_size, Ordering::Relaxed);
        } else {
            self.size_bytes.fetch_sub(old_size - new_size, Ordering::Relaxed);
        }

        Ok(())
    }

    async fn delete(&self, key: &CacheKey) -> Result<Option<CacheEntry>> {
        let storage_key = key.to_storage_key();

        // Get and remove from index
        let data_path = {
            let mut index = self.index.write();
            index.remove(&storage_key)
        };

        let data_path = match data_path {
            Some(p) => p,
            None => return Ok(None),
        };

        let meta_path = data_path.with_extension("meta");

        // Read entry before deleting (for return value)
        let metadata_json = fs::read_to_string(&meta_path).await.ok();
        let entry = if let Some(json) = metadata_json {
            if let Ok(metadata) = serde_json::from_str::<StoredMetadata>(&json) {
                if let Ok(data_bytes) = fs::read(&data_path).await {
                    let cache_key: CacheKey = metadata.key.clone().into();
                    let compression_algo = metadata.compression_algorithm.as_deref().and_then(|s| {
                        match s {
                            "lz4" => Some(CompressionAlgorithm::Lz4),
                            "zstd" => Some(CompressionAlgorithm::Zstd),
                            "snappy" => Some(CompressionAlgorithm::Snappy),
                            _ => None,
                        }
                    });

                    let cache_data = if metadata.compressed {
                        CacheData::compressed(
                            Bytes::from(data_bytes),
                            metadata.original_size,
                            compression_algo.unwrap_or(CompressionAlgorithm::None),
                        )
                    } else {
                        CacheData::uncompressed(Bytes::from(data_bytes))
                    };

                    Some(CacheEntry {
                        key: cache_key,
                        data: Arc::new(cache_data),
                        tier: CacheTier::L2Local,
                        created_at: DateTime::from_timestamp(metadata.created_at, 0)
                            .unwrap_or_else(Utc::now),
                        last_accessed: DateTime::from_timestamp(metadata.last_accessed, 0)
                            .unwrap_or_else(Utc::now),
                        access_count: metadata.access_count,
                        ttl_seconds: metadata.ttl_seconds,
                        content_type: metadata.content_type,
                        etag: metadata.etag,
                    })
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };

        // Get file size and delete files
        let file_size = fs::metadata(&data_path)
            .await
            .map(|m| m.len())
            .unwrap_or(0);

        let _ = fs::remove_file(&data_path).await;
        let _ = fs::remove_file(&meta_path).await;

        // Update stats
        self.entry_count.fetch_sub(1, Ordering::Relaxed);
        self.size_bytes.fetch_sub(file_size, Ordering::Relaxed);

        Ok(entry)
    }

    async fn contains(&self, key: &CacheKey) -> Result<bool> {
        let storage_key = key.to_storage_key();
        Ok(self.index.read().contains_key(&storage_key))
    }

    fn size_bytes(&self) -> u64 {
        self.size_bytes.load(Ordering::Relaxed)
    }

    fn entry_count(&self) -> u64 {
        self.entry_count.load(Ordering::Relaxed)
    }

    async fn keys(&self) -> Result<Vec<CacheKey>> {
        let index = self.index.read();
        let keys: Vec<CacheKey> = index
            .keys()
            .filter_map(|k| CacheKey::from_storage_key(k))
            .collect();
        Ok(keys)
    }

    async fn clear(&self) -> Result<()> {
        // Clear index
        let paths: Vec<PathBuf> = {
            let mut index = self.index.write();
            let paths: Vec<_> = index.values().cloned().collect();
            index.clear();
            paths
        };

        // Delete all files
        for data_path in paths {
            let meta_path = data_path.with_extension("meta");
            let _ = fs::remove_file(&data_path).await;
            let _ = fs::remove_file(&meta_path).await;
        }

        self.size_bytes.store(0, Ordering::Relaxed);
        self.entry_count.store(0, Ordering::Relaxed);

        Ok(())
    }

    async fn health_check(&self) -> Result<bool> {
        // Check that root directory exists and is writable
        let test_path = self.root_path.join(".health_check");
        match fs::write(&test_path, b"ok").await {
            Ok(_) => {
                let _ = fs::remove_file(&test_path).await;
                Ok(true)
            }
            Err(_) => Ok(false),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    async fn test_storage() -> (LocalStorage, TempDir) {
        let tmp = TempDir::new().unwrap();
        let storage = LocalStorage::with_path(tmp.path()).await.unwrap();
        (storage, tmp)
    }

    fn test_entry(id: &str, data: &[u8]) -> CacheEntry {
        let key = CacheKey::new("test", id);
        let cache_data = CacheData::uncompressed(Bytes::copy_from_slice(data));
        CacheEntry::new(key, cache_data, CacheTier::L2Local)
    }

    #[tokio::test]
    async fn test_basic_operations() {
        let (storage, _tmp) = test_storage().await;

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
    async fn test_persistence() {
        let tmp = TempDir::new().unwrap();

        // Create storage and add entry
        {
            let storage = LocalStorage::with_path(tmp.path()).await.unwrap();
            let entry = test_entry("persistent", b"data that persists");
            storage.put(entry).await.unwrap();
            assert_eq!(storage.entry_count(), 1);
        }

        // Create new storage instance and verify entry exists
        {
            let storage = LocalStorage::with_path(tmp.path()).await.unwrap();
            assert_eq!(storage.entry_count(), 1);

            let key = CacheKey::new("test", "persistent");
            let entry = storage.get(&key).await.unwrap().unwrap();
            assert_eq!(entry.data.bytes.as_ref(), b"data that persists");
        }
    }

    #[tokio::test]
    async fn test_clear() {
        let (storage, _tmp) = test_storage().await;

        storage.put(test_entry("file1", b"data1")).await.unwrap();
        storage.put(test_entry("file2", b"data2")).await.unwrap();

        assert_eq!(storage.entry_count(), 2);

        storage.clear().await.unwrap();

        assert_eq!(storage.entry_count(), 0);
        assert_eq!(storage.size_bytes(), 0);
    }
}

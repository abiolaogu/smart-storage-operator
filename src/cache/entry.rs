//! Cache Entry Types
//!
//! Defines cache keys, entries, and data structures.

use crate::cache::tier::CacheTier;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

// =============================================================================
// Cache Key
// =============================================================================

/// Unique identifier for cached data
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheKey {
    /// Namespace (e.g., "objects", "volumes", "metadata")
    pub namespace: String,
    /// Object identifier within namespace
    pub id: String,
    /// Optional version for versioned objects
    pub version: Option<u64>,
}

impl CacheKey {
    /// Create a new cache key
    pub fn new(namespace: impl Into<String>, id: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            id: id.into(),
            version: None,
        }
    }

    /// Create a versioned cache key
    pub fn versioned(namespace: impl Into<String>, id: impl Into<String>, version: u64) -> Self {
        Self {
            namespace: namespace.into(),
            id: id.into(),
            version: Some(version),
        }
    }

    /// Get a string representation for storage
    pub fn to_storage_key(&self) -> String {
        match self.version {
            Some(v) => format!("{}:{}:v{}", self.namespace, self.id, v),
            None => format!("{}:{}", self.namespace, self.id),
        }
    }

    /// Parse from storage key string
    pub fn from_storage_key(key: &str) -> Option<Self> {
        let parts: Vec<&str> = key.splitn(3, ':').collect();
        match parts.as_slice() {
            [namespace, id] => Some(Self {
                namespace: (*namespace).to_string(),
                id: (*id).to_string(),
                version: None,
            }),
            [namespace, id, version_str] => {
                let version = version_str.strip_prefix('v')?.parse().ok()?;
                Some(Self {
                    namespace: (*namespace).to_string(),
                    id: (*id).to_string(),
                    version: Some(version),
                })
            }
            _ => None,
        }
    }

    /// Get the shard index for this key (64-way sharding)
    #[inline]
    pub fn shard_index(&self) -> usize {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        self.hash(&mut hasher);
        (hasher.finish() as usize) % 64
    }
}

impl Hash for CacheKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.namespace.hash(state);
        self.id.hash(state);
        self.version.hash(state);
    }
}

impl fmt::Display for CacheKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_storage_key())
    }
}

// =============================================================================
// Cache Data
// =============================================================================

/// Wrapper for cached data with compression info
#[derive(Debug, Clone)]
pub struct CacheData {
    /// The actual data bytes
    pub bytes: Bytes,
    /// Original size before compression (same as bytes.len() if uncompressed)
    pub original_size: u64,
    /// Whether the data is compressed
    pub compressed: bool,
    /// Compression algorithm used (if compressed)
    pub compression_algorithm: Option<CompressionAlgorithm>,
}

impl CacheData {
    /// Create uncompressed cache data
    pub fn uncompressed(bytes: Bytes) -> Self {
        let size = bytes.len() as u64;
        Self {
            bytes,
            original_size: size,
            compressed: false,
            compression_algorithm: None,
        }
    }

    /// Create compressed cache data
    pub fn compressed(bytes: Bytes, original_size: u64, algorithm: CompressionAlgorithm) -> Self {
        Self {
            bytes,
            original_size,
            compressed: true,
            compression_algorithm: Some(algorithm),
        }
    }

    /// Get the stored size (compressed size if compressed)
    pub fn stored_size(&self) -> u64 {
        self.bytes.len() as u64
    }

    /// Get compression ratio (stored_size / original_size)
    pub fn compression_ratio(&self) -> f64 {
        if self.original_size == 0 {
            1.0
        } else {
            self.stored_size() as f64 / self.original_size as f64
        }
    }
}

/// Compression algorithm identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CompressionAlgorithm {
    #[default]
    None,
    Lz4,
    Zstd,
    Snappy,
}

impl fmt::Display for CompressionAlgorithm {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CompressionAlgorithm::None => write!(f, "none"),
            CompressionAlgorithm::Lz4 => write!(f, "lz4"),
            CompressionAlgorithm::Zstd => write!(f, "zstd"),
            CompressionAlgorithm::Snappy => write!(f, "snappy"),
        }
    }
}

// =============================================================================
// Cache Entry
// =============================================================================

/// A cached entry with metadata
#[derive(Debug, Clone)]
pub struct CacheEntry {
    /// Unique key for this entry
    pub key: CacheKey,
    /// The cached data
    pub data: Arc<CacheData>,
    /// Tier where this entry is stored
    pub tier: CacheTier,
    /// Time when entry was created/cached
    pub created_at: DateTime<Utc>,
    /// Time when entry was last accessed
    pub last_accessed: DateTime<Utc>,
    /// Number of times this entry has been accessed
    pub access_count: u64,
    /// Time-to-live in seconds (None = never expires)
    pub ttl_seconds: Option<u64>,
    /// Content type hint (e.g., "application/octet-stream")
    pub content_type: Option<String>,
    /// ETag for cache validation
    pub etag: Option<String>,
}

impl CacheEntry {
    /// Create a new cache entry
    pub fn new(key: CacheKey, data: CacheData, tier: CacheTier) -> Self {
        let now = Utc::now();
        Self {
            key,
            data: Arc::new(data),
            tier,
            created_at: now,
            last_accessed: now,
            access_count: 1,
            ttl_seconds: None,
            content_type: None,
            etag: None,
        }
    }

    /// Create a new cache entry with TTL
    pub fn with_ttl(key: CacheKey, data: CacheData, tier: CacheTier, ttl_seconds: u64) -> Self {
        let mut entry = Self::new(key, data, tier);
        entry.ttl_seconds = Some(ttl_seconds);
        entry
    }

    /// Record an access to this entry
    pub fn record_access(&mut self) {
        self.last_accessed = Utc::now();
        self.access_count += 1;
    }

    /// Check if the entry has expired
    pub fn is_expired(&self) -> bool {
        if let Some(ttl) = self.ttl_seconds {
            let age = Utc::now().signed_duration_since(self.created_at);
            age.num_seconds() as u64 > ttl
        } else {
            false
        }
    }

    /// Get age in seconds since creation
    pub fn age_seconds(&self) -> u64 {
        let age = Utc::now().signed_duration_since(self.created_at);
        age.num_seconds().max(0) as u64
    }

    /// Get time since last access in seconds
    pub fn idle_seconds(&self) -> u64 {
        let idle = Utc::now().signed_duration_since(self.last_accessed);
        idle.num_seconds().max(0) as u64
    }

    /// Get the stored size in bytes
    pub fn stored_size(&self) -> u64 {
        self.data.stored_size()
    }

    /// Get the original size in bytes
    pub fn original_size(&self) -> u64 {
        self.data.original_size
    }

    /// Get the raw bytes (may be compressed)
    pub fn bytes(&self) -> &Bytes {
        &self.data.bytes
    }
}

// =============================================================================
// Cache Entry Metadata (lightweight version for LRU tracking)
// =============================================================================

/// Lightweight metadata for LRU tracking
#[derive(Debug, Clone)]
pub struct EntryMetadata {
    /// Cache key
    pub key: CacheKey,
    /// Stored size in bytes
    pub size_bytes: u64,
    /// Current tier
    pub tier: CacheTier,
    /// Last access timestamp (Unix millis)
    pub last_accessed_ms: u64,
    /// Access count
    pub access_count: u64,
}

impl EntryMetadata {
    /// Create from a cache entry
    pub fn from_entry(entry: &CacheEntry) -> Self {
        Self {
            key: entry.key.clone(),
            size_bytes: entry.stored_size(),
            tier: entry.tier,
            last_accessed_ms: entry.last_accessed.timestamp_millis() as u64,
            access_count: entry.access_count,
        }
    }

    /// Update access metadata
    pub fn record_access(&mut self) {
        self.last_accessed_ms = Utc::now().timestamp_millis() as u64;
        self.access_count += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_key_creation() {
        let key = CacheKey::new("objects", "bucket/my-file.txt");
        assert_eq!(key.namespace, "objects");
        assert_eq!(key.id, "bucket/my-file.txt");
        assert_eq!(key.version, None);
        assert_eq!(key.to_storage_key(), "objects:bucket/my-file.txt");
    }

    #[test]
    fn test_cache_key_versioned() {
        let key = CacheKey::versioned("objects", "file.txt", 42);
        assert_eq!(key.version, Some(42));
        assert_eq!(key.to_storage_key(), "objects:file.txt:v42");
    }

    #[test]
    fn test_cache_key_parsing() {
        let key1 = CacheKey::from_storage_key("objects:file.txt").unwrap();
        assert_eq!(key1.namespace, "objects");
        assert_eq!(key1.id, "file.txt");
        assert_eq!(key1.version, None);

        let key2 = CacheKey::from_storage_key("objects:file.txt:v42").unwrap();
        assert_eq!(key2.version, Some(42));
    }

    #[test]
    fn test_cache_key_sharding() {
        let key1 = CacheKey::new("objects", "file1.txt");
        let key2 = CacheKey::new("objects", "file1.txt");
        let key3 = CacheKey::new("objects", "file2.txt");

        // Same key should hash to same shard
        assert_eq!(key1.shard_index(), key2.shard_index());

        // Different keys should be within bounds
        assert!(key1.shard_index() < 64);
        assert!(key3.shard_index() < 64);
    }

    #[test]
    fn test_cache_data_compression() {
        let data = CacheData::uncompressed(Bytes::from("hello world"));
        assert!(!data.compressed);
        assert_eq!(data.original_size, 11);
        assert_eq!(data.stored_size(), 11);
        assert!((data.compression_ratio() - 1.0).abs() < 0.001);

        let compressed = CacheData::compressed(Bytes::from("abc"), 100, CompressionAlgorithm::Lz4);
        assert!(compressed.compressed);
        assert_eq!(compressed.original_size, 100);
        assert_eq!(compressed.stored_size(), 3);
        assert!((compressed.compression_ratio() - 0.03).abs() < 0.001);
    }

    #[test]
    fn test_cache_entry_expiry() {
        let key = CacheKey::new("test", "item");
        let data = CacheData::uncompressed(Bytes::from("data"));

        // Entry without TTL never expires
        let entry = CacheEntry::new(key.clone(), data.clone(), CacheTier::L1Memory);
        assert!(!entry.is_expired());

        // Entry with 1 second TTL - should not be expired immediately
        let entry_with_ttl = CacheEntry::with_ttl(key, data, CacheTier::L1Memory, 3600);
        assert!(!entry_with_ttl.is_expired());
    }
}

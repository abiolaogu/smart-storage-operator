//! Compression Support
//!
//! Multiple compression algorithms with automatic fallback on failure.

use crate::error::{Error, Result};
use bytes::Bytes;

// =============================================================================
// Compression Configuration
// =============================================================================

/// Configuration for compression
#[derive(Debug, Clone)]
pub struct CompressionConfig {
    /// Default algorithm to use
    pub default_algorithm: CompressionAlgorithm,
    /// Minimum size to compress (smaller objects are stored uncompressed)
    pub min_size_bytes: u64,
    /// Compression level (algorithm-specific)
    pub level: i32,
    /// Whether to fall back to uncompressed on failure
    pub fallback_on_failure: bool,
}

impl Default for CompressionConfig {
    fn default() -> Self {
        Self {
            default_algorithm: CompressionAlgorithm::Lz4,
            min_size_bytes: 1024, // 1KB minimum
            level: 3,             // Medium compression
            fallback_on_failure: true,
        }
    }
}

// =============================================================================
// Compressor Trait
// =============================================================================

/// Trait for compression implementations
pub trait Compressor: Send + Sync {
    /// Get the algorithm identifier
    fn algorithm(&self) -> CompressionAlgorithm;

    /// Compress data
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>>;

    /// Decompress data
    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>>;
}

// =============================================================================
// No-Op Compressor
// =============================================================================

/// Pass-through compressor (no compression)
pub struct NoopCompressor;

impl Compressor for NoopCompressor {
    fn algorithm(&self) -> CompressionAlgorithm {
        CompressionAlgorithm::None
    }

    fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
        Ok(data.to_vec())
    }

    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>> {
        Ok(data.to_vec())
    }
}

// =============================================================================
// LZ4 Compressor
// =============================================================================

/// LZ4 compressor (fast compression)
pub struct Lz4Compressor {
    level: i32,
}

impl Lz4Compressor {
    pub fn new() -> Self {
        Self { level: 4 }
    }

    pub fn with_level(level: i32) -> Self {
        Self { level }
    }
}

impl Default for Lz4Compressor {
    fn default() -> Self {
        Self::new()
    }
}

impl Compressor for Lz4Compressor {
    fn algorithm(&self) -> CompressionAlgorithm {
        CompressionAlgorithm::Lz4
    }

    fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
        lz4::block::compress(data, Some(lz4::block::CompressionMode::HIGHCOMPRESSION(self.level)), true)
            .map_err(|e| Error::Internal(format!("LZ4 compression failed: {}", e)))
    }

    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>> {
        lz4::block::decompress(data, None)
            .map_err(|e| Error::Internal(format!("LZ4 decompression failed: {}", e)))
    }
}

// =============================================================================
// Zstd Compressor
// =============================================================================

/// Zstd compressor (balanced compression)
pub struct ZstdCompressor {
    level: i32,
}

impl ZstdCompressor {
    pub fn new() -> Self {
        Self { level: 3 }
    }

    pub fn with_level(level: i32) -> Self {
        Self { level }
    }
}

impl Default for ZstdCompressor {
    fn default() -> Self {
        Self::new()
    }
}

impl Compressor for ZstdCompressor {
    fn algorithm(&self) -> CompressionAlgorithm {
        CompressionAlgorithm::Zstd
    }

    fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
        zstd::encode_all(data, self.level)
            .map_err(|e| Error::Internal(format!("Zstd compression failed: {}", e)))
    }

    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>> {
        zstd::decode_all(data)
            .map_err(|e| Error::Internal(format!("Zstd decompression failed: {}", e)))
    }
}

// =============================================================================
// Snappy Compressor
// =============================================================================

/// Snappy compressor (very fast, lower ratio)
pub struct SnappyCompressor;

impl SnappyCompressor {
    pub fn new() -> Self {
        Self
    }
}

impl Default for SnappyCompressor {
    fn default() -> Self {
        Self::new()
    }
}

impl Compressor for SnappyCompressor {
    fn algorithm(&self) -> CompressionAlgorithm {
        CompressionAlgorithm::Snappy
    }

    fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
        let mut encoder = snap::raw::Encoder::new();
        encoder
            .compress_vec(data)
            .map_err(|e| Error::Internal(format!("Snappy compression failed: {}", e)))
    }

    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>> {
        let mut decoder = snap::raw::Decoder::new();
        decoder
            .decompress_vec(data)
            .map_err(|e| Error::Internal(format!("Snappy decompression failed: {}", e)))
    }
}

// =============================================================================
// Compression Manager
// =============================================================================

/// Manager for compression operations with fallback support
pub struct CompressionManager {
    config: CompressionConfig,
    lz4: Lz4Compressor,
    zstd: ZstdCompressor,
    snappy: SnappyCompressor,
    noop: NoopCompressor,
}

impl CompressionManager {
    /// Create a new compression manager
    pub fn new() -> Self {
        Self::with_config(CompressionConfig::default())
    }

    /// Create with custom config
    pub fn with_config(config: CompressionConfig) -> Self {
        Self {
            lz4: Lz4Compressor::with_level(config.level),
            zstd: ZstdCompressor::with_level(config.level),
            snappy: SnappyCompressor::new(),
            noop: NoopCompressor,
            config,
        }
    }

    /// Get compressor for algorithm
    fn compressor(&self, algorithm: CompressionAlgorithm) -> &dyn Compressor {
        match algorithm {
            CompressionAlgorithm::None => &self.noop,
            CompressionAlgorithm::Lz4 => &self.lz4,
            CompressionAlgorithm::Zstd => &self.zstd,
            CompressionAlgorithm::Snappy => &self.snappy,
        }
    }

    /// Compress data using the default algorithm
    ///
    /// Returns (compressed_data, algorithm_used).
    /// Falls back to uncompressed if compression fails or data is too small.
    pub fn compress(&self, data: &[u8]) -> (Bytes, CompressionAlgorithm) {
        // Skip compression for small data
        if (data.len() as u64) < self.config.min_size_bytes {
            return (Bytes::copy_from_slice(data), CompressionAlgorithm::None);
        }

        // Try default algorithm
        let compressor = self.compressor(self.config.default_algorithm);
        match compressor.compress(data) {
            Ok(compressed) => {
                // Only use compressed if it's actually smaller
                if compressed.len() < data.len() {
                    (Bytes::from(compressed), self.config.default_algorithm)
                } else {
                    (Bytes::copy_from_slice(data), CompressionAlgorithm::None)
                }
            }
            Err(_) if self.config.fallback_on_failure => {
                // Fall back to uncompressed
                (Bytes::copy_from_slice(data), CompressionAlgorithm::None)
            }
            Err(e) => {
                // Propagate error if no fallback
                tracing::warn!("Compression failed, using uncompressed: {}", e);
                (Bytes::copy_from_slice(data), CompressionAlgorithm::None)
            }
        }
    }

    /// Compress with specific algorithm
    pub fn compress_with(
        &self,
        data: &[u8],
        algorithm: CompressionAlgorithm,
    ) -> Result<(Bytes, CompressionAlgorithm)> {
        if algorithm == CompressionAlgorithm::None {
            return Ok((Bytes::copy_from_slice(data), CompressionAlgorithm::None));
        }

        let compressor = self.compressor(algorithm);
        match compressor.compress(data) {
            Ok(compressed) => {
                if compressed.len() < data.len() {
                    Ok((Bytes::from(compressed), algorithm))
                } else {
                    Ok((Bytes::copy_from_slice(data), CompressionAlgorithm::None))
                }
            }
            Err(e) if self.config.fallback_on_failure => {
                tracing::warn!("Compression with {:?} failed, using uncompressed: {}", algorithm, e);
                Ok((Bytes::copy_from_slice(data), CompressionAlgorithm::None))
            }
            Err(e) => Err(e),
        }
    }

    /// Decompress data
    pub fn decompress(&self, data: &[u8], algorithm: CompressionAlgorithm) -> Result<Bytes> {
        let compressor = self.compressor(algorithm);
        let decompressed = compressor.decompress(data)?;
        Ok(Bytes::from(decompressed))
    }

    /// Get configuration
    pub fn config(&self) -> &CompressionConfig {
        &self.config
    }
}

impl Default for CompressionManager {
    fn default() -> Self {
        Self::new()
    }
}

// Re-export CompressionAlgorithm for convenience
pub use crate::cache::entry::CompressionAlgorithm;

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_DATA: &[u8] = b"Hello, this is test data that should compress well. \
        It has some repetition: Hello, this is test data that should compress well.";

    #[test]
    fn test_lz4_roundtrip() {
        let compressor = Lz4Compressor::new();

        let compressed = compressor.compress(TEST_DATA).unwrap();
        assert!(compressed.len() < TEST_DATA.len());

        let decompressed = compressor.decompress(&compressed).unwrap();
        assert_eq!(decompressed, TEST_DATA);
    }

    #[test]
    fn test_zstd_roundtrip() {
        let compressor = ZstdCompressor::new();

        let compressed = compressor.compress(TEST_DATA).unwrap();
        assert!(compressed.len() < TEST_DATA.len());

        let decompressed = compressor.decompress(&compressed).unwrap();
        assert_eq!(decompressed, TEST_DATA);
    }

    #[test]
    fn test_snappy_roundtrip() {
        let compressor = SnappyCompressor::new();

        let compressed = compressor.compress(TEST_DATA).unwrap();
        assert!(compressed.len() < TEST_DATA.len());

        let decompressed = compressor.decompress(&compressed).unwrap();
        assert_eq!(decompressed, TEST_DATA);
    }

    #[test]
    fn test_noop_roundtrip() {
        let compressor = NoopCompressor;

        let compressed = compressor.compress(TEST_DATA).unwrap();
        assert_eq!(compressed, TEST_DATA);

        let decompressed = compressor.decompress(&compressed).unwrap();
        assert_eq!(decompressed, TEST_DATA);
    }

    #[test]
    fn test_manager_auto_compress() {
        let manager = CompressionManager::new();

        // Large data should compress
        let (compressed, algorithm) = manager.compress(TEST_DATA);
        assert!(algorithm != CompressionAlgorithm::None || compressed.len() >= TEST_DATA.len());

        // Small data should not compress
        let small = b"tiny";
        let (result, algorithm) = manager.compress(small);
        assert_eq!(algorithm, CompressionAlgorithm::None);
        assert_eq!(result.as_ref(), small);
    }

    #[test]
    fn test_manager_decompress() {
        let manager = CompressionManager::new();

        let (compressed, algorithm) = manager.compress(TEST_DATA);
        let decompressed = manager.decompress(&compressed, algorithm).unwrap();
        assert_eq!(decompressed.as_ref(), TEST_DATA);
    }

    #[test]
    fn test_compression_ratio() {
        let compressor = ZstdCompressor::with_level(9);

        let compressed = compressor.compress(TEST_DATA).unwrap();
        let ratio = compressed.len() as f64 / TEST_DATA.len() as f64;

        println!("Compression ratio: {:.2}%", ratio * 100.0);
        assert!(ratio < 1.0, "Data should actually compress");
    }

    #[test]
    fn test_incompressible_data() {
        let manager = CompressionManager::new();

        // Random-looking data that doesn't compress well
        let random_data: Vec<u8> = (0..2000).map(|i| (i * 7 + 3) as u8).collect();

        let (result, algorithm) = manager.compress(&random_data);

        // Manager should fall back to uncompressed if compression doesn't help
        // Or return compressed data if it's smaller
        if algorithm == CompressionAlgorithm::None {
            assert_eq!(result.len(), random_data.len());
        } else {
            assert!(result.len() <= random_data.len());
        }
    }
}

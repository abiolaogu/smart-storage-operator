//! Async Prefetcher
//!
//! Handles asynchronous prefetching of cache entries based on access patterns.

use crate::cache::entry::CacheKey;
use crate::cache::tier::CacheTier;
use crate::error::Result;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tokio::sync::{mpsc, Mutex};

// =============================================================================
// Prefetch Configuration
// =============================================================================

/// Configuration for the prefetcher
#[derive(Debug, Clone)]
pub struct PrefetchConfig {
    /// Maximum concurrent prefetch operations
    pub max_concurrent: usize,
    /// Maximum prefetch queue size
    pub max_queue_size: usize,
    /// Target tier for prefetched entries
    pub target_tier: CacheTier,
    /// Whether prefetching is enabled
    pub enabled: bool,
}

impl Default for PrefetchConfig {
    fn default() -> Self {
        Self {
            max_concurrent: 8,
            max_queue_size: 1000,
            target_tier: CacheTier::L2Local,
            enabled: true,
        }
    }
}

// =============================================================================
// Prefetch Request
// =============================================================================

/// A request to prefetch data
#[derive(Debug, Clone)]
pub struct PrefetchRequest {
    /// Keys to prefetch
    pub keys: Vec<CacheKey>,
    /// Priority (higher = more urgent)
    pub priority: u8,
    /// Optional callback channel
    pub notify: bool,
}

impl PrefetchRequest {
    /// Create a new prefetch request
    pub fn new(keys: Vec<CacheKey>) -> Self {
        Self {
            keys,
            priority: 0,
            notify: false,
        }
    }

    /// Create a high-priority prefetch request
    pub fn high_priority(keys: Vec<CacheKey>) -> Self {
        Self {
            keys,
            priority: 255,
            notify: true,
        }
    }

    /// Set priority
    pub fn with_priority(mut self, priority: u8) -> Self {
        self.priority = priority;
        self
    }
}

// =============================================================================
// Prefetch Stats
// =============================================================================

/// Statistics for prefetch operations
#[derive(Debug, Default)]
pub struct PrefetchStats {
    /// Total prefetch requests received
    pub requests: AtomicU64,
    /// Total keys requested
    pub keys_requested: AtomicU64,
    /// Keys successfully prefetched
    pub keys_loaded: AtomicU64,
    /// Keys that were already cached (no-op)
    pub keys_cached: AtomicU64,
    /// Keys that failed to prefetch
    pub keys_failed: AtomicU64,
    /// Bytes prefetched
    pub bytes_loaded: AtomicU64,
    /// Currently in-flight operations
    pub in_flight: AtomicU64,
}

impl PrefetchStats {
    /// Create a snapshot of current stats
    pub fn snapshot(&self) -> PrefetchStatsSnapshot {
        PrefetchStatsSnapshot {
            requests: self.requests.load(Ordering::Relaxed),
            keys_requested: self.keys_requested.load(Ordering::Relaxed),
            keys_loaded: self.keys_loaded.load(Ordering::Relaxed),
            keys_cached: self.keys_cached.load(Ordering::Relaxed),
            keys_failed: self.keys_failed.load(Ordering::Relaxed),
            bytes_loaded: self.bytes_loaded.load(Ordering::Relaxed),
            in_flight: self.in_flight.load(Ordering::Relaxed),
        }
    }
}

/// Snapshot of prefetch statistics
#[derive(Debug, Clone, Default)]
pub struct PrefetchStatsSnapshot {
    pub requests: u64,
    pub keys_requested: u64,
    pub keys_loaded: u64,
    pub keys_cached: u64,
    pub keys_failed: u64,
    pub bytes_loaded: u64,
    pub in_flight: u64,
}

impl PrefetchStatsSnapshot {
    /// Calculate hit ratio (keys already in cache / total requested)
    pub fn cache_hit_ratio(&self) -> f64 {
        if self.keys_requested == 0 {
            0.0
        } else {
            self.keys_cached as f64 / self.keys_requested as f64
        }
    }

    /// Calculate success ratio (loaded / requested)
    pub fn success_ratio(&self) -> f64 {
        if self.keys_requested == 0 {
            0.0
        } else {
            self.keys_loaded as f64 / self.keys_requested as f64
        }
    }
}

// =============================================================================
// Prefetcher
// =============================================================================

/// Async prefetcher for cache warming
pub struct Prefetcher {
    /// Configuration
    config: PrefetchConfig,
    /// Request queue
    queue: Mutex<VecDeque<PrefetchRequest>>,
    /// Statistics
    stats: PrefetchStats,
    /// Is prefetcher running
    running: AtomicBool,
    /// Shutdown signal
    shutdown_tx: Option<mpsc::Sender<()>>,
}

impl Prefetcher {
    /// Create a new prefetcher
    pub fn new() -> Self {
        Self::with_config(PrefetchConfig::default())
    }

    /// Create with custom config
    pub fn with_config(config: PrefetchConfig) -> Self {
        Self {
            config,
            queue: Mutex::new(VecDeque::new()),
            stats: PrefetchStats::default(),
            running: AtomicBool::new(false),
            shutdown_tx: None,
        }
    }

    /// Submit a prefetch request
    pub async fn submit(&self, request: PrefetchRequest) -> Result<()> {
        if !self.config.enabled {
            return Ok(());
        }

        self.stats.requests.fetch_add(1, Ordering::Relaxed);
        self.stats
            .keys_requested
            .fetch_add(request.keys.len() as u64, Ordering::Relaxed);

        let mut queue = self.queue.lock().await;

        // Check queue size limit
        if queue.len() >= self.config.max_queue_size {
            // Drop lowest priority requests
            while queue.len() >= self.config.max_queue_size {
                queue.pop_front();
            }
        }

        // Insert based on priority (higher priority = closer to front)
        let pos = queue
            .iter()
            .position(|r| r.priority < request.priority)
            .unwrap_or(queue.len());
        queue.insert(pos, request);

        Ok(())
    }

    /// Get next batch of keys to prefetch
    pub async fn next_batch(&self, max_keys: usize) -> Vec<CacheKey> {
        let mut queue = self.queue.lock().await;
        let mut keys = Vec::new();

        while keys.len() < max_keys {
            if let Some(request) = queue.pop_front() {
                keys.extend(request.keys);
            } else {
                break;
            }
        }

        // Truncate if we got more than requested
        keys.truncate(max_keys);
        keys
    }

    /// Record successful prefetch
    pub fn record_loaded(&self, bytes: u64) {
        self.stats.keys_loaded.fetch_add(1, Ordering::Relaxed);
        self.stats.bytes_loaded.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Record cache hit (already cached)
    pub fn record_cached(&self) {
        self.stats.keys_cached.fetch_add(1, Ordering::Relaxed);
    }

    /// Record failed prefetch
    pub fn record_failed(&self) {
        self.stats.keys_failed.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment in-flight counter
    pub fn start_operation(&self) {
        self.stats.in_flight.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrement in-flight counter
    pub fn complete_operation(&self) {
        self.stats.in_flight.fetch_sub(1, Ordering::Relaxed);
    }

    /// Get current queue size
    pub async fn queue_size(&self) -> usize {
        self.queue.lock().await.len()
    }

    /// Get statistics
    pub fn stats(&self) -> PrefetchStatsSnapshot {
        self.stats.snapshot()
    }

    /// Get configuration
    pub fn config(&self) -> &PrefetchConfig {
        &self.config
    }

    /// Check if prefetcher is enabled
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Check if prefetcher has pending work
    pub async fn has_pending(&self) -> bool {
        !self.queue.lock().await.is_empty()
    }

    /// Clear the prefetch queue
    pub async fn clear(&self) {
        self.queue.lock().await.clear();
    }
}

impl Default for Prefetcher {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_submit_and_retrieve() {
        let prefetcher = Prefetcher::new();

        let keys = vec![
            CacheKey::new("test", "file1"),
            CacheKey::new("test", "file2"),
        ];

        prefetcher.submit(PrefetchRequest::new(keys.clone())).await.unwrap();

        assert_eq!(prefetcher.queue_size().await, 1);

        let batch = prefetcher.next_batch(10).await;
        assert_eq!(batch.len(), 2);
        assert_eq!(batch[0], keys[0]);

        assert_eq!(prefetcher.queue_size().await, 0);
    }

    #[tokio::test]
    async fn test_priority_ordering() {
        let prefetcher = Prefetcher::new();

        // Submit low priority first
        let low = PrefetchRequest::new(vec![CacheKey::new("test", "low")]).with_priority(0);
        prefetcher.submit(low).await.unwrap();

        // Submit high priority second
        let high = PrefetchRequest::new(vec![CacheKey::new("test", "high")]).with_priority(255);
        prefetcher.submit(high).await.unwrap();

        // High priority should come first
        let batch = prefetcher.next_batch(1).await;
        assert_eq!(batch[0].id, "high");

        let batch = prefetcher.next_batch(1).await;
        assert_eq!(batch[0].id, "low");
    }

    #[tokio::test]
    async fn test_stats_tracking() {
        let prefetcher = Prefetcher::new();

        prefetcher
            .submit(PrefetchRequest::new(vec![
                CacheKey::new("test", "file1"),
                CacheKey::new("test", "file2"),
            ]))
            .await
            .unwrap();

        prefetcher.record_loaded(1000);
        prefetcher.record_cached();
        prefetcher.record_failed();

        let stats = prefetcher.stats();
        assert_eq!(stats.requests, 1);
        assert_eq!(stats.keys_requested, 2);
        assert_eq!(stats.keys_loaded, 1);
        assert_eq!(stats.keys_cached, 1);
        assert_eq!(stats.keys_failed, 1);
        assert_eq!(stats.bytes_loaded, 1000);
    }

    #[tokio::test]
    async fn test_queue_limit() {
        let config = PrefetchConfig {
            max_queue_size: 2,
            ..Default::default()
        };
        let prefetcher = Prefetcher::with_config(config);

        // Submit 3 requests (exceeds limit of 2)
        for i in 0..3 {
            prefetcher
                .submit(PrefetchRequest::new(vec![CacheKey::new("test", &format!("file{}", i))]))
                .await
                .unwrap();
        }

        // Queue should be limited to 2
        assert!(prefetcher.queue_size().await <= 2);
    }

    #[test]
    fn test_stats_ratios() {
        let stats = PrefetchStatsSnapshot {
            requests: 10,
            keys_requested: 100,
            keys_loaded: 60,
            keys_cached: 30,
            keys_failed: 10,
            bytes_loaded: 10000,
            in_flight: 0,
        };

        assert!((stats.cache_hit_ratio() - 0.3).abs() < 0.001);
        assert!((stats.success_ratio() - 0.6).abs() < 0.001);
    }
}

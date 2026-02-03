//! Error types for the Smart Storage Operator
//!
//! Provides structured error types for all operator components including
//! hardware discovery, allocation, control plane, and platform adapters.

use std::time::Duration;
use thiserror::Error;

/// Unified error type for the operator
#[derive(Error, Debug)]
pub enum Error {
    // =========================================================================
    // Internal Errors
    // =========================================================================
    #[error("Internal error: {0}")]
    Internal(String),

    #[error("Configuration error: {0}")]
    Configuration(String),

    // =========================================================================
    // Kubernetes Errors
    // =========================================================================
    #[error("Kubernetes API error: {0}")]
    Kube(#[from] kube::Error),

    #[error("Resource not found: {kind}/{name}")]
    ResourceNotFound { kind: String, name: String },

    #[error("Resource already exists: {kind}/{name}")]
    ResourceExists { kind: String, name: String },

    // =========================================================================
    // Prometheus/Metrics Errors
    // =========================================================================
    #[error("Prometheus connection error: {0}")]
    PrometheusConnection(#[from] reqwest::Error),

    #[error("Prometheus query error: {0}")]
    PrometheusQuery(String),

    #[error("Prometheus response parse error: {0}")]
    PrometheusResponseParse(String),

    // =========================================================================
    // Migration Errors
    // =========================================================================
    #[error("Migration failed for volume {volume_name}: {reason}")]
    MigrationFailed { volume_name: String, reason: String },

    #[error("Migration already in progress for volume {volume_name}")]
    MigrationInProgress { volume_name: String },

    #[error("Migration timeout for volume {volume_name} after {duration}")]
    MigrationTimeout { volume_name: String, duration: String },

    #[error("Replica sync failed: {0}")]
    ReplicaSyncFailed(String),

    #[error("No suitable pool found for tier: {tier}")]
    NoSuitablePool { tier: String },

    // =========================================================================
    // Hardware Discovery Errors
    // =========================================================================
    #[error("Hardware discovery failed: {0}")]
    HardwareDiscovery(String),

    #[error("Device not found: {device}")]
    DeviceNotFound { device: String },

    #[error("Device access denied: {device}")]
    DeviceAccessDenied { device: String },

    #[error("NVMe command failed: {command} - {reason}")]
    NvmeCommand { command: String, reason: String },

    #[error("SMART data unavailable for device: {device}")]
    SmartUnavailable { device: String },

    // =========================================================================
    // Allocation Errors
    // =========================================================================
    #[error("Allocation failed: {0}")]
    AllocationFailed(String),

    #[error("Insufficient capacity: requested {requested} bytes, available {available} bytes")]
    InsufficientCapacity { requested: u64, available: u64 },

    #[error("No drives match allocation policy: {policy}")]
    NoDrivesMatchPolicy { policy: String },

    #[error("Placement constraint violated: {constraint}")]
    PlacementConstraintViolated { constraint: String },

    // =========================================================================
    // Node Registry Errors
    // =========================================================================
    #[error("Node not found: {node_id}")]
    NodeNotFound { node_id: String },

    #[error("Node already registered: {node_id}")]
    NodeAlreadyRegistered { node_id: String },

    #[error("Node registration failed: {node_id} - {reason}")]
    NodeRegistrationFailed { node_id: String, reason: String },

    #[error("Registry shard contention: shard {shard_id}")]
    RegistryShardContention { shard_id: usize },

    // =========================================================================
    // Backend Errors
    // =========================================================================
    #[error("Backend unavailable: {backend}")]
    BackendUnavailable { backend: String },

    #[error("Backend operation failed: {backend} - {operation}: {reason}")]
    BackendOperationFailed {
        backend: String,
        operation: String,
        reason: String,
    },

    // =========================================================================
    // Platform Adapter Errors
    // =========================================================================
    #[error("Platform adapter error: {platform} - {reason}")]
    PlatformAdapter { platform: String, reason: String },

    #[error("OpenStack API error: {service} - {reason}")]
    OpenStackApi { service: String, reason: String },

    #[error("Harvester CSI error: {reason}")]
    HarvesterCsi { reason: String },

    // =========================================================================
    // API Errors
    // =========================================================================
    #[error("API request validation failed: {0}")]
    ApiValidation(String),

    #[error("API authentication failed")]
    ApiAuthentication,

    #[error("API rate limit exceeded")]
    ApiRateLimitExceeded,

    // =========================================================================
    // Parse Errors
    // =========================================================================
    #[error("Duration parse error: {0}")]
    DurationParse(String),

    #[error("Capacity parse error: {0}")]
    CapacityParse(String),

    #[error("JSON parse error: {0}")]
    JsonParse(#[from] serde_json::Error),

    // =========================================================================
    // IO Errors
    // =========================================================================
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

/// Action to take on error during reconciliation
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorAction {
    /// Requeue with exponential backoff
    RequeueWithBackoff,
    /// Requeue after specific duration
    RequeueAfter(Duration),
    /// Don't requeue, wait for changes
    NoRequeue,
}

impl Error {
    /// Determine what action to take for this error
    pub fn action(&self) -> ErrorAction {
        match self {
            // Transient errors - retry with backoff
            Error::PrometheusConnection(_)
            | Error::Kube(_)
            | Error::BackendUnavailable { .. }
            | Error::RegistryShardContention { .. } => ErrorAction::RequeueWithBackoff,

            // In-progress operations - wait
            Error::MigrationInProgress { .. } => {
                ErrorAction::RequeueAfter(Duration::from_secs(30))
            }

            // Timeout - longer retry
            Error::MigrationTimeout { .. } => {
                ErrorAction::RequeueAfter(Duration::from_secs(300))
            }

            // Resource issues - medium retry
            Error::InsufficientCapacity { .. }
            | Error::NoSuitablePool { .. }
            | Error::NoDrivesMatchPolicy { .. } => {
                ErrorAction::RequeueAfter(Duration::from_secs(60))
            }

            // Configuration/validation errors - don't retry automatically
            Error::Configuration(_)
            | Error::ApiValidation(_)
            | Error::DurationParse(_)
            | Error::CapacityParse(_) => ErrorAction::NoRequeue,

            // All other errors - retry with backoff
            _ => ErrorAction::RequeueWithBackoff,
        }
    }

    /// Check if this error is retryable
    pub fn is_retryable(&self) -> bool {
        !matches!(self.action(), ErrorAction::NoRequeue)
    }

    /// Check if this error is transient
    pub fn is_transient(&self) -> bool {
        matches!(
            self,
            Error::PrometheusConnection(_)
                | Error::Kube(_)
                | Error::BackendUnavailable { .. }
                | Error::RegistryShardContention { .. }
        )
    }
}

/// Result type alias for the operator
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_actions() {
        let err = Error::MigrationInProgress {
            volume_name: "vol-1".into(),
        };
        assert_eq!(
            err.action(),
            ErrorAction::RequeueAfter(Duration::from_secs(30))
        );

        let err = Error::Configuration("bad config".into());
        assert_eq!(err.action(), ErrorAction::NoRequeue);

        let err = Error::InsufficientCapacity {
            requested: 1000,
            available: 500,
        };
        assert_eq!(
            err.action(),
            ErrorAction::RequeueAfter(Duration::from_secs(60))
        );
    }

    #[test]
    fn test_error_retryable() {
        let transient = Error::BackendUnavailable {
            backend: "mayastor".into(),
        };
        assert!(transient.is_retryable());
        assert!(transient.is_transient());

        let config_err = Error::Configuration("invalid".into());
        assert!(!config_err.is_retryable());
        assert!(!config_err.is_transient());
    }
}

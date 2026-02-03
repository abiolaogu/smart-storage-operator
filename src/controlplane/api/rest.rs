//! REST API Handlers
//!
//! Implements the REST API endpoints for storage provisioning,
//! node management, and capacity queries.

use crate::controlplane::Orchestrator;
use crate::crd::{
    BackendType, CapacitySpec, RedundancySpec, UnifiedStorageClass, UnifiedStorageClassSpec,
    UnifiedTier, UnifiedStorageType,
};
use crate::domain::ports::{ProvisionRequest, StorageTier, StorageType};
use crate::error::{Error, Result};
use crate::hardware::registry::{GlobalStatsSnapshot, NodeId, NodeRegistry};
use axum::{
    extract::{Json, Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{delete, get, post},
    Router,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;
use tracing::{debug, error, info};

// =============================================================================
// Request/Response Types
// =============================================================================

/// Storage provision request
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProvisionStorageRequest {
    /// Name for the storage resource
    pub name: String,
    /// Type of storage: block, file, object
    pub storage_type: String,
    /// Capacity (e.g., "100Gi", "1Ti")
    pub capacity: String,
    /// Tier: hot, warm, cold, auto
    #[serde(default)]
    pub tier: Option<String>,
    /// Maximum IOPS requirement
    #[serde(default)]
    pub max_iops: Option<u64>,
    /// Replication factor
    #[serde(default)]
    pub replication: Option<u32>,
    /// Labels
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
}

/// Storage provision response
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProvisionStorageResponse {
    pub storage_id: String,
    pub name: String,
    pub storage_type: String,
    pub capacity_bytes: u64,
    pub pool_name: String,
    pub backend: String,
    pub status: String,
}

/// Node info response
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeInfoResponse {
    pub node_id: String,
    pub hostname: String,
    pub online: bool,
    pub drive_count: u32,
    pub nvme_count: u32,
    pub total_capacity_bytes: u64,
    pub available_capacity_bytes: u64,
    pub fault_domain: Option<String>,
}

/// Cluster capacity response
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterCapacityResponse {
    pub total_nodes: u64,
    pub online_nodes: u64,
    pub total_drives: u64,
    pub total_capacity_bytes: u64,
    pub available_capacity_bytes: u64,
    pub registrations: u64,
}

/// Pool info response
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PoolInfoResponse {
    pub name: String,
    pub pool_type: String,
    pub backend: String,
    pub drive_count: u32,
    pub node_count: u32,
    pub total_capacity_bytes: u64,
    pub available_capacity_bytes: u64,
    pub utilization_percent: u32,
}

/// API error response
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiErrorResponse {
    pub error: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<String>,
}

// =============================================================================
// REST Router
// =============================================================================

/// REST API router builder
pub struct RestRouter {
    orchestrator: Arc<Orchestrator>,
    registry: Arc<NodeRegistry>,
}

impl RestRouter {
    /// Create a new REST router
    pub fn new(orchestrator: Arc<Orchestrator>, registry: Arc<NodeRegistry>) -> Self {
        Self {
            orchestrator,
            registry,
        }
    }

    /// Build the Axum router
    pub fn build(self) -> Router {
        let state = AppState {
            orchestrator: self.orchestrator,
            registry: self.registry,
        };

        Router::new()
            // Storage endpoints
            .route("/v1/storage", post(provision_storage))
            .route("/v1/storage/:id", get(get_storage))
            .route("/v1/storage/:id", delete(delete_storage))
            // Node endpoints
            .route("/v1/nodes", get(list_nodes))
            .route("/v1/nodes/:name", get(get_node))
            .route("/v1/nodes/:name/classify", post(classify_node))
            // Pool endpoints
            .route("/v1/pools", get(list_pools))
            .route("/v1/pools/:name", get(get_pool))
            // Capacity endpoint
            .route("/v1/capacity", get(get_capacity))
            // Health endpoint
            .route("/health", get(health_check))
            .route("/ready", get(readiness_check))
            .with_state(state)
    }
}

/// Shared application state
#[derive(Clone)]
struct AppState {
    orchestrator: Arc<Orchestrator>,
    registry: Arc<NodeRegistry>,
}

// =============================================================================
// Handlers
// =============================================================================

/// Provision storage
async fn provision_storage(
    State(state): State<AppState>,
    Json(request): Json<ProvisionStorageRequest>,
) -> impl IntoResponse {
    info!("Provisioning storage: {}", request.name);

    // Parse storage type
    let storage_type = match request.storage_type.to_lowercase().as_str() {
        "block" => StorageType::Block,
        "file" => StorageType::File,
        "object" => StorageType::Object,
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ApiErrorResponse {
                    error: "invalid_storage_type".into(),
                    message: format!(
                        "Invalid storage type: {}. Use 'block', 'file', or 'object'",
                        request.storage_type
                    ),
                    details: None,
                }),
            )
                .into_response();
        }
    };

    // Parse capacity
    let capacity_bytes = match parse_capacity(&request.capacity) {
        Ok(bytes) => bytes,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ApiErrorResponse {
                    error: "invalid_capacity".into(),
                    message: format!("Invalid capacity: {}", e),
                    details: None,
                }),
            )
                .into_response();
        }
    };

    // Parse tier
    let tier = request.tier.as_ref().and_then(|t| match t.to_lowercase().as_str() {
        "hot" => Some(StorageTier::Hot),
        "warm" => Some(StorageTier::Warm),
        "cold" => Some(StorageTier::Cold),
        _ => None,
    });

    // Build provision request
    let provision_req = ProvisionRequest {
        request_id: uuid_v4(),
        name: request.name.clone(),
        storage_type,
        capacity_bytes,
        tier,
        max_iops: request.max_iops,
        labels: request.labels.clone(),
        platform_params: BTreeMap::new(),
    };

    // Provision via orchestrator
    match state.orchestrator.provision(provision_req).await {
        Ok(response) => {
            let backend = match storage_type {
                StorageType::Block => "mayastor",
                StorageType::File => "seaweedfs",
                StorageType::Object => "rustfs",
            };

            (
                StatusCode::CREATED,
                Json(ProvisionStorageResponse {
                    storage_id: response.storage_id,
                    name: response.name,
                    storage_type: request.storage_type,
                    capacity_bytes: response.capacity_bytes,
                    pool_name: response.pool_name,
                    backend: backend.into(),
                    status: "provisioned".into(),
                }),
            )
                .into_response()
        }
        Err(e) => {
            error!("Provision failed: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiErrorResponse {
                    error: "provision_failed".into(),
                    message: e.to_string(),
                    details: None,
                }),
            )
                .into_response()
        }
    }
}

/// Get storage info
async fn get_storage(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match state.orchestrator.get_storage(&id).await {
        Ok(Some(response)) => (
            StatusCode::OK,
            Json(ProvisionStorageResponse {
                storage_id: response.storage_id,
                name: response.name,
                storage_type: format!("{:?}", response.storage_type).to_lowercase(),
                capacity_bytes: response.capacity_bytes,
                pool_name: response.pool_name,
                backend: response.platform_details.get("backend").cloned().unwrap_or_default(),
                status: "active".into(),
            }),
        )
            .into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(ApiErrorResponse {
                error: "not_found".into(),
                message: format!("Storage {} not found", id),
                details: None,
            }),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiErrorResponse {
                error: "internal_error".into(),
                message: e.to_string(),
                details: None,
            }),
        )
            .into_response(),
    }
}

/// Delete storage
async fn delete_storage(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match state.orchestrator.delete_storage(&id).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiErrorResponse {
                error: "delete_failed".into(),
                message: e.to_string(),
                details: None,
            }),
        )
            .into_response(),
    }
}

/// List all nodes
async fn list_nodes(State(state): State<AppState>) -> impl IntoResponse {
    let node_ids = state.registry.all_node_ids();
    let mut nodes = Vec::new();

    for node_id in node_ids {
        if let Some(entry) = state.registry.get(node_id.clone()) {
            nodes.push(NodeInfoResponse {
                node_id: node_id.to_string(),
                hostname: entry.hostname.clone(),
                online: entry.online,
                drive_count: entry.status.drives.len() as u32,
                nvme_count: entry.status.nvme_count,
                total_capacity_bytes: entry.status.total_capacity_bytes,
                available_capacity_bytes: entry.status.available_capacity_bytes,
                fault_domain: entry.fault_domain.clone(),
            });
        }
    }

    (StatusCode::OK, Json(nodes))
}

/// Get node info
async fn get_node(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    match state.registry.get(&name) {
        Some(entry) => (
            StatusCode::OK,
            Json(NodeInfoResponse {
                node_id: entry.node_id.to_string(),
                hostname: entry.hostname.clone(),
                online: entry.online,
                drive_count: entry.status.drives.len() as u32,
                nvme_count: entry.status.nvme_count,
                total_capacity_bytes: entry.status.total_capacity_bytes,
                available_capacity_bytes: entry.status.available_capacity_bytes,
                fault_domain: entry.fault_domain.clone(),
            }),
        )
            .into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(ApiErrorResponse {
                error: "not_found".into(),
                message: format!("Node {} not found", name),
                details: None,
            }),
        )
            .into_response(),
    }
}

/// Classify node drives
async fn classify_node(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    // Trigger classification via orchestrator
    match state.orchestrator.classify_node_drives(&name).await {
        Ok(_) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "status": "classification_complete",
                "node": name
            })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiErrorResponse {
                error: "classification_failed".into(),
                message: e.to_string(),
                details: None,
            }),
        )
            .into_response(),
    }
}

/// List pools
async fn list_pools(State(state): State<AppState>) -> impl IntoResponse {
    match state.orchestrator.list_pools().await {
        Ok(pools) => {
            let pool_infos: Vec<PoolInfoResponse> = pools
                .into_iter()
                .map(|p| PoolInfoResponse {
                    name: p.name,
                    pool_type: p.pool_type,
                    backend: p.backend,
                    drive_count: p.drive_count,
                    node_count: p.node_count,
                    total_capacity_bytes: p.total_capacity_bytes,
                    available_capacity_bytes: p.available_capacity_bytes,
                    utilization_percent: p.utilization_percent,
                })
                .collect();
            (StatusCode::OK, Json(pool_infos)).into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiErrorResponse {
                error: "list_pools_failed".into(),
                message: e.to_string(),
                details: None,
            }),
        )
            .into_response(),
    }
}

/// Get pool info
async fn get_pool(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    match state.orchestrator.get_pool(&name).await {
        Ok(Some(pool)) => (
            StatusCode::OK,
            Json(PoolInfoResponse {
                name: pool.name,
                pool_type: pool.pool_type,
                backend: pool.backend,
                drive_count: pool.drive_count,
                node_count: pool.node_count,
                total_capacity_bytes: pool.total_capacity_bytes,
                available_capacity_bytes: pool.available_capacity_bytes,
                utilization_percent: pool.utilization_percent,
            }),
        )
            .into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(ApiErrorResponse {
                error: "not_found".into(),
                message: format!("Pool {} not found", name),
                details: None,
            }),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiErrorResponse {
                error: "get_pool_failed".into(),
                message: e.to_string(),
                details: None,
            }),
        )
            .into_response(),
    }
}

/// Get cluster capacity
async fn get_capacity(State(state): State<AppState>) -> impl IntoResponse {
    let stats = state.registry.stats();

    (
        StatusCode::OK,
        Json(ClusterCapacityResponse {
            total_nodes: stats.total_nodes,
            online_nodes: stats.online_nodes,
            total_drives: stats.total_drives,
            total_capacity_bytes: stats.total_capacity_bytes,
            available_capacity_bytes: stats.available_capacity_bytes,
            registrations: stats.registrations,
        }),
    )
}

/// Health check
async fn health_check() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

/// Readiness check
async fn readiness_check(State(state): State<AppState>) -> impl IntoResponse {
    // Check if we have any registered nodes
    let stats = state.registry.stats();
    if stats.online_nodes > 0 {
        (StatusCode::OK, "ready")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "no nodes registered")
    }
}

// =============================================================================
// Utility Functions
// =============================================================================

/// Parse capacity string (e.g., "100Gi", "1Ti") to bytes
fn parse_capacity(s: &str) -> Result<u64> {
    let s = s.trim();
    if s.is_empty() {
        return Err(Error::CapacityParse("empty capacity string".into()));
    }

    // Find where the number ends and unit begins
    let mut num_end = 0;
    for (i, c) in s.char_indices() {
        if !c.is_ascii_digit() && c != '.' {
            num_end = i;
            break;
        }
        num_end = i + 1;
    }

    let num_str = &s[..num_end];
    let unit_str = s[num_end..].trim();

    let num: f64 = num_str
        .parse()
        .map_err(|_| Error::CapacityParse(format!("invalid number: {}", num_str)))?;

    let multiplier: u64 = match unit_str.to_uppercase().as_str() {
        "" | "B" => 1,
        "K" | "KB" | "KI" | "KIB" => 1024,
        "M" | "MB" | "MI" | "MIB" => 1024 * 1024,
        "G" | "GB" | "GI" | "GIB" => 1024 * 1024 * 1024,
        "T" | "TB" | "TI" | "TIB" => 1024 * 1024 * 1024 * 1024,
        "P" | "PB" | "PI" | "PIB" => 1024 * 1024 * 1024 * 1024 * 1024,
        _ => {
            return Err(Error::CapacityParse(format!(
                "unknown unit: {}",
                unit_str
            )))
        }
    };

    Ok((num * multiplier as f64) as u64)
}

/// Generate a simple UUID v4
fn uuid_v4() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();

    format!(
        "{:08x}-{:04x}-4{:03x}-{:04x}-{:012x}",
        (now >> 96) as u32,
        (now >> 80) as u16,
        (now >> 68) as u16 & 0x0FFF,
        ((now >> 52) as u16 & 0x3FFF) | 0x8000,
        now as u64 & 0xFFFFFFFFFFFF
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_capacity() {
        assert_eq!(parse_capacity("100").unwrap(), 100);
        assert_eq!(parse_capacity("100B").unwrap(), 100);
        assert_eq!(parse_capacity("1K").unwrap(), 1024);
        assert_eq!(parse_capacity("1Ki").unwrap(), 1024);
        assert_eq!(parse_capacity("1M").unwrap(), 1024 * 1024);
        assert_eq!(parse_capacity("1G").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(parse_capacity("1Gi").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(parse_capacity("100Gi").unwrap(), 100 * 1024 * 1024 * 1024);
        assert_eq!(parse_capacity("1T").unwrap(), 1024 * 1024 * 1024 * 1024);

        assert!(parse_capacity("").is_err());
        assert!(parse_capacity("abc").is_err());
        assert!(parse_capacity("100X").is_err());
    }

    #[test]
    fn test_uuid_v4_format() {
        let uuid = uuid_v4();
        assert_eq!(uuid.len(), 36);
        assert_eq!(&uuid[8..9], "-");
        assert_eq!(&uuid[13..14], "-");
        assert_eq!(&uuid[14..15], "4"); // Version 4
        assert_eq!(&uuid[18..19], "-");
        assert_eq!(&uuid[23..24], "-");
    }
}

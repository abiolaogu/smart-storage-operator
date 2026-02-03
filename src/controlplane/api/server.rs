//! Unified API Server
//!
//! Runs both gRPC and REST servers for the control plane API.

use crate::error::{Error, Result};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::{error, info};

use super::rest::RestRouter;
use crate::controlplane::Orchestrator;
use crate::hardware::registry::NodeRegistry;

// =============================================================================
// Server Configuration
// =============================================================================

/// Configuration for the API server
#[derive(Debug, Clone)]
pub struct ApiServerConfig {
    /// REST API bind address
    pub rest_addr: SocketAddr,
    /// gRPC API bind address
    pub grpc_addr: SocketAddr,
    /// Enable TLS
    pub tls_enabled: bool,
    /// TLS certificate path
    pub tls_cert_path: Option<String>,
    /// TLS key path
    pub tls_key_path: Option<String>,
    /// Request timeout in seconds
    pub request_timeout_secs: u64,
    /// Max request body size
    pub max_body_size: usize,
}

impl Default for ApiServerConfig {
    fn default() -> Self {
        Self {
            rest_addr: "0.0.0.0:8090".parse().unwrap(),
            grpc_addr: "0.0.0.0:8091".parse().unwrap(),
            tls_enabled: false,
            tls_cert_path: None,
            tls_key_path: None,
            request_timeout_secs: 30,
            max_body_size: 10 * 1024 * 1024, // 10MB
        }
    }
}

// =============================================================================
// API Context
// =============================================================================

/// Shared context for API handlers
pub struct ApiContext {
    /// Orchestrator reference
    pub orchestrator: Arc<Orchestrator>,
    /// Node registry reference
    pub registry: Arc<NodeRegistry>,
    /// Shutdown signal
    pub shutdown_rx: broadcast::Receiver<()>,
}

impl ApiContext {
    /// Create a new API context
    pub fn new(
        orchestrator: Arc<Orchestrator>,
        registry: Arc<NodeRegistry>,
        shutdown_rx: broadcast::Receiver<()>,
    ) -> Arc<Self> {
        Arc::new(Self {
            orchestrator,
            registry,
            shutdown_rx,
        })
    }
}

// =============================================================================
// API Server
// =============================================================================

/// Unified API Server running gRPC and REST
pub struct ApiServer {
    config: ApiServerConfig,
    orchestrator: Arc<Orchestrator>,
    registry: Arc<NodeRegistry>,
    shutdown_tx: broadcast::Sender<()>,
}

impl ApiServer {
    /// Create a new API server
    pub fn new(
        config: ApiServerConfig,
        orchestrator: Arc<Orchestrator>,
        registry: Arc<NodeRegistry>,
    ) -> Self {
        let (shutdown_tx, _) = broadcast::channel(1);

        Self {
            config,
            orchestrator,
            registry,
            shutdown_tx,
        }
    }

    /// Run the API server
    pub async fn run(&self) -> Result<()> {
        info!("Starting Unified API Server");
        info!("  REST API: {}", self.config.rest_addr);
        info!("  gRPC API: {}", self.config.grpc_addr);

        // Create REST server
        let rest_handle = self.spawn_rest_server();

        // Wait for shutdown
        tokio::select! {
            result = rest_handle => {
                if let Err(e) = result {
                    error!("REST server error: {:?}", e);
                }
            }
        }

        Ok(())
    }

    /// Spawn the REST server
    fn spawn_rest_server(&self) -> tokio::task::JoinHandle<Result<()>> {
        let addr = self.config.rest_addr;
        let orchestrator = self.orchestrator.clone();
        let registry = self.registry.clone();
        let shutdown_rx = self.shutdown_tx.subscribe();

        tokio::spawn(async move {
            run_rest_server(addr, orchestrator, registry, shutdown_rx).await
        })
    }

    /// Trigger graceful shutdown
    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(());
    }
}

/// Run the REST API server
async fn run_rest_server(
    addr: SocketAddr,
    orchestrator: Arc<Orchestrator>,
    registry: Arc<NodeRegistry>,
    mut shutdown_rx: broadcast::Receiver<()>,
) -> Result<()> {
    use axum::Router;

    let router = RestRouter::new(orchestrator, registry);
    let app = router.build();

    info!("REST API listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await.map_err(|e| {
        Error::Internal(format!("Failed to bind REST server: {}", e))
    })?;

    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            let _ = shutdown_rx.recv().await;
            info!("REST server shutting down");
        })
        .await
        .map_err(|e| Error::Internal(format!("REST server error: {}", e)))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = ApiServerConfig::default();
        assert_eq!(config.rest_addr.port(), 8090);
        assert_eq!(config.grpc_addr.port(), 8091);
        assert!(!config.tls_enabled);
    }
}

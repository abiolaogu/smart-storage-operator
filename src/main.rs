//! Smart Storage Operator
//!
//! A Kubernetes operator providing unified storage management across
//! Block (Mayastor), File (SeaweedFS), and Object (RustFS) storage backends.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────────┐
//! │                     Unified Control Plane Orchestrator                       │
//! ├─────────────────────────────────────────────────────────────────────────────┤
//! │  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────────────────┐  │
//! │  │   Unified API   │  │    Hardware     │  │      Allocation             │  │
//! │  │   (gRPC/REST)   │  │    Discovery    │  │      Engine                 │  │
//! │  └────────┬────────┘  └────────┬────────┘  └─────────────┬───────────────┘  │
//! │           │                    │                         │                   │
//! │           └────────────────────┼─────────────────────────┘                   │
//! │                                │                                             │
//! │                    ┌───────────┴───────────┐                                │
//! │                    │  Node Registry (DOD)  │                                │
//! │                    │  (256-way sharded)    │                                │
//! │                    └───────────────────────┘                                │
//! ├─────────────────────────────────────────────────────────────────────────────┤
//! │                          Storage Backends                                    │
//! │  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────────────────┐  │
//! │  │     Block       │  │      File       │  │         Object              │  │
//! │  │   (Mayastor)    │  │   (SeaweedFS)   │  │        (RustFS)             │  │
//! │  └─────────────────┘  └─────────────────┘  └─────────────────────────────┘  │
//! ├─────────────────────────────────────────────────────────────────────────────┤
//! │                        Platform Adapters                                     │
//! │  ┌─────────────────────────────┐  ┌─────────────────────────────────────┐   │
//! │  │      Harvester HCI          │  │           OpenStack                 │   │
//! │  │   (Longhorn CSI)            │  │   (Cinder/Manila/Swift)             │   │
//! │  └─────────────────────────────┘  └─────────────────────────────────────┘   │
//! └─────────────────────────────────────────────────────────────────────────────┘
//! ```

use clap::Parser;
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::{error, info, Level};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

use smart_storage_operator::{
    ApiServer, ApiServerConfig, NodeRegistry, Orchestrator, OrchestratorConfig,
    Result, Error,
};

// =============================================================================
// CLI Arguments
// =============================================================================

/// Smart Storage Operator - Unified Control Plane for Block/File/Object Storage
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// REST API bind address
    #[arg(long, env = "API_ADDR", default_value = "0.0.0.0:8090")]
    api_addr: String,

    /// gRPC API bind address
    #[arg(long, env = "GRPC_ADDR", default_value = "0.0.0.0:8091")]
    grpc_addr: String,

    /// Health server bind address
    #[arg(long, env = "HEALTH_ADDR", default_value = "0.0.0.0:8081")]
    health_addr: String,

    /// Metrics server bind address
    #[arg(long, env = "METRICS_ADDR", default_value = "0.0.0.0:8080")]
    metrics_addr: String,

    /// Mayastor namespace
    #[arg(long, env = "MAYASTOR_NAMESPACE", default_value = "mayastor")]
    mayastor_namespace: String,

    /// Enable auto-discovery of hardware
    #[arg(long, env = "AUTO_DISCOVER")]
    auto_discover: bool,

    /// Discovery interval in seconds
    #[arg(long, env = "DISCOVER_INTERVAL", default_value = "300")]
    discover_interval_secs: u64,

    /// Log level (trace, debug, info, warn, error)
    #[arg(long, env = "LOG_LEVEL", default_value = "info")]
    log_level: String,

    /// Output logs as JSON
    #[arg(long, env = "LOG_JSON")]
    log_json: bool,

    /// Run in standalone mode (no Kubernetes)
    #[arg(long, env = "STANDALONE")]
    standalone: bool,
}

// =============================================================================
// Main
// =============================================================================

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Initialize logging
    init_logging(&args);

    info!("Starting Smart Storage Operator - Unified Control Plane");
    info!("  Version: {}", smart_storage_operator::VERSION);
    info!("  REST API: {}", args.api_addr);
    info!("  gRPC API: {}", args.grpc_addr);
    info!("  Auto-discover: {}", args.auto_discover);
    info!("  Standalone mode: {}", args.standalone);

    // Create node registry
    let registry = NodeRegistry::new();
    info!("Node registry initialized (256-way sharded)");

    // Create orchestrator config
    let mut orch_config = OrchestratorConfig::default();
    orch_config.auto_classify = args.auto_discover;
    orch_config.classify_interval_secs = args.discover_interval_secs;
    orch_config.backends.mayastor.namespace = args.mayastor_namespace.clone();

    // Create orchestrator
    let orchestrator = Orchestrator::new(orch_config, registry.clone());

    // Initialize orchestrator
    orchestrator.initialize().await?;
    info!("Orchestrator initialized");

    // Start health server
    let health_addr = args.health_addr.clone();
    tokio::spawn(async move {
        if let Err(e) = run_health_server(&health_addr).await {
            error!("Health server error: {}", e);
        }
    });

    // Start metrics server
    let metrics_addr = args.metrics_addr.clone();
    tokio::spawn(async move {
        if let Err(e) = run_metrics_server(&metrics_addr).await {
            error!("Metrics server error: {}", e);
        }
    });

    // Create and run API server
    let api_config = ApiServerConfig {
        rest_addr: args.api_addr.parse().map_err(|e| {
            Error::Configuration(format!("Invalid REST API address: {}", e))
        })?,
        grpc_addr: args.grpc_addr.parse().map_err(|e| {
            Error::Configuration(format!("Invalid gRPC API address: {}", e))
        })?,
        ..Default::default()
    };

    let api_server = ApiServer::new(api_config, orchestrator.clone(), registry.clone());

    info!("Starting unified API server");
    api_server.run().await?;

    info!("Operator shutdown complete");
    Ok(())
}

// =============================================================================
// Logging Setup
// =============================================================================

fn init_logging(args: &Args) {
    let level = match args.log_level.to_lowercase().as_str() {
        "trace" => Level::TRACE,
        "debug" => Level::DEBUG,
        "info" => Level::INFO,
        "warn" => Level::WARN,
        "error" => Level::ERROR,
        _ => Level::INFO,
    };

    let filter = EnvFilter::from_default_env()
        .add_directive(level.into())
        .add_directive("hyper=warn".parse().unwrap())
        .add_directive("kube=info".parse().unwrap())
        .add_directive("tower=warn".parse().unwrap())
        .add_directive("axum=info".parse().unwrap());

    if args.log_json {
        tracing_subscriber::registry()
            .with(filter)
            .with(fmt::layer().json())
            .init();
    } else {
        tracing_subscriber::registry()
            .with(filter)
            .with(fmt::layer().with_target(true))
            .init();
    }
}

// =============================================================================
// Health Server
// =============================================================================

async fn run_health_server(addr: &str) -> Result<()> {
    use hyper::service::{make_service_fn, service_fn};
    use hyper::{Body, Request, Response, Server, StatusCode};

    let make_svc = make_service_fn(|_conn| async {
        Ok::<_, std::convert::Infallible>(service_fn(|req: Request<Body>| async move {
            let response = match req.uri().path() {
                "/healthz" | "/livez" => Response::builder()
                    .status(StatusCode::OK)
                    .body(Body::from("ok"))
                    .unwrap(),
                "/readyz" => Response::builder()
                    .status(StatusCode::OK)
                    .body(Body::from("ok"))
                    .unwrap(),
                _ => Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Body::from("not found"))
                    .unwrap(),
            };
            Ok::<_, std::convert::Infallible>(response)
        }))
    });

    let addr: SocketAddr = addr.parse().map_err(|e| {
        Error::Internal(format!("Invalid health server address: {}", e))
    })?;

    info!("Health server listening on {}", addr);
    Server::bind(&addr)
        .serve(make_svc)
        .await
        .map_err(|e| Error::Internal(format!("Health server error: {}", e)))?;

    Ok(())
}

// =============================================================================
// Metrics Server
// =============================================================================

async fn run_metrics_server(addr: &str) -> Result<()> {
    use hyper::service::{make_service_fn, service_fn};
    use hyper::{Body, Request, Response, Server, StatusCode};
    use prometheus::{Encoder, TextEncoder};

    // Register operator metrics
    let _ = prometheus::register_gauge!(
        "unified_control_plane_nodes_total",
        "Total number of registered nodes"
    );
    let _ = prometheus::register_gauge!(
        "unified_control_plane_nodes_online",
        "Number of online nodes"
    );
    let _ = prometheus::register_counter!(
        "unified_control_plane_provisions_total",
        "Total number of storage provisions"
    );
    let _ = prometheus::register_counter_vec!(
        "unified_control_plane_provisions_by_type",
        "Provisions by storage type",
        &["type"]
    );
    let _ = prometheus::register_histogram!(
        "unified_control_plane_provision_duration_seconds",
        "Duration of provision operations"
    );

    let make_svc = make_service_fn(|_conn| async {
        Ok::<_, std::convert::Infallible>(service_fn(|req: Request<Body>| async move {
            let response = match req.uri().path() {
                "/metrics" => {
                    let encoder = TextEncoder::new();
                    let metric_families = prometheus::gather();
                    let mut buffer = Vec::new();
                    encoder.encode(&metric_families, &mut buffer).unwrap();

                    Response::builder()
                        .status(StatusCode::OK)
                        .header("Content-Type", encoder.format_type())
                        .body(Body::from(buffer))
                        .unwrap()
                }
                _ => Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Body::from("not found"))
                    .unwrap(),
            };
            Ok::<_, std::convert::Infallible>(response)
        }))
    });

    let addr: SocketAddr = addr.parse().map_err(|e| {
        Error::Internal(format!("Invalid metrics server address: {}", e))
    })?;

    info!("Metrics server listening on {}", addr);
    Server::bind(&addr)
        .serve(make_svc)
        .await
        .map_err(|e| Error::Internal(format!("Metrics server error: {}", e)))?;

    Ok(())
}

//! API Module
//!
//! Provides unified gRPC and REST APIs for storage provisioning,
//! node management, and metrics streaming.

pub mod server;
pub mod rest;

pub use server::*;
pub use rest::*;

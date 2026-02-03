//! Domain layer - Core business logic and port definitions
//!
//! This module defines the core traits (ports) that adapters implement,
//! following hexagonal architecture principles.

pub mod ports;

pub use ports::*;

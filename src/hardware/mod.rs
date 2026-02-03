//! Hardware Module
//!
//! Provides hardware discovery, classification, allocation, and registry
//! functionality for the unified control plane.

pub mod discovery;
pub mod classification;
pub mod allocation;
pub mod registry;

pub use discovery::*;
pub use classification::*;
pub use allocation::*;
pub use registry::*;

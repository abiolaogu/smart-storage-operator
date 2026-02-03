//! Allocation Module
//!
//! Provides drive allocation based on policies, placement constraints,
//! and cross-node distribution.

pub mod allocator;
pub mod policy;
pub mod placement;

pub use allocator::*;
pub use policy::*;
pub use placement::*;

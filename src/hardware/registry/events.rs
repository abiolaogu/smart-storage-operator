//! Registry Events
//!
//! Events emitted by the node registry for external consumers to react to
//! node lifecycle changes.

use serde::{Deserialize, Serialize};

/// Events emitted by the node registry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RegistryEvent {
    /// A new node was registered
    NodeRegistered {
        node_id: String,
        hostname: String,
        drive_count: u32,
    },

    /// A node was deregistered
    NodeDeregistered { node_id: String },

    /// A node's status was updated
    NodeUpdated { node_id: String },

    /// A node went offline (missed heartbeats)
    NodeWentOffline { node_id: String },

    /// A node came back online
    NodeCameOnline { node_id: String },

    /// A drive was added to a node
    DriveAdded {
        node_id: String,
        drive_id: String,
        capacity_bytes: u64,
    },

    /// A drive was removed from a node
    DriveRemoved { node_id: String, drive_id: String },

    /// A drive's health status changed
    DriveHealthChanged {
        node_id: String,
        drive_id: String,
        healthy: bool,
        reason: Option<String>,
    },

    /// Drive metrics exceeded threshold
    DriveMetricsAlert {
        node_id: String,
        drive_id: String,
        alert_type: MetricsAlertType,
        value: f64,
        threshold: f64,
    },
}

/// Types of metrics alerts
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum MetricsAlertType {
    /// High temperature
    HighTemperature,
    /// High wear level
    HighWearLevel,
    /// High latency
    HighLatency,
    /// High utilization
    HighUtilization,
    /// Low IOPS (possible failure)
    LowIops,
}

impl std::fmt::Display for MetricsAlertType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MetricsAlertType::HighTemperature => write!(f, "high_temperature"),
            MetricsAlertType::HighWearLevel => write!(f, "high_wear_level"),
            MetricsAlertType::HighLatency => write!(f, "high_latency"),
            MetricsAlertType::HighUtilization => write!(f, "high_utilization"),
            MetricsAlertType::LowIops => write!(f, "low_iops"),
        }
    }
}

impl RegistryEvent {
    /// Get the node ID associated with this event
    pub fn node_id(&self) -> &str {
        match self {
            RegistryEvent::NodeRegistered { node_id, .. } => node_id,
            RegistryEvent::NodeDeregistered { node_id } => node_id,
            RegistryEvent::NodeUpdated { node_id } => node_id,
            RegistryEvent::NodeWentOffline { node_id } => node_id,
            RegistryEvent::NodeCameOnline { node_id } => node_id,
            RegistryEvent::DriveAdded { node_id, .. } => node_id,
            RegistryEvent::DriveRemoved { node_id, .. } => node_id,
            RegistryEvent::DriveHealthChanged { node_id, .. } => node_id,
            RegistryEvent::DriveMetricsAlert { node_id, .. } => node_id,
        }
    }

    /// Check if this is a node-level event
    pub fn is_node_event(&self) -> bool {
        matches!(
            self,
            RegistryEvent::NodeRegistered { .. }
                | RegistryEvent::NodeDeregistered { .. }
                | RegistryEvent::NodeUpdated { .. }
                | RegistryEvent::NodeWentOffline { .. }
                | RegistryEvent::NodeCameOnline { .. }
        )
    }

    /// Check if this is a drive-level event
    pub fn is_drive_event(&self) -> bool {
        matches!(
            self,
            RegistryEvent::DriveAdded { .. }
                | RegistryEvent::DriveRemoved { .. }
                | RegistryEvent::DriveHealthChanged { .. }
                | RegistryEvent::DriveMetricsAlert { .. }
        )
    }

    /// Get the drive ID if this is a drive event
    pub fn drive_id(&self) -> Option<&str> {
        match self {
            RegistryEvent::DriveAdded { drive_id, .. } => Some(drive_id),
            RegistryEvent::DriveRemoved { drive_id, .. } => Some(drive_id),
            RegistryEvent::DriveHealthChanged { drive_id, .. } => Some(drive_id),
            RegistryEvent::DriveMetricsAlert { drive_id, .. } => Some(drive_id),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_node_id() {
        let event = RegistryEvent::NodeRegistered {
            node_id: "node-001".to_string(),
            hostname: "host.local".to_string(),
            drive_count: 4,
        };
        assert_eq!(event.node_id(), "node-001");
        assert!(event.is_node_event());
        assert!(!event.is_drive_event());
    }

    #[test]
    fn test_event_drive_id() {
        let event = RegistryEvent::DriveAdded {
            node_id: "node-001".to_string(),
            drive_id: "nvme0n1".to_string(),
            capacity_bytes: 1_000_000_000_000,
        };
        assert_eq!(event.node_id(), "node-001");
        assert_eq!(event.drive_id(), Some("nvme0n1"));
        assert!(event.is_drive_event());
        assert!(!event.is_node_event());
    }
}

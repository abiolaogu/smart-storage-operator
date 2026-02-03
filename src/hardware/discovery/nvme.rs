//! NVMe-specific Discovery
//!
//! Provides detailed NVMe device discovery including namespace enumeration,
//! ZNS detection, and SMART data retrieval via nvme-cli.

use crate::domain::ports::{NvmeNamespaceInfo, SmartData};
use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};
use std::process::Command;
use tracing::{debug, warn};

// =============================================================================
// NVMe Identify Structures
// =============================================================================

/// NVMe controller identify data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NvmeControllerInfo {
    /// Model name
    pub model: String,
    /// Serial number
    pub serial: String,
    /// Firmware revision
    pub firmware: String,
    /// Number of namespaces
    pub namespace_count: u32,
    /// Maximum data transfer size (in 512-byte blocks)
    pub mdts: u32,
    /// Controller ID
    pub cntlid: u16,
    /// NVMe version
    pub version: String,
    /// Supported features
    pub features: NvmeFeatures,
}

/// Supported NVMe features
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NvmeFeatures {
    /// Supports ZNS (Zoned Namespaces)
    pub zns: bool,
    /// Supports namespace management
    pub ns_management: bool,
    /// Supports multi-path
    pub multipath: bool,
    /// Supports SR-IOV
    pub sriov: bool,
    /// Supports persistent memory region
    pub pmr: bool,
}

/// NVMe namespace extended info
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NvmeNamespaceExtended {
    /// Base info
    pub base: NvmeNamespaceInfo,
    /// Formatted LBA size
    pub lba_size: u32,
    /// Metadata size
    pub ms: u16,
    /// Number of LBAs
    pub nsze: u64,
    /// Namespace utilization
    pub nuse: u64,
    /// ZNS specific data
    pub zns_info: Option<ZnsNamespaceInfo>,
}

/// ZNS namespace information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZnsNamespaceInfo {
    /// Zone size in LBAs
    pub zone_size_lba: u64,
    /// Number of zones
    pub zone_count: u64,
    /// Maximum open zones
    pub max_open_zones: u32,
    /// Maximum active zones
    pub max_active_zones: u32,
}

// =============================================================================
// NVMe Discovery
// =============================================================================

/// NVMe-specific discovery operations
pub struct NvmeDiscovery;

impl NvmeDiscovery {
    /// Get controller info using nvme-cli
    pub async fn get_controller_info(device: &str) -> Result<NvmeControllerInfo> {
        // Extract controller from device (e.g., /dev/nvme0n1 -> /dev/nvme0)
        let controller = Self::extract_controller_path(device)?;

        // Run nvme id-ctrl command
        let output = Command::new("nvme")
            .args(["id-ctrl", &controller, "-o", "json"])
            .output()
            .map_err(|e| Error::NvmeCommand {
                command: "id-ctrl".into(),
                reason: e.to_string(),
            })?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Error::NvmeCommand {
                command: "id-ctrl".into(),
                reason: stderr.to_string(),
            });
        }

        // Parse JSON output
        let json: serde_json::Value = serde_json::from_slice(&output.stdout)
            .map_err(|e| Error::NvmeCommand {
                command: "id-ctrl".into(),
                reason: format!("JSON parse error: {}", e),
            })?;

        Ok(NvmeControllerInfo {
            model: json["mn"].as_str().unwrap_or("Unknown").trim().to_string(),
            serial: json["sn"].as_str().unwrap_or("Unknown").trim().to_string(),
            firmware: json["fr"].as_str().unwrap_or("Unknown").trim().to_string(),
            namespace_count: json["nn"].as_u64().unwrap_or(1) as u32,
            mdts: json["mdts"].as_u64().unwrap_or(0) as u32,
            cntlid: json["cntlid"].as_u64().unwrap_or(0) as u16,
            version: format!(
                "{}.{}.{}",
                json["ver"].as_u64().unwrap_or(0) >> 16,
                (json["ver"].as_u64().unwrap_or(0) >> 8) & 0xFF,
                json["ver"].as_u64().unwrap_or(0) & 0xFF
            ),
            features: Self::parse_features(&json),
        })
    }

    /// Get namespace info
    pub async fn get_namespace_info(device: &str) -> Result<NvmeNamespaceExtended> {
        let output = Command::new("nvme")
            .args(["id-ns", device, "-o", "json"])
            .output()
            .map_err(|e| Error::NvmeCommand {
                command: "id-ns".into(),
                reason: e.to_string(),
            })?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Error::NvmeCommand {
                command: "id-ns".into(),
                reason: stderr.to_string(),
            });
        }

        let json: serde_json::Value = serde_json::from_slice(&output.stdout)
            .map_err(|e| Error::NvmeCommand {
                command: "id-ns".into(),
                reason: format!("JSON parse error: {}", e),
            })?;

        let nsze = json["nsze"].as_u64().unwrap_or(0);
        let nuse = json["nuse"].as_u64().unwrap_or(0);

        // Get LBA format info
        let flbas = json["flbas"].as_u64().unwrap_or(0) as usize & 0xF;
        let lbaf = &json["lbafs"][flbas];
        let lba_size = 1u32 << lbaf["ds"].as_u64().unwrap_or(9);
        let ms = lbaf["ms"].as_u64().unwrap_or(0) as u16;

        // Check for ZNS
        let zns_info = Self::get_zns_info(device).await.ok();
        let is_zns = zns_info.is_some();

        let nsid = Self::extract_nsid(device)?;

        Ok(NvmeNamespaceExtended {
            base: NvmeNamespaceInfo {
                nsid,
                capacity_bytes: nsze * lba_size as u64,
                active: true,
                is_zns,
            },
            lba_size,
            ms,
            nsze,
            nuse,
            zns_info,
        })
    }

    /// Get ZNS-specific information
    pub async fn get_zns_info(device: &str) -> Result<ZnsNamespaceInfo> {
        let output = Command::new("nvme")
            .args(["zns", "id-ns", device, "-o", "json"])
            .output()
            .map_err(|e| Error::NvmeCommand {
                command: "zns id-ns".into(),
                reason: e.to_string(),
            })?;

        if !output.status.success() {
            return Err(Error::NvmeCommand {
                command: "zns id-ns".into(),
                reason: "Not a ZNS namespace".into(),
            });
        }

        let json: serde_json::Value = serde_json::from_slice(&output.stdout)
            .map_err(|e| Error::NvmeCommand {
                command: "zns id-ns".into(),
                reason: format!("JSON parse error: {}", e),
            })?;

        Ok(ZnsNamespaceInfo {
            zone_size_lba: json["zsze"].as_u64().unwrap_or(0),
            zone_count: json["nzc"].as_u64().unwrap_or(0),
            max_open_zones: json["mor"].as_u64().unwrap_or(0) as u32 + 1,
            max_active_zones: json["mar"].as_u64().unwrap_or(0) as u32 + 1,
        })
    }

    /// Get SMART data
    pub async fn get_smart_data(device: &str) -> Result<SmartData> {
        let controller = Self::extract_controller_path(device)?;

        let output = Command::new("nvme")
            .args(["smart-log", &controller, "-o", "json"])
            .output()
            .map_err(|e| Error::NvmeCommand {
                command: "smart-log".into(),
                reason: e.to_string(),
            })?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Error::SmartUnavailable {
                device: device.to_string(),
            });
        }

        let json: serde_json::Value = serde_json::from_slice(&output.stdout)
            .map_err(|e| Error::NvmeCommand {
                command: "smart-log".into(),
                reason: format!("JSON parse error: {}", e),
            })?;

        Ok(SmartData {
            temperature_celsius: (json["temperature"].as_i64().unwrap_or(0) - 273) as i32,
            percentage_used: json["percent_used"].as_u64().unwrap_or(0) as u8,
            data_units_read: json["data_units_read"].as_u64().unwrap_or(0),
            data_units_written: json["data_units_written"].as_u64().unwrap_or(0),
            power_on_hours: json["power_on_hours"].as_u64().unwrap_or(0),
            critical_warning: json["critical_warning"].as_u64().unwrap_or(0) as u8,
        })
    }

    /// List all NVMe devices in the system
    pub async fn list_devices() -> Result<Vec<String>> {
        let output = Command::new("nvme")
            .args(["list", "-o", "json"])
            .output()
            .map_err(|e| Error::NvmeCommand {
                command: "list".into(),
                reason: e.to_string(),
            })?;

        if !output.status.success() {
            // nvme list may fail if no devices
            return Ok(Vec::new());
        }

        let json: serde_json::Value = serde_json::from_slice(&output.stdout)
            .map_err(|e| Error::NvmeCommand {
                command: "list".into(),
                reason: format!("JSON parse error: {}", e),
            })?;

        let devices: Vec<String> = json["Devices"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|d| d["DevicePath"].as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();

        Ok(devices)
    }

    /// Check if nvme-cli is available
    pub fn is_nvme_cli_available() -> bool {
        Command::new("nvme")
            .arg("version")
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false)
    }

    // Helper functions

    fn extract_controller_path(device: &str) -> Result<String> {
        // /dev/nvme0n1 -> /dev/nvme0
        // /dev/nvme0n1p1 -> /dev/nvme0
        let path = device.strip_prefix("/dev/").unwrap_or(device);

        // Find the 'n' that separates controller number from namespace number
        // nvme0n1 -> find 'n' after "nvme" prefix and controller number
        if path.starts_with("nvme") {
            // Skip "nvme" prefix, then find the next 'n' (namespace separator)
            if let Some(n_idx) = path[4..].find('n') {
                let ctrl_end = 4 + n_idx;
                return Ok(format!("/dev/{}", &path[..ctrl_end]));
            }
        }

        Err(Error::HardwareDiscovery(format!(
            "Cannot extract controller from {}",
            device
        )))
    }

    fn extract_nsid(device: &str) -> Result<u32> {
        let path = device.strip_prefix("/dev/").unwrap_or(device);

        if let Some(n_idx) = path.rfind('n') {
            let ns_part = &path[n_idx + 1..];
            let ns_num = ns_part.split('p').next().unwrap_or(ns_part);
            return ns_num.parse().map_err(|_| {
                Error::HardwareDiscovery(format!("Invalid namespace ID in {}", device))
            });
        }

        Err(Error::HardwareDiscovery(format!(
            "Cannot extract namespace ID from {}",
            device
        )))
    }

    fn parse_features(json: &serde_json::Value) -> NvmeFeatures {
        let oacs = json["oacs"].as_u64().unwrap_or(0);
        let cmic = json["cmic"].as_u64().unwrap_or(0);

        NvmeFeatures {
            zns: json["cntrltype"].as_u64().unwrap_or(0) == 2, // IO controller
            ns_management: (oacs & 0x8) != 0,
            multipath: (cmic & 0x1) != 0,
            sriov: json["sriov_caps"].as_u64().unwrap_or(0) != 0,
            pmr: json["pmrwbm"].as_u64().unwrap_or(0) != 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_controller_path() {
        assert_eq!(
            NvmeDiscovery::extract_controller_path("/dev/nvme0n1").unwrap(),
            "/dev/nvme0"
        );
        assert_eq!(
            NvmeDiscovery::extract_controller_path("/dev/nvme1n2").unwrap(),
            "/dev/nvme1"
        );
        assert_eq!(
            NvmeDiscovery::extract_controller_path("/dev/nvme0n1p1").unwrap(),
            "/dev/nvme0"
        );
    }

    #[test]
    fn test_extract_nsid() {
        assert_eq!(NvmeDiscovery::extract_nsid("/dev/nvme0n1").unwrap(), 1);
        assert_eq!(NvmeDiscovery::extract_nsid("/dev/nvme0n2").unwrap(), 2);
        assert_eq!(NvmeDiscovery::extract_nsid("/dev/nvme1n1").unwrap(), 1);
    }
}

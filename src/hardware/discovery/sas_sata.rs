//! SAS/SATA Discovery
//!
//! Provides discovery and SMART data retrieval for SAS and SATA devices
//! using smartctl from smartmontools.

use crate::domain::ports::SmartData;
use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};
use std::process::Command;
use tracing::{debug, warn};

// =============================================================================
// SATA/SAS Device Info
// =============================================================================

/// Extended SATA/SAS device information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SataDeviceInfo {
    /// Device path
    pub device_path: String,
    /// Device model
    pub model: String,
    /// Serial number
    pub serial: String,
    /// Firmware version
    pub firmware: String,
    /// Capacity in bytes
    pub capacity_bytes: u64,
    /// Logical sector size
    pub logical_sector_size: u32,
    /// Physical sector size
    pub physical_sector_size: u32,
    /// Is SSD (vs HDD)
    pub is_ssd: bool,
    /// Rotation rate (0 for SSD, RPM for HDD)
    pub rotation_rate: u32,
    /// Interface type (SATA, SAS)
    pub interface: String,
    /// SMART data
    pub smart: Option<SmartData>,
}

// =============================================================================
// SATA/SAS Discovery
// =============================================================================

/// SATA/SAS-specific discovery operations
pub struct SasSataDiscovery;

impl SasSataDiscovery {
    /// Get device info using smartctl
    pub async fn get_device_info(device: &str) -> Result<SataDeviceInfo> {
        let output = Command::new("smartctl")
            .args(["-i", "-j", device])
            .output()
            .map_err(|e| Error::HardwareDiscovery(format!(
                "smartctl failed for {}: {}",
                device, e
            )))?;

        // smartctl returns non-zero for various warnings, check output
        let json: serde_json::Value = serde_json::from_slice(&output.stdout)
            .map_err(|e| Error::HardwareDiscovery(format!(
                "smartctl JSON parse error for {}: {}",
                device, e
            )))?;

        let model = json["model_name"]
            .as_str()
            .or_else(|| json["model_family"].as_str())
            .unwrap_or("Unknown")
            .to_string();

        let serial = json["serial_number"]
            .as_str()
            .unwrap_or("Unknown")
            .to_string();

        let firmware = json["firmware_version"]
            .as_str()
            .unwrap_or("Unknown")
            .to_string();

        let capacity_bytes = json["user_capacity"]["bytes"]
            .as_u64()
            .unwrap_or(0);

        let logical_sector_size = json["logical_block_size"]
            .as_u64()
            .unwrap_or(512) as u32;

        let physical_sector_size = json["physical_block_size"]
            .as_u64()
            .unwrap_or(512) as u32;

        let rotation_rate = json["rotation_rate"]
            .as_u64()
            .unwrap_or(0) as u32;

        let is_ssd = rotation_rate == 0
            || json["device"]["type"].as_str() == Some("nvme")
            || model.to_lowercase().contains("ssd");

        let interface = json["device"]["protocol"]
            .as_str()
            .or_else(|| json["device"]["type"].as_str())
            .unwrap_or("Unknown")
            .to_string();

        // Get SMART data
        let smart = Self::get_smart_data(device).await.ok();

        Ok(SataDeviceInfo {
            device_path: device.to_string(),
            model,
            serial,
            firmware,
            capacity_bytes,
            logical_sector_size,
            physical_sector_size,
            is_ssd,
            rotation_rate,
            interface,
            smart,
        })
    }

    /// Get SMART data using smartctl
    pub async fn get_smart_data(device: &str) -> Result<SmartData> {
        let output = Command::new("smartctl")
            .args(["-A", "-H", "-j", device])
            .output()
            .map_err(|e| Error::SmartUnavailable {
                device: device.to_string(),
            })?;

        let json: serde_json::Value = serde_json::from_slice(&output.stdout)
            .map_err(|e| Error::SmartUnavailable {
                device: device.to_string(),
            })?;

        // Check if SMART is available
        let smart_status = json["smart_status"]["passed"].as_bool();
        if smart_status.is_none() {
            return Err(Error::SmartUnavailable {
                device: device.to_string(),
            });
        }

        // Parse SMART attributes
        let attrs = &json["ata_smart_attributes"]["table"];

        // Temperature
        let temperature = Self::find_smart_attr(attrs, &[194, 190])
            .map(|v| v as i32)
            .unwrap_or(0);

        // Wear level / percentage used
        let percentage_used = Self::find_smart_attr(attrs, &[177, 231, 233])
            .map(|v| 100u64.saturating_sub(v) as u8)
            .unwrap_or(0);

        // Power on hours
        let power_on_hours = Self::find_smart_attr(attrs, &[9])
            .unwrap_or(0);

        // Data read/written (may not be available for all drives)
        let data_units_read = Self::find_smart_attr(attrs, &[241])
            .unwrap_or(0);
        let data_units_written = Self::find_smart_attr(attrs, &[242])
            .unwrap_or(0);

        // Critical warning - derive from SMART status
        let critical_warning = if smart_status == Some(false) { 1 } else { 0 };

        Ok(SmartData {
            temperature_celsius: temperature,
            percentage_used,
            data_units_read,
            data_units_written,
            power_on_hours,
            critical_warning,
        })
    }

    /// Perform a SMART self-test
    pub async fn run_self_test(device: &str, test_type: &str) -> Result<()> {
        let test_arg = match test_type {
            "short" => "-t short",
            "long" => "-t long",
            "conveyance" => "-t conveyance",
            "offline" => "-t offline",
            _ => return Err(Error::HardwareDiscovery(
                format!("Unknown test type: {}", test_type)
            )),
        };

        let output = Command::new("smartctl")
            .args([test_arg, device])
            .output()
            .map_err(|e| Error::HardwareDiscovery(format!(
                "Failed to start SMART test on {}: {}",
                device, e
            )))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Error::HardwareDiscovery(format!(
                "SMART test failed on {}: {}",
                device, stderr
            )));
        }

        Ok(())
    }

    /// Check if smartmontools is available
    pub fn is_smartctl_available() -> bool {
        Command::new("smartctl")
            .arg("--version")
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false)
    }

    /// List all SATA/SAS devices
    pub async fn list_devices() -> Result<Vec<String>> {
        let output = Command::new("smartctl")
            .args(["--scan", "-j"])
            .output()
            .map_err(|e| Error::HardwareDiscovery(format!(
                "smartctl scan failed: {}",
                e
            )))?;

        let json: serde_json::Value = serde_json::from_slice(&output.stdout)
            .unwrap_or(serde_json::json!({"devices": []}));

        let devices: Vec<String> = json["devices"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|d| d["name"].as_str().map(|s| s.to_string()))
                    .filter(|name| !name.starts_with("/dev/nvme")) // Exclude NVMe
                    .collect()
            })
            .unwrap_or_default();

        Ok(devices)
    }

    // Helper to find SMART attribute by ID
    fn find_smart_attr(attrs: &serde_json::Value, ids: &[u32]) -> Option<u64> {
        if let Some(arr) = attrs.as_array() {
            for attr in arr {
                if let Some(id) = attr["id"].as_u64() {
                    if ids.contains(&(id as u32)) {
                        return attr["raw"]["value"].as_u64();
                    }
                }
            }
        }
        None
    }
}

/// Determine drive interface from sysfs
pub fn detect_interface(sysfs_path: &std::path::Path) -> String {
    // Check for SCSI transport
    let transport_path = sysfs_path.join("device/transport");
    if let Ok(transport) = std::fs::read_to_string(&transport_path) {
        return transport.trim().to_string();
    }

    // Check host type
    let host_path = sysfs_path.join("device/host");
    if host_path.exists() {
        // Try to read host type
        if let Ok(entries) = std::fs::read_dir(&host_path) {
            for entry in entries.flatten() {
                let name = entry.file_name().to_string_lossy().to_string();
                if name.contains("sas") {
                    return "SAS".to_string();
                } else if name.contains("ata") {
                    return "SATA".to_string();
                }
            }
        }
    }

    // Default based on device name
    let device_name = sysfs_path.file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_default();

    if device_name.starts_with("sd") {
        "SATA".to_string()
    } else {
        "Unknown".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_find_smart_attr() {
        let attrs = serde_json::json!([
            {"id": 194, "raw": {"value": 42}},
            {"id": 9, "raw": {"value": 12345}},
        ]);

        assert_eq!(SasSataDiscovery::find_smart_attr(&attrs, &[194]), Some(42));
        assert_eq!(SasSataDiscovery::find_smart_attr(&attrs, &[9]), Some(12345));
        assert_eq!(SasSataDiscovery::find_smart_attr(&attrs, &[999]), None);
        assert_eq!(SasSataDiscovery::find_smart_attr(&attrs, &[190, 194]), Some(42));
    }
}

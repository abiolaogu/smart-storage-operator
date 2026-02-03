//! Block Device Scanner
//!
//! Enumerates block devices from sysfs and determines their type
//! (NVMe, SSD, HDD) for further classification.

use crate::domain::ports::{DriveInfo, DriveType, NodeHardwareInfo, NvmeNamespaceInfo, SmartData};
use crate::error::{Error, Result};
use chrono::Utc;
use std::fs;
use std::path::{Path, PathBuf};
use tracing::{debug, info, warn};

// =============================================================================
// Constants
// =============================================================================

const SYSFS_BLOCK: &str = "/sys/class/block";
const SYSFS_NVME: &str = "/sys/class/nvme";

// =============================================================================
// Scanner Configuration
// =============================================================================

/// Configuration for the hardware scanner
#[derive(Debug, Clone)]
pub struct ScannerConfig {
    /// Include loopback devices
    pub include_loopback: bool,
    /// Include RAM disks
    pub include_ram: bool,
    /// Include device mapper devices
    pub include_dm: bool,
    /// Minimum device size to include (bytes)
    pub min_size_bytes: u64,
    /// Path to sysfs (for testing)
    pub sysfs_path: PathBuf,
}

impl Default for ScannerConfig {
    fn default() -> Self {
        Self {
            include_loopback: false,
            include_ram: false,
            include_dm: false,
            min_size_bytes: 1_000_000_000, // 1GB minimum
            sysfs_path: PathBuf::from("/sys"),
        }
    }
}

// =============================================================================
// Hardware Scanner
// =============================================================================

/// Scans for storage hardware on Linux systems
pub struct HardwareScanner {
    config: ScannerConfig,
}

impl HardwareScanner {
    /// Create a new hardware scanner
    pub fn new(config: ScannerConfig) -> Self {
        Self { config }
    }

    /// Create a scanner with default configuration
    pub fn default_scanner() -> Self {
        Self::new(ScannerConfig::default())
    }

    /// Discover all storage hardware on the local node
    pub async fn discover(&self) -> Result<NodeHardwareInfo> {
        let hostname = self.get_hostname()?;
        let node_id = hostname.clone();

        info!("Starting hardware discovery on {}", hostname);

        let mut drives = Vec::new();

        // Discover NVMe devices
        match self.discover_nvme_devices().await {
            Ok(nvme_drives) => {
                info!("Found {} NVMe devices", nvme_drives.len());
                drives.extend(nvme_drives);
            }
            Err(e) => {
                warn!("NVMe discovery failed: {}", e);
            }
        }

        // Discover SATA/SAS devices
        match self.discover_block_devices().await {
            Ok(block_drives) => {
                info!("Found {} SATA/SAS devices", block_drives.len());
                drives.extend(block_drives);
            }
            Err(e) => {
                warn!("Block device discovery failed: {}", e);
            }
        }

        // Get system info
        let (memory_bytes, cpu_count) = self.get_system_info();

        Ok(NodeHardwareInfo {
            node_id,
            hostname,
            drives,
            memory_bytes,
            cpu_count,
            discovered_at: Utc::now(),
        })
    }

    /// Discover NVMe devices
    async fn discover_nvme_devices(&self) -> Result<Vec<DriveInfo>> {
        let nvme_path = self.config.sysfs_path.join("class/nvme");
        if !nvme_path.exists() {
            debug!("No NVMe sysfs path found at {:?}", nvme_path);
            return Ok(Vec::new());
        }

        let mut drives = Vec::new();

        for entry in fs::read_dir(&nvme_path)? {
            let entry = entry?;
            let controller_name = entry.file_name().to_string_lossy().to_string();

            if !controller_name.starts_with("nvme") {
                continue;
            }

            // Find namespaces for this controller
            let block_path = self.config.sysfs_path.join("class/block");
            for ns_entry in fs::read_dir(&block_path)? {
                let ns_entry = ns_entry?;
                let ns_name = ns_entry.file_name().to_string_lossy().to_string();

                // Match nvme0n1, nvme0n2, etc.
                if ns_name.starts_with(&controller_name) && ns_name.contains('n') {
                    let parts: Vec<&str> = ns_name.split('n').collect();
                    if parts.len() >= 2 && !parts[1].contains('p') {
                        // Skip partitions
                        if let Ok(drive) = self.scan_nvme_namespace(&ns_entry.path()).await {
                            if drive.capacity_bytes >= self.config.min_size_bytes {
                                drives.push(drive);
                            }
                        }
                    }
                }
            }
        }

        Ok(drives)
    }

    /// Scan a specific NVMe namespace
    async fn scan_nvme_namespace(&self, sysfs_path: &Path) -> Result<DriveInfo> {
        let device_name = sysfs_path
            .file_name()
            .ok_or_else(|| Error::HardwareDiscovery("Invalid sysfs path".into()))?
            .to_string_lossy()
            .to_string();

        let device_path = format!("/dev/{}", device_name);

        // Read device info from sysfs
        let model = self.read_sysfs_attr(sysfs_path, "device/model")?;
        let serial = self.read_sysfs_attr(sysfs_path, "device/serial")?;
        let firmware = self.read_sysfs_attr(sysfs_path, "device/firmware_rev")
            .unwrap_or_else(|_| "unknown".to_string());

        // Read size (in 512-byte sectors)
        let size_str = self.read_sysfs_attr(sysfs_path, "size")?;
        let sectors: u64 = size_str.trim().parse().map_err(|_| {
            Error::HardwareDiscovery(format!("Invalid size: {}", size_str))
        })?;
        let capacity_bytes = sectors * 512;

        // Read block size
        let block_size_str = self.read_sysfs_attr(sysfs_path, "queue/logical_block_size")
            .unwrap_or_else(|_| "512".to_string());
        let block_size: u32 = block_size_str.trim().parse().unwrap_or(512);

        // Check for ZNS support
        let zns_supported = self.check_zns_support(sysfs_path);

        // Parse namespace ID from device name (nvme0n1 -> nsid 1)
        let nsid = self.parse_nsid(&device_name).unwrap_or(1);

        let namespaces = vec![NvmeNamespaceInfo {
            nsid,
            capacity_bytes,
            active: true,
            is_zns: zns_supported,
        }];

        Ok(DriveInfo {
            device_path,
            device_id: device_name,
            drive_type: DriveType::Nvme,
            model: model.trim().to_string(),
            serial: serial.trim().to_string(),
            firmware: firmware.trim().to_string(),
            capacity_bytes,
            block_size,
            zns_supported,
            nvme_namespaces: namespaces,
            smart_data: None, // Filled by separate SMART query
        })
    }

    /// Discover non-NVMe block devices (SATA/SAS)
    async fn discover_block_devices(&self) -> Result<Vec<DriveInfo>> {
        let block_path = self.config.sysfs_path.join("class/block");
        if !block_path.exists() {
            return Err(Error::HardwareDiscovery(
                "Block device sysfs not found".into(),
            ));
        }

        let mut drives = Vec::new();

        for entry in fs::read_dir(&block_path)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().to_string();

            // Skip devices we don't want
            if !self.should_include_device(&name) {
                continue;
            }

            // Skip NVMe devices (handled separately)
            if name.starts_with("nvme") {
                continue;
            }

            // Skip partitions
            if self.is_partition(&entry.path()) {
                continue;
            }

            if let Ok(drive) = self.scan_block_device(&entry.path()).await {
                if drive.capacity_bytes >= self.config.min_size_bytes {
                    drives.push(drive);
                }
            }
        }

        Ok(drives)
    }

    /// Scan a SATA/SAS block device
    async fn scan_block_device(&self, sysfs_path: &Path) -> Result<DriveInfo> {
        let device_name = sysfs_path
            .file_name()
            .ok_or_else(|| Error::HardwareDiscovery("Invalid sysfs path".into()))?
            .to_string_lossy()
            .to_string();

        let device_path = format!("/dev/{}", device_name);

        // Read device info
        let model = self.read_sysfs_attr(sysfs_path, "device/model")
            .or_else(|_| self.read_sysfs_attr(sysfs_path, "device/name"))
            .unwrap_or_else(|_| "Unknown".to_string());

        let serial = self.read_sysfs_attr(sysfs_path, "device/serial")
            .unwrap_or_else(|_| "Unknown".to_string());

        let firmware = self.read_sysfs_attr(sysfs_path, "device/firmware")
            .or_else(|_| self.read_sysfs_attr(sysfs_path, "device/rev"))
            .unwrap_or_else(|_| "Unknown".to_string());

        // Read size
        let size_str = self.read_sysfs_attr(sysfs_path, "size")?;
        let sectors: u64 = size_str.trim().parse().map_err(|_| {
            Error::HardwareDiscovery(format!("Invalid size: {}", size_str))
        })?;
        let capacity_bytes = sectors * 512;

        // Read block size
        let block_size_str = self.read_sysfs_attr(sysfs_path, "queue/logical_block_size")
            .unwrap_or_else(|_| "512".to_string());
        let block_size: u32 = block_size_str.trim().parse().unwrap_or(512);

        // Determine drive type (SSD vs HDD)
        let drive_type = self.detect_drive_type(sysfs_path);

        Ok(DriveInfo {
            device_path,
            device_id: device_name,
            drive_type,
            model: model.trim().to_string(),
            serial: serial.trim().to_string(),
            firmware: firmware.trim().to_string(),
            capacity_bytes,
            block_size,
            zns_supported: false,
            nvme_namespaces: Vec::new(),
            smart_data: None,
        })
    }

    /// Detect whether a device is SSD or HDD
    fn detect_drive_type(&self, sysfs_path: &Path) -> DriveType {
        // Check rotational flag
        if let Ok(rotational) = self.read_sysfs_attr(sysfs_path, "queue/rotational") {
            if rotational.trim() == "0" {
                return DriveType::Ssd;
            } else if rotational.trim() == "1" {
                return DriveType::Hdd;
            }
        }

        DriveType::Unknown
    }

    /// Check if ZNS is supported
    fn check_zns_support(&self, sysfs_path: &Path) -> bool {
        // Check for zoned model
        if let Ok(zoned) = self.read_sysfs_attr(sysfs_path, "queue/zoned") {
            return zoned.trim() == "host-managed" || zoned.trim() == "host-aware";
        }
        false
    }

    /// Parse namespace ID from device name
    fn parse_nsid(&self, name: &str) -> Option<u32> {
        // nvme0n1 -> 1, nvme1n2 -> 2
        if let Some(idx) = name.rfind('n') {
            let ns_part = &name[idx + 1..];
            // Stop at 'p' for partitions
            let ns_num = ns_part.split('p').next()?;
            return ns_num.parse().ok();
        }
        None
    }

    /// Check if a path is a partition
    fn is_partition(&self, sysfs_path: &Path) -> bool {
        // Partitions have a "partition" file
        sysfs_path.join("partition").exists()
    }

    /// Check if a device should be included
    fn should_include_device(&self, name: &str) -> bool {
        // Skip loopback devices
        if !self.config.include_loopback && name.starts_with("loop") {
            return false;
        }

        // Skip RAM disks
        if !self.config.include_ram && name.starts_with("ram") {
            return false;
        }

        // Skip device mapper
        if !self.config.include_dm && name.starts_with("dm-") {
            return false;
        }

        // Skip md RAID devices
        if name.starts_with("md") {
            return false;
        }

        // Skip zram
        if name.starts_with("zram") {
            return false;
        }

        true
    }

    /// Read a sysfs attribute
    fn read_sysfs_attr(&self, base_path: &Path, attr: &str) -> Result<String> {
        let path = base_path.join(attr);
        fs::read_to_string(&path).map_err(|e| {
            Error::HardwareDiscovery(format!(
                "Failed to read {}: {}",
                path.display(),
                e
            ))
        })
    }

    /// Get the system hostname
    fn get_hostname(&self) -> Result<String> {
        // Try /etc/hostname first
        if let Ok(hostname) = fs::read_to_string("/etc/hostname") {
            return Ok(hostname.trim().to_string());
        }

        // Fall back to hostname command
        #[cfg(unix)]
        {
            use std::process::Command;
            if let Ok(output) = Command::new("hostname").output() {
                if output.status.success() {
                    return Ok(String::from_utf8_lossy(&output.stdout).trim().to_string());
                }
            }
        }

        Ok("unknown".to_string())
    }

    /// Get system memory and CPU count
    fn get_system_info(&self) -> (u64, u32) {
        let memory_bytes = self.get_total_memory().unwrap_or(0);
        let cpu_count = self.get_cpu_count().unwrap_or(1);
        (memory_bytes, cpu_count)
    }

    /// Get total system memory from /proc/meminfo
    fn get_total_memory(&self) -> Option<u64> {
        let meminfo = fs::read_to_string("/proc/meminfo").ok()?;
        for line in meminfo.lines() {
            if line.starts_with("MemTotal:") {
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() >= 2 {
                    let kb: u64 = parts[1].parse().ok()?;
                    return Some(kb * 1024);
                }
            }
        }
        None
    }

    /// Get CPU count
    fn get_cpu_count(&self) -> Option<u32> {
        // Use std::thread::available_parallelism for portability
        std::thread::available_parallelism()
            .ok()
            .map(|p| p.get() as u32)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_nsid() {
        let scanner = HardwareScanner::default_scanner();

        assert_eq!(scanner.parse_nsid("nvme0n1"), Some(1));
        assert_eq!(scanner.parse_nsid("nvme0n2"), Some(2));
        assert_eq!(scanner.parse_nsid("nvme1n1"), Some(1));
        assert_eq!(scanner.parse_nsid("nvme0n1p1"), Some(1)); // Partition
        assert_eq!(scanner.parse_nsid("sda"), None);
    }

    #[test]
    fn test_should_include_device() {
        let scanner = HardwareScanner::default_scanner();

        assert!(scanner.should_include_device("sda"));
        assert!(scanner.should_include_device("nvme0n1"));
        assert!(!scanner.should_include_device("loop0"));
        assert!(!scanner.should_include_device("ram0"));
        assert!(!scanner.should_include_device("dm-0"));
        assert!(!scanner.should_include_device("md0"));
    }

    #[test]
    fn test_scanner_config_defaults() {
        let config = ScannerConfig::default();

        assert!(!config.include_loopback);
        assert!(!config.include_ram);
        assert!(!config.include_dm);
        assert_eq!(config.min_size_bytes, 1_000_000_000);
    }
}

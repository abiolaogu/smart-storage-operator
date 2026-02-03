//! Device Classifier
//!
//! Classifies storage devices into performance tiers based on
//! device characteristics, model fingerprints, and observed metrics.

use crate::crd::{CapacityTier, DriveTier, WorkloadSuitability};
use crate::domain::ports::{DriveInfo, DriveType};
use crate::error::Result;
use serde::{Deserialize, Serialize};
use tracing::debug;

// =============================================================================
// Classification Result
// =============================================================================

/// Complete classification result for a drive
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeviceClassification {
    /// Performance tier
    pub performance: DriveTier,
    /// Capacity tier
    pub capacity: CapacityTier,
    /// Workload suitability
    pub workload: WorkloadSuitability,
    /// Suitable storage backends
    pub suitable_for: Vec<String>,
    /// Classification confidence (0.0 - 1.0)
    pub confidence: f32,
    /// Classification reason/explanation
    pub reason: String,
}

impl DeviceClassification {
    /// Get a confidence score as percentage (0-100)
    pub fn confidence_percent(&self) -> u32 {
        (self.confidence * 100.0) as u32
    }
}

// =============================================================================
// Classifier Configuration
// =============================================================================

/// Configuration for the device classifier
#[derive(Debug, Clone)]
pub struct ClassifierConfig {
    /// Capacity thresholds
    pub small_capacity_bytes: u64,   // < this is Small
    pub large_capacity_bytes: u64,   // >= this is Large, between is Medium

    /// Performance score weights
    pub nvme_base_score: u32,
    pub ssd_base_score: u32,
    pub hdd_base_score: u32,

    /// ZNS bonus score
    pub zns_bonus: u32,

    /// Known high-performance models (substring match)
    pub high_perf_models: Vec<String>,

    /// Known enterprise models (substring match)
    pub enterprise_models: Vec<String>,
}

impl Default for ClassifierConfig {
    fn default() -> Self {
        Self {
            small_capacity_bytes: 1_000_000_000_000, // 1 TB
            large_capacity_bytes: 10_000_000_000_000, // 10 TB

            nvme_base_score: 80,
            ssd_base_score: 60,
            hdd_base_score: 30,

            zns_bonus: 10,

            high_perf_models: vec![
                "Optane".to_string(),
                "P5800".to_string(),
                "P5510".to_string(),
                "PM1733".to_string(),
                "980 PRO".to_string(),
                "SN850".to_string(),
            ],

            enterprise_models: vec![
                "PM1733".to_string(),
                "PM1735".to_string(),
                "P5510".to_string(),
                "P5520".to_string(),
                "Ultrastar".to_string(),
                "HGST".to_string(),
                "Micron".to_string(),
            ],
        }
    }
}

// =============================================================================
// Device Classifier
// =============================================================================

/// Classifies storage devices into tiers
pub struct DeviceClassifier {
    config: ClassifierConfig,
}

impl DeviceClassifier {
    /// Create a new classifier with default config
    pub fn new() -> Self {
        Self {
            config: ClassifierConfig::default(),
        }
    }

    /// Create a classifier with custom config
    pub fn with_config(config: ClassifierConfig) -> Self {
        Self { config }
    }

    /// Classify a drive
    pub fn classify(&self, drive: &DriveInfo) -> DeviceClassification {
        let performance = self.classify_performance(drive);
        let capacity = self.classify_capacity(drive.capacity_bytes);
        let workload = self.classify_workload(drive, &performance, &capacity);
        let suitable_for = self.determine_suitable_backends(drive, &performance, &workload);
        let (confidence, reason) = self.calculate_confidence(drive, &performance);

        DeviceClassification {
            performance,
            capacity,
            workload,
            suitable_for,
            confidence,
            reason,
        }
    }

    /// Classify performance tier
    fn classify_performance(&self, drive: &DriveInfo) -> DriveTier {
        // Check for ultra-fast devices (Optane, PMem)
        if self.is_ultra_fast(drive) {
            return DriveTier::UltraFast;
        }

        // Classify by drive type
        match drive.drive_type {
            DriveType::Nvme => {
                // High-performance NVMe
                if self.is_high_perf_model(&drive.model) {
                    DriveTier::UltraFast
                } else {
                    DriveTier::FastNvme
                }
            }
            DriveType::Ssd => DriveTier::StandardSsd,
            DriveType::Hdd => DriveTier::Hdd,
            DriveType::Unknown => {
                // Try to infer from model
                if drive.model.to_lowercase().contains("ssd") {
                    DriveTier::StandardSsd
                } else {
                    DriveTier::Hdd
                }
            }
        }
    }

    /// Classify capacity tier
    fn classify_capacity(&self, capacity_bytes: u64) -> CapacityTier {
        if capacity_bytes < self.config.small_capacity_bytes {
            CapacityTier::Small
        } else if capacity_bytes >= self.config.large_capacity_bytes {
            CapacityTier::Large
        } else {
            CapacityTier::Medium
        }
    }

    /// Determine workload suitability
    fn classify_workload(
        &self,
        drive: &DriveInfo,
        performance: &DriveTier,
        capacity: &CapacityTier,
    ) -> WorkloadSuitability {
        // ZNS drives are object-optimized
        if drive.zns_supported {
            return WorkloadSuitability::ObjectOptimized;
        }

        // High-capacity HDDs are object-optimized
        if matches!(performance, DriveTier::Hdd) && matches!(capacity, CapacityTier::Large) {
            return WorkloadSuitability::ObjectOptimized;
        }

        // High-performance NVMe is block-optimized
        if matches!(performance, DriveTier::UltraFast | DriveTier::FastNvme)
            && !matches!(capacity, CapacityTier::Large)
        {
            return WorkloadSuitability::BlockOptimized;
        }

        // Default to mixed workload
        WorkloadSuitability::Mixed
    }

    /// Determine suitable storage backends
    fn determine_suitable_backends(
        &self,
        drive: &DriveInfo,
        performance: &DriveTier,
        workload: &WorkloadSuitability,
    ) -> Vec<String> {
        let mut backends = Vec::new();

        match workload {
            WorkloadSuitability::BlockOptimized => {
                backends.push("block".to_string());
                backends.push("cache".to_string());
                if !matches!(performance, DriveTier::UltraFast) {
                    backends.push("file".to_string());
                }
            }
            WorkloadSuitability::ObjectOptimized => {
                backends.push("object".to_string());
                backends.push("archive".to_string());
                if drive.zns_supported {
                    backends.push("zns-object".to_string());
                }
            }
            WorkloadSuitability::Mixed => {
                backends.push("block".to_string());
                backends.push("file".to_string());
                backends.push("object".to_string());
            }
        }

        backends
    }

    /// Calculate classification confidence
    fn calculate_confidence(&self, drive: &DriveInfo, performance: &DriveTier) -> (f32, String) {
        let mut confidence: f32 = 0.7; // Base confidence
        let mut reasons = Vec::new();

        // NVMe detection is reliable
        if drive.drive_type == DriveType::Nvme {
            confidence += 0.15;
            reasons.push("NVMe detected via sysfs");
        }

        // Known model increases confidence
        if self.is_high_perf_model(&drive.model) || self.is_enterprise_model(&drive.model) {
            confidence += 0.1;
            reasons.push("known model fingerprint matched");
        }

        // SMART data available increases confidence
        if drive.smart_data.is_some() {
            confidence += 0.05;
            reasons.push("SMART data available");
        }

        // ZNS detection is definitive
        if drive.zns_supported {
            confidence = confidence.max(0.95);
            reasons.push("ZNS support confirmed");
        }

        let reason = if reasons.is_empty() {
            "basic classification from device type".to_string()
        } else {
            reasons.join("; ")
        };

        (confidence.min(1.0), reason)
    }

    /// Check if drive is ultra-fast (Optane, PMem, etc.)
    fn is_ultra_fast(&self, drive: &DriveInfo) -> bool {
        let model_lower = drive.model.to_lowercase();

        // Intel Optane
        if model_lower.contains("optane") {
            return true;
        }

        // Intel Persistent Memory
        if model_lower.contains("pmem") || model_lower.contains("persistent memory") {
            return true;
        }

        // Check known high-perf models
        self.is_high_perf_model(&drive.model)
            && drive.drive_type == DriveType::Nvme
            && self.is_enterprise_model(&drive.model)
    }

    /// Check if model is a known high-performance model
    fn is_high_perf_model(&self, model: &str) -> bool {
        let model_upper = model.to_uppercase();
        self.config
            .high_perf_models
            .iter()
            .any(|m| model_upper.contains(&m.to_uppercase()))
    }

    /// Check if model is a known enterprise model
    fn is_enterprise_model(&self, model: &str) -> bool {
        let model_upper = model.to_uppercase();
        self.config
            .enterprise_models
            .iter()
            .any(|m| model_upper.contains(&m.to_uppercase()))
    }

    /// Get a numerical score for a drive (0-100)
    pub fn calculate_score(&self, drive: &DriveInfo) -> u32 {
        let mut score = match drive.drive_type {
            DriveType::Nvme => self.config.nvme_base_score,
            DriveType::Ssd => self.config.ssd_base_score,
            DriveType::Hdd => self.config.hdd_base_score,
            DriveType::Unknown => 20,
        };

        // ZNS bonus
        if drive.zns_supported {
            score += self.config.zns_bonus;
        }

        // High-performance model bonus
        if self.is_high_perf_model(&drive.model) {
            score += 10;
        }

        // Enterprise model bonus
        if self.is_enterprise_model(&drive.model) {
            score += 5;
        }

        // Health penalty (if SMART data available)
        if let Some(smart) = &drive.smart_data {
            if smart.critical_warning != 0 {
                score = score.saturating_sub(20);
            }
            if smart.percentage_used > 80 {
                score = score.saturating_sub(10);
            }
        }

        score.min(100)
    }
}

impl Default for DeviceClassifier {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::ports::SmartData;

    fn make_nvme_drive(model: &str, capacity: u64) -> DriveInfo {
        DriveInfo {
            device_path: "/dev/nvme0n1".to_string(),
            device_id: "nvme0n1".to_string(),
            drive_type: DriveType::Nvme,
            model: model.to_string(),
            serial: "TEST123".to_string(),
            firmware: "1.0".to_string(),
            capacity_bytes: capacity,
            block_size: 4096,
            zns_supported: false,
            nvme_namespaces: vec![],
            smart_data: None,
        }
    }

    fn make_hdd_drive(capacity: u64) -> DriveInfo {
        DriveInfo {
            device_path: "/dev/sda".to_string(),
            device_id: "sda".to_string(),
            drive_type: DriveType::Hdd,
            model: "WD Red 18TB".to_string(),
            serial: "WD123".to_string(),
            firmware: "1.0".to_string(),
            capacity_bytes: capacity,
            block_size: 4096,
            zns_supported: false,
            nvme_namespaces: vec![],
            smart_data: None,
        }
    }

    #[test]
    fn test_classify_nvme() {
        let classifier = DeviceClassifier::new();
        let drive = make_nvme_drive("Samsung 980 PRO 2TB", 2_000_000_000_000);

        let result = classifier.classify(&drive);

        assert!(matches!(result.performance, DriveTier::UltraFast | DriveTier::FastNvme));
        assert_eq!(result.capacity, CapacityTier::Medium);
        assert!(result.suitable_for.contains(&"block".to_string()));
    }

    #[test]
    fn test_classify_hdd() {
        let classifier = DeviceClassifier::new();
        let drive = make_hdd_drive(18_000_000_000_000); // 18TB

        let result = classifier.classify(&drive);

        assert_eq!(result.performance, DriveTier::Hdd);
        assert_eq!(result.capacity, CapacityTier::Large);
        assert_eq!(result.workload, WorkloadSuitability::ObjectOptimized);
        assert!(result.suitable_for.contains(&"object".to_string()));
    }

    #[test]
    fn test_classify_zns() {
        let classifier = DeviceClassifier::new();
        let mut drive = make_nvme_drive("WD Ultrastar DC ZN540", 8_000_000_000_000);
        drive.zns_supported = true;

        let result = classifier.classify(&drive);

        assert_eq!(result.workload, WorkloadSuitability::ObjectOptimized);
        assert!(result.suitable_for.contains(&"zns-object".to_string()));
        assert!(result.confidence >= 0.95);
    }

    #[test]
    fn test_capacity_tiers() {
        let classifier = DeviceClassifier::new();

        assert_eq!(
            classifier.classify_capacity(500_000_000_000), // 500GB
            CapacityTier::Small
        );
        assert_eq!(
            classifier.classify_capacity(4_000_000_000_000), // 4TB
            CapacityTier::Medium
        );
        assert_eq!(
            classifier.classify_capacity(20_000_000_000_000), // 20TB
            CapacityTier::Large
        );
    }

    #[test]
    fn test_calculate_score() {
        let classifier = DeviceClassifier::new();

        let nvme = make_nvme_drive("Generic NVMe", 1_000_000_000_000);
        assert!(classifier.calculate_score(&nvme) >= 80);

        let hdd = make_hdd_drive(4_000_000_000_000);
        assert!(classifier.calculate_score(&hdd) <= 40);
    }
}

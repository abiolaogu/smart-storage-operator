//! Model Fingerprinting
//!
//! Identifies drive models from known fingerprints to provide
//! accurate performance expectations without benchmarking.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// =============================================================================
// Drive Fingerprint
// =============================================================================

/// Known performance characteristics for a drive model
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DriveFingerprint {
    /// Model name pattern (substring match)
    pub model_pattern: String,
    /// Vendor name
    pub vendor: String,
    /// Expected sequential read IOPS
    pub seq_read_iops: Option<u64>,
    /// Expected sequential write IOPS
    pub seq_write_iops: Option<u64>,
    /// Expected random read IOPS (4K)
    pub rand_read_iops: Option<u64>,
    /// Expected random write IOPS (4K)
    pub rand_write_iops: Option<u64>,
    /// Expected sequential read throughput (MB/s)
    pub seq_read_mbps: Option<u32>,
    /// Expected sequential write throughput (MB/s)
    pub seq_write_mbps: Option<u32>,
    /// TBW (Total Bytes Written) rating in TB
    pub tbw_tb: Option<u32>,
    /// DWPD (Drive Writes Per Day) rating
    pub dwpd: Option<f32>,
    /// Is enterprise grade
    pub enterprise: bool,
    /// Supports ZNS
    pub zns: bool,
    /// Generation/year
    pub generation: Option<String>,
    /// Notes
    pub notes: Option<String>,
}

impl DriveFingerprint {
    /// Check if a model string matches this fingerprint
    pub fn matches(&self, model: &str) -> bool {
        let model_upper = model.to_uppercase();
        let pattern_upper = self.model_pattern.to_uppercase();
        model_upper.contains(&pattern_upper)
    }

    /// Get an overall performance score (0-100)
    pub fn performance_score(&self) -> u32 {
        // Base score on random read IOPS (most indicative of real workloads)
        let iops_score = self.rand_read_iops.map(|iops| {
            if iops >= 1_000_000 { 100 }
            else if iops >= 500_000 { 90 }
            else if iops >= 200_000 { 80 }
            else if iops >= 100_000 { 70 }
            else if iops >= 50_000 { 60 }
            else if iops >= 10_000 { 40 }
            else { 30 }
        }).unwrap_or(50);

        // Adjust for enterprise grade
        let enterprise_bonus = if self.enterprise { 5 } else { 0 };

        (iops_score + enterprise_bonus).min(100)
    }

    /// Get endurance score (0-100)
    pub fn endurance_score(&self) -> u32 {
        // Score based on DWPD
        self.dwpd.map(|dwpd| {
            if dwpd >= 3.0 { 100 }
            else if dwpd >= 1.0 { 80 }
            else if dwpd >= 0.5 { 60 }
            else if dwpd >= 0.3 { 40 }
            else { 30 }
        }).unwrap_or(50)
    }
}

// =============================================================================
// Fingerprint Database
// =============================================================================

/// Database of known drive fingerprints
pub struct FingerprintDatabase {
    fingerprints: Vec<DriveFingerprint>,
}

impl FingerprintDatabase {
    /// Create a new database with built-in fingerprints
    pub fn new() -> Self {
        let mut db = Self {
            fingerprints: Vec::new(),
        };
        db.load_builtin_fingerprints();
        db
    }

    /// Look up a drive by model
    pub fn lookup(&self, model: &str) -> Option<&DriveFingerprint> {
        self.fingerprints.iter().find(|fp| fp.matches(model))
    }

    /// Look up all matching fingerprints
    pub fn lookup_all(&self, model: &str) -> Vec<&DriveFingerprint> {
        self.fingerprints.iter().filter(|fp| fp.matches(model)).collect()
    }

    /// Add a custom fingerprint
    pub fn add(&mut self, fingerprint: DriveFingerprint) {
        self.fingerprints.push(fingerprint);
    }

    /// Load built-in fingerprints for common enterprise and consumer drives
    fn load_builtin_fingerprints(&mut self) {
        // Intel Optane
        self.fingerprints.push(DriveFingerprint {
            model_pattern: "Optane P5800".to_string(),
            vendor: "Intel".to_string(),
            seq_read_iops: None,
            seq_write_iops: None,
            rand_read_iops: Some(1_500_000),
            rand_write_iops: Some(1_500_000),
            seq_read_mbps: Some(7200),
            seq_write_mbps: Some(5400),
            tbw_tb: Some(100000),
            dwpd: Some(100.0),
            enterprise: true,
            zns: false,
            generation: Some("2021".to_string()),
            notes: Some("Intel Optane SSD DC P5800X".to_string()),
        });

        self.fingerprints.push(DriveFingerprint {
            model_pattern: "Optane P5510".to_string(),
            vendor: "Intel".to_string(),
            seq_read_iops: None,
            seq_write_iops: None,
            rand_read_iops: Some(550_000),
            rand_write_iops: Some(250_000),
            seq_read_mbps: Some(6200),
            seq_write_mbps: Some(3000),
            tbw_tb: Some(27000),
            dwpd: Some(3.0),
            enterprise: true,
            zns: false,
            generation: Some("2020".to_string()),
            notes: Some("Intel Optane SSD DC P5510".to_string()),
        });

        // Samsung Enterprise NVMe
        self.fingerprints.push(DriveFingerprint {
            model_pattern: "PM1733".to_string(),
            vendor: "Samsung".to_string(),
            seq_read_iops: None,
            seq_write_iops: None,
            rand_read_iops: Some(1_500_000),
            rand_write_iops: Some(250_000),
            seq_read_mbps: Some(7000),
            seq_write_mbps: Some(3800),
            tbw_tb: Some(27000),
            dwpd: Some(1.0),
            enterprise: true,
            zns: false,
            generation: Some("2020".to_string()),
            notes: Some("Samsung PM1733 Enterprise NVMe".to_string()),
        });

        self.fingerprints.push(DriveFingerprint {
            model_pattern: "PM1735".to_string(),
            vendor: "Samsung".to_string(),
            seq_read_iops: None,
            seq_write_iops: None,
            rand_read_iops: Some(1_400_000),
            rand_write_iops: Some(330_000),
            seq_read_mbps: Some(7000),
            seq_write_mbps: Some(3800),
            tbw_tb: Some(41000),
            dwpd: Some(3.0),
            enterprise: true,
            zns: false,
            generation: Some("2020".to_string()),
            notes: Some("Samsung PM1735 High-Endurance".to_string()),
        });

        // Samsung Consumer NVMe
        self.fingerprints.push(DriveFingerprint {
            model_pattern: "980 PRO".to_string(),
            vendor: "Samsung".to_string(),
            seq_read_iops: None,
            seq_write_iops: None,
            rand_read_iops: Some(1_000_000),
            rand_write_iops: Some(1_000_000),
            seq_read_mbps: Some(7000),
            seq_write_mbps: Some(5100),
            tbw_tb: Some(600),
            dwpd: Some(0.3),
            enterprise: false,
            zns: false,
            generation: Some("2020".to_string()),
            notes: Some("Samsung 980 PRO Consumer NVMe".to_string()),
        });

        self.fingerprints.push(DriveFingerprint {
            model_pattern: "990 PRO".to_string(),
            vendor: "Samsung".to_string(),
            seq_read_iops: None,
            seq_write_iops: None,
            rand_read_iops: Some(1_200_000),
            rand_write_iops: Some(1_550_000),
            seq_read_mbps: Some(7450),
            seq_write_mbps: Some(6900),
            tbw_tb: Some(600),
            dwpd: Some(0.3),
            enterprise: false,
            zns: false,
            generation: Some("2022".to_string()),
            notes: Some("Samsung 990 PRO Consumer NVMe".to_string()),
        });

        // WD Enterprise NVMe
        self.fingerprints.push(DriveFingerprint {
            model_pattern: "SN850".to_string(),
            vendor: "Western Digital".to_string(),
            seq_read_iops: None,
            seq_write_iops: None,
            rand_read_iops: Some(1_000_000),
            rand_write_iops: Some(720_000),
            seq_read_mbps: Some(7000),
            seq_write_mbps: Some(5300),
            tbw_tb: Some(600),
            dwpd: Some(0.3),
            enterprise: false,
            zns: false,
            generation: Some("2020".to_string()),
            notes: Some("WD Black SN850".to_string()),
        });

        // WD ZNS Drives
        self.fingerprints.push(DriveFingerprint {
            model_pattern: "ZN540".to_string(),
            vendor: "Western Digital".to_string(),
            seq_read_iops: None,
            seq_write_iops: None,
            rand_read_iops: Some(350_000),
            rand_write_iops: Some(50_000),
            seq_read_mbps: Some(3100),
            seq_write_mbps: Some(2100),
            tbw_tb: Some(10000),
            dwpd: Some(1.0),
            enterprise: true,
            zns: true,
            generation: Some("2021".to_string()),
            notes: Some("WD Ultrastar DC ZN540 ZNS SSD".to_string()),
        });

        // Micron Enterprise
        self.fingerprints.push(DriveFingerprint {
            model_pattern: "9400".to_string(),
            vendor: "Micron".to_string(),
            seq_read_iops: None,
            seq_write_iops: None,
            rand_read_iops: Some(1_500_000),
            rand_write_iops: Some(400_000),
            seq_read_mbps: Some(7000),
            seq_write_mbps: Some(5000),
            tbw_tb: Some(56000),
            dwpd: Some(3.0),
            enterprise: true,
            zns: false,
            generation: Some("2022".to_string()),
            notes: Some("Micron 9400 Enterprise NVMe".to_string()),
        });

        // Seagate Enterprise HDD
        self.fingerprints.push(DriveFingerprint {
            model_pattern: "Exos X18".to_string(),
            vendor: "Seagate".to_string(),
            seq_read_iops: None,
            seq_write_iops: None,
            rand_read_iops: Some(170),
            rand_write_iops: Some(170),
            seq_read_mbps: Some(270),
            seq_write_mbps: Some(270),
            tbw_tb: None,
            dwpd: None,
            enterprise: true,
            zns: false,
            generation: Some("2021".to_string()),
            notes: Some("Seagate Exos X18 18TB Enterprise HDD".to_string()),
        });

        self.fingerprints.push(DriveFingerprint {
            model_pattern: "Exos X20".to_string(),
            vendor: "Seagate".to_string(),
            seq_read_iops: None,
            seq_write_iops: None,
            rand_read_iops: Some(170),
            rand_write_iops: Some(170),
            seq_read_mbps: Some(285),
            seq_write_mbps: Some(285),
            tbw_tb: None,
            dwpd: None,
            enterprise: true,
            zns: false,
            generation: Some("2022".to_string()),
            notes: Some("Seagate Exos X20 20TB Enterprise HDD".to_string()),
        });

        // WD Enterprise HDD
        self.fingerprints.push(DriveFingerprint {
            model_pattern: "Ultrastar DC HC550".to_string(),
            vendor: "Western Digital".to_string(),
            seq_read_iops: None,
            seq_write_iops: None,
            rand_read_iops: Some(170),
            rand_write_iops: Some(170),
            seq_read_mbps: Some(269),
            seq_write_mbps: Some(269),
            tbw_tb: None,
            dwpd: None,
            enterprise: true,
            zns: false,
            generation: Some("2021".to_string()),
            notes: Some("WD Ultrastar DC HC550 18TB".to_string()),
        });

        // WD Red (NAS)
        self.fingerprints.push(DriveFingerprint {
            model_pattern: "WD Red".to_string(),
            vendor: "Western Digital".to_string(),
            seq_read_iops: None,
            seq_write_iops: None,
            rand_read_iops: Some(150),
            rand_write_iops: Some(150),
            seq_read_mbps: Some(210),
            seq_write_mbps: Some(210),
            tbw_tb: None,
            dwpd: None,
            enterprise: false,
            zns: false,
            generation: None,
            notes: Some("WD Red NAS HDD".to_string()),
        });
    }
}

impl Default for FingerprintDatabase {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fingerprint_matching() {
        let db = FingerprintDatabase::new();

        // Should match Samsung 980 PRO
        let fp = db.lookup("Samsung 980 PRO 2TB").unwrap();
        assert_eq!(fp.vendor, "Samsung");
        assert!(!fp.enterprise);

        // Should match PM1733
        let fp = db.lookup("SAMSUNG MZWLJ3T2HBLS-00007 (PM1733)").unwrap();
        assert!(fp.enterprise);

        // Should match ZNS drive
        let fp = db.lookup("WD Ultrastar DC ZN540").unwrap();
        assert!(fp.zns);
    }

    #[test]
    fn test_performance_score() {
        let db = FingerprintDatabase::new();

        // Optane should have highest score
        let optane = db.lookup("Optane P5800").unwrap();
        assert!(optane.performance_score() >= 95);

        // HDD should have lower score
        let hdd = db.lookup("WD Red").unwrap();
        assert!(hdd.performance_score() <= 40);
    }

    #[test]
    fn test_endurance_score() {
        let db = FingerprintDatabase::new();

        // High-endurance enterprise drive
        let pm1735 = db.lookup("PM1735").unwrap();
        assert!(pm1735.endurance_score() >= 80);

        // Consumer drive
        let consumer = db.lookup("980 PRO").unwrap();
        assert!(consumer.endurance_score() <= 50);
    }
}

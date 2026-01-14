pub mod time;

use std::path::PathBuf;
use chrono::TimeZone;
use chrono_tz::Europe::Amsterdam;
use crate::config::CONFIG;

/// Converts Option<f32> to f32, returning 0.0 if None
pub fn default_to_zero(option: Option<f32>) -> f32 {
    if let Some(n) = option {
        n
    } else {
        0.0
    }
}

/// Converts String to f32, returning 0.0 if parsing fails
pub fn default_string_to_zero(string: String) -> f32 {
    match string.parse() {
        Ok(n) => n,
        Err(_) => 0.0,
    }
}

/// Converts String to i32, returning 0 if parsing fails
pub fn default_string_to_int(string: String) -> i32 {
    match string.parse() {
        Ok(n) => n,
        Err(_) => 0,
    }
}

/// Converts Option<String> to Option<f32>, returning None if parsing fails
pub fn default_to_zero_option(option: Option<String>) -> Option<f32> {
    if let Some(string) = option {
        match string.parse() {
            Ok(n) => Some(n),
            Err(_) => None,
        }
    } else {
        None
    }
}

/// Converts Option<String> to f32, returning 0.0 if None or if parsing fails
pub fn default_some_string_to_zero(option: Option<String>) -> f32 {
    if let Some(n) = option {
        n.parse().unwrap_or(0.0)
    } else {
        0.0
    }
}

/// Converts String to bool
/// Returns true if the string is "YES" (case insensitive), false otherwise
pub fn convert_string_bool(value: String) -> bool {
    value.to_uppercase() == "YES"
}

/// Searches for CSV files in a specific directory
/// Returns an empty vector if the directory does not exist
pub fn get_files(dir_name: &str) -> Vec<(PathBuf, String)> {
    let dir_path = format!("{}/{}", CONFIG.data.path, dir_name);
    
    match std::fs::read_dir(&dir_path) {
        Ok(entries) => {
            entries
                .filter_map(|res| {
                    res.ok().map(|e| (e.path(), e.file_name().into_string().unwrap()))
                })
                .collect()
        },
        Err(_) => {
            tracing::warn!("Directory not found: {}, skipping CSV import", dir_path);
            Vec::new()
        }
    }
}

/// Extracts start and end timestamps from filename
/// # Returns: Tuple (start_timestamp, end_timestamp)
pub fn get_time_from_file_name(filename: &str, pattern: &str, year_pattern: Option<&str>) -> (i64, i64) {
    let year: i32;
    let month: u32;

    if let Some(yp) = year_pattern {
        if filename.starts_with("0") {
            // Year format (e.g., "0_SETTLEMENT_PRICES_YEAR_2018")
            let split: Vec<&str> = filename.split(yp).collect();
            year = split[1].get(0..4).unwrap().parse().unwrap();
            month = 1;
        } else {
            // Month format (e.g., "1_SETTLEMENT_PRICES_MONTH_2025-01")
            let split: Vec<&str> = filename.split(pattern).collect();
            year = split[1].get(0..4).unwrap().parse().unwrap();
            month = split[1].get(5..7).unwrap().parse().unwrap();
        }
    } else {
        // Default month format (e.g., "MERIT_ORDER_LIST_MONTH_2025-11")
        let split: Vec<&str> = filename.split(pattern).collect();
        year = split[1].get(0..4).unwrap().parse().unwrap();
        month = split[1].get(5..7).unwrap().parse().unwrap();
    }

    let start_time = Amsterdam.with_ymd_and_hms(year, month, 1, 0, 0, 0);
    let end_time = Amsterdam.with_ymd_and_hms(
        if month < 12 { year } else { year + 1 }, 
        if month < 12 { month + 1 } else { 1 }, 
        1,
        0,
        0,
        0
    );

    (
        start_time.earliest().unwrap().timestamp(),
        end_time.earliest().unwrap().timestamp(),
    )
}

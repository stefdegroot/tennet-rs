use std::{io, path::PathBuf, fs};
use crate::config::CONFIG;
use anyhow::{anyhow, Result};
use chrono::{Duration, TimeZone};
use chrono_tz::Europe::Amsterdam;

pub fn get_files_from_data_folder (sub_path: &str) -> Result<Vec<(PathBuf, String, i64, i64)>> {

    let dir_path = if let Some(data_path) = &CONFIG.data.path {
        format!("{}/{}", data_path, sub_path)
    } else {
        return Err(anyhow!("Data path is not configured."));
    };

    let files = fs::read_dir(dir_path)?
        .map(|res|
            res.map(|e| {

                let file_name = e.file_name().into_string().unwrap();
                let (start, end) = get_time_from_file_name(sub_path, &file_name);

                (
                    e.path(),
                    file_name,
                    start,
                    end,
                )
            })
        )
        .collect::<Result<Vec<_>, io::Error>>()?;

    Ok(files)
}

pub fn get_time_from_file_name (base_path: &str, filename: &str) -> (i64, i64) {

    let match_path = format!("{}_", base_path.to_uppercase());
    let date_string = filename
        .replace(&match_path, "")
        .replace(".csv", "");
    let split: Vec<&str> = date_string.split("-").collect();

    let year: i32 = split.first().unwrap().parse().unwrap();
    let month: Option<u32> = split.get(1).map(|d| d.parse().unwrap());
    let day: Option<u32> = split.get(2).map(|d| d.parse().unwrap());

    if let Some(day) = day && let Some(month) = month {

        let start_time = Amsterdam.with_ymd_and_hms(year, month, day, 0, 0, 0);
        let end_time = start_time.earliest().unwrap() + Duration::days(1);

        (
            start_time.earliest().unwrap().timestamp(),
            end_time.timestamp(),
        )

    } else if let Some(month) = month {

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
    } else {

        let start_time = Amsterdam.with_ymd_and_hms(year, 1, 1, 0, 0, 0);
        let end_time = Amsterdam.with_ymd_and_hms(
            year + 1,
            1,
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
}


#[cfg(test)]
mod tests {

    use chrono::DateTime;

    use super::*;

    #[test]
    fn test_get_time_from_file_name () {

        let (start_time, end_time) = get_time_from_file_name("balance_delta_high_res", "BALANCE_DELTA_HIGH_RES_2025-07-01.csv");

        assert_eq!(DateTime::from_timestamp(start_time, 0).unwrap().to_rfc3339(),   "2025-06-30T22:00:00+00:00");
        assert_eq!(DateTime::from_timestamp(end_time, 0).unwrap().to_rfc3339(),     "2025-07-01T22:00:00+00:00");

        let (start_time, end_time) = get_time_from_file_name("balance_delta", "BALANCE_DELTA_2025-07.csv");

        assert_eq!(DateTime::from_timestamp(start_time, 0).unwrap().to_rfc3339(),   "2025-06-30T22:00:00+00:00");
        assert_eq!(DateTime::from_timestamp(end_time, 0).unwrap().to_rfc3339(),     "2025-07-31T22:00:00+00:00");

        let (start_time, end_time) = get_time_from_file_name("merit_order", "MERIT_ORDER_2025-07.csv");

        assert_eq!(DateTime::from_timestamp(start_time, 0).unwrap().to_rfc3339(),   "2025-06-30T22:00:00+00:00");
        assert_eq!(DateTime::from_timestamp(end_time, 0).unwrap().to_rfc3339(),     "2025-07-31T22:00:00+00:00");

        let (start_time, end_time) = get_time_from_file_name("settlement_prices", "SETTLEMENT_PRICES_2025.csv");

        assert_eq!(DateTime::from_timestamp(start_time, 0).unwrap().to_rfc3339(),   "2024-12-31T23:00:00+00:00");
        assert_eq!(DateTime::from_timestamp(end_time, 0).unwrap().to_rfc3339(),     "2025-12-31T23:00:00+00:00");
    }
}

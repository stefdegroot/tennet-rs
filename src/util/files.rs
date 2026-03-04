use std::{io, path::PathBuf, fs};
use crate::config::CONFIG;
use chrono::{Duration, TimeZone};
use chrono_tz::Europe::Amsterdam;

pub fn get_files_from_data_folder (sub_path: &str) -> io::Result<Vec<(PathBuf, String, i64, i64)>> {

    let dir_path = format!("{}/{}", CONFIG.data.path, sub_path);
    
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
    let month: u32 = split.get(1).unwrap().parse().unwrap();
    let day: Option<u32> = split.get(2).map(|d| d.parse().unwrap());

    if let Some(day) = day {

        let start_time = Amsterdam.with_ymd_and_hms(year, month, day, 0, 0, 0);
        let end_time = start_time.earliest().unwrap() + Duration::days(1);

        (
            start_time.earliest().unwrap().timestamp(),
            end_time.timestamp(),
        )

    } else {

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
}
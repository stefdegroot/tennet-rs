use serde::Deserialize;
use std::{io, path::PathBuf};
use chrono::{offset::LocalResult, TimeZone, DateTime, Utc};
use chrono_tz::Europe::Amsterdam;
use std::collections::HashSet;
use lazy_static::lazy_static;

use crate::{
    db::{
        merit_order,
        merit_order::MeritOrderRecord,
        PG_MAX_QUERY_PARAMS,
        RECORD_COLUMNS,
    },
    tennet::time::parse_tennet_time_stamp,
    AppState
};

#[derive(Deserialize, Debug)]
struct MeritOrderPointThreshold {
    capacity_threshold: f32,
    price_up: f32,
    price_down: f32,
}

#[derive(Deserialize, Debug)]
struct MeritOrderPoint {
    #[serde(rename="timeInterval_start")]
    pub time_interval_start: String,
    #[serde(rename="timeInterval_end")]
    pub time_interval_end: String,
    #[serde(rename="Thresholds")]
    pub thresholds: Vec<MeritOrderPointThreshold>,
}

#[derive(Deserialize, Debug)]
struct MeritOrderRow {
    #[serde(rename="Timeinterval Start Loc")]
    pub time_interval_start: String,
    // #[serde(rename="Timeinterval End Loc")]
    // pub time_interval_end: String,
    // #[serde(rename="Isp")]
    // pub sequence: i32,
    // #[serde(rename="Quantity Measurement Unit Name")]
    // pub quantity_unit_name: String,
    // #[serde(rename="Price Measurement Unit Name")]
    // pub price_unit_name: String,
    // #[serde(rename="Currency Unit Name")]
    // pub currency_unit_name: String,
    #[serde(rename="Capacity Threshold")]
    pub capacity_threshold: f32,
    #[serde(rename="Price Down")]
    pub price_down: Option<f32>,
    #[serde(rename="Price Up")]
    pub price_up:  Option<f32>,
}

lazy_static! {
    pub static ref FIRST_MERIT_ORDER_DATE: i64 = Amsterdam.with_ymd_and_hms(2018, 1, 1, 0, 0, 0).unwrap().timestamp();
}

pub async fn import_merit_order (app_state: AppState) {

    let latest_record = merit_order::get_latest(&app_state.db_client).await;
    let mut sync_from = 0;

    println!("{:?}", latest_record);

    if let Some(latest) = latest_record {
        sync_from = latest.time_stamp + 900;
    }

    let files = get_files().unwrap();
    
    for (path, name) in files {
        
        let (start_time, end_time) = get_time_from_file_name(&name);

        if sync_from > end_time {
            continue;
        }

        println!("importing: {:?}", name);

        import_csv(&app_state, path, sync_from).await;
    }
}

pub async fn sync_merit_order (app_state: & AppState) -> Vec<merit_order::MeritOrderList> {

    let lists = vec![];

    lists
}

fn get_files () -> io::Result<Vec<(PathBuf, String)>>  {

    let dir_path = format!("./data/merit_order");
    let files = std::fs::read_dir(dir_path)?
        .map(|res| res.map(|e| (e.path(), e.file_name().into_string().unwrap())))
        .collect::<Result<Vec<_>, io::Error>>()?;

    Ok(files)
}

fn get_time_from_file_name (filename: &String) -> (i64, i64) {

    let split: Vec<&str> = filename.split("MERIT_ORDER_LIST_MONTH_").collect();

    let year: i32 = split[1].get(0..4).unwrap().parse().unwrap();
    let month: u32 = split[1].get(5..7).unwrap().parse().unwrap();

    let start_time = Amsterdam.with_ymd_and_hms(year, month, 1, 0, 0, 0);
    let end_time = Amsterdam.with_ymd_and_hms(
        if month < 12 { year } else { year + 1 }, 
        if month < 12 { month + 1 } else { 1 }, 
        1,
        0,
        0,
        0
    );

    return (
        start_time.earliest().unwrap().timestamp(),
        end_time.earliest().unwrap().timestamp(),
    );
}

async fn import_csv (app_state: &AppState, path: PathBuf, sync_from: i64) {

    let mut records: Vec<MeritOrderRecord> = vec![];

    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .delimiter(b';')
        .trim(csv::Trim::Headers)
        .from_path(path).unwrap();

    let mut ambiguous_times = HashSet::new();

    for result in rdr.deserialize() {

        let row: MeritOrderRow = result.unwrap();

        let time =  match parse_tennet_time_stamp(&row.time_interval_start) {
            LocalResult::Single(t) => Some(t.to_utc()),
            LocalResult::Ambiguous(first, last) => {

                let mut time_stamp = first.to_utc();

                if !ambiguous_times.contains(&time_stamp) {
                    ambiguous_times.insert(time_stamp);
                } else {
                    time_stamp = last.to_utc();
                }

                Some(time_stamp)
            },
            LocalResult::None => None
        };

        if let Some(time_stamp) = time {

            let time_stamp = time_stamp.timestamp();

            if time_stamp < sync_from {
                continue;
            }

            records.push(MeritOrderRecord { 
                time_stamp,
                capacity_threshold: row.capacity_threshold,
                price_down: row.price_down,
                price_up: row.price_up,
            });
        }
    }

    for records_chunk in records.chunks(PG_MAX_QUERY_PARAMS / RECORD_COLUMNS) {
        let result = merit_order::insert_many(&app_state.db_client, records_chunk).await;
        println!("{:?}", result);
    }
}